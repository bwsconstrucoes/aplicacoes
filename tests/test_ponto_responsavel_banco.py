# -*- coding: utf-8 -*-
"""Ponto — o RESPONSÁVEL pelo aparelho (pedidos do dono, 07/10/2026), com
Postgres de verdade: todo aparelho tem um; no ponto da obra só ele entra no
"Meu ponto"; no de equipe ele entra no grupo; a tela de alterar abre com o
grupo preenchido; e quando ele é desligado, o aparelho para."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _entrar_no_app,  # noqa: F401
                                           _schema_ponto2, app, como, mundo)
from tests.test_ponto_regras_0610_banco import _aparelho, _id_do_aparelho

pytestmark = pytest.mark.banco


def test_todo_aparelho_pede_o_cpf_do_responsavel(app, mundo):
    dp = como(app, mundo["dp"])
    _aparelho(app, "ponto-da-obra-sem-responsavel-012")
    t = _id_do_aparelho(dp, "ponto-da-obra-sem-responsavel-012")
    r = dp.post(f"/erp/api/ponto/dispositivos/{t}/aprovar", json={"perfil": "COMPARTILHADO", "obras": ["PG-A"]})
    assert r.status_code == 400 and "responsável" in r.get_json()["erro"]
    r = dp.post(f"/erp/api/ponto/dispositivos/{t}/aprovar",
                json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": "39053344705"})
    assert r.status_code == 400 and "não está no cadastro" in r.get_json()["erro"]
    j = dp.post(f"/erp/api/ponto/dispositivos/{t}/aprovar",
                json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).get_json()["dispositivo"]
    assert j["dono"]["id"] == mundo["maria"]
    # Alterar de novo sem redigitar: o responsável fica o mesmo
    j = dp.post(f"/erp/api/ponto/dispositivos/{t}/aprovar",
                json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "descricao": "Celular do Zé"}).get_json()
    assert j["dispositivo"]["dono"]["id"] == mundo["maria"]


def test_alterar_abre_com_responsavel_e_grupo_e_o_responsavel_entra_no_grupo(app, mundo):
    """O defeito de 07/10/2026: "coloquei o CPF do dono e o dos dois do grupo,
    e quando abro de novo não aparecem"."""
    dp = como(app, mundo["dp"])
    _aparelho(app, "ponto-de-equipe-com-grupo-012345")
    g = _id_do_aparelho(dp, "ponto-de-equipe-com-grupo-012345")
    r = dp.post(f"/erp/api/ponto/dispositivos/{g}/aprovar",
                json={"perfil": "LISTA", "autorizados": [CPF_JOAO], "cpf": CPF_MARIA})
    assert r.status_code == 200, r.get_json()
    d = dp.get(f"/erp/api/ponto/dispositivos/{g}").get_json()["dispositivo"]
    assert d["perfil"] == "LISTA" and d["dono"]["id"] == mundo["maria"]
    assert sorted(p["cpf"].replace(".", "").replace("-", "") for p in d["autorizados"]) == sorted([CPF_JOAO,
                                                                                                    CPF_MARIA])
    assert dp.get("/erp/api/ponto/dispositivos/999999").status_code == 404


def test_no_ponto_da_obra_so_o_responsavel_entra_no_meu_ponto(app, mundo, monkeypatch):
    """Pergunta do dono, 07/10/2026: "essa pessoa, como é que ela faz se ela
    quiser ver o ponto individual dela?" — entra pelo próprio aparelho, que
    continua sendo o ponto da obra para os outros."""
    _entrar_no_app(app, CPF_JOAO, monkeypatch)                   # PIN 481927 do João
    _entrar_no_app(app, CPF_MARIA, monkeypatch)                  # e o da Maria
    dp = como(app, mundo["dp"])
    c, h = _aparelho(app, "ponto-da-obra-da-maria-0123456789")
    t = _id_do_aparelho(dp, "ponto-da-obra-da-maria-0123456789")
    assert dp.post(f"/erp/api/ponto/dispositivos/{t}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    r = c.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=h)
    assert r.status_code == 403 and "ponto da obra" in r.get_json()["erro"]
    r = c.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"}, headers=h)
    assert r.status_code == 200, r.get_json()
    assert c.get("/ponto/app/api/eu").get_json()["primeiro_nome"] == "Maria"


def test_responsavel_desligado_para_o_ponto_da_obra(app, mundo, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import desligamentos
    dp = como(app, mundo["dp"])
    _aparelho(app, "ponto-da-obra-do-desligado-012345")
    t = _id_do_aparelho(dp, "ponto-da-obra-do-desligado-012345")
    dp.post(f"/erp/api/ponto/dispositivos/{t}/aprovar",
            json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_JOAO})
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET situacao = 'DESLIGADO' WHERE id = :j"), {"j": mundo["joao"]})
        conn.commit()
    with db.conexao() as conn:
        assert desligamentos.aplicar(conn)["celulares"] == 1
    a = next(a for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"] if a["id"] == t)
    assert a["status"] == "BLOQUEADO" and "responsável desligado" in a["motivo_bloqueio"]
