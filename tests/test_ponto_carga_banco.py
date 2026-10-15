# -*- coding: utf-8 -*-
"""Ponto — o teste de carga na produção (15/10/2026): celulares aprovados repetem
o trabalho de uma batida SEM batida — nenhuma marcação, nenhum NSR, nenhuma foto
fica no banco —, e só com o teste ligado por quem configura o ponto."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import _png, _schema_ponto2, app, como, mundo  # noqa: F401
from tests.test_ponto_regras_0610_banco import _aparelho, _id_do_aparelho

pytestmark = pytest.mark.banco


def _contar(banco):
    with banco.connect() as conn:
        return (conn.execute(text("SELECT count(*) FROM ponto.marcacoes")).scalar(),
                conn.execute(text("SELECT count(*) FROM ponto.fotos")).scalar(),
                conn.execute(text("SELECT coalesce(max(nsr), 0) FROM ponto.marcacoes")).scalar())


def test_simula_a_batida_sem_gravar_nada_e_so_ligado(app, mundo, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import dispositivos
    c, h = _aparelho(app, "celular-do-teste-de-carga-0123456789")
    dp = como(app, mundo["dp"])
    # pendente: não serve
    assert c.post("/ponto/app/api/teste-carga", json={"foto_base64": _png()}, headers=h).status_code == 403
    with db.conexao() as conn:
        dispositivos.aprovar(conn, _id_do_aparelho(dp, "celular-do-teste-de-carga-0123456789"),
                             perfil="COMPARTILHADO", aprovado_por="teste", obras=[mundo["obra_a"]])
    assert c.get("/ponto/app/api/teste-carga", headers=h).get_json()["ligado"] is False
    desligado = c.post("/ponto/app/api/teste-carga", json={"foto_base64": _png()}, headers=h)
    assert desligado.status_code == 403 and "desligado" in desligado.get_json()["erro"]
    assert app.test_client().post("/ponto/app/api/teste-carga", json={}).status_code == 401   # sem aparelho
    # quem configura liga por 30 min
    assert dp.post("/erp/api/ponto/teste-carga", json={"minutos": 0}).status_code == 400
    ligado = dp.post("/erp/api/ponto/teste-carga", json={"minutos": 30}).get_json()
    assert ligado["ligado"] is True and ligado["resumo"]["simulacoes"] == 0
    antes = _contar(banco)
    for _ in range(3):
        r = c.post("/ponto/app/api/teste-carga", json={"foto_base64": _png()}, headers=h)
        assert r.status_code == 200, r.get_json()
        assert r.get_json()["ms_no_servidor"] > 0
    assert _contar(banco) == antes                       # nem marcação, nem NSR, nem foto
    ruim = c.post("/ponto/app/api/teste-carga", json={"foto_base64": "data:image/jpeg;base64,xx"}, headers=h)
    assert ruim.status_code == 400
    resumo = dp.get("/erp/api/ponto/teste-carga").get_json()["resumo"]
    assert resumo["simulacoes"] == 3 and resumo["aparelhos"] == 1 and resumo["erros"] == 1
    # desligado de novo: para
    dp.post("/erp/api/ponto/teste-carga", json={"desligar": True})
    assert c.post("/ponto/app/api/teste-carga", json={"foto_base64": _png()}, headers=h).status_code == 403
    # a página abre sem login
    assert app.test_client().get("/ponto/app/teste-carga/").status_code == 200


def test_so_quem_configura_o_ponto_liga(app, mundo):
    sup = como(app, mundo["sup"])
    assert sup.post("/erp/api/ponto/teste-carga", json={"minutos": 30}).status_code in (403, 404)
