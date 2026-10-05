# -*- coding: utf-8 -*-
"""Ponto — o modo de teste (pedido do dono, 05/10/2026: "queria fazer teste
comigo mesmo (…) gravar uma geolocalização e colocar meu nome e CPF"). A obra
de teste vale mesmo com a C. Diários como base; a pessoa de teste bate como
ativa mesmo fora do Registro; o código de primeiro acesso aparece na tela só
para pessoa de teste. Postgres de verdade."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, app, como,  # noqa: F401
                                           mundo)
from tests.test_ponto_registro_banco import _cpf

pytestmark = pytest.mark.banco

CPF_DONO = _cpf("246813579")
AQUI = (-3.740000, -38.480000)


@pytest.fixture
def com_planilha_e_registro(banco_analisesps, mundo):
    """A situação de produção: obras da C. Diários e pessoas do Registro."""
    from app.apps.ponto import db
    from app.apps.ponto.core import base_obras, parametros, registro
    from tests.test_ponto_base_obras_banco import _aba
    with db.conexao() as conn:
        base_obras.gravar(conn, base_obras.interpretar(_aba()), "teste")
        parametros.gravar(conn, registro.PARAMETRO_FONTE, registro.FONTE_REGISTRO, "teste")
    registro.esquecer()
    base_obras.esquecer()
    yield
    registro.esquecer()
    base_obras.esquecer()


@pytest.fixture
def limpar_teste(banco):
    yield
    with banco.connect() as conn:
        ids = "SELECT id FROM colaboradores WHERE cpf = :c"
        for t in ("ponto.marcacoes", "ponto.colaborador_obras", "ponto.codigos_acesso",
                  "ponto.dispositivos", "ponto.colaborador_config"):
            conn.execute(text(f"DELETE FROM {t} WHERE colaborador_id IN ({ids})"), {"c": CPF_DONO})
        conn.execute(text("DELETE FROM colaboradores WHERE cpf = :c"), {"c": CPF_DONO})
        conn.execute(text("DELETE FROM ponto.obra_config WHERE obra_id IN "
                          "(SELECT id FROM obras WHERE codigo = 'TESTE-PONTO')"))
        conn.execute(text("DELETE FROM obras WHERE codigo = 'TESTE-PONTO'"))
        conn.commit()


def test_modo_de_teste_de_ponta_a_ponta(app, mundo, com_planilha_e_registro, limpar_teste):
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    assert sup.get("/erp/api/ponto/ensaio").status_code == 403
    assert dp.get("/erp/api/ponto/ensaio").get_json()["obra"] is None
    assert dp.post("/erp/api/ponto/ensaio/pessoa", json={"nome": "X", "cpf": CPF_DONO,
                                                         "celular": "85999990000"}).status_code == 400

    r = dp.post("/erp/api/ponto/ensaio/obra", json={"latitude": AQUI[0], "longitude": AQUI[1],
                                                    "raio_metros": 150}).get_json()
    assert r["obra"]["ligada"] is True and r["obra"]["raio_metros"] == 150
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    with db.conexao() as conn:
        assert "TESTE-PONTO" in {o["codigo"] for o in cadastros.listar_obras(conn)}   # fora da planilha
        assert db.um(conn, "SELECT status FROM public.obras WHERE codigo = 'TESTE-PONTO'")["status"] == "TESTE_PONTO"

    r = dp.post("/erp/api/ponto/ensaio/pessoa", json={"nome": "Dono em Teste", "cpf": CPF_DONO,
                                                      "celular": "(85) 98888-7777"}).get_json()
    p = r["pessoas"][0]
    assert p["nome"] == "Dono em Teste" and p["bate_no_celular"] is True and p["tem_pin"] is False
    with db.conexao() as conn:
        eu = cadastros.colaborador_por_cpf(conn, CPF_DONO)
    assert eu["situacao"] == "ATIVO" and eu["obra_codigo"] == "TESTE-PONTO"     # fora do Registro, mas ativo

    # Primeiro acesso: pede no celular, a gestão mostra o código, o celular cria o PIN
    cel = app.test_client()
    assert cel.post("/ponto/app/api/codigo", json={"cpf": CPF_DONO}).status_code == 200
    codigo = dp.post(f"/erp/api/ponto/ensaio/pessoa/{p['id']}/codigo").get_json()["codigo"]
    assert len(codigo) == 6
    assert cel.post("/ponto/app/api/pin", json={"cpf": CPF_DONO, "codigo": codigo,
                                                "pin": "481927"}).status_code == 200
    assert dp.post(f"/erp/api/ponto/ensaio/pessoa/{mundo['joao']}/codigo").status_code == 404

    # O celular: registra, a gestão aprova, bate dentro da cerca de teste
    uuid = "celular-do-dono-em-teste-0123456"
    token = cel.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    cel.post("/ponto/app/api/aparelho/identificar", json={}, headers=h)
    item = next(i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["tipo"] == "APARELHO")
    assert dp.post(f"/erp/api/ponto/dispositivos/{item['id']}/aprovar",
                   json={"perfil": "INDIVIDUAL", "cpf": CPF_DONO}).status_code == 200
    ok = cel.post("/ponto/app/api/bater", json={"latitude": AQUI[0] + 0.0003, "longitude": AQUI[1],
                                                "precisao": 15}, headers=h)
    assert ok.status_code == 201, ok.get_json()
    assert ok.get_json()["comprovante"]["empregado"] == "Dono em Teste"
    longe = cel.post("/ponto/app/api/bater", json={"latitude": AQUI[0] + 0.05, "longitude": AQUI[1],
                                                   "precisao": 15}, headers=h)
    assert longe.status_code == 403
    assert dp.get("/erp/api/ponto/ensaio").get_json()["pessoas"][0]["batidas"] == 1

    # Desligar: a obra de teste sai do ponto
    assert dp.post("/erp/api/ponto/ensaio/desligar").get_json()["obra"]["ligada"] is False
    with db.conexao() as conn:
        assert "TESTE-PONTO" not in {o["codigo"] for o in cadastros.listar_obras(conn)}
