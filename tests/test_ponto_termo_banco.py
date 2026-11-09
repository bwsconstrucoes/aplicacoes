# -*- coding: utf-8 -*-
"""Ponto — o termo do ponto eletrônico assinado no celular (07/11/2026, migração 028), com Postgres de verdade: a
pessoa assina no próprio celular com o PIN dela; o operador da obra mostra o termo a quem falta e a pessoa assina com o
PIN ou, sem PIN, com a foto na hora; a recusa fica registrada; o DP vê a coluna na lista de Pessoas, a ficha com o PDF
e o alerta por obra; e, mudado o texto, todos voltam a ter a pendência."""
from __future__ import annotations

import base64
import datetime as dt
import io

import pytest

from tests.test_ponto_assinatura_folha_banco import _png
from tests.test_ponto_gestao_banco import (CPF_JOAO, _entrar_no_app, _schema_ponto2, app,  # noqa: F401
                                           bater_via_chave, como, local, mundo)
from tests.test_ponto_papeis_banco import CPF_CARLOS, NA_OBRA_A, OBRA_A, _papeis, carlos  # noqa: F401

pytestmark = pytest.mark.banco


def _jpeg() -> str:
    from PIL import Image
    img = Image.new("RGB", (480, 640), (180, 140, 120))
    b = io.BytesIO()
    img.save(b, format="JPEG", quality=80)
    return "data:image/jpeg;base64," + base64.b64encode(b.getvalue()).decode()


def test_o_termo_do_ponto_do_comeco_ao_fim(app, mundo, carlos, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import alertas, termo
    _papeis(app, mundo, carlos, administrativo_obra=True)
    hoje = dt.date.today()
    bater_via_chave(app, CPF_JOAO, local(hoje, 7, 0))
    from tests.test_ponto_gestao_banco import CPF_MARIA
    bater_via_chave(app, CPF_MARIA, local(hoje, 7, 0))
    aqui = {"latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10}
    dp = como(app, mundo["dp"])
    # quem é admitido já nasce com a pendência: o alerta por obra diz quantos e quem
    with db.conexao() as conn:
        assert {mundo["joao"], mundo["maria"]} <= {p["id"] for p in termo.pendentes(conn)}
        assert alertas._termos_pendentes(conn, set(), hoje).get("TERMO_PENDENTE")
        msgs = " ".join(a["mensagem"] for a in db.todos(conn, "SELECT mensagem FROM ponto.alertas WHERE codigo = 'TERMO_PENDENTE'"))
    assert "sem o termo do ponto assinado" in msgs and "João" in msgs
    # 1) NO CELULAR DA PESSOA: o aviso no começo da tela; o PIN dela de novo e o dedo
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    assert joao.get("/ponto/app/api/inicio").get_json()["pessoa"]["termo_pendente"] is True
    d = joao.get("/ponto/app/api/termo").get_json()
    assert d["situacao"]["situacao"] == "FALTA" and len(d["texto"]["itens"]) == len(termo.ITENS)
    assert joao.post("/ponto/app/api/termo", json={"assinatura": _png(), **aqui}).status_code == 400   # sem PIN
    assert joao.post("/ponto/app/api/termo", json={"pin": "000000", "assinatura": _png(), **aqui}).status_code == 401
    assert joao.post("/ponto/app/api/termo", json={"pin": "481927", "assinatura": "x", **aqui}).status_code == 400
    r = joao.post("/ponto/app/api/termo", json={"pin": "481927", "assinatura": _png(), **aqui})
    assert r.status_code == 201, r.get_json()
    assert joao.get("/ponto/app/api/inicio").get_json()["pessoa"]["termo_pendente"] is False
    assert joao.post("/ponto/app/api/termo", json={"pin": "481927", "assinatura": _png(), **aqui}).status_code == 400
    # 2) NO APARELHO DE QUEM CONDUZ: a lista da equipe, quem falta primeiro
    adm = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    lista = lambda: {p["nome"]: p for p in adm.get(f"/ponto/app/api/termos?{NA_OBRA_A}").get_json()["pessoas"]}  # noqa: E731
    sit = lista()
    assert sit["João Obra A"]["situacao"] == "ASSINADO" and sit["Maria Obra B"]["situacao"] == "FALTA"
    assert sit["Maria Obra B"]["tem_pin"] is False
    url = f"/ponto/app/api/termos/{mundo['maria']}"
    # a recusa fica registrada, com o motivo e quem conduziu
    assert adm.post(f"{url}/recusa", json={"motivo": "x", **aqui}).status_code == 400
    assert adm.post(f"{url}/recusa", json={"motivo": "quer ler com a família antes", **aqui}).status_code == 201
    assert lista()["Maria Obra B"]["situacao"] == "RECUSADO"
    # mudou de ideia: sem PIN, precisa da foto
    assert adm.post(url, json={"assinatura": _png(), **aqui}).status_code == 400
    r = adm.post(url, json={"assinatura": _png(), "foto": _jpeg(), **aqui})
    assert r.status_code == 201, r.get_json()
    assert lista()["Maria Obra B"]["situacao"] == "ASSINADO"
    # quem conduz não assina o próprio por aqui
    assert adm.post(f"/ponto/app/api/termos/{carlos}", json={"assinatura": _png(), "foto": _jpeg(), **aqui}
                    ).status_code in (400, 404)
    # 3) NO ERP: a ficha com o PDF e o histórico; a coluna da lista de Pessoas
    ficha = dp.get(f"/erp/api/ponto/pessoas/{mundo['maria']}").get_json()["pessoa"]["termo"]
    assert ficha["situacao"] == "ASSINADO" and ficha["com_pin"] is False and ficha["forma"] == "CONDUZIDA"
    assert [h["situacao"] for h in ficha["historico"]] == ["ASSINADO", "SUBSTITUIDO"]
    pdf = dp.get(f"/erp/api/ponto/documentos/{ficha['documento_id']}")
    assert pdf.status_code == 200 and pdf.get_data()[:4] == b"%PDF"
    pessoas = {p["nome"]: p for p in dp.get("/erp/api/ponto/pessoas").get_json()["pessoas"]}
    assert pessoas["João Obra A"]["termo"]["situacao"] == "ASSINADO"
    assert pessoas["João Obra A"]["termo"]["forma"] == "PELA_PESSOA"
    with db.conexao() as conn:
        assert not {mundo["joao"], mundo["maria"]} & {p["id"] for p in termo.pendentes(conn)}
    # 4) MUDOU O TEXTO: todos voltam a ter a pendência; o antigo fica guardado
    monkeypatch.setattr(termo, "ITENS", termo.ITENS + ("Um item novo.",))
    assert joao.get("/ponto/app/api/termo").get_json()["situacao"]["versao_antiga"] is True
    assert joao.get("/ponto/app/api/inicio").get_json()["pessoa"]["termo_pendente"] is True
    assert joao.post("/ponto/app/api/termo", json={"pin": "481927", "assinatura": _png(), **aqui}).status_code == 201
    hist = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]["termo"]["historico"]
    assert [h["situacao"] for h in hist] == ["ASSINADO", "SUBSTITUIDO"]


def test_o_pdf_do_termo_e_o_lacre():
    from app.apps.ponto.core import termo
    png = base64.b64decode(_png().split(",", 1)[1])
    foto = base64.b64decode(_jpeg().split(",", 1)[1])
    pdf = termo.termo_pdf({"nome": "Maria da Silva", "cpf": "52998224725"}, assinatura=png, foto=foto,
                          quando=dt.datetime(2026, 11, 7, 15, 0, tzinfo=dt.timezone.utc), forma="CONDUZIDA",
                          com_pin=False, conduzida_por="Carlos", latitude=-3.7, longitude=-38.5, dispositivo_id=3)
    assert pdf[:4] == b"%PDF" and len(pdf) > 3000
    assert len(termo.lacre()) == 64
    # curto e simples (o dono: "o nível de instrução das pessoas é baixo")
    assert len(termo.ITENS) <= 7 and all(len(i) < 220 for i in termo.ITENS)
