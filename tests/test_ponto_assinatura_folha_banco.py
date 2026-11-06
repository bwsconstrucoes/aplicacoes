# -*- coding: utf-8 -*-
"""Ponto — a assinatura da folha na tela (06/11/2026, migração 027), com Postgres de verdade: o administrativo vê a
equipe do mês FECHADO, mostra a folha, a pessoa assina com o PIN DELA e o dedo; o PDF fica arquivado e abre no ERP; a
recusa fica registrada; reaberto o mês e mudada a folha, a assinatura vira substituída; e a pendência por obra."""
from __future__ import annotations

import base64
import datetime as dt
import io

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _entrar_no_app, _schema_ponto2, app,  # noqa: F401
                                           bater_via_chave, como, local, mundo)
from tests.test_ponto_papeis_banco import CPF_CARLOS, NA_OBRA_A, OBRA_A, _papeis, carlos  # noqa: F401

pytestmark = pytest.mark.banco


def _png() -> str:
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (300, 120), "white")
    ImageDraw.Draw(img).line([(10, 100), (80, 20), (150, 90), (290, 15)], fill="black", width=4)
    b = io.BytesIO()
    img.save(b, format="PNG")
    return "data:image/png;base64," + base64.b64encode(b.getvalue()).decode()


def _dia_do_mes_passado() -> dt.date:
    hoje = dt.date.today()
    comp = (hoje.replace(day=1) - dt.timedelta(days=1)).replace(day=1)
    d = comp.replace(day=27)
    while d.weekday() > 4:
        d -= dt.timedelta(days=1)
    return d


def test_a_folha_assinada_na_tela_do_comeco_ao_fim(app, mundo, carlos, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import alertas, assinaturas
    _papeis(app, mundo, carlos, administrativo_obra=True)
    dia = _dia_do_mes_passado()
    comp = dia.strftime("%Y-%m")
    for h in (7, 11, 12, 17):
        bater_via_chave(app, CPF_JOAO, local(dia, h, 0))
    bater_via_chave(app, CPF_MARIA, local(dia, 7, 0))                   # a Maria bateu na obra A também
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)                      # o João cria o PIN dele (481927)
    adm = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    dp = como(app, mundo["dp"])
    lista = lambda: adm.get(f"/ponto/app/api/assinaturas?competencia={comp}&{NA_OBRA_A}").get_json()  # noqa: E731
    aqui = {"latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10}
    url = f"/ponto/app/api/assinaturas/{mundo['joao']}"
    # o mês ainda aberto: a lista aparece, mas não se assina
    d = lista()
    assert d["fechado"] is False and {"João Obra A", "Maria Obra B"} <= {p["nome"] for p in d["pessoas"]}
    assert dt.date.fromisoformat(d["prazo"]).day == 20
    r = adm.post(url, json={"competencia": comp, "pin": "481927", "assinatura": _png(), **aqui})
    assert r.status_code == 400 and "não foi fechado" in r.get_json()["erro"]
    assert dp.post(f"/erp/api/ponto/competencias/{comp}/fechar").status_code == 200
    # a folha para mostrar
    f = adm.get(f"{url}/folha?competencia={comp}&{NA_OBRA_A}").get_json()
    assert f["pessoa"]["nome"] == "João Obra A" and any(x["batidas"] == ["07:00", "11:00", "12:00", "17:00"] for x in f["dias"])
    # o PIN é o DA PESSOA: errado, recusa; certo, assina
    assert adm.post(url, json={"competencia": comp, "pin": "000000", "assinatura": _png(), **aqui}).status_code == 401
    assert adm.post(url, json={"competencia": comp, "pin": "481927", "assinatura": "x", **aqui}).status_code == 400
    r = adm.post(url, json={"competencia": comp, "pin": "481927", "assinatura": _png(), **aqui})
    assert r.status_code == 201, r.get_json()
    assert len(r.get_json()["lacre"]) == 64
    assert adm.post(url, json={"competencia": comp, "pin": "481927", "assinatura": _png(), **aqui}).status_code == 400
    sit = {p["nome"]: p for p in lista()["pessoas"]}
    assert sit["João Obra A"]["situacao"] == "ASSINADA" and "Carlos" in (sit["João Obra A"]["por"] or "")
    # o PDF no ERP, na janela da pessoa
    ficha = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]
    assert ficha["assinaturas"][0]["situacao"] == "ASSINADA" and ficha["assinaturas"][0]["documento_id"]
    pdf = dp.get(f"/erp/api/ponto/documentos/{ficha['assinaturas'][0]['documento_id']}")
    assert pdf.status_code == 200 and pdf.get_data()[:4] == b"%PDF"
    # quem conduz não assina a própria por aqui; e a recusa da Maria fica registrada
    assert adm.post(f"/ponto/app/api/assinaturas/{mundo['maria']}/recusa", json={"competencia": comp, "motivo": "x", **aqui}
                    ).status_code == 400
    r = adm.post(f"/ponto/app/api/assinaturas/{mundo['maria']}/recusa",
                 json={"competencia": comp, "motivo": "disse que confere depois com o encarregado", **aqui})
    assert r.status_code == 201
    assert {p["nome"]: p["situacao"] for p in lista()["pessoas"]}["Maria Obra B"] == "RECUSADA"
    # a pendência por obra: com todos tratados, nada; antes, o alerta
    with db.conexao() as conn:
        hoje_dia_14 = dt.date.today().replace(day=14)
        assert not alertas._assinaturas_pendentes(conn, set(), hoje_dia_14 - dt.timedelta(days=1)).get("ASSINATURA_PENDENTE")
    # reaberto o mês e mudada a folha do João: a assinatura vira substituída, e ele volta a faltar
    assert dp.post(f"/erp/api/ponto/competencias/{comp}/reabrir",
                   json={"motivo": "faltou lançar uma batida do João"}).status_code == 200
    bater_via_chave(app, CPF_JOAO, local(dia - dt.timedelta(days=1), 7, 0))
    assert dp.post(f"/erp/api/ponto/competencias/{comp}/fechar").status_code == 200
    assert {p["nome"]: p["situacao"] for p in lista()["pessoas"]}["João Obra A"] == "FALTA"
    with db.conexao() as conn:
        pend = alertas._assinaturas_pendentes(conn, set(), dt.date.today().replace(day=14) - dt.timedelta(days=1))
        texto = db.um(conn, "SELECT mensagem FROM ponto.alertas WHERE codigo = 'ASSINATURA_PENDENTE'")
    assert pend.get("ASSINATURA_PENDENTE") == 1 and "Obra PG-A: 1 folha(s)" in texto["mensagem"]
    ficha = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]
    assert [a["situacao"] for a in ficha["assinaturas"]] == ["SUBSTITUIDA"]
    # quem não responde pela equipe não vê a lista
    assert joao.get(f"/ponto/app/api/assinaturas?competencia={comp}&{NA_OBRA_A}").status_code in (400, 403)


def test_o_pdf_da_folha_assinada():
    from app.apps.ponto.core import assinaturas
    esp = {"pessoa": {"nome": "João da Silva", "cpf": "52998224725", "obra": "PG-A"}, "competencia": "2026-10",
           "inicio": "2026-10-01", "fim": "2026-10-31", "resumo": {"trabalhado": 540, "extra": 0, "debito": 0, "faltas": 0},
           "dias": [{"data": "2026-10-27", "dia_semana": "terça", "situacao": "OK", "previsto": 540, "trabalhado": 540,
                     "batidas": [{"hora": "07:00", "status": "VALIDA"}, {"hora": "17:00", "status": "AJUSTADA"}]}]}
    png = base64.b64decode(_png().split(",", 1)[1])
    pdf = assinaturas.folha_assinada_pdf(esp, assinatura=png, quando=dt.datetime(2026, 11, 6, 15, 0, tzinfo=dt.timezone.utc),
                                         conduzida_por="Carlos", lacre_=assinaturas.lacre(esp))
    assert pdf[:4] == b"%PDF" and len(pdf) > 2000
    assert assinaturas.prazo(dt.date(2026, 12, 1)) == dt.date(2027, 1, 20)
    outro = {**esp, "resumo": {**esp["resumo"], "extra": 30}}
    assert assinaturas.lacre(esp) != assinaturas.lacre(outro)                     # mudou um número, muda o lacre
