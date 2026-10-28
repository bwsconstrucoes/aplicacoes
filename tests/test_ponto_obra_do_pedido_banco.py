# -*- coding: utf-8 -*-
"""Ponto — a obra escolhida à mão no ajuste (29/10/2026): o aviso quando a pessoa não bate nessa obra
ou ninguém bate nela há dias (core/obra_suspeita.py), e o DP corrigindo a obra do pedido que ainda
espera decisão, com a troca registrada (ocorrencias.trocar_obra, migração 016)."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from app.apps.ponto.core import obra_suspeita as os_
from tests.test_ponto_gestao_banco import (CPF_JOAO, _entrar_no_app, _schema_ponto2, _segunda_passada,  # noqa: F401
                                           app, bater_via_chave, como, local, mundo)

HOJE = dt.date(2026, 10, 29)


def test_os_dois_avisos_da_obra():
    a = os_.avisos(obra="BR-02", obras_da_pessoa={"AP-01": 40, "AP-03": 2}, ultima_da_obra=HOJE, hoje=HOJE)
    assert [x["codigo"] for x in a] == ["OBRA_ESTRANHA"] and "AP-01, AP-03" in a[0]["texto"]
    a = os_.avisos(obra="AP-01", obras_da_pessoa={"AP-01": 40}, ultima_da_obra=HOJE - dt.timedelta(days=20), hoje=HOJE)
    assert [x["codigo"] for x in a] == ["OBRA_PARADA"] and "há 20 dias" in a[0]["texto"]
    assert os_.avisos(obra="AP-09", obras_da_pessoa={}, ultima_da_obra=None, hoje=HOJE)[0]["codigo"] == "OBRA_PARADA"
    # quem não bateu nada no período (pessoa nova) não ganha o aviso de "não bate nesta obra"
    assert os_.avisos(obra="AP-01", obras_da_pessoa={}, ultima_da_obra=HOJE, hoje=HOJE) == []
    assert os_.avisos(obra="AP-01", obras_da_pessoa={"AP-01": 3}, ultima_da_obra=HOJE - dt.timedelta(days=7),
                      hoje=HOJE) == []



@pytest.mark.banco
def test_o_aplicativo_avisa_a_obra_em_que_a_pessoa_nao_bate(app, mundo, monkeypatch):
    from app.apps.ponto import horario
    ontem = horario.hoje() - dt.timedelta(days=1)
    bater_via_chave(app, CPF_JOAO, local(ontem, 7, 0))                    # o João bate na PG-A
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    assert c.get("/ponto/app/api/obra-aviso?obra=PG-A").get_json()["avisos"] == []
    b = c.get("/ponto/app/api/obra-aviso?obra=PG-B").get_json()["avisos"]
    assert {a["codigo"] for a in b} == {"OBRA_ESTRANHA", "OBRA_PARADA"}    # ninguém bateu na PG-B
    # o de outra pessoa só com o alcance da consulta
    assert c.get(f"/ponto/app/api/obra-aviso?obra=PG-A&pessoa={mundo['maria']}").status_code == 404


@pytest.mark.banco
def test_o_dp_corrige_a_obra_do_pedido_antes_de_decidir(app, mundo, banco):
    seg = _segunda_passada()
    bater_via_chave(app, CPF_JOAO, local(seg, 7, 0))
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"], "data_inicio": seg.isoformat(),
        "horario": f"{seg.isoformat()}T17:00:00", "obra": "PG-B", "descricao": "esqueceu de bater"})
    assert r.status_code == 201, r.get_json()
    pid = r.get_json()["ocorrencia"]["id"]
    url = f"/erp/api/ponto/ocorrencias/{pid}/obra"
    motivo = "escolheu a PG-B por engano, as batidas do dia são na PG-A"
    # nas Validações, o ajuste vem marcado: a obra pedida não é onde ele bate
    v = dp.get("/erp/api/ponto/validacoes").get_json()
    item = next(i for i in v["itens"] if i["categoria"] == "PEDIDO" and i["id"] == pid)
    assert "AJ_OBRA_ESTRANHA" in item["motivos"]
    # só o DP, com motivo, e para uma obra diferente
    assert sup.post(url, json={"obra": "PG-A", "motivo": motivo}).status_code in (403, 404)
    assert dp.post(url, json={"obra": "PG-A", "motivo": "curto"}).status_code == 400
    assert dp.post(url, json={"obra": "PG-B", "motivo": motivo}).status_code == 400
    p = dp.post(url, json={"obra": "PG-A", "motivo": motivo})
    assert p.status_code == 200, p.get_json()
    ped = p.get_json()["pedido"]
    assert ped["obra_pedida"]["codigo"] == "PG-A"
    assert ped["alteracoes"][0]["de"] == "PG-B" and ped["alteracoes"][0]["para"] == "PG-A"
    assert ped["alteracoes"][0]["motivo"] == motivo
    # aprovado, a batida entra na obra corrigida; e o pedido aprovado não se corrige mais
    assert dp.post(ped["decidir_em"], json={"aprovar": True}).status_code == 200
    with banco.connect() as conn:
        obra = conn.execute(text("SELECT o.codigo FROM ponto.marcacoes m JOIN obras o ON o.id = m.obra_id "
                                 "WHERE m.status = 'AJUSTADA' AND m.colaborador_id = :c"), {"c": mundo["joao"]}).scalar()
    assert obra == "PG-A"
    assert dp.post(url, json={"obra": "PG-B", "motivo": motivo}).status_code == 400
