# -*- coding: utf-8 -*-
"""Ponto — A DECLARAÇÃO DE COMPARECIMENTO (27/10/2026). O dono: "o cara vai no hospital,
faz uma consulta rápida, não é dado atestado, entrega a declaração e falta o resto do
dia (…) o DP precisa decidir se vai abonar a totalidade das horas, só o período, ou não
vai abonar nada". Postgres de verdade; o mundo de test_ponto_gestao_banco."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, dia_util, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def _declaracao(dp, mundo, dia, ini="08:00", fim="10:00"):
    return dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "DECLARACAO", "colaborador_id": mundo["maria"],
                   "data_inicio": dia.isoformat(), "hora_inicio": ini, "hora_fim": fim,
                   "descricao": "consulta no posto de saúde"})


def _dia(dia, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import espelho
    with db.conexao() as conn:
        return espelho.montar(conn, mundo["maria"], dia, dia)["dias"][0]


def test_a_declaracao_e_o_abono_que_o_dp_escolhe(app, mundo):
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    assert _declaracao(dp, mundo, seg, "10:00", "08:00").status_code == 400          # fim antes do início
    r = _declaracao(dp, mundo, seg)
    assert r.status_code == 201, r.get_json()
    o = r.get_json()["ocorrencia"]
    assert o["declaracao"] and o["declaracao_periodo"] == "08:00 às 10:00" and o["minutos"] == 120
    assert "falta decidir" in o["rotulo"]
    url = f"/erp/api/ponto/afastamentos/{o['id']}/etapa-dp"
    # sem dizer o abono, não aprova
    r = dp.post(url, json={"aprovar": True})
    assert r.status_code == 400 and "abona" in r.get_json()["erro"]
    previsto = _dia(seg, mundo)["previsto"]
    assert dp.post(url, json={"aprovar": True, "abono": "PERIODO"}).status_code == 200
    d = _dia(seg, mundo)
    assert d["situacao"] == "FALTA" and d["debito"] == previsto - 120 and d["declaracoes"][0]["minutos"] == 120
    assert dp.get(f"/erp/api/ponto/ocorrencias/{o['id']}").get_json()["pedido"]["rotulo"].endswith("só o período")
    # o dia todo
    ter = dia_util(seg, 1)
    o2 = _declaracao(dp, mundo, ter).get_json()["ocorrencia"]
    assert dp.post(f"/erp/api/ponto/afastamentos/{o2['id']}/etapa-dp", json={"aprovar": True, "abono": "TOTAL"}).status_code == 200
    assert _dia(ter, mundo)["situacao"] == "ABONADO"
    # nada: fica registrada, o dia continua falta inteira
    qua = dia_util(seg, 2)
    o3 = _declaracao(dp, mundo, qua).get_json()["ocorrencia"]
    assert dp.post(f"/erp/api/ponto/afastamentos/{o3['id']}/etapa-dp", json={"aprovar": True, "abono": "NADA"}).status_code == 200
    d = _dia(qua, mundo)
    assert d["situacao"] == "FALTA" and d["debito"] == d["previsto"] and d["declaracoes"][0]["subtipo"] == "DECL_NADA"
    # o relatório de ausências justificadas diz as horas, não dias
    rel = dp.post("/erp/api/ponto/relatorios", json={"tipo": "justificadas", "de": seg.isoformat(),
                                                    "ate": qua.isoformat()}).get_json()
    por = {l["pedido_id"]: l for l in rel["linhas"]}
    assert por[o["id"]]["dias"] == 0 and por[o["id"]]["horas"] == 120 and por[o2["id"]]["dias"] == 1
    assert por[o3["id"]]["horas"] == 0
    # o DP lançando já aprovado escolhe na hora
    qui = dia_util(seg, 3)
    r = dp.post("/erp/api/ponto/afastamentos", json={"tipo": "DECLARACAO", "colaborador_id": mundo["maria"],
                "data_inicio": qui.isoformat(), "hora_inicio": "14:00", "hora_fim": "15:30", "abono": "PERIODO",
                "aprovar_ja": True})
    assert r.status_code == 201 and r.get_json()["ocorrencia"]["status"] == "APROVADA", r.get_json()
