# -*- coding: utf-8 -*-
"""Ponto — o recesso em lote (22/11/2026, o dono: "pode criar uma tela para o DP lançar o recesso em lote"). As
pessoas marcadas × os pares (dia de folga, dia de pagar): a prévia diz o que entra e o que fica de fora; gravar faz um
pedido de compensação aprovado por pessoa e por par."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import _FERIADOS_FIXOS, _schema_ponto2, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def _folga_e_sabado():
    """Um dia de semana (segunda a quinta) adiante, sem feriado, e o sábado da mesma semana."""
    from app.apps.ponto import horario
    d = horario.hoje() + dt.timedelta(days=3)
    while d.weekday() > 3 or (d.month, d.day) in _FERIADOS_FIXOS:
        d += dt.timedelta(days=1)
    return d, d + dt.timedelta(days=5 - d.weekday())


def test_o_recesso_em_lote_previa_e_gravacao(app, mundo):
    from app.apps.ponto import db
    dp = como(app, mundo["dp"])
    folga, sab = _folga_e_sabado()
    domingo = sab + dt.timedelta(days=1)
    pares = [{"folga": folga.isoformat(), "pagar": sab.isoformat()},
             {"folga": domingo.isoformat(), "pagar": (sab + dt.timedelta(days=7)).isoformat()},
             {"folga": "", "pagar": ""}]                                   # linha em branco: ignorada
    corpo = {"pessoas": [mundo["joao"], mundo["maria"]], "pares": pares, "motivo": "recesso de teste"}

    def pedidos():
        with db.conexao() as conn:
            return db.todos(conn, """SELECT colaborador_id, data_inicio, data_fim, dia_trabalhado, status, descricao
                                       FROM ponto.ocorrencias WHERE tipo = 'COMPENSACAO'
                                        AND colaborador_id = ANY(:ids) ORDER BY colaborador_id""",
                            ids=[mundo["joao"], mundo["maria"]])

    # a prévia: nada gravado; o domingo fica de fora, com o porquê
    r = dp.post("/erp/api/ponto/quadro/recesso", json=corpo).get_json()
    assert r["ok"] and r["simulado"] and r["entram"] == 2 and r["ficam_de_fora"] == 2, r
    joao = next(p for p in r["pessoas"] if p["colaborador_id"] == mundo["joao"])
    assert joao["pares"][0]["ok"] and joao["pares"][0]["nota"] == "folga de 9h00; o dia de pagar precisa render 9h00"
    assert not joao["pares"][1]["ok"] and "não é dia de trabalho" in joao["pares"][1]["erro"]
    assert pedidos() == []
    # gravar: um pedido aprovado por pessoa e por par que entra, de um dia só, com o combinado
    r = dp.post("/erp/api/ponto/quadro/recesso", json={**corpo, "simular": False}).get_json()
    assert r["ok"] and not r["simulado"] and r["entram"] == 2 and r["aprovadas"] == 2, r
    feitos = pedidos()
    assert [(p["data_inicio"], p["data_fim"], p["dia_trabalhado"], p["status"]) for p in feitos] == \
        [(folga, folga, sab, "APROVADA")] * 2
    assert feitos[0]["descricao"].startswith("Recesso: recesso de teste — folga em ")
    # o espelho já mostra a folga a pagar
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={folga:%Y-%m}").get_json()
    dia = next(d for d in esp["dias"] if d["data"] == folga.isoformat())
    assert dia["folga_paga_em"] == sab.isoformat()
    # de novo: já existe o pedido — fica de fora, sem duplicar
    r = dp.post("/erp/api/ponto/quadro/recesso", json={**corpo, "simular": False}).get_json()
    assert r["entram"] == 0 and "já existe um pedido" in r["pessoas"][0]["pares"][0]["erro"]
    assert len(pedidos()) == 2


def test_o_recesso_em_lote_recusa_o_que_nao_fecha(app, mundo):
    dp = como(app, mundo["dp"])
    folga, sab = _folga_e_sabado()
    url, gente = "/erp/api/ponto/quadro/recesso", [mundo["joao"]]
    um = lambda pares: dp.post(url, json={"pessoas": gente, "pares": pares})  # noqa: E731
    outra = folga + dt.timedelta(days=1)
    # o mesmo sábado pagando duas folgas: hora por hora não fecha
    r = um([{"folga": folga.isoformat(), "pagar": sab.isoformat()}, {"folga": outra.isoformat(), "pagar": sab.isoformat()}])
    assert r.status_code == 400 and "uma folga só" in r.get_json()["erro"]
    assert um([{"folga": folga.isoformat()}]).status_code == 400                       # par pela metade
    assert um([]).status_code == 400
    assert um([{"folga": folga.isoformat(), "pagar": folga.isoformat()}]).status_code == 400
    assert dp.post(url, json={"pessoas": [], "pares": [{"folga": folga.isoformat(), "pagar": sab.isoformat()}]}).status_code == 400
    # quem não é do DP não lança
    sup = como(app, mundo["sup"])
    assert sup.post(url, json={"pessoas": gente, "pares": [{"folga": folga.isoformat(), "pagar": sab.isoformat()}]}
                    ).status_code in (400, 403)
