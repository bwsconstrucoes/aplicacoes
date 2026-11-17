# -*- coding: utf-8 -*-
"""Ponto — o PAINEL (11/11/2026, core/indicadores.py): os números de um período e a comparação com outro — batidas,
corretas e corrigidas, funcionários batendo, dias de trabalho, atestados, as batidas dia a dia e as obras com a
tendência (melhorou, manteve, piorou). Contra um Postgres de verdade."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, app, como, mundo  # noqa: F401
from tests.test_ponto_gestao_banco import CPF_JOAO, CPF_MARIA
from tests.test_ponto_qualidade_banco import _bater, _conta_nova  # noqa: F401

pytestmark = pytest.mark.banco


def test_regras_puras_da_tendencia_e_do_periodo():
    from app.apps.ponto import horario
    from app.apps.ponto.core import indicadores as ind
    assert ind.tendencia(80, 90) == "MELHOROU" and ind.tendencia(90, 80) == "PIOROU"
    assert ind.tendencia(80, 83) == "MANTEVE" and ind.tendencia(83, 80) == "MANTEVE"
    assert ind.tendencia(None, 70) == "NOVA" and ind.tendencia(70, None) == "SEM_DADOS"
    hoje = horario.hoje()
    de, ate = ind.periodo_valido(hoje, hoje - dt.timedelta(days=5))      # desvira e corta em ontem
    assert de == hoje - dt.timedelta(days=5) and ate == hoje - dt.timedelta(days=1)
    from app.apps.ponto.erros import ErroDeValidacao
    with pytest.raises(ErroDeValidacao):
        ind.periodo_valido(hoje - dt.timedelta(days=200), hoje)
    a = {"obras": {"A": {"pontos": 90}, "B": {"pontos": 60}, "C": {"pontos": 75}}}
    b = {"obras": {"A": {"pontos": 70}, "B": {"pontos": 80}, "D": {"pontos": 50}}}
    linhas = {x["obra"]: x for x in ind.comparar(a, b)}
    assert linhas["A"]["tendencia"] == "MELHOROU" and linhas["A"]["diferenca"] == 20
    assert linhas["B"]["tendencia"] == "PIOROU" and linhas["C"]["tendencia"] == "NOVA"
    assert linhas["D"]["tendencia"] == "SEM_DADOS"
    assert ind.comparar(a, b)[0]["obra"] == "B"                          # as que pioraram primeiro


def test_o_painel_compara_dois_periodos(app, mundo):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import feriados
    hoje = horario.hoje()
    fim_b = hoje.replace(day=1) - dt.timedelta(days=1)                   # o mês passado inteiro é o "antes"
    inicio_b = fim_b.replace(day=1)
    with db.conexao() as conn:
        cal = feriados.Calendario(conn, inicio_b, fim_b)
        uteis = [d for d in (inicio_b + dt.timedelta(days=i) for i in range((fim_b - inicio_b).days + 1))
                 if d.weekday() < 4 and not cal.feriado(d, mundo["obra_a"])]
    dia1, dia2 = uteis[-2], uteis[-1]
    for h, m in ((7, 0), (11, 0), (12, 0), (17, 0)):                     # o João, dois dias inteiros
        _bater(CPF_JOAO, dia1, h, m)
        _bater(CPF_JOAO, dia2, h, m)
    _bater(CPF_MARIA, dia2, 7, 0, obra="PG-B")                           # a Maria, uma batida só
    dp = como(app, mundo["dp"])
    r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "ATESTADO", "colaborador_id": mundo["maria"],
                "data_inicio": dia1.isoformat(), "data_fim": dia1.isoformat(), "descricao": "consulta",
                "cid": "J11"})
    assert r.status_code in (200, 201), r.get_json()
    oid = r.get_json().get("ocorrencia", r.get_json()).get("id")
    if oid:
        dp.post(f"/erp/api/ponto/ocorrencias/{oid}/decidir", json={"aprovar": True})
    d = dp.get(f"/erp/api/ponto/painel?de={inicio_b}&ate={fim_b}").get_json()
    assert d["ok"], d
    n = d["agora"]["numeros"]
    assert n["batidas"] == 9 and n["funcionarios_batendo"] == 2 and n["corrigidas"] == 0 and n["corretas"] == 9
    assert n["dias_trabalho"] >= 2 and n["dias_em_dia"] >= 2
    assert len(d["agora"]["por_dia"]) == (fim_b - inicio_b).days + 1
    por_dia = {x["dia"]: x for x in d["agora"]["por_dia"]}
    assert por_dia[dia2.isoformat()]["batidas"] == 5 and por_dia[dia2.isoformat()]["pessoas"] == 2
    assert d["antes"] is None and {o["obra"] for o in d["obras"]} >= {"PG-A"}
    # com a comparação: o mês de antes, sem batida nenhuma — só faltas, nota baixa: a obra A melhorou
    antes_fim = inicio_b - dt.timedelta(days=1)
    d = dp.get(f"/erp/api/ponto/painel?de={inicio_b}&ate={fim_b}&antes_de={antes_fim.replace(day=1)}&antes_ate={antes_fim}"
               ).get_json()
    assert d["antes"]["numeros"]["batidas"] == 0
    assert d["antes"]["numeros"]["faltas"] > 0
    assert next(o for o in d["obras"] if o["obra"] == "PG-A")["tendencia"] == "MELHOROU"
    # a obra: só quem bateu nela
    d = dp.get(f"/erp/api/ponto/painel?de={inicio_b}&ate={fim_b}&obra={mundo['obra_b']}").get_json()
    assert d["agora"]["numeros"]["funcionarios_batendo"] == 1 and d["agora"]["numeros"]["batidas"] == 1
    # o supervisor da obra A não vê a obra B
    sup = como(app, mundo["sup"])
    assert sup.get(f"/erp/api/ponto/painel?de={inicio_b}&ate={fim_b}&obra={mundo['obra_b']}").status_code == 404
    # período longo demais
    assert dp.get(f"/erp/api/ponto/painel?de={inicio_b - dt.timedelta(days=120)}&ate={fim_b}").status_code == 400
