# -*- coding: utf-8 -*-
"""Ponto — completar o período de uma pessoa pela escala (13/11/2026): a prévia dia a dia (dia sem batida = o dia
inteiro; dia com batida = só as que faltam; o resto, o porquê), a inclusão só nos dias confirmados, sem tocar nas
batidas que existem, e o alcance. Contra um Postgres de verdade."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, app, como, mundo  # noqa: F401
from tests.test_ponto_gestao_banco import CPF_JOAO
from tests.test_ponto_qualidade_banco import _bater

pytestmark = pytest.mark.banco


def _dias_uteis(mundo, n):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import feriados
    fim = horario.hoje().replace(day=1) - dt.timedelta(days=1)
    inicio = fim.replace(day=1)
    with db.conexao() as conn:
        cal = feriados.Calendario(conn, inicio, fim)
        uteis = [d for d in (inicio + dt.timedelta(days=i) for i in range((fim - inicio).days + 1))
                 if d.weekday() < 4 and not cal.feriado(d, mundo["obra_a"])]
    return uteis[-n:]


def test_completar_o_periodo_pela_escala(app, mundo):
    from app.apps.ponto import db
    vazio, metade, cheio = _dias_uteis(mundo, 3)
    _bater(CPF_JOAO, metade, 7, 0)                                      # só a entrada e a saída
    _bater(CPF_JOAO, metade, 17, 0)
    for h, m in ((7, 0), (11, 0), (12, 0), (17, 0)):
        _bater(CPF_JOAO, cheio, h, m)
    dp = como(app, mundo["dp"])
    url = f"/erp/api/ponto/pessoas/{mundo['joao']}/completar"
    p = dp.get(f"{url}?de={vazio}&ate={cheio}").get_json()
    assert p["ok"], p
    dias = {x["data"]: x for x in p["dias"]}
    assert dias[vazio.isoformat()]["tipo"] == "DIA_INTEIRO" and len(dias[vazio.isoformat()]["incluir"]) == 4
    assert dias[metade.isoformat()]["tipo"] == "COMPLETAR"
    assert [i["hora"] for i in dias[metade.isoformat()]["incluir"]] == ["11:00", "12:00"]
    assert dias[cheio.isoformat()]["tipo"] == "NADA" and dias[cheio.isoformat()]["nota"] == "completo"
    # o motivo é obrigatório
    assert dp.post(url, json={"datas": [vazio.isoformat()], "motivo": "x"}).status_code == 400
    # só os dias confirmados; o dia completo é recusado, os outros entram
    r = dp.post(url, json={"datas": [vazio.isoformat(), metade.isoformat(), cheio.isoformat()],
                           "motivo": "trabalhou na obra a semana toda; aparelho sem sinal"}).get_json()
    assert r["ok"] and r["feitos"] == 2 and r["batidas"] == 6
    assert [x["ok"] for x in r["resultados"]] == [True, True, False]
    with db.conexao() as conn:
        horas = lambda d: [str(l["h"]) for l in db.todos(conn, """
            SELECT to_char(timestamp_servidor AT TIME ZONE 'America/Fortaleza', 'HH24:MI') AS h FROM ponto.marcacoes
             WHERE colaborador_id = :c AND data_referencia = :d AND status <> 'REJEITADA' ORDER BY timestamp_servidor""",
            c=mundo["joao"], d=d)]  # noqa: E731
        assert horas(vazio) == ["07:00", "11:00", "12:00", "17:00"]
        assert horas(metade) == ["07:00", "11:00", "12:00", "17:00"]          # as que existiam ficaram, uma vez só
        assert horas(cheio) == ["07:00", "11:00", "12:00", "17:00"]
        lote = db.todos(conn, """SELECT subtipo FROM ponto.ocorrencias WHERE colaborador_id = :c AND tipo = 'AJUSTE_BATIDA'""",
                        c=mundo["joao"])
    assert lote and all(l["subtipo"] == "LOTE_GESTAO" for l in lote)     # não conta contra a pessoa na qualidade
    # de novo: nada a incluir
    p = dp.get(f"{url}?de={vazio}&ate={cheio}").get_json()
    assert p["batidas"] == 0
    # período longo demais; e o supervisor da obra A não alcança a Maria (obra B)
    assert dp.get(f"{url}?de={vazio - dt.timedelta(days=60)}&ate={cheio}").status_code == 400
    sup = como(app, mundo["sup"])
    assert sup.get(f"/erp/api/ponto/pessoas/{mundo['maria']}/completar?de={vazio}&ate={cheio}").status_code == 404
    assert sup.post(f"/erp/api/ponto/pessoas/{mundo['maria']}/completar",
                    json={"datas": [vazio.isoformat()], "motivo": "teste do alcance da obra"}).status_code == 404
