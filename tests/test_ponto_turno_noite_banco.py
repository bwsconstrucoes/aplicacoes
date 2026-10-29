# -*- coding: utf-8 -*-
"""Ponto — o turno da noite (30/10/2026, core/escalas.py `dia_de_trabalho`, migração 021): a escala que
passa da meia-noite leva a batida da madrugada para o dia em que o turno começou, sem precisar marcar
"vigia noturno" na jornada da pessoa."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401


def test_quando_a_escala_e_do_turno_da_noite():
    from app.apps.ponto.core import apuracao, escalas
    seg = dt.date(2026, 10, 26)
    noite_12x36 = apuracao.Escala(tipo="CICLO_12X36", periodo_ciclo=(17 * 60, 30 * 60), data_base=seg)
    dia_12x36 = apuracao.Escala(tipo="CICLO_12X36", periodo_ciclo=(7 * 60, 19 * 60), data_base=seg)
    assert escalas.turno_da_noite(noite_12x36, seg)
    assert escalas.turno_da_noite(noite_12x36, seg + dt.timedelta(days=1))     # folga no ciclo: a noite fica inteira
    assert not escalas.turno_da_noite(dia_12x36, seg)
    # semanal com uma noite só (sexta): só a manhã de sábado volta
    mista = apuracao.Escala(tipo="SEMANAL", semana={0: [(420, 1020)], 4: [(1320, 1800)]})
    assert escalas.turno_da_noite(mista, seg + dt.timedelta(days=4))
    assert not escalas.turno_da_noite(mista, seg)
    # semanal só de noite: até a noite fora da escala fica no dia da entrada
    so_noite = apuracao.Escala(tipo="SEMANAL", semana={0: [(1080, 1800)], 2: [(1080, 1800)]})
    assert escalas.turno_da_noite(so_noite, seg + dt.timedelta(days=1))
    assert not escalas.turno_da_noite(None, seg)


@pytest.mark.banco
def test_a_saida_da_madrugada_fica_no_dia_da_entrada(app, mundo):
    """O dono: "o dia que ele inicia de tarde (…) é o horário de entrada dele. E no dia seguinte, é de saída." """
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    ter, qua, qui = (seg + dt.timedelta(days=n) for n in (1, 2, 3))

    def dias():
        with db.conexao() as conn:
            return [r["data_referencia"] for r in db.todos(
                conn, "SELECT data_referencia FROM ponto.marcacoes WHERE colaborador_id = :c "
                      "ORDER BY timestamp_servidor", c=mundo["joao"])]

    # na escala do dia, a saída das 6h cai no dia seguinte (era o defeito)
    bater_via_chave(app, CPF_JOAO, local(seg, 17))
    bater_via_chave(app, CPF_JOAO, local(ter, 6))
    assert dias() == [seg, ter]
    # a escala da noite, dada a partir da segunda: a batida já gravada volta para a segunda
    with db.conexao() as conn:
        noite = escalas.gravar(conn, {"nome": "Vigia noturno 12x36", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "06:00"})
        escalas.atribuir(conn, mundo["joao"], noite["id"], seg.isoformat(), ciclo_data_base=seg.isoformat(),
                         por="teste")
    assert dias() == [seg, seg]
    # a batida nova já nasce no dia certo
    bater_via_chave(app, CPF_JOAO, local(qua, 17))
    bater_via_chave(app, CPF_JOAO, local(qui, 5, 55))
    assert dias() == [seg, seg, qua, qua]
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    d = next(x for x in esp["dias"] if x["data"] == seg.isoformat())
    assert d["trabalhado"] == 13 * 60 and d["situacao"] == "OK", d
    assert "INTERVALO_CURTO" not in d["alertas"]

    # a migração 021 acerta o que ficou gravado no dia errado antes do código novo
    with db.conexao() as conn:
        conn.execute(text("UPDATE ponto.marcacoes SET data_referencia = data_referencia + 1 "
                          "WHERE colaborador_id = :c AND EXTRACT(HOUR FROM timestamp_servidor "
                          "AT TIME ZONE 'America/Fortaleza') < 10"), {"c": mundo["joao"]})
    assert dias() == [seg, ter, qua, qui]
    sql = (Path(__file__).resolve().parents[1] / "app/apps/ponto/migracoes/021_turno_da_noite.sql").read_text()
    with db.conexao() as conn:
        conn.execute(text(sql))
    assert dias() == [seg, seg, qua, qua]
    # trocar de volta para a escala do dia desfaz
    with db.conexao() as conn:
        escalas.atribuir(conn, mundo["joao"], mundo["escala"], seg.isoformat(), por="teste")
    assert dias() == [seg, ter, qua, qui]
