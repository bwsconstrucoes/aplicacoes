# -*- coding: utf-8 -*-
"""Ponto — a apuração do dia (regras da CLT), sem banco."""
from __future__ import annotations

import datetime as dt

from app.apps.ponto import horario
from app.apps.ponto.core import apuracao as ap

FUSO = horario.FUSO
SEXTA = dt.date(2026, 10, 2)
SABADO = dt.date(2026, 10, 3)
SEGUNDA = dt.date(2026, 10, 5)

# Obra: segunda a quinta 7h–11h / 12h–17h (9 h); sexta 7h–11h / 12h–16h (8 h).
OBRA = ap.Escala(tipo="SEMANAL", semana={
    **{d: [(ap.hhmm("07:00"), ap.hhmm("11:00")), (ap.hhmm("12:00"), ap.hhmm("17:00"))] for d in range(4)},
    4: [(ap.hhmm("07:00"), ap.hhmm("11:00")), (ap.hhmm("12:00"), ap.hhmm("16:00"))],
})
VIGIA_NOTURNO = ap.Escala(tipo="CICLO_12X36", periodo_ciclo=(ap.hhmm("19:00"), ap.hhmm("31:00")),
                          data_base=SEGUNDA)


def b(dia, *horas):
    """Batidas do dia a partir de 'HH:MM'; '+HH:MM' é no dia seguinte."""
    saida = []
    for h in horas:
        d = dia + dt.timedelta(days=1) if h.startswith("+") else dia
        hh, mm = h.lstrip("+").split(":")
        saida.append(dt.datetime(d.year, d.month, d.day, int(hh), int(mm), tzinfo=FUSO))
    return saida


def test_dia_exato_e_ok():
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "11:00", "12:00", "17:00"))
    assert (r.previsto, r.trabalhado, r.extra, r.debito, r.intervalo) == (540, 540, 0, 0, 60)
    assert r.situacao == "OK" and r.alertas == []


def test_tolerancia_de_5_por_marcacao_e_10_no_dia():
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:04", "11:00", "12:03", "17:02"))
    assert (r.extra, r.debito, r.atraso) == (0, 0, 0) and r.alertas == []


def test_passou_da_tolerancia_conta_tudo_sumula_366():
    # 6 min de atraso numa marcação só: conta os 6, não 1.
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:06", "11:00", "12:00", "17:00"))
    assert r.debito == 6 and r.atraso == 6 and "ATRASO" in r.alertas
    # 4+4+4 = 12 min no dia, cada uma dentro dos 5: passou dos 10 diários, conta tudo.
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "06:56", "11:04", "11:56", "17:00"))
    assert r.extra == 12 and r.debito == 0


def test_hora_extra_e_limite_de_2h():
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "11:00", "12:00", "19:30"))
    assert r.extra == 150 and "EXTRA_ACIMA_DE_2H" in r.alertas


def test_saida_antecipada():
    r = ap.apurar_dia(SEXTA, OBRA, b(SEXTA, "07:00", "11:00", "12:00", "15:00"))
    assert r.previsto == 480 and r.debito == 60 and r.saida_antecipada == 60
    assert "SAIDA_ANTECIPADA" in r.alertas


def test_falta_e_abono():
    r = ap.apurar_dia(SEGUNDA, OBRA, [])
    assert r.falta and r.debito == 540 and r.situacao == "FALTA" and r.alertas == ["FALTA"]
    r = ap.apurar_dia(SEGUNDA, OBRA, [], ocorrencia="atestado")
    assert r.abonado and not r.falta and r.debito == 0 and r.situacao == "ABONADO"
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "11:00"), ocorrencia="FERIAS")
    assert "BATIDA_EM_DIA_ABONADO" in r.alertas


def test_sabado_e_feriado_sao_descanso_e_trabalho_neles_e_extra():
    r = ap.apurar_dia(SABADO, OBRA, [])
    assert r.descanso and r.situacao == "DESCANSO" and not r.falta and r.alertas == []
    r = ap.apurar_dia(SABADO, OBRA, b(SABADO, "07:00", "11:00"))
    assert r.extra == 240 and "TRABALHO_EM_DESCANSO" in r.alertas
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "11:00"), feriado=True)
    assert r.previsto == 0 and r.extra == 240 and "TRABALHO_EM_FERIADO" in r.alertas
    assert not ap.apurar_dia(SEGUNDA, OBRA, [], feriado=True).falta


def test_batida_faltando_nao_inventa_extra_nem_debito():
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "11:00", "12:00"))
    assert r.situacao == "INCOMPLETO" and "BATIDA_FALTANDO" in r.alertas
    assert (r.extra, r.debito) == (0, 0)


def test_intervalo_curto():
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "11:00", "11:30", "16:30"))
    assert r.intervalo == 30 and "INTERVALO_CURTO" in r.alertas
    r = ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "16:00"))   # sem intervalo
    assert "INTERVALO_CURTO" in r.alertas


def test_vigia_noturno_12x36_atravessa_a_meia_noite():
    # Turno de segunda 19h até terça 7h; as batidas da terça de manhã já vêm
    # com data_referencia = segunda (regra das 10h em horario.py).
    r = ap.apurar_dia(SEGUNDA, VIGIA_NOTURNO, b(SEGUNDA, "19:00", "+07:00"))
    assert (r.previsto, r.trabalhado, r.extra, r.debito) == (720, 720, 0, 0)
    assert r.noturno_relogio == 7 * 60          # 22h–5h
    assert r.noturno_reduzido == 480            # 420 min de relógio = 8 h noturnas
    assert "INTERVALO_CURTO" not in r.alertas and "EXTRA_ACIMA_DE_2H" not in r.alertas
    terca = SEGUNDA + dt.timedelta(days=1)
    assert ap.apurar_dia(terca, VIGIA_NOTURNO, []).situacao == "DESCANSO"   # dia de folga do ciclo
    quarta = SEGUNDA + dt.timedelta(days=2)
    assert ap.apurar_dia(quarta, VIGIA_NOTURNO, []).falta


def test_sem_escala_nao_julga():
    r = ap.apurar_dia(SEGUNDA, None, b(SEGUNDA, "07:00", "17:00"))
    assert r.situacao == "SEM_ESCALA" and r.trabalhado == 600 and not r.falta


def test_interjornada():
    fim = b(SEGUNDA, "22:00")[0]
    assert ap.interjornada_curta(fim, b(SEGUNDA, "+07:00")[0], OBRA)       # 9 h
    assert not ap.interjornada_curta(fim, b(SEGUNDA, "+09:00")[0], OBRA)   # 11 h
    assert ap.interjornada_minutos(None, fim) is None


def test_resumo_do_periodo():
    dias = [ap.apurar_dia(SEGUNDA, OBRA, b(SEGUNDA, "07:00", "11:00", "12:00", "18:00")),
            ap.apurar_dia(SEGUNDA + dt.timedelta(days=1), OBRA, []),
            ap.apurar_dia(SEGUNDA + dt.timedelta(days=2), OBRA, [], ocorrencia="ATESTADO")]
    s = ap.resumo_do_periodo(dias)
    assert (s["extra"], s["debito"], s["faltas"], s["abonados"]) == (60, 540, 1, 1)
    assert s["alertas"] == ["FALTA"]
