# -*- coding: utf-8 -*-
"""
Hora do ponto: America/Fortaleza.

O servidor do Render e o Postgres rodam em UTC. A hora oficial da batida é a do
servidor, convertida para o fuso da empresa na saída. Quando o sistema não tem
a base de fusos (Windows sem tzdata), cai para UTC−3 fixo, que é Fortaleza
desde sempre — o estado não adota horário de verão.

A regra do DIA DE TRABALHO (`data_referencia`) mora aqui porque é de hora, não
de banco: para o vigia noturno, a batida antes das 10h da manhã pertence ao dia
em que o turno começou (a véspera). Para as outras jornadas, é o dia da batida.
"""
from __future__ import annotations

import datetime as dt

UTC = dt.timezone.utc
UTC_MENOS_3 = dt.timezone(dt.timedelta(hours=-3))
NOME_DO_FUSO = "America/Fortaleza"

# Até esta hora (exclusive) a batida do vigia noturno ainda é do dia anterior.
HORA_DE_CORTE_NOTURNO = 10

JORNADAS = ("PADRAO_4", "VIGIA_DIURNO_2", "VIGIA_NOTURNO_2")
JORNADA_PADRAO = "PADRAO_4"


def _fuso():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(NOME_DO_FUSO)
    except Exception:          # sem base de fusos no sistema
        return UTC_MENOS_3


FUSO = _fuso()


def agora() -> dt.datetime:
    """O instante atual, com fuso (UTC). É o `timestamp_servidor`."""
    return dt.datetime.now(UTC)


def para_local(momento: dt.datetime | None) -> dt.datetime | None:
    """Converte para a hora de Fortaleza. Sem fuso = UTC (onde o serviço roda)."""
    if momento is None:
        return None
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=UTC)
    return momento.astimezone(FUSO)


def texto(momento: dt.datetime | None) -> str | None:
    """ISO 8601 na hora local, com o deslocamento (ex.: 2026-10-03T07:58:00-03:00)."""
    local = para_local(momento)
    return local.isoformat(timespec="seconds") if local else None


def ler_iso(valor: str | None) -> dt.datetime | None:
    """Lê um instante em ISO 8601 vindo do aparelho. Sem fuso = hora local de
    Fortaleza (é o que um celular daqui manda quando não põe deslocamento).
    Devolve None para vazio; levanta ValueError para texto ilegível."""
    if valor is None or str(valor).strip() == "":
        return None
    bruto = str(valor).strip().replace("Z", "+00:00")
    momento = dt.datetime.fromisoformat(bruto)
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=FUSO)
    return momento


def data_referencia(momento: dt.datetime, tipo_jornada: str) -> dt.date:
    """O dia de trabalho a que a batida pertence."""
    local = para_local(momento)
    if tipo_jornada == "VIGIA_NOTURNO_2" and local.hour < HORA_DE_CORTE_NOTURNO:
        return local.date() - dt.timedelta(days=1)
    return local.date()


def hoje() -> dt.date:
    return para_local(agora()).date()
