# -*- coding: utf-8 -*-
"""
Hora de Brasília nas telas.

O servidor do Render roda em UTC, e o Postgres também. Sem conversão, uma carga
feita às 13h29 aparece como 16h29 — e quem lê acha que o relógio está quebrado,
ou pior, acha que aconteceu outra coisa em outro horário.

A conversão usa o fuso `America/Sao_Paulo` quando o sistema o conhece. Quando
não conhece (Windows sem a base de fusos instalada, que é o caso do computador
onde isto foi escrito), cai para **UTC−3 fixo** — que é o horário de Brasília
desde 2019, quando o horário de verão acabou. Se um dia ele voltar, o caminho
do `zoneinfo` continua correto e só o atalho ficaria defasado.
"""
from __future__ import annotations

import datetime as dt

UTC_MENOS_3 = dt.timezone(dt.timedelta(hours=-3))


def _fuso():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/Sao_Paulo")
    except Exception:      # sem base de fusos no sistema
        return UTC_MENOS_3


FUSO = _fuso()


def para_brasilia(momento: dt.datetime | None) -> dt.datetime | None:
    """Converte um instante para a hora de Brasília.

    Datas vindas do Postgres como TIMESTAMPTZ já sabem seu fuso. As que vierem
    sem fuso são tratadas como UTC, que é onde o serviço roda — o contrário
    (tratar como local) deslocaria o horário de novo, na direção errada."""
    if momento is None:
        return None
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=dt.timezone.utc)
    return momento.astimezone(FUSO)


def agora() -> dt.datetime:
    """O instante atual, em Brasília."""
    return dt.datetime.now(FUSO)


def texto(momento: dt.datetime | None, com_hora: bool = True) -> str:
    """Data legível, já convertida. Vazia vira travessão, nunca 'None'."""
    momento = para_brasilia(momento)
    if momento is None:
        return "—"
    return momento.strftime("%d/%m/%Y às %H:%M" if com_hora else "%d/%m/%Y")
