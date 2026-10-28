# -*- coding: utf-8 -*-
"""
A OBRA ESCOLHIDA À MÃO QUE PROVAVELMENTE ESTÁ ERRADA (29/10/2026).

O dono: *"é muito fácil a pessoa selecionar errado quando ela vai pedir o ajuste (…) colocar um alerta
de que se a obra selecionada não for uma obra que a pessoa vem batendo ponto, ou se a obra selecionada
for uma obra que ultimamente está sem batidas, ou seja, que provavelmente não deveria estar sendo batido
ponto, que o sistema alertasse. A gente teve algumas situações aqui de erro na escolha da obra."*

Dois sinais, os mesmos no aplicativo (na hora de pedir, `app_obra.app_api_obra_aviso`) e nas Validações
(na hora de decidir, `credibilidade.py`):
- **a pessoa não bate nesta obra**: nos últimos 30 dias ela bateu, mas nunca nesta;
- **obra parada**: ninguém bate ponto nela há mais de 7 dias (ou nunca bateu).
É AVISO, não bloqueio: a pessoa pode ter mesmo trabalhado numa obra nova naquele dia.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario

DIAS_DA_PESSOA = 30
DIAS_OBRA_PARADA = 7


def avisos(*, obra: str, obras_da_pessoa: dict[str, int], ultima_da_obra: Optional[dt.date],
           hoje: dt.date) -> list[dict]:
    """PURA. `obras_da_pessoa`: {código: batidas nos últimos 30 dias}; `ultima_da_obra`: o dia da última
    batida de qualquer pessoa na obra (None = nunca). Devolve [{codigo, texto}]."""
    saida = []
    if not obra:
        return saida
    if obras_da_pessoa and obra not in obras_da_pessoa:
        onde = [c for c, _ in sorted(obras_da_pessoa.items(), key=lambda x: (-x[1], x[0]))][:3]
        saida.append({"codigo": "OBRA_ESTRANHA",
                      "texto": f"nos últimos {DIAS_DA_PESSOA} dias, as batidas foram em {', '.join(onde)} — "
                               f"nunca na {obra}"})
    if ultima_da_obra is None:
        saida.append({"codigo": "OBRA_PARADA", "texto": f"ninguém bateu ponto na obra {obra} até hoje"})
    elif (hoje - ultima_da_obra).days > DIAS_OBRA_PARADA:
        dias = (hoje - ultima_da_obra).days
        saida.append({"codigo": "OBRA_PARADA",
                      "texto": f"ninguém bate ponto na obra {obra} há {dias} dias (a última batida foi em "
                               f"{ultima_da_obra:%d/%m}) — pode ser uma obra parada"})
    return saida


def obras_das_pessoas(conn: Connection, ids: list[int]) -> dict[int, dict[str, int]]:
    """{pessoa: {código da obra: batidas nos últimos 30 dias}} — as batidas que valem (não as rejeitadas)."""
    if not ids:
        return {}
    saida: dict[int, dict[str, int]] = {}
    for r in db.todos(conn, """
        SELECT m.colaborador_id, upper(btrim(o.codigo)) AS codigo, count(*) AS n
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.colaborador_id = ANY(:ids) AND m.data_referencia >= :d AND m.status <> 'REJEITADA'
         GROUP BY m.colaborador_id, upper(btrim(o.codigo))""",
            ids=list(ids), d=horario.hoje() - dt.timedelta(days=DIAS_DA_PESSOA)):
        saida.setdefault(int(r["colaborador_id"]), {})[r["codigo"]] = int(r["n"])
    return saida


def ultimas_das_obras(conn: Connection, codigos: list[str]) -> dict[str, Optional[dt.date]]:
    """{código: o dia da última batida de qualquer pessoa na obra}, None quando nunca houve."""
    codigos = sorted({str(c).strip().upper() for c in codigos if c})
    if not codigos:
        return {}
    saida: dict[str, Optional[dt.date]] = {c: None for c in codigos}
    for r in db.todos(conn, """
        SELECT upper(btrim(o.codigo)) AS codigo, max(m.data_referencia) AS ultima
          FROM public.obras o JOIN ponto.marcacoes m ON m.obra_id = o.id AND m.status <> 'REJEITADA'
         WHERE upper(btrim(o.codigo)) = ANY(:c)
         GROUP BY upper(btrim(o.codigo))""", c=codigos):
        saida[r["codigo"]] = r["ultima"]
    return saida


def da_pessoa(conn: Connection, colaborador_id: int, obra: str) -> list[dict]:
    """Os avisos de UMA obra escolhida para UMA pessoa (o aplicativo, ao escolher)."""
    codigo = str(obra or "").strip().upper()
    if not codigo:
        return []
    return avisos(obra=codigo, obras_da_pessoa=obras_das_pessoas(conn, [colaborador_id]).get(int(colaborador_id), {}),
                  ultima_da_obra=ultimas_das_obras(conn, [codigo]).get(codigo), hoje=horario.hoje())
