# -*- coding: utf-8 -*-
"""
A HORA EXTRA NEGADA NA CONFERÊNCIA (29/10/2026, migração 019).

O dono, olhando as horas extras por obra: *"eu preciso poder, por exemplo, discordar com alguma hora — eu
queria poder selecionar e negar que aquela hora daquele dia entre no cálculo."*

O que se nega é a HORA EXTRA DE UM DIA de uma pessoa, inteira, com o motivo. As batidas não mudam (Portaria
671); o espelho zera o extra do dia e mostra "hora extra negada" — e daí o fechamento, os relatórios e o
banco de horas já não contam. Voltar a contar é possível, e fica registrado (a linha não se apaga).
Só o DP (`aprovar_afastamento`), o mesmo do fechamento.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao

MOTIVO_MINIMO = 10


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "horas_extras_negadas", "id")


def das_pessoas(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date) -> dict[tuple[int, dt.date], dict]:
    """{(colaborador_id, data): a negação em vigor} do período."""
    if not ids or not disponivel(conn):
        return {}
    return {(int(r["colaborador_id"]), r["data"]): r for r in db.todos(conn, """
        SELECT colaborador_id, data, minutos, motivo, negada_por, negada_em FROM ponto.horas_extras_negadas
         WHERE colaborador_id = ANY(:ids) AND data BETWEEN :i AND :f AND desfeita_em IS NULL""",
        ids=[int(i) for i in ids], i=inicio, f=fim)}


def _itens(itens) -> list[tuple[int, dt.date]]:
    saida = []
    for x in itens or []:
        try:
            saida.append((int(x["colaborador_id"]), dt.date.fromisoformat(str(x["data"])[:10])))
        except (KeyError, TypeError, ValueError):
            raise ErroDeValidacao("item ilegível (pessoa e dia)", campo="itens") from None
    if not saida:
        raise ErroDeValidacao("marque ao menos uma hora extra", campo="itens")
    return saida


def negar(conn: Connection, itens, motivo: str, por: str, minutos: Optional[dict] = None) -> int:
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 019) antes")
    motivo = " ".join(str(motivo or "").split())
    if len(motivo) < MOTIVO_MINIMO:
        raise ErroDeValidacao(f"diga por que a hora extra não entra (ao menos {MOTIVO_MINIMO} letras)", campo="motivo")
    from . import competencias
    n = 0
    for cid, dia in _itens(itens):
        competencias.exigir_aberta(conn, dia)
        n += db.executar(conn, """
            INSERT INTO ponto.horas_extras_negadas (colaborador_id, data, minutos, motivo, negada_por)
            VALUES (:c, :d, :m, :mot, :p)
            ON CONFLICT (colaborador_id, data) WHERE desfeita_em IS NULL DO NOTHING""",
            c=cid, d=dia, m=int((minutos or {}).get(f"{cid}:{dia.isoformat()}") or 0), mot=motivo[:300], p=por[:120]) or 0
    return n


def voltar(conn: Connection, itens, por: str) -> int:
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 019) antes")
    from . import competencias
    n = 0
    for cid, dia in _itens(itens):
        competencias.exigir_aberta(conn, dia)
        n += db.executar(conn, """
            UPDATE ponto.horas_extras_negadas SET desfeita_por = :p, desfeita_em = now()
             WHERE colaborador_id = :c AND data = :d AND desfeita_em IS NULL""", c=cid, d=dia, p=por[:120]) or 0
    return n


def para_json(r: Optional[dict]) -> Optional[dict]:
    if not r:
        return None
    return {"motivo": r["motivo"], "por": r["negada_por"], "em": horario.texto(r["negada_em"])}
