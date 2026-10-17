# -*- coding: utf-8 -*-
"""
OS FILTROS DAS TELAS DO PONTO: as obras que TÊM batida num período, e quem bateu
nelas.

Pedido do dono, 16/10/2026: *"o padrão de exibição deveria ser as obras que têm
ponto, porque o cadastro de obras é muito grande e fica muita obra sem nada"*; e,
no espelho, *"primeiro fazer o filtro de obra, aí ser listado as pessoas para
poder selecionar"*. A pessoa é da obra quando tem batida nela no período — quem
bateu em duas obras aparece nas duas (a ideia dele: *"se ela tiver batida em mais
de uma obra, aí ela aparece para as duas"*).

Batida rejeitada não conta. O dia é o da `data_referencia` (o do vigia noturno
é o da entrada).
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from . import vigencia


def obras_com_batida(conn: Connection, de: dt.date, ate: dt.date,
                     obras: Optional[list[int]] = None) -> list[dict]:
    """As obras com batida entre `de` e `ate`, com quantas pessoas e batidas.
    `obras` é o alcance de quem pergunta (None = todas)."""
    filtro = "AND m.obra_id = ANY(:obras)" if obras is not None else ""
    de, ate = vigencia.recortar(conn, de, ate)
    return db.todos(conn, f"""
        SELECT o.id, o.codigo, o.nome, count(DISTINCT m.colaborador_id) AS pessoas, count(*) AS batidas
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.data_referencia BETWEEN :de AND :ate AND m.status <> 'REJEITADA' {filtro}
         GROUP BY o.id, o.codigo, o.nome
         ORDER BY o.codigo""", de=de, ate=ate, obras=list(obras or []))


def pessoas_com_batida(conn: Connection, obra_id: int, de: dt.date, ate: dt.date) -> list[dict]:
    """Quem bateu na obra entre `de` e `ate`, com quantas batidas e o último dia."""
    de, ate = vigencia.recortar(conn, de, ate)
    return db.todos(conn, """
        SELECT c.id, c.nome, count(*) AS batidas, max(m.data_referencia) AS ultimo_dia
          FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
         WHERE m.obra_id = :o AND m.data_referencia BETWEEN :de AND :ate AND m.status <> 'REJEITADA'
         GROUP BY c.id, c.nome
         ORDER BY c.nome""", o=obra_id, de=de, ate=ate)


def obras_em_que_bateu(conn: Connection, dias: int = 30) -> dict[int, list[str]]:
    """Para o filtro de Pessoas: em que obras cada pessoa bateu nos últimos dias."""
    saida: dict[int, list[str]] = {}
    for l in db.todos(conn, """
        SELECT DISTINCT m.colaborador_id, o.codigo
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.data_referencia >= (now() AT TIME ZONE 'America/Fortaleza')::date - :n
           AND m.data_referencia >= :vale AND m.status <> 'REJEITADA'
         ORDER BY o.codigo""", n=dias, vale=vigencia.inicio(conn)):
        saida.setdefault(int(l["colaborador_id"]), []).append(l["codigo"])
    return saida
