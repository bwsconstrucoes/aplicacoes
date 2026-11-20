# -*- coding: utf-8 -*-
"""
A OBRA ATUAL, PELO PONTO (migração 118, 20/11/2026).

Ele: *"teremos a obra de cadastro e a obra atual. Para a obra atual, vamos
buscar a informação do ponto. Uma rotina que atualize essa informação."*

- **Obra de cadastro**: `colaboradores.obra_id`. Quem muda é o DP.
- **Obra atual**: onde a pessoa mais bateu ponto nos últimos `DIAS` dias
  (empate: a da batida mais recente — a mesma régua do ponto,
  `ponto.core.cadastros.obra_pelo_ponto`). Quem muda é a rotina diária.

Quando as duas divergem por uma semana, é uma TRANSFERÊNCIA que ninguém
registrou: o agente avisa o DP (`agente_dp.vigia`). O DP decide se muda o
cadastro.

⚑ CUSTO: uma consulta ao banco por dia para todo mundo (o ponto e o ERP estão
no mesmo banco), mais uma leitura do que estava gravado. Nada de internet.
Quem não bateu ponto no período mantém a última obra conhecida.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DIAS = 15
DIAS_PARA_AVISAR = 7

_tabela: dict[str, bool] = {"existe": False}


def tem_tabela(s: Session) -> bool:
    if not _tabela["existe"]:
        _tabela["existe"] = s.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = "
            "'public' AND table_name = 'colaborador_obra_atual'")).first() is not None
    return _tabela["existe"]


def _tem_ponto(s: Session) -> bool:
    return s.execute(text(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'ponto' "
        "AND table_name = 'marcacoes'")).first() is not None


def atualizar(s: Session, *, hoje: Optional[date] = None) -> dict[str, Any]:
    """A rotina do dia. Devolve quantos mudaram de obra."""
    if not tem_tabela(s) or not _tem_ponto(s):
        return {"mudaram": 0, "lidos": 0, "motivo": "sem tabela ou sem ponto"}
    hoje = hoje or date.today()
    pelo_ponto = s.execute(text("""
        WITH c AS (
            SELECT colaborador_id, obra_id, count(*) AS n,
                   max(timestamp_servidor) AS u
              FROM ponto.marcacoes
             WHERE status <> 'REJEITADA' AND obra_id IS NOT NULL
               AND data_referencia >= CAST(:hoje AS date) - CAST(:d AS integer)
             GROUP BY colaborador_id, obra_id)
        SELECT DISTINCT ON (colaborador_id) colaborador_id, obra_id, n, u
          FROM c ORDER BY colaborador_id, n DESC, u DESC"""),
        {"hoje": hoje, "d": DIAS}).mappings().all()
    antes = {r[0]: r[1] for r in s.execute(text(
        "SELECT colaborador_id, obra_id FROM colaborador_obra_atual")).all()}
    mudaram = 0
    for r in pelo_ponto:
        cid, obra = int(r["colaborador_id"]), int(r["obra_id"])
        anterior = antes.get(cid)
        if cid in antes and anterior != obra:
            mudaram += 1
            s.execute(text("""INSERT INTO colaborador_obra_mudancas
                                  (colaborador_id, de_obra_id, para_obra_id, em)
                              VALUES (:c, :de, :para, :em)"""),
                      {"c": cid, "de": anterior, "para": obra, "em": hoje})
        s.execute(text("""
            INSERT INTO colaborador_obra_atual (colaborador_id, obra_id, desde,
                                                ultima_batida, batidas, atualizado_em)
            VALUES (:c, :o, :hoje, :u, :n, now())
            ON CONFLICT (colaborador_id) DO UPDATE
               SET desde = CASE WHEN colaborador_obra_atual.obra_id IS DISTINCT FROM :o
                                THEN :hoje ELSE colaborador_obra_atual.desde END,
                   obra_id = :o, ultima_batida = :u, batidas = :n,
                   atualizado_em = now()"""),
            {"c": cid, "o": obra, "hoje": hoje, "u": r["u"], "n": int(r["n"])})
    logger.info("ERP/obra atual: %s pessoa(s) lidas do ponto, %s mudaram de obra",
                len(pelo_ponto), mudaram)
    return {"mudaram": mudaram, "lidos": len(pelo_ponto)}


def da_pessoa(s: Session, colaborador_id: int) -> Optional[dict[str, Any]]:
    if not tem_tabela(s):
        return None
    l = s.execute(text("""
        SELECT a.obra_id, o.codigo, o.nome, a.desde, a.ultima_batida
          FROM colaborador_obra_atual a LEFT JOIN obras o ON o.id = a.obra_id
         WHERE a.colaborador_id = :c"""), {"c": colaborador_id}).mappings().first()
    return dict(l) if l else None


def divergentes(s: Session, *, hoje: Optional[date] = None,
                dias: int = DIAS_PARA_AVISAR) -> list[dict[str, Any]]:
    """Quem está batendo ponto, há `dias` ou mais, numa obra que não é a do
    cadastro — a transferência que ninguém registrou."""
    if not tem_tabela(s):
        return []
    hoje = hoje or date.today()
    return [dict(l) for l in s.execute(text("""
        SELECT c.id AS colaborador_id, c.nome, oc.codigo AS obra_cadastro,
               oa.codigo AS obra_atual, a.desde
          FROM colaborador_obra_atual a
          JOIN colaboradores c ON c.id = a.colaborador_id
          LEFT JOIN obras oc ON oc.id = c.obra_id
          LEFT JOIN obras oa ON oa.id = a.obra_id
         WHERE a.obra_id IS DISTINCT FROM c.obra_id
           AND UPPER(COALESCE(c.situacao, 'ATIVO')) = 'ATIVO'
           AND a.desde <= CAST(:hoje AS date) - CAST(:d AS integer)
         ORDER BY a.desde, c.nome"""), {"hoje": hoje, "d": dias}).mappings().all()]


def obras_da_pessoa_sql(alias: str = "c") -> str:
    """Trecho de SQL: a pessoa pertence às obras `:obras` pela obra de
    CADASTRO ou pela ATUAL. É o que faz o aviso chegar a quem cuida do ponto
    onde ela está batendo de verdade."""
    return (f"({alias}.obra_id = ANY(:obras) OR EXISTS (SELECT 1 FROM "
            f"colaborador_obra_atual oa_ WHERE oa_.colaborador_id = {alias}.id "
            f"AND oa_.obra_id = ANY(:obras)))")
