# -*- coding: utf-8 -*-
"""
Camada de banco do ponto.

REUSA a engine do ERP (`erp/db/database.py`), e não cria outra: é o mesmo
Postgres, a mesma `DATABASE_URL`, o mesmo pool de 5 conexões — abrir um segundo
pool numa instância de 2 GB com 1 processo seria pagar duas vezes pelo mesmo
banco. O que é do ponto mora no schema `ponto`; o que é do ERP (obras, pessoas)
é lido pelo nome completo `public.<tabela>`.

A engine do ERP é PREGUIÇOSA: nada de banco acontece no import. Sem
`DATABASE_URL` este módulo falha sozinho, na primeira chamada, e os outros
blueprints sobem normalmente.

⚠️ AMARRA ENTRE ÁREAS, dita com todas as letras: se o ERP mudar o nome ou a
forma de `obras.latitude`, `obras.longitude`, `colaboradores.cpf`,
`colaboradores.obra_id` ou `colaboradores.situacao`, o ponto para. O teste
`tests/test_ponto_banco.py::test_contrato_com_o_erp` acusa isso na suíte, antes
da produção — mesma proteção do precedente de 24/09/2026 (CONTEXTO.md §9).
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger("ponto.db")

SCHEMA = "ponto"


def engine():
    """A engine do ERP, criada na primeira chamada."""
    from app.apps.erp.db.database import obter_engine
    return obter_engine()


@contextmanager
def conexao() -> Iterator[Connection]:
    """Uma conexão dentro de UMA transação: confirma ao sair sem erro, desfaz
    se algo estourar. É o único jeito de o módulo falar com o banco."""
    with engine().begin() as conn:
        yield conn


def um(conn: Connection, sql: str, **params: Any) -> Optional[dict]:
    """Executa e devolve a primeira linha como dicionário, ou None."""
    linha = conn.execute(text(sql), params).mappings().first()
    return dict(linha) if linha is not None else None


def todos(conn: Connection, sql: str, **params: Any) -> list[dict]:
    """Executa e devolve todas as linhas como dicionários."""
    return [dict(l) for l in conn.execute(text(sql), params).mappings().all()]


def executar(conn: Connection, sql: str, **params: Any) -> int:
    """Executa um comando e devolve quantas linhas alcançou."""
    return conn.execute(text(sql), params).rowcount


def schema_existe(conn: Connection) -> bool:
    return um(conn, "SELECT 1 AS ok FROM information_schema.schemata "
                    "WHERE schema_name = :s", s=SCHEMA) is not None
