# -*- coding: utf-8 -*-
"""
Banco da mensageria — o mesmo Postgres do ERP, schema `mensageria`.

Mesma decisão do ponto: reusa a engine PREGUIÇOSA do ERP (um pool só, nada de
banco no import). O código chega ao Render antes de alguém apertar "Aplicar
atualizações do banco" no ERP, então tudo aqui precisa responder "ainda não
existe" sem estourar — `disponivel()` é a pergunta que o notificador faz
antes de confiar na mensageria, e a resposta "não" é guardada por 60 s.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger("mensageria.db")

SCHEMA = "mensageria"


def engine():
    from app.apps.erp.db.database import obter_engine
    return obter_engine()


@contextmanager
def conexao() -> Iterator[Connection]:
    """Uma conexão numa transação: confirma ao sair sem erro, desfaz se estourar."""
    with engine().begin() as conn:
        yield conn


def um(conn: Connection, sql: str, **params: Any) -> Optional[dict]:
    linha = conn.execute(text(sql), params).mappings().first()
    return dict(linha) if linha is not None else None


def todos(conn: Connection, sql: str, **params: Any) -> list[dict]:
    return [dict(l) for l in conn.execute(text(sql), params).mappings().all()]


def executar(conn: Connection, sql: str, **params: Any) -> int:
    return conn.execute(text(sql), params).rowcount


_sabe_que_existe = False
_nao_existia_em = 0.0


def disponivel() -> bool:
    """As tabelas da mensageria já existem? Sem banco ou sem migração, False —
    e o notificador segue pelas variáveis de ambiente, como antes."""
    global _sabe_que_existe, _nao_existia_em
    if _sabe_que_existe:
        return True
    if time.time() - _nao_existia_em < 60:
        return False
    try:
        with conexao() as conn:
            existe = um(conn, """SELECT 1 AS x FROM information_schema.tables
                                  WHERE table_schema = :s AND table_name = 'envios'""",
                        s=SCHEMA) is not None
    except Exception as e:  # noqa: BLE001 — sem banco, a mensageria só não decide
        logger.warning("Mensageria: banco indisponível (%s); seguindo pelas variáveis", str(e)[:120])
        existe = False
    if existe:
        _sabe_que_existe = True
    else:
        _nao_existia_em = time.time()
    return existe


def esquecer() -> None:
    """Depois de aplicar migração: pergunta de novo na hora."""
    global _sabe_que_existe, _nao_existia_em
    _sabe_que_existe = False
    _nao_existia_em = 0.0
