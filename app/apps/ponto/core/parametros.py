# -*- coding: utf-8 -*-
"""Parâmetros do ponto mudados pela tela (`ponto.parametros`)."""
from __future__ import annotations

from sqlalchemy.engine import Connection

from .. import db

TELEFONES_RESUMO = "resumo.telefones"
ULTIMA_ROTINA = "rotina.ultima_data"


def ler(conn: Connection, chave: str, padrao: str = "") -> str:
    linha = db.um(conn, "SELECT valor FROM ponto.parametros WHERE chave = :c", c=chave)
    return linha["valor"] if linha else padrao


def gravar(conn: Connection, chave: str, valor: str, por: str = "") -> None:
    db.executar(conn, """
        INSERT INTO ponto.parametros (chave, valor, atualizado_por) VALUES (:c, :v, :p)
        ON CONFLICT (chave) DO UPDATE SET valor = :v, atualizado_por = :p, atualizado_em = now()
    """, c=chave, v=str(valor), p=(por or "")[:120])
