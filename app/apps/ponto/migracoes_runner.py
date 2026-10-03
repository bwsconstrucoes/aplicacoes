# -*- coding: utf-8 -*-
"""
Aplica as migrações do schema `ponto`.

Mesma disciplina do ERP e da Análise de SPs: arquivos .sql numerados, tabela de
controle, cada arquivo na própria transação — se o terceiro falhar, os dois
primeiros ficam. E, pelo mesmo motivo, DELIBERADAMENTE fora do start do
gunicorn: uma migração com defeito no boot derrubaria o monorepo inteiro.

Na fase 1 não há tela; aplica-se pela rota `POST /ponto/api/admin/migrar` (com
a chave de API) ou pelo `scripts/migrar.py` no Shell do Render.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy import text

from . import db

logger = logging.getLogger("ponto.migracoes")

PASTA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migracoes")


def _com_controle(conn) -> None:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS ponto"))
    conn.execute(text("CREATE TABLE IF NOT EXISTS ponto._migracoes ("
                      " nome TEXT PRIMARY KEY,"
                      " aplicada_em TIMESTAMPTZ NOT NULL DEFAULT now())"))


def arquivos() -> list[str]:
    return sorted(f for f in os.listdir(PASTA) if f.endswith(".sql"))


def listar_estado() -> dict[str, Any]:
    """Diz quais migrações já foram aplicadas e quais faltam."""
    from .horario import texto as hora_texto
    with db.conexao() as conn:
        _com_controle(conn)
        aplicadas = {r["nome"]: r["aplicada_em"] for r in db.todos(
            conn, "SELECT nome, aplicada_em FROM ponto._migracoes ORDER BY nome")}
    nomes = arquivos()
    return {
        "aplicadas": [{"nome": n, "em": hora_texto(aplicadas[n])}
                      for n in nomes if n in aplicadas],
        "pendentes": [n for n in nomes if n not in aplicadas],
    }


def aplicar_pendentes() -> dict[str, Any]:
    """Aplica, em ordem, o que falta. Para na primeira que falhar e diz qual."""
    estado = listar_estado()
    aplicadas: list[str] = []
    erro = None
    for nome in estado["pendentes"]:
        with open(os.path.join(PASTA, nome), encoding="utf-8") as f:
            sql = f.read()
        try:
            with db.conexao() as conn:
                conn.execute(text(sql))
                conn.execute(text("INSERT INTO ponto._migracoes (nome) VALUES (:n)"),
                             {"n": nome})
            aplicadas.append(nome)
            logger.info("Ponto: migração %s aplicada", nome)
        except Exception as e:  # noqa: BLE001 — o erro vai inteiro para quem chamou
            erro = {"migracao": nome, "erro": str(e)[:800]}
            logger.exception("Ponto: falha na migração %s", nome)
            break
    return {
        "aplicadas": aplicadas,
        "erro": erro,
        "pendentes_restantes": [n for n in estado["pendentes"]
                                if n not in aplicadas and (not erro or n != erro["migracao"])],
    }
