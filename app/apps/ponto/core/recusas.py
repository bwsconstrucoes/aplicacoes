# -*- coding: utf-8 -*-
"""Batida recusada vira linha em `ponto.recusas` — com o motivo, o aparelho e
o CPF informado. É o que permite investigar um aparelho clonado ou perceber um
tablet mal configurado que está recusando a obra inteira."""
from __future__ import annotations

import json
import logging

from sqlalchemy.engine import Connection

from .. import db

logger = logging.getLogger("ponto.recusas")


def mascarar_cpf(cpf: str | None) -> str:
    """Só os 3 últimos dígitos vão para o log: CPF é dado pessoal."""
    digitos = "".join(ch for ch in str(cpf or "") if ch.isdigit())
    return f"***{digitos[-3:]}" if digitos else "(vazio)"


def registrar(conn: Connection, *, motivo: str, device_uuid: str | None = None,
              cpf: str | None = None, obra: str | None = None, origem: str | None = None,
              ip: str | None = None, **detalhes) -> int:
    linha = db.um(conn, """
        INSERT INTO ponto.recusas (device_uuid, cpf_informado, obra_informada, origem,
                                   motivo, detalhes, ip)
        VALUES (:d, :cpf, :obra, :origem, :motivo, CAST(:det AS jsonb), :ip) RETURNING id
    """, d=device_uuid, cpf=cpf, obra=(str(obra) if obra is not None else None),
         origem=origem, motivo=motivo, det=json.dumps(detalhes, default=str), ip=ip)
    logger.warning("Ponto: batida RECUSADA — %s (aparelho %s, cpf %s, obra %s)",
                   motivo, device_uuid or "-", mascarar_cpf(cpf), obra or "-")
    return int(linha["id"])


def listar(conn: Connection, *, limite: int = 200) -> list[dict]:
    return db.todos(conn, "SELECT * FROM ponto.recusas ORDER BY id DESC LIMIT :n",
                    n=max(1, min(int(limite), 1000)))
