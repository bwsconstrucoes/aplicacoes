# -*- coding: utf-8 -*-
"""O que os três scripts têm em comum: achar a raiz do monorepo, ligar o log e
abrir a conexão. Rodam no Shell do Render ou no PC com `DATABASE_URL`."""
from __future__ import annotations

import logging
import os
import sys

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if RAIZ not in sys.path:
    sys.path.insert(0, RAIZ)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def exigir_schema(conn) -> None:
    from app.apps.ponto import db
    if not db.schema_existe(conn):
        raise SystemExit("O schema `ponto` ainda não existe. Rode primeiro: "
                         "python -m app.apps.ponto.scripts.migrar")
