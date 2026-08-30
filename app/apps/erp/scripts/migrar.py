# ============================================================================
# BWS ERP — scripts/migrar.py
# Runner de migrações: aplica, em ordem, os .sql de scripts/migracoes/ que
# ainda não constam na tabela de controle _migracoes (criada aqui).
# Uso:  python scripts/migrar.py
# ============================================================================
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")))

from sqlalchemy import text
from app.apps.erp.db.database import obter_engine

PASTA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migracoes")


def main() -> None:
    eng = obter_engine()
    with eng.connect() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS _migracoes ("
            " nome TEXT PRIMARY KEY, aplicada_em TIMESTAMPTZ NOT NULL DEFAULT now())"))
        conn.commit()
        aplicadas = {r[0] for r in conn.execute(text("SELECT nome FROM _migracoes"))}
        pendentes = sorted(f for f in os.listdir(PASTA)
                           if f.endswith(".sql") and f not in aplicadas)
        if not pendentes:
            print("Nenhuma migração pendente.")
            return
        for nome in pendentes:
            print(f"Aplicando {nome}...")
            sql = open(os.path.join(PASTA, nome), encoding="utf-8").read()
            conn.execute(text(sql))
            conn.execute(text("INSERT INTO _migracoes (nome) VALUES (:n)"), {"n": nome})
            conn.commit()
            print(f"  OK — {nome}")
        print(f"{len(pendentes)} migração(ões) aplicada(s).")


if __name__ == "__main__":
    main()
