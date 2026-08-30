# ============================================================================
# BWS ERP — scripts/criar_schema.py
# Aplica o schema.sql no banco apontado por DATABASE_URL.
# Uso:  python scripts/criar_schema.py
# Seguro contra dupla execução: aborta se detectar tabela 'titulos' existente.
# ============================================================================
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")))

from sqlalchemy import text
from app.apps.erp.db.database import obter_engine

CAMINHO_SQL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schema.sql")


def main() -> None:
    with open(CAMINHO_SQL, "r", encoding="utf-8") as f:
        sql = f.read()

    with obter_engine().connect() as conn:
        existe = conn.execute(text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='titulos'"
        )).first()
        if existe:
            print("ABORTADO: o schema já existe neste banco (tabela 'titulos' encontrada).")
            print("Evoluções de schema serão feitas por migrações, não por recriação.")
            return

        print("Aplicando schema.sql...")
        conn.execute(text(sql))
        conn.commit()
        print("Schema criado com sucesso.")


if __name__ == "__main__":
    main()
