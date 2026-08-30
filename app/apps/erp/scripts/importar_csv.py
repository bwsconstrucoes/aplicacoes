# ============================================================================
# BWS ERP — scripts/importar_csv.py
# Importa obras ou categorias de um CSV exportado das planilhas.
# Uso:  python scripts/importar_csv.py obras caminho/obras.csv
#       python scripts/importar_csv.py categorias caminho/categorias.csv
# ============================================================================
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")))

from app.apps.erp.core.importadores.planilhas import importar_categorias_csv, importar_obras_csv
from app.apps.erp.db.database import get_session


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in ("obras", "categorias"):
        print(__doc__)
        return
    alvo, caminho = sys.argv[1], sys.argv[2]
    conteudo = open(caminho, "rb").read()
    with get_session() as s:
        fn = importar_obras_csv if alvo == "obras" else importar_categorias_csv
        rel = fn(s, conteudo, usuario=None)
        s.commit()
    chave = "criadas"
    print(f"Linhas no arquivo: {rel['no_arquivo']}  |  Importadas: {rel[chave]}")
    for r in rel["rejeitadas"]:
        print(f"  - linha {r['linha']} ({r['codigo']}): {r['motivo']}")


if __name__ == "__main__":
    main()
