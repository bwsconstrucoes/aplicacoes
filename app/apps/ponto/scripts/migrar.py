# -*- coding: utf-8 -*-
"""Aplica as migrações do ponto. Uso:

    python -m app.apps.ponto.scripts.migrar            # mostra o estado
    python -m app.apps.ponto.scripts.migrar --aplicar  # aplica as pendentes

Nunca roda sozinho no start do serviço — essa é a regra da casa."""
from __future__ import annotations

import argparse
import json

from . import _base  # noqa: F401 — põe a raiz no path e liga o log


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrações do schema ponto")
    parser.add_argument("--aplicar", action="store_true", help="aplica as pendentes")
    args = parser.parse_args()

    from app.apps.ponto import migracoes_runner
    estado = migracoes_runner.listar_estado()
    print(json.dumps(estado, ensure_ascii=False, indent=2))
    if not args.aplicar:
        if estado["pendentes"]:
            print("\nHá pendentes. Use --aplicar para rodar.")
        return 0
    resultado = migracoes_runner.aplicar_pendentes()
    print(json.dumps(resultado, ensure_ascii=False, indent=2))
    return 1 if resultado["erro"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
