# -*- coding: utf-8 -*-
"""Leva para o Google Drive as fotos de batida que ficaram na fila (o Drive
falhou ou não estava configurado na hora). Uso:

    python -m app.apps.ponto.scripts.enviar_fotos            # até 50
    python -m app.apps.ponto.scripts.enviar_fotos --limite 500

A mesma coisa que `POST /ponto/api/admin/fotos/enviar-pendentes` faz."""
from __future__ import annotations

import argparse
import json

from . import _base


def main() -> int:
    parser = argparse.ArgumentParser(description="Reenvia fotos pendentes ao Drive")
    parser.add_argument("--limite", type=int, default=50)
    args = parser.parse_args()
    from app.apps.ponto import db
    from app.apps.ponto.core import fotos
    with db.conexao() as conn:
        _base.exigir_schema(conn)
        resultado = fotos.enviar_pendentes(conn, limite=args.limite)
    print(json.dumps(resultado, ensure_ascii=False, indent=2))
    return 1 if resultado["falhas"] and not resultado["enviadas"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
