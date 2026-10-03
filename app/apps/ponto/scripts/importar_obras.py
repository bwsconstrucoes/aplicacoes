# -*- coding: utf-8 -*-
"""Importa obras de uma planilha (.xlsx ou .csv). Uso:

    python -m app.apps.ponto.scripts.importar_obras obras.xlsx                        # simula
    python -m app.apps.ponto.scripts.importar_obras obras.xlsx --gravar               # grava
    python -m app.apps.ponto.scripts.importar_obras obras.xlsx --gravar --sobrescrever-coordenadas

Colunas: codigo, nome, centro_custo, latitude, longitude, raio_metros. Latitude e
longitude podem vir vazias (serão capturadas depois pelo celular); obra sem
coordenada recebe batida EM_ANALISE até ter.

Obra que não existe no ERP é criada (código e nome). Coordenada que já existe
no ERP só é trocada com --sobrescrever-coordenadas."""
from __future__ import annotations

import argparse

from . import _base


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa obras para o ponto")
    parser.add_argument("arquivo", help="planilha .xlsx ou .csv")
    parser.add_argument("--gravar", action="store_true", help="grava (sem isto, só simula)")
    parser.add_argument("--sobrescrever-coordenadas", action="store_true",
                        help="troca latitude/longitude que já existam no ERP")
    args = parser.parse_args()

    from app.apps.ponto import db
    from app.apps.ponto.core import importacao
    registros = importacao.ler_tabela(args.arquivo)
    print(f"{len(registros)} linhas lidas de {args.arquivo}")
    with db.conexao() as conn:
        _base.exigir_schema(conn)
        relatorio = importacao.importar_obras(
            conn, registros, gravar=args.gravar,
            sobrescrever_coordenadas=args.sobrescrever_coordenadas)
    print(importacao.resumo(relatorio))
    return 1 if relatorio["erros"] and not (relatorio["criadas"] or relatorio["atualizadas"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
