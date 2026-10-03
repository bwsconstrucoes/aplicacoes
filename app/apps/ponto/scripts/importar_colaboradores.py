# -*- coding: utf-8 -*-
"""Importa colaboradores de uma planilha (.xlsx ou .csv). Uso:

    python -m app.apps.ponto.scripts.importar_colaboradores pessoas.xlsx           # simula
    python -m app.apps.ponto.scripts.importar_colaboradores pessoas.xlsx --gravar  # grava

Colunas (nome com ou sem acento, qualquer caixa): cpf, nome, obra (código ou
nome exato do ERP), centro_custo, tipo_jornada (PADRAO_4 | VIGIA_DIURNO_2 |
VIGIA_NOTURNO_2; vazio = PADRAO_4).

Pessoa que não existe no ERP é CRIADA lá (nome, CPF, obra). Pessoa que já existe
não tem nada do ERP alterado; jornada e centro de custo vão para o ponto."""
from __future__ import annotations

import argparse

from . import _base


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa colaboradores para o ponto")
    parser.add_argument("arquivo", help="planilha .xlsx ou .csv")
    parser.add_argument("--gravar", action="store_true", help="grava (sem isto, só simula)")
    args = parser.parse_args()

    from app.apps.ponto import db
    from app.apps.ponto.core import importacao
    registros = importacao.ler_tabela(args.arquivo)
    print(f"{len(registros)} linhas lidas de {args.arquivo}")
    with db.conexao() as conn:
        _base.exigir_schema(conn)
        relatorio = importacao.importar_colaboradores(conn, registros, gravar=args.gravar)
    print(importacao.resumo(relatorio))
    return 1 if relatorio["erros"] and not (relatorio["criados"] or relatorio["atualizados"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
