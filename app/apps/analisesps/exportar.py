# -*- coding: utf-8 -*-
"""
Exportação para planilha.

É **CSV**, não `.xlsx`: gerar Excel de verdade exigiria uma biblioteca nova no
serviço, e a regra da casa é não acrescentar dependência sem combinar.

Três detalhes fazem o Excel em português abrir o arquivo direto, com dois
cliques, sem passar pelo assistente de importação:

  1. **BOM** no começo — sem ele, "Solicitação" vira "SolicitaÃ§Ã£o";
  2. **ponto e vírgula** separando as colunas — porque a vírgula é o separador
     decimal aqui, e o Excel brasileiro espera o ponto e vírgula;
  3. **vírgula nos centavos** — 6.750,00, não 6750.00.

Os três juntos, ou nenhum resolve.

E sai em BLOCOS, direto para o navegador: montar o arquivo inteiro na memória
antes de enviar é justamente o que a instância de 2 GB não suporta quando o
filtro é largo.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("analisesps.exportar")

BOM = "﻿"
SEPARADOR = ";"
FIM_DE_LINHA = "\r\n"          # o Excel espera o do Windows


def celula(valor) -> str:
    """Prepara um valor para caber numa célula.

    Ponto e vírgula ou quebra de linha dentro do texto partiriam a linha em
    duas colunas e desalinhariam a planilha inteira dali para baixo — por isso
    a quebra vira espaço e o texto com separador vai entre aspas, como o
    formato manda."""
    texto = "" if valor is None else str(valor)
    texto = texto.replace("\r", " ").replace("\n", " ").replace('"', '""')
    if SEPARADOR in texto or '"' in texto:
        return f'"{texto}"'
    return texto


def linhas_csv(cabecalho, geradora):
    """Monta o arquivo pedaço a pedaço, sem nunca tê-lo inteiro na memória."""
    yield BOM
    yield SEPARADOR.join(celula(c) for c in cabecalho) + FIM_DE_LINHA
    for linha in geradora:
        yield SEPARADOR.join(celula(c) for c in linha) + FIM_DE_LINHA


def resposta(nome_base: str, cabecalho, geradora):
    """A resposta HTTP com o arquivo, já nomeada com a data."""
    from flask import Response

    from .horario import agora

    nome = f"{nome_base}_{agora().strftime('%Y-%m-%d_%H%M')}.csv"
    return Response(
        linhas_csv(cabecalho, geradora),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{nome}"'})
