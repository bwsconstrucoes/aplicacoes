# -*- coding: utf-8 -*-
"""
O relatório do lote em Excel de verdade — `.xlsx`, não CSV com ponto e vírgula.

Pedido do dono: *"relatório do lote em Excel, por lote e de todos os lotes
juntos"*.

POR QUE AGORA DÁ, e até 05/09/2026 não dava: o `exportar.py` ao lado explica
que a exportação é CSV "porque gerar Excel de verdade exigiria uma biblioteca
nova, e a regra da casa é não acrescentar dependência sem combinar". **Isso
deixou de valer** quando o `openpyxl` entrou por causa do BeeVale. Excel de
verdade passou a ser possível sem nada novo no serviço.

O QUE O XLSX RESOLVE QUE O CSV NÃO RESOLVE, e é o motivo de ele existir:

  - **número é número.** No CSV o valor vai como texto "6.750,00" para o Excel
    brasileiro entender; aqui ele é número de fato, então a pessoa soma,
    ordena e filtra sem converter nada primeiro.
  - **ID de SP não vira notação científica.** "1443253428" o Excel até aguenta,
    mas um código de barras de 47 dígitos ele transforma em 1,23457E+46 — e o
    número volta arredondado, irrecuperável. As colunas de código vão como
    TEXTO de propósito.
  - **uma aba por lote**, na exportação de todos. Trinta lotes empilhados numa
    aba só seria o mesmo problema que o CSV já tem.

A MEMÓRIA É O LIMITE, como em todo este módulo: o `openpyxl` monta o arquivo
inteiro na memória antes de salvar — não há como sair em blocos, como o CSV
sai. Por isso há teto de linhas, e o CSV continua existindo para quem precisar
exportar a base larga.
"""
from __future__ import annotations

import io
import logging
import re

logger = logging.getLogger("analisesps.lote_excel")

# Teto de linhas por arquivo. Um lote de trabalho tem dezenas de SPs; milhares
# seria outro uso (e aí o caminho é o CSV, que sai em blocos). O teto existe
# para uma exportação enorme não derrubar o serviço dos outros 17 módulos.
MAXIMO_LINHAS = 20000

COLUNAS = [
    ("Grupo", "texto", 22),
    ("SP", "texto", 13),
    ("Vencimento", "data", 12),
    ("Credor", "texto", 34),
    ("CPF/CNPJ", "texto", 19),
    ("Descrição", "texto", 46),
    ("Obra", "texto", 18),
    ("Tipo de Despesa", "texto", 22),
    ("Forma", "texto", 16),
    ("Conta", "texto", 20),
    ("Nº NF", "texto", 12),
    ("Status Pgt", "texto", 13),
    ("Agendado", "texto", 12),
    ("Valor", "moeda", 14),
]


def _valor(linha: dict, rotulo: str):
    from .formatos import data_br

    if rotulo == "Grupo":
        return linha.get("_grupo", "")
    if rotulo == "SP":
        return str(linha.get("id") or "")
    if rotulo == "Vencimento":
        return data_br(linha.get("vencimento_d"))
    if rotulo == "Credor":
        return linha.get("credor") or ""
    if rotulo == "CPF/CNPJ":
        return linha.get("documento") or ""
    if rotulo == "Descrição":
        return linha.get("descricao") or ""
    if rotulo == "Obra":
        return linha.get("centro_custo") or ""
    if rotulo == "Tipo de Despesa":
        return linha.get("tipo_despesa") or ""
    if rotulo == "Forma":
        return linha.get("forma_pagamento") or ""
    if rotulo == "Conta":
        return linha.get("conta") or ""
    if rotulo == "Nº NF":
        return str(linha.get("nf") or "")
    if rotulo == "Status Pgt":
        return linha.get("status_pgt") or ""
    if rotulo == "Agendado":
        return linha.get("status_agend") or ""
    if rotulo == "Valor":
        return float(linha.get("valor_num") or 0)
    return ""


def _escrever_aba(aba, montado: dict, com_coluna_de_grupo: bool = True) -> int:
    """Põe um lote numa aba. Devolve quantas linhas de SP foram escritas."""
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    colunas = [c for c in COLUNAS
               if com_coluna_de_grupo or c[0] != "Grupo"]

    aba.append([c[0] for c in colunas])
    cabecalho = aba[1]
    for celula in cabecalho:
        celula.font = Font(bold=True)
        celula.fill = PatternFill("solid", fgColor="EEF2FA")
        celula.alignment = Alignment(vertical="center")

    escritas = 0
    for grupo in montado.get("grupos") or []:
        for linha in grupo.get("linhas") or []:
            if escritas >= MAXIMO_LINHAS:
                logger.warning("Análise de SPs: exportação cortada no teto de "
                               "%d linhas.", MAXIMO_LINHAS)
                break
            linha = dict(linha)
            linha["_grupo"] = grupo.get("titulo_exibido") or ""
            aba.append([_valor(linha, c[0]) for c in colunas])
            escritas += 1

    for n, (rotulo, tipo, largura) in enumerate(colunas, start=1):
        letra = get_column_letter(n)
        aba.column_dimensions[letra].width = largura
        for celula in aba[letra][1:]:          # pula o cabeçalho
            if tipo == "moeda":
                # NÚMERO DE VERDADE: a pessoa soma e ordena sem converter.
                celula.number_format = 'R$ #,##0.00'
            else:
                # TEXTO DE PROPÓSITO. Sem isto o Excel transforma um código de
                # barras de 47 dígitos em 1,23457E+46 — e o número volta
                # arredondado, irrecuperável.
                celula.number_format = "@"

    # O TOTAL VAI NO ARQUIVO, e como FÓRMULA. Quem filtrar a planilha depois
    # espera o total acompanhar; um número fixo mentiria em silêncio.
    if escritas:
        coluna_valor = get_column_letter(len(colunas))
        aba.append([])
        linha_total = aba.max_row + 1
        aba.cell(row=linha_total, column=len(colunas) - 1, value="TOTAL").font = \
            Font(bold=True)
        total = aba.cell(
            row=linha_total, column=len(colunas),
            value=f"=SUBTOTAL(109,{coluna_valor}2:{coluna_valor}{escritas + 1})")
        total.font = Font(bold=True)
        total.number_format = 'R$ #,##0.00'

    aba.freeze_panes = "A2"
    aba.auto_filter.ref = f"A1:{get_column_letter(len(colunas))}{escritas + 1}"
    return escritas


# Os caracteres que o Excel RECUSA num nome de aba. Um título de lote é texto
# livre, escrito por gente — "Pagar 15/09" tem barra, e "Depois: urgente" tem
# dois pontos. Deixar passar faria o arquivo inteiro não abrir por causa disso.
_PROIBIDOS = re.compile(r"[:\\/?*\[\]]")


def _nome_de_aba(bruto: str, usados: set) -> str:
    """Nome de aba que o Excel aceita, sem repetir.

    O Excel recusa `: \\ / ? * [ ]` e mais de 31 caracteres — e um título de
    lote é texto livre, escrito por gente. Silenciar isso faria o arquivo
    inteiro não abrir por causa de uma barra num título."""
    nome = _PROIBIDOS.sub("-", str(bruto or "").strip())[:31]
    nome = nome or "Lote"
    base, n = nome, 2
    while nome.lower() in usados:
        sufixo = f" ({n})"
        nome = base[:31 - len(sufixo)] + sufixo
        n += 1
    usados.add(nome.lower())
    return nome


def de_um_lote(montado: dict, titulo: str = "Lote") -> bytes:
    """O lote de uma pessoa, numa aba só, com a coluna do grupo."""
    from openpyxl import Workbook

    planilha = Workbook()
    aba = planilha.active
    aba.title = _nome_de_aba(titulo, set())
    _escrever_aba(aba, montado)
    memoria = io.BytesIO()
    planilha.save(memoria)
    return memoria.getvalue()


def de_todos(lotes: list) -> bytes:
    """Todos os lotes, UMA ABA POR PESSOA, mais um resumo na primeira.

    `lotes`: [{"nome": "Marcelo", "montado": {...}}, ...].

    O RESUMO VEM PRIMEIRO de propósito: quem abre um arquivo de oito abas quer
    ver o tamanho do todo antes de escolher em qual entrar."""
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    planilha = Workbook()
    resumo = planilha.active
    resumo.title = "Resumo"
    resumo.append(["Pessoa", "SPs", "Total"])
    for celula in resumo[1]:
        celula.font = Font(bold=True)

    usados = {"resumo"}
    for item in lotes:
        montado = item.get("montado") or {}
        aba = planilha.create_sheet(_nome_de_aba(item.get("nome"), usados))
        escritas = _escrever_aba(aba, montado)
        resumo.append([item.get("nome") or "",
                       escritas,
                       float(montado.get("total_geral") or 0)])

    for n, largura in enumerate((30, 10, 16), start=1):
        resumo.column_dimensions[get_column_letter(n)].width = largura
    for celula in resumo["C"][1:]:
        celula.number_format = 'R$ #,##0.00'
    if len(lotes):
        # A ÚLTIMA LINHA DE DADOS É LIDA ANTES de a linha do total existir. Ler
        # depois faria a soma incluir a própria célula — referência circular, e
        # o Excel abre com erro em vez de com o número.
        ultima_com_dado = resumo.max_row
        linha = ultima_com_dado + 1
        resumo.cell(row=linha, column=1, value="TOTAL GERAL").font = Font(bold=True)
        total = resumo.cell(row=linha, column=3,
                            value=f"=SUM(C2:C{ultima_com_dado})")
        total.font = Font(bold=True)
        total.number_format = 'R$ #,##0.00'
    resumo.freeze_panes = "A2"

    memoria = io.BytesIO()
    planilha.save(memoria)
    return memoria.getvalue()
