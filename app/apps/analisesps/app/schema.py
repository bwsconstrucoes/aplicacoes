# -*- coding: utf-8 -*-
"""
schema.py — Fonte ÚNICA de verdade do mapeamento de colunas da aba SPsBD.

A planilha SPsBD (1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA) tem 38 colunas (A..AL).
Aqui mapeamos cada coluna para uma chave interna (snake_case), com:
  - idx     : índice 0-based na linha da planilha
  - letter  : letra da coluna no Sheets
  - label   : rótulo amigável exibido na UI
  - tipo    : 'texto' | 'numero' | 'data' | 'link'
  - drop    : True para colunas que NÃO entram na nova solução (fórmulas S..W e AC)
  - editavel: True para colunas que o usuário altera no dia a dia (O e AB)

Coluna de timestamp: reaproveitamos a coluna V (antiga fórmula, descartável na nova
solução) como "carimbo" de atualização — assim NÃO adicionamos coluna nova e não
mexemos na estrutura usada pelo AppSheet. O gatilho onEdit (apps_script_carimbo.gs)
passa a gravar o timestamp da linha editada nessa coluna V.
"""

from collections import namedtuple

Col = namedtuple("Col", "key idx letter label tipo drop editavel")

# idx, letra, chave, rótulo, tipo, drop, editável
_DEFS = [
    Col("id",               0,  "A",  "ID",                 "texto",  False, False),
    Col("solicitacao",      1,  "B",  "Data",               "data",   False, False),
    Col("vencimento",       2,  "C",  "Vencimento",         "data",   False, False),
    Col("credor",           3,  "D",  "Credor",             "texto",  False, False),
    Col("documento",        4,  "E",  "CPF/CNPJ",           "texto",  False, False),
    Col("descricao",        5,  "F",  "Descrição",          "texto",  False, False),
    Col("valor",            6,  "G",  "Valor",              "numero", False, False),
    Col("centro_custo",     7,  "H",  "Centro de Custo",    "texto",  False, False),
    Col("tipo_despesa",     8,  "I",  "Tipo de Despesa",    "texto",  False, False),
    Col("forma_pagamento",  9,  "J",  "Forma de Pagamento", "texto",  False, False),
    Col("responsavel",      10, "K",  "Responsável",        "texto",  False, False),
    Col("dt_autorizacao",   11, "L",  "Dt. Autorização",    "data",   False, False),
    Col("resp_autorizacao", 12, "M",  "Resp. Autorização",  "texto",  False, False),
    Col("status_aut",       13, "N",  "Autorização SP",     "texto",  False, False),
    Col("status_pgt",       14, "O",  "Status Pgt",         "texto",  False, True),   # editável
    Col("codigo_integracao",15, "P",  "Cód. Integração",    "texto",  False, False),
    Col("anexo_link",       16, "Q",  "Anexo",              "link",   False, False),
    Col("card_link",        17, "R",  "Card",               "link",   False, False),
    Col("_s",               18, "S",  "(fórmula S)",        "texto",  True,  False),
    Col("projeto",          19, "T",  "Projeto",            "texto",  False, False),
    Col("conta",            20, "U",  "Conta",              "texto",  False, False),
    Col("carimbo",          21, "V",  "Carimbo",            "texto",  False, False),  # timestamp p/ sync
    Col("_w",               22, "W",  "(fórmula W)",        "texto",  True,  False),
    Col("data_pagamento",   23, "X",  "Data do Pagamento",  "data",   False, False),
    Col("info_pgt",         24, "Y",  "Informação p/ Pgt",  "texto",  False, False),
    Col("parcela",          25, "Z",  "Parcela",            "texto",  False, False),
    Col("nf",               26, "AA", "Nº NF",              "texto",  False, False),
    Col("agendado",         27, "AB", "Agendado",           "texto",  False, True),   # editável
    Col("_ac",              28, "AC", "(fórmula AC)",       "texto",  True,  False),
    Col("pedido",           29, "AD", "Pedido",             "texto",  False, False),
    Col("anuente",          30, "AE", "Anuente",            "texto",  False, False),
    Col("status_anuencia",  31, "AF", "Status Anuência",    "texto",  False, False),
    Col("comprovante",      32, "AG", "Comprovante",        "texto",  False, False),
    Col("validacao",        33, "AH", "Validação",          "texto",  False, False),
    Col("codigo_barras",    34, "AI", "Código de Barras",   "texto",  False, False),
    Col("id_contrato",      35, "AJ", "ID Pipefy Contrato", "texto",  False, False),
    Col("_ak",              36, "AK", "(não usada)",        "texto",  True,  False),
    Col("analise_ia",       37, "AL", "Análise IA",         "texto",  False, False),
]

COLS = {c.key: c for c in _DEFS}
ALL_KEYS = [c.key for c in _DEFS]

# Colunas que sobrevivem na nova solução (descarta fórmulas S..W e AC)
KEPT = [c for c in _DEFS if not c.drop]
KEPT_KEYS = [c.key for c in KEPT]

# Chaves editáveis no dia a dia
EDITAVEIS = [c.key for c in _DEFS if c.editavel]

# Coluna de timestamp para sincronização incremental
CARIMBO_KEY = "carimbo"

# Ordem padrão de exibição no grid (Relatório), espelhando o uso na planilha
DISPLAY_ORDER = [
    "id", "solicitacao", "vencimento", "credor", "documento",
    "tipo_despesa", "centro_custo", "projeto", "valor", "responsavel",
    "status_pgt", "status_aut", "forma_pagamento", "conta",
    "info_pgt", "nf", "pedido", "agendado", "data_pagamento",
    "anuente", "validacao", "comprovante", "codigo_barras",
    "analise_ia", "descricao", "anexo_link", "card_link",
]


def labels_map():
    """key -> label."""
    return {c.key: c.label for c in _DEFS}


def numero_keys():
    return [c.key for c in _DEFS if c.tipo == "numero"]


def data_keys():
    return [c.key for c in _DEFS if c.tipo == "data"]