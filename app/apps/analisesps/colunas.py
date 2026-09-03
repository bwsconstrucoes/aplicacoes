# -*- coding: utf-8 -*-
"""
Fonte ÚNICA de verdade do mapeamento das colunas da aba SPsBD.

Portado do `schema.py` do Streamlit, sem mudar o significado de nada: a planilha
é a mesma, as letras são as mesmas, e mudar isso aqui quebraria a gravação de
volta. O que mudou foi só o destino — antes as colunas viravam uma tabela
SQLite, agora viram uma tabela Postgres.

A planilha tem 38 colunas (A..AL). Quatro delas são fórmulas que só fazem
sentido dentro do Sheets (S, W, AC) ou não são usadas (AK): ficam marcadas como
descartáveis e não sobem para o banco.

A coluna V é o CARIMBO de atualização. Ela é uma antiga fórmula reaproveitada,
e é o que torna a sincronização barata: em vez de reler 59 mil linhas, lê-se só
as colunas A e V, descobre-se quem mudou desde a última vez, e buscam-se apenas
essas linhas. Quem grava o carimbo é o gatilho `onEdit` da própria planilha.
"""
from __future__ import annotations

from collections import namedtuple

Col = namedtuple("Col", "chave idx letra rotulo tipo descartavel editavel")

# chave, índice 0-based, letra na planilha, rótulo na tela, tipo, descartável, editável
_DEFS = [
    Col("id",                0,  "A",  "ID",                 "texto",  False, False),
    Col("solicitacao",       1,  "B",  "Data",               "data",   False, False),
    Col("vencimento",        2,  "C",  "Vencimento",         "data",   False, False),
    Col("credor",            3,  "D",  "Credor",             "texto",  False, False),
    Col("documento",         4,  "E",  "CPF/CNPJ",           "texto",  False, False),
    Col("descricao",         5,  "F",  "Descrição",          "texto",  False, False),
    Col("valor",             6,  "G",  "Valor",              "numero", False, False),
    Col("centro_custo",      7,  "H",  "Centro de Custo",    "texto",  False, False),
    Col("tipo_despesa",      8,  "I",  "Tipo de Despesa",    "texto",  False, False),
    Col("forma_pagamento",   9,  "J",  "Forma de Pagamento", "texto",  False, False),
    Col("responsavel",       10, "K",  "Responsável",        "texto",  False, False),
    Col("dt_autorizacao",    11, "L",  "Dt. Autorização",    "data",   False, False),
    Col("resp_autorizacao",  12, "M",  "Resp. Autorização",  "texto",  False, False),
    Col("status_aut",        13, "N",  "Autorização SP",     "texto",  False, False),
    Col("status_pgt",        14, "O",  "Status Pgt",         "texto",  False, True),
    Col("codigo_integracao", 15, "P",  "Cód. Integração",    "texto",  False, False),
    Col("anexo_link",        16, "Q",  "Anexo",              "link",   False, False),
    Col("card_link",         17, "R",  "Card",               "link",   False, False),
    Col("_s",                18, "S",  "(fórmula S)",        "texto",  True,  False),
    Col("projeto",           19, "T",  "Projeto",            "texto",  False, False),
    Col("conta",             20, "U",  "Conta",              "texto",  False, False),
    Col("carimbo",           21, "V",  "Carimbo",            "texto",  False, False),
    Col("_w",                22, "W",  "(fórmula W)",        "texto",  True,  False),
    Col("data_pagamento",    23, "X",  "Data do Pagamento",  "data",   False, False),
    Col("info_pgt",          24, "Y",  "Informação p/ Pgt",  "texto",  False, False),
    Col("parcela",           25, "Z",  "Parcela",            "texto",  False, False),
    Col("nf",                26, "AA", "Nº NF",              "texto",  False, False),
    Col("agendado",          27, "AB", "Agendado",           "texto",  False, True),
    Col("_ac",               28, "AC", "(fórmula AC)",       "texto",  True,  False),
    Col("pedido",            29, "AD", "Pedido",             "texto",  False, False),
    Col("anuente",           30, "AE", "Anuente",            "texto",  False, False),
    Col("status_anuencia",   31, "AF", "Status Anuência",    "texto",  False, False),
    Col("comprovante",       32, "AG", "Comprovante",        "texto",  False, False),
    Col("validacao",         33, "AH", "Validação",          "texto",  False, False),
    Col("codigo_barras",     34, "AI", "Código de Barras",   "texto",  False, False),
    Col("id_contrato",       35, "AJ", "ID Pipefy Contrato", "texto",  False, False),
    Col("_ak",               36, "AK", "(não usada)",        "texto",  True,  False),
    Col("analise_ia",        37, "AL", "Análise IA",         "texto",  False, False),
]

COLS = {c.chave: c for c in _DEFS}

# As que sobem para o banco (descarta as fórmulas S, W, AC e a não usada AK).
GUARDADAS = [c for c in _DEFS if not c.descartavel]
CHAVES = [c.chave for c in GUARDADAS]

# As que o operador altera no dia a dia — e, portanto, as únicas que este
# módulo grava de volta na planilha. Qualquer outra é somente leitura.
EDITAVEIS = [c.chave for c in _DEFS if c.editavel]

CHAVE_CARIMBO = "carimbo"
PRIMEIRA_LINHA_DADOS = 2          # a linha 1 é o cabeçalho
ULTIMA_LETRA = "AL"

ROTULOS = {c.chave: c.rotulo for c in _DEFS}

# Colunas derivadas: a mesma informação já convertida para data ou número.
# O sufixo `_d` marca data; `valor_num` é o valor. Ver a migração 001.
DERIVADAS_DATA = {
    "solicitacao": "solicitacao_d",
    "vencimento": "vencimento_d",
    "data_pagamento": "data_pagamento_d",
    "dt_autorizacao": "dt_autorizacao_d",
}
DERIVADA_VALOR = ("valor", "valor_num")

# Ordem em que as colunas aparecem na tela, espelhando o uso na planilha.
ORDEM_TELA = [
    "id", "solicitacao", "vencimento", "credor", "documento",
    "tipo_despesa", "centro_custo", "projeto", "valor", "responsavel",
    "status_pgt", "status_aut", "forma_pagamento", "conta",
    "info_pgt", "nf", "pedido", "agendado", "data_pagamento",
    "anuente", "validacao", "comprovante", "codigo_barras",
    "analise_ia", "descricao", "anexo_link", "card_link",
]


def linha_para_dicionario(valores: list) -> dict:
    """Uma linha crua da planilha vira um dicionário só com o que guardamos.

    Linha curta não é erro: o Sheets corta as colunas vazias do fim, então uma
    SP sem comprovante nem análise chega com menos células do que o cabeçalho.
    O que faltar vira string vazia."""
    return {
        c.chave: (str(valores[c.idx]).strip() if c.idx < len(valores) else "")
        for c in GUARDADAS
    }
