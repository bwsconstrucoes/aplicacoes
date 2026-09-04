# -*- coding: utf-8 -*-
"""
As colunas da tabela de SPs — quais existem, como se chamam, como se mostram.

ESPELHA O `GRID_COLS` DO STREAMLIT, na mesma ordem e com os mesmos rótulos.
A conversão tinha reduzido a tabela a nove colunas; o dono trabalhava com
vinte, e a coluna que falta é sempre a que ele precisava naquele minuto —
Validação, Nº NF, Data Pgt, Responsável, CPF/CNPJ.

CADA PESSOA ESCOLHE O QUE VÊ. Era assim lá (a configuração de tabela ficava
salva na base local), e é assim de novo: a escolha vai para `preferencias`,
com a chave da pessoa. Quem trabalha com pagamento quer Conta e Informação
p/ Pgt; quem confere quer Validação e Autorização. Uma tabela só, com vinte
colunas para todo mundo, não serve a nenhum dos dois.

O `tipo` diz só COMO PINTAR, não o que é o dado: a formatação de data, moeda
e selo vive nos gabaritos, num lugar só, e não repetida em cada `<td>`.
"""
from __future__ import annotations

from collections import namedtuple

Coluna = namedtuple("Coluna", "chave rotulo tipo padrao")

# tipo:
#   texto   — como veio
#   data    — DD/MM/AAAA
#   moeda   — R$ 1.234,56, alinhado à direita
#   id      — o número da SP, que abre o card no Pipefy
#   status  — selo colorido de Status Pgt
#   agend   — selo colorido de Agendamento
#   link    — abre em outra aba quando começa com http
#   alertas — Risco / Cadastro incompleto
#   longo   — texto comprido (a descrição): letra menor e cortado na largura,
#             com o texto inteiro no title. É a única coluna que compete com a
#             tela toda, e por isso tem tratamento próprio.
#
# padrao: o que vem marcado para quem nunca escolheu. O conjunto foi definido
# pelo dono olhando a tela (04/09/2026): entram VALIDAÇÃO — é ela que destrava
# o agendamento, e não vê-la é trabalhar às cegas —, TIPO DE DESPESA e
# DESCRIÇÃO; fica de fora RESPONSÁVEL, que ele não usa no dia a dia. Quem
# quiser qualquer uma das outras acrescenta pelo botão, e a escolha fica
# guardada.
#
# Sobre "Tipo de Despesa": é a classificação de despesa que existe NA SP
# (coluna I da SPsBD). "Categoria de Despesa" no sentido do Omie é outra
# coisa e só aparece na tela de Ratear — ela não é gravada em cada SP.
#
# Sobre "Obra": é o CENTRO DE CUSTO da planilha (coluna H). O cabeçalho usa a
# palavra que o dono usa; a barra de filtros diz "Obra (centro de custo)"
# porque lá cabe, e é ela que faz a ponte com o nome da planilha. A célula
# pode trazer mais de uma obra — ver `consultas.MULTIPLAS_NA_CELULA`.
DEFINICOES = [
    Coluna("id",               "ID",                  "id",      True),
    Coluna("solicitacao_d",    "Data",                "data",    False),
    Coluna("vencimento_d",     "Vencimento",          "data",    True),
    Coluna("credor",           "Credor",              "texto",   True),
    Coluna("descricao",        "Descrição",           "longo",   True),
    Coluna("documento",        "CPF/CNPJ",            "texto",   False),
    Coluna("tipo_despesa",     "Tipo de Despesa",     "texto",   True),
    Coluna("valor_num",        "Valor",               "moeda",   True),
    Coluna("centro_custo",     "Obra",                "texto",   True),
    Coluna("status_pgt",       "Status Pgt",          "status",  True),
    Coluna("status_agend",     "Status Agend",        "agend",   True),
    Coluna("forma_pagamento",  "Forma de Pgt",        "texto",   True),
    Coluna("conta",            "Conta Corrente",      "texto",   True),
    Coluna("validacao",        "Validação",           "texto",   True),
    Coluna("info_pgt",         "Informação p/ Pgt",   "texto",   False),
    Coluna("nf",               "Nº NF",               "texto",   False),
    Coluna("data_pagamento_d", "Data Pgt",            "data",    False),
    Coluna("comprovante",      "Comprovante",         "link",    False),
    Coluna("responsavel",      "Responsável",         "texto",   False),
    Coluna("projeto",          "Projeto",             "texto",   False),
    Coluna("alertas",          "Alertas",             "alertas", True),
]

POR_CHAVE = {c.chave: c for c in DEFINICOES}
CHAVES = [c.chave for c in DEFINICOES]
PADRAO = [c.chave for c in DEFINICOES if c.padrao]

# A chave da preferência. Fica aqui, e não solta numa tela, para o dia em que
# houver uma segunda tabela configurável.
PREFERENCIA = "colunas_tabela"


def escolhidas(guardado) -> list:
    """As colunas que esta pessoa vê, na ordem fixa da tabela.

    `guardado` é o que veio das preferências — pode ser lixo, de uma versão
    anterior ou de um erro. Nada aqui pode derrubar a tela por causa disso:
    o que não se reconhece é ignorado, e uma escolha vazia volta ao padrão.
    Tabela sem coluna nenhuma não é uma escolha, é um acidente."""
    if isinstance(guardado, dict):
        guardado = guardado.get("colunas")
    if not isinstance(guardado, list):
        return [POR_CHAVE[c] for c in PADRAO]
    marcadas = {str(c) for c in guardado if str(c) in POR_CHAVE}
    if not marcadas:
        return [POR_CHAVE[c] for c in PADRAO]
    # A ORDEM é a da definição, nunca a da escolha: a tabela tem de ficar
    # sempre com a mesma cara, senão cada pessoa lê num lugar diferente.
    return [c for c in DEFINICOES if c.chave in marcadas]


# A coluna que mais atrapalha quando não se quer ela: comprida, e no meio da
# tabela. Ganha um botão próprio de mostrar/esconder, ao lado da lista de
# colunas — abrir a lista inteira para tirar uma coluna só é caro demais para
# uma coisa que se faz dez vezes por dia.
ALTERNAVEL = "descricao"
