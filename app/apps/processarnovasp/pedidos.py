# -*- coding: utf-8 -*-
"""
pedidos.py — Vinculação da SP a pedidos de compra existentes.

Equivalente ao sub-fluxo 683→689→{686/687/694 | 690→691→692→696→{693/695}} do Make.

Planilha: 1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY
Aba:      Registros
Colunas relevantes (0-based):
  A (0) = Número do Pedido
  F (5) = Lista de SPs vinculadas (concat por ", ")
  J (9) = card_id Pipefy do pedido
  L (11)= Status (precisa conter "A" para ser ativo)
"""

import logging
from .utils import as_string
from . import pipefy as pipefy_mod

logger = logging.getLogger(__name__)

PLANILHA_PEDIDOS_ID = '1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY'
SHEET_PEDIDOS       = 'Registros'

COL_PEDIDO_NUMERO   = 0   # A
COL_SPS_LISTA       = 5   # F
COL_CARD_PEDIDO     = 9   # J
COL_STATUS          = 11  # L


def vincular(payload: dict, gc) -> dict:
    """
    Lê o campo NumeroPedido, divide por vírgula, e para cada pedido:
      1. Localiza linha em Registros (filtro A == numero E L contém "A")
      2. Atualiza coluna F adicionando o ID da SP atual
      3. Atualiza Pipefy do card do pedido (conex_o_sp) com o ID da SP
    """
    id_sp     = as_string(payload.get('id'))
    pedido    = as_string(payload.get('NumeroPedido') or '')
    if not pedido:
        return {'executado': False, 'motivo': 'NumeroPedido vazio'}

    # Split por vírgula (ou retorna lista de 1 item)
    pedidos = [p.strip() for p in pedido.split(',') if p.strip()]

    ss = gc.open_by_key(PLANILHA_PEDIDOS_ID)
    sh = ss.worksheet(SHEET_PEDIDOS)

    # Lê todas as linhas uma vez (mais rápido que filterRows N vezes)
    todas = sh.get_all_values()
    # mapeia número → (row_index 1-based, valores)
    indice = {}
    for i, row in enumerate(todas[1:], start=2):  # pula header
        if len(row) <= COL_STATUS:
            continue
        num = row[COL_PEDIDO_NUMERO].strip()
        status = row[COL_STATUS].strip()
        if not num or 'A' not in status:
            continue
        # mantém a primeira ocorrência ativa por número
        indice.setdefault(num, (i, row))

    atualizados = []
    not_found   = []
    pipefy_acks = []

    for num in pedidos:
        if num not in indice:
            not_found.append(num)
            continue

        row_idx, row = indice[num]
        sps_atual = row[COL_SPS_LISTA].strip() if len(row) > COL_SPS_LISTA else ''
        # se SP atual já está, pula
        if id_sp in [s.strip() for s in sps_atual.split(',')]:
            atualizados.append({'pedido': num, 'row': row_idx, 'skip': 'já vinculada'})
            continue

        nova_lista = id_sp if not sps_atual else f'{sps_atual}, {id_sp}'
        # Atualiza coluna F da linha
        col_letter = _col_to_letter(COL_SPS_LISTA + 1)
        sh.update(f'{col_letter}{row_idx}', [[nova_lista]], value_input_option='USER_ENTERED')
        atualizados.append({'pedido': num, 'row': row_idx, 'nova_lista': nova_lista})

        # Atualiza Pipefy do card do pedido (conex_o_sp)
        card_pedido = row[COL_CARD_PEDIDO].strip() if len(row) > COL_CARD_PEDIDO else ''
        if card_pedido:
            ack = pipefy_mod.conectar_sp_a_pedido(card_pedido, id_sp)
            pipefy_acks.append({'pedido': num, 'card_pedido': card_pedido, 'ack': ack})

    return {
        'executado':            True,
        'pedidos':              pedidos,
        'pedidos_atualizados':  len(atualizados),
        'detalhes':             atualizados,
        'nao_encontrados':      not_found,
        'pipefy_acks':          pipefy_acks,
    }


def _col_to_letter(col: int) -> str:
    result = ''
    while col > 0:
        col, rem = divmod(col - 1, 26)
        result = chr(65 + rem) + result
    return result
