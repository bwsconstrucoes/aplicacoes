# -*- coding: utf-8 -*-
"""
parametros_omie.py
Grava dados na planilha intermediária e lê o resultado calculado pelas fórmulas.
Mantém a lógica do Sheets como motor de cálculo (não replica as fórmulas no Python).
"""

import time
import threading
import logging
from .utils import as_string, value_or_empty, column_to_letter, letter_to_column

logger = logging.getLogger(__name__)

CONFIG_OMIE_API = {
    'SPREADSHEET_ID': '1FyswS4ZlCr2f8VaVvA52hOmajLIQhQISh49S2l-75Wc',
    'SHEET_BOTAO':    'Bases Resumo Botão',
    'SHEET_PARCELA':  'Bases Resumo Parcela Botão',
    'INPUT_FIRST_COL': 1,   # A
    'INPUT_LAST_COL':  22,  # V
    'CLEAR_LAST_COL':  32,  # AF
    'OUTPUT_FIRST_COL': 1,
    'OUTPUT_LAST_COL':  22,
}

LINHAS_DADOS     = [4, 10, 16, 22, 28]
LINHAS_RESULTADO = [3,  9, 15, 21, 27]

HEADERS_RESULTADO = [
    'Centro de Custo 1', 'Centro de Custo 2', 'Centro de Custo 3',
    'Centro de Custo 4', 'Centro de Custo 5', 'Tipo de Despesa',
    'Vecimento',
    'Centro de Custo 1 Valor', 'Centro de Custo 1 %',
    'Centro de Custo 2 Valor', 'Centro de Custo 2 %',
    'Centro de Custo 3 Valor', 'Centro de Custo 3 %',
    'Centro de Custo 4 Valor', 'Centro de Custo 4 %',
    'Centro de Custo 5 Valor', 'Centro de Custo 5 %',
    'Somatório R$', 'Somatório %', 'Parcelado', 'Cliente Omie', 'Nº do Pedido',
]

# Lock global para serializar acessos aos slots da planilha intermediária
_SLOT_LOCK = threading.Lock()
LOCK_WAIT_S = 30


def secao_parametros_omie(payload: dict, gc) -> dict:
    """
    gc = cliente gspread autenticado.
    Valida, reserva slot, grava dados, lê resultado, libera slot.
    """
    if 'OmieApiA' not in payload:
        return {
            'ok': False, 'ignorado': True,
            'motivo': 'Campos OmieApiA:OmieApiV não enviados.'
        }

    _validar_payload_omie_api(payload)

    ss = gc.open_by_key(CONFIG_OMIE_API['SPREADSHEET_ID'])
    sheet_name = _selecionar_aba(payload)
    sh = ss.worksheet(sheet_name)

    with _SLOT_LOCK:
        slot = _reservar_slot(sh)
        if slot is None:
            raise RuntimeError('Nenhum slot OmieApi disponível no momento.')

        linha_resultado = slot
        linha_dados     = slot + 1

        try:
            _gravar_dados(sh, linha_dados, payload)
            time.sleep(2)  # aguarda o Sheets recalcular as fórmulas
            saida = _ler_resultado(sh, linha_resultado)
        finally:
            _limpar_dados(sh, linha_dados)

    return {
        'ok': True,
        'aba': sheet_name,
        'linhaDados': linha_dados,
        'linhaResultado': linha_resultado,
        'saida': saida,
    }


def _validar_payload_omie_api(payload: dict):
    for col in range(CONFIG_OMIE_API['INPUT_FIRST_COL'], CONFIG_OMIE_API['INPUT_LAST_COL'] + 1):
        key = 'OmieApi' + column_to_letter(col)
        if key not in payload:
            raise ValueError(f'Campo obrigatório não enviado: {key}')


def _selecionar_aba(payload: dict) -> str:
    automacao = as_string(payload.get('OmieApiAutomacao') or payload.get('Automacao') or '')
    return CONFIG_OMIE_API['SHEET_PARCELA'] if automacao else CONFIG_OMIE_API['SHEET_BOTAO']


def _linha_esta_livre(sh, row: int) -> bool:
    num_cols = CONFIG_OMIE_API['INPUT_LAST_COL'] - CONFIG_OMIE_API['INPUT_FIRST_COL'] + 1
    values = sh.row_values(row)
    # pega só as colunas de input
    valores = values[CONFIG_OMIE_API['INPUT_FIRST_COL']-1 : CONFIG_OMIE_API['INPUT_LAST_COL']]
    return all(as_string(v) == '' for v in valores)


def _reservar_slot(sh) -> int | None:
    for linha_dados in LINHAS_DADOS:
        if _linha_esta_livre(sh, linha_dados):
            return linha_dados - 1  # retorna linha de resultado
    return None


def _gravar_dados(sh, row: int, payload: dict):
    if row in LINHAS_RESULTADO:
        raise RuntimeError(f'Bloqueado: tentativa de gravar na linha de resultado {row}')
    values = []
    for col in range(CONFIG_OMIE_API['INPUT_FIRST_COL'], CONFIG_OMIE_API['INPUT_LAST_COL'] + 1):
        key = 'OmieApi' + column_to_letter(col)
        values.append(value_or_empty(payload.get(key)))
    col_a = column_to_letter(CONFIG_OMIE_API['INPUT_FIRST_COL'])
    sh.update(f'{col_a}{row}', [values], value_input_option='USER_ENTERED')


def _ler_resultado(sh, row: int) -> dict:
    if row not in LINHAS_RESULTADO:
        raise RuntimeError(f'Linha de resultado inválida: {row}')
    num_cols = CONFIG_OMIE_API['OUTPUT_LAST_COL'] - CONFIG_OMIE_API['OUTPUT_FIRST_COL'] + 1
    col_a = column_to_letter(CONFIG_OMIE_API['OUTPUT_FIRST_COL'])
    col_b = column_to_letter(CONFIG_OMIE_API['OUTPUT_LAST_COL'])
    values = sh.get(f'{col_a}{row}:{col_b}{row}')
    row_values = values[0] if values else []
    # Garante que temos exatamente num_cols itens
    row_values = (row_values + [''] * num_cols)[:num_cols]
    return {h: (row_values[i] if row_values[i] is not None else '') for i, h in enumerate(HEADERS_RESULTADO)}


def _limpar_dados(sh, row: int):
    if row in LINHAS_RESULTADO:
        raise RuntimeError(f'Bloqueado: tentativa de limpar linha de resultado {row}')
    col_a = column_to_letter(1)
    col_b = column_to_letter(CONFIG_OMIE_API['CLEAR_LAST_COL'])
    sh.batch_clear([f'{col_a}{row}:{col_b}{row}'])
