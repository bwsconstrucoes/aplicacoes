# -*- coding: utf-8 -*-
"""sheets.py — Atualização de Log e SPsBD via gspread (portado do AtualizaLogeSPsBD.gs)"""

import logging
from .utils import (
    as_string, value_or_empty, has_value,
    letter_to_column, column_to_letter,
    formatar_moeda_br
)

logger = logging.getLogger(__name__)

CONFIG = {
    'SHEET_SPSBD':    'SPsBD',
    'SHEET_LOG':      'Log',
    'LOG_BLUR_VALUE': '######',
    'LOG_BLUR_COLUMNS': list('ABCDEFGHILMNS') + ['U'],
}

COLUNAS_SPSBD = (
    list('ABCDEFGHIJKLMNOPQRSTUVWXYZ') +
    ['AA','AB','AC','AD','AE','AF','AG','AH','AI']
)


def secao_atualiza_log_e_spsbd(ss, payload: dict) -> dict:
    sh_sp  = ss.worksheet(payload.get('spsbdSheetName') or CONFIG['SHEET_SPSBD'])
    sh_log = ss.worksheet(payload.get('logSheetName')   or CONFIG['SHEET_LOG'])

    id_val = as_string(payload.get('id'))
    if not id_val:
        raise ValueError('ID não informado.')

    # --- LOG ---
    log_rows = _find_all_rows_by_id(sh_log, id_val)
    log_maps = _build_log_maps(payload)

    if log_rows and len(log_rows) == len(log_maps):
        ordered = sorted(log_rows)
        for i, row in enumerate(ordered):
            _update_row_by_map(sh_log, row, log_maps[i])
        log_modo = 'update'
    else:
        if log_rows:
            _blur_rows(
                sh_log, log_rows,
                payload.get('LogBlurColumns') or CONFIG['LOG_BLUR_COLUMNS'],
                payload.get('LogBlurValue')   or CONFIG['LOG_BLUR_VALUE']
            )
        if log_maps:
            _append_map_rows(sh_log, log_maps)
        log_modo = 'blur_add'

    # --- SPsBD ---
    sp_row = _find_last_row_by_id(sh_sp, id_val)
    if sp_row:
        spsbd_map = _build_spsbd_map(payload, ignore_blanks=True)
        _update_row_by_map(sh_sp, sp_row, spsbd_map)
        spsbd_modo = 'update'
    else:
        spsbd_map = _build_spsbd_map(payload, ignore_blanks=False)
        _append_map_rows(sh_sp, [spsbd_map])
        sp_row = None
        spsbd_modo = 'insert'

    return {
        'ok': True,
        'id': id_val,
        'log':   {'linhasEncontradas': len(log_rows), 'linhasGeradas': len(log_maps), 'modo': log_modo},
        'spsbd': {'modo': spsbd_modo, 'row': sp_row},
    }


def atualizar_spsbd_codigo_integracao_omie(ss, payload: dict, codigo_integracao: str) -> dict:
    sh_sp  = ss.worksheet(payload.get('spsbdSheetName') or CONFIG['SHEET_SPSBD'])
    id_val = as_string(payload.get('id'))
    sp_row = _find_last_row_by_id(sh_sp, id_val)
    if not sp_row:
        return {'ok': False, 'row': None, 'codigo': codigo_integracao, 'motivo': 'Registro não encontrado em SPsBD.'}
    col_p = letter_to_column('P')
    sh_sp.update_cell(sp_row, col_p, codigo_integracao)
    return {'ok': True, 'row': sp_row, 'codigo': codigo_integracao}


# ---------------------------------------------------------------------------
# BUILD DOS MAPS
# ---------------------------------------------------------------------------

def _build_spsbd_map(p: dict, ignore_blanks: bool) -> dict:
    result = {}
    for col in COLUNAS_SPSBD:
        key   = 'SPsBD' + col
        value = p.get(key)
        if ignore_blanks:
            if value is not None and str(value) != '':
                result[col] = value
        else:
            result[col] = '' if value is None else value
    if not ignore_blanks and not result.get('A'):
        result['A'] = as_string(p.get('id'))
    return result


def _build_log_maps(p: dict) -> list:
    maps = []
    base = {
        'A': value_or_empty(p.get('LogA')),
        'B': value_or_empty(p.get('LogB')),
        'C': value_or_empty(p.get('LogC')),
        'D': value_or_empty(p.get('LogD')),
        'E': value_or_empty(p.get('LogE')),
        'F': value_or_empty(p.get('LogF')),
        'I': value_or_empty(p.get('LogI')),
        'L': value_or_empty(p.get('LogL')),
        'M': value_or_empty(p.get('LogM')),
        'N': value_or_empty(p.get('LogN')),
        'U': value_or_empty(p.get('LogU')),
    }
    for i in range(1, 6):
        centro = p.get(f'LogCentro{i}')
        if has_value(centro):
            maps.append({
                **base,
                'G': value_or_empty(centro),
                'H': formatar_moeda_br(p.get(f'LogValor{i}') or ''),
                'S': f'Centro de Custo {i}',
            })
    return maps


# ---------------------------------------------------------------------------
# LEITURA DE LINHAS
# Uma unica chamada col_values(1) traz toda a coluna A em memoria.
# Busca de baixo para cima — IDs recentes ficam no final da planilha.
# ---------------------------------------------------------------------------

def _find_last_row_by_id(sh, id_val: str) -> int | None:
    """Retorna o numero da ultima linha (1-based) que contem id_val na coluna A."""
    values = sh.col_values(1)  # 1 chamada de rede, retorna lista completa
    # Percorre de baixo para cima — para na primeira ocorrencia (mais recente)
    for i in range(len(values) - 1, 0, -1):  # ignora indice 0 = cabecalho
        if as_string(values[i]) == id_val:
            return i + 1  # converte para 1-based
    return None


def _find_all_rows_by_id(sh, id_val: str) -> list:
    """Retorna todas as linhas (1-based) com id_val na coluna A, da mais recente para a mais antiga."""
    values = sh.col_values(1)
    rows = []
    for i in range(len(values) - 1, 0, -1):
        if as_string(values[i]) == id_val:
            rows.append(i + 1)
    return rows


# ---------------------------------------------------------------------------
# ESCRITA
# ---------------------------------------------------------------------------

def _update_row_by_map(sh, row_number: int, map_cols: dict):
    """Atualiza colunas específicas de uma linha via batch."""
    normalized = {k.upper(): v for k, v in map_cols.items() if re.match(r'^[A-Z]+$', k.upper())}
    if not normalized:
        return
    items = sorted(
        [{'col': letter_to_column(k), 'letter': k, 'value': v} for k, v in normalized.items()],
        key=lambda x: x['col']
    )
    # Agrupa colunas contíguas em um único update
    groups, current = [], [items[0]]
    for i in range(1, len(items)):
        if items[i]['col'] == items[i-1]['col'] + 1:
            current.append(items[i])
        else:
            groups.append(current)
            current = [items[i]]
    groups.append(current)

    updates = []
    for group in groups:
        col_a = column_to_letter(group[0]['col'])
        updates.append({
            'range':  f'{col_a}{row_number}',
            'values': [[item['value'] for item in group]],
        })
    sh.batch_update(updates, value_input_option='USER_ENTERED')


def _blur_rows(sh, row_numbers: list, columns: list, blur_value: str):
    """Preenche colunas específicas com blur_value nas linhas indicadas."""
    col_indexes = sorted(set(letter_to_column(c) for c in columns))
    # Agrupa contíguas
    groups, current = [], [col_indexes[0]]
    for i in range(1, len(col_indexes)):
        if col_indexes[i] == col_indexes[i-1] + 1:
            current.append(col_indexes[i])
        else:
            groups.append(current)
            current = [col_indexes[i]]
    groups.append(current)

    updates = []
    for row in row_numbers:
        for group in groups:
            col_a = column_to_letter(group[0])
            updates.append({
                'range':  f'{col_a}{row}',
                'values': [[blur_value] * len(group)],
            })
    if updates:
        sh.batch_update(updates, value_input_option='USER_ENTERED')


def _append_map_rows(sh, maps: list):
    if not maps:
        return
    normalized = [{k.upper(): v for k, v in m.items() if re.match(r'^[A-Z]+$', k.upper())} for m in maps]
    max_col = max(letter_to_column(k) for m in normalized for k in m.keys())
    rows = []
    for m in normalized:
        arr = [''] * max_col
        for col_letter, value in m.items():
            arr[letter_to_column(col_letter) - 1] = value
        rows.append(arr)

    # Usa append_rows em vez de update — nunca ultrapassa o limite do grid
    # e não depende de calcular last_row manualmente
    sh.append_rows(rows, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS')


import re  # noqa: E402 (já importado mas necessário para _update_row_by_map)