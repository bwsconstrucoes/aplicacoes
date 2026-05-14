# -*- coding: utf-8 -*-
"""utils.py — Funções auxiliares portadas do Apps Script Utils.gs"""

import re
import math
from datetime import datetime, timedelta


def as_string(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def value_or_empty(v):
    return "" if v is None else v


def has_value(v) -> bool:
    return v is not None and str(v).strip() != ""


def column_to_letter(col: int) -> str:
    result = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        result = chr(65 + rem) + result
    return result


def letter_to_column(letter: str) -> int:
    col = 0
    for ch in str(letter).strip().upper():
        col = col * 26 + (ord(ch) - 64)
    return col


def limpar_colchetes(valor) -> str:
    s = as_string(valor)
    s = re.sub(r'^\["', '', s)
    s = re.sub(r'"\]$', '', s)
    s = re.sub(r'^\[\\"', '', s)
    s = re.sub(r'\\"\\]$', '', s)
    return s.strip()


def limpar_documento(valor) -> str:
    return re.sub(r'\D+', '', as_string(valor))


def to_number_br(valor) -> float:
    s0 = as_string(valor)
    if not s0:
        return 0.0
    s = re.sub(r'\s', '', s0)
    if '.' in s and ',' in s:
        last_dot = s.rfind('.')
        last_comma = s.rfind(',')
        if last_comma > last_dot:
            n = s.replace('.', '').replace(',', '.')
        else:
            n = s.replace(',', '')
    elif ',' in s:
        n = s.replace('.', '').replace(',', '.')
    elif re.search(r'\.\d{1,10}$', s):  # aceita até 10 casas decimais (planilha US retorna até 7)
        n = s
    else:
        n = s.replace('.', '')
    try:
        return float(n)
    except (ValueError, TypeError):
        return 0.0


def number_to_br(num) -> str:
    n = float(num or 0)
    return f"{n:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def round2(num) -> float:
    return round(float(num or 0) + 1e-10, 2)


def formatar_moeda_br(valor) -> str:
    return number_to_br(to_number_br(valor))


def normalizar_numero_omie(valor) -> float:
    return to_number_br(valor)


def normalizar_percentual_omie(valor) -> float:
    s = as_string(valor).replace('%', '').strip()
    if not s:
        return 0.0
    n = to_number_br(s)
    if n < 0:
        return 0.0
    if n > 100:
        return 100.0
    return n


def as_boolean_omie_sn(valor) -> str:
    s = as_string(valor).lower()
    return 'S' if s in ('sim', 's', 'true', '1') else 'N'