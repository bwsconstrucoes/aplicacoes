# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hashlib
import re
import unicodedata
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime
from typing import Tuple

TWOPLACES = Decimal('0.01')


def as_string(v) -> str:
    if v is None:
        return ''
    return str(v).strip()


def normalize_text(value: str) -> str:
    value = as_string(value).lower()
    value = unicodedata.normalize('NFKD', value)
    value = ''.join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r'\s+', ' ', value)
    return value.strip()


def normalize_compact(value: str) -> str:
    return re.sub(r'[^a-z0-9]', '', normalize_text(value))


def only_digits(value: str) -> str:
    return re.sub(r'\D+', '', as_string(value))


def money_to_decimal(value) -> 'Decimal | None':
    txt = as_string(value)
    if not txt:
        return None
    txt = txt.replace('R$', '').replace('\xa0', ' ').strip()
    # 1.234,56 -> 1234.56 | 1234.56 -> 1234.56
    if ',' in txt:
        txt = txt.replace('.', '').replace(',', '.')
    txt = re.sub(r'[^0-9.\-]', '', txt)
    if not txt:
        return None
    try:
        return Decimal(txt).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None


def decimal_to_br(value) -> str:
    if value is None or value == '':
        return ''
    if not isinstance(value, Decimal):
        value = money_to_decimal(value)
    if value is None:
        return ''
    return f'{value:,.2f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def decimal_to_omie(value) -> str:
    d = money_to_decimal(value)
    return '' if d is None else f'{d:.2f}'


def b64decode_bytes(data: str) -> bytes:
    """Decodifica base64 de forma robusta.

    Aceita:
    - base64 sem padding (caso mais comum vindo do Make.com)
    - base64 com prefixo data URI (data:application/pdf;base64,...)
    - base64 com espaços ou quebras de linha embutidas
    - base64 padrão com padding correto
    """
    data = as_string(data)
    # Remove prefixo data URI se presente
    if ',' in data and data.lower().startswith('data:'):
        data = data.split(',', 1)[1]
    # Remove qualquer whitespace (Make pode enviar com quebras de linha entre chunks)
    data = ''.join(data.split())
    if not data:
        raise ValueError('Base64 vazio após limpeza.')
    # Adiciona padding necessário (len % 4 deve ser 0, 2 ou 3 — nunca 1)
    remainder = len(data) % 4
    if remainder == 1:
        # Dado corrompido — tenta mesmo assim removendo o último char
        data = data[:-1]
    elif remainder in (2, 3):
        data += '=' * (4 - remainder)
    return base64.b64decode(data)


def fingerprint_bytes(data: bytes, filename: str = '') -> str:
    h = hashlib.sha1()
    h.update(as_string(filename).encode('utf-8', errors='ignore'))
    h.update(b'||')
    h.update(data or b'')
    return h.hexdigest()


def now_baixa_id() -> str:
    return 'Int' + datetime.now().strftime('%d%m%Y%H%M%S')


def clean_account(value: str) -> str:
    # 0022069-8 -> 22069-8 / 0007011-4 -> 7011-4
    txt = as_string(value)
    m = re.search(r'(\d{1,8})\s*-\s*(\d)', txt)
    if not m:
        return txt
    n = str(int(m.group(1))) if m.group(1).isdigit() else m.group(1)
    return f'{n}-{m.group(2)}'


def account_key(agencia: str, conta: str) -> str:
    ag = only_digits(agencia)
    cc = clean_account(conta)
    return normalize_compact(f'{ag}|{cc}')