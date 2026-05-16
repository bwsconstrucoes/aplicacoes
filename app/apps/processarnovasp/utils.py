# -*- coding: utf-8 -*-
"""utils.py — funções auxiliares (mesma base do atualizaspbotao + helpers webhook Pipefy)"""

import re
import unicodedata
from datetime import datetime, timedelta, date


# ---------------------------------------------------------------------------
# Tipos básicos
# ---------------------------------------------------------------------------

def as_string(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def value_or_empty(v):
    return "" if v is None else v


def has_value(v) -> bool:
    return v is not None and str(v).strip() != ""


# ---------------------------------------------------------------------------
# Letras de coluna
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Limpeza de campos Pipefy
# ---------------------------------------------------------------------------

def limpar_colchetes(valor) -> str:
    """Remove [" e "] do início e fim de um campo Pipefy (formato lista)."""
    s = as_string(valor)
    # padrões: ["foo"] ou \"foo\" ou apenas "foo"
    s = re.sub(r'^\["', '', s)
    s = re.sub(r'"\]$', '', s)
    s = re.sub(r'^\[\\"', '', s)
    s = re.sub(r'\\"\\]$', '', s)
    s = re.sub(r'^\[', '', s)
    s = re.sub(r'\]$', '', s)
    return s.strip().strip('"').strip()


def limpar_documento(valor) -> str:
    """Remove todos não-dígitos de CPF/CNPJ."""
    return re.sub(r'\D+', '', as_string(valor))


def primeiro_token_dash(valor) -> str:
    """
    Extrai o primeiro 'token' antes do '-' em campos do tipo 'NOME - CPF/email'.
    Igual à fórmula Make: substring(0; indexOf("-") - 1).
    """
    s = limpar_colchetes(valor)
    if ' - ' in s:
        s = s.split(' - ', 1)[0]
    elif '-' in s and not _looks_like_doc(s):
        # cuidado para não cortar CPFs/CNPJs que tenham hífen
        s = s.split('-', 1)[0]
    return s.strip()


def _looks_like_doc(s: str) -> bool:
    digits = re.sub(r'\D', '', s)
    return len(digits) >= 11


# ---------------------------------------------------------------------------
# Números (formatos US e BR)
# ---------------------------------------------------------------------------

def to_number_br(valor) -> float:
    """
    Converte string para float, aceitando formatos BR (1.234,56) e US (1234.56).
    """
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
    elif re.search(r'\.\d{1,10}$', s):
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


# ---------------------------------------------------------------------------
# Datas
# ---------------------------------------------------------------------------

def parse_data_pipefy(valor) -> date | None:
    """
    Aceita formatos comuns do Pipefy:
      'DD/MM/YYYY HH:mm'
      'DD/MM/YYYY'
      'YYYY-MM-DD'
      'YYYY-MM-DDTHH:mm:ssZ'
    """
    s = as_string(valor)
    if not s:
        return None
    formatos = [
        '%d/%m/%Y %H:%M',
        '%d/%m/%Y',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
    ]
    for f in formatos:
        try:
            return datetime.strptime(s[:len(f) + 2 if 'Y' in f else 10], f).date()
        except ValueError:
            continue
    # ISO 8601
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).date()
    except ValueError:
        return None


def formatar_data_br(d) -> str:
    if isinstance(d, str):
        d = parse_data_pipefy(d)
    if not d:
        return ''
    return d.strftime('%d/%m/%Y')


def mes_ano_br(d: date) -> str:
    """Retorna 'M/YYYY' (sem zero à esquerda)."""
    if not d:
        return ''
    return f'{d.month}/{d.year}'


# ---------------------------------------------------------------------------
# Texto / Base64
# ---------------------------------------------------------------------------

def normalizar_texto(s: str) -> str:
    """Para comparações: lowercase, sem acentos, sem espaços extras."""
    if not s:
        return ''
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'\s+', ' ', s).strip().lower()


def decodificar_b64_inline(texto: str) -> str:
    """Decodifica strings base64 que vêm embutidas em campos do Pipefy."""
    import base64
    if not texto:
        return ''
    def tentar_decode(match):
        token = match.group(0)
        try:
            padding = (4 - len(token) % 4) % 4
            decoded = base64.b64decode(token + '=' * padding).decode('utf-8')
            if len(decoded) > 3 and all(c.isprintable() or c in '\n\r\t\xa0' for c in decoded):
                return decoded
        except Exception:
            pass
        return token
    padrao = re.compile(r'[A-Za-z0-9+/]{20,}={0,2}')
    for _ in range(5):
        novo = padrao.sub(tentar_decode, texto)
        if novo == texto:
            break
        texto = novo
    return texto
