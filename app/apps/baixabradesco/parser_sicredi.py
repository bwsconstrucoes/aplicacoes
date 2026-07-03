# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from .models import ExtractedReceipt
from .utils import normalize_text, only_digits, money_to_decimal, decimal_to_br, clean_account, as_string


def is_sicredi(text: str) -> bool:
    """Detecta se o comprovante é do Sicredi."""
    n = normalize_text(text or '')
    return ('sicredi' in n or
            'cooperativa e conta origem' in n or
            ('cooperativa origem' in n and 'conta origem' in n))


def parse_sicredi_text(filename: str, page: int, text: str,
                       drive_link: str = '', fingerprint: str = '') -> ExtractedReceipt:
    r = ExtractedReceipt(
        filename=filename, page=page, text=text or '',
        drive_link=drive_link, fingerprint=fingerprint,
    )
    norm = normalize_text(text)

    r.id_pipefy        = extract_id_pipefy(text)
    r.valor_pago       = extract_valor_pago(text)
    r.acrescimos       = extract_acrescimos(text) or '0,00'
    r.data_pagamento   = extract_data_pagamento(text)
    r.forma_pagamento  = classify_payment_type(text)
    r.agencia_origem, r.conta_origem, r.conta_origem_raw = extract_conta_origem(text)
    r.codigo_barras    = extract_codigo_barras(text)
    r.nome_recebedor   = extract_nome_recebedor(text)
    r.descricao        = extract_descricao(text)

    # Classifica tipo
    if 'boleto' in norm or 'codigo de barras' in norm:
        r.tipo_comprovante = 'boleto'
    elif 'pix' in norm or 'id da transacao' in norm:
        r.tipo_comprovante = 'pix'
    else:
        r.tipo_comprovante = 'sicredi'

    r.confianca = {
        'id_pipefy':      0.98 if r.id_pipefy else 0.0,
        'valor_pago':     0.90 if r.valor_pago else 0.0,
        'data_pagamento': 0.90 if r.data_pagamento else 0.0,
        'conta_origem':   0.85 if r.conta_origem else 0.0,
        'forma_pagamento':0.80 if r.forma_pagamento else 0.0,
    }

    for campo in ('valor_pago', 'data_pagamento'):
        if not getattr(r, campo):
            r.pendencias.append(f'Campo não extraído: {campo}')

    return r


def _first_money(patterns, text: str, skip_zero: bool = True) -> str:
    from decimal import Decimal
    for p in patterns:
        m = re.search(p, text or '', flags=re.I | re.S)
        if m:
            val = money_to_decimal(m.group(1))
            if val is not None:
                if skip_zero and val == Decimal('0.00'):
                    continue
                return decimal_to_br(val)
    return ''


def extract_id_pipefy(text: str) -> str:
    """Sicredi boleto: 'Descrição do Pagamento: 1391008068'."""
    patterns = [
        r'Descri[cç][aã]o\s+do\s+Pagamento\s*:?\s*(\d{7,12})(?!\d)',
        r'Descri[cç][aã]o\s*:?\s*(\d{7,12})(?!\d)',
    ]
    for p in patterns:
        m = re.search(p, text or '', flags=re.I)
        if m:
            val = m.group(1)
            if not val.startswith('000201'):
                return val
    return ''


def extract_valor_pago(text: str) -> str:
    """Sicredi boleto: 'Valor Pago (R$): 1.956,00'. Sicredi PIX: 'Valor: R$ 390,70'."""
    return _first_money([
        r'Valor\s+Pago\s*\(R\$\)\s*:?\s*([\d\.]+,\d{2})',
        r'Valor\s+original\s*:?\s*R\$\s*([\d\.]+,\d{2})',
        r'^Valor\s*:\s*R\$\s*([\d\.]+,\d{2})',
        r'Valor\s*:\s*R\$\s*([\d\.]+,\d{2})',
    ], text)


def extract_acrescimos(text: str) -> str:
    """Soma Juros/Mora + Multa."""
    from decimal import Decimal
    juros_str = _first_money([r'Valor\s+do\s+Juros/Mora\s*\(R\$\)\s*:?\s*([\d\.]+,\d{2})'], text, skip_zero=False)
    multa_str = _first_money([r'Valor\s+da\s+Multa\s*\(R\$\)\s*:?\s*([\d\.]+,\d{2})'], text, skip_zero=False)
    juros = money_to_decimal(juros_str) or Decimal('0')
    multa = money_to_decimal(multa_str) or Decimal('0')
    total = juros + multa
    if total > Decimal('0'):
        return decimal_to_br(total)
    return '0,00'


def extract_data_pagamento(text: str) -> str:
    """Sicredi boleto: 'Data do Pagamento: 29/06/2026'. PIX: 'Realizado em: 25/06/2026'."""
    patterns = [
        r'Data\s+do\s+Pagamento\s*:?\s*(\d{2}/\d{2}/\d{4})',
        r'Realizado\s+em\s*:\s*(\d{2}/\d{2}/\d{4})',
        r'\b(\d{2}/\d{2}/\d{4})\b',
    ]
    for p in patterns:
        m = re.search(p, text or '', flags=re.I)
        if m:
            return m.group(1)
    return ''


def classify_payment_type(text: str) -> str:
    n = normalize_text(text)
    if 'pix' in n or 'id da transacao' in n:
        return 'Pix'
    if 'boleto' in n or 'codigo de barras' in n:
        return 'Boleto'
    return ''


def extract_conta_origem(text: str):
    """Sicredi: 'Conta Origem: 92945-8' + 'Cooperativa Origem: 02205'
    ou 'Cooperativa e conta origem: 2205/92945-8'.
    """
    # Formato PIX: "2205/92945-8"
    m = re.search(r'Cooperativa\s+e\s+conta\s+origem\s*:\s*(\d+)/(\S+)', text or '', re.I)
    if m:
        coop = m.group(1).strip()
        conta = clean_account(m.group(2).strip())
        return coop, conta, f'{coop} | {conta}'

    # Formato boleto separado
    mc = re.search(r'Cooperativa\s+Origem\s*:\s*(\d+)', text or '', re.I)
    ma = re.search(r'Conta\s+Origem\s*:\s*([\d\-]+)', text or '', re.I)
    if mc and ma:
        coop  = mc.group(1).strip()
        conta = clean_account(ma.group(1).strip())
        return coop, conta, f'{coop} | {conta}'

    return '', '', ''


def extract_nome_recebedor(text: str) -> str:
    for p in [
        r'Nome\s+(?:do\s+)?(?:destinat[aá]rio|benefici[aá]rio|fantasia\s+benefici[aá]rio)\s*:?\s*([^\n\r]+)',
        r'Raz[aã]o\s+Social\s+Benefici[aá]rio\s*:?\s*([^\n\r]+)',
    ]:
        m = re.search(p, text or '', flags=re.I)
        if m:
            return as_string(m.group(1))[:120]
    return ''


def extract_descricao(text: str) -> str:
    m = re.search(r'Descri[cç][aã]o\s+do\s+Pagamento\s*:?\s*([^\n\r]+)', text or '', flags=re.I)
    return as_string(m.group(1)) if m else ''


def extract_codigo_barras(text: str) -> str:
    m = re.search(r'C[oó]digo\s+de\s+Barras\s*:?\s*([0-9\s\.\-]{44,80})', text or '', flags=re.I)
    if m:
        digits = only_digits(m.group(1))
        if len(digits) >= 44:
            return digits
    return ''