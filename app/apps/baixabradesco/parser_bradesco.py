# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from .models import ExtractedReceipt
from .utils import normalize_text, only_digits, money_to_decimal, decimal_to_br, clean_account, as_string

FGTS_CNPJ = '00360305000104'
BEEVALE_TEXT = 'beevale pagamentos e beneficios'


def parse_bradesco_text(filename: str, page: int, text: str, drive_link: str = '', fingerprint: str = '') -> ExtractedReceipt:
    r = ExtractedReceipt(filename=filename, page=page, text=text or '', drive_link=drive_link, fingerprint=fingerprint)
    norm = normalize_text(text)
    digits = only_digits(text)

    r.id_pipefy = extract_id_pipefy(text)
    r.descricao = extract_descricao(text)
    r.valor_pago = extract_valor_pago(text)
    r.acrescimos = extract_acrescimos(text) or '0,00'
    r.tarifa = extract_tarifa(text) or '0,00'
    r.data_pagamento = extract_data_pagamento(text)
    r.forma_pagamento = classify_payment_type(text)
    r.nome_recebedor = extract_nome_recebedor(text)
    r.documento_recebedor = extract_documento_recebedor(text)
    r.agencia_origem, r.conta_origem, r.conta_origem_raw = extract_conta_origem(text)
    r.conta_destino_raw = extract_conta_destino(text)
    r.codigo_barras = extract_codigo_barras(text)

    if (('cef matriz' in norm or 'caixa economica federal' in norm) and FGTS_CNPJ in digits):
        r.tipo_comprovante = 'fgts_rescisorio'
    elif BEEVALE_TEXT in norm:
        r.tipo_comprovante = 'beevale'
    elif 'pix' in norm:
        r.tipo_comprovante = 'pix'
    elif 'boleto' in norm or 'codigo de barras' in norm or 'linha digitavel' in norm:
        r.tipo_comprovante = 'boleto'
    elif 'transferencia' in norm or 'ted' in norm or 'doc' in norm:
        r.tipo_comprovante = 'transferencia'
    else:
        r.tipo_comprovante = 'bradesco'

    r.confianca = build_confidence(r)
    for campo in ('valor_pago', 'data_pagamento'):
        if not getattr(r, campo):
            r.pendencias.append(f'Campo não extraído: {campo}')
    if not r.conta_origem and not r.conta_origem_raw:
        r.pendencias.append('Conta origem não extraída')
    return r


def extract_id_pipefy(text: str) -> str:
    patterns = [
        r'Descri[cç][aã]o\s*:?\s*(\d{7,12})',
        r'DESCRI[CÇ][AÃ]O\s*:?\s*(\d{7,12})',
        r'\bSP\s*:?\s*(\d{7,12})\b',
        r'\bID\s*:?\s*(\d{7,12})\b',
    ]
    for p in patterns:
        m = re.search(p, text or '', flags=re.I)
        if m:
            return m.group(1)
    return ''


def extract_descricao(text: str) -> str:
    m = re.search(r'Descri[cç][aã]o\s*:?\s*([^\n\r]+)', text or '', flags=re.I)
    return as_string(m.group(1)) if m else ''


def _first_money_after(patterns, text: str) -> str:
    for p in patterns:
        m = re.search(p, text or '', flags=re.I | re.S)
        if m:
            val = money_to_decimal(m.group(1))
            if val is not None:
                return decimal_to_br(val)
    return ''


def extract_valor_pago(text: str) -> str:
    # Prioriza "Valor total" (total pago) sobre "Valor R$" (valor original do boleto)
    return _first_money_after([
        r'Valor\s+total\s*:?\s*R?\$?\s*([\d\.]+,\d{2})',
        r'Valor\s+do\s+pagamento\s*:?\s*R?\$?\s*([\d\.]+,\d{2})',
        r'Valor\s+(?:pago|transferido)\s*:?\s*R?\$?\s*([\d\.]+,\d{2})',
        r'Valor\s+final\s*R?\$?\s*([\d\.]+,\d{2})',
        r'Valor\s*:?\s*R\$\s*([\d\.]+,\d{2})',
        r'R\$\s*([\d\.]+,\d{2})',
    ], text)


def extract_acrescimos(text: str) -> str:
    """Extrai acréscimos totais (juros + multa somados).
    O Bradesco exibe juros e multa separados. O Omie recebe o total.
    """
    # Tenta campo já somado primeiro
    acresc_direto = _first_money_after([
        r'Acr[eé]scimos?\s*(?:\(.*?\))?\s*:?\s*R?\$?\s*([\d\.]+,\d{2})',
    ], text)
    if acresc_direto and acresc_direto != '0,00':
        return acresc_direto

    # Soma juros + multa individualmente
    from decimal import Decimal
    juros_str = _first_money_after([r'Juros\s*:?\s*R?\$?\s*([\d\.]+,\d{2})'], text)
    multa_str = _first_money_after([r'Multa\s*:?\s*R?\$?\s*([\d\.]+,\d{2})'], text)
    juros = money_to_decimal(juros_str) or Decimal('0')
    multa = money_to_decimal(multa_str) or Decimal('0')
    total = juros + multa
    if total > Decimal('0'):
        return decimal_to_br(total)
    return '0,00'


def extract_tarifa(text: str) -> str:
    return _first_money_after([
        r'Tarifa[^\d]{0,30}([\d\.]+,\d{2})',
    ], text)


def extract_data_pagamento(text: str) -> str:
    patterns = [
        r'Data\s*(?:do pagamento|da opera[cç][aã]o|de pagamento)?\s*:?\s*(\d{2}/\d{2}/\d{4})',
        r'Pagamento\s*realizado\s*em\s*(\d{2}/\d{2}/\d{4})',
        r'\b(\d{2}/\d{2}/\d{4})\b',
    ]
    for p in patterns:
        m = re.search(p, text or '', flags=re.I)
        if m:
            return m.group(1)
    return ''


def classify_payment_type(text: str) -> str:
    n = normalize_text(text)
    if 'beevale' in n:
        return 'BeeVale'
    if 'pix' in n:
        return 'Pix'
    if 'boleto' in n or 'codigo de barras' in n or 'linha digitavel' in n:
        return 'Boleto'
    if 'transferencia' in n or 'ted' in n or 'doc' in n:
        return 'Transferência Bancária'
    return ''


def extract_nome_recebedor(text: str) -> str:
    patterns = [
        r'(?:Favorecido|Recebedor|Benefici[aá]rio|Destino)\s*:?\s*([^\n\r]+)',
        r'Nome\s*do\s*recebedor\s*:?\s*([^\n\r]+)',
    ]
    for p in patterns:
        m = re.search(p, text or '', flags=re.I)
        if m:
            return as_string(m.group(1))[:120]
    return ''


def extract_documento_recebedor(text: str) -> str:
    m = re.search(r'((?:\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})|(?:\d{3}\.?\d{3}\.?\d{3}-?\d{2}))', text or '')
    return only_digits(m.group(1)) if m else ''


def extract_conta_origem(text: str):
    patterns = [
        r'Conta\s*(?:origem|de origem)?\s*:?\s*(\d{3,5})\s*[|/\- ]+\s*(\d{1,8}\s*-\s*\d)',
        r'Ag[eê]ncia\s*:?\s*(\d{3,5}).{0,50}?Conta\s*:?\s*(\d{1,8}\s*-\s*\d)',
        r'(\d{3,5})\s*\|\s*(\d{1,8}\s*-\s*\d)\s*\|\s*Conta',
    ]
    for p in patterns:
        m = re.search(p, text or '', flags=re.I | re.S)
        if m:
            ag, cc = m.group(1), clean_account(m.group(2))
            return ag, cc, f'{ag} | {cc}'
    return '', '', ''


def extract_conta_destino(text: str) -> str:
    m = re.search(r'Conta\s*(?:destino|favorecido|benefici[aá]rio)\s*:?\s*([^\n\r]+)', text or '', flags=re.I)
    return as_string(m.group(1)) if m else ''


def extract_codigo_barras(text: str) -> str:
    """Extrai código de barras/linha digitável de boletos.

    Normaliza para apenas números para permitir comparação direta com SPsBD,
    mesmo que o comprovante venha com espaços e a planilha sem formatação.
    """
    txt = text or ''

    patterns = [
        # Ex.: 23793 45602 90250 077923 58004 480305 2 14370000052240
        r'Código\s+de\s+barras\s*:?\s*([0-9\s\.\-]{44,80})',
        r'Codigo\s+de\s+barras\s*:?\s*([0-9\s\.\-]{44,80})',
        r'Linha\s+digit[aá]vel\s*:?\s*([0-9\s\.\-]{44,80})',
    ]

    for p in patterns:
        m = re.search(p, txt, flags=re.I)
        if m:
            digits = only_digits(m.group(1))
            if len(digits) >= 44:
                return digits

    digits_all = only_digits(txt)
    m = re.search(r'(\d{44,48})', digits_all)
    return m.group(1) if m else ''


def build_confidence(r: ExtractedReceipt) -> dict:
    return {
        'id_pipefy': 0.98 if r.id_pipefy else 0.0,
        'valor_pago': 0.90 if r.valor_pago else 0.0,
        'data_pagamento': 0.90 if r.data_pagamento else 0.0,
        'conta_origem': 0.85 if r.conta_origem else 0.0,
        'forma_pagamento': 0.80 if r.forma_pagamento else 0.0,
    }