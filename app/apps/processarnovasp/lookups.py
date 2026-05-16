# -*- coding: utf-8 -*-
"""
lookups.py — Leituras das 4 planilhas auxiliares usadas pelo cálculo de rateio.

Estratégia escolhida pelo usuário: ler range direto na planilha (sem cache pesado).

Headers REAIS (verificados via Google Drive em 16/05/2026):

  1) Base Centro de Custo (planilha OmieApi)
     A=Código(nome do CC)  ...  N=Código Omie
     → chave em A, valor em N

  2) Base Tipo de Despesa (planilha OmieApi)
     A=Record ID  B=Plano Financeiro(nome)  C=Record ID  D=Código Omie
     → chave em B, valor em D

  3) ClientesOmie (planilha OmieApi)
     A=cnpj_cpf (com formatação: "00.079.526/0001-09" ou "123.456.789-01")
     B=codigo_cliente_omie
     → chave em A (normaliza pra comparar só dígitos), valor em B

  4) SPsDDA (planilha PRINCIPAL)
     A=Data  B=Código de Barras  C=SP
     → chave em B, valor em C
"""

import re
import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# IDs das planilhas
PLANILHA_OMIE      = '1FyswS4ZlCr2f8VaVvA52hOmajLIQhQISh49S2l-75Wc'
PLANILHA_PRINCIPAL = '1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA'

# Configuração das colunas (1-based, igual ao Sheets)
SHEETS_CONFIG = {
    'base_cc': {
        'spreadsheet_id': PLANILHA_OMIE,
        'sheet_name':     'Base Centro de Custo',
        'col_chave':      1,    # A = nome do CC (ex: "SOBRADINHO", "CEIFOR2")
        'col_valor':      14,   # N = código Omie do departamento
        'normaliza_chave': False,
    },
    'base_tipo_despesa': {
        'spreadsheet_id': PLANILHA_OMIE,
        'sheet_name':     'Base Tipo de Despesa',
        'col_chave':      2,    # B = "Plano Financeiro" (nome legível)
        'col_valor':      4,    # D = "Código Omie" (ex: "T2.01.94")
        'normaliza_chave': False,
    },
    'clientes_omie': {
        'spreadsheet_id': PLANILHA_OMIE,
        'sheet_name':     'ClientesOmie',
        'col_chave':      1,    # A = CPF/CNPJ formatado
        'col_valor':      2,    # B = codigo_cliente_omie
        'normaliza_chave': True,    # normaliza pra só dígitos antes de comparar
    },
    'spsdda': {
        'spreadsheet_id': PLANILHA_PRINCIPAL,
        'sheet_name':     'SPsDDA',
        'col_chave':      2,    # B = código de barras
        'col_valor':      3,    # C = ID da SP
        'normaliza_chave': False,
    },
}

# -----------------------------------------------------------------------------
# Cache em processo (TTL curto — 30s) — alivia chamadas concorrentes vindas
# do mesmo deploy, mas continua "fresh" no contexto humano.
# Cada lookup ainda gasta no máx. 1 chamada / 30s à planilha por tabela.
# -----------------------------------------------------------------------------

_CACHE_LOCK = threading.Lock()
_CACHE: dict = {}     # tabela -> {'expira': ts, 'mapa': {chave: valor}}
_CACHE_TTL_S = 30


def _agora() -> float:
    import time
    return time.time()


def _carregar_tabela(gc, tabela: str) -> dict:
    """Lê a aba inteira e devolve mapa chave→valor. Cacheia por TTL."""
    with _CACHE_LOCK:
        item = _CACHE.get(tabela)
        if item and item['expira'] > _agora():
            return item['mapa']

    cfg = SHEETS_CONFIG[tabela]
    sh = gc.open_by_key(cfg['spreadsheet_id']).worksheet(cfg['sheet_name'])
    todas = sh.get_all_values()
    mapa = {}
    normaliza = cfg.get('normaliza_chave', False)
    for row in todas[1:]:  # pula header
        if len(row) < max(cfg['col_chave'], cfg['col_valor']):
            continue
        chave_raw = row[cfg['col_chave'] - 1].strip()
        valor     = row[cfg['col_valor'] - 1].strip()
        if not chave_raw or not valor:
            continue
        if normaliza:
            chave = re.sub(r'\D', '', chave_raw)
        else:
            chave = chave_raw
        if chave and chave not in mapa:
            mapa[chave] = valor

    with _CACHE_LOCK:
        _CACHE[tabela] = {'expira': _agora() + _CACHE_TTL_S, 'mapa': mapa}

    logger.info(f"[lookups] tabela={tabela} carregada com {len(mapa)} linhas")
    return mapa


# -----------------------------------------------------------------------------
# Lookups públicos
# -----------------------------------------------------------------------------

def codigo_centro_custo(gc, nome: str) -> Optional[str]:
    if not nome:
        return None
    mapa = _carregar_tabela(gc, 'base_cc')
    if nome in mapa:
        return mapa[nome]
    # tenta normalizado (sem acentos, lowercase)
    from .utils import normalizar_texto
    alvo = normalizar_texto(nome)
    for k, v in mapa.items():
        if normalizar_texto(k) == alvo:
            return v
    return None


def codigo_tipo_despesa(gc, nome: str) -> Optional[str]:
    if not nome:
        return None
    mapa = _carregar_tabela(gc, 'base_tipo_despesa')
    if nome in mapa:
        return mapa[nome]
    from .utils import normalizar_texto
    alvo = normalizar_texto(nome)
    for k, v in mapa.items():
        if normalizar_texto(k) == alvo:
            return v
    return None


def codigo_cliente_omie(gc, cpf_cnpj: str) -> Optional[str]:
    """Busca por CPF/CNPJ. Retorna None se não cadastrado."""
    if not cpf_cnpj:
        return None
    chave = re.sub(r'\D', '', cpf_cnpj)
    if not chave:
        return None
    mapa = _carregar_tabela(gc, 'clientes_omie')
    # Tenta exatamente, depois sem zeros à esquerda
    if chave in mapa:
        return mapa[chave]
    chave_curta = chave.lstrip('0')
    for k, v in mapa.items():
        if k.lstrip('0') == chave_curta:
            return v
    return None


def sp_por_codigo_barras(gc, codigo: str) -> Optional[str]:
    """Retorna o ID da SP que já registrou esse código de barras (ou None)."""
    if not codigo:
        return None
    mapa = _carregar_tabela(gc, 'spsdda')
    if codigo in mapa:
        return mapa[codigo]
    # tenta sem ‘0’ à esquerda
    sem_zero = codigo.lstrip('0')
    for k, v in mapa.items():
        if k.lstrip('0') == sem_zero:
            return v
    return None


def invalidar_cache():
    """Limpa cache (chamado pelo testing ou manualmente)."""
    with _CACHE_LOCK:
        _CACHE.clear()


# -----------------------------------------------------------------------------
# Normalizador de código de barras (reaproveitado do atualizaspbotao)
# -----------------------------------------------------------------------------

def normalizar_codigo_barras(valor: str) -> str:
    s = (valor or '').strip()
    s = s.replace('INVALIDO', '').strip()
    if '-' in s:
        s = s[:s.index('-')]
    s = re.sub(r'\s', '', s)
    s = re.sub(r'[.,]', '', s)
    s = re.sub(r'\D', '', s)
    return s
