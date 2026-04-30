# -*- coding: utf-8 -*-
"""
chatbot/sheets_cache.py
Cache em memória da planilha de colaboradores (Dados Documentos).
TTL de 30 minutos para evitar chamadas repetidas à API do Google.

Mapeamento de colunas (aba Dados Documentos):
  A  → CPF (formato 000.000.000-00)
  E  → Nome Completo
  W  → Telefone
  AX → Status (ex: 'Colaboradores Desligados')
"""

import os
import re
import json
import time
import logging
import threading
from base64 import b64decode

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SPREADSHEET_ID  = '1fqi4QUOVGUd1_4Gg4vK5qP_IMOSgFaw8DD9MDgmM3vo'
WORKSHEET_NAME  = 'Dados Documentos'
CACHE_TTL       = 30 * 60  # 30 minutos

# Índices das colunas (0-based)
COL_CPF    = 0   # A
COL_NOME   = 4   # E
COL_TEL    = 22  # W
COL_STATUS = 49  # AX

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

_cache: dict = {'dados': None, 'carregado_em': 0}
_lock = threading.Lock()


def _get_gc():
    creds_b64 = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
    creds_dict = json.loads(b64decode(creds_b64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def _normalizar_cpf(cpf: str) -> str:
    """Remove formatação, retorna apenas dígitos."""
    return re.sub(r'\D', '', str(cpf or ''))


def _normalizar_telefone(tel: str) -> str:
    """Remove não-dígitos e garante prefixo 55."""
    digits = re.sub(r'\D', '', str(tel or ''))
    if digits and not digits.startswith('55'):
        digits = '55' + digits
    return digits


def _carregar_planilha() -> list[dict]:
    """Carrega todos os colaboradores da planilha."""
    logger.info("[sheets_cache] Carregando planilha de colaboradores...")
    gc = _get_gc()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(WORKSHEET_NAME)
    rows = ws.get_all_values()

    colaboradores = []
    # Pula cabeçalho (linha 0) e linhas vazias
    for row in rows[1:]:
        if not row or not row[COL_CPF].strip():
            continue
        try:
            cpf    = _normalizar_cpf(row[COL_CPF])
            nome   = row[COL_NOME].strip() if len(row) > COL_NOME else ''
            tel    = _normalizar_telefone(row[COL_TEL]) if len(row) > COL_TEL else ''
            status = row[COL_STATUS].strip() if len(row) > COL_STATUS else ''
            if cpf:
                colaboradores.append({
                    'cpf':    cpf,
                    'nome':   nome,
                    'tel':    tel,
                    'status': status,
                })
        except Exception:
            continue

    logger.info(f"[sheets_cache] {len(colaboradores)} colaboradores carregados.")
    return colaboradores


def get_colaboradores() -> list[dict]:
    """Retorna lista de colaboradores, usando cache se disponível."""
    with _lock:
        agora = time.time()
        if _cache['dados'] is not None and (agora - _cache['carregado_em']) < CACHE_TTL:
            return _cache['dados']

    # Carrega fora do lock para não bloquear outras threads
    dados = _carregar_planilha()
    with _lock:
        _cache['dados'] = dados
        _cache['carregado_em'] = time.time()
    return dados


def invalidar_cache():
    """Força recarga na próxima chamada."""
    with _lock:
        _cache['dados'] = None
        _cache['carregado_em'] = 0


def buscar_por_cpf(cpf: str) -> dict | None:
    """Busca colaborador pelo CPF. Retorna dict ou None."""
    cpf_norm = _normalizar_cpf(cpf)
    if not cpf_norm:
        return None
    for c in get_colaboradores():
        if c['cpf'] == cpf_norm:
            return c
    return None


def buscar_por_telefone(telefone: str) -> dict | None:
    """Busca colaborador pelo telefone normalizado."""
    tel_norm = _normalizar_telefone(telefone)
    if not tel_norm:
        return None
    for c in get_colaboradores():
        if c['tel'] == tel_norm:
            return c
    return None


def esta_desligado(colaborador: dict) -> bool:
    return 'Colaboradores Desligados' in (colaborador.get('status') or '')
