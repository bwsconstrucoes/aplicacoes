# -*- coding: utf-8 -*-
"""
sync_logs/utils.py
Funções utilitárias do módulo: cliente gspread, helpers de data, chave A+G.
"""

import os
import json
import logging
from base64 import b64decode
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from .config import (
    COL_A_INDEX,
    COL_G_INDEX,
    FALLBACK_HORAS_ATRAS,
    LAST_SYNC_FILE,
    N_COLUNAS_DADOS,
    TIMEZONE_BR,
)

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ------------------------------------------------------------------
# Cliente gspread (mesmo padrão dos outros módulos do projeto)
# ------------------------------------------------------------------

def get_gc():
    """Retorna cliente gspread autenticado via GOOGLE_CREDENTIALS_BASE64."""
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    if not creds_b64:
        raise RuntimeError("GOOGLE_CREDENTIALS_BASE64 não configurado.")
    creds_dict = json.loads(b64decode(creds_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ------------------------------------------------------------------
# Lista de Análises (configurável via envvar)
# ------------------------------------------------------------------

def ler_analises_ids() -> List[str]:
    """
    Retorna lista de IDs das Análises a sincronizar.

    Configurada via envvar SYNC_LOGS_ANALISES_IDS, formato JSON array:
        SYNC_LOGS_ANALISES_IDS=["id1","id2","id3"]

    Pode começar com 1 ID, depois adicionar mais sem mudar código.
    """
    raw = os.getenv("SYNC_LOGS_ANALISES_IDS", "")
    if not raw:
        raise RuntimeError(
            "SYNC_LOGS_ANALISES_IDS não configurada. Deve ser JSON array, "
            'ex: \'["1em1QlCKx1Mele..."]\''
        )
    try:
        ids = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"SYNC_LOGS_ANALISES_IDS inválida: {e}")
    if not isinstance(ids, list) or not ids:
        raise RuntimeError("SYNC_LOGS_ANALISES_IDS deve ser array não-vazio")
    return ids


# ------------------------------------------------------------------
# Estado do last_sync (persistido em /tmp)
# ------------------------------------------------------------------

def agora_iso() -> str:
    """Timestamp ISO 8601 no fuso de Fortaleza, sem timezone suffix."""
    return datetime.now(ZoneInfo(TIMEZONE_BR)).strftime("%Y-%m-%dT%H:%M:%S")


def ler_last_sync() -> str:
    """
    Lê timestamp do último sync incremental.

    Se nunca rodou (arquivo inexistente), retorna timestamp de N horas atrás
    como fallback seguro: pega o que mudou recentemente sem reler tudo.
    """
    try:
        return Path(LAST_SYNC_FILE).read_text().strip()
    except FileNotFoundError:
        fallback = datetime.now(ZoneInfo(TIMEZONE_BR)) - timedelta(hours=FALLBACK_HORAS_ATRAS)
        return fallback.strftime("%Y-%m-%dT%H:%M:%S")


def gravar_last_sync(ts: str) -> None:
    """Persiste o timestamp do sync atual."""
    Path(LAST_SYNC_FILE).write_text(ts)


# ------------------------------------------------------------------
# Helpers de dados
# ------------------------------------------------------------------

def chave_ag(linha: List) -> str:
    """
    Gera chave única lógica A||G a partir de uma linha de dados.

    Retorna string vazia se A ou G estiverem vazios — nesse caso a linha
    NÃO pode ser sincronizada (sem identidade estável).
    """
    if len(linha) <= COL_G_INDEX:
        return ""
    a_val = linha[COL_A_INDEX] if linha[COL_A_INDEX] is not None else ""
    g_val = linha[COL_G_INDEX] if linha[COL_G_INDEX] is not None else ""
    a = str(a_val).strip()
    g = str(g_val).strip()
    if not a or not g:
        return ""
    return f"{a}||{g}"


def normalizar_largura(linha: List, n_colunas: int = N_COLUNAS_DADOS) -> List:
    """Garante largura exata da linha (pad com '' ou trunca)."""
    if len(linha) < n_colunas:
        return list(linha) + [""] * (n_colunas - len(linha))
    if len(linha) > n_colunas:
        return list(linha[:n_colunas])
    return list(linha)


def ano_da_data(valor) -> Optional[int]:
    """
    Extrai o ano de um valor de data.

    Aceita:
    - int/float: serial date do Sheets (origem 30/12/1899)
    - string ISO ('YYYY-MM-DD'), BR ('DD/MM/YYYY'), ou número como string
    - vazio/None: retorna None

    Usado pelo reset pra filtrar linhas por ano corrente + anterior.
    """
    if valor is None or valor == "":
        return None

    if isinstance(valor, (int, float)):
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(valor))).year
        except (ValueError, OverflowError):
            return None

    s = str(valor).strip()
    if not s:
        return None

    # Tenta como serial number em string
    try:
        n = float(s)
        return (datetime(1899, 12, 30) + timedelta(days=n)).year
    except ValueError:
        pass

    # Tenta formatos comuns de data
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:10], fmt).year
        except ValueError:
            continue

    return None


def anos_alvo() -> Tuple[int, int]:
    """Retorna (ano_anterior, ano_corrente) no fuso de Fortaleza."""
    ano = datetime.now(ZoneInfo(TIMEZONE_BR)).year
    return (ano - 1, ano)


def col_indice_para_letra(n: int) -> str:
    """Converte índice 1-based de coluna para letra (1=A, 26=Z, 27=AA, 33=AG)."""
    resultado = ""
    while n > 0:
        n, resto = divmod(n - 1, 26)
        resultado = chr(65 + resto) + resultado
    return resultado
