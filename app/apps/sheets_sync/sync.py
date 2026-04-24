# -*- coding: utf-8 -*-
"""
sheets_sync/sync.py
Copia abas da planilha de ORIGEM para a planilha de DESTINO.
Usa gspread com batch_get + update — uma requisição HTTP por aba.
"""

import os
import json
import time
import logging
from base64 import b64decode

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ID da planilha de origem (fixa — sempre a mesma fonte)
ORIGEM_ID = "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M"

# Abas a sincronizar: nome na origem → configuração no destino
ABAS_CONFIG = {
    "SSEspelho": {
        "aba_destino": "SSEspelho",
        "col_inicio": 2,   # escreve a partir da coluna B (índice 1 em gspread = col B)
    },
    "SSEspelhoRecebidos": {
        "aba_destino": "SSEspelhoRecebidos",
        "col_inicio": 2,
    },
}


def _get_client():
    """Cria cliente gspread autenticado via service account."""
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    if not creds_b64:
        raise RuntimeError("GOOGLE_CREDENTIALS_BASE64 não configurado.")
    creds_dict = json.loads(b64decode(creds_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def sincronizar(destino_id: str) -> dict:
    """
    Lê cada aba da planilha de ORIGEM e escreve na planilha de DESTINO.

    Args:
        destino_id: ID da planilha que fez a requisição (quem vai receber os dados).

    Returns:
        dict com resultado por aba e tempo total.
    """
    inicio = time.time()
    gc = _get_client()

    # Abre origem e destino (2 chamadas de API, feitas uma vez)
    try:
        ss_origem = gc.open_by_key(ORIGEM_ID)
    except Exception as e:
        raise RuntimeError(f"Não foi possível abrir planilha de origem: {e}")

    try:
        ss_destino = gc.open_by_key(destino_id)
    except Exception as e:
        raise RuntimeError(f"Não foi possível abrir planilha de destino ({destino_id}): {e}")

    resultados = {}

    for nome_origem, cfg in ABAS_CONFIG.items():
        try:
            resultado = _sincronizar_aba(
                ss_origem=ss_origem,
                ss_destino=ss_destino,
                nome_origem=nome_origem,
                nome_destino=cfg["aba_destino"],
                col_inicio=cfg["col_inicio"],
            )
            resultados[nome_origem] = resultado
            logger.info(f"[sheets_sync] {nome_origem}: {resultado['linhas']} linhas em {resultado['segundos']:.1f}s")

        except Exception as e:
            logger.error(f"[sheets_sync] Erro em {nome_origem}: {e}")
            resultados[nome_origem] = {"ok": False, "erro": str(e)}

    tempo_total = round(time.time() - inicio, 2)
    return {
        "ok": all(r.get("ok", False) for r in resultados.values()),
        "abas": resultados,
        "segundos": tempo_total,
    }


def _sincronizar_aba(ss_origem, ss_destino, nome_origem, nome_destino, col_inicio):
    """
    Copia uma aba da origem para o destino.
    - Leitura: get_all_values() — uma requisição HTTP
    - Limpeza: batch_clear() — uma requisição HTTP
    - Escrita: update() — uma requisição HTTP
    """
    t = time.time()

    # --- LEITURA da origem ---
    try:
        ws_origem = ss_origem.worksheet(nome_origem)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"Aba '{nome_origem}' não encontrada na origem.")

    dados = ws_origem.get_all_values()  # lista de listas, inclui linhas vazias
    dados = _remover_linhas_vazias(dados)

    if not dados:
        return {"ok": True, "linhas": 0, "segundos": round(time.time() - t, 2)}

    # --- DESTINO: abre ou cria aba ---
    try:
        ws_destino = ss_destino.worksheet(nome_destino)
    except gspread.WorksheetNotFound:
        ws_destino = ss_destino.add_worksheet(
            title=nome_destino,
            rows=max(len(dados) + 100, 1000),
            cols=50
        )

    # --- LIMPEZA do destino (só as colunas que vamos usar, da linha 1 até o fim) ---
    num_cols = len(dados[0]) if dados else 0
    if num_cols > 0:
        col_fim_letra = _col_numero_para_letra(col_inicio + num_cols - 1)
        col_inicio_letra = _col_numero_para_letra(col_inicio)
        range_limpar = f"{col_inicio_letra}1:{col_fim_letra}"
        ws_destino.batch_clear([range_limpar])

    # --- ESCRITA no destino ---
    # gspread.update aceita range A1 notation e lista de listas
    col_inicio_letra = _col_numero_para_letra(col_inicio)
    range_escrita = f"{col_inicio_letra}1"
    ws_destino.update(range_escrita, dados, value_input_option="USER_ENTERED")

    return {
        "ok": True,
        "linhas": len(dados),
        "colunas": num_cols,
        "segundos": round(time.time() - t, 2),
    }


def _remover_linhas_vazias(dados: list) -> list:
    """Remove linhas completamente vazias do final."""
    while dados and all(c == "" for c in dados[-1]):
        dados.pop()
    return dados


def _col_numero_para_letra(n: int) -> str:
    """Converte número de coluna (1-based) para letra(s) A1 notation."""
    resultado = ""
    while n > 0:
        n, resto = divmod(n - 1, 26)
        resultado = chr(65 + resto) + resultado
    return resultado
