# -*- coding: utf-8 -*-
"""
sheets_sync/sync.py
Motor de sincronização — lê config e executa cópia da origem para o destino.
Suporta modo contínuo e modo gap (blocos com colunas vazias no meio).
"""

import os
import json
import time
import logging
from base64 import b64decode

import gspread
from google.oauth2.service_account import Credentials

from .config import identificar_planilha

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _get_client():
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    if not creds_b64:
        raise RuntimeError("GOOGLE_CREDENTIALS_BASE64 não configurado.")
    creds_dict = json.loads(b64decode(creds_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def sincronizar(destino_id: str, nome_planilha: str) -> dict:
    """
    Sincroniza as abas da planilha de destino conforme sua configuração.

    Args:
        destino_id:     ID da planilha destino.
        nome_planilha:  Nome da planilha (usado para identificar qual config usar).
    """
    inicio = time.time()

    config = identificar_planilha(nome_planilha)
    if not config:
        raise RuntimeError(
            f"Planilha '{nome_planilha}' não tem configuração registrada. "
            f"Verifique o nome ou adicione a config em sheets_sync/config.py"
        )

    gc = _get_client()

    try:
        ss_destino = gc.open_by_key(destino_id)
    except Exception as e:
        raise RuntimeError(f"Não foi possível abrir planilha de destino ({destino_id}): {e}")

    # Abre cada planilha de origem uma única vez (evita abrir a mesma várias vezes)
    cache_origens = {}

    resultados = {}

    for cfg_aba in config["abas"]:
        nome_destino = cfg_aba["aba_destino"]
        try:
            origem_id = cfg_aba["origem_id"]
            if origem_id not in cache_origens:
                cache_origens[origem_id] = gc.open_by_key(origem_id)
            ss_origem = cache_origens[origem_id]

            if cfg_aba["modo"] == "continuo":
                resultado = _sincronizar_continuo(ss_origem, ss_destino, cfg_aba)
            elif cfg_aba["modo"] == "gap":
                resultado = _sincronizar_gap(ss_origem, ss_destino, cfg_aba)
            else:
                raise ValueError(f"Modo desconhecido: {cfg_aba['modo']}")

            resultados[nome_destino] = resultado
            logger.info(f"[sheets_sync] {nome_destino}: {resultado['linhas']} linhas em {resultado['segundos']:.1f}s")

        except Exception as e:
            logger.error(f"[sheets_sync] Erro em {nome_destino}: {e}")
            resultados[nome_destino] = {"ok": False, "erro": str(e)}

    tempo_total = round(time.time() - inicio, 2)
    return {
        "ok"      : all(r.get("ok", False) for r in resultados.values()),
        "abas"    : resultados,
        "segundos": tempo_total,
    }


# ---------------------------------------------------------------------------
# MODO CONTÍNUO — escrita sequencial a partir de col_inicio_destino
# ---------------------------------------------------------------------------

def _sincronizar_continuo(ss_origem, ss_destino, cfg):
    t = time.time()

    ws_origem = _abrir_aba(ss_origem, cfg["aba_origem"])
    ws_destino = _garantir_aba(ss_destino, cfg["aba_destino"])

    last_row = ws_origem.getLastRow() if hasattr(ws_origem, "getLastRow") else None
    dados = ws_origem.get_all_values()
    dados = _remover_linhas_vazias(dados)

    if not dados:
        return {"ok": True, "linhas": 0, "segundos": round(time.time() - t, 2)}

    # Fatia as colunas da origem conforme configuração
    ci = cfg["col_inicio_origem"] - 1       # índice 0-based
    nc = cfg.get("num_cols")
    if nc:
        dados = [linha[ci:ci + nc] for linha in dados]
    else:
        dados = [linha[ci:] for linha in dados]

    num_cols = len(dados[0]) if dados else 0

    # Limpa destino — respeita col_protegida_de
    _limpar_aba(
        ws_destino,
        col_inicio=cfg["col_inicio_destino"],
        num_cols=num_cols,
        col_protegida_de=cfg.get("col_protegida_de"),
    )

    # Escreve
    col_letra = _col_para_letra(cfg["col_inicio_destino"])
    ws_destino.update(f"{col_letra}1", dados, value_input_option="USER_ENTERED")

    return {
        "ok"      : True,
        "linhas"  : len(dados),
        "colunas" : num_cols,
        "segundos": round(time.time() - t, 2),
    }


# ---------------------------------------------------------------------------
# MODO GAP — dois blocos com colunas vazias no meio
# ---------------------------------------------------------------------------

def _sincronizar_gap(ss_origem, ss_destino, cfg):
    t = time.time()

    ws_origem  = _abrir_aba(ss_origem, cfg["aba_origem"])
    ws_destino = _garantir_aba(ss_destino, cfg["aba_destino"])

    dados_origem = ws_origem.get_all_values()
    dados_origem = _remover_linhas_vazias(dados_origem)

    if not dados_origem:
        return {"ok": True, "linhas": 0, "segundos": round(time.time() - t, 2)}

    total_linhas = len(dados_origem)

    # Limpa o intervalo seguro (até col_limpar_ate, respeitando col_protegida_de)
    col_limpar_ate    = cfg.get("col_limpar_ate", 18)
    col_protegida_de  = cfg.get("col_protegida_de")
    num_cols_limpar   = col_limpar_ate  # limpa de A(1) até col_limpar_ate
    _limpar_aba(
        ws_destino,
        col_inicio=1,
        num_cols=num_cols_limpar,
        col_protegida_de=col_protegida_de,
    )

    # Escreve cada bloco separadamente
    total_escritas = 0
    for bloco in cfg["blocos"]:
        ci      = bloco["col_inicio_origem"] - 1   # 0-based
        nc      = bloco["num_cols"]
        cd      = bloco["col_inicio_destino"]
        excluir = bloco.get("excluir_indices", [])  # índices 0-based dentro do bloco a excluir

        # Fatia o bloco de colunas da origem
        dados_bloco = []
        for linha in dados_origem:
            fatia = linha[ci:ci + nc]
            # Remove colunas indesejadas (de trás para frente para não deslocar índices)
            for idx in sorted(excluir, reverse=True):
                if idx < len(fatia):
                    fatia.pop(idx)
            dados_bloco.append(fatia)

        if not dados_bloco or not dados_bloco[0]:
            continue

        col_letra = _col_para_letra(cd)
        ws_destino.update(f"{col_letra}1", dados_bloco, value_input_option="USER_ENTERED")
        total_escritas += len(dados_bloco[0])

    return {
        "ok"      : True,
        "linhas"  : total_linhas,
        "colunas" : total_escritas,
        "segundos": round(time.time() - t, 2),
    }


# ---------------------------------------------------------------------------
# UTILITÁRIOS
# ---------------------------------------------------------------------------

def _abrir_aba(ss, nome_aba):
    try:
        return ss.worksheet(nome_aba)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"Aba '{nome_aba}' não encontrada na origem.")


def _garantir_aba(ss, nome_aba):
    try:
        return ss.worksheet(nome_aba)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=nome_aba, rows=1000, cols=50)


def _limpar_aba(ws, col_inicio: int, num_cols: int, col_protegida_de: int | None):
    """
    Limpa o intervalo de col_inicio até col_inicio+num_cols-1.
    Se col_protegida_de estiver definida, não ultrapassa ela.
    """
    col_fim = col_inicio + num_cols - 1
    if col_protegida_de:
        col_fim = min(col_fim, col_protegida_de - 1)
    if col_fim < col_inicio:
        return
    col_a = _col_para_letra(col_inicio)
    col_b = _col_para_letra(col_fim)
    ws.batch_clear([f"{col_a}1:{col_b}"])


def _remover_linhas_vazias(dados: list) -> list:
    while dados and all(c == "" for c in dados[-1]):
        dados.pop()
    return dados


def _col_para_letra(n: int) -> str:
    resultado = ""
    while n > 0:
        n, resto = divmod(n - 1, 26)
        resultado = chr(65 + resto) + resultado
    return resultado
