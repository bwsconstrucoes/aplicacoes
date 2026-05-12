# -*- coding: utf-8 -*-
"""
sheets_sync/sync.py
Motor de sincronização — lê config e executa cópia da origem para o destino.

Modos suportados:
  - "continuo"  : escrita sequencial a partir de col_inicio_destino
  - "gap"       : escrita em blocos com colunas vazias no meio
  - "filtrado"  : filtra linhas e seleciona colunas, substituindo QUERY

Inclui:
  - Retry com backoff exponencial para erros transientes da Sheets API
  - Leitura em chunks para evitar OOM em abas grandes (Pedidos com 70k linhas)
"""

import os
import gc
import json
import time
import logging
import re
from base64 import b64decode
from datetime import date, datetime, timedelta

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

from .config import (
    identificar_planilha,
    ABA_CONFIG_INTERNA,
    RANGE_CONFIG_INTERNA,
)

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

HTTP_RETRY_CODES = {429, 500, 502, 503, 504}
RETRY_BACKOFF_S = [3, 10, 30]
MAX_TENTATIVAS_API = len(RETRY_BACKOFF_S) + 1

CHUNK_SIZE_LINHAS = 10000


def _get_client():
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    if not creds_b64:
        raise RuntimeError("GOOGLE_CREDENTIALS_BASE64 não configurado.")
    creds_dict = json.loads(b64decode(creds_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ===========================================================================
# RETRY WRAPPER
# ===========================================================================

def _extrair_codigo_http(erro: APIError):
    msg = str(erro)
    match = re.search(r"\[(\d{3})\]", msg)
    return int(match.group(1)) if match else None


def _chamar_api(funcao, descricao: str):
    ultima_excecao = None

    for tentativa in range(1, MAX_TENTATIVAS_API + 1):
        try:
            return funcao()

        except APIError as e:
            ultima_excecao = e
            codigo = _extrair_codigo_http(e)

            if codigo not in HTTP_RETRY_CODES:
                logger.error(f"[sheets_sync] {descricao}: erro nao-retryable HTTP {codigo}")
                raise

            if tentativa >= MAX_TENTATIVAS_API:
                logger.error(f"[sheets_sync] {descricao}: esgotadas {MAX_TENTATIVAS_API} tentativas (ultima: HTTP {codigo})")
                raise

            espera = RETRY_BACKOFF_S[tentativa - 1]
            logger.warning(f"[sheets_sync] {descricao}: HTTP {codigo} na tentativa {tentativa}, aguardando {espera}s...")
            time.sleep(espera)

        except gspread.WorksheetNotFound:
            raise  # nao tenta de novo

        except Exception as e:
            ultima_excecao = e
            if tentativa >= MAX_TENTATIVAS_API:
                logger.error(f"[sheets_sync] {descricao}: esgotadas tentativas com erro: {e}")
                raise
            espera = RETRY_BACKOFF_S[tentativa - 1]
            logger.warning(f"[sheets_sync] {descricao}: erro '{e}' na tentativa {tentativa}, aguardando {espera}s...")
            time.sleep(espera)

    if ultima_excecao:
        raise ultima_excecao


# ===========================================================================
# SINCRONIZACAO PRINCIPAL
# ===========================================================================

def sincronizar(destino_id: str, nome_planilha: str) -> dict:
    inicio = time.time()

    config = identificar_planilha(nome_planilha)
    if not config:
        raise RuntimeError(
            f"Planilha '{nome_planilha}' não tem configuração registrada."
        )

    gc_client = _get_client()

    try:
        ss_destino = _chamar_api(lambda: gc_client.open_by_key(destino_id), f"open destino {destino_id}")
    except Exception as e:
        raise RuntimeError(f"Não foi possível abrir planilha de destino ({destino_id}): {e}")

    cache_origens = {}
    cache_thresholds = {}
    resultados = {}

    for cfg_aba in config["abas"]:
        nome_destino = cfg_aba["aba_destino"]
        try:
            origem_id = cfg_aba["origem_id"]

            if origem_id not in cache_origens:
                cache_origens[origem_id] = _chamar_api(
                    lambda oid=origem_id: gc_client.open_by_key(oid),
                    f"open origem {origem_id}"
                )
            ss_origem = cache_origens[origem_id]

            modo = cfg_aba["modo"]
            if modo == "continuo":
                resultado = _sincronizar_continuo(ss_origem, ss_destino, cfg_aba)
            elif modo == "gap":
                resultado = _sincronizar_gap(ss_origem, ss_destino, cfg_aba)
            elif modo == "filtrado":
                if origem_id not in cache_thresholds:
                    cache_thresholds[origem_id] = _carregar_thresholds(ss_origem)
                thresholds = cache_thresholds[origem_id]
                resultado = _sincronizar_filtrado(ss_origem, ss_destino, cfg_aba, thresholds)
            else:
                raise ValueError(f"Modo desconhecido: {modo}")

            resultados[nome_destino] = resultado
            logger.info(f"[sheets_sync] {nome_destino}: {resultado.get('linhas', 0)} linhas em {resultado.get('segundos', 0):.1f}s")

        except Exception as e:
            logger.error(f"[sheets_sync] Erro em {nome_destino}: {e}")
            resultados[nome_destino] = {"ok": False, "erro": str(e)}

        gc.collect()

    tempo_total = round(time.time() - inicio, 2)
    return {
        "ok"      : all(r.get("ok", False) for r in resultados.values()),
        "abas"    : resultados,
        "segundos": tempo_total,
    }


# ===========================================================================
# CARREGA THRESHOLDS DA ABA _Config
# ===========================================================================

def _carregar_thresholds(ss_origem) -> dict:
    try:
        ws = ss_origem.worksheet(ABA_CONFIG_INTERNA)
    except gspread.WorksheetNotFound:
        logger.warning(f"[sheets_sync] Aba {ABA_CONFIG_INTERNA} não encontrada na origem.")
        return {}
    except Exception as e:
        logger.warning(f"[sheets_sync] Erro ao abrir {ABA_CONFIG_INTERNA}: {e}")
        return {}

    dados = _chamar_api(lambda: ws.get(RANGE_CONFIG_INTERNA), "get _Config")
    thresholds = {}
    for linha in dados:
        if len(linha) >= 2 and linha[0].strip():
            chave = linha[0].strip()
            valor = linha[1].strip()
            thresholds[chave] = valor
    logger.info(f"[sheets_sync] _Config carregada: {list(thresholds.keys())}")
    return thresholds


# ===========================================================================
# MODO FILTRADO
# ===========================================================================

def _col_letra_para_indice(letra: str) -> int:
    """A=0, B=1, ..., Z=25, AA=26..."""
    letra = letra.upper()
    n = 0
    for c in letra:
        n = n * 26 + (ord(c) - ord('A') + 1)
    return n - 1


def _parse_data(s: str):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def _obter_valor_threshold(f: dict, thresholds: dict):
    if "threshold_chave" in f:
        chave = f["threshold_chave"]
        if chave not in thresholds:
            raise RuntimeError(f"Threshold '{chave}' não encontrado na aba _Config da origem.")
        return thresholds[chave]
    return f.get("valor")


def _aplicar_filtros(linha: list, filtros: list, thresholds: dict, hoje: date) -> bool:
    for f in filtros:
        op = f["op"]

        if op == "exclude_recebidos_antigos_30d":
            idx_status = _col_letra_para_indice(f["col_status"])
            idx_data   = _col_letra_para_indice(f["col_data"])
            if idx_status >= len(linha):
                continue
            status = (linha[idx_status] or "").strip()
            if status != "RECEBIDO":
                continue
            if idx_data >= len(linha):
                continue
            data_linha = _parse_data(linha[idx_data])
            if data_linha and data_linha <= (hoje - timedelta(days=30)):
                return False
            continue

        idx = _col_letra_para_indice(f["col"])
        if idx >= len(linha):
            return False

        valor_celula = (linha[idx] or "").strip()

        if op == "gt":
            limite = _obter_valor_threshold(f, thresholds)
            if limite is None:
                continue
            try:
                if int(valor_celula) <= int(limite):
                    return False
            except (ValueError, TypeError):
                return False

        elif op == "lt":
            limite = _obter_valor_threshold(f, thresholds)
            try:
                if int(valor_celula) >= int(limite):
                    return False
            except (ValueError, TypeError):
                return False

        elif op == "eq":
            if valor_celula != f["valor"]:
                return False

        elif op == "ne":
            if valor_celula == f["valor"]:
                return False

        elif op == "in":
            if valor_celula not in f["valor"]:
                return False

        elif op == "not_in":
            if valor_celula in f["valor"]:
                return False

    return True


def _ler_aba_em_chunks(ws, linha_inicial: int, chunk_size: int, max_col: str = "Z"):
    """Gera chunks de linhas da aba, parando quando vier vazio."""
    pos = linha_inicial
    while True:
        fim = pos + chunk_size - 1
        range_str = f"A{pos}:{max_col}{fim}"

        chunk = _chamar_api(
            lambda r=range_str: ws.get(r),
            f"get {ws.title} {range_str}"
        )

        if not chunk:
            return

        yield chunk

        if len(chunk) < chunk_size:
            return

        pos += chunk_size


def _sincronizar_filtrado(ss_origem, ss_destino, cfg: dict, thresholds: dict) -> dict:
    t = time.time()
    hoje = date.today()

    if "saida_continua" in cfg:
        modo_saida = "continua"
        s = cfg["saida_continua"]
        indices_saida = [_col_letra_para_indice(c) for c in s["colunas_origem"]]
    elif "saida_blocos" in cfg:
        modo_saida = "blocos"
        s = cfg["saida_blocos"]
    else:
        raise ValueError(f"Config sem saida_continua nem saida_blocos: {cfg['aba_destino']}")

    linhas_filtradas = []

    for fonte in cfg["fontes"]:
        ws_origem = _abrir_aba(ss_origem, fonte["aba"])
        linha_inicial = fonte["linha_inicial"]
        filtros = fonte["filtros"]

        contador_fonte = 0
        for chunk in _ler_aba_em_chunks(ws_origem, linha_inicial, CHUNK_SIZE_LINHAS):
            for linha in chunk:
                if _aplicar_filtros(linha, filtros, thresholds, hoje):
                    linhas_filtradas.append(linha)
                    contador_fonte += 1
            del chunk
        gc.collect()
        logger.info(f"[sheets_sync] {fonte['aba']}: {contador_fonte} linhas apos filtro")

    if not linhas_filtradas:
        # ainda assim limpa o destino para nao deixar dados stale
        ws_destino = _garantir_aba(ss_destino, cfg["aba_destino"])
        if modo_saida == "continua":
            num_cols = len(indices_saida)
            _limpar_aba(ws_destino, s["col_inicio_destino"], num_cols, s.get("col_protegida_de"))
        else:
            for bloco in s["blocos"]:
                cd = bloco["col_inicio_destino"]
                nc = len(bloco["colunas_origem"])
                _limpar_aba(ws_destino, cd, nc, s.get("col_protegida_de"))
        return {"ok": True, "linhas": 0, "segundos": round(time.time() - t, 2)}

    ws_destino = _garantir_aba(ss_destino, cfg["aba_destino"])

    if modo_saida == "continua":
        return _escrever_saida_continua(ws_destino, linhas_filtradas, indices_saida, s, t)
    else:
        return _escrever_saida_blocos(ws_destino, linhas_filtradas, s, t)


def _escrever_saida_continua(ws_destino, linhas_filtradas, indices_saida, s, t):
    dados_saida = [
        [linha[i] if i < len(linha) else "" for i in indices_saida]
        for linha in linhas_filtradas
    ]
    num_cols = len(indices_saida)

    _limpar_aba(
        ws_destino,
        col_inicio=s["col_inicio_destino"],
        num_cols=num_cols,
        col_protegida_de=s.get("col_protegida_de"),
    )

    col_letra = _col_para_letra(s["col_inicio_destino"])
    _chamar_api(
        lambda: ws_destino.update(f"{col_letra}1", dados_saida, value_input_option="USER_ENTERED"),
        f"update destino {ws_destino.title}"
    )

    return {
        "ok"      : True,
        "linhas"  : len(dados_saida),
        "colunas" : num_cols,
        "segundos": round(time.time() - t, 2),
    }


def _escrever_saida_blocos(ws_destino, linhas_filtradas, s, t):
    col_protegida_de = s.get("col_protegida_de")

    # Limpa cada bloco
    for bloco in s["blocos"]:
        cd = bloco["col_inicio_destino"]
        nc = len(bloco["colunas_origem"])
        col_fim = cd + nc - 1
        if col_protegida_de:
            col_fim = min(col_fim, col_protegida_de - 1)
        if col_fim >= cd:
            col_a = _col_para_letra(cd)
            col_b = _col_para_letra(col_fim)
            _chamar_api(
                lambda a=col_a, b=col_b: ws_destino.batch_clear([f"{a}1:{b}"]),
                f"batch_clear destino {ws_destino.title} {col_a}:{col_b}"
            )

    # Escreve cada bloco
    total_cols = 0
    for bloco in s["blocos"]:
        indices_bloco = [_col_letra_para_indice(c) for c in bloco["colunas_origem"]]
        cd = bloco["col_inicio_destino"]
        dados_bloco = [
            [linha[i] if i < len(linha) else "" for i in indices_bloco]
            for linha in linhas_filtradas
        ]
        if not dados_bloco:
            continue
        col_letra = _col_para_letra(cd)
        _chamar_api(
            lambda d=dados_bloco, c=col_letra: ws_destino.update(f"{c}1", d, value_input_option="USER_ENTERED"),
            f"update destino {ws_destino.title} bloco {col_letra}"
        )
        total_cols += len(indices_bloco)

    return {
        "ok"      : True,
        "linhas"  : len(linhas_filtradas),
        "colunas" : total_cols,
        "segundos": round(time.time() - t, 2),
    }


# ===========================================================================
# MODO CONTINUO
# ===========================================================================

def _sincronizar_continuo(ss_origem, ss_destino, cfg):
    t = time.time()

    ws_origem  = _abrir_aba(ss_origem, cfg["aba_origem"])
    ws_destino = _garantir_aba(ss_destino, cfg["aba_destino"])

    dados = _chamar_api(
        lambda: ws_origem.get_all_values(),
        f"get_all_values origem {cfg['aba_origem']}"
    )
    dados = _remover_linhas_vazias(dados)

    if not dados:
        return {"ok": True, "linhas": 0, "segundos": round(time.time() - t, 2)}

    ci = cfg["col_inicio_origem"] - 1
    nc = cfg.get("num_cols")
    if nc:
        dados = [linha[ci:ci + nc] for linha in dados]
    else:
        dados = [linha[ci:] for linha in dados]

    num_cols = len(dados[0]) if dados else 0

    _limpar_aba(
        ws_destino,
        col_inicio=cfg["col_inicio_destino"],
        num_cols=num_cols,
        col_protegida_de=cfg.get("col_protegida_de"),
    )

    col_letra = _col_para_letra(cfg["col_inicio_destino"])
    _chamar_api(
        lambda: ws_destino.update(f"{col_letra}1", dados, value_input_option="USER_ENTERED"),
        f"update destino {cfg['aba_destino']}"
    )

    return {
        "ok"      : True,
        "linhas"  : len(dados),
        "colunas" : num_cols,
        "segundos": round(time.time() - t, 2),
    }


# ===========================================================================
# MODO GAP
# ===========================================================================

def _sincronizar_gap(ss_origem, ss_destino, cfg):
    t = time.time()

    ws_origem  = _abrir_aba(ss_origem, cfg["aba_origem"])
    ws_destino = _garantir_aba(ss_destino, cfg["aba_destino"])

    dados_origem = _chamar_api(
        lambda: ws_origem.get_all_values(),
        f"get_all_values origem {cfg['aba_origem']}"
    )
    dados_origem = _remover_linhas_vazias(dados_origem)

    if not dados_origem:
        return {"ok": True, "linhas": 0, "segundos": round(time.time() - t, 2)}

    total_linhas = len(dados_origem)
    col_protegida_de = cfg.get("col_protegida_de")

    for bloco in cfg["blocos"]:
        cd      = bloco["col_inicio_destino"]
        nc      = bloco["num_cols"] - len(bloco.get("excluir_indices", []))
        col_fim = cd + nc - 1
        if col_protegida_de:
            col_fim = min(col_fim, col_protegida_de - 1)
        if col_fim >= cd:
            col_a = _col_para_letra(cd)
            col_b = _col_para_letra(col_fim)
            _chamar_api(
                lambda a=col_a, b=col_b: ws_destino.batch_clear([f"{a}1:{b}"]),
                f"batch_clear destino {cfg['aba_destino']} {col_a}:{col_b}"
            )

    total_escritas = 0
    for bloco in cfg["blocos"]:
        ci      = bloco["col_inicio_origem"] - 1
        nc      = bloco["num_cols"]
        cd      = bloco["col_inicio_destino"]
        excluir = bloco.get("excluir_indices", [])

        dados_bloco = []
        for linha in dados_origem:
            fatia = list(linha[ci:ci + nc])
            for idx in sorted(excluir, reverse=True):
                if idx < len(fatia):
                    fatia.pop(idx)
            dados_bloco.append(fatia)

        if not dados_bloco or not dados_bloco[0]:
            continue

        col_letra = _col_para_letra(cd)
        _chamar_api(
            lambda d=dados_bloco, c=col_letra: ws_destino.update(f"{c}1", d, value_input_option="USER_ENTERED"),
            f"update destino {cfg['aba_destino']} bloco {col_letra}"
        )
        total_escritas += len(dados_bloco[0])

    return {
        "ok"      : True,
        "linhas"  : total_linhas,
        "colunas" : total_escritas,
        "segundos": round(time.time() - t, 2),
    }


# ===========================================================================
# UTILITARIOS
# ===========================================================================

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


def _limpar_aba(ws, col_inicio: int, num_cols: int, col_protegida_de):
    col_fim = col_inicio + num_cols - 1
    if col_protegida_de:
        col_fim = min(col_fim, col_protegida_de - 1)
    if col_fim < col_inicio:
        return
    col_a = _col_para_letra(col_inicio)
    col_b = _col_para_letra(col_fim)
    _chamar_api(
        lambda: ws.batch_clear([f"{col_a}1:{col_b}"]),
        f"batch_clear {ws.title} {col_a}:{col_b}"
    )


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