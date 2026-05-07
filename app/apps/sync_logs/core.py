# -*- coding: utf-8 -*-
"""
sync_logs/core.py
Lógica de sincronização Log Base ↔ Análises de Pagamentos.

Duas operações:
- sync_incremental: lê linhas com AG > last_sync e propaga via chave A+G
- reset_completo: apaga A2:AE de cada Análise e reescreve com snapshot da Log Base
"""

import logging
import time
from typing import Dict, List, Tuple

import gspread

from .config import (
    ABA_LOG,
    COLUNA_DADOS_FIM,
    COLUNA_TIMESTAMP,
    COL_DATA_INDEX,
    COL_TIMESTAMP_INDEX,
    LINHA_INICIO_ESCRITA,
    LOG_BASE_FILE_ID,
    N_COLUNAS_DADOS,
)
from .utils import (
    agora_iso,
    anos_alvo,
    ano_da_data,
    chave_ag,
    col_indice_para_letra,
    get_gc,
    gravar_last_sync,
    ler_analises_ids,
    ler_last_sync,
    normalizar_largura,
)

logger = logging.getLogger(__name__)


# ============================================================
# SYNC INCREMENTAL
# ============================================================

def sync_incremental() -> dict:
    """
    Lê linhas da Log Base com AG > last_sync e propaga pras Análises configuradas.

    Para cada Análise:
      1. Lê A:G inteira (chave A+G existente em cada linha)
      2. Pra cada linha alterada:
         - Se chave A+G existe na Análise → update naquela linha
         - Se não existe                  → append no final
    """
    inicio = time.time()
    gc = get_gc()
    analises_ids = ler_analises_ids()

    last_sync = ler_last_sync()
    novo_sync = agora_iso()

    # 1) Lê Log Base inteira (A:AG) numa chamada
    ss_log = gc.open_by_key(LOG_BASE_FILE_ID)
    ws_log = ss_log.worksheet(ABA_LOG)
    range_origem = f"A:{COLUNA_TIMESTAMP}"
    valores = ws_log.get_values(
        range_origem,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER",
    )

    if not valores:
        gravar_last_sync(novo_sync)
        return {
            "ok": True,
            "linhas_alteradas": 0,
            "mensagem": "Log Base vazia",
            "duracao_s": round(time.time() - inicio, 2),
        }

    # 2) Filtra por AG > last_sync (descarta cabeçalho)
    linhas_alteradas: List[Tuple[str, List]] = []
    for linha in valores[1:]:
        if len(linha) <= COL_TIMESTAMP_INDEX:
            continue
        ts = str(linha[COL_TIMESTAMP_INDEX]).strip()
        if not ts or ts <= last_sync:
            continue

        # Mantém só A:AE (descarta AF e AG ao propagar)
        linha_dados = normalizar_largura(linha[:N_COLUNAS_DADOS])
        chave = chave_ag(linha_dados)
        if not chave:
            logger.warning(
                "Linha sem chave A+G ignorada no sync incremental: %r",
                linha[:7],
            )
            continue
        linhas_alteradas.append((chave, linha_dados))

    if not linhas_alteradas:
        gravar_last_sync(novo_sync)
        return {
            "ok": True,
            "linhas_alteradas": 0,
            "duracao_s": round(time.time() - inicio, 2),
            "last_sync": last_sync,
            "novo_sync": novo_sync,
        }

    # 3) Aplica em cada Análise
    resultados: Dict[str, dict] = {}
    for analise_id in analises_ids:
        try:
            r = _aplicar_em_analise(gc, analise_id, linhas_alteradas)
            resultados[analise_id] = r
        except Exception as e:
            logger.exception("Erro propagando para %s", analise_id)
            resultados[analise_id] = {"ok": False, "erro": str(e)}

    gravar_last_sync(novo_sync)
    duracao = round(time.time() - inicio, 2)

    logger.info(
        "Sync incremental: %d linhas, %d análises, %.2fs",
        len(linhas_alteradas), len(analises_ids), duracao,
    )

    return {
        "ok": True,
        "linhas_alteradas": len(linhas_alteradas),
        "analises": resultados,
        "duracao_s": duracao,
        "last_sync": last_sync,
        "novo_sync": novo_sync,
    }


def _aplicar_em_analise(
    gc,
    analise_id: str,
    linhas_alteradas: List[Tuple[str, List]],
) -> dict:
    """Aplica lista de (chave, dados) em uma Análise específica."""
    ss = gc.open_by_key(analise_id)
    ws = ss.worksheet(ABA_LOG)

    # Lê A:G da Análise pra montar índice {chave: numero_linha}
    valores = ws.get_values(
        "A:G",
        value_render_option="UNFORMATTED_VALUE",
    )

    indice: Dict[str, int] = {}
    for i, linha in enumerate(valores):
        if i == 0:  # cabeçalho
            continue
        # Linha pode vir curta — completa pra calcular chave corretamente
        linha_normalizada = list(linha) + [""] * (N_COLUNAS_DADOS - len(linha))
        chave = chave_ag(linha_normalizada)
        if chave:
            indice[chave] = i + 1  # 0-based -> 1-based

    # Separa updates de inserts
    batch_updates = []  # lista de {"range": ..., "values": [[...]]}
    inserts: List[List] = []

    for chave, dados_linha in linhas_alteradas:
        if chave in indice:
            row_num = indice[chave]
            range_destino = f"A{row_num}:{COLUNA_DADOS_FIM}{row_num}"
            batch_updates.append({"range": range_destino, "values": [dados_linha]})
        else:
            inserts.append(dados_linha)

    # Aplica updates em batch (1 chamada agrupando tudo)
    if batch_updates:
        ws.batch_update(batch_updates, value_input_option="USER_ENTERED")

    # Aplica inserts (append no final)
    if inserts:
        ws.append_rows(
            inserts,
            value_input_option="USER_ENTERED",
            insert_data_option="INSERT_ROWS",
        )

    return {
        "ok": True,
        "updates": len(batch_updates),
        "inserts": len(inserts),
    }


# ============================================================
# RESET COMPLETO
# ============================================================

def reset_completo(analise_id_solicitada: str = None) -> dict:
    """
    Apaga A2:AE de cada Análise (até o fim da aba) e reescreve com snapshot
    da Log Base filtrado por ano corrente + anterior.

    Não toca:
    - Linha 1 (cabeçalho)
    - Colunas AF+ (fórmulas do usuário)

    Se analise_id_solicitada vier preenchido, roda só nessa Análise (botão
    de menu da planilha que apertou). Senão, roda em todas configuradas.
    """
    inicio = time.time()
    gc = get_gc()

    # 1) Lê Log Base e filtra por ano (coluna B é data)
    ss_log = gc.open_by_key(LOG_BASE_FILE_ID)
    ws_log = ss_log.worksheet(ABA_LOG)
    range_origem = f"A:{COLUNA_DADOS_FIM}"
    valores = ws_log.get_values(
        range_origem,
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER",
    )

    anos = anos_alvo()
    snapshot: List[List] = []
    for linha in valores[1:]:  # pula cabeçalho
        if len(linha) <= COL_DATA_INDEX:
            continue
        ano = ano_da_data(linha[COL_DATA_INDEX])
        if ano is None or ano not in anos:
            continue
        snapshot.append(normalizar_largura(linha))

    # 2) Decide quais Análises atualizar
    if analise_id_solicitada:
        analises_ids = [analise_id_solicitada]
    else:
        analises_ids = ler_analises_ids()

    # 3) Reescreve cada Análise
    resultados: Dict[str, dict] = {}
    for analise_id in analises_ids:
        try:
            _resetar_analise(gc, analise_id, snapshot)
            resultados[analise_id] = {
                "ok": True,
                "linhas_escritas": len(snapshot),
            }
        except Exception as e:
            logger.exception("Erro resetando %s", analise_id)
            resultados[analise_id] = {"ok": False, "erro": str(e)}

    # 4) Atualiza last_sync — incremental não vai tentar reaplicar tudo
    gravar_last_sync(agora_iso())

    duracao = round(time.time() - inicio, 2)
    logger.info(
        "Reset completo: %d linhas, %d análises, %.2fs",
        len(snapshot), len(analises_ids), duracao,
    )

    return {
        "ok": True,
        "linhas_escritas": len(snapshot),
        "anos_filtrados": list(anos),
        "analises": resultados,
        "duracao_s": duracao,
    }


def _resetar_analise(gc, analise_id: str, snapshot: List[List]) -> None:
    """Limpa A2:AE até o fim da aba e escreve o snapshot."""
    ss = gc.open_by_key(analise_id)
    ws = ss.worksheet(ABA_LOG)

    # 1) Limpa A2:AE até o fim da aba.
    # gspread usa a notação A1; "A2:AE" sem linha final = até o fim da aba.
    range_limpeza = f"A{LINHA_INICIO_ESCRITA}:{COLUNA_DADOS_FIM}"
    ws.batch_clear([range_limpeza])

    # 2) Escreve snapshot a partir de A2 (se houver dados)
    if not snapshot:
        return

    n = len(snapshot)
    linha_fim = LINHA_INICIO_ESCRITA + n - 1
    range_escrita = f"A{LINHA_INICIO_ESCRITA}:{COLUNA_DADOS_FIM}{linha_fim}"
    ws.update(range_escrita, snapshot, value_input_option="USER_ENTERED")
