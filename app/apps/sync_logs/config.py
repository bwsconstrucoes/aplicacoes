# -*- coding: utf-8 -*-
"""
sync_logs/config.py
Constantes centrais do módulo de sincronização de Log.
"""

# ID da Log Base (Registros de SP) — fonte da verdade
LOG_BASE_FILE_ID = "1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA"

# Nome da aba Log nas duas pontas (Log Base e Análises)
ABA_LOG = "Log"

# Estrutura de colunas da Log Base
# A:AE = 31 colunas de dados (espelhadas para as Análises)
# AF   = ARRAYFORMULA do usuário (intacta)
# AG   = "Atualizado em" — timestamp ISO 8601, escrito pelos scripts
COLUNA_DADOS_FIM = "AE"
N_COLUNAS_DADOS = 31
COLUNA_TIMESTAMP = "AG"
COL_TIMESTAMP_INDEX = 32  # AG = índice 32 (0-based: A=0, AG=32)

# Chave única lógica das linhas: A + "||" + G
# (mesma chave do script noturno limparLog do usuário)
COL_A_INDEX = 0  # A
COL_G_INDEX = 6  # G

# Coluna B = data do registro (usada para filtro de ano no reset)
COL_DATA_INDEX = 1

# Cabeçalho fica na linha 1; escrita começa em A2
LINHA_INICIO_ESCRITA = 2

# Fuso horário usado em timestamps
TIMEZONE_BR = "America/Fortaleza"

# Caminho do arquivo de estado (último sync incremental).
# /tmp persiste entre requests no plano pago do Render.
LAST_SYNC_FILE = "/tmp/sync_logs_last_sync.txt"

# Fallback de last_sync na primeira execução (ou se /tmp foi zerado)
# 2 horas atrás é seguro: pega mudanças recentes sem reler tudo
FALLBACK_HORAS_ATRAS = 2
