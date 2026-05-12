# -*- coding: utf-8 -*-
"""
sheets_sync/config.py
Configurações centralizadas de todas as planilhas suportadas.

Modos suportados:
  - "continuo"  : escrita sequencial a partir de col_inicio_destino
  - "gap"       : escrita em blocos com colunas vazias no meio
  - "filtrado"  : filtra linhas e seleciona colunas de uma ou mais abas origem,
                  concatenando e escrevendo no destino (substitui QUERYs)

Para adicionar uma nova planilha, basta incluir uma nova entrada em PLANILHAS.
"""

# ---------------------------------------------------------------------------
# Estrutura modo "filtrado":
#
# {
#   "modo"               : "filtrado",
#   "aba_destino"        : nome da aba destino
#   "origem_id"          : ID da planilha origem
#   "fontes": [
#       {
#           "aba"           : nome da aba origem (ex: "Pendências")
#           "linha_inicial" : primeira linha de dados (ex: 3)
#           "filtros": [
#               {"col": "A", "op": "gt",      "valor": 779166760, "tipo": "int"},
#               {"col": "J", "op": "not_in",  "valor": ["RECEBIDO", "CANCELADO"]},
#               {"col": "J", "op": "ne",      "valor": "CANCELADO"},
#               {"col": "...", "op": "exclude_recebidos_antigos_30d"},  # filtro especial
#           ],
#           "threshold_chave": "threshold_pendencias",  # se o valor vier de _Config (opcional)
#       },
#       ...
#   ]
#   -- escolha UM dos dois modos de saída:
#
#   -- "saida_continua": colunas concatenadas a partir de col_inicio_destino
#   "saida_continua": {
#       "colunas_origem"     : ["A", "B", "E", ...],   # quais colunas extrair
#       "col_inicio_destino" : 2,                       # B no destino
#       "col_protegida_de"   : None,
#   }
#
#   -- "saida_blocos": múltiplos blocos contíguos com gaps no destino
#   "saida_blocos": {
#       "blocos": [
#           {"colunas_origem": [...], "col_inicio_destino": 1},
#           {"colunas_origem": [...], "col_inicio_destino": 14},
#       ],
#       "col_limpar_ate"   : 24,    # limpa A..X antes de escrever
#       "col_protegida_de" : 25,
#   }
# }
#
# OPERADORES de filtro suportados:
#   "gt"      : maior que
#   "lt"      : menor que
#   "eq"      : igual
#   "ne"      : diferente
#   "in"      : valor em lista
#   "not_in"  : valor NÃO em lista
#   "exclude_recebidos_antigos_30d": filtro especial — exclui linhas com J=RECEBIDO E coluna O <= HOJE-30
# ---------------------------------------------------------------------------

PLANILHAS = {

    # =======================================================================
    # Mapa de Cotação
    # =======================================================================
    "Mapa de Cotação": {
        "abas": [
            # -----------------------------------------------------------
            # SSEspelho (filtrado — substitui QUERY)
            # -----------------------------------------------------------
            {
                "modo"        : "filtrado",
                "aba_destino" : "SSEspelho",
                "origem_id"   : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "fontes": [
                    {
                        "aba"            : "Pendências",
                        "linha_inicial"  : 3,
                        "filtros": [
                            {"col": "A", "op": "gt", "tipo": "int", "threshold_chave": "threshold_pendencias"},
                            {"col": "J", "op": "not_in", "valor": ["RECEBIDO", "CANCELADO"]},
                        ],
                    },
                    {
                        "aba"            : "Pedidos",
                        "linha_inicial"  : 3,
                        "filtros": [
                            {"col": "A", "op": "gt", "tipo": "int", "threshold_chave": "threshold_pedidos"},
                            {"col": "J", "op": "not_in", "valor": ["RECEBIDO", "CANCELADO"]},
                        ],
                    },
                ],
                "saida_continua": {
                    "colunas_origem"     : ["A", "B", "E", "F", "G", "H", "I", "J", "K", "U", "V", "W", "X"],
                    "col_inicio_destino" : 2,     # B
                    "col_protegida_de"   : None,
                },
            },

            # -----------------------------------------------------------
            # SSEspelhoRecebidos (cópia simples, mantém modo contínuo)
            # -----------------------------------------------------------
            {
                "modo"               : "continuo",
                "aba_origem"         : "SSEspelhoRecebidos",
                "origem_id"          : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "aba_destino"        : "SSEspelhoRecebidos",
                "col_inicio_destino" : 2,
                "col_inicio_origem"  : 1,
                "num_cols"           : 5,
                "col_protegida_de"   : None,
            },

            # -----------------------------------------------------------
            # RegistroFornecedores (Mapa de Cotação)
            # -----------------------------------------------------------
            {
                "modo"             : "gap",
                "aba_origem"       : "Registro",
                "origem_id"        : "1xIXuYhPRBgAnIk4aLV93kyWGikR7RKMAWVHPGiLYPQk",
                "aba_destino"      : "RegistroFornecedores",
                "col_limpar_ate"   : 12,
                "col_protegida_de" : 13,
                "blocos": [
                    {"col_inicio_origem": 1,  "num_cols": 11, "col_inicio_destino": 1},
                    {"col_inicio_origem": 14, "num_cols": 1,  "col_inicio_destino": 12},
                ],
            },

            # -----------------------------------------------------------
            # RegistroCotePedEspelho (Mapa de Cotação)
            # -----------------------------------------------------------
            {
                "modo"               : "continuo",
                "aba_origem"         : "RegistrosCotaçõesMapa",
                "origem_id"          : "1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY",
                "aba_destino"        : "RegistroCotePedEspelho",
                "col_inicio_destino" : 1,
                "col_inicio_origem"  : 1,
                "num_cols"           : 6,
                "col_protegida_de"   : 7,
            },
        ]
    },

    # =======================================================================
    # Cotação de Suprimentos
    # =======================================================================
    "Cotação de Suprimentos": {
        "abas": [
            # -----------------------------------------------------------
            # RegistroCotePedEspelho (já era gap, sem mudança)
            # -----------------------------------------------------------
            {
                "modo"             : "gap",
                "aba_origem"       : "Registros",
                "origem_id"        : "1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY",
                "aba_destino"      : "RegistroCotePedEspelho",
                "col_limpar_ate"   : 18,
                "col_protegida_de" : 19,
                "blocos": [
                    {"col_inicio_origem": 1, "num_cols": 3,  "col_inicio_destino": 1},
                    {"col_inicio_origem": 5, "num_cols": 3,  "col_inicio_destino": 5},
                    {"col_inicio_origem": 9, "num_cols": 11, "col_inicio_destino": 9, "excluir_indices": [8]},
                ],
            },

            # -----------------------------------------------------------
            # SSEspelho (Cotação de Suprimentos) - filtrado em blocos
            # Substitui as 3 QUERYs (A1, N1, T1) em SSEspelhoCotações
            # -----------------------------------------------------------
            {
                "modo"        : "filtrado",
                "aba_destino" : "SSEspelho",
                "origem_id"   : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "fontes": [
                    {
                        "aba"            : "Pendências",
                        "linha_inicial"  : 3,
                        "filtros": [
                            {"col": "J", "op": "ne", "valor": "CANCELADO"},
                            {"op": "exclude_recebidos_antigos_30d", "col_status": "J", "col_data": "O"},
                        ],
                    },
                    {
                        "aba"            : "Pedidos",
                        "linha_inicial"  : 4,
                        "filtros": [
                            {"col": "J", "op": "ne", "valor": "CANCELADO"},
                            {"op": "exclude_recebidos_antigos_30d", "col_status": "J", "col_data": "O"},
                        ],
                    },
                ],
                "saida_blocos": {
                    "blocos": [
                        # Bloco A1: A:L origem → B:M destino (12 colunas, escrita a partir de B)
                        {
                            "colunas_origem"     : ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"],
                            "col_inicio_destino" : 2,     # B
                        },
                        # Bloco N1: N:R origem → N:R destino (5 colunas)
                        {
                            "colunas_origem"     : ["N", "O", "P", "Q", "R"],
                            "col_inicio_destino" : 14,    # N
                        },
                        # Bloco T1: T:X origem → T:X destino (5 colunas)
                        {
                            "colunas_origem"     : ["T", "U", "V", "W", "X"],
                            "col_inicio_destino" : 20,    # T
                        },
                    ],
                    "col_limpar_ate"   : 24,    # limpa B..X (col 2..24)
                    "col_protegida_de" : 26,    # Z em diante = não toca (mantido do original)
                },
            },

            # -----------------------------------------------------------
            # SSEspelhoSparkline (gap, sem mudança)
            # -----------------------------------------------------------
            {
                "modo"             : "gap",
                "aba_origem"       : "SSEspelhoRecebimentoPedidos",
                "origem_id"        : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "aba_destino"      : "SSEspelhoSparkline",
                "col_limpar_ate"   : 9,
                "col_protegida_de" : None,
                "blocos": [
                    {"col_inicio_origem": 1, "num_cols": 3, "col_inicio_destino": 1},
                    {"col_inicio_origem": 5, "num_cols": 5, "col_inicio_destino": 5},
                ],
            },

            # -----------------------------------------------------------
            # RegistroFornecedores (Cotação de Suprimentos)
            # -----------------------------------------------------------
            {
                "modo"             : "gap",
                "aba_origem"       : "Registro",
                "origem_id"        : "1xIXuYhPRBgAnIk4aLV93kyWGikR7RKMAWVHPGiLYPQk",
                "aba_destino"      : "RegistroFornecedores",
                "col_limpar_ate"   : 13,
                "col_protegida_de" : 14,
                "blocos": [
                    {"col_inicio_origem": 1,  "num_cols": 11, "col_inicio_destino": 1},
                    {"col_inicio_origem": 14, "num_cols": 1,  "col_inicio_destino": 12},
                    {"col_inicio_origem": 19, "num_cols": 1,  "col_inicio_destino": 13},
                ],
            },

            # -----------------------------------------------------------
            # RegistroCotaçõesFornecedor (continuo, sem mudança)
            # -----------------------------------------------------------
            {
                "modo"               : "continuo",
                "aba_origem"         : "Cotações",
                "origem_id"          : "1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY",
                "aba_destino"        : "RegistroCotaçõesFornecedor",
                "col_inicio_destino" : 1,
                "col_inicio_origem"  : 1,
                "num_cols"           : 17,
                "col_protegida_de"   : 18,
            },
        ]
    },

}


# ---------------------------------------------------------------------------
# Configuração da aba interna _Config (presente nas planilhas origem
# que precisam fornecer thresholds dinâmicos para filtros)
# ---------------------------------------------------------------------------

ABA_CONFIG_INTERNA = "_Config"
RANGE_CONFIG_INTERNA = "A2:B"   # lê chave (col A) e valor (col B), pulando cabeçalho


def identificar_planilha(nome: str) -> dict | None:
    """Retorna a config da planilha cujo identificador está contido no nome."""
    for chave, config in PLANILHAS.items():
        if chave.lower() in nome.lower():
            return config
    return None