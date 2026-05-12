# -*- coding: utf-8 -*-
"""
sheets_sync/config.py
Configurações centralizadas de todas as planilhas suportadas.

Modos suportados:
  - "continuo"  : escrita sequencial a partir de col_inicio_destino
  - "gap"       : escrita em blocos com colunas vazias no meio
  - "filtrado"  : filtra linhas e seleciona colunas, substituindo QUERY
"""

PLANILHAS = {

    # =======================================================================
    # Mapa de Cotação
    # =======================================================================
    "Mapa de Cotação": {
        "abas": [
            # -----------------------------------------------------------
            # SSEspelho (filtrado — substitui QUERY)
            # Dados em B2 — preserva cabeçalho na linha 1
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
                    "colunas_origem"      : ["A", "B", "E", "F", "G", "H", "I", "J", "K", "U", "V", "W", "X"],
                    "col_inicio_destino"  : 2,     # B
                    "linha_inicio_destino": 2,     # preserva linha 1 (cabeçalho)
                    "col_protegida_de"    : None,
                },
            },

            # -----------------------------------------------------------
            # SSEspelhoRecebidos
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
            # RegistroCotePedEspelho
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
            # SSEspelho (Cotação de Suprimentos)
            # Lê A:X filtrado das abas origem e cola em B:Y do destino.
            # Dados começam em B1 (sem preservar linha 1).
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
                "saida_continua": {
                    "colunas_origem"      : ["A","B","C","D","E","F","G","H","I","J","K","L",
                                             "M","N","O","P","Q","R","S","T","U","V","W","X"],
                    "col_inicio_destino"  : 2,     # B
                    "col_protegida_de"    : 26,    # Z em diante = não toca
                },
            },

            # -----------------------------------------------------------
            # SSEspelhoSparkline
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
            # RegistroCotaçõesFornecedor
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
# Aba interna _Config (lê thresholds dinâmicos)
# ---------------------------------------------------------------------------

ABA_CONFIG_INTERNA = "_Config"
RANGE_CONFIG_INTERNA = "A2:B"


def identificar_planilha(nome: str) -> dict | None:
    """Retorna a config da planilha cujo identificador está contido no nome."""
    for chave, config in PLANILHAS.items():
        if chave.lower() in nome.lower():
            return config
    return None