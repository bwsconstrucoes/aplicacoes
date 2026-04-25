# -*- coding: utf-8 -*-
"""
sheets_sync/config.py
Configurações centralizadas de todas as planilhas suportadas.
A identificação é feita pelo nome da planilha (substring).
Para adicionar uma nova planilha, basta incluir uma nova entrada em PLANILHAS.
"""

# ---------------------------------------------------------------------------
# Estrutura de uma aba:
#
# {
#   "aba_origem":   nome da aba na planilha de origem
#   "origem_id":    ID da planilha de origem
#   "aba_destino":  nome da aba na planilha de destino
#   "modo":         "continuo" | "gap"
#
#   -- modo "continuo" (escrita sequencial a partir de col_inicio_destino):
#   "col_inicio_destino": coluna inicial no destino (1=A, 2=B...)
#   "col_inicio_origem":  coluna inicial na origem  (1=A, 2=B...)
#   "num_cols":           quantas colunas copiar (None = até lastColumn)
#   "col_protegida_de":   coluna a partir da qual NÃO limpar (None = sem proteção)
#
#   -- modo "gap" (escrita em dois blocos com colunas vazias no meio):
#   "blocos": [
#       {"col_inicio_origem": X, "num_cols": N, "col_inicio_destino": Y},
#       {"col_inicio_origem": X, "num_cols": N, "col_inicio_destino": Y},
#   ]
#   "col_limpar_ate": última coluna a limpar antes de escrever (ex: 18 = R)
#   "col_protegida_de": coluna a partir da qual NÃO limpar (None = sem proteção)
# }
# ---------------------------------------------------------------------------

PLANILHAS = {

    # -----------------------------------------------------------------------
    # Mapa de Cotação
    # -----------------------------------------------------------------------
    "Mapa de Cotação": {
        "abas": [
            {
                "modo"               : "continuo",
                "aba_origem"         : "SSEspelho",
                "origem_id"          : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "aba_destino"        : "SSEspelho",
                "col_inicio_destino" : 2,     # B
                "col_inicio_origem"  : 1,     # A
                "num_cols"           : 24,    # A:X
                "col_protegida_de"   : None,
            },
            {
                "modo"               : "continuo",
                "aba_origem"         : "SSEspelhoRecebidos",
                "origem_id"          : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "aba_destino"        : "SSEspelhoRecebidos",
                "col_inicio_destino" : 2,     # B
                "col_inicio_origem"  : 1,     # A
                "num_cols"           : 5,     # A:E
                "col_protegida_de"   : None,
            },
        ]
    },

    # -----------------------------------------------------------------------
    # Cotação de Suprimentos
    # -----------------------------------------------------------------------
    "Cotação de Suprimentos": {
        "abas": [
            {
                # Colunas do destino RegistroCotePedEspelho:
                #   A→C  : Registros A→C  (bloco 1)
                #   D    : vazio (gap intencional)
                #   E→G  : Registros E→G  (bloco 2)
                #   H    : vazio (gap intencional)
                #   I→R  : Registros I→S excluindo Q (bloco 3, 10 colunas)
                #   S+   : fórmulas — NÃO TOCAR
                #
                # Mapeamento Registros I→S (11 colunas, índices 0-based):
                #   idx 0=I, 1=J, 2=K, 3=L, 4=M, 5=N, 6=O, 7=P, 8=Q(excluir), 9=R, 10=S
                "modo"             : "gap",
                "aba_origem"       : "Registros",
                "origem_id"        : "1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY",
                "aba_destino"      : "RegistroCotePedEspelho",
                "col_limpar_ate"   : 18,   # limpa A→R antes de escrever
                "col_protegida_de" : 19,   # S em diante = não toca
                "blocos": [
                    # Bloco 1: A→C da origem → A→C do destino
                    {
                        "col_inicio_origem"  : 1,   # A
                        "num_cols"           : 3,   # A, B, C
                        "col_inicio_destino" : 1,   # A
                    },
                    # Bloco 2: E→G da origem → E→G do destino
                    {
                        "col_inicio_origem"  : 5,   # E
                        "num_cols"           : 3,   # E, F, G
                        "col_inicio_destino" : 5,   # E
                    },
                    # Bloco 3: I→S da origem → I→R do destino (exclui Q = idx 8)
                    {
                        "col_inicio_origem"  : 9,    # I
                        "num_cols"           : 11,   # I→S = 11 colunas
                        "col_inicio_destino" : 9,    # I
                        "excluir_indices"    : [8],  # Q = índice 8 (0-based) dentro do bloco
                    },
                ],
            },
            {
                "modo"               : "continuo",
                "aba_origem"         : "SSEspelhoCotações",
                "origem_id"          : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "aba_destino"        : "SSEspelho",
                "col_inicio_destino" : 2,     # B
                "col_inicio_origem"  : 1,     # A
                "num_cols"           : 24,    # A:X
                "col_protegida_de"   : 26,    # Z em diante = não toca
            },
            {
                # SSEspelhoSparkline:
                #   Copia A→C da origem → A→C do destino
                #   Coluna D do destino tem fórmulas — NÃO TOCAR
                #   Copia E→I da origem → E→I do destino
                "modo"             : "gap",
                "aba_origem"       : "SSEspelhoRecebimentoPedidos",
                "origem_id"        : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "aba_destino"      : "SSEspelhoSparkline",
                "col_limpar_ate"   : 9,    # limpa até I (col 9) antes de escrever
                "col_protegida_de" : None,
                "blocos": [
                    # Bloco 1: A→C da origem → A→C do destino
                    {
                        "col_inicio_origem"  : 1,  # A
                        "num_cols"           : 3,  # A, B, C
                        "col_inicio_destino" : 1,  # A
                    },
                    # Bloco 2: E→I da origem → E→I do destino (D fica intocada)
                    {
                        "col_inicio_origem"  : 5,  # E
                        "num_cols"           : 5,  # E, F, G, H, I
                        "col_inicio_destino" : 5,  # E
                    },
                ],
            },
        ]
    },

}


def identificar_planilha(nome: str) -> dict | None:
    """
    Retorna a config da planilha cujo identificador está contido no nome.
    Retorna None se não encontrar.
    """
    for chave, config in PLANILHAS.items():
        if chave.lower() in nome.lower():
            return config
    return None