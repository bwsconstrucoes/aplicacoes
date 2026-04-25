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
                # Bloco 1: Registros A→C  → destino A→C
                # Gap:     D→H vazio (intencionalmente)
                # Bloco 2: Registros I→S (exceto H) → destino I→R (10 colunas)
                # Protege: S em diante (tem fórmulas)
                "modo"             : "gap",
                "aba_origem"       : "Registros",
                "origem_id"        : "1JKhvjAUlTuqt2yMbqZNnzk4IGJ57Cx0MwMU4hGH_ajY",
                "aba_destino"      : "RegistroCotePedEspelho",
                "col_limpar_ate"   : 18,   # limpa A→R (cols 1-18) antes de escrever
                "col_protegida_de" : 19,   # S em diante = não toca
                "blocos": [
                    # Bloco 1: colunas A→C da origem → A→C do destino
                    {
                        "col_inicio_origem"  : 1,   # A
                        "num_cols"           : 3,   # A, B, C
                        "col_inicio_destino" : 1,   # A
                    },
                    # Bloco 2: colunas I→S da origem (excluindo H) → I→R do destino
                    # O SELECT já exclui H (Col8) e entrega 10 colunas
                    # Origem: I=9, J=10, K=11, L=12, M=13, N=14, O=15, P=16, Q=17, R=18, S=19
                    # SELECT retorna: Col1,2,3,4,5,6,7,8,10,11 = I,J,K,L,M,N,O,P,R,S (pula Q=Col9)
                    # Na pratica o Python le I:S inteiro e filtra colunas pelo indice
                    {
                        "col_inicio_origem"  : 9,    # I
                        "num_cols"           : 11,   # I→S = 11 colunas
                        "col_inicio_destino" : 9,    # I
                        "excluir_indices"    : [7],  # índice 7 dentro do bloco = coluna P da origem (H relativo) -- ajustar se necessário
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
                "col_protegida_de"   : 26,    # Z em diante = não toca (Z=26)
            },
            {
                "modo"               : "continuo",
                "aba_origem"         : "SSEspelhoRecebimentoPedidos",
                "origem_id"          : "1PvecWVPcqMmj1o056ZErevt0cjna6ggz48uNFTahu_M",
                "aba_destino"        : "SSEspelhoSparkline",
                "col_inicio_destino" : 1,     # A
                "col_inicio_origem"  : 1,     # A
                "num_cols"           : 6,     # A:F
                "col_protegida_de"   : None,
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
