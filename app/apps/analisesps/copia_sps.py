# -*- coding: utf-8 -*-
"""
A cópia da SPsBD no banco, lida por QUEM PRECISA ACHAR SP SEM BAIXAR A PLANILHA.

Hoje quem usa é o BaixaBradesco (13/11/2026). Antes, cada comprovante baixado
custava a aba SPsBD inteira — `A:AK`, ~52 mil linhas × 37 colunas, 150–250 MB —
e, com várias baixas ao mesmo tempo, as quatro linhas de atendimento do serviço
ficavam presas esperando o Google (o log que o dono colou em 13/11 mostrou duas
baixas ocupando linha havia mais de sete minutos).

Esta tabela já existe e já é mantida em dia pela sincronização
(`sincronizacao.sincronizar_delta`). O que este módulo faz é só LER, em duas
perguntas pequenas:

1. `situacao_por_sp` — de cada SP, o carimbo, o status e o agendamento que a
   cópia conhece. É o que permite ao robô dizer "esta linha mudou na planilha
   depois da última sincronização" comparando com a planilha, sem confiar na
   cópia às cegas.
2. `linhas_candidatas` — as SPs que PODEM receber um comprovante (a pagar e
   agendadas, ou pagas há pouco com comprovante), já no formato de uma linha da
   planilha: uma lista de células na ordem A..AK.

⚠️ O STATUS NUNCA VEM DAQUI. A cópia pode estar minutos — ou horas, se ninguém
estiver com a tela aberta — atrás da planilha. Quem decide se uma SP está "Pagar"
ou "Pago" é a leitura que o robô faz da planilha, na hora (ver
`baixabradesco/sheets.py` › `load_spsbd_pela_copia`). Daqui saem só os campos
descritivos (valor, credor, código de barras…), e só das linhas cujo carimbo bate
com o da planilha.
"""
from __future__ import annotations

from . import colunas

# A planilha que o BaixaBradesco lê vai de A a AK (37 colunas). A AL (análise da
# IA) não entra: ninguém casa comprovante por ela.
LARGURA = colunas.COLS["_ak"].idx + 1

STATUS_AGENDA = ("agendar", "agendado", "falhaagendar")


def situacao_por_sp() -> dict:
    """SP → (carimbo, status do pagamento, agendado), como a cópia conhece.

    São três textos curtos por SP: cabe folgado, perto do que era a planilha
    inteira. Levanta erro se o banco não responder — quem chama decide voltar
    ao caminho antigo."""
    from .db import consultar

    linhas = consultar(
        "SELECT id, carimbo, status_pgt, agendado FROM analisesps.sps")
    saida = {}
    for sp_id, carimbo, status, agendado in linhas:
        chave = str(sp_id or "").strip()
        if chave:
            saida[chave] = (str(carimbo or "").strip(),
                            str(status or "").strip(),
                            str(agendado or "").strip())
    return saida


def _sql_candidatas() -> tuple:
    campos = [c.chave for c in colunas.GUARDADAS if c.idx < LARGURA]
    nomes = ", ".join(f'"{c}"' for c in campos)
    marcadores = ", ".join(["?"] * len(STATUS_AGENDA))
    # A janela da data vai DOIS DIAS ALÉM da do robô de propósito: aqui é a
    # data convertida pelo banco, lá é o texto conferido em Python. Folga para
    # o fuso e para a borda do dia; quem decide de verdade é o robô.
    sql = (
        f"SELECT {nomes} FROM analisesps.sps "
        " WHERE (lower(btrim(coalesce(status_pgt, ''))) = 'pagar' "
        f"        AND lower(btrim(coalesce(agendado, ''))) IN ({marcadores})) "
        "    OR (lower(btrim(coalesce(status_pgt, ''))) = 'pago' "
        "        AND btrim(coalesce(comprovante, '')) <> '' "
        "        AND (btrim(coalesce(data_pagamento, '')) = '' "
        "             OR data_pagamento_d IS NULL "
        "             OR data_pagamento_d >= current_date - CAST(? AS integer)))")
    return sql, campos


def linhas_candidatas(dias_pago: int = 30) -> dict:
    """SP → linha no formato da planilha (lista de células, A..AK).

    Candidatas são as que podem receber um comprovante: a pagar e agendadas, ou
    pagas há pouco com comprovante (a baixa pode ter ficado pela metade no Omie).
    Colunas que o banco não guarda (as fórmulas S, W, AC e a conta AK) vêm
    vazias; a AK quem preenche é o robô, lendo da planilha."""
    from .db import consultar

    sql, campos = _sql_candidatas()
    linhas = consultar(sql, tuple(STATUS_AGENDA) + (int(dias_pago) + 2,))
    saida = {}
    for valores in linhas:
        celulas = [""] * LARGURA
        for chave, valor in zip(campos, valores):
            celulas[colunas.COLS[chave].idx] = str(valor if valor is not None else "").strip()
        sp_id = celulas[0]
        if sp_id:
            saida[sp_id] = celulas
    return saida
