# -*- coding: utf-8 -*-
"""A leitura da cópia da SPsBD que o BaixaBradesco usa — contra Postgres de verdade.

O filtro das candidatas vive no SQL (`lower(btrim(...))`, a janela da data com
`current_date - CAST(? AS integer)`), e o dublê da suíte ignora WHERE. Um erro
ali passaria pela suíte e apareceria como comprovante "não localizado".
"""
from __future__ import annotations

import datetime as dt

import pytest

pytestmark = pytest.mark.banco


def _semear(*registros):
    from app.apps.analisesps import colunas, sincronizacao
    from app.apps.analisesps.db import conexao
    completos = []
    for r in registros:
        base = {c: "" for c in colunas.CHAVES}
        base.update(r)
        completos.append(base)
    with conexao() as conn:
        sincronizacao.gravar_registros(conn, completos)


def _data(dias_atras: int) -> str:
    return (dt.date.today() - dt.timedelta(days=dias_atras)).strftime("%d/%m/%Y")


def test_candidatas_sao_as_agendadas_a_pagar_e_as_pagas_ha_pouco(banco_analisesps):
    from app.apps.analisesps import copia_sps
    _semear(
        {"id": "1", "status_pgt": "Pagar", "agendado": "agendado", "valor": "10,00"},
        {"id": "2", "status_pgt": " pagar ", "agendado": "FalhaAgendar"},
        {"id": "3", "status_pgt": "Pagar", "agendado": ""},                  # não agendada
        {"id": "4", "status_pgt": "Pago", "comprovante": "https://x/4",
         "data_pagamento": _data(3)},
        {"id": "5", "status_pgt": "Pago", "comprovante": "https://x/5",
         "data_pagamento": _data(120)},                                     # velha
        {"id": "6", "status_pgt": "Pago", "comprovante": "",
         "data_pagamento": _data(1)},                                       # sem comprovante
        {"id": "7", "status_pgt": "Pago", "comprovante": "https://x/7",
         "data_pagamento": ""},                                             # sem data
    )
    linhas = copia_sps.linhas_candidatas(30)
    assert set(linhas) == {"1", "2", "4", "7"}


def test_linha_vem_no_formato_da_planilha(banco_analisesps):
    from app.apps.analisesps import copia_sps
    _semear({"id": "1", "status_pgt": "Pagar", "agendado": "agendado",
             "valor": "1.234,56", "credor": "ACME", "carimbo": "2026-11-13 10:00:00",
             "comprovante": "", "codigo_barras": "2379"})
    celulas = copia_sps.linhas_candidatas(30)["1"]
    assert len(celulas) == 37                      # A..AK
    assert celulas[0] == "1"
    assert celulas[3] == "ACME"                    # D
    assert celulas[6] == "1.234,56"                # G, como a planilha escreve
    assert celulas[14] == "Pagar"                  # O
    assert celulas[21] == "2026-11-13 10:00:00"    # V
    assert celulas[27] == "agendado"               # AB
    assert celulas[34] == "2379"                   # AI
    assert celulas[18] == "" and celulas[36] == "" # S (fórmula) e AK: não guardadas


def test_situacao_traz_carimbo_status_e_agendado_de_todas(banco_analisesps):
    from app.apps.analisesps import copia_sps
    _semear({"id": "1", "status_pgt": "Pago", "carimbo": "c1", "agendado": "agendado"},
            {"id": "2", "status_pgt": "Cancelado", "carimbo": "", "agendado": ""})
    assert copia_sps.situacao_por_sp() == {
        "1": ("c1", "Pago", "agendado"), "2": ("", "Cancelado", "")}


def test_copia_vazia_responde_vazio(banco_analisesps):
    from app.apps.analisesps import copia_sps
    assert copia_sps.situacao_por_sp() == {}
    assert copia_sps.linhas_candidatas(30) == {}


def test_o_robo_le_a_copia_de_verdade_e_chega_ao_mesmo_que_a_planilha_inteira(banco_analisesps):
    """As duas pontas ligadas: a cópia gravada pelo caminho da sincronização
    (`colunas.linha_para_dicionario` + `gravar_registros`), lida pelo módulo de
    verdade, e o resultado do robô igual ao do caminho antigo."""
    from app.apps.analisesps import colunas, sincronizacao
    from app.apps.analisesps.db import conexao
    from app.apps.baixabradesco.sheets import (
        load_spsbd_omie_pendente, load_spsbd_operacional, load_spsbd_pela_copia)
    from tests.test_baixabradesco_spsbd_pela_copia import (
        AbaFalsa, GoogleFalso, _cortar, _planilha_variada, resumo)

    grade = _planilha_variada()
    with conexao() as conn:
        sincronizacao.gravar_registros(
            conn, [colunas.linha_para_dicionario(l) for l in grade[1:] if l[0]])

    aba = AbaFalsa(grade)
    op, pend, info = load_spsbd_pela_copia(GoogleFalso(aba))

    valores = [_cortar(l) for l in grade]
    assert resumo(op) == resumo(load_spsbd_operacional(values=valores))
    assert resumo(pend) == resumo(load_spsbd_omie_pendente(values=valores))
    assert info["fonte"] == "copia" and info["relidas_da_planilha"] == 0
    assert aba.linhas_inteiras_relidas() == []
