"""Conciliação com atribuição ótima (core/pagamentos/conciliacao.py).

O caso difícil é vários pagamentos do MESMO valor na mesma janela: casar por
ordem de data erra quando os nomes dizem o contrário. A atribuição ótima
minimiza o custo do conjunto, não de cada par isolado.

Estas funções são puras — não tocam banco.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.pagamentos import conciliacao as C

from conftest import novo_extrato, novo_pagamento


# ---------------------------------------------------------------------------
# Custo do par
# ---------------------------------------------------------------------------
def test_par_com_valores_que_nao_se_anulam_e_impossivel():
    pg = novo_pagamento(1, valor="1000.00", data=date(2026, 1, 10))
    ex = novo_extrato(1, valor="-990.00", data=date(2026, 1, 10))

    assert C._custo(pg, ex, "ALFA") is None


def test_par_fora_da_janela_de_dias_e_impossivel():
    pg = novo_pagamento(1, valor="1000.00", data=date(2026, 1, 10))
    ex = novo_extrato(1, valor="-1000.00",
                      data=date(2026, 1, 10 + C.JANELA_DIAS + 1))

    assert C._custo(pg, ex, "ALFA") is None


def test_mesmo_dia_e_mesmo_nome_tem_custo_minimo():
    pg = novo_pagamento(1, valor="1000.00", data=date(2026, 1, 10))
    ex = novo_extrato(1, valor="-1000.00", data=date(2026, 1, 10),
                      nome="ALFA CONSTRUCOES LTDA")

    custo = C._custo(pg, ex, "ALFA CONSTRUCOES LTDA")

    assert custo is not None
    assert custo < Decimal("0.05")


def test_nome_divergente_encarece_o_par():
    pg = novo_pagamento(1, valor="1000.00", data=date(2026, 1, 10))
    igual = novo_extrato(1, valor="-1000.00", data=date(2026, 1, 10),
                         nome="ALFA CONSTRUCOES")
    outro = novo_extrato(2, valor="-1000.00", data=date(2026, 1, 10),
                         nome="ZETA TRANSPORTES")

    assert C._custo(pg, igual, "ALFA CONSTRUCOES") < C._custo(pg, outro, "ALFA CONSTRUCOES")


def test_distancia_de_data_encarece_o_par():
    pg = novo_pagamento(1, valor="1000.00", data=date(2026, 1, 10))
    perto = novo_extrato(1, valor="-1000.00", data=date(2026, 1, 10))
    longe = novo_extrato(2, valor="-1000.00", data=date(2026, 1, 14))

    assert C._custo(pg, perto, "ALFA") < C._custo(pg, longe, "ALFA")


def test_confianca_cai_conforme_o_custo_sobe():
    assert C._confianca(Decimal("0")) == Decimal("1.000")
    assert C._confianca(Decimal("0.30")) < C._confianca(Decimal("0.10"))
    assert C._confianca(Decimal("5")) == Decimal("0")  # nunca negativa


# ---------------------------------------------------------------------------
# Atribuição ótima: o coração da conciliação
# ---------------------------------------------------------------------------
def test_atribuicao_otima_prefere_o_nome_a_coincidencia_de_data():
    """Duas SPs de R$ 1.000 na mesma janela, com as datas cruzadas.

    Casar por data pareia ALFA com o extrato da BETA. O ótimo do conjunto
    aceita 2 dias de distância nos dois pares para acertar os credores.
    """
    pg_alfa = novo_pagamento(1, valor="1000.00", data=date(2026, 1, 10))
    pg_beta = novo_pagamento(2, valor="1000.00", data=date(2026, 1, 12))
    ex_alfa = novo_extrato(1, valor="-1000.00", data=date(2026, 1, 12),
                           nome="ALFA CONSTRUCOES LTDA")
    ex_beta = novo_extrato(2, valor="-1000.00", data=date(2026, 1, 10),
                           nome="BETA MATERIAIS ME")
    credores = {1: "ALFA CONSTRUCOES LTDA", 2: "BETA MATERIAIS ME"}

    pares, ambiguo = C._resolver_grupo([pg_alfa, pg_beta], [ex_alfa, ex_beta], credores)

    assert ambiguo is False
    casado = {pg.id: ex.id for pg, ex, _ in pares}
    assert casado == {1: 1, 2: 2}, "pareou pela data em vez do credor"


def test_empate_entre_credores_diferentes_e_ambiguidade_para_humano():
    """Sem nome no extrato e mesma data, a troca mudaria o credor: não chutar."""
    pg1 = novo_pagamento(1, valor="500.00", data=date(2026, 1, 10))
    pg2 = novo_pagamento(2, valor="500.00", data=date(2026, 1, 10))
    ex1 = novo_extrato(1, valor="-500.00", data=date(2026, 1, 10))
    ex2 = novo_extrato(2, valor="-500.00", data=date(2026, 1, 10))
    credores = {1: "ALFA", 2: "BETA"}

    pares, ambiguo = C._resolver_grupo([pg1, pg2], [ex1, ex2], credores)

    assert ambiguo is True
    assert pares == []


def test_empate_do_mesmo_credor_resolve_sozinho():
    """Dois pagamentos do MESMO credor e mesmo valor dão no mesmo: não jogar
    trabalho para o humano."""
    pg1 = novo_pagamento(1, valor="500.00", data=date(2026, 1, 10))
    pg2 = novo_pagamento(2, valor="500.00", data=date(2026, 1, 10))
    ex1 = novo_extrato(1, valor="-500.00", data=date(2026, 1, 10))
    ex2 = novo_extrato(2, valor="-500.00", data=date(2026, 1, 10))
    credores = {1: "ALFA CONSTRUCOES", 2: "ALFA CONSTRUCOES"}

    pares, ambiguo = C._resolver_grupo([pg1, pg2], [ex1, ex2], credores)

    assert ambiguo is False
    assert len(pares) == 2


def test_grupo_sem_par_possivel_devolve_vazio_sem_ambiguidade():
    pg = novo_pagamento(1, valor="100.00", data=date(2026, 1, 10))
    ex = novo_extrato(1, valor="-999.00", data=date(2026, 1, 10))

    pares, ambiguo = C._resolver_grupo([pg], [ex], {1: "ALFA"})

    assert pares == []
    assert ambiguo is False


def test_grupo_vazio_nao_quebra():
    assert C._resolver_grupo([], [], {}) == ([], False)


def test_mais_extratos_que_pagamentos_casa_o_melhor_subconjunto():
    pg = novo_pagamento(1, valor="300.00", data=date(2026, 1, 10))
    perto = novo_extrato(1, valor="-300.00", data=date(2026, 1, 10))
    longe = novo_extrato(2, valor="-300.00", data=date(2026, 1, 13))

    pares, ambiguo = C._resolver_grupo([pg], [perto, longe], {1: "ALFA"})

    assert ambiguo is False
    assert len(pares) == 1
    assert pares[0][1].id == 1


# ---------------------------------------------------------------------------
# Classificação do que não é pagamento de título
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("historico, esperado", [
    ("TARIFA MANUTENCAO DE CONTA", "TARIFA"),
    ("CESTA DE SERVICOS", "TARIFA"),
    ("IOF SOBRE OPERACAO", "IOF"),
    ("RENDIMENTO APLICACAO AUTOMATICA", "RENDIMENTO"),
    ("TED MESMA TITULARIDADE", "TRANSFERENCIA_PROPRIA"),
    ("DEVOLUCAO DE VALOR", "NEUTRA"),
    ("PAGAMENTO DARF", "IMPOSTO"),
    ("CREDITO SALARIO FOLHA", "SALARIO"),
])
def test_classifica_movimentacao_que_nao_e_titulo(historico, esperado):
    assert C.classificar_extrato(historico) == esperado


def test_pagamento_comum_nao_recebe_classificacao():
    assert C.classificar_extrato("PIX ENVIADO ALFA CONSTRUCOES") is None


def test_classificacao_ignora_acento_e_caixa():
    assert C.classificar_extrato("Transferência entre contas") == "TRANSFERENCIA_PROPRIA"


# ---------------------------------------------------------------------------
# Semelhança de nome (o extrato costuma truncar a razão social)
# ---------------------------------------------------------------------------
def test_nome_contido_no_outro_e_praticamente_certo():
    assert C._semelhanca_nome("ALFA CONSTRUCOES", "ALFA CONSTRUCOES LTDA ME") == 0.95


def test_nome_ausente_nao_gera_semelhanca():
    assert C._semelhanca_nome(None, "ALFA") == 0.0
    assert C._semelhanca_nome("ALFA", "") == 0.0


def test_nomes_distintos_tem_semelhanca_baixa():
    assert C._semelhanca_nome("ALFA CONSTRUCOES", "ZETA TRANSPORTES") < 0.5
