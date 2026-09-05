"""As parcelas de uma compra, a partir da condição de pagamento.

A planilha de compras tinha 121 formas de pagamento escritas à mão. Todas
cabem em "quanto entra na hora" + "em quantos dias vencem as demais", e é
assim que ficam guardadas. Este é o cálculo que liga o pedido de compra ao
financeiro: errar aqui é gerar título com vencimento ou valor errado.

O que não pode falhar:
  - a SOMA das parcelas bate com o total, sempre, em qualquer arranjo;
  - o centavo que sobra vai para a ÚLTIMA parcela, não para a entrada;
  - condição sem entrada e sem prazo é recusada, e não vira compra sem cobrança.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos.pagamento import (
    descrever, gerar_parcelas, parcelas_da_condicao,
)

BASE = date(2026, 9, 10)


def _soma(parcelas):
    return sum(p.valor for p in parcelas)


# ---------------------------------------------------------------------------
# Os arranjos reais da planilha
# ---------------------------------------------------------------------------
def test_a_vista_e_uma_parcela_na_data_da_compra():
    p = gerar_parcelas("1000.00", BASE, entrada_percentual=100)
    assert len(p) == 1
    assert p[0].vencimento == BASE and p[0].entrada is True
    assert p[0].valor == Decimal("1000.00")


def test_trinta_dias_e_uma_parcela_um_mes_depois():
    p = gerar_parcelas("1000.00", BASE, dias=[30])
    assert len(p) == 1
    assert p[0].vencimento == date(2026, 10, 10)
    assert p[0].entrada is False


def test_trinta_sessenta_noventa_divide_em_tres():
    p = gerar_parcelas("900.00", BASE, dias=[30, 60, 90])
    assert [x.valor for x in p] == [Decimal("300.00")] * 3
    assert [x.vencimento for x in p] == [date(2026, 10, 10), date(2026, 11, 9),
                                         date(2026, 12, 9)]


def test_entrada_mais_parcelas():
    """'30% + 28/56 dias': 300 na hora, 350 em cada uma das duas seguintes."""
    p = gerar_parcelas("1000.00", BASE, entrada_percentual=30, dias=[28, 56])
    assert p[0].entrada is True and p[0].valor == Decimal("300.00")
    assert p[0].vencimento == BASE
    assert [x.valor for x in p[1:]] == [Decimal("350.00"), Decimal("350.00")]
    assert _soma(p) == Decimal("1000.00")


def test_seis_parcelas_mensais():
    p = gerar_parcelas("600.00", BASE, dias=[30, 60, 90, 120, 150, 180])
    assert len(p) == 6
    assert _soma(p) == Decimal("600.00")


def test_os_prazos_saem_em_ordem_mesmo_se_cadastrados_fora_de_ordem():
    p = gerar_parcelas("300.00", BASE, dias=[90, 30, 60])
    assert [x.vencimento for x in p] == sorted(x.vencimento for x in p)


# ---------------------------------------------------------------------------
# O centavo — onde este tipo de código costuma errar
# ---------------------------------------------------------------------------
def test_a_sobra_de_centavo_vai_para_a_ultima_parcela():
    p = gerar_parcelas("100.00", BASE, dias=[30, 60, 90])
    assert [x.valor for x in p] == [Decimal("33.33"), Decimal("33.33"), Decimal("33.34")]
    assert _soma(p) == Decimal("100.00")


def test_a_entrada_nunca_recebe_a_sobra():
    """Se a sobra caísse na entrada, o fornecedor receberia um centavo a mais
    logo no ato — e a diferença só apareceria na conciliação."""
    p = gerar_parcelas("100.00", BASE, entrada_percentual=33, dias=[30, 60])
    assert p[0].valor == Decimal("33.00")
    assert _soma(p) == Decimal("100.00")


@pytest.mark.parametrize("total", ["0.01", "0.02", "7.77", "1000.03", "99999.99"])
@pytest.mark.parametrize("entrada", [0, 10, 33, 50, 70])
@pytest.mark.parametrize("prazos", [[30], [30, 60], [28, 42, 56],
                                    [30, 60, 90, 120, 150, 180]])
def test_a_soma_fecha_sempre(total, entrada, prazos):
    p = gerar_parcelas(total, BASE, entrada_percentual=entrada, dias=prazos)
    assert _soma(p) == Decimal(total), (
        f"{entrada}% + {prazos} sobre {total} não fecha")


# ---------------------------------------------------------------------------
# O que tem de ser recusado
# ---------------------------------------------------------------------------
def test_condicao_vazia_e_recusada():
    with pytest.raises(ErroValidacao, match="não gera parcela"):
        gerar_parcelas("100.00", BASE)


def test_entrada_de_cem_por_cento_com_prazo_e_recusada():
    with pytest.raises(ErroValidacao, match="não deixa saldo"):
        gerar_parcelas("100.00", BASE, entrada_percentual=100, dias=[30])


@pytest.mark.parametrize("valor", ["0", "-1", "-0.01"])
def test_valor_zerado_ou_negativo_e_recusado(valor):
    with pytest.raises(ErroValidacao, match="maior que zero"):
        gerar_parcelas(valor, BASE, dias=[30])


def test_prazo_negativo_e_recusado():
    with pytest.raises(ErroValidacao, match="negativo"):
        gerar_parcelas("100.00", BASE, dias=[-30])


def test_entrada_fora_da_faixa_e_recusada():
    with pytest.raises(ErroValidacao, match="entre 0% e 100%"):
        gerar_parcelas("100.00", BASE, entrada_percentual=120, dias=[30])


# ---------------------------------------------------------------------------
# A partir da linha cadastrada, e o texto que a tela mostra
# ---------------------------------------------------------------------------
def test_le_a_condicao_cadastrada():
    from app.apps.erp.db.models.cadastros import CondicaoPagamento
    c = CondicaoPagamento(nome="30% + 30/60 dias", entrada_percentual=Decimal("30"),
                          dias=[30, 60])

    p = parcelas_da_condicao(c, "1000.00", BASE)

    assert p[0].valor == Decimal("300.00") and p[0].entrada
    assert _soma(p) == Decimal("1000.00")


def test_sem_condicao_escolhida_recusa():
    with pytest.raises(ErroValidacao, match="condição"):
        parcelas_da_condicao(None, "100.00", BASE)


@pytest.mark.parametrize("entrada,prazos,texto", [
    (100, [], "à vista"),
    (0, [30], "30 dias"),
    (0, [30, 60, 90], "30/60/90 dias"),
    (30, [28, 56], "30% de entrada + 28/56 dias"),
])
def test_a_condicao_em_palavras(entrada, prazos, texto):
    assert descrever(entrada, prazos) == texto
