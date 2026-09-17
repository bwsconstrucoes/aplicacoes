"""O NÚMERO-ÍNDICE, ao lado da variação mensal.

Pedido do dono em 14/09/2026: *"você puxou e colocou a variação de um mês pro
outro, mas a gente precisa do índice mesmo (…) caso a gente queira saber qual
índice inicial, qual índice final, a gente precisa visualizar eles dessa forma
(…) pode manter a coluna de percentual, contanto que tenha também a de índice"*.

O que estes testes seguram, e é a parte que erra em silêncio: **a razão entre
dois meses tem de ser exatamente o fator de reajuste**. O número absoluto
depende da base — a nossa é 100 no mês mais antigo, e não a da FGV —, e é por
isso que o que se confere aqui é a DIVISÃO, não o número.

COM BANCO DE VERDADE porque a série é recalculada com `ORDER BY` e gravada
linha a linha.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.indices import bcb, reajuste
from app.apps.erp.db.models.cadastros import IndiceEconomico

pytestmark = pytest.mark.banco


def _serie(s, variacoes, codigo="INCC-DI", ano=2025):
    """Meses consecutivos a partir de janeiro do ano dado."""
    for i, v in enumerate(variacoes):
        mes = date(ano + i // 12, 1 + i % 12, 1)
        s.add(IndiceEconomico(codigo=codigo, competencia=mes,
                              variacao_pct=Decimal(str(v)), fonte="MANUAL"))
    s.flush()


def test_o_primeiro_mes_da_serie_e_a_base(sessao_real):
    s = sessao_real
    _serie(s, ["0.50", "1.00", "0.25"])
    bcb.recalcular_numeros(s, "INCC-DI")
    linhas = {i.competencia: i.numero_indice for i in s.query(IndiceEconomico).all()}
    assert linhas[date(2025, 1, 1)] == Decimal("100.000000")


def test_cada_mes_acumula_a_variacao_do_anterior(sessao_real):
    """100 → +1,00% → 101 → +0,50% → 101,505."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.recalcular_numeros(s, "INCC-DI")
    linhas = {i.competencia: i.numero_indice for i in s.query(IndiceEconomico).all()}
    assert linhas[date(2025, 2, 1)] == Decimal("101.000000")
    assert linhas[date(2025, 3, 1)] == Decimal("101.505000")


def test_a_razao_entre_dois_meses_E_o_fator_de_reajuste(sessao_real):
    """O teste que importa: dividir o índice final pelo inicial dá o mesmo
    número que acumular as variações — que é como o reajuste é calculado."""
    s = sessao_real
    variacoes = ["0.30", "0.42", "1.15", "0.08", "0.97", "0.33"]
    _serie(s, variacoes)
    bcb.recalcular_numeros(s, "INCC-DI")

    acc = reajuste.acumulado(s, indice="INCC-DI",
                             de=date(2025, 1, 15), ate=date(2025, 6, 1))
    razao = (Decimal(str(acc["indice_final"])) /
             Decimal(str(acc["indice_inicial"])))
    assert razao.quantize(Decimal("0.000001")) == \
        acc["fator"].quantize(Decimal("0.000001"))


def test_o_indice_inicial_e_o_do_mes_da_data_base(sessao_real):
    """A data-base é o ponto zero — o mês dela já está dentro do preço. Por
    isso o acumulado começa no mês SEGUINTE, e o índice inicial é o dela."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "1.00", "1.00"])
    bcb.recalcular_numeros(s, "INCC-DI")
    acc = reajuste.acumulado(s, indice="INCC-DI",
                             de=date(2025, 2, 20), ate=date(2025, 4, 1))
    assert acc["mes_do_indice_inicial"] == "2025-02-01"
    assert Decimal(str(acc["indice_inicial"])) == Decimal("101.000000")
    # jan 100 → fev 101 → mar 102,01 → abr 103,0301
    assert Decimal(str(acc["indice_final"])) == Decimal("103.030100")
    # e a divisão bate com o acumulado de março e abril: 1,01² = 1,0201
    assert (Decimal(str(acc["indice_final"])) /
            Decimal(str(acc["indice_inicial"]))).quantize(
                Decimal("0.000001")) == Decimal("1.020100")


def test_mes_revisado_desloca_todos_os_seguintes(sessao_real):
    """O Banco Central revisa série. Refazer só o mês novo deixaria a metade
    velha e a metade nova incompatíveis — e a razão entre um mês de cada lado
    daria um fator errado, com cara de certo."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "1.00"])
    bcb.recalcular_numeros(s, "INCC-DI")
    antes = s.get(IndiceEconomico, ("INCC-DI", date(2025, 3, 1))).numero_indice

    fevereiro = s.get(IndiceEconomico, ("INCC-DI", date(2025, 2, 1)))
    fevereiro.variacao_pct = Decimal("2.00")
    s.flush()
    bcb.recalcular_numeros(s, "INCC-DI")
    depois = s.get(IndiceEconomico, ("INCC-DI", date(2025, 3, 1))).numero_indice

    assert antes == Decimal("102.010000")
    assert depois == Decimal("103.020000")


def test_lancar_a_mao_ja_recalcula_a_serie(sessao_real):
    """Quem digita o boletim da FGV não pode precisar lembrar de um segundo
    passo para o índice aparecer."""
    s = sessao_real
    _serie(s, ["0.00", "1.00"])
    bcb.recalcular_numeros(s, "INCC-DI")
    bcb.lancar_manual(s, codigo="INCC-DI", competencia=date(2025, 3, 1),
                      variacao_pct="0.50")
    linha = s.get(IndiceEconomico, ("INCC-DI", date(2025, 3, 1)))
    assert linha.numero_indice == Decimal("101.505000")


def test_a_tela_recebe_o_indice_e_a_base(sessao_real):
    """Número-índice sem a base escrita vira número solto: quem comparar com o
    boletim da FGV vai achar que o sistema está errado."""
    s = sessao_real
    _serie(s, ["0.00", "1.00"])
    bcb.recalcular_numeros(s, "INCC-DI")
    painel = bcb.listar(s, "INCC-DI")
    # A frase ganhou o aviso da régua (migração 069): sem âncora, o número
    # não é o do boletim, e a tela tem de dizer isso.
    assert painel["base_do_numero"].startswith("100,000000 em 01/2025")
    assert painel["meses"][0]["numero_indice"] == 101.0
    # A variação continua na resposta: o dono pediu as DUAS colunas.
    assert painel["meses"][0]["variacao_pct"] == 1.0


def test_serie_vazia_nao_quebra(sessao_real):
    assert bcb.recalcular_numeros(sessao_real, "IPCA") == 0
