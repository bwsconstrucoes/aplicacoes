"""Análise de SPs — conversão do jeito brasileiro para o do banco.

Os casos aqui não foram inventados: saíram da comparação, SP a SP, entre este
conversor e o do Streamlit sobre a base real de 59.055 solicitações. Os valores
bateram na casa do centavo (R$ 250.061.950,39 nos dois). As datas revelaram
duas coisas que só dado de verdade mostra, e as duas viraram teste abaixo.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from app.apps.analisesps.formatos import data_br, moeda, para_data, para_numero


# ---------------------------------------------------------------------------
# Valores
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("texto,esperado", [
    ("6.750,00", Decimal("6750.00")),
    ("1.234,56", Decimal("1234.56")),
    ("600", Decimal("600")),
    ("R$ 1.000,00", Decimal("1000.00")),
    ("1.234", Decimal("1234")),          # ponto de milhar, sem decimal
    ("1234.56", Decimal("1234.56")),     # ponto decimal, estilo americano
    ("0,01", Decimal("0.01")),
    ("(1.000,00)", Decimal("-1000.00")),  # negativo contábil
    ("-500,00", Decimal("-500.00")),
])
def test_le_valor_como_a_planilha_escreve(texto, esperado):
    assert para_numero(texto) == esperado


@pytest.mark.parametrize("texto", ["", "   ", None, "abc", "-", "R$"])
def test_valor_ausente_vira_nada_e_nao_zero(texto):
    """None e zero são coisas diferentes, e precisam continuar sendo: uma SP
    sem valor preenchido não é uma SP de R$ 0,00. Somá-las como zero esconderia
    o preenchimento faltando."""
    assert para_numero(texto) is None


# ---------------------------------------------------------------------------
# Datas — os dois achados da base real
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("texto,esperado", [
    ("31/12/2026", dt.date(2026, 12, 31)),
    ("01/11/2024", dt.date(2024, 11, 1)),
    ("1/11/2024", dt.date(2024, 11, 1)),        # dia sem zero à esquerda
    ("15/7/2025", dt.date(2025, 7, 15)),        # mês sem zero à esquerda
    ("2026-12-31", dt.date(2026, 12, 31)),
    ("15/7/2025 00:00", dt.date(2025, 7, 15)),  # com hora colada
])
def test_le_data_nas_formas_que_aparecem(texto, esperado):
    assert para_data(texto) == esperado


@pytest.mark.parametrize("texto,esperado", [
    ("24/07/2024\n24/07/2024", dt.date(2024, 7, 24)),
    ("1/11/2024\n01/11/2024\n01/11/2024", dt.date(2024, 11, 1)),
])
def test_data_repetida_com_quebra_de_linha(texto, esperado):
    """1.664 SPs da base têm a data de autorização gravada duas ou três vezes,
    separada por quebra de linha — alguma automação escreveu repetido. As
    metades são iguais; vale a primeira.

    Sem este tratamento, essas 1.664 autorizações apareceriam em branco."""
    assert para_data(texto) == esperado


@pytest.mark.parametrize("texto", [
    "18/12/0202",   # dedo escorregado em 2024
    "02/02/0204",
    "20/03/0203",
    "10/02/0260",
    "19/11/2925",   # e em 2025
])
def test_ano_impossivel_e_recusado(texto):
    """A base tem cinco SPs com o ano digitado errado. Aceitá-las seria pior do
    que recusá-las: um vencimento no ano 202 encabeça qualquer lista ordenada
    por data, e um no ano 2925 nunca vence — os dois envenenariam
    silenciosamente todo filtro por período.

    Recusadas, aparecem como data em branco: visível, e cobrável de quem
    preencheu."""
    assert para_data(texto) is None


@pytest.mark.parametrize("texto", ["", "   ", None, "31/02/2026", "não é data"])
def test_data_ilegivel_vira_nada_sem_estourar(texto):
    """Dado ruim na planilha não pode derrubar a carga inteira."""
    assert para_data(texto) is None


# ---------------------------------------------------------------------------
# O caminho de volta
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("valor,esperado", [
    (Decimal("6750"), "6.750,00"),
    (Decimal("1234567.89"), "1.234.567,89"),
    (Decimal("0.5"), "0,50"),
    (Decimal("-1000"), "-1.000,00"),
    (0, "0,00"),
])
def test_escreve_valor_em_portugues(valor, esperado):
    assert moeda(valor) == esperado


def test_valor_vazio_nao_vira_a_palavra_none():
    assert moeda(None) == ""


def test_escreve_data_em_portugues():
    assert data_br(dt.date(2026, 12, 31)) == "31/12/2026"
    assert data_br(None) == ""


def test_ida_e_volta_preserva_o_valor():
    """O que a planilha escreve, o banco guarda, e a tela mostra de novo igual."""
    for texto in ("6.750,00", "1.234.567,89", "0,01", "999,99"):
        assert moeda(para_numero(texto)) == texto
