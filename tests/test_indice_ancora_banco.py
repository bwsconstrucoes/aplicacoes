"""A RÉGUA do número-índice: alinhar a tabela com o boletim que o dono confere.

O que aconteceu, em 14/09/2026, logo depois de publicar a coluna de índice:

    "Apareceram os índices, mas os números estão diferentes do que eu costumo
    ver. Veja o de 08/2026: 305,943822."

O número não estava errado — estava noutra régua. O Banco Central republica só
a VARIAÇÃO mensal do INCC-DI, então o sistema acumula o índice sozinho, e
acumular exige escolher onde a contagem começa; começava em 100 no mês mais
antigo guardado. O boletim da FGV usa outra base e mostra outro número para o
mesmo mês.

O que estes testes seguram:

1. **O mês informado recebe exatamente o número informado** — se não receber, a
   pessoa digita o do boletim e continua vendo outro, que é o problema todo.
2. **Os outros meses saem dele nos dois sentidos** — para a frente
   multiplicando pelas variações, para trás dividindo.
3. **O fator de reajuste não muda ao trocar a régua.** Esta é a que mais
   importa: se mudasse, alinhar a tela com o boletim mexeria em dinheiro de
   contrato — e mexeria em silêncio.

COM BANCO DE VERDADE porque a série é lida com `ORDER BY`, recalculada linha a
linha e a âncora é outra tabela.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.indices import bcb, reajuste
from app.apps.erp.db.models.cadastros import IndiceAncora, IndiceEconomico

pytestmark = pytest.mark.banco


def _serie(s, variacoes, codigo="INCC-DI", ano=2025):
    """Meses consecutivos a partir de janeiro do ano dado."""
    for i, v in enumerate(variacoes):
        mes = date(ano + i // 12, 1 + i % 12, 1)
        s.add(IndiceEconomico(codigo=codigo, competencia=mes,
                              variacao_pct=Decimal(str(v)), fonte="MANUAL"))
    s.flush()


def _numeros(s, codigo="INCC-DI"):
    return {i.competencia: i.numero_indice
            for i in s.query(IndiceEconomico).filter_by(codigo=codigo).all()}


# ---------------------------------------------------------------------------
# O que a régua faz
# ---------------------------------------------------------------------------
def test_o_mes_informado_fica_com_o_numero_informado(sessao_real):
    """O centro de tudo: ele digita o do boletim e vê o do boletim."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 3, 1),
                       numero_indice="1234.567890", observacao="boletim FGV")
    assert _numeros(s)[date(2025, 3, 1)] == Decimal("1234.567890")


def test_os_meses_seguintes_saem_da_ancora_para_a_frente(sessao_real):
    """1000 em fevereiro, +0,50% em março → 1005."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 2, 1), numero_indice="1000")
    assert _numeros(s)[date(2025, 3, 1)] == Decimal("1005.000000")


def test_os_meses_anteriores_saem_da_ancora_para_tras(sessao_real):
    """Março vale 1005 e subiu 0,50% no mês: fevereiro tinha de ser 1000."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 3, 1), numero_indice="1005")
    n = _numeros(s)
    assert n[date(2025, 2, 1)] == Decimal("1000.000000")
    # E janeiro, desfazendo o +1,00% de fevereiro.
    assert n[date(2025, 1, 1)] == Decimal("990.099010")


def test_ancora_no_primeiro_mes_desloca_a_serie_inteira(sessao_real):
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 1, 1), numero_indice="200")
    n = _numeros(s)
    assert n[date(2025, 1, 1)] == Decimal("200.000000")
    assert n[date(2025, 2, 1)] == Decimal("202.000000")


def test_ancora_no_ultimo_mes_tambem_vale(sessao_real):
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 3, 1), numero_indice="300")
    assert _numeros(s)[date(2025, 3, 1)] == Decimal("300.000000")


# ---------------------------------------------------------------------------
# O que a régua NÃO pode fazer: mexer em dinheiro
# ---------------------------------------------------------------------------
def test_o_fator_de_reajuste_nao_muda_ao_alinhar_a_tabela(sessao_real):
    """A garantia que faz esta funcionalidade ser segura de publicar."""
    s = sessao_real
    _serie(s, ["0.30", "0.42", "1.15", "0.08", "0.97", "0.33"])
    bcb.recalcular_numeros(s, "INCC-DI")
    antes = reajuste.acumulado(s, de=date(2025, 1, 15), ate=date(2025, 6, 1),
                                indice="INCC-DI")

    bcb.definir_ancora(s, competencia=date(2025, 4, 1),
                       numero_indice="1876.543210", observacao="boletim")
    depois = reajuste.acumulado(s, de=date(2025, 1, 15), ate=date(2025, 6, 1),
                                indice="INCC-DI")

    assert depois["fator"] == antes["fator"]
    assert depois["variacao_pct"] == antes["variacao_pct"]


def test_a_razao_entre_dois_meses_continua_sendo_o_fator(sessao_real):
    """Depois de alinhar, dividir final por inicial ainda dá o fator — é assim
    que ele confere na mão, com o boletim ao lado."""
    s = sessao_real
    _serie(s, ["0.30", "0.42", "1.15", "0.08", "0.97", "0.33"])
    bcb.definir_ancora(s, competencia=date(2025, 6, 1), numero_indice="1400.123456")
    r = reajuste.acumulado(s, de=date(2025, 1, 10), ate=date(2025, 6, 1),
                            indice="INCC-DI")
    razao = (Decimal(str(r["indice_final"])) / Decimal(str(r["indice_inicial"])))
    assert abs(razao - r["fator"]) < Decimal("0.0000001")


# ---------------------------------------------------------------------------
# Recusas — número que não dá para usar não entra
# ---------------------------------------------------------------------------
def test_recusa_ancora_num_mes_que_a_tabela_nao_tem(sessao_real):
    """Sem o mês na tabela não há por onde propagar: o número ficaria solto."""
    s = sessao_real
    _serie(s, ["0.00", "1.00"])
    with pytest.raises(ErroValidacao) as e:
        bcb.definir_ancora(s, competencia=date(2024, 5, 1), numero_indice="100")
    assert "05/2024" in str(e.value)


def test_recusa_numero_zero_ou_negativo(sessao_real):
    s = sessao_real
    _serie(s, ["0.00", "1.00"])
    for ruim in ("0", "-12.5"):
        with pytest.raises(ErroValidacao):
            bcb.definir_ancora(s, competencia=date(2025, 1, 1), numero_indice=ruim)


def test_recusa_indice_fora_do_catalogo(sessao_real):
    s = sessao_real
    _serie(s, ["0.00"])
    with pytest.raises(ErroValidacao):
        bcb.definir_ancora(s, codigo="INCC-INVENTADO",
                           competencia=date(2025, 1, 1), numero_indice="100")


# ---------------------------------------------------------------------------
# Trocar, tirar e sobreviver à coleta
# ---------------------------------------------------------------------------
def test_trocar_a_ancora_refaz_a_serie_toda(sessao_real):
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 1, 1), numero_indice="100")
    bcb.definir_ancora(s, competencia=date(2025, 1, 1), numero_indice="500")
    assert _numeros(s)[date(2025, 2, 1)] == Decimal("505.000000")
    # Uma âncora por índice: trocar substitui, não acumula.
    assert s.query(IndiceAncora).filter_by(codigo="INCC-DI").count() == 1


def test_tirar_a_ancora_volta_para_a_regua_do_sistema(sessao_real):
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 1, 1), numero_indice="777")
    assert bcb.limpar_ancora(s) is True
    assert _numeros(s)[date(2025, 1, 1)] == Decimal("100.000000")
    assert bcb.limpar_ancora(s) is False       # tirar de novo não quebra nada


def test_lancar_mes_a_mao_depois_mantem_a_regua(sessao_real):
    """O mês novo entra por cima e a série continua pendurada no mesmo ponto."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 2, 1), numero_indice="1000")
    bcb.lancar_manual(s, codigo="INCC-DI", competencia=date(2025, 4, 1),
                      variacao_pct="1.00")
    n = _numeros(s)
    assert n[date(2025, 2, 1)] == Decimal("1000.000000")     # a âncora não saiu
    assert n[date(2025, 4, 1)] == Decimal("1015.050000")     # 1005 × 1,01


def test_ancora_apontando_para_mes_que_sumiu_nao_derruba(sessao_real):
    """Cai para a régua do sistema em vez de espalhar número deslocado."""
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    bcb.definir_ancora(s, competencia=date(2025, 2, 1), numero_indice="1000")
    s.query(IndiceEconomico).filter_by(codigo="INCC-DI",
                                       competencia=date(2025, 2, 1)).delete()
    s.flush()
    bcb.recalcular_numeros(s, "INCC-DI")
    assert _numeros(s)[date(2025, 1, 1)] == Decimal("100.000000")


# ---------------------------------------------------------------------------
# O que a tela lê
# ---------------------------------------------------------------------------
def test_a_listagem_diz_qual_regua_esta_valendo(sessao_real):
    s = sessao_real
    _serie(s, ["0.00", "1.00", "0.50"])
    d = bcb.listar(s, "INCC-DI")
    assert "100,000000 em 01/2025" in d["base_do_numero"]
    assert d["ancora"] is None

    bcb.definir_ancora(s, competencia=date(2025, 2, 1),
                       numero_indice="1234.5", observacao="boletim FGV 02/2025")
    d = bcb.listar(s, "INCC-DI")
    assert "1.234,500000 em 02/2025" in d["base_do_numero"]
    assert d["ancora"]["observacao"] == "boletim FGV 02/2025"
    assert d["ancora"]["competencia"] == "2025-02-01"
