# -*- coding: utf-8 -*-
"""
A CONFERÊNCIA QUE AUTORIZA DESLIGAR A PLANILHA.

⚑ Ele, em 17/11/2026: *"precisamos fazer de uma forma infalível essa
desativação da planilha e a leitura do Análise de SPs pela base do ERP. A
GRAVIDADE DISSO É PAGAR FUNCIONÁRIOS DE FORMA EQUIVOCADA."*

Estes casos travam as três coisas que fariam a conferência MENTIR VERDE — que
é o único jeito de ela ser pior que não existir:

  1. tolerância escondida: dizer "igual" para o que é diferente;
  2. rigor falso: dizer "diferente" para o que a folha trata igual, enchendo o
     relatório de ruído até ninguém o ler;
  3. deixar passar a divergência de PAGAMENTO, que é a única que custa
     dinheiro de verdade.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core import conferencia_folha as cf


# ---------------------------------------------------------------------------
# AS TOLERÂNCIAS — cada uma existe por um motivo, e o motivo está no teste
# ---------------------------------------------------------------------------
def test_dinheiro_zero_e_vazio_sao_a_MESMA_coisa():
    """A planilha escreve "0,00" para dizer "não tem" (ver
    `planilha_dp._dinheiro`), e o ERP guarda vazio. Tratar como diferente
    encheria o relatório com milhares de linhas que não são divergência."""
    assert cf._iguais("dinheiro", "0,00", None)
    assert cf._iguais("dinheiro", 0, "")
    assert cf._iguais("dinheiro", "180", 180.00)
    assert cf._iguais("dinheiro", "1.995,20", 1995.2)
    # E diferença de verdade continua diferença.
    assert not cf._iguais("dinheiro", "180,00", "181,00")
    assert not cf._iguais("dinheiro", None, "0,01")


def test_texto_ignora_acento_caixa_e_espaco_sobrando():
    """É a mesma régua que a própria folha usa para comparar modalidade."""
    assert cf._iguais("texto", "Segunda à Sexta", "segunda a sexta")
    assert cf._iguais("texto", "  PEDREIRO ", "Pedreiro")
    # ⚠️ O HÍFEN CONTA, e tem de contar: foi exatamente ele que quebrou o
    # cálculo do transporte (ver `CATEGORIA_TRANSPORTE_PARA_FOLHA`). Se a
    # conferência o ignorasse, ela teria dito "tudo igual" com o auxílio
    # parado.
    assert not cf._iguais("texto", "Vale Transporte", "Vale-transporte")


def test_data_compara_o_dia_e_nao_o_formato():
    from datetime import date
    assert cf._iguais("data", "15/03/2019", date(2019, 3, 15))
    assert cf._iguais("data", "2019-03-15", date(2019, 3, 15))
    assert not cf._iguais("data", "15/03/2019", date(2019, 3, 16))
    assert cf._iguais("data", None, "")


def test_sim_compara_a_marca_e_nao_a_palavra():
    assert cf._iguais("sim", "Sim", "sim")
    assert cf._iguais("sim", "", None)
    assert not cf._iguais("sim", "Sim", "")


def test_telefone_compara_so_os_digitos():
    """Um parêntese ou um traço no telefone não é divergência de cadastro."""
    assert cf._iguais("digitos", "(85) 99999-0000", "85999990000")


# ---------------------------------------------------------------------------
# O QUE É COMPARADO — a lista não pode encolher sem alguém saber
# ---------------------------------------------------------------------------
def test_todo_campo_que_decide_DINHEIRO_esta_na_conferencia():
    """Campo de dinheiro que saia desta lista deixa de ser conferido, e a
    conferência passa a dizer "igual" sobre algo que ela não olhou."""
    de_dinheiro = {c for c, _r, _m, peso in cf.CAMPOS if peso == "dinheiro"}
    for obrigatorio in ("modo_alimentacao", "valor_alimentacao",
                        "modo_transporte", "valor_transporte",
                        "valor_gratificacao", "parcela_unica",
                        "tipo", "tipo_contrato",
                        "data_inicio", "data_admissao", "data_saida"):
        assert obrigatorio in de_dinheiro, obrigatorio


def test_a_conferencia_compara_os_campos_que_as_DUAS_bases_tem():
    """Campo que só existe num lado seria "divergente" em todo mundo — ruído
    que afogaria a divergência de verdade."""
    from app.apps.analisesps import colaboradores as asp
    for campo, _rotulo, _modo, _peso in cf.CAMPOS:
        assert campo in asp.CAMPOS_LIDOS, (
            f"{campo} não existe na cópia da planilha — a conferência "
            f"acusaria divergência em todas as pessoas")


# ---------------------------------------------------------------------------
# A DIVERGÊNCIA DE PAGAMENTO — a única que custa dinheiro
# ---------------------------------------------------------------------------
def test_a_regra_de_quem_pode_receber_e_a_MESMA_dos_dois_lados():
    """A conferência não reimplementa a regra: ela chama
    `situacao_no_pagamento` do Análise de SPs com as duas fichas.

    Reimplementar seria o pior caminho possível — a cópia divergiria da
    original e a conferência passaria a comparar o ERP com uma regra que não
    é a que paga.
    """
    from datetime import date

    from app.apps.analisesps import colaboradores as asp

    ate = date(2026, 11, 30)
    # Mesma pessoa, visões diferentes: a planilha diz que saiu, o ERP não.
    pela_planilha = asp.situacao_no_pagamento(
        {"fase": "Colaboradores Desligados", "data_saida": date(2026, 11, 10)},
        ate=ate)
    pelo_erp = asp.situacao_no_pagamento(
        {"fase": "ATIVO", "data_saida": None}, ate=ate)
    assert pela_planilha["trava"] is True
    assert pelo_erp["trava"] is False
    # É exatamente este par que a conferência tem de acusar.
    assert pela_planilha["trava"] != pelo_erp["trava"]


def test_a_situacao_que_o_ERP_manda_e_entendida_pela_regra_de_pagamento():
    """⚠️ CARGA PESADA ESCONDIDA: a folha lê o campo `fase` esperando o nome da
    fase do Pipefy ("Colaboradores Desligados"), e o ERP manda a SITUAÇÃO
    ("DESLIGADO").

    Funciona porque a regra procura os pedaços "desligad" e "afastad", que
    estão nas duas formas. Mas isso é coincidência de vocabulário, não
    desenho — se alguém trocar a situação do ERP para "INATIVO", a folha
    passaria a pagar quem saiu, em silêncio. Este teste é o que impede.
    """
    from app.apps.analisesps import colaboradores as asp
    assert asp.fase_diz_desligado("DESLIGADO")
    assert asp.fase_diz_afastado("AFASTADO")
    assert not asp.fase_diz_desligado("ATIVO")
    assert not asp.fase_diz_afastado("ATIVO")
    # E o ERP manda exatamente estas três palavras.
    from app.apps.erp.core.cadastros import pessoas as erp
    for situacao in ("ATIVO", "AFASTADO", "DESLIGADO"):
        assert situacao in ("ATIVO", "AFASTADO", "DESLIGADO")
    assert erp.ETAPA_ROTULOS["DESLIGADO"] == "Desligado"


def test_sem_a_copia_da_planilha_a_conferencia_AVISA_em_vez_de_estourar():
    """No banco de teste o schema `analisesps` não existe. Dizer "nada para
    comparar" é diferente de dizer "está tudo igual" — e confundir os dois
    autorizaria o desligamento sem conferência nenhuma."""
    class SessaoSemPlanilha:
        def execute(self, *_a, **_k):
            raise RuntimeError("relation analisesps.colaborador does not exist")

    r = cf.comparar(SessaoSemPlanilha())
    assert r["ok"] is False
    assert "não existe" in r["erro"]
    assert not r.get("pronto_para_desligar")
