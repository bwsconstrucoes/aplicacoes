# -*- coding: utf-8 -*-
"""
O CONTRATO DE EXPERIÊNCIA — até quando vai, e o que isso muda (15/11/2026).

Ele: *"se é encerramento de contrato de experiência, nós temos essa
informação, nós temos campo para ela na base de dados (…) a gente pode
contratar 45 mais 45, 30 mais 60, 60 mais 30, ou pode contratar 90 dias
direto (…) eu quero facilitar a seleção do tipo de desligamento."* E: *"quando
é encerramento de contrato de experiência, eu não preciso dar aviso."*

⚑ POR QUE TANTO TESTE NUMA CONTA DE DATA: o número que sai daqui decide a
FORMA do desligamento, e forma errada muda verba rescisória. Errar um dia no
fim do contrato é a diferença entre encerrar no prazo e romper antes — que
tem aviso, multa e outra conta.

Roda sem banco: é aritmética de calendário.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.apps.erp.core import pessoal_experiencia as exp


# ---------------------------------------------------------------------------
# A LEITURA DO ARRANJO
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("texto,esperado", [
    ("45+45 dias", [45, 45]),
    ("45 + 45", [45, 45]),
    ("30+60 dias", [30, 60]),
    ("60+30 dias", [60, 30]),
    ("30+30 dias", [30, 30]),
    ("90 dias", [90]),
    ("45 dias, prorrogável por mais 45 dias", [45, 45]),
])
def test_entende_os_arranjos_como_o_cadastro_os_escreve(texto, esperado):
    """O cadastro tem três grafias do mesmo arranjo — vindas da planilha, do
    Pipe e da digitação. A leitura é por NÚMEROS, não por texto exato."""
    assert exp.partes(texto) == esperado


@pytest.mark.parametrize("texto", [
    "", None, "Sem contrato de experiência", "sem contrato de experiencia", "-",
])
def test_sem_experiencia_nao_inventa_prazo(texto):
    assert exp.partes(texto) == []
    assert exp.situacao(date(2026, 3, 1), texto)["tem"] is False


def test_numero_acima_do_teto_legal_nao_entra():
    """⚑ 90 dias é o teto da CLT. Um "120" escrito no cadastro é erro de
    digitação, e tratá-lo como prazo faria o sistema avisar a data errada."""
    assert exp.partes("120 dias") == []


# ---------------------------------------------------------------------------
# A CONTA
# ---------------------------------------------------------------------------
def test_o_primeiro_dia_conta():
    """⚑ Contrato de 45 dias que começa em 1º de março acaba em 14 de ABRIL,
    não em 15: o dia da admissão é o dia 1. Um dia a mais aqui é a diferença
    entre encerrar no prazo e romper o contrato antes."""
    s = exp.situacao(date(2026, 3, 1), "45 dias", hoje=date(2026, 3, 10))
    assert s["fim_total"] == "2026-04-14"


def test_a_prorrogacao_comeca_no_dia_seguinte_ao_fim_da_primeira():
    s = exp.situacao(date(2026, 3, 1), "45+45 dias", hoje=date(2026, 3, 10))
    assert s["fim_primeira"] == "2026-04-14"
    assert s["fim_total"] == "2026-05-29"          # 14/04 + 45 dias
    assert s["prorrogavel"] is True


@pytest.mark.parametrize("hoje,fase", [
    (date(2026, 3, 10), "primeira"),
    (date(2026, 4, 14), "primeira"),               # o último dia ainda é dele
    (date(2026, 4, 15), "prorrogado"),
    (date(2026, 5, 29), "prorrogado"),
    (date(2026, 5, 30), "acabou"),
])
def test_a_fase_vira_no_dia_certo(hoje, fase):
    s = exp.situacao(date(2026, 3, 1), "45+45 dias", hoje=hoje)
    assert s["fase"] == fase


def test_contrato_vencido_avisa_que_virou_prazo_indeterminado():
    """⚑ É o aviso que evita o pedido errado: passou do fim sem ninguém
    encerrar, não é mais encerramento de experiência."""
    s = exp.situacao(date(2026, 1, 5), "30+30 dias", hoje=date(2026, 6, 1))
    assert s["ja_acabou"] is True
    assert "já venceu" in s["texto"]
    assert "indeterminado" in s["texto"]


def test_sem_data_de_admissao_o_sistema_diz_que_falta_o_dado():
    """Ter o contrato e não ter a data é exatamente o caso em que calar seria
    pior: a tela precisa dizer o que falta no cadastro."""
    s = exp.situacao(None, "45+45 dias")
    assert s["tem"] is False
    assert "SEM a data de admissão" in s["texto"]


# ---------------------------------------------------------------------------
# O QUE ISSO MUDA NO PEDIDO
# ---------------------------------------------------------------------------
def test_encerramento_de_experiencia_nao_pede_aviso_previo():
    """Ele: *"quando é encerramento de contrato de experiência, eu não preciso
    dar aviso. Então essa data de aviso já se torna desnecessária."* É direito:
    o contrato acaba porque o prazo acabou — aviso prévio é para quem ROMPE
    antes do fim."""
    assert exp.exige_aviso_previo(exp.FORMA_EXPERIENCIA) is False
    # ⚑ E PELO CÓDIGO TAMBÉM: a tela manda o rótulo, o servidor trabalha com
    # o código gravado. Valer só um dos dois faria a tela esconder o campo e o
    # servidor continuar exigindo — a pessoa não veria onde preencher nem
    # conseguiria enviar.
    assert exp.exige_aviso_previo("FIM_EXPERIENCIA") is False
    assert exp.exige_aviso_previo("Demissão com aviso trabalhado") is True
    assert exp.exige_aviso_previo("AVISO_TRABALHADO") is True
    assert exp.exige_aviso_previo("A pedido do colaborador") is True


def test_a_forma_vem_sugerida_so_enquanto_a_experiencia_corre():
    """Sugestão, nunca decisão: quem desliga é gente. Mas começar com a forma
    errada muda verba, muda aviso e muda o que a contabilidade faz."""
    correndo = exp.situacao(date(2026, 3, 1), "45+45 dias", hoje=date(2026, 4, 1))
    acabou = exp.situacao(date(2026, 1, 5), "30+30 dias", hoje=date(2026, 6, 1))
    assert exp.sugestao_de_forma(correndo) == exp.FORMA_EXPERIENCIA
    assert exp.sugestao_de_forma(acabou) == ""
    assert exp.sugestao_de_forma({"tem": False}) == ""


def test_a_frase_diz_a_data_em_portugues_e_quantos_dias_faltam():
    s = exp.situacao(date(2026, 3, 1), "45+45 dias", hoje=date(2026, 4, 1))
    assert "14/04/2026" in s["texto"]
    assert "29/05/2026" in s["texto"]
    assert "prorroga" in s["texto"]


# ---------------------------------------------------------------------------
# A TRAVA DO SERVIDOR, que é a que vale
# ---------------------------------------------------------------------------
def test_o_pedido_de_desligamento_por_experiencia_nao_cobra_a_data_do_aviso():
    """⚑ A tela esconde o campo; o SERVIDOR é que recusa o pedido. As duas
    precisam concordar — e concordam porque leem a mesma função."""
    from app.apps.erp.core import pessoal_solicitacoes as svc
    com_aviso = svc.exigencias("DESLIGAMENTO",
                               {"forma_desligamento": "Demissão com Aviso Trabalhado"})
    sem_aviso = svc.exigencias("DESLIGAMENTO",
                               {"forma_desligamento": "Encerramento de Contrato de Experiência"})
    assert "aviso_previo" in com_aviso["campos"]
    assert "aviso_previo" not in sem_aviso["campos"]
    # a forma em si continua obrigatória nos dois casos
    assert "forma_desligamento" in sem_aviso["campos"]


def test_sem_escolher_a_forma_a_data_do_aviso_continua_exigida():
    """Enquanto ninguém disse qual é a forma, o seguro é cobrar: deixar de
    pedir por falta de informação seria o caminho para o pedido sair pela
    metade."""
    from app.apps.erp.core import pessoal_solicitacoes as svc
    assert "aviso_previo" in svc.exigencias("DESLIGAMENTO", {})["campos"]
