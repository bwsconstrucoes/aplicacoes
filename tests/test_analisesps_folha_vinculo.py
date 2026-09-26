# -*- coding: utf-8 -*-
"""
CTPS ou DIÁRIA — o vínculo de cada DIA.

⚠️ TRADUÇÃO FIEL DE UMA FÓRMULA QUE JÁ RODA EM PRODUÇÃO: a coluna AH da aba
`Mobponto` da planilha "Folha de Pagamento - Fortes", que o dono mandou em
26/09/2026 depois de eu classificar errado (eu olhava só o `Tipo de Cadastro`).

⚠️ E A DESCOBERTA QUE MUDA O DESENHO: a classificação é **por DIA**, não por
pessoa. A mesma pessoa tem dias de diária e dias de CTPS no mês em que foi
admitida — os dias entre começar a trabalhar e ser registrada são diária.
Classificar por pessoa jogaria metade do dinheiro dela para o método de pagamento
errado.

A ordem dos testes da fórmula NÃO pode ser "arrumada": vários casos se sobrepõem,
e a ordem é o que decide qual vence.
"""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.analisesps import folha_vinculo as fv


def cadastro(inicio=None, admissao=None, tipo="CTPS",
             contrato="CLT (tempo Indeterminado)"):
    return {"inicio": inicio, "admissao": admissao, "tipo": tipo,
            "contrato": contrato}


D = dt.date


# ---------------------------------------------------------------------------
# O CASO QUE MUDA TUDO: admitido no meio do período
# ---------------------------------------------------------------------------
def test_os_dias_ANTES_da_admissao_sao_DIARIA_e_os_de_depois_CTPS():
    """⚠️ É a razão de a classificação ser por dia. Começou no dia 1, foi
    registrada no dia 10: os dias 1 a 9 são diária, do 10 em diante são CTPS — na
    MESMA quinzena."""
    c = cadastro(inicio=D(2026, 8, 1), admissao=D(2026, 8, 10))

    assert fv.classificar_dia(D(2026, 8, 1), c) == "DIÁRIA"
    assert fv.classificar_dia(D(2026, 8, 9), c) == "DIÁRIA"
    assert fv.classificar_dia(D(2026, 8, 10), c) == "CTPS"
    assert fv.classificar_dia(D(2026, 8, 15), c) == "CTPS"


def test_a_contagem_por_vinculo_mostra_a_quinzena_partida():
    c = cadastro(inicio=D(2026, 8, 1), admissao=D(2026, 8, 10))
    dias = [D(2026, 8, d) for d in range(1, 16)]
    assert fv.classificar_dias(dias, c) == {"DIÁRIA": 9, "CTPS": 6}
    # E ALGUM dia de CTPS já faz a pessoa ser da folha da contabilidade.
    assert fv.da_contabilidade(dias, c) is True


def test_quem_so_tem_dia_de_diaria_NAO_e_da_contabilidade():
    c = cadastro(inicio=D(2026, 8, 20), admissao=D(2026, 9, 1))
    dias = [D(2026, 8, 20), D(2026, 8, 25)]
    assert fv.classificar_dias(dias, c) == {"DIÁRIA": 2}
    assert fv.da_contabilidade(dias, c) is False


# ---------------------------------------------------------------------------
# Os casos da fórmula, um por um
# ---------------------------------------------------------------------------
def test_comecou_e_nunca_foi_registrada_e_DIARIA():
    """Regra 3 da fórmula: `início <= dia` e admissão em branco."""
    c = cadastro(inicio=D(2026, 8, 1), admissao=None,
                 tipo="Prestador de Serviço", contrato="Autônomo (RPA)")
    assert fv.classificar_dia(D(2026, 8, 5), c) == "DIÁRIA"


@pytest.mark.parametrize("inicio,admissao", [
    (D(2026, 8, 1), None),                 # regra 4: sem admissão
    (D(2026, 8, 1), D(2026, 8, 1)),        # regra 5: início == admissão
    (D(2026, 8, 10), D(2026, 8, 1)),       # regra 6: início > admissão
])
def test_prestador_de_servico_com_RPA_e_DIARIA_nos_tres_arranjos(inicio, admissao):
    c = cadastro(inicio=inicio, admissao=admissao,
                 tipo="Prestador de Serviço", contrato="Autônomo (RPA)")
    assert fv.classificar_dia(D(2026, 8, 5), c) == "DIÁRIA"


def test_as_duas_datas_em_branco_pedem_CADASTRO_e_nao_chutam_vinculo():
    """Regra 1: sem as duas datas não há como decidir. Chutar aqui mandaria a
    pessoa para o método de pagamento errado."""
    assert fv.classificar_dia(D(2026, 8, 5), cadastro()) == "DT INÍCIO/ADMISSÃO"


def test_CLT_sem_data_de_admissao_e_cadastro_pela_metade_nao_diarista():
    """Regra 1, segunda parte: CLT sem admissão não é diarista — é cadastro
    incompleto. E é a parte da fórmula que mais protege: sem ela, todo CLT com
    admissão em branco cairia em DIÁRIA pela regra 3."""
    for contrato in ("CLT (tempo Indeterminado)", "CLT (tempo determinado)"):
        c = cadastro(inicio=D(2026, 8, 1), admissao=None, contrato=contrato)
        assert fv.classificar_dia(D(2026, 8, 5), c) == "DT INÍCIO/ADMISSÃO", contrato


def test_o_normal_e_CTPS():
    c = cadastro(inicio=D(2026, 1, 10), admissao=D(2026, 2, 1))
    assert fv.classificar_dia(D(2026, 8, 5), c) == "CTPS"


def test_sem_cadastro_responde_NAO_ENCONTRADO():
    """É o `SEERRO` da fórmula: o PROCV não achou a pessoa."""
    assert fv.classificar_dia(D(2026, 8, 5), None) == "NÃO ENCONTRADO"
    assert fv.classificar_dia(D(2026, 8, 5), {}) == "NÃO ENCONTRADO"


# ---------------------------------------------------------------------------
# O vazio, que no Sheets vale ZERO
# ---------------------------------------------------------------------------
def test_inicio_em_branco_com_admissao_futura_da_DIARIA_como_na_planilha():
    """⚠️ NÃO É ESCOLHA MINHA — é o comportamento do Google Sheets, onde célula
    vazia numa comparação de data vale zero (30/12/1899). Então "início <= dia" é
    verdade com início em branco, e o dia antes da admissão cai em DIÁRIA.

    Está replicado de propósito: mudar aqui faria a classificação divergir da
    planilha justamente nos casos de cadastro incompleto, que são os que mais
    aparecem. Mas é candidato a pergunta para o dono."""
    c = cadastro(inicio=None, admissao=D(2026, 8, 10))
    assert fv.classificar_dia(D(2026, 8, 5), c) == "DIÁRIA"


def test_a_data_aceita_texto_em_portugues_e_datetime():
    c = cadastro(inicio="01/08/2026", admissao="10/08/2026")
    assert fv.classificar_dia("05/08/2026", c) == "DIÁRIA"
    assert fv.classificar_dia(dt.datetime(2026, 8, 12, 7, 30), c) == "CTPS"


def test_o_tipo_e_o_contrato_nao_se_importam_com_maiuscula_e_espaco():
    c = cadastro(inicio=D(2026, 8, 1), admissao=D(2026, 8, 1),
                 tipo="  prestador de SERVIÇO ", contrato="autônomo (RPA)")
    assert fv.classificar_dia(D(2026, 8, 5), c) == "DIÁRIA"
