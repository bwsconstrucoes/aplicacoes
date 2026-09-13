# -*- coding: utf-8 -*-
"""Planilha diz Pago, Omie não baixou: o reenvio tem de alcançar.

Caso real, 13/09/2026. O dono achou **duas SPs** em que a planilha estava
gravada por inteiro e o Omie não tinha baixado — conferiu nas duas fontes. Ao
reenviar o comprovante, nada acontecia.

A causa: existe um caminho próprio para "planilha paga, Omie pendente", mas ele
exigia a **data de pagamento vazia**. Só que a gravação da planilha escreve
status, carimbo, data, comprovante e conta **de uma vez** — então uma SP com a
planilha completa e o Omie pendente ficava fora do índice, invisível para o
reenvio. O caminho de conserto só funcionava para uma gravação pela metade.

A janela de 30 dias existe por memória: sem ela, "Pago + com comprovante" traria
dezenas de milhares das ~52 mil linhas da planilha.
"""
from datetime import datetime, timedelta

import pytest

from app.apps.baixabradesco.sheets import (DIAS_OMIE_PENDENTE, _pago_ha_pouco,
                                           load_spsbd_omie_pendente)

CABECALHO = [
    'ID', 'Solicitação', 'Vencim.', 'Nome do Credor', 'CPF/CNPJ',
    'Descrição da Despesa', 'Valor Total', 'Centro de Custo', 'Tipo de Despesa',
    'Tipo de Pagamento', 'Responsável pelo Registro', 'Dt. Autorização',
    'Responsável Autorização', 'Status Aut.', 'Status Pgt', 'Código Integração',
    'Anexo Link', 'Card Link', 'Anexo', 'Card', 'Status Aut. Símbolo',
    'Status Pgt. Simbolo', 'Pesquisa', 'Data do Pagamento', 'Info de Pgt',
    'Parcela', 'Nº da NF', 'Agendado', 'Linha', 'Nº do Pedido', 'Anuente',
    'Status Anuencia', 'Comprovante', 'Validação', 'Código de Barras',
    'ID Pipefy Contrato', 'Conta Pagamento', 'Reserva',
]


def linha(sp_id, data_pagamento='', status_pgt='Pago',
          comprovante='https://dropbox/comprovante.pdf'):
    r = [''] * len(CABECALHO)
    r[0] = sp_id
    r[6] = '1.000,00'
    r[14] = status_pgt
    r[23] = data_pagamento
    r[32] = comprovante
    r[36] = '50024-0'
    return r


def dias_atras(n):
    return (datetime.now() - timedelta(days=n)).strftime('%d/%m/%Y')


# ── O caso que motivou ────────────────────────────────────────────────────────

def test_planilha_gravada_por_inteiro_continua_alcancavel():
    """O caso das duas SPs do dono: data preenchida, Omie pendente."""
    carregadas = load_spsbd_omie_pendente(values=[CABECALHO, linha('1442630306', dias_atras(1))])
    assert '1442630306' in carregadas


def test_gravacao_pela_metade_continua_alcancavel():
    """Sem data de pagamento — o único caso que a regra antiga pegava."""
    carregadas = load_spsbd_omie_pendente(values=[CABECALHO, linha('7001', '')])
    assert '7001' in carregadas


# ── Os limites, que existem por memória e por segurança ───────────────────────

def test_pagamento_antigo_fica_de_fora():
    """Sem a janela, este índice traria dezenas de milhares de linhas."""
    carregadas = load_spsbd_omie_pendente(values=[CABECALHO, linha('7002', dias_atras(200))])
    assert carregadas == {}


def test_a_janela_tem_trinta_dias():
    assert DIAS_OMIE_PENDENTE == 30
    assert _pago_ha_pouco(dias_atras(29)) is True
    assert _pago_ha_pouco(dias_atras(31)) is False


def test_sp_ainda_a_pagar_nao_entra_aqui():
    """Esse é o caminho normal, não o de conserto."""
    carregadas = load_spsbd_omie_pendente(
        values=[CABECALHO, linha('7003', dias_atras(1), status_pgt='Pagar')])
    assert carregadas == {}


def test_sp_sem_comprovante_nao_entra():
    """Sem comprovante guardado, não houve baixa anterior para consertar."""
    carregadas = load_spsbd_omie_pendente(
        values=[CABECALHO, linha('7004', dias_atras(1), comprovante='')])
    assert carregadas == {}


@pytest.mark.parametrize('escrito', ['15/09/2026', '2026-09-15', '15/09/26'])
def test_aceita_os_formatos_de_data_que_a_planilha_usa(escrito):
    """A coluna às vezes vem como texto, às vezes como data do Google."""
    # Janela enorme de propósito: aqui se confere só a LEITURA da data.
    assert _pago_ha_pouco(escrito, dias=100000) is True


def test_data_ilegivel_nao_derruba_a_carga():
    assert _pago_ha_pouco('sem data') is False
    assert _pago_ha_pouco('') is False
