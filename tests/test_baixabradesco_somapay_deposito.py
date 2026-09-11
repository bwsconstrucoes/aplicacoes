# -*- coding: utf-8 -*-
"""Rescisão paga direto na conta Somapay, baixada direto na conta Somapay.

Este comprovante é emitido pela **própria Somapay**, não pelo Bradesco. Ele não
tem o número da SP, não tem a conta da empresa — só o valor, a data, e o nome e
o CPF de quem recebeu. É outro fluxo do comprovante Bradesco de transferência
para a Somapay: ali o robô lança a transferência antes de baixar; aqui o dinheiro
já estava na Somapay, então a baixa é direta, sem transferência nenhuma.

O exemplo em `exemplos_baixabradesco/somapay_deposito_rescisorio.txt` é real
(11/09/2026), com nome, CPF, CNPJ, contas e número do depósito trocados por
fictícios.
"""
from decimal import Decimal
from pathlib import Path

import pytest

from app.apps.baixabradesco.matcher import match_receipt, match_cpf_valor
from app.apps.baixabradesco.models import BankAccount, SpRecord
from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text
from app.apps.baixabradesco.sheets import find_somapay_account

EXEMPLOS = Path(__file__).parent / 'exemplos_baixabradesco'
CPF_FUNCIONARIO = '12345678909'
CNPJ_EMPRESA = '11222333000144'


@pytest.fixture
def comprovante():
    texto = (EXEMPLOS / 'somapay_deposito_rescisorio.txt').read_text(encoding='utf-8')
    return parse_bradesco_text('rescisao.pdf', 1, texto)


def sp(**kwargs):
    base = dict(
        row_number=2, id='7001', nome_credor='JOAO DA SILVA EXEMPLO',
        cpf_cnpj='123.456.789-09', valor_total='452,40',
        status_pgt='Pagar', status_agendamento='agendado',
        raw={'Tipo de Despesa': 'Rescisões e Indenizações Trabalhistas'},
    )
    base.update(kwargs)
    return SpRecord(**base)


# ── A leitura do comprovante ──────────────────────────────────────────────────

def test_reconhece_o_comprovante_da_somapay(comprovante):
    assert comprovante.tipo_comprovante == 'somapay_deposito'


def test_le_valor_e_data_do_deposito(comprovante):
    assert comprovante.valor_pago == '452,40'
    assert comprovante.data_pagamento == '11/09/2026'


def test_le_o_cpf_de_quem_recebeu_e_nao_o_numero_do_deposito(comprovante):
    """O número do depósito tem 14 dígitos — o tamanho de um CNPJ.

    A varredura solta pegava esse número como se fosse o documento do
    beneficiário, e o casamento por CPF nunca aconteceria.
    """
    assert comprovante.documento_recebedor == CPF_FUNCIONARIO
    assert comprovante.nome_recebedor == 'JOAO DA SILVA EXEMPLO'


def test_le_o_cnpj_de_quem_depositou(comprovante):
    """É a única pista do papel sobre qual conta Somapay usar."""
    assert comprovante.documento_pagador == CNPJ_EMPRESA


def test_o_comprovante_nao_traz_numero_de_sp(comprovante):
    assert comprovante.id_pipefy == ''


# ── O casamento por CPF + valor ───────────────────────────────────────────────

def test_casa_pelo_cpf_e_valor(comprovante):
    resultado = match_receipt(comprovante, {'7001': sp()}, [])
    assert resultado.status == 'localizado'
    assert resultado.id == '7001'
    assert resultado.metodo == 'somapay_deposito_cpf_valor'


def test_cpf_certo_e_valor_diferente_nao_casa(comprovante):
    assert match_cpf_valor(comprovante, [sp(valor_total='500,00')]) == []


def test_valor_certo_e_cpf_de_outra_pessoa_nao_casa(comprovante):
    assert match_cpf_valor(comprovante, [sp(cpf_cnpj='987.654.321-00')]) == []


def test_sp_ja_paga_nao_e_baixada_de_novo(comprovante):
    assert match_cpf_valor(comprovante, [sp(status_pgt='Pago')]) == []


def test_falhaagendar_tambem_casa(comprovante):
    """Agendamento que falhou costuma virar pagamento na mão — é justamente o
    caso deste comprovante."""
    achados = match_cpf_valor(comprovante, [sp(status_agendamento='falhaagendar')])
    assert [r.id for r in achados] == ['7001']


def test_o_cpf_casa_com_a_planilha_em_qualquer_formatacao(comprovante):
    """Na planilha o CPF pode estar com ponto, sem ponto ou com zero à esquerda."""
    for escrito in ('123.456.789-09', '12345678909', '123456789-09'):
        achados = match_cpf_valor(comprovante, [sp(cpf_cnpj=escrito)])
        assert [r.id for r in achados] == ['7001'], escrito


def test_duas_sps_da_mesma_pessoa_e_mesmo_valor_ficam_pendentes(comprovante):
    """Sem desempate possível, não executa: conferência humana."""
    duas = {
        '7001': sp(id='7001', raw={'Tipo de Despesa': 'Rescisões e Indenizações Trabalhistas'}),
        '7002': sp(id='7002', raw={'Tipo de Despesa': 'Rescisões e Indenizações Trabalhistas'}),
    }
    resultado = match_receipt(comprovante, duas, [])
    assert resultado.status == 'pendente_validacao'


def test_desempate_pela_verba_rescisoria(comprovante):
    """Mesma pessoa, mesmo valor: fica a SP de rescisão, que é o que o papel prova."""
    duas = {
        '7001': sp(id='7001', raw={'Tipo de Despesa': 'Rescisões e Indenizações Trabalhistas'}),
        '7002': sp(id='7002', raw={'Tipo de Despesa': 'Material de Construção'}),
    }
    resultado = match_receipt(comprovante, duas, [])
    assert resultado.status == 'localizado'
    assert resultado.id == '7001'


# ── A conta em que a baixa é lançada ──────────────────────────────────────────

def conta(banco, cnpj, codigo_omie):
    return BankAccount(row_number=2, banco=banco, conta='22005-1',
                       codigo_omie=codigo_omie, raw={'CNPJ': cnpj})


def test_acha_a_conta_somapay_pelo_cnpj_de_quem_depositou():
    contas = [
        conta('Somapay BWS', '11.222.333/0001-44', '586876091'),
        conta('Somapay IFPESANTACRUZ', '99.888.777/0001-66', '11119266982'),
        conta('Bradesco', '11.222.333/0001-44', '583772104'),
    ]
    achada = find_somapay_account(contas, CNPJ_EMPRESA)
    assert achada is not None
    assert achada.codigo_omie == '586876091'


def test_uma_unica_conta_somapay_dispensa_o_cnpj():
    contas = [conta('Somapay BWS', '', '586876091')]
    assert find_somapay_account(contas, '').codigo_omie == '586876091'


def test_duas_contas_somapay_com_o_mesmo_cnpj_nao_sao_adivinhadas():
    """Errar a conta joga o dinheiro na contabilidade errada. Melhor parar."""
    contas = [
        conta('Somapay BWS', '11.222.333/0001-44', '586876091'),
        conta('Somapay IFPESANTACRUZ', '11.222.333/0001-44', '11119266982'),
    ]
    assert find_somapay_account(contas, CNPJ_EMPRESA) is None


def test_sem_conta_somapay_cadastrada_nao_inventa():
    contas = [conta('Bradesco', '11.222.333/0001-44', '583772104')]
    assert find_somapay_account(contas, CNPJ_EMPRESA) is None


def test_cnpj_desconhecido_com_duas_contas_nao_escolhe():
    contas = [
        conta('Somapay BWS', '11.222.333/0001-44', '586876091'),
        conta('Somapay IFPESANTACRUZ', '99.888.777/0001-66', '11119266982'),
    ]
    assert find_somapay_account(contas, '00000000000000') is None


# ── A baixa é direta, sem transferência ───────────────────────────────────────
#
# Existem DOIS caminhos Somapay, e confundi-los custa caro. Quando o dinheiro
# sai do Bradesco para a Somapay, o robô precisa lançar a transferência antes de
# baixar. Quando o pagamento já saiu da própria Somapay — este comprovante — a
# transferência não existe: lançá-la criaria no Omie um dinheiro que não andou.

def plano_para(comprovante, banco):
    from app.apps.baixabradesco.models import ExecutionPlan, MatchResult
    registro = sp()
    return ExecutionPlan(
        receipt=comprovante,
        match=MatchResult(status='localizado', metodo='somapay_deposito_cpf_valor',
                          id=registro.id, sp=registro),
        banco=banco,
    )


def test_a_baixa_e_lancada_na_conta_somapay(comprovante):
    from app.apps.baixabradesco.omie import build_omie_plan

    somapay = conta('Somapay BWS', '11.222.333/0001-44', '586876091')
    passos = build_omie_plan(plano_para(comprovante, somapay), {})

    baixa = [p for p in passos if p['step'] == 'baixar']
    assert len(baixa) == 1
    param = baixa[0]['request']['param'][0]
    assert param['codigo_conta_corrente'] == '586876091'
    assert param['valor'] == '452.40'
    assert param['data'] == '11/09/2026'


def test_nao_ha_transferencia_no_caminho(comprovante):
    from app.apps.baixabradesco.omie import build_omie_plan

    somapay = conta('Somapay BWS', '11.222.333/0001-44', '586876091')
    passos = build_omie_plan(plano_para(comprovante, somapay), {})

    assert [p['step'] for p in passos] == ['consultar', 'alterar_se_necessario', 'baixar']
    assert not any(p.get('lanccc') for p in passos)


def test_o_core_nao_manda_este_comprovante_pelo_fluxo_de_transferencia():
    """O fluxo com transferência é acionado pelo tipo 'somapay', não por este."""
    import inspect
    from app.apps.baixabradesco import core

    fonte = inspect.getsource(core.processar_baixabradesco)
    assert "rec.tipo_comprovante == 'somapay' and" in fonte
    assert "build_somapay_plan" in fonte
    assert "rec.tipo_comprovante == 'somapay_deposito'" in fonte


def test_sem_conta_resolvida_a_baixa_e_bloqueada(comprovante):
    """Fail closed: sem saber a conta, fica pendente em vez de chutar."""
    from app.apps.baixabradesco.core import _decidir_execucao

    plano = plano_para(comprovante, banco=None)
    _decidir_execucao(plano, executar_omie=True, atualizar_pipefy=True,
                      atualizar_spsbd=True, enviar_whatsapp=False)

    assert plano.pode_executar is False
    assert plano.acao == 'pendente_validacao'
    assert any('Somapay' in m for m in plano.motivos_bloqueio)


def test_a_planilha_registra_a_conta_somapay_como_conta_de_pagamento(comprovante):
    """Coluna AK da SPsBD: quem pagou foi a Somapay, e é isso que deve constar."""
    from app.apps.baixabradesco.sheets import build_spsbd_updates

    somapay = conta('Somapay BWS', '11.222.333/0001-44', '586876091')
    atualizacoes = build_spsbd_updates(plano_para(comprovante, somapay))

    assert atualizacoes[0]['updates']['AK'] == '22005-1'
    assert atualizacoes[0]['updates']['O'] == 'Pago'
    assert atualizacoes[0]['updates']['X'] == '11/09/2026'
