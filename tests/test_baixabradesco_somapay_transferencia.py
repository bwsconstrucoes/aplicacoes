# -*- coding: utf-8 -*-
"""Transferência do Bradesco para a Somapay: transferir e depois baixar.

Este é o outro caminho da Somapay. O comprovante é emitido pelo **Bradesco** e
prova que o dinheiro saiu de uma conta da empresa e entrou na conta Somapay. O
Omie precisa receber as duas coisas, nesta ordem: a transferência entre as
contas, e só depois a baixa do título — na conta **Somapay**, não na do
Bradesco.

O que estava errado antes de 11/09/2026, e valia em produção: o comprovante era
lido como um Pix comum, casava com a SP de rescisão e mandava baixar o título na
conta do **Bradesco**, sem transferência nenhuma. O saldo da Somapay no Omie
nunca receberia o dinheiro.

O comprovante de exemplo é real (11/09/2026), com conta, CNPJ e identificadores
trocados por fictícios. A chave PIX foi mantida porque é ela que o robô usa para
saber qual conta Somapay recebeu — e ela também está trocada, casando com a
BaseBancos fictícia dos testes.
"""
from pathlib import Path

import pytest

from app.apps.baixabradesco.matcher import match_receipt
from app.apps.baixabradesco.models import BankAccount, ExecutionPlan, MatchResult, SpRecord
from app.apps.baixabradesco.omie import build_somapay_plan
from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text
from app.apps.baixabradesco.sheets import find_account_by_pix_key, build_spsbd_updates

EXEMPLOS = Path(__file__).parent / 'exemplos_baixabradesco'
CHAVE_SOMAPAY_BWS = 'aaaa1111-bbbb-2222-cccc-333344445555'
OMIE_BRADESCO = '900000001'
OMIE_SOMAPAY_BWS = '900000002'


@pytest.fixture
def comprovante():
    texto = (EXEMPLOS / 'bradesco_transferencia_somapay.txt').read_text(encoding='utf-8')
    return parse_bradesco_text('transferencia.pdf', 1, texto)


def conta(banco, numero, codigo_omie, chave=''):
    return BankAccount(row_number=2, banco=banco, conta=numero, agencia='0624',
                       codigo_omie=codigo_omie, descricao=f'{banco} - {numero}',
                       raw={'Chave PIX': chave})


def base_bancos():
    return [
        conta('Bradesco', '50024-0', OMIE_BRADESCO, '3106d248-0000-1111-2222-333344445555'),
        conta('Somapay BWS', '22005-1', OMIE_SOMAPAY_BWS, CHAVE_SOMAPAY_BWS),
        conta('Somapay INFRADENDE', '9452-8', '900000003', '06e2e8ba-9999-8888-7777-666655554444'),
        conta('Somapay IFPESANTACRUZ', '908146-1', '900000004', 'c757202e-1234-5678-9012-345678901234'),
    ]


def sp_rescisao(**kwargs):
    """A SP de rescisão no formato real da SPsBD (conferido em 11/09/2026)."""
    base = dict(
        row_number=10, id='1442630306',
        nome_credor='BWS CONSTRUÇÕES LTDA', cpf_cnpj='00.079.526/0001-09',
        descricao='Conta Origem: 50024-0  TRCT FUNCIONARIO EXEMPLO',
        valor_total='8.128,17', tipo_pagamento='Pix', conta_pagamento='50024-0',
        status_pgt='Pagar', status_agendamento='agendado',
        raw={'Tipo de Despesa': 'Rescisões e Indenizações Trabalhistas'},
    )
    base.update(kwargs)
    return SpRecord(**base)


# ── A leitura do comprovante ──────────────────────────────────────────────────

def test_reconhece_a_transferencia_para_a_somapay(comprovante):
    assert comprovante.tipo_comprovante == 'somapay'


def test_le_a_chave_pix_de_quem_recebeu(comprovante):
    """É o identificador exato da conta Somapay que recebeu."""
    assert comprovante.chave_pix_destino == CHAVE_SOMAPAY_BWS


def test_le_valor_data_e_conta_debitada(comprovante):
    assert comprovante.valor_pago == '8.128,17'
    assert comprovante.data_pagamento == '11/09/2026'
    assert comprovante.conta_origem == '50024-0'


def test_a_transferencia_nao_traz_numero_de_sp(comprovante):
    """A descrição é o nome do arquivo de remessa, não a SP."""
    assert comprovante.id_pipefy == ''


# ── Qual conta Somapay recebeu ────────────────────────────────────────────────

def test_a_chave_pix_diz_qual_das_tres_contas_somapay(comprovante):
    achada = find_account_by_pix_key(base_bancos(), comprovante.chave_pix_destino)
    assert achada is not None
    assert achada.banco == 'Somapay BWS'
    assert achada.codigo_omie == OMIE_SOMAPAY_BWS


def test_chave_desconhecida_nao_resolve_conta():
    """Fail closed: sem conta resolvida o comprovante fica pendente."""
    assert find_account_by_pix_key(base_bancos(), 'chave-que-nao-existe') is None


def test_sem_chave_no_comprovante_nao_resolve_conta():
    assert find_account_by_pix_key(base_bancos(), '') is None


# ── O casamento com a SP ──────────────────────────────────────────────────────

def test_casa_com_a_rescisao_pelo_valor_e_tipo_de_despesa(comprovante):
    resultado = match_receipt(comprovante, {'1442630306': sp_rescisao()}, [])
    assert resultado.status == 'localizado'
    assert resultado.id == '1442630306'


def test_despesa_que_nao_e_de_folha_nao_casa(comprovante):
    """A transferência para a Somapay só paga folha: rescisão, férias,
    gratificação e participação nos lucros."""
    outra = sp_rescisao(raw={'Tipo de Despesa': 'Material de Construção'})
    resultado = match_receipt(comprovante, {'1442630306': outra}, [])
    assert resultado.status != 'localizado'


def test_duas_rescisoes_do_mesmo_valor_ficam_pendentes(comprovante):
    """O comprovante da transferência NÃO traz o nome do funcionário — só valor,
    data e conta. Duas rescisões pendentes de mesmo valor na mesma conta não têm
    como ser distinguidas, e aí ninguém baixa nada.

    Aconteceu de verdade: em 09/09/2026 havia quatro rescisões de R$ 452,40.
    """
    duas = {
        '1442630306': sp_rescisao(id='1442630306'),
        '1442630307': sp_rescisao(id='1442630307'),
    }
    resultado = match_receipt(comprovante, duas, [])
    assert resultado.status == 'pendente_validacao'


def test_desempate_pela_conta_que_foi_debitada(comprovante):
    """Mesmo valor, contas de origem diferentes: fica a que o comprovante pagou."""
    duas = {
        '1442630306': sp_rescisao(id='1442630306', conta_pagamento='50024-0'),
        '1442630307': sp_rescisao(id='1442630307', conta_pagamento='7011-4'),
    }
    resultado = match_receipt(comprovante, duas, [])
    assert resultado.status == 'localizado'
    assert resultado.id == '1442630306'


# ── A sequência no Omie: transferir, e só então baixar ────────────────────────

PADRAO = object()   # distingue "não informado" de "informado como None"


def plano(comprovante, banco=PADRAO, banco_destino=PADRAO, sp=None):
    registro = sp if sp is not None else sp_rescisao()
    return ExecutionPlan(
        receipt=comprovante,
        match=MatchResult(status='localizado', metodo='somapay_valor_tipo_despesa',
                          id=registro.id, sp=registro),
        banco=conta('Bradesco', '50024-0', OMIE_BRADESCO) if banco is PADRAO else banco,
        banco_destino=(conta('Somapay BWS', '22005-1', OMIE_SOMAPAY_BWS, CHAVE_SOMAPAY_BWS)
                       if banco_destino is PADRAO else banco_destino),
    )


def test_a_transferencia_vem_antes_da_baixa(comprovante):
    passos = build_somapay_plan(plano(comprovante), {})
    assert [p['step'] for p in passos] == [
        'transferencia_somapay', 'consultar', 'alterar_se_necessario', 'baixar']
    assert passos[0].get('lanccc') is True


def test_a_baixa_acontece_na_conta_somapay_e_nao_na_do_bradesco(comprovante):
    """O erro que estava valendo em produção: baixar na conta do Bradesco."""
    passos = build_somapay_plan(plano(comprovante), {})
    baixa = [p for p in passos if p['step'] == 'baixar'][0]['request']['param'][0]
    assert baixa['codigo_conta_corrente'] == OMIE_SOMAPAY_BWS
    assert baixa['codigo_conta_corrente'] != OMIE_BRADESCO
    assert baixa['valor'] == '8128.17'


def test_a_alteracao_do_titulo_tambem_aponta_para_a_somapay(comprovante):
    passos = build_somapay_plan(plano(comprovante), {})
    alterar = [p for p in passos if p['step'] == 'alterar_se_necessario'][0]['request']['param'][0]
    assert alterar['id_conta_corrente'] == OMIE_SOMAPAY_BWS


def test_a_transferencia_liga_as_duas_contas_certas(comprovante):
    """⚠️ A semântica do Omie é invertida em relação ao nome dos campos, e isso
    foi validado em produção: o campo do cabeçalho recebe a conta que RECEBE
    (Somapay) e o de destino recebe a conta que PAGA (Bradesco). Não "corrigir".
    """
    passos = build_somapay_plan(plano(comprovante), {})
    transferencia = passos[0]['request']['param'][0]
    assert transferencia['cabecalho']['nCodCC'] == OMIE_SOMAPAY_BWS
    assert transferencia['transferencia']['nCodCCDestino'] == OMIE_BRADESCO
    assert transferencia['cabecalho']['nValorLanc'] == '8128.17'
    assert transferencia['detalhes']['cTipo'] == 'TRA'


def test_o_codigo_da_transferencia_cabe_no_limite_do_omie(comprovante):
    """O Omie corta em 20 caracteres — e ID de SP tem 10 dígitos."""
    transferencia = build_somapay_plan(plano(comprovante), {})[0]['request']['param'][0]
    assert len(transferencia['cCodIntLanc']) <= 20
    assert transferencia['cCodIntLanc'].startswith('CCS')


def test_sem_conta_somapay_resolvida_nao_executa(comprovante):
    """Fail closed: chave PIX que a BaseBancos não conhece trava a execução."""
    from app.apps.baixabradesco.core import _decidir_execucao

    p = plano(comprovante, banco_destino=None)
    _decidir_execucao(p, executar_omie=True, atualizar_pipefy=True,
                      atualizar_spsbd=True, enviar_whatsapp=False)
    assert p.pode_executar is False
    assert any('Somapay' in m for m in p.motivos_bloqueio)


def test_a_planilha_registra_a_conta_do_bradesco(comprovante):
    """Coluna AK: o dinheiro saiu do Bradesco, e é isso que fica registrado —
    a Somapay foi só o caminho."""
    atualizacoes = build_spsbd_updates(plano(comprovante))
    assert atualizacoes[0]['updates']['AK'] == '50024-0'
    assert atualizacoes[0]['updates']['O'] == 'Pago'


# ── Os dois comprovantes Somapay continuam separados ──────────────────────────

def test_o_deposito_da_somapay_nao_vira_transferencia():
    """O papel emitido pela Somapay prova o pagamento ao funcionário, não a
    movimentação entre contas. Se virasse transferência, o Omie receberia um
    dinheiro andando que não andou."""
    texto = (EXEMPLOS / 'somapay_deposito_rescisorio.txt').read_text(encoding='utf-8')
    r = parse_bradesco_text('deposito.pdf', 1, texto)
    assert r.tipo_comprovante == 'somapay_deposito'


def test_a_transferencia_nao_vira_deposito(comprovante):
    assert comprovante.tipo_comprovante == 'somapay'
