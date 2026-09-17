# -*- coding: utf-8 -*-
"""Comprovante do Sicredi: ler com o leitor certo.

O leitor do Sicredi existia desde sempre, completo, e **nunca era chamado** — o
robô mandava toda página para o leitor do Bradesco. Em 11/09/2026 isso ficou
assim por um mal-entendido meu: o dono disse que não usava as outras duas contas
**Somapay**, e eu entendi Sicredi.

O custo apareceu em 13/09/2026, num comprovante real de R$ 10.861,20: o leitor do
Bradesco saía **sem valor, sem conta e sem número de SP**, e a baixa não
acontecia. O Sicredi escreve o valor como "Valor Pago (R$): 10.861,20" — com o
"(R$)" no meio — e põe o número da SP em "Descrição do Pagamento".

O exemplo é esse comprovante real, com empresa, fornecedor, CNPJ, conta, código
de barras e autenticação trocados por fictícios.
"""
from pathlib import Path

import pytest

from app.apps.baixabradesco.matcher import match_receipt
from app.apps.baixabradesco.models import SpRecord
from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text
from app.apps.baixabradesco.parser_sicredi import is_sicredi, parse_sicredi_text

EXEMPLOS = Path(__file__).parent / 'exemplos_baixabradesco'


@pytest.fixture
def texto():
    return (EXEMPLOS / 'sicredi_boleto.txt').read_text(encoding='utf-8')


@pytest.fixture
def comprovante(texto):
    return parse_sicredi_text('sicredi.pdf', 1, texto)


# ── O desvio para o leitor certo ──────────────────────────────────────────────

def test_reconhece_o_comprovante_do_sicredi(texto):
    assert is_sicredi(texto) is True


def test_comprovante_do_bradesco_nao_vai_para_o_leitor_do_sicredi():
    """A palavra "sicredi" aparece em comprovante do Bradesco quando o destino é
    uma conta Sicredi. Ler com o leitor errado pode sair com valor errado — pior
    do que não reconhecer."""
    bradesco = (
        'Comprovante de Transação Bancária\nPIX\n'
        'Conta de débito: Agência: 0624 | Conta: 0050024-0\n'
        'Instituição destino: SICREDI\n'
        'Valor: R$ 100,00\nData da operação: 11/09/2026\n'
    )
    assert is_sicredi(bradesco) is False


def test_o_fluxo_escolhe_o_leitor_pelo_formato_do_papel():
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core.processar_baixabradesco)
    assert 'parse_sicredi_text if is_sicredi(text) else parse_bradesco_text' in fonte


def test_o_diagnostico_le_do_mesmo_jeito_que_o_fluxo():
    """Diagnóstico que lê diferente do fluxo real mente para quem investiga."""
    import inspect
    from app.apps.baixabradesco import diagnostico
    fonte = inspect.getsource(diagnostico)
    assert 'parse_sicredi_text if is_sicredi(txt) else parse_bradesco_text' in fonte


# ── O que o leitor certo enxerga e o errado não ───────────────────────────────

def test_o_leitor_do_bradesco_nao_enxerga_esse_comprovante(texto):
    """Registra o defeito que causou a falha real de 13/09/2026."""
    r = parse_bradesco_text('sicredi.pdf', 1, texto)
    assert r.valor_pago == ''
    assert r.conta_origem == ''
    assert r.id_pipefy == ''


def test_le_o_valor_pago(comprovante):
    assert comprovante.valor_pago == '10.861,20'


def test_le_a_data_do_pagamento(comprovante):
    assert comprovante.data_pagamento == '08/09/2026'


def test_le_a_cooperativa_e_a_conta_de_origem(comprovante):
    assert comprovante.agencia_origem == '02205'
    assert comprovante.conta_origem == '99999-9'


def test_le_o_numero_da_sp(comprovante):
    """Vem em "Descrição do Pagamento" — é o casamento mais confiável que existe,
    não depende de valor nem de conta."""
    assert comprovante.id_pipefy == '1432850275'


def test_le_o_codigo_de_barras(comprovante):
    assert comprovante.codigo_barras.startswith('34191111122222')
    assert len(comprovante.codigo_barras) >= 44


def test_reconhece_como_boleto(comprovante):
    assert comprovante.tipo_comprovante == 'boleto'


# ── E com isso a SP é encontrada ──────────────────────────────────────────────

def test_casa_com_a_sp_pelo_numero_do_comprovante(comprovante):
    sp = SpRecord(row_number=2, id='1432850275', valor_total='10.861,20',
                  nome_credor='FORNECEDOR EXEMPLO CIMENTOS S.A.',
                  status_pgt='Pagar', status_agendamento='agendado')
    resultado = match_receipt(comprovante, {'1432850275': sp}, [])
    assert resultado.status == 'localizado'
    assert resultado.id == '1432850275'
    assert resultado.metodo == 'id_comprovante'


def test_comprovante_recusado_do_sicredi_continua_barrado():
    """A trava de "não efetivado" tem de valer nos dois leitores."""
    texto = ('Associado: EMPRESA EXEMPLO Cooperativa: 2205\n'
             'Cooperativa Origem: 02205 Conta Origem: 99999-9\n'
             'Transação não realizada\nValor Pago (R$): 500,00\n')
    r = parse_sicredi_text('sicredi.pdf', 1, texto)
    assert r.tipo_comprovante == 'operacao_nao_realizada'
    assert r.valor_pago == ''


# ── Dois Pix do Sicredi, mesmo valor, pagamentos diferentes ───────────────────
#
# Caso real, 17/09/2026: dois Pix de R$ 7.300,00 para duas SPs de mesmo valor,
# as duas agendadas. O desempate por lote se recusou a distribuir, dizendo que
# "parecem ser o MESMO pagamento (identificador repetido ou ausente)".
#
# Estava ausente, e a culpa era de dois descuidos somados:
#   1. o leitor do Sicredi nunca preenchia o identificador — quando o liguei, em
#      13/09, esqueci de levar esse campo junto;
#   2. mesmo se preenchesse, os rótulos do Sicredi ("ID da transação",
#      "Autenticação Eletrônica", "Número de Controle") não estavam na lista de
#      onde procurar — ela conhecia só os do Bradesco.

def le(nome):
    texto = (EXEMPLOS / nome).read_text(encoding='utf-8')
    return parse_sicredi_text(nome, 1, texto)


def test_o_leitor_do_sicredi_preenche_o_identificador():
    assert le('sicredi_pix_a.txt').identificador


def test_dois_pix_diferentes_tem_identificadores_diferentes():
    a, b = le('sicredi_pix_a.txt'), le('sicredi_pix_b.txt')
    assert a.valor_pago == b.valor_pago == '7.300,00'
    assert a.identificador != b.identificador


def test_prefere_o_id_da_transacao(): 
    """É o identificador canônico do Pix — melhor que autenticação ou controle."""
    assert le('sicredi_pix_a.txt').identificador.startswith('E00000000000000000000')


@pytest.mark.parametrize('rotulo,esperado', [
    ('ID da transação: E4118009220260917112335INPrdDqZr', 'E4118009220260917112335INPrdDqZr'),
    ('Autenticação Eletrônica: E411.8009.2202.6091.7112.335I.NPrd.DqZr',
     'E411.8009.2202.6091.7112.335I.NPrd.DqZr'),
    ('Número de Controle: 14807610737', '14807610737'),
])
def test_conhece_os_rotulos_do_sicredi(rotulo, esperado):
    from app.apps.baixabradesco.parser_bradesco import extract_identificador
    assert extract_identificador(rotulo) == esperado


def test_os_dois_sao_distribuidos_entre_as_duas_sps():
    """Ponta a ponta com os dois comprovantes reais e as duas SPs do dono."""
    from app.apps.baixabradesco.core import resolver_empates_do_lote
    from app.apps.baixabradesco.models import MatchResult

    candidatas = [
        SpRecord(row_number=1, id='1445859706', valor_total='7.300,00',
                 status_pgt='Pagar', status_agendamento='agendado'),
        SpRecord(row_number=2, id='1445866267', valor_total='7.300,00',
                 status_pgt='Pagar', status_agendamento='agendado'),
    ]
    lote = [
        {'page_num': 1, 'rec': le('sicredi_pix_a.txt'),
         'match': MatchResult(status='pendente_validacao', metodo='valor_conta_agendado',
                              candidatos=candidatas)},
        {'page_num': 2, 'rec': le('sicredi_pix_b.txt'),
         'match': MatchResult(status='pendente_validacao', metodo='valor_conta_agendado',
                              candidatos=candidatas)},
    ]

    assert resolver_empates_do_lote(lote) == 2
    assert sorted(i['match'].id for i in lote) == ['1445859706', '1445866267']
