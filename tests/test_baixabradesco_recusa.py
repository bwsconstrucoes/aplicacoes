# -*- coding: utf-8 -*-
"""Comprovante que o banco NÃO efetivou nunca pode virar baixa.

Este é o teste que segura dinheiro: um comprovante recusado traz valor, data,
conta e código de barras exatamente como um comprovante bom. Se ele passar pelo
leitor, o casador acha a SP de verdade, o Omie baixa o título e a planilha marca
como pago — um pagamento que nunca saiu do banco.

O caso do arquivo `exemplos_baixabradesco/bradesco_boleto_transacao_nao_realizada.txt`
é real (de 16/06/2026), com conta, CNPJ, nomes e código de barras trocados por
fictícios. Ele diz "Transação Não Realizada" — e não "Operação Não Realizada",
que era a única frase que o leitor conhecia até 04/09/2026.
"""
from pathlib import Path

import pytest

from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text, FRASES_RECUSA
from app.apps.baixabradesco.parser_sicredi import parse_sicredi_text

EXEMPLOS = Path(__file__).parent / 'exemplos_baixabradesco'


def ler_exemplo(nome: str) -> str:
    return (EXEMPLOS / nome).read_text(encoding='utf-8')


# ── O comprovante real recusado pelo banco ────────────────────────────────────

def test_boleto_transacao_nao_realizada_e_recusado():
    texto = ler_exemplo('bradesco_boleto_transacao_nao_realizada.txt')
    r = parse_bradesco_text('exemplo.pdf', 1, texto)
    assert r.tipo_comprovante == 'operacao_nao_realizada'


def test_recusado_nao_entrega_valor_nem_codigo_de_barras():
    """Sem valor e sem código de barras, o casador não tem por onde achar SP."""
    texto = ler_exemplo('bradesco_boleto_transacao_nao_realizada.txt')
    r = parse_bradesco_text('exemplo.pdf', 1, texto)
    assert r.valor_pago == ''
    assert r.codigo_barras == ''
    assert r.id_pipefy == ''
    assert r.data_pagamento == ''


def test_recusado_diz_o_motivo():
    texto = ler_exemplo('bradesco_boleto_transacao_nao_realizada.txt')
    r = parse_bradesco_text('exemplo.pdf', 1, texto)
    assert any('recusado' in p.lower() for p in r.pendencias)


# ── As outras redações que o banco usa para a mesma coisa ─────────────────────

REDACOES_RECUSA = [
    'Operação Não Realizada\nPix\nValor: R$ 1.000,00\nData: 01/09/2026',
    'Transação Não Realizada\nBoletos de Cobrança\nValor: R$ 1.000,00',
    'Pagamento não efetivado\nValor: R$ 1.000,00',
    'Essa transação está pendente de aprovação e ainda não foi efetuada.',
    'Transação cancelada pelo emissor.\nValor: R$ 1.000,00',
    'OPERAÇÃO NÃO REALIZADA',
    'operacao nao realizada',  # sem acento, como alguns PDFs saem
]


@pytest.mark.parametrize('texto', REDACOES_RECUSA)
def test_todas_as_redacoes_de_recusa_sao_barradas(texto):
    r = parse_bradesco_text('exemplo.pdf', 1, texto)
    assert r.tipo_comprovante == 'operacao_nao_realizada'
    assert r.valor_pago == ''


def test_sicredi_usa_a_mesma_trava():
    texto = ('Sicredi\nComprovante\nTransação não realizada\n'
             'Cooperativa e conta origem: 2205/92945-8\nValor: R$ 500,00')
    r = parse_sicredi_text('exemplo.pdf', 1, texto)
    assert r.tipo_comprovante == 'operacao_nao_realizada'
    assert r.valor_pago == ''


def test_as_frases_de_recusa_estao_normalizadas():
    """A comparação é feita contra texto sem acento e em minúsculas.

    Uma frase com acento na lista nunca casaria — e o comprovante passaria.
    """
    for frase in FRASES_RECUSA:
        assert frase == frase.lower()
        assert all(ord(ch) < 128 for ch in frase), frase


# ── O contrário: comprovante bom não pode ser barrado por engano ──────────────

def test_comprovante_bom_continua_passando():
    texto = (
        'Comprovante de Pagamento\n'
        'Boletos de Cobrança\n'
        'Data do pagamento: 16/06/2026\n'
        'Conta de débito: Agência: 0999 | Conta: 0001234-5\n'
        'Código de barras: 34191 11111 22222 333333 44444 555555 6 77770000217125\n'
        'Descrição: 1234567\n'
        'Valor total: R$ 2.171,25\n'
    )
    r = parse_bradesco_text('exemplo.pdf', 1, texto)
    assert r.tipo_comprovante == 'boleto'
    assert r.valor_pago == '2.171,25'
    assert r.id_pipefy == '1234567'


def test_a_palavra_cancelamentos_do_rodape_nao_barra_comprovante():
    """O rodapé de todo comprovante Bradesco tem 'Cancelamentos, Reclamações'.

    Se a lista de recusa tivesse a palavra solta 'cancelado', TODO comprovante
    do banco seria barrado. Este teste existe para segurar isso.
    """
    texto = (
        'Comprovante de Pagamento\n'
        'Data do pagamento: 16/06/2026\n'
        'Valor total: R$ 100,00\n'
        'SAC - Serviço de Alô Bradesco Cancelamentos, Reclamações e Informações.\n'
    )
    r = parse_bradesco_text('exemplo.pdf', 1, texto)
    assert r.tipo_comprovante != 'operacao_nao_realizada'
    assert r.valor_pago == '100,00'
