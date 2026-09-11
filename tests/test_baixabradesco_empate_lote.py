# -*- coding: utf-8 -*-
"""Comprovantes iguais para SPs iguais: distribuir, em vez de travar tudo.

O caso real, 11/09/2026: duas rescisões de R$ 5.532,57, de duas pessoas, ambas
agendadas — e um PDF com dois comprovantes de transferência de R$ 5.532,57. Um a
um, cada comprovante via duas SPs possíveis e parava como pendente. Mas olhando
o lote inteiro são **dois pagamentos para duas SPs**: dá para baixar as duas.

Decisão do dono: não importa qual comprovante fica com qual SP — os papéis são
intercambiáveis (mesmo valor, mesma data, mesma conta, e o da transferência nem
traz o nome do funcionário). O que importa é baixar.

O perigo que isso abre, e por isso as travas: se os "dois comprovantes" forem na
verdade o MESMO comprovante mandado duas vezes, distribuir baixaria duas SPs
para um pagamento só.
"""
from pathlib import Path

import pytest

from app.apps.baixabradesco.core import resolver_empates_do_lote
from app.apps.baixabradesco.models import ExtractedReceipt, MatchResult, SpRecord
from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text

EXEMPLOS = Path(__file__).parent / 'exemplos_baixabradesco'
VALOR = '5.532,57'


def sp(sp_id, valor=VALOR):
    return SpRecord(row_number=1, id=sp_id, valor_total=valor,
                    nome_credor='BWS CONSTRUÇÕES LTDA',
                    raw={'Tipo de Despesa': 'Rescisões e Indenizações Trabalhistas'})


def pendente(pagina, identificador, candidatos, valor=VALOR):
    rec = ExtractedReceipt(filename='comprovantes.pdf', page=pagina,
                           valor_pago=valor, identificador=identificador)
    return {'page_num': pagina, 'rec': rec,
            'match': MatchResult(status='pendente_validacao',
                                 metodo='somapay_valor_tipo_despesa',
                                 candidatos=candidatos)}


# ── O caso que motivou ────────────────────────────────────────────────────────

def test_dois_comprovantes_para_duas_sps_baixam_os_dois():
    candidatos = [sp('1442670864'), sp('1442703969')]
    lote = [pendente(1, 'IDENT-AAA', candidatos),
            pendente(2, 'IDENT-BBB', candidatos)]

    assert resolver_empates_do_lote(lote) == 2
    assert [i['match'].status for i in lote] == ['localizado', 'localizado']
    assert sorted(i['match'].id for i in lote) == ['1442670864', '1442703969']


def test_cada_sp_fica_com_um_comprovante_so():
    """Duas baixas na mesma SP seriam pagamento em dobro."""
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, 'A', candidatos), pendente(2, 'B', candidatos)]
    resolver_empates_do_lote(lote)
    ids = [i['match'].id for i in lote]
    assert len(set(ids)) == len(ids)


def test_tres_para_tres_tambem_funciona():
    candidatos = [sp('7001'), sp('7002'), sp('7003')]
    lote = [pendente(1, 'A', candidatos), pendente(2, 'B', candidatos),
            pendente(3, 'C', candidatos)]
    assert resolver_empates_do_lote(lote) == 3
    assert sorted(i['match'].id for i in lote) == ['7001', '7002', '7003']


def test_o_motivo_explica_o_que_aconteceu():
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, 'A', candidatos), pendente(2, 'B', candidatos)]
    resolver_empates_do_lote(lote)
    assert '2 comprovantes distintos para 2 SPs' in lote[0]['match'].motivo
    assert 'distribuido_no_lote' in lote[0]['match'].metodo


# ── As travas ─────────────────────────────────────────────────────────────────

def test_o_mesmo_comprovante_mandado_duas_vezes_nao_distribui():
    """Identificador repetido = um pagamento só. Distribuir baixaria duas SPs."""
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, 'MESMO-IDENT', candidatos),
            pendente(2, 'MESMO-IDENT', candidatos)]

    assert resolver_empates_do_lote(lote) == 0
    assert all(i['match'].status == 'pendente_validacao' for i in lote)


def test_comprovante_sem_identificador_nao_distribui():
    """Sem como provar que são pagamentos diferentes, não se arrisca."""
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, '', candidatos), pendente(2, '', candidatos)]
    assert resolver_empates_do_lote(lote) == 0


def test_dois_comprovantes_para_tres_sps_nao_distribui():
    """Sobraria uma SP, e escolher qual seria chute: uma delas ficaria paga
    sem ter sido."""
    candidatos = [sp('7001'), sp('7002'), sp('7003')]
    lote = [pendente(1, 'A', candidatos), pendente(2, 'B', candidatos)]
    assert resolver_empates_do_lote(lote) == 0


def test_tres_comprovantes_para_duas_sps_nao_distribui():
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, 'A', candidatos), pendente(2, 'B', candidatos),
            pendente(3, 'C', candidatos)]
    assert resolver_empates_do_lote(lote) == 0


def test_grupos_de_valores_diferentes_nao_se_misturam():
    """Comprovante de R$ 5.532,57 não pode cair numa SP de R$ 8.128,17."""
    candidatos_a = [sp('7001'), sp('7002')]
    candidatos_b = [sp('8001', '8.128,17'), sp('8002', '8.128,17')]
    lote = [pendente(1, 'A', candidatos_a), pendente(2, 'B', candidatos_a),
            pendente(3, 'C', candidatos_b, valor='8.128,17'),
            pendente(4, 'D', candidatos_b, valor='8.128,17')]

    assert resolver_empates_do_lote(lote) == 4
    assert sorted(i['match'].id for i in lote[:2]) == ['7001', '7002']
    assert sorted(i['match'].id for i in lote[2:]) == ['8001', '8002']


def test_comprovante_ja_localizado_nao_e_mexido():
    ja = {'page_num': 1, 'rec': ExtractedReceipt(filename='x.pdf', page=1),
          'match': MatchResult(status='localizado', id='9999', sp=sp('9999'))}
    assert resolver_empates_do_lote([ja]) == 0
    assert ja['match'].id == '9999'


def test_comprovante_sem_candidato_nenhum_continua_sem():
    sozinho = {'page_num': 1, 'rec': ExtractedReceipt(filename='x.pdf', page=1),
               'match': MatchResult(status='nao_localizado')}
    assert resolver_empates_do_lote([sozinho]) == 0
    assert sozinho['match'].status == 'nao_localizado'


def test_um_comprovante_para_duas_sps_continua_pendente():
    """Sem par, não há o que distribuir — e é o caso mais comum de conferência."""
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, 'A', candidatos)]
    assert resolver_empates_do_lote(lote) == 0
    assert lote[0]['match'].status == 'pendente_validacao'


# ── O emparelhamento é estável ────────────────────────────────────────────────

def test_o_mesmo_lote_reprocessado_da_o_mesmo_resultado():
    """Se o Make reenviar o lote, a distribuição tem de ser idêntica — senão a
    mesma SP pode receber comprovantes diferentes em execuções diferentes."""
    def monta():
        candidatos = [sp('7002'), sp('7001')]   # de propósito fora de ordem
        return [pendente(2, 'B', candidatos), pendente(1, 'A', candidatos)]

    primeiro, segundo = monta(), monta()
    resolver_empates_do_lote(primeiro)
    resolver_empates_do_lote(segundo)

    por_pagina = lambda lote: {i['page_num']: i['match'].id for i in lote}
    assert por_pagina(primeiro) == por_pagina(segundo)
    assert por_pagina(primeiro) == {1: '7001', 2: '7002'}


# ── O identificador sai do comprovante de verdade ─────────────────────────────

def test_o_identificador_vem_do_comprovante():
    texto = (EXEMPLOS / 'bradesco_transferencia_somapay.txt').read_text(encoding='utf-8')
    rec = parse_bradesco_text('transferencia.pdf', 1, texto)
    assert rec.identificador == 'E00000000202609111744I0624AAAAAA'


def test_o_numero_de_controle_nao_serve_de_identificador_sozinho():
    """Ele é do lote inteiro e se repete entre as páginas. Se fosse usado, dois
    comprovantes do mesmo lote pareceriam o mesmo pagamento — e, pior, dois
    pagamentos diferentes seriam recusados."""
    texto = (EXEMPLOS / 'bradesco_transferencia_somapay.txt').read_text(encoding='utf-8')
    rec = parse_bradesco_text('transferencia.pdf', 1, texto)
    assert rec.identificador != '111222333444555666'


# ── Quando não dá para distribuir, o aviso explica por quê ────────────────────
#
# O motivo que vem do casador é técnico ("retornou 2 candidatos"). Quem recebe o
# aviso no WhatsApp precisa saber o que aconteceu e o que olhar.

def test_identificador_repetido_explica_no_motivo():
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, 'MESMO', candidatos), pendente(2, 'MESMO', candidatos)]
    resolver_empates_do_lote(lote)

    motivo = lote[0]['match'].motivo
    assert 'MESMO pagamento' in motivo
    assert 'baixaria mais de uma SP' in motivo
    assert 'candidatos' not in motivo.lower()


def test_quantidades_diferentes_explicam_no_motivo():
    candidatos = [sp('7001'), sp('7002'), sp('7003')]
    lote = [pendente(1, 'A', candidatos), pendente(2, 'B', candidatos)]
    resolver_empates_do_lote(lote)

    motivo = lote[0]['match'].motivo
    assert '2 comprovante(s) de mesmo valor para 3 SPs' in motivo
    assert 'paga sem ter sido' in motivo


def test_explicar_o_motivo_nao_muda_o_status():
    """Explicar melhor não pode virar autorização para baixar."""
    candidatos = [sp('7001'), sp('7002')]
    lote = [pendente(1, 'MESMO', candidatos), pendente(2, 'MESMO', candidatos)]
    resolver_empates_do_lote(lote)

    for item in lote:
        assert item['match'].status == 'pendente_validacao'
        assert not item['match'].id
