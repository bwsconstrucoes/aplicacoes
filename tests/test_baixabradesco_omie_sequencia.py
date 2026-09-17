# -*- coding: utf-8 -*-
"""A sequência do Omie: consultar → alterar (se precisar) → baixar.

⚠️ ESTE ARQUIVO NASCEU DE UM DEFEITO QUE CUSTOU DOIS DIAS AO DONO — 16/09/2026.

Dois comprovantes do Sicredi, enviados pela tela do Análise de SPs, voltaram
assim:

    Falha ao alterar título. Baixa cancelada.
    O Omie respondeu: A chave de acesso não está preenchida ou não é válida.

E a pergunta que ele fez é a que destrava tudo: *"só não compreendo por que o
baixabradesco no método anterior funciona e via Análise não."*

O robô é o MESMO. O que muda é de onde vem a chave de acesso do Omie: o Make
manda `app_key` e `app_secret` dentro do pedido; o pedido que sai do Análise de
SPs conta com as variáveis de ambiente do servidor. Faltando elas, o pedido sai
com a chave vazia — e "chave de acesso", no Omie, é a CREDENCIAL DA API, não a
chave do título. A mensagem parecia falar do título, e a investigação olhou
para o lado errado.

Três regras ficam cravadas aqui:

  1. sem credencial, não se manda nada;
  2. consulta que não deu certo interrompe — não se altera nem se paga um
     título que ninguém confirmou;
  3. a alteração só acontece se algo diverge de verdade.
"""
from __future__ import annotations

import pytest

from app.apps.baixabradesco import core
from app.apps.baixabradesco.models import (BankAccount, ExecutionPlan,
                                           ExtractedReceipt, MatchResult,
                                           SpRecord)

CREDENCIAIS = {'omie': {'app_key': 'chave', 'app_secret': 'segredo'}}


def _plano(valor='925,83', conta_omie='4243157171'):
    recibo = ExtractedReceipt(
        filename='sicredi.pdf', page=1, tipo_comprovante='pix',
        valor_pago=valor, data_pagamento='16/09/2026',
        nome_recebedor='HUMBERTO JOSE DA SILVA GOMES')
    sp = SpRecord(row_number=2, id='1440028616',
                  codigo_integracao_omie='Int1440028616')
    plano = ExecutionPlan(receipt=recibo,
                          match=MatchResult(status='localizado',
                                            id='1440028616', sp=sp),
                          banco=BankAccount(row_number=2,
                                            codigo_omie=conta_omie))
    from app.apps.baixabradesco.omie import build_omie_plan
    plano.omie_requests = build_omie_plan(plano, CREDENCIAIS)
    plano.pode_executar = True
    return plano


def _respostas(monkeypatch, por_step: dict, registro: list):
    """Dubla o Omie: cada chamada devolve o que o teste mandou, na ordem."""
    chamadas = iter(por_step)

    def falso(body):
        chamada = body.get('call')
        registro.append(chamada)
        return por_step[chamada]

    monkeypatch.setattr(core, 'execute_omie', falso)
    return chamadas


# ── 1. Sem credencial, não se manda nada ─────────────────────────────────────

def test_SEM_credencial_do_omie_nao_manda_nada(monkeypatch):
    """⚠️ Era este o caso do dono. E o pior não é falhar: é falhar depois de
    ter mandado uma alteração com a chave vazia."""
    mandou = []
    monkeypatch.setattr(core, 'execute_omie',
                        lambda body: mandou.append(body.get('call')) or {'ok': True})

    resultados = core._executar_sequencia_omie(_plano(), {})

    assert mandou == [], 'mandou pedido ao Omie sem credencial'
    assert resultados[0]['step'] == 'abort_sem_credencial'
    assert 'OMIE_BWS_APP_KEY' in resultados[0]['motivo'], (
        'a mensagem não diz qual variável falta — quem lê não sabe o que fazer')


def test_a_credencial_do_PEDIDO_vale_quando_o_servidor_nao_tem(monkeypatch):
    """É assim que o Make funciona hoje: ele manda a chave dentro do pedido."""
    monkeypatch.delenv('OMIE_BWS_APP_KEY', raising=False)
    monkeypatch.delenv('OMIE_BWS_APP_SECRET', raising=False)
    monkeypatch.setattr(core, 'execute_omie', lambda body: {'ok': True, 'body': {}})

    resultados = core._executar_sequencia_omie(_plano(), CREDENCIAIS)

    assert not any(r['step'] == 'abort_sem_credencial' for r in resultados)


# ── 2. Consulta que falhou interrompe tudo ───────────────────────────────────

def test_consulta_que_FALHOU_nao_deixa_alterar_nem_pagar(monkeypatch):
    """⚠️ O Omie responde 200 COM `faultstring` em vários casos. Antes só
    interrompia com HTTP 500 ou faultcode "nao_encontrado" — os outros passavam
    e iam alterar um título que ninguém confirmou que existe."""
    mandou = []

    def falso(body):
        mandou.append(body.get('call'))
        return {'ok': False, 'status': 200,
                'body': {'faultstring': 'A chave de acesso não está preenchida '
                                        'ou não é válida.'}}

    monkeypatch.setattr(core, 'execute_omie', falso)
    resultados = core._executar_sequencia_omie(_plano(), CREDENCIAIS)

    assert mandou == ['ConsultarContaPagar'], (
        f'mandou mais do que a consulta depois de ela falhar: {mandou}')
    parada = [r for r in resultados if r['step'] == 'abort']
    assert parada, 'não interrompeu'
    assert 'chave de acesso' in parada[0]['motivo'], (
        'a frase do Omie não chegou na mensagem')
    assert 'Nada foi alterado nem pago' in parada[0]['motivo']


# ── 3. A alteração só acontece se algo diverge ───────────────────────────────

def test_titulo_JA_CERTO_nao_e_alterado_e_a_baixa_segue(monkeypatch):
    """⚠️ O defeito de desenho que o dono cobrou: *"se eu estou mandando pra
    baixar é pra baixar"*.

    O passo de alterar rodava SEMPRE, apoiado num comentário que dizia "Omie
    idempotente" — suposição nunca verificada. Quando o Omie recusava a
    alteração, a BAIXA era cancelada: um passo que não precisava acontecer
    impedia o que precisava."""
    mandou = []

    def falso(body):
        chamada = body.get('call')
        mandou.append(chamada)
        if chamada == 'ConsultarContaPagar':
            return {'ok': True, 'status': 200,
                    'body': {'status_titulo': 'ABERTO',
                             'valor_documento': 925.83,
                             'id_conta_corrente': 4243157171}}
        return {'ok': True, 'status': 200, 'body': {}}

    monkeypatch.setattr(core, 'execute_omie', falso)
    resultados = core._executar_sequencia_omie(_plano(), CREDENCIAIS)

    assert 'AlterarContaPagar' not in mandou, (
        'alterou um título que já estava certo — e é isso que cancela a baixa')
    assert 'LancarPagamento' in mandou, 'a baixa não aconteceu'
    assert any(r['step'] == 'alterar_nao_precisou' for r in resultados)


def test_titulo_com_VALOR_DIFERENTE_e_alterado(monkeypatch):
    """Deixar de alterar quando PRECISA seria baixar com o valor errado — pior
    do que uma alteração a mais. Na dúvida, altera."""
    mandou = []

    def falso(body):
        chamada = body.get('call')
        mandou.append(chamada)
        if chamada == 'ConsultarContaPagar':
            return {'ok': True, 'status': 200,
                    'body': {'status_titulo': 'ABERTO',
                             'valor_documento': 100.00,
                             'id_conta_corrente': 4243157171}}
        return {'ok': True, 'status': 200, 'body': {}}

    monkeypatch.setattr(core, 'execute_omie', falso)
    core._executar_sequencia_omie(_plano(), CREDENCIAIS)

    assert 'AlterarContaPagar' in mandou


def test_consulta_INCOMPLETA_manda_alterar_como_antes(monkeypatch):
    """Se a consulta não trouxe os campos, o comportamento volta a ser o de
    antes. A regra nova nunca pode DEIXAR de alterar por não saber."""
    mandou = []

    def falso(body):
        chamada = body.get('call')
        mandou.append(chamada)
        if chamada == 'ConsultarContaPagar':
            return {'ok': True, 'status': 200, 'body': {'status_titulo': 'ABERTO'}}
        return {'ok': True, 'status': 200, 'body': {}}

    monkeypatch.setattr(core, 'execute_omie', falso)
    core._executar_sequencia_omie(_plano(), CREDENCIAIS)

    assert 'AlterarContaPagar' in mandou


def test_titulo_JA_PAGO_nao_altera_nem_paga_de_novo(monkeypatch):
    """Continua valendo: reenviar um comprovante não pode pagar duas vezes."""
    mandou = []

    def falso(body):
        mandou.append(body.get('call'))
        return {'ok': True, 'status': 200, 'body': {'status_titulo': 'PAGO'}}

    monkeypatch.setattr(core, 'execute_omie', falso)
    resultados = core._executar_sequencia_omie(_plano(), CREDENCIAIS)

    assert mandou == ['ConsultarContaPagar']
    assert any(r['step'] == 'skip' for r in resultados)


# ── 4. O Omie manda na planilha e no card ────────────────────────────────────
#
# Cobrança do dono em 14/09/2026: *"baixam na planilha, mas não baixam no Omie.
# Não tem sentido. (…) Se eu estou mandando pra baixar é pra baixar, tem que
# baixar as duas coisas."*
#
# A planilha e o card eram marcados INDEPENDENTE do que o Omie respondesse.
# Conferir isso de verdade exigiria PDF, planilha e Omie reais — então o que se
# confere é a LIGAÇÃO e a ORDEM dela no código, como já se faz com a
# duplicidade neste mesmo módulo.
import inspect  # noqa: E402

FONTE = inspect.getsource(core.processar_baixabradesco)


def test_a_planilha_so_e_marcada_DEPOIS_de_o_omie_confirmar():
    posicao_guarda = FONTE.find('devia_baixar and not omie_confirmou')
    posicao_sheets = FONTE.find('_executar_sheets_async(plan, payload)')
    posicao_pipefy = FONTE.find('build_update_card_mutation(plan, card_info)')

    assert posicao_guarda != -1, (
        'sumiu a guarda que impede marcar a planilha sem o Omie ter confirmado')
    assert posicao_guarda < posicao_sheets, 'a planilha é marcada antes da guarda'
    assert posicao_guarda < posicao_pipefy, 'o card é marcado antes da guarda'


def test_o_que_NAO_foi_marcado_e_dito():
    """Não marcar em silêncio seria trocar um engano por outro: ele precisa
    saber que a SP continua a pagar."""
    assert "plan.responses['nao_marcou']" in FONTE
    assert 'reenvie' in FONTE.lower()


def test_ja_pago_no_omie_CONTA_como_confirmacao():
    """Aí a planilha e o card É QUE estão atrasados — e é para atualizá-los que
    aquele trecho existe."""
    assert '_omie_ok or _omie_ja_pago' in FONTE


# ── 5. O NOME da variável no servidor ────────────────────────────────────────
#
# ⚠️ O defeito que fez tudo isto acontecer, e ele é de nome, não de lógica.
#
# Este módulo procurava SÓ por OMIE_BWS_APP_KEY/OMIE_BWS_APP_SECRET. No Render
# as variáveis chamam-se OMIE_KEY/OMIE_SECRET — que é o nome que o resto do
# monorepo já aceitava há meses (painel e emissaonf resolvem os dois apelidos).
#
# Resultado: o painel e a emissão de NF achavam a credencial, e este robô não.
# Pelo Make nunca apareceu, porque o Make manda a chave dentro do pedido.

NOMES_OMIE = ('OMIE_KEY', 'OMIE_SECRET', 'OMIE_BWS_APP_KEY',
              'OMIE_BWS_APP_SECRET', 'OMIE_APP_KEY', 'OMIE_APP_SECRET')


@pytest.fixture
def ambiente_limpo(monkeypatch):
    for nome in NOMES_OMIE:
        monkeypatch.delenv(nome, raising=False)


def test_aceita_OMIE_KEY_e_OMIE_SECRET_que_e_o_nome_do_servidor(
        ambiente_limpo, monkeypatch):
    from app.apps.baixabradesco.omie import credentials_from_payload

    monkeypatch.setenv('OMIE_KEY', 'a-chave')
    monkeypatch.setenv('OMIE_SECRET', 'o-segredo')

    assert credentials_from_payload({}) == ('a-chave', 'o-segredo')


def test_continua_aceitando_o_apelido_ANTIGO(ambiente_limpo, monkeypatch):
    """Quem tem `.env` local com o nome velho não pode quebrar."""
    from app.apps.baixabradesco.omie import credentials_from_payload

    monkeypatch.setenv('OMIE_BWS_APP_KEY', 'a-chave')
    monkeypatch.setenv('OMIE_BWS_APP_SECRET', 'o-segredo')

    assert credentials_from_payload({}) == ('a-chave', 'o-segredo')


def test_os_MESMOS_apelidos_dos_outros_modulos(ambiente_limpo):
    """⚠️ Apelido resolvido em três lugares diferentes é apelido que vai
    divergir — e foi exatamente o que aconteceu aqui."""
    import inspect

    from app.apps.baixabradesco import omie as omie_bradesco
    from app.apps.painel.sync import omie_client

    fonte_painel = inspect.getsource(omie_client.OmieClient.de_ambiente)
    for nome in omie_bradesco.NOMES_APP_KEY:
        assert nome in fonte_painel, (
            f'{nome} é aceito aqui e não no painel — os dois vão divergir')


def test_a_credencial_do_portal_vem_com_espaco_e_e_limpa(ambiente_limpo,
                                                         monkeypatch):
    """Copiar e colar do portal do Omie traz espaço e caractere invisível
    grudado. É o mesmo tratamento que o painel faz."""
    from app.apps.baixabradesco.omie import credentials_from_payload

    monkeypatch.setenv('OMIE_KEY', '  1234567890​ ')
    monkeypatch.setenv('OMIE_SECRET', 'abc﻿def ')

    assert credentials_from_payload({}) == ('1234567890', 'abcdef')


def test_a_mensagem_de_falta_DIZ_TODOS_os_nomes_aceitos(ambiente_limpo):
    """Quem vai cadastrar precisa saber quais nomes valem — dizer um só manda
    a pessoa criar uma variável que já existia com outro nome."""
    plano = _plano()
    resultados = core._executar_sequencia_omie(plano, {})

    motivo = resultados[0]['motivo']
    assert 'OMIE_KEY' in motivo and 'OMIE_SECRET' in motivo
    assert 'OMIE_BWS_APP_KEY' in motivo
