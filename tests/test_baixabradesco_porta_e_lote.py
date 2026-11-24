# -*- coding: utf-8 -*-
"""A porta da baixa e a execução em lote — 17/11/2026.

Dois pedidos do dono no mesmo dia, e eles se completam.

**O Make não espera mais.** Ele contou que o módulo HTTP dava "This module
timed out" o tempo todo e que o cenário tem um "break" que REPETE a chamada
inteira — e os registros do Render confirmaram: duas execuções do mesmo
arquivo lado a lado, ocupando duas das quatro linhas de atendimento. A decisão
dele: *"no Make eu não preciso de resposta para cada baixa (…) basta dizer: ó,
tá processando (…) o que precisa ser garantido é que, uma vez feita a chamada,
a baixa aconteça (…) o retorno ao final você me manda no Telegram."*

**E o trabalho repetido foi agrupado.** *"é uma série de tarefas repetidas,
né? (…) um único comando para atualizar a planilha (…) um único comando pra
atualizar o pipefy, fazendo uma mutation e atualizar vários cards ao mesmo
tempo."* Cada comprovante gastava três idas ao Google e uma ao Pipefy; agora o
lote inteiro gasta três e uma.

O que este arquivo trava, porque cada item já foi defeito ou seria um:

1. A resposta sai na hora e NÃO carrega credencial nem o trabalho feito.
2. O mesmo lote não roda duas vezes ao mesmo tempo.
3. O lote vai para o disco ANTES da resposta e só sai de lá quando termina.
4. A tela do Análise de SPs e a fila continuam recebendo o resultado completo.
5. A gravação da planilha e a do Pipefy acontecem em lote — e o comprovante
   que falhar vai para a fila SOZINHO, não o lote inteiro.
"""
import threading
import time

import pytest

from app.apps.baixabradesco import core, fila_tardia, pipefy, sheets
from app.apps.baixabradesco.models import (ExecutionPlan, ExtractedReceipt,
                                           MatchResult)


def _pedido(nome='comprovante.pdf', base64='JVBERi0xLjQK'):
    return {
        'modo_teste': False,
        'attachments': [{'filename': nome, 'base64': base64}],
        'omie': {'app_key': 'CHAVE-SECRETA', 'app_secret': 'SEGREDO'},
    }


@pytest.fixture(autouse=True)
def porta_limpa(tmp_path, monkeypatch):
    """Cada teste começa sem lote em andamento e com uma pasta sua."""
    monkeypatch.setattr(fila_tardia, 'FILA_DIR', str(tmp_path))
    core._LOTES_EM_ANDAMENTO.clear()
    yield tmp_path
    core._LOTES_EM_ANDAMENTO.clear()


# =====================================================================
# 1. A resposta sai na hora
# =====================================================================

def test_a_resposta_sai_sem_esperar_a_baixa(monkeypatch):
    """Era isto que estourava o tempo do Make e disparava o reenvio."""
    liberar = threading.Event()
    terminou = threading.Event()

    def _demorado(payload):
        liberar.wait(5)
        terminou.set()
        return {'resumo': {}}

    monkeypatch.setattr(core, '_processar_lote', _demorado)

    comecou = time.time()
    r = core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    gasto = time.time() - comecou

    assert r['recebido'] is True
    assert gasto < 1.0, 'a resposta esperou a baixa terminar'
    assert terminou.is_set() is False
    liberar.set()
    terminou.wait(5)


def test_a_resposta_nao_leva_credencial_nem_lista_de_planos(monkeypatch):
    monkeypatch.setattr(core, '_processar_lote', lambda p: {'resumo': {}})
    r = core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    texto = str(r)
    assert 'CHAVE-SECRETA' not in texto and 'SEGREDO' not in texto
    assert 'planos' not in r


def test_a_resposta_diz_para_nao_reenviar():
    """O cenário do Make repete sozinho; a resposta tem de desencorajar isso."""
    assert 'NÃO reenvie' in core.processar_baixabradesco.__doc__ or True
    import inspect
    fonte = inspect.getsource(core.processar_baixabradesco)
    assert 'NÃO reenvie' in fonte


# =====================================================================
# 2. O mesmo lote não roda duas vezes
# =====================================================================

def test_o_mesmo_lote_reenviado_nao_roda_de_novo(monkeypatch):
    """O "break" do cenário remandava o arquivo inteiro enquanto o primeiro
    ainda rodava — duas execuções, cota dobrada, aviso dobrado."""
    liberar = threading.Event()
    chamadas = []

    def _demorado(payload):
        chamadas.append(payload)
        liberar.wait(5)
        return {'resumo': {}}

    monkeypatch.setattr(core, '_processar_lote', _demorado)

    primeira = core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    segunda = core.processar_baixabradesco(_pedido(), em_segundo_plano=True)

    assert primeira['recebido'] is True
    assert segunda.get('ja_em_andamento') is True
    assert 'nada foi baixado duas vezes' in segunda['motivo'].lower()
    assert len(chamadas) == 1
    liberar.set()


def test_lote_diferente_passa(monkeypatch):
    """A trava é pelo ARQUIVO. Comprovante novo nunca pode ficar preso nela."""
    liberar = threading.Event()
    chamadas = []
    monkeypatch.setattr(core, '_processar_lote',
                        lambda p: chamadas.append(p) or liberar.wait(5) or {'resumo': {}})

    core.processar_baixabradesco(_pedido('a.pdf', 'AAAA'), em_segundo_plano=True)
    core.processar_baixabradesco(_pedido('b.pdf', 'BBBB'), em_segundo_plano=True)

    for _ in range(50):
        if len(chamadas) == 2:
            break
        time.sleep(0.02)
    assert len(chamadas) == 2
    liberar.set()


def test_a_trava_e_solta_quando_a_baixa_termina(monkeypatch):
    monkeypatch.setattr(core, '_processar_lote', lambda p: {'resumo': {}})
    core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    for _ in range(50):
        if not core._LOTES_EM_ANDAMENTO:
            break
        time.sleep(0.02)
    assert core._LOTES_EM_ANDAMENTO == {}


def test_a_trava_e_solta_mesmo_quando_a_baixa_estoura(monkeypatch):
    """Trava presa por um erro deixaria aquele comprovante impossível de reenviar."""
    def _explode(payload):
        raise RuntimeError('boom')
    monkeypatch.setattr(core, '_processar_lote', _explode)

    core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    for _ in range(50):
        if not core._LOTES_EM_ANDAMENTO:
            break
        time.sleep(0.02)
    assert core._LOTES_EM_ANDAMENTO == {}


def test_lotes_em_andamento_nao_mostra_payload(monkeypatch):
    liberar = threading.Event()
    monkeypatch.setattr(core, '_processar_lote', lambda p: liberar.wait(5) or {'resumo': {}})
    core.processar_baixabradesco(_pedido(), em_segundo_plano=True)

    visivel = core.lotes_em_andamento()
    assert len(visivel) == 1
    assert visivel[0]['arquivos'] == ['comprovante.pdf']
    assert 'CHAVE-SECRETA' not in str(visivel)
    liberar.set()


# =====================================================================
# 3. "Uma vez chamado, a baixa acontece"
# =====================================================================

def test_o_lote_vai_para_o_disco_antes_da_resposta(porta_limpa, monkeypatch):
    no_disco = {}
    liberar = threading.Event()

    def _demorado(payload):
        no_disco['durante'] = [n for n in porta_limpa.iterdir()]
        liberar.wait(5)
        return {'resumo': {}}

    monkeypatch.setattr(core, '_processar_lote', _demorado)
    r = core.processar_baixabradesco(_pedido(), em_segundo_plano=True)

    assert r['guardado_em_disco'] is True
    for _ in range(50):
        if no_disco.get('durante'):
            break
        time.sleep(0.02)
    assert len(no_disco['durante']) == 1, 'o lote não foi guardado antes de responder'
    liberar.set()


def test_o_lote_sai_do_disco_quando_termina(porta_limpa, monkeypatch):
    monkeypatch.setattr(core, '_processar_lote', lambda p: {'resumo': {}})
    core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    for _ in range(50):
        if not list(porta_limpa.iterdir()):
            break
        time.sleep(0.02)
    assert list(porta_limpa.iterdir()) == [], 'lote concluído continuou ocupando a pasta'


def test_o_lote_que_estourou_FICA_no_disco(porta_limpa, monkeypatch):
    """É o que permite o serviço automático terminar o que morreu no meio."""
    def _explode(payload):
        raise RuntimeError('o trabalhador morreu')
    monkeypatch.setattr(core, '_processar_lote', _explode)

    core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    time.sleep(0.2)
    assert len(list(porta_limpa.iterdir())) == 1


# =====================================================================
# 4. Quem precisa do resultado completo continua recebendo
# =====================================================================

def test_quem_chama_por_dentro_recebe_o_resultado_completo(monkeypatch):
    """A tela do Análise de SPs mostra "baixado" lendo esta resposta. Ela chama
    a função direto, sem passar pela rota — e não pede segundo plano."""
    monkeypatch.setattr(core, '_processar_lote',
                        lambda p: {'resumo': {'executaveis': 3}, 'planos': []})
    r = core.processar_baixabradesco(_pedido())
    assert r['resumo']['executaveis'] == 3
    assert 'recebido' not in r


def test_a_fila_nao_pede_segundo_plano():
    import inspect
    fonte = inspect.getsource(fila_tardia._processar_adiados)
    assert 'em_segundo_plano' not in fonte, (
        'em segundo plano a fila apagaria um adiado que nem começou a ser processado')


def test_o_analise_de_sps_nao_pede_segundo_plano():
    """Ele chama a função por dentro e MOSTRA o resultado na tela. Se um dia
    passar a pedir segundo plano, a tela volta a dizer "baixado" sem ter sido —
    que é exatamente o defeito de 13/09/2026."""
    import inspect
    from app.apps.analisesps import comprovantes
    fonte = inspect.getsource(comprovantes._mandar_ao_robo)
    assert 'em_segundo_plano' not in fonte


def test_modo_teste_continua_sincrono(monkeypatch):
    monkeypatch.setattr(core, '_processar_lote', lambda p: {'modo_teste': True})
    r = core.processar_baixabradesco({'modo_teste': True,
                                      'attachments': [{'filename': 'a.pdf', 'base64': 'x'}]})
    assert r == {'modo_teste': True}


# =====================================================================
# 5. A planilha: uma conversa para o lote inteiro
# =====================================================================

class _AbaContada:
    """Conta quantas idas ao Google a gravação gasta."""

    def __init__(self, ids_por_linha):
        self.ids = ids_por_linha
        self.leituras = 0
        self.gravacoes = 0
        self.gravado = []

    def batch_get(self, faixas):
        self.leituras += 1
        saida = []
        for f in faixas:
            if f.startswith('A') and f[1:].isdigit():
                saida.append([[self.ids.get(int(f[1:]), '')]])
                continue
            valor = ''
            for item in self.gravado:
                if item['range'] == f:
                    valor = item['values'][0][0]
            saida.append([[valor]])
        return saida

    def batch_update(self, data, value_input_option=None):
        self.gravacoes += 1
        self.gravado.extend(data)


class _Planilha:
    def __init__(self, abas):
        self.abas = abas

    def worksheet(self, nome):
        return self.abas[nome]


class _Google:
    def __init__(self, planilha):
        self.planilha = planilha

    def open_by_key(self, _k):
        return self.planilha


def _updates(sp, linha):
    return {'sheet_id': 'x', 'aba': 'SPsBD', 'linha': linha,
            'filtros': {'A': '=' + sp}, 'updates': {'O': 'Pago', 'X': '16/11/2026'}}


def test_dez_comprovantes_gastam_tres_idas_ao_google(monkeypatch):
    """Antes eram três POR COMPROVANTE — trinta idas, uma esperando a outra."""
    aba = _AbaContada({100 + i: f'SP{i}' for i in range(10)})
    monkeypatch.setattr(sheets, 'get_gc', lambda: _Google(_Planilha({'SPsBD': aba})))

    r = sheets.execute_spsbd_updates([_updates(f'SP{i}', 100 + i) for i in range(10)])

    assert r['ok'] is True
    assert r['gravados'] == 10
    assert aba.gravacoes == 1, 'a gravação ainda é uma por comprovante'
    assert aba.leituras == 2, 'conferir as linhas e reler o gravado deveria ser uma ida cada'


def test_o_resultado_vem_por_comprovante(monkeypatch):
    """Sem isso, um comprovante ruim mandaria o lote inteiro para a fila."""
    aba = _AbaContada({100: 'SP0'})      # a linha 101 tem outro ID
    monkeypatch.setattr(sheets, 'get_gc', lambda: _Google(_Planilha({'SPsBD': aba})))

    r = sheets.execute_spsbd_updates([_updates('SP0', 100), _updates('SP1', 101)])

    assert [d['ok'] for d in r['por_update']] == [True, False]
    assert r['gravados'] == 1


def test_so_o_comprovante_que_falhou_vai_para_a_fila(monkeypatch):
    aba = _AbaContada({100: 'SP0'})
    monkeypatch.setattr(sheets, 'get_gc', lambda: _Google(_Planilha({'SPsBD': aba})))
    monkeypatch.setattr(core.time, 'sleep', lambda s: None)
    enfileirados = []
    monkeypatch.setattr(core, 'enqueue_failure',
                        lambda plan, etapa, tipo, msg, payload:
                        enfileirados.append(plan.match.id) or {'ok': True})

    bons = []
    for sp, linha in (('SP0', 100), ('SP1', 101)):
        plano = ExecutionPlan(receipt=ExtractedReceipt(filename='x.pdf'),
                              match=MatchResult(status='localizado', id=sp))
        plano.sheets_updates = [_updates(sp, linha)]
        bons.append(plano)

    core._gravar_planilha_em_lote(bons, {})

    assert enfileirados == ['SP1']
    assert bons[0].responses['sheets']['ok'] is True
    assert bons[1].responses['sheets']['ok'] is False


# =====================================================================
# 6. O Pipefy: uma mutation para vários cards
# =====================================================================

def _plano_com_card(cid):
    plano = ExecutionPlan(receipt=ExtractedReceipt(filename='x.pdf', page=1),
                          match=MatchResult(status='localizado', id=cid))
    plano.pode_executar = True
    return plano


def test_uma_mutation_leva_varios_cards():
    itens = [(_plano_com_card('111'), None), (_plano_com_card('222'), None)]
    texto, mapa = pipefy.build_update_cards_mutation(itens)

    assert texto.count('mutation {') == 1
    assert '"111"' in texto and '"222"' in texto
    assert mapa == {'c0_': 0, 'c1_': 1}


def test_os_apelidos_nao_se_atropelam():
    """Dois cards com o mesmo apelido fariam o Pipefy recusar o documento."""
    itens = [(_plano_com_card('111'), None), (_plano_com_card('222'), None)]
    texto, _ = pipefy.build_update_cards_mutation(itens)

    apelidos = [l.strip().split(':')[0] for l in texto.splitlines()
                if ':' in l and 'mutation' not in l]
    assert len(apelidos) == len(set(apelidos))


def test_o_card_recusado_e_identificado_pelo_apelido():
    """É o que permite enfileirar só o card ruim, e não os cinco do pedaço."""
    mapa = {'c0_': 0, 'c1_': 1, 'c2_': 2}
    resposta = {'ok': False, 'body': {'errors': [{'path': ['c1_n4'], 'message': 'x'}]}}
    assert pipefy.cards_recusados(resposta, mapa) == {1}


def test_erro_sem_dono_recusa_todos():
    """Token inválido, rede fora: ninguém pode ser dado como atualizado."""
    mapa = {'c0_': 0, 'c1_': 1}
    assert pipefy.cards_recusados({'ok': False, 'body': {}}, mapa) == {0, 1}
    assert pipefy.cards_recusados(
        {'ok': False, 'body': {'errors': [{'message': 'token'}]}}, mapa) == {0, 1}


def test_card_aceito_nao_e_recusado():
    assert pipefy.cards_recusados({'ok': True, 'body': {}}, {'c0_': 0}) == set()


def test_cinco_cards_viram_uma_chamada(monkeypatch):
    chamadas = []
    monkeypatch.setattr(core, 'execute_graphql',
                        lambda texto: chamadas.append(texto) or {'ok': True, 'status': 200})
    planos = [_plano_com_card(str(i)) for i in range(5)]

    r = core._atualizar_cards_em_lote(planos, {}, {})

    assert len(chamadas) == 1
    assert r['atualizados'] == 5
    assert r['chamadas_poupadas'] == 4


def test_pipefy_fora_do_ar_enfileira_cada_card(monkeypatch):
    monkeypatch.setattr(core, 'execute_graphql',
                        lambda texto: {'ok': False, 'status': 500, 'body': {}})
    enfileirados = []
    monkeypatch.setattr(core, 'enqueue_failure',
                        lambda plan, etapa, tipo, msg, payload:
                        enfileirados.append(plan.match.id) or {'ok': True})

    planos = [_plano_com_card('1'), _plano_com_card('2')]
    r = core._atualizar_cards_em_lote(planos, {}, {})

    assert r['recusados'] == 2
    assert enfileirados == ['1', '2']
    # a fila reprocessa UM card por vez: ela precisa da mutation daquele card
    assert '"1"' in planos[0].pipefy_update_mutation


def test_a_rota_do_make_pede_segundo_plano():
    """É a única porta que pede — e é ela que o Make chama."""
    import inspect
    from app.apps.baixabradesco import routes
    fonte = inspect.getsource(routes.executar)
    assert 'em_segundo_plano=True' in fonte


# =====================================================================
# 7. A pasta do servidor não cresce sem fim
# =====================================================================

def test_o_lote_guardado_nao_duplica_o_que_ja_esta_la(porta_limpa):
    """Sobrou um arquivo de uma execução que morreu: é ELE que esta execução
    conclui e apaga, não um segundo arquivo igual."""
    primeiro = fila_tardia.guardar_recebido(_pedido())
    segundo = fila_tardia.guardar_recebido(_pedido())
    assert primeiro and primeiro == segundo
    assert len(list(porta_limpa.iterdir())) == 1


def test_lote_velho_demais_e_jogado_fora_antes_de_guardar_outro(porta_limpa):
    """Com o serviço automático desligado, a pasta só cresceria — e cada lote
    carrega os PDFs inteiros dentro dele."""
    import os
    import time as _t
    velho = porta_limpa / '1600000000_velho.json'
    velho.write_text('{}', encoding='utf-8')
    antigo = _t.time() - (fila_tardia.IDADE_MAXIMA_DIAS + 1) * 86400
    os.utime(velho, (antigo, antigo))

    fila_tardia.guardar_recebido(_pedido())

    assert not velho.exists()
    assert len(list(porta_limpa.iterdir())) == 1


def test_lote_grande_demais_nao_vai_para_o_disco(porta_limpa, monkeypatch):
    """A /tmp do Render não é grande; melhor ficar sem a rede de proteção (e
    dizer isso na resposta) do que encher o disco e derrubar tudo."""
    monkeypatch.setattr(fila_tardia, 'TETO_GUARDAR_BYTES', 100)
    assert fila_tardia.guardar_recebido(_pedido(base64='X' * 5000)) == ''
    assert list(porta_limpa.iterdir()) == []


def test_a_resposta_diz_quando_ficou_sem_rede_de_protecao(monkeypatch):
    monkeypatch.setattr(core, '_processar_lote', lambda p: {'resumo': {}})
    monkeypatch.setattr(core, 'guardar_recebido', lambda p: '')
    r = core.processar_baixabradesco(_pedido(), em_segundo_plano=True)
    assert r['guardado_em_disco'] is False
