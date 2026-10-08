# -*- coding: utf-8 -*-
"""A gravação na SPsBD tem de ser confirmada, não "mandada e esquecida".

Queixa do dono em 07/10/2026: *"tem algo que tem acontecido com muita
frequência: a não atualização silenciosa da aba SPsBD"*.

Eram quatro causas somadas, e todas deixavam a SP como "Pagar" sem avisar:

1. **A gravação rodava numa thread solta**, e a resposta ao Make saía antes dela
   terminar. O gunicorn recicla o trabalhador a cada mil pedidos e o serviço
   reinicia a cada publicação — nos dois casos a thread morria no meio.
2. **O resultado da gravação era escrito no plano depois** de a resposta já
   estar montada: quem lia o retorno nunca via o que havia acontecido.
3. **Ninguém conferia** se a célula ficou com o valor.
4. **A fila de falhas só andava se alguém chamasse a rota à mão** — e o
   reprocessamento de planilha marcava "sucesso" ignorando o resultado da
   gravação, então o item saía da fila sem ter sido gravado.
"""
import pytest

from app.apps.baixabradesco.sheets import SPS_SHEET_ID, execute_spsbd_updates


class _AbaFalsa:
    """Dublê da aba SPsBD: serve a busca do filtro, a gravação e a conferência."""

    def __init__(self, ids, gravar_de_verdade=True, explodir_na_gravacao=False):
        self._ids = ids                      # coluna A, a partir da linha 2
        self._celulas = {}                   # 'O5' -> valor
        self.gravar_de_verdade = gravar_de_verdade
        self.explodir_na_gravacao = explodir_na_gravacao
        self.gravacoes = 0

    def batch_get(self, ranges):
        saida = []
        for r in ranges:
            if r.startswith('A2:'):
                saida.append([[v] for v in self._ids])
            else:
                saida.append([[self._celulas.get(r.replace(':', ''), '')]])
        return saida

    def batch_update(self, data, value_input_option=None):
        self.gravacoes += 1
        if self.explodir_na_gravacao:
            raise RuntimeError('Google recusou (429)')
        if self.gravar_de_verdade:
            for item in data:
                self._celulas[item['range']] = item['values'][0][0]


class _PlanilhaFalsa:
    def __init__(self, aba):
        self._aba = aba

    def worksheet(self, nome):
        return self._aba


class _GoogleFalso:
    def __init__(self, aba):
        self._p = _PlanilhaFalsa(aba)

    def open_by_key(self, chave):
        return self._p


def updates(sp_id='7001'):
    return [{
        'sheet_id': SPS_SHEET_ID, 'aba': 'SPsBD',
        'filtros': {'A': '=' + sp_id},
        'updates': {'O': 'Pago', 'V': '2026-10-07 10:00:00',
                    'X': '07/10/2026', 'AG': 'https://dropbox/x.pdf',
                    'AK': '50024-0'},
    }]


@pytest.fixture
def google(monkeypatch):
    def montar(aba):
        monkeypatch.setattr('app.apps.baixabradesco.sheets.get_gc',
                            lambda: _GoogleFalso(aba))
        return aba
    return montar


# ── A conferência ─────────────────────────────────────────────────────────────

def test_gravacao_que_chegou_e_confirmada(google):
    aba = google(_AbaFalsa(['7001', '7002']))
    assert execute_spsbd_updates(updates('7001'))['ok'] is True


def test_gravacao_que_nao_chegou_e_recusada(google):
    """O caso silencioso: o Google aceita a chamada e a célula não muda."""
    aba = google(_AbaFalsa(['7001'], gravar_de_verdade=False))
    r = execute_spsbd_updates(updates('7001'))
    assert r['ok'] is False
    assert r['gravados'] == 0
    assert 'não confirmada' in r['erros'][0]


def test_erro_na_gravacao_e_reportado(google):
    google(_AbaFalsa(['7001'], explodir_na_gravacao=True))
    r = execute_spsbd_updates(updates('7001'))
    assert r['ok'] is False
    assert '429' in r['erros'][0]


def test_sp_que_nao_existe_na_planilha_e_reportada(google):
    google(_AbaFalsa(['9999']))
    r = execute_spsbd_updates(updates('7001'))
    assert r['ok'] is False
    assert 'não encontrada' in r['erros'][0]


def test_so_confere_coluna_de_texto(google):
    """Data e número são reescritos pelo Google conforme o formato da célula;
    conferir esses daria divergência onde não há."""
    from app.apps.baixabradesco.sheets import COLUNAS_CONFERIDAS
    assert COLUNAS_CONFERIDAS == ('O',)


# ── A gravação acontece ANTES da resposta sair ────────────────────────────────

def test_a_gravacao_nao_roda_mais_em_thread_solta():
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core._gravar_planilha)
    assert 'Thread' not in fonte, 'gravação em thread solta morre com o trabalhador'
    assert 'execute_spsbd_updates' in fonte


def test_o_fluxo_grava_de_forma_confirmada():
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core.processar_baixabradesco)
    assert '_gravar_planilha(plan, payload)' in fonte
    assert '_executar_sheets_async' not in fonte


def test_tenta_duas_vezes_antes_de_desistir(google, monkeypatch):
    monkeypatch.setattr('app.apps.baixabradesco.core.time.sleep', lambda s: None)
    aba = google(_AbaFalsa(['7001'], gravar_de_verdade=False))

    from app.apps.baixabradesco.core import _gravar_planilha
    from app.apps.baixabradesco.models import ExecutionPlan, ExtractedReceipt, MatchResult

    plano = ExecutionPlan(receipt=ExtractedReceipt(filename='x.pdf'),
                          match=MatchResult(status='localizado', id='7001'))
    plano.sheets_updates = updates('7001')
    monkeypatch.setattr('app.apps.baixabradesco.core.enqueue_failure',
                        lambda *a, **k: {'ok': True})

    r = _gravar_planilha(plano, {})
    assert r['ok'] is False
    assert r['tentativas'] == 2
    assert aba.gravacoes == 2


def test_falha_na_gravacao_vai_para_a_fila(google, monkeypatch):
    monkeypatch.setattr('app.apps.baixabradesco.core.time.sleep', lambda s: None)
    google(_AbaFalsa(['7001'], gravar_de_verdade=False))
    enfileirados = []

    from app.apps.baixabradesco.core import _gravar_planilha
    from app.apps.baixabradesco.models import ExecutionPlan, ExtractedReceipt, MatchResult
    monkeypatch.setattr('app.apps.baixabradesco.core.enqueue_failure',
                        lambda plan, etapa, tipo, msg, payload: enfileirados.append(etapa) or {'ok': True})

    plano = ExecutionPlan(receipt=ExtractedReceipt(filename='x.pdf'),
                          match=MatchResult(status='localizado', id='7001'))
    plano.sheets_updates = updates('7001')
    _gravar_planilha(plano, {})

    assert enfileirados == ['sheets']
    assert plano.responses['fila_sheets']['ok'] is True


def test_o_resultado_da_gravacao_entra_na_resposta(google):
    google(_AbaFalsa(['7001']))
    from app.apps.baixabradesco.core import _gravar_planilha
    from app.apps.baixabradesco.models import ExecutionPlan, ExtractedReceipt, MatchResult

    plano = ExecutionPlan(receipt=ExtractedReceipt(filename='x.pdf'),
                          match=MatchResult(status='localizado', id='7001'))
    plano.sheets_updates = updates('7001')
    _gravar_planilha(plano, {})
    assert plano.responses['sheets']['ok'] is True


# ── A fila anda sozinha, e o reprocessamento não mente ────────────────────────

def test_a_fila_e_drenada_junto_com_o_lote():
    """Antes ela só andava se alguém chamasse a rota à mão."""
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core.processar_baixabradesco)
    assert "_drenar_fila(payload)" in fonte


def test_drenar_fila_nunca_derruba_a_resposta(monkeypatch):
    from app.apps.baixabradesco.core import _drenar_fila
    import app.apps.baixabradesco.fila as fila
    monkeypatch.setattr(fila, 'reprocessar_fila',
                        lambda p: (_ for _ in ()).throw(RuntimeError('planilha fora')))
    r = _drenar_fila({})
    assert r['ok'] is False
    assert 'planilha fora' in r['erro']


def test_reprocessar_planilha_nao_mente_mais_sobre_sucesso(monkeypatch):
    """Ele chamava a gravação e devolvia ok=True sem olhar o resultado — o item
    saía da fila sem ter sido gravado."""
    import app.apps.baixabradesco.fila as fila
    monkeypatch.setattr(fila, 'execute_spsbd_updates',
                        lambda u: {'ok': False, 'gravados': 0, 'erros': ['não confirmada']})
    item = {'Payload Resumido': '{"sheets_updates": [{"a": 1}]}'}
    assert fila._retry_sheets(item, {})['ok'] is False


def test_reprocessar_planilha_confirma_sucesso_de_verdade(monkeypatch):
    import app.apps.baixabradesco.fila as fila
    monkeypatch.setattr(fila, 'execute_spsbd_updates',
                        lambda u: {'ok': True, 'gravados': 1, 'erros': []})
    item = {'Payload Resumido': '{"sheets_updates": [{"a": 1}]}'}
    assert fila._retry_sheets(item, {})['ok'] is True
