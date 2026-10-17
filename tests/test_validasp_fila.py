# -*- coding: utf-8 -*-
"""A fila do validasp (17/10/2026): a validação não pode se perder.

O dono: "se a pessoa sabe que validou, precisamos garantir que a validação
aconteça". Estes testes cobrem o caminho do disco (banco fora) e a regra de
cada rodada; o caminho do banco está em test_validasp_fila_banco.py."""
from __future__ import annotations

import time

import pytest

from app.apps.validasp import core, fila


class CelulaFalsa:
    def __init__(self, valor):
        self.value = valor


class AbaFalsa:
    """Imita a SPsBD: coluna A com os IDs e a coluna AH (34) de validação."""

    def __init__(self, ids, engole_escrita=False, falha_escrita=0):
        self.ids = ['ID'] + list(ids)
        self.ah = {}
        self.engole_escrita = engole_escrita
        self.falha_escrita = falha_escrita
        self.anotadas = []

    def col_values(self, n):
        assert n == 1
        return list(self.ids)

    def update_cell(self, linha, coluna, valor):
        assert coluna == 34
        if self.falha_escrita:
            self.falha_escrita -= 1
            raise RuntimeError('400 erro de validação qualquer')   # não-retryable
        if not self.engole_escrita:
            self.ah[linha] = valor

    def cell(self, linha, coluna):
        return CelulaFalsa(self.ah.get(linha, ''))

    def append_row(self, linha):
        self.anotadas.append(linha)


class PlanilhaFalsa:
    def __init__(self, aba):
        self.aba = aba

    def worksheet(self, nome):
        return self.aba


@pytest.fixture
def no_disco(tmp_path, monkeypatch):
    """Banco fora do ar: tudo vai para o disco, numa pasta do teste."""
    monkeypatch.setattr(fila, 'PASTA_DISCO', str(tmp_path))
    monkeypatch.setattr(fila, 'banco_disponivel', lambda: False)
    return tmp_path


def _planilha(monkeypatch, aba):
    monkeypatch.setattr(fila, '_abrir_planilha', lambda: PlanilhaFalsa(aba))


def _vencer_tudo():
    for nome in fila._disco_listar():
        dado = fila._disco_ler(nome)
        dado['proxima'] = 0
        fila._disco_gravar(dado, nome)


def test_sem_banco_a_validacao_fica_guardada_em_disco(no_disco):
    r = fila.registrar(' 123 ')
    assert r['ok'] and r['onde'] == 'disco'
    dado = fila._disco_ler(fila._disco_listar()[0])
    assert dado['sp_id'] == '123' and dado['situacao'] == fila.PENDENTE
    # A primeira tentativa espera a linha da SP nascer na SPsBD
    assert dado['proxima'] - dado['criado'] == fila.ESPERA_INICIAL_S


def test_antes_da_hora_a_rodada_nao_toca_na_planilha(no_disco, monkeypatch):
    fila.registrar('123')
    monkeypatch.setattr(fila, '_abrir_planilha', lambda: pytest.fail('abriu a planilha cedo'))
    assert fila.rodada()['processados'] == 0


def test_marca_sim_na_ultima_linha_da_sp_e_sai_da_fila(no_disco, monkeypatch):
    aba = AbaFalsa(['100', '123', '200', '123'])
    _planilha(monkeypatch, aba)
    fila.registrar('123')
    _vencer_tudo()
    r = fila.rodada()
    assert r['feito'] == 1
    assert aba.ah == {5: 'Sim'}                 # linha 5 = a última ocorrência
    assert fila._disco_listar() == []


def test_gravacao_aceita_que_nao_pegou_nao_tira_da_fila(no_disco, monkeypatch):
    """O defeito que o dono via: o Google "aceita" e a célula fica vazia."""
    aba = AbaFalsa(['123'], engole_escrita=True)
    _planilha(monkeypatch, aba)
    fila.registrar('123')
    _vencer_tudo()
    r = fila.rodada()
    assert r['feito'] == 0 and r['pendente'] == 1
    dado = fila._disco_ler(fila._disco_listar()[0])
    assert dado['tentativas'] == 1 and 'leitura devolveu' in dado['ultimo_erro']
    assert dado['proxima'] > time.time()        # volta mais tarde, não na hora


def test_erro_do_google_ao_gravar_fica_para_depois_e_depois_entra(no_disco, monkeypatch):
    aba = AbaFalsa(['123'], falha_escrita=1)
    _planilha(monkeypatch, aba)
    fila.registrar('123')
    _vencer_tudo()
    assert fila.rodada()['pendente'] == 1
    assert 'erro do Google' in fila._disco_ler(fila._disco_listar()[0])['ultimo_erro']
    _vencer_tudo()
    assert fila.rodada()['feito'] == 1
    assert aba.ah == {2: 'Sim'}


def test_planilha_fora_do_ar_nao_perde_ninguem(no_disco, monkeypatch):
    def _cai():
        raise RuntimeError('403 permissão negada')
    monkeypatch.setattr(fila, '_abrir_planilha', _cai)
    fila.registrar('1')
    fila.registrar('2')
    _vencer_tudo()
    r = fila.rodada()
    assert r['pendente'] == 2
    assert len(fila._disco_listar()) == 2


def test_sp_que_ainda_nao_esta_na_spsbd_espera_cada_vez_mais(no_disco, monkeypatch):
    _planilha(monkeypatch, AbaFalsa(['999']))
    fila.registrar('123')
    esperas = []
    for _ in range(3):
        _vencer_tudo()
        antes = time.time()
        fila.rodada()
        dado = fila._disco_ler(fila._disco_listar()[0])
        esperas.append(round(dado['proxima'] - antes))
        assert dado['ultimo_erro'] == 'SP não encontrada na SPsBD'
    assert esperas[0] < esperas[1] < esperas[2]


def test_depois_de_sete_dias_desiste_e_anota_na_falha_processar(no_disco, monkeypatch):
    aba = AbaFalsa(['999'])
    _planilha(monkeypatch, aba)
    fila.registrar('123')
    nome = fila._disco_listar()[0]
    dado = fila._disco_ler(nome)
    dado['criado'] = time.time() - 8 * 86400
    dado['proxima'] = 0
    fila._disco_gravar(dado, nome)
    assert fila.rodada()['desistiu'] == 1
    assert aba.anotadas and aba.anotadas[0][0] == '123' and aba.anotadas[0][2] == 'Validar'
    # Fica registrada em disco como desistida, e não volta a ser tentada
    assert fila._disco_ler(nome)['situacao'] == fila.DESISTIU
    assert fila.rodada()['processados'] == 0


def test_sem_banco_e_sem_disco_a_resposta_avisa(no_disco, monkeypatch):
    def _sem_disco(*a, **k):
        raise OSError('disco cheio')
    monkeypatch.setattr(fila, '_disco_gravar', _sem_disco)
    assert fila.registrar('123')['ok'] is False


def test_executar_nao_mexe_mais_no_pipefy_e_guarda_antes_das_mensagens(monkeypatch):
    ordem = []
    monkeypatch.setattr(fila, 'registrar', lambda sp: ordem.append(('fila', sp)) or {'ok': True})
    monkeypatch.setattr(core, '_enviar_msg_responsavel',
                        lambda p, z: ordem.append(('zapi', p['id'])) or {'ok': True})

    def _sem_rede(*a, **k):
        raise AssertionError('chamou a internet')
    monkeypatch.setattr(core.requests, 'post', _sem_rede)

    r = core.executar({'id': '123', 'secret': 'x'})
    assert ordem == [('fila', '123'), ('zapi', '123')]
    assert 'pipefy' not in r['secoes'] and 'zapiErroPipefy' not in r['secoes']
    assert r['secoes']['spsbd'] == {'ok': True}
