# -*- coding: utf-8 -*-
"""A fila do validasp com banco de verdade: a migração 084, a pendência que não
duplica, a passagem do disco para o banco e o "Sim" conferido."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.validasp import fila
from tests.test_validasp_fila import AbaFalsa, PlanilhaFalsa

pytestmark = pytest.mark.banco


@pytest.fixture
def limpa(banco, tmp_path, monkeypatch):
    with banco.begin() as c:
        c.execute(text("DELETE FROM validasp.pendencias"))
    monkeypatch.setattr(fila, 'PASTA_DISCO', str(tmp_path))
    fila.esquecer_banco()
    yield banco
    with banco.begin() as c:
        c.execute(text("DELETE FROM validasp.pendencias"))
    fila.esquecer_banco()


def _linhas(banco):
    with banco.connect() as c:
        return [dict(r) for r in c.execute(text(
            "SELECT sp_id, situacao, tentativas, linha_planilha, ultimo_erro "
            "FROM validasp.pendencias ORDER BY id")).mappings()]


def _vencer(banco):
    with banco.begin() as c:
        c.execute(text("UPDATE validasp.pendencias SET proxima_tentativa = NOW() - interval '1 second'"))


def test_registrar_guarda_no_banco_e_nao_duplica(limpa):
    a = fila.registrar('123')
    b = fila.registrar('123')
    assert a['onde'] == 'banco' and a['id'] == b['id']
    assert [l['sp_id'] for l in _linhas(limpa)] == ['123']


def test_o_que_estava_em_disco_passa_para_o_banco(limpa, monkeypatch):
    fora = {'sim': True}
    original = fila.banco_disponivel
    monkeypatch.setattr(fila, 'banco_disponivel', lambda: False if fora['sim'] else original())
    assert fila.registrar('777')['onde'] == 'disco'
    fora['sim'] = False
    fila.rodada()
    assert fila._disco_listar() == []
    assert [l['sp_id'] for l in _linhas(limpa)] == ['777']


def test_rodada_marca_confere_e_fecha_a_pendencia(limpa, monkeypatch):
    aba = AbaFalsa(['5', '123'])
    monkeypatch.setattr(fila, '_abrir_planilha', lambda: PlanilhaFalsa(aba))
    fila.registrar('123')
    assert fila.rodada()['processados'] == 0          # ainda dentro dos 90 s
    _vencer(limpa)
    assert fila.rodada()['feito'] == 1
    [l] = _linhas(limpa)
    assert l['situacao'] == fila.FEITO and l['linha_planilha'] == 3 and aba.ah == {3: 'Sim'}
    # Validar de novo a mesma SP abre uma pendência nova (a anterior está feita)
    assert fila.registrar('123')['id'] != None and len(_linhas(limpa)) == 2


def test_falha_fica_pendente_com_o_motivo(limpa, monkeypatch):
    monkeypatch.setattr(fila, '_abrir_planilha', lambda: PlanilhaFalsa(AbaFalsa(['1'])))
    fila.registrar('123')
    _vencer(limpa)
    assert fila.rodada()['pendente'] == 1
    [l] = _linhas(limpa)
    assert l['situacao'] == fila.PENDENTE and l['tentativas'] == 1
    assert l['ultimo_erro'] == 'SP não encontrada na SPsBD'
    assert fila.rodada()['processados'] == 0          # esperando a próxima hora
    r = fila.resumo()
    assert r['pendentes'] == 1 and r['pendente_mais_antiga']['sp'] == '123'
