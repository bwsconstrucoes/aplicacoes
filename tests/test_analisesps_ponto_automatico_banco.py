# -*- coding: utf-8 -*-
"""
O PONTO DO DIA SEM AGENDADOR — 15/10/2026, com banco de verdade.

O dono: "o ponto não tá automático" (a tela dizia "Importado em 02/10/2026 por
MARCELO"). O `ponto_diario` esperava um agendador de fora que nunca foi
configurado; agora a tela aberta dispara, como já faz com a sincronização.
"""
import datetime as dt

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def ponto_devendo(banco_analisesps, monkeypatch):
    from app.apps.analisesps import ponto, tarefas
    pedidos = []
    monkeypatch.setattr(ponto, "configurado", lambda: True)
    monkeypatch.setattr(ponto, "o_que_fazer_no_automatico", lambda a, m, hoje=None: "trazer")
    monkeypatch.setattr(tarefas, "estado", lambda pista="geral": {"rodando": False, "detalhe": None,
                                                                   "interrompida": None})
    monkeypatch.setattr(tarefas, "_minutos_desde_a_ultima_sincronizacao", lambda: 1.0)
    monkeypatch.setattr(tarefas, "disparar",
                        lambda modo, disparo="manual": pedidos.append((modo, disparo)) or {"ok": True})
    return pedidos


def test_a_TELA_ABERTA_dispara_o_ponto_do_dia_uma_vez_por_hora(ponto_devendo):
    from app.apps.analisesps import tarefas
    assert tarefas.manter_fresco()["disparou"] is True
    assert ponto_devendo == [("ponto_diario", "tela aberta")]
    # 90 segundos depois, outra tela pergunta: não dispara de novo
    assert tarefas.manter_fresco()["disparou"] is False
    assert len(ponto_devendo) == 1


def test_a_tentativa_de_MAIS_DE_UMA_HORA_libera_outra(ponto_devendo):
    from app.apps.analisesps import tarefas
    from app.apps.analisesps.db import conexao
    from app.apps.analisesps.horario import agora
    from app.apps.analisesps.sincronizacao import _meta_gravar
    with conexao() as conn:
        _meta_gravar(conn, tarefas.CHAVE_PONTO_TENTADO,
                     (agora() - dt.timedelta(minutes=61)).isoformat())
    assert tarefas.manter_fresco()["disparou"] is True
    assert ponto_devendo == [("ponto_diario", "tela aberta")]


def test_mes_que_JA_ENTROU_HOJE_nao_dispara_nada(ponto_devendo, monkeypatch):
    from app.apps.analisesps import ponto, tarefas
    monkeypatch.setattr(ponto, "o_que_fazer_no_automatico", lambda a, m, hoje=None: "pular")
    assert tarefas.manter_fresco()["disparou"] is False and ponto_devendo == []


def test_a_SINCRONIZACAO_velha_tem_a_vez_antes_do_ponto(ponto_devendo, monkeypatch):
    from app.apps.analisesps import tarefas
    monkeypatch.setattr(tarefas, "_minutos_desde_a_ultima_sincronizacao", lambda: 20.0)
    tarefas.manter_fresco()
    assert ponto_devendo == [("sincronizar", "tela aberta")]


def test_sem_as_credenciais_do_Mobponto_nao_tenta(ponto_devendo, monkeypatch):
    from app.apps.analisesps import ponto, tarefas
    monkeypatch.setattr(ponto, "configurado", lambda: False)
    assert tarefas.manter_fresco()["disparou"] is False and ponto_devendo == []
