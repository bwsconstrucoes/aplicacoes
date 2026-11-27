"""Homologação: quem não tem a ação é barrado antes de qualquer outra coisa.

⚑ POR QUE ESTE ARQUIVO EXISTE SEPARADO (17/11/2026).

A homologação por perfil era UM arquivo com 1.659 casos — e `--dist loadfile`,
que a suíte usa, mantém os testes de um mesmo arquivo no MESMO trabalhador.
Resultado: um processo levava 482 s sozinho enquanto os outros três ficavam
parados, e a suíte inteira levava 491 s. Ele cobrou: *"precisa otimizar eles."*

Dividir em quatro arquivos é o que deixa o paralelismo alcançá-los. A regra
não mudou nem um pouco: o que era conferido continua sendo, nos mesmos casos.
O que mora em comum (o inventário de rotas, os perfis, os usuários de teste)
está em `tests/homologacao.py`, que NÃO é arquivo de teste — é a peça que os
quatro leem.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.permissoes import decidir
from homologacao import (  # noqa: F401  (`operadores` é fixture, usada por nome)
    ESCRITAS, PERFIS_DO_ROTEIRO, como, identificacao,
    operadores)

pytestmark = pytest.mark.banco

# ---------------------------------------------------------------------------
# Escritas: quem não tem a ação é barrado antes de qualquer outra checagem
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("perfil", PERFIS_DO_ROTEIRO, ids=lambda p: p.value)
@pytest.mark.parametrize("caso", ESCRITAS, ids=identificacao)
def test_escrita_sem_alcada_e_recusada(app_real, operadores, perfil, caso):
    endpoint, caminho, _metodo, acao = caso
    # ⚑ A EXPECTATIVA VEM DE `decidir`, NÃO DA TABELA CRUA (27/11/2026).
    # `PERMISSOES` é só um dos pedaços da decisão: há também as ações
    # IMPLICADAS — quem tem o trabalho de uma tela abre a tela. Comparar com a
    # tabela crua fazia este teste cobrar 403 de quem o sistema deixa entrar
    # com razão (o supervisor de obra, que autoriza compra fora do fluxo e
    # precisa da fila de pedidos).
    if decidir(perfil, acao, {}, None):
        pytest.skip("perfil tem a ação; escrita real exige dados e é coberta pelos testes de domínio")
    c = como(app_real, operadores[perfil].id)

    r = c.post(caminho, json={})

    assert r.status_code == 403, (
        f"{perfil.value} NÃO devia poder {caminho} (recebeu {r.status_code})")
