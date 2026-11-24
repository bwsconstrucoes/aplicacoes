"""Homologação: toda LEITURA abre para quem tem a ação e recusa quem não tem.

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

from homologacao import (  # noqa: F401  (`operadores` é fixture, usada por nome)
    LEITURAS, PERFIS_DO_ROTEIRO, PERMISSOES, como, identificacao,
    operadores)

pytestmark = pytest.mark.banco

# ---------------------------------------------------------------------------
# Leituras: abre para quem tem a ação, recusa para quem não tem
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("perfil", PERFIS_DO_ROTEIRO, ids=lambda p: p.value)
@pytest.mark.parametrize("caso", LEITURAS, ids=identificacao)
def test_leitura_abre_ou_recusa_conforme_o_perfil(app_real, operadores, perfil, caso):
    endpoint, caminho, _metodo, acao = caso
    c = como(app_real, operadores[perfil].id)

    r = c.get(caminho)

    if perfil in PERMISSOES[acao]:
        assert r.status_code != 403, f"{perfil.value} devia abrir {caminho}"
        assert r.status_code < 500, (
            f"{caminho} quebrou (HTTP {r.status_code}) para {perfil.value} "
            f"com a base vazia: {r.get_data(as_text=True)[:300]}")
    else:
        assert r.status_code == 403, (
            f"{perfil.value} NÃO devia abrir {caminho} (recebeu {r.status_code})")
