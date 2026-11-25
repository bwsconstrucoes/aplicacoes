"""Homologação: toda TELA é desenhada de verdade por quem pode abri-la.

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
    PERMISSOES, TELAS, como, identificacao, operadores)

pytestmark = pytest.mark.banco

# ---------------------------------------------------------------------------
# Toda tela ABRE de verdade para alguém
#
# O teste acima percorre quatro perfis do roteiro. Quando NENHUM deles tem a
# ação da tela, ele só confere o 403 — e a tela nunca chega a ser desenhada.
# Foi por esse buraco que a tela de Banco de preços foi para produção com um
# `{% endblock %}` faltando: ela exige "comprar", nenhum dos quatro perfis tem
# essa ação, e o erro só apareceu quando uma pessoa abriu a página.
#
# Aqui cada tela é desenhada por um perfil que PODE abri-la. Se o template não
# compila, se falta uma variável no contexto, se a consulta quebra com a base
# vazia — cai aqui, e não na frente do dono.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("caso", TELAS, ids=identificacao)
def test_toda_tela_e_desenhada_por_quem_pode_abrir(app_real, operadores, caso):
    _endpoint, caminho, _metodo, acao = caso
    quem = sorted(PERMISSOES[acao], key=lambda p: p.value)
    assert quem, f"a ação {acao} não pertence a perfil nenhum"

    c = como(app_real, operadores[quem[0]].id)
    # Segue o desvio: alguns endereços antigos continuam existindo e levam à
    # tela nova (é o caso de /erp/suprimentos/cadastros). O que importa é que
    # no fim uma página seja DESENHADA.
    r = c.get(caminho, follow_redirects=True)

    assert r.status_code == 200, (
        f"{caminho} não abriu para {quem[0].value} (HTTP {r.status_code}). "
        f"Com a base vazia, toda tela do ERP tem de desenhar. "
        f"{r.get_data(as_text=False)[:300]!r}")

    # ⚑ NEM TODA ROTA DE TELA DEVOLVE TELA (25/11/2026). O manual do portal
    # tem um endereço que entrega o PDF — ele é do mesmo mundo (abre pelo
    # navegador, exige a mesma ação), mas o que volta é arquivo, não página.
    # Pedir "<body" dele é pedir a coisa errada, e `as_text` ainda estoura no
    # primeiro byte que não é UTF-8. O que vale conferir aqui é o que já está
    # conferido: abriu 200 e não jogou ninguém para o login.
    tipo = (r.headers.get("Content-Type") or "").lower()
    if "html" not in tipo:
        assert r.get_data(), f"{caminho} respondeu 200 vazio"
        assert "/erp/entrar" not in r.request.path, (
            f"{caminho} jogou {quem[0].value} para o login em vez de abrir")
        return

    corpo = r.get_data(as_text=True)
    assert "<body" in corpo.lower(), f"{caminho} respondeu 200 sem página"
    assert "/erp/entrar" not in r.request.path, (
        f"{caminho} jogou {quem[0].value} para o login em vez de abrir")
