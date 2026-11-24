"""Homologação: os casos do roteiro, por extenso, e as separações de alçada.

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
    INVENTARIO, LEITURAS, ESCRITAS, P, PERMISSOES, como, operadores)

pytestmark = pytest.mark.banco

# ---------------------------------------------------------------------------
# Os casos do roteiro, por extenso — para o documento e o teste dizerem o mesmo
# ---------------------------------------------------------------------------
ROTEIRO = [
    # perfil,               caminho,               abre?
    # ⚑ 24/11/2026: quem é da obra só tem o portal — as telas do ERP o levam
    # para lá (decisão do dono: *"o pessoal da obra (…) têm acesso ao portal
    # (…) Isso em tudo"*). Antes estas três abriam.
    (P.ADMINISTRATIVO_OBRA, "/erp/titulos",        "PORTAL"),
    (P.ADMINISTRATIVO_OBRA, "/erp/lancar",         "PORTAL"),
    (P.ADMINISTRATIVO_OBRA, "/erp/dc",             "PORTAL"),
    (P.ADMINISTRATIVO_OBRA, "/erp/pagamentos",     False),
    (P.ADMINISTRATIVO_OBRA, "/erp/relatorios",     False),
    (P.ADMINISTRATIVO_OBRA, "/erp/configuracoes",  False),
    (P.SUPERVISOR_OBRA,     "/erp/relatorios",     True),     # o supervisor vê relatórios
    (P.SUPERVISOR_OBRA,     "/erp/pagamentos",     False),
    (P.SUPERVISOR_OBRA,     "/erp/configuracoes",  False),
    (P.FINANCEIRO,          "/erp/pagamentos",     True),
    (P.FINANCEIRO,          "/erp/conciliacao",    True),
    (P.FINANCEIRO,          "/erp/receber",        True),
    (P.FINANCEIRO,          "/erp/importar",       True),
    (P.FINANCEIRO,          "/erp/configuracoes",  False),    # a ÚNICA fechada para ele
    (P.DEPARTAMENTO_PESSOAL, "/erp/dc",            True),
    (P.DEPARTAMENTO_PESSOAL, "/erp/colaboradores", True),
    (P.DEPARTAMENTO_PESSOAL, "/erp/pagamentos",    False),
    (P.DEPARTAMENTO_PESSOAL, "/erp/relatorios",    False),
]


@pytest.mark.parametrize("perfil,caminho,abre", ROTEIRO,
                         ids=lambda v: v.value if isinstance(v, P) else str(v))
def test_casos_do_roteiro(app_real, operadores, perfil, caminho, abre):
    r = como(app_real, operadores[perfil].id).get(caminho)
    if abre == "PORTAL":
        assert r.status_code == 302 and r.headers["Location"].endswith("/erp/portal"), (
            f"{perfil.value} devia ser levado ao portal em {caminho}: {r.status_code}")
    elif abre:
        assert r.status_code == 200, f"{perfil.value} devia abrir {caminho}: {r.status_code}"
    else:
        assert r.status_code == 403, f"{perfil.value} devia ser recusado em {caminho}"


def test_financeiro_nao_avaliza_e_supervisor_nao_aprova(app_real, operadores):
    """A separação que evita a mesma pessoa pedir e liberar."""
    fin = como(app_real, operadores[P.FINANCEIRO].id)
    sup = como(app_real, operadores[P.SUPERVISOR_OBRA].id)

    assert fin.get("/erp/api/avais/pendentes").status_code == 403       # avalizar
    assert sup.post("/erp/api/titulos/acao", json={}).status_code == 403  # aprovar
    assert sup.get("/erp/api/avais/pendentes").status_code != 403


def test_so_o_dp_e_o_diretor_editam_colaboradores(app_real, operadores):
    for perfil in (P.FINANCEIRO, P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.GESTOR_OBRA):
        r = como(app_real, operadores[perfil].id).post("/erp/api/colaboradores", json={})
        assert r.status_code == 403, f"{perfil.value} não devia editar colaborador"
    for perfil in (P.DEPARTAMENTO_PESSOAL, P.DIRETOR_FINANCEIRO, P.ADMIN):
        r = como(app_real, operadores[perfil].id).post("/erp/api/colaboradores", json={})
        assert r.status_code != 403, f"{perfil.value} devia poder editar colaborador"


def test_inventario_cobre_as_telas_do_roteiro():
    """Se uma tela do roteiro sumir do registro, este teste acusa — em vez de
    o caso parametrizado simplesmente deixar de existir."""
    caminhos = {c for _, c, m, _ in INVENTARIO if m == "GET"}
    for _, caminho, _ in ROTEIRO:
        assert caminho in caminhos, f"{caminho} não está mais no registro de rotas"
    assert len(LEITURAS) >= 40 and len(ESCRITAS) >= 40
