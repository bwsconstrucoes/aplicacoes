"""Homologação por perfil — automatizada, com banco de verdade.

O roteiro de homologação (`HOMOLOGACAO_PERFIS.md`) pede que o dono navegue
com um usuário de cada perfil e confira o que abre e o que é recusado. Isto
aqui faz a parte mecânica disso a cada envio, contra um Postgres real:

  - para CADA perfil e CADA tela ou consulta sem parâmetro na URL, a resposta
    é "abre" (não 403, não 500) quando o perfil tem a ação, e 403 quando não;
  - para CADA ação de escrita sem parâmetro, o perfil que não a tem recebe 403
    antes de qualquer outra coisa.

A lista de telas e ações NÃO é escrita à mão: vem do registro de permissões
do próprio código. Tela nova entra sozinha; tela que mude de ação também.

O que sobra para o olho humano: o visual, a leitura de documentos por IA e os
fluxos que exigem dados reais (avalizar, pagar). O roteiro continua valendo
para isso — mas encolheu.
"""
from __future__ import annotations

import pytest
from flask import Flask

from app.apps.erp import routes
from app.apps.erp.core.auth.permissoes import PERMISSOES, ROTULOS
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Usuario

from conftest import como

pytestmark = pytest.mark.banco

# Endpoints deixados de fora, com o motivo escrito:
FORA = {
    "erp.api_aplicar_migracoes",   # abre conexão própria, fora da transação do teste
    "erp.pagina_login", "erp.sair", "erp.health",   # públicos, não têm perfil
}

PERFIS_DO_ROTEIRO = (P.ADMINISTRATIVO_OBRA, P.SUPERVISOR_OBRA, P.FINANCEIRO,
                     P.DEPARTAMENTO_PESSOAL)
TODOS_OS_PERFIS = tuple(P)


def _inventario():
    """(endpoint, caminho, método, ação) para toda rota SEM parâmetro na URL."""
    a = Flask(__name__)
    a.secret_key = "inventario"
    a.register_blueprint(routes.bp)
    saida = []
    for regra in a.url_map.iter_rules():
        if not regra.endpoint.startswith("erp.") or regra.arguments:
            continue
        if regra.endpoint in FORA or regra.endpoint in routes._ISENTOS:
            continue
        mapa = routes._REGISTRO_PERMISSOES.get(regra.endpoint) or {}
        for metodo in ("GET", "POST"):
            if metodo in regra.methods and mapa.get(metodo):
                saida.append((regra.endpoint, str(regra), metodo, mapa[metodo]))
    return sorted(saida)


INVENTARIO = _inventario()
LEITURAS = [x for x in INVENTARIO if x[2] == "GET"]
ESCRITAS = [x for x in INVENTARIO if x[2] == "POST"]


def _id(caso):
    endpoint, caminho, metodo, acao = caso
    return f"{metodo} {caminho} [{acao}]"


@pytest.fixture
def operadores(sessao_real) -> dict[P, Usuario]:
    """Um usuário de cada perfil, no banco de verdade."""
    senha = gerar_hash("senha-de-homologacao-1")
    pessoas = {}
    for perfil in TODOS_OS_PERFIS:
        u = Usuario(nome=f"Homologação {ROTULOS.get(perfil, perfil.value)}",
                    email=f"homolog.{perfil.value.lower()}@teste.bws.local",
                    senha_hash=senha, perfil=perfil)
        sessao_real.add(u)
        pessoas[perfil] = u
    sessao_real.flush()
    sessao_real.commit()
    return pessoas


# ---------------------------------------------------------------------------
# Leituras: abre para quem tem a ação, recusa para quem não tem
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("perfil", PERFIS_DO_ROTEIRO, ids=lambda p: p.value)
@pytest.mark.parametrize("caso", LEITURAS, ids=_id)
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


# ---------------------------------------------------------------------------
# Escritas: quem não tem a ação é barrado antes de qualquer outra checagem
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("perfil", PERFIS_DO_ROTEIRO, ids=lambda p: p.value)
@pytest.mark.parametrize("caso", ESCRITAS, ids=_id)
def test_escrita_sem_alcada_e_recusada(app_real, operadores, perfil, caso):
    endpoint, caminho, _metodo, acao = caso
    if perfil in PERMISSOES[acao]:
        pytest.skip("perfil tem a ação; escrita real exige dados e é coberta pelos testes de domínio")
    c = como(app_real, operadores[perfil].id)

    r = c.post(caminho, json={})

    assert r.status_code == 403, (
        f"{perfil.value} NÃO devia poder {caminho} (recebeu {r.status_code})")


# ---------------------------------------------------------------------------
# Os casos do roteiro, por extenso — para o documento e o teste dizerem o mesmo
# ---------------------------------------------------------------------------
ROTEIRO = [
    # perfil,               caminho,               abre?
    (P.ADMINISTRATIVO_OBRA, "/erp/titulos",        True),
    (P.ADMINISTRATIVO_OBRA, "/erp/lancar",         True),
    (P.ADMINISTRATIVO_OBRA, "/erp/dc",             True),
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
    if abre:
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
