"""Bloco 0 — o padrão de autorização é NEGAR.

Estes testes são a rede que torna a inversão durável. O teste de inventário
falha quando alguém acrescenta rota sem declarar permissão: o esquecimento
passa a quebrar a suíte antes de virar brecha em produção.

Rodam sem banco. O guard nega antes de o handler abrir sessão, e o
`_usuario_logado` só faz `s.get(Usuario, id)` — que a sessão dublada atende.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp import routes
from app.apps.erp.core.auth.permissoes import PERMISSOES
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario


@pytest.fixture
def app():
    """App mínimo só com o blueprint do ERP.

    Não usa `create_app()` de propósito: ele importa os outros 13 blueprints,
    que puxam gspread, dropbox e openai — dependências que o teste não precisa.
    """
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


def endpoints_do_erp(app):
    return sorted({r.endpoint for r in app.url_map.iter_rules()
                   if r.endpoint.startswith("erp.")})


# ---------------------------------------------------------------------------
# Inventário — nenhuma rota fica sem declarar
# ---------------------------------------------------------------------------
def test_toda_rota_declara_permissao_ou_e_publica_explicita(app):
    declarados = set(routes._REGISTRO_PERMISSOES) | set(routes._ENDPOINTS_PUBLICOS)
    sem_declaracao = [e for e in endpoints_do_erp(app)
                      if e not in declarados and e not in routes._ISENTOS]

    assert sem_declaracao == [], (
        "Rota sem @permissao(...) nem @permissao_publica(...). O guard vai negar "
        "em produção; declare a ação: " + ", ".join(sem_declaracao))


def test_nenhuma_acao_declarada_e_desconhecida():
    """Erro de digitação em @permissao viraria rota impossível de acessar."""
    usadas = {a for mapa in routes._REGISTRO_PERMISSOES.values() for a in mapa.values()}

    assert usadas <= set(PERMISSOES), f"ações inexistentes: {usadas - set(PERMISSOES)}"


def test_rota_publica_tem_motivo_escrito():
    for endpoint, motivo in routes._ENDPOINTS_PUBLICOS.items():
        assert motivo and len(motivo) > 10, f"{endpoint} sem motivo real"


def test_o_conjunto_de_rotas_publicas_e_pequeno_e_conhecido():
    """Se esta lista crescer, alguém abriu uma rota — que apareça na revisão."""
    assert set(routes._ENDPOINTS_PUBLICOS) == {
        "erp.pagina_login", "erp.sair", "erp.health"}


# ---------------------------------------------------------------------------
# O default é negar
# ---------------------------------------------------------------------------
def test_rota_nao_declarada_e_negada(app, monkeypatch):
    """O coração da inversão: rota que ninguém declarou não passa.

    Simula o esquecimento tirando a declaração de uma rota real — é o estado
    exato em que uma rota nova nasce. Não adianta registrar rota solta no app
    de teste: `bp.before_request` só governa endpoints DO blueprint, e é essa
    a fronteira que o guard cobre.
    """
    monkeypatch.delitem(routes._REGISTRO_PERMISSOES, "erp.api_titulos")
    monkeypatch.setattr(routes, "get_session",
                        _sessao_com(novo_usuario(1, P.ADMIN)))

    with app.test_client() as c:
        with c.session_transaction() as sessao:
            sessao["erp_usuario_id"] = 1
        r = c.get("/erp/api/titulos")

    assert r.status_code == 403, "rota sem declaração deveria ser negada, mesmo ao ADMIN"


def _sessao_com(usuario):
    @contextlib.contextmanager
    def _fake():
        yield SessaoFalsa(usuario)
    return _fake


def _pedir(app, monkeypatch, caminho, perfil, metodo="get"):
    monkeypatch.setattr(routes, "get_session", _sessao_com(novo_usuario(1, perfil)))
    with app.test_client() as c:
        with c.session_transaction() as sessao:
            sessao["erp_usuario_id"] = 1
        return getattr(c, metodo)(caminho)


@pytest.mark.parametrize("perfil", [P.CONSULTA, P.ADMINISTRATIVO_OBRA, P.LANCADOR])
def test_perfil_sem_alcada_nao_registra_pagamento(app, monkeypatch, perfil):
    r = _pedir(app, monkeypatch, "/erp/api/pagamentos/baixar", perfil, "post")

    assert r.status_code == 403


@pytest.mark.parametrize("perfil", [P.CONSULTA, P.ADMINISTRATIVO_OBRA, P.SUPERVISOR_OBRA])
def test_perfil_sem_alcada_nao_altera_configuracao(app, monkeypatch, perfil):
    r = _pedir(app, monkeypatch, "/erp/api/config/conta", perfil, "post")

    assert r.status_code == 403


@pytest.mark.parametrize("perfil", [P.ADMINISTRATIVO_OBRA, P.LANCADOR, P.CONSULTA])
def test_dados_de_pagamento_negados_a_quem_nao_pode_ve_los(app, monkeypatch, perfil):
    """A regra que já existia em /api/titulos agora vale na rota que a burlava."""
    r = _pedir(app, monkeypatch, "/erp/api/pagamentos/detalhe/1", perfil)

    assert r.status_code == 403


@pytest.mark.parametrize("perfil", [P.ADMINISTRATIVO_OBRA, P.LANCADOR, P.CONSULTA])
def test_relatorio_gerencial_negado_a_perfil_operacional(app, monkeypatch, perfil):
    r = _pedir(app, monkeypatch, "/erp/api/relatorios", perfil, "post")

    assert r.status_code == 403


def test_consulta_nao_cancela_titulo(app, monkeypatch):
    """Cancelar não tinha checagem nenhuma: qualquer logado cancelava em lote."""
    r = _pedir(app, monkeypatch, "/erp/api/titulos/acao", P.CONSULTA, "post")

    assert r.status_code == 403


def test_quem_tem_alcada_passa_pelo_guard(app, monkeypatch):
    """O guard libera FINANCEIRO em pagamento.

    Depois dele o handler roda com a sessão dublada e falha por outro motivo —
    então aqui só se afirma o que este teste pode afirmar: não foi barrado por
    permissão. O caminho feliz completo depende de Postgres e é verificado em
    homologação.
    """
    r = _pedir(app, monkeypatch, "/erp/api/pagamentos/baixar", P.FINANCEIRO, "post")

    assert r.status_code != 403


@pytest.mark.parametrize("caminho, metodo", [
    ("/erp/api/importar/pipefy", "post"),
    ("/erp/api/importar/csv", "post"),
    ("/erp/api/importar/ofx", "post"),
    ("/erp/api/receber/baixar", "post"),
    ("/erp/api/conciliacao/executar", "post"),
    ("/erp/api/conciliacao/manual", "post"),
    ("/erp/api/obras/1/fase", "post"),
    ("/erp/api/config/categoria", "post"),
    ("/erp/api/config/obra", "post"),
    ("/erp/api/config/depara/definir", "post"),
])
def test_administrativo_de_obra_nao_executa_acao_de_alcada(app, monkeypatch,
                                                           caminho, metodo):
    """Bloco 2: todas estas rodavam para qualquer pessoa com login."""
    r = _pedir(app, monkeypatch, caminho, P.ADMINISTRATIVO_OBRA, metodo)

    assert r.status_code == 403


@pytest.mark.parametrize("caminho", [
    "/erp/api/pagamentos/agenda",
    "/erp/api/lotes",
    "/erp/api/conciliacao/painel",
    "/erp/api/conciliacao/extrato",
    "/erp/api/movimentacoes/neutras",
    "/erp/api/mapa",
])
def test_painel_financeiro_negado_a_perfil_operacional(app, monkeypatch, caminho):
    """Bloco 4: agregados que expunham a posição financeira inteira."""
    r = _pedir(app, monkeypatch, caminho, P.ADMINISTRATIVO_OBRA)

    assert r.status_code == 403


def test_consulta_nao_enumera_operadores(app, monkeypatch):
    """Lista de quem existe é o passo 1 para escolher alvo."""
    r = _pedir(app, monkeypatch, "/erp/api/operadores/contato", P.CONSULTA)

    assert r.status_code == 403


def test_rota_publica_responde_sem_sessao(app):
    with app.test_client() as c:
        r = c.get("/erp/health")

    assert r.status_code != 403


def test_metodo_nao_declarado_no_mapa_e_recusado(app, monkeypatch):
    """`api_interessados` declara GET/POST/DELETE. PUT não existe no mapa."""
    mapa = routes._REGISTRO_PERMISSOES["erp.api_interessados"]

    assert "PUT" not in mapa
    assert set(mapa) == {"GET", "POST", "DELETE"}


# ---------------------------------------------------------------------------
# Escopo de objeto — a outra metade (usada a partir do bloco 1)
# ---------------------------------------------------------------------------
def test_fora_do_escopo_responde_404_e_nao_403(app):
    """Nunca 403 aqui: dizer 'sem permissão' confirmaria que o id existe.

    Exercita o handler diretamente. A prova ponta a ponta vem no bloco 1, com
    as rotas que passam a chamar `exigir_titulo_no_escopo`.
    """
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado

    with app.app_context():
        resposta, codigo = routes._fora_do_escopo(
            ErroNaoEncontrado("Título não encontrado."))

    assert codigo == 404
    assert resposta.get_json()["erro"] == "Título não encontrado."


def test_handler_de_fora_do_escopo_esta_registrado_no_blueprint():
    """Sem o registro, o vazamento volta calado como 500."""
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado

    registrados = set()
    for por_codigo in routes.bp.error_handler_spec.values():
        for mapa in por_codigo.values():
            registrados.update(mapa)

    assert ErroNaoEncontrado in registrados
