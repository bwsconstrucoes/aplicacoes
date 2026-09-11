"""Permissão fina: o cargo é a base, o cadastro da pessoa corrige.

O dono pediu (04/09/2026) que cada pessoa tenha uma função principal e, além
dela, permissões marcadas uma a uma — "de repente o diretor sai de férias e eu
quero deixar outra pessoa autorizando". Isto aqui prova as quatro coisas que
não podem falhar:

  1. marcar CONCEDE de verdade — e a guarda que roda antes da rota respeita;
  2. desmarcar NEGA de verdade, mesmo quando o cargo daria;
  3. o ADMIN não consegue se trancar para fora das telas que consertam o erro;
  4. enquanto a migração 032 não rodar, tudo continua valendo pelo cargo — o
     ERP não pode cair porque a tabela de exceções ainda não existe.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp.core.auth.permissoes import (
    ACAO_ROTULOS, PERMISSOES, PROTEGIDAS_DO_ADMIN, decidir, pode,
)
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario


def _app(sessao):
    from app.apps.erp import routes
    app = Flask(__name__)
    app.secret_key = "teste"
    app.register_blueprint(routes.bp)
    return app, routes


def _como(app, routes, monkeypatch, sessao, usuario_id=1):
    monkeypatch.setattr(routes, "get_session",
                        lambda: contextlib.nullcontext(sessao))
    c = app.test_client()
    with c.session_transaction() as sessao_web:
        sessao_web["erp_usuario_id"] = usuario_id
    return c


# ---------------------------------------------------------------------------
# 1. A regra, sem tela e sem banco
# ---------------------------------------------------------------------------
def test_sem_marcacao_vale_exatamente_o_cargo():
    u = novo_usuario(1, P.ADMINISTRATIVO_OBRA)
    assert pode(u, "lancar") is True
    assert pode(u, "pagar") is False


def test_marcar_concede_o_que_o_cargo_nao_da():
    u = novo_usuario(1, P.ADMINISTRATIVO_OBRA)
    u.permissoes_extras = {"aprovar": True}
    assert pode(u, "aprovar") is True
    assert pode(u, "pagar") is False, "conceder uma não pode abrir as outras"


def test_desmarcar_tira_o_que_o_cargo_daria():
    u = novo_usuario(1, P.FINANCEIRO)
    u.permissoes_extras = {"pagar": False}
    assert pode(u, "pagar") is False
    assert pode(u, "conciliar") is True


@pytest.mark.parametrize("acao", PROTEGIDAS_DO_ADMIN)
def test_o_administrador_nao_se_tranca_para_fora(acao):
    u = novo_usuario(1, P.ADMIN)
    u.permissoes_extras = {acao: False}
    assert pode(u, acao) is True, (
        f"desmarcar {acao} do ADMIN deixaria o sistema sem quem conserte")


def test_o_administrador_pode_perder_uma_acao_comum():
    """A proteção é só das telas de conserto — não é imunidade geral."""
    u = novo_usuario(1, P.ADMIN)
    u.permissoes_extras = {"pagar": False}
    assert pode(u, "pagar") is False


def test_decidir_responde_igual_a_pode():
    """A guarda usa `decidir` (valores soltos) e o resto usa `pode` (objeto).
    Se as duas divergirem, a tela mostra uma coisa e a rota faz outra."""
    for perfil in P:
        for acao in PERMISSOES:
            for marcada in (None, True, False):
                u = novo_usuario(1, perfil)
                excecoes = {} if marcada is None else {acao: marcada}
                u.permissoes_extras = excecoes
                assert pode(u, acao) == decidir(perfil, acao, excecoes), (
                    f"{perfil.value}/{acao}/{marcada}")


def test_toda_acao_tem_nome_em_portugues():
    """Quem marca a caixinha não é programador."""
    assert set(ACAO_ROTULOS) == set(PERMISSOES)


# ---------------------------------------------------------------------------
# 2. A guarda que roda antes de toda rota
# ---------------------------------------------------------------------------
def test_a_guarda_recusa_quem_teve_a_acao_desmarcada(monkeypatch):
    u = novo_usuario(1, P.FINANCEIRO)
    s = SessaoFalsa(u, permissoes_por_usuario={1: {"pagar": False}})
    app, routes = _app(s)
    c = _como(app, routes, monkeypatch, s)

    r = c.post("/erp/api/pagamentos/baixar", json={})

    assert r.status_code == 403
    assert "permissão" in r.get_json()["erro"].lower()


def test_a_guarda_libera_quem_teve_a_acao_marcada(monkeypatch):
    """O administrativo de obra não aprova — a não ser que o dono marque."""
    u = novo_usuario(1, P.ADMINISTRATIVO_OBRA)
    s = SessaoFalsa(u)
    app, routes = _app(s)

    negado = _como(app, routes, monkeypatch, s).post("/erp/api/titulos/acao", json={})
    assert negado.status_code == 403

    s.permissoes_por_usuario = {1: {"aprovar": True}}
    liberado = _como(app, routes, monkeypatch, s).post("/erp/api/titulos/acao", json={})
    assert liberado.status_code != 403


def test_sem_a_tabela_o_erp_continua_de_pe(monkeypatch):
    """Migração 032 ainda não aplicada: a leitura das exceções falha, e a
    resposta certa é valer o cargo — não derrubar o sistema."""
    u = novo_usuario(1, P.FINANCEIRO)
    s = SessaoFalsa(u)
    original = s.execute

    def explode(stmt, params=None):
        if "usuario_permissoes" in str(stmt):
            raise RuntimeError('relation "usuario_permissoes" does not exist')
        return original(stmt, params)
    s.execute = explode

    app, routes = _app(s)
    c = _como(app, routes, monkeypatch, s)

    r = c.post("/erp/api/pagamentos/baixar", json={})

    assert r.status_code != 403, "o cargo dá 'pagar'; a tabela ausente não pode tirar"
    assert r.status_code < 500


# ---------------------------------------------------------------------------
# 3. A gravação: só o que difere do cargo vira exceção
# ---------------------------------------------------------------------------
def test_gravar_nao_guarda_o_que_o_cargo_ja_da(monkeypatch):
    from app.apps.erp.db.models.cadastros import UsuarioPermissao
    admin = novo_usuario(1, P.ADMIN)
    alvo = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    s = SessaoFalsa(admin, alvo)
    app, routes = _app(s)
    c = _como(app, routes, monkeypatch, s)

    r = c.post("/erp/api/usuarios/7/permissoes",
               json={"permissoes": {"lancar": True,      # o cargo já dá
                                    "aprovar": True,     # o cargo NÃO dá
                                    "ver_pessoal": False}})   # o cargo dá

    assert r.status_code == 200
    gravadas = {o.acao: o.concedida for o in s.adicionados
                if isinstance(o, UsuarioPermissao)}
    assert gravadas == {"aprovar": True, "ver_pessoal": False}
    assert r.get_json()["permissoes"] == {"aprovar": True, "ver_pessoal": False}


def test_gravar_recusa_acao_inventada(monkeypatch):
    admin = novo_usuario(1, P.ADMIN)
    alvo = novo_usuario(7, P.FINANCEIRO)
    s = SessaoFalsa(admin, alvo)
    app, routes = _app(s)
    c = _como(app, routes, monkeypatch, s)

    r = c.post("/erp/api/usuarios/7/permissoes",
               json={"permissoes": {"mandar_no_mundo": True}})

    assert r.status_code == 400
    assert "desconhecida" in r.get_json()["erro"].lower()


def test_o_ajuste_de_permissao_fica_registrado(monkeypatch):
    admin = novo_usuario(1, P.ADMIN)
    alvo = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    s = SessaoFalsa(admin, alvo)
    app, routes = _app(s)
    c = _como(app, routes, monkeypatch, s)

    c.post("/erp/api/usuarios/7/permissoes", json={"permissoes": {"aprovar": True}})

    assert any(e.get("ac") == "PERMISSOES_AJUSTADAS" for e in s.eventos), (
        "mudar quem pode o quê sem deixar rastro é o tipo de coisa que ninguém "
        "consegue explicar depois")


def test_so_quem_gere_usuarios_ajusta_permissoes(monkeypatch):
    u = novo_usuario(1, P.FINANCEIRO)      # opera tudo, mas não cadastra gente
    s = SessaoFalsa(u, novo_usuario(7, P.ADMINISTRATIVO_OBRA))
    app, routes = _app(s)
    c = _como(app, routes, monkeypatch, s)

    assert c.get("/erp/api/usuarios/7/permissoes").status_code == 403
    assert c.post("/erp/api/usuarios/7/permissoes",
                  json={"permissoes": {}}).status_code == 403


# ---------------------------------------------------------------------------
# 4. Com banco de verdade: a marcação atravessa a tabela e chega à rota
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_com_banco_real_a_marcacao_libera_e_a_desmarcacao_recusa(app_real, sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import Usuario, UsuarioPermissao
    from conftest import como

    u = Usuario(nome="Ajustado", email="ajuste@teste.bws.local",
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMINISTRATIVO_OBRA)
    sessao_real.add(u)
    sessao_real.flush()
    sessao_real.commit()

    c = como(app_real, u.id)
    assert c.get("/erp/relatorios").status_code == 403, "o cargo não dá relatórios"

    sessao_real.add(UsuarioPermissao(usuario_id=u.id, acao="ver_relatorios", concedida=True))
    sessao_real.commit()
    assert como(app_real, u.id).get("/erp/relatorios").status_code == 200

    sessao_real.add(UsuarioPermissao(usuario_id=u.id, acao="ver_erp", concedida=False))
    sessao_real.commit()
    assert como(app_real, u.id).get("/erp/titulos").status_code == 403, (
        "desmarcar tem de fechar mesmo a tela que o cargo abria")


@pytest.mark.banco
def test_a_migracao_032_criou_a_tabela(sessao_real):
    from sqlalchemy import text
    colunas = {linha[0] for linha in sessao_real.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'usuario_permissoes'")).all()}
    assert {"usuario_id", "acao", "concedida", "definida_em", "definida_por"} <= colunas
