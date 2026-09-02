"""O botão que atualiza o banco tem de funcionar com o banco atrasado.

O que derrubou o ERP em 2026-09-02: o código novo esperava uma coluna nova em
`usuarios` (migração 029), a migração ainda não tinha sido aplicada, e a guarda
de permissão — que roda antes de TODA rota — carregava o objeto Usuario pelo
ORM. O SELECT pedia a coluna, o Postgres respondia "não existe", e a guarda
estourava em "Internal Server Error" antes de qualquer tela abrir. Inclusive a
tela de Configurações, onde fica o botão que aplicaria a migração. Impasse.

Estes testes seguram a saída do impasse:
  1. a guarda decide pelo perfil lido por SQL direto, sem tocar no ORM;
  2. um erro de "coluna não existe" vira aviso legível (503), não página branca.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask
from sqlalchemy.exc import ProgrammingError

from app.apps.erp import routes
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario


class SessaoComBancoAtrasado(SessaoFalsa):
    """Responde SQL direto normalmente, mas qualquer carga pelo ORM estoura
    como o Postgres estoura quando falta coluna."""

    def get(self, modelo, ident, options=None):
        raise ProgrammingError(
            "SELECT usuarios.escopo_visao ...", {},
            Exception('column usuarios.escopo_visao does not exist'))

    def scalars(self, stmt):
        raise ProgrammingError("SELECT ...", {},
                               Exception('column usuarios.escopo_visao does not exist'))


@pytest.fixture
def app():
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


def _cliente(app, monkeypatch, sessao, usuario_id=1, perfil="ADMIN"):
    @contextlib.contextmanager
    def _fake():
        yield sessao
    monkeypatch.setattr(routes, "get_session", _fake)
    c = app.test_client()
    with c.session_transaction() as s:
        s["erp_usuario_id"] = usuario_id
        s["erp_usuario_perfil"] = perfil
    return c


def test_admin_alcanca_a_tela_do_botao_com_o_banco_atrasado(app, monkeypatch):
    c = _cliente(app, monkeypatch, SessaoComBancoAtrasado(novo_usuario(1, P.ADMIN)))

    r = c.get("/erp/configuracoes")

    assert r.status_code == 200, r.data[:300]
    assert "Aplicar atualiza" in r.get_data(as_text=True)


def test_estado_do_banco_responde_com_o_banco_atrasado(app, monkeypatch):
    """A rota que lista as migrações pendentes não pode depender do ORM."""
    c = _cliente(app, monkeypatch, SessaoComBancoAtrasado(novo_usuario(1, P.ADMIN)))

    r = c.get("/erp/api/manutencao/banco")

    # sem banco de verdade a listagem falha por outro motivo, mas a GUARDA
    # passou — o que se prova é que não foi 403 nem página branca
    assert r.status_code in (200, 500)
    assert r.is_json


def test_quem_nao_e_admin_continua_barrado_mesmo_com_o_banco_atrasado(app, monkeypatch):
    c = _cliente(app, monkeypatch,
                 SessaoComBancoAtrasado(novo_usuario(7, P.ADMINISTRATIVO_OBRA)),
                 usuario_id=7, perfil="ADMINISTRATIVO_OBRA")

    r = c.get("/erp/configuracoes")

    assert r.status_code == 403


def test_tela_comum_com_banco_atrasado_explica_em_vez_de_pagina_branca(app, monkeypatch):
    """Rota que carrega o Usuario pelo ORM: o erro do banco vira 503 legível."""
    c = _cliente(app, monkeypatch, SessaoComBancoAtrasado(novo_usuario(1, P.ADMIN)))

    r = c.get("/erp/api/titulos/1")          # exige escopo → carrega Usuario

    assert r.status_code in (500, 503)
    corpo = r.get_data(as_text=True)
    assert "Internal Server Error" not in corpo
    assert "escopo_visao" in corpo or "desatualizado" in corpo


def test_guarda_nao_usa_o_orm(app, monkeypatch):
    """Prova estrutural: a guarda inteira funciona numa sessão cujo ORM
    estoura, então ela não depende dele — e nunca voltará a depender sem
    quebrar este teste."""
    chamadas = []

    class Espia(SessaoComBancoAtrasado):
        def get(self, *a, **kw):
            chamadas.append("get")
            return super().get(*a, **kw)

    c = _cliente(app, monkeypatch, Espia(novo_usuario(1, P.FINANCEIRO)),
                 perfil="FINANCEIRO")
    r = c.get("/erp/pagamentos")             # página simples, sem ORM no corpo

    assert r.status_code == 200
    assert chamadas == []
