"""A PESSOA TROCA A PRÓPRIA SENHA.

Dono, 17/09/2026: *"eu sou um usuário, e se eu quiser alterar a minha senha de
usuário, onde é que eu consigo fazer isso? Eu não encontrei"*. Não dava: só um
ADMIN trocava senha, pelo cadastro de operadores — o que obriga a pessoa a
contar a senha nova para alguém, ou a ficar para sempre com a que recebeu.

O que estes testes seguram é a parte perigosa de deixar qualquer um trocar
senha:

  1. **Exige a senha atual.** Sem isso, computador destravado vira conta tomada:
     quem passasse pela mesa trocaria a senha e fecharia o dono para fora.
  2. **Troca a senha de QUEM ESTÁ LOGADO**, e de mais ninguém.
  3. **A senha antiga para de valer e a nova passa a valer** — é o que prova que
     a troca aconteceu de verdade, e não só devolveu "ok".

COM BANCO DE VERDADE porque a rota grava no usuário e relê o hash.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash, verificar_senha
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Usuario
from tests.conftest import como

pytestmark = pytest.mark.banco

SENHA = "senha-de-teste-123"


@pytest.fixture
def pessoas(sessao_real):
    s = sessao_real
    eu = Usuario(nome="Operador", email="troca.senha@teste.local", ativo=True,
                 senha_hash=gerar_hash(SENHA), perfil=P.LANCADOR)
    outro = Usuario(nome="Colega", email="colega.senha@teste.local", ativo=True,
                    senha_hash=gerar_hash("outra-senha-456"), perfil=P.LANCADOR)
    s.add_all([eu, outro])
    s.flush()
    return {"s": s, "eu": eu, "outro": outro}


def _trocar(app, usuario_id, **corpo):
    return como(app, usuario_id).post("/erp/api/minha-senha", json=corpo)


def test_troca_a_senha_e_a_nova_passa_a_valer(app_real, pessoas):
    s, eu = pessoas["s"], pessoas["eu"]
    r = _trocar(app_real, eu.id, senha_atual=SENHA,
                senha_nova="nova-senha-987", senha_repetida="nova-senha-987")
    assert r.status_code == 200, r.get_data(as_text=True)
    assert verificar_senha("nova-senha-987", s.get(Usuario, eu.id).senha_hash)
    assert not verificar_senha(SENHA, s.get(Usuario, eu.id).senha_hash), \
        "a antiga tem de parar de valer"


def test_sem_a_senha_atual_nao_troca(app_real, pessoas):
    """A trava que impede a conta ser tomada num computador destravado."""
    s, eu = pessoas["s"], pessoas["eu"]
    r = _trocar(app_real, eu.id, senha_atual="chute-qualquer",
                senha_nova="nova-senha-987", senha_repetida="nova-senha-987")
    assert r.status_code == 400
    assert "não confere" in r.get_json()["erro"]
    assert verificar_senha(SENHA, s.get(Usuario, eu.id).senha_hash)


def test_repeticao_diferente_nao_troca(app_real, pessoas):
    """Senha nova digitada errada duas vezes trancaria a pessoa fora."""
    s, eu = pessoas["s"], pessoas["eu"]
    r = _trocar(app_real, eu.id, senha_atual=SENHA,
                senha_nova="nova-senha-987", senha_repetida="nova-senha-000")
    assert r.status_code == 400
    assert verificar_senha(SENHA, s.get(Usuario, eu.id).senha_hash)


def test_senha_curta_e_recusada(app_real, pessoas):
    s, eu = pessoas["s"], pessoas["eu"]
    r = _trocar(app_real, eu.id, senha_atual=SENHA,
                senha_nova="1234", senha_repetida="1234")
    assert r.status_code == 400
    assert "8" in r.get_json()["erro"]
    assert verificar_senha(SENHA, s.get(Usuario, eu.id).senha_hash)


def test_a_mesma_senha_de_novo_e_recusada(app_real, pessoas):
    r = _trocar(app_real, pessoas["eu"].id, senha_atual=SENHA,
                senha_nova=SENHA, senha_repetida=SENHA)
    assert r.status_code == 400


def test_a_troca_nao_encosta_na_senha_de_outra_pessoa(app_real, pessoas):
    s, eu, outro = pessoas["s"], pessoas["eu"], pessoas["outro"]
    _trocar(app_real, eu.id, senha_atual=SENHA,
            senha_nova="nova-senha-987", senha_repetida="nova-senha-987")
    assert verificar_senha("outra-senha-456", s.get(Usuario, outro.id).senha_hash)


def test_a_troca_fica_na_trilha(app_real, pessoas):
    """Senha trocada é evento de segurança: tem de ficar registrado — e o que
    fica registrado nunca é a senha."""
    from app.apps.erp.db.models.financeiro import Evento
    s, eu = pessoas["s"], pessoas["eu"]
    _trocar(app_real, eu.id, senha_atual=SENHA,
            senha_nova="nova-senha-987", senha_repetida="nova-senha-987")
    evento = s.query(Evento).filter_by(acao="SENHA_TROCADA_PELO_PROPRIO").first()
    assert evento is not None and evento.entidade_id == eu.id
    assert "nova-senha-987" not in str(evento.detalhe)
