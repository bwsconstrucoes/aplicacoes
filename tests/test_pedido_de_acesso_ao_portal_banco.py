"""A FILA DE PEDIDOS DE ACESSO AO PORTAL — com banco de verdade.

Por que `@pytest.mark.banco` e não o dublê: esta regra é toda `WHERE`, índice
único parcial e `UPDATE … WHERE status = 'PENDENTE'`. A `SessaoFalsa` ignora
`WHERE` (devolve todos os objetos do tipo pedido), então ela aprovaria um
código que libera o pedido errado. O que se quer provar aqui é justamente o
que ela não alcança.

Cobre, nesta ordem:
  1. e-mail torto não entra na fila;
  2. um pedido pendente por pessoa (e o e-mail do pendente se CORRIGE);
  3. quem já tem acesso não abre pedido — é "esqueci a senha";
  4. liberar cria o acesso COM o perfil de administrativo de obra, liga a obra
     que veio do ponto e tira o pedido da fila;
  5. recusar guarda o motivo e o motivo volta para quem pediu;
  6. pedido já decidido não se decide de novo;
  7. quem não tem `gerir_usuarios` não enxerga nem resolve a fila.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth import pedidos_de_portal as fila
from app.apps.erp.db.models.cadastros import (
    Colaborador, Obra, PerfilUsuario, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco


# ---------------------------------------------------------------------------
# Cenário
# ---------------------------------------------------------------------------
@pytest.fixture
def obra(sessao_real):
    o = Obra(codigo="OBRPT", nome="Obra do Pedido", status="ATIVA")
    sessao_real.add(o)
    sessao_real.flush()
    return o


@pytest.fixture
def pessoa(sessao_real, obra):
    c = Colaborador(nome="MARIA DO PEDIDO", cpf="39053344705", situacao="ATIVO",
                    obra_id=obra.id, telefone="81999990000")
    sessao_real.add(c)
    sessao_real.flush()
    return c


@pytest.fixture
def dp(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    u = Usuario(nome="DP que libera", email="dp.libera@teste.com",
                senha_hash=gerar_hash("x" * 12), perfil=PerfilUsuario.ADMIN,
                ativo=True)
    sessao_real.add(u)
    sessao_real.flush()
    return u


def _conn(sessao_real):
    return sessao_real.connection()


# ---------------------------------------------------------------------------
# 1. e-mail torto
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("ruim", ["", "maria", "maria@", "@gmail.com",
                                  "maria@gmail", "maria do pedido@gmail.com"])
def test_email_torto_nao_entra_na_fila(sessao_real, pessoa, ruim):
    r = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                       cpf=pessoa.cpf, nome=pessoa.nome, email=ruim)

    assert r["ok"] is False
    assert "@" in r["erro"]            # a recusa EXPLICA o que falta


def test_email_bom_entra(sessao_real, pessoa):
    r = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                       cpf=pessoa.cpf, nome=pessoa.nome,
                       email="Maria.Pedido@Gmail.COM ")

    assert r["ok"] is True
    [p] = fila.listar(_conn(sessao_real))
    assert p["email"] == "maria.pedido@gmail.com"      # minúsculas, sem espaço


# ---------------------------------------------------------------------------
# 2. um pendente por pessoa — e o e-mail do pendente se corrige
# ---------------------------------------------------------------------------
def test_pedir_duas_vezes_nao_duplica(sessao_real, pessoa):
    conn = _conn(sessao_real)
    a = fila.registrar(conn, colaborador_id=pessoa.id, cpf=pessoa.cpf,
                       nome=pessoa.nome, email="maria@gmail.com")
    b = fila.registrar(conn, colaborador_id=pessoa.id, cpf=pessoa.cpf,
                       nome=pessoa.nome, email="maria@gmail.com")

    assert b["id"] == a["id"] and b.get("repetido")
    assert len(fila.listar(conn)) == 1


def test_email_errado_se_corrige_no_pedido_que_espera(sessao_real, pessoa):
    conn = _conn(sessao_real)
    fila.registrar(conn, colaborador_id=pessoa.id, cpf=pessoa.cpf,
                   nome=pessoa.nome, email="maria@gmial.com")

    r = fila.registrar(conn, colaborador_id=pessoa.id, cpf=pessoa.cpf,
                       nome=pessoa.nome, email="maria@gmail.com")

    assert r.get("corrigido") is True
    [p] = fila.listar(conn)
    assert p["email"] == "maria@gmail.com"


# ---------------------------------------------------------------------------
# 3. quem já tem acesso não abre pedido
# ---------------------------------------------------------------------------
def test_quem_ja_tem_acesso_e_mandado_para_esqueci_a_senha(sessao_real, pessoa, dp):
    pessoa.usuario_id = dp.id
    sessao_real.flush()

    r = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                       cpf=pessoa.cpf, nome=pessoa.nome, email="x@gmail.com")

    assert r["ok"] is False and r["ja_tem"] is True
    assert "squeci a senha" in r["erro"]


# ---------------------------------------------------------------------------
# 4. liberar: a MESMA porta da ficha, com perfil e obra
# ---------------------------------------------------------------------------
def test_liberar_cria_acesso_com_administrativo_de_obra_e_a_obra_do_ponto(
        app_real, sessao_real, pessoa, obra, dp):
    from tests.conftest import como                      # type: ignore
    conn = _conn(sessao_real)
    # A obra do PEDIDO é outra, de propósito: é a que veio do ponto, e ela é
    # que tem de valer — não a do cadastro, que envelhece.
    outra = Obra(codigo="OBRPT2", nome="Obra onde ela bate", status="ATIVA")
    sessao_real.add(outra)
    sessao_real.flush()
    pedido = fila.registrar(conn, colaborador_id=pessoa.id, cpf=pessoa.cpf,
                            nome=pessoa.nome, email="maria@gmail.com",
                            obra_id=outra.id)

    c = como(app_real, dp.id)
    r = c.post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
               json={"acao": "LIBERAR"})

    assert r.status_code == 200, r.get_data(as_text=True)
    assert r.get_json()["ok"] is True
    sessao_real.expire_all()
    novo = sessao_real.get(Usuario, r.get_json()["usuario_id"])
    assert novo.email == "maria@gmail.com"
    assert novo.perfil == PerfilUsuario.ADMINISTRATIVO_OBRA
    assert sessao_real.get(Colaborador, pessoa.id).usuario_id == novo.id
    alcance = {u.obra_id for u in sessao_real.query(UsuarioObra)
               .filter(UsuarioObra.usuario_id == novo.id).all()}
    assert outra.id in alcance, "o acesso tem de nascer vendo onde ela bate ponto"
    assert fila.listar(_conn(sessao_real)) == [], "o pedido tem de sair da fila"


def test_liberar_so_quem_libera(app_real, sessao_real, pessoa, dp):
    from app.apps.erp.core.auth.service import gerar_hash
    from tests.conftest import como                      # type: ignore
    conn = _conn(sessao_real)
    pedido = fila.registrar(conn, colaborador_id=pessoa.id, cpf=pessoa.cpf,
                            nome=pessoa.nome, email="maria@gmail.com")
    zé = Usuario(nome="Administrativo", email="adm.obra@teste.com",
                 senha_hash=gerar_hash("x" * 12), ativo=True,
                 perfil=PerfilUsuario.ADMINISTRATIVO_OBRA)
    sessao_real.add(zé)
    sessao_real.flush()

    c = como(app_real, zé.id)

    assert c.get("/erp/api/portal/pedidos-de-acesso").status_code == 403
    assert c.post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
                  json={"acao": "LIBERAR"}).status_code == 403
    assert fila.listar(_conn(sessao_real))[0]["status"] == "PENDENTE"


# ---------------------------------------------------------------------------
# 5 e 6. recusar guarda o motivo; decidido não se decide de novo
# ---------------------------------------------------------------------------
def test_recusar_guarda_o_motivo_e_ele_volta_para_quem_pediu(
        app_real, sessao_real, pessoa, dp):
    from tests.conftest import como                      # type: ignore
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com")

    c = como(app_real, dp.id)
    r = c.post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
               json={"acao": "RECUSAR", "motivo": "não é da função dela"})

    assert r.status_code == 200 and r.get_json()["status"] == "RECUSADO"
    meu = fila.meu(_conn(sessao_real), pessoa.id)
    assert meu["status"] == "RECUSADO"
    assert meu["motivo"] == "não é da função dela"
    assert sessao_real.get(Colaborador, pessoa.id).usuario_id is None


def test_pedido_decidido_nao_se_decide_de_novo(app_real, sessao_real, pessoa, dp):
    from tests.conftest import como                      # type: ignore
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com")
    c = como(app_real, dp.id)
    c.post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
           json={"acao": "RECUSAR", "motivo": "x"})

    r = c.post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
               json={"acao": "LIBERAR"})

    assert r.status_code == 400
    assert "já foi decidido" in r.get_json()["erro"]


# ---------------------------------------------------------------------------
# 7. o pedido NÃO trava a exclusão do cadastro
#
# Era o que ele cobrou em 17/11: *"Não quero descobrir, quero EXCLUIR"*. Um
# pedido de acesso não é movimento da pessoa no sistema — a migração 108 põe
# `ON DELETE CASCADE` justamente para isso, e aqui se prova.
# ---------------------------------------------------------------------------
def test_pedido_de_acesso_nao_impede_apagar_o_cadastro(sessao_real, pessoa):
    from app.apps.erp.core.cadastros import pessoas as svc
    fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id, cpf=pessoa.cpf,
                   nome=pessoa.nome, email="maria@gmail.com")

    quem = svc._quem_aponta_para_colaborador(sessao_real)

    assert "portal_pedidos_de_acesso" not in quem, (
        "o pedido de acesso não pode entrar na lista do que impede apagar")


def test_apagar_o_cadastro_leva_o_pedido_junto(sessao_real, pessoa):
    conn = _conn(sessao_real)
    fila.registrar(conn, colaborador_id=pessoa.id, cpf=pessoa.cpf,
                   nome=pessoa.nome, email="maria@gmail.com")
    sessao_real.flush()

    conn.execute(text("DELETE FROM colaboradores WHERE id = :i"), {"i": pessoa.id})

    assert fila.listar(conn, status="TODOS") == []


# ---------------------------------------------------------------------------
# ⚑ SEM CONTA DE E-MAIL, O LINK AINDA SAI (18/11/2026)
#
# Ele perguntou se a liberação de acesso ao portal estava pronta, e ao conferir
# achei isto: sem a conta de e-mail do sistema configurada, a liberação
# ACONTECIA (o operador era criado, a tela dizia que deu certo) e **ninguém
# tinha como entrar** — nem a pessoa, que não recebia nada, nem quem liberou,
# que não tinha link para entregar.
#
# O código já fazia a coisa certa para o outro caso (servidor fora do ar):
# devolvia o link para entregar de outro jeito. A falta de conta merecia o
# mesmo tratamento, e merece MAIS: é o estado do sistema ANTES de alguém
# configurar o e-mail — ou seja, exatamente quando a primeira liberação
# acontece.
# ---------------------------------------------------------------------------
def test_sem_conta_de_email_o_convite_devolve_o_LINK(sessao_real, dp):
    from app.apps.erp.core.auth import convites
    from app.apps.erp.core.comum import email_sistema
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    from app.apps.erp.core.auth.service import gerar_hash

    # garante que NÃO há conta de e-mail do sistema usável
    assert email_sistema.conta(sessao_real) is None, (
        "este teste precisa do sistema SEM conta de e-mail configurada")

    pessoa = Usuario(nome="Quem Vai Entrar", email="quem@entrar.teste",
                     ativo=True, senha_hash=gerar_hash("provisoria-123456"),
                     perfil=PerfilUsuario.ADMINISTRATIVO_OBRA)
    sessao_real.add(pessoa)
    sessao_real.flush()

    r = convites.enviar(sessao_real, pessoa.id,
                        link_base="https://exemplo/erp/primeiro-acesso",
                        autor=dp)

    assert r["enviado"] is False
    assert "e-mail do sistema" in r["erro"].lower()
    # ⚠️ O QUE IMPORTA: o link existe, e é por ele que a pessoa entra.
    assert r["link"] and "/erp/primeiro-acesso/" in r["link"]
    assert r["para"] == "quem@entrar.teste"


def test_o_convite_do_link_sem_email_FUNCIONA_de_verdade(sessao_real, dp):
    """Link que volta e não funciona seria pior que link nenhum — daria a
    impressão de saída onde não há."""
    from app.apps.erp.core.auth import convites
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    from app.apps.erp.core.auth.service import gerar_hash

    pessoa = Usuario(nome="Quem Vai Entrar", email="quem2@entrar.teste",
                     ativo=True, senha_hash=gerar_hash("provisoria-123456"),
                     perfil=PerfilUsuario.ADMINISTRATIVO_OBRA)
    sessao_real.add(pessoa)
    sessao_real.flush()

    r = convites.enviar(sessao_real, pessoa.id,
                        link_base="https://exemplo/erp/primeiro-acesso",
                        autor=dp)
    token = r["link"].rsplit("/", 1)[-1]

    achado = convites.conferir(sessao_real, token)
    assert achado["usuario_id"] == pessoa.id

    convites.usar(sessao_real, token, "senha-escolhida-por-ela-1")
    sessao_real.flush()
    # e o link não serve duas vezes
    with pytest.raises(Exception):
        convites.conferir(sessao_real, token)
