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
    # 23/11/2026: o CPF vem do cadastro, como o nome e o telefone
    assert novo.cpf == pessoa.cpf
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


# ---------------------------------------------------------------------------
# ⚑ O DP RESOLVE A FILA — decisão dele em 18/11/2026: *"libera o DP também."*
#
# A pergunta que estes dois casos respondem não é "o DP consegue?" (consegue,
# é uma linha na tabela de permissões). É a que vem junto: **o DP ganhou
# SÓ isso?** Liberar acesso cria operador do sistema. Se a rota aceitasse o
# perfil que vem do navegador, o DP mandaria "ADMIN" na mão e criaria um
# administrador por uma fila de acesso de obra — alçada de ADMIN conquistada
# por um botão de obra, sem ninguém notar.
# ---------------------------------------------------------------------------
@pytest.fixture
def pessoal(sessao_real):
    """Alguém do DP: tem a fila, NÃO tem a tela de operadores."""
    from app.apps.erp.core.auth.service import gerar_hash
    u = Usuario(nome="DP de verdade", email="dp.pessoal@teste.com",
                senha_hash=gerar_hash("x" * 12), ativo=True,
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL)
    sessao_real.add(u)
    sessao_real.flush()
    return u


def test_o_dp_enxerga_e_resolve_a_fila_do_portal(app_real, sessao_real,
                                                 pessoa, pessoal):
    from tests.conftest import como                      # type: ignore
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com")

    c = como(app_real, pessoal.id)

    r = c.get("/erp/api/portal/pedidos-de-acesso")
    assert r.status_code == 200
    assert [p["id"] for p in r.get_json()["pedidos"]] == [pedido["id"]]

    r = c.post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
               json={"acao": "LIBERAR"})
    assert r.status_code == 200, r.get_json()
    assert fila.listar(_conn(sessao_real)) == []


def test_o_dp_NAO_escolhe_o_perfil_do_acesso_que_libera(app_real, sessao_real,
                                                        pessoa, pessoal):
    """A trava contra a escalada: o DP pede ADMIN e leva administrativo de obra."""
    from tests.conftest import como                      # type: ignore
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com")

    r = como(app_real, pessoal.id).post(
        f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
        json={"acao": "LIBERAR", "perfil": "ADMIN"})
    assert r.status_code == 200, r.get_json()

    novo = sessao_real.get(Colaborador, pessoa.id).usuario_id
    assert novo
    assert (sessao_real.get(Usuario, novo).perfil
            == PerfilUsuario.ADMINISTRATIVO_OBRA)


def test_a_fila_do_portal_NAO_da_ao_dp_a_tela_de_operadores(app_real, pessoal):
    """O DP ganhou uma fila, não a administração do sistema."""
    from tests.conftest import como                      # type: ignore
    c = como(app_real, pessoal.id)

    assert c.get("/erp/api/usuarios").status_code == 403
    assert c.post("/erp/api/usuarios", json={"nome": "Eu mesmo",
                                             "email": "eu@teste.com",
                                             "perfil": "ADMIN"}).status_code == 403


def test_quem_gere_operadores_continua_enxergando_a_fila(app_real, sessao_real,
                                                         pessoa, dp):
    """A implicação: ADMIN não perdeu a fila ao ela virar ação própria."""
    from tests.conftest import como                      # type: ignore
    fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                   cpf=pessoa.cpf, nome=pessoa.nome, email="maria@gmail.com")

    r = como(app_real, dp.id).get("/erp/api/portal/pedidos-de-acesso")
    assert r.status_code == 200
    assert len(r.get_json()["pedidos"]) == 1


# ---------------------------------------------------------------------------
# ⚑ LIBERAR É CONFIGURAR O ACESSO (18/11/2026)
#
# Ele, fechando o ciclo: *"o administrador vai liberar a pessoa, mas tem que
# definir o acesso dele, o perfil, as obras, etc e tal. Eu acho que assim fecha
# essa parte de liberação e forma um ciclo perfeito."*
#
# O que estes casos prendem é o equilíbrio: quem gere operadores ESCOLHE, quem
# só resolve a fila NÃO — nem perfil, nem alcance. Deixar escolher obra sem
# poder escolher perfil seria meia porta.
# ---------------------------------------------------------------------------
def test_quem_administra_escolhe_o_perfil_e_as_obras(app_real, sessao_real,
                                                     pessoa, dp, obra):
    from tests.conftest import como                      # type: ignore
    outra = Obra(codigo="OBR2L", nome="Segunda Obra", status="ATIVA")
    sessao_real.add(outra)
    sessao_real.flush()
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com", obra_id=obra.id)

    r = como(app_real, dp.id).post(
        f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
        json={"acao": "LIBERAR", "perfil": "GESTOR_OBRA",
              "obra_ids": [outra.id]})
    assert r.status_code == 200, r.get_json()

    novo = sessao_real.get(Colaborador, pessoa.id).usuario_id
    assert sessao_real.get(Usuario, novo).perfil == PerfilUsuario.GESTOR_OBRA
    alcance = {u.obra_id for u in sessao_real.query(UsuarioObra)
               .filter(UsuarioObra.usuario_id == novo).all()}
    assert outra.id in alcance, "a obra escolhida tem de valer"


def test_quem_administra_pode_dar_todas_as_obras(app_real, sessao_real,
                                                 pessoa, dp):
    from tests.conftest import como                      # type: ignore
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com")

    r = como(app_real, dp.id).post(
        f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
        json={"acao": "LIBERAR", "todas_as_obras": True})
    assert r.status_code == 200, r.get_json()

    novo = sessao_real.get(Colaborador, pessoa.id).usuario_id
    assert sessao_real.get(Usuario, novo).ve_todas_as_obras is True


def test_o_dp_nao_escolhe_obra_nem_alcance(app_real, sessao_real, pessoa,
                                           pessoal, obra):
    """A trava completa: o DP pediu outra obra e todas as obras, e levou a
    obra do cadastro, como sempre.

    ⚑ DESDE 18/11/2026 NENHUMA TELA MANDA ESSES CAMPOS — quem administra é
    levado para Configurações › Usuários e configura lá. A trava continua
    valendo, e este teste continua existindo, porque a rota segue aceitando
    os campos: quem souber montar o pedido na mão não passa por ela.
    """
    from tests.conftest import como                      # type: ignore
    outra = Obra(codigo="OBR3L", nome="Obra Que Ele Pediu", status="ATIVA")
    sessao_real.add(outra)
    sessao_real.flush()
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com", obra_id=obra.id)

    r = como(app_real, pessoal.id).post(
        f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
        json={"acao": "LIBERAR", "perfil": "ADMIN",
              "obra_ids": [outra.id], "todas_as_obras": True})
    assert r.status_code == 200, r.get_json()

    novo = sessao_real.get(Colaborador, pessoa.id).usuario_id
    u = sessao_real.get(Usuario, novo)
    assert u.perfil == PerfilUsuario.ADMINISTRATIVO_OBRA
    assert not u.ve_todas_as_obras
    alcance = {x.obra_id for x in sessao_real.query(UsuarioObra)
               .filter(UsuarioObra.usuario_id == novo).all()}
    assert alcance == {obra.id}, "o DP leva a obra do cadastro, e só"


def test_a_fila_diz_quem_administra_o_acesso(app_real, sessao_real,
                                            pessoa, dp, pessoal):
    """A tela usa isto para mandar quem administra direto para a configuração
    do operador, depois de liberar. Quem só resolve a fila fica onde está."""
    from tests.conftest import como                      # type: ignore
    fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                   cpf=pessoa.cpf, nome=pessoa.nome, email="maria@gmail.com")

    do_admin = como(app_real, dp.id).get(
        "/erp/api/portal/pedidos-de-acesso").get_json()
    do_dp = como(app_real, pessoal.id).get(
        "/erp/api/portal/pedidos-de-acesso").get_json()

    assert do_admin["administra_acesso"] is True
    assert do_dp["administra_acesso"] is False
    # A lista de perfis NÃO viaja mais nesta resposta: ela mora na tela de
    # Configurações, que é para onde quem administra é levado.
    assert "perfis" not in do_admin and "cargos" not in do_admin


def test_quem_administra_pode_liberar_mais_de_uma_obra(app_real, sessao_real,
                                                       pessoa, dp, obra):
    """*"o administrador (…) tem que definir o acesso dele, o perfil, as
    obras"* — AS obras. Quem responde por três não sai daqui com uma."""
    from tests.conftest import como                      # type: ignore
    segunda = Obra(codigo="OBR4L", nome="Segunda Obra Dela", status="ATIVA")
    terceira = Obra(codigo="OBR5L", nome="Terceira Obra Dela", status="ATIVA")
    sessao_real.add_all([segunda, terceira])
    sessao_real.flush()
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com", obra_id=obra.id)

    r = como(app_real, dp.id).post(
        f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
        json={"acao": "LIBERAR", "obra_ids": [segunda.id, terceira.id]})
    assert r.status_code == 200, r.get_json()

    novo = sessao_real.get(Colaborador, pessoa.id).usuario_id
    alcance = {x.obra_id for x in sessao_real.query(UsuarioObra)
               .filter(UsuarioObra.usuario_id == novo).all()}
    assert alcance == {segunda.id, terceira.id}


def test_lista_de_obras_vazia_e_escolha_nao_esquecimento(app_real, sessao_real,
                                                         pessoa, dp, obra):
    """Desmarcar tudo libera um acesso sem obra (o caso do escritório). Antes
    isto caía no padrão e a pessoa saía enxergando a obra do cadastro — o
    contrário do que quem decidiu marcou na tela."""
    from tests.conftest import como                      # type: ignore
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="maria@gmail.com", obra_id=obra.id)

    r = como(app_real, dp.id).post(
        f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
        json={"acao": "LIBERAR", "obra_ids": []})
    assert r.status_code == 200, r.get_json()

    novo = sessao_real.get(Colaborador, pessoa.id).usuario_id
    alcance = [x.obra_id for x in sessao_real.query(UsuarioObra)
               .filter(UsuarioObra.usuario_id == novo).all()]
    assert alcance == []


def test_o_cpf_do_operador_se_corrige_na_edicao(app_real, sessao_real, pessoa, dp, obra):
    """23/11/2026, ele: *"coloquei (…), salvei, e o CPF não ficou preenchido na
    tela de operadores."* A edição ignorava o CPF — só o cadastro novo gravava."""
    from tests.conftest import como                      # type: ignore
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="cpf@gmail.com", obra_id=obra.id)
    c = como(app_real, dp.id)
    uid = c.post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
                 json={"acao": "LIBERAR"}).get_json()["usuario_id"]
    assert c.post(f"/erp/api/usuarios/{uid}", json={"cpf": "123"}).status_code == 400
    r = c.post(f"/erp/api/usuarios/{uid}", json={"cpf": "529.982.247-25"})
    assert r.status_code == 200, r.get_json()
    sessao_real.expire_all()
    assert sessao_real.get(Usuario, uid).cpf == "52998224725"
    # vazio não apaga
    c.post(f"/erp/api/usuarios/{uid}", json={"cpf": ""})
    sessao_real.expire_all()
    assert sessao_real.get(Usuario, uid).cpf == "52998224725"


def test_o_alcance_acompanha_a_obra_onde_a_pessoa_bate_ponto(app_real, sessao_real,
                                                             pessoa, dp, obra):
    """23/11/2026, ele: *"A obra que aquela pessoa está batendo ponto, ele já
    teria acesso."* Dinâmico: muda a obra no ponto, muda o alcance; a marcada
    à mão continua."""
    from sqlalchemy import text as _t
    from tests.conftest import como                      # type: ignore
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id,
                            cpf=pessoa.cpf, nome=pessoa.nome,
                            email="ponto@gmail.com", obra_id=obra.id)
    uid = como(app_real, dp.id).post(
        f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
        json={"acao": "LIBERAR"}).get_json()["usuario_id"]
    b = Obra(codigo="OBRPTB", nome="Obra B", status="ATIVA")
    c = Obra(codigo="OBRPTC", nome="Obra C", status="ATIVA")
    sessao_real.add_all([b, c])
    sessao_real.flush()
    sessao_real.expire_all()
    u = sessao_real.get(Usuario, uid)
    assert set(obras_de_registro_sem_autor(sessao_real, u)) == {obra.id}

    sessao_real.execute(_t("INSERT INTO colaborador_obra_atual (colaborador_id, obra_id) "
                           "VALUES (:c, :o)"), {"c": pessoa.id, "o": b.id})
    assert set(obras_de_registro_sem_autor(sessao_real, u)) == {obra.id, b.id}

    sessao_real.execute(_t("UPDATE colaborador_obra_atual SET obra_id = :o "
                           "WHERE colaborador_id = :c"), {"c": pessoa.id, "o": c.id})
    assert set(obras_de_registro_sem_autor(sessao_real, u)) == {obra.id, c.id}, \
        "a obra antiga do ponto sai; a marcada à mão fica"


def test_administrativo_liberado_pelo_portal_so_ve_o_pessoal_da_obra_dele(
        app_real, sessao_real, pessoa, obra, dp, pessoal):
    """24/11/2026, ele: *"estão vendo (…) as solicitações do DP (…) 2
    desligamentos (…) que ele não tem nada a ver."* Quem enxerga por autoria
    ganhava "sem filtro" em toda tela do Pessoal. O DP continua vendo tudo."""
    from tests.conftest import como                      # type: ignore
    from app.apps.erp.core import pessoal_solicitacoes as svc
    outra = Obra(codigo="OBROUT", nome="Obra de outra pessoa", status="ATIVA")
    sessao_real.add(outra)
    sessao_real.flush()
    de_fora = Colaborador(nome="JOSE DE OUTRA OBRA", cpf="11144477735",
                          situacao="ATIVO", obra_id=outra.id)
    sessao_real.add(de_fora)
    sessao_real.flush()
    sol_fora = svc.abrir(sessao_real, {"tipo": "DESLIGAMENTO", "colaborador_id": de_fora.id,
                                       "obra_id": outra.id,
                                       "forma_desligamento": "Demissão com Aviso Trabalhado",
                                       "aviso_previo": "2026-12-01",
                                       "continua_prestador": "Não, está saindo da empresa"},
                         dp)
    sessao_real.commit()
    pedido = fila.registrar(_conn(sessao_real), colaborador_id=pessoa.id, cpf=pessoa.cpf,
                            nome=pessoa.nome, email="adm.obra@gmail.com", obra_id=obra.id)
    uid = como(app_real, dp.id).post(f"/erp/api/portal/pedidos-de-acesso/{pedido['id']}",
                                     json={"acao": "LIBERAR"}).get_json()["usuario_id"]
    sessao_real.commit()
    adm = como(app_real, uid)

    fila_dele = adm.get("/erp/api/dp/solicitacoes").get_json()
    assert sol_fora["id"] not in [x["id"] for x in fila_dele["solicitacoes"]]
    assert adm.get(f"/erp/api/dp/solicitacoes/{sol_fora['id']}").status_code == 404
    nomes = [c.get("nome") for c in adm.get("/erp/api/colaboradores").get_json()["colaboradores"]]
    assert "JOSE DE OUTRA OBRA" not in nomes and "MARIA DO PEDIDO" in nomes

    # o DP continua vendo a empresa inteira
    fila_dp = como(app_real, pessoal.id).get("/erp/api/dp/solicitacoes").get_json()
    assert sol_fora["id"] in [x["id"] for x in fila_dp["solicitacoes"]]
