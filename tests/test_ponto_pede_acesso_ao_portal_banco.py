# -*- coding: utf-8 -*-
"""O CAMINHO INTEIRO: a pessoa pede acesso ao portal pelo celular do ponto, e o
DP libera no ERP — com Postgres de verdade.

Desenho do dono, 17/11/2026: *"que tal a partir do ponto eu requerer a
liberação do acesso ao portal? (…) Ela já tem o cadastro no ponto, e lá pelo
ponto ela vai informar o e-mail dela, na hora que ela pediu acesso ao portal."*

O que estes casos provam, e que o dublê de sessão não alcança:

  1. quem está autenticado no ponto (CPF + PIN) consegue pedir, e o pedido
     nasce com a obra em que ela BATE PONTO — não com a do cadastro, que
     envelhece;
  2. a tela inicial do aplicativo diz em que pé está o pedido;
  3. depois de o DP liberar, o aplicativo passa a dizer "você já tem acesso",
     com o e-mail — e não oferece pedir de novo;
  4. sem PIN, ninguém pede acesso em nome de ninguém.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, OBRA_A, _entrar_no_app,  # noqa: F401
                                           _schema_ponto2, app, como, mundo)

pytestmark = pytest.mark.banco


@pytest.fixture(autouse=True)
def _fila_vazia(banco):
    """A FILA COMEÇA E TERMINA VAZIA. As rotas do ponto confirmam a transação de
    verdade (não é a sessão desfeita do ERP), então o que um teste grava sobra
    para o próximo — e para os outros arquivos do mesmo trabalhador do xdist.
    Foi o que aconteceu na primeira rodada deste arquivo."""
    def limpar():
        with banco.connect() as conn:
            conn.execute(text("TRUNCATE portal_pedidos_de_acesso RESTART IDENTITY"))
            conn.commit()
    limpar()
    yield
    limpar()


def _bater(cpf, obra, minutos_atras=30):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import marcacoes
    with db.conexao() as conn:
        marcacoes.registrar(conn, cpf=cpf, obra=obra, origem="IDFACE", via_chave=True,
                            agora=horario.agora() - dt.timedelta(minutes=minutos_atras))


def _fila(banco):
    from app.apps.erp.core.auth import pedidos_de_portal as f
    with banco.connect() as conn:
        return f.listar(conn, status="TODOS")


def test_pede_pelo_celular_e_a_obra_vem_do_ponto(app, mundo, banco, monkeypatch):
    # O João é cadastrado na obra A, mas quem está batendo ponto é na B: é a B
    # que tem de valer, porque é onde ele está de verdade.
    _bater(CPF_JOAO, "PG-B", 60)
    _bater(CPF_JOAO, "PG-B", 30)
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)

    r = c.post("/ponto/app/api/portal/pedir", json={"email": "Joao.Silva@Gmail.com"})

    assert r.status_code == 200, r.get_json()
    [p] = _fila(banco)
    assert p["email"] == "joao.silva@gmail.com"
    assert p["obra_id"] == mundo["obra_b"], "a obra do pedido é a do PONTO"
    assert p["status"] == "PENDENTE" and p["origem"] == "PONTO"


def test_a_tela_do_celular_diz_em_que_pe_esta(app, mundo, banco, monkeypatch):
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    antes = c.get("/ponto/app/api/inicio").get_json()["portal"]
    assert antes["disponivel"] is True and antes["tem_acesso"] is False
    assert antes["pedido"] is None

    c.post("/ponto/app/api/portal/pedir", json={"email": "joao@gmail.com"})

    depois = c.get("/ponto/app/api/inicio").get_json()["portal"]
    assert depois["pedido"]["status"] == "PENDENTE"
    assert depois["pedido"]["email"] == "joao@gmail.com"


def test_email_torto_nao_vira_pedido(app, mundo, banco, monkeypatch):
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)

    r = c.post("/ponto/app/api/portal/pedir", json={"email": "joao arroba gmail"})

    assert r.status_code == 400
    assert _fila(banco) == []


@pytest.fixture
def admin(banco):
    """⚑ QUEM LIBERA ACESSO É ADMIN, NÃO O DP, e isto não é descuido do teste: a
    ação `gerir_usuarios` já era de administrador antes desta fila existir
    ("criar acesso ao sistema é ato de administrador, não de quem opera o DP",
    `api_colaborador_acesso`). A fila respeita a regra que já havia — mas vale
    dizer em voz alta que, com ela, o DP VÊ a fila e não resolve."""
    from app.apps.erp.core.auth.service import gerar_hash
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM usuarios WHERE email = 'admin.fila@teste.com'"))
        uid = conn.execute(text(
            "INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, perfil_id) "
            "VALUES ('Admin da fila', 'admin.fila@teste.com', :h, 'ADMIN', TRUE, "
            "        (SELECT id FROM perfis WHERE nome = 'Cargo ADMIN')) "
            "RETURNING id"), {"h": gerar_hash("x" * 12)}).scalar_one()
        conn.commit()
    yield uid
    _apagar_operador(banco, uid)


def _apagar_operador(banco, uid):
    """Apaga o operador de teste e a trilha dele.

    `eventos` é append-only por GATILHO em produção, de propósito — a trilha
    não se apaga. Aqui a limpeza precisa apagar, e a chave é a mesma do
    `conftest.py` e do `test_ponto_gestao_banco.py`:
    `session_replication_role = replica` desliga os gatilhos (e as chaves
    estrangeiras) só nesta transação.
    """
    with banco.connect() as conn:
        conn.execute(text("SET LOCAL session_replication_role = replica"))
        conn.execute(text("DELETE FROM usuario_obras WHERE usuario_id = :u"), {"u": uid})
        conn.execute(text("DELETE FROM usuario_convites WHERE usuario_id = :u"), {"u": uid})
        conn.execute(text("UPDATE colaboradores SET usuario_id = NULL WHERE usuario_id = :u"), {"u": uid})
        conn.execute(text("DELETE FROM eventos WHERE usuario_id = :u"), {"u": uid})
        conn.execute(text("DELETE FROM usuarios WHERE id = :u"), {"u": uid})
        conn.commit()


# ⚑ ESTE CASO VIROU O CONTRÁRIO EM 18/11/2026, por decisão dele: *"libera o DP
# também."* Antes, só ADMIN liberava (`gerir_usuarios`) — e na prática só ele
# próprio, enquanto o DP, que é quem trabalha a fila, não enxergava nem o
# cartão dela.
#
# O caso ficou porque a pergunta dele continua valendo, só mudou de lado: o DP
# ganhou a FILA, não a administração do sistema. Quem percorre o resto da
# alçada (o perfil que ele NÃO escolhe, a tela de operadores que ele não abre)
# é `tests/test_pedido_de_acesso_ao_portal_banco.py`.
def test_o_administrador_libera_a_fila_de_ponta_a_ponta(app, mundo, banco, admin,
                                                         monkeypatch):
    """25/11/2026: liberar é do administrador; o DP só vê."""
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    c.post("/ponto/app/api/portal/pedir", json={"email": "joao@gmail.com"})
    [p] = _fila(banco)

    assert como(app, mundo["dp"]).post(
        f"/erp/api/portal/pedidos-de-acesso/{p['id']}",
        json={"acao": "LIBERAR"}).status_code == 403
    r = como(app, admin).post(
        f"/erp/api/portal/pedidos-de-acesso/{p['id']}", json={"acao": "LIBERAR"})

    assert r.status_code == 200, r.get_json()
    assert _fila(banco)[0]["status"] == "LIBERADO"
    _apagar_operador(banco, r.get_json()["usuario_id"])


def test_quem_so_trata_ponto_nao_libera_acesso_nenhum(app, mundo, banco,
                                                      monkeypatch):
    """A alçada nova é do DP, não de quem responde pela obra."""
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    c.post("/ponto/app/api/portal/pedir", json={"email": "joao@gmail.com"})
    [p] = _fila(banco)

    sup = como(app, mundo["sup"])

    assert sup.get("/erp/api/portal/pedidos-de-acesso").status_code == 403
    assert sup.post(f"/erp/api/portal/pedidos-de-acesso/{p['id']}",
                    json={"acao": "LIBERAR"}).status_code == 403
    assert _fila(banco)[0]["status"] == "PENDENTE"


def test_depois_de_liberado_o_app_para_de_oferecer_e_diz_o_email(
        app, mundo, banco, admin, monkeypatch):
    _bater(CPF_JOAO, "PG-A", 30)
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    c.post("/ponto/app/api/portal/pedir", json={"email": "joao@gmail.com"})
    [p] = _fila(banco)

    r = como(app, admin).post(
        f"/erp/api/portal/pedidos-de-acesso/{p['id']}", json={"acao": "LIBERAR"})
    assert r.status_code == 200, r.get_json()

    estado = c.get("/ponto/app/api/inicio").get_json()["portal"]
    assert estado["tem_acesso"] is True and estado["email"] == "joao@gmail.com"
    # … e o acesso nasceu enxergando a obra em que ele bate ponto
    with banco.connect() as conn:
        obras = {x[0] for x in conn.execute(text(
            "SELECT obra_id FROM usuario_obras WHERE usuario_id = :u"),
            {"u": r.get_json()["usuario_id"]})}
    _apagar_operador(banco, r.get_json()["usuario_id"])
    assert mundo["obra_a"] in obras


# ---------------------------------------------------------------------------
# A OBRA SUGERIDA NO PEDIDO DO PORTAL (17/11/2026)
#
# Ele: *"quando eu falei em geolocalização, na verdade eu quis dizer mais onde é
# que a pessoa (…) de qual obra ela está cuidando."* Quem sabe responder é o
# ponto. A sugestão é SÓ sugestão: a caixinha continua aberta.
# ---------------------------------------------------------------------------
def test_a_obra_do_pedido_vem_de_onde_a_pessoa_bate_ponto(app, mundo, banco):
    from app.apps.erp.core.auth.service import gerar_hash
    # Operador ligado ao João, que é CADASTRADO na obra A e BATE na B.
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM usuarios WHERE email = 'joao.portal@teste.com'"))
        uid = conn.execute(text(
            "INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, "
            "                      perfil_id) "
            "VALUES ('João operador', 'joao.portal@teste.com', :h, "
            "        'ADMINISTRATIVO_OBRA', TRUE, "
            "        (SELECT id FROM perfis "
            "          WHERE nome = 'Cargo ADMINISTRATIVO_OBRA')) "
            "RETURNING id"),
            {"h": gerar_hash("x" * 12)}).scalar_one()
        conn.execute(text("UPDATE colaboradores SET usuario_id = :u WHERE cpf = :c"),
                     {"u": uid, "c": CPF_JOAO})
        conn.execute(text("INSERT INTO usuario_obras (usuario_id, obra_id) "
                          "VALUES (:u, :a), (:u, :b)"),
                     {"u": uid, "a": mundo["obra_a"], "b": mundo["obra_b"]})
        conn.commit()
    _bater(CPF_JOAO, "PG-B", 90)
    _bater(CPF_JOAO, "PG-B", 60)
    _bater(CPF_JOAO, "PG-A", 30)          # uma na A: a B continua sendo a mais batida

    try:
        d = como(app, uid).get("/erp/portal/api/inicio").get_json()

        assert d["obra_sugerida"] == mundo["obra_b"], (
            "a sugestão é a obra em que ela mais bate ponto, não a do cadastro")
        # … e a lista continua inteira: sugestão não é trava
        assert {o["id"] for o in d["obras_lista"]} >= {mundo["obra_a"], mundo["obra_b"]}
    finally:
        _apagar_operador(banco, uid)


def test_sem_batida_a_sugestao_cai_para_a_obra_do_cadastro(app, mundo, banco):
    from app.apps.erp.core.auth.service import gerar_hash
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM usuarios WHERE email = 'joao.portal2@teste.com'"))
        uid = conn.execute(text(
            "INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, "
            "                      perfil_id) "
            "VALUES ('João sem batida', 'joao.portal2@teste.com', :h, "
            "        'ADMINISTRATIVO_OBRA', TRUE, "
            "        (SELECT id FROM perfis "
            "          WHERE nome = 'Cargo ADMINISTRATIVO_OBRA')) "
            "RETURNING id"),
            {"h": gerar_hash("x" * 12)}).scalar_one()
        conn.execute(text("UPDATE colaboradores SET usuario_id = :u WHERE cpf = :c"),
                     {"u": uid, "c": CPF_JOAO})
        conn.execute(text("INSERT INTO usuario_obras (usuario_id, obra_id) VALUES (:u, :a)"),
                     {"u": uid, "a": mundo["obra_a"]})
        conn.commit()

    try:
        d = como(app, uid).get("/erp/portal/api/inicio").get_json()

        assert d["obra_sugerida"] == mundo["obra_a"]
    finally:
        _apagar_operador(banco, uid)


def test_sem_pin_ninguem_pede_por_ninguem(app, mundo):
    r = app.test_client().post("/ponto/app/api/portal/pedir",
                               json={"email": "qualquer@gmail.com"})

    assert r.status_code in (401, 403)
