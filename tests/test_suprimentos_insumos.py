"""Pedir o cadastro de um insumo, e alguém decidir.

Cadastro de insumo aberto a todo mundo é como uma base de suprimentos apodrece:
em um mês existem "Cimento CP-II", "cimento cp 2" e "CIMENTO CPII 50KG" como
três insumos, cada um com uma conta do plano, e os relatórios param de somar
coisa que preste. Por isso o procedimento é pedir → decidir → avisar.

O que não pode falhar:
  - pedir algo que JÁ EXISTE é recusado, com o nome do que existe;
  - pedir duas vezes a mesma coisa não gera dois pedidos;
  - cadastrar sem conta do plano é recusado — sem ela o pedido de compra não
    vira previsão apropriada;
  - recusar exige motivo escrito;
  - decidir duas vezes o mesmo pedido não passa;
  - quem pediu é avisado, e uma falha no aviso não desfaz o cadastro.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import insumos as svc
from app.apps.erp.db.models.cadastros import (
    Insumo, InsumoSolicitacao, PerfilUsuario as P, StatusSolicitacaoInsumo,
)

from conftest import SessaoFalsa, novo_usuario


@pytest.fixture
def quem_pede():
    return novo_usuario(5, P.ADMINISTRATIVO_OBRA, nome="Amanda", telefone="85999990000")


@pytest.fixture
def quem_decide():
    return novo_usuario(1, P.ADMIN, nome="Marcelo")


@pytest.fixture(autouse=True)
def telegram_mudo(monkeypatch):
    """Nenhum teste manda mensagem de verdade. Quem quiser conferir o aviso
    troca por uma função que registra."""
    import sys, types
    modulo = types.ModuleType("app.apps.notificador")
    modulo.enviar_telegram = lambda **kw: {"ok": True}
    monkeypatch.setitem(sys.modules, "app.apps.notificador", modulo)
    return modulo


# ---------------------------------------------------------------------------
# Pedir
# ---------------------------------------------------------------------------
def test_pedir_cria_o_registro_pendente(quem_pede):
    s = SessaoFalsa(quem_pede)

    pedido = svc.solicitar(s, {"descricao": "  Tarucel  p/ Junta ",
                               "justificativa": "piso da CREPETERRA",
                               "unidade": "m"}, quem_pede)

    assert pedido.descricao == "Tarucel p/ Junta", "espaço repetido é limpo"
    assert pedido.unidade == "M"
    assert pedido.status is StatusSolicitacaoInsumo.PENDENTE
    assert pedido.solicitante_id == 5
    assert any(e.get("ac") == "SOLICITADO" for e in s.eventos)


def test_pedir_o_que_ja_existe_e_recusado_dizendo_qual(quem_pede):
    s = SessaoFalsa(quem_pede, Insumo(id=2, codigo="INS-0002", descricao="Pó de Pedra"))

    with pytest.raises(ErroValidacao, match="INS-0002"):
        svc.solicitar(s, {"descricao": "pó   de pedra"}, quem_pede)


def test_pedir_duas_vezes_a_mesma_coisa_nao_gera_dois_pedidos(quem_pede):
    ja_pedido = InsumoSolicitacao(id=1, descricao="Cola PU", solicitante_id=5,
                                  status=StatusSolicitacaoInsumo.PENDENTE)
    s = SessaoFalsa(quem_pede, ja_pedido)

    with pytest.raises(ErroValidacao, match="aguardando decisão"):
        svc.solicitar(s, {"descricao": "COLA PU"}, quem_pede)


@pytest.mark.parametrize("descricao", ["", "  ", "ab"])
def test_descricao_curta_demais_e_recusada(quem_pede, descricao):
    with pytest.raises(ErroValidacao, match="mínimo 3"):
        svc.solicitar(SessaoFalsa(quem_pede), {"descricao": descricao}, quem_pede)


# ---------------------------------------------------------------------------
# Decidir
# ---------------------------------------------------------------------------
def _pedido_pendente():
    return InsumoSolicitacao(id=7, descricao="Cola/Selante PU 800ml",
                             solicitante_id=5, unidade="UN",
                             status=StatusSolicitacaoInsumo.PENDENTE)


def test_cadastrar_cria_o_insumo_e_fecha_o_pedido(quem_pede, quem_decide):
    pedido = _pedido_pendente()
    s = SessaoFalsa(quem_pede, quem_decide, pedido)

    insumo = svc.cadastrar(s, 7, {"descricao": "Cola/Selante PU Sache 800ml",
                                  "categoria_insumo_id": 3, "categoria_id": 50},
                           quem_decide)

    assert insumo.codigo == "INS-0001"
    assert insumo.categoria_insumo_id == 3 and insumo.categoria_id == 50
    assert insumo.unidade == "UN", "a unidade do pedido vale se ninguém trocar"
    assert pedido.status is StatusSolicitacaoInsumo.CADASTRADO
    assert pedido.insumo_id is insumo.id or pedido.insumo_id == insumo.id
    assert pedido.decidido_por == 1 and pedido.decidido_em is not None


def test_cadastrar_sem_conta_do_plano_e_recusado(quem_pede, quem_decide):
    s = SessaoFalsa(quem_pede, quem_decide, _pedido_pendente())

    with pytest.raises(ErroValidacao, match="conta do plano"):
        svc.cadastrar(s, 7, {"categoria_insumo_id": 3}, quem_decide)


def test_cadastrar_sem_categoria_de_insumo_e_recusado(quem_pede, quem_decide):
    s = SessaoFalsa(quem_pede, quem_decide, _pedido_pendente())

    with pytest.raises(ErroValidacao, match="categoria de insumo"):
        svc.cadastrar(s, 7, {"categoria_id": 50}, quem_decide)


def test_cadastrar_com_nome_que_ja_existe_e_recusado(quem_pede, quem_decide):
    s = SessaoFalsa(quem_pede, quem_decide, _pedido_pendente(),
                    Insumo(id=9, codigo="INS-0009", descricao="Cimento CP-II"))

    with pytest.raises(ErroValidacao, match="INS-0009"):
        svc.cadastrar(s, 7, {"descricao": "cimento cp-ii", "categoria_insumo_id": 3,
                             "categoria_id": 50}, quem_decide)


def test_recusar_exige_motivo(quem_pede, quem_decide):
    s = SessaoFalsa(quem_pede, quem_decide, _pedido_pendente())

    with pytest.raises(ErroValidacao, match="motivo"):
        svc.recusar(s, 7, "não", quem_decide)


def test_recusar_com_motivo_fecha_o_pedido(quem_pede, quem_decide):
    pedido = _pedido_pendente()
    s = SessaoFalsa(quem_pede, quem_decide, pedido)

    svc.recusar(s, 7, "Já existe equivalente: use o Selante PU 400ml.", quem_decide)

    assert pedido.status is StatusSolicitacaoInsumo.RECUSADO
    assert "Selante PU 400ml" in pedido.motivo
    assert any(e.get("ac") == "RECUSADO" for e in s.eventos)


def test_decidir_duas_vezes_o_mesmo_pedido_nao_passa(quem_pede, quem_decide):
    pedido = _pedido_pendente()
    pedido.status = StatusSolicitacaoInsumo.CADASTRADO
    s = SessaoFalsa(quem_pede, quem_decide, pedido)

    with pytest.raises(ErroValidacao, match="já foi resolvido"):
        svc.cadastrar(s, 7, {"categoria_insumo_id": 3, "categoria_id": 50}, quem_decide)


def test_pedido_inexistente_responde_nao_encontrado(quem_decide):
    with pytest.raises(ErroNaoEncontrado):
        svc.recusar(SessaoFalsa(quem_decide), 999, "motivo suficiente", quem_decide)


def test_a_decisao_trava_a_linha():
    """Duas pessoas decidindo o mesmo pedido no mesmo segundo criariam dois
    insumos. A trava é conferida lendo o código, para não sumir num refactor."""
    import ast, inspect
    arvore = ast.parse(inspect.getsource(svc._obter_pendente))
    achou = any(
        isinstance(no, ast.Call) and getattr(no.func, "attr", "") == "get"
        and {kw.arg: getattr(kw.value, "value", None) for kw in no.keywords}
        == {"with_for_update": True, "populate_existing": True}
        for no in ast.walk(arvore))
    assert achou, "decidir cadastro de insumo sem FOR UPDATE gera insumo duplicado"


# ---------------------------------------------------------------------------
# O aviso a quem pediu
# ---------------------------------------------------------------------------
def test_quem_pediu_e_avisado_do_cadastro(quem_pede, quem_decide, telegram_mudo):
    enviados = []
    telegram_mudo.enviar_telegram = lambda **kw: (enviados.append(kw) or {"ok": True})
    pedido = _pedido_pendente()
    s = SessaoFalsa(quem_pede, quem_decide, pedido)

    svc.cadastrar(s, 7, {"categoria_insumo_id": 3, "categoria_id": 50}, quem_decide)

    assert len(enviados) == 1
    assert "cadastrado" in enviados[0]["mensagem"].lower()
    assert pedido.avisado_em is not None


def test_o_aviso_da_recusa_leva_o_motivo(quem_pede, quem_decide, telegram_mudo):
    enviados = []
    telegram_mudo.enviar_telegram = lambda **kw: (enviados.append(kw) or {"ok": True})
    s = SessaoFalsa(quem_pede, quem_decide, _pedido_pendente())

    svc.recusar(s, 7, "Use o Selante PU 400ml que já existe.", quem_decide)

    assert "Selante PU 400ml" in enviados[0]["mensagem"]


def test_falha_no_aviso_nao_desfaz_o_cadastro(quem_pede, quem_decide, telegram_mudo):
    """O insumo já está criado; perder o aviso é chato, perder o cadastro é pior.
    Quem ficou sem aviso aparece com `avisado` em branco na lista."""
    def explode(**kw):
        raise RuntimeError("telegram fora do ar")
    telegram_mudo.enviar_telegram = explode
    pedido = _pedido_pendente()
    s = SessaoFalsa(quem_pede, quem_decide, pedido)

    insumo = svc.cadastrar(s, 7, {"categoria_insumo_id": 3, "categoria_id": 50},
                           quem_decide)

    assert insumo.codigo == "INS-0001"
    assert pedido.status is StatusSolicitacaoInsumo.CADASTRADO
    assert pedido.avisado_em is None


def test_sem_telefone_e_sem_cpf_nao_tenta_avisar(quem_decide, telegram_mudo):
    def explode(**kw):
        raise AssertionError("não deveria tentar enviar")
    telegram_mudo.enviar_telegram = explode
    sem_contato = novo_usuario(5, P.ADMINISTRATIVO_OBRA, nome="Sem contato")
    s = SessaoFalsa(sem_contato, quem_decide, _pedido_pendente())

    svc.cadastrar(s, 7, {"categoria_insumo_id": 3, "categoria_id": 50}, quem_decide)


# ---------------------------------------------------------------------------
# As rotas
# ---------------------------------------------------------------------------
def _cliente(sessao, monkeypatch, usuario_id):
    from app.apps.erp import routes
    app = Flask(__name__)
    app.secret_key = "teste"
    app.register_blueprint(routes.bp)
    monkeypatch.setattr(routes, "get_session",
                        lambda: contextlib.nullcontext(sessao))
    c = app.test_client()
    with c.session_transaction() as web:
        web["erp_usuario_id"] = usuario_id
    return c


def test_a_obra_pede_mas_nao_decide(quem_pede, monkeypatch):
    s = SessaoFalsa(quem_pede, _pedido_pendente())
    c = _cliente(s, monkeypatch, 5)

    assert c.post("/erp/api/suprimentos/insumos/solicitacoes",
                  json={"descricao": "Item novo de obra"}).status_code == 200
    assert c.post("/erp/api/suprimentos/insumos/solicitacoes/7",
                  json={"acao": "recusar", "motivo": "não quero"}).status_code == 403


def test_decidir_sem_dizer_o_que_e_recusado(quem_pede, quem_decide, monkeypatch):
    s = SessaoFalsa(quem_pede, quem_decide, _pedido_pendente())
    c = _cliente(s, monkeypatch, 1)

    r = c.post("/erp/api/suprimentos/insumos/solicitacoes/7", json={})

    assert r.status_code == 400
    assert "cadastrar ou recusar" in r.get_json()["erro"].lower()


def test_a_lista_mostra_quem_pediu_e_o_que_falta_avisar(quem_pede, quem_decide, monkeypatch):
    s = SessaoFalsa(quem_pede, quem_decide, _pedido_pendente())
    c = _cliente(s, monkeypatch, 1)

    r = c.get("/erp/api/suprimentos/insumos/solicitacoes?pendentes=1")

    assert r.status_code == 200
    linha = r.get_json()["solicitacoes"][0]
    assert linha["solicitante"] == "Amanda" and linha["status"] == "PENDENTE"
    assert linha["avisado"] is False
