"""A solicitação de suprimentos: cabeçalho, itens e acompanhamento.

Duas mudanças em relação à planilha de hoje, ambas pedidas pelo dono:
a obra é DO ITEM (uma solicitação pode pedir para obras diferentes) e o
acompanhamento é POR ITEM (os itens seguem caminhos diferentes).

O que não pode falhar:
  - pedido sem item, sem título ou com quantidade zerada não entra;
  - o fluxo recusa salto que não faz sentido — sem isso, um clique errado
    devolve um item de RECEBIDO para SOLICITAÇÃO e o histórico perde o sentido;
  - o escopo é o MESMO do financeiro: quem só vê os próprios lançamentos também
    só vê os próprios pedidos;
  - item fora do alcance responde "não encontrado", nunca "sem permissão".
"""
from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest
from flask import Flask

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import solicitacao as svc
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Insumo, Obra, PerfilUsuario as P, PrioridadeSolicitacao,
    StatusItemSuprimento as ST, SuprimentoItem, SuprimentoSolicitacao,
    UnidadeCompra, UsuarioObra,
)

from conftest import SessaoFalsa, novo_usuario

UN = UnidadeCompra(codigo="UN", descricao="Unidade")
M = UnidadeCompra(codigo="M", descricao="Metro")
INSUMO = Insumo(id=10, codigo="INS-0010", descricao="Tarucel p/ Junta", unidade="M")
OBRA_A = Obra(id=1, codigo="CREPETERRA", nome="Crepeterra")
OBRA_B = Obra(id=2, codigo="IGARASSU", nome="Igarassu")


def _base(*extras):
    return SessaoFalsa(UN, M, INSUMO, OBRA_A, OBRA_B, *extras)


def _item_bom(**troca):
    base = {"insumo_id": 10, "quantidade": "180", "unidade": "M", "obra_id": 1,
            "especificacao": "6mm"}
    base.update(troca)
    return base


# ---------------------------------------------------------------------------
# Criar
# ---------------------------------------------------------------------------
def test_cria_com_numero_proprio_e_itens_numerados():
    quem = novo_usuario(5, P.ADMINISTRATIVO_OBRA)
    s = _base(quem)

    sol = svc.criar(s, {"titulo": "  piso  concreto polido ",
                        "previsao_entrega": "2026-09-18", "prioridade": "ALTA",
                        "itens": [_item_bom(), _item_bom(obra_id=2, quantidade="12",
                                                         unidade="UN")]}, quem)

    assert sol.numero == "SS-0001"
    assert sol.titulo == "piso concreto polido"
    assert sol.prioridade is PrioridadeSolicitacao.ALTA
    itens = [o for o in s.adicionados if isinstance(o, SuprimentoItem)]
    assert [i.numero for i in itens] == [1, 2]
    assert [i.obra_id for i in itens] == [1, 2], "obras diferentes no mesmo pedido"
    assert all(i.status is ST.SOLICITACAO for i in itens)
    assert any(e.get("ac") == "CRIADA" for e in s.eventos)


def test_a_numeracao_continua_de_onde_parou():
    quem = novo_usuario(5, P.ADMINISTRATIVO_OBRA)
    s = _base(quem, SuprimentoSolicitacao(id=1, numero="SS-0041", titulo="x",
                                          solicitante_id=5))

    sol = svc.criar(s, {"titulo": "novo pedido", "itens": [_item_bom()]}, quem)

    assert sol.numero == "SS-0042"


def test_a_unidade_do_insumo_vale_quando_ninguem_informa():
    quem = novo_usuario(5, P.ADMINISTRATIVO_OBRA)
    s = _base(quem)

    svc.criar(s, {"titulo": "sem unidade", "itens": [_item_bom(unidade="")]}, quem)

    item = next(o for o in s.adicionados if isinstance(o, SuprimentoItem))
    assert item.unidade == "M", "veio do cadastro do insumo"


@pytest.mark.parametrize("dados,erro", [
    ({"titulo": "pedido de teste", "itens": []}, "pelo menos um item"),
    ({"titulo": "ab", "itens": [{}]}, "título"),
    ({"titulo": "pedido de teste", "itens": [{"insumo_id": 999, "quantidade": 1, "obra_id": 1}]}, "insumo"),
    ({"titulo": "pedido de teste", "itens": [{"insumo_id": 10, "quantidade": 1, "obra_id": 99}]}, "obra"),
    ({"titulo": "pedido de teste", "itens": [{"insumo_id": 10, "quantidade": "0", "obra_id": 1}]}, "maior que zero"),
    ({"titulo": "pedido de teste", "itens": [{"insumo_id": 10, "quantidade": "abc", "obra_id": 1}]}, "número"),
    ({"titulo": "pedido de teste", "previsao_entrega": "32/13/2026", "itens": [_item_bom()]}, "Data"),
    ({"titulo": "pedido de teste", "prioridade": "URGENTISSIMA", "itens": [_item_bom()]}, "Prioridade"),
])
def test_o_que_nao_entra(dados, erro):
    quem = novo_usuario(5, P.ADMINISTRATIVO_OBRA)
    with pytest.raises(ErroValidacao, match=erro):
        svc.criar(_base(quem), dados, quem)


def test_unidade_que_nao_existe_no_cadastro_e_recusada():
    quem = novo_usuario(5, P.ADMINISTRATIVO_OBRA)
    with pytest.raises(ErroValidacao, match="não existe no cadastro"):
        svc.criar(_base(quem), {"titulo": "pedido de teste",
                                "itens": [_item_bom(unidade="TONEL")]}, quem)


# ---------------------------------------------------------------------------
# O fluxo por item
# ---------------------------------------------------------------------------
def _com_item(status=ST.SOLICITACAO, solicitante=5):
    sol = SuprimentoSolicitacao(id=1, numero="SS-0001", titulo="pedido",
                                solicitante_id=solicitante)
    item = SuprimentoItem(id=7, solicitacao_id=1, numero=1, insumo_id=10,
                          quantidade=Decimal("10"), quantidade_recebida=Decimal("0"),
                          unidade="M", obra_id=1, status=status)
    return sol, item


def test_move_o_item_pelo_fluxo():
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item()
    s = _base(quem, sol, item)

    svc.mudar_situacao(s, 7, "COTACAO", quem)

    assert item.status is ST.COTACAO
    assert any(e.get("ac") == "SITUACAO" for e in s.eventos)


def test_salto_sem_sentido_e_recusado():
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item(ST.RECEBIDO)
    s = _base(quem, sol, item)

    with pytest.raises(ErroValidacao, match="Não dá para ir de Recebido"):
        svc.mudar_situacao(s, 7, "SOLICITACAO", quem)
    assert item.status is ST.RECEBIDO


@pytest.mark.parametrize("de", [ST.SOLICITACAO, ST.COTACAO, ST.EM_TRANSITO, ST.ENTREGUE])
def test_cancelar_e_suspender_valem_de_qualquer_lugar(de):
    """A realidade cancela a qualquer momento — o sistema não pode discordar."""
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item(de)
    s = _base(quem, sol, item)

    svc.mudar_situacao(s, 7, "CANCELADO", quem)

    assert item.status is ST.CANCELADO


def test_mudar_para_a_mesma_situacao_nao_faz_nada():
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item(ST.COTACAO)
    s = _base(quem, sol, item)

    svc.mudar_situacao(s, 7, "COTACAO", quem)

    assert s.eventos == []


def test_situacao_inventada_e_recusada():
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item()
    with pytest.raises(ErroValidacao, match="desconhecida"):
        svc.mudar_situacao(_base(quem, sol, item), 7, "VOANDO", quem)


def test_item_inexistente_responde_nao_encontrado():
    quem = novo_usuario(1, P.ADMIN)
    with pytest.raises(ErroNaoEncontrado):
        svc.mudar_situacao(_base(quem), 999, "COTACAO", quem)


def test_a_mudanca_trava_a_linha():
    import ast, inspect
    arvore = ast.parse(inspect.getsource(svc.mudar_situacao))
    assert any(
        isinstance(no, ast.Call) and getattr(no.func, "attr", "") == "get"
        and {kw.arg: getattr(kw.value, "value", None) for kw in no.keywords}
        == {"with_for_update": True, "populate_existing": True}
        for no in ast.walk(arvore)), "duas pessoas movendo o mesmo item se atropelam"


# ---------------------------------------------------------------------------
# Escopo: a mesma regra do financeiro
# ---------------------------------------------------------------------------
def test_quem_ve_tudo_ve_todos_os_itens():
    financeiro = novo_usuario(1, P.FINANCEIRO)
    sol, item = _com_item(solicitante=99)
    s = _base(financeiro, sol, item)

    assert len(svc.listar_itens(s, financeiro)) == 1


def test_quem_so_ve_os_proprios_nao_ve_o_pedido_do_outro():
    operador = novo_usuario(5, P.ADMINISTRATIVO_OBRA,
                            escopo_visao=EscopoVisao.PROPRIOS)
    sol, item = _com_item(solicitante=99)     # pedido de outra pessoa
    s = _base(operador, sol, item)

    assert svc.listar_itens(s, operador) == []


def test_quem_ve_as_obras_designadas_ve_o_pedido_da_obra_dele():
    operador = novo_usuario(5, P.ADMINISTRATIVO_OBRA,
                            escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    sol, item = _com_item(solicitante=99)     # pedido de outra pessoa, obra 1
    s = _base(operador, sol, item, UsuarioObra(id=1, usuario_id=5, obra_id=1))

    assert len(svc.listar_itens(s, operador)) == 1


def test_o_proprio_pedido_aparece_mesmo_fora_da_obra_designada():
    """Quem pediu sempre vê o que pediu — senão a pessoa perde o próprio pedido
    de vista ao ser remanejada de obra."""
    operador = novo_usuario(5, P.ADMINISTRATIVO_OBRA,
                            escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    sol, item = _com_item(solicitante=5)
    s = _base(operador, sol, item)            # sem obra designada nenhuma

    assert len(svc.listar_itens(s, operador)) == 1


def test_abrir_solicitacao_fora_do_alcance_responde_nao_encontrado():
    """Dizer 'sem permissão' confirmaria que a solicitação existe."""
    operador = novo_usuario(5, P.ADMINISTRATIVO_OBRA,
                            escopo_visao=EscopoVisao.PROPRIOS)
    sol, item = _com_item(solicitante=99)
    s = _base(operador, sol, item)

    with pytest.raises(ErroNaoEncontrado):
        svc.obter(s, 1, operador)


# ---------------------------------------------------------------------------
# Busca e filtro
# ---------------------------------------------------------------------------
def test_a_busca_cobre_titulo_insumo_e_especificacao():
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item()
    item.especificacao = "6mm"
    s = _base(quem, sol, item)

    assert len(svc.listar_itens(s, quem, busca="tarucel")) == 1
    assert len(svc.listar_itens(s, quem, busca="6mm")) == 1
    assert len(svc.listar_itens(s, quem, busca="SS-0001")) == 1
    assert svc.listar_itens(s, quem, busca="cimento") == []


def test_filtro_por_situacao_e_por_obra():
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item(ST.COTACAO)
    s = _base(quem, sol, item)

    assert len(svc.listar_itens(s, quem, status="COTACAO")) == 1
    assert svc.listar_itens(s, quem, status="RECEBIDO") == []
    assert len(svc.listar_itens(s, quem, obra_id=1)) == 1
    assert svc.listar_itens(s, quem, obra_id=2) == []


def test_a_lista_traz_o_saldo_e_os_proximos_passos():
    quem = novo_usuario(1, P.ADMIN)
    sol, item = _com_item(ST.ENTREGUE)
    item.quantidade_recebida = Decimal("4")
    s = _base(quem, sol, item)

    linha = svc.listar_itens(s, quem)[0]

    assert linha["saldo"] == "6"
    assert "RECEBIDO" in linha["proximas"] and "PENDENCIA" in linha["proximas"]
    assert linha["status_rotulo"] == "Entregue"


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


def test_mover_item_que_a_pessoa_nao_enxerga_responde_404(monkeypatch):
    operador = novo_usuario(5, P.ADMINISTRATIVO_OBRA,
                            escopo_visao=EscopoVisao.PROPRIOS)
    sol, item = _com_item(solicitante=99)
    s = _base(operador, sol, item)
    c = _cliente(s, monkeypatch, 5)

    r = c.post("/erp/api/suprimentos/itens/7/situacao", json={"status": "COTACAO"})

    assert r.status_code == 404, "403 num id que existe confirmaria a existência"
    assert item.status is ST.SOLICITACAO


def test_quem_nao_solicita_nao_cria_pedido(monkeypatch):
    financeiro = novo_usuario(1, P.FINANCEIRO)   # vê suprimentos, não pede material
    s = _base(financeiro)
    c = _cliente(s, monkeypatch, 1)

    assert c.post("/erp/api/suprimentos/solicitacoes",
                  json={"titulo": "x", "itens": []}).status_code == 403
    assert c.get("/erp/api/suprimentos/solicitacoes").status_code == 200
