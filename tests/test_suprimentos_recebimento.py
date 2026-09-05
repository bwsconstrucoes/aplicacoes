"""O material chegou na obra — e o que não chegou vira saldo.

Quem confere é a obra, não o suprimento. E a PENDÊNCIA aqui é o saldo do
próprio item, não um registro novo em outra tabela: com duas tabelas o mesmo
material passa a ter duas verdades e o histórico se perde.

O que não pode falhar:
  - receber mais do que foi pedido é recusado (sobra não é recebimento);
  - receber em partes deixa o item em PENDÊNCIA com o saldo certo, e o último
    recebimento fecha em RECEBIDO;
  - o cruzamento entre suprimento e financeiro AVISA, sem bloquear: parcela
    vencida com material não recebido, e material recebido sem título lançado.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import recebimento as svc
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Insumo, Obra, PedidoCompra, PedidoItem, PerfilUsuario as P,
    PrevisaoPagamento, Recebimento, RecebimentoItem, StatusItemSuprimento as ST,
    StatusPedidoCompra as SP, SuprimentoItem, SuprimentoSolicitacao,
)

from conftest import SessaoFalsa, novo_usuario

OBRA = novo_usuario(5, P.ADMINISTRATIVO_OBRA, nome="Amanda")
HOJE = date.today()


def _cenario(quantidade="100", recebida="0", status_pedido=SP.AUTORIZADO):
    item = SuprimentoItem(id=7, solicitacao_id=1, numero=1, insumo_id=10,
                          especificacao="12.5mm", quantidade=Decimal(quantidade),
                          quantidade_recebida=Decimal(recebida), unidade="M",
                          obra_id=1, status=ST.PEDIDO_EMITIDO)
    pedido = PedidoCompra(id=50, numero="PC-0001", fornecedor_id=100,
                          frete=Decimal("0"), desconto=Decimal("0"),
                          status=status_pedido, criado_por=1)
    linha = PedidoItem(id=60, pedido_id=50, suprimento_item_id=7, numero=1,
                       quantidade=Decimal(quantidade), preco_unitario=Decimal("38"))
    s = SessaoFalsa(OBRA, item, pedido, linha,
                    Insumo(id=10, codigo="INS-0010", descricao="Vergalhão CA50 12.5mm"),
                    Obra(id=1, codigo="CREPETERRA", nome="Crepeterra"),
                    SuprimentoSolicitacao(id=1, numero="SS-0001", titulo="armadura",
                                          solicitante_id=5))
    return s, item, pedido, linha


# ---------------------------------------------------------------------------
# Receber
# ---------------------------------------------------------------------------
def test_receber_tudo_fecha_o_item():
    s, item, pedido, linha = _cenario()

    r = svc.registrar(s, 50, {"itens": [{"pedido_item_id": 60, "quantidade": "100"}],
                              "nota_numero": "12345"}, OBRA)

    assert isinstance(r, Recebimento) and r.nota_numero == "12345"
    assert item.quantidade_recebida == Decimal("100")
    assert item.status is ST.RECEBIDO
    assert any(e.get("ac") == "RECEBIMENTO" for e in s.eventos)


def test_receber_em_partes_deixa_o_saldo_como_pendencia():
    """'Este item foi pedido em janeiro, chegou pela metade em fevereiro, o
    resto em março' — é essa história que o saldo preserva."""
    s, item, pedido, linha = _cenario()

    svc.registrar(s, 50, {"itens": [{"pedido_item_id": 60, "quantidade": "40"}]}, OBRA)

    assert item.quantidade_recebida == Decimal("40")
    assert item.status is ST.PENDENCIA
    assert item.saldo == Decimal("60")

    svc.registrar(s, 50, {"itens": [{"pedido_item_id": 60, "quantidade": "60"}]}, OBRA)

    assert item.status is ST.RECEBIDO and item.saldo == Decimal("0")


def test_receber_mais_do_que_foi_pedido_e_recusado():
    """Sobra não é recebimento — é outra conversa, e o registro tem de bater
    com a nota."""
    s, item, pedido, linha = _cenario(quantidade="100", recebida="90")

    with pytest.raises(ErroValidacao, match="mais do que foi pedido"):
        svc.registrar(s, 50, {"itens": [{"pedido_item_id": 60, "quantidade": "20"}]}, OBRA)

    assert item.quantidade_recebida == Decimal("90")


def test_pedido_nao_autorizado_nao_recebe():
    s, item, pedido, linha = _cenario(status_pedido=SP.AGUARDANDO_AUTORIZACAO)

    with pytest.raises(ErroValidacao, match="não está autorizado"):
        svc.registrar(s, 50, {"itens": [{"pedido_item_id": 60, "quantidade": "10"}]}, OBRA)


def test_item_de_outro_pedido_e_recusado():
    s, item, pedido, linha = _cenario()

    with pytest.raises(ErroValidacao, match="não pertence a este pedido"):
        svc.registrar(s, 50, {"itens": [{"pedido_item_id": 999, "quantidade": "1"}]}, OBRA)


def test_nao_se_recebe_no_futuro():
    s, item, pedido, linha = _cenario()
    amanha = (HOJE + timedelta(days=1)).isoformat()

    with pytest.raises(ErroValidacao, match="no futuro"):
        svc.registrar(s, 50, {"data": amanha,
                              "itens": [{"pedido_item_id": 60, "quantidade": "1"}]}, OBRA)


@pytest.mark.parametrize("itens,erro", [
    ([], "Diga o que chegou"),
    ([{"pedido_item_id": 60, "quantidade": "0"}], "Nenhuma quantidade"),
    ([{"pedido_item_id": 60, "quantidade": "abc"}], "número"),
])
def test_o_que_nao_registra(itens, erro):
    s, item, pedido, linha = _cenario()
    with pytest.raises(ErroValidacao, match=erro):
        svc.registrar(s, 50, {"itens": itens}, OBRA)


def test_o_recebimento_trava_o_item():
    import ast, inspect
    arvore = ast.parse(inspect.getsource(svc.registrar))
    travas = [no for no in ast.walk(arvore)
              if isinstance(no, ast.Call) and getattr(no.func, "attr", "") == "get"
              and {kw.arg: getattr(kw.value, "value", None) for kw in no.keywords}
              == {"with_for_update": True, "populate_existing": True}]
    assert len(travas) >= 2, (
        "dois recebimentos no mesmo segundo somariam sobre o mesmo saldo")


# ---------------------------------------------------------------------------
# O encontro entre suprimento e financeiro
# ---------------------------------------------------------------------------
def test_avisa_quando_a_parcela_venceu_e_o_material_nao_chegou():
    """Não bloqueia: avisa. O financeiro procura o suprimento e pergunta —
    pode pagar mesmo?"""
    s, item, pedido, linha = _cenario()
    s.objetos.append(PrevisaoPagamento(id=90, pedido_id=50, numero=1,
                                       vencimento=HOJE - timedelta(days=3),
                                       valor=Decimal("3800")))

    r = svc.situacao(s, 50)

    assert r["tudo_recebido"] is False
    assert any("parcela vencida" in a.lower() for a in r["avisos"])


def test_avisa_quando_o_material_chegou_e_ninguem_lancou_a_nota():
    s, item, pedido, linha = _cenario(quantidade="100", recebida="100")
    item.status = ST.RECEBIDO
    s.objetos.append(PrevisaoPagamento(id=90, pedido_id=50, numero=1,
                                       vencimento=HOJE + timedelta(days=20),
                                       valor=Decimal("3800")))

    r = svc.situacao(s, 50)

    assert r["tudo_recebido"] is True
    assert any("falta lançar a nota" in a.lower() for a in r["avisos"])


def test_avisa_quando_a_entrega_atrasou():
    s, item, pedido, linha = _cenario()
    pedido.previsao_entrega = HOJE - timedelta(days=5)

    r = svc.situacao(s, 50)

    assert any("não chegou" in a.lower() for a in r["avisos"])


def test_pedido_em_dia_nao_gera_aviso_nenhum():
    s, item, pedido, linha = _cenario(quantidade="100", recebida="100")
    item.status = ST.RECEBIDO
    s.objetos.append(PrevisaoPagamento(id=90, pedido_id=50, numero=1,
                                       vencimento=HOJE + timedelta(days=20),
                                       valor=Decimal("3800"), titulo_id=777))

    assert svc.situacao(s, 50)["avisos"] == []


def test_a_situacao_mostra_pedido_recebido_e_saldo_por_item():
    s, item, pedido, linha = _cenario(quantidade="100", recebida="40")

    linha_saida = svc.situacao(s, 50)["itens"][0]

    assert linha_saida["pedido"] == "100"
    assert linha_saida["recebido"] == "40"
    assert linha_saida["saldo"] == "60"


def test_situacao_de_pedido_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.situacao(SessaoFalsa(OBRA), 999)


# ---------------------------------------------------------------------------
# A fila de pendências
# ---------------------------------------------------------------------------
def test_a_lista_de_pendencias_traz_so_o_que_ficou_com_saldo():
    s, item, pedido, linha = _cenario(quantidade="100", recebida="40")
    item.status = ST.PENDENCIA
    OBRA.escopo_visao = EscopoVisao.PROPRIOS

    pendentes = svc.pendencias(s, OBRA)

    assert len(pendentes) == 1
    assert pendentes[0]["saldo"] == "60"
    assert pendentes[0]["status"] == "PENDENCIA"
