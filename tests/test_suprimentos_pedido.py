"""O pedido de compra: fechamento, autorização e previsão de pagamento.

Duas regras que o dono deixou claras:
  1. o comprador NÃO compra sozinho — o pedido nasce aguardando autorização,
     venha ele do mapa ou direto;
  2. pedido autorizado gera PREVISÃO DE PAGAMENTO, e antes disso não gera nada.

E as travas que evitam prejuízo:
  - o mesmo item não entra em dois pedidos vivos (comprar duas vezes o mesmo
    material é o erro que a reserva no banco evita);
  - compra à vista com fornecedor sem Pix nem conta é recusada — não teria como
    ser paga;
  - cancelar pedido que já virou título é recusado, para o financeiro e o
    suprimento não contarem histórias diferentes;
  - quem autoriza pode recusar PARTE, e o que sai volta para a fila.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import pedido as svc
from app.apps.erp.db.models.cadastros import (
    CondicaoPagamento, Cotacao, CotacaoFornecedor, CotacaoItem, CotacaoPreco,
    Fornecedor, FornecedorConta, Insumo, ModoEntrega, Obra, PedidoCompra,
    PedidoItem, PedidoItemReserva, PerfilUsuario as P, PrecoHistorico,
    PrevisaoPagamento, StatusCotacao, StatusItemSuprimento as ST,
    StatusPedidoCompra as SP, SuprimentoItem, TipoPessoa, TipoPreco,
)

from conftest import SessaoFalsa, novo_usuario

COMPRADOR = novo_usuario(1, P.ADMIN, nome="Comprador")
DIRETOR = novo_usuario(2, P.DIRETOR_FINANCEIRO, nome="Diretor")


def _cadastro():
    return [
        COMPRADOR, DIRETOR,
        Insumo(id=10, codigo="INS-0010", descricao="Vergalhão CA50 12.5mm"),
        Obra(id=1, codigo="CREPETERRA", nome="Crepeterra"),
        Fornecedor(id=100, tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                   razao_social="ACO FORTE LTDA"),
        CondicaoPagamento(id=3, nome="30/60 dias", entrada_percentual=Decimal("0"),
                          dias=[30, 60]),
    ]


def _item(id_=7, quantidade="100"):
    return SuprimentoItem(id=id_, solicitacao_id=1, numero=1, insumo_id=10,
                          especificacao="12.5mm", quantidade=Decimal(quantidade),
                          quantidade_recebida=Decimal("0"), unidade="M", obra_id=1,
                          status=ST.ANALISE_PROPOSTAS)


def _mapa_fechavel(preco="38.00"):
    return [
        Cotacao(id=1, numero="COT-0001", titulo="armadura",
                status=StatusCotacao.ABERTA, criado_por=1),
        CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100,
                          condicao_pagamento_id=3, frete=Decimal("200"),
                          desconto=Decimal("0"), entrega=ModoEntrega.ENTREGA),
        CotacaoItem(id=11, cotacao_id=1, suprimento_item_id=7, numero=1),
        CotacaoPreco(id=31, cotacao_fornecedor_id=21, cotacao_item_id=11,
                     preco_unitario=Decimal(preco)),
    ]


# ---------------------------------------------------------------------------
# Fechar
# ---------------------------------------------------------------------------
def test_fechar_do_mapa_traz_preco_condicao_e_frete():
    item = _item()
    s = SessaoFalsa(*_cadastro(), item, *_mapa_fechavel())

    pedido = svc.fechar_do_mapa(s, 1, 21, [11], {}, COMPRADOR)

    assert pedido.numero == "PC-0001"
    assert pedido.status is SP.AGUARDANDO_AUTORIZACAO, "o comprador não compra sozinho"
    assert pedido.condicao_pagamento_id == 3
    assert pedido.frete == Decimal("200")
    assert pedido.entrega is ModoEntrega.ENTREGA
    linha = next(o for o in s.adicionados if isinstance(o, PedidoItem))
    assert linha.preco_unitario == Decimal("38.00")
    assert item.status is ST.AUTORIZACAO


def test_o_total_soma_itens_e_frete_e_tira_desconto():
    item = _item()
    s = SessaoFalsa(*_cadastro(), item, *_mapa_fechavel())
    pedido = svc.fechar_do_mapa(s, 1, 21, [11], {"desconto": "100"}, COMPRADOR)
    s.objetos.extend([pedido] + [o for o in s.adicionados if isinstance(o, PedidoItem)])

    assert svc.total(s, pedido) == Decimal("3900.00")   # 3800 + 200 − 100


def test_nao_fecha_item_sem_preco_daquele_fornecedor():
    item = _item()
    mapa = [o for o in _mapa_fechavel() if not isinstance(o, CotacaoPreco)]
    s = SessaoFalsa(*_cadastro(), item, *mapa)

    with pytest.raises(ErroValidacao, match="sem preço"):
        svc.fechar_do_mapa(s, 1, 21, [11], {}, COMPRADOR)


def test_o_mesmo_item_nao_entra_em_dois_pedidos_vivos():
    """Comprar duas vezes o mesmo material é o erro que a reserva evita."""
    item = _item()
    s = SessaoFalsa(*_cadastro(), item, *_mapa_fechavel(),
                    PedidoItemReserva(suprimento_item_id=7, pedido_id=99))

    with pytest.raises(ErroValidacao, match="já está num pedido em aberto"):
        svc.fechar_do_mapa(s, 1, 21, [11], {}, COMPRADOR)


def test_pedido_direto_dispensa_mapa():
    item = _item()
    s = SessaoFalsa(*_cadastro(), item)

    pedido = svc.fechar_direto(s, {
        "fornecedor_id": 100, "condicao_pagamento_id": 3,
        "itens": [{"suprimento_item_id": 7, "preco_unitario": "40,00"}]}, COMPRADOR)

    assert pedido.cotacao_id is None
    assert pedido.status is SP.AGUARDANDO_AUTORIZACAO
    linha = next(o for o in s.adicionados if isinstance(o, PedidoItem))
    assert linha.preco_unitario == Decimal("40.00")


def test_compra_a_vista_com_fornecedor_sem_dados_de_pagamento_e_recusada():
    """À vista não gera boleto depois: ou há Pix/conta no cadastro, ou não há
    como pagar."""
    s = SessaoFalsa(*_cadastro(), _item())

    with pytest.raises(ErroValidacao, match="chave Pix nem dados bancários"):
        svc.fechar_direto(s, {"fornecedor_id": 100, "antecipado": True,
                              "itens": [{"suprimento_item_id": 7,
                                         "preco_unitario": "40"}]}, COMPRADOR)


def test_compra_a_vista_passa_quando_o_fornecedor_tem_conta():
    s = SessaoFalsa(*_cadastro(), _item(),
                    FornecedorConta(id=1, fornecedor_id=100))

    pedido = svc.fechar_direto(s, {"fornecedor_id": 100, "antecipado": True,
                                   "itens": [{"suprimento_item_id": 7,
                                              "preco_unitario": "40"}]}, COMPRADOR)

    assert pedido.antecipado is True


@pytest.mark.parametrize("dados,erro", [
    ({"fornecedor_id": 100, "itens": []}, "Escolha os itens"),
    ({"fornecedor_id": 999, "itens": [{"suprimento_item_id": 7, "preco_unitario": "1"}]},
     "Fornecedor não encontrado"),
    ({"fornecedor_id": 100, "itens": [{"suprimento_item_id": 7, "preco_unitario": "0"}]},
     "maior que zero"),
    ({"fornecedor_id": 100, "previsao_entrega": "31/02/2026",
      "itens": [{"suprimento_item_id": 7, "preco_unitario": "1"}]}, "Data de previsão"),
])
def test_o_que_nao_fecha(dados, erro):
    s = SessaoFalsa(*_cadastro(), _item())
    with pytest.raises(ErroValidacao, match=erro):
        svc.fechar_direto(s, dados, COMPRADOR)


# ---------------------------------------------------------------------------
# Autorizar
# ---------------------------------------------------------------------------
def _pedido_pronto(condicao_id=3, frete="0", quantidade="100", preco="38"):
    pedido = PedidoCompra(id=50, numero="PC-0001", fornecedor_id=100,
                          condicao_pagamento_id=condicao_id,
                          frete=Decimal(frete), desconto=Decimal("0"),
                          previsao_entrega=date(2026, 9, 10),
                          status=SP.AGUARDANDO_AUTORIZACAO, criado_por=1)
    linha = PedidoItem(id=60, pedido_id=50, suprimento_item_id=7, numero=1,
                       quantidade=Decimal(quantidade), preco_unitario=Decimal(preco))
    reserva = PedidoItemReserva(suprimento_item_id=7, pedido_id=50)
    return pedido, linha, reserva


def test_autorizar_gera_a_previsao_pela_condicao_de_pagamento():
    pedido, linha, reserva = _pedido_pronto()
    item = _item()
    item.status = ST.AUTORIZACAO
    s = SessaoFalsa(*_cadastro(), item, pedido, linha, reserva)

    svc.autorizar(s, 50, DIRETOR)

    assert pedido.status is SP.AUTORIZADO and pedido.autorizado_por == 2
    previsoes = [o for o in s.adicionados if isinstance(o, PrevisaoPagamento)]
    assert [str(p.valor) for p in previsoes] == ["1900.00", "1900.00"]
    assert [p.vencimento for p in previsoes] == [date(2026, 10, 10), date(2026, 11, 9)]
    assert item.status is ST.PEDIDO_EMITIDO


def test_autorizar_registra_o_preco_como_COMPRADO_no_banco_de_precos():
    """O que a empresa aceitou pagar vale mais do que o que ofereceram."""
    pedido, linha, reserva = _pedido_pronto()
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva)

    svc.autorizar(s, 50, DIRETOR)

    historico = [o for o in s.adicionados if isinstance(o, PrecoHistorico)]
    assert len(historico) == 1
    assert historico[0].tipo is TipoPreco.COMPRADO
    assert historico[0].preco_unitario == Decimal("38")


def test_sem_condicao_cadastrada_a_previsao_e_parcela_unica():
    """Melhor uma previsão grosseira do que nenhuma — o financeiro precisa
    enxergar o compromisso."""
    pedido, linha, reserva = _pedido_pronto(condicao_id=None)
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva)

    svc.autorizar(s, 50, DIRETOR)

    previsoes = [o for o in s.adicionados if isinstance(o, PrevisaoPagamento)]
    assert len(previsoes) == 1
    assert previsoes[0].vencimento == date(2026, 9, 10)
    assert previsoes[0].entrada is True


def test_quem_autoriza_pode_recusar_parte_do_pedido():
    pedido = PedidoCompra(id=50, numero="PC-0001", fornecedor_id=100,
                          condicao_pagamento_id=None, frete=Decimal("0"),
                          desconto=Decimal("0"), previsao_entrega=date(2026, 9, 10),
                          status=SP.AGUARDANDO_AUTORIZACAO, criado_por=1)
    l1 = PedidoItem(id=60, pedido_id=50, suprimento_item_id=7, numero=1,
                    quantidade=Decimal("100"), preco_unitario=Decimal("38"))
    l2 = PedidoItem(id=61, pedido_id=50, suprimento_item_id=8, numero=2,
                    quantidade=Decimal("10"), preco_unitario=Decimal("5"))
    item1, item2 = _item(), _item(8, "10")
    s = SessaoFalsa(*_cadastro(), item1, item2, pedido, l1, l2,
                    PedidoItemReserva(suprimento_item_id=7, pedido_id=50),
                    PedidoItemReserva(suprimento_item_id=8, pedido_id=50))

    svc.autorizar(s, 50, DIRETOR, itens_recusados=[61])

    assert item2.status is ST.SOLICITACAO, "o item recusado volta para a fila"
    assert l2 in s.removidos
    previsoes = [o for o in s.adicionados if isinstance(o, PrevisaoPagamento)]
    assert str(previsoes[0].valor) == "3800.00", "a previsão nasce só sobre o que ficou"


def test_recusar_todos_os_itens_nao_e_autorizar():
    pedido, linha, reserva = _pedido_pronto()
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva)

    with pytest.raises(ErroValidacao, match="use Recusar o pedido"):
        svc.autorizar(s, 50, DIRETOR, itens_recusados=[60])


def test_autorizar_duas_vezes_nao_passa():
    pedido, linha, reserva = _pedido_pronto()
    pedido.status = SP.AUTORIZADO
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva)

    with pytest.raises(ErroValidacao, match="já está autorizado"):
        svc.autorizar(s, 50, DIRETOR)


def test_a_autorizacao_trava_a_linha():
    import ast, inspect
    arvore = ast.parse(inspect.getsource(svc._pedido_pendente))
    assert any(
        isinstance(no, ast.Call) and getattr(no.func, "attr", "") == "get"
        and {kw.arg: getattr(kw.value, "value", None) for kw in no.keywords}
        == {"with_for_update": True, "populate_existing": True}
        for no in ast.walk(arvore)), (
        "duas autorizações no mesmo segundo gerariam duas previsões")


# ---------------------------------------------------------------------------
# Recusar e cancelar
# ---------------------------------------------------------------------------
def test_recusar_devolve_os_itens_e_exige_motivo():
    pedido, linha, reserva = _pedido_pronto()
    item = _item()
    item.status = ST.AUTORIZACAO
    s = SessaoFalsa(*_cadastro(), item, pedido, linha, reserva)

    with pytest.raises(ErroValidacao, match="motivo"):
        svc.recusar(s, 50, "não", DIRETOR)

    svc.recusar(s, 50, "Preço acima do último comprado; recotar.", DIRETOR)

    assert pedido.status is SP.RECUSADO
    assert item.status is ST.SOLICITACAO
    assert reserva in s.removidos, "o item volta a poder entrar em outro pedido"


def test_cancelar_pedido_autorizado_derruba_a_previsao():
    pedido, linha, reserva = _pedido_pronto()
    pedido.status = SP.AUTORIZADO
    previsao = PrevisaoPagamento(id=90, pedido_id=50, numero=1,
                                 vencimento=date(2026, 10, 10), valor=Decimal("3800"))
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva, previsao)

    svc.cancelar(s, 50, "Fornecedor não tem o material.", DIRETOR)

    assert pedido.status is SP.CANCELADO
    assert previsao in s.removidos, (
        "compromisso cancelado não pode continuar aparecendo como dinheiro a sair")


def test_nao_cancela_pedido_que_ja_virou_titulo():
    pedido, linha, reserva = _pedido_pronto()
    pedido.status = SP.AUTORIZADO
    previsao = PrevisaoPagamento(id=90, pedido_id=50, numero=1,
                                 vencimento=date(2026, 10, 10),
                                 valor=Decimal("3800"), titulo_id=555)
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva, previsao)

    with pytest.raises(ErroValidacao, match="já virou título"):
        svc.cancelar(s, 50, "desisti da compra", DIRETOR)


def test_pedido_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.autorizar(SessaoFalsa(*_cadastro()), 999, DIRETOR)


# ---------------------------------------------------------------------------
# O que quem autoriza vê
# ---------------------------------------------------------------------------
def test_o_detalhe_traz_o_mapa_de_origem():
    """Julgar a escolha do comprador exige ver as alternativas que ele tinha."""
    pedido, linha, reserva = _pedido_pronto()
    pedido.cotacao_id = 1
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva, *_mapa_fechavel())

    d = svc.detalhar(s, 50)

    assert d["numero"] == "PC-0001" and d["fornecedor"] == "ACO FORTE LTDA"
    assert d["mapa"] is not None and d["mapa"]["numero"] == "COT-0001"
    assert d["itens"][0]["total"] == "3800.00"


def test_pedido_direto_nao_tem_mapa_e_isso_e_normal():
    pedido, linha, reserva = _pedido_pronto()
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva)

    assert svc.detalhar(s, 50)["mapa"] is None


# ---------------------------------------------------------------------------
# As rotas: quem fecha, quem autoriza, e quem não entra
# ---------------------------------------------------------------------------
import contextlib

from flask import Flask


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


def test_quem_pede_material_na_obra_nao_ve_a_fila_de_pedidos(monkeypatch):
    """A tela mostra preço e fornecedor — é de quem compra ou autoriza."""
    obra = novo_usuario(5, P.ADMINISTRATIVO_OBRA)
    s = SessaoFalsa(obra, *_cadastro())
    c = _cliente(s, monkeypatch, 5)

    assert c.get("/erp/api/suprimentos/pedidos").status_code == 403
    assert c.get("/erp/suprimentos/pedidos").status_code == 403


def test_o_diretor_ve_a_fila_mesmo_sem_a_acao_de_comprar(monkeypatch):
    s = SessaoFalsa(DIRETOR, *_cadastro())
    assert _cliente(s, monkeypatch, 2).get(
        "/erp/api/suprimentos/pedidos").status_code == 200


def test_o_comprador_nao_autoriza_o_proprio_pedido(monkeypatch):
    """Separação básica: quem pede não libera. O comprador tem 'comprar', mas
    'autorizar_pedido' é de outra pessoa."""
    comprador = novo_usuario(9, P.FINANCEIRO)
    comprador.permissoes_extras = {"comprar": True}
    pedido, linha, reserva = _pedido_pronto()
    s = SessaoFalsa(comprador, *_cadastro(), _item(), pedido, linha, reserva,
                    permissoes_por_usuario={9: {"comprar": True}})
    c = _cliente(s, monkeypatch, 9)

    assert c.post("/erp/api/suprimentos/pedidos/50/autorizar", json={}).status_code == 403
    assert c.get("/erp/api/suprimentos/pedidos").status_code == 200


def test_autorizar_pela_rota_gera_a_previsao(monkeypatch):
    pedido, linha, reserva = _pedido_pronto()
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva)
    c = _cliente(s, monkeypatch, 2)

    r = c.post("/erp/api/suprimentos/pedidos/50/autorizar", json={})

    assert r.status_code == 200
    assert pedido.status is SP.AUTORIZADO
    assert any(isinstance(o, PrevisaoPagamento) for o in s.adicionados)


def test_acao_desconhecida_no_pedido_e_recusada(monkeypatch):
    pedido, linha, reserva = _pedido_pronto()
    s = SessaoFalsa(*_cadastro(), _item(), pedido, linha, reserva)

    r = _cliente(s, monkeypatch, 2).post("/erp/api/suprimentos/pedidos/50/voar", json={})

    assert r.status_code == 400


# ---------------------------------------------------------------------------
# O relatório que vai para o fornecedor
# ---------------------------------------------------------------------------
def test_o_relatorio_agrupa_por_endereco_de_entrega():
    """Um mesmo pedido leva material para obras diferentes, e o motorista
    precisa saber o que desce em cada lugar."""
    obra_b = Obra(id=2, codigo="IGARASSU", nome="Igarassu",
                  endereco="Rua B", numero_endereco="10", municipio="Igarassu", uf="PE")
    obra_a = Obra(id=1, codigo="CREPETERRA", nome="Crepeterra",
                  endereco="Rua A", numero_endereco="100", municipio="Eusébio", uf="CE")
    item1 = _item(7, "100")
    item2 = _item(8, "50")
    item2.obra_id = 2
    pedido = PedidoCompra(id=50, numero="PC-0001", fornecedor_id=100,
                          frete=Decimal("0"), desconto=Decimal("0"),
                          status=SP.AUTORIZADO, criado_por=1)
    l1 = PedidoItem(id=60, pedido_id=50, suprimento_item_id=7, numero=1,
                    quantidade=Decimal("100"), preco_unitario=Decimal("38"))
    l2 = PedidoItem(id=61, pedido_id=50, suprimento_item_id=8, numero=2,
                    quantidade=Decimal("50"), preco_unitario=Decimal("10"))
    s = SessaoFalsa(*[o for o in _cadastro() if not isinstance(o, Obra)],
                    obra_a, obra_b, item1, item2, pedido, l1, l2)

    r = svc.relatorio_para_o_fornecedor(s, 50)

    assert len(r["locais"]) == 2
    enderecos = {b["endereco"] for b in r["locais"]}
    assert "Rua A, 100, Eusébio, CE" in enderecos
    assert "Rua B, 10, Igarassu, PE" in enderecos
    assert r["total"] == "4300.00"


def test_duas_obras_no_mesmo_endereco_entram_no_mesmo_bloco():
    """O dono pediu: permitir entrega de mais de uma obra em um único endereço."""
    obra_a = Obra(id=1, codigo="OBRA-A", nome="A", endereco="Rua Única",
                  numero_endereco="1", municipio="Fortaleza", uf="CE")
    obra_b = Obra(id=2, codigo="OBRA-B", nome="B", endereco="Rua Única",
                  numero_endereco="1", municipio="Fortaleza", uf="CE")
    item1, item2 = _item(7), _item(8, "50")
    item2.obra_id = 2
    pedido = PedidoCompra(id=50, numero="PC-0001", fornecedor_id=100,
                          frete=Decimal("0"), desconto=Decimal("0"),
                          status=SP.AUTORIZADO, criado_por=1)
    l1 = PedidoItem(id=60, pedido_id=50, suprimento_item_id=7, numero=1,
                    quantidade=Decimal("100"), preco_unitario=Decimal("38"))
    l2 = PedidoItem(id=61, pedido_id=50, suprimento_item_id=8, numero=2,
                    quantidade=Decimal("50"), preco_unitario=Decimal("10"))
    s = SessaoFalsa(*[o for o in _cadastro() if not isinstance(o, Obra)],
                    obra_a, obra_b, item1, item2, pedido, l1, l2)

    r = svc.relatorio_para_o_fornecedor(s, 50)

    assert len(r["locais"]) == 1
    assert len(r["locais"][0]["obras"]) == 2
    assert len(r["locais"][0]["itens"]) == 2


def test_obra_sem_endereco_aparece_dita_no_relatorio():
    """Em branco, o motorista descobre no caminho."""
    obra = Obra(id=1, codigo="SEM-END", nome="Sem endereço")
    pedido = PedidoCompra(id=50, numero="PC-0001", fornecedor_id=100,
                          frete=Decimal("0"), desconto=Decimal("0"),
                          status=SP.AUTORIZADO, criado_por=1)
    linha = PedidoItem(id=60, pedido_id=50, suprimento_item_id=7, numero=1,
                       quantidade=Decimal("1"), preco_unitario=Decimal("1"))
    s = SessaoFalsa(*[o for o in _cadastro() if not isinstance(o, Obra)],
                    obra, _item(7, "1"), pedido, linha)

    r = svc.relatorio_para_o_fornecedor(s, 50)

    assert r["locais"][0]["endereco"] == "Endereço não informado"
