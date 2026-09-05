"""O mapa de cotação e o banco de preços.

O mapa é o coração das compras: fornecedores em coluna, insumos em linha,
preço na célula. O que ele precisa fazer e a planilha não fazia:

  - comparar o CUSTO REAL, com frete, desconto e acréscimo. O mais barato por
    item pode sair mais caro no total, e é isso que decide a compra;
  - guardar todo preço no banco de preços, com data, fornecedor e origem;
  - marcar o preço HERDADO de outra cotação — comprar com base num preço de
    três meses atrás sem perceber é o risco que a herança cria.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import cotacao as svc
from app.apps.erp.db.models.cadastros import (
    CondicaoPagamento, Cotacao, CotacaoFornecedor, CotacaoItem, CotacaoPreco,
    Fornecedor, Insumo, Obra, PerfilUsuario as P, PrecoHistorico, StatusCotacao,
    StatusItemSuprimento as ST, SuprimentoItem, SuprimentoSolicitacao, TipoPessoa,
    TipoPreco, UnidadeCompra,
)

from conftest import SessaoFalsa, novo_usuario

COMPRADOR = novo_usuario(1, P.ADMIN)


def _cadastro():
    return [
        UnidadeCompra(codigo="M", descricao="Metro"),
        Insumo(id=10, codigo="INS-0010", descricao="Vergalhão CA50 12.5mm", unidade="M"),
        Obra(id=1, codigo="CREPETERRA", nome="Crepeterra"),
        Fornecedor(id=100, tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                   razao_social="ACO FORTE LTDA"),
        Fornecedor(id=200, tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="34028316000103",
                   razao_social="FERRO BOM LTDA"),
        CondicaoPagamento(id=3, nome="30/60 dias", entrada_percentual=Decimal("0"),
                          dias=[30, 60]),
    ]


def _item(id_=7, quantidade="100", status=ST.SOLICITACAO):
    return SuprimentoItem(id=id_, solicitacao_id=1, numero=1, insumo_id=10,
                          especificacao="CA50", quantidade=Decimal(quantidade),
                          quantidade_recebida=Decimal("0"), unidade="M",
                          obra_id=1, status=status)


def _sessao(*extras):
    return SessaoFalsa(COMPRADOR, *_cadastro(), *extras)


# ---------------------------------------------------------------------------
# Abrir a cotação
# ---------------------------------------------------------------------------
def test_abrir_cotacao_leva_os_itens_para_cotacao():
    item = _item()
    s = _sessao(item, SuprimentoSolicitacao(id=1, numero="SS-0001", titulo="x",
                                            solicitante_id=1))

    cot = svc.criar(s, {"titulo": "armadura da fundação", "itens": [7]}, COMPRADOR)

    assert cot.numero == "COT-0001"
    assert item.status is ST.COTACAO
    linhas = [o for o in s.adicionados if isinstance(o, CotacaoItem)]
    assert [l.numero for l in linhas] == [1]


def test_item_ja_em_cotacao_aberta_nao_entra_em_outra():
    """Dois preços para a mesma coisa e ninguém sabe qual vale."""
    item = _item()
    aberta = Cotacao(id=9, numero="COT-0009", titulo="anterior",
                     status=StatusCotacao.ABERTA, criado_por=1)
    s = _sessao(item, aberta, CotacaoItem(id=1, cotacao_id=9, suprimento_item_id=7,
                                          numero=1))

    with pytest.raises(ErroValidacao, match="já estão numa cotação aberta"):
        svc.criar(s, {"titulo": "nova rodada", "itens": [7]}, COMPRADOR)


def test_item_de_cotacao_fechada_pode_ser_cotado_de_novo():
    item = _item(status=ST.PENDENCIA)
    fechada = Cotacao(id=9, numero="COT-0009", titulo="anterior",
                      status=StatusCotacao.FECHADA, criado_por=1)
    s = _sessao(item, fechada, CotacaoItem(id=1, cotacao_id=9, suprimento_item_id=7,
                                           numero=1))

    cot = svc.criar(s, {"titulo": "recotar a pendência", "itens": [7]}, COMPRADOR)

    assert cot.numero == "COT-0010"
    assert item.status is ST.COTACAO


@pytest.mark.parametrize("dados,erro", [
    ({"titulo": "ok cotação", "itens": []}, "pelo menos um item"),
    ({"titulo": "ab", "itens": [7]}, "título"),
    ({"titulo": "cotação boa", "itens": [999]}, "não encontrado"),
])
def test_o_que_nao_abre(dados, erro):
    with pytest.raises(ErroValidacao, match=erro):
        svc.criar(_sessao(_item()), dados, COMPRADOR)


# ---------------------------------------------------------------------------
# Colunas do mapa
# ---------------------------------------------------------------------------
def _cotacao_montada(*extras):
    cot = Cotacao(id=1, numero="COT-0001", titulo="armadura",
                  status=StatusCotacao.ABERTA, criado_por=1)
    linha = CotacaoItem(id=11, cotacao_id=1, suprimento_item_id=7, numero=1)
    return _sessao(cot, _item(), linha, *extras), cot, linha


def test_adicionar_fornecedor_cria_a_coluna():
    s, cot, _ = _cotacao_montada()

    coluna = svc.adicionar_fornecedor(s, 1, {
        "fornecedor_id": 100, "condicao_pagamento_id": 3, "entrega": "COLETA",
        "frete": "150,00", "respondido_por": "Pedro"}, COMPRADOR)

    assert coluna.fornecedor_id == 100
    assert coluna.frete == Decimal("150.00")
    assert coluna.entrega.value == "COLETA"


def test_o_mesmo_fornecedor_nao_entra_duas_vezes():
    s, cot, _ = _cotacao_montada(
        CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100))

    with pytest.raises(ErroValidacao, match="já está no mapa"):
        svc.adicionar_fornecedor(s, 1, {"fornecedor_id": 100}, COMPRADOR)


def test_nao_se_mexe_em_cotacao_fechada():
    cot = Cotacao(id=1, numero="COT-0001", titulo="x",
                  status=StatusCotacao.FECHADA, criado_por=1)
    s = _sessao(cot)

    with pytest.raises(ErroValidacao, match="já está fechada"):
        svc.adicionar_fornecedor(s, 1, {"fornecedor_id": 100}, COMPRADOR)


# ---------------------------------------------------------------------------
# Preços
# ---------------------------------------------------------------------------
def test_lancar_preco_grava_a_celula_e_alimenta_o_banco_de_precos():
    s, cot, linha = _cotacao_montada(
        CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100,
                          condicao_pagamento_id=3))

    svc.lancar_preco(s, 21, 11, "38,50", COMPRADOR)

    celula = next(o for o in s.adicionados if isinstance(o, CotacaoPreco))
    assert celula.preco_unitario == Decimal("38.50")
    historico = next(o for o in s.adicionados if isinstance(o, PrecoHistorico))
    assert historico.tipo is TipoPreco.COTADO
    assert historico.insumo_id == 10 and historico.fornecedor_id == 100
    assert historico.especificacao == "CA50", "a especificação vai junto"
    assert historico.condicao_pagamento_id == 3


def test_lancar_de_novo_substitui_mas_o_historico_guarda_os_dois():
    """O fornecedor corrige a proposta e isso é normal — mas o que ele ofereceu
    antes não pode sumir."""
    ja = CotacaoPreco(id=31, cotacao_fornecedor_id=21, cotacao_item_id=11,
                      preco_unitario=Decimal("40"))
    s, cot, linha = _cotacao_montada(
        CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100), ja)

    svc.lancar_preco(s, 21, 11, "38,50", COMPRADOR)

    assert ja.preco_unitario == Decimal("38.50")
    assert not [o for o in s.adicionados if isinstance(o, CotacaoPreco)]
    assert len([o for o in s.adicionados if isinstance(o, PrecoHistorico)]) == 1


@pytest.mark.parametrize("preco,erro", [
    ("0", "maior que zero"), ("-5", "maior que zero"), ("abc", "número"),
    ("", "obrigatório")])
def test_preco_invalido_e_recusado(preco, erro):
    s, cot, linha = _cotacao_montada(
        CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100))

    with pytest.raises(ErroValidacao, match=erro):
        svc.lancar_preco(s, 21, 11, preco, COMPRADOR)


def test_celula_de_cotacoes_diferentes_e_recusada():
    s, cot, linha = _cotacao_montada(
        CotacaoFornecedor(id=21, cotacao_id=99, fornecedor_id=100))

    with pytest.raises(ErroValidacao, match="cotações diferentes"):
        svc.lancar_preco(s, 21, 11, "10", COMPRADOR)


def test_celula_inexistente_responde_nao_encontrado():
    s, cot, linha = _cotacao_montada()
    with pytest.raises(ErroNaoEncontrado):
        svc.lancar_preco(s, 999, 11, "10", COMPRADOR)


# ---------------------------------------------------------------------------
# O mapa: onde a comparação acontece
# ---------------------------------------------------------------------------
def _mapa_com_dois_fornecedores(frete_a="0", frete_b="0", desconto_b="0"):
    cot = Cotacao(id=1, numero="COT-0001", titulo="armadura",
                  status=StatusCotacao.ABERTA, criado_por=1)
    linha = CotacaoItem(id=11, cotacao_id=1, suprimento_item_id=7, numero=1)
    col_a = CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100,
                              frete=Decimal(frete_a), desconto=Decimal("0"),
                              acrescimo_percentual=Decimal("0"), ordem=1)
    col_b = CotacaoFornecedor(id=22, cotacao_id=1, fornecedor_id=200,
                              frete=Decimal(frete_b), desconto=Decimal(desconto_b),
                              acrescimo_percentual=Decimal("0"), ordem=2)
    preco_a = CotacaoPreco(id=31, cotacao_fornecedor_id=21, cotacao_item_id=11,
                           preco_unitario=Decimal("38.00"))
    preco_b = CotacaoPreco(id=32, cotacao_fornecedor_id=22, cotacao_item_id=11,
                           preco_unitario=Decimal("40.00"))
    return _sessao(cot, _item(), linha, col_a, col_b, preco_a, preco_b)


def test_o_mapa_marca_o_menor_preco_de_cada_linha():
    mapa = svc.montar_mapa(_mapa_com_dois_fornecedores(), 1)

    linha = mapa["itens"][0]
    assert linha["menor_preco_de"] == 21
    assert linha["precos"][21]["total"] == "3800.00", "38 x 100 unidades"
    assert linha["insumo"] == "Vergalhão CA50 12.5mm"
    assert linha["insumo_id"] == 10, (
        "a tela puxa o preço anterior POR INSUMO — sem este campo ela "
        "consultaria pelo número do item e não acharia nada")


def test_o_frete_muda_quem_e_o_melhor():
    """O mais barato por item pode sair mais caro no total. Sem isso, o frete
    de R$ 800 apareceria só na nota."""
    s = _mapa_com_dois_fornecedores(frete_a="800", frete_b="0")

    mapa = svc.montar_mapa(s, 1)

    por_id = {c["id"]: c for c in mapa["fornecedores"]}
    assert por_id[21]["total"] == "4600.00"    # 3800 + 800 de frete
    assert por_id[22]["total"] == "4000.00"
    assert mapa["melhor_fornecedor_unico"] == 22, (
        "o menor preço unitário é o 21, mas com frete o 22 é o melhor negócio")


def test_o_desconto_tambem_entra_na_conta():
    s = _mapa_com_dois_fornecedores(desconto_b="500")

    mapa = svc.montar_mapa(s, 1)

    por_id = {c["id"]: c for c in mapa["fornecedores"]}
    assert por_id[22]["total"] == "3500.00"
    assert mapa["melhor_fornecedor_unico"] == 22


def test_o_acrescimo_percentual_incide_sobre_os_itens():
    s = _mapa_com_dois_fornecedores()
    coluna = next(o for o in s.objetos
                  if isinstance(o, CotacaoFornecedor) and o.id == 21)
    coluna.acrescimo_percentual = Decimal("10")

    mapa = svc.montar_mapa(s, 1)

    por_id = {c["id"]: c for c in mapa["fornecedores"]}
    assert por_id[21]["total"] == "4180.00"    # 3800 + 10%


def test_fornecedor_que_nao_cotou_tudo_nao_disputa_o_pedido_unico():
    """Comparar quem cotou 3 de 5 itens com quem cotou os 5 daria vitória a
    quem respondeu menos."""
    cot = Cotacao(id=1, numero="COT-0001", titulo="x",
                  status=StatusCotacao.ABERTA, criado_por=1)
    l1 = CotacaoItem(id=11, cotacao_id=1, suprimento_item_id=7, numero=1)
    l2 = CotacaoItem(id=12, cotacao_id=1, suprimento_item_id=8, numero=2)
    col_a = CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100)
    col_b = CotacaoFornecedor(id=22, cotacao_id=1, fornecedor_id=200)
    s = _sessao(cot, _item(), _item(8, "50"), l1, l2, col_a, col_b,
                CotacaoPreco(id=31, cotacao_fornecedor_id=21, cotacao_item_id=11,
                             preco_unitario=Decimal("38")),
                CotacaoPreco(id=32, cotacao_fornecedor_id=21, cotacao_item_id=12,
                             preco_unitario=Decimal("10")),
                CotacaoPreco(id=33, cotacao_fornecedor_id=22, cotacao_item_id=11,
                             preco_unitario=Decimal("1")))   # só um item, baratíssimo

    mapa = svc.montar_mapa(s, 1)

    assert mapa["melhor_fornecedor_unico"] == 21
    por_id = {c["id"]: c for c in mapa["fornecedores"]}
    assert por_id[22]["itens_cotados"] == 1 and por_id[22]["itens_no_mapa"] == 2


def test_o_total_pulverizado_e_o_piso_da_compra_item_a_item():
    mapa = svc.montar_mapa(_mapa_com_dois_fornecedores(), 1)
    assert mapa["total_pulverizado"] == "3800.00"


def test_mapa_de_cotacao_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.montar_mapa(_sessao(), 999)


# ---------------------------------------------------------------------------
# Banco de preços
# ---------------------------------------------------------------------------
def _historico(*precos):
    registros = []
    for i, (valor, tipo, dia) in enumerate(precos, start=1):
        registros.append(PrecoHistorico(
            id=i, insumo_id=10, especificacao="CA50", unidade="M",
            preco_unitario=Decimal(valor), quantidade=Decimal("100"),
            fornecedor_id=100, obra_id=1, tipo=tipo, data=dia))
    return registros


def test_o_resumo_traz_ultimo_menor_maior_e_media():
    s = _sessao(*_historico(("40", TipoPreco.COTADO, date(2026, 7, 1)),
                            ("30", TipoPreco.COMPRADO, date(2026, 8, 1)),
                            ("50", TipoPreco.COTADO, date(2026, 9, 1))))

    r = svc.historico_de_precos(s, insumo_id=10)

    assert r["resumo"]["ocorrencias"] == 3
    assert r["resumo"]["ultimo"] == "50"
    assert r["resumo"]["menor"] == "30" and r["resumo"]["maior"] == "50"
    assert r["resumo"]["media"] == "40.0000"
    assert r["resumo"]["ultimo_comprado"] == "30", (
        "o que a empresa aceitou pagar vale mais do que o que ofereceram")


def test_o_historico_vem_do_mais_novo_para_o_mais_velho():
    s = _sessao(*_historico(("40", TipoPreco.COTADO, date(2026, 7, 1)),
                            ("50", TipoPreco.COTADO, date(2026, 9, 1))))

    r = svc.historico_de_precos(s, insumo_id=10)

    assert [x["preco_unitario"] for x in r["registros"]] == ["50", "40"]


def test_filtra_por_periodo_e_por_fornecedor():
    s = _sessao(*_historico(("40", TipoPreco.COTADO, date(2026, 1, 1)),
                            ("50", TipoPreco.COTADO, date(2026, 9, 1))))

    assert svc.historico_de_precos(s, desde=date(2026, 6, 1))["resumo"]["ocorrencias"] == 1
    assert svc.historico_de_precos(s, fornecedor_id=999)["resumo"]["ocorrencias"] == 0


def test_a_busca_cobre_insumo_especificacao_e_fornecedor():
    s = _sessao(*_historico(("40", TipoPreco.COTADO, date(2026, 9, 1))))

    assert svc.historico_de_precos(s, busca="vergalhão")["resumo"]["ocorrencias"] == 1
    assert svc.historico_de_precos(s, busca="CA50")["resumo"]["ocorrencias"] == 1
    assert svc.historico_de_precos(s, busca="aco forte")["resumo"]["ocorrencias"] == 1
    assert svc.historico_de_precos(s, busca="cimento")["resumo"]["ocorrencias"] == 0


def test_sugerir_preco_traz_a_data_e_a_origem():
    """O mapa TEM de mostrar que o preço é herdado — comprar com base num preço
    de três meses atrás sem perceber é o risco."""
    s = _sessao(*_historico(("40", TipoPreco.COTADO, date(2026, 6, 1)),
                            ("45", TipoPreco.COMPRADO, date(2026, 9, 1))))

    sugestao = svc.sugerir_preco(s, 10)

    assert sugestao["preco_unitario"] == "45"
    assert sugestao["data"] == "2026-09-01"
    assert sugestao["fornecedor"] == "ACO FORTE LTDA"
    assert sugestao["tipo"] == "COMPRADO"


def test_sem_historico_nao_ha_sugestao():
    assert svc.sugerir_preco(_sessao(), 10) is None


def test_preco_herdado_fica_marcado_na_celula():
    s, cot, linha = _cotacao_montada(
        CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100))

    svc.lancar_preco(s, 21, 11, "38", COMPRADOR, origem="HERDADO", herdado_de=9)

    celula = next(o for o in s.adicionados if isinstance(o, CotacaoPreco))
    assert celula.origem.value == "HERDADO" and celula.herdado_de_cotacao_id == 9


# ---------------------------------------------------------------------------
# As rotas: preço é assunto de quem compra
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


def test_quem_pede_material_na_obra_nao_ve_preco(monkeypatch):
    """Decisão do dono: a obra acompanha a entrega, mas preço e fornecedor
    ficam com suprimentos."""
    s = SessaoFalsa(novo_usuario(5, P.ADMINISTRATIVO_OBRA), *_cadastro())
    c = _cliente(s, monkeypatch, 5)

    for caminho in ("/erp/suprimentos/cotacoes", "/erp/suprimentos/precos",
                    "/erp/api/suprimentos/precos",
                    "/erp/api/suprimentos/cotacoes"):
        assert c.get(caminho).status_code == 403, caminho


def test_o_comprador_ve_o_mapa_e_o_banco_de_precos(monkeypatch):
    s = SessaoFalsa(COMPRADOR, *_cadastro())
    c = _cliente(s, monkeypatch, 1)

    assert c.get("/erp/api/suprimentos/cotacoes").status_code == 200
    assert c.get("/erp/api/suprimentos/precos").status_code == 200


def test_lancar_precos_em_lote_conta_o_que_entrou_e_o_que_foi_recusado(monkeypatch):
    cot = Cotacao(id=1, numero="COT-0001", titulo="x",
                  status=StatusCotacao.ABERTA, criado_por=1)
    linha = CotacaoItem(id=11, cotacao_id=1, suprimento_item_id=7, numero=1)
    coluna = CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100)
    s = SessaoFalsa(COMPRADOR, *_cadastro(), cot, _item(), linha, coluna)
    c = _cliente(s, monkeypatch, 1)

    r = c.post("/erp/api/suprimentos/cotacoes/1/precos", json={"precos": [
        {"cotacao_fornecedor_id": 21, "cotacao_item_id": 11, "preco": "38,50"},
        {"cotacao_fornecedor_id": 21, "cotacao_item_id": 11, "preco": "0"},
    ]})

    corpo = r.get_json()
    assert r.status_code == 200
    assert corpo["gravados"] == 1 and len(corpo["recusados"]) == 1
    assert "maior que zero" in corpo["recusados"][0]["motivo"]


def test_lote_vazio_avisa(monkeypatch):
    s = SessaoFalsa(COMPRADOR, *_cadastro())
    r = _cliente(s, monkeypatch, 1).post("/erp/api/suprimentos/cotacoes/1/precos",
                                         json={"precos": []})
    assert r.status_code == 400
