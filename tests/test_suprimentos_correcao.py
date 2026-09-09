"""A obra pede errado — e quem recebe corrige, com assinatura.

O que estes testes garantem, em português:

  - correção sem MOTIVO é recusada: sem o motivo escrito, a mudança vira "eu
    não pedi isso" duas semanas depois;
  - depois que o pedido de compra saiu, corrigir aqui é RECUSADO — senão o
    fornecedor recebeu uma coisa e o sistema passa a dizer outra;
  - trocar o material ou a unidade APAGA os preços já digitados no mapa
    daquela linha, porque eles eram de outra coisa;
  - o que não veio na correção não é mexido: corrigir a unidade não pode
    apagar a especificação.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import solicitacao as svc
from app.apps.erp.db.models.cadastros import (
    Cotacao, CotacaoItem, CotacaoPreco, Insumo, Obra, PedidoItemReserva,
    PerfilUsuario as P, StatusCotacao, StatusItemSuprimento, SuprimentoItem,
    UnidadeCompra,
)

from conftest import SessaoFalsa, novo_usuario

COMPRADOR = novo_usuario(1, P.ADMIN, nome="Ruan Pablo")

CIMENTO = Insumo(id=10, codigo="INS-0010", descricao="Cimento CP-II")
CIMENTO_ESTRUTURAL = Insumo(id=11, codigo="INS-0011", descricao="Cimento CP-V ARI")
SACO = UnidadeCompra(codigo="SC", descricao="Saco")
BAG = UnidadeCompra(codigo="BAG", descricao="Big bag")
OBRA_A = Obra(id=1, codigo="CREPETRIUNFO", nome="Creche Triunfo")
OBRA_B = Obra(id=2, codigo="ESCPLANALTO", nome="Escola Planalto")


def item(**extra):
    dados = dict(id=100, solicitacao_id=5, numero=1, insumo_id=CIMENTO.id,
                 especificacao="cinza", quantidade=Decimal("50"),
                 quantidade_recebida=Decimal("0"), unidade="SC",
                 obra_id=OBRA_A.id, status=StatusItemSuprimento.SOLICITACAO)
    dados.update(extra)
    return SuprimentoItem(**dados)


def sessao(alvo, *mais):
    return SessaoFalsa(alvo, CIMENTO, CIMENTO_ESTRUTURAL, SACO, BAG,
                       OBRA_A, OBRA_B, COMPRADOR, *mais)


def detalhe(s):
    return json.loads(s.eventos[-1]["dt"])


# ---------------------------------------------------------------------------
# O motivo é obrigatório
# ---------------------------------------------------------------------------
def test_correcao_sem_motivo_e_recusada():
    i = item()
    s = sessao(i)
    with pytest.raises(ErroValidacao, match="motivo"):
        svc.editar_item(s, i.id, {"unidade": "BAG"}, COMPRADOR)
    assert i.unidade == "SC", "a recusa vem antes de mexer no item"


def test_motivo_de_uma_palavra_nao_serve():
    i = item()
    with pytest.raises(ErroValidacao, match="motivo"):
        svc.editar_item(sessao(i), i.id, {"unidade": "BAG", "motivo": "erro"},
                        COMPRADOR)


def test_correcao_que_nao_muda_nada_e_recusada():
    i = item()
    with pytest.raises(ErroValidacao, match="Nada foi alterado"):
        svc.editar_item(sessao(i), i.id,
                        {"unidade": "SC", "motivo": "conferindo o pedido"},
                        COMPRADOR)


# ---------------------------------------------------------------------------
# O que a correção grava
# ---------------------------------------------------------------------------
def test_trocar_a_unidade_grava_de_e_para():
    i = item()
    s = sessao(i)
    r = svc.editar_item(s, i.id,
                        {"unidade": "BAG", "motivo": "a obra pediu em saco, "
                                                     "mas o fornecedor só vende em bag"},
                        COMPRADOR)
    assert i.unidade == "BAG"
    assert r["mudancas"]["unidade"] == {"de": "SC", "para": "BAG"}
    d = detalhe(s)
    assert d["motivo"].startswith("a obra pediu")
    assert d["mudancas"]["unidade"]["de"] == "SC"
    assert s.eventos[-1]["ac"] == "CORRIGIDO"
    assert s.eventos[-1]["ui"] == COMPRADOR.id, "quem corrigiu tem de ficar gravado"


def test_trocar_o_insumo_grava_o_nome_e_nao_o_numero():
    """O registro é lido por gente. "insumo 10 → 11" não diz nada a ninguém."""
    i = item()
    s = sessao(i)
    svc.editar_item(s, i.id, {"insumo_id": CIMENTO_ESTRUTURAL.id,
                              "motivo": "material solicitado errado"}, COMPRADOR)
    assert i.insumo_id == CIMENTO_ESTRUTURAL.id
    assert detalhe(s)["mudancas"]["insumo_id"] == {
        "de": "Cimento CP-II", "para": "Cimento CP-V ARI"}


def test_trocar_a_obra_grava_o_codigo_da_obra():
    i = item()
    s = sessao(i)
    svc.editar_item(s, i.id, {"obra_id": OBRA_B.id,
                              "motivo": "material era da escola, não da creche"},
                    COMPRADOR)
    assert i.obra_id == OBRA_B.id
    assert detalhe(s)["mudancas"]["obra_id"] == {
        "de": "CREPETRIUNFO", "para": "ESCPLANALTO"}


def test_corrigir_a_unidade_nao_apaga_a_especificacao():
    """O que não veio na correção não é mexido. Sem isto, corrigir uma célula
    limparia as outras — que é exatamente o erro que a planilha comete."""
    i = item()
    svc.editar_item(sessao(i), i.id,
                    {"unidade": "BAG", "motivo": "conversão de saco para bag"},
                    COMPRADOR)
    assert i.especificacao == "cinza"
    assert i.quantidade == Decimal("50")


def test_quantidade_com_virgula_e_aceita():
    i = item()
    svc.editar_item(sessao(i), i.id,
                    {"quantidade": "37,5", "motivo": "conferência do quantitativo"},
                    COMPRADOR)
    assert i.quantidade == Decimal("37.5")


def test_quantidade_zerada_e_recusada():
    i = item()
    with pytest.raises(ErroValidacao, match="maior que zero"):
        svc.editar_item(sessao(i), i.id,
                        {"quantidade": "0", "motivo": "não precisa mais"}, COMPRADOR)


def test_unidade_fora_do_cadastro_e_recusada():
    i = item()
    with pytest.raises(ErroValidacao, match="não existe no cadastro"):
        svc.editar_item(sessao(i), i.id,
                        {"unidade": "CARRADA", "motivo": "unidade errada na obra"},
                        COMPRADOR)


# ---------------------------------------------------------------------------
# Quando é tarde demais
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("situacao", sorted(
    svc.CORRECAO_TARDE_DEMAIS, key=lambda x: x.value))
def test_depois_do_pedido_emitido_a_correcao_e_recusada(situacao):
    i = item(status=situacao)
    with pytest.raises(ErroValidacao, match="Cancele o item|já está em"):
        svc.editar_item(sessao(i), i.id,
                        {"unidade": "BAG", "motivo": "unidade errada na obra"},
                        COMPRADOR)


def test_item_com_recebimento_nao_e_corrigido():
    i = item(quantidade_recebida=Decimal("10"))
    with pytest.raises(ErroValidacao, match="recebimento"):
        svc.editar_item(sessao(i), i.id,
                        {"quantidade": "80", "motivo": "quantitativo revisado"},
                        COMPRADOR)


def test_item_reservado_em_pedido_vivo_nao_e_corrigido():
    i = item(status=StatusItemSuprimento.AUTORIZACAO)
    s = sessao(i, PedidoItemReserva(suprimento_item_id=i.id, pedido_id=9))
    with pytest.raises(ErroValidacao, match="pedido de compra que está em pé"):
        svc.editar_item(s, i.id, {"unidade": "BAG", "motivo": "unidade errada"},
                        COMPRADOR)


def test_pode_corrigir_concorda_com_a_recusa():
    """Se as duas divergirem, a tela oferece um botão que o servidor nega — e
    quem usa acha que o sistema está quebrado."""
    for situacao in StatusItemSuprimento:
        i = item(status=situacao)
        s = sessao(i)
        oferece = svc.pode_corrigir(s, i)
        try:
            svc.editar_item(s, i.id, {"unidade": "BAG", "motivo": "unidade errada"},
                            COMPRADOR)
            aceitou = True
        except ErroValidacao as e:
            aceitou = "já está em" not in str(e)
        assert oferece == aceitou, f"divergem em {situacao.value}"


# ---------------------------------------------------------------------------
# O mapa de cotação
# ---------------------------------------------------------------------------
def mapa_com_preco(item_id, status=StatusCotacao.ABERTA):
    return [
        Cotacao(id=7, numero="COT-0007", titulo="Cimento", status=status, criado_por=1),
        CotacaoItem(id=70, cotacao_id=7, suprimento_item_id=item_id, numero=1),
        CotacaoPreco(id=700, cotacao_fornecedor_id=1, cotacao_item_id=70,
                     preco_unitario=Decimal("38.50")),
    ]


def test_trocar_o_insumo_apaga_os_precos_daquela_linha_no_mapa():
    """O preço era de outro material. Deixar ali fecharia a compra pelo preço
    da coisa errada — pior que apagar."""
    i = item(status=StatusItemSuprimento.COTACAO)
    objetos = mapa_com_preco(i.id)
    s = sessao(i, *objetos)
    r = svc.editar_item(s, i.id, {"insumo_id": CIMENTO_ESTRUTURAL.id,
                                  "motivo": "material solicitado errado"}, COMPRADOR)
    assert objetos[2] in s.removidos
    assert r["avisos"] and "apagados" in r["avisos"][0]


def test_trocar_a_unidade_tambem_apaga_o_preco():
    i = item(status=StatusItemSuprimento.COTACAO)
    objetos = mapa_com_preco(i.id)
    s = sessao(i, *objetos)
    svc.editar_item(s, i.id, {"unidade": "BAG", "motivo": "conversão de unidade"},
                    COMPRADOR)
    assert objetos[2] in s.removidos


def test_corrigir_so_a_especificacao_nao_apaga_preco():
    """Ajustar a grafia não muda o que está sendo comprado."""
    i = item(status=StatusItemSuprimento.COTACAO)
    objetos = mapa_com_preco(i.id)
    s = sessao(i, *objetos)
    r = svc.editar_item(s, i.id, {"especificacao": "cinza CP-II 50kg",
                                  "motivo": "especificação mal escrita"}, COMPRADOR)
    assert s.removidos == []
    assert r["avisos"] == []


def test_cotacao_fechada_nao_tem_preco_apagado():
    """Mapa fechado é histórico. Apagar preço de lá seria apagar o passado."""
    i = item(status=StatusItemSuprimento.COTACAO)
    objetos = mapa_com_preco(i.id, status=StatusCotacao.FECHADA)
    s = sessao(i, *objetos)
    svc.editar_item(s, i.id, {"unidade": "BAG", "motivo": "conversão de unidade"},
                    COMPRADOR)
    assert s.removidos == []


def test_preco_de_outro_item_do_mesmo_mapa_nao_e_apagado():
    i = item(status=StatusItemSuprimento.COTACAO)
    objetos = mapa_com_preco(i.id)
    de_outro = CotacaoPreco(id=701, cotacao_fornecedor_id=1, cotacao_item_id=71,
                            preco_unitario=Decimal("12.00"))
    s = sessao(i, *objetos, CotacaoItem(id=71, cotacao_id=7,
                                        suprimento_item_id=999, numero=2), de_outro)
    svc.editar_item(s, i.id, {"unidade": "BAG", "motivo": "conversão de unidade"},
                    COMPRADOR)
    assert de_outro not in s.removidos
