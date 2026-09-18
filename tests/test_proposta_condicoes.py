# ============================================================================
# AS CONDIÇÕES LIDAS NA PROPOSTA — pagamento, frete, CIF/FOB, prazo, validade.
#
# Pedido do dono, 18/09/2026: *"não só do mapa com os valores, mas de condição
# de pagamento, frete, valor de frete, se é frete CIF, se é FOB"*.
#
# POR QUE ISTO MERECE TESTE PRÓPRIO: frete e forma de pagamento entram no TOTAL
# do mapa. Uma leitura errada aqui não deixa um campo feio — troca o vencedor
# da cotação, e o comprador fecha com quem não era o mais barato acreditando
# que era.
# ============================================================================
import pytest

from app.apps.erp.core.suprimentos import proposta
from app.apps.erp.db.models.cadastros import CondicaoPagamento
from tests.conftest import SessaoFalsa


def _ler(condicoes, observacoes="", cadastradas=()):
    s = SessaoFalsa(*cadastradas)
    return proposta._condicoes(s, {"condicoes": condicoes,
                                   "observacoes": observacoes})


# ---------------------------------------------------------------------------
# 1. CIF e FOB, que quase nunca vêm escritos assim
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("texto", [
    "CIF", "posto obra", "frete incluso", "entrega", "FRETE GRÁTIS"])
def test_o_fornecedor_entrega_dito_de_cinco_jeitos(texto):
    assert _ler({"entrega": texto})["entrega"] == "ENTREGA"


@pytest.mark.parametrize("texto", ["FOB", "a retirar", "retirada na loja", "coleta"])
def test_nos_retiramos_dito_de_quatro_jeitos(texto):
    assert _ler({"entrega": texto})["entrega"] == "COLETA"


def test_proposta_com_as_duas_opcoes_nao_escolhe_por_ninguem():
    """O vendedor mandou preço com entrega E com retirada. Escolher uma seria
    inventar a decisão do comprador."""
    assert _ler({"entrega": "CIF ou FOB, a combinar"})["entrega"] is None


# ---------------------------------------------------------------------------
# 2. Frete: zero é resposta, vazio não é
# ---------------------------------------------------------------------------
def test_frete_gratis_vira_zero_e_nao_vazio():
    """Deixar vazio faria a coluna herdar o frete que estava lá antes — e o
    total do mapa sairia errado sem ninguém tocar em nada."""
    assert _ler({"frete": "grátis"})["frete"] == "0"


def test_frete_em_reais_no_formato_brasileiro():
    assert _ler({"frete": "R$ 1.250,50"})["frete"] == "1250.50"


def test_frete_que_a_proposta_nao_diz_fica_vazio():
    assert _ler({})["frete"] is None


# ---------------------------------------------------------------------------
# 3. Prazo e validade
# ---------------------------------------------------------------------------
def test_prazo_em_dias_sai_do_texto():
    assert _ler({"prazo_entrega_dias": "entrega em 15 dias"})["prazo_entrega_dias"] == 15


def test_prazo_absurdo_e_descartado():
    """"900 dias" é leitura errada, não prazo. Entrar no campo faria a tela
    mostrar uma entrega para 2029."""
    assert _ler({"prazo_entrega_dias": "900"})["prazo_entrega_dias"] is None


def test_validade_em_formato_brasileiro_vira_data_de_verdade():
    assert _ler({"validade": "30/09/2026"})["validade"] == "2026-09-30"


def test_validade_ja_em_iso_passa_direto():
    assert _ler({"validade": "2026-09-30"})["validade"] == "2026-09-30"


# ---------------------------------------------------------------------------
# 4. A forma de pagamento casada com o CADASTRO
# ---------------------------------------------------------------------------
def test_forma_de_pagamento_casa_com_a_condicao_cadastrada():
    cond = CondicaoPagamento(id=4, nome="28 dias", ativo=True)
    r = _ler({"forma_pagamento": "28 dias"}, cadastradas=(cond,))
    assert r["condicao_pagamento_id"] == 4


def test_forma_de_pagamento_que_nao_existe_no_cadastro_nao_chuta():
    """Preencher o campo que define o vencimento com a condição errada é pior
    do que deixá-lo em branco: o financeiro herda a data errada calado."""
    cond = CondicaoPagamento(id=4, nome="28 dias", ativo=True)
    r = _ler({"forma_pagamento": "faturado em 90/120/150"}, cadastradas=(cond,))
    assert r["condicao_pagamento_id"] is None
    assert r["forma_pagamento_lida"] == "faturado em 90/120/150"
    assert "não achei essa condição no cadastro" in r["resumo"]


def test_proposta_muda_sem_dizer_condicao_nenhuma():
    assert _ler({})["resumo"] == "a proposta não diz as condições"


def test_o_resumo_e_escrito_para_gente_ler():
    cond = CondicaoPagamento(id=4, nome="28 dias", ativo=True)
    r = _ler({"forma_pagamento": "28 dias", "frete": "0", "entrega": "CIF",
              "prazo_entrega_dias": "10"}, cadastradas=(cond,))
    assert "pagamento: 28 dias" in r["resumo"]
    assert "frete grátis" in r["resumo"]
    assert "o fornecedor entrega (CIF)" in r["resumo"]
    assert "entrega em 10 dia(s)" in r["resumo"]
