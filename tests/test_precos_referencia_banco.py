# ============================================================================
# "QUANTO ISSO COSTUMA CUSTAR?" — com BANCO DE VERDADE.
#
# Pedido do dono, 18/09/2026: *"na tela das solicitações já poder estar
# visualizando: ó, o insumo, onde está o último menor preço, qual o fornecedor,
# qual o valor"*.
#
# POR QUE ESTE ARQUIVO PRECISA DE POSTGRES, e não pode viver no dublê: o resumo
# de preços é WHERE, ORDER BY, GROUP BY e janela de data. O dublê da suíte
# IGNORA WHERE — devolveria o preço de outro insumo com cara de certo, que é
# exatamente o erro que este código existe para não cometer. Um teste que passa
# no dublê aqui não prova nada.
# ============================================================================
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.suprimentos import precos
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Insumo, PrecoHistorico, TipoPessoa, TipoPreco,
)

pytestmark = pytest.mark.banco

HOJE = date(2026, 9, 18)


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    arame = Insumo(codigo="T-ARAME", descricao="Arame Recozido BWG 18 (teste)", ativo=True)
    cimento = Insumo(codigo="T-CIMENTO", descricao="Cimento CPII 50kg (teste)", ativo=True)
    s.add_all([arame, cimento])
    almeida = Fornecedor(razao_social="ALMEIDA TESTE LTDA", tipo_pessoa=TipoPessoa.PJ,
                         nome_fantasia="Almeida", cnpj_cpf="35419548000199")
    gerdau = Fornecedor(razao_social="GERDAU TESTE S.A.", tipo_pessoa=TipoPessoa.PJ,
                        nome_fantasia="Gerdau", cnpj_cpf="07358761021399")
    s.add_all([almeida, gerdau])
    s.flush()

    def preco(insumo, valor, dias_atras, fornecedor=None):
        s.add(PrecoHistorico(
            insumo_id=insumo.id, preco_unitario=Decimal(valor),
            fornecedor_id=fornecedor.id if fornecedor else None,
            tipo=TipoPreco.COTADO, data=HOJE - timedelta(days=dias_atras),
            origem="PLANILHA"))

    preco(arame, "11.03", 5, almeida)          # o mais recente
    preco(arame, "9.09", 40, gerdau)           # o menor do ano
    preco(arame, "15.95", 100, almeida)
    preco(cimento, "49.58", 10, gerdau)
    s.flush()
    return {"s": s, "arame": arame, "cimento": cimento,
            "almeida": almeida, "gerdau": gerdau}


def test_o_ultimo_preco_e_o_mais_recente_com_quem_deu(cenario):
    r = precos.resumo_por_insumo(cenario["s"], [cenario["arame"].id], hoje=HOJE)
    d = r[cenario["arame"].id]
    assert d["ultimo"] == "11.0300"
    assert d["ultimo_de"] == "Almeida"
    assert d["ultimo_em"] == (HOJE - timedelta(days=5)).isoformat()


def test_o_menor_do_ano_vem_com_o_fornecedor_que_deu(cenario):
    """É a régua da negociação: "da última vez fulano fez por 9,09"."""
    d = precos.resumo_por_insumo(
        cenario["s"], [cenario["arame"].id], hoje=HOJE)[cenario["arame"].id]
    assert d["menor"] == "9.0900"
    assert d["menor_de"] == "Gerdau"


def test_o_preco_de_um_insumo_nao_vaza_para_o_outro(cenario):
    """O teste que o dublê não consegue fazer: ele ignora WHERE e devolveria o
    arame no resumo do cimento, com cara de certo."""
    r = precos.resumo_por_insumo(
        cenario["s"], [cenario["arame"].id, cenario["cimento"].id], hoje=HOJE)
    assert r[cenario["cimento"].id]["ultimo"] == "49.5800"
    assert r[cenario["cimento"].id]["menor"] == "49.5800"
    assert r[cenario["arame"].id]["ultimo"] == "11.0300"


def test_preco_fora_da_janela_de_doze_meses_nao_entra_no_menor(cenario):
    s = cenario["s"]
    s.add(PrecoHistorico(
        insumo_id=cenario["arame"].id, preco_unitario=Decimal("2.00"),
        tipo=TipoPreco.COTADO, data=HOJE - timedelta(days=800), origem="PLANILHA"))
    s.flush()
    d = precos.resumo_por_insumo(s, [cenario["arame"].id], hoje=HOJE)[cenario["arame"].id]
    assert d["menor"] == "9.0900", "preço de dois anos atrás não é referência"


def test_insumo_cujo_unico_preco_e_velho_avisa_que_e_velho(sessao_real):
    """Um número de 2023 mostrado sem aviso vira orçamento errado."""
    s = sessao_real
    velho = Insumo(codigo="T-TELHA", descricao="Telha Velha (teste)", ativo=True)
    s.add(velho)
    s.flush()
    s.add(PrecoHistorico(insumo_id=velho.id, preco_unitario=Decimal("50.00"),
                         tipo=TipoPreco.COTADO, data=HOJE - timedelta(days=700),
                         origem="PLANILHA"))
    s.flush()
    d = precos.resumo_por_insumo(s, [velho.id], hoje=HOJE)[velho.id]
    assert d["ultimo_fora_da_janela"] is True
    assert "mais de um ano atrás" in d["resumo"]


def test_insumo_sem_historico_nenhum_nao_aparece(sessao_real):
    s = sessao_real
    novo = Insumo(codigo="T-SEMPRECO", descricao="Insumo Sem Preço (teste)", ativo=True)
    s.add(novo)
    s.flush()
    assert precos.resumo_por_insumo(s, [novo.id], hoje=HOJE) == {}


def test_lista_vazia_nao_vai_ao_banco(sessao_real):
    assert precos.resumo_por_insumo(sessao_real, []) == {}


def test_a_frase_e_escrita_como_o_comprador_falaria(cenario):
    d = precos.resumo_por_insumo(
        cenario["s"], [cenario["arame"].id], hoje=HOJE)[cenario["arame"].id]
    assert "último R$ 11,03 (Almeida)" in d["resumo"]
    assert "menor no ano R$ 9,09 (Gerdau)" in d["resumo"]
    assert "cotação(ões) no último ano" in d["resumo"]
