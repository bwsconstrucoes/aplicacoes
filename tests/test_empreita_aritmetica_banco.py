"""A conta da medição por item: o total tem de bater com as linhas.

A falha achada na varredura de 12/09/2026: a medição por item somava as linhas
SEM arredondar e só arredondava no fim, enquanto cada linha era GRAVADA
arredondada. Com preço de três ou quatro casas — e a coluna aceita quatro,
porque preço de R$ 12,345 por metro existe — o total da medição saía diferente
da soma das próprias linhas dela.

Não é cosmético. O `valor_medido` é o que consome o saldo do contrato, o que a
retenção de garantia calcula em cima e o que vira título a pagar. Quem confere
a medição soma as linhas na calculadora e encontra outro número — e é aí que se
perde a confiança em TODOS os números, inclusive nos que estão certos.

COM BANCO DE VERDADE porque a soma passa por JOIN e a gravação por NUMERIC.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.titulos import empreita as svc
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, Obra, PerfilUsuario as P, RegimeTributario,
    TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    ContratoServico, ContratoServicoItem, EmpreitaAlcada, MedicaoItem,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    if not s.query(EmpreitaAlcada).count():
        s.add(EmpreitaAlcada(valor_ate=None,
                             perfis=["ADMIN", "DIRETOR_FINANCEIRO", "GESTOR_OBRA"],
                             descricao="Tudo"))
        s.flush()
    quem = Usuario(nome="Gestor", email="arit-gestor@teste.local", ativo=True,
                   senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.GESTOR_OBRA)
    prestador = Fornecedor(razao_social="Empreiteiro Silva ME",
                           cnpj_cpf="22333444000155", tipo_pessoa=TipoPessoa.PJ,
                           ativo=True, regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto")
    cat = Categoria(codigo="3.1.05", descricao="Mão de obra de terceiros")
    s.add_all([quem, prestador, obra, cat])
    s.flush()

    c = ContratoServico(
        numero=svc.proximo_numero(s), obra_id=obra.id, fornecedor_id=prestador.id,
        categoria_id=cat.id, objeto="Alvenaria de vedação", modo="ITEM",
        valor_total=Decimal("100000.00"), valor_aditivos=Decimal("0.00"),
        retencao_garantia_pct=Decimal("5.00"), exige_foto=False, status="VIGENTE",
        criado_por=quem.id)
    s.add(c)
    s.flush()

    # Preço de R$ 12,345 por m² — três casas, que a coluna aceita e a obra usa.
    itens = []
    for ordem in (1, 2, 3):
        i = ContratoServicoItem(
            contrato_id=c.id, ordem=ordem, descricao=f"Serviço {ordem}",
            unidade="M2", quantidade=Decimal("100.0000"),
            quantidade_aditivada=Decimal("0.0000"),
            preco_unitario=Decimal("12.3450"))
        s.add(i)
        itens.append(i)
    s.flush()
    return {"s": s, "gestor": quem, "contrato": c, "itens": itens}


def test_o_total_da_medicao_bate_com_a_soma_das_linhas(cenario):
    """Três linhas de R$ 12,345 cada. Somadas sem arredondar dão R$ 37,035 →
    R$ 37,04; arredondadas uma a uma dão R$ 12,34 × 3 = R$ 37,02. O sistema
    gravava a primeira conta e mostrava a segunda."""
    d = cenario
    s = d["s"]
    m = svc.registrar_medicao(s, d["contrato"].id, {
        "periodo_inicio": "2026-08-01", "periodo_fim": "2026-08-31",
        "itens": [{"contrato_item_id": i.id, "quantidade": "1"} for i in d["itens"]],
    }, d["gestor"])
    s.flush()

    linhas = s.scalars(select(MedicaoItem).where(
        MedicaoItem.medicao_id == m.id)).all()
    soma = sum(Decimal(l.valor) for l in linhas)
    assert Decimal(m.valor_medido) == soma, (
        f"a medição diz R$ {m.valor_medido} e as linhas dela somam R$ {soma}")


def test_a_retencao_sai_do_mesmo_valor_que_as_linhas_mostram(cenario):
    """A garantia é 5% do medido. Se o medido estiver 2 centavos errado, a
    garantia retida do contrato inteiro nasce errada junto."""
    d = cenario
    s = d["s"]
    m = svc.registrar_medicao(s, d["contrato"].id, {
        "periodo_inicio": "2026-08-01", "periodo_fim": "2026-08-31",
        "itens": [{"contrato_item_id": i.id, "quantidade": "1"} for i in d["itens"]],
    }, d["gestor"])
    s.flush()

    soma = sum(Decimal(l.valor) for l in s.scalars(
        select(MedicaoItem).where(MedicaoItem.medicao_id == m.id)).all())
    esperado = (soma * Decimal("5.00") / 100).quantize(Decimal("0.01"))
    assert Decimal(m.valor_retido) == esperado


def test_o_saldo_do_contrato_desconta_exatamente_o_que_foi_medido(cenario):
    """Saldo é valor vigente menos medido. Medido errado, saldo errado — e o
    erro se acumula a cada medição do contrato."""
    d = cenario
    s = d["s"]
    m = svc.registrar_medicao(s, d["contrato"].id, {
        "periodo_inicio": "2026-08-01", "periodo_fim": "2026-08-31",
        "itens": [{"contrato_item_id": i.id, "quantidade": "1"} for i in d["itens"]],
    }, d["gestor"])
    s.flush()

    est = svc.saldo(s, d["contrato"].id)
    assert Decimal(str(est["medido"])) == Decimal(m.valor_medido)
    assert Decimal(str(est["saldo"])) == (
        Decimal("100000.00") - Decimal(m.valor_medido))


def test_o_saldo_do_item_desconta_a_quantidade_medida(cenario):
    """O que sobra de cada serviço tem de refletir o que já foi medido dele."""
    d = cenario
    s = d["s"]
    svc.registrar_medicao(s, d["contrato"].id, {
        "periodo_inicio": "2026-08-01", "periodo_fim": "2026-08-31",
        "itens": [{"contrato_item_id": d["itens"][0].id, "quantidade": "30"}],
    }, d["gestor"])
    s.flush()

    por_item = svc._saldo_por_item(s, d["contrato"].id)
    assert por_item[d["itens"][0].id]["medido"] == 30.0
    assert por_item[d["itens"][0].id]["saldo"] == 70.0
    assert por_item[d["itens"][1].id]["saldo"] == 100.0


def test_nao_se_mede_mais_do_que_resta_do_item(cenario):
    """A trava que impede medir o mesmo serviço duas vezes por descuido."""
    from app.apps.erp.core.comum.auditoria import ErroValidacao

    d = cenario
    s = d["s"]
    svc.registrar_medicao(s, d["contrato"].id, {
        "periodo_inicio": "2026-08-01", "periodo_fim": "2026-08-31",
        "itens": [{"contrato_item_id": d["itens"][0].id, "quantidade": "100"}],
    }, d["gestor"])
    s.flush()

    with pytest.raises(ErroValidacao, match="restam"):
        svc.registrar_medicao(s, d["contrato"].id, {
            "periodo_inicio": "2026-09-01", "periodo_fim": "2026-09-30",
            "itens": [{"contrato_item_id": d["itens"][0].id, "quantidade": "1"}],
        }, d["gestor"])
