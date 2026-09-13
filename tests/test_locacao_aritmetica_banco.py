"""A conta da locação: o valor do período tem de bater com os itens da ficha.

A mesma falha da medição de empreita, achada na mesma varredura (12/09/2026):
`valor_periodo` somava os itens SEM arredondar e arredondava só no fim,
enquanto a ficha do contrato mostra o valor de CADA item já arredondado. Com
preço de três ou quatro casas — e a coluna aceita quatro, porque diária de
R$ 12,3450 por escora existe — a soma da tela e o total do contrato divergiam.

Aqui é pior que na medição: esse total vira o `valor_previsto` de cada parcela,
ou seja, **é o valor que o ERP diz que a BWS deve pagar todo mês**. A pessoa
soma os itens da ficha, confere com o boleto da locadora e encontra centavos de
diferença que ninguém sabe explicar.

COM BANCO DE VERDADE porque a soma percorre os itens e o valor é gravado em
NUMERIC na parcela.
"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core import locacoes as svc
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Obra, PerfilUsuario as P, RegimeTributario, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    ContratoLocacao, LocacaoItem, LocacaoParcela,
)

from conftest import hoje

pytestmark = pytest.mark.banco


@pytest.fixture
def contrato(sessao_real):
    s = sessao_real
    u = Usuario(nome="Ruan do administrativo", email="ruan-arit@teste.local",
                senha_hash=gerar_hash("senha-de-teste-1234"),
                perfil=P.ADMINISTRATIVO_OBRA, ativo=True)
    obra = Obra(codigo="OBRATESTE", nome="Obra de teste")
    loc = Fornecedor(razao_social="Locadora Teste Ltda", cnpj_cpf="71000001000184",
                     tipo_pessoa=TipoPessoa.PJ,
                     regime_tributario=RegimeTributario.SIMPLES, ativo=True)
    s.add_all([u, obra, loc])
    s.flush()

    c = ContratoLocacao(numero="LOCARIT1", fornecedor_id=loc.id, obra_id=obra.id,
                        periodicidade="MENSAL", dia_vencimento=5,
                        data_inicio=hoje() - timedelta(days=40),
                        status="ATIVO", criado_por=u.id)
    s.add(c)
    s.flush()
    # Diária de R$ 12,3450 — três casas, que a coluna aceita e a locadora usa.
    for n in range(3):
        s.add(LocacaoItem(contrato_id=c.id, descricao=f"Escora metálica {n + 1}",
                          quantidade=Decimal("1"), quantidade_devolvida=Decimal("0"),
                          valor_unitario=Decimal("12.3450"), obra_id=obra.id))
    s.flush()
    return {"s": s, "contrato": c, "usuario": u}


def _soma_da_ficha(s, contrato_id):
    ficha = svc.detalhar(s, contrato_id)
    return sum(Decimal(str(i["valor_periodo"])) for i in ficha["itens"])


def test_o_valor_do_periodo_bate_com_os_itens_da_ficha(contrato):
    """Três escoras de R$ 12,345. A ficha mostra R$ 12,34 em cada; o contrato
    dizia R$ 37,04, e a soma da tela dá R$ 37,02."""
    d = contrato
    s = d["s"]
    total = svc.valor_periodo(s, d["contrato"].id)
    assert total == _soma_da_ficha(s, d["contrato"].id), (
        f"o contrato diz R$ {total} e os itens da ficha somam "
        f"R$ {_soma_da_ficha(s, d['contrato'].id)}")


def test_a_parcela_prevista_nasce_do_mesmo_valor_que_a_ficha_mostra(contrato):
    """É este número que vira a conta a pagar do mês — e que alguém vai
    conferir contra o boleto da locadora."""
    d = contrato
    s = d["s"]
    svc.gerar_previsao(s, d["contrato"].id, meses=1, usuario=d["usuario"])
    s.flush()

    parcela = s.scalars(select(LocacaoParcela).where(
        LocacaoParcela.contrato_id == d["contrato"].id)).first()
    assert Decimal(parcela.valor_previsto) == _soma_da_ficha(s, d["contrato"].id)


def test_devolver_um_item_reduz_o_valor_pelo_valor_daquele_item(contrato):
    """Devolveu uma escora, o contrato cai exatamente o que aquela escora
    custava na ficha — nem um centavo a mais."""
    d = contrato
    s = d["s"]
    antes = svc.valor_periodo(s, d["contrato"].id)
    item = s.scalars(select(LocacaoItem).where(
        LocacaoItem.contrato_id == d["contrato"].id)).first()
    valor_do_item = (Decimal(item.quantidade) * Decimal(item.valor_unitario)
                     ).quantize(Decimal("0.01"))

    svc.devolver(s, d["contrato"].id,
                 {"item_id": item.id, "quantidade": "1"}, d["usuario"])
    s.flush()

    assert svc.valor_periodo(s, d["contrato"].id) == antes - valor_do_item
    assert svc.valor_periodo(s, d["contrato"].id) == _soma_da_ficha(
        s, d["contrato"].id)


def test_devolver_tudo_zera_e_encerra(contrato):
    """Sem equipamento em obra não há aluguel — e contrato que continua
    gerando parcela depois de tudo devolvido é dinheiro saindo à toa."""
    d = contrato
    s = d["s"]
    for item in s.scalars(select(LocacaoItem).where(
            LocacaoItem.contrato_id == d["contrato"].id)).all():
        svc.devolver(s, d["contrato"].id,
                     {"item_id": item.id, "quantidade": "1"}, d["usuario"])
    s.flush()

    assert svc.valor_periodo(s, d["contrato"].id) == Decimal("0.00")
    assert s.get(ContratoLocacao, d["contrato"].id).status == "ENCERRADO"


def test_nao_se_devolve_mais_do_que_esta_em_obra(contrato):
    from app.apps.erp.core.comum.auditoria import ErroValidacao

    d = contrato
    s = d["s"]
    item = s.scalars(select(LocacaoItem).where(
        LocacaoItem.contrato_id == d["contrato"].id)).first()
    with pytest.raises(ErroValidacao, match="em obra"):
        svc.devolver(s, d["contrato"].id,
                     {"item_id": item.id, "quantidade": "2"}, d["usuario"])
