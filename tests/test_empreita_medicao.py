"""Medição de empreita consumindo o saldo do item (core/titulos/empreita.py).

O contrato tem uma planilha de serviços. Cada medição consome quantidade de
itens específicos, e ninguém pode medir mais do que resta — sem aditivo.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import empreita
from app.apps.erp.db.models.cadastros import PerfilUsuario as P
from app.apps.erp.db.models.financeiro import ContratoServico, MedicaoItem

from conftest import (
    SessaoFalsa, nova_medicao_item, novo_item_contrato, novo_usuario,
)


def _contrato(id_=1, total="1000.00", aditivos="0.00", status="VIGENTE"):
    """Só se mede contrato VIGENTE — qualquer outro status é bloqueio (M7)."""
    return ContratoServico(id=id_, valor_total=Decimal(total),
                           valor_aditivos=Decimal(aditivos), status=status)


# ---------------------------------------------------------------------------
# Valor vigente
# ---------------------------------------------------------------------------
def test_valor_vigente_soma_os_aditivos():
    c = _contrato(total="1000.00", aditivos="250.00")

    assert empreita._valor_vigente(c) == Decimal("1250.00")


def test_valor_vigente_aceita_aditivo_negativo():
    c = _contrato(total="1000.00", aditivos="-100.00")

    assert empreita._valor_vigente(c) == Decimal("900.00")


# ---------------------------------------------------------------------------
# Saldo por item
# ---------------------------------------------------------------------------
def test_item_sem_medicao_tem_saldo_cheio():
    item = novo_item_contrato(1, quantidade="100", preco="10.00")
    s = SessaoFalsa(item)

    saldos = empreita._saldo_por_item(s, 1)

    assert saldos[1]["quantidade"] == 100.0
    assert saldos[1]["medido"] == 0.0
    assert saldos[1]["saldo"] == 100.0
    assert saldos[1]["percentual"] == 0.0
    assert saldos[1]["valor_total"] == 1000.0


def test_medicao_consome_o_saldo_do_item():
    item = novo_item_contrato(1, quantidade="100", preco="10.00")
    s = SessaoFalsa(item, nova_medicao_item(1, medicao_id=1, contrato_item_id=1,
                                            quantidade="30"))

    saldos = empreita._saldo_por_item(s, 1)

    assert saldos[1]["medido"] == 30.0
    assert saldos[1]["saldo"] == 70.0
    assert saldos[1]["percentual"] == 30.0


def test_medicoes_sucessivas_acumulam_no_mesmo_item():
    item = novo_item_contrato(1, quantidade="100", preco="10.00")
    s = SessaoFalsa(
        item,
        nova_medicao_item(1, medicao_id=1, contrato_item_id=1, quantidade="30"),
        nova_medicao_item(2, medicao_id=2, contrato_item_id=1, quantidade="45"))

    saldos = empreita._saldo_por_item(s, 1)

    assert saldos[1]["medido"] == 75.0
    assert saldos[1]["saldo"] == 25.0


def test_quantidade_aditivada_aumenta_o_saldo_do_item():
    item = novo_item_contrato(1, quantidade="100", aditivada="50", preco="10.00")
    s = SessaoFalsa(item, nova_medicao_item(1, medicao_id=1, contrato_item_id=1,
                                            quantidade="120"))

    saldos = empreita._saldo_por_item(s, 1)

    assert saldos[1]["quantidade"] == 150.0
    assert saldos[1]["saldo"] == 30.0


def test_saldo_de_cada_item_e_independente():
    s = SessaoFalsa(
        novo_item_contrato(1, descricao="Alvenaria", quantidade="100", preco="10.00", ordem=1),
        novo_item_contrato(2, descricao="Pintura", quantidade="200", preco="5.00", ordem=2),
        nova_medicao_item(1, medicao_id=1, contrato_item_id=1, quantidade="40"))

    saldos = empreita._saldo_por_item(s, 1)

    assert saldos[1]["saldo"] == 60.0
    assert saldos[2]["saldo"] == 200.0


# ---------------------------------------------------------------------------
# A trava: não se mede além do saldo
# ---------------------------------------------------------------------------
def test_nao_deixa_medir_alem_do_saldo_do_item():
    item = novo_item_contrato(1, descricao="Alvenaria", unidade="m2",
                              quantidade="100", preco="10.00")
    s = SessaoFalsa(_contrato(), item,
                    nova_medicao_item(1, medicao_id=1, contrato_item_id=1,
                                      quantidade="90"))
    usuario = novo_usuario(5, P.SUPERVISOR_OBRA)

    with pytest.raises(ErroValidacao) as erro:
        empreita.registrar_medicao(
            s, 1, {"itens": [{"contrato_item_id": 1, "quantidade": "20"}]}, usuario)

    assert "Alvenaria" in str(erro.value)
    assert "Aditive o item antes" in str(erro.value)


def test_medir_exatamente_o_saldo_restante_e_permitido():
    """A trava é 'mais do que resta'; zerar o item é legítimo.

    O valor da medição sai da planilha do contrato — 10 unidades a R$ 10,00 —
    e não de um número digitado à mão.
    """
    item = novo_item_contrato(1, descricao="Alvenaria", quantidade="100", preco="10.00")
    s = SessaoFalsa(_contrato(), item,
                    nova_medicao_item(1, medicao_id=1, contrato_item_id=1,
                                      quantidade="90"))
    usuario = novo_usuario(5, P.SUPERVISOR_OBRA)

    medicao = empreita.registrar_medicao(
        s, 1, {"itens": [{"contrato_item_id": 1, "quantidade": "10"}]}, usuario)

    assert medicao.valor_medido == Decimal("100.00")
    assert medicao.status == "MEDIDA"
    assert medicao.medido_por == 5
    linhas = [o for o in s.adicionados if isinstance(o, MedicaoItem)]
    assert len(linhas) == 1
    assert linhas[0].quantidade == Decimal("10")
    assert linhas[0].valor == Decimal("100.00")


def test_contrato_nao_vigente_nao_aceita_medicao():
    item = novo_item_contrato(1, quantidade="100", preco="10.00")
    s = SessaoFalsa(_contrato(status="RASCUNHO"), item)
    usuario = novo_usuario(5, P.SUPERVISOR_OBRA)

    with pytest.raises(ErroValidacao) as erro:
        empreita.registrar_medicao(
            s, 1, {"itens": [{"contrato_item_id": 1, "quantidade": "10"}]}, usuario)

    assert "só se mede contrato vigente" in str(erro.value)


def test_medicao_registrada_deixa_rastro_na_auditoria():
    item = novo_item_contrato(1, quantidade="100", preco="10.00")
    s = SessaoFalsa(_contrato(), item)
    usuario = novo_usuario(5, P.SUPERVISOR_OBRA)

    empreita.registrar_medicao(
        s, 1, {"itens": [{"contrato_item_id": 1, "quantidade": "10"}]}, usuario)

    assert len(s.eventos) == 1
    assert s.eventos[0]["ac"] == "MEDICAO_REGISTRADA"


def test_item_de_outro_contrato_e_recusado():
    s = SessaoFalsa(_contrato(), novo_item_contrato(1, quantidade="100", preco="10.00"))
    usuario = novo_usuario(5, P.SUPERVISOR_OBRA)

    with pytest.raises(ErroValidacao) as erro:
        empreita.registrar_medicao(
            s, 1, {"itens": [{"contrato_item_id": 999, "quantidade": "1"}]}, usuario)

    assert "não pertence a este contrato" in str(erro.value)


def test_medicao_com_todas_as_quantidades_zeradas_e_recusada():
    s = SessaoFalsa(_contrato(), novo_item_contrato(1, quantidade="100", preco="10.00"))
    usuario = novo_usuario(5, P.SUPERVISOR_OBRA)

    with pytest.raises(ErroValidacao) as erro:
        empreita.registrar_medicao(
            s, 1, {"itens": [{"contrato_item_id": 1, "quantidade": "0"}]}, usuario)

    assert "ao menos um serviço" in str(erro.value)


def test_contrato_inexistente_e_recusado():
    usuario = novo_usuario(5, P.SUPERVISOR_OBRA)

    with pytest.raises(ErroValidacao) as erro:
        empreita.registrar_medicao(SessaoFalsa(), 42, {"itens": []}, usuario)

    assert "Contrato não encontrado" in str(erro.value)
