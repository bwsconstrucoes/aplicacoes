# ============================================================================
# ERP — core/suprimentos/lancamento.py
# A conta a pagar nasce do recebimento.
#
# O dono, em 20/11/2026: "eu quero gerar o financeiro a partir do recebimento
# (…) que atualmente é assim que funciona. A pessoa vai receber o pedido e
# lança nota, lança boleto, lança tudo e gera a obrigação financeira. Ou
# confirma uma previsão que já existe."
#
# COMO, E POR QUE ASSIM. Este módulo NÃO cria título. Ele faz duas coisas:
#
#   1. `preparar` — entrega à tela de Lançar tudo o que o pedido já sabe:
#      credor, valor do que chegou, rateio por obra com a conta do plano de
#      cada insumo, e as parcelas da previsão de pagamento. A pessoa joga a
#      nota e o boleto, confere e envia.
#   2. `vincular` — depois que o título foi criado PELO CAMINHO DE SEMPRE,
#      grava que ele é deste pedido e baixa a previsão correspondente.
#
# O título continua nascendo em `titulos.service.criar_titulo`, com as
# críticas fiscais, de duplicidade e de alçada que já valem para qualquer
# lançamento. Um segundo caminho para o dinheiro sair, por dentro do
# suprimento, é justamente o que o ERP evita (`SUPRIMENTOS.md` §8.2).
#
# "CONFIRMA UMA PREVISÃO QUE JÁ EXISTE": as parcelas vêm da previsão gerada na
# autorização do pedido. Lançar o título marca essas previsões como
# cumpridas — e elas saem da Projeção de pagamentos, que é só do que AINDA
# não virou conta.
# ============================================================================
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.core.suprimentos import acompanhamento as acomp
from app.apps.erp.db.models.cadastros import (
    Insumo, PedidoCompra, PedidoItem, PedidoTitulo, PrevisaoPagamento,
    Recebimento, RecebimentoItem, StatusPedidoCompra, SuprimentoItem, Usuario,
)

logger = logging.getLogger(__name__)

_CENT = Decimal("0.01")
_ZERO = Decimal("0")


def _d(v: Any) -> Decimal:
    return Decimal(str(v if v is not None else 0))


def _pedido_autorizado(s: Session, usuario: Usuario, pedido_id: int) -> PedidoCompra:
    from app.apps.erp.core.suprimentos.recebimento import exigir_pedido_no_escopo
    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    exigir_pedido_no_escopo(s, usuario, pedido_id)
    if pedido.status is not StatusPedidoCompra.AUTORIZADO:
        raise ErroValidacao(
            f"O pedido {pedido.numero} não está autorizado — não há o que lançar.")
    return pedido


def preparar(s: Session, usuario: Usuario, pedido_id: int, *,
             recebimento_id: Optional[int] = None,
             hoje: Optional[date] = None) -> dict[str, Any]:
    """O lançamento já preenchido com o que o pedido sabe.

    O VALOR é o do que CHEGOU e ainda não virou conta — não o do pedido
    inteiro: dez carradas viram dez notas. Com o pedido entregue por inteiro,
    o frete e o desconto do pedido entram na conta final.
    """
    hoje = hoje or date.today()
    pedido = _pedido_autorizado(s, usuario, pedido_id)
    base = acomp.carregar(s, [pedido])
    itens = base["itens"].get(pedido.id, [])
    fin = acomp.financeiro_do_pedido(pedido, base)
    total = acomp.total_do_pedido(pedido, itens)
    entregue = bool(itens) and all(i["saldo"] <= 0 for i in itens)

    recebimento = None
    if recebimento_id:
        recebimento = s.get(Recebimento, int(recebimento_id))
        if recebimento is None or recebimento.pedido_id != pedido.id:
            raise ErroNaoEncontrado("Recebimento não encontrado.")

    contas = {i.id: i.categoria_id for i in s.scalars(select(Insumo).where(
        Insumo.id.in_({x for x in s.scalars(
            select(SuprimentoItem.insumo_id)
            .join(PedidoItem, PedidoItem.suprimento_item_id == SuprimentoItem.id)
            .where(PedidoItem.pedido_id == pedido.id)).all()} or {0}))).all()}
    insumo_de = dict(s.execute(
        select(PedidoItem.id, SuprimentoItem.insumo_id)
        .join(SuprimentoItem, SuprimentoItem.id == PedidoItem.suprimento_item_id)
        .where(PedidoItem.pedido_id == pedido.id)).all())

    # O que chegou, por (obra, conta do plano): é o rateio do título.
    pesos: dict[tuple[int, Optional[int]], Decimal] = defaultdict(lambda: _ZERO)
    for i in itens:
        if i["valor_recebido"] > 0:
            conta = contas.get(insumo_de.get(i["pedido_item_id"]))
            pesos[(i["obra_id"], conta)] += i["valor_recebido"]
    recebido = sum(pesos.values(), _ZERO)

    if entregue:
        valor = total - fin["em_titulo"]
    else:
        valor = recebido - fin["em_titulo"]
    valor = max(valor, _ZERO).quantize(_CENT)

    avisos = []
    if recebido <= 0:
        avisos.append("Ainda não há recebimento registrado para este pedido. "
                      "Registre o que chegou antes de lançar a nota.")
    elif valor <= 0:
        avisos.append("Tudo o que chegou deste pedido já tem conta a pagar lançada.")
    if not entregue and recebido > 0:
        avisos.append("O pedido ainda não chegou por inteiro: o valor sugerido é "
                      "o do que já chegou. Confira com o valor da nota.")
    sem_conta = [k for k in pesos if k[1] is None]
    if sem_conta:
        avisos.append("Há insumo deste pedido sem conta do plano financeiro no "
                      "cadastro — escolha a conta no rateio.")

    from app.apps.erp.core.suprimentos.projecao import repartir
    rateios = [{"obra_id": obra_id, "categoria_id": conta, "valor": str(parte)}
               for (obra_id, conta), parte in
               (repartir(valor, dict(pesos)).items() if valor > 0 else [])
               if parte > 0]

    # AS PARCELAS VÊM DA PREVISÃO — é a "previsão que já existe" sendo
    # confirmada. O que já venceu vai para hoje: boleto não vence no passado.
    abertas = [p for p in base["previsoes"].get(pedido.id, []) if not p.titulo_id]
    parcelas = []
    if valor > 0 and abertas:
        soma = sum((_d(p.valor) for p in abertas), _ZERO)
        partes = repartir(valor, {p.id: _d(p.valor) for p in abertas}) if soma > 0 else {}
        for p in abertas:
            if partes.get(p.id, _ZERO) > 0:
                parcelas.append({"vencimento": max(p.vencimento, hoje).isoformat(),
                                 "valor": str(partes[p.id])})
    elif valor > 0:
        parcelas.append({"vencimento": hoje.isoformat(), "valor": str(valor)})

    nomes = []
    for i in itens:
        if i["valor_recebido"] > 0 and i["insumo"] not in nomes:
            nomes.append(i["insumo"])
    descricao = f"Pedido {pedido.numero} — " + ", ".join(nomes[:3]) + (
        f" e mais {len(nomes) - 3}" if len(nomes) > 3 else "")

    contas_validas = {r["categoria_id"] for r in rateios if r["categoria_id"]}
    return {
        "pedido_id": pedido.id, "numero": pedido.numero,
        "recebimento_id": recebimento.id if recebimento else None,
        "fornecedor_id": pedido.fornecedor_id,
        "fornecedor": base["fornecedores"].get(pedido.fornecedor_id, ""),
        "descricao": descricao[:200],
        "valor": str(valor),
        "documento_numero": (recebimento.nota_numero if recebimento else None)
                            or next((r.nota_numero for r in reversed(
                                base["chegadas"].get(pedido.id, []))
                                if r.nota_numero), None),
        # Uma conta só no pedido inteiro vira a conta do título; mais de uma
        # fica só no rateio, e a pessoa escolhe a principal.
        "categoria_id": next(iter(contas_validas)) if len(contas_validas) == 1 else None,
        "rateios": rateios, "parcelas": parcelas,
        "total_do_pedido": str(total), "ja_lancado": str(fin["em_titulo"]),
        "entregue": entregue, "avisos": avisos,
    }


def vincular(s: Session, titulo, pedido_id: int, usuario: Usuario, *,
             recebimento_id: Optional[int] = None) -> PedidoTitulo:
    """Grava que o título recém-criado é deste pedido e baixa a previsão."""
    pedido = _pedido_autorizado(s, usuario, int(pedido_id))
    if titulo.fornecedor_id != pedido.fornecedor_id:
        raise ErroValidacao(
            f"O credor do lançamento não é o fornecedor do pedido {pedido.numero}. "
            "Confira o credor ou tire o vínculo com o pedido.")
    if recebimento_id:
        rec = s.get(Recebimento, int(recebimento_id))
        if rec is None or rec.pedido_id != pedido.id:
            recebimento_id = None

    vinculo = PedidoTitulo(titulo_id=titulo.id, pedido_id=pedido.id,
                           recebimento_id=int(recebimento_id) if recebimento_id else None,
                           criado_por=usuario.id)
    s.add(vinculo)

    # Baixa as previsões mais antigas até cobrir o valor lançado.
    falta = _d(titulo.valor_liquido)
    baixadas = []
    for prev in s.scalars(select(PrevisaoPagamento).where(
            PrevisaoPagamento.pedido_id == pedido.id,
            PrevisaoPagamento.titulo_id.is_(None))
            .order_by(PrevisaoPagamento.vencimento, PrevisaoPagamento.numero)).all():
        if falta <= acomp.tolerancia(_d(prev.valor)) and falta < _d(prev.valor) / 2:
            break
        prev.titulo_id = titulo.id
        baixadas.append(prev.numero)
        falta -= _d(prev.valor)
    s.flush()
    registrar_evento(s, "pedido_compra", pedido.id, "TITULO_LANCADO",
                     {"titulo_id": titulo.id, "numero_sp": titulo.numero_sp,
                      "valor": str(titulo.valor_liquido),
                      "recebimento_id": vinculo.recebimento_id,
                      "previsoes_baixadas": baixadas}, usuario.id)
    return vinculo


def itens_do_recebimento(s: Session, recebimento_id: int) -> list[dict[str, Any]]:
    """O que veio numa remessa — para a tela mostrar ao lado do lançamento."""
    return [{"pedido_item_id": r.pedido_item_id, "quantidade": str(r.quantidade)}
            for r in s.scalars(select(RecebimentoItem).where(
                RecebimentoItem.recebimento_id == recebimento_id)).all()]
