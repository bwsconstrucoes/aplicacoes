# ============================================================================
# ERP — core/suprimentos/projecao.py
# O que as compras ainda vão custar, e quando — antes de virar conta a pagar.
#
# O PEDIDO, nas palavras do dono (20/11/2026):
#
#   "O financeiro não pode ficar no escuro em relação ao planejamento do
#    compras. Precisamos ainda poder fazer projeções futuras associando mapas
#    de cotação, previsão de entrega e forma de pagamento das compras. Com
#    isso podemos projetar despesas."
#
# O QUE ENTRA, em três graus de certeza — e a tela nunca os mistura num número
# só sem dizer, porque somar "comprado" com "talvez" é como a projeção perde
# o crédito:
#
#   FIRME        pedido AUTORIZADO: a compra existe. As parcelas vêm da
#                previsão de pagamento gerada na autorização.
#   A AUTORIZAR  pedido fechado esperando autorização: provável. As parcelas
#                são simuladas pela mesma regra.
#   EM COTAÇÃO   mapa aberto: estimativa pelo MENOR preço de cada item, com a
#                condição de pagamento de quem deu esse preço.
#
# O QUE NÃO ENTRA: o que já virou título. Conta lançada está em Financeiro ›
# Pagamentos, e somá-la aqui contaria a mesma dívida duas vezes. De cada
# pedido, a projeção desconta o que já tem título — abatendo das parcelas mais
# antigas primeiro — e mostra só o que ainda vai nascer.
#
# A PARCELA "VENCIDA SEM CONTA" é o alerta que ele pediu: a data prevista de
# pagamento já passou e ninguém lançou a nota. É a obra atrasando o
# lançamento, visto pelo lado do financeiro.
# ============================================================================
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
from app.apps.erp.core.suprimentos import acompanhamento as acomp
from app.apps.erp.core.suprimentos.pagamento import gerar_parcelas
from app.apps.erp.db.models.cadastros import (
    CondicaoPagamento, Cotacao, CotacaoFornecedor, CotacaoItem, CotacaoPreco,
    Fornecedor, Obra, PedidoItemReserva, StatusCotacao, StatusItemSuprimento,
    StatusPedidoCompra, SuprimentoItem, SuprimentoSolicitacao, Usuario,
)

logger = logging.getLogger(__name__)

_CENT = Decimal("0.01")
_ZERO = Decimal("0")

GRAUS = {
    "FIRME": "Comprado (pedido autorizado)",
    "A_AUTORIZAR": "Fechado, aguardando autorização",
    "EM_COTACAO": "Em cotação (estimativa pelo menor preço)",
    # ⚑ 21/11/2026 — o dono: "se eu já tenho preços de insumos (...) eu já
    # consigo extrair algum valor, mesmo que a compra não tenha sido fechada".
    # O pedido da obra que ainda nem foi cotado entra pelo ÚLTIMO PREÇO
    # conhecido do insumo. É o grau mais fraco dos quatro, e a tela o separa.
    "SOLICITADO": "Solicitado, sem cotação (estimativa pelo último preço)",
}


def _d(v: Any) -> Decimal:
    return Decimal(str(v if v is not None else 0))


def repartir(valor: Decimal, pesos: dict[int, Decimal]) -> dict[int, Decimal]:
    """Divide um valor entre obras na proporção dos pesos, fechando o centavo.

    A sobra do arredondamento vai para a obra de maior peso: a soma das partes
    tem de dar exatamente o valor, senão a projeção por obra não fecha com a
    geral.
    """
    total = sum(pesos.values(), _ZERO)
    if total <= 0 or not pesos:
        return {}
    partes = {k: (valor * p / total).quantize(_CENT) for k, p in pesos.items()}
    sobra = valor - sum(partes.values(), _ZERO)
    if sobra:
        maior = max(pesos, key=lambda k: pesos[k])
        partes[maior] += sobra
    return partes


def _linhas_dos_pedidos(s: Session, usuario: Optional[Usuario],
                        hoje: date) -> list[dict[str, Any]]:
    pedidos = acomp.pedidos_no_escopo(
        s, usuario, (StatusPedidoCompra.AUTORIZADO,
                     StatusPedidoCompra.AGUARDANDO_AUTORIZACAO))
    base = acomp.carregar(s, pedidos)
    condicoes = {c.id: c for c in s.scalars(select(CondicaoPagamento)).all()}
    obras = {}
    saida = []
    for pedido in pedidos:
        itens = base["itens"].get(pedido.id, [])
        if not itens:
            continue
        total = acomp.total_do_pedido(pedido, itens)
        if total <= 0:
            continue
        pesos: dict[int, Decimal] = defaultdict(lambda: _ZERO)
        for i in itens:
            pesos[i["obra_id"]] += i["valor"]
            obras[i["obra_id"]] = i["obra"]
        entregue = all(i["saldo"] <= 0 for i in itens)
        chegou_algo = any(i["recebido"] > 0 for i in itens)

        if pedido.status is StatusPedidoCompra.AUTORIZADO:
            grau = "FIRME"
            parcelas = [(p.numero, p.vencimento, _d(p.valor))
                        for p in base["previsoes"].get(pedido.id, [])]
            # O QUE JÁ VIROU TÍTULO sai, das parcelas mais antigas primeiro.
            abater = acomp.financeiro_do_pedido(pedido, base)["em_titulo"]
        else:
            grau = "A_AUTORIZAR"
            cond = condicoes.get(pedido.condicao_pagamento_id)
            base_data = pedido.previsao_entrega or hoje
            geradas = (gerar_parcelas(total, base_data, 100, []) if cond is None
                       else gerar_parcelas(total, base_data,
                                           cond.entrada_percentual or 0,
                                           list(cond.dias or [])))
            parcelas = [(p.numero, p.vencimento, p.valor) for p in geradas]
            abater = _ZERO

        quantas = len(parcelas)
        for numero, vencimento, valor in sorted(parcelas, key=lambda x: (x[1], x[0])):
            if abater > 0:
                corte = min(abater, valor)
                abater -= corte
                valor -= corte
            if valor <= 0:
                continue
            for obra_id, parte in repartir(valor, dict(pesos)).items():
                if parte <= 0:
                    continue
                saida.append({
                    "grau": grau, "origem": "pedido", "origem_id": pedido.id,
                    "referencia": pedido.numero,
                    "fornecedor": base["fornecedores"].get(pedido.fornecedor_id, ""),
                    "obra_id": obra_id, "obra": obras.get(obra_id, ""),
                    "parcela": f"{numero}/{quantas}",
                    "vencimento": vencimento.isoformat(),
                    "valor": float(parte),
                    "vencida": grau == "FIRME" and vencimento < hoje,
                    "material": ("chegou" if entregue else
                                 "chegou em parte" if chegou_algo else "não chegou"),
                    "previsao_entrega": (pedido.previsao_entrega.isoformat()
                                         if pedido.previsao_entrega else None),
                })
    return saida


def _linhas_das_cotacoes(s: Session, usuario: Optional[Usuario],
                         hoje: date) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """A estimativa dos mapas abertos, pelo menor preço de cada item.

    Item que já entrou num pedido sai daqui — ele já está contado como FIRME
    ou A AUTORIZAR, e somá-lo de novo dobraria a projeção.
    """
    permitidas = obras_de_registro_sem_autor(s, usuario) if usuario is not None else None
    cotacoes = {c.id: c for c in s.scalars(
        select(Cotacao).where(Cotacao.status == StatusCotacao.ABERTA)).all()}
    fora = {"itens_sem_preco": 0, "solicitados_sem_cotacao": 0}
    if cotacoes:
        linhas = s.execute(
            select(CotacaoItem, SuprimentoItem, SuprimentoSolicitacao, Obra)
            .join(SuprimentoItem, SuprimentoItem.id == CotacaoItem.suprimento_item_id)
            .join(SuprimentoSolicitacao,
                  SuprimentoSolicitacao.id == SuprimentoItem.solicitacao_id)
            .join(Obra, Obra.id == SuprimentoItem.obra_id)
            .where(CotacaoItem.cotacao_id.in_(list(cotacoes)))).all()
    else:
        linhas = []
    ids_sup = [sup.id for _, sup, _, _ in linhas] or [0]
    reservados = set(s.scalars(select(PedidoItemReserva.suprimento_item_id).where(
        PedidoItemReserva.suprimento_item_id.in_(ids_sup))).all())
    colunas = {c.id: c for c in s.scalars(select(CotacaoFornecedor).where(
        CotacaoFornecedor.cotacao_id.in_(list(cotacoes) or [0]))).all()}
    precos: dict[int, list[CotacaoPreco]] = defaultdict(list)
    for preco in s.scalars(select(CotacaoPreco).where(
            CotacaoPreco.cotacao_fornecedor_id.in_(list(colunas) or [0]))).all():
        precos[preco.cotacao_item_id].append(preco)
    fornecedores = {f.id: f.razao_social for f in s.scalars(select(Fornecedor).where(
        Fornecedor.id.in_({c.fornecedor_id for c in colunas.values()} or {0}))).all()}
    condicoes = {c.id: c for c in s.scalars(select(CondicaoPagamento)).all()}

    # (cotação, coluna vencedora, obra, data base) → valor
    blocos: dict[tuple, Decimal] = defaultdict(lambda: _ZERO)
    nomes_obra = {}
    mortos = (StatusItemSuprimento.CANCELADO, StatusItemSuprimento.SUSPENSO)
    for linha, sup, sol, obra in linhas:
        if sup.id in reservados or sup.status in mortos:
            continue
        if permitidas is not None and sup.obra_id not in permitidas:
            continue
        candidatos = [p for p in precos.get(linha.id, [])
                      if not getattr(colunas.get(p.cotacao_fornecedor_id),
                                     "sem_interesse", False)]
        if not candidatos:
            fora["itens_sem_preco"] += 1
            continue
        melhor = min(candidatos, key=lambda p: _d(p.preco_unitario))
        quando = sol.previsao_entrega if (sol.previsao_entrega and
                                          sol.previsao_entrega > hoje) else hoje
        nomes_obra[obra.id] = obra.codigo
        blocos[(linha.cotacao_id, melhor.cotacao_fornecedor_id, obra.id, quando)] += (
            _d(sup.quantidade) * _d(melhor.preco_unitario)).quantize(_CENT)

    saida = []
    for (cotacao_id, coluna_id, obra_id, quando), valor in blocos.items():
        if valor <= 0:
            continue
        coluna = colunas[coluna_id]
        cond = condicoes.get(coluna.condicao_pagamento_id)
        geradas = (gerar_parcelas(valor, quando, 100, []) if cond is None
                   else gerar_parcelas(valor, quando, cond.entrada_percentual or 0,
                                       list(cond.dias or [])))
        for p in geradas:
            saida.append({
                "grau": "EM_COTACAO", "origem": "cotacao", "origem_id": cotacao_id,
                "referencia": cotacoes[cotacao_id].numero,
                "fornecedor": fornecedores.get(coluna.fornecedor_id, ""),
                "obra_id": obra_id, "obra": nomes_obra.get(obra_id, ""),
                "parcela": f"{p.numero}/{len(geradas)}",
                "vencimento": p.vencimento.isoformat(), "valor": float(p.valor),
                "vencida": False, "material": "não comprado",
                "previsao_entrega": quando.isoformat(),
            })

    # O que nem cotação tem ainda: fora da conta, mas DITO — projeção que cala
    # sobre o que não sabe parece completa sem ser.
    stmt = (select(SuprimentoItem, SuprimentoSolicitacao, Obra)
            .join(SuprimentoSolicitacao,
                  SuprimentoSolicitacao.id == SuprimentoItem.solicitacao_id)
            .join(Obra, Obra.id == SuprimentoItem.obra_id)
            .where(SuprimentoItem.status.in_(
                (StatusItemSuprimento.SOLICITACAO, StatusItemSuprimento.SALA_TECNICA))))
    if permitidas is not None:
        stmt = stmt.where(SuprimentoItem.obra_id.in_(permitidas or [0]))
    pedidos_da_obra = s.execute(stmt).all()
    # O último preço SÓ dos insumos que estão na fila — não a tabela inteira.
    from app.apps.erp.db.models.cadastros import PrecoHistorico
    ultimo: dict[int, Any] = {}
    insumos = {sup.insumo_id for sup, _, _ in pedidos_da_obra if sup.insumo_id}
    if insumos:
        for r in s.scalars(select(PrecoHistorico).where(
                PrecoHistorico.insumo_id.in_(insumos))).all():
            atual = ultimo.get(r.insumo_id)
            if atual is None or (r.data, r.id or 0) >= (atual.data, atual.id or 0):
                ultimo[r.insumo_id] = r
    estimado: dict[tuple, Decimal] = defaultdict(lambda: _ZERO)
    for sup, sol, obra in pedidos_da_obra:
        preco = ultimo.get(sup.insumo_id)
        if preco is None or _d(preco.preco_unitario) <= 0:
            fora["solicitados_sem_cotacao"] += 1
            continue
        quando = sol.previsao_entrega if (sol.previsao_entrega and
                                          sol.previsao_entrega > hoje) else hoje
        estimado[(sol.id, sol.numero, obra.id, obra.codigo, quando)] += (
            _d(sup.quantidade) * _d(preco.preco_unitario)).quantize(_CENT)
    for (sol_id, numero, obra_id, codigo, quando), valor in estimado.items():
        # Sem fornecedor não há condição de pagamento: a estimativa cai inteira
        # na data em que a obra precisa do material.
        saida.append({
            "grau": "SOLICITADO", "origem": "solicitacao", "origem_id": sol_id,
            "referencia": numero, "fornecedor": "(ainda sem fornecedor)",
            "obra_id": obra_id, "obra": codigo, "parcela": "1/1",
            "vencimento": quando.isoformat(), "valor": float(valor),
            "vencida": False, "material": "não cotado",
            "previsao_entrega": quando.isoformat(),
        })
    return saida, fora


def projetar(s: Session, usuario: Optional[Usuario] = None, *,
             hoje: Optional[date] = None) -> dict[str, Any]:
    """A projeção inteira: as linhas, o resumo por mês e o que ficou de fora."""
    hoje = hoje or date.today()
    linhas = _linhas_dos_pedidos(s, usuario, hoje)
    das_cotacoes, fora = _linhas_das_cotacoes(s, usuario, hoje)
    linhas += das_cotacoes
    linhas.sort(key=lambda l: (l["vencimento"], l["referencia"], l["obra"]))

    meses: dict[str, dict[str, Any]] = {}
    totais = {g: 0.0 for g in GRAUS}
    vencido = {"valor": 0.0, "parcelas": 0, "pedidos": set()}
    for l in linhas:
        totais[l["grau"]] = round(totais[l["grau"]] + l["valor"], 2)
        if l["vencida"]:
            vencido["valor"] = round(vencido["valor"] + l["valor"], 2)
            vencido["parcelas"] += 1
            vencido["pedidos"].add(l["origem_id"])
            chave = "VENCIDO"
        else:
            chave = l["vencimento"][:7]
        mes = meses.setdefault(chave, {"mes": chave, **{g: 0.0 for g in GRAUS},
                                       "total": 0.0})
        mes[l["grau"]] = round(mes[l["grau"]] + l["valor"], 2)
        mes["total"] = round(mes["total"] + l["valor"], 2)

    ordenados = sorted(meses.values(),
                       key=lambda m: ("0" if m["mes"] == "VENCIDO" else "1") + m["mes"])
    return {
        "hoje": hoje.isoformat(), "graus": GRAUS,
        "linhas": linhas, "meses": ordenados, "totais": totais,
        "vencido_sem_conta": {"valor": vencido["valor"],
                              "parcelas": vencido["parcelas"],
                              "pedidos": len(vencido["pedidos"])},
        "fora_da_conta": fora,
    }
