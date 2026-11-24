# ============================================================================
# ERP — core/suprimentos/relatorios.py
# Os relatórios de gestão das compras.
#
# O dono, em 20 e 21/11/2026: "eu quero relatórios em tela e exportáveis (…)
# que permitam à gestão a análise e a compra" — e, das sugestões: "os
# relatórios todos, sugeridos todos."
#
# Os quatro do MAPA (por item, por fornecedor, tudo de um, comparativo) moram
# em `cotacao.relatorio` e respondem "de quem compro ESTA cotação?". Estes aqui
# olham para TRÁS, para muitas compras de uma vez, e respondem à gestão:
#
#   ECONOMIA     a cotação valeu a pena? quanto se pagou contra a média e
#                contra o maior preço do mapa
#   HISTORICO    este preço subiu? cada compra contra a compra anterior do
#                mesmo material
#   FORNECEDOR   quem entrega no prazo, quem entrega incompleto, quem responde
#   CICLO        quantos dias entre pedir, comprar, autorizar e receber
#
# TODOS DEVOLVEM O MESMO FORMATO — título, colunas, linhas, resumo e o aviso do
# que NÃO está na conta —, e uma tela só desenha qualquer um. Relatório novo é
# uma função aqui e uma linha em `RELATORIOS`.
#
# ⚑ O QUE FICOU DE FORA: "gasto da obra contra o previsto". O ERP não tem
# orçamento de material por obra para comparar; inventar a régua faria o
# relatório parecer que responde. Entra quando houver de onde vir o previsto.
# ============================================================================
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.suprimentos import acompanhamento as acomp
from app.apps.erp.db.models.cadastros import (
    Cotacao, CotacaoFornecedor, CotacaoItem, CotacaoPreco, Fornecedor, Insumo,
    InsumoCategoria, PedidoCompra, PedidoItem, PrecoHistorico, StatusPedidoCompra,
    SuprimentoItem, SuprimentoSolicitacao, TipoPreco, Usuario,
)

logger = logging.getLogger(__name__)

_CENT = Decimal("0.01")
_ZERO = Decimal("0")

RELATORIOS = {
    "ECONOMIA": "Economia da cotação",
    "HISTORICO": "Preço contra a compra anterior",
    "FORNECEDOR": "Desempenho do fornecedor",
    "CICLO": "Tempo do ciclo de compra",
}


def _d(v: Any) -> Decimal:
    return Decimal(str(v if v is not None else 0))


def _f(v: Any) -> float:
    return float(_d(v).quantize(_CENT))


def _pct(parte: Decimal, todo: Decimal) -> Optional[float]:
    return round(float(parte / todo * 100), 1) if todo else None


def _pedidos(s: Session, usuario: Optional[Usuario], de: Optional[date],
             ate: Optional[date], obra_id: Optional[int],
             fornecedor_id: Optional[int]) -> tuple[list[PedidoCompra], dict[str, Any]]:
    """Os pedidos AUTORIZADOS no recorte da pessoa e dos filtros, com tudo
    carregado de uma vez (as mesmas consultas do painel)."""
    pedidos = acomp.pedidos_no_escopo(s, usuario, (StatusPedidoCompra.AUTORIZADO,))
    if fornecedor_id:
        pedidos = [p for p in pedidos if p.fornecedor_id == int(fornecedor_id)]
    if de:
        pedidos = [p for p in pedidos if p.criado_em and p.criado_em.date() >= de]
    if ate:
        pedidos = [p for p in pedidos if p.criado_em and p.criado_em.date() <= ate]
    base = acomp.carregar(s, pedidos)
    if obra_id:
        pedidos = [p for p in pedidos
                   if any(i["obra_id"] == int(obra_id) for i in base["itens"].get(p.id, []))]
    return pedidos, base


def gerar(s: Session, tipo: str, usuario: Optional[Usuario] = None, *,
          de: Optional[date] = None, ate: Optional[date] = None,
          obra_id: Optional[int] = None,
          fornecedor_id: Optional[int] = None) -> dict[str, Any]:
    tipo = (tipo or "ECONOMIA").upper()
    if tipo not in RELATORIOS:
        tipo = "ECONOMIA"
    pedidos, base = _pedidos(s, usuario, de, ate, obra_id, fornecedor_id)
    dados = {"ECONOMIA": _economia, "HISTORICO": _historico,
             "FORNECEDOR": _fornecedor, "CICLO": _ciclo}[tipo](s, pedidos, base)
    return {"tipo": tipo, "titulo": RELATORIOS[tipo],
            "relatorios": [{"chave": k, "rotulo": v} for k, v in RELATORIOS.items()],
            "pedidos_considerados": len(pedidos), **dados}


# ---------------------------------------------------------------------------
# 1. A cotação valeu a pena?
# ---------------------------------------------------------------------------
def _economia(s: Session, pedidos: list[PedidoCompra], base: dict[str, Any]) -> dict[str, Any]:
    ids_cot = {p.cotacao_id for p in pedidos if p.cotacao_id} or {0}
    linhas_cot = {(ci.cotacao_id, ci.suprimento_item_id): ci.id for ci in s.scalars(
        select(CotacaoItem).where(CotacaoItem.cotacao_id.in_(ids_cot))).all()}
    precos: dict[int, list[Decimal]] = defaultdict(list)
    colunas = {c.id for c in s.scalars(select(CotacaoFornecedor).where(
        CotacaoFornecedor.cotacao_id.in_(ids_cot))).all()}
    for preco in s.scalars(select(CotacaoPreco).where(
            CotacaoPreco.cotacao_fornecedor_id.in_(colunas or {0}))).all():
        precos[preco.cotacao_item_id].append(_d(preco.preco_unitario))
    item_de = dict(s.execute(select(PedidoItem.id, PedidoItem.suprimento_item_id).where(
        PedidoItem.pedido_id.in_([p.id for p in pedidos] or [0]))).all())
    compradores = {u.id: u.nome for u in s.scalars(select(Usuario).where(
        Usuario.id.in_({p.criado_por for p in pedidos} or {0}))).all()}

    saida = []
    pago_total = media_total = maior_total = _ZERO
    diretos = 0
    for p in pedidos:
        itens = base["itens"].get(p.id, [])
        pago = sum((i["valor"] for i in itens), _ZERO)
        obras = ", ".join(sorted({i["obra"] for i in itens}))
        if not p.cotacao_id:
            diretos += 1
            saida.append([p.criado_em.date().isoformat() if p.criado_em else "", p.numero,
                          "sem cotação", base["fornecedores"].get(p.fornecedor_id, ""), obras,
                          compradores.get(p.criado_por, ""), 0, _f(pago), None, None, None, None])
            continue
        pela_media = pelo_maior = _ZERO
        concorrentes = 0
        for i in itens:
            linha_id = linhas_cot.get((p.cotacao_id, item_de.get(i["pedido_item_id"])))
            cotados = precos.get(linha_id, []) if linha_id else []
            concorrentes = max(concorrentes, len(cotados))
            if cotados:
                pela_media += (sum(cotados, _ZERO) / len(cotados)) * i["quantidade"]
                pelo_maior += max(cotados) * i["quantidade"]
            else:
                pela_media += i["valor"]; pelo_maior += i["valor"]
        pago_total += pago; media_total += pela_media; maior_total += pelo_maior
        saida.append([p.criado_em.date().isoformat() if p.criado_em else "", p.numero,
                      base["cotacoes"].get(p.cotacao_id, ""),
                      base["fornecedores"].get(p.fornecedor_id, ""), obras,
                      compradores.get(p.criado_por, ""), concorrentes, _f(pago),
                      _f(pela_media), _f(pela_media - pago), _pct(pela_media - pago, pela_media),
                      _f(pelo_maior - pago)])
    return {
        "colunas": [{"rotulo": "Data", "tipo": "data"}, {"rotulo": "Pedido"},
                    {"rotulo": "Cotação"}, {"rotulo": "Fornecedor"}, {"rotulo": "Obra"},
                    {"rotulo": "Comprador"},
                    {"rotulo": "Preços no mapa", "tipo": "inteiro"},
                    {"rotulo": "Pago", "tipo": "dinheiro"},
                    {"rotulo": "Pela média do mapa", "tipo": "dinheiro"},
                    {"rotulo": "Economia vs média", "tipo": "dinheiro"},
                    {"rotulo": "%", "tipo": "percentual"},
                    {"rotulo": "Economia vs maior preço", "tipo": "dinheiro"}],
        "linhas": saida,
        "resumo": [
            {"rotulo": "Comprado com cotação", "valor": _f(pago_total), "tipo": "dinheiro"},
            {"rotulo": "Economia contra a média", "valor": _f(media_total - pago_total),
             "tipo": "dinheiro", "detalhe": (f"{_pct(media_total - pago_total, media_total)}%"
                                             if media_total else "")},
            {"rotulo": "Economia contra o maior preço", "valor": _f(maior_total - pago_total),
             "tipo": "dinheiro"},
            {"rotulo": "Pedidos sem cotação", "valor": diretos, "tipo": "inteiro",
             "detalhe": "compra direta — sem régua para medir"}],
        "aviso": ("Economia = o que se pagaria pela MÉDIA dos preços do mapa, menos o que "
                  "se pagou. Só os itens; frete e desconto do pedido não entram. Item com "
                  "um preço só no mapa não gera economia — não houve concorrência."),
    }


# ---------------------------------------------------------------------------
# 2. Este preço subiu?
# ---------------------------------------------------------------------------
def _historico(s: Session, pedidos: list[PedidoCompra], base: dict[str, Any]) -> dict[str, Any]:
    insumo_de = dict(s.execute(
        select(PedidoItem.id, SuprimentoItem.insumo_id)
        .join(SuprimentoItem, SuprimentoItem.id == PedidoItem.suprimento_item_id)
        .where(PedidoItem.pedido_id.in_([p.id for p in pedidos] or [0]))).all())
    ids_insumo = set(insumo_de.values()) or {0}
    anteriores: dict[int, list[PrecoHistorico]] = defaultdict(list)
    for h in s.scalars(select(PrecoHistorico).where(
            PrecoHistorico.insumo_id.in_(ids_insumo),
            PrecoHistorico.tipo == TipoPreco.COMPRADO)
            .order_by(PrecoHistorico.data.desc(), PrecoHistorico.id.desc())).all():
        anteriores[h.insumo_id].append(h)
    nomes = {f.id: f.razao_social for f in s.scalars(select(Fornecedor).where(
        Fornecedor.id.in_({h.fornecedor_id for l in anteriores.values() for h in l
                           if h.fornecedor_id} or {0}))).all()}
    saida, subiram, sem_base = [], 0, 0
    for p in pedidos:
        quando = (p.autorizado_em or p.criado_em).date() if (p.autorizado_em or p.criado_em) else None
        for i in base["itens"].get(p.id, []):
            insumo_id = insumo_de.get(i["pedido_item_id"])
            # A compra ANTERIOR: mais antiga que esta, ou do mesmo dia com outro
            # fornecedor. O próprio pedido também está no histórico — sem este
            # corte, o preço seria comparado com ele mesmo.
            antes = next((h for h in anteriores.get(insumo_id, [])
                          if quando is None or h.data < quando
                          or (h.data == quando and h.fornecedor_id != p.fornecedor_id)), None)
            variacao = None
            if antes is None:
                sem_base += 1
            else:
                variacao = _pct(i["preco_unitario"] - _d(antes.preco_unitario),
                                _d(antes.preco_unitario))
                if variacao is not None and variacao > 0:
                    subiram += 1
            saida.append([quando.isoformat() if quando else "", p.numero, i["insumo"],
                          i["especificacao"], i["obra"],
                          base["fornecedores"].get(p.fornecedor_id, ""),
                          float(i["quantidade"]), i["unidade"], _f(i["preco_unitario"]),
                          _f(antes.preco_unitario) if antes else None,
                          antes.data.isoformat() if antes else "",
                          nomes.get(antes.fornecedor_id, "") if antes else "", variacao])
    saida.sort(key=lambda l: -(l[12] if l[12] is not None else -9999))
    return {
        "colunas": [{"rotulo": "Data", "tipo": "data"}, {"rotulo": "Pedido"},
                    {"rotulo": "Material"}, {"rotulo": "Especificação"}, {"rotulo": "Obra"},
                    {"rotulo": "Fornecedor"}, {"rotulo": "Qtd", "tipo": "quantidade"},
                    {"rotulo": "Un."}, {"rotulo": "Preço pago", "tipo": "dinheiro"},
                    {"rotulo": "Compra anterior", "tipo": "dinheiro"},
                    {"rotulo": "Em", "tipo": "data"}, {"rotulo": "De quem"},
                    {"rotulo": "Variação %", "tipo": "percentual"}],
        "linhas": saida,
        "resumo": [{"rotulo": "Itens comprados", "valor": len(saida), "tipo": "inteiro"},
                   {"rotulo": "Subiram de preço", "valor": subiram, "tipo": "inteiro"},
                   {"rotulo": "Sem compra anterior", "valor": sem_base, "tipo": "inteiro",
                    "detalhe": "primeira compra do material no sistema"}],
        "aviso": ("Compara com a compra anterior do MESMO insumo, sem olhar a "
                  "especificação nem corrigir a inflação: bitola, marca ou unidade "
                  "diferentes dão variação que não é aumento. Ordenado pela maior alta."),
    }


# ---------------------------------------------------------------------------
# 3. Quem entrega no prazo, quem entrega incompleto, quem responde
# ---------------------------------------------------------------------------
def _fornecedor(s: Session, pedidos: list[PedidoCompra], base: dict[str, Any]) -> dict[str, Any]:
    f: dict[int, dict[str, Any]] = defaultdict(lambda: {
        "pedidos": 0, "valor": _ZERO, "entregues": 0, "no_prazo": 0, "atraso": 0,
        "dias": 0, "partes": 0, "abertos_atrasados": 0, "cotado": 0, "respondeu": 0})
    hoje = date.today()
    for p in pedidos:
        itens = base["itens"].get(p.id, [])
        chegadas = base["chegadas"].get(p.id, [])
        d = f[p.fornecedor_id]
        d["pedidos"] += 1
        d["valor"] += acomp.total_do_pedido(p, itens)
        completo = bool(itens) and all(i["saldo"] <= 0 for i in itens)
        if len(chegadas) > 1 or (chegadas and not completo):
            d["partes"] += 1
        if completo and chegadas:
            d["entregues"] += 1
            if p.previsao_entrega:
                dias = (chegadas[-1].data - p.previsao_entrega).days
                if dias <= 0:
                    d["no_prazo"] += 1
                else:
                    d["atraso"] += 1; d["dias"] += dias
        elif p.previsao_entrega and p.previsao_entrega < hoje:
            d["abertos_atrasados"] += 1

    # A resposta às cotações é do fornecedor inteiro, não só de quem ganhou.
    com_preco = set(s.scalars(select(CotacaoPreco.cotacao_fornecedor_id).distinct()).all())
    for coluna in s.scalars(select(CotacaoFornecedor)).all():
        d = f[coluna.fornecedor_id]
        d["cotado"] += 1
        if coluna.id in com_preco or coluna.respondido_em:
            d["respondeu"] += 1
    nomes = {x.id: x.razao_social for x in s.scalars(select(Fornecedor).where(
        Fornecedor.id.in_(set(f) or {0}))).all()}
    saida = []
    for fid, d in f.items():
        medidos = d["no_prazo"] + d["atraso"]
        saida.append([nomes.get(fid, ""), d["pedidos"], _f(d["valor"]), d["entregues"],
                      _pct(Decimal(d["no_prazo"]), Decimal(medidos)) if medidos else None,
                      round(d["dias"] / d["atraso"], 1) if d["atraso"] else None,
                      d["partes"], d["abertos_atrasados"], d["cotado"],
                      _pct(Decimal(d["respondeu"]), Decimal(d["cotado"])) if d["cotado"] else None])
    saida.sort(key=lambda l: -l[2])
    return {
        "colunas": [{"rotulo": "Fornecedor"}, {"rotulo": "Pedidos", "tipo": "inteiro"},
                    {"rotulo": "Comprado", "tipo": "dinheiro"},
                    {"rotulo": "Entregues", "tipo": "inteiro"},
                    {"rotulo": "No prazo %", "tipo": "percentual"},
                    {"rotulo": "Atraso médio (dias)", "tipo": "numero"},
                    {"rotulo": "Entregou em partes", "tipo": "inteiro"},
                    {"rotulo": "Em aberto e atrasados", "tipo": "inteiro"},
                    {"rotulo": "Cotações recebidas", "tipo": "inteiro"},
                    {"rotulo": "Respondeu %", "tipo": "percentual"}],
        "linhas": saida,
        "resumo": [{"rotulo": "Fornecedores", "valor": len(saida), "tipo": "inteiro"},
                   {"rotulo": "Pedidos com entrega atrasada em aberto",
                    "valor": sum(l[7] for l in saida), "tipo": "inteiro"}],
        "aviso": ("\"No prazo\" compara a data combinada no pedido com a data do ÚLTIMO "
                  "recebimento registrado pela obra — se a obra demora a registrar, o "
                  "fornecedor leva a culpa. \"Em partes\" = mais de uma entrega, ou entrega "
                  "que ainda não completou. As cotações contam todas, de qualquer período."),
    }


# ---------------------------------------------------------------------------
# 4. Quantos dias leva
# ---------------------------------------------------------------------------
def _ciclo(s: Session, pedidos: list[PedidoCompra], base: dict[str, Any]) -> dict[str, Any]:
    pedido_de = {p.id: p for p in pedidos}
    linhas_item = s.execute(
        select(PedidoItem.pedido_id, SuprimentoItem, SuprimentoSolicitacao, Insumo)
        .join(SuprimentoItem, SuprimentoItem.id == PedidoItem.suprimento_item_id)
        .join(SuprimentoSolicitacao, SuprimentoSolicitacao.id == SuprimentoItem.solicitacao_id)
        .join(Insumo, Insumo.id == SuprimentoItem.insumo_id)
        .where(PedidoItem.pedido_id.in_(list(pedido_de) or [0]))).all()
    categorias = {c.id: c.nome for c in s.scalars(select(InsumoCategoria)).all()}
    obras = {i["obra_id"]: i["obra"] for l in base["itens"].values() for i in l}

    def novo():
        return {"itens": 0, "comprar": [], "autorizar": [], "entregar": [], "total": []}
    por_categoria: dict[str, dict[str, Any]] = defaultdict(novo)
    por_obra: dict[str, dict[str, Any]] = defaultdict(novo)
    geral = novo()
    for pedido_id, sup, sol, insumo in linhas_item:
        p = pedido_de[pedido_id]
        chegadas = base["chegadas"].get(p.id, [])
        pediu = sol.criado_em.date() if sol.criado_em else None
        fechou = p.criado_em.date() if p.criado_em else None
        autorizou = p.autorizado_em.date() if p.autorizado_em else None
        chegou = (chegadas[-1].data if chegadas
                  and _d(sup.quantidade_recebida) >= _d(sup.quantidade) else None)
        for alvo in (por_categoria[categorias.get(insumo.categoria_insumo_id, "Sem categoria")],
                     por_obra[obras.get(sup.obra_id, "?")], geral):
            alvo["itens"] += 1
            if pediu and fechou:
                alvo["comprar"].append(max((fechou - pediu).days, 0))
            if fechou and autorizou:
                alvo["autorizar"].append(max((autorizou - fechou).days, 0))
            if autorizou and chegou:
                alvo["entregar"].append(max((chegou - autorizou).days, 0))
            if pediu and chegou:
                alvo["total"].append(max((chegou - pediu).days, 0))

    def media(v):
        return round(sum(v) / len(v), 1) if v else None

    def linha(visao, nome, d):
        return [visao, nome, d["itens"], media(d["comprar"]), media(d["autorizar"]),
                media(d["entregar"]), media(d["total"]),
                max(d["total"]) if d["total"] else None, len(d["total"])]
    saida = [linha("Tipo de insumo", k, v) for k, v in sorted(por_categoria.items())]
    saida += [linha("Obra", k, v) for k, v in sorted(por_obra.items())]
    return {
        "colunas": [{"rotulo": "Visão"}, {"rotulo": "Grupo"},
                    {"rotulo": "Itens", "tipo": "inteiro"},
                    {"rotulo": "Pedir → comprar (dias)", "tipo": "numero"},
                    {"rotulo": "Comprar → autorizar", "tipo": "numero"},
                    {"rotulo": "Autorizar → receber", "tipo": "numero"},
                    {"rotulo": "Total (dias)", "tipo": "numero"},
                    {"rotulo": "Pior caso", "tipo": "numero"},
                    {"rotulo": "Itens já recebidos", "tipo": "inteiro"}],
        "linhas": saida,
        "resumo": [{"rotulo": "Do pedido da obra ao recebimento", "tipo": "numero",
                    "valor": media(geral["total"]), "detalhe": "dias, em média"},
                   {"rotulo": "Pedir → comprar", "valor": media(geral["comprar"]), "tipo": "numero"},
                   {"rotulo": "Comprar → autorizar", "valor": media(geral["autorizar"]), "tipo": "numero"},
                   {"rotulo": "Autorizar → receber", "valor": media(geral["entregar"]), "tipo": "numero"}],
        "aviso": ("Dias corridos, só dos itens que viraram pedido AUTORIZADO. O total e o "
                  "\"autorizar → receber\" contam só o que já chegou por inteiro. É daqui que "
                  "vai sair o prazo típico de cada tipo de insumo, para o painel avisar "
                  "\"não vai dar tempo\" antes de haver pedido."),
    }
