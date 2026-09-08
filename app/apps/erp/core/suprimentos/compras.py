# ============================================================================
# ERP — core/suprimentos/compras.py
# O que cada obra comprou, aberto até o insumo.
#
# O PEDIDO, nas palavras do dono: "de pedido a gente vai conseguir ver tudo de
# uma obra… eu quero ver tudo que foi de cimento, tudo que foi de cerâmica".
#
# São DUAS perguntas, e é a mesma consulta lida por dois lados:
#
#   "o que a obra X comprou?"     → agrupa por obra e abre até o insumo
#   "tudo que foi de cimento?"    → filtra o insumo e abre por obra
#
# Por isso não existem dois relatórios: existe UM, com o agrupamento
# escolhido por quem olha. Dois relatórios divergiriam no primeiro mês.
#
# O QUE CONTA COMO COMPRADO. Só pedido AUTORIZADO. Pedido esperando
# autorização ainda pode não acontecer, e recusado/cancelado não aconteceu —
# somá-los faria o relatório dizer que a obra gastou o que não gastou. Quem
# quiser ver o que está em andamento pede `incluir_pendentes`.
#
# DE ONDE VÊM A OBRA E O INSUMO. Não do pedido: da SOLICITAÇÃO que originou
# cada linha dele. A obra é do ITEM da solicitação, não da solicitação — uma
# mesma solicitação pede material para obras diferentes, e é assim que o
# relatório separa certo.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.auth.permissoes import obras_do_usuario
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Insumo, InsumoCategoria, Obra, PedidoCompra, PedidoItem,
    StatusPedidoCompra, SuprimentoItem, SuprimentoSolicitacao, Usuario,
)

logger = logging.getLogger(__name__)

_CENT = Decimal("0.01")

AGRUPAMENTOS = {
    "obra": "Por obra",
    "insumo": "Por insumo",
    "categoria": "Por categoria de insumo",
    "fornecedor": "Por fornecedor",
}


def _dinheiro(v: Any) -> float:
    return float(Decimal(str(v or 0)).quantize(_CENT))


def linhas(s: Session, *, usuario: Optional[Usuario] = None,
           obra_id: Optional[int] = None, insumo_id: Optional[int] = None,
           categoria_id: Optional[int] = None, fornecedor_id: Optional[int] = None,
           de: Optional[date] = None, ate: Optional[date] = None,
           busca: str = "", incluir_pendentes: bool = False) -> list[dict[str, Any]]:
    """Uma linha por item comprado, com obra, insumo, fornecedor e valor.

    É a base de tudo: a árvore por obra, o total por insumo e a exportação
    saem daqui. Uma consulta só — se ela mudar, mudam todas juntas, e nenhuma
    tela pode divergir de outra.
    """
    aceitos = {StatusPedidoCompra.AUTORIZADO}
    if incluir_pendentes:
        aceitos.add(StatusPedidoCompra.AGUARDANDO_AUTORIZACAO)

    stmt = (select(PedidoItem, PedidoCompra, SuprimentoItem, SuprimentoSolicitacao,
                   Insumo, Obra, Fornecedor)
            .join(PedidoCompra, PedidoItem.pedido_id == PedidoCompra.id)
            .join(SuprimentoItem, PedidoItem.suprimento_item_id == SuprimentoItem.id)
            .join(SuprimentoSolicitacao,
                  SuprimentoItem.solicitacao_id == SuprimentoSolicitacao.id)
            .join(Insumo, SuprimentoItem.insumo_id == Insumo.id)
            .join(Obra, SuprimentoItem.obra_id == Obra.id)
            .join(Fornecedor, PedidoCompra.fornecedor_id == Fornecedor.id)
            .where(PedidoCompra.status.in_(aceitos)))

    # O ESCOPO DA PESSOA vale aqui como vale em todo lugar: quem enxerga só as
    # obras dele não pode descobrir o gasto das outras por um relatório.
    if usuario is not None:
        permitidas = obras_do_usuario(s, usuario)
        if permitidas is not None:
            stmt = stmt.where(SuprimentoItem.obra_id.in_(permitidas or [0]))

    if obra_id:
        stmt = stmt.where(SuprimentoItem.obra_id == obra_id)
    if insumo_id:
        stmt = stmt.where(SuprimentoItem.insumo_id == insumo_id)
    if categoria_id:
        stmt = stmt.where(Insumo.categoria_insumo_id == categoria_id)
    if fornecedor_id:
        stmt = stmt.where(PedidoCompra.fornecedor_id == fornecedor_id)
    if de:
        stmt = stmt.where(PedidoCompra.criado_em >= de)
    if ate:
        stmt = stmt.where(PedidoCompra.criado_em <= ate)

    categorias = {c.id: c.nome for c in s.scalars(select(InsumoCategoria)).all()}

    saida = []
    for item, pedido, sup, solic, insumo, obra, forn in s.execute(stmt).all():
        total = (Decimal(str(item.quantidade)) *
                 Decimal(str(item.preco_unitario))).quantize(_CENT)
        linha = {
            "pedido_id": pedido.id, "pedido": pedido.numero,
            "pedido_status": pedido.status.value,
            "data": pedido.criado_em.date().isoformat() if pedido.criado_em else None,
            "obra_id": obra.id, "obra": obra.codigo, "obra_nome": obra.nome,
            "insumo_id": insumo.id, "insumo_codigo": insumo.codigo,
            "insumo": insumo.descricao,
            "categoria_id": insumo.categoria_insumo_id,
            "categoria": categorias.get(insumo.categoria_insumo_id, "Sem categoria"),
            "especificacao": sup.especificacao or "",
            "unidade": sup.unidade,
            "quantidade": float(item.quantidade),
            "recebida": float(sup.quantidade_recebida or 0),
            "preco_unitario": float(item.preco_unitario),
            "total": _dinheiro(total),
            "fornecedor_id": forn.id, "fornecedor": forn.razao_social,
            "solicitacao": solic.numero, "solicitacao_titulo": solic.titulo,
        }
        if busca:
            # A busca livre é do jeito que a pessoa procura: qualquer palavra
            # que ela digitar tem de aparecer em ALGUM campo da linha.
            texto = " ".join(str(v) for v in linha.values()).lower()
            if not all(p in texto for p in busca.lower().split()):
                continue
        saida.append(linha)

    saida.sort(key=lambda x: (x["obra"], x["insumo"], x["data"] or ""))
    return saida


def _resumo(linhas_do_grupo: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "itens": len(linhas_do_grupo),
        "quantidade": round(sum(l["quantidade"] for l in linhas_do_grupo), 3),
        "total": _dinheiro(sum(Decimal(str(l["total"])) for l in linhas_do_grupo)),
        "pedidos": len({l["pedido_id"] for l in linhas_do_grupo}),
        "fornecedores": len({l["fornecedor_id"] for l in linhas_do_grupo}),
    }


def arvore(s: Session, *, agrupar: str = "obra", **filtros) -> dict[str, Any]:
    """O relatório em dois níveis, para a tela que abre e fecha.

    Agrupando por obra, o segundo nível é o insumo — "o que esta obra comprou".
    Agrupando por insumo, o segundo nível é a obra — "tudo que foi de cimento,
    e para onde foi". A mesma consulta, lida pelo outro lado.
    """
    if agrupar not in AGRUPAMENTOS:
        agrupar = "obra"
    todas = linhas(s, **filtros)

    # o segundo nível é sempre o "outro" eixo — é o que torna o relatório útil
    segundo = "insumo" if agrupar in ("obra", "fornecedor", "categoria") else "obra"

    grupos: dict[Any, dict[str, Any]] = {}
    for l in todas:
        chave = l[f"{agrupar}_id"]
        g = grupos.setdefault(chave, {
            "id": chave, "rotulo": l[agrupar],
            "detalhe": l.get("obra_nome") if agrupar == "obra" else "",
            "filhos": {}, "linhas": []})
        g["linhas"].append(l)
        f = g["filhos"].setdefault(l[f"{segundo}_id"], {
            "id": l[f"{segundo}_id"], "rotulo": l[segundo],
            "unidade": l["unidade"], "linhas": []})
        f["linhas"].append(l)

    saida = []
    for g in grupos.values():
        filhos = []
        for f in g["filhos"].values():
            filhos.append({"id": f["id"], "rotulo": f["rotulo"],
                           "unidade": f["unidade"], **_resumo(f["linhas"])})
        filhos.sort(key=lambda x: -x["total"])
        saida.append({"id": g["id"], "rotulo": g["rotulo"],
                      "detalhe": g["detalhe"], "filhos": filhos,
                      **_resumo(g["linhas"])})
    saida.sort(key=lambda x: -x["total"])

    return {
        "agrupar": agrupar, "agrupar_rotulo": AGRUPAMENTOS[agrupar],
        "segundo_nivel": segundo,
        "grupos": saida,
        "total_geral": _resumo(todas),
    }
