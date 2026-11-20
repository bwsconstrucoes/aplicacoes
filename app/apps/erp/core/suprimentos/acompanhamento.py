# ============================================================================
# ERP — core/suprimentos/acompanhamento.py
# O painel do coordenador: cada compra, do pedido ao pagamento, numa linha.
#
# O PEDIDO, nas palavras do dono (20/11/2026):
#
#   "O coordenador de suprimentos precisa monitorar e acompanhar o que ele
#    compra. (…) precisa fechar todo o ciclo do pedido à entrega e poder
#    acompanhar o pagamento. É necessária visão geral e cruzada das
#    informações."
#
# E os dois problemas que ele frisou, porque são os que existem hoje:
#
#   1. "O fechamento do ciclo, que se completa com a entrega do material de
#      forma integral" — o recebimento tem de dizer o que foi pedido e o que
#      chegou, e a diferença vira pendência NA MÃO DO COORDENADOR.
#   2. "Muitas vezes chega material e a obra atrasa o lançamento das notas e
#      boleto. E o financeiro fica cego da informação."
#
# O QUE ESTE MÓDULO FAZ É CRUZAR, e não guardar. Nada aqui é digitado: a
# entrega vem do recebimento que a obra registra, a nota vem do cruzamento de
# notas (`documentos_fiscais.pedido_compra_id`), e o pagamento vem do título
# que nasceu daquela nota. Um campo "situação" digitado à mão divergiria da
# realidade na primeira semana — foi o que aconteceu com a planilha.
#
# A SITUAÇÃO DO CICLO é uma só por pedido, e é sempre O PRÓXIMO PASSO QUE
# FALTA. O pedido só está FECHADO quando o material chegou inteiro, a nota
# está lançada e a conta foi paga. Antes disso, alguém deve alguma coisa — e a
# linha diz quem.
#
# CUSTO: sete consultas para a tela inteira, seja com dez pedidos ou com dez
# mil — nunca uma consulta por pedido. Não usa o padrão do resto do
# suprimento (ler a tabela inteira e filtrar em Python), que não aguenta
# volume de verdade.
# ============================================================================
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
from app.apps.erp.db.models.cadastros import (
    Cotacao, CotacaoItem, Fornecedor, Insumo, InsumoCategoria, Obra, PedidoCompra,
    PedidoEntrega, PedidoItem, PedidoItemReserva, StatusCotacao,
    StatusItemSuprimento,
    PrevisaoPagamento, Recebimento, RecebimentoItem, StatusPedidoCompra,
    SuprimentoItem, SuprimentoSolicitacao, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    DocumentoFiscal, Parcela, SituacaoNota, StatusParcela, StatusTitulo, Titulo,
)

logger = logging.getLogger(__name__)

_CENT = Decimal("0.01")
_ZERO = Decimal("0")

# As situações do ciclo, NA ORDEM em que a compra anda. O rótulo é o que a
# tela mostra; "de_quem" é quem tem de agir — é a pergunta que o coordenador
# faz ao olhar a linha.
CICLO = {
    "AGUARDANDO_AUTORIZACAO": ("Aguardando autorização", "diretoria"),
    "AGUARDANDO_ENTREGA":     ("Aguardando entrega", "fornecedor"),
    "ENTREGA_ATRASADA":       ("Entrega atrasada", "fornecedor"),
    "ENTREGA_PARCIAL":        ("Chegou em parte", "fornecedor"),
    "ENTREGUE_SEM_NOTA":      ("Entregue, falta lançar a nota", "obra"),
    "ENTREGUE_A_PAGAR":       ("Entregue, a pagar", "financeiro"),
    "FECHADO":                ("Ciclo fechado", None),
    # Recusado ou cancelado: não é compra. Só aparece quando alguém pede para
    # ver TODOS os registros — o histórico não pode sumir.
    "SEM_EFEITO":             ("Recusado ou cancelado", None),
}
ORDEM_DO_CICLO = list(CICLO)

_TITULO_MORTO = (StatusTitulo.CANCELADO, StatusTitulo.ESTORNADO)


def _d(v: Any) -> Decimal:
    return Decimal(str(v if v is not None else 0))


def _f(v: Any) -> float:
    return float(_d(v).quantize(_CENT))


def tolerancia(total: Decimal) -> Decimal:
    """Quanto a nota pode diferir do pedido e ainda contar como "veio tudo".

    Nota raramente bate no centavo com o pedido — arredondamento do
    fornecedor, um frete rateado. 1% (com piso de cinco centavos) cobre isso
    sem deixar passar uma nota que veio pela metade.
    """
    return max(Decimal("0.05"), (total * Decimal("0.01")).quantize(_CENT))


def pedidos_no_escopo(s: Session, usuario: Optional[Usuario],
                      status: tuple) -> list[PedidoCompra]:
    """Os pedidos que a pessoa enxerga: todos, ou os que têm item de obra dela.

    É a mesma regra do cruzamento de notas (nota → pedido → item → obra): o
    pedido é um documento só, e aparece inteiro para quem tem uma das obras.

    `obras_de_registro_sem_autor`, e NÃO `obras_do_usuario`: pedido de compra
    não tem autor que sirva de recorte, e a segunda devolve None ("sem filtro")
    para quem enxerga por autoria — o administrativo de obra passaria a ver as
    compras da empresa inteira. É a armadilha achada nas Locações em 11/09/2026.
    """
    stmt = select(PedidoCompra).where(PedidoCompra.status.in_(status))
    permitidas = obras_de_registro_sem_autor(s, usuario) if usuario is not None else None
    if permitidas is not None:
        do_escopo = (select(PedidoItem.pedido_id)
                     .join(SuprimentoItem,
                           SuprimentoItem.id == PedidoItem.suprimento_item_id)
                     .where(SuprimentoItem.obra_id.in_(permitidas or [0])))
        stmt = stmt.where(PedidoCompra.id.in_(do_escopo))
    return list(s.scalars(stmt.order_by(PedidoCompra.id.desc())).all())


def carregar(s: Session, pedidos: list[PedidoCompra]) -> dict[str, Any]:
    """Tudo que pertence a estes pedidos, em seis consultas.

    Separado do `painel` porque a projeção de pagamentos precisa exatamente do
    mesmo retrato — e duas contas do mesmo pedido divergiriam.
    """
    ids = [p.id for p in pedidos] or [0]

    itens: dict[int, list[dict[str, Any]]] = defaultdict(list)
    recebido_por_linha: dict[int, Decimal] = defaultdict(lambda: _ZERO)
    chegadas: dict[int, list[Recebimento]] = defaultdict(list)

    for rec in s.scalars(select(Recebimento).where(
            Recebimento.pedido_id.in_(ids)).order_by(Recebimento.data)).all():
        chegadas[rec.pedido_id].append(rec)
    for linha_id, soma in s.execute(
            select(RecebimentoItem.pedido_item_id, func.sum(RecebimentoItem.quantidade))
            .join(Recebimento, Recebimento.id == RecebimentoItem.recebimento_id)
            .where(Recebimento.pedido_id.in_(ids))
            .group_by(RecebimentoItem.pedido_item_id)).all():
        recebido_por_linha[linha_id] = _d(soma)

    categorias = {c.id: c.nome for c in s.scalars(select(InsumoCategoria)).all()}
    for linha, sup, insumo, obra, sol in s.execute(
            select(PedidoItem, SuprimentoItem, Insumo, Obra, SuprimentoSolicitacao)
            .join(SuprimentoItem, SuprimentoItem.id == PedidoItem.suprimento_item_id)
            .join(Insumo, Insumo.id == SuprimentoItem.insumo_id)
            .join(Obra, Obra.id == SuprimentoItem.obra_id)
            .join(SuprimentoSolicitacao,
                  SuprimentoSolicitacao.id == SuprimentoItem.solicitacao_id)
            .where(PedidoItem.pedido_id.in_(ids))
            .order_by(PedidoItem.pedido_id, PedidoItem.numero)).all():
        quantidade = _d(linha.quantidade)
        recebido = min(recebido_por_linha[linha.id], quantidade)
        preco = _d(linha.preco_unitario)
        itens[linha.pedido_id].append({
            "pedido_item_id": linha.id, "numero": linha.numero,
            "insumo": insumo.descricao, "especificacao": sup.especificacao or "",
            "categoria_id": insumo.categoria_insumo_id,
            "categoria": categorias.get(insumo.categoria_insumo_id, "Sem categoria"),
            "unidade": sup.unidade, "obra_id": obra.id, "obra": obra.codigo,
            "solicitacao": sol.numero, "solicitacao_previsao": sol.previsao_entrega,
            "quantidade": quantidade, "recebido": recebido,
            "saldo": quantidade - recebido, "preco_unitario": preco,
            "valor": (quantidade * preco).quantize(_CENT),
            "valor_recebido": (recebido * preco).quantize(_CENT),
        })

    previsoes: dict[int, list[PrevisaoPagamento]] = defaultdict(list)
    for prev in s.scalars(select(PrevisaoPagamento).where(
            PrevisaoPagamento.pedido_id.in_(ids))
            .order_by(PrevisaoPagamento.vencimento, PrevisaoPagamento.numero)).all():
        previsoes[prev.pedido_id].append(prev)

    notas: dict[int, list[DocumentoFiscal]] = defaultdict(list)
    for nota in s.scalars(select(DocumentoFiscal).where(
            DocumentoFiscal.pedido_compra_id.in_(ids),
            DocumentoFiscal.situacao != SituacaoNota.CANCELADA)).all():
        notas[nota.pedido_compra_id].append(nota)
    ids_notas = [n.id for lista in notas.values() for n in lista] or [0]

    # O TÍTULO QUE NASCEU DA NOTA. É por aqui que o pedido chega ao financeiro:
    # pedido → nota vinculada → título lançado com aquela nota.
    titulos_por_nota: dict[int, list[Titulo]] = defaultdict(list)
    todos_titulos: list[Titulo] = []
    for titulo in s.scalars(select(Titulo).where(
            Titulo.documento_fiscal_id.in_(ids_notas),
            Titulo.status.notin_(_TITULO_MORTO))).all():
        titulos_por_nota[titulo.documento_fiscal_id].append(titulo)
        todos_titulos.append(titulo)
    parcelas: dict[int, list[Parcela]] = defaultdict(list)
    if todos_titulos:
        for parcela in s.scalars(select(Parcela).where(
                Parcela.titulo_id.in_([t.id for t in todos_titulos]),
                Parcela.status != StatusParcela.CANCELADA)
                .order_by(Parcela.vencimento)).all():
            parcelas[parcela.titulo_id].append(parcela)

    fornecedores = {f.id: f.razao_social for f in s.scalars(select(Fornecedor).where(
        Fornecedor.id.in_({p.fornecedor_id for p in pedidos} or {0}))).all()}
    desvios: dict[int, list[PedidoEntrega]] = defaultdict(list)
    for desvio in s.scalars(select(PedidoEntrega).where(
            PedidoEntrega.pedido_id.in_(ids))).all():
        desvios[desvio.pedido_id].append(desvio)

    cotacoes = {c.id: c.numero for c in s.scalars(select(Cotacao).where(
        Cotacao.id.in_({p.cotacao_id for p in pedidos if p.cotacao_id} or {0}))).all()}

    return {"itens": itens, "chegadas": chegadas, "previsoes": previsoes,
            "cotacoes": cotacoes,
            "notas": notas, "titulos_por_nota": titulos_por_nota,
            "parcelas": parcelas, "fornecedores": fornecedores,
            "desvios": desvios}


def total_do_pedido(pedido: PedidoCompra, itens: list[dict[str, Any]]) -> Decimal:
    soma = sum((i["valor"] for i in itens), _ZERO)
    soma += _d(pedido.frete) - _d(pedido.desconto)
    return max(soma, _ZERO).quantize(_CENT)


def financeiro_do_pedido(pedido: PedidoCompra, base: dict[str, Any]) -> dict[str, Any]:
    """Quanto do pedido já tem nota, quanto virou conta a pagar e quanto foi pago."""
    notas = base["notas"].get(pedido.id, [])
    em_nota = sum((_d(n.valor_total) for n in notas), _ZERO)
    em_titulo = pago = _ZERO
    notas_sem_titulo = 0
    proximo: Optional[date] = None
    numeros = []
    for nota in notas:
        titulos = base["titulos_por_nota"].get(nota.id, [])
        if not titulos:
            notas_sem_titulo += 1
        for titulo in titulos:
            numeros.append(titulo.numero_sp)
            em_titulo += _d(titulo.valor_liquido)
            for parcela in base["parcelas"].get(titulo.id, []):
                if parcela.status is StatusParcela.PAGA:
                    pago += _d(parcela.valor)
                elif proximo is None or parcela.vencimento < proximo:
                    proximo = parcela.vencimento
    return {"notas": len(notas), "em_nota": em_nota.quantize(_CENT),
            "notas_sem_titulo": notas_sem_titulo,
            "em_titulo": em_titulo.quantize(_CENT), "pago": pago.quantize(_CENT),
            "proximo_vencimento": proximo, "titulos": numeros}


def retrato(pedido: PedidoCompra, base: dict[str, Any], hoje: date) -> dict[str, Any]:
    """Um pedido, cruzado: entrega × nota × pagamento, e o próximo passo."""
    itens = base["itens"].get(pedido.id, [])
    total = total_do_pedido(pedido, itens)
    tol = tolerancia(total)
    valor_itens = sum((i["valor"] for i in itens), _ZERO)
    valor_recebido = sum((i["valor_recebido"] for i in itens), _ZERO)
    com_saldo = [i for i in itens if i["saldo"] > 0]
    chegadas = base["chegadas"].get(pedido.id, [])
    fin = financeiro_do_pedido(pedido, base)
    previsoes = base["previsoes"].get(pedido.id, [])
    previsto = sum((_d(p.valor) for p in previsoes), _ZERO)

    entregue = bool(itens) and not com_saldo
    atraso = 0
    if pedido.previsao_entrega and not entregue and pedido.previsao_entrega < hoje:
        atraso = (hoje - pedido.previsao_entrega).days

    # Quanto do que CHEGOU ainda não tem nota. É a cegueira do financeiro,
    # medida em reais: material no canteiro e nenhuma conta a pagar à vista.
    sem_nota = max(valor_recebido - fin["em_nota"], _ZERO)
    if sem_nota <= tol:
        sem_nota = _ZERO
    dias_sem_nota = 0
    if sem_nota > 0 and chegadas:
        # Sem nota nenhuma, conta da PRIMEIRA chegada; com nota parcial, da
        # última — a nota que falta é a da remessa mais recente.
        referencia = chegadas[0].data if not fin["notas"] else chegadas[-1].data
        dias_sem_nota = max((hoje - referencia).days, 0)

    autorizado = pedido.status is StatusPedidoCompra.AUTORIZADO
    if pedido.status in (StatusPedidoCompra.RECUSADO, StatusPedidoCompra.CANCELADO):
        ciclo = "SEM_EFEITO"
        atraso, sem_nota, dias_sem_nota = 0, _ZERO, 0
    elif not autorizado:
        ciclo = "AGUARDANDO_AUTORIZACAO"
    elif not entregue:
        ciclo = ("ENTREGA_ATRASADA" if atraso else
                 "ENTREGA_PARCIAL" if valor_recebido > 0 else "AGUARDANDO_ENTREGA")
    elif fin["em_nota"] + tol < total or fin["notas_sem_titulo"]:
        ciclo = "ENTREGUE_SEM_NOTA"
    elif fin["pago"] + tol < fin["em_titulo"] or fin["em_titulo"] + tol < total:
        ciclo = "ENTREGUE_A_PAGAR"
    else:
        ciclo = "FECHADO"

    alertas = []
    if ciclo == "SEM_EFEITO":
        com_saldo = []
    if atraso:
        alertas.append({"tipo": "ENTREGA_ATRASADA", "grave": atraso > 7,
                        "texto": f"Entrega prevista para "
                                 f"{pedido.previsao_entrega.strftime('%d/%m/%Y')} — "
                                 f"{atraso} dia(s) de atraso. Cobrar nova data do fornecedor."})
    if chegadas and com_saldo:
        alertas.append({"tipo": "PENDENCIA", "grave": True,
                        "texto": f"Chegou incompleto: {len(com_saldo)} item(ns) com "
                                 f"saldo a receber. Resolver com o fornecedor."})
    if sem_nota > 0:
        alertas.append({"tipo": "SEM_NOTA", "grave": dias_sem_nota >= 3,
                        "texto": f"Material recebido há {dias_sem_nota} dia(s) e "
                                 f"R$ {_br(sem_nota)} ainda sem nota lançada. O "
                                 f"financeiro não enxerga esta conta."})
    if fin["notas_sem_titulo"]:
        alertas.append({"tipo": "NOTA_SEM_TITULO", "grave": True,
                        "texto": f"{fin['notas_sem_titulo']} nota(s) vinculada(s) ao "
                                 f"pedido que ainda não viraram conta a pagar."})
    if fin["em_nota"] > valor_recebido + tol and not entregue:
        alertas.append({"tipo": "NOTA_SEM_MATERIAL", "grave": False,
                        "texto": "Há nota lançada por mais do que a obra registrou "
                                 "como recebido. Conferir se o material chegou e o "
                                 "recebimento não foi lançado."})
    if fin["pago"] > 0 and not entregue and not pedido.antecipado:
        alertas.append({"tipo": "PAGO_SEM_MATERIAL", "grave": True,
                        "texto": "Já há pagamento e o material não chegou por inteiro."})
    if autorizado and not pedido.previsao_entrega and not entregue:
        alertas.append({"tipo": "SEM_PREVISAO", "grave": False,
                        "texto": "Pedido sem previsão de entrega — o sistema não "
                                 "tem como avisar do atraso."})

    obras = []
    for i in itens:
        if i["obra"] not in obras:
            obras.append(i["obra"])
    rotulo, de_quem = CICLO[ciclo]
    return {
        "id": pedido.id, "numero": pedido.numero,
        "status": pedido.status.value,
        "fornecedor_id": pedido.fornecedor_id,
        "fornecedor": base["fornecedores"].get(pedido.fornecedor_id, ""),
        "obras": obras, "obra_ids": sorted({i["obra_id"] for i in itens}),
        "cotacao_id": pedido.cotacao_id,
        "cotacao": base.get("cotacoes", {}).get(pedido.cotacao_id),
        "motivo": pedido.motivo,
        "antecipado": bool(pedido.antecipado),
        "categoria_ids": sorted({i["categoria_id"] for i in itens if i["categoria_id"]}),
        "categorias": sorted({i["categoria"] for i in itens}),
        # Todos os itens, para a busca achar o pedido pelo material comprado —
        # e não só pelos que estão pendentes.
        "itens": [{"insumo": i["insumo"], "especificacao": i["especificacao"],
                   "unidade": i["unidade"], "obra": i["obra"],
                   "solicitacao": i["solicitacao"],
                   "pedido": float(i["quantidade"]),
                   "recebido": float(i["recebido"]),
                   "saldo": float(i["saldo"]),
                   "preco_unitario": float(i["preco_unitario"]),
                   "valor": float(i["valor"])} for i in itens],
        "fechado_dia": pedido.criado_em.date().isoformat() if pedido.criado_em else None,
        "autorizado_dia": (pedido.autorizado_em.date().isoformat()
                          if pedido.autorizado_em else None),
        "previsao_entrega": (pedido.previsao_entrega.isoformat()
                             if pedido.previsao_entrega else None),
        "total": _f(total),
        "ciclo": ciclo, "ciclo_rotulo": rotulo, "de_quem": de_quem,
        "fora_da_obra": [d.rotulo for d in base["desvios"].get(pedido.id, [])],
        "entrega": {
            "itens": len(itens), "completos": len(itens) - len(com_saldo),
            "com_saldo": len(com_saldo),
            "valor_recebido": _f(valor_recebido),
            "percentual": (round(float(valor_recebido / valor_itens * 100))
                           if valor_itens > 0 else 0),
            "chegadas": len(chegadas),
            "ultima_chegada": chegadas[-1].data.isoformat() if chegadas else None,
            "dias_de_atraso": atraso, "completa": entregue,
        },
        "nota": {"quantidade": fin["notas"], "valor": _f(fin["em_nota"]),
                 "recebido_sem_nota": _f(sem_nota), "dias_sem_nota": dias_sem_nota,
                 "sem_titulo": fin["notas_sem_titulo"]},
        "pagamento": {
            "previsto": _f(previsto), "em_titulo": _f(fin["em_titulo"]),
            "pago": _f(fin["pago"]),
            "a_pagar": _f(max(fin["em_titulo"] - fin["pago"], _ZERO)),
            "sem_conta": _f(max(total - fin["em_titulo"], _ZERO)) if autorizado else 0.0,
            "proximo_vencimento": (fin["proximo_vencimento"].isoformat()
                                   if fin["proximo_vencimento"] else None),
            "titulos": fin["titulos"],
        },
        "alertas": alertas,
        "pendentes": [{"insumo": i["insumo"], "especificacao": i["especificacao"],
                       "unidade": i["unidade"], "obra": i["obra"],
                       "solicitacao": i["solicitacao"],
                       "pedido": float(i["quantidade"]),
                       "recebido": float(i["recebido"]),
                       "saldo": float(i["saldo"])} for i in com_saldo],
    }


def _br(valor: Decimal) -> str:
    inteiro, _, centavos = f"{valor.quantize(_CENT):,.2f}".partition(".")
    return inteiro.replace(",", ".") + "," + centavos


def painel(s: Session, usuario: Optional[Usuario] = None, *,
           incluir_fechados: bool = False, incluir_sem_efeito: bool = False,
           hoje: Optional[date] = None) -> dict[str, Any]:
    """A tela inteira: uma linha por pedido de pé, e o resumo por situação.

    Ciclo fechado fica de fora por padrão — é o que já não pede ação de
    ninguém. Quem quiser conferir o passado marca "incluir fechados".
    """
    hoje = hoje or date.today()
    situacoes = [StatusPedidoCompra.AUTORIZADO,
                 StatusPedidoCompra.AGUARDANDO_AUTORIZACAO]
    if incluir_sem_efeito:
        situacoes += [StatusPedidoCompra.RECUSADO, StatusPedidoCompra.CANCELADO]
    pedidos = pedidos_no_escopo(s, usuario, tuple(situacoes))
    base = carregar(s, pedidos)
    linhas = [retrato(p, base, hoje) for p in pedidos]

    resumo = {chave: {"chave": chave, "rotulo": CICLO[chave][0],
                      "de_quem": CICLO[chave][1], "pedidos": 0, "valor": 0.0}
              for chave in ORDEM_DO_CICLO}
    for linha in linhas:
        resumo[linha["ciclo"]]["pedidos"] += 1
        resumo[linha["ciclo"]]["valor"] = round(
            resumo[linha["ciclo"]]["valor"] + linha["total"], 2)

    abertos = [l for l in linhas if l["ciclo"] not in ("FECHADO", "SEM_EFEITO")]
    return {
        "hoje": hoje.isoformat(),
        "pedidos": [l for l in linhas
                    if (incluir_fechados or l["ciclo"] != "FECHADO")
                    and (incluir_sem_efeito or l["ciclo"] != "SEM_EFEITO")],
        "resumo": [resumo[c] for c in ORDEM_DO_CICLO],
        "destaques": {
            # Os dois números que respondem aos dois problemas que ele frisou.
            "itens_pendentes": sum(len(l["pendentes"]) for l in abertos
                                   if l["entrega"]["chegadas"]),
            "recebido_sem_nota": round(sum(l["nota"]["recebido_sem_nota"]
                                           for l in abertos), 2),
            "pedidos_sem_nota": sum(1 for l in abertos
                                    if l["nota"]["recebido_sem_nota"] > 0),
            "atrasados": sum(1 for l in abertos if l["entrega"]["dias_de_atraso"]),
            "a_pagar": round(sum(l["pagamento"]["a_pagar"] for l in abertos), 2),
            "fechados": resumo["FECHADO"]["pedidos"],
        },
    }


# ---------------------------------------------------------------------------
# O PLANEJAMENTO DO COORDENADOR — item a item, do pedido da obra à chegada
#
# O dono, em 20/11/2026: "o coordenador precisa entender, de acordo com os
# pedidos, previsão de entrega, dentro do planejamento do suprimento, como é
# que está o planejamento dele: o que é prioritário, o que não é (…) o que
# está atrasando, o que atrasou, o que deveria ter chegado e não chegou."
#
# A unidade aqui é o ITEM que a obra pediu, e não o pedido de compra: é o item
# que tem a data em que a obra PRECISA do material, e ele existe desde antes
# de haver cotação. Três datas contam a história:
#
#   precisa até     a data que a obra informou na solicitação
#   previsto        a data que o fornecedor deu, no pedido de compra
#   hoje
#
# e a comparação entre elas é a situação do prazo — que é o que ordena a fila.
#
# ⚠ O QUE NÃO EXISTE: prazo de entrega típico por insumo. A tabela de prazos
# da planilha antiga NÃO foi migrada (decisão dele de 04/09/2026), então "vai
# dar tempo?" antes de haver pedido é respondido por uma régua simples: sem
# pedido autorizado e faltando `FOLGA_DIAS` ou menos, o item está EM RISCO.
# ---------------------------------------------------------------------------
FOLGA_DIAS = 7

PRAZOS = {
    "ATRASADO":     "Já deveria ter chegado",
    "FORNECEDOR":   "Fornecedor estourou a data dele",
    "VAI_ATRASAR":  "Vai chegar depois do que a obra precisa",
    "EM_RISCO":     "Em risco: prazo curto e ainda sem pedido",
    "NO_PRAZO":     "No prazo",
    "SEM_DATA":     "Sem data informada pela obra",
}
ORDEM_DOS_PRAZOS = list(PRAZOS)

ETAPAS = {
    "SEM_COTACAO":  "Sem cotação",
    "EM_COTACAO":   "Em cotação",
    "A_AUTORIZAR":  "Pedido aguardando autorização",
    "A_CAMINHO":    "Comprado, aguardando entrega",
    "EM_PARTE":     "Chegou em parte",
}
ORDEM_DAS_ETAPAS = list(ETAPAS)
_PESO_PRIORIDADE = {"ALTA": 0, "MEDIA": 1, "NORMAL": 2}


def situacao_do_prazo(hoje: date, precisa_ate: Optional[date],
                      previsto: Optional[date], comprado: bool) -> tuple[str, int, str]:
    """(chave, dias, frase). `dias` é o tamanho do problema, para ordenar."""
    if precisa_ate is not None and hoje > precisa_ate:
        dias = (hoje - precisa_ate).days
        return "ATRASADO", dias, f"a obra precisava em {precisa_ate:%d/%m} — {dias} dia(s) de atraso"
    if comprado and previsto is not None and previsto < hoje:
        dias = (hoje - previsto).days
        return "FORNECEDOR", dias, f"o fornecedor prometeu para {previsto:%d/%m} — {dias} dia(s) atrás"
    if precisa_ate is None:
        return "SEM_DATA", 0, "a obra não informou para quando precisa"
    if previsto is not None and previsto > precisa_ate:
        dias = (previsto - precisa_ate).days
        return "VAI_ATRASAR", dias, (f"previsto para {previsto:%d/%m}, "
                                     f"{dias} dia(s) depois do que a obra precisa")
    faltam = (precisa_ate - hoje).days
    if not comprado and faltam <= FOLGA_DIAS:
        return "EM_RISCO", FOLGA_DIAS - faltam, (
            f"faltam {faltam} dia(s) e ainda não há pedido autorizado")
    return "NO_PRAZO", 0, f"faltam {faltam} dia(s)"


def planejamento(s: Session, usuario: Optional[Usuario] = None, *,
                 hoje: Optional[date] = None) -> dict[str, Any]:
    """Tudo que a obra pediu e ainda não chegou por inteiro, na ordem em que
    o coordenador deve olhar. Cinco consultas, com qualquer volume."""
    hoje = hoje or date.today()
    encerrados = (StatusItemSuprimento.RECEBIDO, StatusItemSuprimento.CANCELADO,
                  StatusItemSuprimento.ALMOXARIFADO)
    stmt = (select(SuprimentoItem, SuprimentoSolicitacao, Insumo, Obra)
            .join(SuprimentoSolicitacao,
                  SuprimentoSolicitacao.id == SuprimentoItem.solicitacao_id)
            .join(Insumo, Insumo.id == SuprimentoItem.insumo_id)
            .join(Obra, Obra.id == SuprimentoItem.obra_id)
            .where(SuprimentoItem.status.notin_(encerrados)))
    permitidas = obras_de_registro_sem_autor(s, usuario) if usuario is not None else None
    if permitidas is not None:
        stmt = stmt.where(SuprimentoItem.obra_id.in_(permitidas or [0]))
    linhas = s.execute(stmt).all()
    ids = [sup.id for sup, _, _, _ in linhas] or [0]

    categorias = {c.id: c.nome for c in s.scalars(select(InsumoCategoria)).all()}
    pedido_de: dict[int, PedidoCompra] = {}
    for item_id, pedido in s.execute(
            select(PedidoItemReserva.suprimento_item_id, PedidoCompra)
            .join(PedidoCompra, PedidoCompra.id == PedidoItemReserva.pedido_id)
            .where(PedidoItemReserva.suprimento_item_id.in_(ids))).all():
        pedido_de[item_id] = pedido
    cotacao_de: dict[int, Cotacao] = {}
    for item_id, cotacao in s.execute(
            select(CotacaoItem.suprimento_item_id, Cotacao)
            .join(Cotacao, Cotacao.id == CotacaoItem.cotacao_id)
            .where(CotacaoItem.suprimento_item_id.in_(ids),
                   Cotacao.status == StatusCotacao.ABERTA)
            .order_by(Cotacao.id)).all():
        cotacao_de[item_id] = cotacao
    fornecedores = {f.id: f.razao_social for f in s.scalars(select(Fornecedor).where(
        Fornecedor.id.in_({p.fornecedor_id for p in pedido_de.values()} or {0}))).all()}

    saida = []
    for sup, sol, insumo, obra in linhas:
        pedido = pedido_de.get(sup.id)
        cotacao = cotacao_de.get(sup.id)
        comprado = (pedido is not None
                    and pedido.status is StatusPedidoCompra.AUTORIZADO)
        recebido = _d(sup.quantidade_recebida)
        if comprado:
            etapa = "EM_PARTE" if recebido > 0 else "A_CAMINHO"
        elif pedido is not None:
            etapa = "A_AUTORIZAR"
        elif cotacao is not None:
            etapa = "EM_COTACAO"
        else:
            etapa = "SEM_COTACAO"
        previsto = pedido.previsao_entrega if pedido is not None else None
        prazo, dias, frase = situacao_do_prazo(hoje, sol.previsao_entrega,
                                               previsto, comprado)
        prioridade = sol.prioridade.value if sol.prioridade else "NORMAL"
        saida.append({
            "id": sup.id, "insumo": insumo.descricao,
            "especificacao": sup.especificacao or "",
            "categoria_id": insumo.categoria_insumo_id,
            "categoria": categorias.get(insumo.categoria_insumo_id, "Sem categoria"),
            "quantidade": float(sup.quantidade), "recebido": float(recebido),
            "saldo": float(_d(sup.quantidade) - recebido), "unidade": sup.unidade,
            "obra_id": obra.id, "obra": obra.codigo,
            "solicitacao_id": sol.id, "solicitacao": sol.numero,
            "titulo": sol.titulo, "prioridade": prioridade,
            "pedido_em": sol.criado_em.date().isoformat() if sol.criado_em else None,
            "precisa_ate": (sol.previsao_entrega.isoformat()
                            if sol.previsao_entrega else None),
            "previsto": previsto.isoformat() if previsto else None,
            "etapa": etapa, "etapa_rotulo": ETAPAS[etapa],
            "situacao": sup.status.value,
            "prazo": prazo, "prazo_rotulo": PRAZOS[prazo], "prazo_dias": dias,
            "prazo_frase": frase,
            "cotacao_id": cotacao.id if cotacao else None,
            "cotacao": cotacao.numero if cotacao else None,
            "pedido_id": pedido.id if pedido else None,
            "pedido": pedido.numero if pedido else None,
            "fornecedor": (fornecedores.get(pedido.fornecedor_id, "")
                           if pedido else ""),
        })

    # A ORDEM É A RESPOSTA: primeiro o pior prazo; dentro dele, a prioridade
    # que a obra deu; depois o maior atraso; depois a data mais próxima.
    saida.sort(key=lambda i: (ORDEM_DOS_PRAZOS.index(i["prazo"]),
                              _PESO_PRIORIDADE.get(i["prioridade"], 2),
                              -i["prazo_dias"], i["precisa_ate"] or "9999"))

    def contar(chave: str, ordem: list[str], rotulos: dict[str, str]):
        return [{"chave": c, "rotulo": rotulos[c],
                 "itens": sum(1 for i in saida if i[chave] == c),
                 "alta": sum(1 for i in saida
                             if i[chave] == c and i["prioridade"] == "ALTA")}
                for c in ordem]

    return {"hoje": hoje.isoformat(), "folga_dias": FOLGA_DIAS, "itens": saida,
            "prazos": contar("prazo", ORDEM_DOS_PRAZOS, PRAZOS),
            "etapas": contar("etapa", ORDEM_DAS_ETAPAS, ETAPAS)}
