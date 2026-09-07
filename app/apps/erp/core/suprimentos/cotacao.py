# ============================================================================
# ERP — core/suprimentos/cotacao.py
# O mapa de cotação: fornecedores em coluna, insumos em linha, preço na célula.
#
# É o coração do processo de compras. Três coisas que o mapa em planilha não
# fazia e aqui faz:
#
#   1. COMPARA O CUSTO REAL, não o preço unitário. Frete, desconto e acréscimo
#      entram na conta — o fornecedor mais barato por item pode sair mais caro
#      no total, e é isso que decide a compra.
#   2. GUARDA TODO PREÇO NO BANCO DE PREÇOS, com data, fornecedor e origem.
#   3. NÃO TEM LIMITE de 50 insumos x 10 fornecedores.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.formato import (  # noqa: F401
    _dinheiro_br, _quantidade_br,
)
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    CondicaoPagamento, Cotacao, CotacaoFornecedor, CotacaoItem, CotacaoPreco,
    Fornecedor, Insumo, ModoEntrega, OrigemPreco, PrecoHistorico, StatusCotacao,
    StatusItemSuprimento, SuprimentoItem, TipoPreco, Usuario,
)

logger = logging.getLogger(__name__)

CENTAVO = Decimal("0.01")


def _dinheiro(valor: Any, campo: str, *, obrigatorio: bool = True) -> Decimal:
    texto = str(valor if valor is not None else "").strip()
    if not texto:
        if obrigatorio:
            raise ErroValidacao(f"{campo} é obrigatório.")
        return Decimal("0")
    if "," in texto:                        # 1.234,56 → 1234.56
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto)
    except InvalidOperation:
        raise ErroValidacao(f"{campo} tem de ser um número.")


def proximo_numero(s: Session) -> str:
    numeros = []
    for c in s.scalars(select(Cotacao)).all():
        numero = getattr(c, "numero", "") or ""
        sufixo = numero.split("-", 1)[1] if numero.startswith("COT-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return f"COT-{(max(numeros) + 1) if numeros else 1:04d}"


# ---------------------------------------------------------------------------
# Montar a cotação
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> Cotacao:
    """Abre a cotação com os itens escolhidos e leva cada um para COTAÇÃO."""
    titulo = " ".join((dados.get("titulo") or "").split())
    if len(titulo) < 3:
        raise ErroValidacao("Dê um título à cotação — é por ele que se procura depois.")

    ids = [int(x) for x in (dados.get("itens") or [])]
    if not ids:
        raise ErroValidacao("Escolha pelo menos um item para cotar.")

    itens = [s.get(SuprimentoItem, i) for i in ids]
    faltando = [i for i, obj in zip(ids, itens) if obj is None]
    if faltando:
        raise ErroValidacao(f"Item {faltando[0]} não encontrado.")

    ja_cotados = _itens_em_cotacao_aberta(s, ids)
    if ja_cotados:
        raise ErroValidacao(
            "Estes itens já estão numa cotação aberta: "
            f"{', '.join(str(i) for i in sorted(ja_cotados))}. "
            "Cotar duas vezes ao mesmo tempo dá dois preços para a mesma coisa.")

    cot = Cotacao(numero=proximo_numero(s), titulo=titulo,
                  observacoes=(dados.get("observacoes") or "").strip() or None,
                  criado_por=usuario.id, status=StatusCotacao.ABERTA)
    s.add(cot)
    s.flush()

    for n, item in enumerate(itens, start=1):
        s.add(CotacaoItem(cotacao_id=cot.id, suprimento_item_id=item.id, numero=n))
        if item.status in (StatusItemSuprimento.SOLICITACAO,
                           StatusItemSuprimento.SALA_TECNICA,
                           StatusItemSuprimento.PENDENCIA):
            item.status = StatusItemSuprimento.COTACAO
    s.flush()

    registrar_evento(s, "cotacao", cot.id, "ABERTA",
                     {"numero": cot.numero, "titulo": titulo, "itens": len(itens)},
                     usuario.id)
    return cot


def _itens_em_cotacao_aberta(s: Session, ids: list[int]) -> set[int]:
    abertas = {c.id for c in s.scalars(select(Cotacao)).all()
               if c.status is StatusCotacao.ABERTA}
    return {ci.suprimento_item_id for ci in s.scalars(select(CotacaoItem)).all()
            if ci.cotacao_id in abertas and ci.suprimento_item_id in set(ids)}


def adicionar_fornecedor(s: Session, cotacao_id: int, dados: dict[str, Any],
                         usuario: Usuario) -> CotacaoFornecedor:
    """Acrescenta uma coluna ao mapa."""
    cot = _cotacao_aberta(s, cotacao_id)
    fornecedor_id = int(dados.get("fornecedor_id") or 0)
    if s.get(Fornecedor, fornecedor_id) is None:
        raise ErroValidacao("Fornecedor não encontrado.")
    ja = [f for f in s.scalars(select(CotacaoFornecedor)).all()
          if f.cotacao_id == cot.id and f.fornecedor_id == fornecedor_id]
    if ja:
        raise ErroValidacao("Este fornecedor já está no mapa.")

    entrega = (dados.get("entrega") or "").strip().upper()
    if entrega and entrega not in {m.value for m in ModoEntrega}:
        raise ErroValidacao("Modo de entrega inválido.")

    coluna = CotacaoFornecedor(
        cotacao_id=cot.id, fornecedor_id=fornecedor_id,
        contato_id=int(dados["contato_id"]) if dados.get("contato_id") else None,
        condicao_pagamento_id=(int(dados["condicao_pagamento_id"])
                               if dados.get("condicao_pagamento_id") else None),
        entrega=ModoEntrega(entrega) if entrega else None,
        frete=_dinheiro(dados.get("frete"), "Frete", obrigatorio=False),
        desconto=_dinheiro(dados.get("desconto"), "Desconto", obrigatorio=False),
        acrescimo_percentual=_dinheiro(dados.get("acrescimo_percentual"),
                                       "Acréscimo", obrigatorio=False),
        respondido_por=(dados.get("respondido_por") or "").strip() or None,
        ordem=int(dados.get("ordem") or 0))
    s.add(coluna)
    s.flush()
    registrar_evento(s, "cotacao", cot.id, "FORNECEDOR_ADICIONADO",
                     {"fornecedor_id": fornecedor_id}, usuario.id)
    return coluna


def _cotacao_aberta(s: Session, cotacao_id: int) -> Cotacao:
    cot = s.get(Cotacao, cotacao_id)
    if cot is None:
        raise ErroNaoEncontrado("Cotação não encontrada.")
    if cot.status is not StatusCotacao.ABERTA:
        raise ErroValidacao(f"A cotação {cot.numero} já está "
                            f"{cot.status.value.lower()}.")
    return cot


# ---------------------------------------------------------------------------
# Lançar preços
# ---------------------------------------------------------------------------
def lancar_preco(s: Session, cotacao_fornecedor_id: int, cotacao_item_id: int,
                 preco: Any, usuario: Usuario, *,
                 origem: str = "DIGITADO", observacao: str = "",
                 herdado_de: Optional[int] = None) -> CotacaoPreco:
    """Grava o preço de um item para um fornecedor, e alimenta o banco de preços.

    Lançar de novo SUBSTITUI o valor — o fornecedor corrige a proposta e isso é
    normal. Cada lançamento entra no histórico, então a correção não apaga o
    que foi ofertado antes.
    """
    coluna = s.get(CotacaoFornecedor, cotacao_fornecedor_id)
    linha = s.get(CotacaoItem, cotacao_item_id)
    if coluna is None or linha is None:
        raise ErroNaoEncontrado("Célula do mapa não encontrada.")
    if coluna.cotacao_id != linha.cotacao_id:
        raise ErroValidacao("Fornecedor e item são de cotações diferentes.")
    _cotacao_aberta(s, coluna.cotacao_id)

    valor = _dinheiro(preco, "Preço")
    if valor <= 0:
        raise ErroValidacao("Preço tem de ser maior que zero.")
    if origem not in {o.value for o in OrigemPreco}:
        raise ErroValidacao("Origem de preço inválida.")

    atual = _celula(s, coluna.id, linha.id)
    if atual is None:
        atual = CotacaoPreco(cotacao_fornecedor_id=coluna.id, cotacao_item_id=linha.id,
                             preco_unitario=valor, origem=OrigemPreco(origem),
                             observacao=observacao or None,
                             herdado_de_cotacao_id=herdado_de)
        s.add(atual)
    else:
        atual.preco_unitario = valor
        atual.origem = OrigemPreco(origem)
        atual.observacao = observacao or atual.observacao
        atual.herdado_de_cotacao_id = herdado_de
    s.flush()

    item = s.get(SuprimentoItem, linha.suprimento_item_id)
    if item is not None:
        registrar_no_banco_de_precos(
            s, item=item, preco_unitario=valor, fornecedor_id=coluna.fornecedor_id,
            tipo=TipoPreco.COTADO, cotacao_id=coluna.cotacao_id,
            condicao_pagamento_id=coluna.condicao_pagamento_id)
    return atual


def _celula(s: Session, coluna_id: int, linha_id: int) -> Optional[CotacaoPreco]:
    for p in s.scalars(select(CotacaoPreco)).all():
        if p.cotacao_fornecedor_id == coluna_id and p.cotacao_item_id == linha_id:
            return p
    return None


def registrar_no_banco_de_precos(s: Session, *, item: SuprimentoItem,
                                 preco_unitario: Decimal, fornecedor_id: Optional[int],
                                 tipo: TipoPreco, cotacao_id: Optional[int] = None,
                                 condicao_pagamento_id: Optional[int] = None) -> PrecoHistorico:
    """Todo preço que passa pelo sistema fica guardado. É o que responde
    'este preço está bom?' sem depender da memória de ninguém."""
    registro = PrecoHistorico(
        insumo_id=item.insumo_id, especificacao=item.especificacao,
        unidade=item.unidade, preco_unitario=preco_unitario,
        quantidade=item.quantidade, fornecedor_id=fornecedor_id,
        obra_id=item.obra_id, condicao_pagamento_id=condicao_pagamento_id,
        tipo=tipo, cotacao_id=cotacao_id, data=date.today())
    s.add(registro)
    return registro


# ---------------------------------------------------------------------------
# O mapa
# ---------------------------------------------------------------------------
def montar_mapa(s: Session, cotacao_id: int) -> dict[str, Any]:
    """O mapa inteiro: linhas, colunas, preços e o total de cada fornecedor.

    O menor preço de cada linha vem marcado, e o total por fornecedor já
    considera frete, desconto e acréscimo — comparar só o unitário engana.
    """
    cot = s.get(Cotacao, cotacao_id)
    if cot is None:
        raise ErroNaoEncontrado("Cotação não encontrada.")

    linhas = sorted([x for x in s.scalars(select(CotacaoItem)).all()
                     if x.cotacao_id == cotacao_id], key=lambda x: x.numero or 0)
    colunas = sorted([x for x in s.scalars(select(CotacaoFornecedor)).all()
                      if x.cotacao_id == cotacao_id],
                     key=lambda x: (x.ordem or 0, x.id or 0))
    precos = {(p.cotacao_fornecedor_id, p.cotacao_item_id): p
              for p in s.scalars(select(CotacaoPreco)).all()}

    from app.apps.erp.core.suprimentos.solicitacao import (
        _quantas_correcoes, pode_corrigir,
    )
    correcoes = _quantas_correcoes(s)

    itens_saida, menores = [], {}
    for linha in linhas:
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        celulas = {}
        melhor = None
        for coluna in colunas:
            p = precos.get((coluna.id, linha.id))
            if p is None:
                continue
            total = (Decimal(str(p.preco_unitario)) *
                     Decimal(str(getattr(item, "quantidade", 0) or 0)))
            celulas[coluna.id] = {
                "preco_unitario": str(p.preco_unitario),
                "total": str(total.quantize(CENTAVO, rounding=ROUND_HALF_UP)),
                # `or DIGITADO` porque objeto recém-construído ainda não tem
                # o padrão do banco — e "em branco" aqui significa digitado.
                "origem": p.origem.value if p.origem else OrigemPreco.DIGITADO.value,
                "herdado_de": p.herdado_de_cotacao_id,
                "observacao": p.observacao,
            }
            if melhor is None or Decimal(str(p.preco_unitario)) < melhor[1]:
                melhor = (coluna.id, Decimal(str(p.preco_unitario)))
        if melhor:
            menores[linha.id] = melhor[0]
        itens_saida.append({
            "id": linha.id, "numero": linha.numero,
            "suprimento_item_id": linha.suprimento_item_id,
            "insumo_id": getattr(item, "insumo_id", None),
            "insumo": getattr(insumo, "descricao", ""),
            "especificacao": getattr(item, "especificacao", None),
            "quantidade": str(getattr(item, "quantidade", "")),
            "unidade": getattr(item, "unidade", ""),
            "obra_id": getattr(item, "obra_id", None),
            "obra": _codigo_da_obra(s, getattr(item, "obra_id", None)),
            # A SITUAÇÃO ATUAL do item, ao lado dele. Quem abre um mapa de
            # duas semanas atrás precisa ver, sem sair da tela, o que já
            # virou pedido, o que já chegou e o que ainda está em cotação.
            "status": (item.status.value if item is not None and item.status
                       else None),
            "status_rotulo": _rotulo_do_status(item),
            # O comprador corrige a linha do próprio mapa: é aqui que ele
            # descobre que a obra pediu em saco o que só vem em bag.
            "correcoes": correcoes.get(getattr(item, "id", 0), 0),
            "corrigivel": bool(item is not None and pode_corrigir(s, item)),
            "precos": celulas,
            "menor_preco_de": menores.get(linha.id),
        })

    colunas_saida = []
    for coluna in colunas:
        forn = s.get(Fornecedor, coluna.fornecedor_id)
        cond = (s.get(CondicaoPagamento, coluna.condicao_pagamento_id)
                if coluna.condicao_pagamento_id else None)
        soma = Decimal("0")
        cotados = 0
        for linha in itens_saida:
            celula = linha["precos"].get(coluna.id)
            if celula:
                soma += Decimal(celula["total"])
                cotados += 1
        total = _total_com_encargos(soma, coluna)
        colunas_saida.append({
            "id": coluna.id, "fornecedor_id": coluna.fornecedor_id,
            "fornecedor": getattr(forn, "razao_social", ""),
            "condicao": getattr(cond, "nome", None),
            "entrega": coluna.entrega.value if coluna.entrega else None,
            "frete": str(coluna.frete or 0), "desconto": str(coluna.desconto or 0),
            "acrescimo_percentual": str(coluna.acrescimo_percentual or 0),
            "respondido_por": coluna.respondido_por,
            "respondido_em": (coluna.respondido_em.isoformat()
                              if coluna.respondido_em else None),
            "anexo_id": coluna.anexo_id,
            "itens_cotados": cotados, "itens_no_mapa": len(itens_saida),
            "soma_itens": str(soma.quantize(CENTAVO, rounding=ROUND_HALF_UP)),
            "total": str(total),
        })

    completos = [c for c in colunas_saida if c["itens_cotados"] == len(itens_saida)
                 and itens_saida]
    fornecedor_unico = min(completos, key=lambda c: Decimal(c["total"])) if completos else None

    return {
        "id": cot.id, "numero": cot.numero, "titulo": cot.titulo,
        "status": cot.status.value,
        "itens": itens_saida, "fornecedores": colunas_saida,
        "melhor_fornecedor_unico": fornecedor_unico["id"] if fornecedor_unico else None,
        "total_pulverizado": str(_total_pulverizado(itens_saida)),
    }


def _codigo_da_obra(s: Session, obra_id: Optional[int]) -> str:
    from app.apps.erp.db.models.cadastros import Obra
    obra = s.get(Obra, obra_id) if obra_id else None
    return getattr(obra, "codigo", "") or ""


def _rotulo_do_status(item: Optional[SuprimentoItem]) -> str:
    from app.apps.erp.core.suprimentos.solicitacao import ROTULOS_STATUS
    if item is None or not item.status:
        return ""
    return ROTULOS_STATUS.get(item.status, item.status.value)


def _total_com_encargos(soma: Decimal, coluna: CotacaoFornecedor) -> Decimal:
    """Frete e desconto entram na conta; o acréscimo em % incide sobre os itens.

    Sem isso, o mapa mostraria o fornecedor mais barato por item como o melhor
    negócio — e o frete de R$ 800 apareceria só na nota.
    """
    acrescimo = soma * (Decimal(str(coluna.acrescimo_percentual or 0)) / 100)
    total = soma + acrescimo + Decimal(str(coluna.frete or 0)) - Decimal(str(coluna.desconto or 0))
    return max(total, Decimal("0")).quantize(CENTAVO, rounding=ROUND_HALF_UP)


def _total_pulverizado(itens: list[dict[str, Any]]) -> Decimal:
    """Quanto custaria comprar cada item de quem tem o menor preço. É o piso
    teórico — não inclui frete, porque aí seriam vários fretes."""
    total = Decimal("0")
    for linha in itens:
        precos = [Decimal(c["total"]) for c in linha["precos"].values()]
        if precos:
            total += min(precos)
    return total.quantize(CENTAVO, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Banco de preços
# ---------------------------------------------------------------------------
def historico_de_precos(s: Session, *, insumo_id: Optional[int] = None,
                        fornecedor_id: Optional[int] = None,
                        desde: Optional[date] = None,
                        busca: str = "") -> dict[str, Any]:
    """O que já foi cotado e comprado, com o resumo que interessa ao comprador:
    último preço, menor, maior e média."""
    registros = list(s.scalars(select(PrecoHistorico)).all())
    if insumo_id:
        registros = [r for r in registros if r.insumo_id == int(insumo_id)]
    if fornecedor_id:
        registros = [r for r in registros if r.fornecedor_id == int(fornecedor_id)]
    if desde:
        registros = [r for r in registros if r.data and r.data >= desde]

    saida = []
    for r in sorted(registros, key=lambda x: (x.data or date.min, x.id or 0), reverse=True):
        insumo = s.get(Insumo, r.insumo_id)
        forn = s.get(Fornecedor, r.fornecedor_id) if r.fornecedor_id else None
        linha = {
            "id": r.id, "insumo_id": r.insumo_id,
            "insumo": getattr(insumo, "descricao", ""),
            "especificacao": r.especificacao, "unidade": r.unidade,
            "preco_unitario": str(r.preco_unitario),
            "quantidade": str(r.quantidade) if r.quantidade is not None else None,
            "fornecedor": getattr(forn, "razao_social", None),
            "fornecedor_id": r.fornecedor_id,
            "tipo": r.tipo.value, "cotacao_id": r.cotacao_id,
            "data": r.data.isoformat() if r.data else None,
        }
        if busca and busca.lower() not in (
                f"{linha['insumo']} {linha['especificacao'] or ''} "
                f"{linha['fornecedor'] or ''}").lower():
            continue
        saida.append(linha)

    valores = [Decimal(x["preco_unitario"]) for x in saida]
    comprados = [Decimal(x["preco_unitario"]) for x in saida if x["tipo"] == "COMPRADO"]
    resumo = {
        "ocorrencias": len(saida),
        "ultimo": saida[0]["preco_unitario"] if saida else None,
        "menor": str(min(valores)) if valores else None,
        "maior": str(max(valores)) if valores else None,
        "media": str((sum(valores) / len(valores)).quantize(Decimal("0.0001")))
                 if valores else None,
        "ultimo_comprado": str(comprados[0]) if comprados else None,
    }
    return {"registros": saida, "resumo": resumo}


def sugerir_preco(s: Session, insumo_id: int) -> Optional[dict[str, Any]]:
    """O preço mais recente deste insumo, para herdar num mapa novo.

    Vem com a data e a cotação de origem porque o mapa TEM de mostrar que o
    preço é herdado: comprar com base num preço de três meses atrás sem
    perceber é exatamente o risco que isto cria.
    """
    dados = historico_de_precos(s, insumo_id=insumo_id)
    if not dados["registros"]:
        return None
    r = dados["registros"][0]
    return {"preco_unitario": r["preco_unitario"], "data": r["data"],
            "fornecedor": r["fornecedor"], "fornecedor_id": r["fornecedor_id"],
            "cotacao_id": r["cotacao_id"], "tipo": r["tipo"]}


# ============================================================================
# OS QUATRO RELATÓRIOS DO MAPA
#
# São os mesmos quatro que a planilha "Relatório Mapa de Cotação" tem nas abas
# R2 a R5, e que a equipe já usa para decidir a compra. Cada um responde uma
# pergunta diferente — e é por isso que são quatro, e não um:
#
#   POR ITEM (R2)         "de quem compro cada coisa pelo menor preço?"
#   POR FORNECEDOR (R3)   "então o que compro de cada um?" — é a lista de compra
#   UM FORNECEDOR SÓ (R4) "e se eu comprar tudo deste aqui?"
#   COMPARATIVO (R5)      "quem sai melhor no total, com frete e desconto?"
#
# TODOS SAEM DO MESMO MAPA (`montar_mapa`), de propósito. Se cada relatório
# refizesse a conta por conta própria, um dia dois deles dariam números
# diferentes para a mesma cotação — e aí nenhum serviria para decidir nada.
#
# O QUE OS NÚMEROS SIGNIFICAM, para não enganar quem lê:
#   - "Melhor preço" é o menor PREÇO UNITÁRIO da linha. Comprar cada item de
#     quem tem o menor preço (o "pulverizado") ignora que cada fornecedor cobra
#     o SEU frete — por isso o comparativo existe e mostra o total com encargos.
#   - O total de um fornecedor já inclui frete, desconto e acréscimo.
# ============================================================================
TIPOS_DE_RELATORIO = {
    "POR_ITEM": "Resumo da cotação por item",
    "POR_FORNECEDOR": "Resumo da cotação por fornecedor",
    "UM_FORNECEDOR": "Resumo da cotação — comprando tudo de um fornecedor",
    "COMPARATIVO": "Resumo da cotação — comparativo de fornecedores",
}

COLUNAS_DO_ITEM = [
    {"rotulo": "Item"}, {"rotulo": "Descrição"},
    {"rotulo": "Quant.", "tipo": "quantidade"}, {"rotulo": "Unid."},
    {"rotulo": "Obra"}, {"rotulo": "Situação"}, {"rotulo": "Fornecedor"},
    {"rotulo": "Melhor preço", "tipo": "dinheiro"},
    {"rotulo": "Total", "tipo": "dinheiro"},
]


def _linha_do_item(item: dict[str, Any], coluna: Optional[dict[str, Any]],
                   celula: Optional[dict[str, Any]]) -> list[Any]:
    return [
        item["numero"],
        (f"{item['insumo']} — {item['especificacao']}" if item.get("especificacao")
         else item["insumo"]),
        _quantidade_br(item["quantidade"]), item["unidade"],
        item.get("obra") or "", item.get("status_rotulo") or "",
        (coluna or {}).get("fornecedor") or "não cotado",
        _dinheiro_br((celula or {}).get("preco_unitario")),
        _dinheiro_br((celula or {}).get("total")),
    ]


def _cabecalho(mapa: dict[str, Any], tipo: str) -> dict[str, Any]:
    """O que identifica o relatório: qual cotação, quais obras e quais itens.

    A planilha do dono imprime isso no alto de cada aba (Obras, Insumos, Nº
    SS) — e faz sentido: sem os filtros, não se sabe de onde o número veio.
    """
    obras = sorted({i["obra"] for i in mapa["itens"] if i.get("obra")})
    return {
        "tipo": tipo,
        "titulo": TIPOS_DE_RELATORIO[tipo],
        "cotacao": mapa["numero"],
        "subtitulo": f"{mapa['numero']} · {mapa['titulo']}",
        "filtros": [
            f"Obras: {', '.join(obras) or '—'}",
            f"Itens: {len(mapa['itens'])}",
            f"Fornecedores no mapa: {len(mapa['fornecedores'])}",
        ],
    }


def relatorio(s: Session, cotacao_id: int, tipo: str,
              cotacao_fornecedor_id: Optional[int] = None) -> dict[str, Any]:
    """Um dos quatro relatórios do mapa, pronto para a tela e para o arquivo."""
    tipo = (tipo or "POR_ITEM").upper()
    if tipo not in TIPOS_DE_RELATORIO:
        raise ErroValidacao(f"Relatório desconhecido: {tipo}")

    mapa = montar_mapa(s, cotacao_id)
    por_coluna = {c["id"]: c for c in mapa["fornecedores"]}
    dados = _cabecalho(mapa, tipo)

    if tipo == "COMPARATIVO":
        dados.update(_comparativo(mapa))
    elif tipo == "UM_FORNECEDOR":
        dados.update(_de_um_fornecedor(mapa, por_coluna, cotacao_fornecedor_id))
    else:
        dados.update(_por_menor_preco(mapa, por_coluna,
                                      agrupar=(tipo == "POR_FORNECEDOR")))
    return dados


def _por_menor_preco(mapa: dict[str, Any], por_coluna: dict[int, dict],
                     *, agrupar: bool) -> dict[str, Any]:
    """R2 e R3: cada item pelo MENOR preço. A diferença entre os dois é só a
    ordem — por item, ou agrupado por fornecedor (que é a lista de compra)."""
    linhas, total = [], Decimal("0")
    sem_preco = 0
    for item in mapa["itens"]:
        melhor_id = item.get("menor_preco_de")
        coluna = por_coluna.get(melhor_id) if melhor_id else None
        celula = item["precos"].get(melhor_id) if melhor_id else None
        if celula is None:
            sem_preco += 1
        else:
            total += Decimal(celula["total"])
        linhas.append((coluna, _linha_do_item(item, coluna, celula)))

    if agrupar:
        # ordena por fornecedor, mantendo a ordem do mapa dentro de cada um;
        # quem não foi cotado por ninguém fica no fim, para não se perder
        linhas.sort(key=lambda x: ((x[0] or {}).get("fornecedor") or "zzz").lower())

    # A soma sai do MAPA, não da linha já formatada: "4.155,68" com ponto de
    # milhar não é número, e somar texto formatado é como se erra por 1.000.
    por_fornecedor: dict[str, dict[str, Any]] = {}
    for item in mapa["itens"]:
        melhor_id = item.get("menor_preco_de")
        coluna = por_coluna.get(melhor_id) if melhor_id else None
        celula = item["precos"].get(melhor_id) if melhor_id else None
        nome = (coluna or {}).get("fornecedor") or "não cotado"
        agr = por_fornecedor.setdefault(nome, {"fornecedor": nome, "itens": 0,
                                               "total": Decimal("0")})
        agr["itens"] += 1
        if celula is not None:
            agr["total"] += Decimal(celula["total"])

    return {
        "colunas": COLUNAS_DO_ITEM,
        "linhas": [l for _c, l in linhas],
        "total": str(total.quantize(CENTAVO, rounding=ROUND_HALF_UP)),
        "total_br": _dinheiro_br(total),
        "sem_preco": sem_preco,
        "resumo_por_fornecedor": [
            {"fornecedor": v["fornecedor"], "itens": v["itens"],
             "total": _dinheiro_br(v["total"])}
            for v in sorted(por_fornecedor.values(),
                            key=lambda x: x["fornecedor"].lower())],
        "aviso": ("Comprando cada item de quem tem o menor preço. O frete de "
                  "cada fornecedor NÃO está somado aqui — compare com o "
                  "relatório de fornecedores antes de decidir."),
    }


def _de_um_fornecedor(mapa: dict[str, Any], por_coluna: dict[int, dict],
                      cotacao_fornecedor_id: Optional[int]) -> dict[str, Any]:
    """R4: todos os itens de UM fornecedor. Sem escolha, usa o que sai melhor
    no total entre os que cotaram tudo — que é a pergunta "e se eu comprar
    tudo de um só?"."""
    escolhido = cotacao_fornecedor_id or mapa.get("melhor_fornecedor_unico")
    if not escolhido and mapa["fornecedores"]:
        # ninguém cotou tudo: fica com quem cotou mais itens
        escolhido = max(mapa["fornecedores"],
                        key=lambda c: (c["itens_cotados"], -Decimal(c["total"])))["id"]
    coluna = por_coluna.get(escolhido)
    if coluna is None:
        raise ErroValidacao("Não há fornecedor com preço neste mapa.")

    linhas, soma = [], Decimal("0")
    faltando = 0
    for item in mapa["itens"]:
        celula = item["precos"].get(coluna["id"])
        if celula is None:
            faltando += 1
        else:
            soma += Decimal(celula["total"])
        linhas.append(_linha_do_item(item, coluna if celula else None, celula))

    return {
        "colunas": COLUNAS_DO_ITEM,
        "linhas": linhas,
        "fornecedor": coluna["fornecedor"],
        "cotacao_fornecedor_id": coluna["id"],
        "condicao": coluna.get("condicao"),
        "frete": coluna.get("frete"),
        "desconto": coluna.get("desconto"),
        "soma_itens": _dinheiro_br(soma),
        "total": _dinheiro_br(coluna["total"]),
        "itens_sem_preco": faltando,
        "escolhas": [{"id": c["id"], "fornecedor": c["fornecedor"],
                      "itens_cotados": c["itens_cotados"],
                      "itens_no_mapa": c["itens_no_mapa"],
                      "total": _dinheiro_br(c["total"])}
                     for c in mapa["fornecedores"]],
        "aviso": (f"{faltando} item(ns) sem preço deste fornecedor — comprando "
                  f"tudo dele, esses ficam de fora." if faltando else
                  "Este fornecedor cotou todos os itens do mapa."),
    }


def _comparativo(mapa: dict[str, Any]) -> dict[str, Any]:
    """R5: uma linha por fornecedor. É a conta que decide de verdade, porque
    inclui frete, desconto e acréscimo — comparar só o unitário engana."""
    colunas = [
        {"rotulo": "#"}, {"rotulo": "Fornecedor"}, {"rotulo": "Condição"},
        {"rotulo": "Entrega"},
        {"rotulo": "Itens cotados", "tipo": "inteiro"},
        {"rotulo": "Soma dos itens", "tipo": "dinheiro"},
        {"rotulo": "Frete", "tipo": "dinheiro"},
        {"rotulo": "Desconto", "tipo": "dinheiro"},
        {"rotulo": "Total", "tipo": "dinheiro"},
    ]
    linhas = []
    for n, c in enumerate(sorted(mapa["fornecedores"],
                                 key=lambda x: Decimal(x["total"]) or 0), start=1):
        linhas.append([
            n, c["fornecedor"], c.get("condicao") or "—",
            ("coleta" if c.get("entrega") == "COLETA"
             else ("entrega" if c.get("entrega") else "—")),
            f"{c['itens_cotados']}/{c['itens_no_mapa']}",
            _dinheiro_br(c["soma_itens"]), _dinheiro_br(c["frete"]),
            _dinheiro_br(c["desconto"]), _dinheiro_br(c["total"]),
        ])
    return {
        "colunas": colunas,
        "linhas": linhas,
        "melhor_fornecedor_unico": mapa.get("melhor_fornecedor_unico"),
        "total_pulverizado": mapa.get("total_pulverizado"),
        "total_pulverizado_br": _dinheiro_br(mapa.get("total_pulverizado")),
        "aviso": ("O total já inclui frete, desconto e acréscimo. Quem cotou "
                  "menos itens aparece com total menor por isso, e não por ser "
                  "mais barato — olhe a coluna 'itens cotados'."),
    }
