# ============================================================================
# ERP — core/suprimentos/solicitacao.py
# O pedido de material: cabeçalho, itens e acompanhamento.
#
# Duas coisas mudam em relação à planilha de hoje, e as duas foram pedidas:
#   - a OBRA é do item, não da solicitação: uma solicitação pode pedir material
#     para obras diferentes;
#   - o acompanhamento é POR ITEM: os itens de um mesmo pedido seguem caminhos
#     diferentes (um vai a cotação, outro sai do almoxarifado, outro é
#     cancelado).
#
# Escopo: quem enxerga o quê passa pelas MESMAS regras do financeiro
# (core/auth/permissoes). Um administrativo de obra vê o que ele mesmo pediu —
# ou tudo das obras designadas, se assim estiver configurado no cadastro dele.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.auth.permissoes import obras_do_usuario
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Insumo, Obra, PrioridadeSolicitacao, StatusItemSuprimento,
    SuprimentoItem, SuprimentoSolicitacao, UnidadeCompra, Usuario,
)

logger = logging.getLogger(__name__)

# De onde para onde um item pode ir. Não é enfeite: sem isso, um item volta de
# RECEBIDO para SOLICITACAO num clique errado e o histórico deixa de fazer
# sentido. CANCELADO e SUSPENSO saem de qualquer lugar, porque a realidade
# cancela a qualquer momento.
FLUXO: dict[StatusItemSuprimento, tuple[StatusItemSuprimento, ...]] = {
    StatusItemSuprimento.SOLICITACAO: (
        StatusItemSuprimento.SALA_TECNICA, StatusItemSuprimento.COTACAO,
        StatusItemSuprimento.ALMOXARIFADO),
    StatusItemSuprimento.SALA_TECNICA: (
        StatusItemSuprimento.COTACAO, StatusItemSuprimento.ALMOXARIFADO,
        StatusItemSuprimento.SOLICITACAO),
    StatusItemSuprimento.COTACAO: (
        StatusItemSuprimento.ANALISE_PROPOSTAS, StatusItemSuprimento.SOLICITACAO),
    StatusItemSuprimento.ANALISE_PROPOSTAS: (
        StatusItemSuprimento.AUTORIZACAO, StatusItemSuprimento.COTACAO),
    StatusItemSuprimento.AUTORIZACAO: (
        StatusItemSuprimento.PEDIDO_EMITIDO, StatusItemSuprimento.ANALISE_PROPOSTAS),
    StatusItemSuprimento.PEDIDO_EMITIDO: (
        StatusItemSuprimento.AGUARDANDO_COLETA, StatusItemSuprimento.AGUARDANDO_ENTREGA),
    StatusItemSuprimento.ALMOXARIFADO: (StatusItemSuprimento.ENTREGUE,),
    StatusItemSuprimento.AGUARDANDO_COLETA: (
        StatusItemSuprimento.EM_TRANSITO, StatusItemSuprimento.ENTREGUE),
    StatusItemSuprimento.AGUARDANDO_ENTREGA: (
        StatusItemSuprimento.EM_TRANSITO, StatusItemSuprimento.ENTREGUE),
    StatusItemSuprimento.EM_TRANSITO: (StatusItemSuprimento.ENTREGUE,),
    StatusItemSuprimento.ENTREGUE: (
        StatusItemSuprimento.RECEBIDO, StatusItemSuprimento.PENDENCIA),
    StatusItemSuprimento.RECEBIDO: (StatusItemSuprimento.PENDENCIA,),
    StatusItemSuprimento.PENDENCIA: (
        StatusItemSuprimento.COTACAO, StatusItemSuprimento.RECEBIDO),
    StatusItemSuprimento.CANCELADO: (),
    StatusItemSuprimento.SUSPENSO: (StatusItemSuprimento.SOLICITACAO,),
}

DE_QUALQUER_LUGAR = (StatusItemSuprimento.CANCELADO, StatusItemSuprimento.SUSPENSO)

ROTULOS_STATUS = {
    StatusItemSuprimento.SOLICITACAO: "Solicitação",
    StatusItemSuprimento.SALA_TECNICA: "Sala técnica",
    StatusItemSuprimento.COTACAO: "Cotação",
    StatusItemSuprimento.ANALISE_PROPOSTAS: "Análise de propostas",
    StatusItemSuprimento.AUTORIZACAO: "Autorização",
    StatusItemSuprimento.PEDIDO_EMITIDO: "Pedido emitido",
    StatusItemSuprimento.ALMOXARIFADO: "Almoxarifado",
    StatusItemSuprimento.AGUARDANDO_COLETA: "Aguardando coleta",
    StatusItemSuprimento.AGUARDANDO_ENTREGA: "Aguardando entrega",
    StatusItemSuprimento.EM_TRANSITO: "Em trânsito",
    StatusItemSuprimento.ENTREGUE: "Entregue",
    StatusItemSuprimento.RECEBIDO: "Recebido",
    StatusItemSuprimento.PENDENCIA: "Pendência",
    StatusItemSuprimento.CANCELADO: "Cancelado",
    StatusItemSuprimento.SUSPENSO: "Suspenso",
}


def _decimal(valor: Any, campo: str) -> Decimal:
    try:
        d = Decimal(str(valor).replace(".", "").replace(",", ".")
                    if isinstance(valor, str) and "," in str(valor) else str(valor))
    except (InvalidOperation, ValueError, TypeError):
        raise ErroValidacao(f"{campo} tem de ser um número.")
    return d


def proximo_numero(s: Session) -> str:
    """SS-0001, SS-0002… Numeração própria, separada das SPs do financeiro,
    para ninguém confundir pedido de material com solicitação de pagamento."""
    numeros = []
    for sol in s.scalars(select(SuprimentoSolicitacao)).all():
        numero = getattr(sol, "numero", "") or ""
        sufixo = numero.split("-", 1)[1] if numero.startswith("SS-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return f"SS-{(max(numeros) + 1) if numeros else 1:04d}"


def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> SuprimentoSolicitacao:
    """Cria a solicitação com os itens. Sem item, não é solicitação nenhuma."""
    titulo = " ".join((dados.get("titulo") or "").split())
    if len(titulo) < 3:
        raise ErroValidacao(
            "Dê um título ao pedido (ex.: 'armadura da fundação'). É por ele "
            "que você vai encontrar isto depois.")

    itens = dados.get("itens") or []
    if not itens:
        raise ErroValidacao("Inclua pelo menos um item.")

    previsao = None
    if dados.get("previsao_entrega"):
        try:
            previsao = date.fromisoformat(str(dados["previsao_entrega"])[:10])
        except ValueError:
            raise ErroValidacao("Data de previsão inválida.")

    prioridade = (dados.get("prioridade") or "NORMAL").strip().upper()
    if prioridade not in {p.value for p in PrioridadeSolicitacao}:
        raise ErroValidacao("Prioridade inválida.")

    sol = SuprimentoSolicitacao(
        numero=proximo_numero(s), titulo=titulo, previsao_entrega=previsao,
        prioridade=PrioridadeSolicitacao(prioridade),
        observacoes=(dados.get("observacoes") or "").strip() or None,
        solicitante_id=usuario.id)
    s.add(sol)
    s.flush()

    for i, bruto in enumerate(itens, start=1):
        s.add(_montar_item(s, sol, i, bruto))
    s.flush()

    registrar_evento(s, "suprimento_solicitacao", sol.id, "CRIADA",
                     {"numero": sol.numero, "titulo": titulo,
                      "itens": len(itens), "prioridade": prioridade}, usuario.id)
    return sol


def _montar_item(s: Session, sol: SuprimentoSolicitacao, numero: int,
                 bruto: dict[str, Any]) -> SuprimentoItem:
    insumo = s.get(Insumo, int(bruto["insumo_id"])) if bruto.get("insumo_id") else None
    if insumo is None:
        raise ErroValidacao(f"Item {numero}: escolha um insumo do cadastro.")
    obra = s.get(Obra, int(bruto["obra_id"])) if bruto.get("obra_id") else None
    if obra is None:
        raise ErroValidacao(f"Item {numero}: escolha a obra que vai receber.")

    quantidade = _decimal(bruto.get("quantidade"), f"Item {numero}: quantidade")
    if quantidade <= 0:
        raise ErroValidacao(f"Item {numero}: quantidade tem de ser maior que zero.")

    unidade = (bruto.get("unidade") or getattr(insumo, "unidade", "") or "").strip().upper()
    if not unidade:
        raise ErroValidacao(f"Item {numero}: informe a unidade.")
    if s.get(UnidadeCompra, unidade) is None:
        raise ErroValidacao(f"Item {numero}: unidade {unidade} não existe no cadastro.")

    return SuprimentoItem(
        solicitacao_id=sol.id, numero=numero, insumo_id=insumo.id,
        especificacao=(bruto.get("especificacao") or "").strip() or None,
        quantidade=quantidade, unidade=unidade, obra_id=obra.id,
        observacoes=(bruto.get("observacoes") or "").strip() or None,
        status=StatusItemSuprimento.SOLICITACAO)


def mudar_situacao(s: Session, item_id: int, novo: str, usuario: Usuario,
                   observacao: str = "") -> SuprimentoItem:
    """Move um item pelo fluxo, recusando salto que não faz sentido.

    Sem essa recusa, um clique errado devolve um item de RECEBIDO para
    SOLICITAÇÃO e o histórico deixa de significar coisa alguma.
    """
    item = s.get(SuprimentoItem, item_id, with_for_update=True, populate_existing=True)
    if item is None:
        raise ErroNaoEncontrado("Item não encontrado.")
    try:
        destino = StatusItemSuprimento(novo.strip().upper())
    except (ValueError, AttributeError):
        raise ErroValidacao(f"Situação desconhecida: {novo!r}")

    atual = item.status
    if destino is atual:
        return item
    permitidos = FLUXO.get(atual, ()) + DE_QUALQUER_LUGAR
    if destino not in permitidos:
        raise ErroValidacao(
            f"Não dá para ir de {ROTULOS_STATUS[atual]} para "
            f"{ROTULOS_STATUS[destino]}.")

    item.status = destino
    if observacao:
        item.observacoes = observacao.strip() or item.observacoes
    registrar_evento(s, "suprimento_item", item.id, "SITUACAO",
                     {"de": atual.value, "para": destino.value,
                      "observacao": observacao or None}, usuario.id)
    return item


# ---------------------------------------------------------------------------
# Leitura, com o mesmo escopo do financeiro
# ---------------------------------------------------------------------------
def _itens_visiveis(s: Session, usuario: Usuario) -> list[SuprimentoItem]:
    """Filtra pelo alcance da pessoa: tudo, as obras designadas, ou só o que
    ela mesma pediu. Mesma regra do financeiro — não se inventa escopo novo."""
    itens = list(s.scalars(select(SuprimentoItem)).all())
    obras = obras_do_usuario(s, usuario)
    if obras is None and _ve_tudo(s, usuario):
        return itens
    if obras is not None:
        permitidas = set(obras)
        return [i for i in itens
                if i.obra_id in permitidas or _pediu(s, i, usuario)]
    return [i for i in itens if _pediu(s, i, usuario)]


def _ve_tudo(s: Session, usuario: Usuario) -> bool:
    from app.apps.erp.core.auth.permissoes import VE_TUDO
    return usuario is not None and usuario.perfil in VE_TUDO


def _pediu(s: Session, item: SuprimentoItem, usuario: Usuario) -> bool:
    sol = s.get(SuprimentoSolicitacao, item.solicitacao_id)
    return sol is not None and sol.solicitante_id == usuario.id


def listar_itens(s: Session, usuario: Usuario, *, status: Optional[str] = None,
                 obra_id: Optional[int] = None,
                 busca: str = "") -> list[dict[str, Any]]:
    """Os itens que esta pessoa pode ver, com filtro de situação, obra e busca.

    A busca cobre o título da solicitação, o insumo e a especificação — que é
    como as pessoas realmente procuram ("aquele pedido da armadura").
    """
    itens = _itens_visiveis(s, usuario)
    if status:
        alvo = status.strip().upper()
        itens = [i for i in itens if i.status.value == alvo]
    if obra_id:
        itens = [i for i in itens if i.obra_id == int(obra_id)]

    saida = []
    for i in sorted(itens, key=lambda x: (x.solicitacao_id or 0, x.numero or 0)):
        sol = s.get(SuprimentoSolicitacao, i.solicitacao_id)
        insumo = s.get(Insumo, i.insumo_id)
        obra = s.get(Obra, i.obra_id)
        linha = {
            "id": i.id, "numero": i.numero,
            "solicitacao_id": i.solicitacao_id,
            "solicitacao": getattr(sol, "numero", ""),
            "titulo": getattr(sol, "titulo", ""),
            "prioridade": getattr(getattr(sol, "prioridade", None), "value", "NORMAL"),
            "previsao_entrega": (sol.previsao_entrega.isoformat()
                                 if sol is not None and sol.previsao_entrega else None),
            "insumo": getattr(insumo, "descricao", ""),
            "especificacao": i.especificacao,
            "quantidade": str(i.quantidade), "unidade": i.unidade,
            "quantidade_recebida": str(i.quantidade_recebida or 0),
            "saldo": str(i.saldo),
            "obra": getattr(obra, "codigo", ""), "obra_id": i.obra_id,
            "status": i.status.value, "status_rotulo": ROTULOS_STATUS[i.status],
            "proximas": [x.value for x in FLUXO.get(i.status, ()) + DE_QUALQUER_LUGAR],
        }
        if busca:
            alvo = busca.lower()
            campos = " ".join(str(linha[c] or "") for c in
                              ("titulo", "insumo", "especificacao", "solicitacao"))
            if alvo not in campos.lower():
                continue
        saida.append(linha)
    return saida


def obter(s: Session, solicitacao_id: int, usuario: Usuario) -> dict[str, Any]:
    """Uma solicitação com seus itens. Fora do alcance responde NÃO ENCONTRADO,
    nunca 'sem permissão' — dizer 'sem permissão' confirmaria que ela existe."""
    sol = s.get(SuprimentoSolicitacao, solicitacao_id)
    if sol is None:
        raise ErroNaoEncontrado("Solicitação não encontrada.")
    visiveis = {i.id for i in _itens_visiveis(s, usuario)}
    itens = [i for i in s.scalars(select(SuprimentoItem)).all()
             if i.solicitacao_id == solicitacao_id]
    if not any(i.id in visiveis for i in itens):
        raise ErroNaoEncontrado("Solicitação não encontrada.")
    quem = s.get(Usuario, sol.solicitante_id)
    return {
        "id": sol.id, "numero": sol.numero, "titulo": sol.titulo,
        "prioridade": sol.prioridade.value,
        "previsao_entrega": sol.previsao_entrega.isoformat() if sol.previsao_entrega else None,
        "observacoes": sol.observacoes,
        "solicitante": getattr(quem, "nome", ""),
        "criado_em": sol.criado_em.isoformat() if sol.criado_em else None,
        "itens": [x for x in listar_itens(s, usuario)
                  if x["solicitacao_id"] == solicitacao_id],
    }
