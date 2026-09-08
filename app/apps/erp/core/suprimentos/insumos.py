# ============================================================================
# ERP — core/suprimentos/insumos.py
# Quem precisa PEDE o cadastro do insumo; quem responde por suprimentos decide.
#
# Por que não deixar todo mundo cadastrar: em um mês a base tem "Cimento CP-II",
# "cimento cp 2" e "CIMENTO CPII 50KG" como três insumos diferentes, cada um com
# uma conta do plano, e os relatórios param de significar coisa alguma. O
# procedimento que já existe no papel vira tela aqui.
# ============================================================================
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Insumo, InsumoSolicitacao, StatusSolicitacaoInsumo, Usuario,
)

logger = logging.getLogger(__name__)


def _agora() -> datetime:
    return datetime.now(timezone.utc)


def _normalizar(texto: str) -> str:
    return " ".join((texto or "").split())


def solicitar(s: Session, dados: dict[str, Any], usuario: Usuario) -> InsumoSolicitacao:
    """Pedido de cadastro. Quem pede descreve; quem decide dá o nome final."""
    descricao = _normalizar(dados.get("descricao") or "")
    if len(descricao) < 3:
        raise ErroValidacao("Descreva o insumo (mínimo 3 letras).")

    ja_existe = _insumo_parecido(s, descricao)
    if ja_existe is not None:
        raise ErroValidacao(
            f"Já existe o insumo {ja_existe.codigo} · {ja_existe.descricao}. "
            f"Use esse, ou peça com outro nome se for realmente outro material.")

    pendente = [p for p in s.scalars(select(InsumoSolicitacao).where(
        InsumoSolicitacao.status == StatusSolicitacaoInsumo.PENDENTE)).all()
        if p.status is StatusSolicitacaoInsumo.PENDENTE
        and _normalizar(p.descricao).lower() == descricao.lower()]
    if pendente:
        raise ErroValidacao("Esse insumo já foi pedido e está aguardando decisão.")

    pedido = InsumoSolicitacao(
        descricao=descricao,
        justificativa=_normalizar(dados.get("justificativa") or "") or None,
        unidade=(dados.get("unidade") or "").strip().upper() or None,
        obra_id=int(dados["obra_id"]) if dados.get("obra_id") else None,
        solicitante_id=usuario.id,
        status=StatusSolicitacaoInsumo.PENDENTE)
    s.add(pedido)
    s.flush()
    registrar_evento(s, "insumo_solicitacao", pedido.id, "SOLICITADO",
                     {"descricao": descricao}, usuario.id)
    return pedido


def _insumo_parecido(s: Session, descricao: str) -> Optional[Insumo]:
    """Casa ignorando caixa e espaço repetido — o suficiente para pegar o
    pedido de algo que já está cadastrado com outra digitação."""
    alvo = _normalizar(descricao).lower()
    for i in s.scalars(select(Insumo)).all():
        if _normalizar(getattr(i, "descricao", "")).lower() == alvo:
            return i
    return None


def _obter_pendente(s: Session, solicitacao_id: int) -> InsumoSolicitacao:
    pedido = s.get(InsumoSolicitacao, solicitacao_id, with_for_update=True,
                   populate_existing=True)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido de cadastro não encontrado.")
    if pedido.status is not StatusSolicitacaoInsumo.PENDENTE:
        raise ErroValidacao(
            f"Este pedido já foi resolvido ({pedido.status.value.lower()}).")
    return pedido


def cadastrar(s: Session, solicitacao_id: int, dados: dict[str, Any],
              usuario: Usuario) -> Insumo:
    """Efetiva o cadastro: o nome final, a categoria e a conta do plano são de
    quem decide, não de quem pediu. É isso que mantém a base limpa."""
    pedido = _obter_pendente(s, solicitacao_id)

    descricao = _normalizar(dados.get("descricao") or pedido.descricao)
    if len(descricao) < 3:
        raise ErroValidacao("Dê um nome ao insumo (mínimo 3 letras).")
    existente = _insumo_parecido(s, descricao)
    if existente is not None:
        raise ErroValidacao(
            f"Já existe o insumo {existente.codigo} · {existente.descricao}.")
    if not dados.get("categoria_insumo_id"):
        raise ErroValidacao("Escolha a categoria de insumo.")
    if not dados.get("categoria_id"):
        raise ErroValidacao(
            "Escolha a conta do plano financeiro. Sem ela, o pedido de compra "
            "não vira previsão de pagamento apropriada.")

    insumo = Insumo(
        codigo=proximo_codigo(s),
        descricao=descricao,
        categoria_insumo_id=int(dados["categoria_insumo_id"]),
        categoria_id=int(dados["categoria_id"]),
        unidade=(dados.get("unidade") or pedido.unidade or "").strip().upper() or None)
    s.add(insumo)
    s.flush()

    pedido.status = StatusSolicitacaoInsumo.CADASTRADO
    pedido.insumo_id = insumo.id
    pedido.decidido_por = usuario.id
    pedido.decidido_em = _agora()
    registrar_evento(s, "insumo_solicitacao", pedido.id, "CADASTRADO",
                     {"insumo_id": insumo.id, "codigo": insumo.codigo,
                      "descricao": descricao}, usuario.id)
    avisar_solicitante(s, pedido)
    return insumo


def recusar(s: Session, solicitacao_id: int, motivo: str,
            usuario: Usuario) -> InsumoSolicitacao:
    """Recusa COM motivo. Recusa sem motivo faz a pessoa pedir de novo amanhã."""
    pedido = _obter_pendente(s, solicitacao_id)
    motivo = _normalizar(motivo)
    if len(motivo) < 5:
        raise ErroValidacao("Escreva o motivo da recusa — quem pediu precisa saber.")
    pedido.status = StatusSolicitacaoInsumo.RECUSADO
    pedido.motivo = motivo
    pedido.decidido_por = usuario.id
    pedido.decidido_em = _agora()
    registrar_evento(s, "insumo_solicitacao", pedido.id, "RECUSADO",
                     {"motivo": motivo}, usuario.id)
    avisar_solicitante(s, pedido)
    return pedido


def proximo_codigo(s: Session) -> str:
    """Continua a numeração INS-0001 de onde parou."""
    numeros = []
    for insumo in s.scalars(select(Insumo)).all():
        codigo = getattr(insumo, "codigo", "") or ""
        sufixo = codigo.split("-", 1)[1] if codigo.startswith("INS-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return f"INS-{(max(numeros) + 1) if numeros else 1:04d}"


def avisar_solicitante(s: Session, pedido: InsumoSolicitacao) -> bool:
    """Telegram para quem pediu. O aviso é o que fecha o ciclo: sem ele, a
    pessoa fica perguntando no corredor se já saiu.

    Falha de envio NÃO derruba a decisão — o cadastro já está feito, e o
    `avisado_em` em branco mostra quem ficou sem aviso.
    """
    quem = s.get(Usuario, pedido.solicitante_id)
    if quem is None or not (getattr(quem, "telefone", None) or getattr(quem, "cpf", None)):
        return False
    if pedido.status is StatusSolicitacaoInsumo.CADASTRADO:
        texto = (f"✅ O insumo *{pedido.descricao}* que você pediu foi cadastrado. "
                 f"Já pode usar na solicitação de suprimentos.")
    else:
        texto = (f"❌ O cadastro do insumo *{pedido.descricao}* não foi feito.\n"
                 f"Motivo: {pedido.motivo}")
    try:
        from app.apps.notificador import enviar_telegram
        r = enviar_telegram(telefone=quem.telefone, cpf=quem.cpf, mensagem=texto)
    except Exception as e:
        logger.warning("ERP/suprimentos: aviso de cadastro não saiu (%s)", e)
        return False
    if r and r.get("ok"):
        pedido.avisado_em = _agora()
        return True
    return False


def listar(s: Session, usuario: Usuario, apenas_pendentes: bool = False) -> list[dict[str, Any]]:
    """Os pedidos, com quem pediu e o que foi decidido."""
    pedidos = s.scalars(select(InsumoSolicitacao)).all()
    if apenas_pendentes:
        pedidos = [p for p in pedidos if p.status is StatusSolicitacaoInsumo.PENDENTE]
    saida = []
    for p in sorted(pedidos, key=lambda x: (x.id or 0), reverse=True):
        quem = s.get(Usuario, p.solicitante_id)
        saida.append({
            "id": p.id, "descricao": p.descricao, "justificativa": p.justificativa,
            "unidade": p.unidade, "status": p.status.value,
            "solicitante": getattr(quem, "nome", ""), "obra_id": p.obra_id,
            "insumo_id": p.insumo_id, "motivo": p.motivo,
            "avisado": p.avisado_em is not None,
            "criado_em": p.criado_em.isoformat() if p.criado_em else None,
        })
    return saida
