# ============================================================================
# ERP — core/titulos/ajustes.py
# Correções do dia a dia sem o suplício do Omie.
#
# Duas operações que lá exigem desmontar tudo (desconciliar → estornar baixa →
# corrigir → baixar de novo → conciliar de novo) e aqui são diretas:
#
#   1. RECLASSIFICAR — trocar a conta do plano e/ou a obra do rateio, MESMO com
#      o título pago e conciliado. Isso não é "editar o passado": o dinheiro
#      saiu do mesmo jeito, para o mesmo credor, no mesmo valor; muda apenas a
#      GAVETA onde a despesa está guardada. Baixa e conciliação seguem
#      intocadas, e a mudança fica na trilha com o antes e o depois.
#
#   2. DESFAZER EM CADEIA — um comando que remove a conciliação, estorna a
#      baixa e devolve o título ao estado anterior, na ordem certa, dentro de
#      uma transação. O usuário diz o que quer; o sistema cuida da ordem.
#
# O que continua protegido: valor, credor, data e documento de um título pago
# não se alteram por aqui — isso é estorno, com registro próprio.
# ============================================================================
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Categoria, Obra, PerfilUsuario, Usuario
from app.apps.erp.db.models.financeiro import (
    Conciliacao, Pagamento, Parcela, Rateio, StatusParcela, StatusTitulo, Titulo,
)

logger = logging.getLogger(__name__)


def reclassificar(s: Session, titulo_id: int, *, categoria_id: Optional[int] = None,
                  rateios: Optional[list[dict[str, Any]]] = None,
                  motivo: str = "", usuario: Usuario) -> dict[str, Any]:
    """Troca conta do plano e/ou obra do rateio em qualquer situação do título,
    inclusive pago e conciliado. Não mexe em baixa nem em conciliação."""
    t = s.get(Titulo, titulo_id, options=[
        selectinload(Titulo.rateios).selectinload(Rateio.obra),
        selectinload(Titulo.categoria)])
    if t is None:
        raise ErroValidacao("Título não encontrado.")
    if t.status in (StatusTitulo.CANCELADO, StatusTitulo.ESTORNADO):
        raise ErroValidacao(f"Título {t.status.value} não é reclassificado — "
                            f"a correção vai no lançamento que o substituiu.")
    motivo = (motivo or "").strip()
    if len(motivo) < 5:
        raise ErroValidacao("Informe o motivo da reclassificação (fica na trilha).")

    antes = {
        "categoria": f"{t.categoria.codigo} · {t.categoria.descricao}",
        "rateios": [{"obra": r.obra.codigo, "valor": str(r.valor)} for r in t.rateios],
    }
    mudou: list[str] = []

    # ---- conta do plano
    if categoria_id and int(categoria_id) != t.categoria_id:
        nova = s.get(Categoria, int(categoria_id))
        if nova is None or not nova.ativo:
            raise ErroValidacao("Conta de destino inexistente ou aposentada.")
        permitidos = [x.value if hasattr(x, "value") else str(x)
                      for x in (nova.tipos_permitidos or [])]
        if permitidos and t.tipo.value not in permitidos:
            raise ErroValidacao(
                f"A conta {nova.codigo} não aceita este tipo de documento. "
                f"Se a classificação certa é essa, ajuste os tipos aceitos da conta "
                f"em Configurações.")
        t.categoria_id = nova.id
        mudou.append(f"conta {antes['categoria']} → {nova.codigo} · {nova.descricao}")

    # ---- rateio por obra
    if rateios:
        total = Decimal("0.00")
        novos = []
        for i, r in enumerate(rateios, start=1):
            obra = s.get(Obra, int(r.get("obra_id") or 0))
            if obra is None:
                raise ErroValidacao(f"Rateio {i}: obra inexistente.")
            try:
                valor = Decimal(str(r.get("valor")).replace(",", ".")).quantize(Decimal("0.01"))
            except Exception:
                raise ErroValidacao(f"Rateio {i}: valor inválido.")
            if valor <= 0:
                raise ErroValidacao(f"Rateio {i}: valor deve ser maior que zero.")
            total += valor
            novos.append((obra, valor))
        if abs(total - Decimal(t.valor_liquido)) > Decimal("0.01"):
            raise ErroValidacao(
                f"A soma do rateio (R$ {total}) tem que fechar com o líquido do título "
                f"(R$ {t.valor_liquido}). Reclassificar não muda valor.")
        for r in list(t.rateios):
            s.delete(r)
        s.flush()
        for obra, valor in novos:
            s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=valor,
                         percentual=(valor / Decimal(t.valor_liquido) * 100
                                     ).quantize(Decimal("0.0001"))))
        mudou.append("rateio " + " / ".join(f"{o.codigo} R$ {v}" for o, v in novos))

    if not mudou:
        raise ErroValidacao("Nada a alterar — informe a nova conta ou o novo rateio.")

    s.flush()
    registrar_evento(s, "titulo", t.id, "RECLASSIFICADO", {
        "numero_sp": t.numero_sp, "situacao_do_titulo": t.status.value,
        "antes": antes, "mudancas": mudou, "motivo": motivo,
        "observacao": "baixa e conciliação preservadas"}, usuario.id)
    logger.info("ERP: %s reclassificado por %s — %s", t.numero_sp, usuario.email, "; ".join(mudou))
    return {"numero_sp": t.numero_sp, "mudancas": mudou,
            "status": t.status.value,
            "preservado": "Baixa e conciliação não foram tocadas."}


def diagnosticar_desfazer(s: Session, titulo_id: int) -> dict[str, Any]:
    """Diz o que existe amarrado ao título e o que será desfeito — para o
    usuário confirmar sabendo o tamanho do estrago."""
    t = s.get(Titulo, titulo_id, options=[selectinload(Titulo.parcelas)])
    if t is None:
        raise ErroValidacao("Título não encontrado.")
    parcela_ids = [p.id for p in t.parcelas]
    pagamentos = s.scalars(select(Pagamento).where(
        Pagamento.parcela_id.in_(parcela_ids or [0]))).all()
    concs = s.scalars(select(Conciliacao).where(
        Conciliacao.pagamento_id.in_([p.id for p in pagamentos] or [0]),
        Conciliacao.desfeita_em.is_(None))).all()
    return {
        "numero_sp": t.numero_sp, "status": t.status.value,
        "parcelas": len(t.parcelas),
        "parcelas_pagas": sum(1 for p in t.parcelas if p.status == StatusParcela.PAGA),
        "pagamentos": [{"id": p.id, "valor": float(p.valor_pago),
                        "data": p.data_pagamento.isoformat()} for p in pagamentos],
        "conciliacoes": len(concs),
        "passos": ([f"desfazer {len(concs)} conciliação(ões)"] if concs else [])
                  + ([f"estornar {len(pagamentos)} pagamento(s)"] if pagamentos else [])
                  + ["reabrir as parcelas", "devolver o título para APROVADO"],
    }


def desfazer_em_cadeia(s: Session, titulo_id: int, motivo: str, usuario: Usuario,
                       ate: str = "APROVADO") -> dict[str, Any]:
    """Desfaz conciliação e baixa de uma vez, na ordem certa. Substitui o
    ritual de desconciliar → estornar → reabrir do Omie.

    `ate`: 'APROVADO' devolve o título pronto para nova baixa;
           'RASCUNHO' devolve para edição completa (usar quando o erro está no
           próprio lançamento)."""
    if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
        raise ErroPermissao("Desfazer baixa/conciliação é restrito a FINANCEIRO/ADMIN.")
    motivo = (motivo or "").strip()
    if len(motivo) < 10:
        raise ErroValidacao("Explique o motivo (mínimo 10 caracteres) — fica na trilha.")

    t = s.get(Titulo, titulo_id, options=[selectinload(Titulo.parcelas)])
    if t is None:
        raise ErroValidacao("Título não encontrado.")

    parcela_ids = [p.id for p in t.parcelas]
    pagamentos = list(s.scalars(select(Pagamento).where(
        Pagamento.parcela_id.in_(parcela_ids or [0]))).all())
    concs = list(s.scalars(select(Conciliacao).where(
        Conciliacao.pagamento_id.in_([p.id for p in pagamentos] or [0]),
        Conciliacao.desfeita_em.is_(None))).all())

    agora = datetime.now(timezone.utc)
    feito: list[str] = []

    # 1) conciliações primeiro — o vínculo é removido de fato, senão a chave
    #    estrangeira impede estornar o pagamento. O registro do que existia
    #    fica na trilha de auditoria, que é append-only.
    for c in concs:
        registrar_evento(s, "conciliacao", c.id, "DESFEITA_EM_CADEIA", {
            "titulo": t.numero_sp, "pagamento_id": c.pagamento_id,
            "extrato_id": c.extrato_id, "metodo": c.metodo,
            "confianca": str(c.confianca) if c.confianca else None,
            "motivo": motivo}, usuario.id)
        s.delete(c)
    if concs:
        feito.append(f"{len(concs)} conciliação(ões) desfeita(s)")
    s.flush()   # some com o vínculo antes de mexer no pagamento

    # 2) pagamentos
    for pg in pagamentos:
        registrar_evento(s, "pagamento", pg.id, "ESTORNADO_EM_CADEIA", {
            "titulo": t.numero_sp, "valor": str(pg.valor_pago),
            "data": pg.data_pagamento.isoformat(), "motivo": motivo}, usuario.id)
        s.delete(pg)
    if pagamentos:
        feito.append(f"{len(pagamentos)} pagamento(s) estornado(s)")

    # 3) parcelas voltam a aberto
    for p in t.parcelas:
        if p.status == StatusParcela.PAGA:
            p.status = StatusParcela.ABERTA
    feito.append("parcelas reabertas")

    # 4) título volta ao estado pedido
    t.status = StatusTitulo.RASCUNHO if ate == "RASCUNHO" else StatusTitulo.APROVADO
    if ate == "RASCUNHO":
        t.aprovador_id = None
        t.aprovado_em = None
        feito.append("título devolvido para edição (RASCUNHO)")
    else:
        feito.append("título voltou para APROVADO, pronto para nova baixa")

    s.flush()
    registrar_evento(s, "titulo", t.id, "BAIXA_DESFEITA", {
        "numero_sp": t.numero_sp, "motivo": motivo, "passos": feito,
        "por": usuario.email}, usuario.id)
    logger.info("ERP: baixa de %s desfeita por %s", t.numero_sp, usuario.email)
    return {"numero_sp": t.numero_sp, "status": t.status.value, "passos": feito}
