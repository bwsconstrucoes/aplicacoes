# ============================================================================
# ERP — core/titulos/duplicidade.py
# Crítica de duplicidade ANTES de gravar o título — o momento em que ela custa
# barato. Depois, com centenas de títulos na fila, ninguém enxerga.
#
# O que é BLOQUEIO (não deixa passar) e o que é ALERTA (mostra e deixa seguir,
# registrando a decisão) está separado de propósito: bloqueio só para
# evidência objetiva de repetição; o resto é alerta com o título parecido ao
# lado, para a pessoa comparar e decidir.
#
# Checagens:
#   D1 nota fiscal já lançada (mesmo emitente + número)            BLOQUEIO
#   D2 linha digitável já lançada em parcela ativa                 BLOQUEIO
#   D3 mesmo credor + mesmo valor + vencimento em ±15 dias         ALERTA
#   D4 mesmo credor + mesmo valor no mesmo mês de competência      ALERTA
#   D5 despesa recorrente (aluguel/locação) já lançada na          ALERTA
#      competência — o caso do "aluguel de dois meses misturados"
#   D6 descrição muito parecida do mesmo credor em 60 dias         ALERTA
# ============================================================================
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros.validadores import somente_digitos
from app.apps.erp.db.models.financeiro import Parcela, StatusParcela, Titulo

_ATIVOS = ("RASCUNHO", "EM_ANALISE", "AGUARDANDO_APROVACAO", "APROVADO",
           "BLOQUEADO", "PAGO_PARCIAL", "PAGO", "DEVOLVIDO")
_SEMELHANCA_DESCRICAO = 0.82


def _dec(v: Any) -> Optional[Decimal]:
    try:
        s = str(v or "").strip().replace("R$", "").strip()
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        return Decimal(s).quantize(Decimal("0.01")) if s else None
    except InvalidOperation:
        return None


def _data(v: Any) -> Optional[date]:
    if isinstance(v, date):
        return v
    s = str(v or "").strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _resumo(t: Titulo) -> dict[str, Any]:
    venc = min((p.vencimento for p in t.parcelas), default=None)
    return {"id": t.id, "numero_sp": t.numero_sp, "descricao": t.descricao,
            "valor": float(t.valor_liquido), "status": t.status.value,
            "vencimento": venc.isoformat() if venc else None,
            "competencia": t.competencia.strftime("%m/%Y")}


def checar(s: Session, dados: dict[str, Any]) -> dict[str, Any]:
    """Roda as críticas sobre um lançamento em preparo (ainda não gravado).
    dados: fornecedor_id, valor, parcelas[], competencia, descricao,
           documento_numero, categoria_id."""
    bloqueios: list[dict[str, Any]] = []
    alertas: list[dict[str, Any]] = []

    forn_id = dados.get("fornecedor_id")
    valor = _dec(dados.get("valor"))
    descricao = (dados.get("descricao") or "").strip()
    competencia = _data(dados.get("competencia"))
    parcelas = dados.get("parcelas") or []
    vencimentos = [v for v in (_data(p.get("vencimento")) for p in parcelas) if v]
    linhas = [somente_digitos(p.get("linha_digitavel") or "") for p in parcelas]
    linhas = [l for l in linhas if l]
    numero_doc = (dados.get("documento_numero") or "").strip()

    # ---- D1: nota já lançada para o mesmo emitente
    if forn_id and numero_doc:
        achado = s.execute(text(
            "SELECT t.id, t.numero_sp, t.descricao, t.valor_liquido, t.status "
            "FROM titulos t JOIN documentos_fiscais d ON d.id = t.documento_fiscal_id "
            "WHERE t.fornecedor_id = :f AND d.numero = :n "
            "AND t.status NOT IN ('CANCELADO','ESTORNADO') LIMIT 1"),
            {"f": forn_id, "n": numero_doc}).first()
        if achado:
            bloqueios.append({
                "codigo": "D1",
                "msg": f"A nota {numero_doc} deste credor já está lançada em {achado[1]} "
                       f"(R$ {achado[3]}, {achado[4]}).",
                "titulo": {"id": achado[0], "numero_sp": achado[1]}})

    # ---- D2: linha digitável já em parcela ativa
    for linha in linhas:
        p = s.scalars(select(Parcela).where(
            Parcela.linha_digitavel == linha,
            Parcela.status != StatusParcela.CANCELADA)).first()
        if p is not None:
            t = s.get(Titulo, p.titulo_id)
            bloqueios.append({
                "codigo": "D2",
                "msg": f"O boleto {linha[:12]}… já está lançado em "
                       f"{t.numero_sp if t else '?'} (parcela {p.numero}, "
                       f"vence {p.vencimento:%d/%m/%Y}).",
                "titulo": _resumo(t) if t else None})

    # ---- demais checagens dependem de credor + valor
    if not (forn_id and valor):
        return {"bloqueios": bloqueios, "alertas": alertas}

    candidatos = s.scalars(select(Titulo).where(
        Titulo.fornecedor_id == forn_id,
        Titulo.status.in_(_ATIVOS)).order_by(Titulo.id.desc()).limit(400)).all()

    # ---- D3: mesmo valor, vencimento próximo
    if vencimentos:
        v1 = min(vencimentos)
        for t in candidatos:
            if abs(Decimal(t.valor_liquido) - valor) > Decimal("0.01"):
                continue
            tv = min((p.vencimento for p in t.parcelas), default=None)
            if tv and abs((tv - v1).days) <= 15:
                alertas.append({
                    "codigo": "D3",
                    "msg": f"Mesmo credor, mesmo valor (R$ {valor}) e vencimento a "
                           f"{abs((tv - v1).days)} dia(s) de {t.numero_sp}. Confira se não é o mesmo título.",
                    "titulo": _resumo(t)})

    # ---- D4: mesmo valor na mesma competência
    if competencia:
        mes = competencia.replace(day=1)
        for t in candidatos:
            if (abs(Decimal(t.valor_liquido) - valor) <= Decimal("0.01")
                    and t.competencia == mes
                    and not any(a["codigo"] == "D3" and a["titulo"]["id"] == t.id for a in alertas)):
                alertas.append({
                    "codigo": "D4",
                    "msg": f"Já existe {t.numero_sp} do mesmo credor, mesmo valor, "
                           f"na competência {mes:%m/%Y}.",
                    "titulo": _resumo(t)})

    # ---- D5: recorrente (aluguel/locação) já lançado na competência
    cat_id = dados.get("categoria_id")
    if cat_id and competencia:
        from app.apps.erp.db.models.cadastros import Categoria
        cat = s.get(Categoria, int(cat_id))
        recorrente = bool(cat and (cat.subgrupo_codigo or "").startswith("3.3")
                          or (cat and "alug" in (cat.descricao or "").lower())
                          or (cat and "loca" in (cat.descricao or "").lower()))
        if recorrente:
            mes = competencia.replace(day=1)
            for t in candidatos:
                if t.categoria_id == int(cat_id) and t.competencia == mes:
                    alertas.append({
                        "codigo": "D5",
                        "msg": f"Despesa recorrente: {t.numero_sp} já cobre a competência "
                               f"{mes:%m/%Y} nesta mesma conta e credor. "
                               f"Confirme se este não é o mês seguinte.",
                        "titulo": _resumo(t)})
                    break

    # ---- D6: descrição muito parecida em 60 dias
    if descricao and len(descricao) >= 12:
        limite = date.today() - timedelta(days=60)
        for t in candidatos:
            if t.criado_em and t.criado_em.date() < limite:
                continue
            r = SequenceMatcher(None, descricao.lower(), (t.descricao or "").lower()).ratio()
            if r >= _SEMELHANCA_DESCRICAO and not any(
                    a["titulo"] and a["titulo"]["id"] == t.id for a in alertas):
                alertas.append({
                    "codigo": "D6",
                    "msg": f"Descrição {r:.0%} parecida com {t.numero_sp} "
                           f"(R$ {t.valor_liquido}, {t.competencia:%m/%Y}).",
                    "titulo": _resumo(t)})

    # limita o ruído: no máximo 6 alertas, os mais relevantes primeiro
    ordem = {"D3": 0, "D5": 1, "D4": 2, "D6": 3}
    alertas.sort(key=lambda a: ordem.get(a["codigo"], 9))
    return {"bloqueios": bloqueios, "alertas": alertas[:6]}
