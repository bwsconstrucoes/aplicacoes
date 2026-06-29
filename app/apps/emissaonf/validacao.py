# -*- coding: utf-8 -*-
"""
Validações pré-emissão da NFS-e.

  TETO DE VALOR: a nota não pode passar do Valor da Medição — nem individualmente,
  nem pela SOMA das notas já VÁLIDAS (slots A–E com status 'Válida').

  MÍNIMO OBRIGATÓRIO: confere o básico para emitir.

Devolve 'bloqueios' (impedem emitir) e 'avisos' (só alertam).
"""
from __future__ import annotations
import unicodedata
from decimal import Decimal

from pipefy import _num
import pipefy_update as pf

CENT = Decimal("0.01")


def brl(v) -> str:
    s = f"{Decimal(str(v)):,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def _dec_norm(v) -> Decimal:
    """Para valores que JÁ vêm normalizados pelo extrair_card (ex.: '336606.43')."""
    try:
        s = str(v).strip()
        return Decimal(s) if s else Decimal("0")
    except Exception:
        return Decimal("0")


def _dec_br(v) -> Decimal:
    """Para valores CRUS do Pipefy em formato BR (ex.: '110.124,91' / '110124,91')."""
    try:
        return Decimal(_num(v))
    except Exception:
        return Decimal("0")


def _norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return s.upper().strip()


def slots_preenchidos(card: dict) -> list[dict]:
    """Slots A–E já preenchidos no card, com status/número/valor e flag 'valida'.
    Lê pelos field_id verificados em pipefy_update.CAMPOS_SLOT."""
    por_id = card.get("campos_por_id", {}) or {}
    out = []
    for s in "ABCDE":
        f = pf.CAMPOS_SLOT[s]
        status = (por_id.get(f["status"]) or "").strip()
        if not status:
            continue
        out.append({
            "slot": s,
            "status": status,
            "numero": (por_id.get(f["numero"]) or "").strip(),
            "valor": _dec_br(por_id.get(f["valor"])),
            "valida": _norm(status).startswith("VALID"),   # 'Válida'
        })
    return out


def checar(card: dict, r) -> dict:
    cap = _dec_norm(card.get("valor_medicao"))
    atual = Decimal(str(getattr(r, "valor_total", 0) or 0))
    slots = slots_preenchidos(card)
    ja_valido = sum((x["valor"] for x in slots if x["valida"]), Decimal("0"))
    total = ja_valido + atual

    bloqueios, avisos = [], []

    # ---- teto de valor ----
    if cap <= 0:
        avisos.append("Valor da Medição não informado no card — não dá para conferir o teto de valor.")
    else:
        if atual > cap + CENT:
            bloqueios.append(
                f"Valor desta nota (R$ {brl(atual)}) é MAIOR que o Valor da Medição (R$ {brl(cap)}).")
        if total > cap + CENT:
            bloqueios.append(
                f"Soma das notas válidas (R$ {brl(ja_valido)}) + esta (R$ {brl(atual)}) "
                f"= R$ {brl(total)} excede o Valor da Medição (R$ {brl(cap)}).")

    # ---- mínimo obrigatório para emitir ----
    if atual <= 0:
        bloqueios.append("Valor da nota está zerado (confira 'Valor Parcial' / 'Valor da Medição').")
    emi = _norm(card.get("emissao_nf"))
    if "PARCIAL" in emi and _dec_norm(card.get("valor_parcial")) <= 0:
        bloqueios.append("Emissão PARCIAL exige o campo 'Valor Parcial' preenchido.")
    if not _norm(card.get("tipo_medicao")):
        bloqueios.append("'Tipo de Medição' é obrigatório e está vazio.")
    if not _norm(card.get("banco")):
        bloqueios.append("'Banco para Recebimento' é obrigatório e está vazio.")

    return {
        "ok": not bloqueios,
        "bloqueios": bloqueios,
        "avisos": avisos,
        "cap": cap,
        "ja_valido": ja_valido,
        "atual": atual,
        "total": total,
        "restante": (cap - ja_valido) if cap > 0 else None,
        "slots": slots,
    }
