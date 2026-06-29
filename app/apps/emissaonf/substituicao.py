# -*- coding: utf-8 -*-
"""
Substituição de NFS-e.

Quando uma nova nota substitui uma antiga, este módulo:
  - marca o slot da nota antiga (A–E) no card como 'Cancelada';
  - marca a linha da nota antiga na planilha 'Notas BWS' (coluna L = Observação).

A linha da planilha NÃO é apagada (preserva o histórico/fórmulas); só recebe a
observação de cancelamento.

ATENÇÃO (fora do escopo): este módulo NÃO cancela a nota na prefeitura
(ABRASF CancelarNfse). O cancelamento fiscal junto ao município, se necessário,
é um passo manual/separado.
"""
from __future__ import annotations
import pipefy_update as pf

STATUS_CANCELADA = "Cancelada"   # vocabulário que a validação trata como NÃO-válida


def localizar_slot_por_numero(card: dict, numero) -> str | None:
    """Slot A–E cujo campo 'número da nota' == numero. None se não achar."""
    por_id = card.get("campos_por_id", {}) or {}
    alvo = str(numero).strip()
    if not alvo:
        return None
    for s in "ABCDE":
        f = pf.CAMPOS_SLOT[s]
        if (por_id.get(f["numero"]) or "").strip() == alvo:
            return s
    return None


def cancelar_no_card(card: dict, numero, token: str, novo_numero=None) -> dict:
    """Marca o slot da nota antiga como 'Cancelada' no card (Pipefy)."""
    slot = localizar_slot_por_numero(card, numero)
    if not slot:
        return {"ok": False, "slot": None,
                "msg": f"nº {numero} não encontrado nos slots A–E do card (nada cancelado no card)"}
    fid = pf.CAMPOS_SLOT[slot]["status"]
    mutation = "mutation {\n" + f"c1: {pf._campo(card['card_id'], fid, STATUS_CANCELADA)}" + "\n}"
    pf.executar(mutation, token)
    return {"ok": True, "slot": slot,
            "msg": f"slot {slot} (nº {numero}) marcado como '{STATUS_CANCELADA}' no card"}


def cancelar_na_planilha(ws_notas, numero, novo_numero=None) -> dict:
    """Marca a linha da nota antiga na 'Notas BWS' (coluna L = Observação).
    Localiza pela coluna F (Nº Nota)."""
    col = ws_notas.col_values(6)  # F = Nº Nota
    alvo = str(numero).strip()
    obs = "CANCELADA" + (f" — substituída pela NF {novo_numero}" if novo_numero else "")
    for i, v in enumerate(col, start=1):
        if str(v).strip() == alvo:
            ws_notas.update(f"L{i}", [[obs]], value_input_option="USER_ENTERED")
            return {"ok": True, "linha": i, "msg": f"linha {i} da 'Notas BWS' marcada: {obs}"}
    return {"ok": False, "linha": None,
            "msg": f"nº {numero} não encontrado na coluna F da 'Notas BWS' (nada marcado na planilha)"}
