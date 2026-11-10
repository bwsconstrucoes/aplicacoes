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


def _so_digitos(v) -> str:
    """Mantém só os dígitos (ex.: '3.076' / '3076,00' -> '3076')."""
    return "".join(c for c in str(v or "") if c.isdigit())


def _chave_numero(v) -> str:
    """O número da nota reduzido ao SEQUENCIAL, para comparar.

    ⚠️ **Sem isto a substituição não acha a nota antiga, e foi o que aconteceu em
    09/11/2026.** No padrão nacional a nota é gravada com 13 dígitos (ano +
    sequencial): o slot do card e a coluna F da planilha têm `2600000003352`.
    Quem vai substituir digita o número que tem na mão — e às vezes é o curto,
    `3352`. A comparação por dígitos crus dava "não encontrado", **a nota antiga
    continuava válida no card e na planilha**, e o aviso passava no meio do log.

    Comparar pelo sequencial faz os dois formatos casarem, nos dois sentidos.
    """
    from worker import sequencial_da_nota
    seq = sequencial_da_nota(v)
    return str(seq) if seq else _so_digitos(v)


def localizar_slot_por_numero(card: dict, numero) -> str | None:
    """Slot A–E cujo campo 'número da nota' == numero. None se não achar.
    Compara só por dígitos (robusto a '3.076' vindo do Pipefy)."""
    por_id = card.get("campos_por_id", {}) or {}
    alvo = _chave_numero(numero)
    if not alvo:
        return None
    for s in "ABCDE":
        f = pf.CAMPOS_SLOT[s]
        if _chave_numero(por_id.get(f["numero"])) == alvo:
            return s
    return None


def cancelar_no_card(card: dict, numero, token: str, novo_numero=None) -> dict:
    """Marca o slot da nota antiga como 'Cancelada' no card (Pipefy)."""
    slot = localizar_slot_por_numero(card, numero)
    if not slot:
        return {"ok": False, "slot": None,
                "msg": f"⚠️ NÃO CANCELADO NO CARD: o nº {numero} não está em "
                       f"nenhum dos slots A–E. A nota antiga continua valendo lá — "
                       f"confira o número e marque o slot à mão se preciso"}
    fid = pf.CAMPOS_SLOT[slot]["status"]
    mutation = "mutation {\n" + f"c1: {pf._campo(card['card_id'], fid, STATUS_CANCELADA)}" + "\n}"
    pf.executar(mutation, token)
    return {"ok": True, "slot": slot,
            "msg": f"slot {slot} (nº {numero}) marcado como '{STATUS_CANCELADA}' no card"}


def cancelar_na_planilha(ws_notas, numero, novo_numero=None) -> dict:
    """Marca a linha da nota antiga na 'Notas BWS' (coluna L = Observação).
    Localiza pela coluna F (Nº Nota)."""
    col = ws_notas.col_values(6)  # F = Nº Nota
    alvo = _chave_numero(numero)
    obs = "CANCELADA" + (f" — substituída pela NF {novo_numero}" if novo_numero else "")
    for i, v in enumerate(col, start=1):
        if _chave_numero(v) == alvo:
            ws_notas.update(f"L{i}", [[obs]], value_input_option="USER_ENTERED")
            return {"ok": True, "linha": i, "msg": f"linha {i} da 'Notas BWS' marcada: {obs}"}
    return {"ok": False, "linha": None,
            "msg": f"⚠️ NÃO MARCADO NA PLANILHA: o nº {numero} não está na coluna "
                   f"'Nº Nota' da 'Notas BWS'. A linha da nota antiga continua "
                   f"sem a marca de cancelada"}
