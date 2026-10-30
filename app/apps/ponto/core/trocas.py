# -*- coding: utf-8 -*-
"""
A TROCA DE PLANTÃO IDENTIFICADA SOZINHA (31/10/2026).

O dono, vendo dois vigias da mesma obra no calendário de horas extras — um com 13h57 de extra no dia 17 e falta no
18, o outro com falta no 17 e 13h41 de extra no 18: *"eles trocaram o plantão. Como podemos resolver isso? (…) O
ideal é que o sistema identifique de forma automática essa permuta, mas alerte pra validação, e ainda que haja um
registro no espelho."*

A REGRA (pura, `detectar`): na MESMA obra, A trabalhou a noite X fora do ciclo dele (alerta FORA_DO_CICLO do 12x36
automático) e B faltou nessa mesma noite X; e, até JANELA_DIAS depois ou antes, B trabalhou a noite Y fora do ciclo e
A faltou nela. Só a troca completa (as duas pontas) é proposta — quem só cobriu a noite do outro continua com a
extra e a falta, para o DP decidir.

NADA É GRAVADO SOZINHO: a troca é PROPOSTA (na janela da pessoa e na Conferência do fechamento). Confirmada pelo DP
(`confirmar`), viram duas COMPENSAÇÕES aprovadas, uma de cada um — a folga é a noite que ele deixou, o dia trabalhado
é a do colega —, com a descrição "Troca de plantão com …", que é o que o espelho mostra nos dois dias. Com isso a
falta e a extra somem (a compensação já fazia isso) e o ciclo automático não se perde (`escalas.dias_trabalhados`).
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db

JANELA_DIAS = 15
PREFIXO = "Troca de plantão com"         # a de antes da migração 024; depois, "Permuta com" (ocorrencias.PERMUTA)


def _dias(esp: dict) -> tuple[dict, dict]:
    """PURA. Do espelho: {data: obra} das noites fora do ciclo e das faltas sem justificativa."""
    fora, falta = {}, {}
    for d in esp.get("dias") or []:
        obra = d.get("obra_do_dia")
        if any(p.get("tipo") == "COMPENSACAO" for p in d.get("pendentes") or []):
            continue                     # já tem permuta (ou compensação) pedida esperando decisão
        if "FORA_DO_CICLO" in (d.get("alertas") or []) and not d.get("compensacao"):
            fora[d["data"]] = obra
        elif d.get("situacao") == "FALTA" and not d.get("ocorrencia"):
            falta[d["data"]] = obra
    return fora, falta


def detectar(espelhos: dict[int, dict]) -> list[dict]:
    """PURA. As trocas completas entre as pessoas dos espelhos (de preferência, as de uma obra)."""
    info = {cid: _dias(e) for cid, e in espelhos.items()}
    nomes = {cid: (e.get("pessoa") or {}).get("nome") or f"pessoa nº {cid}" for cid, e in espelhos.items()}
    usados: set = set()
    trocas = []
    for a, (fora_a, falta_a) in sorted(info.items()):
        for x, obra in sorted(fora_a.items()):
            if (a, x) in usados:
                continue
            for b, (fora_b, falta_b) in sorted(info.items()):
                if b == a or x not in falta_b or falta_b[x] != obra or (b, x) in usados:
                    continue
                dx = dt.date.fromisoformat(x)
                volta = next((y for y in sorted(fora_b, key=lambda y: abs((dt.date.fromisoformat(y) - dx).days))
                              if y in falta_a and falta_a[y] == fora_b[y] == obra and y != x
                              and abs((dt.date.fromisoformat(y) - dx).days) <= JANELA_DIAS
                              and (b, y) not in usados and (a, y) not in usados), None)
                if not volta:
                    continue
                usados |= {(a, x), (b, x), (a, volta), (b, volta)}
                trocas.append({"obra": obra,
                               "a": {"id": a, "nome": nomes[a], "cobriu": x, "deixou": volta},
                               "b": {"id": b, "nome": nomes[b], "cobriu": volta, "deixou": x}})
                break
    return sorted(trocas, key=lambda t: (t["a"]["cobriu"], t["a"]["nome"]))


def _br(iso: str) -> str:
    return f"{iso[8:10]}/{iso[5:7]}"


def da_pessoa(conn: Connection, colaborador_id: int, inicio: dt.date, fim: dt.date,
              esp: Optional[dict] = None) -> list[dict]:
    """As trocas propostas de uma pessoa no período: olha as obras em que ela bateu e os outros do 12x36 que
    bateram nelas. Barata quando não há o que procurar: sem noite fora do ciclo nem falta no espelho dela, nada."""
    if esp is not None:
        fora, falta = _dias(esp)
        if not fora and not falta:
            return []
    obras = [int(r["obra_id"]) for r in db.todos(conn, """
        SELECT DISTINCT obra_id FROM ponto.marcacoes
         WHERE colaborador_id = :c AND data_referencia BETWEEN :i AND :f AND status <> 'REJEITADA'""",
                                                   c=int(colaborador_id), i=inicio, f=fim)]
    if not obras:
        return []
    # só quem está numa escala 12x36 (a troca de plantão é do ciclo automático): a obra pode ter cem pessoas
    ids = {int(r["colaborador_id"]) for r in db.todos(conn, """
        SELECT DISTINCT m.colaborador_id FROM ponto.marcacoes m
         WHERE m.obra_id = ANY(:o) AND m.data_referencia BETWEEN :i AND :f AND m.status <> 'REJEITADA'
           AND EXISTS (SELECT 1 FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
                        WHERE ce.colaborador_id = m.colaborador_id AND e.tipo = 'CICLO_12X36')""",
                                                         o=obras, i=inicio - dt.timedelta(days=JANELA_DIAS),
                                                         f=fim + dt.timedelta(days=JANELA_DIAS))}
    if int(colaborador_id) not in ids or len(ids) < 2:
        return []
    return [t for t in de_pessoas(conn, sorted(ids), inicio, fim)
            if colaborador_id in (t["a"]["id"], t["b"]["id"])]


def de_pessoas(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date,
               espelhos: Optional[dict[int, dict]] = None) -> list[dict]:
    """As trocas entre estas pessoas no período (com os espelhos já montados, se vierem)."""
    from . import cadastros, espelho
    if espelhos is None:
        pessoas = [p for p in (cadastros.colaborador_por_id(conn, i) for i in ids) if p]
        if len(pessoas) < 2:
            return []
        lote = espelho.Lote(conn, pessoas, inicio, fim)
        espelhos = {int(p["id"]): espelho.montar(conn, int(p["id"]), inicio, fim, lote=lote) for p in pessoas}
    return detectar(espelhos)


# AS SUGESTÕES NAS VALIDAÇÕES (31/10/2026, o dono: "de repente na tela de validação já aparece alguma espécie de
# sugestão do sistema?"): as trocas completas de todos os vigias do 12x36 nos últimos DIAS_SUGESTAO dias. A conta
# monta o espelho de cada vigia — fica guardada GUARDA_S segundos (a tela de Validações abre muitas vezes), e
# `esquecer()` a apaga quando uma permuta é aplicada.
DIAS_SUGESTAO = 35
GUARDA_S = 600
_guardadas: dict = {}


def esquecer() -> None:
    _guardadas.clear()


def sugeridas(conn: Connection, obras: Optional[list[int]] = None, hoje: Optional[dt.date] = None) -> list[dict]:
    """As permutas que as batidas mostram e ninguém lançou, de quem está no 12x36 (só nas `obras`, se vier)."""
    import time
    from .. import horario
    hoje = hoje or horario.hoje()
    chave = (tuple(sorted(obras)) if obras is not None else None, hoje)
    guardada = _guardadas.get(chave)
    if guardada and time.monotonic() - guardada[0] < GUARDA_S:
        return guardada[1]
    inicio, fim = hoje - dt.timedelta(days=DIAS_SUGESTAO), hoje - dt.timedelta(days=1)
    ids = sorted({int(r["colaborador_id"]) for r in db.todos(conn, """
        SELECT DISTINCT m.colaborador_id FROM ponto.marcacoes m
         WHERE m.data_referencia BETWEEN :i AND :f AND m.status <> 'REJEITADA'
           AND (CAST(:o AS BIGINT[]) IS NULL OR m.obra_id = ANY(:o))
           AND EXISTS (SELECT 1 FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
                        WHERE ce.colaborador_id = m.colaborador_id AND e.tipo = 'CICLO_12X36')""",
                                                       i=inicio, f=fim, o=list(obras) if obras is not None else None)})
    achadas = de_pessoas(conn, ids, inicio, fim) if len(ids) >= 2 else []
    _guardadas.clear()
    _guardadas[chave] = (time.monotonic(), achadas)
    return achadas


def confirmar(conn: Connection, quem, troca: dict) -> list[dict]:
    """A troca confirmada pelo DP: as duas compensações, aprovadas. Devolve as ocorrências. Com a migração 024, é
    uma PERMUTA (31/10/2026) — a mesma do pedido lançado à mão: um pedido, e o lado do colega entra junto."""
    from . import ocorrencias
    a, b = troca["a"], troca["b"]
    if not (a.get("cobriu") and a.get("deixou") and b.get("id")):
        raise ValueError("troca incompleta")
    if db.tem_coluna(conn, "ocorrencias", "colega_id"):
        feita = ocorrencias.criar(conn, quem, {
            "tipo": ocorrencias.PERMUTA, "colaborador_id": int(a["id"]), "colega_id": int(b["id"]),
            "data_inicio": a["deixou"], "dia_trabalhado": a["cobriu"],
            "descricao": "identificada pelo sistema nas batidas", "aprovar_ja": True}, origem="GESTAO")
        outro = db.um(conn, """SELECT id FROM ponto.ocorrencias WHERE colaborador_id = :c AND subtipo = :s
                                  AND data_inicio = :i AND dia_trabalhado = :t AND status = 'APROVADA'""",
                      c=int(b["id"]), s=ocorrencias.PERMUTA, i=dt.date.fromisoformat(a["cobriu"]),
                      t=dt.date.fromisoformat(a["deixou"]))
        return [feita] + ([ocorrencias.obter(conn, int(outro["id"]), quem)] if outro else [])
    feitas = []
    for eu, outro in ((a, b), (b, a)):
        feitas.append(ocorrencias.criar(conn, quem, {
            "tipo": "COMPENSACAO", "colaborador_id": int(eu["id"]),
            "data_inicio": eu["deixou"], "dia_trabalhado": eu["cobriu"],
            "descricao": (f"{PREFIXO} {outro['nome']}: trabalhou a noite de {_br(eu['cobriu'])} no lugar do colega, "
                          f"que trabalhou a de {_br(eu['deixou'])}"),
            "aprovar_ja": True}, origem="GESTAO"))
    return feitas
