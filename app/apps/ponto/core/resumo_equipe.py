# -*- coding: utf-8 -*-
"""
O RESUMO GERENCIAL DO PONTO, NO APLICATIVO (31/10/2026).

O dono: *"esse mesmo resumo do WhatsApp a gente poderia ter para a pessoa que bate os pontos da obra (…) se eu estou
batendo os pontos da obra X, eu gero o resumo da minha obra, ou das obras, se eu tiver relacionado a mais de uma (…) um
que está perfeito e um que não está (…) fulano, está faltando batida (…) as faltas, o resumo dos atestados, os dias de
cada um (…) o que está em erro porque está faltando uma batida, duas batidas, ou o que está provavelmente em erro (…)
para orientar as pessoas a corrigir os pontos."*

Para cada pessoa, os dias JÁ PASSADOS do período (`classificar`, pura): faltas; batida faltando (quantas e o que foi
batido); "provavelmente errado" (batida repetida, batida no meio do turno da noite, noite fora do ciclo 12x36, batida
sem par em dia de folga); atrasos e saídas antes da hora; atestados e abonos (informação); pedidos esperando validação
e negados. Quem tem só informação (atestado, pedido esperando) está "em dia".

Quem aparece: o alcance da consulta (administrativo, liberado pelo DP) e, para quem responde por um ponto da obra, quem
bateu no aparelho dele no período (`qualidade.equipe_do_responsavel`). A obra de cada um é a do ponto (onde mais bateu
no período); sem batida, a do cadastro.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

# alerta do dia → o texto do "provavelmente errado"
SUSPEITAS = {
    "BATIDA_REPETIDA": "batida repetida (bateu duas vezes seguidas)",
    "BATIDA_NO_MEIO_DO_TURNO": "batida no meio do turno da noite",
    "FORA_DO_CICLO": "noite fora do ciclo 12x36 (troca de plantão ou extra?)",
}
SITUACOES_COM_BATIDA_FALTANDO = ("FALTA_PARCIAL", "INCOMPLETO")
MAX_PESSOAS = 250          # acima disso (todas as obras liberadas), escolhe-se a obra antes


def classificar(dias: list[dict], ate: dt.date) -> dict:
    """PURA. Os problemas de uma pessoa nos dias do espelho até `ate` (inclusive)."""
    r = {"faltas": [], "batida": [], "suspeitas": [], "atrasos": [], "justificados": [], "pendentes": [], "negados": []}
    limite = ate.isoformat()
    for d in dias:
        if d["data"] > limite or d.get("situacao") == "ANTES_DO_PONTO":
            continue
        horas = [b.get("hora") for b in d.get("batidas") or [] if b.get("hora")]
        s = d.get("situacao")
        if d.get("ocorrencia"):
            r["justificados"].append({"data": d["data"], "rotulo": (d["ocorrencia"] or {}).get("rotulo") or "justificado"})
        elif s == "FALTA":
            r["faltas"].append({"data": d["data"]})
        elif s in SITUACOES_COM_BATIDA_FALTANDO or (d.get("faltando") and horas):
            r["batida"].append({"data": d["data"], "faltam": len(d.get("faltando") or []) or 1, "batidas": horas,
                                "sugestao": [f.get("sugestao") for f in d.get("faltando") or [] if f.get("sugestao")]})
        elif s == "OK" and (d.get("debito") or 0) > 0:
            r["atrasos"].append({"data": d["data"], "minutos": int(d["debito"])})
        alertas = d.get("alertas") or []
        motivos = [t for k, t in SUSPEITAS.items() if k in alertas and not (k == "FORA_DO_CICLO" and d.get("compensacao"))]
        if s == "DESCANSO" and "BATIDA_FALTANDO" in alertas:
            motivos.append("batida sem par em dia de folga")
        if motivos:
            r["suspeitas"].append({"data": d["data"], "motivos": motivos, "batidas": horas})
        for p in d.get("pendentes") or []:
            r["pendentes"].append({"data": d["data"], "rotulo": p.get("rotulo") or p.get("tipo") or "pedido",
                                   "hora": p.get("hora")})
        for n in d.get("negados") or []:
            r["negados"].append({"data": d["data"], "rotulo": n.get("rotulo") or "pedido", "motivo": n.get("motivo")})
    r["problemas"] = len(r["faltas"]) + len(r["batida"]) + len(r["suspeitas"]) + len(r["atrasos"])
    r["em_dia"] = r["problemas"] == 0
    return r


def periodo(qual: Optional[str], hoje: dt.date) -> tuple[dt.date, dt.date, str]:
    """PURA. (início, fim, rótulo): "mes" (do dia 1 até ontem — o padrão), "7dias", "anterior" (o mês passado)."""
    ontem = hoje - dt.timedelta(days=1)
    if qual == "7dias":
        return ontem - dt.timedelta(days=6), ontem, "últimos 7 dias"
    if qual == "anterior":
        fim = hoje.replace(day=1) - dt.timedelta(days=1)
        return fim.replace(day=1), fim, f"{fim:%m/%Y}"
    inicio = hoje.replace(day=1) if hoje.day > 1 else (hoje - dt.timedelta(days=1)).replace(day=1)
    return inicio, ontem, f"{inicio:%m/%Y} até ontem"


def montar(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date, *,
           obra_aqui: Optional[dict] = None, abre: Optional[set] = None) -> list[dict]:
    """Os resumos das pessoas `ids` no período, agrupados por obra: [{obra, pessoas: [...]}], com quem tem problema
    primeiro. `abre`: os ids que quem pede pode abrir (consultar/corrigir) daqui."""
    from . import cadastros, espelho
    pessoas = [p for p in (cadastros.colaborador_por_id(conn, i) for i in sorted(set(ids))) if p]
    if not pessoas:
        return []
    lote = espelho.Lote(conn, pessoas, inicio, fim)
    grupos: dict[str, dict] = {}
    for p in pessoas:
        esp = espelho.montar(conn, int(p["id"]), inicio, fim, lote=lote)
        c = classificar(esp["dias"], fim)
        obra = ((obra_aqui or {}).get(int(p["id"])) or cadastros.obra_mais_batida(esp["dias"])
                or p.get("obra_codigo") or "")
        g = grupos.setdefault(obra, {"obra": obra, "pessoas": []})
        g["pessoas"].append({"id": int(p["id"]), "nome": p["nome"], "funcao": p.get("funcao") or "",
                             "pode_abrir": abre is None or int(p["id"]) in abre, **c})
    for g in grupos.values():
        g["pessoas"].sort(key=lambda x: (x["em_dia"], -x["problemas"], x["nome"]))
        g["em_dia"] = sum(1 for x in g["pessoas"] if x["em_dia"])
    return sorted(grupos.values(), key=lambda g: (not g["obra"], g["obra"]))
