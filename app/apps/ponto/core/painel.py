# -*- coding: utf-8 -*-
"""
O painel "Hoje": por obra, quem bateu, quem não bateu, quem está afastado, quem
está de folga. É a tela do encarregado às 7h30 — tem de abrir rápido com 400
pessoas, então tudo é carregado em POUCAS consultas (batidas do dia, ocorrências
do dia, vigências de escala, feriados), nunca uma consulta por pessoa.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from . import cadastros, escalas, espelho, feriados

TOLERANCIA_CHEGADA_MIN = 15

SITUACOES = {
    "PRESENTE": "Bateu", "INCOMPLETO": "Batida faltando", "AUSENTE": "Não bateu",
    "AGUARDANDO": "Ainda não é hora", "AFASTADO": "Afastado", "FOLGA": "Folga / descanso",
    "FERIADO": "Feriado", "SEM_ESCALA": "Sem escala",
}


def hoje(conn: Connection, *, dia: Optional[dt.date] = None, obras: Optional[list[int]] = None,
         obra_id: Optional[int] = None, agora: Optional[dt.datetime] = None) -> dict:
    dia = dia or horario.hoje()
    agora_local = horario.para_local(agora or horario.agora())
    pessoas = cadastros.listar_colaboradores(conn, so_ativos=True, obra_id=obra_id)
    if obras is not None:
        alcance = set(obras)
        pessoas = [p for p in pessoas if (p.get("obra_id") in alcance)
                   or any(o["id"] in alcance for o in p.get("obras_adicionais", []))]
    ids = [p["id"] for p in pessoas] or [0]

    batidas = defaultdict(list)
    for b in db.todos(conn, """
        SELECT m.id, m.colaborador_id, m.timestamp_servidor, m.status, m.dentro_da_cerca, m.nsr,
               m.obra_id, m.foto_id, o.codigo AS obra
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.data_referencia = :d AND m.colaborador_id = ANY(:ids) AND m.status <> 'REJEITADA'
         ORDER BY m.timestamp_servidor
    """, d=dia, ids=ids):
        batidas[b["colaborador_id"]].append(b)
    ocorr = defaultdict(list)
    for o in db.todos(conn, """
        SELECT colaborador_id, tipo, status FROM ponto.ocorrencias
         WHERE colaborador_id = ANY(:ids) AND status NOT IN ('NEGADA', 'CANCELADA')
           AND data_inicio <= :d AND data_fim >= :d
    """, d=dia, ids=ids):
        ocorr[o["colaborador_id"]].append(o)
    vigencias = defaultdict(list)
    for v in db.todos(conn, """
        SELECT ce.colaborador_id, ce.vigencia_inicio, ce.ciclo_data_base, e.*
          FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
         WHERE ce.colaborador_id = ANY(:ids) AND ce.vigencia_inicio <= :d
         ORDER BY ce.vigencia_inicio
    """, d=dia, ids=ids):
        vigencias[v["colaborador_id"]].append(v)
    calendario = feriados.Calendario(conn, dia, dia)

    linhas, por_obra = [], defaultdict(lambda: defaultdict(int))
    for p in pessoas:
        do_dia = batidas.get(p["id"], [])
        vig = vigencias.get(p["id"], [])
        atual = vig[-1] if vig else None
        esc = escalas.para_apuracao(atual, atual["ciclo_data_base"]) if atual else None
        periodos = esc.periodos(dia) if esc else []
        obra_do_dia = do_dia[0]["obra_id"] if do_dia else p.get("obra_id")
        nome_feriado = calendario.feriado(dia, obra_do_dia)
        aprovada = next((o for o in ocorr.get(p["id"], []) if o["status"] == "APROVADA"
                         and o["tipo"] in espelho.ABONO_POR_TIPO), None)
        pendente = next((o for o in ocorr.get(p["id"], []) if o["status"].startswith("AGUARDANDO")), None)
        if aprovada or p.get("situacao") == "AFASTADO":     # atestado aprovado, ou afastado no cadastro
            situacao = "AFASTADO"
        elif do_dia:
            # Número ímpar de batidas durante o dia é normal (entrou e ainda não
            # saiu). Só vira "batida faltando" quando o dia já passou.
            situacao = "INCOMPLETO" if (len(do_dia) % 2 and dia < agora_local.date()) else "PRESENTE"
        elif esc is None:
            situacao = "SEM_ESCALA"
        elif nome_feriado:
            situacao = "FERIADO"
        elif not periodos:
            situacao = "FOLGA"
        else:
            entrada = dt.datetime.combine(dia, dt.time(0, 0), tzinfo=horario.FUSO) \
                + dt.timedelta(minutes=periodos[0][0] + TOLERANCIA_CHEGADA_MIN)
            situacao = "AUSENTE" if agora_local >= entrada else "AGUARDANDO"
        obra_codigo = (do_dia[0]["obra"] if do_dia else p.get("obra_codigo")) or "—"
        por_obra[obra_codigo][situacao] += 1
        linhas.append({
            "colaborador_id": p["id"], "nome": p["nome"], "cpf": p["cpf"], "obra": obra_codigo,
            "situacao": situacao, "situacao_rotulo": SITUACOES[situacao],
            "escala": atual["nome"] if atual else None,
            "previsto_entrada": (f"{periodos[0][0] // 60:02d}:{periodos[0][0] % 60:02d}"
                                 if periodos else None),
            "batidas": [{"id": b["id"], "tem_foto": b["foto_id"] is not None,
                         "hora": horario.para_local(b["timestamp_servidor"]).strftime("%H:%M"),
                         "status": b["status"], "fora_da_cerca": b["dentro_da_cerca"] is False,
                         "nsr": b["nsr"], "obra": b["obra"]} for b in do_dia],
            "em_analise": sum(1 for b in do_dia if b["status"] == "EM_ANALISE"),
            "ocorrencia": (espelho.ROTULO_TIPO[aprovada["tipo"]] if aprovada else None),
            "pendente": (espelho.ROTULO_TIPO[pendente["tipo"]] if pendente else None),
            "feriado": nome_feriado,
        })
    ordem = {"AUSENTE": 0, "INCOMPLETO": 1, "SEM_ESCALA": 2, "AGUARDANDO": 3, "PRESENTE": 4,
             "AFASTADO": 5, "FERIADO": 6, "FOLGA": 7}
    linhas.sort(key=lambda l: (ordem[l["situacao"]], l["obra"], l["nome"]))
    totais = defaultdict(int)
    for l in linhas:
        totais[l["situacao"]] += 1
    return {"data": dia.isoformat(), "agora": agora_local.strftime("%H:%M"),
            "totais": dict(totais), "pessoas": linhas,
            "obras": [{"obra": o, **dict(c)} for o, c in sorted(por_obra.items())]}
