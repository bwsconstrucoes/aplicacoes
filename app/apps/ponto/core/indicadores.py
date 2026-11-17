# -*- coding: utf-8 -*-
"""
O PAINEL DO PONTO — os números do período em gráficos, e a comparação com outro período (11/11/2026).

O dono: *"não tem um dashboard para eu visualizar dentro do mês: a quantidade de batidas, quantos funcionários estão
batendo ponto, as batidas corretas, as corrigidas, os atestados, as batidas faltando (…) dentro de um gráfico (…)
poder comparar um período com outro: esta quinzena com a anterior, um mês com o outro (…) saber a evolução (…) se uma
obra estava num patamar e piorou, se manteve."*

DE ONDE VEM CADA NÚMERO (nada novo inventado — as regras são as da tela Qualidade, `core/qualidade.py`):
  · DIAS DE TRABALHO, EM DIA, COM AJUSTE, BATIDA FALTANDO, FALTA, ÍNDICE, PONTOS e o % DE BATIDAS CORRIGIDAS:
    a mesma conta da Qualidade (o espelho de cada pessoa). A batida incluída em lote pela gestão (falha do sistema)
    não conta como corrigida, como lá.
  · BATIDAS POR DIA e FUNCIONÁRIOS BATENDO: contados direto das batidas (as rejeitadas não entram).
  · ATESTADOS: os aprovados que tocam o período (as declarações de comparecimento, à parte), e os dias de atestado
    DENTRO do período.
  · O período vai até ONTEM: o dia de hoje ainda está acontecendo e puxaria tudo para baixo.

A OBRA MELHOROU OU PIOROU: pela nota (os pontos da Qualidade, 0 a 100) de um período para o outro. Uma diferença de
até PATAMAR pontos é "manteve" — mexer 1 ou 2 pontos é ruído de um mês para o outro, não mudança.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from . import qualidade, vigencia

PATAMAR = 3          # pontos: até isto, para cima ou para baixo, a obra "manteve"
DIAS_MAX = 62        # o período mais longo que se compara (dois meses)


def periodo_valido(de: dt.date, ate: dt.date) -> tuple[dt.date, dt.date]:
    """PURA (com o hoje). Desvira, corta em ontem e recusa período grande demais."""
    if de > ate:
        de, ate = ate, de
    ontem = horario.hoje() - dt.timedelta(days=1)
    ate = min(ate, ontem)
    if de > ate:
        de = ate
    if (ate - de).days + 1 > DIAS_MAX:
        from ..erros import ErroDeValidacao
        raise ErroDeValidacao(f"período longo demais — até {DIAS_MAX} dias de cada vez")
    return de, ate


def tendencia(antes: Optional[int], agora: Optional[int]) -> str:
    """PURA. MELHOROU / PIOROU / MANTEVE, ou NOVA (sem nota antes) / SEM_DADOS (sem nota agora)."""
    if agora is None:
        return "SEM_DADOS"
    if antes is None:
        return "NOVA"
    if agora - antes > PATAMAR:
        return "MELHOROU"
    if antes - agora > PATAMAR:
        return "PIOROU"
    return "MANTEVE"


def _por_dia(conn: Connection, ids: list[int], de: dt.date, ate: dt.date) -> list[dict]:
    if not ids:
        linhas = []
    else:
        linhas = db.todos(conn, """
            SELECT m.data_referencia AS dia, count(*) AS batidas,
                   count(*) FILTER (WHERE m.status = 'AJUSTADA'
                                      AND COALESCE(m.motivo_analise, '') NOT LIKE 'incluída em lote%') AS corrigidas,
                   count(DISTINCT m.colaborador_id) AS pessoas
              FROM ponto.marcacoes m
             WHERE m.colaborador_id = ANY(:ids) AND m.data_referencia BETWEEN :de AND :ate AND m.status <> 'REJEITADA'
             GROUP BY m.data_referencia""", ids=ids, de=de, ate=ate)
    por = {l["dia"]: l for l in linhas}
    saida, d = [], de
    while d <= ate:
        l = por.get(d) or {}
        saida.append({"dia": d.isoformat(), "batidas": int(l.get("batidas") or 0),
                      "corrigidas": int(l.get("corrigidas") or 0), "pessoas": int(l.get("pessoas") or 0)})
        d += dt.timedelta(days=1)
    return saida


def _batendo(conn: Connection, ids: list[int], de: dt.date, ate: dt.date) -> int:
    if not ids:
        return 0
    return int(db.um(conn, """SELECT count(DISTINCT colaborador_id) AS n FROM ponto.marcacoes
                               WHERE colaborador_id = ANY(:ids) AND data_referencia BETWEEN :de AND :ate
                                 AND status <> 'REJEITADA'""", ids=ids, de=de, ate=ate)["n"])


def _atestados(conn: Connection, ids: list[int], de: dt.date, ate: dt.date) -> dict:
    from .ocorrencias import NOME_DECLARACAO
    if not ids:
        return {"atestados": 0, "dias_de_atestado": 0, "declaracoes": 0}
    linhas = db.todos(conn, """
        SELECT subtipo, GREATEST(data_inicio, :de) AS i, LEAST(COALESCE(data_fim, data_inicio), :ate) AS f
          FROM ponto.ocorrencias
         WHERE tipo = 'ATESTADO' AND status = 'APROVADA' AND colaborador_id = ANY(:ids)
           AND data_inicio <= :ate AND COALESCE(data_fim, data_inicio) >= :de""", ids=ids, de=de, ate=ate)
    atestados = [l for l in linhas if str(l["subtipo"] or "") not in NOME_DECLARACAO]
    return {"atestados": len(atestados), "dias_de_atestado": sum((l["f"] - l["i"]).days + 1 for l in atestados),
            "declaracoes": len(linhas) - len(atestados)}


def do_periodo(conn: Connection, ids: list[int], de: dt.date, ate: dt.date) -> dict:
    """Os números de um período: os cartões, as batidas dia a dia e as obras. Levanta qualidade.Ocupado."""
    de, ate = vigencia.recortar(conn, de, ate)
    if ate < de:
        ate = de
    q = qualidade.avaliar(conn, ids, de, ate, esperar=0)
    g = q["geral"]
    numeros = {
        "batidas": g.get("batidas", 0), "corrigidas": g.get("batidas_ajustadas", 0),
        "corretas": max(0, g.get("batidas", 0) - g.get("batidas_ajustadas", 0)),
        "pct_corrigidas": g.get("pct_ajustadas"),
        "funcionarios_batendo": _batendo(conn, ids, de, ate),
        "dias_trabalho": g.get("dias_trabalho", 0), "dias_em_dia": g.get("dias_em_dia", 0),
        "dias_com_ajuste": g.get("dias_com_ajuste", 0), "dias_faltando": g.get("incompletos", 0),
        "faltas": g.get("faltas", 0), "ajustes_pedidos": g.get("ajustes", 0), "indice": g.get("indice"),
        "pontos": g.get("pontos"), "selo": g.get("selo"), "fotos_ok": g.get("fotos_ok"),
        "fora_da_cerca": g.get("fora_da_cerca", 0),
        **_atestados(conn, ids, de, ate),
    }
    obras = {o["obra"]: {"obra": o["obra"], "pessoas": o["pessoas"], "pontos": o["pontos"], "indice": o["indice"],
                         "dias_trabalho": o["dias_trabalho"], "faltas": o["faltas"], "incompletos": o["incompletos"],
                         "pct_corrigidas": o["pct_ajustadas"], "selo": o["selo"]} for o in q["por_obra"]}
    return {"de": de.isoformat(), "ate": ate.isoformat(), "dias": (ate - de).days + 1, "numeros": numeros,
            "por_dia": _por_dia(conn, ids, de, ate), "obras": obras}


def comparar(a: dict, b: Optional[dict]) -> list[dict]:
    """PURA. As obras dos dois períodos lado a lado, com a tendência — as que pioraram primeiro."""
    nomes = set(a["obras"]) | set((b or {}).get("obras", {}))
    linhas = []
    for codigo in nomes:
        oa, ob = a["obras"].get(codigo), (b or {}).get("obras", {}).get(codigo)
        pa, pb = (oa or {}).get("pontos"), (ob or {}).get("pontos")
        linhas.append({"obra": codigo, "agora": oa, "antes": ob, "tendencia": tendencia(pb, pa),
                       "diferenca": (pa - pb) if pa is not None and pb is not None else None})
    ordem = {"PIOROU": 0, "MANTEVE": 1, "MELHOROU": 2, "NOVA": 3, "SEM_DADOS": 4}
    linhas.sort(key=lambda x: (ordem[x["tendencia"]], x["diferenca"] if x["diferenca"] is not None else 0, x["obra"]))
    return linhas
