# -*- coding: utf-8 -*-
"""
Consulta das marcações para sistemas externos (Análise de SPs, planilhas).

O formato é o DO PONTO, não o do Mobponto: cada marcação é uma linha completa
(hora oficial, obra, status, cerca, origem), e um resumo por pessoa e dia
acompanha. Quem consome se adapta a isto — decisão do dono, 03/10/2026: o ponto
é a solução definitiva; as outras se conectam a ele.

Teto de 62 dias por chamada e de linhas por resposta: a instância tem 2 GB
dividida com 17 módulos (CONTEXTO.md §3.7). Quem precisa de mais pagina por
período.
"""
from __future__ import annotations

import datetime as dt
from collections import OrderedDict

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao
from . import cadastros, marcacoes

MAX_DIAS = 62
MAX_LINHAS = 20_000
STATUS = ("VALIDA", "EM_ANALISE", "AJUSTADA", "REJEITADA")


def _data(valor, campo: str, padrao: dt.date | None = None) -> dt.date:
    if valor is None or str(valor).strip() == "":
        if padrao is None:
            raise ErroDeValidacao(f"informe {campo} (AAAA-MM-DD)", campo=campo)
        return padrao
    try:
        return dt.date.fromisoformat(str(valor).strip())
    except ValueError as e:
        raise ErroDeValidacao(f"{campo} ilegível (use AAAA-MM-DD)", campo=campo) from e


def periodo(data_inicio, data_fim) -> tuple[dt.date, dt.date]:
    inicio = _data(data_inicio, "data_inicio")
    fim = _data(data_fim, "data_fim", padrao=inicio)
    if fim < inicio:
        raise ErroDeValidacao("data_fim anterior a data_inicio", campo="data_fim")
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ErroDeValidacao(f"período máximo por consulta: {MAX_DIAS} dias", campo="data_fim")
    return inicio, fim


def listar(conn: Connection, *, cpf=None, data_inicio=None, data_fim=None, obra=None,
           status=None) -> dict:
    inicio, fim = periodo(data_inicio, data_fim)
    sql = marcacoes._SQL_MARCACAO + " WHERE m.data_referencia BETWEEN :ini AND :fim"
    params: dict = {"ini": inicio, "fim": fim}
    if cpf:
        params["cpf"] = cadastros.normalizar_cpf(cpf)
        sql += " AND regexp_replace(c.cpf, '\\D', '', 'g') = :cpf"
    if obra:
        o = cadastros.resolver_obra(conn, obra)
        if not o:
            raise ErroDeValidacao("obra não cadastrada", campo="obra")
        params["obra_id"] = o["id"]
        sql += " AND m.obra_id = :obra_id"
    if status:
        valor = str(status).strip().upper()
        if valor not in STATUS:
            raise ErroDeValidacao(f"status desconhecido: {status!r}", campo="status")
        params["status"] = valor
        sql += " AND m.status = :status"
    sql += " ORDER BY c.cpf, m.timestamp_servidor LIMIT :lim"
    params["lim"] = MAX_LINHAS + 1
    linhas = db.todos(conn, sql, **params)
    truncado = len(linhas) > MAX_LINHAS
    linhas = linhas[:MAX_LINHAS]

    dias: "OrderedDict[tuple, dict]" = OrderedDict()
    for m in linhas:
        chave = (m["cpf"], m["data_referencia"])
        d = dias.setdefault(chave, {
            "cpf": m["cpf"], "nome": m["colaborador_nome"],
            "data_referencia": m["data_referencia"].isoformat(),
            "marcacoes": [], "obras": [], "em_analise": 0,
        })
        d["marcacoes"].append(m["nsr"])
        codigo = m["obra_codigo"]
        if codigo not in d["obras"]:
            d["obras"].append(codigo)
        if m["status"] == "EM_ANALISE":
            d["em_analise"] += 1
    return {
        "periodo": {"inicio": inicio.isoformat(), "fim": fim.isoformat()},
        "quantidade": len(linhas),
        "truncado": truncado,
        "marcacoes": [marcacoes.para_json(m) for m in linhas],
        "dias": list(dias.values()),
    }
