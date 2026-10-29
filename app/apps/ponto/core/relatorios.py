# -*- coding: utf-8 -*-
"""
OS RELATÓRIOS DO PONTO (26/10/2026).

O dono: *"a gente não tem nenhum relatório nesse sistema novo. A gente precisa de
relatórios, por exemplo, de faltas, de ausências justificadas — dentre elas as por lei,
além de atestados médicos —, de horas extras (…) e de banco de horas. Em relação ao
relatório de faltas, depois a gente vai gerar arquivo .ps para a contabilidade, das
faltas e das horas extras — mas a princípio faz só o relatório."*

AS CONTAS SÃO AS DO ESPELHO, sem conta nova: falta é o dia que o espelho chama de falta
(sem justificativa); hora extra é a do espelho, dia a dia; ausência justificada é o
pedido APROVADO (atestado, licença da lei, férias, afastamento, abono — viagem e
dispensa inclusive —, folga do banco e compensação); o banco é o saldo de `banco.py`.
Por isso o relatório bate com o espelho de cada pessoa e com o Quadro.

QUEM ENTRA: quem está ativo e — para que o desligado no meio do mês não suma do
relatório do mês — quem tem batida ou pedido aprovado no período. O recorte por obra
de quem pergunta e os filtros de cadastro são os do Quadro.

UM DE CADA VEZ, como o Quadro: o relatório monta o espelho de muita gente; com outro
montando, a tela diz "tente em instantes" em vez de dobrar o trabalho do servidor.
"""
from __future__ import annotations

import datetime as dt
import threading
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao
from . import banco, cadastros, espelho, ocorrencias, quadro, vigencia
from .ocorrencias import Quem

MAX_DIAS = 62
TIPOS = {"faltas": "Faltas", "justificadas": "Ausências justificadas", "extras": "Horas extras",
         "banco": "Banco de horas"}
TIPOS_JUSTIFICADOS = ("ATESTADO", "LICENCA", "FERIAS", "AFASTAMENTO", "ABONO", "FOLGA_BANCO", "COMPENSACAO")

_uma_por_vez = threading.Semaphore(1)
Ocupado = quadro.Ocupado


def periodo(de, ate) -> tuple[dt.date, dt.date]:
    inicio = quadro._data(de, "de")
    fim = quadro._data(ate, "ate") if ate else inicio
    if fim < inicio:
        raise ErroDeValidacao("o fim é antes do começo", campo="ate")
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ErroDeValidacao(f"no máximo {MAX_DIAS} dias de uma vez", campo="ate")
    return inicio, fim


def _pessoas(conn: Connection, quem: Quem, f: dict, inicio: dt.date, fim: dt.date) -> list[dict]:
    todas = cadastros.listar_colaboradores(conn, so_ativos=False)
    com_algo = {int(l["c"]) for l in db.todos(conn, """
        SELECT colaborador_id AS c FROM ponto.marcacoes
         WHERE data_referencia BETWEEN :i AND :f AND status <> 'REJEITADA'
        UNION SELECT colaborador_id FROM ponto.ocorrencias
         WHERE status = 'APROVADA' AND data_fim >= :i AND data_inicio <= :f""", i=inicio, f=fim)}
    ativas = {int(p["id"]) for p in cadastros.listar_colaboradores(conn, so_ativos=True)}
    pessoas = [p for p in todas if int(p["id"]) in ativas or int(p["id"]) in com_algo]
    from . import filtro_pessoas
    return filtro_pessoas.aplicar(conn, quem, pessoas, f, inicio, min(fim, horario.hoje()))[0]


def _base(p: dict) -> dict:
    return {"colaborador_id": int(p["id"]), "nome": p["nome"], "cpf": p.get("cpf"), "funcao": p.get("funcao"),
            "obra": p.get("obra_codigo")}


def _espelhos(conn: Connection, pessoas: list[dict], inicio: dt.date, fim: dt.date):
    """O espelho de cada pessoa no período, lido de uma vez (o mesmo Lote do Quadro)."""
    if not pessoas:
        return
    lote = espelho.Lote(conn, pessoas, inicio, fim)
    for p in pessoas:
        yield p, espelho.montar(conn, int(p["id"]), inicio, fim, lote=lote)


def faltas(conn: Connection, pessoas: list[dict], inicio: dt.date, fim: dt.date) -> dict:
    linhas, por_pessoa = [], []
    for p, esp in _espelhos(conn, pessoas, inicio, fim):
        dias = [d for d in esp["dias"] if d["situacao"] == "FALTA" and not d.get("ocorrencia")]
        for d in dias:
            linhas.append({**_base(p), "data": d["data"], "dia_semana": d["dia_semana"], "escala": d.get("escala"),
                           "previsto": d["previsto"],
                           "pendente": ", ".join(x["rotulo"] for x in d.get("pendentes") or [])})
        if dias:
            por_pessoa.append({**_base(p), "faltas": len(dias), "previsto": sum(d["previsto"] for d in dias)})
    por_pessoa.sort(key=lambda x: (-x["faltas"], x["nome"]))
    return {"linhas": linhas, "por_pessoa": por_pessoa,
            "totais": {"faltas": len(linhas), "pessoas": len(por_pessoa), "previsto": sum(x["previsto"] for x in linhas)}}


def _dias_no_periodo(a: dt.date, b: dt.date, inicio: dt.date, fim: dt.date) -> int:
    """PURA. Quantos dias do pedido (a..b) caem no período (inicio..fim)."""
    de, ate = max(a, inicio), min(b, fim)
    return (ate - de).days + 1 if ate >= de else 0


def justificadas(conn: Connection, quem: Quem, pessoas: list[dict], inicio: dt.date, fim: dt.date,
                 tipos: Optional[list[str]] = None) -> dict:
    ids = [int(p["id"]) for p in pessoas]
    if not ids:
        return {"linhas": [], "por_tipo": [], "totais": {"pedidos": 0, "dias": 0, "pessoas": 0}}
    quer = [t for t in (tipos or []) if t in TIPOS_JUSTIFICADOS] or list(TIPOS_JUSTIFICADOS)
    por_id = {int(p["id"]): p for p in pessoas}
    ocorrencias._nomes_das_licencas(conn)
    from . import tipos_ausencia
    nao_abonam = tipos_ausencia.nao_abonam(conn)      # o tipo que não abona não conta dia abonado
    linhas = []
    for o in db.todos(conn, ocorrencias._SQL + """
         WHERE oc.colaborador_id = ANY(:ids) AND oc.status = 'APROVADA' AND oc.tipo = ANY(:tipos)
           AND oc.data_fim >= :i AND oc.data_inicio <= :f
         ORDER BY oc.data_inicio, c.nome""", ids=ids, tipos=quer, i=inicio, f=fim):
        j = ocorrencias._para_json(o, quem)
        p = por_id[int(o["colaborador_id"])]
        linhas.append({**_base(p), "pedido_id": j["id"], "tipo": j["tipo"], "rotulo": j["rotulo"],
                       "de": j["data_inicio"], "ate": j["data_fim"],
                       # a declaração que abonou só o período (ou nada) não abona dia: abona horas
                       "dias": (0 if o.get("subtipo") in ("DECL_PERIODO", "DECL_NADA", "DECLARACAO")
                                or (o["tipo"], o.get("subtipo")) in nao_abonam
                                else _dias_no_periodo(o["data_inicio"], o["data_fim"], inicio, fim)),
                       "horas": int(o.get("minutos") or 0) if o.get("subtipo") == "DECL_PERIODO" else 0,
                       "periodo": j.get("declaracao_periodo"),
                       "cid": j.get("cid"), "tem_documento": j["tem_documento"], "documento_id": j.get("documento_id"),
                       "sigiloso": j["sigiloso"],
                       "lancado_por": ocorrencias.pedido_por(j["origem"], j["solicitado_por"], j["colaborador"])["nome"],
                       "aprovado_por": j.get("dp") or j.get("supervisor"), "descricao": j.get("descricao")})
    tipos_vistos: dict[str, dict] = {}
    for l in linhas:
        t = tipos_vistos.setdefault(l["rotulo"], {"rotulo": l["rotulo"], "pedidos": 0, "dias": 0, "pessoas": set()})
        t["pedidos"] += 1
        t["dias"] += l["dias"]
        t["pessoas"].add(l["colaborador_id"])
    for l in linhas:
        tipos_vistos[l["rotulo"]]["horas"] = tipos_vistos[l["rotulo"]].get("horas", 0) + l["horas"]
    por_tipo = sorted(({**t, "pessoas": len(t["pessoas"])} for t in tipos_vistos.values()), key=lambda x: -x["dias"])
    return {"linhas": linhas, "por_tipo": por_tipo,
            "totais": {"pedidos": len(linhas), "dias": sum(l["dias"] for l in linhas),
                       "pessoas": len({l["colaborador_id"] for l in linhas})}}


def tipo_do_dia(d: dict) -> str:
    """PURA. Dia de trabalho, descanso ou feriado — é o que muda o adicional da hora extra."""
    if d.get("feriado"):
        return "FERIADO"
    if d.get("situacao") == "DESCANSO" or not d.get("previstas"):
        return "DESCANSO"
    return "UTIL"


def extras(conn: Connection, pessoas: list[dict], inicio: dt.date, fim: dt.date) -> dict:
    linhas, por_pessoa = [], []
    for p, esp in _espelhos(conn, pessoas, inicio, fim):
        soma = {"UTIL": 0, "DESCANSO": 0, "FERIADO": 0, "noturno": 0, "dias": 0}
        for d in esp["dias"]:
            if not d.get("extra"):
                continue
            t = tipo_do_dia(d)
            soma[t] += d["extra"]
            soma["noturno"] += d.get("noturno") or 0
            soma["dias"] += 1
            linhas.append({**_base(p), "data": d["data"], "dia_semana": d["dia_semana"], "tipo_dia": t,
                           "previsto": d["previsto"], "trabalhado": d["trabalhado"], "extra": d["extra"],
                           "noturno": d.get("noturno") or 0, "feriado": d.get("feriado"),
                           "batidas": [b["hora"] for b in d["batidas"]]})
        if soma["dias"]:
            por_pessoa.append({**_base(p), "dias": soma["dias"], "util": soma["UTIL"], "descanso": soma["DESCANSO"],
                               "feriado": soma["FERIADO"], "noturno": soma["noturno"],
                               "total": soma["UTIL"] + soma["DESCANSO"] + soma["FERIADO"]})
    por_pessoa.sort(key=lambda x: (-x["total"], x["nome"]))
    return {"linhas": linhas, "por_pessoa": por_pessoa,
            "totais": {k: sum(x[k] for x in por_pessoa) for k in ("util", "descanso", "feriado", "noturno", "total")}
            | {"pessoas": len(por_pessoa)}}


def banco_de_horas(conn: Connection, pessoas: list[dict], fim: dt.date) -> dict:
    linhas = []
    for p in pessoas:
        if p.get("regime_banco") in (None, "SEM_BANCO"):
            continue
        s = banco.saldo(conn, int(p["id"]), ate=min(fim, horario.hoje() - dt.timedelta(days=1)))
        venc = sorted(s.get("vencimentos") or [], key=lambda v: v["vence_em"])
        linhas.append({**_base(p), "regime": s["regime_rotulo"], "inicio": s.get("inicio"), "saldo": s["saldo"],
                       "vence_primeiro": venc[0] if venc else None, "aviso": s.get("aviso")})
    linhas.sort(key=lambda x: x["nome"])
    return {"linhas": linhas, "totais": {"pessoas": len(linhas), "positivo": sum(max(0, x["saldo"]) for x in linhas),
                                         "negativo": sum(min(0, x["saldo"]) for x in linhas)}}


def montar(conn: Connection, quem: Quem, tipo: str, de, ate, f: Optional[dict] = None) -> dict:
    tipo = str(tipo or "").strip().lower()
    if tipo not in TIPOS:
        raise ErroDeValidacao("escolha o relatório", campo="tipo")
    f = f or {}
    inicio, fim = periodo(de, ate)
    vale = vigencia.inicio(conn)
    if not _uma_por_vez.acquire(blocking=False):
        raise Ocupado("outro relatório está sendo montado agora — tente de novo em instantes")
    try:
        pessoas = _pessoas(conn, quem, f, inicio, fim)
        if tipo == "faltas":
            corpo = faltas(conn, pessoas, max(inicio, vale), fim) if fim >= vale else faltas(conn, [], inicio, fim)
        elif tipo == "extras":
            corpo = extras(conn, pessoas, max(inicio, vale), fim) if fim >= vale else extras(conn, [], inicio, fim)
        elif tipo == "justificadas":
            corpo = justificadas(conn, quem, pessoas, inicio, fim, f.get("tipos_justificados"))
        else:
            corpo = banco_de_horas(conn, pessoas, fim)
    finally:
        _uma_por_vez.release()
    return {"tipo": tipo, "titulo": TIPOS[tipo], "de": inicio.isoformat(), "ate": fim.isoformat(),
            "vale_desde": vale.isoformat(), "pessoas_no_recorte": len(pessoas),
            "funcoes": sorted({p.get("funcao") for p in pessoas if p.get("funcao")}), **corpo}
