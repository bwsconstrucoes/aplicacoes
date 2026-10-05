# -*- coding: utf-8 -*-
"""
Competência = o mês da folha. Fechada, ela trava o ponto daquele mês: nenhuma
ocorrência nova, nenhuma decisão sobre batida, nenhum lançamento no banco de
horas. Reabrir é possível, com nome, data e motivo — fechar não pode virar
"apaga e faz de novo" escondido.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao


def primeiro_dia(valor) -> dt.date:
    """Aceita date, 'AAAA-MM' ou 'AAAA-MM-DD'."""
    if isinstance(valor, dt.date):
        return valor.replace(day=1)
    texto = str(valor or "").strip()
    try:
        if len(texto) == 7:
            return dt.date.fromisoformat(texto + "-01")
        return dt.date.fromisoformat(texto).replace(day=1)
    except ValueError as e:
        raise ErroDeValidacao("competência ilegível (use AAAA-MM)", campo="competencia") from e


def ultimo_dia(competencia: dt.date) -> dt.date:
    proximo = (competencia.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return proximo - dt.timedelta(days=1)


def situacao(conn: Connection, dia) -> dict:
    comp = primeiro_dia(dia)
    linha = db.um(conn, "SELECT * FROM ponto.competencias WHERE competencia = :c", c=comp)
    return linha or {"competencia": comp, "situacao": "ABERTA"}


def esta_fechada(conn: Connection, dia) -> bool:
    return situacao(conn, dia)["situacao"] == "FECHADA"


def exigir_aberta(conn: Connection, *dias) -> None:
    for dia in dias:
        if dia is None:
            continue
        if esta_fechada(conn, dia):
            comp = primeiro_dia(dia)
            raise ErroDeValidacao(
                f"o mês {comp:%m/%Y} está fechado; peça a quem fechou para reabrir", campo="data")


def fechar(conn: Connection, competencia, por: str) -> dict:
    comp = primeiro_dia(competencia)
    db.executar(conn, """
        INSERT INTO ponto.competencias (competencia, situacao, fechada_por, fechada_em)
        VALUES (:c, 'FECHADA', :p, now())
        ON CONFLICT (competencia) DO UPDATE SET situacao = 'FECHADA', fechada_por = :p,
               fechada_em = now()
    """, c=comp, p=por[:120])
    return situacao(conn, comp)


def reabrir(conn: Connection, competencia, por: str, motivo: str) -> dict:
    comp = primeiro_dia(competencia)
    if len((motivo or "").strip()) < 10:
        raise ErroDeValidacao("diga por que está reabrindo (ao menos 10 letras)", campo="motivo")
    if not esta_fechada(conn, comp):
        raise ErroDeValidacao("este mês não está fechado", campo="competencia")
    db.executar(conn, """
        UPDATE ponto.competencias SET situacao = 'ABERTA', reaberta_por = :p, reaberta_em = now(),
               motivo = :m WHERE competencia = :c
    """, c=comp, p=por[:120], m=motivo.strip()[:500])
    return situacao(conn, comp)


def listar(conn: Connection, quantos: int = 12) -> list[dict]:
    from .. import horario
    hoje = horario.hoje().replace(day=1)
    meses = []
    atual = hoje
    for _ in range(quantos):
        meses.append(atual)
        atual = (atual - dt.timedelta(days=1)).replace(day=1)
    registradas = {r["competencia"]: r for r in db.todos(
        conn, "SELECT * FROM ponto.competencias WHERE competencia >= :m", m=meses[-1])}
    return [registradas.get(m, {"competencia": m, "situacao": "ABERTA"}) for m in meses]
