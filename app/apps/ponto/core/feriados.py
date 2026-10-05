# -*- coding: utf-8 -*-
"""Feriados por abrangência: nacional, estadual (UF da obra), municipal
(município da obra) ou de uma obra só. A obra que vale é a do dia."""
from __future__ import annotations

import datetime as dt
import unicodedata

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao, NaoEncontrado

ABRANGENCIAS = ("NACIONAL", "ESTADUAL", "MUNICIPAL", "OBRA")


def _sem_acento(texto: str | None) -> str:
    base = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode()
    return " ".join(base.upper().split())


def listar(conn: Connection, *, ano: int | None = None) -> list[dict]:
    sql = """SELECT f.*, o.codigo AS obra_codigo FROM ponto.feriados f
               LEFT JOIN public.obras o ON o.id = f.obra_id"""
    params = {}
    if ano:
        sql += " WHERE EXTRACT(YEAR FROM f.data) = :a"
        params["a"] = int(ano)
    return db.todos(conn, sql + " ORDER BY f.data, f.abrangencia", **params)


def gravar(conn: Connection, dados: dict) -> dict:
    try:
        data = dt.date.fromisoformat(str(dados.get("data")))
    except ValueError as e:
        raise ErroDeValidacao("data ilegível (AAAA-MM-DD)", campo="data") from e
    nome = str(dados.get("nome") or "").strip()
    if not nome:
        raise ErroDeValidacao("dê o nome do feriado", campo="nome")
    abr = str(dados.get("abrangencia") or "NACIONAL").upper()
    if abr not in ABRANGENCIAS:
        raise ErroDeValidacao("abrangência inválida", campo="abrangencia")
    uf = (str(dados.get("uf") or "").strip().upper() or None)
    municipio = (str(dados.get("municipio") or "").strip() or None)
    obra_id = dados.get("obra_id") or None
    if abr == "ESTADUAL" and not uf:
        raise ErroDeValidacao("feriado estadual precisa da UF", campo="uf")
    if abr == "MUNICIPAL" and not (uf and municipio):
        raise ErroDeValidacao("feriado municipal precisa da UF e do município", campo="municipio")
    if abr == "OBRA" and not obra_id:
        raise ErroDeValidacao("feriado de obra precisa da obra", campo="obra_id")
    linha = db.um(conn, """
        INSERT INTO ponto.feriados (data, nome, abrangencia, uf, municipio, obra_id)
        VALUES (:d, :n, :a, :uf, :m, :o)
        ON CONFLICT DO NOTHING RETURNING id
    """, d=data, n=nome[:120], a=abr, uf=(uf if abr in ("ESTADUAL", "MUNICIPAL") else None),
         m=(municipio if abr == "MUNICIPAL" else None),
         o=(int(obra_id) if abr == "OBRA" else None))
    if not linha:
        raise ErroDeValidacao("esse feriado já está cadastrado", campo="data")
    return db.um(conn, "SELECT * FROM ponto.feriados WHERE id = :id", id=linha["id"])


def apagar(conn: Connection, feriado_id: int) -> None:
    if not db.executar(conn, "DELETE FROM ponto.feriados WHERE id = :id", id=feriado_id):
        raise NaoEncontrado("feriado não encontrado")


class Calendario:
    """Os feriados de um período, carregados uma vez, respondendo por obra."""

    def __init__(self, conn: Connection, inicio: dt.date, fim: dt.date):
        self.linhas = db.todos(conn, "SELECT * FROM ponto.feriados WHERE data BETWEEN :i AND :f",
                               i=inicio, f=fim)
        self._obras: dict[int, dict] = {}
        self._conn = conn

    def _obra(self, obra_id: int | None) -> dict:
        if not obra_id:
            return {}
        if obra_id not in self._obras:
            self._obras[obra_id] = db.um(self._conn, "SELECT id, uf, municipio FROM public.obras "
                                                     "WHERE id = :id", id=obra_id) or {}
        return self._obras[obra_id]

    def feriado(self, dia: dt.date, obra_id: int | None) -> str | None:
        """O nome do feriado que vale neste dia para esta obra, ou None."""
        obra = self._obra(obra_id)
        uf, municipio = _sem_acento(obra.get("uf")), _sem_acento(obra.get("municipio"))
        for f in self.linhas:
            if f["data"] != dia:
                continue
            a = f["abrangencia"]
            if (a == "NACIONAL"
                    or (a == "ESTADUAL" and uf and _sem_acento(f["uf"]) == uf)
                    or (a == "MUNICIPAL" and municipio and _sem_acento(f["uf"]) == uf
                        and _sem_acento(f["municipio"]) == municipio)
                    or (a == "OBRA" and obra_id and f["obra_id"] == obra_id)):
                return f["nome"]
        return None
