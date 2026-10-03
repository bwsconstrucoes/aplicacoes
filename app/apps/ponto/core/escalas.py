# -*- coding: utf-8 -*-
"""
Escalas de trabalho — configuráveis pela gestão (decisão do dono, 03/10/2026:
"horários configuráveis, deve ser assim mesmo").

Uma escala é um modelo ("Obra seg–qui 7–17, sex 7–16"); cada pessoa recebe uma
escala com DATA DE INÍCIO. Trocar a escala de alguém não reescreve o passado: o
espelho de agosto continua usando a escala que valia em agosto.

O formato guardado é o que a pessoa lê ("07:00"), e a conversão para minutos
— que é o que o cálculo do dia usa — acontece aqui, num lugar só.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao, NaoEncontrado
from . import apuracao

TIPOS = ("SEMANAL", "CICLO_12X36")
DIAS = ("segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo")
_HORA = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _minutos(texto: str, campo: str) -> int:
    valor = str(texto or "").strip()
    if not _HORA.match(valor):
        raise ErroDeValidacao(f"horário inválido: {texto!r} (use HH:MM)", campo=campo)
    h, m = valor.split(":")
    return int(h) * 60 + int(m)


def _periodo(entrada: str, saida: str, campo: str) -> tuple[int, int]:
    """Converte um par "HH:MM"; saída menor que a entrada é no dia seguinte."""
    e, s = _minutos(entrada, campo), _minutos(saida, campo)
    if s <= e:
        s += 1440
    if s - e > 16 * 60:
        raise ErroDeValidacao(f"período de {entrada} a {saida} passa de 16 horas", campo=campo)
    return e, s


def validar_semana(semana) -> dict[str, list[list[str]]]:
    """Normaliza {"0": [["07:00","11:00"], …], …}. Confere sobreposição."""
    if isinstance(semana, str):
        try:
            semana = json.loads(semana or "{}")
        except json.JSONDecodeError as e:
            raise ErroDeValidacao("semana ilegível", campo="semana") from e
    if not isinstance(semana, dict):
        raise ErroDeValidacao("semana deve ser um dicionário por dia", campo="semana")
    saida: dict[str, list[list[str]]] = {}
    for chave, periodos in semana.items():
        try:
            dia = int(chave)
        except (TypeError, ValueError) as e:
            raise ErroDeValidacao(f"dia da semana inválido: {chave!r}", campo="semana") from e
        if not 0 <= dia <= 6:
            raise ErroDeValidacao(f"dia da semana inválido: {chave!r}", campo="semana")
        if not periodos:
            continue
        convertidos = sorted(_periodo(p[0], p[1], f"semana.{dia}") for p in periodos)
        for (_, s1), (e2, _) in zip(convertidos, convertidos[1:]):
            if e2 < s1:
                raise ErroDeValidacao(f"períodos se sobrepõem na {DIAS[dia]}", campo=f"semana.{dia}")
        saida[str(dia)] = [[str(p[0]).strip(), str(p[1]).strip()] for p in
                           sorted(periodos, key=lambda p: _minutos(p[0], "semana"))]
    return saida


def para_apuracao(escala: dict, data_base: Optional[dt.date] = None) -> apuracao.Escala:
    """A escala do banco → o objeto que o cálculo do dia entende."""
    if escala["tipo"] == "CICLO_12X36":
        return apuracao.Escala(
            tipo="CICLO_12X36",
            periodo_ciclo=_periodo(escala["ciclo_entrada"], escala["ciclo_saida"], "ciclo"),
            data_base=data_base)
    semana = escala["semana"] if isinstance(escala["semana"], dict) else json.loads(escala["semana"] or "{}")
    return apuracao.Escala(tipo="SEMANAL", semana={
        int(d): [_periodo(p[0], p[1], "semana") for p in periodos]
        for d, periodos in semana.items() if periodos})


def horas_semanais(escala: dict) -> float:
    """Para a tela: quantas horas a escala prevê numa semana (12x36 ≈ 42 h)."""
    if escala["tipo"] == "CICLO_12X36":
        e, s = _periodo(escala["ciclo_entrada"], escala["ciclo_saida"], "ciclo")
        return round((s - e) * 3.5 / 60, 1)
    esc = para_apuracao(escala)
    return round(sum(s - e for ps in esc.semana.values() for e, s in ps) / 60, 1)


# ---------------------------------------------------------------------------
# Banco
# ---------------------------------------------------------------------------
def listar(conn: Connection, *, so_ativas: bool = False) -> list[dict]:
    sql = "SELECT * FROM ponto.escalas" + (" WHERE ativo" if so_ativas else "") + " ORDER BY nome"
    linhas = db.todos(conn, sql)
    for e in linhas:
        e["horas_semanais"] = horas_semanais(e)
        e["pessoas"] = int(db.um(conn, """
            SELECT count(DISTINCT colaborador_id) AS n FROM ponto.colaborador_escalas ce
             WHERE ce.escala_id = :id AND ce.vigencia_inicio = (
                   SELECT max(x.vigencia_inicio) FROM ponto.colaborador_escalas x
                    WHERE x.colaborador_id = ce.colaborador_id AND x.vigencia_inicio <= CURRENT_DATE)
        """, id=e["id"])["n"])
    return linhas


def obter(conn: Connection, escala_id: int) -> dict:
    e = db.um(conn, "SELECT * FROM ponto.escalas WHERE id = :id", id=escala_id)
    if not e:
        raise NaoEncontrado("escala não encontrada")
    return e


def gravar(conn: Connection, dados: dict, escala_id: int | None = None) -> dict:
    nome = str(dados.get("nome") or "").strip()
    if len(nome) < 3:
        raise ErroDeValidacao("dê um nome à escala (ex.: Obra seg–sex 7h–17h)", campo="nome")
    tipo = str(dados.get("tipo") or "SEMANAL").strip().upper()
    if tipo not in TIPOS:
        raise ErroDeValidacao(f"tipo de escala desconhecido: {tipo}", campo="tipo")
    semana: dict = {}
    entrada = saida = None
    if tipo == "SEMANAL":
        semana = validar_semana(dados.get("semana") or {})
        if not semana:
            raise ErroDeValidacao("a escala precisa de ao menos um dia com horário", campo="semana")
    else:
        entrada, saida = str(dados.get("ciclo_entrada") or ""), str(dados.get("ciclo_saida") or "")
        _periodo(entrada, saida, "ciclo")
    params = dict(nome=nome, tipo=tipo, semana=json.dumps(semana), e=entrada, s=saida,
                  d=str(dados.get("descricao") or "")[:300],
                  a=bool(dados.get("ativo", True)))
    duplicada = db.um(conn, "SELECT id FROM ponto.escalas WHERE lower(nome) = lower(:n) "
                            "AND id <> COALESCE(:id, 0)", n=nome, id=escala_id)
    if duplicada:
        raise ErroDeValidacao("já existe uma escala com esse nome", campo="nome")
    if escala_id:
        obter(conn, escala_id)
        db.executar(conn, """
            UPDATE ponto.escalas SET nome = :nome, tipo = :tipo, semana = CAST(:semana AS jsonb),
                   ciclo_entrada = :e, ciclo_saida = :s, descricao = :d, ativo = :a,
                   atualizado_em = now() WHERE id = :id
        """, id=escala_id, **params)
        return obter(conn, escala_id)
    linha = db.um(conn, """
        INSERT INTO ponto.escalas (nome, tipo, semana, ciclo_entrada, ciclo_saida, descricao, ativo)
        VALUES (:nome, :tipo, CAST(:semana AS jsonb), :e, :s, :d, :a) RETURNING id
    """, **params)
    return obter(conn, int(linha["id"]))


def atribuir(conn: Connection, colaborador_id: int, escala_id: int, vigencia_inicio,
             *, ciclo_data_base=None, por: str = "") -> None:
    escala = obter(conn, escala_id)
    if not escala["ativo"]:
        raise ErroDeValidacao("escala inativa", campo="escala_id")
    try:
        inicio = dt.date.fromisoformat(str(vigencia_inicio))
    except ValueError as e:
        raise ErroDeValidacao("data de início ilegível (AAAA-MM-DD)", campo="vigencia_inicio") from e
    base = None
    if escala["tipo"] == "CICLO_12X36":
        try:
            base = dt.date.fromisoformat(str(ciclo_data_base or vigencia_inicio))
        except ValueError as e:
            raise ErroDeValidacao("data-base do ciclo ilegível", campo="ciclo_data_base") from e
    db.executar(conn, """
        INSERT INTO ponto.colaborador_escalas (colaborador_id, escala_id, vigencia_inicio,
                                               ciclo_data_base, definido_por)
        VALUES (:c, :e, :i, :b, :p)
        ON CONFLICT (colaborador_id, vigencia_inicio) DO UPDATE
           SET escala_id = :e, ciclo_data_base = :b, definido_por = :p, criado_em = now()
    """, c=colaborador_id, e=escala_id, i=inicio, b=base, p=(por or "")[:120])


def historico_da_pessoa(conn: Connection, colaborador_id: int) -> list[dict]:
    return db.todos(conn, """
        SELECT ce.*, e.nome AS escala_nome, e.tipo AS escala_tipo
          FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
         WHERE ce.colaborador_id = :c ORDER BY ce.vigencia_inicio DESC
    """, c=colaborador_id)


class EscalasDaPessoa:
    """As vigências de uma pessoa, carregadas UMA vez para um período inteiro."""

    def __init__(self, conn: Connection, colaborador_id: int):
        self.vigencias = db.todos(conn, """
            SELECT ce.vigencia_inicio, ce.ciclo_data_base, e.*
              FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
             WHERE ce.colaborador_id = :c ORDER BY ce.vigencia_inicio
        """, c=colaborador_id)
        self._cache: dict = {}

    def no_dia(self, dia: dt.date) -> tuple[Optional[apuracao.Escala], Optional[dict]]:
        atual = None
        for v in self.vigencias:
            if v["vigencia_inicio"] <= dia:
                atual = v
            else:
                break
        if atual is None:
            return None, None
        chave = (atual["id"], atual["vigencia_inicio"])
        if chave not in self._cache:
            self._cache[chave] = para_apuracao(atual, atual["ciclo_data_base"])
        return self._cache[chave], atual
