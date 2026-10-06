# -*- coding: utf-8 -*-
"""
AS LICENÇAS QUE A LEI GARANTE, numa lista (`ponto.tipos_licenca`, migração 006).

Pedido do dono, 06/10/2026: *"precisa ter uma base de dados de afastamentos
permitidos por lei para facilitar a inclusão."*

Quem registra a licença escolhe o TIPO (casamento, luto, paternidade, doação de
sangue…) em vez de escrever: o último dia sai sozinho dos dias da lei, o
documento exigido aparece na tela, e o servidor confere —
  · dias além do que a lei dá → recusado (a convenção pode dar mais: os dias se
    ajustam na Configuração);
  · tipo com limite por ano (doação de sangue: 1 a cada 12 meses) já usado →
    recusado, dizendo quando foi;
  · documento obrigatório sem documento → recusado.

Atestado médico, férias, maternidade e afastamento do INSS NÃO estão aqui: têm
caminho próprio (atestado com documento; os demais lançados pelo DP).
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "tipos_licenca", "codigo")


def listar(conn: Connection, *, so_ativos: bool = True) -> list[dict]:
    if not disponivel(conn):
        return []
    sql = "SELECT * FROM ponto.tipos_licenca" + (" WHERE ativo" if so_ativos else "") + " ORDER BY ordem, nome"
    return [para_json(l) for l in db.todos(conn, sql)]


def para_json(l: dict) -> dict:
    return {"codigo": l["codigo"], "nome": l["nome"], "base_legal": l["base_legal"], "dias": l["dias"],
            "limite_ano": l["limite_ano"], "exige_documento": bool(l["exige_documento"]),
            "documento": l["documento"], "ativo": bool(l["ativo"])}


def obter(conn: Connection, codigo: str) -> Optional[dict]:
    if not disponivel(conn):
        return None
    return db.um(conn, "SELECT * FROM ponto.tipos_licenca WHERE codigo = :c", c=str(codigo or "").upper())


def conferir(conn: Connection, *, colaborador_id: int, subtipo: str, inicio: dt.date, fim: dt.date,
             tem_documento: bool) -> dict:
    """Levanta ErroDeValidacao com a frase que a pessoa lê; devolve o tipo."""
    t = obter(conn, subtipo)
    if not t or not t["ativo"]:
        raise ErroDeValidacao("escolha o tipo de licença na lista", campo="subtipo")
    dias = (fim - inicio).days + 1
    if t["dias"] and dias > t["dias"]:
        raise ErroDeValidacao(f"{t['nome'].split(' (')[0].lower()}: a lei dá {t['dias']} dia(s) — "
                              f"o pedido tem {dias}", campo="data_fim")
    if t["exige_documento"] and not tem_documento:
        raise ErroDeValidacao(f"anexe o documento ({t['documento']})", campo="documento")
    if t["limite_ano"]:
        usados = db.todos(conn, """
            SELECT data_inicio FROM ponto.ocorrencias
             WHERE colaborador_id = :c AND tipo = 'LICENCA' AND subtipo = :s
               AND status NOT IN ('NEGADA', 'CANCELADA') AND data_inicio > :desde
             ORDER BY data_inicio DESC""", c=colaborador_id, s=t["codigo"],
            desde=inicio - dt.timedelta(days=365))
        if len(usados) >= t["limite_ano"]:
            ultima = usados[0]["data_inicio"]
            raise ErroDeValidacao(f"{t['nome'].split(' (')[0].lower()}: o limite é {t['limite_ano']} vez(es) "
                                  f"em 12 meses, e já houve em {ultima:%d/%m/%Y}", campo="subtipo")
    return t


def gravar(conn: Connection, codigo: str, dados: dict, por: str) -> dict:
    """Ajuste da Configuração: dias, limite, documento, ativo — ou um tipo novo."""
    codigo = "".join(ch for ch in str(codigo or "").upper() if ch.isalnum() or ch == "_")[:40]
    if not codigo:
        raise ErroDeValidacao("diga o código do tipo", campo="codigo")
    nome = str(dados.get("nome") or "").strip()

    def inteiro(campo, minimo, maximo):
        v = dados.get(campo)
        if v in (None, ""):
            return None
        try:
            n = int(v)
        except (TypeError, ValueError):
            raise ErroDeValidacao(f"{campo}: só números", campo=campo)
        if not minimo <= n <= maximo:
            raise ErroDeValidacao(f"{campo}: entre {minimo} e {maximo}", campo=campo)
        return n
    existe = obter(conn, codigo)
    if not existe and not nome:
        raise ErroDeValidacao("diga o nome do tipo novo", campo="nome")
    db.executar(conn, """
        INSERT INTO ponto.tipos_licenca (codigo, nome, base_legal, dias, limite_ano, exige_documento,
                                         documento, ativo, atualizado_por)
        VALUES (:c, :n, :b, :d, :l, :e, :doc, :a, :p)
        ON CONFLICT (codigo) DO UPDATE SET
            nome = COALESCE(NULLIF(:n, ''), ponto.tipos_licenca.nome),
            base_legal = :b, dias = :d, limite_ano = :l, exige_documento = :e, documento = :doc,
            ativo = :a, atualizado_por = :p, atualizado_em = now()
    """, c=codigo, n=nome, b=str(dados.get("base_legal") or "")[:200], d=inteiro("dias", 1, 365),
         l=inteiro("limite_ano", 1, 50), e=bool(dados.get("exige_documento")),
         doc=str(dados.get("documento") or "")[:200], a=bool(dados.get("ativo", True)), p=por[:120])
    return para_json(obter(conn, codigo))
