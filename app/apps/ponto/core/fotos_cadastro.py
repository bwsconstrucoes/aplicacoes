# -*- coding: utf-8 -*-
"""
AS FOTOS DE CADASTRO, PARA REGULARIZAR (26/10/2026).

O dono: *"teve um pessoal que começou a utilizar e a primeira foto foi uma foto de
teste. E quem bateu foi outra pessoa, ou uma foto do chão (…) o mosaico está se
perdendo nisso. Como é que a gente pode regularizar essas fotos de cadastro de forma
fácil?"*

A foto de cadastro nasce sozinha: é a PRIMEIRA FOTO BOA da pessoa (rosto.py). Se essa
primeira foto for de teste, de outra pessoa ou do chão, todas as seguintes passam a
"não conferir" — e é por aí que a errada se denuncia. Esta tela põe, por pessoa, a foto
de cadastro ao lado das últimas fotos das batidas, com o resultado da conferência de
cada uma, e diz quem PROVAVELMENTE está com a foto errada: das últimas conferidas, a
maioria deu "outra pessoa". Trocar é um clique na foto certa; tirar a foto faz a
próxima foto boa virar a de cadastro.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from ..erros import NaoEncontrado

ULTIMAS = 6                 # fotos de batida mostradas por pessoa
OLHAR = 5                   # das últimas conferidas, quantas olhar para a suspeita
DIAS = 45

FILTROS = ("SUSPEITA", "AUTOMATICA", "SEM_FOTO", "TODAS")


def suspeita(resultados: list[str]) -> bool:
    """PURA. A foto de cadastro provavelmente está errada? Das últimas conferidas (as
    mais novas primeiro), ao menos 2 deram "outra pessoa" e elas são a maioria."""
    olhadas = [r for r in resultados if r in ("MESMA_PESSOA", "OUTRA_PESSOA")][:OLHAR]
    outras = sum(1 for r in olhadas if r == "OUTRA_PESSOA")
    return outras >= 2 and outras * 2 > len(olhadas)


def listar(conn: Connection, pessoas: list[dict], filtro: str = "SUSPEITA") -> dict:
    filtro = filtro if filtro in FILTROS else "SUSPEITA"
    ids = [int(p["id"]) for p in pessoas]
    if not ids:
        return {"pessoas": [], "contagem": {f: 0 for f in FILTROS}}
    cadastral = {int(l["colaborador_id"]): l["foto_cadastral_id"] for l in db.todos(conn, """
        SELECT colaborador_id, foto_cadastral_id FROM ponto.colaborador_config
         WHERE colaborador_id = ANY(:ids)""", ids=ids)}
    # a foto de cadastro que nasceu sozinha (e ninguém trocou): a batida dela ficou
    # marcada "virou cadastral" com a frase da escolha automática
    automaticas = {int(l["colaborador_id"]) for l in db.todos(conn, """
        SELECT m.colaborador_id FROM ponto.conferencias_rosto cr
          JOIN ponto.marcacoes m ON m.id = cr.marcacao_id
          JOIN ponto.colaborador_config pc ON pc.colaborador_id = m.colaborador_id AND pc.foto_cadastral_id = m.foto_id
         WHERE m.colaborador_id = ANY(:ids) AND cr.resultado = 'VIROU_CADASTRAL'
           AND cr.detalhe LIKE :automatica""", ids=ids, automatica="foto cadastral automática%")}
    fotos: dict[int, list[dict]] = {}
    for l in db.todos(conn, """
        SELECT * FROM (
          SELECT m.id, m.colaborador_id, m.timestamp_servidor, m.foto_id, o.codigo AS obra,
                 cr.resultado, cr.semelhanca,
                 row_number() OVER (PARTITION BY m.colaborador_id ORDER BY m.timestamp_servidor DESC) AS n
            FROM ponto.marcacoes m
            JOIN public.obras o ON o.id = m.obra_id
            LEFT JOIN ponto.conferencias_rosto cr ON cr.marcacao_id = m.id
           WHERE m.colaborador_id = ANY(:ids) AND m.foto_id IS NOT NULL
             AND m.timestamp_servidor > now() - make_interval(days => :dias)) x
         WHERE n <= :ate""", ids=ids, dias=DIAS, ate=max(ULTIMAS, OLHAR * 3)):
        fotos.setdefault(int(l["colaborador_id"]), []).append(l)
    from .. import horario
    saida, contagem = [], {f: 0 for f in FILTROS}
    for p in pessoas:
        cid = int(p["id"])
        lista = fotos.get(cid, [])
        if not lista and not cadastral.get(cid):
            continue                     # nunca bateu com foto: nada a regularizar
        tem = bool(cadastral.get(cid))
        susp = tem and suspeita([l["resultado"] for l in lista])
        auto = tem and cid in automaticas
        marcas = {"TODAS"} | ({"SUSPEITA"} if susp else set()) | ({"AUTOMATICA"} if auto else set()) \
            | (set() if tem else {"SEM_FOTO"})
        for m in marcas:
            contagem[m] += 1
        if filtro not in marcas:
            continue
        olhadas = [l for l in lista if l["resultado"] in ("MESMA_PESSOA", "OUTRA_PESSOA")][:OLHAR]
        saida.append({
            "id": cid, "nome": p["nome"], "funcao": p.get("funcao"), "obra": p.get("obra_codigo"),
            "foto_cadastral_id": cadastral.get(cid), "tem_foto_cadastral": tem,
            "automatica": auto, "suspeita": susp,
            "conferidas": len(olhadas), "outra_pessoa": sum(1 for l in olhadas if l["resultado"] == "OUTRA_PESSOA"),
            "fotos": [{"marcacao_id": l["id"], "quando": horario.texto(l["timestamp_servidor"]), "obra": l["obra"],
                       "resultado": l["resultado"],
                       "semelhanca": float(l["semelhanca"]) if l["semelhanca"] is not None else None,
                       "e_a_cadastral": l["foto_id"] == cadastral.get(cid)} for l in lista[:ULTIMAS]],
        })
    saida.sort(key=lambda x: (not x["suspeita"], -x["outra_pessoa"], x["nome"]))
    return {"pessoas": saida, "contagem": contagem, "filtro": filtro}


def tirar(conn: Connection, colaborador_id: int) -> None:
    """Tira a foto de cadastro: a próxima foto boa da pessoa vira a nova (rosto.py)."""
    if not db.executar(conn, """UPDATE ponto.colaborador_config SET foto_cadastral_id = NULL, atualizado_em = now()
                                 WHERE colaborador_id = :c AND foto_cadastral_id IS NOT NULL""", c=colaborador_id):
        raise NaoEncontrado("esta pessoa não tem foto de cadastro")


