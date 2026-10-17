# -*- coding: utf-8 -*-
"""
A QUALIDADE DO PONTO — quem está batendo direito, quem não está, e o ranking.

Pedido do dono, 17/10/2026: *"o problema que nós temos hoje é muitas vezes não se
levar a sério a batida e querer ficar pedindo ajuste, e é isso que a gente quer
tentar acabar"*; *"todo mundo que entrar no ponto enxergar de forma fácil os
problemas seus e os problemas do que está sob a responsabilidade dela"*; e o
ranking/gamificação "já pode ser colocado agora dentro do ERP, na seção do ponto".

AS REGRAS (escritas aqui, testadas — nada é palpite):
  · DIA DE TRABALHO: o espelho diz que havia trabalho previsto — situação Normal,
    Falta ou Batida faltando. Abonado, descanso, feriado, sem escala, fora do
    contrato e o dia de hoje (ainda em andamento) não contam.
  · DIA EM DIA: dia de trabalho com TODAS as batidas previstas registradas na
    hora — sem falta, sem batida faltando e sem batida incluída por ajuste.
  · ÍNDICE "PONTO EM DIA" = dias em dia ÷ dias de trabalho (%).
  · FOTOS OK = batidas do aplicativo com foto e sem sinal estranho (escura, sem
    rosto) ÷ batidas do aplicativo (%). Batida do iDFace ou lançada à mão não entra.
  · AJUSTES = pedidos de ajuste de batida no período (os negados contam: foram
    pedidos; os cancelados, não). Acima do LIMITE do mês (Configuração; nasce em
    3 — decisão deixada ao sistema pelo dono: "define aí pra mim, pode ser algo
    configurável"), a pessoa ganha o aviso "muitos ajustes".
  · PONTOS (0 a 100) = 70% do índice + 30% das fotos ok (sem batida do aplicativo,
    só o índice), menos 5 por ajuste acima do limite. SELO: Ouro ≥ 95, Prata ≥ 85,
    Bronze ≥ 70; abaixo disso, "Atenção".

QUEM VÊ O QUÊ (o dono, 17/10/2026): a pessoa que só consulta vê o dela; quem é
RESPONSÁVEL por um ponto da obra (ou de equipe) vê as pessoas que batem no
aparelho dele — "ele é responsável por aquelas pessoas, precisa batalhar para que
o ponto seja batido por completo"; a gestão vê o ranking por obra, por
responsável e por pessoa, dentro das obras que alcança.

A OBRA DE UM DIA é a da primeira batida do dia; dia sem batida (falta) fica na
obra em que a pessoa MAIS bateu no período (o dono: "a obra principal deve ser a
obra em que a pessoa está batendo ponto") — e, se ela não bateu em nenhuma, na do
cadastro. A EQUIPE de um responsável é quem bateu nos aparelhos
de que ele é responsável no período.
"""
from __future__ import annotations

import datetime as dt
import threading
import time
from collections import defaultdict
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from . import cadastros, espelho, parametros

PARAMETRO_LIMITE = "qualidade.ajustes_limite"
LIMITE_PADRAO = 3
DIAS_DE_TRABALHO = ("OK", "FALTA", "INCOMPLETO")
PESO_INDICE, PESO_FOTOS = 0.7, 0.3
PENALIDADE_POR_AJUSTE = 5
SELOS = ((95, "OURO", "Ouro"), (85, "PRATA", "Prata"), (70, "BRONZE", "Bronze"), (0, "ATENCAO", "Atenção"))
CACHE_S = 600

_cache: dict = {}
_trava = threading.Lock()


def limite_de_ajustes(conn: Connection) -> int:
    try:
        n = int(parametros.ler(conn, PARAMETRO_LIMITE, str(LIMITE_PADRAO)) or LIMITE_PADRAO)
    except (TypeError, ValueError):
        n = LIMITE_PADRAO
    return max(n, 0)


def gravar_limite(conn: Connection, valor, por: str) -> int:
    from ..erros import ErroDeValidacao
    try:
        n = int(valor)
    except (TypeError, ValueError):
        raise ErroDeValidacao("diga um número de ajustes", campo="limite")
    if not 0 <= n <= 31:
        raise ErroDeValidacao("de 0 a 31 ajustes no mês", campo="limite")
    parametros.gravar(conn, PARAMETRO_LIMITE, str(n), por)
    with _trava:
        _cache.clear()
    return n


# ---------------------------------------------------------------------------
# As contas (PURAS: recebem o espelho já montado)
# ---------------------------------------------------------------------------
def _pct(parte: int, todo: int) -> Optional[int]:
    return round(100 * parte / todo) if todo else None


def pontos_e_selo(indice: Optional[int], fotos: Optional[int], ajustes: int, limite: int):
    """PURA. Os pontos (0–100) e o selo — None quando não houve dia de trabalho."""
    if indice is None:
        return None, None
    base = indice if fotos is None else PESO_INDICE * indice + PESO_FOTOS * fotos
    base -= PENALIDADE_POR_AJUSTE * max(0, ajustes - limite)
    pontos = max(0, min(100, round(base)))
    codigo, rotulo = next((c, r) for minimo, c, r in SELOS if pontos >= minimo)
    return pontos, {"codigo": codigo, "rotulo": rotulo}


def avaliar_dias(dias: list[dict], obra_principal: Optional[str]) -> dict:
    """PURA. Lê os dias do espelho e devolve os números e os problemas, dia a dia,
    com a obra de cada dia (para o ranking por obra)."""
    t = defaultdict(int)
    problemas, por_obra = [], defaultdict(lambda: defaultdict(int))
    frequencia = defaultdict(int)
    for d in dias:
        for b in d.get("batidas") or []:
            if b.get("obra"):
                frequencia[b["obra"]] += 1
    da_falta = max(frequencia, key=frequencia.get) if frequencia else (obra_principal or "—")
    for d in dias:
        batidas = d.get("batidas") or []
        app = [b for b in batidas if b.get("origem") == "PWA"]
        t["batidas_app"] += len(app)
        for b in app:
            sinal = "foto " in (b.get("motivo_analise") or "")
            if b.get("tem_foto") and not sinal:
                t["fotos_ok"] += 1
            elif not b.get("tem_foto"):
                t["sem_foto"] += 1
                problemas.append({"data": d["data"], "tipo": "SEM_FOTO", "texto": f"batida sem foto às {b['hora']}"})
            else:
                t["foto_com_sinal"] += 1
                problemas.append({"data": d["data"], "tipo": "FOTO", "texto": f"foto com problema às {b['hora']}"})
            if b.get("fora_da_cerca"):
                t["fora_da_cerca"] += 1
                problemas.append({"data": d["data"], "tipo": "FORA", "texto": f"bateu fora da obra às {b['hora']}"})
        if d.get("situacao") not in DIAS_DE_TRABALHO:
            continue
        obra = next((b["obra"] for b in batidas if b.get("obra")), None) or da_falta
        o = por_obra[obra]
        t["dias_trabalho"] += 1
        o["dias_trabalho"] += 1
        ajustadas = [b for b in batidas if b.get("status") == "AJUSTADA"]
        faltando = d.get("faltando") or []
        if d["situacao"] == "FALTA":
            t["faltas"] += 1
            o["faltas"] += 1
            problemas.append({"data": d["data"], "tipo": "FALTA", "texto": "faltou — sem batida e sem justificativa"})
        elif d["situacao"] == "INCOMPLETO" or faltando:
            t["incompletos"] += 1
            o["incompletos"] += 1
            quais = ", ".join(f["rotulo"] for f in faltando) or "uma batida"
            problemas.append({"data": d["data"], "tipo": "FALTANDO", "texto": f"faltou bater: {quais.lower()}"})
        elif ajustadas:
            t["dias_com_ajuste"] += 1
            o["dias_com_ajuste"] += 1
            problemas.append({"data": d["data"], "tipo": "AJUSTE",
                              "texto": f"{len(ajustadas)} batida(s) incluída(s) por ajuste"})
        else:
            t["dias_em_dia"] += 1
            o["dias_em_dia"] += 1
    problemas.sort(key=lambda p: p["data"], reverse=True)
    return {"totais": dict(t), "problemas": problemas, "por_obra": {k: dict(v) for k, v in por_obra.items()}}


def _numeros(t: dict, ajustes: int, limite: int) -> dict:
    indice = _pct(t.get("dias_em_dia", 0), t.get("dias_trabalho", 0))
    fotos = _pct(t.get("fotos_ok", 0), t.get("batidas_app", 0))
    pontos, selo = pontos_e_selo(indice, fotos, ajustes, limite)
    return {"dias_trabalho": t.get("dias_trabalho", 0), "dias_em_dia": t.get("dias_em_dia", 0),
            "faltas": t.get("faltas", 0), "incompletos": t.get("incompletos", 0),
            "dias_com_ajuste": t.get("dias_com_ajuste", 0), "sem_foto": t.get("sem_foto", 0),
            "foto_com_sinal": t.get("foto_com_sinal", 0), "fora_da_cerca": t.get("fora_da_cerca", 0),
            "batidas_app": t.get("batidas_app", 0), "ajustes": ajustes,
            "muitos_ajustes": ajustes > limite, "indice": indice, "fotos_ok": fotos,
            "pontos": pontos, "selo": selo}


# ---------------------------------------------------------------------------
# Com o banco
# ---------------------------------------------------------------------------
def _ajustes_pedidos(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date) -> dict[int, int]:
    return {int(l["c"]): int(l["n"]) for l in db.todos(conn, """
        SELECT colaborador_id AS c, count(*) AS n FROM ponto.ocorrencias
         WHERE tipo = 'AJUSTE_BATIDA' AND status <> 'CANCELADA' AND colaborador_id = ANY(:ids)
           AND data_inicio BETWEEN :i AND :f
         GROUP BY colaborador_id""", ids=ids, i=inicio, f=fim)} if ids else {}


def _responsaveis(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date) -> dict[int, set]:
    """Pessoa → responsáveis dos aparelhos de obra ou de equipe em que ela bateu."""
    saida: dict[int, set] = defaultdict(set)
    if not ids:
        return saida
    for l in db.todos(conn, """
        SELECT DISTINCT m.colaborador_id AS c, d.colaborador_id AS r
          FROM ponto.marcacoes m JOIN ponto.dispositivos d ON d.id = m.dispositivo_id
         WHERE m.colaborador_id = ANY(:ids) AND m.data_referencia BETWEEN :i AND :f
           AND d.perfil IN ('COMPARTILHADO', 'LISTA') AND d.colaborador_id IS NOT NULL
           AND m.status <> 'REJEITADA'""", ids=ids, i=inicio, f=fim):
        saida[int(l["c"])].add(int(l["r"]))
    return saida


def da_pessoa(conn: Connection, colaborador_id: int, inicio: dt.date, fim: dt.date,
              limite: Optional[int] = None) -> dict:
    """Os números e os problemas de UMA pessoa no período."""
    limite = limite_de_ajustes(conn) if limite is None else limite
    from . import vigencia
    inicio, fim = vigencia.recortar(conn, inicio, fim)
    if fim < inicio:                       # o mês inteiro é antes do ponto valer
        inicio = fim
    esp = espelho.montar(conn, colaborador_id, inicio, fim)
    p = esp["pessoa"]
    av = avaliar_dias(esp["dias"], p.get("obra"))
    ajustes = _ajustes_pedidos(conn, [colaborador_id], inicio, fim).get(colaborador_id, 0)
    return {"id": colaborador_id, "nome": p["nome"], "obra": p.get("obra"),
            **_numeros(av["totais"], ajustes, limite), "problemas": av["problemas"],
            "_por_obra": av["por_obra"], "limite_ajustes": limite}


def avaliar(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date) -> dict:
    """O ranking de um grupo de pessoas: cada uma, e as somas por obra e por
    responsável. Guardado por 10 minutos (o mês de centenas de pessoas é pesado)."""
    # A chave leva a última batida e o último pedido: chegou coisa nova, recalcula.
    ultimo = db.um(conn, """SELECT (SELECT max(id) FROM ponto.marcacoes) AS m,
                                   (SELECT max(id) FROM ponto.ocorrencias) AS o""") or {}
    chave = (inicio, fim, tuple(sorted(set(ids))), ultimo.get("m"), ultimo.get("o"), limite_de_ajustes(conn))
    with _trava:
        guardado = _cache.get(chave)
        if guardado and time.time() - guardado[0] < CACHE_S:
            return guardado[1]
    limite = limite_de_ajustes(conn)
    pessoas = [da_pessoa(conn, cid, inicio, fim, limite) for cid in sorted(set(ids))]
    pessoas = [p for p in pessoas if p["dias_trabalho"] or p["batidas_app"]]
    resp = _responsaveis(conn, [p["id"] for p in pessoas], inicio, fim)

    obras: dict = defaultdict(lambda: defaultdict(int))
    gente_da_obra: dict = defaultdict(set)
    for p in pessoas:
        for codigo, n in p["_por_obra"].items():
            for k, v in n.items():
                obras[codigo][k] += v
            gente_da_obra[codigo].add(p["id"])
    por_obra = []
    for codigo, n in obras.items():
        num = _numeros(n, 0, limite)
        por_obra.append({"obra": codigo, "pessoas": len(gente_da_obra[codigo]), **num})

    equipes: dict = defaultdict(list)
    for p in pessoas:
        for r in resp.get(p["id"], ()):
            equipes[r].append(p)
    por_responsavel = []
    for r, gente in equipes.items():
        soma = defaultdict(int)
        for p in gente:
            for k in ("dias_trabalho", "dias_em_dia", "faltas", "incompletos", "dias_com_ajuste",
                      "sem_foto", "foto_com_sinal", "fora_da_cerca", "batidas_app"):
                soma[k] += p[k]
            soma["fotos_ok"] += round((p["fotos_ok"] or 0) * p["batidas_app"] / 100) if p["batidas_app"] else 0
        quem = cadastros.colaborador_por_id(conn, r) or {"nome": f"pessoa nº {r}"}
        num = _numeros(soma, sum(p["ajustes"] for p in gente), limite * len(gente))
        por_responsavel.append({"id": r, "nome": quem["nome"], "pessoas": len(gente), **num})

    for p in pessoas:
        p.pop("_por_obra", None)
    ordem = lambda x: (x["pontos"] is None, -(x["pontos"] or 0), x.get("nome") or x.get("obra") or "")  # noqa: E731
    total = defaultdict(int)
    for p in pessoas:
        for k in ("dias_trabalho", "dias_em_dia", "faltas", "incompletos", "dias_com_ajuste", "sem_foto",
                  "foto_com_sinal", "fora_da_cerca", "batidas_app"):
            total[k] += p[k]
        total["fotos_ok"] += round((p["fotos_ok"] or 0) * p["batidas_app"] / 100) if p["batidas_app"] else 0
    resultado = {
        "inicio": inicio.isoformat(), "fim": fim.isoformat(), "limite_ajustes": limite,
        "geral": {**_numeros(total, sum(p["ajustes"] for p in pessoas), limite * max(len(pessoas), 1)),
                  "pessoas": len(pessoas), "muitos_ajustes_pessoas": sum(1 for p in pessoas if p["muitos_ajustes"])},
        "pessoas": sorted(pessoas, key=ordem),
        "por_obra": sorted(por_obra, key=ordem),
        "por_responsavel": sorted(por_responsavel, key=ordem),
    }
    with _trava:
        _cache[chave] = (time.time(), resultado)
    return resultado


def equipe_do_responsavel(conn: Connection, responsavel_id: int, inicio: dt.date, fim: dt.date) -> list[int]:
    """Quem bateu nos aparelhos de obra ou de equipe de que a pessoa é responsável
    no período — mais a lista do aparelho de equipe dela."""
    ids = {int(l["c"]) for l in db.todos(conn, """
        SELECT DISTINCT m.colaborador_id AS c
          FROM ponto.marcacoes m JOIN ponto.dispositivos d ON d.id = m.dispositivo_id
         WHERE d.colaborador_id = :r AND d.perfil IN ('COMPARTILHADO', 'LISTA')
           AND m.data_referencia BETWEEN :i AND :f AND m.status <> 'REJEITADA'""",
        r=responsavel_id, i=inicio, f=fim)}
    ids |= {int(l["c"]) for l in db.todos(conn, """
        SELECT a.colaborador_id AS c FROM ponto.dispositivo_autorizados a
          JOIN ponto.dispositivos d ON d.id = a.dispositivo_id
         WHERE d.colaborador_id = :r AND d.perfil = 'LISTA' AND d.status = 'APROVADO'""", r=responsavel_id)}
    return sorted(ids)


def e_responsavel(conn: Connection, colaborador_id: int) -> bool:
    return db.um(conn, """SELECT 1 AS x FROM ponto.dispositivos WHERE colaborador_id = :c
                            AND perfil IN ('COMPARTILHADO', 'LISTA') AND status = 'APROVADO' LIMIT 1""",
                 c=colaborador_id) is not None
