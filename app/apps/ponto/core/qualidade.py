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
    só o índice), MENOS 5 POR AJUSTE PEDIDO — desde o primeiro — e menos 5 por dia com
    batida faltando. SELO: Ouro ≥ 95, Prata ≥ 85, Bronze ≥ 70; abaixo disso, "Atenção".
    O AJUSTE É A MÉTRICA PRINCIPAL (o dono, 29/10/2026: "quanto mais ajuste, pior (…) tem
    que ser essa a métrica principal. A gente não quer que o pessoal faça ajuste, quer que
    eles batam corretamente"). Até 29/10/2026 só o ajuste ACIMA do limite tirava ponto. O
    dia com batida faltando custa o mesmo que o ajuste: senão, deixar o buraco sem pedir
    ajuste sairia mais barato que pedir — e o buraco é pior para a folha. No desempate, à
    frente quem tem menos ajustes, depois menos batidas faltando, menos faltas e mais dias.
  · O RANKING (29/10/2026): funcionário entra com 10 dias de trabalho ou mais no período;
    obra, com 10 funcionários ou mais. Os pontos da OBRA são a média dos pontos das pessoas
    que trabalharam nela, pesada pelos dias de cada uma na obra — assim os ajustes e as
    fotos contam para a obra (antes, a obra só via os dias em dia, e uma obra de uma pessoa
    só ficava em primeiro).

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
DIAS_DE_TRABALHO = ("OK", "FALTA", "FALTA_PARCIAL", "INCOMPLETO")
PESO_INDICE, PESO_FOTOS = 0.7, 0.3
PENALIDADE_POR_AJUSTE = 5           # por ajuste pedido, desde o primeiro (29/10/2026)
PENALIDADE_POR_FALTANDO = 5         # por dia com batida faltando — o mesmo do ajuste
MINIMO_DIAS_PESSOA = 10             # para o funcionário entrar no ranking
MINIMO_PESSOAS_OBRA = 10            # para a obra entrar no ranking
SELOS = ((95, "OURO", "Ouro"), (85, "PRATA", "Prata"), (70, "BRONZE", "Bronze"), (0, "ATENCAO", "Atenção"))
CACHE_S = 600

_cache: dict = {}
_trava = threading.Lock()

# UMA CONTA PESADA POR VEZ (21/10/2026, incidente: na hora da saída o aplicativo
# não carregou para ninguém). Os aparelhos da obra recalculavam a equipe inteira a
# cada volta ao menu — um espelho do mês por pessoa —, a guarda caía a cada batida
# nova, e as contas, todas juntas, tomavam o servidor (1 processo, 4 linhas). Agora:
# guardado por 10 minutos SEM cair a cada batida, e nunca duas contas ao mesmo
# tempo — quem chega com outra rodando ouve "tente em instantes", na hora.
_uma_por_vez = threading.Semaphore(1)


class Ocupado(Exception):
    """Outra conta da qualidade está rodando; esta não espera."""


def _guardado(chave):
    with _trava:
        g = _cache.get(chave)
        return g[1] if g and time.time() - g[0] < CACHE_S else None


def _guardar(chave, valor):
    with _trava:
        if len(_cache) > 2000:             # teto de memória: some o mais velho
            for k in sorted(_cache, key=lambda k: _cache[k][0])[:500]:
                _cache.pop(k, None)
        _cache[chave] = (time.time(), valor)


def esquecer() -> None:
    with _trava:
        _cache.clear()


class _Vez:
    def __init__(self, esperar: float):
        self.esperar = esperar

    def __enter__(self):
        ok = (_uma_por_vez.acquire(timeout=self.esperar) if self.esperar > 0
              else _uma_por_vez.acquire(blocking=False))
        if not ok:
            raise Ocupado("o resumo do ponto está sendo calculado — tente de novo em instantes")
        return self

    def __exit__(self, *_):
        _uma_por_vez.release()


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


def pontos_e_selo(indice: Optional[int], fotos: Optional[int], ajustes: int, limite: int, incompletos: int = 0):
    """PURA. Os pontos (0–100) e o selo — None quando não houve dia de trabalho. `limite` só decide o aviso
    "muitos ajustes" (`_numeros`); todo ajuste tira ponto."""
    if indice is None:
        return None, None
    base = indice if fotos is None else PESO_INDICE * indice + PESO_FOTOS * fotos
    base -= PENALIDADE_POR_AJUSTE * max(0, ajustes) + PENALIDADE_POR_FALTANDO * max(0, incompletos)
    pontos = max(0, min(100, round(base)))
    return pontos, selo(pontos)


def selo(pontos: Optional[int]) -> Optional[dict]:
    if pontos is None:
        return None
    codigo, rotulo = next((c, r) for minimo, c, r in SELOS if pontos >= minimo)
    return {"codigo": codigo, "rotulo": rotulo}


def ordem_das_pessoas(p: dict) -> tuple:
    """PURA. O ranking dos funcionários: quem entra primeiro; mais pontos; menos ajustes; menos batidas
    faltando; menos faltas; mais dias de trabalho."""
    return (not p.get("entra_no_ranking", True), p["pontos"] is None, -(p["pontos"] or 0), p.get("ajustes", 0),
            p.get("incompletos", 0), p.get("faltas", 0), -(p.get("dias_trabalho") or 0), p.get("nome") or "")


def pontos_da_obra(gente: list[tuple[Optional[int], int]]) -> Optional[int]:
    """PURA. A média dos pontos das pessoas da obra, pesada pelos dias de cada uma nela: [(pontos, dias)]."""
    validos = [(p, d) for p, d in gente if p is not None and d > 0]
    total = sum(d for _, d in validos)
    return round(sum(p * d for p, d in validos) / total) if total else None


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
        obra = next((b["obra"] for b in batidas if b.get("obra")), None) or da_falta
        # O % DE AJUSTES (30/10/2026, o dono: "na tela de qualidade uma coluna com o % de ajustes de batidas
        # do total"): das batidas do período, quantas entraram por ajuste — a incluída em lote pela gestão
        # (falha do sistema) não conta, como não conta nos problemas.
        ajustadas = [b for b in batidas if b.get("status") == "AJUSTADA"
                     and not (b.get("motivo_analise") or "").startswith("incluída em lote")]
        for chave, n in (("batidas", len(batidas)), ("batidas_ajustadas", len(ajustadas))):
            t[chave] += n
            if n:
                por_obra[obra][chave] += n
        if d.get("situacao") not in DIAS_DE_TRABALHO:
            continue
        o = por_obra[obra]
        t["dias_trabalho"] += 1
        o["dias_trabalho"] += 1
        # A batida incluída em lote pela gestão (falha do sistema) não pesa contra a pessoa (acima).
        faltando = d.get("faltando") or []
        if d["situacao"] == "FALTA":
            t["faltas"] += 1
            o["faltas"] += 1
            problemas.append({"data": d["data"], "tipo": "FALTA", "texto": "faltou — sem batida e sem justificativa"})
        elif d["situacao"] in ("FALTA_PARCIAL", "INCOMPLETO") or faltando:
            t["incompletos"] += 1
            o["incompletos"] += 1
            quais = ", ".join(f["rotulo"] for f in faltando) or "uma batida"
            problemas.append({"data": d["data"], "tipo": "FALTANDO", "texto": f"falta parcial — faltou bater: {quais.lower()}"})
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
    pontos, o_selo = pontos_e_selo(indice, fotos, ajustes, limite, t.get("incompletos", 0))
    return {"dias_trabalho": t.get("dias_trabalho", 0), "dias_em_dia": t.get("dias_em_dia", 0),
            "faltas": t.get("faltas", 0), "incompletos": t.get("incompletos", 0),
            "dias_com_ajuste": t.get("dias_com_ajuste", 0), "sem_foto": t.get("sem_foto", 0),
            "foto_com_sinal": t.get("foto_com_sinal", 0), "fora_da_cerca": t.get("fora_da_cerca", 0),
            "batidas_app": t.get("batidas_app", 0), "ajustes": ajustes,
            "batidas": t.get("batidas", 0), "batidas_ajustadas": t.get("batidas_ajustadas", 0),
            "pct_ajustadas": _pct(t.get("batidas_ajustadas", 0), t.get("batidas", 0)),
            "muitos_ajustes": ajustes > limite, "indice": indice, "fotos_ok": fotos,
            "pontos": pontos, "selo": o_selo}


# ---------------------------------------------------------------------------
# Com o banco
# ---------------------------------------------------------------------------
def _ajustes_pedidos(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date) -> dict[int, int]:
    return {int(l["c"]): int(l["n"]) for l in db.todos(conn, """
        SELECT colaborador_id AS c, count(*) AS n FROM ponto.ocorrencias
         WHERE tipo = 'AJUSTE_BATIDA' AND status <> 'CANCELADA' AND colaborador_id = ANY(:ids)
           AND subtipo IS DISTINCT FROM 'LOTE_GESTAO'    -- a inclusão em lote da gestão não é da pessoa
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


def da_pessoa_guardada(conn: Connection, colaborador_id: int, inicio: dt.date, fim: dt.date,
                       esperar: float = 0) -> dict:
    """`da_pessoa` para o aplicativo: guardada por 10 minutos e uma conta por vez
    (levanta Ocupado se outra estiver rodando e `esperar` acabar)."""
    chave = ("pessoa", int(colaborador_id), inicio, fim, limite_de_ajustes(conn))
    feito = _guardado(chave)
    if feito is not None:
        return feito
    with _Vez(esperar):
        feito = _guardado(chave)
        if feito is None:
            feito = da_pessoa(conn, colaborador_id, inicio, fim)
            _guardar(chave, feito)
    return feito


def avaliar(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date, esperar: float = 30) -> dict:
    """O ranking de um grupo de pessoas: cada uma, e as somas por obra e por
    responsável. Guardado por 10 minutos (o mês de centenas de pessoas é pesado) e
    uma conta por vez — levanta Ocupado se outra rodar além de `esperar` segundos."""
    # A guarda NÃO cai a cada batida nova (21/10/2026): no pico, isso refazia a
    # conta a cada segundo. O número tem até 10 minutos de atraso.
    chave = (inicio, fim, tuple(sorted(set(ids))), limite_de_ajustes(conn))
    feito = _guardado(chave)
    if feito is not None:
        return feito
    with _Vez(esperar):
        feito = _guardado(chave)
        if feito is not None:
            return feito
        return _avaliar(conn, ids, inicio, fim, chave)


def _avaliar(conn: Connection, ids: list[int], inicio: dt.date, fim: dt.date, chave) -> dict:
    limite = limite_de_ajustes(conn)
    pessoas = []
    for cid in sorted(set(ids)):
        pessoas.append(da_pessoa(conn, cid, inicio, fim, limite))
        time.sleep(0.005)                  # cede a vez às batidas entre uma pessoa e outra
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
        if not n.get("dias_trabalho"):      # só batida em dia de descanso: conta no % da pessoa, não faz obra
            continue
        num = _numeros(n, 0, limite)
        # os pontos da obra: a média das pessoas, pesada pelos dias de cada uma nela (29/10/2026)
        num["pontos"] = pontos_da_obra([(p["pontos"], p["_por_obra"].get(codigo, {}).get("dias_trabalho", 0))
                                        for p in pessoas if p["id"] in gente_da_obra[codigo]])
        num["selo"] = selo(num["pontos"])
        num["ajustes"] = sum(p["ajustes"] for p in pessoas if p["id"] in gente_da_obra[codigo])
        por_obra.append({"obra": codigo, "pessoas": len(gente_da_obra[codigo]), **num,
                         "entra_no_ranking": len(gente_da_obra[codigo]) >= MINIMO_PESSOAS_OBRA})

    equipes: dict = defaultdict(list)
    for p in pessoas:
        for r in resp.get(p["id"], ()):
            equipes[r].append(p)
    por_responsavel = []
    for r, gente in equipes.items():
        soma = defaultdict(int)
        for p in gente:
            for k in ("dias_trabalho", "dias_em_dia", "faltas", "incompletos", "dias_com_ajuste",
                      "sem_foto", "foto_com_sinal", "fora_da_cerca", "batidas_app", "batidas", "batidas_ajustadas"):
                soma[k] += p[k]
            soma["fotos_ok"] += round((p["fotos_ok"] or 0) * p["batidas_app"] / 100) if p["batidas_app"] else 0
        quem = cadastros.colaborador_por_id(conn, r) or {"nome": f"pessoa nº {r}"}
        num = _numeros(soma, sum(p["ajustes"] for p in gente), limite * len(gente))
        # a equipe, como a obra: a média dos pontos das pessoas, pesada pelos dias (29/10/2026) — com todo
        # ajuste tirando ponto, a soma das penalidades da equipe inteira zeraria qualquer equipe grande
        num["pontos"] = pontos_da_obra([(p["pontos"], p["dias_trabalho"] or 0) for p in gente])
        num["selo"] = selo(num["pontos"])
        por_responsavel.append({"id": r, "nome": quem["nome"], "pessoas": len(gente), **num})

    for p in pessoas:
        # a obra da pessoa no ranking: a em que ela mais trabalhou no período
        por = p.pop("_por_obra", None) or {}
        p["obra_do_periodo"] = max(por, key=lambda c: por[c].get("dias_trabalho", 0)) if por else p.get("obra")
        p["entra_no_ranking"] = (p["dias_trabalho"] or 0) >= MINIMO_DIAS_PESSOA
    ordem = lambda x: (not x.get("entra_no_ranking", True), x["pontos"] is None, -(x["pontos"] or 0),  # noqa: E731
                       x.get("nome") or x.get("obra") or "")
    total = defaultdict(int)
    for p in pessoas:
        for k in ("dias_trabalho", "dias_em_dia", "faltas", "incompletos", "dias_com_ajuste", "sem_foto",
                  "foto_com_sinal", "fora_da_cerca", "batidas_app", "batidas", "batidas_ajustadas"):
            total[k] += p[k]
        total["fotos_ok"] += round((p["fotos_ok"] or 0) * p["batidas_app"] / 100) if p["batidas_app"] else 0
    geral = _numeros(total, sum(p["ajustes"] for p in pessoas), limite * max(len(pessoas), 1))
    geral["pontos"] = pontos_da_obra([(p["pontos"], p["dias_trabalho"] or 0) for p in pessoas])
    geral["selo"] = selo(geral["pontos"])
    resultado = {
        "inicio": inicio.isoformat(), "fim": fim.isoformat(), "limite_ajustes": limite,
        "geral": {**geral,
                  "pessoas": len(pessoas), "muitos_ajustes_pessoas": sum(1 for p in pessoas if p["muitos_ajustes"])},
        "pessoas": sorted(pessoas, key=ordem_das_pessoas),
        "minimos": {"dias_pessoa": MINIMO_DIAS_PESSOA, "pessoas_obra": MINIMO_PESSOAS_OBRA},
        "por_obra": sorted(por_obra, key=ordem),
        "por_responsavel": sorted(por_responsavel, key=ordem),
    }
    _guardar(chave, resultado)
    return resultado


# ---------------------------------------------------------------------------
# A MENSAGEM PARA O WHATSAPP (30/10/2026, o dono: "crie uma mensagem para whatsapp da qualidade do ponto.
# Quero as top 10 obras. Top 20 funcionários. Quero ainda um explicativo do cálculo"). Só os que ENTRAM
# no ranking (as mesmas regras da tela); do funcionário, só o primeiro e o último nome — a mensagem vai
# para grupo. O texto diz o que é a conta, sem conselho.
# ---------------------------------------------------------------------------
TOP_OBRAS_MENSAGEM = 10
TOP_PESSOAS_MENSAGEM = 20
_MEDALHA = {"OURO": "🥇", "PRATA": "🥈", "BRONZE": "🥉", "ATENCAO": "⚠️"}


def _ajustes_txt(n: dict) -> str:
    v = n.get("pct_ajustadas")
    return f"{v}% de ajustes" if v is not None else "sem batidas"


def explicacao_do_calculo() -> str:
    """PURA. Como a nota é feita, em palavras — a mesma conta de `pontos_e_selo` e `pontos_da_obra`."""
    ouro, prata, bronze = (m for m, _, _ in SELOS[:3])
    return "\n".join([
        "*Como a nota é calculada*",
        f"• A nota vai de 0 a 100. {round(PESO_INDICE * 100)}% dela vem dos *dias em dia* (todas as batidas "
        f"do dia na hora, sem falta e sem ajuste) e {round(PESO_FOTOS * 100)}% das *fotos boas* (com foto e sem "
        "problema nela, como foto escura ou sem rosto).",
        f"• Cada *pedido de ajuste* tira {PENALIDADE_POR_AJUSTE} pontos, desde o primeiro. Cada dia com "
        f"*batida faltando* tira {PENALIDADE_POR_FALTANDO}.",
        f"• Selo: 🥇 Ouro a partir de {ouro} · 🥈 Prata a partir de {prata} · 🥉 Bronze a partir de {bronze}.",
        "• A nota da *obra* é a média das notas de quem trabalhou nela, pesada pelos dias de cada um na obra.",
        f"• Entram no ranking as obras com {MINIMO_PESSOAS_OBRA} funcionários ou mais e os funcionários com "
        f"{MINIMO_DIAS_PESSOA} dias de trabalho ou mais.",
        "• *% de ajustes* = das batidas do mês, quantas entraram por pedido de ajuste.",
        "• Não contam: dia abonado (atestado, férias, licença), folga, feriado e o dia de hoje.",
    ])


def mensagem_whatsapp(r: dict, nomes_obras: dict[str, str], competencia: str, ate: dt.date) -> str:
    """PURA. Do resultado de `avaliar` ao texto para colar no WhatsApp — na ordem da tela."""
    from .busca_nome import nome_curto
    mes = f"{competencia[5:7]}/{competencia[:4]}"
    linhas = [f"*🏆 Qualidade do ponto — {mes}*", f"_Até {ate:%d/%m/%Y}_", ""]
    obras = [o for o in r.get("por_obra") or [] if o.get("entra_no_ranking") and o.get("pontos") is not None]
    linhas.append(f"*Top {TOP_OBRAS_MENSAGEM} obras*")
    if obras:
        for n, o in enumerate(obras[:TOP_OBRAS_MENSAGEM], start=1):
            nome = nomes_obras.get(o["obra"])
            rotulo = f"{o['obra']} — {nome}" if nome else o["obra"]
            linhas.append(f"{n}º {_MEDALHA.get((o.get('selo') or {}).get('codigo'), '')} {rotulo}: *{o['pontos']}* "
                          f"({o['pessoas']} pessoas · {_ajustes_txt(o)})")
    else:
        linhas.append(f"Nenhuma obra com {MINIMO_PESSOAS_OBRA} funcionários ou mais no mês ainda.")
    linhas += ["", f"*Top {TOP_PESSOAS_MENSAGEM} funcionários*"]
    pessoas = [p for p in r.get("pessoas") or [] if p.get("entra_no_ranking") and p.get("pontos") is not None]
    if pessoas:
        for n, p in enumerate(pessoas[:TOP_PESSOAS_MENSAGEM], start=1):
            obra = p.get("obra_do_periodo") or p.get("obra")
            linhas.append(f"{n}º {_MEDALHA.get((p.get('selo') or {}).get('codigo'), '')} {nome_curto(p.get('nome'))}"
                          f"{f' ({obra})' if obra else ''}: *{p['pontos']}* · {contar_ajustes(p.get('ajustes', 0))}")
    else:
        linhas.append(f"Nenhum funcionário com {MINIMO_DIAS_PESSOA} dias de trabalho ou mais no mês ainda.")
    linhas += ["", explicacao_do_calculo()]
    return "\n".join(linhas)


def contar_ajustes(n: int) -> str:
    return "nenhum ajuste" if not n else ("1 ajuste" if n == 1 else f"{n} ajustes")


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
