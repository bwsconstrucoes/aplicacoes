# -*- coding: utf-8 -*-
"""
O RANKING DO PONTO, GUARDADO (29/10/2026) — para o aplicativo abrir na hora.

O dono: *"eu quero premiar as melhores obras (…) que apareça obras que tenham pelo menos 10 funcionários
(…) dentro do app a gente pudesse ver esse ranking, tanto das obras quanto dos funcionários (…) quando a
gente entra ele demora para calcular (…) ficaria em cache? Você faria um cálculo com alguma frequência?"*
Quem vê: *"quem bate ponto de obra"* — todo mundo que entra no aplicativo.

A CONTA é a da qualidade (`qualidade.py`: o ajuste como métrica principal), para TODO mundo que bateu no
mês: um espelho por pessoa, pesado. Por isso ela NÃO roda quando alguém abre a tela: roda numa linha
separada, de 2 em 2 horas, das 6h às 20h (acordada pelas próprias requisições do ponto — o serviço não
tem relógio, como a rotina do dia e a fila do WhatsApp), e o resultado fica guardado no banco
(`ponto.parametros`, "ranking.AAAA-MM"). O mês passado é refeito uma vez por dia (um ajuste aprovado
tarde ainda muda o mês). A tela mostra a hora da última conta.

O QUE SE GUARDA: as obras (todas, com a marca de quem entra — 10 funcionários ou mais), os 25 primeiros
funcionários entre os que entram (10 dias de trabalho ou mais) e a posição de cada pessoa, para ela
ver a dela. Do funcionário, só o primeiro e o último nome, a função e a obra — nunca o CPF.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import threading
import time
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from . import competencias, parametros, qualidade

logger = logging.getLogger("ponto.ranking")

TOP_PESSOAS = 25
HORAS_ENTRE_CONTAS = 2
HORAS_MES_PASSADO = 24
JANELA = (6, 20)
OLHAR_A_CADA_S = 60
CHAVE = "ranking.{}"

_trabalhando = threading.Lock()
_ultima_olhada = 0.0


def montar(r: dict, nomes: dict[int, dict], obras: dict[str, str], competencia: str, agora: dt.datetime) -> dict:
    """PURA. Do resultado de `qualidade.avaliar` ao que se guarda e se mostra."""
    from .busca_nome import nome_curto
    entram = [p for p in r["pessoas"] if p.get("entra_no_ranking") and p.get("pontos") is not None]
    posicoes = {}
    for n, p in enumerate(entram, start=1):
        posicoes[str(p["id"])] = {"posicao": n, "pontos": p["pontos"]}
    for p in r["pessoas"]:
        if str(p["id"]) not in posicoes:
            posicoes[str(p["id"])] = {"posicao": None, "pontos": p.get("pontos"), "dias": p.get("dias_trabalho"),
                                      "por_que": f"entra com {qualidade.MINIMO_DIAS_PESSOA} dias de trabalho ou mais"}
    pessoas = [{"posicao": n, "nome": nome_curto(p.get("nome")), "funcao": (nomes.get(int(p["id"])) or {}).get("funcao"),
                "obra": p.get("obra_do_periodo"), "pontos": p["pontos"], "selo": p.get("selo"),
                "ajustes": p.get("ajustes", 0), "faltas": p.get("faltas", 0), "dias": p.get("dias_trabalho", 0)}
               for n, p in enumerate(entram[:TOP_PESSOAS], start=1)]
    lista_obras = sorted(r["por_obra"], key=lambda o: (not o.get("entra_no_ranking"), o.get("pontos") is None,
                                                       -(o.get("pontos") or 0), o["obra"]))
    pos = 0
    saida_obras = []
    for o in lista_obras:
        entra = bool(o.get("entra_no_ranking")) and o.get("pontos") is not None
        pos += 1 if entra else 0
        saida_obras.append({"posicao": pos if entra else None, "obra": o["obra"], "nome": obras.get(o["obra"]),
                            "pessoas": o["pessoas"], "pontos": o.get("pontos"), "selo": o.get("selo"),
                            "ajustes": o.get("ajustes", 0), "entra": entra})
    return {"competencia": competencia, "calculado_em": agora.isoformat(),
            "minimos": {"pessoas_obra": qualidade.MINIMO_PESSOAS_OBRA, "dias_pessoa": qualidade.MINIMO_DIAS_PESSOA},
            "top": TOP_PESSOAS, "obras": saida_obras, "pessoas": pessoas, "posicoes": posicoes,
            "total_no_ranking": len(entram)}


def calcular(conn: Connection, competencia: str, esperar: float = 120) -> dict:
    """A conta do mês inteiro, para todo mundo que bateu nele, e guarda."""
    inicio = competencias.primeiro_dia(competencia)
    fim = min(competencias.ultimo_dia(inicio), horario.hoje())
    ids = [int(r["colaborador_id"]) for r in db.todos(conn, """
        SELECT DISTINCT colaborador_id FROM ponto.marcacoes
         WHERE data_referencia BETWEEN :i AND :f AND status <> 'REJEITADA'""", i=inicio, f=fim)]
    r = qualidade.avaliar(conn, ids, inicio, fim, esperar=esperar) if ids else {"pessoas": [], "por_obra": []}
    nomes = {}
    if ids:
        from . import cadastros
        for i in ids:
            p = cadastros.colaborador_por_id(conn, i)
            if p:
                nomes[i] = {"funcao": p.get("funcao")}
    codigos = sorted({o["obra"] for o in r["por_obra"]})
    obras = {o["codigo"]: o["nome"] for o in db.todos(conn, "SELECT codigo, nome FROM public.obras WHERE codigo = ANY(:c)",
                                                    c=codigos)} if codigos else {}
    dados = montar(r, nomes, obras, f"{inicio:%Y-%m}", horario.agora())
    parametros.gravar(conn, CHAVE.format(f"{inicio:%Y-%m}"), json.dumps(dados, ensure_ascii=False, default=str),
                      "ranking automático")
    logger.info("Ponto: ranking de %s calculado — %d pessoas, %d obras", dados["competencia"], len(ids),
                len(dados["obras"]))
    return dados


def ler(conn: Connection, competencia: str) -> Optional[dict]:
    bruto = parametros.ler(conn, CHAVE.format(competencia[:7]), "")
    try:
        return json.loads(bruto) if bruto else None
    except ValueError:
        return None


def vencido(dados: Optional[dict], horas: float, agora: dt.datetime) -> bool:
    """PURA. Precisa refazer? Sem conta guardada, sim; senão, quando passou o prazo."""
    if not dados:
        return True
    try:
        feito = dt.datetime.fromisoformat(dados["calculado_em"])
    except (KeyError, ValueError):
        return True
    return (agora - feito).total_seconds() > horas * 3600


# ---------------------------------------------------------------------------
# A linha que calcula
# ---------------------------------------------------------------------------
def _meses(hoje: dt.date) -> list[tuple[str, float]]:
    anterior = (hoje.replace(day=1) - dt.timedelta(days=1))
    return [(f"{hoje:%Y-%m}", HORAS_ENTRE_CONTAS), (f"{anterior:%Y-%m}", HORAS_MES_PASSADO)]


def _trabalhar() -> None:
    try:
        agora = horario.agora()
        for comp, horas in _meses(horario.hoje()):
            with db.conexao() as conn:
                if not vencido(ler(conn, comp), horas, agora):
                    continue
            with db.conexao() as conn:
                calcular(conn, comp)
    except qualidade.Ocupado:
        logger.info("Ponto: ranking adiado — outra conta da qualidade rodando")
    except Exception:  # noqa: BLE001 — o ranking nunca derruba nada
        logger.exception("Ponto: o ranking não calculou; tenta de novo mais tarde")
    finally:
        _trabalhando.release()


def na_janela(agora: Optional[dt.datetime] = None) -> bool:
    """Das 6h às 20h (hora local): fora disso, a conta não roda."""
    return JANELA[0] <= horario.para_local(agora or horario.agora()).hour < JANELA[1]


def disparar_se_preciso() -> bool:
    """Chamada barata em toda requisição do ponto: no máximo uma olhada por minuto, só das 6h às 20h."""
    global _ultima_olhada
    import sys
    if "pytest" in sys.modules:          # nos testes, a conta só roda quando o teste chama (como as rondas)
        return False
    agora = time.time()
    if agora - _ultima_olhada < OLHAR_A_CADA_S:
        return False
    _ultima_olhada = agora
    if not na_janela():
        return False
    try:
        with db.conexao() as conn:
            if not any(vencido(ler(conn, c), h, horario.agora()) for c, h in _meses(horario.hoje())):
                return False
    except Exception:  # noqa: BLE001
        return False
    if not _trabalhando.acquire(blocking=False):
        return False
    threading.Thread(target=_trabalhar, name="ponto-ranking", daemon=True).start()
    return True
