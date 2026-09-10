# ============================================================================
# ERP — core/comum/saude.py
# O termômetro: quanto tempo cada tela leva, quanta memória o serviço usa, e o
# que está ocupando espaço no banco.
#
# POR QUE ISTO EXISTE
#
# O dono perguntou se o sistema aguenta crescer, e a resposta que dei foi de
# raciocínio, não de medição. A próxima pergunta dele vai ser sobre GASTAR —
# trocar de plano no Render, subir o banco — e decisão de gastar não pode ser
# palpite. Aqui ficam os números.
#
# TRÊS CUIDADOS QUE ATRAVESSAM O ARQUIVO
#
#   1. MEDIR NÃO PODE CUSTAR MAIS QUE O QUE SE MEDE. Os tempos se acumulam na
#      memória do processo e descem ao banco de tempos em tempos, agregados por
#      dia e por rota. Uma linha por requisição faria a tabela de medição virar
#      o problema que ela veio medir.
#
#   2. A MEDIÇÃO NUNCA DERRUBA A TELA. Todo caminho daqui está embrulhado: se a
#      gravação falhar, o número se perde e a vida segue. Sistema que cai por
#      causa do próprio termômetro é pior que sistema sem termômetro.
#
#   3. UM PROCESSO SÓ. O acumulador em memória funciona porque o `--workers 1`
#      é decisão de projeto (há estado em memória no chatbot). Se um dia forem
#      dois workers, cada um manda a sua parte e o banco soma — os números
#      continuam certos, só chegam em duas levas.
# ============================================================================
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Acima disto a chamada é contada como LENTA. Não é um erro — é o limite acima
# do qual a pessoa percebe que esperou.
LIMITE_LENTA_MS = 1500

# De quanto em quanto tempo os números descem ao banco.
INTERVALO_GRAVACAO = 60          # segundos
MAX_ROTAS_NA_MEMORIA = 400       # trava de segurança contra rota gerada

_trava = threading.Lock()
_acumulado: dict[tuple[date, str], dict[str, int]] = {}
_ultima_gravacao = time.monotonic()


# ---------------------------------------------------------------------------
# A coleta
# ---------------------------------------------------------------------------
def registrar(rota: str, ms: int, *, erro: bool = False) -> None:
    """Guarda uma chamada. Chamada de dentro do `after_request`, então tem de
    ser barata e não pode levantar nada."""
    try:
        chave = (date.today(), (rota or "?")[:120])
        with _trava:
            if chave not in _acumulado and len(_acumulado) >= MAX_ROTAS_NA_MEMORIA:
                return
            linha = _acumulado.setdefault(
                chave, {"chamadas": 0, "ms_total": 0, "ms_maior": 0,
                        "erros": 0, "lentas": 0})
            linha["chamadas"] += 1
            linha["ms_total"] += ms
            linha["ms_maior"] = max(linha["ms_maior"], ms)
            if erro:
                linha["erros"] += 1
            if ms >= LIMITE_LENTA_MS:
                linha["lentas"] += 1
    except Exception:                                      # pragma: no cover
        pass


def hora_de_gravar() -> bool:
    return (time.monotonic() - _ultima_gravacao) >= INTERVALO_GRAVACAO


def gravar(engine=None) -> int:
    """Desce para o banco o que está acumulado. Devolve quantas rotas gravou."""
    global _ultima_gravacao
    with _trava:
        pendente = dict(_acumulado)
        _acumulado.clear()
        _ultima_gravacao = time.monotonic()
    if not pendente:
        return 0

    try:
        from app.apps.erp.db.database import obter_engine
        motor = engine or obter_engine()
        with motor.begin() as c:
            for (dia, rota), v in pendente.items():
                # SOMA em cima do que já existe: o processo reinicia a cada 150
                # requisições (max-requests), então o dia é montado em pedaços.
                c.execute(text("""
                    INSERT INTO saude_tempos
                        (dia, rota, chamadas, ms_total, ms_maior, erros, lentas)
                    VALUES (:dia, :rota, :ch, :tot, :maior, :err, :lent)
                    ON CONFLICT (dia, rota) DO UPDATE SET
                        chamadas = saude_tempos.chamadas + EXCLUDED.chamadas,
                        ms_total = saude_tempos.ms_total + EXCLUDED.ms_total,
                        ms_maior = GREATEST(saude_tempos.ms_maior, EXCLUDED.ms_maior),
                        erros    = saude_tempos.erros + EXCLUDED.erros,
                        lentas   = saude_tempos.lentas + EXCLUDED.lentas,
                        atualizado_em = now()
                """), {"dia": dia, "rota": rota, "ch": v["chamadas"],
                       "tot": v["ms_total"], "maior": v["ms_maior"],
                       "err": v["erros"], "lent": v["lentas"]})
        return len(pendente)
    except Exception as e:
        # Perder a medição é aceitável; derrubar a tela por causa dela não.
        logger.warning("ERP/saúde: não consegui gravar os tempos (%s)", e)
        return 0


# ---------------------------------------------------------------------------
# As leituras
# ---------------------------------------------------------------------------
def memoria() -> dict[str, Any]:
    """Quanta memória o processo está usando, lida do próprio sistema.

    Sem biblioteca nova: `/proc/self/status` já tem o número, e acrescentar
    dependência para ler um arquivo de texto seria caro pelo que entrega.
    """
    saida: dict[str, Any] = {"rss_mb": None, "pico_mb": None, "teto_mb": 2048}
    try:
        with open("/proc/self/status") as f:
            for linha in f:
                if linha.startswith("VmRSS:"):
                    saida["rss_mb"] = round(int(linha.split()[1]) / 1024, 1)
                elif linha.startswith("VmHWM:"):
                    saida["pico_mb"] = round(int(linha.split()[1]) / 1024, 1)
    except Exception:
        pass
    if saida["rss_mb"] is not None:
        saida["pct"] = round(saida["rss_mb"] / saida["teto_mb"] * 100, 1)
    return saida


def _consultar(s, sql: str, **p) -> list[Any]:
    """Uma consulta do painel, isolada num ponto de salvamento.

    O isolamento não é zelo: no Postgres, UMA consulta que falha aborta a
    transação inteira, e todas as seguintes falham junto. Sem o ponto de
    salvamento, uma tabela que ainda não existe (migração não aplicada) deixaria
    o painel INTEIRO em branco em vez de faltar só aquele pedaço — e um painel
    vazio faz a pessoa achar que o sistema parou.
    """
    ponto = None
    try:
        ponto = s.begin_nested()
        linhas = list(s.execute(text(sql), p).all())
        ponto.commit()
        return linhas
    except Exception as e:
        try:
            if ponto is not None:
                ponto.rollback()
        except Exception:                                  # pragma: no cover
            pass
        logger.warning("ERP/saúde: consulta indisponível (%s)", e)
        return []


def banco(s) -> dict[str, Any]:
    """O tamanho do banco e o que mais ocupa espaço.

    A pergunta prática: se um dia o plano do banco apertar, o que está pesando?
    Quase sempre a resposta é ANEXO — e é por isso que existe a opção de mandar
    os documentos para o Drive.
    """
    tamanho = _consultar(s, "SELECT pg_database_size(current_database())")
    # A contagem de linhas do Postgres é ESTIMATIVA: vem da última análise da
    # tabela. Numa base recém-criada ela ainda é desconhecida — e mostrar
    # "0 linhas" ao lado de uma tabela de 300 KB seria uma afirmação falsa.
    # Por isso o desconhecido volta como None, e a tela mostra um traço.
    maiores = _consultar(s, """
        SELECT c.relname,
               pg_total_relation_size(c.oid) AS bytes,
               GREATEST(COALESCE(s.n_live_tup, 0), 0) AS vivas,
               c.reltuples AS estimadas
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE c.relkind = 'r' AND n.nspname = 'public'
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 12
    """)
    total = int(tamanho[0][0]) if tamanho else 0

    def _linhas(vivas, estimadas) -> Optional[int]:
        if int(vivas or 0) > 0:
            return int(vivas)
        if estimadas is not None and float(estimadas) > 0:
            return int(float(estimadas))
        return None

    return {
        "total_mb": round(total / (1024 * 1024), 1),
        "tabelas": [{"nome": r[0], "mb": round(int(r[1]) / (1024 * 1024), 2),
                     "linhas": _linhas(r[2], r[3])} for r in maiores],
    }


def anexos(s) -> dict[str, Any]:
    """Onde estão os documentos, e quanto ocupam dentro do banco.

    É o número que sustenta a decisão de ligar (ou não) o Google Drive: um
    anexo no Drive não pesa no plano do banco.
    """
    linhas = _consultar(s, """
        SELECT guardado_em, count(*), COALESCE(sum(tamanho_bytes), 0)
        FROM anexos GROUP BY guardado_em
    """)
    saida = {"no_banco": 0, "no_drive": 0, "mb_no_banco": 0.0}
    for onde, quantos, bytes_ in linhas:
        if (onde or "BANCO") == "DRIVE":
            saida["no_drive"] += int(quantos)
        else:
            saida["no_banco"] += int(quantos)
            saida["mb_no_banco"] += int(bytes_ or 0) / (1024 * 1024)
    saida["mb_no_banco"] = round(saida["mb_no_banco"], 1)
    return saida


def telas(s, *, dias: int = 7, limite: int = 25) -> dict[str, Any]:
    """As telas mais lentas dos últimos dias.

    Ordenadas pelo TEMPO TOTAL gasto, e não pela média: uma tela que leva três
    segundos e é aberta uma vez por mês incomoda menos que uma de meio segundo
    aberta duzentas vezes por dia. O que se quer consertar é onde a equipe
    espera mais no fim das contas.
    """
    desde = date.today() - timedelta(days=max(0, dias - 1))
    linhas = _consultar(s, """
        SELECT rota, sum(chamadas), sum(ms_total), max(ms_maior),
               sum(erros), sum(lentas)
        FROM saude_tempos WHERE dia >= :desde
        GROUP BY rota ORDER BY sum(ms_total) DESC LIMIT :limite
    """, desde=desde, limite=limite)

    rotas = [{
        "rota": r[0], "chamadas": int(r[1]),
        "media_ms": round(int(r[2]) / int(r[1])) if r[1] else 0,
        "pior_ms": int(r[3]),
        "erros": int(r[4]), "lentas": int(r[5]),
        "segundos_no_periodo": round(int(r[2]) / 1000, 1),
    } for r in linhas]

    total = _consultar(s, """
        SELECT sum(chamadas), sum(ms_total), sum(erros), sum(lentas)
        FROM saude_tempos WHERE dia >= :desde
    """, desde=desde)
    ch, ms, err, lent = (total[0] if total and total[0][0] is not None
                         else (0, 0, 0, 0))
    return {
        "desde": desde.isoformat(), "dias": dias,
        "rotas": rotas,
        "chamadas": int(ch or 0),
        "media_ms": round(int(ms or 0) / int(ch)) if ch else 0,
        "erros": int(err or 0),
        "lentas": int(lent or 0),
        "limite_lenta_ms": LIMITE_LENTA_MS,
    }


def por_dia(s, *, dias: int = 14) -> list[dict[str, Any]]:
    """O movimento dia a dia — para enxergar se está piorando com o tempo."""
    desde = date.today() - timedelta(days=max(0, dias - 1))
    return [{"dia": r[0].isoformat(), "chamadas": int(r[1]),
             "media_ms": round(int(r[2]) / int(r[1])) if r[1] else 0,
             "lentas": int(r[3]), "erros": int(r[4])}
            for r in _consultar(s, """
                SELECT dia, sum(chamadas), sum(ms_total), sum(lentas), sum(erros)
                FROM saude_tempos WHERE dia >= :desde
                GROUP BY dia ORDER BY dia
            """, desde=desde)]


def panorama(s, *, dias: int = 7) -> dict[str, Any]:
    """Tudo junto, do jeito que a tela mostra."""
    m = memoria()
    b = banco(s)
    t = telas(s, dias=dias)
    a = anexos(s)

    # Os avisos são o que transforma número em decisão. Sem eles a tela vira
    # um painel bonito que ninguém sabe interpretar.
    avisos = []
    if m.get("pct") and m["pct"] >= 80:
        avisos.append(f"A memória está em {m['pct']}% do teto do plano "
                      f"({m['rss_mb']} MB de {m['teto_mb']} MB). Foi assim que "
                      f"o serviço caiu em julho de 2026.")
    if t["lentas"] and t["chamadas"]:
        pct = round(t["lentas"] / t["chamadas"] * 100, 1)
        if pct >= 5:
            avisos.append(f"{pct}% das aberturas de tela passaram de "
                          f"{LIMITE_LENTA_MS / 1000:.1f}s nos últimos {dias} dias.")
    if a["mb_no_banco"] >= 200:
        avisos.append(f"Os documentos ocupam {a['mb_no_banco']} MB dentro do "
                      f"banco. Ligar o Google Drive em Configurações tira esse "
                      f"peso daqui.")
    if not t["chamadas"]:
        avisos.append("Ainda não há medições — elas começam a aparecer conforme "
                      "as telas forem usadas.")
    return {"memoria": m, "banco": b, "telas": t, "anexos": a,
            "por_dia": por_dia(s), "avisos": avisos}
