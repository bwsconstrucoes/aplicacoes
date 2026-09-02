# -*- coding: utf-8 -*-
"""
A atualizacao da base — o que antes era o `atualiza_omie.bat`.

Duas formas de disparar, as duas caindo aqui:
  - AGENDADA: o cron-job.org chama POST /painel/api/sincronizar toda madrugada,
    com o segredo do modulo. E o mesmo arranjo que o `baixabradesco` ja usa para
    a fila tardia — sem agendador dentro do processo, que gastaria memoria e nao
    sobreviveria a um reinicio do Render.
  - MANUAL: o botao "Atualizar agora" na tela de Configuracoes.

Roda numa thread de fundo, porque a atualizacao leva minutos e a requisicao HTTP
nao pode ficar esperando. UMA de cada vez: se ja houver atualizacao rodando, a
segunda e recusada em vez de duplicar o trabalho e o consumo de memoria.
"""
from __future__ import annotations

import logging
import threading
import datetime as dt

logger = logging.getLogger("painel.tarefas")

# Trava de processo. Com --workers 1 (obrigatorio neste servico, ver CLAUDE.md)
# uma trava em memoria basta: so existe um processo servindo o painel.
_trava = threading.Lock()
_em_andamento: dict | None = None

MODOS = {
    "rapida": "Atualização do dia — baixa o que mudou e refaz os números",
    "completa": "Atualização completa — inclui a varredura de títulos excluídos no OMIE",
    "so_numeros": "Só refazer os números, sem baixar nada do OMIE",
    "carga_inicial": "Primeira carga — baixa toda a base do OMIE (demorado)",
}


def estado() -> dict:
    """O que a tela mostra: se ha atualizacao rodando e desde quando."""
    return {"rodando": _em_andamento is not None,
            "detalhe": dict(_em_andamento) if _em_andamento else None}


def _abrir_execucao(conn, modo: str, disparo: str) -> int:
    cur = conn.execute(
        "INSERT INTO execucoes (tipo, disparo) VALUES (?,?) RETURNING id",
        (modo, disparo))
    execucao_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return execucao_id


def _fechar_execucao(conn, execucao_id: int, ok: bool, mensagem: str, linhas: int | None):
    conn.execute(
        "UPDATE execucoes SET fim = now(), ok = ?, mensagem = ?, linhas_fato = ? "
        " WHERE id = ?", (ok, mensagem[:2000], linhas, execucao_id))
    conn.commit()


def _executar(modo: str, disparo: str) -> None:
    """O trabalho de verdade. Sempre dentro da trava."""
    global _em_andamento
    from .db import conexao
    from .sync import espelho, fato

    from .horario import agora
    inicio = agora()
    _em_andamento = {"modo": modo, "disparo": disparo, "inicio": inicio.isoformat(),
                     "etapa": "iniciando"}
    execucao_id = None
    try:
        with conexao() as conn:
            execucao_id = _abrir_execucao(conn, modo, disparo)

        # ---- etapa 1: trazer do OMIE o que mudou -------------------------
        if modo == "carga_inicial":
            _em_andamento["etapa"] = "baixando a base inteira do OMIE"
            espelho.carga_inicial()
        elif modo in ("rapida", "completa"):
            _em_andamento["etapa"] = "baixando o que mudou no OMIE"
            espelho.sync_incremental()
            if modo == "completa":
                # A varredura de exclusoes le TODOS os ids do OMIE para descobrir
                # o que foi apagado la e continua aqui. E a parte lenta; por isso
                # nao entra na atualizacao do dia.
                _em_andamento["etapa"] = "procurando títulos excluídos no OMIE"
                espelho.reconcile()

        # ---- etapa 2: refazer os numeros que as telas leem ---------------
        _em_andamento["etapa"] = "recalculando os números do painel"
        with conexao() as conn:
            n_fato, n_receb = fato.reconstruir(conn)

        duracao = (agora() - inicio).total_seconds()
        mensagem = (f"{n_fato:,} linhas de lançamento e {n_receb:,} recebimentos "
                    f"em {duracao/60:.1f} min.").replace(",", ".")
        logger.info("Painel: atualização %s concluída — %s", modo, mensagem)
        with conexao() as conn:
            _fechar_execucao(conn, execucao_id, True, mensagem, n_fato)

    except Exception as e:  # noqa: BLE001 — a falha tem de aparecer na tela
        logger.exception("Painel: falha na atualização %s", modo)
        if execucao_id is not None:
            try:
                with conexao() as conn:
                    _fechar_execucao(conn, execucao_id, False, str(e), None)
            except Exception:
                logger.exception("Painel: não consegui registrar a falha no banco")
    finally:
        _em_andamento = None
        if _trava.locked():
            _trava.release()


def disparar(modo: str, disparo: str = "manual") -> dict:
    """Comeca a atualizacao em segundo plano. Devolve o que dizer a quem pediu."""
    if modo not in MODOS:
        return {"ok": False, "erro": f"Modo desconhecido: {modo}"}
    if not _trava.acquire(blocking=False):
        atual = _em_andamento or {}
        return {"ok": False,
                "erro": "Já existe uma atualização em andamento "
                        f"({atual.get('etapa', 'iniciando')}). Espere ela terminar."}
    # a trava e liberada dentro de _executar, no finally
    try:
        threading.Thread(target=_executar, args=(modo, disparo),
                         name=f"painel-sync-{modo}", daemon=True).start()
    except Exception as e:   # noqa: BLE001
        # se a thread nem chegou a comecar, ninguem vai soltar a trava la dentro
        _trava.release()
        logger.exception("Painel: nao consegui iniciar a atualização")
        return {"ok": False, "erro": f"Não consegui iniciar a atualização: {e}"}
    return {"ok": True, "modo": modo, "descricao": MODOS[modo]}
