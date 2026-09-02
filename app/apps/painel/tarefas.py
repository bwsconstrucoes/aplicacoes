# -*- coding: utf-8 -*-
"""
A atualização da base — o que antes era o `atualiza_omie.bat`.

Duas formas de disparar, as duas caindo aqui:
  - AGENDADA: o cron-job.org chama POST /painel/api/sincronizar toda madrugada,
    com o segredo do módulo. É o mesmo arranjo que o `baixabradesco` já usa para
    a fila tardia — sem agendador dentro do processo, que gastaria memória e não
    sobreviveria a um reinício do Render.
  - MANUAL: o botão "Atualizar agora" na tela de Configurações.

Roda numa thread de fundo, porque a atualização leva minutos (ou horas, na
primeira vez) e a requisição HTTP não pode ficar esperando. UMA de cada vez: se
já houver atualização rodando, a segunda é recusada em vez de duplicar o
trabalho e o consumo de memória.

O ANDAMENTO VAI PARA O BANCO, não só para a memória do processo. Isso conserta
dois problemas que só apareceram no primeiro uso de verdade:

  - uma carga de horas dizia apenas "baixando", sem nunca dizer quanto já andou;
  - quando o serviço reiniciava no meio — um envio de código faz isso —, a
    execução sumia da tela, e o dono via a falha ANTERIOR como se fosse a atual.

Agora a execução carimba a hora no banco enquanto está viva. Se parar de
carimbar, a tela sabe dizer que ela foi interrompida, e por quê.
"""
from __future__ import annotations

import logging
import threading

logger = logging.getLogger("painel.tarefas")

# Trava de processo. Com --workers 1 (obrigatório neste serviço, ver CLAUDE.md)
# uma trava em memória basta: só existe um processo servindo o painel.
_trava = threading.Lock()
_em_andamento: dict | None = None

# De quanto em quanto tempo o andamento vai para o banco. Não é a cada página:
# numa carga de mil páginas seriam mil escritas só para dizer "ainda estou
# aqui". A cada 10 segundos dá andamento suficiente na tela sem incomodar o banco.
SEGUNDOS_ENTRE_BATIMENTOS = 10

# Sem carimbo por mais que isso, a execução é dada como morta. O valor é
# generoso de propósito: uma página lenta do OMIE não pode ser confundida com
# um servidor que caiu.
MINUTOS_ATE_DAR_POR_MORTA = 10

MODOS = {
    "rapida": "Atualização do dia — baixa o que mudou e refaz os números",
    "completa": "Atualização completa — inclui a varredura de títulos excluídos no OMIE",
    "so_numeros": "Só refazer os números, sem baixar nada do OMIE",
    "carga_inicial": "Primeira carga — baixa toda a base do OMIE (demorado)",
}


def estado() -> dict:
    """O que a tela mostra.

    Junta o que está na memória DESTE processo com o que está registrado no
    banco — porque quem reinicia perde a memória, mas o banco continua sabendo
    que havia uma execução em curso."""
    memoria = dict(_em_andamento) if _em_andamento else None
    try:
        from .consultas import execucao_em_andamento
        registro = execucao_em_andamento()
    except Exception:      # banco fora do ar, ou migração ainda não aplicada
        registro = None

    if memoria:
        if registro and not memoria.get("detalhe_progresso"):
            memoria["detalhe_progresso"] = registro.get("progresso") or ""
        return {"rodando": True, "detalhe": memoria, "interrompida": None}

    if registro and registro["viva"]:
        # está rodando, mas este processo não é o dono da thread
        return {"rodando": True, "detalhe": registro, "interrompida": None}
    return {"rodando": False, "detalhe": None, "interrompida": registro}


# ---------------------------------------------------------------------------
# O registro da execução
# ---------------------------------------------------------------------------
def _fechar_execucoes_orfas(conn) -> int:
    """Encerra execuções que ficaram abertas de um processo que morreu.

    Sem isto, uma carga interrompida por reinício ficaria "em aberto" para
    sempre, e a tela nunca mais mostraria o resultado de nada."""
    cur = conn.execute(
        "UPDATE execucoes SET fim = now(), ok = FALSE, mensagem = ? "
        " WHERE fim IS NULL",
        ("Interrompida: o serviço reiniciou durante a atualização. Nada foi "
         "corrompido — é só rodar de novo.",))
    quantas = cur.rowcount or 0
    cur.close()
    conn.commit()
    if quantas:
        logger.warning("Painel: %d execução(ões) órfã(s) encerrada(s).", quantas)
    return quantas


def _abrir_execucao(conn, modo: str, disparo: str) -> int:
    _fechar_execucoes_orfas(conn)
    cur = conn.execute(
        "INSERT INTO execucoes (tipo, disparo, etapa, visto_em) "
        "VALUES (?,?,?, now()) RETURNING id", (modo, disparo, "começando"))
    execucao_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return execucao_id


def _fechar_execucao(conn, execucao_id: int, ok: bool, mensagem: str,
                     linhas: int | None):
    conn.execute(
        "UPDATE execucoes SET fim = now(), ok = ?, mensagem = ?, linhas_fato = ?, "
        "       etapa = NULL, progresso = NULL WHERE id = ?",
        (ok, mensagem[:2000], linhas, execucao_id))
    conn.commit()


def _carimbar(execucao_id: int, etapa: str, detalhe: str) -> None:
    """Grava onde a atualização está, e que ela continua viva.

    Falha aqui nunca derruba a atualização: perder o andamento é chato, perder
    a carga é caro."""
    from .db import conexao
    try:
        with conexao() as conn:
            conn.execute(
                "UPDATE execucoes SET etapa = ?, progresso = ?, visto_em = now() "
                " WHERE id = ?", (etapa[:200], (detalhe or "")[:200], execucao_id))
            conn.commit()
    except Exception:
        logger.exception("Painel: não consegui gravar o andamento")


def _executar(modo: str, disparo: str) -> None:
    """O trabalho de verdade. Sempre dentro da trava."""
    global _em_andamento
    import time

    from .db import conexao
    from .horario import agora
    from .sync import espelho, fato

    inicio = agora()
    _em_andamento = {"modo": modo, "disparo": disparo, "inicio": inicio.isoformat(),
                     "etapa": "começando", "detalhe_progresso": ""}
    estado_local = {"id": None, "ultimo": 0.0}

    def _anotar(etapa: str, detalhe: str = "") -> None:
        """Memória sempre; banco de tempos em tempos."""
        _em_andamento["etapa"] = etapa
        _em_andamento["detalhe_progresso"] = detalhe
        if estado_local["id"] is None:
            return
        if time.time() - estado_local["ultimo"] < SEGUNDOS_ENTRE_BATIMENTOS:
            return
        estado_local["ultimo"] = time.time()
        _carimbar(estado_local["id"], etapa, detalhe)

    def _etapa(etapa: str, detalhe: str = "") -> None:
        """Mudança de etapa: vai para o banco na hora, sem esperar o intervalo."""
        _em_andamento["etapa"] = etapa
        _em_andamento["detalhe_progresso"] = detalhe
        if estado_local["id"] is not None:
            estado_local["ultimo"] = time.time()
            _carimbar(estado_local["id"], etapa, detalhe)

    try:
        with conexao() as conn:
            estado_local["id"] = _abrir_execucao(conn, modo, disparo)

        espelho.definir_progresso(_anotar)
        try:
            # ---- etapa 1: trazer do OMIE o que mudou ---------------------
            if modo == "carga_inicial":
                _etapa("baixando a base inteira do OMIE", "começando")
                espelho.carga_inicial()
            elif modo in ("rapida", "completa"):
                _etapa("baixando o que mudou no OMIE")
                espelho.sync_incremental()
                if modo == "completa":
                    # A varredura de exclusões lê TODOS os ids do OMIE para
                    # descobrir o que foi apagado lá e continua aqui. É a parte
                    # lenta; por isso não entra na atualização do dia.
                    _etapa("procurando títulos excluídos no OMIE")
                    espelho.reconcile()
        finally:
            espelho.definir_progresso(None)

        # ---- etapa 2: refazer os números que as telas leem ---------------
        _etapa("recalculando os números do painel")
        with conexao() as conn:
            n_fato, n_receb = fato.reconstruir(conn)

        duracao = (agora() - inicio).total_seconds()
        mensagem = (f"{n_fato:,} linhas de lançamento e {n_receb:,} recebimentos "
                    f"em {duracao/60:.1f} min.").replace(",", ".")
        logger.info("Painel: atualização %s concluída — %s", modo, mensagem)
        with conexao() as conn:
            _fechar_execucao(conn, estado_local["id"], True, mensagem, n_fato)

    except Exception as e:  # noqa: BLE001 — a falha tem de aparecer na tela
        logger.exception("Painel: falha na atualização %s", modo)
        if estado_local["id"] is not None:
            try:
                with conexao() as conn:
                    _fechar_execucao(conn, estado_local["id"], False, str(e), None)
            except Exception:
                logger.exception("Painel: não consegui registrar a falha no banco")
    finally:
        _em_andamento = None
        if _trava.locked():
            _trava.release()


def disparar(modo: str, disparo: str = "manual") -> dict:
    """Começa a atualização em segundo plano. Devolve o que dizer a quem pediu."""
    if modo not in MODOS:
        return {"ok": False, "erro": f"Modo desconhecido: {modo}"}
    if not _trava.acquire(blocking=False):
        atual = _em_andamento or {}
        return {"ok": False,
                "erro": "Já existe uma atualização em andamento "
                        f"({atual.get('etapa', 'começando')}). Espere ela terminar."}
    # a trava é liberada dentro de _executar, no finally
    try:
        threading.Thread(target=_executar, args=(modo, disparo),
                         name=f"painel-sync-{modo}", daemon=True).start()
    except Exception as e:   # noqa: BLE001
        # se a thread nem chegou a começar, ninguém vai soltar a trava lá dentro
        _trava.release()
        logger.exception("Painel: não consegui iniciar a atualização")
        return {"ok": False, "erro": f"Não consegui iniciar a atualização: {e}"}
    return {"ok": True, "modo": modo, "descricao": MODOS[modo]}
