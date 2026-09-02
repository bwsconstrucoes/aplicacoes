# -*- coding: utf-8 -*-
"""
A atualização da base — o que antes era o `atualiza_omie.bat`.

Duas formas de disparar, as duas caindo aqui:
  - AGENDADA: o cron-job.org chama POST /painel/api/sincronizar toda madrugada,
    com o segredo do módulo. É o mesmo arranjo que o `baixabradesco` já usa para
    a fila tardia — sem agendador dentro do processo, que gastaria memória e não
    sobreviveria a um reinício do Render.
  - MANUAL: o botão "Atualizar agora" na tela de Configurações.

A ATUALIZAÇÃO RODA EM PROCESSO SEPARADO, e isso não é preciosismo.

O serviço sobe com `--workers 1 --max-requests 150`: o gunicorn **reinicia o
processo** a cada ~150 requisições, uma proteção contra vazamento de memória
posta depois do OOM de julho de 2026. Com um worker só, esse reinício leva
junto qualquer thread de fundo. A própria tela de acompanhamento consulta o
servidor a cada poucos segundos; somando o resto do monorepo, o processo se
recicla a cada poucos minutos.

Uma carga de horas rodando numa thread **nunca teria chance de terminar** — e
foi o que aconteceu três vezes seguidas, sem que a causa aparecesse. Mexer no
`--max-requests` não é opção: ele protege os outros 14 módulos.

Então quem faz o trabalho é `executar_sync.py`, iniciado destacado do worker.
O andamento vai para o banco, e é de lá que a tela lê — o que já era assim, e é
o que torna a separação possível sem inventar canal de comunicação nenhum.

UMA DE CADA VEZ. A trava não pode mais ser de memória (processos diferentes não
a enxergam): é o próprio banco que responde se já há execução viva.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys

logger = logging.getLogger("painel.tarefas")

# De quanto em quanto tempo o andamento vai para o banco. Não é a cada página:
# numa carga de mil páginas seriam mil escritas só para dizer "ainda estou
# aqui". A cada 10 segundos dá andamento suficiente na tela sem incomodar o banco.
SEGUNDOS_ENTRE_BATIMENTOS = 10

MODOS = {
    "rapida": "Atualização do dia — baixa o que mudou e refaz os números",
    "completa": "Atualização completa — inclui a varredura de títulos excluídos no OMIE",
    "so_numeros": "Só refazer os números, sem baixar nada do OMIE",
    "carga_inicial": "Primeira carga — baixa toda a base do OMIE (demorado)",
}


def estado() -> dict:
    """O que a tela mostra, lido do banco.

    Antes isto olhava também uma variável em memória. Não olha mais: quem faz o
    trabalho é outro processo, e memória não se compartilha entre processos. O
    banco é a única fonte que os dois enxergam."""
    try:
        from .consultas import execucao_em_andamento
        registro = execucao_em_andamento()
    except Exception:      # banco fora do ar, ou migração ainda não aplicada
        return {"rodando": False, "detalhe": None, "interrompida": None}

    if registro and registro["viva"]:
        return {"rodando": True, "detalhe": registro, "interrompida": None}
    return {"rodando": False, "detalhe": None, "interrompida": registro}


# ---------------------------------------------------------------------------
# O registro da execução
# ---------------------------------------------------------------------------
def _fechar_execucoes_orfas(conn) -> int:
    """Encerra execuções que ficaram abertas de um processo que morreu.

    Sem isto, uma carga interrompida ficaria "em aberto" para sempre, e a tela
    nunca mais mostraria o resultado de nenhuma atualização."""
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


# ---------------------------------------------------------------------------
# O trabalho
# ---------------------------------------------------------------------------
def executar_trabalho(modo: str, execucao_id: int) -> bool:
    """Faz a atualização inteira. Chamado pelo processo separado.

    Recebe a execução já aberta: quem clicou no botão a abriu, para a tela ter
    o que mostrar mesmo antes de o processo separado começar."""
    import time

    from .db import conexao
    from .horario import agora
    from .sync import espelho, fato

    inicio = agora()
    ultimo = [0.0]

    def _anotar(etapa: str, detalhe: str = "") -> None:
        """Vai para o banco de tempos em tempos, não a cada página."""
        if time.time() - ultimo[0] < SEGUNDOS_ENTRE_BATIMENTOS:
            return
        ultimo[0] = time.time()
        _carimbar(execucao_id, etapa, detalhe)

    def _etapa(etapa: str, detalhe: str = "") -> None:
        """Mudança de etapa: vai na hora, sem esperar o intervalo."""
        ultimo[0] = time.time()
        _carimbar(execucao_id, etapa, detalhe)

    try:
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
            _fechar_execucao(conn, execucao_id, True, mensagem, n_fato)
        return True

    except Exception as e:  # noqa: BLE001 — a falha tem de aparecer na tela
        logger.exception("Painel: falha na atualização %s", modo)
        try:
            with conexao() as conn:
                _fechar_execucao(conn, execucao_id, False, str(e), None)
        except Exception:
            logger.exception("Painel: não consegui registrar a falha no banco")
        return False


# ---------------------------------------------------------------------------
# O disparo
# ---------------------------------------------------------------------------
def _iniciar_processo(modo: str, execucao_id: int) -> None:
    """Inicia o processo separado, DESTACADO do worker do gunicorn.

    "Destacado" é o ponto: sem isto o processo morre junto quando o gunicorn
    recicla o worker — que é exatamente o que estava matando a carga."""
    comando = [sys.executable, "-m", "app.apps.painel.executar_sync",
               modo, str(execucao_id)]
    raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

    extras: dict = {}
    if os.name == "posix":
        # sessão própria: o sinal que o gunicorn manda ao worker não o alcança
        extras["start_new_session"] = True
    else:
        extras["creationflags"] = (getattr(subprocess, "DETACHED_PROCESS", 0)
                                   | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))

    subprocess.Popen(comando, cwd=raiz, stdin=subprocess.DEVNULL,
                     stdout=None, stderr=None, close_fds=True, **extras)
    logger.info("Painel: atualização '%s' iniciada em processo separado "
                "(execução %d).", modo, execucao_id)


def disparar(modo: str, disparo: str = "manual") -> dict:
    """Começa a atualização. Devolve o que dizer a quem pediu."""
    if modo not in MODOS:
        return {"ok": False, "erro": f"Modo desconhecido: {modo}"}

    from .db import conexao

    # A trava é o banco, não a memória: quem faz o trabalho é outro processo,
    # e memória não se compartilha entre processos.
    atual = estado()
    if atual["rodando"]:
        etapa = (atual["detalhe"] or {}).get("etapa", "começando")
        return {"ok": False,
                "erro": f"Já existe uma atualização em andamento ({etapa}). "
                        "Espere ela terminar."}

    with conexao() as conn:
        execucao_id = _abrir_execucao(conn, modo, disparo)

    try:
        _iniciar_processo(modo, execucao_id)
    except Exception as e:  # noqa: BLE001
        logger.exception("Painel: não consegui iniciar o processo da atualização")
        with conexao() as conn:
            _fechar_execucao(conn, execucao_id, False,
                             f"Não consegui iniciar a atualização: {e}", None)
        return {"ok": False, "erro": f"Não consegui iniciar a atualização: {e}"}

    return {"ok": True, "modo": modo, "descricao": MODOS[modo],
            "execucao": execucao_id}
