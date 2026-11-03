"""Configuração complementar do gunicorn — carregada sozinha.

O gunicorn lê `./gunicorn.conf.py` da pasta onde é iniciado (a raiz do
repositório, no Render) mesmo sem `-c` no comando. Os parâmetros de linha de
comando (Procfile / Start Command) continuam valendo e têm precedência; este
arquivo só acrescenta ganchos.

Por que existe (03/11/2026): na renovação automática do worker
(`--max-requests`), o gunicorn espera até `--graceful-timeout` pelas
requisições em curso e depois encerra o processo. Só que o Python, ao sair,
espera TODAS as threads de trabalho terminarem — inclusive a de uma requisição
que ainda não acabou. Com 1 worker só, o mestre não sobe o substituto enquanto
o antigo não morre, e o antigo só é morto pelo `--timeout 3600`: até uma hora
fora do ar. Foi o que o log de 03/11 mostrou ("Worker exiting" e nenhum
"Booting worker" depois, até o reset manual).

O gancho abaixo põe um prazo nessa saída: se o processo não terminar sozinho
em PRAZO_SAIDA_S, registra no log quais threads o prenderam (e onde estavam)
e força a saída — o mestre sobe o worker novo na hora.
"""

import logging
import os
import sys
import threading
import traceback

# Folga depois do graceful-timeout. A requisição presa já teve os 120 s dele.
PRAZO_SAIDA_S = 30


def _threads_presas() -> list[threading.Thread]:
    # A principal fica de fora: ela é quem está esperando as outras.
    fora = (threading.current_thread(), threading.main_thread())
    return [t for t in threading.enumerate()
            if t not in fora and not t.daemon and t.is_alive()]


def _forcar_saida(log) -> None:
    """Disparado pelo cronômetro: o processo não saiu no prazo."""
    try:
        quadros = sys._current_frames()
        presas = _threads_presas()
        log.warning("Worker %s não terminou em %s s depois de encerrar; "
                    "saída forçada. Threads presas: %s", os.getpid(), PRAZO_SAIDA_S,
                    ", ".join(t.name for t in presas) or "nenhuma")
        for t in presas:
            quadro = quadros.get(t.ident)
            if quadro is not None:
                pilha = "".join(traceback.format_stack(quadro, limit=15))
                log.warning("Onde a thread %s estava:\n%s", t.name, pilha)
        error_log = getattr(log, "error_log", None)
        for h in logging.getLogger().handlers + list(getattr(error_log, "handlers", [])):
            try:
                h.flush()
            except Exception:
                pass
        sys.stdout.flush()
        sys.stderr.flush()
    finally:
        os._exit(0)


def worker_exit(server, worker):
    """Arma o cronômetro de saída no próprio processo do worker.

    O gunicorn também chama este gancho no processo MESTRE quando recolhe um
    worker morto — ali não se faz nada, senão o mestre seria derrubado.
    """
    if os.getpid() != worker.pid:
        return
    cronometro = threading.Timer(PRAZO_SAIDA_S, _forcar_saida, args=(server.log,))
    cronometro.daemon = True
    cronometro.name = "prazo-de-saida"
    cronometro.start()
