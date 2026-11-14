# -*- coding: utf-8 -*-
"""
A sincronização rodando FORA do processo que atende as telas.

Por que existe. O serviço sobe com:

    gunicorn ... --workers 1 --max-requests 150 --max-requests-jitter 40

`--max-requests` manda o gunicorn REINICIAR o processo a cada ~150 requisições
— proteção contra vazamento de memória, posta depois do OOM de julho de 2026
(`CONTEXTO.md` §9). Com um worker só, esse reinício leva junto qualquer thread
de fundo.

A própria tela de acompanhamento consulta o servidor a cada poucos segundos
enquanto alguém olha a carga. Somando o resto do monorepo, o processo se recicla
a cada poucos minutos. Uma carga longa rodando numa thread NUNCA teria chance de
terminar — foi o que aconteceu três vezes seguidas na conversão do painel, sem
que a causa aparecesse em lugar nenhum.

A saída não é mexer no `--max-requests`: ele protege os outros 15 módulos, e
mudá-lo é decisão de infraestrutura, não de um módulo. A saída é a carga não
morar dentro do worker. Este arquivo é o processo separado:

    python -m app.apps.analisesps.executar_sync <modo> <id da execução>

Ele é iniciado destacado do worker (sessão própria), então continua vivo quando
o gunicorn recicla. O andamento vai para o banco, e é de lá que a tela lê.

O que ainda o derruba: o contêiner inteiro reiniciar (uma publicação de código).
Para esse caso existe a retomada por etapas, em `tarefas.py`.
"""
from __future__ import annotations

import logging
import sys


def _configurar_log() -> None:
    """Sem isto o processo separado não escreveria nada nos logs do Render — e
    a única janela para dentro de uma carga longa se fecharia."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
        stream=sys.stdout,
    )


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2:
        print("uso: python -m app.apps.analisesps.executar_sync "
              "<modo> <execucao_id>", file=sys.stderr)
        return 2
    modo, execucao_id = argv[0], int(argv[1])

    _configurar_log()
    log = logging.getLogger("analisesps.executar")
    log.info("Iniciando '%s' (execução %d) em processo separado.",
             modo, execucao_id)

    from .tarefas import encadear_comprovantes, executar_trabalho, manter_viva
    # ⚠️ O BATIMENTO NÃO DEPENDE DO TRABALHO (13/11/2026). O dono, com a baixa dos
    # comprovantes parada na "leva 3 de 5, desde 09:30": a execução só batia ponto
    # quando o trabalho anotava uma etapa — e uma leva no robô do BaixaBradesco
    # passou a levar MINUTOS (a SPsBD inteira, disputada com o Make). Com 3 minutos
    # sem batimento a execução era dada por morta, outra começava por cima e
    # devolvia à fila o lote que a primeira ainda processava. Agora um relógio à
    # parte bate de minuto em minuto enquanto ESTE processo vive; se ele morrer
    # (publicação, reinício), o batimento para junto e a execução é dada por morta
    # como antes.
    parar = manter_viva(execucao_id)
    try:
        ok = executar_trabalho(modo, execucao_id)
    finally:
        parar()
    log.info("'%s' terminada: %s", modo, "ok" if ok else "com falha")
    # A fila de comprovantes não espera ninguém apertar botão (08/10/2026).
    encadear_comprovantes(modo)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
