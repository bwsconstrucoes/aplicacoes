# -*- coding: utf-8 -*-
"""
A atualização rodando FORA do processo que atende as telas.

Por que existe. O serviço sobe com:

    gunicorn ... --workers 1 --max-requests 150 --max-requests-jitter 40

`--max-requests` manda o gunicorn **reiniciar o processo** a cada ~150
requisições — é uma proteção contra vazamento de memória, colocada depois do
incidente de OOM de julho de 2026 (`CONTEXTO.md` §9). Com um worker só, esse
reinício leva junto qualquer thread de fundo.

A própria tela de Configurações consulta o servidor a cada poucos segundos
enquanto alguém acompanha a carga. Somando o resto do monorepo, o processo se
recicla a cada poucos minutos. Uma carga inicial de horas, rodando numa thread,
**nunca teria chance de terminar** — e foi exatamente o que aconteceu três
vezes seguidas, sem que a causa aparecesse em lugar nenhum.

A saída não é mexer no `--max-requests`: ele protege os outros 14 módulos, e
mudá-lo é decisão de infraestrutura, não de um módulo. A saída é a atualização
não morar dentro do worker. Este arquivo é o processo separado:

    python -m app.apps.painel.executar_sync <modo> <id da execução>

Ele é iniciado destacado do worker (sessão própria), então continua vivo quando
o gunicorn recicla. O andamento vai para o banco, e é de lá que a tela lê — o
que já funcionava assim, e é o que torna esta separação possível sem inventar
canal de comunicação nenhum.

O que ainda o derruba: o contêiner inteiro reiniciar (uma publicação de código).
Para esse caso existe a retomada por etapas.
"""
from __future__ import annotations

import logging
import sys


def _configurar_log() -> None:
    """Sem isto o processo separado não escreveria nada nos logs do Render —
    e a única janela para dentro de uma carga de horas se fecharia."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
        stream=sys.stdout,
    )


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2:
        print("uso: python -m app.apps.painel.executar_sync <modo> <execucao_id>",
              file=sys.stderr)
        return 2
    modo, execucao_id = argv[0], int(argv[1])

    _configurar_log()
    log = logging.getLogger("painel.executar")
    log.info("Iniciando a atualização '%s' (execução %d) em processo separado.",
             modo, execucao_id)

    from .tarefas import executar_trabalho
    ok = executar_trabalho(modo, execucao_id)
    log.info("Atualização '%s' terminada: %s", modo, "ok" if ok else "com falha")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
