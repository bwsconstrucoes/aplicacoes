"""
O QUE ESTÁ SEGURANDO O SERVIÇO (31/10/2026, o dono: "estamos numa situação de lentidão no ERP de maneira geral
que a princípio não vejo motivo. Memória tá ok, CPU no Render tá ok, mas tá super lento. Às vezes até fica meio
parado.").

A produção roda com UM processo e QUATRO linhas de atendimento (Procfile: --workers 1 --threads 4). Memória e CPU
baixas com tudo lento é o retrato de linhas ESPERANDO — o banco, o Google, o OMIE, o WhatsApp — e não trabalhando:
quatro pedidos demorados ao mesmo tempo e o quinto, de qualquer área, fica na fila. Sem medir, não dá para dizer
quais são.

Este módulo só MEDE, no log do Render (nada muda no que as telas fazem):
  · "LENTA: GET /erp/... — 8,3 s" para todo pedido que passa de LIMITE_S;
  · "AS 4 LINHAS OCUPADAS: ..." quando o pedido que entra toma a última linha livre — com o caminho e o tempo de
    cada uma (no máximo uma vez a cada AVISO_A_CADA_S). O próximo pedido, de qualquer área, espera na fila. É a
    linha que diz quem travou o resto.
Para achar no Render: Logs › buscar "LENTA" ou "LINHAS OCUPADAS".
"""
from __future__ import annotations

import logging
import os
import threading
import time

from flask import g, request

logger = logging.getLogger("lentidao")

LIMITE_S = float(os.getenv("LENTIDAO_LIMITE_S", "3"))
LINHAS = int(os.getenv("LENTIDAO_LINHAS", "4"))          # as --threads do Procfile
# o que é demorado de propósito e não interessa medir a cada vez (o envio de arquivo grande segue medido)
IGNORAR = ("/static/",)

AVISO_A_CADA_S = 30

_em_andamento: dict[int, tuple[str, float]] = {}
_trava = threading.Lock()
_ultimo_aviso = [0.0]


def _caminho() -> str:
    return f"{request.method} {request.path}"


def ligar(app) -> None:
    """Pendura a medição no app. Nunca derruba um pedido: qualquer erro aqui só vira log."""

    @app.before_request
    def _inicio():
        try:
            if request.path.startswith(IGNORAR):
                return
            agora = time.monotonic()
            g._lentidao_inicio = agora
            with _trava:
                _em_andamento[threading.get_ident()] = (_caminho(), agora)
                ocupadas = list(_em_andamento.values())
                avisar = len(ocupadas) >= LINHAS and agora - _ultimo_aviso[0] >= AVISO_A_CADA_S
                if avisar:
                    _ultimo_aviso[0] = agora
            if avisar:
                lista = "; ".join(f"{c} há {agora - t:.0f} s" for c, t in sorted(ocupadas, key=lambda x: x[1]))
                logger.warning("AS %d LINHAS OCUPADAS: %s", LINHAS, lista)
        except Exception:  # noqa: BLE001
            pass

    @app.teardown_request
    def _fim(_erro=None):
        try:
            inicio = g.pop("_lentidao_inicio", None)
            with _trava:
                _em_andamento.pop(threading.get_ident(), None)
            if inicio is None:
                return
            gasto = time.monotonic() - inicio
            if gasto >= LIMITE_S:
                logger.warning("LENTA: %s — %s s", _caminho(), f"{gasto:.1f}".replace(".", ","))
        except Exception:  # noqa: BLE001
            pass


def agora_em_andamento() -> list[dict]:
    """O retrato de agora (para teste e diagnóstico): o que cada linha está fazendo, há quanto tempo."""
    agora = time.monotonic()
    with _trava:
        return [{"pedido": c, "segundos": round(agora - t, 1)} for c, t in _em_andamento.values()]
