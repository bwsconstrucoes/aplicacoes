# -*- coding: utf-8 -*-
"""
DE QUANDO O PONTO VALE.

Decisão do dono, 17/10/2026: *"os pontos deverão ser considerados a partir do dia
16/10/2026. Desconsiderar pra todos os efeitos qualquer lançamento antes disso."*
O que veio antes foi teste (os dias de implantação).

NADA É APAGADO: a batida tem número sequencial (NSR) e cada uma é encadeada à
anterior; apagar deixaria a corrente presa a batidas que não existem. O que veio
antes fica guardado e é DESCONSIDERADO em tudo que se vê e se calcula: espelho
(o dia aparece como "antes do ponto", sem falta e sem batida), cálculo, banco de
horas, exportação para a folha, mosaico, Validações, alertas, rondas, painel do
dia, filtros e a qualidade do ponto.

A data mora em `ponto.parametros` e se muda em Configuração; sem nada gravado,
vale 16/10/2026.
"""
from __future__ import annotations

import datetime as dt
import time

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao

PARAMETRO = "ponto.vale_a_partir_de"
PADRAO = dt.date(2026, 10, 16)
SITUACAO_ANTES = "ANTES_DO_PONTO"

_guardado: dict = {}


def inicio(conn: Connection) -> dt.date:
    """O primeiro dia que conta. Lido no máximo uma vez por minuto."""
    agora = time.time()
    if _guardado and agora - _guardado["em"] < 60:
        return _guardado["data"]
    data = PADRAO
    import os
    # Para os testes (tests/conftest.py): o ponto vale desde a data da variável,
    # a menos que a Configuração tenha gravado outra.
    if os.environ.get("PONTO_VALE_A_PARTIR_DE"):
        try:
            data = dt.date.fromisoformat(os.environ["PONTO_VALE_A_PARTIR_DE"])
        except ValueError:
            pass
    if db.tem_coluna(conn, "parametros", "valor"):
        from . import parametros
        valor = (parametros.ler(conn, PARAMETRO, "") or "").strip()
        try:
            data = dt.date.fromisoformat(valor) if valor else data
        except ValueError:
            pass
    _guardado.update(data=data, em=agora)
    return data


def gravar(conn: Connection, valor, por: str) -> dt.date:
    from . import parametros
    try:
        data = dt.date.fromisoformat(str(valor or "").strip())
    except ValueError as e:
        raise ErroDeValidacao("data ilegível (AAAA-MM-DD)", campo="data") from e
    if data.year < 2020 or data > dt.date.today() + dt.timedelta(days=366):
        raise ErroDeValidacao("data fora do razoável", campo="data")
    parametros.gravar(conn, PARAMETRO, data.isoformat(), por)
    _guardado.clear()
    return data


def recortar(conn: Connection, de: dt.date, ate: dt.date) -> tuple[dt.date, dt.date]:
    """O período sem o que vem antes do ponto valer (`de` nunca antes do início)."""
    return max(de, inicio(conn)), ate
