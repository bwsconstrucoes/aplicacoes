# -*- coding: utf-8 -*-
"""
O TESTE DE CARGA NA PRODUÇÃO — celulares de verdade, a internet de verdade, o
servidor de verdade, e NENHUMA batida de mentira.

Pedido do dono, 15/10/2026: *"pegar uns 4–5 telefones já cadastrados, que acessem
algum endereço e na prática começar a disparar uma simulação de envio do ponto e
dados como se fossem as imagens, pra simular o comportamento do servidor, e deixar
rodando cada um por alguns minutos ao mesmo tempo."*

POR QUE NÃO É UMA BATIDA: batida gravada é registro de ponto — ganha NSR, que pela
Portaria 671 é sequencial, sem furo, e não se apaga. Cem batidas de teste ficariam
para sempre no AFD. Então a simulação faz O MESMO TRABALHO de uma batida, menos o
registro:
  · recebe a foto do tamanho da real (o celular gera, ~800 px, JPEG 0,8);
  · decodifica, reduz, calcula o hash e os sinais da foto (o que mais pesa no
    processador);
  · faz as leituras que a batida faz (o aparelho, o dono, as batidas recentes no
    aparelho, as obras dele);
  · GRAVA a foto em `ponto.fotos`, com os bytes — e desfaz (SAVEPOINT + ROLLBACK).
    O banco faz o trabalho de escrever; nada fica.
O que ela NÃO faz: NSR, marcação, alerta, subida ao Drive (que na batida de
verdade acontece depois da resposta, numa linha separada — não pesa na fila).

SÓ LIGADO: um ADMIN liga por até 60 minutos em Configuração. Fora disso, a rota
recusa — ela gasta processador de propósito, não pode ficar aberta. E só aparelho
APROVADO (os celulares "já cadastrados").

O resumo do servidor (quantas, tempo médio e máximo) fica na MEMÓRIA do serviço:
se ele se renovar no meio do teste (o `--max-requests`), o resumo recomeça — e é
justamente uma das coisas que o teste vai mostrar. Os números de cada celular
ficam na tela dele.
"""
from __future__ import annotations

import datetime as dt
import threading
import time

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao
from . import fotos, parametros

PARAMETRO = "carga.ligado_ate"
MAXIMO_MIN = 60

_trava = threading.Lock()
_resumo: dict = {}


def _zerar() -> None:
    _resumo.clear()
    _resumo.update({"desde": horario.agora(), "simulacoes": 0, "erros": 0, "ms_total": 0.0, "ms_max": 0.0,
                    "aparelhos": set()})


_zerar()


def ligado_ate(conn: Connection):
    valor = parametros.ler(conn, PARAMETRO, "") if db.tem_coluna(conn, "parametros", "valor") else ""
    if not valor:
        return None
    try:
        ate = dt.datetime.fromisoformat(valor)
    except ValueError:
        return None
    return ate if ate > horario.agora() else None


def ligar(conn: Connection, minutos: int, por: str) -> dt.datetime:
    try:
        minutos = int(minutos)
    except (TypeError, ValueError):
        raise ErroDeValidacao("diga por quantos minutos", campo="minutos")
    if not 1 <= minutos <= MAXIMO_MIN:
        raise ErroDeValidacao(f"de 1 a {MAXIMO_MIN} minutos", campo="minutos")
    ate = horario.agora() + dt.timedelta(minutes=minutos)
    parametros.gravar(conn, PARAMETRO, ate.isoformat(), por)
    with _trava:
        _zerar()
    return ate


def desligar(conn: Connection, por: str) -> None:
    parametros.gravar(conn, PARAMETRO, "", por)


def hora(momento) -> str | None:
    return f"{horario.para_local(momento):%d/%m às %H:%M}" if momento else None


def resumo() -> dict:
    with _trava:
        r = dict(_resumo)
    n = r["simulacoes"]
    return {"desde": hora(r["desde"]), "simulacoes": n, "erros": r["erros"],
            "aparelhos": len(r["aparelhos"]),
            "ms_medio": round(r["ms_total"] / n, 1) if n else None, "ms_max": round(r["ms_max"], 1) if n else None}


def simular(conn: Connection, aparelho: dict, foto_base64: str) -> dict:
    """O trabalho de uma batida, sem a batida. Devolve o tempo no servidor."""
    inicio = time.perf_counter()
    try:
        foto = fotos.preparar(foto_base64)
        db.um(conn, "SELECT id, perfil, status, colaborador_id FROM ponto.dispositivos WHERE id = :d", d=aparelho["id"])
        if aparelho.get("colaborador_id"):
            from . import cadastros
            cadastros.colaborador_por_id(conn, int(aparelho["colaborador_id"]))
        db.todos(conn, """SELECT colaborador_id, timestamp_servidor FROM ponto.marcacoes
                           WHERE dispositivo_id = :d AND timestamp_servidor > now() - interval '1 minute'""",
                 d=aparelho["id"])
        db.todos(conn, "SELECT obra_id FROM ponto.dispositivo_obras WHERE dispositivo_id = :d", d=aparelho["id"])
        ponto_de_volta = conn.begin_nested()           # grava de verdade… e desfaz
        try:
            db.um(conn, """INSERT INTO ponto.fotos (sha256, conteudo, tamanho, mime, largura, altura, nome_arquivo)
                           VALUES (:h, :c, :t, 'image/jpeg', :w, :a, 'teste-de-carga.jpg') RETURNING id""",
                  h=foto.hash, c=foto.dados, t=len(foto.dados), w=foto.largura, a=foto.altura)
        finally:
            ponto_de_volta.rollback()
    except Exception:
        with _trava:
            _resumo["erros"] += 1
        raise
    ms = (time.perf_counter() - inicio) * 1000
    with _trava:
        _resumo["simulacoes"] += 1
        _resumo["ms_total"] += ms
        _resumo["ms_max"] = max(_resumo["ms_max"], ms)
        _resumo["aparelhos"].add(int(aparelho["id"]))
    return {"ms_no_servidor": round(ms, 1), "bytes_da_foto": len(foto.dados)}
