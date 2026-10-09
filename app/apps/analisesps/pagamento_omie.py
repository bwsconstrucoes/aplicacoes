# -*- coding: utf-8 -*-
"""
CONSULTAR NO OMIE E EQUALIZAR A SPsBD — 08/10/2026.

O dono: *"Preciso poder consultar um ou vários títulos no Omie (…) num modal
(…) se for Pago, poder equalizar essa informação na planilha SPsBD, que com
alguma frequência não tem atualizado corretamente (…) coletar a Data do
Pagamento, o link do comprovante (…) e o Banco do Pagamento do Pipefy (…)
caso o card não esteja na fase 'Pago / Alimentar Omie', fazer esse movimento
também. Usar mutation para fazer requisições ao Pipefy em lote."*

Duas metades, as duas sem tela:

  consultar(ids)     — pergunta ao Omie o status de cada título, pelo código de
                       integração (coluna P; vazia, vale "Int" + nº da SP, que
                       é como o ProcessarNovaSP o cria). Só lê.
  ler_pagamentos()   — o que o card diz do pagamento (data, comprovante,
                       banco) e a fase em que ele está. Só lê.

Quem grava (SPsBD e fase do card) é a rota, em `web.py`: a gravação passa pelo
mesmo caminho de toda alteração da tela (banco + fila da planilha + log).
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger("analisesps.pagamento_omie")

# Cada título é uma chamada ao Omie (ele não consulta vários de uma vez por
# código de integração). Passar disso é tela parada esperando.
MAX_POR_CONSULTA = 60

STATUS_PAGO = "PAGO"


def codigo_de_integracao(sp_id, da_planilha: str = "") -> str:
    """O código do título no Omie: o da coluna P, ou "Int" + nº da SP."""
    codigo = str(da_planilha or "").strip()
    return codigo or f"Int{str(sp_id).strip()}"


def _cliente():
    from .conciliacao_omie import _cliente as cliente_de_tela
    return cliente_de_tela()


def _registros(ids) -> dict:
    from .db import consultar
    marcadores = ", ".join("?" for _ in ids)
    linhas = consultar(
        "SELECT id, credor, valor, status_pgt, codigo_integracao, "
        "       data_pagamento, comprovante "
        f"  FROM analisesps.sps WHERE id IN ({marcadores})", tuple(ids))
    chaves = ("id", "credor", "valor", "status_pgt", "codigo_integracao",
              "data_pagamento", "comprovante")
    return {str(l[0]): dict(zip(chaves, l)) for l in linhas}


def falta_na_planilha(reg: dict) -> list[str]:
    """O que a SPsBD ainda não diz de um título que o Omie diz PAGO."""
    falta = []
    if str(reg.get("status_pgt") or "").strip().lower() != "pago":
        falta.append("status")
    if not str(reg.get("data_pagamento") or "").strip():
        falta.append("data")
    if not str(reg.get("comprovante") or "").strip():
        falta.append("comprovante")
    return falta


# A ÚLTIMA RESPOSTA DO OMIE POR TÍTULO, guardada um pouco (09/10/2026). O Omie
# bloqueia quem repete a mesma pergunta em seguida ("consumo excessivo"), e é
# exatamente o que acontece quando se consulta, marca, e consulta de novo.
_GUARDADO: dict = {}
GUARDAR_POR_SEGUNDOS = 120


def consultar(ids, cliente=None) -> dict:
    """O status no Omie de cada SP, ao lado do que a planilha diz.

    Devolve `{"linhas": [...], "espera": segundos}`. ⚠️ QUANDO O OMIE PEDE UMA
    PAUSA (09/10/2026 — *"tem que contornar essas mensagens: o Omie bloqueou
    as chamadas por consumo excessivo e pediu 60 segundos"*), a consulta PARA
    ali: insistir em outro título cai no mesmo bloqueio e o prolonga. As que
    faltaram voltam marcadas `pendente`, com `espera` = o tempo pedido, e a
    janela continua sozinha depois desse tempo — a pessoa não refaz nada.

    UMA DE CADA VEZ (eram três juntas): três perguntas ao mesmo tempo é o
    jeito mais rápido de o Omie achar que é consumo excessivo."""
    import time
    from app.apps.painel.sync.omie_client import URL_CONTAPAGAR, OmieBloqueada

    ids = list(dict.fromkeys(str(i).strip() for i in ids if str(i).strip()))
    registros = _registros(ids)
    cli = None
    linhas, espera = [], 0
    for sp_id in ids:
        reg = registros.get(sp_id) or {"id": sp_id}
        codigo = codigo_de_integracao(sp_id, reg.get("codigo_integracao"))
        linha = {"id": sp_id, "credor": reg.get("credor") or "",
                 "valor": reg.get("valor") or "",
                 "status_planilha": reg.get("status_pgt") or "",
                 "codigo": codigo, "status_omie": "", "valor_pago": None,
                 "erro": "", "pago": False, "falta": [], "equalizar": False,
                 "pendente": False}
        linhas.append(linha)
        if sp_id not in registros:
            linha["erro"] = "SP não está na base."
            continue
        if espera:
            linha["pendente"] = True
            continue
        guardado = _GUARDADO.get(codigo)
        if guardado and time.monotonic() - guardado[0] < GUARDAR_POR_SEGUNDOS:
            titulo = guardado[1]
        else:
            try:
                cli = cli or cliente or _cliente()
                titulo = cli._call(URL_CONTAPAGAR, "ConsultarContaPagar",
                                   {"codigo_lancamento_integracao": codigo}) or {}
            except OmieBloqueada as e:
                espera = max(int(e.segundos), 1)
                linha["pendente"] = True
                logger.info("Consultar Omie: pausa de %ss pedida pelo Omie.", espera)
                continue
            except Exception as e:  # noqa: BLE001 — a frase do Omie vai inteira
                texto = str(e)
                linha["erro"] = ("Título não encontrado no Omie com o código "
                                 f"{codigo}." if re.search(r"n[ãa]o (foi )?encontrad",
                                                            texto, re.I)
                                 else f"O Omie não respondeu: {texto[:200]}")
                continue
            _GUARDADO[codigo] = (time.monotonic(), titulo)
        linha["status_omie"] = str(titulo.get("status_titulo") or "").upper()
        linha["valor_pago"] = titulo.get("valor_pago")
        linha["pago"] = linha["status_omie"] == STATUS_PAGO
        if linha["pago"]:
            linha["falta"] = falta_na_planilha(reg)
            linha["equalizar"] = bool(linha["falta"])
    return {"linhas": linhas, "espera": espera}


# ---------------------------------------------------------------------------
# O que o card diz — e como vira célula da SPsBD
# ---------------------------------------------------------------------------
def data_da_planilha(valor: str) -> str:
    """A data do card no formato da coluna X (dd/mm/aaaa). Vazio se ilegível."""
    from .formatos import para_data
    data = para_data(valor)
    return data.strftime("%d/%m/%Y") if data else ""


# "50024-0", "7011-4", "92945-8": o número da conta com o dígito.
_NUMERO_DA_CONTA = re.compile(r"\b\d{3,}-[\dXx]\b")


def conta_da_planilha(banco: str) -> str:
    """A coluna AK leva só o número da conta (ex.: '50024-0'), como o
    BaixaBradesco grava. Sem número reconhecível, vai o texto do card."""
    texto = str(banco or "").strip()
    achado = _NUMERO_DA_CONTA.findall(texto)
    return achado[-1] if achado else texto


def valores_do_card(card: dict) -> dict:
    """{coluna: valor} para a SPsBD — só o que o card TEM. Campo vazio no card
    não apaga o que a planilha já sabe."""
    saida = {}
    data = data_da_planilha(card.get("data"))
    if data:
        saida["data_pagamento"] = data
    if str(card.get("comprovante") or "").strip():
        saida["comprovante"] = str(card["comprovante"]).strip()
    conta = conta_da_planilha(card.get("banco"))
    if conta:
        saida["_ak"] = conta
    return saida
