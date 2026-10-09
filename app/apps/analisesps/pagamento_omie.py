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
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("analisesps.pagamento_omie")

# Cada título é uma chamada ao Omie (ele não consulta vários de uma vez por
# código de integração). Passar disso é tela parada esperando.
MAX_POR_CONSULTA = 60
# Três de cada vez: o Omie corta quem passa de algumas por segundo, e o
# cliente do painel já sabe esperar quando isso acontece.
EM_PARALELO = 3

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


def consultar(ids, cliente=None) -> list[dict]:
    """O status no Omie de cada SP, ao lado do que a planilha diz."""
    from app.apps.painel.sync.omie_client import URL_CONTAPAGAR

    ids = list(dict.fromkeys(str(i).strip() for i in ids if str(i).strip()))
    registros = _registros(ids)
    def um(sp_id):
        # Um cliente por consulta: a sessão HTTP dele não é para dividir
        # entre as linhas que correm juntas.
        cli = cliente or _cliente()
        reg = registros.get(sp_id) or {"id": sp_id}
        codigo = codigo_de_integracao(sp_id, reg.get("codigo_integracao"))
        linha = {"id": sp_id, "credor": reg.get("credor") or "",
                 "valor": reg.get("valor") or "",
                 "status_planilha": reg.get("status_pgt") or "",
                 "codigo": codigo, "status_omie": "", "valor_pago": None,
                 "erro": "", "pago": False, "falta": [], "equalizar": False}
        if sp_id not in registros:
            linha["erro"] = "SP não está na base."
            return linha
        try:
            titulo = cli._call(URL_CONTAPAGAR, "ConsultarContaPagar",
                               {"codigo_lancamento_integracao": codigo}) or {}
        except Exception as e:  # noqa: BLE001 — a frase do Omie vai inteira
            texto = str(e)
            linha["erro"] = ("Título não encontrado no Omie com o código "
                             f"{codigo}." if re.search(r"n[ãa]o (foi )?encontrad",
                                                        texto, re.I)
                             else f"O Omie não respondeu: {texto[:200]}")
            return linha
        linha["status_omie"] = str(titulo.get("status_titulo") or "").upper()
        linha["valor_pago"] = titulo.get("valor_pago")
        linha["pago"] = linha["status_omie"] == STATUS_PAGO
        if linha["pago"]:
            linha["falta"] = falta_na_planilha(reg)
            linha["equalizar"] = bool(linha["falta"])
        return linha

    with ThreadPoolExecutor(max_workers=EM_PARALELO) as grupo:
        return list(grupo.map(um, ids))


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
