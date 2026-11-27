# -*- coding: utf-8 -*-
"""
O RELATÓRIO ANUAL DE PRESENÇAS DO MOBPONTO, PARA O RATEIO DO 13º (27/11/2026).

O dono: a folha do 13º dizia *"Sem ponto carregado em 01/2026 … 08/2026"* — o
Mobponto, depois do pedido de cancelamento, passou a devolver a lista vazia para os
meses antigos. Ele mandou o "Presenças por Locais - ( Anual )" do Mobponto: *"Nele
temos pessoa, mês e obra onde ele trabalhou. Veja se com esse relatório
conseguiríamos contornar o problema."*

O ARQUIVO: tem extensão `.xls`, mas é uma TABELA HTML (o Mobponto exporta assim).
Uma linha por pessoa: CPF, nome e, para cada mês de JAN a DEZ, LOCAL e ETAPA. UM local
por mês — a obra do mês —, SEM quantos dias. O arquivo real de 27/11/2026: 1.506
pessoas, 66 locais (os mesmos códigos de obra do ponto), ETAPA sempre vazia.

COMO ENTRA NO 13º (`ponto.dias_do_ano`): o 13º se conta em avos — cada mês trabalhado
vale 1/12 —, então no rateio do 13º CADA MÊS VALE O MESMO. Mês que o sistema tem dia a
dia (a carga do Mobponto ou o ponto eletrônico): o mês se divide entre as obras pelos
dias. Mês SEM carga: vale a obra deste relatório, o mês inteiro. Escolha minha,
dita ao dono; a alternativa (pesar pelos dias) não é possível sem os dias.

Guardado em `analisesps.meta` (`ponto_anual.<ano>`), sem migração: ~1.500 pessoas ×
até 12 meses é texto pequeno. Importar de novo substitui.
"""
from __future__ import annotations

import html
import json
import logging
import re

logger = logging.getLogger("analisesps.ponto_anual")

TITULO = "Presenças por Locais"
_LINHA = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_CELULA = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
MAXIMO = 15 * 1024 * 1024


class ErroDoRelatorioAnual(RuntimeError):
    """Frase pronta para a tela."""


def _texto(conteudo: bytes) -> str:
    for codificacao in ("utf-8", "latin-1"):
        try:
            return conteudo.decode(codificacao)
        except UnicodeDecodeError:
            continue
    return conteudo.decode("latin-1", errors="replace")


def ler(conteudo: bytes) -> dict:
    """`{cpf: {mes: obra}}` (mes 1–12, cpf só dígitos). Levanta com a frase."""
    if not conteudo:
        raise ErroDoRelatorioAnual("arquivo vazio.")
    if len(conteudo) > MAXIMO:
        raise ErroDoRelatorioAnual("arquivo grande demais para o relatório anual de presenças.")
    texto = _texto(conteudo)
    if TITULO.lower() not in html.unescape(texto).lower() or "<table" not in texto.lower():
        raise ErroDoRelatorioAnual(
            'não é o relatório "Presenças por Locais - ( Anual )" do Mobponto. Exporte esse '
            "relatório e envie o arquivo como veio.")
    corpo = texto[texto.lower().find("<tbody"):] if "<tbody" in texto.lower() else texto
    pessoas: dict = {}
    for linha in _LINHA.findall(corpo):
        celulas = [" ".join(html.unescape(_TAG.sub("", c)).split()) for c in _CELULA.findall(linha)]
        if len(celulas) < 26:
            continue
        cpf = re.sub(r"\D", "", celulas[0])
        if len(cpf) != 11:
            continue
        meses = {}
        for m in range(12):
            obra = celulas[2 + 2 * m].strip().upper()
            if obra:
                meses[m + 1] = obra
        if meses:
            pessoas[cpf] = meses
    if not pessoas:
        raise ErroDoRelatorioAnual("nenhuma pessoa com local encontrada no relatório.")
    return pessoas


def _chave(ano: int) -> str:
    return f"ponto_anual.{int(ano)}"


def guardar(ano: int, pessoas: dict, quem: str = "", arquivo: str = "") -> dict:
    from .db import conexao
    from .horario import agora
    from .sincronizacao import _meta_gravar
    registro = {"importado_em": agora().isoformat(), "por": str(quem or "")[:80],
                "arquivo": str(arquivo or "")[:200],
                "pessoas": {cpf: {str(m): o for m, o in meses.items()}
                            for cpf, meses in pessoas.items()}}
    with conexao() as conn:
        _meta_gravar(conn, _chave(ano), json.dumps(registro, ensure_ascii=False))
        conn.commit()
    _GUARDADO.pop(int(ano), None)
    from . import ponto
    ponto._ANO_GUARDADO.clear()          # o rateio do 13º precisa refazer a conta
    por_mes = {}
    for meses in pessoas.values():
        for m in meses:
            por_mes[m] = por_mes.get(m, 0) + 1
    logger.info("Relatório anual de presenças %d importado por %s: %d pessoa(s).",
                ano, quem or "?", len(pessoas))
    return {"pessoas": len(pessoas), "por_mes": dict(sorted(por_mes.items()))}


_GUARDADO: dict = {}


def carregar(ano: int) -> dict | None:
    """`{"importado_em", "por", "arquivo", "pessoas": {cpf: {mes(int): obra}}}` ou None."""
    ano = int(ano)
    if ano in _GUARDADO:
        return _GUARDADO[ano]
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?", (_chave(ano),))
        dado = json.loads(linha[0]) if linha and linha[0] else None
    except Exception:  # noqa: BLE001 — guardado ilegível vale como "não há"
        logger.exception("Relatório anual de presenças %d: não consegui ler", ano)
        dado = None
    if dado:
        dado["pessoas"] = {cpf: {int(m): o for m, o in meses.items()}
                           for cpf, meses in (dado.get("pessoas") or {}).items()}
    _GUARDADO[ano] = dado
    return dado
