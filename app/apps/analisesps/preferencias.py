# -*- coding: utf-8 -*-
"""
O que cada pessoa deixou do jeito dela.

Hoje guarda uma coisa só — o último filtro usado — mas a tabela é genérica
(pessoa + chave + valor) porque a segunda preferência sempre vem, e criar uma
tabela por preferência é como se ganha uma dúzia de tabelas de uma linha.

POR QUE NO BANCO, E NÃO NO NAVEGADOR. O filtro salvo no navegador seria
perdido ao trocar de máquina, e é justamente trocando de máquina (do
computador da mesa para o notebook) que a pessoa mais sente falta dele. No
banco, ela entra com o nome dela em qualquer aparelho e encontra o filtro
como deixou. Era assim no Streamlit, que gravava na base local.

NADA AQUI DERRUBA TELA. Preferência é conforto: se a leitura falhar, a tela
abre sem filtro nenhum, que é um estado perfeitamente utilizável. Uma tela que
estoura porque não conseguiu lembrar de um filtro seria uma troca péssima.
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger("analisesps.preferencias")

FILTRO = "ultimo_filtro"

# Teto do que se aceita guardar. Um filtro real tem alguns milhares de bytes no
# pior caso (muitos centros de custo marcados). O teto existe para que um
# defeito em outro lugar não escreva um texto enorme no banco sem ninguém ver.
MAX_BYTES = 20_000


def ler(pessoa: str, chave: str) -> dict:
    """A preferência guardada, ou um dicionário vazio."""
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT valor FROM analisesps.preferencias "
            " WHERE pessoa = ? AND chave = ?", (str(pessoa or ""), chave))
    except Exception:  # noqa: BLE001 — banco fora, ou migração 003 não aplicada
        logger.exception("Análise de SPs: não consegui ler a preferência %r", chave)
        return {}
    if not linha or not linha[0]:
        return {}
    try:
        valor = json.loads(linha[0])
    except (ValueError, TypeError):
        logger.warning("Análise de SPs: preferência %r de %r não é JSON válido; "
                       "ignorada.", chave, pessoa)
        return {}
    return valor if isinstance(valor, dict) else {}


def gravar(pessoa: str, chave: str, valor: dict) -> None:
    """Guarda a preferência. Silencioso de propósito — ver o cabeçalho."""
    try:
        texto = json.dumps(valor or {}, ensure_ascii=False)
    except (TypeError, ValueError):
        logger.warning("Análise de SPs: preferência %r não é serializável.", chave)
        return
    if len(texto.encode("utf-8")) > MAX_BYTES:
        logger.warning("Análise de SPs: preferência %r grande demais (%d bytes); "
                       "não guardada.", chave, len(texto))
        return
    try:
        from .db import conexao
        with conexao() as conn:
            conn.execute(
                "INSERT INTO analisesps.preferencias (pessoa, chave, valor, salvo_em) "
                "VALUES (?, ?, ?, now()) "
                "ON CONFLICT (pessoa, chave) DO UPDATE SET "
                "  valor = EXCLUDED.valor, salvo_em = now()",
                (str(pessoa or ""), chave, texto))
            conn.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: não consegui guardar a preferência %r",
                         chave)
