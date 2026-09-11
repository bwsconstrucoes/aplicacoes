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


# ---------------------------------------------------------------------------
# O ARMÁRIO DE RESERVA
#
# A tabela `preferencias` nasce na migração 003, e migração só entra quando
# alguém aperta "Aplicar atualizações do banco". Enquanto isso não acontecia,
# o filtro simplesmente NÃO ERA GUARDADO — o dono digitava o nome todo dia
# achando que estava separando o trabalho dele, e não estava.
#
# Depender de um botão para uma coisa que a pessoa espera que "só funcione" é
# um jeito de nunca funcionar. Então há um segundo lugar, `analisesps.meta`,
# que existe desde a migração 001 e portanto está no ar desde o primeiro dia:
# ele é (chave, valor), e a chave carrega dentro dela a pessoa e a preferência.
#
# QUANDO A TABELA BOA APARECER, ela passa a valer — e o que estiver no armário
# de reserva é COPIADO para lá na primeira leitura, em vez de virar lixo. Sem
# isso, apertar o botão pareceria apagar os filtros de todo mundo.
# ---------------------------------------------------------------------------
PREFIXO_RESERVA = "pref:"


def _tem_tabela() -> bool:
    """A tabela da migração 003 já existe?"""
    from .db import tem_coluna
    return tem_coluna("preferencias", "pessoa")


def _chave_reserva(pessoa: str, chave: str) -> str:
    return f"{PREFIXO_RESERVA}{str(pessoa or '')}:{chave}"


def _ler_reserva(pessoa: str, chave: str) -> str:
    from .db import consultar_um
    linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                         (_chave_reserva(pessoa, chave),))
    return (linha[0] if linha and linha[0] else "")


def _gravar_reserva(pessoa: str, chave: str, texto: str) -> None:
    from .db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.meta (chave, valor) VALUES (?, ?) "
            "ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor",
            (_chave_reserva(pessoa, chave), texto))
        conn.commit()


def _como_dicionario(texto: str, pessoa: str, chave: str) -> dict:
    if not texto:
        return {}
    try:
        valor = json.loads(texto)
    except (ValueError, TypeError):
        logger.warning("Análise de SPs: preferência %r de %r não é JSON válido; "
                       "ignorada.", chave, pessoa)
        return {}
    return valor if isinstance(valor, dict) else {}


def ler(pessoa: str, chave: str) -> dict:
    """A preferência guardada, ou um dicionário vazio."""
    from .db import consultar_um
    try:
        if not _tem_tabela():
            return _como_dicionario(_ler_reserva(pessoa, chave), pessoa, chave)

        linha = consultar_um(
            "SELECT valor FROM analisesps.preferencias "
            " WHERE pessoa = ? AND chave = ?", (str(pessoa or ""), chave))
        if linha and linha[0]:
            return _como_dicionario(linha[0], pessoa, chave)

        # A tabela boa existe mas está vazia para esta pessoa: pode ser que o
        # botão tenha sido apertado agora, e o que ela guardou antes esteja no
        # armário de reserva. Traz para cá, uma vez.
        guardado = _ler_reserva(pessoa, chave)
        if guardado:
            gravar(pessoa, chave, _como_dicionario(guardado, pessoa, chave))
            logger.info("Análise de SPs: preferência %r de %r trazida do "
                        "armário de reserva.", chave, pessoa)
            return _como_dicionario(guardado, pessoa, chave)
        return {}
    except Exception:  # noqa: BLE001 — banco fora do ar
        logger.exception("Análise de SPs: não consegui ler a preferência %r", chave)
        return {}


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
        if not _tem_tabela():
            _gravar_reserva(pessoa, chave, texto)
            return
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


def pessoas_conhecidas() -> list[dict]:
    """Quem já tem lote ou preferência guardados aqui.

    Existe por causa de uma fragilidade real do "nome como chave": digitar
    "Marcelo" hoje e "Marcelo Leitão" amanhã dá DUAS pessoas, e a segunda
    encontra o lote vazio sem entender por quê. O navegador já devolve o nome
    da última vez, o que resolve o caso comum; isto resolve o outro — o de
    quem trocou de máquina e digitou diferente.

    Não é controle de acesso: as quatro pessoas dividem a mesma senha, e a
    separação por nome é organizacional, não uma tranca. Dito assim para
    ninguém confundir as duas coisas."""
    try:
        from .db import consultar
        linhas = consultar(
            "SELECT pessoa, max(salvo_por), max(salvo_em) "
            "  FROM analisesps.lote WHERE pessoa <> '' "
            " GROUP BY pessoa ORDER BY 3 DESC LIMIT 20")
    except Exception:  # noqa: BLE001 — banco fora, ou migração 003 por aplicar
        logger.exception("Análise de SPs: não consegui listar quem já usou")
        return []
    return [{"chave": l[0], "nome": l[1] or l[0]} for l in linhas]
