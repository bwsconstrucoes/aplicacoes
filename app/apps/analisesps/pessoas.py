# -*- coding: utf-8 -*-
"""
Quem usa este módulo — a lista que aparece na entrada.

POR QUE UMA LISTA, E NÃO UM CAMPO LIVRE. O nome é a chave de tudo o que é
"seu": o seu lote, os seus filtros, as suas colunas. Com campo livre, digitar
"Marcelo" hoje e "Marcelo Leitão" amanhã dá DUAS pessoas — e a segunda abre o
sistema e encontra o lote vazio, sem entender por quê. Foi exatamente essa a
dúvida do dono. Escolher numa lista acaba com o problema na raiz: não há como
digitar diferente aquilo que não se digita.

ISTO NÃO É CONTROLE DE ACESSO, e a distinção importa. As quatro pessoas usam a
MESMA senha, e é a senha que decide o que se pode fazer. Escolher "KARLA" na
lista não dá poder nenhum a mais nem a menos — é uma etiqueta honesta entre
colegas, para o sistema saber de quem é cada lote. Quem precisar impedir que
alguém se passe por outro tem de usar o cadastro de usuários do ERP; aqui não
é o lugar.

ONDE A LISTA MORA. Na tabela `meta`, que existe desde a migração 001 — logo,
dá para mexer nela sem esperar botão nenhum. Editável em Configurações, para
o dono acrescentar ou tirar gente sem depender de uma publicação.
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger("analisesps.pessoas")

CHAVE = "pessoas"

# Quem o dono informou em 09/09/2026. É só o ponto de partida: a lista de
# verdade é a que estiver guardada, e ela se edita na tela.
PADRAO = ["MARCELO", "THIAGO", "KARLA", "RAFAEL"]

# Teto de gente. Não é limite de negócio — é para um defeito em outro lugar
# não encher a tela de entrada com mil opções.
MAXIMO = 40


def _limpar(nomes) -> list[str]:
    """Arruma a lista: sem vazios, sem repetidos, sem espaço sobrando.

    A comparação para "repetido" é a MESMA que separa as pessoas no resto do
    módulo (sem maiúscula e sem acento). Sem isso, "Karla" e "KARLA" entrariam
    como duas opções e cairiam no mesmo lote — uma lista que confunde em vez
    de resolver."""
    from .auth import chave_pessoa, limpar_nome

    vistos, saida = set(), []
    for bruto in (nomes or []):
        nome = limpar_nome(bruto)
        if not nome:
            continue
        chave = chave_pessoa(nome)
        if chave in vistos:
            continue
        vistos.add(chave)
        saida.append(nome)
        if len(saida) >= MAXIMO:
            break
    return saida


def listar() -> list[str]:
    """A lista de quem aparece na entrada.

    Nunca devolve vazio por falha: sem banco, ou com a linha ainda não
    gravada, vale a lista padrão. Uma tela de entrada sem nenhuma opção
    trancaria todo mundo do lado de fora."""
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (CHAVE,))
    except Exception:  # noqa: BLE001 — banco fora do ar não tranca a entrada
        logger.exception("Análise de SPs: não consegui ler a lista de pessoas")
        return list(PADRAO)

    if not linha or not linha[0]:
        return list(PADRAO)
    try:
        guardado = json.loads(linha[0])
    except (ValueError, TypeError):
        logger.warning("Análise de SPs: a lista de pessoas guardada não é JSON "
                       "válido; valendo a lista padrão.")
        return list(PADRAO)
    return _limpar(guardado) or list(PADRAO)


def gravar(nomes) -> list[str]:
    """Guarda a lista. Devolve como ela ficou, já arrumada."""
    from .db import conexao

    limpa = _limpar(nomes)
    if not limpa:
        # Lista vazia trancaria a entrada de todo mundo. Recusar aqui é mais
        # honesto do que aceitar e deixar a tela de entrada sem opção.
        raise ValueError("A lista não pode ficar vazia — sem nomes, ninguém "
                         "consegue se identificar na entrada.")
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.meta (chave, valor) VALUES (?, ?) "
            "ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor",
            (CHAVE, json.dumps(limpa, ensure_ascii=False)))
        conn.commit()
    logger.info("Análise de SPs: lista de pessoas gravada (%d).", len(limpa))
    return limpa


def da_lista(nome: str) -> str:
    """O nome DA LISTA que corresponde ao que veio da tela, ou "".

    A tela manda o texto escolhido; conferir contra a lista impede que um
    pedido montado à mão crie uma quinta pessoa por fora — e devolve o nome
    com a grafia oficial, não com a que veio no pedido."""
    from .auth import chave_pessoa

    procurado = chave_pessoa(nome)
    if not procurado:
        return ""
    for oficial in listar():
        if chave_pessoa(oficial) == procurado:
            return oficial
    return ""
