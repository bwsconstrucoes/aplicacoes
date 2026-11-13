# -*- coding: utf-8 -*-
"""O cliente GraphQL do Pipefy, na medida do que o ERP precisa: LER cards.

POR QUE MAIS UM, se `analisesps`, `baixabradesco` e `emissaonf` já têm o deles?
Porque é o padrão da casa, e porque o contrário é pior: o ERP importando o
módulo de outra área ficaria quebrado no dia em que aquela área refizesse o
arquivo dela — e quem mexeu não teria como saber. Cada área fala com o Pipefy
pela porta dela; o token é que é um só.

⚑ O TOKEN É O `PIPEFY_API_TOKEN` QUE A PRODUÇÃO JÁ TEM. Não se cria variável
nova para credencial (regra do `CLAUDE.md`), e aqui nem seria preciso: o
monorepo inteiro já usa esse nome. Aceita `PIPEFY_TOKEN` também, porque o
Análise de SPs usa os dois.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

API = "https://api.pipefy.com/graphql"

# Quantos cards por ida à API. O Pipefy aceita vários apelidos numa consulta
# só (`c0: card(...) c1: card(...)`), e cada ida custa tempo de rede: com 575
# pessoas, 20 por vez são 29 idas em vez de 575.
POR_VEZ = 20

# Segundos. Generoso de propósito: a consulta carrega os campos de 20 cards.
ESPERA = 60


class ErroDoPipefy(RuntimeError):
    """Deu errado do lado do Pipefy — e a frase dele vem junto."""


def tem_token() -> bool:
    """A tela pergunta isto ANTES de oferecer o botão.

    Oferecer uma importação que vai falhar por falta de token é fazer a pessoa
    descobrir no erro o que dava para saber na porta."""
    return bool(_token_ou_nada())


def _token_ou_nada() -> str:
    return (os.environ.get("PIPEFY_API_TOKEN")
            or os.environ.get("PIPEFY_TOKEN") or "").strip()


def _token() -> str:
    valor = _token_ou_nada()
    if not valor:
        raise ErroDoPipefy(
            "O token do Pipefy não está configurado neste servidor "
            "(PIPEFY_API_TOKEN). Sem ele não dá para ler os cards.")
    return valor


def _numero_do_card(valor: Any) -> int:
    """O id do card como número — e recusa o que não for.

    ⚑ Os ids entram na consulta SEM aspas (é assim que a API os quer), então
    um texto qualquer aqui seria injeção de GraphQL. Como todo id de card é
    numérico, exigir número fecha a porta sem custo nenhum. E aqui o valor vem
    de `colaboradores.ref_pipefy`, que é texto digitado por gente."""
    try:
        return int(str(valor).strip())
    except (TypeError, ValueError):
        raise ErroDoPipefy(
            f"{valor!r} não é um número de card do Pipefy.") from None


def card_valido(valor: Any) -> bool:
    """Sem levantar erro — para a simulação poder CONTAR os inválidos em vez
    de parar no primeiro."""
    try:
        _numero_do_card(valor)
        return True
    except ErroDoPipefy:
        return False


def _blocos(lista: Iterable[Any], tamanho: int) -> list[list[Any]]:
    lista = list(lista)
    return [lista[i:i + tamanho] for i in range(0, len(lista), tamanho)]


def _postar(consulta: str, token: Optional[str] = None) -> dict:
    import requests

    token = token or _token()
    try:
        resposta = requests.post(
            API, json={"query": consulta},
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/json"}, timeout=ESPERA)
    except Exception as e:                       # noqa: BLE001 — a rede caiu
        raise ErroDoPipefy(f"Não consegui falar com o Pipefy: {e}") from e
    try:
        dados = resposta.json()
    except Exception as e:                # noqa: BLE001 — veio HTML, não JSON
        raise ErroDoPipefy(
            f"O Pipefy respondeu algo que não é JSON (HTTP "
            f"{resposta.status_code}): {resposta.text[:200]}") from e
    if not 200 <= resposta.status_code < 300 and not (dados or {}).get("errors"):
        raise ErroDoPipefy(f"O Pipefy recusou (HTTP {resposta.status_code}): "
                           f"{resposta.text[:240]}")
    return dados or {}


def graphql_parcial(consulta: str,
                    token: Optional[str] = None) -> tuple[dict, dict]:
    """Devolve `(dados, {apelido: erro})`.

    ⚑ UM CARD COM PROBLEMA NÃO ESCONDE OS OUTROS. Numa importação de 575
    pessoas, parar no primeiro erro é transformar um card apagado no Pipefy em
    "a importação não funciona"."""
    dados = _postar(consulta, token)
    erros = {}
    for e in dados.get("errors") or []:
        caminho = (e or {}).get("path") or ["?"]
        erros[str(caminho[0])] = str((e or {}).get("message") or e)
    return dados.get("data") or {}, erros


def ler_cards(ids: Iterable[Any],
              token: Optional[str] = None) -> tuple[dict, dict]:
    """Os campos de cada card, por id — e os que não vieram, com o motivo.

    Devolve `({id: {"id", "titulo", "fase", "campos": {id_do_campo: valor}}},
    {id: motivo})`. O valor vem como TEXTO, igual ao que a API devolve: quem
    sabe o que o campo significa é o de-para, não este arquivo.
    """
    token = token or _token()
    saida: dict[str, dict] = {}
    falhas: dict[str, str] = {}
    pedidos = [str(i) for i in ids]
    for bloco in _blocos(pedidos, POR_VEZ):
        apelidos = {}
        pedacos = []
        for i, cid in enumerate(bloco):
            try:
                numero = _numero_do_card(cid)
            except ErroDoPipefy as e:
                falhas[str(cid)] = str(e)
                continue
            apelidos[f"c{i}"] = str(cid)
            pedacos.append(
                f"c{i}: card(id: {numero}) {{ id title "
                f"current_phase {{ name }} "
                f"fields {{ field {{ id label type }} value }} }}")
        if not pedacos:
            continue
        dados, erros = graphql_parcial("query { " + "\n".join(pedacos) + " }",
                                       token)
        for apelido, frase in erros.items():
            falhas[apelidos.get(apelido, apelido)] = frase
        for apelido, card in (dados or {}).items():
            pedido = apelidos.get(apelido, apelido)
            if not card:
                falhas.setdefault(
                    pedido, "O Pipefy não devolveu este card (apagado?).")
                continue
            campos = {}
            for campo in (card.get("fields") or []):
                ident = (campo.get("field") or {}).get("id")
                if ident:
                    campos[ident] = ("" if campo.get("value") is None
                                     else str(campo.get("value")))
            saida[str(pedido)] = {
                "id": str(card.get("id") or pedido),
                "titulo": str(card.get("title") or ""),
                "fase": str((card.get("current_phase") or {}).get("name") or ""),
                "campos": campos}
    logger.info("ERP/pipefy: %d card(s) lidos, %d sem resposta",
                len(saida), len(falhas))
    return saida, falhas


PIPE_CADASTRO = "301487297"          # CADASTRO COLABORADORES


def varrer_pipe(pipe_id: str = PIPE_CADASTRO, token: Optional[str] = None,
                campos: Optional[tuple[str, ...]] = None,
                teto_de_paginas: int = 200,
                aviso=None) -> list[dict]:
    """TODOS os cards do pipe, página por página — só os campos pedidos.

    ⚑ POR QUE ISTO EXISTE, e é o conserto do defeito de 13/11/2026: eu tinha
    construído a importação assumindo que o número do card já estava no
    cadastro de cada pessoa (`colaboradores.ref_pipefy`). **Não estava** — e,
    como não havia onde digitar, o botão nunca ia funcionar. Ele viu na hora:
    *"onde é que eu coloco os números do card se não tem campo?"*

    A resposta certa é que **não deve haver campo nenhum**: o card tem o CPF,
    o cadastro tem o CPF, e é o sistema que casa os dois. Ninguém digita
    3.664 números.

    A paginação é a mesma do `scripts/exportar_pipefy.py`, que já roda: 50 por
    vez, seguindo o cursor. `campos` enxuga o que vem de cada card — varrer o
    pipe inteiro trazendo todos os 51 campos seria muito mais dado do que o
    casamento por CPF precisa.
    """
    token = token or _token()
    saida: list[dict] = []
    cursor, pagina = None, 0
    pedacos = ("field { id } value" if not campos else "field { id } value")
    while pagina < teto_de_paginas:
        depois = f', after: "{cursor}"' if cursor else ""
        consulta = (
            "query { allCards(pipeId: %s, first: 50%s) {"
            " pageInfo { hasNextPage endCursor }"
            " edges { node { id title fields { %s } } } } }"
            % (_numero_do_card(pipe_id), depois, pedacos))
        dados, erros = graphql_parcial(consulta, token)
        if erros:
            raise ErroDoPipefy("O Pipefy devolveu erro ao varrer o pipe: "
                               + " | ".join(str(v) for v in erros.values()))
        bloco = (dados or {}).get("allCards") or {}
        for borda in (bloco.get("edges") or []):
            no = (borda or {}).get("node") or {}
            lidos = {}
            for campo in (no.get("fields") or []):
                ident = (campo.get("field") or {}).get("id")
                if ident and (campos is None or ident in campos):
                    lidos[ident] = ("" if campo.get("value") is None
                                    else str(campo.get("value")))
            saida.append({"id": str(no.get("id") or ""),
                          "titulo": str(no.get("title") or ""),
                          "campos": lidos})
        pagina += 1
        if aviso:
            aviso(len(saida), pagina)
        if not (bloco.get("pageInfo") or {}).get("hasNextPage"):
            break
        cursor = (bloco.get("pageInfo") or {}).get("endCursor")
        if not cursor:
            break
    logger.info("ERP/pipefy: %d card(s) varridos em %d página(s)",
                len(saida), pagina)
    return saida


# ---------------------------------------------------------------------------
# OS ANEXOS
# ---------------------------------------------------------------------------
# O Pipefy devolve campo de anexo como uma LISTA EM TEXTO (`["...", "..."]`),
# e cada item ora é o endereço completo do arquivo, ora só o caminho dele
# dentro do armazenamento. Os dois casos são tratados — e o que não dá para
# entender é DITO, nunca chutado: baixar o arquivo errado e arquivá-lo como
# documento de alguém é pior que não baixar nada.
# ---------------------------------------------------------------------------
def lista_do_campo(valor: Any) -> list[Any]:
    """Os itens de um campo que guarda vários (anexo, conector, caixinha)."""
    if valor in (None, "", "[]"):
        return []
    if isinstance(valor, list):
        return [x for x in valor if x]
    texto = str(valor).strip()
    if texto.startswith("["):
        try:
            lido = json.loads(texto)
        except ValueError:
            return [texto]
        if isinstance(lido, list):
            return [x for x in lido if x]
        return [lido]
    return [texto]


def endereco_do_anexo(item: Any) -> str:
    """O endereço de onde baixar — ou VAZIO quando não dá para saber.

    ⚑ E VAZIO É UMA RESPOSTA LEGÍTIMA, não um caso a contornar. Eu ia montar o
    endereço a partir de um caminho solto (`orgs/…/uploads/…`), chutando a
    raiz do armazenamento do Pipefy. Não é preciso: o importador de cards que
    este monorepo JÁ roda em produção (`core/importadores/pipefy_cards.py`)
    mostra que, nesta conta, o campo de anexo vem com o endereço COMPLETO —
    ele aceita só o que começa com http, e funciona há meses.

    Então a regra é a mesma, e o que não for endereço completo vira linha do
    relatório. Chutar a raiz daria um endereço que ou não responde, ou — pior —
    responde outra coisa, e aí o arquivo de alguém entraria no cadastro de
    outra pessoa.
    """
    alvo = item.get("url") if isinstance(item, dict) else item
    alvo = str(alvo or "").strip()
    return alvo if alvo.startswith("http://") or alvo.startswith("https://") else ""


def nome_do_anexo(item: Any) -> str:
    """O nome do arquivo, para ele não chegar ao cadastro chamado 'anexo'."""
    alvo = item.get("url") if isinstance(item, dict) else item
    pedaco = str(alvo or "").split("?")[0].rstrip("/")
    nome = pedaco.rsplit("/", 1)[-1] if "/" in pedaco else pedaco
    from urllib.parse import unquote
    return unquote(nome)[:180] or "arquivo-do-pipefy"


def baixar(endereco: str, teto_bytes: int) -> bytes:
    """Baixa o arquivo — com TETO, porque arquivo sem teto já derrubou o
    monorepo uma vez (o OOM de julho de 2026, `CONTEXTO.md` §9)."""
    import requests

    if not endereco:
        raise ErroDoPipefy("Anexo sem endereço para baixar.")
    try:
        with requests.get(endereco, timeout=ESPERA, stream=True) as r:
            if not 200 <= r.status_code < 300:
                raise ErroDoPipefy(
                    f"O arquivo não veio (HTTP {r.status_code}).")
            pedacos, total = [], 0
            for pedaco in r.iter_content(chunk_size=64 * 1024):
                if not pedaco:
                    continue
                total += len(pedaco)
                if total > teto_bytes:
                    raise ErroDoPipefy(
                        f"Arquivo acima de {teto_bytes // (1024 * 1024)} MB — "
                        f"não baixei.")
                pedacos.append(pedaco)
            return b"".join(pedacos)
    except ErroDoPipefy:
        raise
    except Exception as e:                       # noqa: BLE001 — a rede caiu
        raise ErroDoPipefy(f"Não consegui baixar o arquivo: {e}") from e
