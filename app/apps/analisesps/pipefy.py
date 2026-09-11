# -*- coding: utf-8 -*-
"""
O pouco que este módulo precisa falar com o Pipefy — e só isso.

O Pipefy é a origem das SPs, mas a base de trabalho aqui é a planilha SPsBD.
Este arquivo existe por uma razão só: o **BeeVale**. Para montar as planilhas
é preciso o CPF e o valor que estão NO CARD (não na SPsBD), e no fim é preciso
escrever de volta no card os links dos arquivos.

⚠️ ESTE É O ÚNICO LUGAR DO MÓDULO QUE ESCREVE FORA. Tudo o mais grava na
planilha SPsBD e no banco. Aqui se altera o card de verdade, no Pipefy, e
**não há desfazer**. Duas consequências que não são opinião:

  - a descrição do card é REESCRITA. Por isso `montar_descricao` preserva o
    texto que já existia e só troca as linhas de link — perder a descrição de
    um pagamento seria perder o histórico de quem pediu o quê.
  - quem chama tem de ser OPERADOR. A regra da casa (`auth.py`) já cuida
    disso; o lembrete fica aqui porque quem mexer neste arquivo talvez não
    leia aquele.

O token vem de `credenciais.token("PIPEFY_TOKEN")` — ambiente primeiro, aba
Credenciais depois, como todo segredo daqui.
"""
from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger("analisesps.pipefy")

API = "https://api.pipefy.com/graphql"

# A "database" do Pipefy com o cadastro dos colaboradores BeeVale.
BASE_BEEVALE = "307056545"

# ---------------------------------------------------------------------------
# OS CAMPOS DO CARD, pelos identificadores que o Pipefy usa.
#
# Mudar um destes quebra em SILÊNCIO: o Pipefy aceita a chamada e não grava
# nada. Por isso a origem fica escrita e o UUID vai ao lado — o identificador
# muda se alguém renomear o campo na tela do Pipefy; o UUID, não.
#
# Os quatro primeiros já rodam em produção desde o BeeVale (05/09/2026). Os
# cinco da conciliação fiscal saíram da estrutura do pipe que o dono colou em
# 11/09/2026, conferidos um a um contra aquele JSON.
# ---------------------------------------------------------------------------
CAMPO_DESCRICAO = "descri_o"                    # f59b3fa0-8365-472d-b61e-73b5b538ad5b
CAMPO_CADASTRO = "cadastro_bee_vale"
CAMPO_VALOR = "valor"
CAMPO_DOC_FISCAL = "documenta_o_fiscal"         # 40c54379-ca2b-4410-a2a4-6aeab3ca6401

# Os campos que a conciliação fiscal preenche. A ORDEM ABAIXO É A DA ESCRITA,
# e ela não é arbitrária: "gerou nota" é o que destrava o resto no fluxo do
# Pipefy, e a chave é o que permite baixar o documento depois.
CAMPO_GEROU_NOTA = "a_despesa_gerou_emiss_o_de_nota_fiscal"   # f2453ccf-fb3c-4ba6-8667-d36a1133cc18
CAMPO_NUMERO_NOTA = "n_da_nota_fiscal"          # d19d97ad-6fac-4301-a18f-bbf4745600ac
CAMPO_CHAVE_ACESSO = "chave_de_acesso"          # fa6f8252-7634-468e-87ce-68dfb9eba167
CAMPO_ANALISE_DEDUT = "an_lise_dedutibilidade"  # 969cf0da-4a66-4e63-9072-54d31c66b90e
CAMPO_ETIQUETAS = "etiquetas"                   # 88ba0d09-06fa-41ce-9e7b-42c03fe040c5

# "A despesa gerou emissão de Nota Fiscal?" é Sim/Não — as duas únicas opções.
GEROU_NOTA_SIM = "Sim"
GEROU_NOTA_NAO = "Não"

# As 22 opções do campo Documentação Fiscal vivem em `fiscal.CATEGORIAS`, e são
# as MESMAS do JSON do pipe, na mesma ordem — conferido em 11/09/2026. Escrever
# ali um texto que não seja uma delas faz o Pipefy recusar o card inteiro, e é
# por isso que a lista tem um dono só.

# Quantos cards por ida à API. O Pipefy aceita várias consultas numa só
# requisição; vinte é o que o Apps Script usava e nunca deu problema.
POR_VEZ = 20


class ErroDoPipefy(RuntimeError):
    """Falha na conversa com o Pipefy, com a mensagem já pronta para a tela."""


def _token() -> str:
    from . import credenciais
    valor = credenciais.token("PIPEFY_TOKEN")
    if not valor:
        raise ErroDoPipefy(
            "O token do Pipefy não está configurado (PIPEFY_TOKEN), nem no "
            "Render nem na aba Credenciais.")
    return valor


def _blocos(lista, tamanho):
    lista = list(lista)
    return [lista[i:i + tamanho] for i in range(0, len(lista), tamanho)]


def _texto_gql(valor) -> str:
    """Texto virando literal de GraphQL, com aspas e escapes corretos.

    Usa o `json.dumps` de propósito: a descrição tem quebras de linha e aspas,
    e montar isso na mão é como se erra."""
    return json.dumps(str("" if valor is None else valor))


def _numero_do_card(valor) -> int:
    """O id do card como número — e recusa o que não for.

    Os ids entram na consulta SEM aspas (é assim que a API os quer), então uma
    string qualquer aqui seria injeção de GraphQL. Como todo id de card é
    numérico, exigir número fecha a porta sem custo nenhum."""
    try:
        return int(str(valor).strip())
    except (TypeError, ValueError):
        raise ErroDoPipefy(f"'{valor}' não é um número de card válido.") from None


def graphql(consulta: str, token: str | None = None) -> dict:
    import requests

    token = token or _token()
    try:
        resposta = requests.post(
            API, json={"query": consulta},
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/json"}, timeout=60)
    except Exception as e:  # noqa: BLE001 — rede caiu
        raise ErroDoPipefy(f"Não consegui falar com o Pipefy: {e}") from e

    try:
        dados = resposta.json()
    except Exception as e:  # noqa: BLE001 — veio HTML de erro, não JSON
        raise ErroDoPipefy(
            f"O Pipefy respondeu algo que não é JSON (HTTP "
            f"{resposta.status_code}): {resposta.text[:200]}") from e

    if not 200 <= resposta.status_code < 300:
        raise ErroDoPipefy(f"O Pipefy recusou (HTTP {resposta.status_code}): "
                           f"{resposta.text[:240]}")
    if dados.get("errors"):
        raise ErroDoPipefy("O Pipefy devolveu erro: "
                           + json.dumps(dados["errors"], ensure_ascii=False)[:300])
    return dados.get("data") or {}


def extrair_cpf(valor_do_conector) -> str:
    """O CPF dentro do campo "Cadastro BeeVale".

    O Pipefy devolve esse campo ora como lista JSON, ora como texto solto —
    depende de como o card foi preenchido. Os dois casos são tratados."""
    if not valor_do_conector:
        return ""
    try:
        analisado = json.loads(valor_do_conector)
        if isinstance(analisado, list) and analisado:
            return str(analisado[0] or "").strip()
    except Exception:  # noqa: BLE001 — não era JSON; tenta como texto
        pass
    achado = re.search(r"\d{3}\.\d{3}\.\d{3}-\d{2}", str(valor_do_conector))
    return achado.group(0) if achado else ""


def buscar_cards(ids, token=None) -> dict:
    """Os campos de cada card, por id: {id: {'id', 'campos': {...}}}."""
    token = token or _token()
    saida = {}
    for bloco in _blocos([str(i) for i in ids], POR_VEZ):
        pedacos = [
            f"c{i}: card(id: {_numero_do_card(cid)}) "
            f"{{ id fields {{ field {{ id label }} value }} }}"
            for i, cid in enumerate(bloco)]
        dados = graphql("query { " + "\n".join(pedacos) + " }", token)
        for card in (dados or {}).values():
            if not card:
                continue
            campos = {}
            for campo in (card.get("fields") or []):
                identificador = (campo.get("field") or {}).get("id")
                if identificador:
                    campos[identificador] = (
                        "" if campo.get("value") is None
                        else str(campo.get("value")))
            saida[str(card["id"])] = {"id": str(card["id"]), "campos": campos}
    return saida


def buscar_cadastros(cpfs, token=None) -> dict:
    """O cadastro do colaborador na database BeeVale, por CPF.

    São duas voltas: a primeira acha o registro pelo CPF, a segunda lê os
    campos dele. A API não devolve as duas coisas de uma vez."""
    token = token or _token()

    cpf_para_registro = {}
    for bloco in _blocos(list(cpfs), POR_VEZ):
        pedacos = [
            f'r{i}: findRecords(tableId: "{BASE_BEEVALE}", '
            f'search: {{ fieldId: "cpf", fieldValue: {_texto_gql(cpf)} }}) '
            f"{{ edges {{ node {{ id title }} }} }}"
            for i, cpf in enumerate(bloco)]
        dados = graphql("query { " + "\n".join(pedacos) + " }", token)
        for embrulho in (dados or {}).values():
            arestas = (embrulho or {}).get("edges") or []
            if not arestas:
                continue
            no = arestas[0].get("node") or {}
            cpf = str(no.get("title", "")).strip()
            registro = str(no.get("id", "")).strip()
            if cpf and registro:
                cpf_para_registro[cpf] = registro

    saida = {}
    for bloco in _blocos(list(cpf_para_registro.items()), POR_VEZ):
        pedacos = [
            f"t{i}: table_record(id: {_numero_do_card(registro)}) "
            f"{{ id title record_fields {{ indexName name value }} }}"
            for i, (_cpf, registro) in enumerate(bloco)]
        dados = graphql("query { " + "\n".join(pedacos) + " }", token)
        for no in (dados or {}).values():
            if not no:
                continue
            # O mesmo campo aparece com nome de tela e com nome interno; os
            # dois entram no dicionário para a busca abaixo não depender de
            # qual deles a database usa.
            por_nome = {}
            for campo in (no.get("record_fields") or []):
                nome = str(campo.get("name", "")).strip().lower()
                interno = str(campo.get("indexName", "")).strip().lower()
                valor = ("" if campo.get("value") is None
                         else str(campo.get("value")).strip())
                if nome:
                    por_nome[nome] = valor
                if interno:
                    por_nome[interno] = valor
            cpf = por_nome.get("cpf") or str(no.get("title", "")).strip()
            saida[cpf] = {
                "id": str(no.get("id", "")),
                "nome_completo": (por_nome.get("nome completo")
                                  or por_nome.get("nome_completo") or ""),
                "data_de_nascimento": (por_nome.get("data de nascimento")
                                       or por_nome.get("data_de_nascimento") or ""),
                "telefone_celular": (por_nome.get("telefone celular")
                                     or por_nome.get("telefone_celular") or ""),
                "cpf": cpf,
            }
    return saida


def atualizar_descricao_e_doc_fiscal(atualizacoes, token=None) -> list:
    """Escreve a descrição nova e marca Documentação Fiscal = "BeeVale".

    `atualizacoes`: [{'card': '123', 'descricao': '...'}, ...].
    Devolve a lista dos cards que FALHARAM — vazia quer dizer tudo certo.

    ⚠️ Esta é a chamada sem volta. Ver o aviso no alto do arquivo."""
    token = token or _token()
    falhas = []
    for bloco in _blocos(list(atualizacoes), POR_VEZ):
        pedacos = []
        for i, item in enumerate(bloco):
            pedacos.append(
                f"m{i}: updateFieldsValues(input: {{ "
                f"nodeId: {_numero_do_card(item['card'])}, values: ["
                f'{{ fieldId: "{CAMPO_DESCRICAO}", '
                f"value: {_texto_gql(item['descricao'])} }}, "
                f'{{ fieldId: "{CAMPO_DOC_FISCAL}", value: "BeeVale" }} '
                f"] }}) {{ success }}")
        dados = graphql("mutation { " + "\n".join(pedacos) + " }", token)
        for i, item in enumerate(bloco):
            resultado = (dados or {}).get(f"m{i}")
            if not resultado or resultado.get("success") is not True:
                falhas.append(str(item["card"]))
    if falhas:
        logger.warning("Análise de SPs: %d card(s) não aceitaram a atualização "
                       "no Pipefy.", len(falhas))
    return falhas
