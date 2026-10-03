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
        # ⚠️ A FRASE INTEIRA, NÃO OS PRIMEIROS 300 CARACTERES. Em 01/10/2026 o
        # recado de "campo obrigatório" chegou cortado no meio do quarto nome de
        # campo — justamente a lista que diz o que falta preencher.
        frases = [str((e or {}).get("message") or e) for e in dados["errors"]]
        raise ErroDoPipefy("O Pipefy devolveu erro: " + " | ".join(frases)[:1500])
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


def atualizar_documentacao_fiscal(atualizacoes, token=None) -> dict:
    """Escreve a análise fiscal no card: categoria, chave, "gerou nota" e o nº.

    `atualizacoes`: [{'card': '123', 'documentacao': 'NF-e (Mercadoria)',
                      'chave': '...', 'numero': '1430'}, ...].
    Devolve {'ok': [cards], 'falhas': {card: motivo}} — quem chama precisa
    saber QUAIS passaram, não só quantos, porque só esses podem ser marcados
    como escritos.

    ⚠️ Esta é a chamada sem volta. Ver o aviso no alto do arquivo.

    SÓ ESCREVE O QUE TEM VALOR. Mandar chave vazia para um card que já tem a
    chave preenchida APAGARIA a chave — e apagar o que outra pessoa preencheu
    à mão seria o pior efeito possível desta tela.

    "A despesa gerou emissão de Nota Fiscal?" só vira "Sim", nunca "Não":
    quando a nota foi encontrada, a resposta é sim. Não ter encontrado não
    prova que não existe — pode ser nota fora do relatório do FSist —, e
    escrever "Não" ali seria afirmar o que este módulo não sabe."""
    token = token or _token()
    passaram, falhas = [], {}

    for bloco in _blocos(list(atualizacoes), POR_VEZ):
        pedacos, cards = [], []
        for i, item in enumerate(bloco):
            valores = []
            documentacao = str(item.get("documentacao") or "").strip()
            if documentacao:
                valores.append(f'{{ fieldId: "{CAMPO_DOC_FISCAL}", '
                               f"value: {_texto_gql(documentacao)} }}")
            chave = re.sub(r"\D", "", str(item.get("chave") or ""))
            if len(chave) == 44:
                valores.append(f'{{ fieldId: "{CAMPO_CHAVE_ACESSO}", '
                               f"value: {_texto_gql(chave)} }}")
                valores.append(f'{{ fieldId: "{CAMPO_GEROU_NOTA}", '
                               f'value: "{GEROU_NOTA_SIM}" }}')
            numero = str(item.get("numero") or "").strip()
            if numero:
                valores.append(f'{{ fieldId: "{CAMPO_NUMERO_NOTA}", '
                               f"value: {_texto_gql(numero)} }}")
            if not valores:
                falhas[str(item.get("card"))] = "nada para escrever"
                continue
            pedacos.append(
                f"m{len(cards)}: updateFieldsValues(input: {{ "
                f"nodeId: {_numero_do_card(item['card'])}, "
                f"values: [{', '.join(valores)}] }}) {{ success }}")
            cards.append(str(item["card"]))

        if not pedacos:
            continue
        try:
            dados = graphql("mutation { " + "\n".join(pedacos) + " }", token)
        except Exception as e:  # noqa: BLE001 — um bloco ruim não derruba os outros
            logger.exception("Análise de SPs: falhou um bloco no Pipefy")
            for card in cards:
                falhas[card] = str(e)[:300]
            continue
        for i, card in enumerate(cards):
            resultado = (dados or {}).get(f"m{i}")
            if resultado and resultado.get("success") is True:
                passaram.append(card)
            else:
                falhas[card] = "o Pipefy não confirmou a gravação"

    if falhas:
        logger.warning("Análise de SPs: %d card(s) não aceitaram a análise "
                       "fiscal.", len(falhas))
    return {"ok": passaram, "falhas": falhas}


# ---------------------------------------------------------------------------
# OS CAMPOS DE UM PIPE, LIDOS DELE MESMO
#
# ⚠️ POR QUE LER EM VEZ DE ESCREVER OS IDs NO CÓDIGO. Os campos do pipe de
# Despesa com Colaboradores estão hoje espalhados no blueprint do Make, e o
# blueprint tem defeito conhecido: o par 62 grava em `valor_centro_de_custo_63`
# (ver `docs/FOLHA_DE_PAGAMENTO.md` §4). Copiar essa lista para cá copiaria o
# defeito, e um campo trocado num card de despesa põe valor de um centro de custo
# no vizinho — erro que só aparece no fechamento da obra, meses depois.
#
# Lendo do pipe, o mapeamento é o que o Pipefy diz que é HOJE. Campo renomeado
# passa a aparecer com o nome novo, e campo que eu não reconheço fica DITO em vez
# de preenchido no escuro.
# ---------------------------------------------------------------------------
# As três formas de perguntar os campos, da mais completa à mais simples. A
# primeira traz o OBRIGATÓRIO e a tabela ligada a um campo de conexão; se o
# Pipefy recusar alguma dessas palavras, cai para a seguinte em vez de deixar a
# conferência inteira de pé no chão.
_CONSULTAS_DOS_CAMPOS = (
    "id label type options required "
    "connectedRepo { __typename ... on Table { id name } ... on Pipe { id name } }",
    "id label type options required connectedRepo { __typename ... on Table { id name } }",
    "id label type options required",
    "id label type options",
)


def campos_do_pipe(pipe_id, token=None) -> dict:
    """Os campos do formulário inicial do pipe:
    `{id: {label, tipo, opcoes, obrigatorio, ligado_a}}`.

    Só leitura — não cria nem muda nada. É o passo de conferência antes de deixar
    alguém apertar "lançar no Pipefy".

    ⚠️ O "OBRIGATÓRIO" É O QUE FALTAVA. Em 01/10/2026 o primeiro lançamento de
    verdade voltou recusado: "Data de Vencimento", "Tipo de Despesa",
    "Requisição Solicitada por um Terceiro?", "Responsável pela Solicitação"…
    são obrigatórios no pipe, e o card saía sem eles. Sabendo quais são, a tela
    pergunta antes de criar, em vez de o Pipefy recusar depois."""
    pipe = _numero_do_card(pipe_id)
    dados, ultimo_erro, recusas = None, None, []
    for pedaco in _CONSULTAS_DOS_CAMPOS:
        try:
            dados = graphql(
                "{ pipe(id: %d) { id name start_form_fields { %s } "
                "  phases { id name } } }" % (pipe, pedaco), token)
            break
        except ErroDoPipefy as e:
            # Token ausente ou rede caída não melhoram com consulta menor.
            if "devolveu erro" not in str(e):
                raise
            ultimo_erro = e
            # Guardada para a tela: sem ela, "a conexão não diz a que tabela
            # está ligada" não tem explicação (03/10/2026).
            recusas.append(str(e))
    if dados is None:
        raise ultimo_erro
    bruto = (dados or {}).get("pipe") or {}
    campos = {}
    for campo in bruto.get("start_form_fields") or []:
        repo = campo.get("connectedRepo") or {}
        campos[str(campo.get("id") or "")] = {
            "label": str(campo.get("label") or ""),
            "tipo": str(campo.get("type") or ""),
            "opcoes": campo.get("options") or [],
            "obrigatorio": bool(campo.get("required")),
            "ligado_a": ({"tipo": "tabela" if repo.get("__typename") == "Table"
                          else "pipe", "id": str(repo.get("id") or ""),
                          "nome": str(repo.get("name") or "")}
                         if repo.get("id") else None)}
    # Os campos das FASES (o que não está no formulário inicial). Leitura à parte e
    # tolerante: serve para saber o que vai na criação e o que vai depois, e uma
    # recusa aqui não pode derrubar a conferência.
    das_fases = {}
    try:
        fases = graphql("{ pipe(id: %d) { phases { id fields { id label type } } } }"
                        % pipe, token)
        for fase in ((fases or {}).get("pipe") or {}).get("phases") or []:
            for campo in (fase or {}).get("fields") or []:
                das_fases[str(campo.get("id") or "")] = {
                    "label": str(campo.get("label") or ""),
                    "tipo": str(campo.get("type") or ""),
                    "fase": str((fase or {}).get("id") or "")}
    except ErroDoPipefy:
        logger.exception("Análise de SPs: não consegui ler os campos das fases")
    return {"id": str(bruto.get("id") or ""), "nome": bruto.get("name") or "",
            "campos": campos, "campos_das_fases": das_fases, "recusas": recusas,
            "fases": [{"id": str(f.get("id") or ""), "nome": f.get("name") or ""}
                      for f in (bruto.get("phases") or [])]}


def achar_campo(campos: dict, *pedacos, fora=()) -> str:
    """O id do campo cujo rótulo casa com os pedaços (sem caixa e sem acento).

    ⚠️ O RÓTULO EXATO GANHA DO PARECIDO, e isto não é refinamento: o pipe de
    Despesa tem um campo "Valor" e setenta e cinco campos "Valor Centro de Custo
    N". Procurando só por conter "valor", o TOTAL da despesa poderia ser escrito
    dentro do valor de um centro de custo — e valor de centro de custo trocado só
    aparece no fechamento da obra, meses depois.

    `fora` lista pedaços que DESQUALIFICAM o campo, para o caso de o rótulo exato
    não existir e a busca precisar cair na aproximação.

    Devolve "" quando não achou — e quem chama tem de tratar isso dizendo o que
    ficou de fora, nunca escolhendo um campo parecido."""
    import unicodedata

    def limpar(texto):
        sem = unicodedata.normalize("NFKD", str(texto or ""))
        return "".join(c for c in sem if not unicodedata.combining(c)).lower()

    procurados = [limpar(p) for p in pedacos if p]
    if not procurados:
        return ""
    proibidos = [limpar(p) for p in (fora or ()) if p]
    alvo = " ".join(procurados)

    # 1ª passada: o rótulo É exatamente o que se procura.
    for campo_id, dados in (campos or {}).items():
        if limpar(dados.get("label")).strip() == alvo:
            return campo_id
    # 2ª passada: contém todos os pedaços e nenhum dos proibidos.
    for campo_id, dados in (campos or {}).items():
        rotulo = limpar(dados.get("label"))
        if all(p in rotulo for p in procurados) and not any(
                p in rotulo for p in proibidos):
            return campo_id
    return ""


# Teto de registros lidos de uma tabela de conexão: 10 páginas de 50. Tabela de
# tipo de despesa ou de pessoas cabe com folga; uma maior que isso não deveria
# virar lista de escolher.
PAGINAS_DE_REGISTROS = 10


def registros_da_tabela(tabela_id, token=None) -> list:
    """`[{'id', 'nome'}]` — os registros de uma tabela do Pipefy (campo de
    conexão). Só leitura."""
    tabela = _texto_gql(str(tabela_id or "").strip())
    saida, depois = [], None
    for _ in range(PAGINAS_DE_REGISTROS):
        cursor = f", after: {_texto_gql(depois)}" if depois else ""
        dados = graphql(
            "{ table_records(table_id: %s, first: 50%s) { "
            "  pageInfo { hasNextPage endCursor } "
            "  edges { node { id title } } } }" % (tabela, cursor), token)
        bloco = (dados or {}).get("table_records") or {}
        for aresta in bloco.get("edges") or []:
            no = (aresta or {}).get("node") or {}
            if no.get("id"):
                saida.append({"id": str(no["id"]),
                              "nome": str(no.get("title") or no["id"])})
        pagina = bloco.get("pageInfo") or {}
        if not pagina.get("hasNextPage") or not pagina.get("endCursor"):
            break
        depois = pagina["endCursor"]
    return sorted(saida, key=lambda x: x["nome"].lower())


def tabela_do_registro(registro_id, token=None) -> dict | None:
    """`{'id', 'nome'}` da tabela a que um registro pertence — ou None.

    Serve para descobrir a tabela de um campo de conexão quando o Pipefy não
    diz a que ela está ligada: parte-se de um registro conhecido (os ids que o
    cenário do Make usava) e pergunta-se de que tabela ele é. Só leitura."""
    registro = _texto_gql(str(registro_id or "").strip())
    dados = graphql("{ table_record(id: %s) { id title table { id name } } }"
                    % registro, token)
    tabela = ((dados or {}).get("table_record") or {}).get("table") or {}
    if not tabela.get("id"):
        return None
    return {"id": str(tabela["id"]), "nome": str(tabela.get("name") or "")}


def cards_por_titulo(pipe_id, titulo: str, token=None) -> list:
    """`[{'id', 'nome'}]` — os cards de um pipe com este título (campo de
    conexão ligado a um PIPE, e não a uma tabela). Só leitura."""
    pipe = _numero_do_card(pipe_id)
    dados = graphql(
        "{ cards(pipe_id: %d, first: 50, search: { title: %s }) { "
        "  edges { node { id title } } } }" % (pipe, _texto_gql(titulo)), token)
    saida = []
    for aresta in ((dados or {}).get("cards") or {}).get("edges") or []:
        no = (aresta or {}).get("node") or {}
        if no.get("id"):
            saida.append({"id": str(no["id"]), "nome": str(no.get("title") or "")})
    return saida


def criar_card(pipe_id, titulo: str, valores: list, token=None) -> dict:
    """Cria um card. `valores`: `[{'campo': id, 'valor': texto}]`.

    ⚠️ CHAMADA SEM VOLTA: card criado no Pipefy não se apaga por aqui — quem
    cancela é gente, lá. Decisão do dono em 26/09/2026: *"a gente faz o
    cancelamento no Pipefy e gera de novo quando for necessário"*."""
    pipe = _numero_do_card(pipe_id)
    pedacos = []
    for item in valores or []:
        campo = str((item or {}).get("campo") or "").strip()
        if not campo:
            continue
        # O id do campo vai entre aspas na consulta, então escapá-lo fecha a porta
        # de injeção do mesmo jeito que o `_numero_do_card` fecha a dos ids.
        valor = (item or {}).get("valor")
        # Responsável, conexão, etiqueta e lista de marcar vão como LISTA de ids
        # (ou de opções) — é assim que a API os aceita; o resto, como texto.
        if isinstance(valor, (list, tuple)):
            literal = "[" + ", ".join(_texto_gql(v) for v in valor) + "]"
        else:
            literal = _texto_gql(valor)
        pedacos.append(f"{{ field_id: {_texto_gql(campo)}, "
                       f"field_value: {literal} }}")
    consulta = ("mutation { createCard(input: { pipe_id: %d, title: %s, "
                "fields_attributes: [%s] }) { card { id title url } } }"
                % (pipe, _texto_gql(titulo), " ".join(pedacos)))
    dados = graphql(consulta, token)
    card = ((dados or {}).get("createCard") or {}).get("card") or {}
    if not card.get("id"):
        raise ErroDoPipefy(
            "o Pipefy aceitou a chamada mas não devolveu o card criado.")
    logger.info("Análise de SPs: card %s criado no pipe %s.", card["id"], pipe)
    return {"id": str(card["id"]), "titulo": card.get("title") or titulo,
            "link": card.get("url") or f"https://app.pipefy.com/open-cards/{card['id']}"}


# Quantos campos por ida à API na atualização em lote. O Make liga até dez SPs
# numa chamada só; trinta campos curtos cabem com folga numa requisição.
CAMPOS_POR_VEZ = 30


def atualizar_campos(card_id, valores: list, token=None) -> int:
    """Grava vários campos de um card já criado, em poucas idas à API.

    `valores`: `[{'campo': id, 'valor': texto ou lista}]`. É o que o Make faz
    depois de criar o card (links das planilhas, banco, códigos do OMIE, a
    conexão entre os cards). Devolve quantos campos foram gravados.

    ⚠️ SEM VOLTA, como criar o card."""
    card = _numero_do_card(card_id)
    gravados = 0
    for bloco in _blocos([v for v in valores or [] if (v or {}).get("campo")],
                         CAMPOS_POR_VEZ):
        partes = []
        for i, item in enumerate(bloco):
            valor = item.get("valor")
            if isinstance(valor, (list, tuple)):
                literal = "[" + ", ".join(_texto_gql(v) for v in valor) + "]"
            else:
                literal = _texto_gql(valor)
            partes.append(
                f"c{i}: updateCardField(input: {{card_id: {card}, "
                f"field_id: {_texto_gql(item['campo'])}, new_value: {literal}}}) "
                "{ clientMutationId }")
        graphql("mutation { " + " ".join(partes) + " }", token)
        gravados += len(bloco)
    return gravados
