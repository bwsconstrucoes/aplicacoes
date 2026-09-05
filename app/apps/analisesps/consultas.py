# -*- coding: utf-8 -*-
"""
As perguntas que as telas fazem ao banco.

AQUI ESTÁ A DIFERENÇA para a versão em Streamlit. Lá, as 59 mil SPs eram
abertas na memória (162 MB por pessoa conectada, medidos) e o filtro rodava em
cima delas. Aqui, quem filtra e quem soma é o Postgres: a tela recebe a página
que vai mostrar e os poucos números do rodapé.

As regras de negócio são as MESMAS — status de agendamento, risco de
duplicidade, cadastro incompleto, boleto inválido e boleto duplicado foram
traduzidas uma a uma do `dados.py` original, sem mudar significado. Onde a
tradução muda alguma coisa, está escrito no comentário.
"""
from __future__ import annotations

import logging

from . import colunas

logger = logging.getLogger("analisesps.consultas")

# Quantas linhas por página. O Streamlit mandava até 2.000 de uma vez para a
# tabela e avisava quando cortava; uma página de 200 abre instantaneamente e
# não tem teto — a navegação alcança qualquer linha.
POR_PAGINA = 200

# Colunas cuja célula pode trazer MAIS DE UM valor. Hoje só o centro de custo
# (as obras): a planilha aceita duas na mesma célula quando a despesa é
# rateada entre elas. Tanto a lista do filtro quanto o casamento abrem a
# célula antes de comparar.
MULTIPLAS_NA_CELULA = {"centro_custo"}

# COMO A CÉLULA É ABERTA. A vírgula é o separador que o dono citou
# ("CONS, CRECHE SWAP"), mas a base real também traz BARRA
# ("OBRA-12 / OBRA-13") — está num teste escrito na época da conversão, a
# partir do dado de verdade. Aceitar os dois, e o ponto e vírgula de quebra,
# custa nada e evita descobrir o terceiro em produção.
SEPARADOR_DE_OBRAS = "[,;/]"

# Os campos varridos pela busca livre. Iguais aos do Streamlit.
CAMPOS_BUSCA = ["id", "credor", "documento", "descricao", "tipo_despesa",
                "centro_custo", "responsavel", "nf", "pedido", "analise_ia"]

# ---------------------------------------------------------------------------
# Pedaços de SQL que repetem a regra de negócio original
# ---------------------------------------------------------------------------

# Status de agendamento normalizado. Só aparece quando o status de pagamento é
# "Pagar"; nos demais (Pago, Cancelado) fica em branco — igual ao original.
SQL_STATUS_AGEND = """
CASE WHEN lower(trim(coalesce(status_pgt,''))) = 'pagar' THEN
  CASE
    WHEN lower(trim(coalesce(agendado,''))) LIKE '%falha%'  THEN 'Falha Agendar'
    WHEN lower(trim(coalesce(agendado,''))) = 'verificar'   THEN 'Verificar'
    WHEN lower(trim(coalesce(agendado,''))) = 'agendado'    THEN 'Agendado'
    WHEN lower(trim(coalesce(agendado,''))) = 'agendar'     THEN 'Agendar'
    ELSE '' END
ELSE '' END
"""

# Só os dígitos do código de barras — usado por "inválido" e "duplicado".
SQL_BARRAS_DIGITOS = r"regexp_replace(coalesce(codigo_barras,''), '\D', '', 'g')"

# Risco de duplicidade: é o que a análise da IA escreveu na coluna AL.
SQL_RISCO = "upper(coalesce(analise_ia,'')) LIKE '%COM RISCO%'"

# Cadastro incompleto — o "alerta laranja" do original. Três causas, e basta
# uma: (a) Pix/BeeVale sem a chave, (b) sem centro de custo, (c) sem código de
# integração do Omie num título ainda ativo.
SQL_CADASTRO_INCOMPLETO = r"""(
    (   (lower(coalesce(forma_pagamento,'')) LIKE '%pix%'
      OR lower(coalesce(forma_pagamento,'')) LIKE '%beevale%')
     AND trim(regexp_replace(coalesce(info_pgt,''),
                             'chave[[:space:]]*pix[[:space:]]*:?[[:space:]]*',
                             '', 'gi')) = '' )
 OR trim(coalesce(centro_custo,'')) = ''
 OR (    trim(coalesce(codigo_integracao,'')) = ''
     AND lower(trim(coalesce(status_pgt,''))) NOT IN ('cancelado','pago') )
)"""

# Boleto inválido: só entre os que estão a Pagar, como no original.
SQL_BOLETO_INVALIDO = (
    "( lower(trim(coalesce(forma_pagamento,''))) = 'boleto'"
    " AND lower(trim(coalesce(status_pgt,''))) = 'pagar'"
    " AND ( upper(translate(coalesce(codigo_barras,''),'Á','A')) LIKE '%INVALIDO%'"
    f"      OR {SQL_BARRAS_DIGITOS} = ''"
    f"      OR {SQL_BARRAS_DIGITOS} ~ '^0+$' ) )")

# Boleto duplicado. A contagem de repetições é feita no universo COMPLETO e
# considera Pagar UNIÃO Pago (é o conjunto em que existe risco de pagar duas
# vezes); Cancelado não conta. Mostra só os que estão a Pagar — assim, um par
# "1 Pago + 1 Pagar" exibe o Pagar, que é o que ainda pode ser pago em
# duplicidade. Idêntico ao original.
SQL_BOLETO_DUPLICADO = (
    "( lower(trim(coalesce(forma_pagamento,''))) = 'boleto'"
    " AND lower(trim(coalesce(status_pgt,''))) = 'pagar'"
    f" AND {SQL_BARRAS_DIGITOS} IN ("
    f"      SELECT {SQL_BARRAS_DIGITOS} FROM analisesps.sps"
    "        WHERE lower(trim(coalesce(forma_pagamento,''))) = 'boleto'"
    "          AND lower(trim(coalesce(status_pgt,''))) IN ('pagar','pago')"
    f"          AND {SQL_BARRAS_DIGITOS} <> ''"
    f"          AND {SQL_BARRAS_DIGITOS} !~ '^0+$'"
    "        GROUP BY 1 HAVING count(*) > 1 ) )")

# Hoje, em Brasília — não no UTC em que o servidor roda.
#
# A diferença é de três horas, e ela muda a resposta: entre 21h e meia-noite
# de Brasília o servidor já virou o dia. Sem a conversão, uma SP que vence
# amanhã apareceria em vermelho como atrasada para quem confere à noite. A
# conversão é feita AQUI, na fonte, e não em cada tela — é a mesma regra do
# `horario.py`.
SQL_HOJE = "(now() AT TIME ZONE 'America/Sao_Paulo')::date"

ORDENS = {
    "vencimento": "vencimento_d ASC NULLS LAST, id",
    "vencimento_desc": "vencimento_d DESC NULLS LAST, id",
    "valor": "valor_num ASC NULLS LAST, id",
    "valor_desc": "valor_num DESC NULLS LAST, id",
    "credor": "credor ASC, id",
    "id": "id",
}

SITUACOES = {
    "pendencias": "lower(trim(coalesce(status_pgt,''))) = 'pagar'",
    "risco": SQL_RISCO,
    "cadastro_incompleto": SQL_CADASTRO_INCOMPLETO,
    "boleto_invalido": SQL_BOLETO_INVALIDO,
    "boleto_duplicado": SQL_BOLETO_DUPLICADO,
}


# ---------------------------------------------------------------------------
# Montagem do filtro
# ---------------------------------------------------------------------------
def _como_texto_literal(termo: str) -> str:
    """Prepara um termo digitado para entrar num LIKE, sem virar curinga.

    No LIKE, `%` quer dizer "qualquer coisa" e `_` quer dizer "um caractere
    qualquer". Quem procura por "100%" quer o texto "100%", não "100 seguido de
    qualquer coisa" — que casaria com "1000". E quem procura "nota_1" quer o
    sublinhado, não um caractere qualquer no lugar dele.

    O Streamlit fazia busca literal (`contains` sem expressão regular), e a
    tradução tem de manter isso: a barra invertida escapa os três caracteres
    especiais, que é como o Postgres entende por padrão.
    """
    return (termo.replace("\\", "\\\\")
                 .replace("%", "\\%")
                 .replace("_", "\\_"))


def _condicoes(f: dict) -> tuple[list[str], list]:
    """Traduz o dicionário de filtros da tela em pedaços de SQL e parâmetros.

    Tudo entra como PARÂMETRO, nunca costurado dentro do texto do SQL — é o que
    impede que um credor com aspas no nome, ou um texto de busca mal
    intencionado, vire comando."""
    onde: list[str] = []
    params: list = []

    # Busca livre: termos separados por vírgula, TODOS têm de aparecer.
    busca = str(f.get("busca") or "").strip()
    if busca:
        alvo = " || ' ' || ".join(f"lower(coalesce({c},''))" for c in CAMPOS_BUSCA)
        for termo in [t.strip().lower() for t in busca.split(",") if t.strip()]:
            onde.append(f"({alvo}) LIKE ?")
            params.append(f"%{_como_texto_literal(termo)}%")

    # Listas de seleção simples.
    for campo, coluna in (("status_pgt", "status_pgt"),
                          ("conta", "conta"),
                          ("forma", "forma_pagamento"),
                          ("tipo_despesa", "tipo_despesa"),
                          ("projeto", "projeto"),
                          ("responsavel", "responsavel")):
        escolhidos = [v for v in (f.get(campo) or []) if str(v).strip()]
        if escolhidos:
            marcadores = ",".join(["?"] * len(escolhidos))
            onde.append(f"trim(coalesce({coluna},'')) IN ({marcadores})")
            params.extend(escolhidos)

    # Status de agendamento: "Sem Agendamento" quer dizer o valor vazio.
    agend = [v for v in (f.get("status_agend") or []) if str(v).strip()]
    if agend:
        alvos = [v for v in agend if v != "Sem Agendamento"]
        pedacos = []
        if alvos:
            pedacos.append(f"({SQL_STATUS_AGEND}) IN ({','.join(['?'] * len(alvos))})")
            params.extend(alvos)
        if "Sem Agendamento" in agend:
            pedacos.append(f"({SQL_STATUS_AGEND}) = ''")
        onde.append("(" + " OR ".join(pedacos) + ")")

    # Centro de custo (as OBRAS). A célula às vezes traz mais de uma, separadas
    # por vírgula: "CONS, CRECHE SWAP".
    #
    # Antes o casamento era por "contém", copiado do Streamlit. Funcionava na
    # maioria dos casos e errava num que aparece: procurar a obra "CONS"
    # trazia também "CONSTRUÇÃO DO GALPÃO", porque uma é pedaço da outra.
    # Agora a célula é ABERTA na vírgula e a comparação é com a obra INTEIRA —
    # que é o que a pessoa escolheu na lista.
    centros = [str(v).strip() for v in (f.get("centro_custo") or [])
               if str(v).strip()]
    if centros:
        pedacos = []
        for c in centros:
            pedacos.append(
                "EXISTS (SELECT 1 FROM unnest(regexp_split_to_array("
                f"          coalesce(centro_custo,''), '{SEPARADOR_DE_OBRAS}')"
                "        ) AS o WHERE lower(btrim(o)) = ?)")
            params.append(c.lower())
        onde.append("(" + " OR ".join(pedacos) + ")")

    # Situações (as caixas de marcar). Somam-se: marcar duas exige as duas.
    for chave in (f.get("situacoes") or []):
        if chave in SITUACOES:
            onde.append(SITUACOES[chave])

    # Períodos e faixa de valor.
    for campo, coluna, operador in (
            ("periodo_ini", "vencimento_d", ">="),
            ("periodo_fim", "vencimento_d", "<="),
            ("pgt_ini", "data_pagamento_d", ">="),
            ("pgt_fim", "data_pagamento_d", "<="),
            ("valor_ini", "valor_num", ">="),
            ("valor_fim", "valor_num", "<=")):
        valor = f.get(campo)
        if valor not in (None, ""):
            onde.append(f"{coluna} {operador} ?")
            params.append(valor)

    return onde, params


def _where(f: dict) -> tuple[str, list]:
    onde, params = _condicoes(f)
    return (" WHERE " + " AND ".join(onde)) if onde else "", params


# ---------------------------------------------------------------------------
# As perguntas
# ---------------------------------------------------------------------------
def resumo(f: dict) -> dict:
    """Quantas SPs o filtro alcança e quanto somam.

    Uma consulta só, sem trazer linha nenhuma. É o que substitui abrir a base
    inteira na memória para somar uma coluna."""
    from .db import consultar_um
    where, params = _where(f)
    linha = consultar_um(
        "SELECT count(*), coalesce(sum(valor_num), 0), "
        "       count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) = 'pagar'), "
        "       coalesce(sum(valor_num) FILTER "
        "                (WHERE lower(trim(coalesce(status_pgt,''))) = 'pagar'), 0) "
        f"  FROM analisesps.sps{where}", tuple(params))
    if not linha:
        return {"quantidade": 0, "total": 0, "quantidade_pagar": 0, "total_pagar": 0}
    return {"quantidade": linha[0], "total": linha[1],
            "quantidade_pagar": linha[2], "total_pagar": linha[3]}


def contagem_agendamento(f: dict) -> dict:
    """Agendar / Agendado / Pago / Falha — a mesma divisão do Streamlit.

    Os quatro grupos são EXCLUDENTES e nesta ordem de prioridade: pago ganha
    de tudo (não interessa como foi agendado, já saiu); depois falha; depois
    agendado; o resto é "a agendar". Sem essa ordem, uma SP paga que tinha
    ficado com "Falha Agendar" apareceria nas duas contas, e a soma dos
    quatro passaria do total."""
    from .db import consultar_um
    where, params = _where(f)
    pago = "lower(trim(coalesce(status_pgt,''))) = 'pagar'"
    linha = consultar_um(
        "SELECT "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) = 'pago'), "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) <> 'pago' "
        "                     AND lower(coalesce(agendado,'')) LIKE '%falha%'), "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) <> 'pago' "
        "                     AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                     AND lower(trim(coalesce(agendado,''))) = 'agendado'), "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) <> 'pago' "
        "                     AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                     AND lower(trim(coalesce(agendado,''))) <> 'agendado') "
        f"  FROM analisesps.sps{where}", tuple(params))
    if not linha:
        return {"Pago": 0, "Falha Agendar": 0, "Agendado": 0, "Agendar": 0}
    return {"Pago": linha[0], "Falha Agendar": linha[1],
            "Agendado": linha[2], "Agendar": linha[3]}


def resumo_e_agendamento(f: dict) -> tuple[dict, dict]:
    """Os dois de cima NUMA IDA SÓ ao banco.

    Separados, cada um varria a tabela filtrada por conta própria: medidos aqui
    com as 59 mil SPs, 44 ms + 48 ms. Juntos, 59 ms — porque a varredura é uma
    só e as contagens vão de carona. As duas funções acima continuam existindo
    para quem precisa de um dos dois sozinho (a exportação, por exemplo).

    O SQL é montado a partir das MESMAS peças das duas funções, de propósito:
    duas cópias do texto divergiriam no dia em que a regra de "pago ganha de
    tudo" mudasse em uma delas."""
    from .db import consultar_um
    where, params = _where(f)
    # O `lower(...)` vai escrito em cada linha, e não numa variável costurada
    # depois: há um teste que lê este arquivo linha a linha procurando LIKE
    # contra texto sem `lower()` — no Postgres o LIKE distingue maiúscula, e
    # esse já foi um defeito de verdade aqui. Esconder a normalização atrás de
    # uma variável cega o teste sem consertar nada.
    pago = "lower(trim(coalesce(status_pgt,'')))"
    linha = consultar_um(
        "SELECT count(*), coalesce(sum(valor_num), 0), "
        f"       count(*) FILTER (WHERE {pago} = 'pagar'), "
        "       coalesce(sum(valor_num) FILTER "
        f"                (WHERE {pago} = 'pagar'), 0), "
        f"       count(*) FILTER (WHERE {pago} = 'pago'), "
        f"       count(*) FILTER (WHERE {pago} <> 'pago' "
        "                          AND lower(coalesce(agendado,'')) LIKE '%falha%'), "
        f"       count(*) FILTER (WHERE {pago} <> 'pago' "
        "                          AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                          AND lower(trim(coalesce(agendado,''))) = 'agendado'), "
        f"       count(*) FILTER (WHERE {pago} <> 'pago' "
        "                          AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                          AND lower(trim(coalesce(agendado,''))) <> 'agendado') "
        f"  FROM analisesps.sps{where}", tuple(params))
    if not linha:
        return ({"quantidade": 0, "total": 0, "quantidade_pagar": 0,
                 "total_pagar": 0},
                {"Pago": 0, "Falha Agendar": 0, "Agendado": 0, "Agendar": 0})
    return ({"quantidade": linha[0], "total": linha[1],
             "quantidade_pagar": linha[2], "total_pagar": linha[3]},
            {"Pago": linha[4], "Falha Agendar": linha[5],
             "Agendado": linha[6], "Agendar": linha[7]})


def soma_por(f: dict, coluna: str, limite: int = 12) -> list[dict]:
    """Σ do valor por conta ou por forma de pagamento, como no Streamlit.

    A coluna é escolhida por NÓS, de uma lista fechada — nunca vem de quem
    chama. Nome de coluna não entra como parâmetro do banco, e concatenar o
    que veio de fora aqui seria a porta aberta clássica."""
    permitidas = {"conta", "forma_pagamento"}
    if coluna not in permitidas:
        raise ValueError(f"Não somo por '{coluna}'.")
    from .db import consultar
    where, params = _where(f)
    linhas = consultar(
        f"SELECT coalesce(nullif(trim({coluna}), ''), '(sem informação)'), "
        "       count(*), coalesce(sum(valor_num), 0) "
        f"  FROM analisesps.sps{where} "
        f" GROUP BY 1 ORDER BY 3 DESC LIMIT ?", tuple(params) + (limite,))
    return [{"nome": l[0], "quantidade": l[1], "total": l[2]} for l in linhas]


CAMPOS_LISTA = [
    "id", "solicitacao_d", "vencimento_d", "credor", "documento",
    "tipo_despesa", "centro_custo", "projeto", "valor_num", "responsavel",
    "status_pgt", "status_aut", "forma_pagamento", "conta", "info_pgt",
    "nf", "pedido", "data_pagamento_d", "anuente", "validacao",
    "comprovante", "codigo_barras", "analise_ia", "descricao",
    "anexo_link", "card_link",
]


def listar(f: dict, ordem: str = "vencimento", pagina: int = 1) -> list[dict]:
    """Uma página de SPs, já ordenada. Só as colunas que a tela mostra."""
    from .db import consultar
    where, params = _where(f)
    ordenacao = ORDENS.get(ordem, ORDENS["vencimento"])
    pagina = max(1, int(pagina or 1))

    campos = ", ".join(CAMPOS_LISTA)
    linhas = consultar(
        f"SELECT {campos}, ({SQL_STATUS_AGEND}) AS status_agend, "
        f"       ({SQL_RISCO}) AS risco, "
        f"       {SQL_CADASTRO_INCOMPLETO} AS cadastro_incompleto, "
        # O atraso é calculado aqui, uma vez, e não linha a linha na tela.
        f"       (vencimento_d IS NOT NULL AND vencimento_d < {SQL_HOJE} "
        "         AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vencido, "
        f"       (vencimento_d = {SQL_HOJE} "
        "         AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vence_hoje "
        f"  FROM analisesps.sps{where} "
        f" ORDER BY {ordenacao} LIMIT ? OFFSET ?",
        tuple(params) + (POR_PAGINA, (pagina - 1) * POR_PAGINA))

    nomes = CAMPOS_LISTA + ["status_agend", "risco", "cadastro_incompleto",
                            "vencido", "vence_hoje"]
    return [dict(zip(nomes, linha)) for linha in linhas]


def uma(sp_id: str) -> dict | None:
    """A ficha completa de uma SP."""
    from .db import consultar
    campos = ", ".join(f'"{c}"' for c in colunas.CHAVES)
    linhas = consultar(
        f"SELECT {campos}, valor_num, solicitacao_d, vencimento_d, "
        f"       data_pagamento_d, dt_autorizacao_d, ({SQL_STATUS_AGEND}) "
        "  FROM analisesps.sps WHERE id = ?", (str(sp_id),))
    if not linhas:
        return None
    nomes = list(colunas.CHAVES) + [
        "valor_num", "solicitacao_d", "vencimento_d", "data_pagamento_d",
        "dt_autorizacao_d", "status_agend"]
    return dict(zip(nomes, linhas[0]))


def opcoes(coluna: str, limite: int = 400) -> list[str]:
    """Os valores distintos de uma coluna, para montar as listas de filtro.

    Limitado de propósito: uma coluna com milhares de valores diferentes
    (credor, por exemplo) não cabe numa lista de seleção — para essas, a busca
    livre é o caminho certo."""
    permitidas = {"status_pgt", "conta", "forma_pagamento", "tipo_despesa",
                  "projeto", "responsavel", "centro_custo", "status_aut"}
    if coluna not in permitidas:
        raise ValueError(f"Coluna não permitida em filtro: {coluna}")
    from .db import consultar

    if coluna in MULTIPLAS_NA_CELULA:
        # A célula pode trazer MAIS DE UMA obra, separadas por vírgula
        # ("CONS, CRECHE SWAP"). Sem separar, a lista do filtro oferecia a
        # combinação inteira como se fosse uma obra — e a obra sozinha, que é
        # o que se procura, não aparecia em lugar nenhum.
        #
        # `unnest(string_to_array(...))` abre a célula em uma linha por obra;
        # o resto é o mesmo agrupamento de sempre.
        linhas = consultar(
            "SELECT obra, count(*) FROM ("
            "  SELECT btrim(unnest(regexp_split_to_array("
            f"           {coluna}, '{SEPARADOR_DE_OBRAS}'))) AS obra "
            f"    FROM analisesps.sps WHERE trim(coalesce({coluna},'')) <> ''"
            ") AS abertas WHERE obra <> '' "
            " GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT ?", (limite,))
        return [linha[0] for linha in linhas]

    linhas = consultar(
        f"SELECT trim({coluna}), count(*) FROM analisesps.sps "
        f" WHERE trim(coalesce({coluna},'')) <> '' "
        f" GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT ?", (limite,))
    return [linha[0] for linha in linhas]


# ---------------------------------------------------------------------------
# AS LISTAS DE FILTRO, GUARDADAS ATÉ A PRÓXIMA CARGA
#
# Medido nesta máquina, com as 59.055 SPs de verdade: montar as sete listas
# custa 194 ms, e era isso a CADA clique no filtro. Cada uma varre a tabela
# inteira para descobrir quais valores existem naquela coluna, e o índice não
# ajuda — a consulta limpa o texto antes de agrupar, e aí o banco lê tudo.
# Índice de expressão foi tentado e o Postgres continuou preferindo a varredura;
# não é caminho.
#
# O desperdício é que essas listas quase nunca mudam: os projetos, as contas e
# os tipos de despesa da empresa são os mesmos hoje e amanhã. Só mudam quando
# entra SP nova — ou seja, quando a carga da planilha roda.
#
# Então a chave do que fica guardado é O CARIMBO DA ÚLTIMA SINCRONIZAÇÃO. Ele
# muda, as listas são refeitas; não muda, valem as de antes. Funciona ENTRE
# PROCESSOS sem combinação nenhuma: a carga roda num processo separado e não
# tem como avisar este, mas o carimbo que ela grava no banco é o próprio aviso.
#
# O CUSTO, dito na cara: um projeto novo cadastrado na planilha só aparece na
# listinha depois da próxima sincronização (a tela dispara uma a cada 5 min).
# A SP nova aparece na LISTA normalmente — é só o menu de filtro que demora a
# saber do valor novo.
# ---------------------------------------------------------------------------
COLUNAS_DE_FILTRO = {
    "status_pgt": ("status_pgt", 400),
    "conta": ("conta", 400),
    "forma": ("forma_pagamento", 400),
    "tipo_despesa": ("tipo_despesa", 400),
    "projeto": ("projeto", 400),
    "responsavel": ("responsavel", 400),
    "centro_custo": ("centro_custo", 200),
}

# Trocado inteiro a cada recálculo, nunca alterado no lugar: com 4 threads no
# mesmo processo, duas podem recalcular ao mesmo tempo — e trocar a referência
# de uma vez faz com que a pior consequência disso seja trabalho repetido, e
# nunca uma lista pela metade na tela.
_LISTAS_GUARDADAS: dict = {"carimbo": object(), "valores": {}}


def opcoes_de_filtro(carimbo=None) -> dict:
    """As sete listas da barra lateral, de uma vez.

    `carimbo` é o valor de `ultima_sincronizacao` — quem chama normalmente já
    o tem em mãos (veio do `base_carregada()`), e passá-lo evita uma consulta
    a mais só para descobrir se o que está guardado ainda serve."""
    if carimbo is None:
        from .db import consultar_um
        try:
            linha = consultar_um("SELECT valor FROM analisesps.meta "
                                 "WHERE chave = 'ultima_sincronizacao'")
            carimbo = linha[0] if linha else ""
        except Exception:  # noqa: BLE001 — sem carimbo, recalcula; não quebra
            carimbo = None

    guardado = _LISTAS_GUARDADAS
    if guardado["carimbo"] == carimbo and guardado["valores"]:
        return dict(guardado["valores"], status_agend=opcoes_agendamento())

    valores = {apelido: opcoes(coluna, limite=limite)
               for apelido, (coluna, limite) in COLUNAS_DE_FILTRO.items()}
    _substituir_listas(carimbo, valores)
    return dict(valores, status_agend=opcoes_agendamento())


def _substituir_listas(carimbo, valores) -> None:
    global _LISTAS_GUARDADAS
    _LISTAS_GUARDADAS = {"carimbo": carimbo, "valores": valores}


def esquecer_opcoes_de_filtro() -> None:
    """Joga fora o que está guardado. Para os testes e para quem mexer na
    estrutura sem passar por uma sincronização."""
    _substituir_listas(object(), {})


def opcoes_agendamento() -> list[str]:
    """Os valores possíveis do status de agendamento — lista fixa, curta, e na
    ordem em que o operador pensa neles."""
    return ["Agendar", "Agendado", "Verificar", "Falha Agendar", "Sem Agendamento"]


def base_carregada() -> dict:
    """Quantas SPs existem e quando foi a última sincronização.

    Serve para a tela dizer "a base ainda não foi carregada" em vez de mostrar
    uma lista vazia como se não houvesse nada a pagar.

    `desconhecida` separa dois estados que parecem iguais e não são: a base
    VAZIA (a pergunta foi feita, e a resposta é zero) e a base que NÃO DEU
    PARA CONSULTAR (banco fora do ar, ou estrutura ainda não criada). Dizer
    "vazia" no segundo caso é afirmar o que não se sabe — e foi assim que a
    tela de Configurações chegou a informar "o banco está em dia" justamente
    quando não conseguia falar com ele."""
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT count(*) FROM analisesps.sps")
        quantas = linha[0] if linha else 0
    except Exception:  # noqa: BLE001 — tabela ainda não criada
        return {"pronta": False, "quantidade": 0, "ultima": None,
                "desconhecida": True}
    try:
        linha = consultar_um(
            "SELECT valor FROM analisesps.meta WHERE chave = 'ultima_sincronizacao'")
        ultima = linha[0] if linha else None
    except Exception:  # noqa: BLE001 — não saber a data não justifica derrubar a tela
        ultima = None
    return {"pronta": quantas > 0, "quantidade": quantas, "ultima": ultima,
            "desconhecida": False}


# ---------------------------------------------------------------------------
# RELATÓRIO
#
# As mesmas contas do `relatorio.py` do Streamlit, feitas pelo banco. Lá elas
# rodavam sobre o DataFrame inteiro na memória; aqui cada uma é uma consulta
# que devolve dezenas de linhas, não dezenas de milhares.
#
# Uma regra vale para o relatório todo e vem do original: CANCELADAS FICAM DE
# FORA. Uma SP cancelada não é despesa, e somá-la inflaria todo total.
# ---------------------------------------------------------------------------
SEM_CANCELADAS = "lower(trim(coalesce(status_pgt,''))) <> 'cancelado'"

# As dimensões que o relatório sabe quebrar. É uma lista fechada de propósito:
# o nome da coluna entra no texto do SQL, então ele não pode vir de fora.
DIMENSOES = {
    "projeto": "Projeto",
    "centro_custo": "Centro de Custo",
    "tipo_despesa": "Tipo de Despesa",
    "conta": "Conta",
    "responsavel": "Responsável",
    "status_pgt": "Status de Pagamento",
    "forma_pagamento": "Forma de Pagamento",
}

VAZIO = "(vazio)"

# Os três recortes do relatório, e a data que manda em cada um.
TIPOS = {
    "geral": "Visão geral",
    "pagar": "Contas a pagar",
    "pagas": "Contas pagas",
}

PERIODOS = {"tudo": "Todo o período", "semana": "Esta semana", "mes": "Este mês"}


def _where_relatorio(f: dict, tipo: str) -> tuple[str, list]:
    """O filtro da barra lateral, mais o recorte do relatório.

    `tipo` escolhe o universo e, com ele, a data que importa: contas a pagar se
    olham pelo VENCIMENTO; contas pagas, pela DATA DO PAGAMENTO. Misturar as
    duas dá um total que não fecha com nada."""
    onde, params = _condicoes(f)
    onde.append(SEM_CANCELADAS)

    if tipo == "pagar":
        onde.append("lower(trim(coalesce(status_pgt,''))) = 'pagar'")
    elif tipo == "pagas":
        onde.append("lower(trim(coalesce(status_pgt,''))) = 'pago'")

    return " WHERE " + " AND ".join(onde), params


def coluna_de_data(tipo: str) -> str:
    """Qual data manda em cada recorte. Ver `_where_relatorio`."""
    return "data_pagamento_d" if tipo == "pagas" else "vencimento_d"


def _periodo(tipo: str, periodo: str) -> str:
    """O atalho de período: esta semana, este mês, ou tudo.

    Calculado com a data de Brasília, não com a do servidor — senão, entre 21h
    e meia-noite, "este mês" viraria o mês seguinte.

    `date_trunc('week')` do Postgres começa na SEGUNDA, que é como a semana é
    contada no original (`hoje.weekday()`)."""
    coluna = coluna_de_data(tipo)
    if periodo == "semana":
        return (f" AND {coluna} >= date_trunc('week', {SQL_HOJE})::date"
                f" AND {coluna} <= (date_trunc('week', {SQL_HOJE})"
                " + interval '6 days')::date")
    if periodo == "mes":
        return (f" AND {coluna} >= date_trunc('month', {SQL_HOJE})::date"
                f" AND {coluna} < (date_trunc('month', {SQL_HOJE})"
                " + interval '1 month')::date")
    return ""


def numeros_do_relatorio(f: dict, tipo: str = "geral",
                         periodo: str = "tudo") -> dict:
    """Total, quantidade, ticket médio e vencidos — numa consulta só."""
    from .db import consultar_um
    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)
    linha = consultar_um(
        "SELECT count(*), coalesce(sum(valor_num),0), "
        f"       count(*) FILTER (WHERE vencimento_d < {SQL_HOJE}), "
        "       coalesce(sum(valor_num) FILTER "
        f"               (WHERE vencimento_d < {SQL_HOJE}), 0) "
        f"  FROM analisesps.sps{where}{recorte}", tuple(params))
    if not linha:
        return {"quantidade": 0, "total": 0, "ticket": 0,
                "vencidos_qtd": 0, "vencidos_total": 0}
    quantidade, total = linha[0], linha[1]
    return {
        "quantidade": quantidade,
        "total": total,
        "ticket": (total / quantidade) if quantidade else 0,
        "vencidos_qtd": linha[2],
        "vencidos_total": linha[3],
    }


def agregar(f: dict, dimensao: str, tipo: str = "geral", periodo: str = "tudo",
            limite: int = 30) -> list[dict]:
    """Soma por uma dimensão, da maior para a menor.

    Valor em branco vira "(vazio)" em vez de sumir — é assim no original, e é o
    certo: uma despesa sem centro de custo continua sendo despesa, e escondê-la
    faria a soma das partes não bater com o total."""
    if dimensao not in DIMENSOES:
        raise ValueError(f"Dimensão não permitida no relatório: {dimensao}")
    from .db import consultar
    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)
    linhas = consultar(
        f"SELECT CASE WHEN trim(coalesce({dimensao},'')) = '' THEN ? "
        f"            ELSE trim({dimensao}) END AS rotulo, "
        "        count(*), coalesce(sum(valor_num),0) "
        f"  FROM analisesps.sps{where}{recorte} "
        "  GROUP BY 1 ORDER BY 3 DESC, 1 LIMIT ?",
        (VAZIO,) + tuple(params) + (limite,))
    return [{"rotulo": r[0], "quantidade": r[1], "total": r[2]} for r in linhas]


def top_credores(f: dict, tipo: str = "geral", periodo: str = "tudo",
                 limite: int = 30) -> list[dict]:
    """Os maiores credores, agrupados por CPF/CNPJ — não pelo nome.

    O agrupamento por documento é o do original, e existe porque o mesmo credor
    aparece escrito de vários jeitos ("ACME LTDA", "Acme Ltda ME"). Somar por
    nome partiria o mesmo fornecedor em três."""
    from .db import consultar
    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)
    linhas = consultar(
        "SELECT CASE WHEN trim(coalesce(documento,'')) = '' THEN ? "
        "            ELSE trim(documento) END AS doc, "
        # O nome exibido é o mais frequente naquele documento — mesma escolha
        # do original, que usava a moda.
        "       mode() WITHIN GROUP (ORDER BY trim(coalesce(credor,''))), "
        "       count(*), coalesce(sum(valor_num),0) "
        f"  FROM analisesps.sps{where}{recorte} "
        "  GROUP BY 1 ORDER BY 4 DESC, 2 LIMIT ?",
        ("(sem CPF/CNPJ)",) + tuple(params) + (limite,))
    return [{"documento": r[0], "credor": r[1], "quantidade": r[2], "total": r[3]}
            for r in linhas]


# As faixas de atraso do original, na mesma ordem e com os mesmos limites.
FAIXAS_ATRASO = [(1, 7, "1 a 7 dias"), (8, 15, "8 a 15 dias"),
                 (16, 30, "16 a 30 dias"), (31, 60, "31 a 60 dias"),
                 (61, 90, "61 a 90 dias"), (91, None, "mais de 90 dias")]


def aging_vencidos(f: dict, periodo: str = "tudo") -> list[dict]:
    """Quanto está atrasado, e há quanto tempo. Só o que está a pagar.

    A conta do atraso usa o dia de Brasília. Uma SP que vence HOJE não está
    atrasada — o original exige atraso maior que zero, e a tradução mantém.

    As faixas entram no texto do SQL, e não como parâmetro, porque são números
    fixos escritos aqui — nunca chegam de fora."""
    from .db import consultar
    where, params = _where_relatorio(f, "pagar")
    recorte = _periodo("pagar", periodo)

    casos = []
    for inicio, fim, nome in FAIXAS_ATRASO:
        ate = f" AND atraso <= {int(fim)}" if fim else ""
        casos.append(f"WHEN atraso >= {int(inicio)}{ate} THEN '{nome}'")

    linhas = consultar(
        "SELECT faixa, count(*), coalesce(sum(valor_num),0) FROM ("
        f"  SELECT valor_num, CASE {' '.join(casos)} END AS faixa FROM ("
        f"    SELECT valor_num, ({SQL_HOJE} - vencimento_d) AS atraso "
        f"      FROM analisesps.sps{where}{recorte}"
        "  ) AS com_atraso WHERE atraso > 0"
        ") AS por_faixa WHERE faixa IS NOT NULL GROUP BY 1",
        tuple(params))

    achados = {r[0]: {"faixa": r[0], "quantidade": r[1], "total": r[2]}
               for r in linhas}
    return [achados[nome] for _, _, nome in FAIXAS_ATRASO if nome in achados]
