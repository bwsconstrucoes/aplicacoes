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

    # Centro de custo casa por "contém": na planilha ele às vezes vem com mais
    # de um código na mesma célula. Mesma regra do original.
    centros = [v for v in (f.get("centro_custo") or []) if str(v).strip()]
    if centros:
        pedacos = []
        for c in centros:
            pedacos.append("lower(coalesce(centro_custo,'')) LIKE ?")
            params.append(f"%{_como_texto_literal(str(c).lower())}%")
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
    linhas = consultar(
        f"SELECT trim({coluna}), count(*) FROM analisesps.sps "
        f" WHERE trim(coalesce({coluna},'')) <> '' "
        f" GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT ?", (limite,))
    return [linha[0] for linha in linhas]


def opcoes_agendamento() -> list[str]:
    """Os valores possíveis do status de agendamento — lista fixa, curta, e na
    ordem em que o operador pensa neles."""
    return ["Agendar", "Agendado", "Verificar", "Falha Agendar", "Sem Agendamento"]


def base_carregada() -> dict:
    """Quantas SPs existem e quando foi a última sincronização.

    Serve para a tela dizer "a base ainda não foi carregada" em vez de mostrar
    uma lista vazia como se não houvesse nada a pagar."""
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT count(*) FROM analisesps.sps")
        quantas = linha[0] if linha else 0
    except Exception:  # noqa: BLE001 — tabela ainda não criada
        return {"pronta": False, "quantidade": 0, "ultima": None}
    linha = consultar_um(
        "SELECT valor FROM analisesps.meta WHERE chave = 'ultima_sincronizacao'")
    return {"pronta": quantas > 0, "quantidade": quantas,
            "ultima": linha[0] if linha else None}
