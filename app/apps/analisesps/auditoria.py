# -*- coding: utf-8 -*-
"""
As checagens da tela de Auditoria.

Traduzidas uma a uma do `auditoria.py` do Streamlit, sem mudar o significado de
nenhuma. A diferença é onde a conta acontece: lá, sobre as 59 mil SPs abertas
na memória; aqui, dentro do banco, que devolve só as linhas com problema.

Uma regra vale para todas e vem do original: CANCELADAS FICAM DE FORA. Uma SP
cancelada não é despesa nem pendência — apontá-la seria dar trabalho a quem lê.
"""
from __future__ import annotations

import logging

from .consultas import SEM_CANCELADAS, SQL_HOJE, _condicoes

logger = logging.getLogger("analisesps.auditoria")

# Teto de linhas por checagem. Uma auditoria que devolvesse 40 mil linhas não
# seria lida por ninguém e ainda tiraria a instância do ar. Quando o teto é
# alcançado, a tela diz que há mais.
TETO = 500


# Por qual data o período recorta. A coluna é escolhida por NÓS, de uma lista
# fechada: nome de coluna não entra como parâmetro do banco, e concatenar o
# que veio de fora seria a porta aberta clássica.
CAMPOS_PERIODO = {
    "vencimento": ("vencimento_d", "vencimento"),
    "solicitacao": ("solicitacao_d", "data da solicitação"),
}


def _where(f: dict, aplicar_filtros: bool, periodo: dict | None = None
           ) -> tuple[str, list]:
    """A barra lateral só entra se a pessoa pedir.

    O padrão é auditar a BASE INTEIRA, e não o que está filtrado — é o padrão
    do original, e o certo: auditoria que enxerga só o pedaço já filtrado
    encontra só o que já se estava olhando.

    O PERÍODO é diferente disso, e por isso é um controle separado. Auditar a
    base inteira dá o retrato de sempre; auditar um mês responde "o que entrou
    errado NESTE fechamento". As duas perguntas são legítimas, e o dono pediu
    a segunda em 04/09/2026. Só o período recortando: nem por isso a auditoria
    passa a herdar o filtro da lista."""
    onde, params = ([], [])
    if aplicar_filtros:
        onde, params = _condicoes(f)

    periodo = periodo or {}
    coluna = CAMPOS_PERIODO.get(periodo.get("campo") or "vencimento",
                                CAMPOS_PERIODO["vencimento"])[0]
    if periodo.get("de"):
        onde.append(f"{coluna} >= ?")
        params.append(periodo["de"])
    if periodo.get("ate"):
        onde.append(f"{coluna} <= ?")
        params.append(periodo["ate"])

    onde.append(SEM_CANCELADAS)
    return " WHERE " + " AND ".join(onde), params


# ---------------------------------------------------------------------------
# 1. Pontualidade do registro
# ---------------------------------------------------------------------------
def pontualidade(f: dict, aplicar_filtros: bool = False,
                 minimo_lancamentos: int = 5,
                 periodo: dict | None = None) -> list[dict]:
    """Antecedência = vencimento menos solicitação, em dias, por responsável.

    Negativo quer dizer que a SP foi registrada DEPOIS de vencer — o que gera
    juros. É a conta que responde "quem lança em cima da hora".

    Só entram as SPs que têm as duas datas: sem uma delas não há antecedência
    para calcular, e chutar zero puxaria a média de todo mundo para baixo.

    `minimo_lancamentos` evita o ranking injusto: quem tem uma SP só não deve
    encabeçar a lista dos piores por causa dela."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros, periodo)
    linhas = consultar(
        "SELECT responsavel, count(*), round(avg(antecedencia)::numeric, 1), "
        "       round((percentile_cont(0.5) WITHIN GROUP "
        "              (ORDER BY antecedencia))::numeric, 1), "
        "       count(*) FILTER (WHERE antecedencia < 0), "
        "       coalesce(sum(valor_num) FILTER (WHERE antecedencia < 0), 0), "
        "       coalesce(sum(valor_num), 0) "
        "  FROM ("
        "    SELECT CASE WHEN trim(coalesce(responsavel,'')) = '' "
        "                THEN '(sem responsável)' ELSE trim(responsavel) END "
        "                AS responsavel, "
        "           (vencimento_d - solicitacao_d) AS antecedencia, valor_num "
        f"      FROM analisesps.sps{where} "
        "       AND vencimento_d IS NOT NULL AND solicitacao_d IS NOT NULL"
        "  ) AS com_antecedencia "
        " GROUP BY responsavel HAVING count(*) >= ? "
        " ORDER BY 3 ASC",
        tuple(params) + (minimo_lancamentos,))

    return [{
        "responsavel": r[0], "quantidade": r[1], "media_dias": r[2],
        "mediana_dias": r[3], "atrasados": r[4],
        "percentual_atrasados": round(r[4] * 100.0 / r[1], 1) if r[1] else 0,
        "valor_atrasado": r[5], "valor_total": r[6],
    } for r in linhas]


# ---------------------------------------------------------------------------
# 2. Risco de duplicidade apontado pela análise
# ---------------------------------------------------------------------------
def risco_ia(f: dict, aplicar_filtros: bool = False,
                 periodo: dict | None = None) -> list[dict]:
    """O que a análise da coluna AL marcou como "COM RISCO"."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros, periodo)
    linhas = consultar(
        "SELECT id, credor, documento, valor_num, vencimento_d, analise_ia "
        f"  FROM analisesps.sps{where} "
        "   AND upper(coalesce(analise_ia,'')) LIKE '%COM RISCO%' "
        " ORDER BY valor_num DESC NULLS LAST LIMIT ?",
        tuple(params) + (TETO,))
    return [{"id": r[0], "credor": r[1], "documento": r[2], "valor_num": r[3],
             "vencimento_d": r[4], "analise_ia": r[5]} for r in linhas]


# ---------------------------------------------------------------------------
# 3. Nota fiscal repetida
# ---------------------------------------------------------------------------
# Como se descobre que duas SPs com a mesma nota são PARCELAS, e não
# duplicidade. A coluna Parcela é a fonte boa — vem preenchida como "001/003".
# Quando ela está vazia, o número costuma estar escrito na descrição, e aí
# vale procurar: "2/3", "parcela 2", "2ª parcela". A ordem importa: a coluna
# ganha da descrição, porque descrição é texto livre e erra mais.
MARCA_PARCELA = (
    "coalesce("
    "  nullif(trim(coalesce(parcela,'')), ''),"
    "  substring(coalesce(descricao,'') from '[0-9]{1,3}[[:space:]]*/[[:space:]]*[0-9]{1,3}'),"
    "  substring(lower(coalesce(descricao,'')) from "
    "            'parcela[[:space:]]*n?[ºo°]?[[:space:]]*([0-9]{1,3})'),"
    "  substring(lower(coalesce(descricao,'')) from "
    "            '([0-9]{1,3})[[:space:]]*[ªa][[:space:]]*parcela')"
    ")")


def nf_duplicada(f: dict, aplicar_filtros: bool = False,
                 periodo: dict | None = None) -> list[dict]:
    """Mesmo CPF/CNPJ com o mesmo número de nota em SPs diferentes.

    É o indício mais direto de pagamento em duplicidade: a mesma nota não
    deveria dar origem a duas solicitações.

    EXCETO QUANDO SÃO PARCELAS. Uma nota parcelada em três gera três SPs com o
    mesmo número — e apontar as três como duplicidade todo mês é o jeito mais
    rápido de fazer alguém parar de olhar a auditoria. Pedido do dono em
    04/09/2026.

    A regra, dita como ela é: o grupo só sai da lista quando TODAS as SPs dele
    têm marca de parcela e essas marcas são TODAS DIFERENTES. Se duas dividem
    a mesma parcela, ou se alguma está sem marca nenhuma, o grupo continua
    aparecendo — porque aí a explicação "é parcelamento" não fecha, e o certo
    é alguém olhar. Na dúvida, aponta: o custo de conferir à toa é pequeno
    perto do custo de pagar duas vezes."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros, periodo)
    linhas = consultar(
        "WITH marcado AS ("
        "  SELECT id, credor, valor_num, vencimento_d, "
        "         trim(coalesce(documento,'')) AS doc, "
        "         trim(coalesce(nf,'')) AS nota, "
        f"        {MARCA_PARCELA} AS marca "
        f"    FROM analisesps.sps{where} "
        "     AND trim(coalesce(documento,'')) <> '' "
        "     AND trim(coalesce(nf,'')) <> ''"
        "), grupos AS ("
        "  SELECT doc, nota, count(*) AS quantos, "
        "         count(marca) AS com_marca, "
        "         count(DISTINCT marca) AS marcas_distintas "
        "    FROM marcado GROUP BY doc, nota HAVING count(*) > 1"
        ") "
        "SELECT m.doc, m.nota, m.id, m.credor, m.valor_num, m.vencimento_d, "
        "       g.quantos, coalesce(m.marca, '') "
        "  FROM marcado m JOIN grupos g ON g.doc = m.doc AND g.nota = m.nota "
        # A negação é o coração da regra: fica de fora só o parcelamento que
        # se explica inteiro.
        " WHERE NOT (g.com_marca = g.quantos AND g.marcas_distintas = g.quantos) "
        " ORDER BY g.quantos DESC, m.doc, m.nota, m.id LIMIT ?",
        tuple(params) + (TETO,))
    return [{"documento": r[0], "nf": r[1], "id": r[2], "credor": r[3],
             "valor_num": r[4], "vencimento_d": r[5], "quantos": r[6],
             "parcela": r[7]}
            for r in linhas]


# ---------------------------------------------------------------------------
# 4. Possível duplicidade por valor
# ---------------------------------------------------------------------------
def possivel_duplicidade(f: dict, aplicar_filtros: bool = False,
                         dias: int = 7,
                         periodo: dict | None = None) -> list[dict]:
    """Mesmo CPF/CNPJ e mesmo valor, com as SPs a poucos dias uma da outra.

    Complementa a análise da IA por um caminho determinístico: não depende de
    ninguém ter escrito nada na coluna AL.

    A janela é a distância entre a primeira e a última solicitação do grupo.
    Grupos mais espalhados que `dias` ficam de fora — dois aluguéis iguais com
    trinta dias de diferença são o normal, não duplicidade."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros, periodo)
    linhas = consultar(
        "SELECT documento, valor_num, id, credor, solicitacao_d, quantos, janela "
        "  FROM ("
        "    SELECT trim(coalesce(documento,'')) AS documento, valor_num, id, "
        "           credor, solicitacao_d, "
        "           count(*) OVER j AS quantos, "
        "           (max(solicitacao_d) OVER j - min(solicitacao_d) OVER j) AS janela "
        f"      FROM analisesps.sps{where} "
        "       AND trim(coalesce(documento,'')) <> '' AND valor_num > 0 "
        "    WINDOW j AS (PARTITION BY trim(coalesce(documento,'')), valor_num)"
        "  ) AS agrupado "
        " WHERE quantos > 1 AND (janela IS NULL OR janela <= ?) "
        " ORDER BY valor_num DESC, documento LIMIT ?",
        tuple(params) + (dias, TETO))
    return [{"documento": r[0], "valor_num": r[1], "id": r[2], "credor": r[3],
             "solicitacao_d": r[4], "quantos": r[5], "janela": r[6] or 0}
            for r in linhas]


# ---------------------------------------------------------------------------
# 5. Cadastro sem classificação
# ---------------------------------------------------------------------------
def sem_classificacao(f: dict, aplicar_filtros: bool = False,
                 periodo: dict | None = None) -> list[dict]:
    """SPs sem centro de custo, sem projeto, ou sem os dois.

    Sem essas duas informações a despesa não entra em nenhum relatório por
    obra — ela existe, mas some da análise."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros, periodo)
    linhas = consultar(
        "SELECT id, credor, valor_num, vencimento_d, centro_custo, projeto, "
        "       CASE WHEN trim(coalesce(centro_custo,'')) = '' "
        "             AND trim(coalesce(projeto,'')) = '' "
        "            THEN 'Centro de Custo + Projeto' "
        "            WHEN trim(coalesce(centro_custo,'')) = '' "
        "            THEN 'Centro de Custo' ELSE 'Projeto' END AS faltando "
        f"  FROM analisesps.sps{where} "
        "   AND (trim(coalesce(centro_custo,'')) = '' "
        "     OR trim(coalesce(projeto,'')) = '') "
        " ORDER BY valor_num DESC NULLS LAST LIMIT ?",
        tuple(params) + (TETO,))
    return [{"id": r[0], "credor": r[1], "valor_num": r[2], "vencimento_d": r[3],
             "centro_custo": r[4], "projeto": r[5], "faltando": r[6]}
            for r in linhas]


# ---------------------------------------------------------------------------
# 6. Falta integrar no Omie
# ---------------------------------------------------------------------------
def sem_integracao_omie(f: dict, aplicar_filtros: bool = False,
                 periodo: dict | None = None) -> list[dict]:
    """SPs ainda ativas e sem código de integração do Omie.

    "Ativa" quer dizer nem paga nem cancelada. Uma SP já paga sem código é
    problema de histórico; uma ativa é trabalho que ainda precisa ser feito, e
    é essa que a tela cobra."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros, periodo)
    linhas = consultar(
        "SELECT id, credor, valor_num, vencimento_d, status_pgt "
        f"  FROM analisesps.sps{where} "
        "   AND trim(coalesce(codigo_integracao,'')) = '' "
        "   AND lower(trim(coalesce(status_pgt,''))) NOT IN ('cancelado','pago') "
        " ORDER BY vencimento_d ASC NULLS LAST LIMIT ?",
        tuple(params) + (TETO,))
    return [{"id": r[0], "credor": r[1], "valor_num": r[2], "vencimento_d": r[3],
             "status_pgt": r[4]} for r in linhas]


# ---------------------------------------------------------------------------
# 7. Códigos de barras
# ---------------------------------------------------------------------------
def codigos_de_barras(f: dict, aplicar_filtros: bool = False,
                 periodo: dict | None = None) -> dict:
    """Os dois problemas de boleto, juntos: inválidos e repetidos.

    Reusa exatamente o mesmo SQL do filtro da tela de Solicitações. Se a regra
    mudar, muda num lugar só, e as duas telas continuam concordando — divergir
    aqui seria pior do que não ter a tela."""
    from . import consultas
    from .db import consultar

    where, params = _where(f, aplicar_filtros, periodo)
    resultado = {}
    for chave, condicao in (("invalidos", consultas.SQL_BOLETO_INVALIDO),
                            ("duplicados", consultas.SQL_BOLETO_DUPLICADO)):
        linhas = consultar(
            "SELECT id, credor, valor_num, vencimento_d, codigo_barras "
            f"  FROM analisesps.sps{where} AND {condicao} "
            " ORDER BY valor_num DESC NULLS LAST LIMIT ?",
            tuple(params) + (TETO,))
        resultado[chave] = [
            {"id": r[0], "credor": r[1], "valor_num": r[2],
             "vencimento_d": r[3], "codigo_barras": r[4]} for r in linhas]
    return resultado


CHECAGENS = {
    "pontualidade": "Pontualidade do registro",
    "risco_ia": "Risco de duplicidade (análise)",
    "possivel_duplicidade": "Possível duplicidade (mesmo valor)",
    "nf_duplicada": "Nota fiscal repetida",
    "codigos_barras": "Códigos de barras",
    "sem_classificacao": "Sem centro de custo ou projeto",
    "sem_integracao": "Falta integrar no Omie",
}


def resumo(f: dict, aplicar_filtros: bool = False,
                 periodo: dict | None = None) -> dict:
    """Quantos problemas cada checagem encontrou.

    Uma consulta por checagem, todas devolvendo um número só. É o que permite a
    tela abrir mostrando onde há trabalho, sem carregar sete listas."""
    from . import consultas
    from .db import consultar_um

    where, params = _where(f, aplicar_filtros, periodo)

    def contar(condicao: str) -> int:
        linha = consultar_um(
            f"SELECT count(*) FROM analisesps.sps{where} AND {condicao}",
            tuple(params))
        return linha[0] if linha else 0

    contagens = {
        "risco_ia": contar("upper(coalesce(analise_ia,'')) LIKE '%COM RISCO%'"),
        "codigos_barras": contar(
            f"({consultas.SQL_BOLETO_INVALIDO} OR {consultas.SQL_BOLETO_DUPLICADO})"),
        "sem_classificacao": contar(
            "(trim(coalesce(centro_custo,'')) = '' "
            " OR trim(coalesce(projeto,'')) = '')"),
        "sem_integracao": contar(
            "trim(coalesce(codigo_integracao,'')) = '' "
            "AND lower(trim(coalesce(status_pgt,''))) NOT IN ('cancelado','pago')"),
    }
    # Estas duas precisam de agrupamento, então contam as linhas do resultado.
    contagens["nf_duplicada"] = len(nf_duplicada(f, aplicar_filtros))
    contagens["possivel_duplicidade"] = len(possivel_duplicidade(f, aplicar_filtros))
    contagens["pontualidade"] = len(pontualidade(f, aplicar_filtros))
    return contagens
