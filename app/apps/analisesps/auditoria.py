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


def _where(f: dict, aplicar_filtros: bool) -> tuple[str, list]:
    """A barra lateral só entra se a pessoa pedir.

    O padrão é auditar a BASE INTEIRA, e não o que está filtrado — é o padrão
    do original, e o certo: auditoria que enxerga só o pedaço já filtrado
    encontra só o que já se estava olhando."""
    onde, params = ([], [])
    if aplicar_filtros:
        onde, params = _condicoes(f)
    onde.append(SEM_CANCELADAS)
    return " WHERE " + " AND ".join(onde), params


# ---------------------------------------------------------------------------
# 1. Pontualidade do registro
# ---------------------------------------------------------------------------
def pontualidade(f: dict, aplicar_filtros: bool = False,
                 minimo_lancamentos: int = 5) -> list[dict]:
    """Antecedência = vencimento menos solicitação, em dias, por responsável.

    Negativo quer dizer que a SP foi registrada DEPOIS de vencer — o que gera
    juros. É a conta que responde "quem lança em cima da hora".

    Só entram as SPs que têm as duas datas: sem uma delas não há antecedência
    para calcular, e chutar zero puxaria a média de todo mundo para baixo.

    `minimo_lancamentos` evita o ranking injusto: quem tem uma SP só não deve
    encabeçar a lista dos piores por causa dela."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros)
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
def risco_ia(f: dict, aplicar_filtros: bool = False) -> list[dict]:
    """O que a análise da coluna AL marcou como "COM RISCO"."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros)
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
def nf_duplicada(f: dict, aplicar_filtros: bool = False) -> list[dict]:
    """Mesmo CPF/CNPJ com o mesmo número de nota em SPs diferentes.

    É o indício mais direto de pagamento em duplicidade: a mesma nota não
    deveria dar origem a duas solicitações."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros)
    linhas = consultar(
        "SELECT documento, nf, id, credor, valor_num, vencimento_d, quantos "
        "  FROM ("
        "    SELECT trim(coalesce(documento,'')) AS documento, "
        "           trim(coalesce(nf,'')) AS nf, id, credor, valor_num, "
        "           vencimento_d, "
        "           count(*) OVER (PARTITION BY trim(coalesce(documento,'')), "
        "                                       trim(coalesce(nf,''))) AS quantos "
        f"      FROM analisesps.sps{where} "
        "       AND trim(coalesce(documento,'')) <> '' "
        "       AND trim(coalesce(nf,'')) <> ''"
        "  ) AS agrupado WHERE quantos > 1 "
        " ORDER BY quantos DESC, documento, nf LIMIT ?",
        tuple(params) + (TETO,))
    return [{"documento": r[0], "nf": r[1], "id": r[2], "credor": r[3],
             "valor_num": r[4], "vencimento_d": r[5], "quantos": r[6]}
            for r in linhas]


# ---------------------------------------------------------------------------
# 4. Possível duplicidade por valor
# ---------------------------------------------------------------------------
def possivel_duplicidade(f: dict, aplicar_filtros: bool = False,
                         dias: int = 7) -> list[dict]:
    """Mesmo CPF/CNPJ e mesmo valor, com as SPs a poucos dias uma da outra.

    Complementa a análise da IA por um caminho determinístico: não depende de
    ninguém ter escrito nada na coluna AL.

    A janela é a distância entre a primeira e a última solicitação do grupo.
    Grupos mais espalhados que `dias` ficam de fora — dois aluguéis iguais com
    trinta dias de diferença são o normal, não duplicidade."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros)
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
def sem_classificacao(f: dict, aplicar_filtros: bool = False) -> list[dict]:
    """SPs sem centro de custo, sem projeto, ou sem os dois.

    Sem essas duas informações a despesa não entra em nenhum relatório por
    obra — ela existe, mas some da análise."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros)
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
def sem_integracao_omie(f: dict, aplicar_filtros: bool = False) -> list[dict]:
    """SPs ainda ativas e sem código de integração do Omie.

    "Ativa" quer dizer nem paga nem cancelada. Uma SP já paga sem código é
    problema de histórico; uma ativa é trabalho que ainda precisa ser feito, e
    é essa que a tela cobra."""
    from .db import consultar
    where, params = _where(f, aplicar_filtros)
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
def codigos_de_barras(f: dict, aplicar_filtros: bool = False) -> dict:
    """Os dois problemas de boleto, juntos: inválidos e repetidos.

    Reusa exatamente o mesmo SQL do filtro da tela de Solicitações. Se a regra
    mudar, muda num lugar só, e as duas telas continuam concordando — divergir
    aqui seria pior do que não ter a tela."""
    from . import consultas
    from .db import consultar

    where, params = _where(f, aplicar_filtros)
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


def resumo(f: dict, aplicar_filtros: bool = False) -> dict:
    """Quantos problemas cada checagem encontrou.

    Uma consulta por checagem, todas devolvendo um número só. É o que permite a
    tela abrir mostrando onde há trabalho, sem carregar sete listas."""
    from . import consultas
    from .db import consultar_um

    where, params = _where(f, aplicar_filtros)

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
