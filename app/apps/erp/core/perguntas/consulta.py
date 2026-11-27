# -*- coding: utf-8 -*-
"""
RODAR A CONSULTA QUE A IA MONTOU — com a porta trancada por fora.

⚑ ESTE ARQUIVO É A TRAVA, e ele não chama IA nenhuma: recebe um texto de SQL
de qualquer origem e decide se ele pode rodar. Está separado de propósito —
assim dá para testá-lo inteiro sem gastar um centavo de IA, e é o que a suíte
faz.

O RISCO QUE ELE EXISTE PARA CONTER, nas palavras do próprio dono sobre outra
coisa: um número errado com cara de certo é pior que resposta nenhuma. Aqui
há um segundo risco, maior: consulta montada na hora é código executando no
banco da empresa. Sem trava, o assistente seria a porta mais larga do ERP.

AS SEIS TRAVAS, e por que cada uma:

1. **Um SELECT só.** Nada de `;` emendando um segundo comando — o truque mais
   velho que existe.
2. **Começa em SELECT.** Não em WITH, não em nada: as visões já entram como
   `WITH` montado por NÓS, e deixar a IA abrir o próprio `WITH` permitiria
   que ela redefinisse uma visão nossa com outro conteúdo.
3. **Só os nomes do mapa.** Todo `FROM` e todo `JOIN` tem de apontar para uma
   visão do mapa. É isto que impede alcançar `usuarios`, `titulos` ou
   qualquer tabela que a pessoa não poderia ver pela tela.
4. **Lista de palavras proibidas** (INSERT, UPDATE, DELETE, DROP, COPY,
   pg_sleep, dblink…). Cinto além do suspensório: a trava 3 já barraria, mas
   esta falha barulhento e cedo.
5. **Prazo de 8 segundos.** Consulta montada na hora pode cruzar tudo com
   tudo sem querer. Oito segundos é o que uma pergunta de tela aguenta.
6. **Teto de linhas.** A resposta é para uma pessoa ler no celular, não um
   despejo de tabela. Acima do teto, ela diz que cortou.

⚠️ **O QUE ISTO NÃO É:** não é um usuário de banco só-leitura. A garantia vem
da conferência do texto, não do Postgres. Um usuário próprio, sem permissão
de escrita, seria mais forte — e está anotado como o passo seguinte no
`HISTORICO_PESSOAL.md`. Enquanto não existe, as travas 1 a 4 são o que
seguram, e são as que a suíte testa uma a uma.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

PRAZO_SEGUNDOS = 8
TETO_DE_LINHAS = 200

# Palavras que não têm o que fazer numa pergunta de leitura. Conferidas como
# PALAVRA INTEIRA: "update" barra, mas "atualizado_em" não.
PROIBIDAS = (
    "insert", "update", "delete", "drop", "alter", "create", "truncate",
    "grant", "revoke", "copy", "vacuum", "analyze", "reindex", "cluster",
    "comment", "merge", "call", "do", "execute", "prepare", "listen",
    "notify", "lock", "set", "reset", "begin", "commit", "rollback",
    "savepoint", "pg_sleep", "pg_read_file", "pg_ls_dir", "dblink",
    "lo_import", "lo_export", "information_schema", "pg_catalog",
    "current_setting", "set_config", "into",
)

_PALAVRA = re.compile(r"[a-z_][a-z0-9_]*", re.IGNORECASE)
# De onde se lê: FROM x, JOIN x. O apelido que vem depois não interessa.
_FONTES = re.compile(r"\b(?:from|join)\s+([a-z_][a-z0-9_.]*)", re.IGNORECASE)


def _sem_textos(sql: str) -> str:
    """Troca o conteúdo entre aspas simples por espaço, mantendo o tamanho.

    Serve às conferências de palavra: o que a pessoa procura ("Residencial do
    Vale", "Em análise") não pode ser lido como comando.
    """
    return re.sub(r"'(?:[^']|'')*'", lambda m: " " * len(m.group(0)), sql)


class ConsultaRecusada(ErroValidacao):
    """A consulta montada não passou nas travas. A mensagem diz o porquê em
    português — ela aparece na tela, e quem lê não é programador."""


def conferir(sql: str, visoes_permitidas: set[str]) -> str:
    """Devolve o SQL limpo, ou levanta `ConsultaRecusada` dizendo o motivo."""
    bruto = (sql or "").strip()
    # A IA às vezes devolve dentro de ```sql ... ``` mesmo mandada não pôr.
    bruto = re.sub(r"^```[a-z]*\s*|\s*```$", "", bruto, flags=re.IGNORECASE).strip()
    bruto = bruto.rstrip(";").strip()
    if not bruto:
        raise ConsultaRecusada("A consulta veio vazia.")

    # 1) um comando só
    if ";" in bruto:
        raise ConsultaRecusada(
            "A consulta trouxe mais de um comando. Só é permitido um, de leitura.")

    # 2) começa em SELECT
    if not re.match(r"^select\b", bruto, re.IGNORECASE):
        raise ConsultaRecusada(
            "A consulta precisa começar com SELECT — é só leitura.")

    # ⚠️ O TEXTO ENTRE ASPAS SAI ANTES DA CONFERÊNCIA, e isto é um defeito que
    # eu mesmo escrevi e peguei relendo: sem tirar, `WHERE obra = 'Residencial
    # do Vale'` seria recusado porque "do" está na lista de proibidas. Nome de
    # obra em português tem "do", "da", "set"… A trava vale para o COMANDO,
    # não para o que a pessoa está procurando.
    minusculo = _sem_textos(bruto).lower()
    # 3) palavras proibidas, como palavra inteira
    for p in PROIBIDAS:
        if re.search(rf"\b{re.escape(p)}\b", minusculo):
            raise ConsultaRecusada(
                f"A consulta usa “{p}”, que não é permitido aqui — este "
                f"caminho só lê.")

    # 4) toda fonte é uma visão do mapa (também sem o que está entre aspas:
    # uma literal com "from" dentro não é leitura de tabela nenhuma)
    fontes = {f.lower() for f in _FONTES.findall(_sem_textos(bruto))}
    # `JOIN LATERAL`, `FROM (SELECT ...)` e afins não casam com o padrão; o
    # que casa e não é visão, barra.
    fora = {f for f in fontes if f not in visoes_permitidas and f != "lateral"}
    if fora:
        raise ConsultaRecusada(
            "A consulta tentou ler de "
            + ", ".join(f"“{f}”" for f in sorted(fora))
            + ", que não faz parte do que o assistente pode consultar.")
    if not fontes:
        raise ConsultaRecusada(
            "A consulta não diz de onde está lendo.")
    return bruto


def _com_as_visoes(sql: str, visoes: list[dict[str, Any]]) -> str:
    """Põe as visões como `WITH` antes da consulta. É aqui que o recorte da
    pessoa entra — e por isso a IA não precisa (nem pode) cuidar dele."""
    blocos = ",\n".join(f"{v['nome']} AS (\n{v['sql'].strip()}\n)" for v in visoes)
    return f"WITH {blocos}\n{sql}\nLIMIT {TETO_DE_LINHAS + 1}"


def rodar(s: Session, sql: str, visoes: list[dict[str, Any]]) -> dict[str, Any]:
    """Roda a consulta já conferida e devolve as linhas.

    ⚑ EM SAVEPOINT: consulta montada na hora erra de jeitos criativos (coluna
    que não existe, tipo que não bate). Sem o savepoint, o erro envenenaria a
    transação da requisição inteira e a tela cairia com um erro que não tem
    nada a ver com a pergunta.
    """
    limpo = conferir(sql, {v["nome"] for v in visoes})
    inteiro = _com_as_visoes(limpo, visoes)
    parametros: dict[str, Any] = {}
    for v in visoes:
        parametros.update(v.get("parametros") or {})

    with s.begin_nested():
        s.execute(text(f"SET LOCAL statement_timeout = '{PRAZO_SEGUNDOS}s'"))
        try:
            resultado = s.execute(text(inteiro), parametros)
            linhas = [dict(l) for l in resultado.mappings().all()]
        except Exception as e:                              # noqa: BLE001
            logger.info("ERP/perguntar: consulta recusada pelo banco: %s", e)
            raise ConsultaRecusada(_motivo_do_banco(e)) from e
        # O prazo valia só para esta consulta: `SET LOCAL` dura a transação
        # inteira, e deixá-lo de pé encurtaria o prazo de tudo que vier
        # depois na mesma requisição.
        s.execute(text("SET LOCAL statement_timeout = DEFAULT"))

    cortou = len(linhas) > TETO_DE_LINHAS
    return {"linhas": linhas[:TETO_DE_LINHAS], "cortou": cortou,
            "quantas": len(linhas[:TETO_DE_LINHAS]), "sql": limpo}


def _motivo_do_banco(e: Exception) -> str:
    """Traduz a reclamação do Postgres para quem está na tela.

    Não é frescura: "UndefinedColumn: column c.foo does not exist" na tela do
    dono é exatamente o tipo de coisa que faz ele achar que o sistema quebrou.
    """
    texto = str(e).lower()
    if "statement timeout" in texto or "canceling statement" in texto:
        return (f"A consulta demorou mais de {PRAZO_SEGUNDOS} segundos e foi "
                f"interrompida. Tente perguntar de um período menor.")
    if "does not exist" in texto or "undefined" in texto:
        return ("A consulta montada citou um campo que não existe. A pergunta "
                "não foi respondida — e isso ficou registrado para melhorar.")
    return ("A consulta montada não pôde ser executada. A pergunta não foi "
            "respondida — e isso ficou registrado para melhorar.")
