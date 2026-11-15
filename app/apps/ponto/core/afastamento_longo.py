# -*- coding: utf-8 -*-
"""
O ATESTADO QUE PASSA DE 15 DIAS ABRE O AFASTAMENTO SOZINHO (15/11/2026).

Decisão do dono, escolhendo entre três caminhos: *"Acima de 15 dias, o sistema
abre sozinho."*

POR QUE 15, E POR QUE AUTOMÁTICO
--------------------------------
Os primeiros 15 dias de atestado são **da empresa** — ela paga o salário e
abona as faltas, e o atestado resolve tudo sozinho. Do 16º dia em diante o
benefício é **do INSS**, e aí deixa de ser abono de falta e passa a ser
AFASTAMENTO: há perícia a agendar, documento a juntar, e a folha trata a
pessoa de outro jeito.

É a mesma conta quando os atestados vêm **picados**: a lei soma os dias de
atestado da MESMA doença dentro de 60 dias. Quinze dias somados, mesmo em três
atestados de cinco, e o 16º já é do INSS.

E é justamente aí que escapa: ninguém do DP vai somar à mão os atestados de
575 pessoas para descobrir que o terceiro de alguém estourou o limite. Ele
disse por que não quis o caminho manual: *"é justamente aí que escapa."*

O QUE ESTE MÓDULO FAZ, E O QUE NÃO FAZ
--------------------------------------
FAZ: ao aprovar um atestado, soma os dias de atestado aprovado da pessoa na
janela de 60 dias; se passou de 15, abre um pedido de AFASTAMENTO
**aguardando o DP** — não aprovado. Quem decide continua sendo gente.

NÃO FAZ: não abona nada por conta própria, não mexe no atestado que o
disparou, não agenda perícia e não decide se o caso é do INSS. O afastamento
nasce como PEDIDO, com a conta escrita na descrição, para o DP conferir.

⚑ POR QUE NASCE "AGUARDANDO_DP" E NÃO APROVADO: afastamento aprovado muda o
que a folha paga. Um número somado errado (atestado de doença diferente, por
exemplo — e a lei fala da MESMA doença, coisa que só quem lê o CID sabe) viraria
dinheiro errado sem ninguém olhar. Aberto como pedido, o pior caso é o DP negar.

⚑ E NÃO ABRE DUAS VEZES: se já existe afastamento da pessoa cobrindo o período,
em qualquer situação menos negado/cancelado, o módulo não faz nada. Sem isso,
cada atestado novo da mesma pessoa abriria outro pedido, e a fila do DP viraria
uma lista de repetições — que é o jeito mais rápido de uma fila deixar de ser
olhada.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Optional

from sqlalchemy.engine import Connection

from .. import db

logger = logging.getLogger("ponto.afastamento_longo")

# Os primeiros 15 dias são da empresa; do 16º em diante, do INSS.
DIAS_DA_EMPRESA = 15
# A janela em que a lei soma atestados da mesma doença.
JANELA_DIAS = 60
MARCA = "ABERTO PELO SISTEMA"


def _dias(inicio: dt.date, fim: dt.date) -> int:
    """Quantos dias o atestado cobre, contando o primeiro e o último."""
    return (fim - inicio).days + 1


def somar_atestados(conn: Connection, colaborador_id: int,
                    ate: dt.date) -> dict[str, Any]:
    """Quantos dias de atestado aprovado a pessoa tem na janela de 60 dias.

    Conta DIAS DE CALENDÁRIO cobertos, e não a soma dos tamanhos de cada
    atestado: dois atestados que se sobrepõem (acontece, quando a pessoa volta
    ao médico antes de o primeiro acabar) somariam o mesmo dia duas vezes e
    estourariam o limite sem motivo.

    A declaração de comparecimento fica FORA: ela é algumas horas de consulta,
    não dia de doença — somá-la empurraria gente para o INSS por ter ido ao
    dentista três vezes.
    """
    desde = ate - dt.timedelta(days=JANELA_DIAS - 1)
    linhas = db.todos(conn, """
        SELECT data_inicio, data_fim
          FROM ponto.ocorrencias
         WHERE colaborador_id = :c AND tipo = 'ATESTADO' AND status = 'APROVADA'
           AND COALESCE(subtipo, '') NOT LIKE 'DECLARACAO%'
           AND data_fim >= :desde AND data_inicio <= :ate
         ORDER BY data_inicio""", c=int(colaborador_id), desde=desde, ate=ate)
    cobertos: set[dt.date] = set()
    for l in linhas:
        d = max(l["data_inicio"], desde)
        fim = min(l["data_fim"], ate)
        while d <= fim:
            cobertos.add(d)
            d += dt.timedelta(days=1)
    return {"dias": len(cobertos), "janela_de": desde, "janela_ate": ate,
            "passou": len(cobertos) > DIAS_DA_EMPRESA,
            "atestados": len(linhas)}


def ja_tem_afastamento(conn: Connection, colaborador_id: int,
                       inicio: dt.date, fim: dt.date) -> bool:
    """Já existe afastamento desta pessoa tocando este período?"""
    return db.um(conn, """
        SELECT 1 FROM ponto.ocorrencias
         WHERE colaborador_id = :c AND tipo = 'AFASTAMENTO'
           AND status NOT IN ('NEGADA', 'CANCELADA')
           AND data_fim >= :i AND data_inicio <= :f
         LIMIT 1""", c=int(colaborador_id), i=inicio, f=fim) is not None


def conferir(conn: Connection, atestado: dict[str, Any], *,
             quem_nome: str = "") -> Optional[dict[str, Any]]:
    """Chamado quando um ATESTADO termina aprovado. Abre o afastamento se cabe.

    Devolve o pedido criado, ou `None` quando não havia o que fazer — que é o
    caso comum (atestado de três dias não abre nada).
    """
    if atestado.get("tipo") != "ATESTADO":
        return None
    if str(atestado.get("subtipo") or "").startswith("DECLARACAO"):
        return None
    colaborador_id = int(atestado["colaborador_id"])
    fim = atestado["data_fim"]
    conta = somar_atestados(conn, colaborador_id, fim)
    if not conta["passou"]:
        return None

    # O afastamento começa no dia seguinte ao 15º dia coberto. Na prática, com
    # um atestado único e longo, é o 16º dia dele; com atestados picados, é o
    # começo do que estourou a conta — e o DP ajusta a data ao conferir, que é
    # para isso que o pedido nasce aberto.
    excedente = conta["dias"] - DIAS_DA_EMPRESA
    inicio = fim - dt.timedelta(days=excedente - 1)
    if inicio < atestado["data_inicio"]:
        inicio = atestado["data_inicio"]
    if ja_tem_afastamento(conn, colaborador_id, inicio, fim):
        logger.info("Ponto: atestado %s passou de %d dias, mas a pessoa %s já "
                    "tem afastamento no período — nada a abrir",
                    atestado.get("id"), DIAS_DA_EMPRESA, colaborador_id)
        return None

    descricao = (
        f"{MARCA}: os atestados aprovados desta pessoa somam {conta['dias']} "
        f"dias entre {conta['janela_de']:%d/%m/%Y} e {conta['janela_ate']:%d/%m/%Y} "
        f"({conta['atestados']} atestado(s)). Passou dos {DIAS_DA_EMPRESA} dias "
        f"que são da empresa, então do 16º dia em diante o benefício é do INSS. "
        f"CONFIRA antes de aprovar: a soma vale para a MESMA doença, e só quem "
        f"lê o atestado sabe se é o caso. Ajuste as datas se precisar."
    )
    linha = db.um(conn, """
        INSERT INTO ponto.ocorrencias
               (colaborador_id, tipo, data_inicio, data_fim, obra_id, descricao,
                status, origem, solicitado_por)
        VALUES (:c, 'AFASTAMENTO', :i, :f, :o, :d, 'AGUARDANDO_DP', 'GESTAO', :por)
        RETURNING id""",
        c=colaborador_id, i=inicio, f=fim, o=atestado.get("obra_id"),
        d=descricao[:1000],
        por=f"sistema (atestado nº {atestado.get('id')})"[:120])
    logger.info("Ponto: atestado %s somou %d dias — afastamento %s aberto para o DP",
                atestado.get("id"), conta["dias"], linha["id"])
    return {"id": int(linha["id"]), "data_inicio": inicio, "data_fim": fim,
            "dias_somados": conta["dias"], "descricao": descricao}
