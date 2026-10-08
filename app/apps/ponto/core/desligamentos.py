# -*- coding: utf-8 -*-
"""
QUEM SAIU PERDE O ACESSO SOZINHO.

Pedido do dono, 06/10/2026: *"o acesso dos colaboradores ao ponto precisa ser
bloqueado automaticamente quando o colaborador for desligado."*

O que já valia antes, e continua: a batida e a entrada no "Meu ponto" de quem
está DESLIGADO são recusadas, e a sessão aberta cai na primeira tela que ele
abrir. O que esta rotina acrescenta é o que sobrava ATIVO em nome da pessoa:

  · o celular pessoal aprovado → BLOQUEADO ("desligado");
  · o ponto da obra ou de equipe de que ela era a RESPONSÁVEL → BLOQUEADO
    também (07/10/2026: todo aparelho tem responsável; sem ele, ninguém
    responde pelo aparelho — reativa-se aprovando de novo com outra pessoa);
  · os QR Codes ainda valendo → cancelados (o print no celular não serve mais);
  · os WhatsApps de QR esperando na fila → CANCELADOS;
  · o lugar nos aparelhos de grupo (quem bate pela equipe) → retirado.

"Desligado" é o MESMO critério da base de pessoas em uso (o Registro de
Colaboradores ou o ERP — `cadastros.colaborador_por_id`): saída que já passou,
ou fase "desligado" no Pipefy. Roda a cada 15 minutos junto com a base de
pessoas (`registro.manter_em_dia`) e na rotina do dia.
"""
from __future__ import annotations

import logging

from sqlalchemy.engine import Connection

from .. import db
from . import cadastros

logger = logging.getLogger("ponto.desligamentos")

POR = "o próprio ponto (pessoa desligada)"


def _candidatos(conn: Connection) -> set[int]:
    """Quem ainda tem alguma coisa valendo no ponto — a lista é curta."""
    ids = {int(l["c"]) for l in db.todos(conn, """
        SELECT colaborador_id AS c FROM ponto.dispositivos
         WHERE status = 'APROVADO' AND colaborador_id IS NOT NULL
        UNION SELECT colaborador_id FROM ponto.dispositivo_autorizados""")}
    if db.tem_003(conn):
        ids |= {int(l["c"]) for l in db.todos(conn, """
            SELECT DISTINCT colaborador_id AS c FROM ponto.qr_codigos WHERE revogado_em IS NULL
            UNION SELECT DISTINCT colaborador_id FROM ponto.envios
             WHERE tipo = 'QR' AND status = 'PENDENTE' AND colaborador_id IS NOT NULL""")}
    return ids


def aplicar(conn: Connection) -> dict:
    feito = {"pessoas": 0, "celulares": 0, "qr": 0, "envios": 0, "grupos": 0}
    for cid in sorted(_candidatos(conn)):
        p = cadastros.colaborador_por_id(conn, cid)
        if p and p["situacao"] != "DESLIGADO":
            continue
        feito["pessoas"] += 1
        feito["celulares"] += db.executar(conn, """
            UPDATE ponto.dispositivos SET status = 'BLOQUEADO', bloqueado_em = now(),
                   motivo_bloqueio = CASE WHEN perfil = 'INDIVIDUAL'
                                          THEN 'pessoa desligada (bloqueio automático)'
                                          ELSE 'responsável desligado — aprove de novo com outro responsável' END
             WHERE colaborador_id = :c AND status = 'APROVADO'""", c=cid)
        feito["grupos"] += db.executar(conn, "DELETE FROM ponto.dispositivo_autorizados WHERE colaborador_id = :c",
                                       c=cid)
        if db.tem_003(conn):
            from . import qr
            feito["qr"] += qr.revogar_todos(conn, cid, POR)
            feito["envios"] += db.executar(conn, """
                UPDATE ponto.envios SET status = 'CANCELADO', resultado = 'pessoa desligada'
                 WHERE colaborador_id = :c AND tipo = 'QR' AND status = 'PENDENTE'""", c=cid)
    if feito["pessoas"]:
        logger.info("Ponto: acesso fechado de %d pessoa(s) desligada(s) — %s", feito["pessoas"], feito)
    return feito
