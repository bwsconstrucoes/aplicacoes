# -*- coding: utf-8 -*-
"""
O QUE O PONTO LANÇA E PRECISA DA CONTABILIDADE VIRA SOLICITAÇÃO AO DP
(20/11/2026).

Ele: *"não é para ser o seguinte, não é para um lançamento via ponto que gera
um afastamento. Eu acho que ele automaticamente gera uma solicitação do DP
para ser dado o devido tratamento junto à contabilidade (…) se o que lançar no
ponto demandar ações do DP junto à contabilidade (…) tem que ter essa
tratativa (…) no mesmo modelo que a gente já trabalha com outras situações."*

O QUE ATRAVESSA:
  · afastamento lançado no ponto — inclusive o que o próprio ponto abre
    sozinho quando os atestados passam de 15 dias
    (`ponto/core/afastamento_longo.py`) — vira "Comunicação de Afastamento",
    de quem é de carteira (prestador não tem eSocial; resolve-se aqui).

O QUE NÃO ATRAVESSA: atestado até 15 dias, licenças da lei (casamento, luto…),
ajuste de batida — esses vão para a folha no FECHAMENTO do ponto, não como
pedido avulso.

⚑ SÓ LÊ O PONTO. Nada é escrito no schema `ponto`. O vínculo fica na própria
solicitação (`dados.ocorrencia_ponto`), e é ele que impede a segunda.

⚑ QUANDO RODA: ao abrir a tela de Solicitações do DP e na rodada diária. Uma
consulta ao banco; nada de internet.

⚑ SÓ DAQUI PARA A FRENTE: afastamento do ponto anterior a 20/11/2026 já foi
tratado por fora — trazer o passado encheria a fila de pedidos velhos.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DESDE = date(2026, 11, 20)


def _tem_ponto(s: Session) -> bool:
    return s.execute(text(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'ponto' "
        "AND table_name = 'ocorrencias'")).first() is not None


def trazer(s: Session) -> dict[str, Any]:
    """Abre a solicitação dos afastamentos novos do ponto e cancela a dos que
    o ponto negou ou cancelou. Nunca levanta."""
    from app.apps.erp.core import pessoal_solicitacoes as svc
    feito = {"abertas": 0, "canceladas": 0}
    try:
        if not _tem_ponto(s):
            return feito
        with s.begin_nested():
            novos = s.execute(text("""
                SELECT o.id, o.colaborador_id, o.data_inicio, o.data_fim, o.descricao,
                       o.status, o.obra_id, o.solicitado_por, o.dp_nome
                  FROM ponto.ocorrencias o
                  JOIN colaboradores c ON c.id = o.colaborador_id
                 WHERE o.tipo = 'AFASTAMENTO'
                   AND o.status IN ('AGUARDANDO_SUPERVISOR', 'AGUARDANDO_DP', 'APROVADA')
                   AND o.criado_em >= :desde
                   AND UPPER(COALESCE(c.vinculo, 'CTPS')) = 'CTPS'
                   AND NOT EXISTS (
                       SELECT 1 FROM dp_solicitacoes d
                        WHERE d.tipo = 'AFASTAMENTO'
                          AND d.dados ->> 'ocorrencia_ponto' = o.id::text)
                 ORDER BY o.id"""), {"desde": DESDE}).mappings().all()
            for o in novos:
                previsao = o["data_fim"].isoformat() if o["data_fim"] else ""
                # A soma dos atestados acima de 15 dias é auxílio-doença do
                # INSS — vem marcada; o DP confere e troca se for outra coisa.
                natureza = ("INSS" if str(o["solicitado_por"] or "")
                            .startswith("sistema (atestado") else "")
                p = svc.abrir(s, {
                    "tipo": "AFASTAMENTO", "colaborador_id": o["colaborador_id"],
                    **({"afastamento_natureza": natureza} if natureza else {}),
                    "afast_inicio": o["data_inicio"].isoformat(),
                    "afast_previsao": previsao,
                    "observacao": (f"Lançado no ponto (pedido nº {o['id']}): "
                                   f"{o['descricao'] or ''}")[:900]}, None)
                s.execute(text("""
                    UPDATE dp_solicitacoes
                       SET dados = dados || CAST(:j AS jsonb),
                           requerente = :r, obra_id = COALESCE(obra_id, :o)
                     WHERE id = :i"""),
                    {"i": p["id"], "o": o["obra_id"],
                     "r": f"Ponto ({o['solicitado_por'] or 'lançamento'})"[:120],
                     "j": json.dumps({"ocorrencia_ponto": str(o["id"])})})
                feito["abertas"] += 1
            # O ponto negou ou cancelou: a solicitação que ainda não andou cai.
            for d in s.execute(text("""
                SELECT d.id, d.numero, o.status FROM dp_solicitacoes d
                  JOIN ponto.ocorrencias o ON o.id::text = d.dados ->> 'ocorrencia_ponto'
                 WHERE d.tipo = 'AFASTAMENTO' AND d.situacao IN ('ABERTA', 'EM_ANALISE')
                   AND o.status IN ('NEGADA', 'CANCELADA')""")).mappings().all():
                s.execute(text("""
                    UPDATE dp_solicitacoes SET situacao = 'CANCELADA', cancelado_em = now(),
                           atualizado_em = now(),
                           motivo_cancelamento = :m, resposta = :m WHERE id = :i"""),
                    {"i": d["id"], "m": f"o afastamento foi {d['status'].lower()} no ponto"})
                svc._evento(s, d["id"], "CANCELADA",
                            {"motivo": f"{d['status'].lower()} no ponto"}, None)
                feito["canceladas"] += 1
        if feito["abertas"] or feito["canceladas"]:
            logger.info("ERP/ponte do ponto: %s", feito)
    except Exception:  # noqa: BLE001 — ponte quebrada não derruba a tela do DP
        logger.exception("ERP/ponte do ponto: falhou")
    return feito
