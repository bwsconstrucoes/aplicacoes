# -*- coding: utf-8 -*-
"""
A OBRA DO MÊS — automática, sem pedido ao DP (migração 119, 20/11/2026).

Ele: *"Isso é um trabalho a mais para o DP (…) não quero que o DP tenha
trabalho numa coisa que o sistema pode detectar automaticamente (…) num
determinado mês, aquela pessoa deve ser considerada na obra que ela mais
trabalhou. Porque isso, em relação a cálculo de FGTS, INSS, é o que deve ser
levado em consideração. Por exemplo (…) trabalhou 15 dias numa obra, 5 dias na
outra (…) a obra que ela trabalhou os 15 dias. E todo mês isso a gente faz
essa alteração."*

A REGRA:
  · conta-se DIAS trabalhados por obra no mês (dia com ao menos uma batida
    válida naquela obra), não batidas;
  · vence a obra com mais dias; empate, a do dia trabalhado mais recente;
  · o mês é o do CALENDÁRIO (a competência das guias de FGTS e INSS);
  · no começo de cada mês a rotina diária calcula o mês que acabou e põe a
    obra vencedora como obra de CADASTRO. Quem não bateu ponto no mês não muda.
  · cada mês fica guardado (com os dias de cada obra e a obra de antes): é a
    base do relatório de lotação para a contabilidade — que ainda vai ser
    feito (ele: *"demanda outro relatório"*).

⚑ CUSTO: uma consulta ao banco por mês para todo mundo, mais uma gravação por
pessoa. Nada de internet. Só LÊ o ponto.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _tem(s: Session) -> bool:
    return (s.execute(text("SELECT 1 FROM information_schema.tables WHERE "
                           "table_schema = 'public' AND table_name = "
                           "'colaborador_obra_mes'")).first() is not None
            and s.execute(text("SELECT 1 FROM information_schema.tables WHERE "
                               "table_schema = 'ponto' AND table_name = "
                               "'marcacoes'")).first() is not None)


def mes_anterior(hoje: date) -> date:
    return (hoje.replace(day=1) - timedelta(days=1)).replace(day=1)


def calcular(s: Session, competencia: date) -> list[dict[str, Any]]:
    """Por pessoa: a obra do mês, os dias dela e os dias de cada obra."""
    inicio = competencia.replace(day=1)
    fim = (inicio + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    linhas = s.execute(text("""
        SELECT colaborador_id, obra_id, count(DISTINCT data_referencia) AS dias,
               max(data_referencia) AS ultimo
          FROM ponto.marcacoes
         WHERE status <> 'REJEITADA' AND obra_id IS NOT NULL
           AND data_referencia BETWEEN :i AND :f
         GROUP BY colaborador_id, obra_id"""), {"i": inicio, "f": fim}).mappings().all()
    por_pessoa: dict[int, list] = {}
    for l in linhas:
        por_pessoa.setdefault(int(l["colaborador_id"]), []).append(l)
    saida = []
    for cid, obras in por_pessoa.items():
        venc = max(obras, key=lambda o: (o["dias"], o["ultimo"]))
        saida.append({"colaborador_id": cid, "obra_id": int(venc["obra_id"]),
                      "dias": int(venc["dias"]),
                      "dias_por_obra": {str(o["obra_id"]): int(o["dias"]) for o in obras}})
    return saida


def aplicar(s: Session, competencia: date) -> dict[str, Any]:
    """Guarda o mês e troca a obra de cadastro de quem mudou. Uma vez por mês:
    pessoa já calculada neste mês não é mexida de novo."""
    from app.apps.erp.core.comum.auditoria import registrar_evento
    competencia = competencia.replace(day=1)
    feito = {"competencia": str(competencia), "pessoas": 0, "mudaram": 0}
    for r in calcular(s, competencia):
        atual = s.execute(text("""
            SELECT obra_id, UPPER(COALESCE(situacao, 'ATIVO')) AS situacao
              FROM colaboradores WHERE id = :c"""), {"c": r["colaborador_id"]}).mappings().first()
        if atual is None:
            continue
        mudou = (atual["situacao"] == "ATIVO" and atual["obra_id"] != r["obra_id"])
        novo = s.execute(text("""
            INSERT INTO colaborador_obra_mes (colaborador_id, competencia, obra_id, dias,
                                              dias_por_obra, obra_anterior_id, mudou)
            VALUES (:c, :comp, :o, :d, CAST(:pd AS jsonb), :ant, :m)
            ON CONFLICT (colaborador_id, competencia) DO NOTHING
            RETURNING colaborador_id"""),
            {"c": r["colaborador_id"], "comp": competencia, "o": r["obra_id"],
             "d": r["dias"], "pd": json.dumps(r["dias_por_obra"]),
             "ant": atual["obra_id"], "m": mudou}).first()
        if novo is None:
            continue
        feito["pessoas"] += 1
        if mudou:
            s.execute(text("UPDATE colaboradores SET obra_id = :o WHERE id = :c"),
                      {"o": r["obra_id"], "c": r["colaborador_id"]})
            registrar_evento(s, "colaborador", r["colaborador_id"], "OBRA_DO_MES",
                             {"competencia": str(competencia), "de": atual["obra_id"],
                              "para": r["obra_id"], "dias_por_obra": r["dias_por_obra"]},
                             None)
            feito["mudaram"] += 1
    logger.info("ERP/obra do mês: %s", feito)
    return feito


def se_preciso(s: Session, *, hoje: Optional[date] = None) -> Optional[dict[str, Any]]:
    """Na rodada diária: calcula o mês que acabou, se ainda não calculou.
    Nunca levanta."""
    try:
        if not _tem(s):
            return None
        comp = mes_anterior(hoje or date.today())
        ja = s.execute(text("SELECT 1 FROM colaborador_obra_mes WHERE competencia = :c "
                            "LIMIT 1"), {"c": comp}).first()
        if ja:
            return None
        with s.begin_nested():
            return aplicar(s, comp)
    except Exception:  # noqa: BLE001
        logger.exception("ERP/obra do mês: falhou")
        return None


def do_mes(s: Session, competencia: date, *, so_quem_mudou: bool = False) -> list[dict[str, Any]]:
    """O mês calculado — a base do relatório de lotação para a contabilidade."""
    if not _tem(s):
        return []
    return [dict(l) for l in s.execute(text("""
        SELECT m.colaborador_id, c.nome, c.cpf, c.vinculo, o.codigo AS obra, o.cno,
               a.codigo AS obra_anterior, m.dias, m.dias_por_obra, m.mudou
          FROM colaborador_obra_mes m
          JOIN colaboradores c ON c.id = m.colaborador_id
          LEFT JOIN obras o ON o.id = m.obra_id
          LEFT JOIN obras a ON a.id = m.obra_anterior_id
         WHERE m.competencia = :c AND (NOT :so OR m.mudou)
         ORDER BY c.nome"""), {"c": competencia.replace(day=1),
                               "so": so_quem_mudou}).mappings().all()]
