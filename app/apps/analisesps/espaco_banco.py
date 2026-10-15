# -*- coding: utf-8 -*-
"""
QUANTO CADA ÁREA OCUPA NO BANCO — 14/10/2026.

O dono, ao ver o banco do Render em 73%: *"o que é isso tanto? (…) se eu
começar a rodar o ERP, não vai dar para nada (…) o combinado é que tudo que
seja arquivo, foto, vai para o Google Drive. Queria entender se a gente
consegue saber o que é tanto e como enxugar."*

A tela de saúde do ERP mede só o espaço DELE (o esquema `public`). Painel,
Análise de SPs, Ponto e Mensageria vivem cada um no seu esquema, no MESMO
banco — e ficavam fora da conta. Aqui a medida é do banco inteiro.

SÓ LÊ. Os números vêm do catálogo do Postgres (tamanho de cada tabela, com
índices e a parte guardada à parte dos textos grandes), não de varrer as
tabelas: a medida custa milissegundos.

"ESPAÇO MORTO": linha apagada ou regravada não devolve o espaço na hora — ele
fica marcado como reaproveitável. Tabela que é apagada e recarregada inteira
(espelhos, cargas) acumula muito disso. O número é estimativa do Postgres.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("analisesps.espaco")

AREAS = {
    "public": "ERP",
    "analisesps": "Análise de SPs",
    "painel": "Painel OMIE",
    "ponto": "Ponto eletrônico",
    "mensageria": "Mensageria",
}
QUANTAS_TABELAS = 30


def _mb(bytes_) -> float:
    return round(int(bytes_ or 0) / (1024 * 1024), 1)


def medir() -> dict:
    """{"total_mb", "areas": [...], "tabelas": [...], "morto_mb"} ou {"erro"}."""
    from .db import consultar, consultar_um
    try:
        total = consultar_um("SELECT pg_database_size(current_database())")
        linhas = consultar(
            "SELECT n.nspname, c.relname, pg_total_relation_size(c.oid), "
            "       pg_relation_size(c.oid), pg_indexes_size(c.oid), "
            "       coalesce(s.n_live_tup, 0), coalesce(s.n_dead_tup, 0), "
            "       greatest(s.last_autovacuum, s.last_vacuum) "
            "  FROM pg_class c "
            "  JOIN pg_namespace n ON n.oid = c.relnamespace "
            "  LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid "
            " WHERE c.relkind IN ('r', 'm') "
            "   AND n.nspname <> 'information_schema' "
            "   AND n.nspname !~ '^pg_' "
            " ORDER BY 3 DESC")
    except Exception as e:  # noqa: BLE001 — a tela de configuração não cai
        logger.exception("Espaço no banco: não consegui medir")
        return {"erro": f"Não consegui medir: {str(e)[:200]}"}

    total_bytes = int((total or (0,))[0] or 0)
    areas: dict = {}
    tabelas = []
    morto_estimado = 0
    for esquema, nome, tudo, dados, indices, vivas, mortas, limpeza in linhas:
        tudo = int(tudo or 0)
        area = AREAS.get(esquema, esquema)
        a = areas.setdefault(area, {"area": area, "bytes": 0, "tabelas": 0})
        a["bytes"] += tudo
        a["tabelas"] += 1
        vivas, mortas = int(vivas or 0), int(mortas or 0)
        # A parte morta, proporcional às linhas mortas — estimativa grosseira,
        # mas é a ordem de grandeza que decide se vale uma limpeza.
        morto = int(tudo * mortas / (vivas + mortas)) if (vivas + mortas) else 0
        morto_estimado += morto
        tabelas.append({
            "area": area, "tabela": f"{esquema}.{nome}", "mb": _mb(tudo),
            "dados_mb": _mb(dados), "indices_mb": _mb(indices),
            # o que sobra é texto grande guardado à parte (o "TOAST")
            "textos_mb": _mb(max(0, tudo - int(dados or 0) - int(indices or 0))),
            "linhas": vivas, "mortas": mortas, "morto_mb": _mb(morto),
            "limpeza": limpeza,
        })
    ordem = sorted(areas.values(), key=lambda x: -x["bytes"])
    tabelas_bytes = sum(a["bytes"] for a in ordem)
    for a in ordem:
        a["mb"] = _mb(a["bytes"])
        a["por_cento"] = round(100 * a["bytes"] / total_bytes) if total_bytes else 0
    return {"total_mb": _mb(total_bytes), "areas": ordem,
            # O que o banco tem além das tabelas (catálogo, arquivos internos).
            "outros_mb": _mb(max(0, total_bytes - tabelas_bytes)),
            "morto_mb": _mb(morto_estimado),
            "tabelas": tabelas[:QUANTAS_TABELAS]}
