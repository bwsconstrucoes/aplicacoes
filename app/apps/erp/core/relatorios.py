# ============================================================================
# ERP — core/relatorios.py
# Relatórios do financeiro. Consultas agregadas direto no banco (nada de
# carregar milhares de títulos em memória — a instância tem 2 GB e divide o
# processo com os outros módulos do monorepo).
#
# Dimensões: grupo do plano, conta, obra, credor, competência, situação.
# Regime: COMPETÊNCIA (pela competência do título) ou CAIXA (pela data do
# pagamento) — a diferença que costuma gerar discussão no fechamento.
# ============================================================================
from __future__ import annotations

from datetime import date
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

DIMENSOES = {
    "grupo": ("c.grupo_codigo || ' · ' || COALESCE(c.grupo_nome,'Sem grupo')", "Grupo"),
    "subgrupo": ("COALESCE(c.subgrupo_codigo,'') || ' ' || COALESCE(c.subgrupo_nome,'')", "Subgrupo"),
    "categoria": ("c.codigo || ' · ' || c.descricao", "Conta"),
    "obra": ("o.codigo || ' · ' || o.nome", "Obra"),
    "credor": ("f.razao_social", "Credor"),
    "competencia": ("to_char(t.competencia, 'YYYY-MM')", "Competência"),
    "situacao": ("t.status::text", "Situação"),
    "natureza": ("c.natureza", "Natureza"),
    "dedutibilidade": ("t.dedutibilidade::text", "Dedutibilidade"),
}

_ATIVOS = "('EM_ANALISE','AGUARDANDO_APROVACAO','APROVADO','BLOQUEADO','PAGO_PARCIAL','PAGO')"


def _filtros(f: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Monta o WHERE. Sempre por parâmetro — nunca interpolando valor."""
    cond = [f"t.status IN {_ATIVOS}"]
    p: dict[str, Any] = {}
    if f.get("competencia_de"):
        cond.append("t.competencia >= :comp_de")
        p["comp_de"] = date.fromisoformat(f["competencia_de"] + "-01")
    if f.get("competencia_ate"):
        cond.append("t.competencia <= :comp_ate")
        p["comp_ate"] = date.fromisoformat(f["competencia_ate"] + "-01")
    if f.get("obra_id"):
        cond.append("r.obra_id = :obra_id")
        p["obra_id"] = int(f["obra_id"])
    if f.get("grupo"):
        cond.append("c.grupo_codigo = :grupo")
        p["grupo"] = str(f["grupo"])
    if f.get("credor_id"):
        cond.append("t.fornecedor_id = :credor_id")
        p["credor_id"] = int(f["credor_id"])
    if f.get("natureza"):
        cond.append("c.natureza = :natureza")
        p["natureza"] = str(f["natureza"])
    if f.get("apenas_pagos"):
        cond.append("t.status = 'PAGO'")
    return " AND ".join(cond), p


def resumo(s: Session, dimensao: str, filtros: dict[str, Any]) -> dict[str, Any]:
    """Total por dimensão, rateado por obra (o valor de cada título é
    distribuído pelos rateios — é assim que 'custo por obra' fecha)."""
    if dimensao not in DIMENSOES:
        raise ValueError(f"Dimensão inválida: {dimensao}")
    expr, rotulo = DIMENSOES[dimensao]
    where, params = _filtros(filtros)

    sql = text(f"""
        SELECT {expr} AS chave,
               COUNT(DISTINCT t.id) AS titulos,
               SUM(r.valor)         AS total,
               SUM(CASE WHEN t.status = 'PAGO' THEN r.valor ELSE 0 END) AS pago,
               SUM(CASE WHEN t.status <> 'PAGO' THEN r.valor ELSE 0 END) AS aberto
          FROM titulos t
          JOIN rateios r     ON r.titulo_id = t.id
          JOIN categorias c  ON c.id = t.categoria_id
          JOIN obras o       ON o.id = r.obra_id
          JOIN fornecedores f ON f.id = t.fornecedor_id
         WHERE {where}
         GROUP BY chave
         ORDER BY total DESC
    """)
    linhas = [{"chave": r[0] or "—", "titulos": r[1],
               "total": float(r[2] or 0), "pago": float(r[3] or 0),
               "aberto": float(r[4] or 0)} for r in s.execute(sql, params)]
    total = sum(l["total"] for l in linhas)
    for l in linhas:
        l["percentual"] = round(l["total"] / total * 100, 2) if total else 0.0
    return {"dimensao": dimensao, "rotulo": rotulo, "linhas": linhas,
            "total": round(total, 2),
            "total_pago": round(sum(l["pago"] for l in linhas), 2),
            "total_aberto": round(sum(l["aberto"] for l in linhas), 2)}


def analitico(s: Session, filtros: dict[str, Any], limite: int = 2000) -> list[dict[str, Any]]:
    """Lista os títulos por trás dos números — o detalhamento que o contador
    e a auditoria pedem."""
    where, params = _filtros(filtros)
    params["limite"] = limite
    sql = text(f"""
        SELECT t.numero_sp, t.descricao, f.razao_social,
               c.codigo || ' · ' || c.descricao AS conta,
               c.grupo_codigo || ' · ' || COALESCE(c.grupo_nome,'') AS grupo,
               o.codigo AS obra, r.valor, to_char(t.competencia,'MM/YYYY') AS competencia,
               t.status::text, t.dedutibilidade::text, c.natureza,
               (SELECT MIN(p.vencimento) FROM parcelas p WHERE p.titulo_id = t.id) AS vencimento,
               (SELECT MAX(pg.data_pagamento) FROM pagamentos pg
                  JOIN parcelas p2 ON p2.id = pg.parcela_id
                 WHERE p2.titulo_id = t.id) AS pagamento
          FROM titulos t
          JOIN rateios r     ON r.titulo_id = t.id
          JOIN categorias c  ON c.id = t.categoria_id
          JOIN obras o       ON o.id = r.obra_id
          JOIN fornecedores f ON f.id = t.fornecedor_id
         WHERE {where}
         ORDER BY t.competencia DESC, t.numero_sp
         LIMIT :limite
    """)
    return [{"numero_sp": r[0], "descricao": r[1], "credor": r[2], "conta": r[3],
             "grupo": r[4], "obra": r[5], "valor": float(r[6] or 0),
             "competencia": r[7], "situacao": r[8], "dedutibilidade": r[9],
             "natureza": r[10],
             "vencimento": r[11].isoformat() if r[11] else None,
             "pagamento": r[12].isoformat() if r[12] else None}
            for r in s.execute(sql, params)]


def dre_gerencial(s: Session, filtros: dict[str, Any]) -> dict[str, Any]:
    """Resultado do período: só contas de natureza RESULTADO, na ordem do
    plano. Contas de FLUXO aparecem à parte, porque não são resultado."""
    where, params = _filtros(filtros)
    sql = text(f"""
        SELECT c.natureza, c.grupo_codigo,
               COALESCE(c.grupo_nome,'Sem grupo') AS grupo_nome,
               COALESCE(c.subgrupo_codigo,'') AS sub_cod,
               COALESCE(c.subgrupo_nome,'') AS sub_nome,
               SUM(r.valor) AS total
          FROM titulos t
          JOIN rateios r    ON r.titulo_id = t.id
          JOIN categorias c ON c.id = t.categoria_id
          JOIN obras o      ON o.id = r.obra_id
          JOIN fornecedores f ON f.id = t.fornecedor_id
         WHERE {where}
         GROUP BY c.natureza, c.grupo_codigo, grupo_nome, sub_cod, sub_nome
         ORDER BY c.grupo_codigo, sub_cod
    """)
    resultado: dict[str, Any] = {}
    fluxo: dict[str, Any] = {}
    for nat, gcod, gnome, scod, snome, total in s.execute(sql, params):
        destino = resultado if nat == "RESULTADO" else fluxo
        g = destino.setdefault(gcod or "0", {"codigo": gcod, "nome": gnome,
                                             "total": 0.0, "subgrupos": []})
        g["subgrupos"].append({"codigo": scod, "nome": snome, "total": float(total or 0)})
        g["total"] += float(total or 0)

    def _ordenar(d):
        return sorted(d.values(), key=lambda g: g["codigo"] or "")

    grupos_result = _ordenar(resultado)
    receitas = sum(g["total"] for g in grupos_result if (g["codigo"] or "").startswith("1"))
    custos = sum(g["total"] for g in grupos_result if (g["codigo"] or "") in ("2", "3"))
    despesas = sum(g["total"] for g in grupos_result if (g["codigo"] or "") in ("4", "5", "6", "7"))
    return {
        "resultado": grupos_result, "fluxo": _ordenar(fluxo),
        "receitas": round(receitas, 2), "custos": round(custos, 2),
        "despesas": round(despesas, 2),
        "resultado_periodo": round(receitas - custos - despesas, 2),
    }


def para_csv(linhas: list[dict[str, Any]], colunas: list[tuple[str, str]]) -> str:
    """CSV com ponto e vírgula e vírgula decimal — abre direto no Excel BR."""
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow([rotulo for _, rotulo in colunas])
    for l in linhas:
        linha = []
        for chave, _ in colunas:
            v = l.get(chave, "")
            linha.append(f"{v:.2f}".replace(".", ",") if isinstance(v, float) else v)
        w.writerow(linha)
    return buf.getvalue()
