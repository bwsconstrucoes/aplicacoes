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

from app.apps.erp.core.auth.permissoes import condicao_escopo_sql
from app.apps.erp.db.models.cadastros import Usuario

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
    # CONSOLIDADO POR EMPRESA (12/09/2026). A BWS opera com mais de um CNPJ, e
    # cada obra aponta para a empresa que a executa — era a única visão que os
    # relatórios não davam.
    "empresa": ("COALESCE(e.nome_fantasia, e.razao_social, 'Sem empresa')", "Empresa"),
    # SOMADO POR PROJETO (13/09/2026). Pedido do dono: *"se a obra estiver
    # dentro de algum projeto, tudo que eu for visualizar em relação a elas —
    # relatórios, resultados, custos — eu poder visualizar o projeto, ou seja,
    # o somatório daquelas obras"*. Obra fora de projeto cai numa linha só,
    # com esse nome, em vez de sumir — número que some é pior que número
    # errado, porque ninguém procura o que não sabe que falta.
    "projeto": ("COALESCE(pr.codigo || ' · ' || pr.nome, 'Sem projeto')", "Projeto"),
    # ESPÉCIE existe como dimensão para o caso "os dois": sem ela, a linha de
    # receita e a de custo se somariam num número só.
    "especie": ("CASE t.especie::text WHEN 'RECEBER' THEN 'A receber' "
                "ELSE 'A pagar' END", "Espécie"),
}

# ESPÉCIE — a separação que faltava, e que era um erro de verdade.
#
# Títulos A RECEBER (medição de obra) moram na MESMA tabela dos títulos a
# pagar, com rateio por obra igual. Os relatórios nunca filtraram por espécie:
# "totais por obra" somava o que a obra vai RECEBER com o que ela CUSTOU, num
# número positivo só. Quem lesse não teria como desconfiar.
#
# O DRE sempre esteve certo, porque ele separa pela conta do plano (grupo 1 é
# receita). O erro estava no resumo por dimensão e no analítico.
#
# Achado em 12/09/2026, ao revisar os relatórios a pedido do dono.
ESPECIES = {
    "pagar": "t.especie::text = 'PAGAR'",
    "receber": "t.especie::text = 'RECEBER'",
    "tudo": "",
}

_ATIVOS = "('EM_ANALISE','AGUARDANDO_APROVACAO','APROVADO','BLOQUEADO','PAGO_PARCIAL','PAGO')"

# Conta REDUTORA abate o custo em vez de somar (devolução de material, estorno,
# reembolso de custas). Ela entra nos totais com sinal negativo: R$ 10.000 de
# compra e R$ 500 de devolução fecham em R$ 9.500 de custo, com as duas linhas
# visíveis no analítico. Ver migração 058 e o princípio 7 do plano padrão.
_VALOR = "(CASE WHEN c.redutora THEN -r.valor ELSE r.valor END)"

# Quanto do título já saiu do caixa, rateado pela linha. Antes o relatório
# dizia "pago" ou "em aberto" olhando a SITUAÇÃO do título — e um título com
# duas parcelas, uma paga, aparecia com o valor INTEIRO em aberto e zero pago.
# Agora a conta é a soma dos pagamentos de verdade, distribuída na mesma
# proporção do rateio. Corrigido em 11/09/2026.
# O COALESCE de fora é para o caso improvável de líquido zero: sem ele a
# divisão daria nulo, e a linha sumiria CALADA da coluna "em aberto" também.
_PAGO = ("COALESCE({valor} * COALESCE(pgs.pago, 0) / NULLIF(t.valor_liquido, 0), 0)"
         .format(valor=_VALOR))
_ABERTO = f"({_VALOR} - {_PAGO})"
_JOIN_PAGO = """
          LEFT JOIN LATERAL (
              SELECT SUM(pg.valor_pago) AS pago
                FROM pagamentos pg
                JOIN parcelas p2 ON p2.id = pg.parcela_id
               WHERE p2.titulo_id = t.id
          ) pgs ON TRUE"""


def _filtros(f: dict[str, Any], s: Session,
             usuario: Usuario) -> tuple[str, dict[str, Any]]:
    """Monta o WHERE. Sempre por parâmetro — nunca interpolando valor.

    `usuario` é OBRIGATÓRIO de propósito: com valor padrão, esquecer de passar
    devolveria a empresa inteira em silêncio — que foi exatamente a falha
    corrigida em 11/09/2026. Sem usuário, o relatório nem roda.
    """
    if usuario is None:
        raise ValueError("Relatório exige o usuário — o recorte por obra "
                         "depende de quem está perguntando.")
    cond = [f"t.status IN {_ATIVOS}"]
    p: dict[str, Any] = {}
    onde, params = condicao_escopo_sql(s, usuario)
    cond.append(onde)
    p.update(params)
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
    if f.get("empresa_id"):
        cond.append("o.empresa_id = :empresa_id")
        p["empresa_id"] = int(f["empresa_id"])
    if f.get("projeto_id"):
        cond.append("o.projeto_id = :projeto_id")
        p["projeto_id"] = int(f["projeto_id"])
    # A espécie é escolha EXPLÍCITA da tela, e o padrão é "a pagar": este é um
    # relatório de custo. Quem quer os dois pede os dois, e aí a tela mostra
    # qual foi a escolha — nada acontece calado.
    especie = str(f.get("especie") or "pagar").lower()
    if especie not in ESPECIES:
        raise ValueError(f"Espécie inválida: {especie}")
    if ESPECIES[especie]:
        cond.append(ESPECIES[especie])
    return " AND ".join(cond), p


def resumo(s: Session, dimensao: str, filtros: dict[str, Any],
           usuario: Usuario) -> dict[str, Any]:
    """Total por dimensão, rateado por obra (o valor de cada título é
    distribuído pelos rateios — é assim que 'custo por obra' fecha)."""
    if dimensao not in DIMENSOES:
        raise ValueError(f"Dimensão inválida: {dimensao}")
    expr, rotulo = DIMENSOES[dimensao]
    where, params = _filtros(filtros, s, usuario)

    sql = text(f"""
        SELECT {expr} AS chave,
               COUNT(DISTINCT t.id) AS titulos,
               SUM({_VALOR})       AS total,
               SUM({_PAGO})        AS pago,
               SUM({_ABERTO})      AS aberto
          FROM titulos t
          JOIN rateios r     ON r.titulo_id = t.id
          JOIN categorias c  ON c.id = COALESCE(r.categoria_id, t.categoria_id)
          JOIN obras o       ON o.id = r.obra_id
          LEFT JOIN empresas e ON e.id = o.empresa_id
          LEFT JOIN projetos pr ON pr.id = o.projeto_id
          JOIN fornecedores f ON f.id = t.fornecedor_id{_JOIN_PAGO}
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


def analitico(s: Session, filtros: dict[str, Any], usuario: Usuario,
              limite: int = 2000) -> list[dict[str, Any]]:
    """Lista os títulos por trás dos números — o detalhamento que o contador
    e a auditoria pedem."""
    where, params = _filtros(filtros, s, usuario)
    params["limite"] = limite
    sql = text(f"""
        SELECT t.numero_sp, t.descricao, f.razao_social,
               c.codigo || ' · ' || c.descricao AS conta,
               c.grupo_codigo || ' · ' || COALESCE(c.grupo_nome,'') AS grupo,
               o.codigo AS obra, {_VALOR} AS valor,
               to_char(t.competencia,'MM/YYYY') AS competencia,
               t.status::text, t.dedutibilidade::text, c.natureza,
               (SELECT MIN(p.vencimento) FROM parcelas p WHERE p.titulo_id = t.id) AS vencimento,
               (SELECT MAX(pg.data_pagamento) FROM pagamentos pg
                  JOIN parcelas p2 ON p2.id = pg.parcela_id
                 WHERE p2.titulo_id = t.id) AS pagamento
          FROM titulos t
          JOIN rateios r     ON r.titulo_id = t.id
          JOIN categorias c  ON c.id = COALESCE(r.categoria_id, t.categoria_id)
          JOIN obras o       ON o.id = r.obra_id
          LEFT JOIN empresas e ON e.id = o.empresa_id
          LEFT JOIN projetos pr ON pr.id = o.projeto_id
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


def dre_gerencial(s: Session, filtros: dict[str, Any],
                  usuario: Usuario) -> dict[str, Any]:
    """Resultado do período: só contas de natureza RESULTADO, na ordem do
    plano. Contas de FLUXO aparecem à parte, porque não são resultado."""
    where, params = _filtros(filtros, s, usuario)
    sql = text(f"""
        SELECT c.natureza, c.grupo_codigo,
               COALESCE(c.grupo_nome,'Sem grupo') AS grupo_nome,
               COALESCE(c.subgrupo_codigo,'') AS sub_cod,
               COALESCE(c.subgrupo_nome,'') AS sub_nome,
               SUM({_VALOR}) AS total
          FROM titulos t
          JOIN rateios r    ON r.titulo_id = t.id
          JOIN categorias c ON c.id = COALESCE(r.categoria_id, t.categoria_id)
          JOIN obras o      ON o.id = r.obra_id
          LEFT JOIN empresas e ON e.id = o.empresa_id
          LEFT JOIN projetos pr ON pr.id = o.projeto_id
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
    # O grupo 8 (aquisição de bens) virou RESULTADO em 10/09/2026 — o bem
    # comprado para uma obra precisa aparecer no custo dela. Sem entrar aqui,
    # ele apareceria na lista de grupos e sumiria do resultado do período.
    despesas = sum(g["total"] for g in grupos_result
                   if (g["codigo"] or "") in ("4", "5", "6", "7", "8"))
    return {
        "resultado": grupos_result, "fluxo": _ordenar(fluxo),
        "receitas": round(receitas, 2), "custos": round(custos, 2),
        "despesas": round(despesas, 2),
        "resultado_periodo": round(receitas - custos - despesas, 2),
    }


# ---------------------------------------------------------------------------
# CURVA ABC — onde o dinheiro realmente está
#
# Pedido do dono em 12/09/2026. A ideia é velha e continua valendo: numa
# construtora, uns poucos fornecedores (ou umas poucas contas) respondem pela
# maior parte do gasto. Negociar com esses vale dez vezes mais que economizar
# no resto.
#
# A régua é a usual: ordena do maior para o menor, acumula o percentual, e
# corta em 80% (A) e 95% (B). Não é lei — é convenção de mercado, e está
# escrita aqui para quem ler o relatório saber de onde veio o corte.
#
# Reusa o `resumo`, de propósito: a classificação é uma leitura por cima do
# mesmo número. Duas consultas diferentes divergiriam no dia em que alguém
# corrigisse uma.
# ---------------------------------------------------------------------------
CORTE_A = 80.0
CORTE_B = 95.0


def curva_abc(s: Session, dimensao: str, filtros: dict[str, Any],
              usuario: Usuario) -> dict[str, Any]:
    """O mesmo total por dimensão, ordenado e classificado em A, B e C."""
    base = resumo(s, dimensao, filtros, usuario)
    acumulado = 0.0
    total = base["total"] or 0.0
    for i, linha in enumerate(base["linhas"], start=1):
        # A CLASSE OLHA O ACUMULADO **ANTES** DESTA LINHA, e o item que CRUZA o
        # corte fica na classe de baixo. Parece detalhe e não é: com um
        # fornecedor respondendo por 83% do gasto, olhar o acumulado DEPOIS o
        # jogaria para a classe B — e a classe A ficaria vazia, no caso em que
        # ela é mais óbvia. Quem cruza os 80% é justamente quem os alcançou.
        antes = (acumulado / total * 100) if total else 0.0
        linha["classe"] = ("A" if antes < CORTE_A
                           else "B" if antes < CORTE_B else "C")
        acumulado += linha["total"]
        linha["ordem"] = i
        linha["acumulado"] = round(acumulado, 2)
        linha["acumulado_pct"] = round((acumulado / total * 100)
                                       if total else 0.0, 2)
    resumo_classes = {}
    for linha in base["linhas"]:
        c = resumo_classes.setdefault(linha["classe"],
                                      {"classe": linha["classe"], "quantos": 0,
                                       "total": 0.0})
        c["quantos"] += 1
        c["total"] += linha["total"]
    for c in resumo_classes.values():
        c["total"] = round(c["total"], 2)
        c["percentual"] = round(c["total"] / total * 100, 2) if total else 0.0
    base["classes"] = [resumo_classes[k] for k in ("A", "B", "C")
                       if k in resumo_classes]
    base["corte_a"] = CORTE_A
    base["corte_b"] = CORTE_B
    return base


# ---------------------------------------------------------------------------
# FLUXO DE CAIXA PROJETADO — o que entra e o que sai, semana a semana
#
# Pedido do dono em 12/09/2026. É o relatório que uma construtora olha
# primeiro, e o único que a gente não tinha: os outros contam o que JÁ
# aconteceu.
#
# O QUE ELE CONTA, dito com todas as letras na tela: PARCELA EM ABERTO, pela
# data de VENCIMENTO. Não é competência e não é o que já foi pago — é a
# previsão de caixa. Título cancelado, estornado e parcela paga ficam de fora,
# porque já não movimentam nada.
#
# O SALDO ACUMULADO começa em ZERO, a menos que a pessoa informe o saldo de
# hoje. O sistema NÃO sabe o saldo do banco: ele conhece o extrato importado,
# que é histórico, não saldo. Inventar um saldo seria o pior desfecho — o
# acumulado pareceria conta bancária e não seria. Por isso o campo é da
# pessoa, e a tela diz o que o número significa.
# ---------------------------------------------------------------------------
def fluxo_de_caixa(s: Session, filtros: dict[str, Any], usuario: Usuario, *,
                   periodos: int = 13, passo: str = "semana",
                   saldo_inicial: float = 0.0) -> dict[str, Any]:
    """Entradas e saídas previstas por semana (ou mês), com acumulado."""
    if usuario is None:
        raise ValueError("Relatório exige o usuário — o recorte por obra "
                         "depende de quem está perguntando.")
    if passo not in ("semana", "mes"):
        raise ValueError("O passo do fluxo é 'semana' ou 'mes'.")
    periodos = max(1, min(int(periodos or 13), 53))

    onde_escopo, params = condicao_escopo_sql(s, usuario)
    cond = [onde_escopo,
            "t.status NOT IN ('CANCELADO','ESTORNADO','DEVOLVIDO','RASCUNHO')",
            "p.status IN ('ABERTA','AGENDADA')"]
    if filtros.get("obra_id"):
        cond.append("r.obra_id = :obra_id")
        params["obra_id"] = int(filtros["obra_id"])
    if filtros.get("empresa_id"):
        cond.append("o.empresa_id = :empresa_id")
        params["empresa_id"] = int(filtros["empresa_id"])

    trunc = "week" if passo == "semana" else "month"
    params["periodos"] = periodos
    # O rateio distribui a parcela pela obra — é o que permite filtrar por obra
    # sem que a parcela inteira apareça na obra errada. Sem rateio a parcela
    # entraria uma vez por obra do título, e o total dobraria.
    sql = text(f"""
        SELECT date_trunc('{trunc}', p.vencimento)::date AS quando,
               t.especie::text AS especie,
               SUM(p.valor * COALESCE(r.percentual, 100) / 100.0) AS valor
          FROM parcelas p
          JOIN titulos t ON t.id = p.titulo_id
          JOIN rateios r ON r.titulo_id = t.id
          JOIN obras o   ON o.id = r.obra_id
          JOIN categorias c ON c.id = COALESCE(r.categoria_id, t.categoria_id)
          JOIN fornecedores f ON f.id = t.fornecedor_id
         WHERE {" AND ".join(cond)}
           AND p.vencimento >= date_trunc('{trunc}', CURRENT_DATE)
           AND p.vencimento < date_trunc('{trunc}', CURRENT_DATE)
                              + (:periodos || ' {trunc}')::interval
         GROUP BY quando, especie
         ORDER BY quando
    """)

    por_periodo: dict[Any, dict[str, Any]] = {}
    for quando, especie, valor in s.execute(sql, params):
        linha = por_periodo.setdefault(quando, {"quando": quando.isoformat(),
                                                "entra": 0.0, "sai": 0.0})
        if especie == "RECEBER":
            linha["entra"] += float(valor or 0)
        else:
            linha["sai"] += float(valor or 0)

    acumulado = float(saldo_inicial or 0)
    linhas = []
    for quando in sorted(por_periodo):
        l = por_periodo[quando]
        l["resultado"] = round(l["entra"] - l["sai"], 2)
        acumulado += l["resultado"]
        l["acumulado"] = round(acumulado, 2)
        l["entra"] = round(l["entra"], 2)
        l["sai"] = round(l["sai"], 2)
        linhas.append(l)

    # O ATRASADO entra separado, e não diluído nas semanas: ele já venceu, e
    # misturá-lo na semana atual faria a previsão parecer pior do que é — ou
    # melhor, se alguém o ignorasse.
    sql_atraso = text(f"""
        SELECT t.especie::text,
               SUM(p.valor * COALESCE(r.percentual, 100) / 100.0)
          FROM parcelas p
          JOIN titulos t ON t.id = p.titulo_id
          JOIN rateios r ON r.titulo_id = t.id
          JOIN obras o   ON o.id = r.obra_id
          JOIN categorias c ON c.id = COALESCE(r.categoria_id, t.categoria_id)
          JOIN fornecedores f ON f.id = t.fornecedor_id
         WHERE {" AND ".join(cond)}
           AND p.vencimento < date_trunc('{trunc}', CURRENT_DATE)
         GROUP BY t.especie::text
    """)
    atrasado = {"entra": 0.0, "sai": 0.0}
    for especie, valor in s.execute(sql_atraso, params):
        atrasado["entra" if especie == "RECEBER" else "sai"] += float(valor or 0)

    return {
        "passo": passo, "periodos": periodos,
        "saldo_inicial": round(float(saldo_inicial or 0), 2),
        "linhas": linhas,
        "atrasado": {"entra": round(atrasado["entra"], 2),
                     "sai": round(atrasado["sai"], 2),
                     "resultado": round(atrasado["entra"] - atrasado["sai"], 2)},
        "total_entra": round(sum(l["entra"] for l in linhas), 2),
        "total_sai": round(sum(l["sai"] for l in linhas), 2),
        "saldo_final": round(acumulado, 2),
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
