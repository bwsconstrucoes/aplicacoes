# -*- coding: utf-8 -*-
"""
A FICHA DE UMA OBRA — o que está por trás dos números dela na prestação.

Pedido do dono em 24/11/2026:

    "Para cada obra que eu esteja analisando, eu gostaria de um facilitador
    (…) clicar na obra, abrir um modal, aí visualizar lá as despesas, as
    receitas (…) como e quanto mensalmente foi apropriado de custo de
    administração, de juros (…) quanto das despesas de cada obra foram pagas
    em qual conta corrente (…) Pode até ser meio similar à DRE, mas a partir da
    tela da prestação de contas. (…) Até para confrontar, verificar se os
    dados estão batendo: o volume financeiro que eu vejo na DRE bate com o que
    eu vejo na prestação de contas."

Quatro partes: o RESUMO (receita, despesa por grupo e categoria, resultado
direto, estrutura, juros, resultado final e a parte de cada sócio), o MÊS A
MÊS, a CONTA CORRENTE e a CONFERÊNCIA COM O DRE.

Os totais vêm do cálculo do cenário (`por_obra`, `apurado`, `quotas`); o
detalhe por categoria e por conta vem de `consultas.detalhe_da_obra`, que usa a
mesma régua da apuração. A ficha confere uma coisa contra a outra e diz quando
não fecha — é para isso que ela existe.

Sem banco e sem Flask: recebe dicionários, devolve dicionários.
"""
from __future__ import annotations

TOLERANCIA = 0.05          # centavos de arredondamento entre duas somas da mesma coisa


def _rotulo(mes: str) -> str:
    ano, _, m = (mes or "").partition("-")
    return f"{m}/{ano}" if m and ano.isdigit() else (mes or "")


def montar(calculo: dict, obra: str, detalhe: dict, dre: dict, corte: str = "") -> dict | None:
    """A ficha da obra, ou None se ela não está nesta prestação.

    `dre` é o que a tela do DRE mostra para a mesma obra, na mesma medida do
    cenário: {"receita", "despesa", "resultado"}."""
    n = (calculo.get("por_obra") or {}).get(obra)
    if n is None:
        return None
    info = (calculo.get("obras") or {}).get(obra) or {}

    # --- as despesas, por grupo e categoria -------------------------------
    grupos: dict[str, dict] = {}
    for d in detalhe.get("despesas") or ():
        g = grupos.setdefault(d["grupo"], {"grupo": d["grupo"], "valor": 0.0, "categorias": []})
        g["valor"] += d["valor"]
        g["categorias"].append({"categoria": d["categoria"], "valor": d["valor"]})
    despesas = sorted(grupos.values(), key=lambda g: g["valor"])
    soma_das_despesas = sum(g["valor"] for g in despesas)

    receitas = [r for r in detalhe.get("receitas") or () if not r["retido"]]
    retidas = [r for r in detalhe.get("receitas") or () if r["retido"]]

    # --- quem divide -------------------------------------------------------
    socios = [{"socio": q["socio"], "tipo": q.get("tipo") or "Interno",
               "pct": float(q["pct"]), "quota": float(q["quota"]),
               "direto": float(q.get("parte_direto", 0.0))}
              for q in calculo.get("quotas") or () if q["obra"] == obra]

    # --- mês a mês ---------------------------------------------------------
    meses, acumulado = [], 0.0
    for l in sorted((l for l in calculo.get("apurado") or () if l["obra"] == obra),
                    key=lambda l: l["mes"]):
        acumulado += l["resultado"]
        meses.append({
            "mes": l["mes"], "rotulo": _rotulo(l["mes"]),
            "receita": l["receita_liquida"], "despesa": l["despesas"],
            "direto": l["resultado_direto"], "estrutura": l["rateio"],
            "juros": l["juros"], "resultado": l["resultado"], "acumulado": acumulado,
        })

    # --- por conta corrente -------------------------------------------------
    contas = list(detalhe.get("contas") or ())
    total_pago = sum(c["pago"] for c in contas)
    total_recebido = sum(c["recebido"] for c in contas)
    for c in contas:
        c["pct"] = abs(c["pago"] / total_pago * 100) if abs(total_pago) > 0.005 else 0.0

    # --- a conferência com o DRE --------------------------------------------
    conferencia = []
    for titulo, daqui, de_la in (
            ("Receita líquida", n["receita_liquida"], dre.get("receita", 0.0)),
            ("Despesas", n["despesas"], dre.get("despesa", 0.0)),
            ("Resultado direto", n["resultado_direto"], dre.get("resultado", 0.0))):
        diferenca = float(daqui) - float(de_la)
        conferencia.append({"o_que": titulo, "prestacao": float(daqui), "dre": float(de_la),
                            "diferenca": diferenca, "bate": abs(diferenca) <= TOLERANCIA})

    return {
        "obra": obra, "projeto": (info.get("projeto") or "").strip(),
        "corte": corte, "corte_rotulo": _rotulo(corte),
        "n": {c: float(n.get(c, 0.0)) for c in (
            "receita_bruta", "retencoes", "receita_liquida", "despesas",
            "resultado_direto", "rateio", "juros", "resultado")},
        "receitas": receitas, "retidas": retidas, "despesas": despesas,
        # a soma das categorias tem de dar a despesa da prestação
        "despesas_fecham": abs(soma_das_despesas - float(n.get("despesas", 0.0))) <= TOLERANCIA,
        "soma_das_despesas": soma_das_despesas,
        "socios": socios, "meses": meses,
        "contas": contas, "total_pago": total_pago, "total_recebido": total_recebido,
        "conferencia": conferencia, "bate_com_o_dre": all(c["bate"] for c in conferencia),
    }


# ---------------------------------------------------------------------------
# O relatório da obra: as mesmas quatro partes, para a planilha e para o PDF
# ---------------------------------------------------------------------------
def abas(f: dict) -> list[tuple]:
    n = f["n"]
    resumo = [{"o_que": "Receita bruta", "valor": n["receita_bruta"]},
              {"o_que": "(-) Retido na fonte", "valor": -n["retencoes"]},
              {"o_que": "Receita líquida", "valor": n["receita_liquida"]}]
    for g in f["despesas"]:
        resumo.append({"o_que": g["grupo"], "valor": g["valor"]})
        for c in g["categorias"]:
            resumo.append({"o_que": "     " + c["categoria"], "valor": c["valor"]})
    resumo += [{"o_que": "RESULTADO DIRETO DA OBRA", "valor": n["resultado_direto"]},
               {"o_que": "Estrutura apropriada (matriz e filial)", "valor": n["rateio"]},
               {"o_que": "Juros apropriados", "valor": n["juros"]},
               {"o_que": "RESULTADO FINAL", "valor": n["resultado"]}]
    for s in f["socios"]:
        resumo.append({"o_que": f"     {s['socio']} ({s['pct']:.2f}%)".replace(".", ","),
                       "valor": s["quota"]})

    categorias = [{"grupo": g["grupo"], "categoria": c["categoria"], "valor": c["valor"]}
                  for g in f["despesas"] for c in g["categorias"]]
    categorias.append({"grupo": "TOTAL", "categoria": "", "valor": f["soma_das_despesas"]})

    meses = [dict(m) for m in f["meses"]]
    if meses:
        meses.append({"rotulo": "TOTAL", "receita": n["receita_liquida"],
                      "despesa": n["despesas"], "direto": n["resultado_direto"],
                      "estrutura": n["rateio"], "juros": n["juros"],
                      "resultado": n["resultado"], "acumulado": n["resultado"]})

    contas = [dict(c, pct=round(c["pct"], 2)) for c in f["contas"]]
    contas.append({"conta": "TOTAL", "pago": f["total_pago"], "pct": 100.0 if contas else "",
                   "pagamentos": sum(c["pagamentos"] for c in f["contas"]),
                   "recebido": f["total_recebido"],
                   "recebimentos": sum(c["recebimentos"] for c in f["contas"])})
    return [
        ("Resumo", [("o_que", "Conta"), ("valor", "Valor")], resumo),
        ("Despesas por categoria",
         [("grupo", "Grupo"), ("categoria", "Categoria"), ("valor", "Valor")], categorias),
        ("Mes a mes",
         [("rotulo", "Mês"), ("receita", "Receita líquida"), ("despesa", "Despesa"),
          ("direto", "Resultado direto"), ("estrutura", "Estrutura apropriada"),
          ("juros", "Juros apropriados"), ("resultado", "Resultado do mês"),
          ("acumulado", "Acumulado")], meses),
        ("Por conta corrente",
         [("conta", "Conta corrente"), ("pago", "Valor pago"), ("pct", "% dos pagamentos"),
          ("pagamentos", "Pagamentos"), ("recebido", "Valor recebido"),
          ("recebimentos", "Recebimentos")], contas),
        ("Conferencia com o DRE",
         [("o_que", "O que"), ("prestacao", "Valor na prestação"), ("dre", "Valor no DRE"),
          ("diferenca", "Valor da diferença")], f["conferencia"]),
    ]
