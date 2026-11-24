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
    # Além do resultado, o que o dono pediu em 24/11/2026: "o custo com
    # despesa com pessoal, mês a mês, bem como o caixa da obra". O PESSOAL é
    # parte da despesa (não soma de novo) — é a régua do rateio, e por isso
    # interessa ver ao lado da estrutura que a obra absorveu. O CAIXA é o
    # mesmo da tela de Evolução: o que a obra recebeu menos o que pagou no mês,
    # e o acumulado já descontada a estrutura — é esse acumulado que diz se
    # ela estava no vermelho, e é por ele que os juros são repartidos.
    apurado_do_mes = {l["mes"]: l for l in calculo.get("apurado") or () if l["obra"] == obra}
    pessoal_do_mes: dict[str, float] = {}
    for mes, nome, valor in calculo.get("pessoal") or ():
        if nome == obra:
            pessoal_do_mes[mes] = pessoal_do_mes.get(mes, 0.0) - abs(float(valor or 0))
    caixa_do_mes: dict[str, float] = {}
    for mes, nome, valor in calculo.get("caixa") or ():
        if nome == obra:
            caixa_do_mes[mes] = caixa_do_mes.get(mes, 0.0) + float(valor or 0)

    meses, acumulado, caixa_acumulado = [], 0.0, 0.0
    vazio = {"receita_liquida": 0.0, "despesas": 0.0, "resultado_direto": 0.0,
             "rateio": 0.0, "juros": 0.0, "resultado": 0.0}
    for mes in sorted(set(apurado_do_mes) | set(pessoal_do_mes) | set(caixa_do_mes)):
        l = apurado_do_mes.get(mes, vazio)
        acumulado += l["resultado"]
        caixa = caixa_do_mes.get(mes, 0.0)
        caixa_acumulado += caixa + l["rateio"]
        meses.append({
            "mes": mes, "rotulo": _rotulo(mes),
            "receita": l["receita_liquida"], "despesa": l["despesas"],
            "pessoal": pessoal_do_mes.get(mes, 0.0),
            "direto": l["resultado_direto"], "estrutura": l["rateio"],
            "juros": l["juros"], "resultado": l["resultado"], "acumulado": acumulado,
            "caixa": caixa, "caixa_acumulado": caixa_acumulado,
        })
    total_pessoal = sum(m["pessoal"] for m in meses)
    total_caixa = sum(m["caixa"] for m in meses)

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
        "total_pessoal": total_pessoal, "total_caixa": total_caixa,
        "caixa_final": caixa_acumulado,
        "contas": contas, "total_pago": total_pago, "total_recebido": total_recebido,
        "conferencia": conferencia, "bate_com_o_dre": all(c["bate"] for c in conferencia),
    }


# ---------------------------------------------------------------------------
# O relatório da obra: as mesmas quatro partes, para a planilha e para o PDF
# ---------------------------------------------------------------------------
COLUNAS_DO_ANALITICO = [
    ("data", "Data"), ("tipo", "Tipo"), ("quem", "Cliente ou fornecedor"),
    ("grupo", "Grupo"), ("categoria", "Categoria"), ("documento", "Documento"),
    ("conta", "Conta corrente"), ("situacao", "Situação"), ("observacao", "Observação"),
    ("codigo", "Nº no OMIE"), ("valor", "Valor"),
]


def abas(f: dict, lancamentos=None) -> list[tuple]:
    """As abas do relatório da obra. `lancamentos` é o analítico
    (`consultas.lancamentos_da_obra`): quando vem, entra em duas abas no fim —
    receitas e despesas, lançamento a lançamento."""
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
                      "despesa": n["despesas"], "pessoal": f["total_pessoal"],
                      "direto": n["resultado_direto"],
                      "estrutura": n["rateio"], "juros": n["juros"],
                      "resultado": n["resultado"], "acumulado": n["resultado"],
                      "caixa": f["total_caixa"], "caixa_acumulado": f["caixa_final"]})

    contas = [dict(c, pct=round(c["pct"], 2)) for c in f["contas"]]
    contas.append({"conta": "TOTAL", "pago": f["total_pago"], "pct": 100.0 if contas else "",
                   "pagamentos": sum(c["pagamentos"] for c in f["contas"]),
                   "recebido": f["total_recebido"],
                   "recebimentos": sum(c["recebimentos"] for c in f["contas"])})
    analitico = []
    if lancamentos is not None:
        receitas = [l for l in lancamentos if l["tipo"] != "Despesa"]
        despesas = [l for l in lancamentos if l["tipo"] == "Despesa"]
        for nome, linhas in (("Receitas - analitico", receitas), ("Despesas - analitico", despesas)):
            linhas = list(linhas)
            if linhas:
                # o imposto retido aparece na lista, mas não é receita da obra
                # (o cliente ficou com ele): o total é o da ficha, sem ele
                total = sum(l["valor"] for l in linhas if l["tipo"] != "Retido na fonte")
                linhas.append({"data": "", "tipo": "TOTAL" + (" (sem o retido)" if nome.startswith("Receitas") else ""),
                               "quem": "", "grupo": "", "categoria": "", "documento": "", "conta": "",
                               "situacao": "", "observacao": "", "codigo": "", "valor": total})
            analitico.append((nome, COLUNAS_DO_ANALITICO, linhas))
    return [
        ("Resumo", [("o_que", "Conta"), ("valor", "Valor")], resumo),
        ("Despesas por categoria",
         [("grupo", "Grupo"), ("categoria", "Categoria"), ("valor", "Valor")], categorias),
        ("Mes a mes",
         [("rotulo", "Mês"), ("receita", "Receita líquida"), ("despesa", "Despesa"),
          ("pessoal", "Despesa com pessoal (dentro da despesa)"),
          ("direto", "Resultado direto"), ("estrutura", "Estrutura apropriada"),
          ("juros", "Juros apropriados"), ("resultado", "Resultado do mês"),
          ("acumulado", "Resultado acumulado"),
          ("caixa", "Caixa do mês"), ("caixa_acumulado", "Caixa acumulado")], meses),
        ("Por conta corrente",
         [("conta", "Conta corrente"), ("pago", "Valor pago"), ("pct", "% dos pagamentos"),
          ("pagamentos", "Pagamentos"), ("recebido", "Valor recebido"),
          ("recebimentos", "Recebimentos")], contas),
        ("Conferencia com o DRE",
         [("o_que", "O que"), ("prestacao", "Valor na prestação"), ("dre", "Valor no DRE"),
          ("diferenca", "Valor da diferença")], f["conferencia"]),
    ] + analitico
