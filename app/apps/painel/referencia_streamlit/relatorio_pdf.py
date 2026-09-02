"""Geracao do relatorio PDF (graficos + tabelas) do dashboard financeiro BWS.

Usa matplotlib (graficos) e reportlab (documento). Importados de forma tardia
dentro de montar_pdf para nao quebrar o dashboard caso as libs nao estejam instaladas.
"""
import io
import datetime as dt
import pandas as pd

REC = "1. Contas a Receber"
PAG = "2. Contas a Pagar"

# Tabelas grandes sao limitadas para o PDF nao ficar gigante (o completo vai no Excel).
CAP_LINHAS = 2500


def _brl(v):
    try:
        v = float(v)
    except Exception:
        return ""
    s = f"{abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-R$ " if v < 0 else "R$ ") + s


def _brl_short(v):
    a = abs(float(v))
    sign = "-" if v < 0 else ""
    if a >= 1e9:
        return f"{sign}R$ {a/1e9:.1f} bi".replace(".", ",")
    if a >= 1e6:
        return f"{sign}R$ {a/1e6:.1f} mi".replace(".", ",")
    if a >= 1e3:
        return f"{sign}R$ {a/1e3:.0f} mil"
    return f"{sign}R$ {a:.0f}"


def _trunc(s, n):
    s = str(s) if s is not None else ""
    return s if len(s) <= n else s[: n - 1] + "\u2026"


def construir_medicoes(rec_df):
    d = rec_df.copy()
    doc = d["Número do Documento"].astype(str).str.strip()
    obs = d["Observação da Conta"].astype(str).str.strip()
    valido = doc.str.upper().ne("N/D") & ~doc.isin(["", "nan", "None", "NaN"])
    chave = ("DOC:" + doc).where(valido, "OBS:" + obs)
    sem = (~valido) & obs.isin(["", "nan", "None", "NaN"])
    chave = chave.mask(sem, "ROW:" + d.index.astype(str))
    d["_chave"] = chave.values
    base = ~d["IsRetido"]
    pago = d["Pago"]
    d["_rec"] = d["PagoRecebido"].where(base & pago, 0.0)
    d["_ret"] = d["PagoRecebido"].where(d["IsRetido"] & pago, 0.0)
    d["_arl"] = d["APagarReceber"].where(base, 0.0)
    d["_rfut"] = d["PagoRecebido"].where(d["IsRetido"] & ~pago, 0.0)
    g = d.groupby("_chave").agg(
        Recebido=("_rec", "sum"), Retido=("_ret", "sum"),
        ARecLiq=("_arl", "sum"), RetFut=("_rfut", "sum"),
        Cliente=("Cliente ou Fornecedor (Razão Social)", "first"),
        Departamento=("Departamento", "first"),
        Data=("Data", "max"),
    ).reset_index(drop=True)
    g["AReceber"] = g["ARecLiq"] + g["RetFut"]
    g["Bruto"] = g["Recebido"] + g["Retido"] + g["AReceber"]
    g["SitSimpl"] = "A Receber"
    g.loc[(g["Recebido"].abs() > 0.005) & (g["AReceber"].abs() <= 0.005), "SitSimpl"] = "Recebido"
    g.loc[(g["Recebido"].abs() > 0.005) & (g["AReceber"].abs() > 0.005), "SitSimpl"] = "Recebido Parcialmente"
    return g


def split_receitas(rec):
    is_obra = rec["Categoria"].astype(str).str.strip().eq("Receita de Obras") | rec["IsRetido"]
    return rec[is_obra].copy(), rec[~is_obra].copy()


def _chart_mensal(base, titulo, comprometido=False):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import FuncFormatter

    if comprometido:
        pm = base[base["Data"].notna()].copy()
        vc = "Comprometido"
    else:
        pm = base[base["Pago"]].copy()
        vc = "PagoRecebido"
    if pm.empty:
        return None
    pm["AnoMes"] = pm["Data"].dt.to_period("M").astype(str)
    recm = pm[(pm["Tipo"] == REC) & (~pm["IsRetido"])].groupby("AnoMes")[vc].sum()
    despm = pm[pm["Tipo"] == PAG].groupby("AnoMes")[vc].sum()
    mr = pd.concat([recm.rename("R"), despm.rename("D")], axis=1).fillna(0.0).sort_index()
    if mr.empty:
        return None
    mr["Res"] = mr["R"] + mr["D"]
    mr["Ac"] = mr["Res"].cumsum()
    x = pd.to_datetime(mr.index + "-01")
    fig, ax = plt.subplots(figsize=(10.5, 3.5))
    ax.bar(x, mr["R"], width=22, color="#2e7d32", label="Receita liquida")
    ax.bar(x, mr["D"], width=22, color="#c62828", label="Despesa")
    ax.plot(x, mr["Ac"], color="#1565c0", lw=2, marker="o", ms=2.5, label="Resultado acumulado")
    ax.axhline(0, color="#666", lw=0.8)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _brl_short(v)))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=max(1, len(mr) // 12)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))
    ax.set_title(titulo, fontsize=11, fontweight="bold")
    ax.legend(fontsize=8, loc="best")
    ax.grid(axis="y", alpha=0.3)
    fig.autofmt_xdate(rotation=45)
    bio = io.BytesIO()
    fig.savefig(bio, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    bio.seek(0)
    return bio


def _chart_grupos(pag):
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    g = pag.groupby("Grupo")["Comprometido"].sum().abs().sort_values(ascending=True).tail(15)
    if g.empty:
        return None
    fig, ax = plt.subplots(figsize=(10.5, 4.2))
    ax.barh([_trunc(i, 30) for i in g.index], g.values, color="#ef6c00")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: _brl_short(v)))
    ax.set_title("Despesas por Grupo (comprometido)", fontsize=11, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    bio = io.BytesIO()
    fig.savefig(bio, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    bio.seek(0)
    return bio


def montar_pdf(rec, pag, dff):
    """Retorna os bytes de um PDF com graficos + tabelas, respeitando os filtros (rec/pag/dff ja filtrados)."""
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle, Paragraph,
                                    Spacer, Image, PageBreak)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=16, spaceAfter=4)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=12, spaceBefore=8, spaceAfter=4,
                        textColor=colors.HexColor("#1565c0"))
    small = ParagraphStyle("small", parent=styles["Normal"], fontSize=8, textColor=colors.HexColor("#555555"))

    def _tbl(df, aligns, widths, font=7, nota=None):
        head = list(df.columns)
        data = [head] + df.astype(str).values.tolist()
        t = Table(data, colWidths=widths, repeatRows=1)
        style = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1565c0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), font),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f2f2")]),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cccccc")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 1.5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5),
        ]
        for i, a in enumerate(aligns):
            style.append(("ALIGN", (i, 0), (i, -1), a))
        t.setStyle(TableStyle(style))
        out = [t]
        if nota:
            out.append(Spacer(1, 3))
            out.append(Paragraph(nota, small))
        return out

    # ---- KPIs ----
    ret = rec["IsRetido"]
    rl = rec.loc[~ret, "Comprometido"].sum()
    rt = rec.loc[ret, "Comprometido"].sum()
    rb = rl + rt
    dp = pag["Comprometido"].sum()
    rl_e = rec.loc[~ret, "Executado"].sum()
    dp_e = pag["Executado"].sum()
    datas = pd.to_datetime(dff["Data"], errors="coerce").dropna()
    periodo = f"{datas.min():%d/%m/%Y} a {datas.max():%d/%m/%Y}" if len(datas) else "—"
    deps = sorted(dff["Departamento"].dropna().astype(str).unique())
    dep_txt = deps[0] if len(deps) == 1 else f"{len(deps)} departamentos"

    story = []
    story.append(Paragraph("Relatorio Financeiro BWS Construcoes", h1))
    story.append(Paragraph(
        f"Gerado em {dt.datetime.now():%d/%m/%Y %H:%M} &nbsp;|&nbsp; Periodo: {periodo} "
        f"&nbsp;|&nbsp; Escopo: {dep_txt} &nbsp;|&nbsp; {len(dff):,} lancamentos".replace(",", "."),
        small))
    story.append(Spacer(1, 8))

    kpi = pd.DataFrame([
        ["Resultado (DRE) - comprometido", ""],
        ["Receita liquida", _brl(rl)],
        ["(+) Retencoes na fonte", _brl(rt)],
        ["(=) Receita bruta", _brl(rb)],
        ["Despesas", _brl(dp)],
        ["Resultado", _brl(rl + dp)],
    ], columns=["Indicador", "Valor"])
    story += _tbl(kpi, ["LEFT", "RIGHT"], [10 * cm, 7 * cm], font=9)
    story.append(Paragraph(
        f"Executado (DRE): receita liquida {_brl(rl_e)}, despesas {_brl(dp_e)}, resultado {_brl(rl_e + dp_e)}.", small))

    # ---- Graficos ----
    story.append(PageBreak())
    story.append(Paragraph("Fluxo Financeiro", h2))
    for img, leg in [
        (_chart_mensal(pd.concat([rec, pag]), "Fluxo Financeiro mensal - Executado (realizado)", False),
         None),
        (_chart_mensal(pd.concat([rec, pag]), "Fluxo Financeiro mensal - Comprometido (realizado + em aberto)", True),
         None),
        (_chart_grupos(pag), None),
    ]:
        if img is not None:
            story.append(Image(img, width=25 * cm, height=25 * cm * 0.34))
            story.append(Spacer(1, 6))

    # ---- DRE ----
    story.append(PageBreak())
    story.append(Paragraph("DRE - Executado / Em aberto / Comprometido", h2))
    rl_e2, rl_a = rec.loc[~ret, "Executado"].sum(), rec.loc[~ret, "EmAberto"].sum()
    rt_e, rt_a = rec.loc[ret, "Executado"].sum(), rec.loc[ret, "EmAberto"].sum()
    drows = [("Receita Bruta de Servicos", rl_e2 + rt_e, rl_a + rt_a),
             ("(-) Retencoes na fonte", -rt_e, -rt_a),
             ("Receita Liquida", rl_e2, rl_a),
             ("", None, None)]
    for grupo, g in pag.groupby("Grupo"):
        drows.append(("  " + str(grupo), g["Executado"].sum(), g["EmAberto"].sum()))
    de, da = pag["Executado"].sum(), pag["EmAberto"].sum()
    drows += [("Total Custos/Despesas", de, da), ("", None, None),
              ("RESULTADO", rl_e2 + de, rl_a + da)]
    dre_df = pd.DataFrame([{
        "Linha": n,
        "Executado": _brl(e) if e is not None else "",
        "Em aberto": _brl(a) if a is not None else "",
        "Comprometido": _brl(e + a) if e is not None else "",
    } for n, e, a in drows])
    story += _tbl(dre_df, ["LEFT", "RIGHT", "RIGHT", "RIGHT"], [11 * cm, 4.5 * cm, 4.5 * cm, 4.5 * cm], font=8)

    # ---- Despesas por Categoria ----
    story.append(PageBreak())
    story.append(Paragraph("Despesas por Categoria (comprometido)", h2))
    dc = pag.groupby(["Grupo", "Categoria"])["Comprometido"].sum().reset_index()
    dc = dc[dc["Comprometido"].abs() > 0.005].reindex(
        dc["Comprometido"].abs().sort_values(ascending=False).index)
    nota = None
    if len(dc) > CAP_LINHAS:
        nota = f"Exibindo as {CAP_LINHAS} maiores de {len(dc):,} categorias. Lista completa no Excel.".replace(",", ".")
        dc = dc.head(CAP_LINHAS)
    dc_show = pd.DataFrame({"Grupo": dc["Grupo"].map(lambda s: _trunc(s, 30)),
                            "Categoria": dc["Categoria"].map(lambda s: _trunc(s, 40)),
                            "Comprometido": dc["Comprometido"].map(_brl)})
    story += _tbl(dc_show, ["LEFT", "LEFT", "RIGHT"], [8 * cm, 11 * cm, 5 * cm], font=7, nota=nota)

    # ---- Top Credores ----
    story.append(PageBreak())
    story.append(Paragraph("Top Credores (comprometido)", h2))
    tc = pag.groupby("Cliente ou Fornecedor (Razão Social)")["Comprometido"].sum().reset_index()
    tc.columns = ["Credor", "Valor"]
    tc = tc[tc["Valor"].abs() > 0.005]
    tot = tc["Valor"].sum()
    tc = tc.reindex(tc["Valor"].abs().sort_values(ascending=False).index)
    tc["pct"] = (tc["Valor"] / tot * 100) if tot else 0.0
    nota = None
    if len(tc) > CAP_LINHAS:
        nota = f"Exibindo os {CAP_LINHAS} maiores de {len(tc):,} credores. Lista completa no Excel.".replace(",", ".")
        tc = tc.head(CAP_LINHAS)
    tc_show = pd.DataFrame({"Credor": tc["Credor"].map(lambda s: _trunc(s, 60)),
                            "Valor": tc["Valor"].map(_brl),
                            "% do Total": tc["pct"].map(lambda x: f"{x:.2f}%".replace(".", ","))})
    story += _tbl(tc_show, ["LEFT", "RIGHT", "RIGHT"], [17 * cm, 4.5 * cm, 2.5 * cm], font=7, nota=nota)

    # ---- Receitas por Medicao (Receita de Obra) ----
    story.append(PageBreak())
    story.append(Paragraph("Receita de Obra (por medicao)", h2))
    rec_obra, _ = split_receitas(rec)
    med = construir_medicoes(rec_obra)
    med = med[med["Bruto"].abs() > 0.005].reindex(
        med["Bruto"].abs().sort_values(ascending=False).index)
    nota = None
    if len(med) > CAP_LINHAS:
        nota = f"Exibindo as {CAP_LINHAS} maiores de {len(med):,} medicoes. Lista completa no Excel.".replace(",", ".")
        med = med.head(CAP_LINHAS)
    med_show = pd.DataFrame({
        "Data": pd.to_datetime(med["Data"], errors="coerce").dt.strftime("%d/%m/%Y").fillna(""),
        "Cliente": med["Cliente"].map(lambda s: _trunc(s, 28)),
        "Depto": med["Departamento"].map(lambda s: _trunc(s, 16)),
        "Bruto": med["Bruto"].map(_brl),
        "Recebido": med["Recebido"].map(_brl),
        "Retido": med["Retido"].map(_brl),
        "A Receber": med["AReceber"].map(_brl),
        "Situacao": med["SitSimpl"],
    })
    story += _tbl(med_show, ["LEFT", "LEFT", "LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT", "LEFT"],
                  [2.2 * cm, 5.5 * cm, 3 * cm, 3 * cm, 3 * cm, 2.6 * cm, 3 * cm, 3.2 * cm], font=6.5, nota=nota)

    # ---- Contas a Pagar ----
    story.append(PageBreak())
    story.append(Paragraph("Contas a Pagar (em aberto)", h2))
    cp = pag.copy()
    cp["Valor"] = cp["APagarReceber"]
    cp = cp[cp["Valor"].abs() > 0.005].sort_values("Data", na_position="last")
    nota = None
    if len(cp) > CAP_LINHAS:
        nota = f"Exibindo as {CAP_LINHAS} primeiras (por vencimento) de {len(cp):,} contas. Lista completa no Excel.".replace(",", ".")
        cp = cp.head(CAP_LINHAS)
    cp_show = pd.DataFrame({
        "Vencimento": pd.to_datetime(cp["Data"], errors="coerce").dt.strftime("%d/%m/%Y").fillna(""),
        "Fornecedor": cp["Cliente ou Fornecedor (Razão Social)"].map(lambda s: _trunc(s, 30)),
        "Valor": cp["Valor"].map(_brl),
        "Categoria": cp["Categoria"].map(lambda s: _trunc(s, 24)),
        "Depto": cp["Departamento"].map(lambda s: _trunc(s, 16)),
        "Situacao Vcto": cp["SituacaoVencimento"].map(lambda s: _trunc(s, 22)),
    })
    story += _tbl(cp_show, ["LEFT", "LEFT", "RIGHT", "LEFT", "LEFT", "LEFT"],
                  [2.4 * cm, 6.5 * cm, 3.2 * cm, 5 * cm, 3 * cm, 4.5 * cm], font=6.5, nota=nota)

    # ---- Despesas Analitico (linha a linha) ----
    story.append(PageBreak())
    story.append(Paragraph("Despesas Analitico (linha a linha)", h2))
    da = pag.copy()
    da["Valor"] = da["PagoRecebido"].where(da["Pago"], da["APagarReceber"])
    da = da[da["Valor"].abs() > 0.005]
    da = da.reindex(da["Valor"].abs().sort_values(ascending=False).index)
    nota = None
    if len(da) > CAP_LINHAS:
        nota = (f"Exibindo os {CAP_LINHAS} maiores de {len(da):,} lancamentos. "
                f"Detalhe completo no Excel.").replace(",", ".")
        da = da.head(CAP_LINHAS)
    da_show = pd.DataFrame({
        "Data": pd.to_datetime(da["Data"], errors="coerce").dt.strftime("%d/%m/%Y").fillna(""),
        "Fornecedor": da["Cliente ou Fornecedor (Razão Social)"].map(lambda s: _trunc(s, 30)),
        "Valor": da["Valor"].map(_brl),
        "Categoria": da["Categoria"].map(lambda s: _trunc(s, 24)),
        "Depto": da["Departamento"].map(lambda s: _trunc(s, 16)),
        "Situacao Vcto": da["SituacaoVencimento"].map(lambda s: _trunc(s, 22)),
    })
    story += _tbl(da_show, ["LEFT", "LEFT", "RIGHT", "LEFT", "LEFT", "LEFT"],
                  [2.4 * cm, 6.5 * cm, 3.2 * cm, 5 * cm, 3 * cm, 4.5 * cm], font=6.5, nota=nota)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                            leftMargin=1 * cm, rightMargin=1 * cm,
                            topMargin=1 * cm, bottomMargin=1 * cm,
                            title="Relatorio Financeiro BWS")
    doc.build(story)
    buf.seek(0)
    return buf.getvalue()