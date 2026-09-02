# -*- coding: utf-8 -*-
"""
Dashboard Financeiro BWS - Fase 1 (v2)
Modelo: Executado (pago/recebido, pela Situacao) | Em aberto (saldo A Pagar/Receber) | Comprometido (soma)
Paginas: Visao Geral, DRE, Fluxo de Caixa, Resultado por Obra/Projeto, Comprometido vs Executado
Rodar: streamlit run app.py
"""
import os
import io
import re
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Chave de medicao compartilhada com o gerador do parquet, para a Receita de Obra
# (consolidada) e a Receita Analitico (aberta por recebimento) contarem o mesmo.
from fonte_dados import (chave_medicao, rotulo_medicao, classificar_aporte,
                         TIPOS_NO_SALDO)

st.set_page_config(page_title="Financeiro BWS", page_icon=":bar_chart:", layout="wide")
VERSAO = "2026-08-12.3"   # carimbo de versao — confira com o diagnostico.py

PARQUET_OMIE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados_omie.parquet")
# Parquet irmao: 1 linha por MOVIMENTO liquidado (cada recebimento com data e valor
# exatos). Gerado junto com o principal pelo fonte_dados.py. So existe na fonte Omie.
PARQUET_RECEB = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "dados_omie_recebimentos.parquet")
REC, PAG = "1. Contas a Receber", "2. Contas a Pagar"
DASH = "\u2014"  # travessao, fora de f-string (compat. Python < 3.12)


@st.cache_data(show_spinner="Carregando base...")
def carregar(mtime=None):
    # mtime (data de modificacao do parquet) faz PARTE da chave do cache: quando o
    # arquivo e regenerado (gerar_parquet/atualizar), o mtime muda, o cache invalida
    # sozinho e os dados novos aparecem. Sem isso, o cache servia a versao antiga.
    df = pd.read_parquet(PARQUET_OMIE)  # unica fonte: Omie (API)
    for c in ["PagoRecebido", "APagarReceber"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
    df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce")
    # MODELO: a coluna F (Data) agora traz UMA data por título — data de pagamento/
    # recebimento se houve, senão a data de vencimento. Por isso "foi pago/recebido"
    # NÃO é mais "tem data": identificamos pelo texto da Situação.
    pago = df["Situação"].astype(str).str.contains("Pago|Recebido|Conciliado", case=False, na=False)
    df["Executado"] = df["PagoRecebido"].where(pago, 0.0)
    df["EmAberto"] = df["APagarReceber"]
    df["Comprometido"] = df["Executado"] + df["EmAberto"]
    # Juros/Multa: existem na fonte Omie (API); na fonte Log nao existem -> 0.
    for c in ["Juros", "Multa"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        else:
            df[c] = 0.0
    # Encargo = juros + multa efetivamente PAGOS (despesa/receita financeira, regime caixa).
    df["Encargo"] = (df["Juros"] + df["Multa"]).where(pago, 0.0)
    df["IsRetido"] = df["Categoria"].astype(str).str.contains("Retido", na=False)
    df["Pago"] = pago
    return df


@st.cache_data(show_spinner="Carregando recebimentos...")
def carregar_recebimentos(mtime=None):
    """Analitico por movimento (1 linha por recebimento). None se o arquivo nao existir
    — nesse caso a aba 'Receita Analitico' simplesmente nao entra no Excel."""
    if not os.path.exists(PARQUET_RECEB):
        return None
    df = pd.read_parquet(PARQUET_RECEB)
    for c in ["Valor", "Juros", "Multa", "Desconto", "Valor do Movimento", "Rateio %"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
    df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce")
    return df


def brl(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "\u2014"
    s = f"{abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"-R$ {s}" if v < 0 else f"R$ {s}"


def pipefy_link(doc):
    """Documento no padrao 2 letras + digitos (SP/PM/DC...) vira link do card Pipefy."""
    m = re.fullmatch(r"[A-Z]{2}(\d+)", str(doc).strip().upper())
    return f"https://app.pipefy.com/open-cards/{m.group(1)}" if m else ""


@st.dialog("Detalhe do lancamento", width="large")
def dialog_detalhe(rec):
    data = rec.get("Data")
    data_fmt = data.strftime("%d/%m/%Y") if pd.notna(data) else "\u2014"
    det = [
        ("Grupo", rec.get("Grupo")),
        ("Categoria", rec.get("Categoria")),
        ("Projeto", rec.get("Projeto")),
        ("Cliente/Fornecedor", rec.get("Cliente ou Fornecedor (Razão Social)")),
        ("CNPJ/CPF", rec.get("CNPJ/CPF")),
        ("Departamento", rec.get("Departamento")),
        ("Data de Pagto/Recbto/Vencto", data_fmt),
        ("Situacao do Vencimento", rec.get("SituacaoVencimento")),
    ]
    # So mostra a medida que se aplica ao titulo: pago -> Pago/Recebido; aberto -> A Pagar/Receber.
    if rec.get("Pago"):
        det.append(("Pago ou Recebido", brl(rec.get("PagoRecebido"))))
    else:
        det.append(("A Pagar ou Receber", brl(rec.get("APagarReceber"))))
    det += [
        ("Numero do Documento", rec.get("Número do Documento")),
        ("Pedido de Compra", rec.get("Pedido de Compra")),
        ("Conta Corrente", rec.get("Conta Corrente")),
        ("Observacao", rec.get("Observação da Conta")),
    ]
    dl, dr = st.columns(2)
    meio = (len(det) + 1) // 2
    for i, (k, v) in enumerate(det):
        txt = str(v).strip()
        (dl if i < meio else dr).write(f"**{k}:** {txt if txt and txt.lower() != 'nan' else DASH}")
    link = rec.get("Link")
    if link:
        st.markdown(f"[Abrir card no Pipefy]({link})")


st.sidebar.title("Financeiro BWS")

# --- Fonte de dados: Omie (API) e a unica fonte. Mostra a data do parquet. ---
_mtime_fonte = os.path.getmtime(PARQUET_OMIE) if os.path.exists(PARQUET_OMIE) else 0.0
if not os.path.exists(PARQUET_OMIE):
    st.error("Base Omie (dados_omie.parquet) nao encontrada. Rode a atualizacao (atualiza_omie.bat).")
    st.stop()
if st.sidebar.button("🔄 Recarregar dados", help="Le o parquet de novo (apos rodar a atualizacao)."):
    st.cache_data.clear()
    st.rerun()
df = carregar(_mtime_fonte)
import datetime as _dtmod
st.sidebar.caption("Dados do Omie de " +
                   _dtmod.datetime.fromtimestamp(_mtime_fonte).strftime("%d/%m/%Y %H:%M"))

pagina = st.sidebar.radio("Pagina", ["Visao Geral", "DRE", "Fluxo de Caixa",
                                     "Resultado por Obra/Projeto", "Comprometido vs Executado",
                                     "Necessidade de Caixa", "Prestacao de Contas"])

if pagina == "Prestacao de Contas":
    import prestacao_contas
    prestacao_contas.pagina()
    st.stop()

st.sidebar.markdown("---")
st.sidebar.subheader("Filtros")
anos = sorted([int(a) for a in df["Ano"].dropna().unique() if 2015 <= a <= 2100])
anos_sel = st.sidebar.multiselect("Ano", anos, default=anos)
projetos = sorted([p for p in df["Projeto"].dropna().unique() if p and p != "nan"])
proj_sel = st.sidebar.multiselect("Projeto", projetos, default=[])
deptos = sorted([d for d in df["Departamento"].dropna().unique() if d and d != "nan"])
dep_sel = st.sidebar.multiselect("Departamento (obra)", deptos, default=[])
excluir_trf = st.sidebar.checkbox("Excluir transferencias (TRF)", value=True)
st.sidebar.caption("Filtros vazios = todos. Executado = pago/recebido (pela Situacao). "
                   "Em aberto = saldo a pagar/receber. Comprometido = soma.")


def filtrar(base):
    f = base
    if anos_sel:
        # Itens em aberto (sem data) sao backlog atual, sem ano de realizacao (Ano=1900).
        # O filtro de ano nao deve descarta-los, senao "a pagar/a receber" some.
        f = f[f["Ano"].isin(anos_sel) | f["Data"].isna()]
    if proj_sel:
        f = f[f["Projeto"].isin(proj_sel)]
    if dep_sel:
        f = f[f["Departamento"].isin(dep_sel)]
    if excluir_trf:
        f = f[f["An\u00e1lise"] != "TRF"]
    return f


dff = filtrar(df)
ANALISE = "An\u00e1lise"

# Analitico por recebimento (fonte Omie). Passa pelos MESMOS filtros da barra lateral,
# porque carrega Ano/Projeto/Departamento/Analise com o mesmo significado.
_mtime_receb = os.path.getmtime(PARQUET_RECEB) if os.path.exists(PARQUET_RECEB) else 0.0
_receb = carregar_recebimentos(_mtime_receb)
receb_f = filtrar(_receb) if _receb is not None and not _receb.empty else None


def linha3(nome, e, a):
    return {"Linha": nome, "Executado": brl(e), "Em aberto": brl(a), "Comprometido": brl(e + a)}


def pagina_visao_geral():
    st.title("Visao Geral")
    st.caption("Duas leituras do mesmo periodo e filtros: o RESULTADO (DRE, regime de competencia, "
               "receita liquida sem retencoes) e o CAIXA (o que efetivamente entrou e saiu, incluindo aportes).")

    dre = dff[dff[ANALISE] == "DRE"]
    rec_liq = dre.loc[(dre["Tipo"] == REC) & (~dre["IsRetido"]), "Comprometido"].sum()
    desp = dre.loc[dre["Tipo"] == PAG, "Comprometido"].sum()
    rec_liq_e = dre.loc[(dre["Tipo"] == REC) & (~dre["IsRetido"]), "Executado"].sum()
    desp_e = dre.loc[dre["Tipo"] == PAG, "Executado"].sum()

    st.subheader("Resultado (DRE) — comprometido")
    c1, c2, c3 = st.columns(3)
    c1.metric("Receita liquida", brl(rec_liq))
    c2.metric("Despesas", brl(desp))
    c3.metric("Resultado", brl(rec_liq + desp))
    st.caption(f"Executado (ja realizado): receita liquida {brl(rec_liq_e)}, "
               f"despesas {brl(desp_e)}, resultado {brl(rec_liq_e + desp_e)}.")

    da = dre.copy()
    da["RecLiq"] = da["Comprometido"].where((da["Tipo"] == REC) & (~da["IsRetido"]), 0.0)
    da["Desp"] = da["Comprometido"].where(da["Tipo"] == PAG, 0.0)
    gy = da[da["Ano"].between(2015, 2100)].groupby("Ano").agg(
        Receita=("RecLiq", "sum"), Despesa=("Desp", "sum")).reset_index()
    gy["Resultado"] = gy["Receita"] + gy["Despesa"]
    if not gy.empty:
        figd = go.Figure()
        figd.add_bar(x=gy["Ano"], y=gy["Receita"], name="Receita liquida", marker_color="#2e7d32")
        figd.add_bar(x=gy["Ano"], y=gy["Despesa"], name="Despesa", marker_color="#c62828")
        figd.add_scatter(x=gy["Ano"], y=gy["Resultado"], name="Resultado", mode="lines+markers",
                         line=dict(color="#1565c0", width=3))
        figd.update_layout(barmode="relative", height=380, separators=",.", xaxis_title="",
                           legend=dict(orientation="h", y=1.12, x=0), margin=dict(t=30, b=0),
                           hovermode="x unified", yaxis=dict(tickprefix="R$ ", rangemode="tozero"))
        figd.update_traces(hovertemplate="R$ %{y:,.2f}<extra></extra>")
        figd.update_xaxes(dtick=1)
        st.plotly_chart(figd, use_container_width=True)

    st.markdown("---")
    st.subheader("Caixa (Fluxo de Caixa) — realizado")
    cx = dff[dff["Pago"] & ~((dff["Tipo"] == REC) & dff["IsRetido"])]
    entradas = cx.loc[cx["PagoRecebido"] > 0, "PagoRecebido"].sum()
    saidas = cx.loc[cx["PagoRecebido"] < 0, "PagoRecebido"].sum()
    f1, f2, f3 = st.columns(3)
    f1.metric("Entradas", brl(entradas))
    f2.metric("Saidas", brl(saidas))
    f3.metric("Geracao de caixa", brl(entradas + saidas))
    st.caption("Tudo que foi efetivamente pago/recebido (liquido), incluindo aportes e devolucoes "
               "(Analise = Fluxo de Caixa). As retencoes na fonte nao entram (sao retidas pelo cliente). "
               "Detalhe mes a mes na pagina Fluxo de Caixa.")
    gy2 = cx[cx["Ano"].between(2015, 2100)].groupby("Ano")["PagoRecebido"].sum().reset_index()
    if not gy2.empty:
        gy2 = gy2.sort_values("Ano")
        gy2["Acumulado"] = gy2["PagoRecebido"].cumsum()
        figf = go.Figure()
        figf.add_bar(x=gy2["Ano"], y=gy2["PagoRecebido"], name="Geracao de caixa no ano",
                     marker_color="#26a69a")
        figf.add_scatter(x=gy2["Ano"], y=gy2["Acumulado"], name="Caixa acumulado",
                         mode="lines+markers", line=dict(color="#1565c0", width=3))
        figf.update_layout(barmode="relative", height=380, separators=",.", xaxis_title="",
                           legend=dict(orientation="h", y=1.12, x=0), margin=dict(t=30, b=0),
                           hovermode="x unified", yaxis=dict(tickprefix="R$ ", rangemode="tozero"))
        figf.update_traces(hovertemplate="R$ %{y:,.2f}<extra></extra>")
        figf.update_xaxes(dtick=1)
        st.plotly_chart(figf, use_container_width=True)


def construir_medicoes(rec_df):
    """Agrupa receitas de obra por MEDICAO. A chave sai do fonte_dados.chave_medicao:
    Numero do Documento quando valido, senao obra + numero da medicao extraidos da
    observacao (as parcelas de uma medicao sao titulos separados, e o texto completo
    da observacao varia entre elas). Calcula Bruto/Recebido/Retido/AReceber."""
    d = rec_df.copy()
    doc = d["Número do Documento"].astype(str).str.strip()
    obs = d["Observação da Conta"].astype(str).str.strip()
    chave = pd.Series([chave_medicao(a, b) for a, b in zip(doc, obs)], index=d.index)
    chave = chave.fillna(pd.Series("ROW:" + d.index.astype(str), index=d.index))
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
        Projeto=("Projeto", "first"),
        CNPJ=("CNPJ/CPF", "first"),
        Doc=("Número do Documento", "first"),
        Obs=("Observação da Conta", "first"),
        Data=("Data", "max"),
    ).reset_index()
    g["Medição"] = g["_chave"].map(rotulo_medicao)
    g = g.drop(columns=["_chave"])
    g["AReceber"] = g["ARecLiq"] + g["RetFut"]
    g["Bruto"] = g["Recebido"] + g["Retido"] + g["AReceber"]
    g["SitSimpl"] = "A Receber"
    g.loc[(g["Recebido"].abs() > 0.005) & (g["AReceber"].abs() <= 0.005), "SitSimpl"] = "Recebido"
    g.loc[(g["Recebido"].abs() > 0.005) & (g["AReceber"].abs() > 0.005), "SitSimpl"] = "Recebido Parcialmente"
    return g


def split_receitas(rec):
    """Separa receitas em Receita de Obra (Receita de Obras + retencoes) e Outras Receitas."""
    is_obra = rec["Categoria"].astype(str).str.strip().eq("Receita de Obras") | rec["IsRetido"]
    return rec[is_obra].copy(), rec[~is_obra].copy()


def _detalhe_recebimentos(rec):
    """Quebra a medicao por recebimento (data e valor exatos de cada entrada).
    Silencioso quando o parquet de recebimentos nao existe (fonte Log)."""
    if receb_f is None or receb_f.empty:
        return
    doc = str(rec.get("Doc") or "").strip()
    obs = str(rec.get("Obs") or "").strip()
    alvo = chave_medicao(doc, obs)
    if not alvo:
        return
    r = receb_f[receb_f["Tipo"] == REC]
    chaves = [chave_medicao(a, b) for a, b in
              zip(r["Número do Documento"].astype(str), r["Observação"].astype(str))]
    r = r[[c == alvo for c in chaves]]
    if r.empty:
        return
    r = r.sort_values("Data", na_position="last")
    st.markdown(f"**Recebimentos ({len(r['Parcela'].unique())} entradas):**")
    st.dataframe(
        pd.DataFrame({
            "Parcela": r["Parcela"],
            "Data": pd.to_datetime(r["Data"], errors="coerce").dt.strftime("%d/%m/%Y").fillna(DASH),
            "Valor": r["Valor"].apply(brl),
            "Conta Corrente": r["Conta Corrente"],
            "Departamento": r["Departamento"],
        }),
        use_container_width=True, hide_index=True)
    st.caption(f"Total recebido: {brl(r['Valor'].sum())} — so entradas de caixa "
               "(retencoes e saldo a receber nao entram aqui).")


@st.dialog("Detalhe da medicao", width="large")
def dialog_medicao(rec):
    data = rec.get("Data")
    try:
        data_fmt = pd.to_datetime(data).strftime("%d/%m/%Y") if pd.notna(data) else "\u2014"
    except Exception:
        data_fmt = "\u2014"
    det = [
        ("Departamento (obra)", rec.get("Departamento")),
        ("Cliente", rec.get("Cliente")),
        ("Projeto", rec.get("Projeto")),
        ("CNPJ/CPF", rec.get("CNPJ")),
        ("Numero do Documento", rec.get("Doc")),
        ("Data", data_fmt),
        ("Situacao", rec.get("SitSimpl") or rec.get("Situacao")),
        ("Valor da medicao (bruto)", brl(rec.get("Bruto"))),
        ("Recebido (liquido)", brl(rec.get("Recebido"))),
        ("Retido", brl(rec.get("Retido"))),
        ("A receber", brl(rec.get("AReceber"))),
    ]
    dl, dr = st.columns(2)
    meio = (len(det) + 1) // 2
    for i, (k, v) in enumerate(det):
        txt = str(v).strip()
        (dl if i < meio else dr).write(f"**{k}:** {txt if txt and txt.lower() != 'nan' else DASH}")
    obs = str(rec.get("Obs") or "").strip()
    if obs and obs.lower() not in ("nan", "n/d", ""):
        st.markdown("**Observacao:**")
        st.caption(obs)
    _detalhe_recebimentos(rec)
    link = pipefy_link(rec.get("Doc"))
    if link:
        st.markdown(f"[Abrir card no Pipefy]({link})")


_MONEY_COLS = {"Executado", "Em aberto", "Comprometido", "Valor", "Bruto", "Recebido",
               "Retido", "A Receber", "AReceber", "ARecLiq", "RetFut", "Pago (R$)", "A Pagar (R$)",
               "Juros (R$)", "Multa (R$)", "Total (R$)",
               "Juros", "Multa", "Desconto", "Valor do Movimento", "Total da Medição",
               "Aportado", "Devolvido", "Saldo", "Falta p/ igualar",
               "Recebido", "Pago", "Liquido",
               "Resultado", "Dividendos pagos", "Disponivel",
               "Se proporcional ao aporte"}


def _fmt_sheet(xw, name, df):
    """Aplica largura, formato contabil, data dd/mm/yyyy e quebra de texto via xlsxwriter."""
    ws = xw.sheets[name]
    wb = xw.book
    f_money = wb.add_format({"num_format": '_-"R$" * #,##0.00_-;-"R$" * #,##0.00_-;_-"R$" * "-"??_-;_-@_-'})
    f_date = wb.add_format({"num_format": "dd/mm/yyyy"})
    f_pct = wb.add_format({"num_format": '0.00"%"'})
    f_wrap = wb.add_format({"text_wrap": True, "valign": "top"})
    for i, c in enumerate(df.columns):
        cl = str(c)
        if cl in ("Data", "Vencimento"):
            ws.set_column(i, i, 12, f_date)
        elif cl in _MONEY_COLS:
            ws.set_column(i, i, 16, f_money)
        elif cl.startswith("% "):
            ws.set_column(i, i, 11, f_pct)
        elif "Observ" in cl:
            ws.set_column(i, i, 50)
        else:
            try:
                m = int(df[c].astype(str).str.len().head(3000).max() or 10)
            except Exception:
                m = 10
            ws.set_column(i, i, min(max(len(cl) + 2, m + 2), 60))
    ws.freeze_panes(1, 0)


def construir_resultado_dividendos(df_base):
    """
    Ponte entre RESULTADO e DIVIDENDO, por obra.

    O resultado sai da DRE (categorias de resultado, em caixa: so o que foi pago/
    recebido). O dividendo sai do FLUXO — ele nao e despesa, e distribuicao do
    resultado ja apurado. Por isso os dois nunca se somam: um alimenta o outro.

      Disponivel = Resultado realizado - Dividendos ja pagos

    Devolve (por_obra, resumo) ou (None, None).
    """
    d = df_base.copy()
    d["Obra"] = (d["Departamento"].astype(str).str.strip()
                 .replace({"": "(sem obra)", "nan": "(sem obra)"}))

    res = d[(d[ANALISE] == "DRE") & d["Pago"]]
    g_res = res.groupby("Obra")["PagoRecebido"].sum().rename("Resultado")

    div = d[d["Categoria"].map(classificar_aporte).eq("Dividendos") & d["Pago"]]
    # dividendo pago = saida de caixa (negativo no parquet) -> converte para positivo
    v = div["PagoRecebido"]
    g_div = (-v.where(v < 0, 0.0)).groupby(div["Obra"]).sum().rename("Dividendos pagos")

    if g_res.empty and g_div.empty:
        return None, None
    t = pd.concat([g_res, g_div], axis=1).fillna(0.0).reset_index()
    t["Disponivel"] = t["Resultado"] - t["Dividendos pagos"]
    t = t.sort_values("Resultado", ascending=False).reset_index(drop=True)
    resumo = {
        "resultado": float(t["Resultado"].sum()),
        "dividendos": float(t["Dividendos pagos"].sum()),
        "disponivel": float(t["Disponivel"].sum()),
    }
    return t, resumo


def construir_aportes(df_base):
    """
    Conta corrente de aportes. Trabalha em CAIXA (so o que entrou/saiu de fato),
    porque saldo de socio e posicao financeira, nao competencia.

    Devolve (por_socio, por_obra, detalhe, por_tipo) — tudo None se nao houver
    lancamento de aporte no filtro atual.

    Convencao de sinal do parquet: receita positiva, despesa negativa.
      Aportado  = entradas  (dinheiro que o socio/parceiro colocou)
      Devolvido = saidas    (dinheiro que voltou para ele)
      Saldo     = Aportado - Devolvido  (quanto ele ainda tem na obra/empresa)
    """
    d = df_base.copy()
    d["TipoAporte"] = d["Categoria"].map(classificar_aporte)
    d = d[d["TipoAporte"].notna() & d["Pago"]]
    if d.empty:
        return None, None, None, None, None
    # Dividendo e distribuicao de LUCRO, nao devolucao de capital: fica fora do
    # saldo de aporte (ver TIPOS_NO_SALDO no fonte_dados.py) e vai num quadro a parte.
    fora = d[~d["TipoAporte"].isin(TIPOS_NO_SALDO)].copy()
    d = d[d["TipoAporte"].isin(TIPOS_NO_SALDO)]
    if d.empty and fora.empty:
        return None, None, None, None, None

    d["Socio"] = (d["Cliente ou Fornecedor (Razão Social)"].astype(str).str.strip()
                  .replace({"": "(sem contraparte)", "nan": "(sem contraparte)"}))
    d["Obra"] = (d["Departamento"].astype(str).str.strip()
                 .replace({"": "(sem obra)", "nan": "(sem obra)"}))
    v = d["PagoRecebido"]
    d["Aportado"] = v.where(v > 0, 0.0)
    d["Devolvido"] = (-v).where(v < 0, 0.0)

    def _agrega(chaves):
        g = d.groupby(chaves, dropna=False).agg(
            Aportado=("Aportado", "sum"), Devolvido=("Devolvido", "sum"),
            Lancamentos=("PagoRecebido", "size")).reset_index()
        g["Saldo"] = g["Aportado"] - g["Devolvido"]
        return g.sort_values("Saldo", ascending=False).reset_index(drop=True)

    por_socio = _agrega(["Socio"])
    # Por obra: uma linha por socio, com aporte e devolucao ja compensados. Agrupar
    # tambem por TipoAporte quebraria o socio em varias linhas e o saldo perderia
    # o sentido (a devolucao ficaria separada do aporte).
    por_obra = _agrega(["Obra", "Socio"])
    por_tipo = _agrega(["Obra", "TipoAporte"])

    # "Quanto falta para equilibrar": diferenca para o MAIOR aportador da obra.
    # E uma referencia de igualdade simples — o sistema nao sabe a quota acordada
    # de cada socio, entao esta coluna e um comparativo, nao uma cobranca.
    # transform() mantem o alinhamento linha a linha (um loop com groupby nao manteria).
    if not por_obra.empty:
        por_obra["Falta p/ igualar"] = (
            por_obra.groupby("Obra")["Saldo"].transform("max") - por_obra["Saldo"])

    det = d[["Data", "Obra", "Socio", "TipoAporte", "Categoria", "PagoRecebido",
             "Aportado", "Devolvido", "Número do Documento", "Conta Corrente",
             "Observação da Conta"]].copy()
    det = det.sort_values(["Obra", "Socio", "Data"], na_position="last")

    # Dividendos (fora do saldo), em quadro proprio
    if not fora.empty:
        fora["Socio"] = (fora["Cliente ou Fornecedor (Razão Social)"].astype(str).str.strip()
                         .replace({"": "(sem contraparte)", "nan": "(sem contraparte)"}))
        vf = fora["PagoRecebido"]
        div = fora.assign(Recebido=vf.where(vf > 0, 0.0), Pago=(-vf).where(vf < 0, 0.0))
        div = div.groupby("Socio", dropna=False).agg(
            Recebido=("Recebido", "sum"), Pago=("Pago", "sum"),
            Lancamentos=("PagoRecebido", "size")).reset_index()
        div["Liquido"] = div["Pago"] - div["Recebido"]
        div = div.sort_values("Liquido", ascending=False).reset_index(drop=True)
    else:
        div = None
    return por_socio, por_obra, det, por_tipo, div


def montar_xlsx(rec, pag, receb=None, aportes=None):
    """xlsx com dados COMPLETOS (sem paginacao), formatado. Respeita os filtros atuais."""
    ret = rec["IsRetido"]
    rl_e, rl_a = rec.loc[~ret, "Executado"].sum(), rec.loc[~ret, "EmAberto"].sum()
    rt_e, rt_a = rec.loc[ret, "Executado"].sum(), rec.loc[ret, "EmAberto"].sum()
    rb_e, rb_a = rl_e + rt_e, rl_a + rt_a
    drows = [("Receita Bruta de Servicos", rb_e, rb_a),
             ("(-) Retencoes na fonte", -rt_e, -rt_a),
             ("Receita Liquida", rl_e, rl_a),
             ("", None, None)]
    for grupo, g in pag.groupby("Grupo"):
        drows.append(("  " + str(grupo), g["Executado"].sum(), g["EmAberto"].sum()))
    enc = pag["Encargo"].sum()  # juros + multa pagos (despesa financeira)
    if abs(enc) > 0.005:
        drows.append(("  Juros e Multas Pagos", enc, 0.0))
    desp_e, desp_a = pag["Executado"].sum() + enc, pag["EmAberto"].sum()
    drows += [("Total Custos/Despesas", desp_e, desp_a), ("", None, None),
              ("RESULTADO", rl_e + desp_e, rl_a + desp_a)]
    dre_num = pd.DataFrame([{"Linha": n, "Executado": e, "Em aberto": a,
                             "Comprometido": (None if e is None else e + a)} for n, e, a in drows])

    dc = pag.groupby(["Grupo", "Categoria"]).agg(
        Executado=("Executado", "sum"), EmAberto=("EmAberto", "sum"),
        Comprometido=("Comprometido", "sum")).reset_index()
    if abs(enc) > 0.005:  # linha financeira na Categoria, para bater com o DRE
        dc = pd.concat([dc, pd.DataFrame([{
            "Grupo": "Despesas Financeiras", "Categoria": "Juros e Multas Pagos",
            "Executado": enc, "EmAberto": 0.0, "Comprometido": enc}])], ignore_index=True)
    dc = dc[dc["Comprometido"].abs() > 0.005].sort_values("Comprometido")
    dc = dc.rename(columns={"EmAberto": "Em aberto"})

    tc = pag.groupby("Cliente ou Fornecedor (Razão Social)")["Comprometido"].sum().reset_index()
    tc.columns = ["Credor", "Valor"]
    tc = tc[tc["Valor"].abs() > 0.005]
    tot = tc["Valor"].sum()
    tc = tc.reindex(tc["Valor"].abs().sort_values(ascending=False).index)
    tc["% do Total"] = (tc["Valor"] / tot * 100).round(2) if tot else 0.0

    rec_obra, rec_outras = split_receitas(rec)
    med = construir_medicoes(rec_obra)
    med = med[med["Bruto"].abs() > 0.005]
    med = med.reindex(med["Bruto"].abs().sort_values(ascending=False).index)
    med_out = med[["Data", "Cliente", "Departamento", "Projeto", "Doc", "SitSimpl",
                   "Bruto", "Recebido", "Retido", "AReceber", "Obs"]].copy()
    med_out.columns = ["Data", "Cliente", "Departamento", "Projeto", "Documento", "Situacao",
                       "Bruto", "Recebido", "Retido", "A Receber", "Observacao"]
    med_out["Data"] = pd.to_datetime(med_out["Data"], errors="coerce").dt.tz_localize(None)

    o = rec_outras.copy()
    o["Recebido"] = o["PagoRecebido"].where(o["Pago"], 0.0)
    o["A Receber"] = o["APagarReceber"]
    o = o[(o["Recebido"].abs() + o["A Receber"].abs()) > 0.005]
    sit = pd.Series("A Receber", index=o.index)
    sit[(o["Recebido"].abs() > 0.005) & (o["A Receber"].abs() <= 0.005)] = "Recebido"
    sit[(o["Recebido"].abs() > 0.005) & (o["A Receber"].abs() > 0.005)] = "Recebido Parcialmente"
    outras_out = pd.DataFrame({
        "Data": pd.to_datetime(o["Data"], errors="coerce").dt.tz_localize(None),
        "Cliente": o["Cliente ou Fornecedor (Razão Social)"],
        "Categoria": o["Categoria"],
        "Departamento": o["Departamento"],
        "Recebido": o["Recebido"],
        "A Receber": o["A Receber"],
        "Situacao": sit.values,
        "Documento": o["Número do Documento"],
        "Observacao": o["Observação da Conta"],
    })
    outras_out = outras_out.reindex(
        (outras_out["Recebido"].abs() + outras_out["A Receber"].abs()).sort_values(ascending=False).index)

    da = pag.copy()
    # Colunas detalhadas para tudo rastrear e os totais baterem:
    #   Pago = principal liquidado | A Pagar = saldo | Juros/Multa = encargos pagos
    #   Total = Pago + A Pagar + Juros + Multa = valor de fato (bate com DRE/Categoria).
    da["Pago (R$)"] = da["Executado"]
    da["A Pagar (R$)"] = da["EmAberto"]
    da["Juros (R$)"] = da["Juros"]
    da["Multa (R$)"] = da["Multa"]
    da["Total (R$)"] = da["Executado"] + da["EmAberto"] + da["Juros"] + da["Multa"]
    da = da[(da["Comprometido"].abs() + da["Juros"].abs() + da["Multa"].abs()) > 0.005] \
        .sort_values("Data", ascending=False, na_position="last")
    da["Pipefy"] = da["Número do Documento"].apply(pipefy_link)
    cols = ["Data", "Cliente ou Fornecedor (Razão Social)", "Pago (R$)", "A Pagar (R$)",
            "Juros (R$)", "Multa (R$)", "Total (R$)", "Grupo", "Categoria", "Departamento",
            "Situação", "SituacaoVencimento", "Número do Documento", "Pipefy",
            "Conta Corrente", "Observação da Conta"]
    cols = [c for c in cols if c in da.columns]
    da_out = da[cols].head(1000000).copy()  # Excel suporta ~1,048,576 linhas; nao truncar a base
    da_out["Data"] = pd.to_datetime(da_out["Data"], errors="coerce").dt.tz_localize(None)
    # "Conta Corrente" ja vem com o NOME da conta (fonte_dados resolve o nCodCC);
    # "Observacao da Conta" e a observacao do titulo no Omie -> cabecalho mais direto.
    da_out = da_out.rename(columns={"Observação da Conta": "Observação"})

    cp = pag.copy()
    cp["Valor"] = cp["APagarReceber"]
    cp = cp[cp["Valor"].abs() > 0.005].sort_values("Data", na_position="last")
    cp["Pipefy"] = cp["Número do Documento"].apply(pipefy_link)
    cp = cp.rename(columns={"Data": "Vencimento"})
    ccols = ["Vencimento", "Cliente ou Fornecedor (Razão Social)", "Valor", "Grupo", "Categoria",
             "Departamento", "SituacaoVencimento", "Número do Documento", "Pipefy",
             "Conta Corrente", "Observação da Conta"]
    ccols = [c for c in ccols if c in cp.columns]
    cp_out = cp[ccols].copy()
    cp_out["Vencimento"] = pd.to_datetime(cp_out["Vencimento"], errors="coerce").dt.tz_localize(None)
    cp_out = cp_out.rename(columns={"Observação da Conta": "Observação"})

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter", datetime_format="dd/mm/yyyy") as xw:
        sheets = [("DRE", dre_num), ("Despesas Categoria", dc), ("Top Credores", tc),
                  ("Receita de Obra", med_out), ("Outras Receitas", outras_out),
                  ("Despesas Analitico", da_out), ("Contas a Pagar", cp_out)]
        # Receita Analitico: 1 linha por RECEBIMENTO (data e valor exatos de cada
        # entrada). Entra logo depois de Receita de Obra, que e a visao consolidada.
        ra_out = montar_receita_analitico(receb)
        if ra_out is not None:
            sheets.insert(4, ("Receita Analitico", ra_out))
        # Aportes: nao entram no resultado, mas explicam como a obra se banca.
        if aportes:
            por_socio, por_obra, det, por_tipo, div = aportes
            if por_socio is not None and not por_socio.empty:
                sheets.append(("Aportes por Socio", por_socio))
                sheets.append(("Aportes por Obra", por_obra))
                if por_tipo is not None and not por_tipo.empty:
                    sheets.append(("Aportes por Tipo", por_tipo))
                if div is not None and not div.empty:
                    sheets.append(("Dividendos por Socio", div))
            rd, _res = construir_resultado_dividendos(dff)
            if rd is not None and not rd.empty:
                sheets.append(("Resultado x Dividendos", rd))
                d2 = det.copy()
                d2["Data"] = pd.to_datetime(d2["Data"], errors="coerce").dt.tz_localize(None)
                d2 = d2.rename(columns={"PagoRecebido": "Valor",
                                        "Observação da Conta": "Observação"})
                sheets.append(("Aportes Analitico", d2))
        for name, d in sheets:
            d.to_excel(xw, sheet_name=name, index=False)
            _fmt_sheet(xw, name, d)
    buf.seek(0)
    return buf.getvalue()


def montar_receita_analitico(receb):
    """Aba 'Receita Analitico': cada recebimento em uma linha, com data e valor exatos.
    Resolve a incoerencia da aba 'Receita de Obra', que consolida o titulo numa data so
    (a ultima) quando a medicao foi recebida em parcelas.
    Devolve None quando o parquet de recebimentos nao existe (fonte Log)."""
    if receb is None or receb.empty:
        return None
    r = receb[receb["Tipo"] == REC].copy()
    if r.empty:
        return None
    r = r.sort_values(["Medição", "Data", "Parcela"], na_position="last")
    out = pd.DataFrame({
        "Medição": r["Medição"],
        "Parcela": r["Parcela"],
        "Recebimentos": r["Recebimentos"],
        "Data": pd.to_datetime(r["Data"], errors="coerce").dt.tz_localize(None),
        "Recebido": r["Valor"],
        "Total da Medição": r["Total da Medição"],
        "Cliente": r["Cliente ou Fornecedor (Razão Social)"],
        "Documento": r["Número do Documento"],
        "Observação": r["Observação"],
        "Receita": r["TipoReceita"],
        "Categoria": r["Categoria"],
        "Departamento": r["Departamento"],
        "Projeto": r["Projeto"],
        "Conta Corrente": r["Conta Corrente"],
        "Juros": r["Juros"],
        "Multa": r["Multa"],
        "Desconto": r["Desconto"],
        "Valor do Movimento": r["Valor do Movimento"],
        "Rateio %": r["Rateio %"],
        "Situacao": r["Situação"],
        "Origem": r["Origem"],
        "Codigo Omie": r["Código Omie"],
    })
    return out.reset_index(drop=True)


def bloco_aportes():
    """
    Conta corrente de aportes, dentro da propria tela da DRE. Usa os MESMOS filtros
    da barra lateral (obra, ano, etc.), entao ao olhar o resultado de uma obra voce
    ve, logo abaixo, quem bancou o caixa dela.

    Aportes nao entram no resultado — sao fluxo. Por isso ficam num bloco separado,
    e nao somados a nenhuma linha da DRE.
    """
    por_socio, por_obra, det, por_tipo, div = construir_aportes(dff)
    st.markdown("---")
    st.subheader("Aportes e devolucoes (fluxo, fora do resultado)")
    if por_socio is None:
        st.info("Nenhum lancamento de aporte/devolucao no filtro atual. "
                "Se voce esperava ver aportes aqui, confira se as categorias do plano "
                "financeiro batem com a lista TIPOS_APORTE do fonte_dados.py "
                "(rode: python diagnostico.py --aportes).")
        # Sem aporte a obra ainda pode ter resultado e dividendo — a secao continua.
        bloco_resultado_dividendos(0.0, None)
        return

    tot_ap = por_socio["Aportado"].sum()
    tot_dev = por_socio["Devolvido"].sum()
    c1, c2, c3 = st.columns(3)
    c1.metric("Aportado", brl(tot_ap))
    c2.metric("Devolvido", brl(tot_dev))
    c3.metric("Saldo em aberto", brl(tot_ap - tot_dev))
    st.caption("Saldo = aportado - devolvido, em CAIXA (so o que entrou/saiu de fato). "
               "Valor positivo = o socio/parceiro ainda tem dinheiro na obra.")

    abas = ["Por socio/parceiro", "Por obra", "Por tipo", "Lancamentos"]
    if div is not None:
        abas.append("Dividendos")
    _tabs = st.tabs(abas)
    t1, t2, t4, t3 = _tabs[0], _tabs[1], _tabs[2], _tabs[3]
    if div is not None:
        with _tabs[4]:
            v = div.copy()
            st.dataframe(
                v.assign(**{c: v[c].apply(brl) for c in ("Recebido", "Pago", "Liquido")}),
                use_container_width=True, hide_index=True)
            st.caption("Dividendo e distribuicao de LUCRO — NAO abate o saldo de aporte. "
                       "Para somar tudo num numero so, inclua 'Dividendos' em "
                       "TIPOS_NO_SALDO no fonte_dados.py.")
    with t1:
        v = por_socio.copy()
        v["% do aportado"] = (v["Aportado"] / tot_ap * 100).round(1) if tot_ap else 0.0
        st.dataframe(
            v.assign(**{c: v[c].apply(brl) for c in ("Aportado", "Devolvido", "Saldo")}),
            use_container_width=True, hide_index=True)
    with t2:
        v = por_obra.copy()
        st.dataframe(
            v.assign(**{c: v[c].apply(brl)
                        for c in ("Aportado", "Devolvido", "Saldo", "Falta p/ igualar")}),
            use_container_width=True, hide_index=True)
        st.caption("'Falta p/ igualar' compara cada um com o MAIOR aportador da obra. "
                   "E so uma referencia de igualdade: o sistema nao conhece a quota "
                   "acordada entre voces.")
    with t4:
        v = por_tipo.copy()
        st.dataframe(
            v.assign(**{c: v[c].apply(brl) for c in ("Aportado", "Devolvido", "Saldo")}),
            use_container_width=True, hide_index=True)
        st.caption("Aporte BWS = recurso proprio alocado a obra. Aporte de Parceiro = "
                   "dinheiro de terceiro. Devolucao = retorno ao aportador.")
    with t3:
        v = det.copy()
        v["Data"] = pd.to_datetime(v["Data"], errors="coerce").dt.strftime("%d/%m/%Y")
        v["Valor"] = v["PagoRecebido"].apply(brl)
        st.dataframe(v[["Data", "Obra", "Socio", "TipoAporte", "Categoria", "Valor",
                        "Conta Corrente"]],
                     use_container_width=True, hide_index=True, height=320)

    bloco_resultado_dividendos(tot_ap - tot_dev, por_socio)


def bloco_resultado_dividendos(aporte_liquido, por_socio=None):
    """Resultado x dividendo: de onde saiu e quanto ainda haveria a distribuir."""
    tab, resumo = construir_resultado_dividendos(dff)
    st.markdown("---")
    st.subheader("Resultado x dividendos")
    if tab is None:
        st.info("Sem dados de resultado ou dividendo no filtro atual.")
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Resultado realizado", brl(resumo["resultado"]))
    c2.metric("Dividendos pagos", brl(resumo["dividendos"]))
    c3.metric("Disponivel", brl(resumo["disponivel"]))
    c4.metric("Aporte liquido na obra", brl(aporte_liquido))
    st.caption("Disponivel = resultado realizado (caixa) - dividendos ja pagos. "
               "Dividendo NAO e despesa: sai do fluxo, nao da DRE, e por isso nao "
               "aparece em nenhuma linha de custo. Este numero e gerencial — o que "
               "pode de fato ser distribuido depende do lucro contabil apurado e das "
               "regras do contrato social.")
    if resumo["disponivel"] < 0:
        st.warning(f"Foi distribuido {brl(-resumo['disponivel'])} a mais do que o "
                   "resultado realizado no filtro atual. Verifique se o periodo "
                   "selecionado cobre o exercicio em que o lucro foi gerado.")
    v = tab.copy()
    st.dataframe(
        v.assign(**{c: v[c].apply(brl)
                    for c in ("Resultado", "Dividendos pagos", "Disponivel")}),
        use_container_width=True, hide_index=True)

    # Quanto cada socio "poderia" retirar — hipotese proporcional ao aporte.
    if por_socio is not None and not por_socio.empty and resumo["disponivel"] > 0:
        base = por_socio[por_socio["Saldo"] > 0]
        tot = base["Saldo"].sum()
        if tot > 0:
            h = base[["Socio", "Saldo"]].copy()
            h["% do aporte"] = (h["Saldo"] / tot * 100).round(1)
            h["Se proporcional ao aporte"] = (h["Saldo"] / tot * resumo["disponivel"])
            st.markdown("**Hipotese de distribuicao proporcional ao aporte:**")
            st.dataframe(
                h.assign(Saldo=h["Saldo"].apply(brl),
                         **{"Se proporcional ao aporte":
                            h["Se proporcional ao aporte"].apply(brl)}),
                use_container_width=True, hide_index=True)
            st.caption("Simulacao: reparte o disponivel na proporcao do aporte de cada "
                       "um. O sistema NAO conhece a quota acordada entre voces — se a "
                       "divisao contratada for outra, use isto so como referencia.")


def pagina_dre():
    st.title("DRE - Executado / Em aberto / Comprometido")
    st.caption("Receita liquida = valor recebido pela BWS (base). Retencoes reconstroem o bruto faturado. "
               "Executado/Em aberto sao separados pela Situacao do titulo. A data (coluna F) traz o "
               "pagamento quando houve, senao o vencimento.")
    base = dff[dff[ANALISE] == "DRE"]
    rec = base[base["Tipo"] == REC]
    pag = base[base["Tipo"] == PAG]
    ret = rec["IsRetido"]
    rl_e, rl_a = rec.loc[~ret, "Executado"].sum(), rec.loc[~ret, "EmAberto"].sum()
    rt_e, rt_a = rec.loc[ret, "Executado"].sum(), rec.loc[ret, "EmAberto"].sum()
    rb_e, rb_a = rl_e + rt_e, rl_a + rt_a

    linhas = [
        linha3("Receita Bruta de Servicos", rb_e, rb_a),
        linha3("(-) Retencoes na fonte", -rt_e, -rt_a),
        linha3("= Receita Liquida", rl_e, rl_a),
        {"Linha": "", "Executado": "", "Em aberto": "", "Comprometido": ""},
    ]
    for grupo, g in pag.groupby("Grupo"):
        linhas.append(linha3("  " + str(grupo), g["Executado"].sum(), g["EmAberto"].sum()))
    enc = pag["Encargo"].sum()  # juros + multa pagos (despesa financeira)
    if abs(enc) > 0.005:
        linhas.append(linha3("  Juros e Multas Pagos", enc, 0.0))
    desp_e, desp_a = pag["Executado"].sum() + enc, pag["EmAberto"].sum()
    linhas.append(linha3("= Total Custos/Despesas", desp_e, desp_a))
    linhas.append({"Linha": "", "Executado": "", "Em aberto": "", "Comprometido": ""})
    linhas.append(linha3("= RESULTADO", rl_e + desp_e, rl_a + desp_a))
    dre_df = pd.DataFrame(linhas)

    c1, c2, c3 = st.columns(3)
    c1.metric("Receita Liquida (comp.)", brl(rl_e + rl_a))
    c2.metric("(+) Retencoes na fonte", brl(rt_e + rt_a))
    c3.metric("(=) Receita Bruta", brl(rb_e + rb_a))
    c4, c5 = st.columns(2)
    c4.metric("Despesas (comp.)", brl(desp_e + desp_a))
    c5.metric("Resultado (comp.)", brl(rl_e + rl_a + desp_e + desp_a))
    st.dataframe(dre_df, use_container_width=True, hide_index=True, height=520)

    bloco_aportes()

    # ---------- Resultado mensal: Executado x Comprometido ----------
    st.markdown("---")
    st.subheader("Fluxo Financeiro")
    medm = st.radio("Medida do grafico",
                    ["Executado (realizado)", "Comprometido (realizado + em aberto)"],
                    horizontal=True, key="dre_medm")
    is_exec = medm.startswith("Executado")
    if is_exec:
        pm = base[base["Pago"]].copy()
        valcol = "PagoRecebido"
        rotulo = "Executado"
        st.caption("Receitas e despesas REALIZADAS, pelo mes de pagamento/recebimento (sem aportes). "
                   "Resultado acumulado mostra a geracao de caixa ja efetivada.")
    else:
        pm = base[base["Data"].notna()].copy()
        valcol = "Comprometido"
        rotulo = "Comprometido"
        st.caption("Receitas e despesas COMPROMETIDAS (realizado + em aberto), alocadas pelo mes da data "
                   "(pagamento quando houve, senao vencimento). Mostra a geracao de caixa projetada do "
                   "comprometido global. (Itens em aberto so entram no mes correto apos regerar a base com "
                   "o vencimento na coluna F.)")
    if pm.empty:
        st.info("Sem lancamentos para os filtros atuais.")
    else:
        pm["AnoMes"] = pm["Data"].dt.to_period("M").astype(str)
        rec_m = pm[(pm["Tipo"] == REC) & (~pm["IsRetido"])].groupby("AnoMes")[valcol].sum()
        desp_m = pm[pm["Tipo"] == PAG].groupby("AnoMes")[valcol].sum()
        mr = pd.concat([rec_m.rename("Receita"), desp_m.rename("Despesa")], axis=1).fillna(0.0)
        mr = mr.reset_index().sort_values("AnoMes")
        mr["Resultado"] = mr["Receita"] + mr["Despesa"]
        mr["Acumulado"] = mr["Resultado"].cumsum()
        mr["MesData"] = pd.to_datetime(mr["AnoMes"] + "-01", errors="coerce")
        mr = mr.dropna(subset=["MesData"])
        figm = go.Figure()
        figm.add_bar(x=mr["MesData"], y=mr["Receita"], name=f"Receita ({rotulo})", marker_color="#2e7d32")
        figm.add_bar(x=mr["MesData"], y=mr["Despesa"], name=f"Despesa ({rotulo})", marker_color="#c62828")
        figm.add_scatter(x=mr["MesData"], y=mr["Acumulado"], name=f"Resultado acumulado ({rotulo})",
                         mode="lines+markers", line=dict(color="#1565c0", width=3))
        figm.update_layout(barmode="relative", height=460, separators=",.", xaxis_title="",
                           yaxis_title="R$", legend=dict(orientation="h", y=1.08, x=0),
                           margin=dict(t=30, b=0), hovermode="x unified")
        figm.update_xaxes(tickformat="%m/%Y")
        figm.update_yaxes(rangemode="tozero")
        figm.update_traces(hovertemplate="R$ %{y:,.2f}<extra></extra>")
        st.plotly_chart(figm, use_container_width=True)

    # ---------- Export ----------
    st.markdown("---")
    st.subheader("Exportar relatorio")
    cb1, cb2 = st.columns(2)
    if cb1.button("Gerar Excel (dados completos)", key="dre_xlsx_btn"):
        with st.spinner("Montando Excel..."):
            st.session_state["dre_xlsx"] = montar_xlsx(
                rec, pag, receb_f, construir_aportes(dff))
    if st.session_state.get("dre_xlsx"):
        cb1.download_button("Baixar Excel", st.session_state["dre_xlsx"],
                            file_name="relatorio_financeiro_bws.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="dre_xlsx_dl")
    if cb2.button("Gerar PDF (graficos + tabelas)", key="dre_pdf_btn"):
        try:
            import relatorio_pdf
            with st.spinner("Montando PDF (alguns segundos)..."):
                st.session_state["dre_pdf"] = relatorio_pdf.montar_pdf(rec, pag, dff)
        except ImportError:
            st.error("Para gerar o PDF instale as bibliotecas: pip install matplotlib reportlab")
        except Exception as e:
            st.error(f"Falha ao gerar PDF: {e}")
    if st.session_state.get("dre_pdf"):
        cb2.download_button("Baixar PDF", st.session_state["dre_pdf"],
                            file_name="relatorio_financeiro_bws.pdf",
                            mime="application/pdf", key="dre_pdf_dl")
    if receb_f is not None and not receb_f.empty:
        st.caption("A aba **Receita Analitico** abre cada medicao por recebimento: "
                   "uma linha por entrada, com a data e o valor exatos de cada parcela. "
                   "Soma so o que virou caixa — retencoes e saldo a receber ficam na "
                   "aba Receita de Obra.")
    else:
        st.caption("Aba 'Receita Analitico' indisponivel: rode o fonte_dados.py de novo "
                   "para gerar o dados_omie_recebimentos.parquet.")
    st.caption("Excel: dados completos sem paginacao (Despesas Analitico com a base inteira; abas DRE, "
               "Despesas Categoria, Top Credores, Receita de Obra, Receita Analitico, Outras Receitas, "
               "Despesas Analitico, Contas a Pagar). "
               "PDF: graficos + DRE + Despesas por Categoria + Top Credores + Receita de Obra + Contas a Pagar "
               "+ Despesas Analitico (tabelas longas limitadas a 2500 linhas; o detalhe completo fica no Excel). "
               "Ambos respeitam os filtros atuais; gere de novo se mudar os filtros.")

    # ---------- Abas analiticas ----------
    st.markdown("---")
    tab_desp, tab_rec, tab_cred = st.tabs(["Despesas", "Receitas", "Top Credores"])

    # ===== DESPESAS =====
    with tab_desp:
        kd1, kd2, kd3 = st.columns(3)
        kd1.metric("Pagos (executado)", brl(pag["Executado"].sum()))
        kd2.metric("A pagar (em aberto)", brl(pag["EmAberto"].sum()))
        kd3.metric("Comprometido", brl(pag["Comprometido"].sum()))
        st.markdown("")
        st.subheader("Despesas por Categoria")
        c1, c2 = st.columns(2)
        visao = c1.radio("Visao", ["Pagas (executado)", "A Pagar (em aberto)", "Comprometido"], key="dre_visao")
        por = c2.radio("Quebrar por", ["Categoria", "Grupo"], horizontal=True, key="dre_por")
        medida = {"Pagas (executado)": "Executado", "A Pagar (em aberto)": "EmAberto",
                  "Comprometido": "Comprometido"}[visao]
        chaves = ["Grupo", "Categoria"] if por == "Categoria" else ["Grupo"]
        d = pag.groupby(chaves)[medida].sum().reset_index()
        d = d[d[medida].abs() > 0.005]
        if d.empty:
            st.info("Sem valores para esta visao com os filtros atuais.")
        else:
            d["Valor"] = d[medida].abs()
            st.caption(f"{visao} - total {brl(d[medida].sum())}.")
            path = [px.Constant("Despesas")] + chaves
            fig = px.treemap(d, path=path, values="Valor", color="Valor",
                             color_continuous_scale="Oranges")
            fig.update_traces(texttemplate="%{label}<br>R$ %{value:,.2f}<br>%{percentParent}",
                              hovertemplate="%{label}<br>R$ %{value:,.2f}<extra></extra>")
            fig.update_layout(height=540, separators=",.", margin=dict(t=10, l=0, r=0, b=0),
                              coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True, key="tree_desp")

            st.markdown(f"**Clique numa linha abaixo para abrir o Analitico daquele(a) {por.lower()}:**")
            tab = d.sort_values("Valor", ascending=False).reset_index(drop=True)
            tab_show = tab[[por, medida]].copy()
            tab_show[medida] = tab_show[medida].apply(brl)
            tab_show.columns = [por, "Total"]
            ev_tab = st.dataframe(tab_show, use_container_width=True, hide_index=True, height=420,
                                  on_select="rerun", selection_mode="single-row", key="cat_tab")

            gset = set(pag["Grupo"].dropna().astype(str).unique())
            cset = set(pag["Categoria"].dropna().astype(str).unique())

            def _set_drill(label):
                label = str(label)
                if label in gset:
                    st.session_state["an_grupo"] = label
                    st.session_state["an_cat"] = "(Todas)"
                elif label in cset:
                    gr = pag.loc[pag["Categoria"].astype(str) == label, "Grupo"]
                    if len(gr):
                        st.session_state["an_grupo"] = str(gr.iloc[0])
                    st.session_state["an_cat"] = label
                else:
                    return
                st.toast(f"Analitico filtrado para: {label}")

            try:
                trows = ev_tab.selection.rows
            except Exception:
                trows = None
            if trows:
                lbl = tab.iloc[trows[0]][por]
                ssig = f"tab|{por}|{lbl}"
                if st.session_state.get("tab_last") != ssig:
                    st.session_state["tab_last"] = ssig
                    _set_drill(lbl)

        st.markdown("---")
        st.subheader("Despesas Analitico")
        st.caption("Lancamento a lancamento. Clique numa linha para ver o detalhe. "
                   "A coluna Link abre o card no Pipefy quando o Numero do Documento e SP/PM/DC + numero.")
        fa, fb, fc = st.columns(3)
        gopts = ["(Todos)"] + sorted(pag["Grupo"].dropna().unique())
        gsel = fa.selectbox("Grupo", gopts, key="an_grupo")
        base_an = pag if gsel == "(Todos)" else pag[pag["Grupo"] == gsel]
        copts = ["(Todas)"] + sorted(base_an["Categoria"].dropna().unique())
        if st.session_state.get("an_cat") not in copts:
            st.session_state["an_cat"] = "(Todas)"
        csel = fb.selectbox("Categoria", copts, key="an_cat")
        if csel != "(Todas)":
            base_an = base_an[base_an["Categoria"] == csel]
        vsel = fc.radio("Visao", ["Todos", "Pagas", "A Pagar"], horizontal=True, key="an_vis")
        if vsel == "Pagas":
            base_an = base_an[base_an["Pago"]]
        elif vsel == "A Pagar":
            base_an = base_an[~base_an["Pago"]]

        base_an = base_an.copy()
        base_an["Valor"] = base_an["PagoRecebido"].where(base_an["Pago"], base_an["APagarReceber"])
        base_an = base_an[base_an["Valor"].abs() > 0.005]

        termo = st.text_input(
            "Pesquisar", key="an_busca",
            placeholder="credor, CPF/CNPJ, numero do documento ou observacao").strip()
        if termo:
            cols_busca = ["Cliente ou Fornecedor (Razão Social)", "CNPJ/CPF",
                          "Número do Documento", "Observação da Conta"]
            mask = pd.Series(False, index=base_an.index)
            for cbu in cols_busca:
                if cbu in base_an.columns:
                    mask = mask | base_an[cbu].astype(str).str.contains(termo, case=False, na=False, regex=False)
            base_an = base_an[mask]

        so1, so2 = st.columns([2, 1])
        ordcol = so1.selectbox("Ordenar por",
                               ["Valor", "Data", "Cliente/Fornecedor", "Categoria",
                                "Departamento", "Situacao Vcto"], key="an_ord")
        ordasc = so2.radio("Ordem", ["Decrescente", "Crescente"], horizontal=True,
                           key="an_ord_dir") == "Crescente"
        st.caption("Ordenacao aplicada a base inteira (vale para todas as paginas). "
                   "Clicar no cabecalho da tabela so reordena a pagina atual.")
        _cmap = {"Cliente/Fornecedor": "Cliente ou Fornecedor (Razão Social)",
                 "Situacao Vcto": "SituacaoVencimento"}
        sc = _cmap.get(ordcol, ordcol)
        if ordcol == "Valor":
            base_an = base_an.reindex(base_an["Valor"].abs().sort_values(ascending=ordasc).index)
        else:
            base_an = base_an.sort_values(sc, ascending=ordasc, na_position="last", kind="stable")
        base_an = base_an.reset_index(drop=True)

        PG = 500
        total_lin = len(base_an)
        n_pag = max(1, (total_lin + PG - 1) // PG)
        cpa, cpb = st.columns([1, 3])
        pag_n = cpa.number_input("Pagina", min_value=1, max_value=n_pag, value=1, step=1, key="an_pag")
        cpb.caption(f"Pagina {pag_n} de {n_pag} — {total_lin:,} lancamentos ({PG} por pagina).")
        ini = (int(pag_n) - 1) * PG
        fatia = base_an.iloc[ini:ini + PG].reset_index(drop=True)

        if fatia.empty:
            st.info("Sem lancamentos para esses filtros.")
        else:
            fatia["Link"] = fatia["Número do Documento"].apply(pipefy_link)
            show = pd.DataFrame({
                "Data": fatia["Data"].dt.strftime("%d/%m/%Y").fillna(""),
                "Cliente/Fornecedor": fatia["Cliente ou Fornecedor (Razão Social)"],
                "Valor": fatia["Valor"].apply(brl),
                "Categoria": fatia["Categoria"],
                "Departamento": fatia["Departamento"],
                "Situacao Vcto": fatia["SituacaoVencimento"],
                "Documento": fatia["Número do Documento"],
                "Link": fatia["Link"],
            })
            sel = st.dataframe(
                show, use_container_width=True, hide_index=True, height=440,
                on_select="rerun", selection_mode="single-row", key="an_tab",
                column_config={
                    "Valor": st.column_config.TextColumn("Valor (R$)"),
                    "Link": st.column_config.LinkColumn("Link", display_text="Abrir card"),
                },
            )
            rows = []
            try:
                rows = sel.selection.rows
            except Exception:
                rows = []
            if rows:
                idx = rows[0]
                recd = fatia.iloc[idx].to_dict()
                sig = f"{gsel}|{csel}|{vsel}|{termo}|{int(pag_n)}|{idx}"
                if st.session_state.get("an_detail_sig") != sig:
                    st.session_state["an_detail_sig"] = sig
                    dialog_detalhe(recd)

    # ===== RECEITAS (por medicao) =====
    with tab_rec:
        rec_obra, rec_outras = split_receitas(rec)

        st.subheader("Receita de Obra")
        st.caption("Cada linha e uma MEDICAO: agrupada por Numero do Documento quando existe, "
                   "senao pela Observacao da Conta. Bruto = liquido recebido + retencoes + a receber. "
                   "Clique numa linha para detalhar.")
        med = construir_medicoes(rec_obra)
        med = med[med["Bruto"].abs() > 0.005]
        med = med.reindex(med["Bruto"].abs().sort_values(ascending=False).index).reset_index(drop=True)
        fr1, _ = st.columns([2, 2])
        vis_r = fr1.radio("Visao", ["Todas", "Com saldo a receber", "Quitadas"],
                          horizontal=True, key="rec_vis")
        if vis_r == "Com saldo a receber":
            med_f = med[med["AReceber"].abs() > 0.005]
        elif vis_r == "Quitadas":
            med_f = med[med["AReceber"].abs() <= 0.005]
        else:
            med_f = med
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Bruto (medicoes)", brl(med_f["Bruto"].sum()))
        k2.metric("Recebido", brl(med_f["Recebido"].sum()))
        k3.metric("Retido", brl(med_f["Retido"].sum()))
        k4.metric("A receber", brl(med_f["AReceber"].sum()))

        PGR = 500
        tot_r = len(med_f)
        np_r = max(1, (tot_r + PGR - 1) // PGR)
        cra, crb = st.columns([1, 3])
        pgr = cra.number_input("Pagina ", min_value=1, max_value=np_r, value=1, step=1, key="rec_pag")
        crb.caption(f"Pagina {pgr} de {np_r} — {tot_r:,} medicoes ({PGR} por pagina).")
        ir = (int(pgr) - 1) * PGR
        fr = med_f.iloc[ir:ir + PGR].reset_index(drop=True)
        if fr.empty:
            st.info("Sem medicoes para esses filtros.")
        else:
            fr["Link"] = fr["Doc"].apply(pipefy_link)
            showr = pd.DataFrame({
                "Data": pd.to_datetime(fr["Data"], errors="coerce").dt.strftime("%d/%m/%Y").fillna(""),
                "Cliente": fr["Cliente"],
                "Departamento": fr["Departamento"],
                "Bruto": fr["Bruto"].apply(brl),
                "Recebido": fr["Recebido"].apply(brl),
                "Retido": fr["Retido"].apply(brl),
                "A Receber": fr["AReceber"].apply(brl),
                "Situacao": fr["SitSimpl"],
                "Link": fr["Link"],
            })
            selr = st.dataframe(
                showr, use_container_width=True, hide_index=True, height=440,
                on_select="rerun", selection_mode="single-row", key="rec_tab",
                column_config={"Link": st.column_config.LinkColumn("Link", display_text="Abrir card")},
            )
            rowsr = []
            try:
                rowsr = selr.selection.rows
            except Exception:
                rowsr = []
            if rowsr:
                idxr = rowsr[0]
                recm = fr.iloc[idxr].to_dict()
                sigr = f"obra|{vis_r}|{int(pgr)}|{idxr}"
                if st.session_state.get("rec_detail_sig") != sigr:
                    st.session_state["rec_detail_sig"] = sigr
                    dialog_medicao(recm)

        # ----- Outras Receitas -----
        o = rec_outras.copy()
        if not o.empty:
            o["Valor"] = o["PagoRecebido"].where(o["Pago"], o["APagarReceber"])
            o = o[o["Valor"].abs() > 0.005]
        if not o.empty:
            st.markdown("---")
            st.subheader("Outras Receitas")
            st.caption("Receitas que nao sao de obra (rendimentos, estornos, devolucoes etc.). "
                       "A categoria identifica do que se trata. Clique para detalhar.")
            o = o.reindex(o["Valor"].abs().sort_values(ascending=False).index).reset_index(drop=True)
            rec_o = o["PagoRecebido"].where(o["Pago"], 0.0).sum()
            arec_o = o["APagarReceber"].sum()
            ko1, ko2, ko3 = st.columns(3)
            ko1.metric("Recebido", brl(rec_o))
            ko2.metric("A receber", brl(arec_o))
            ko3.metric("Lancamentos", f"{len(o):,}")
            PGO = 500
            tot_o = len(o)
            np_o = max(1, (tot_o + PGO - 1) // PGO)
            coa, cob = st.columns([1, 3])
            pgo = coa.number_input("Pagina   ", min_value=1, max_value=np_o, value=1, step=1, key="out_pag")
            cob.caption(f"Pagina {pgo} de {np_o} — {tot_o:,} lancamentos ({PGO} por pagina).")
            io_ = (int(pgo) - 1) * PGO
            fo = o.iloc[io_:io_ + PGO].reset_index(drop=True)
            fo["Link"] = fo["Número do Documento"].apply(pipefy_link)
            showo = pd.DataFrame({
                "Data": fo["Data"].dt.strftime("%d/%m/%Y").fillna(""),
                "Cliente": fo["Cliente ou Fornecedor (Razão Social)"],
                "Categoria": fo["Categoria"],
                "Departamento": fo["Departamento"],
                "Valor": fo["Valor"].apply(brl),
                "Situacao": fo["Situação"],
                "Link": fo["Link"],
            })
            selo = st.dataframe(
                showo, use_container_width=True, hide_index=True, height=360,
                on_select="rerun", selection_mode="single-row", key="out_tab",
                column_config={"Link": st.column_config.LinkColumn("Link", display_text="Abrir card")},
            )
            rowso = []
            try:
                rowso = selo.selection.rows
            except Exception:
                rowso = []
            if rowso:
                idxo = rowso[0]
                reco = fo.iloc[idxo].to_dict()
                reco["Link"] = pipefy_link(reco.get("Número do Documento"))
                sigo = f"outra|{int(pgo)}|{idxo}"
                if st.session_state.get("out_detail_sig") != sigo:
                    st.session_state["out_detail_sig"] = sigo
                    dialog_detalhe(reco)

    # ===== TOP CREDORES =====
    with tab_cred:
        st.subheader("Top Credores")
        st.caption("Credores agrupados por Cliente ou Fornecedor (Razao Social). "
                   "Valor = comprometido (pago + a pagar) da despesa. Respeita os filtros da barra lateral.")
        tc = pag.groupby("Cliente ou Fornecedor (Razão Social)")["Comprometido"].sum().reset_index()
        tc.columns = ["Credor", "Valor"]
        tc = tc[tc["Valor"].abs() > 0.005]
        tot = tc["Valor"].sum()
        tc = tc.reindex(tc["Valor"].abs().sort_values(ascending=False).index).reset_index(drop=True)
        tc["pct"] = (tc["Valor"] / tot * 100) if tot else 0.0
        k1, k2 = st.columns(2)
        k1.metric("Total despesa (comprometido)", brl(tot))
        k2.metric("Credores", f"{len(tc):,}")
        PGC = 100
        totc = len(tc)
        npc = max(1, (totc + PGC - 1) // PGC)
        cca, ccb = st.columns([1, 3])
        pgc = cca.number_input("Pagina  ", min_value=1, max_value=npc, value=1, step=1, key="cred_pag")
        ccb.caption(f"Pagina {pgc} de {npc} — {totc:,} credores ({PGC} por pagina).")
        ic = (int(pgc) - 1) * PGC
        fcred = tc.iloc[ic:ic + PGC].reset_index(drop=True)
        showc = pd.DataFrame({
            "Credor": fcred["Credor"],
            "Valor": fcred["Valor"].apply(brl),
            "% do Total": fcred["pct"].map(lambda x: f"{x:.2f}%".replace(".", ",")),
        })
        st.dataframe(showc, use_container_width=True, hide_index=True, height=460)


def pagina_fluxo():
    st.title("Fluxo de Caixa (executado)")
    st.caption("Entradas e saidas mensais (titulos pagos/recebidos) e a geracao de caixa acumulada no tempo. "
               "As retencoes na fonte (IR/INSS/ISS) nao entram como caixa - sao retidas pelo cliente; "
               "entra apenas o liquido recebido.")
    base = dff[dff["Pago"] & ~((dff["Tipo"] == REC) & dff["IsRetido"])].copy()
    if base.empty:
        st.info("Sem lancamentos pagos/recebidos para os filtros atuais.")
        return
    base["AnoMes"] = base["Data"].dt.to_period("M").astype(str)
    mensal = base.groupby("AnoMes").agg(
        Entradas=("PagoRecebido", lambda s: s[s > 0].sum()),
        Saidas=("PagoRecebido", lambda s: s[s < 0].sum()),
        Liquido=("PagoRecebido", "sum"),
    ).reset_index().sort_values("AnoMes")
    mensal["Acumulado"] = mensal["Liquido"].cumsum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Entradas", brl(mensal["Entradas"].sum()))
    c2.metric("Saidas", brl(mensal["Saidas"].sum()))
    c3.metric("Geracao de caixa (liquido)", brl(mensal["Liquido"].sum()))

    mensal["MesData"] = pd.to_datetime(mensal["AnoMes"] + "-01", errors="coerce")
    fig = go.Figure()
    fig.add_bar(x=mensal["MesData"], y=mensal["Entradas"], name="Entradas", marker_color="#2e7d32")
    fig.add_bar(x=mensal["MesData"], y=mensal["Saidas"], name="Saidas", marker_color="#c62828")
    fig.add_scatter(x=mensal["MesData"], y=mensal["Acumulado"], name="Caixa acumulado",
                    mode="lines+markers", line=dict(color="#1565c0", width=3))
    fig.update_layout(barmode="relative", height=470, separators=",.", xaxis_title="",
                      legend=dict(orientation="h", y=1.08, x=0), margin=dict(t=30, b=0),
                      hovermode="x unified", yaxis=dict(tickprefix="R$ ", rangemode="tozero"))
    fig.update_xaxes(tickformat="%m/%Y")
    fig.update_traces(hovertemplate="R$ %{y:,.2f}<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)

    tb = mensal.drop(columns=["MesData"]).copy()
    for c in ["Entradas", "Saidas", "Liquido", "Acumulado"]:
        tb[c] = tb[c].apply(brl)
    tb.columns = ["Mes", "Entradas", "Saidas", "Liquido", "Caixa acumulado"]
    st.dataframe(tb, use_container_width=True, hide_index=True, height=320)


def pagina_resultado():
    st.title("Resultado por Obra / Projeto")
    nivel = st.radio("Agrupar por", ["Projeto", "Departamento"], horizontal=True)
    medida = st.radio("Medida", ["Executado", "Comprometido"], horizontal=True)
    st.caption("Receita LIQUIDA (sem as retencoes) e despesas, na mesma base da DRE. "
               "Resultado = Receita liquida + Despesa.")
    base = dff[dff[ANALISE] == "DRE"]
    g = base.groupby(nivel).apply(
        lambda x: pd.Series({
            "Receita": x.loc[(x["Tipo"] == REC) & (~x["IsRetido"]), medida].sum(),
            "Despesa": x.loc[x["Tipo"] == PAG, medida].sum(),
        }), include_groups=False
    ).reset_index()
    g["Resultado"] = g["Receita"] + g["Despesa"]
    g = g.sort_values("Resultado", ascending=False)
    top = g.head(25)
    fig = px.bar(top, x="Resultado", y=nivel, orientation="h", color="Resultado",
                 color_continuous_scale=["#c62828", "#cccccc", "#2e7d32"], text_auto=".2s")
    fig.update_layout(height=min(700, 80 + 26 * len(top)), separators=",.", yaxis_title="",
                      xaxis=dict(title="Resultado " + medida + " (R$)", tickprefix="R$ "))
    fig.update_yaxes(autorange="reversed")
    fig.update_traces(texttemplate="R$ %{x:,.0f}", hovertemplate="R$ %{x:,.2f}<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)
    show = g.copy()
    for c in ["Receita", "Despesa", "Resultado"]:
        show[c] = show[c].apply(brl)
    st.dataframe(show, use_container_width=True, hide_index=True, height=420)


def pagina_comp_exec():
    st.title("Comprometido vs Executado")
    st.caption("Executado = pago/recebido. A executar = saldo em aberto (A Pagar/Receber).")
    nivel = st.radio("Agrupar por", ["Projeto", "Departamento"], horizontal=True)
    tipo = st.radio("Tipo", [PAG, REC], horizontal=True,
                    format_func=lambda x: "Pagar" if "Pagar" in x else "Receber")
    base = dff[dff["Tipo"] == tipo]
    g = base.groupby(nivel).agg(Executado=("Executado", "sum"),
                                AExecutar=("EmAberto", "sum")).reset_index()
    g["Comprometido"] = g["Executado"] + g["AExecutar"]
    g["% Exec"] = (g["Executado"] / g["Comprometido"] * 100).where(g["Comprometido"] != 0, 0)
    g = g.reindex(g["Comprometido"].abs().sort_values(ascending=False).index).head(25)
    fig = go.Figure()
    fig.add_bar(y=g[nivel], x=g["Executado"], name="Executado", orientation="h", marker_color="#1565c0")
    fig.add_bar(y=g[nivel], x=g["AExecutar"], name="A executar", orientation="h", marker_color="#f9a825")
    fig.update_layout(barmode="stack", height=min(700, 80 + 26 * len(g)), separators=",.",
                      yaxis_title="", xaxis=dict(title="R$", tickprefix="R$ "))
    fig.update_yaxes(autorange="reversed")
    fig.update_traces(hovertemplate="R$ %{x:,.2f}<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)
    show = g.copy()
    show["% Exec"] = show["% Exec"].apply(lambda v: f"{v:,.0f}%")
    for c in ["Executado", "AExecutar", "Comprometido"]:
        show[c] = show[c].apply(brl)
    st.dataframe(show, use_container_width=True, hide_index=True, height=420)



# =============================================================================
# NECESSIDADE DE CAIXA — SIMULACAO POR CONJUNTO DE OBRAS (base caixa)
# Voce escolhe QUAIS obras entram; a pagina mostra o acumulado LIQUIDO desse conjunto
# (positivos e negativos se anulando) e, por cima, as TOMADAS de emprestimo como eventos.
# Nao ha "sobra das outras" nem veredito automatico: so o fato, para simular e comparar.
# =============================================================================
def _nc_base(base):
    d = base[base["Data"].notna() & (base["Executado"].abs() > 0.005)].copy()
    d["Mes"] = d["Data"].dt.to_period("M")
    todos = pd.period_range(d["Mes"].min(), d["Mes"].max(), freq="M")
    op = d[(d[ANALISE] == "DRE") & ~((d["Tipo"] == REC) & d["IsRetido"])]
    mensal = (op.groupby(["Mes", "Departamento"])["Executado"].sum()
                .unstack().astype(float).fillna(0.0).reindex(todos).fillna(0.0))
    fx = d[d[ANALISE] == "Fluxo de Caixa"]
    cat = fx["Categoria"].astype(str)
    e_emp = cat.str.contains(r"Empr[eé]st", case=False, regex=True)
    emp_in = fx[e_emp & (fx["Executado"] > 0)]          # tomadas de emprestimo
    emp_out = fx[e_emp & (fx["Executado"] < 0)]         # principal pago
    apo_in = fx[cat.str.contains("Aporte", case=False) & (fx["Executado"] > 0)]
    fin_out = fx[(cat.str.contains("Aporte", case=False) & (fx["Executado"] < 0))
                 | cat.str.contains("Dividendo", case=False)]
    e_apo = cat.str.contains("Aporte", case=False) | cat.str.contains("Dividendo", case=False)
    e_apl = cat.str.contains(r"Aplica|Resgate", case=False, regex=True)   # neutros p/ caixa total
    outros = fx[~e_emp & ~e_apo & ~e_apl]                                  # venda de ativos, aumento de capital...
    def _m(x):
        return x.groupby("Mes")["Executado"].sum().astype(float).reindex(todos).fillna(0.0)
    return op, mensal, todos, _m(emp_in), _m(emp_out), _m(apo_in), _m(fin_out), emp_in, _m(outros)


def _nc_acumulado(mensal, obras, todos, extra=None):
    s = mensal[obras].sum(axis=1) if obras else pd.Series(0.0, index=todos)
    if extra is not None:
        s = s + extra
    return s.cumsum()


def _mes_br(p):
    """Period mensal -> MM/AAAA."""
    try:
        return p.strftime("%m/%Y")
    except Exception:
        return str(p)


def _md(v):
    """brl() para markdown do Streamlit: escapa o $ (senao vira LaTeX e embaralha o texto)."""
    return brl(v).replace("$", "\\$")


def _nc_opcoes(obras_all, proj_all):
    return [f"[Projeto] {p}" for p in proj_all] + [f"[Obra] {o}" for o in obras_all]


def _nc_pesos(tabela, dep2proj, obras_all):
    """tabela [Item, Pct] -> Series obra->peso (0..1). Projeto expande para suas obras.
    Se a mesma obra aparecer em mais de uma linha, os % somam (limitado a 100%)."""
    pesos = pd.Series(0.0, index=obras_all, dtype=float)
    if tabela is None or tabela.empty:
        return pesos
    for _, r in tabela.iterrows():
        item = str(r.get("Item") or "").strip()
        try:
            pct = float(r.get("Pct")) / 100.0
        except (TypeError, ValueError):
            pct = 1.0
        if not item or item == "None" or pct <= 0:
            continue
        if item.startswith("[Projeto] "):
            p = item[len("[Projeto] "):]
            for o in obras_all:
                if str(dep2proj.get(o, "")) == p:
                    pesos[o] += pct
        elif item.startswith("[Obra] "):
            o = item[len("[Obra] "):]
            if o in pesos.index:
                pesos[o] += pct
    return pesos.clip(0.0, 1.0)


def _nc_editor(rotulo, key, opcoes):
    st.markdown(rotulo)
    vazio = pd.DataFrame({"Item": pd.Series(dtype="object"), "Pct": pd.Series(dtype="float")})
    ed = st.data_editor(
        vazio, num_rows="dynamic", use_container_width=True, hide_index=True, key=key,
        column_config={
            "Item": st.column_config.SelectboxColumn("Obra ou Projeto", options=opcoes, required=True, width="large"),
            "Pct": st.column_config.NumberColumn("% que entra", min_value=0.0, max_value=100.0, step=5.0,
                                                 default=100.0, format="%.0f%%"),
        })
    return ed


def _nc_leitura(meses, ac_a, ac_resto, fin_ac, ac_total, ac_b, tom_m, b_compl, inc_fin, caixa_rec=None):
    """Gera a leitura em portugues, com os numeros, a partir das series (o que se faz 'no olho')."""
    L = []
    idx = list(meses)
    # 1) empresa: janelas no vermelho + emprestimos nelas
    neg_emp = ac_total < -0.5
    if neg_emp.any():
        runs, ini, prev = [], None, False
        for m, v in zip(idx, neg_emp.values):
            if v and not prev: ini = m
            if (not v) and prev: runs.append((ini, m - 1))
            prev = v
        if prev: runs.append((ini, idx[-1]))
        runs.sort(key=lambda r: (r[1].ordinal - r[0].ordinal), reverse=True)
        a0, a1 = runs[0]
        jan = ac_total[(ac_total.index >= a0) & (ac_total.index <= a1)]
        pior_emp = jan.idxmin()
        tom_jan = tom_m[(tom_m.index >= a0) & (tom_m.index <= a1)]
        n_tom = int((tom_jan.abs() > 0.5).sum())
        L.append(f"**A empresa precisou do banco.** De **{_mes_br(a0)}** a **{_mes_br(a1)}** o caixa gerado pela empresa (sem empr\u00e9stimo) "
                 f"ficou abaixo de zero, chegando a **{_md(jan.min())}** em {_mes_br(pior_emp)}. Nesse per\u00edodo entraram "
                 f"**{n_tom} tomadas** de empr\u00e9stimo somando **{_md(tom_jan.sum())}**"
                 + (f" (h\u00e1 {len(runs) - 1} outra(s) janela(s) negativa(s) menores)." if len(runs) > 1 else "."))
        # quem puxava no pior mes da empresa: ranqueia A, resto e dividendos
        va, vr, vf = ac_a[pior_emp], ac_resto[pior_emp], fin_ac[pior_emp]
        contrib = [("o conjunto A", va), ("o resto das obras", vr)]
        if inc_fin:
            contrib.append(("os dividendos/devolu\u00e7\u00f5es j\u00e1 pagos (l\u00edquido de aportes)", vf))
        negs = sorted([c for c in contrib if c[1] < -0.5], key=lambda c: c[1])
        poss = [c for c in contrib if c[1] > 0.5]
        if negs:
            partes = " e ".join(f"**{n}** ({_md(v)})" for n, v in negs)
            seg = f", enquanto " + ", ".join(f"{n} estava em {_md(v)}" for n, v in poss) if poss else ""
            L.append(f"**Quem cavou o buraco** no pior m\u00eas ({_mes_br(pior_emp)}): {partes}{seg}. "
                     + ("Sem o A a empresa teria ficado positiva." if (va < -0.5 and (vr + vf) >= 0) else ""))
        # como se manteve
        L.append(f"**Como a empresa se manteve:** o que ela gerou sozinha chegou a {_md(jan.min())}, e os "
                 f"**{_md(tom_jan.sum())}** de empr\u00e9stimo tomados na janela cobriram esse gap \u2014 o caixa real "
                 f"\u2248 linha preta + empr\u00e9stimos. Ressalva: o acumulado parte de zero em {_mes_br(idx[0])} "
                 f"(in\u00edcio dos dados); se havia caixa antes, todas as linhas sobem esse valor.")
    else:
        L.append("**A empresa n\u00e3o precisou do banco** neste per\u00edodo: o caixa gerado por ela mesma nunca ficou abaixo de zero.")
    # 2) A: perfil
    neg_a = ac_a[ac_a < -0.5]
    if len(neg_a):
        L.append(f"**Conjunto A** ficou no vermelho em **{len(neg_a)} de {len(idx)} meses**, fundo de **{_md(neg_a.min())}** em {_mes_br(neg_a.idxmin())}, "
                 f"e termina em **{_md(ac_a.iloc[-1])}**" + (" \u2014 nunca voltou a ficar positivo." if ac_a.iloc[-1] < 0 else "."))
    else:
        L.append(f"**Conjunto A** nunca ficou no vermelho; termina em **{_md(ac_a.iloc[-1])}**.")
    # 3) resto / B
    L.append(f"**Resto das obras** termina em **{_md(ac_resto.iloc[-1])}** (pico {_md(ac_resto.max())} em {_mes_br(ac_resto.idxmax())})."
             + (f" **Conjunto B (manual)** termina em **{_md(ac_b.iloc[-1])}**." if ac_b is not None else ""))
    if inc_fin and abs(fin_ac.iloc[-1]) > 0.5:
        L.append(f"**Aportes \u2212 dividendos** acumulam **{_md(fin_ac.iloc[-1])}**: "
                 + ("a empresa distribuiu/devolveu mais do que recebeu de aportes \u2014 isso reduz o caixa que as obras geraram."
                    if fin_ac.iloc[-1] < 0 else "entrou mais aporte do que saiu em dividendos."))
    # 4) teste do caixa reconstruido (deveria ser sempre >= 0)
    if caixa_rec is not None:
        negc = caixa_rec[caixa_rec < -0.5]
        if len(negc):
            L.append(f"**Teste do caixa reconstru\u00eddo: FALHOU em {len(negc)} m\u00eas(es)** \u2014 fundo de **{_md(negc.min())}** "
                     f"em {_mes_br(negc.idxmin())}. Caixa n\u00e3o fica negativo na vida real: nesses meses entrou dinheiro de "
                     f"uma fonte que o Omie n\u00e3o classifica (empr\u00e9stimo em outra categoria, cheque especial, antecipa\u00e7\u00e3o, "
                     f"saldo inicial maior). \u00c9 exatamente onde investigar. Termina em **{_md(caixa_rec.iloc[-1])}**.")
        else:
            L.append(f"**Teste do caixa reconstru\u00eddo: passou** \u2014 nunca ficou negativo (m\u00ednimo {_md(caixa_rec.min())} "
                     f"em {_mes_br(caixa_rec.idxmin())}); termina em **{_md(caixa_rec.iloc[-1])}**. As fontes de dinheiro "
                     f"capturadas explicam o caixa, ao menos no agregado.")
    # 5) fechamento
    L.append(f"**Empresa inteira** termina em **{_md(ac_total.iloc[-1])}** = A ({_md(ac_a.iloc[-1])}) + resto ({_md(ac_resto.iloc[-1])})"
             + (f" + aportes\u2212dividendos ({_md(fin_ac.iloc[-1])})" if inc_fin else "") + ".")
    return L


def pagina_necessidade_caixa():
    st.title("Necessidade de Caixa \u2014 simula\u00e7\u00e3o")
    st.caption("Base **caixa**. Monte o **conjunto A** como uma receita: cada linha \u00e9 uma obra ou projeto e o "
               "**% que entra** (ex.: BWSCE 100% + BWS 50%). O **resto da empresa** \u00e9 tudo que n\u00e3o entrou em A "
               "(inclusive a fra\u00e7\u00e3o que sobrou). A linha **Empresa inteira** \u00e9 fixa (n\u00e3o muda com a simula\u00e7\u00e3o) e diz se a empresa toda precisou do banco; "
               "A e resto dizem **quem** estava puxando pra baixo e quem estava segurando.")
    op, mensal, todos, emp_in, emp_out, apo_in, fin_out, emp_rows, outros_m = _nc_base(df)
    if mensal.empty:
        st.warning("Sem dados suficientes."); return
    obras_all = sorted([c for c in mensal.columns if mensal[c].abs().sum() > 0.5])
    dep2proj = (df[df["Departamento"].isin(obras_all)].groupby("Departamento")["Projeto"]
                  .agg(lambda s: s.dropna().astype(str).mode().iloc[0] if len(s.dropna()) else ""))
    proj_all = sorted(p for p in dep2proj.unique() if str(p).strip() and str(p).lower() != "nan")
    opcoes = _nc_opcoes(obras_all, proj_all)

    ed_a = _nc_editor("**Conjunto A** \u2014 clique no + e adicione linhas: obra/projeto e o % que entra", "nc_ed_a", opcoes)
    pesos_a = _nc_pesos(ed_a, dep2proj, obras_all)

    b_compl = st.checkbox("Montar o B automaticamente como complemento de A (tudo que n\u00e3o entrou em A, "
                          "inclusive a fra\u00e7\u00e3o que sobrou)", value=False, key="nc_bcompl")
    if b_compl:
        pesos_b = (1.0 - pesos_a).clip(0.0, 1.0)
        st.caption("B = complemento de A. A composi\u00e7\u00e3o obra a obra est\u00e1 no expansor abaixo.")
    else:
        ed_b = _nc_editor("**Conjunto B** \u2014 clique no + e monte livremente (pode repetir itens de A com outro %)", "nc_ed_b", opcoes)
        pesos_b = _nc_pesos(ed_b, dep2proj, obras_all)
    _lab = [_mes_br(m) for m in todos]
    _lab2per = dict(zip(_lab, todos))
    ini = st.selectbox("A partir de", ["(tudo)"] + _lab, key="nc_ini")

    if pesos_a.sum() <= 0:
        st.info("Adicione ao menos uma linha no conjunto A (clique no + da tabela) para calcular."); return
    cf1, cf2 = st.columns([2, 1])
    inc_fin = cf1.checkbox("Mostrar aportes recebidos e dividendos/devolu\u00e7\u00f5es pagos (linha verde) na empresa inteira",
                           value=True, key="nc_fin")
    saldo_ini = cf2.number_input(f"Saldo de caixa em {_mes_br(todos[0])} (R$)", value=0.0, step=50000.0, format="%.2f",
                                 key="nc_saldo_ini",
                                 help="Caixa que a empresa tinha no in\u00edcio dos dados (conta + aplica\u00e7\u00f5es). "
                                      "Entra na linha 'Caixa reconstru\u00eddo'.")

    meses = todos if ini == "(tudo)" else todos[todos >= _lab2per[ini]]
    fin_m = (apo_in + fin_out) if inc_fin else pd.Series(0.0, index=todos)
    fin_ac = fin_m.cumsum().reindex(meses)                       # aportes - dividendos (acum.)
    pesos_resto = (1.0 - pesos_a).clip(0.0, 1.0)
    ac_a = mensal.mul(pesos_a, axis=1).sum(axis=1).cumsum().reindex(meses)
    ac_resto = mensal.mul(pesos_resto, axis=1).sum(axis=1).cumsum().reindex(meses)   # so obras
    ac_total = ac_a + ac_resto + fin_ac                            # empresa inteira
    # B: se e complemento, B == resto (nao desenha em dobro); se manual, desenha
    ac_b = None if b_compl else (mensal.mul(pesos_b, axis=1).sum(axis=1).cumsum().reindex(meses) if pesos_b.sum() > 0 else None)
    tom_m = emp_in.reindex(meses); tom_ac = emp_in.cumsum().reindex(meses); pag_ac = emp_out.cumsum().reindex(meses)
    emp_liq = tom_ac + pag_ac
    # CAIXA RECONSTRUIDO = obras + (aportes - dividendos) + emprestimos liquidos + outras fin. + saldo inicial
    obras_ac = ac_a + ac_resto
    fin_all = (apo_in + fin_out).cumsum().reindex(meses)
    outros_ac = outros_m.cumsum().reindex(meses)
    caixa_rec = obras_ac + fin_all + emp_liq + outros_ac + float(saldo_ini)

    def _proj_rot(o):
        p = str(dep2proj.get(o, "") or "").strip()
        return p if p and p.lower() != "nan" else "(sem projeto)"
    comp = pd.DataFrame({"Obra": obras_all, "Projeto": [_proj_rot(o) for o in obras_all],
                         "% em A": (pesos_a * 100).round(0).values, "% em B": (pesos_b * 100).round(0).values})
    _acum_fim = mensal.cumsum().iloc[-1]
    comp["Acumulado (caixa, at\u00e9 hoje)"] = [_acum_fim.get(o, 0.0) for o in obras_all]
    with st.expander("Composi\u00e7\u00e3o dos conjuntos (peso por obra)"):
        c_in = comp[(comp["% em A"] > 0) | (comp["% em B"] > 0)].copy()
        c_in["Acumulado (caixa, at\u00e9 hoje)"] = c_in["Acumulado (caixa, at\u00e9 hoje)"].map(brl)
        st.dataframe(c_in, use_container_width=True, hide_index=True)
    fora = comp[(comp["% em A"] <= 0) & (comp["% em B"] <= 0)].copy()
    n_sem = int((fora["Projeto"] == "(sem projeto)").sum())
    with st.expander(f"Fora de A e B \u2014 s\u00f3 no 'resto' ({len(fora)} obras/deptos; {n_sem} sem projeto)"):
        st.caption("Tudo que n\u00e3o entrou em nenhum conjunto: departamentos administrativos (matriz), obras "
                   "**sem projeto** e obras que voc\u00ea n\u00e3o listou. \u00c9 o que separa 'obras' de 'estrutura'. "
                   "Obra sem projeto = departamento no Omie sem projeto associado na planilha de projetos.")
        if fora.empty:
            st.info("Nada ficou de fora: A e B cobrem todas as obras/departamentos.")
        else:
            fora = fora.sort_values("Acumulado (caixa, at\u00e9 hoje)")
            tot_fora = fora["Acumulado (caixa, at\u00e9 hoje)"].sum()
            fora["Acumulado (caixa, at\u00e9 hoje)"] = fora["Acumulado (caixa, at\u00e9 hoje)"].map(brl)
            st.dataframe(fora[["Obra", "Projeto", "Acumulado (caixa, at\u00e9 hoje)"]], use_container_width=True, hide_index=True)
            st.caption(f"Soma do que est\u00e1 fora: **{_md(tot_fora)}** (acumulado at\u00e9 hoje).")

    neg = ac_a[ac_a < -0.5]
    pior = neg.idxmin() if len(neg) else None
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Meses no vermelho (A)", int(len(neg)))
    k2.metric("Pior posi\u00e7\u00e3o de A", brl(neg.min()) if len(neg) else brl(0), _mes_br(pior) if pior else "")
    k3.metric("Resto das obras no pior m\u00eas", brl(ac_resto[pior]) if pior else DASH)
    k4.metric("Empresa inteira no pior m\u00eas (sem empr\u00e9stimo)", brl(ac_total[pior]) if pior else DASH,
              help="Caixa gerado pela empresa toda (todas as obras). N\u00e3o muda com a simula\u00e7\u00e3o: \u00e9 a r\u00e9gua fixa. Negativo = a empresa precisou do banco.")
    if pior is not None:
        if ac_total[pior] < -0.5:
            st.warning(f"Em **{_mes_br(pior)}**: A estava em **{_md(neg.min())}**, o resto das obras em **{_md(ac_resto[pior])}** "
                       f"e aportes\u2212dividendos em **{_md(fin_ac[pior])}** \u2014 empresa **{_md(ac_total[pior])}**, negativa. "
                       f"O resto **n\u00e3o cobria**: foi o banco. "
                       f"Empr\u00e9stimo l\u00edquido at\u00e9 a\u00ed: **{_md(emp_liq[pior])}** (tomado {_md(tom_ac[pior])}, "
                       f"principal pago {_md(-pag_ac[pior])}).")
        else:
            st.success(f"Em **{_mes_br(pior)}**: A estava em **{_md(neg.min())}**, mas o resto das obras em **{_md(ac_resto[pior])}** "
                       f"e aportes\u2212dividendos em **{_md(fin_ac[pior])}** \u2014 empresa **{_md(ac_total[pior])}**, positiva. "
                       f"O resto **cobria** o buraco de A. "
                       f"Empr\u00e9stimo l\u00edquido at\u00e9 a\u00ed: **{_md(emp_liq[pior])}**.")
        if ac_b is not None:
            st.caption(f"Conjunto B em {_mes_br(pior)}: {_md(ac_b[pior])}.")
    if (tom_ac.iloc[-1] + pag_ac.iloc[-1]) < -0.5:
        st.caption("\u26a0 Principal pago maior que o tomado registrado: parte das tomadas pode estar em outra "
                   "categoria no Omie, ou o pagamento inclui juros. Trate o 'empr\u00e9stimo l\u00edquido' como aproxima\u00e7\u00e3o.")

    x = [_mes_br(m) for m in meses]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=tom_m.values, name="Empr\u00e9stimo tomado no m\u00eas", marker_color="#8e44ad", opacity=0.45))
    fig.add_trace(go.Scatter(x=x, y=ac_a.values, name="Conjunto A", mode="lines", line=dict(color="#c0392b", width=3)))
    fig.add_trace(go.Scatter(x=x, y=ac_resto.values, name=("Resto das obras (= B)" if b_compl else "Resto das obras"),
                             mode="lines", line=dict(color="#2980b9", width=2, dash="dot")))
    if inc_fin:
        fig.add_trace(go.Scatter(x=x, y=fin_ac.values, name="Aportes \u2212 dividendos (acum.)", mode="lines",
                                 line=dict(color="#16a085", width=1.5, dash="dashdot")))
    fig.add_trace(go.Scatter(x=x, y=ac_total.values, name="Empresa inteira (sem empr\u00e9stimo) \u2014 fixa", mode="lines", line=dict(color="#111111", width=2)))
    if ac_b is not None:
        fig.add_trace(go.Scatter(x=x, y=ac_b.values, name="Conjunto B (manual)", mode="lines", line=dict(color="#e67e22", width=2, dash="dash")))
    _cd = list(zip(obras_ac.values, fin_all.values, tom_ac.values, pag_ac.values, outros_ac.values,
                   [float(saldo_ini)] * len(meses)))
    fig.add_trace(go.Scatter(
        x=x, y=caixa_rec.values, name="CAIXA RECONSTRU\u00cdDO (deveria ser sempre \u2265 0)", mode="lines",
        line=dict(color="#d4a017", width=3), customdata=_cd,
        hovertemplate=("<b>Caixa reconstru\u00eddo \u2014 %{x}</b><br>"
                       "<b>%{y:,.0f}</b><br><br>"
                       "= obras (A + resto): %{customdata[0]:,.0f}<br>"
                       "+ aportes \u2212 dividendos: %{customdata[1]:,.0f}<br>"
                       "+ empr\u00e9stimos tomados: %{customdata[2]:,.0f}<br>"
                       "\u2212 principal pago: %{customdata[3]:,.0f}<br>"
                       "+ outras entradas/sa\u00eddas fin.: %{customdata[4]:,.0f}<br>"
                       "+ saldo inicial informado: %{customdata[5]:,.0f}<br>"
                       "<i>Aplica\u00e7\u00f5es/resgates e transfer\u00eancias entre contas<br>"
                       "ficam de fora (dinheiro da pr\u00f3pria empresa mudando de bolso).<br>"
                       "Se ficar negativo, h\u00e1 dinheiro entrando de fonte<br>"
                       "que o Omie n\u00e3o est\u00e1 classificando.</i><extra></extra>")))
    fig.add_hline(y=0, line_color="#7f8c8d", line_width=1)
    fig.update_layout(height=460, margin=dict(l=10, r=10, t=30, b=10), legend=dict(orientation="h", y=-0.22),
                      yaxis_tickformat=",.0f", barmode="overlay")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Leitura autom\u00e1tica")
    for frase in _nc_leitura(meses, ac_a, ac_resto, fin_ac, ac_total, ac_b, tom_m, b_compl, inc_fin, caixa_rec):
        st.markdown("\u2022 " + frase)

    tab = pd.DataFrame({"Mes": x, "Conjunto A": ac_a.values, "Resto das obras": ac_resto.values,
                        "Aportes \u2212 dividendos (acum.)": fin_ac.values,
                        "Empresa inteira (sem empr\u00e9stimo)": ac_total.values,
                        "CAIXA RECONSTRU\u00cdDO": caixa_rec.values,
                        "Empr\u00e9stimo tomado (m\u00eas)": tom_m.values, "Empr\u00e9stimo l\u00edquido (acum.)": emp_liq.values})
    if ac_b is not None:
        tab.insert(2, "Conjunto B", ac_b.values)
    show = tab.copy()
    for c in show.columns:
        if c != "Mes":
            show[c] = show[c].map(brl)
    st.dataframe(show, use_container_width=True, hide_index=True, height=min(60 + 35 * len(show), 600))

    if pior is not None:
        with st.expander(f"Obras de A em {_mes_br(pior)} (acumulado \u00d7 peso)"):
            snap = mensal.mul(pesos_a, axis=1).cumsum().loc[pior]
            snap = snap[pesos_a > 0].sort_values()
            st.dataframe(pd.DataFrame({"Obra": snap.index, "Projeto": [dep2proj.get(o, "") for o in snap.index],
                                       "% em A": (pesos_a[snap.index] * 100).round(0).values,
                                       "Acumulado (\u00d7 peso)": snap.map(brl).values}),
                         use_container_width=True, hide_index=True)
    with st.expander("Tomadas de empr\u00e9stimo (lan\u00e7amento a lan\u00e7amento)"):
        e = emp_rows[emp_rows["Mes"].isin(meses)].sort_values("Data")
        cols = [c for c in ["Data", "Departamento", "Categoria", "Cliente ou Fornecedor (Raz\u00e3o Social)",
                            "N\u00famero do Documento", "Executado"] if c in e.columns]
        ee = e[cols].copy()
        if "Data" in ee: ee["Data"] = ee["Data"].dt.strftime("%d/%m/%Y")
        ee["Executado"] = ee["Executado"].map(brl)
        st.dataframe(ee, use_container_width=True, hide_index=True)

    import io
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as wr:
        tab.to_excel(wr, sheet_name="Simulacao", index=False)
        comp.to_excel(wr, sheet_name="Composicao", index=False)
        money = wr.book.add_format({"num_format": 'R$ #,##0.00;[Red]-R$ #,##0.00'})
        ws = wr.sheets["Simulacao"]; ws.set_column(0, 0, 10); ws.set_column(1, 80, 20, money); ws.freeze_panes(1, 1)
    st.download_button("Baixar Excel da simula\u00e7\u00e3o", buf.getvalue(), file_name="necessidade_caixa_simulacao.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

PAGINAS = {"Visao Geral": pagina_visao_geral, "DRE": pagina_dre, "Fluxo de Caixa": pagina_fluxo,
           "Resultado por Obra/Projeto": pagina_resultado, "Comprometido vs Executado": pagina_comp_exec,
           "Necessidade de Caixa": pagina_necessidade_caixa}
PAGINAS[pagina]()