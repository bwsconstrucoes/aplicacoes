# -*- coding: utf-8 -*-
"""
Prestacao de Contas entre Socios - modulo do Dashboard BWS.

Conceito:
  1) A base e a MESMA da DRE (dados_omie.parquet, Analise == "DRE").
  2) Custos administrativos (Depto "BWS Construções" = Matriz; "BWSNE" = Filial)
     sao rateados mensalmente para as obras via REGRAS parametrizaveis:
       - selecao de Grupos/Categorias (multiselect a partir da propria base);
       - % aplicavel (ex.: parte "compartilhada" do pessoal admin da Matriz);
       - vigencia (mes inicial/final, ex.: compartilhamento Matriz->Filial ate 2025-10);
       - escopo de destino: AMBAS (Matriz+Filial), so FILIAL ou so MATRIZ.
     Driver do rateio: custo do Grupo "Despesas com Pessoal" de cada obra no mes.
     O residuo do admin nao capturado pelas regras vai 100% para as obras do
     proprio lado (Matriz -> obras BWSCE; BWSNE -> obras da Filial), garantindo
     que o resultado total da empresa nao mude com o rateio.
  3) Cadastro Projeto x Socio x % define a distribuicao do resultado.
     Projeto SEM externo: quota = % x resultado COM rateio administrativo.
     Projeto COM socio externo (visao da parceria): base comum =
     resultado direto - taxa adm % x Receita Bruta; todos recebem % x base;
     a taxa adm e RECEITA dos socios internos (BWS) e o rateio administrativo
     da obra fica so com eles, na proporcao interna normalizada. A soma das
     quotas fecha com o resultado da obra com rateio.
  4) Ajustes manuais por socio: Valor Percebido (-), Divida Assumida (+), Outro (+/-).
  5) Tudo recalculado on-the-fly a partir do parquet + parametros (SQLite local).

Plugado no app.py:  if pagina == "Prestacao de Contas": import prestacao_contas; ...
"""
import os
import io
import json
import sqlite3
import datetime as _dt

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET = os.path.join(BASE_DIR, "dados_omie.parquet")
DB_PATH = os.path.join(BASE_DIR, "prestacao_contas.db")
REC, PAG = "1. Contas a Receber", "2. Contas a Pagar"
ANALISE = "An\u00e1lise"
DASH = "\u2014"
SEM_DATA = "(sem data)"
TIPOS_AJUSTE = ["Valor Percebido (-)", "D\u00edvida Assumida (+)", "Outro (+/-)"]

DEFAULTS = {
    "projeto_matriz": "BWSCE",
    "depto_admin_matriz": "BWS Constru\u00e7\u00f5es",
    "depto_admin_filial": "BWSNE",
    "grupo_pessoal": "Despesas com Pessoal",
    "taxa_adm_pct": "1.5",
    "residual": "1",
}


# ------------------------------------------------------------------ helpers
def brl(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return DASH
    if v != v:  # NaN
        return DASH
    s = "{:,.2f}".format(abs(v)).replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-R$ " if v < 0 else "R$ ") + s


def pct_fmt(v):
    try:
        return ("{:.2f}%".format(float(v))).replace(".", ",")
    except (TypeError, ValueError):
        return DASH


def _mes_valido(m):
    """'AAAA-MM' valido ou string vazia (= sem limite)."""
    m = (m or "").strip()
    if not m:
        return True
    if len(m) == 7 and m[4] == "-":
        a, _, mm = m.partition("-")
        return a.isdigit() and mm.isdigit() and 1 <= int(mm) <= 12
    return False


# ------------------------------------------------------------------ banco (SQLite)
@st.cache_resource(show_spinner=False)
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    _init_schema(conn)
    return conn


def _init_schema(conn):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS socios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT UNIQUE NOT NULL,
        tipo TEXT NOT NULL DEFAULT 'Interno',
        ativo INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS participacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        projeto TEXT NOT NULL,
        socio_id INTEGER NOT NULL,
        pct REAL NOT NULL DEFAULT 0,
        UNIQUE (projeto, socio_id)
    );
    CREATE TABLE IF NOT EXISTS regras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        depto TEXT NOT NULL,
        todas INTEGER NOT NULL DEFAULT 0,
        grupos TEXT NOT NULL DEFAULT '[]',
        categorias TEXT NOT NULL DEFAULT '[]',
        pct REAL NOT NULL DEFAULT 100,
        escopo TEXT NOT NULL DEFAULT 'AMBAS',
        mes_ini TEXT NOT NULL DEFAULT '',
        mes_fim TEXT NOT NULL DEFAULT '',
        ativo INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS ajustes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        socio_id INTEGER NOT NULL,
        projeto TEXT NOT NULL DEFAULT '',
        data TEXT NOT NULL DEFAULT '',
        tipo TEXT NOT NULL,
        valor REAL NOT NULL,
        descricao TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS config (
        chave TEXT PRIMARY KEY,
        valor TEXT NOT NULL
    );
    """)
    for k, v in DEFAULTS.items():
        cur.execute("INSERT OR IGNORE INTO config (chave, valor) VALUES (?,?)", (k, v))
    # Regras-padrao na primeira execucao (todas editaveis/excluiveis depois).
    if cur.execute("SELECT COUNT(*) FROM regras").fetchone()[0] == 0:
        adm_m = DEFAULTS["depto_admin_matriz"]
        adm_f = DEFAULTS["depto_admin_filial"]
        seeds = [
            ("Pessoal compartilhado da Matriz (ate out/25)", adm_m, 0,
             json.dumps(["Despesas com Pessoal"]), "[]", 50.0, "AMBAS", "", "2025-10", 1),
            ("Categorias compartilhadas da Matriz (ate out/25)", adm_m, 0,
             "[]", json.dumps(["Internet Telefonia e Sistemas",
                               "Consultoria Contabilidade Jur\u00eddica"]),
             100.0, "AMBAS", "", "2025-10", 1),
            ("Admin da Filial - BWSNE (todas as categorias)", adm_f, 1,
             "[]", "[]", 100.0, "FILIAL", "", "", 1),
        ]
        cur.executemany(
            "INSERT INTO regras (nome, depto, todas, grupos, categorias, pct, escopo,"
            " mes_ini, mes_fim, ativo) VALUES (?,?,?,?,?,?,?,?,?,?)", seeds)
    conn.commit()


def cfg_all(conn):
    cfg = dict(conn.execute("SELECT chave, valor FROM config").fetchall())
    for k, v in DEFAULTS.items():
        cfg.setdefault(k, v)
    return cfg


def cfg_set(conn, chave, valor):
    conn.execute("INSERT INTO config (chave, valor) VALUES (?,?) "
                 "ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor", (chave, str(valor)))
    conn.commit()


def socios_df(conn, apenas_ativos=False):
    q = "SELECT id, nome, tipo, ativo FROM socios" + (" WHERE ativo=1" if apenas_ativos else "")
    return pd.read_sql_query(q + " ORDER BY nome", conn)


def participacoes_df(conn):
    return pd.read_sql_query(
        "SELECT p.id, p.projeto, p.socio_id, s.nome AS socio, s.tipo, p.pct "
        "FROM participacoes p JOIN socios s ON s.id = p.socio_id "
        "ORDER BY p.projeto, s.nome", conn)


def regras_df(conn):
    return pd.read_sql_query("SELECT * FROM regras ORDER BY id", conn)


def ajustes_df(conn):
    return pd.read_sql_query(
        "SELECT a.id, a.socio_id, s.nome AS socio, a.projeto, a.data, a.tipo, a.valor, a.descricao "
        "FROM ajustes a JOIN socios s ON s.id = a.socio_id ORDER BY a.data, a.id", conn)


# ------------------------------------------------------------------ base de dados (parquet)
@st.cache_data(show_spinner="Carregando base...")
def carregar_base():
    df = pd.read_parquet(PARQUET)
    for c in ["PagoRecebido", "APagarReceber"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    pago = df["Situa\u00e7\u00e3o"].astype(str).str.contains("Pago|Recebido|Conciliado",
                                                            case=False, na=False)
    df["Executado"] = df["PagoRecebido"].where(pago, 0.0)
    df["EmAberto"] = df["APagarReceber"]
    df["Comprometido"] = df["Executado"] + df["EmAberto"]
    df["IsRetido"] = df["Categoria"].astype(str).str.contains("Retido", na=False)
    df["Pago"] = pago
    df["AnoMes"] = df["Data"].dt.to_period("M").astype(str)
    df.loc[df["Data"].isna(), "AnoMes"] = SEM_DATA
    return df


def base_dre(df):
    return df[df[ANALISE] == "DRE"].copy()


def meses_da_base(dre):
    return sorted(m for m in dre["AnoMes"].dropna().unique() if m != SEM_DATA)


def classificar_obras(dre, cfg):
    """Departamento (obra) -> Projeto dominante e Escopo (MATRIZ/FILIAL).
    Obras = departamentos que nao sao os administrativos."""
    adm = {cfg["depto_admin_matriz"], cfg["depto_admin_filial"]}
    ob = dre[~dre["Departamento"].isin(adm)].copy()
    ob = ob[ob["Departamento"].astype(str).str.strip().ne("") &
            ob["Departamento"].astype(str).str.lower().ne("nan")]
    if ob.empty:
        return pd.DataFrame(columns=["Departamento", "Projeto", "Escopo"])
    m = (ob.groupby(["Departamento", "Projeto"]).size().reset_index(name="n")
           .sort_values("n", ascending=False).drop_duplicates("Departamento"))
    m["Escopo"] = (m["Projeto"] == cfg["projeto_matriz"]).map({True: "MATRIZ", False: "FILIAL"})
    return m[["Departamento", "Projeto", "Escopo"]].reset_index(drop=True)


def driver_pessoal(dre, obras, cfg, mcol):
    """Custo de pessoal por obra x mes (valor absoluto) - driver do rateio."""
    base = dre[(dre["Tipo"] == PAG) &
               (dre["Grupo"].astype(str).str.strip() == cfg["grupo_pessoal"]) &
               (dre["Departamento"].isin(obras["Departamento"]))]
    if base.empty:
        return pd.DataFrame()
    pv = base.pivot_table(index="Departamento", columns="AnoMes", values=mcol,
                          aggfunc="sum", fill_value=0.0).abs()
    return pv


def _meses_na_vigencia(meses, mes_ini, mes_fim):
    mi, mf = (mes_ini or "").strip(), (mes_fim or "").strip()
    out = []
    for m in meses:
        if m == SEM_DATA:
            continue
        if mi and m < mi:
            continue
        if mf and m > mf:
            continue
        out.append(m)
    return out


def calcular_rateio(dre, cfg, regras, mcol):
    """Aplica as regras + residuo. Retorna:
       aloc:    DataFrame [Departamento, AnoMes, Origem, Valor]
       sobras:  DataFrame [Origem, AnoMes, Valor, Motivo]  (nao rateado)
    """
    obras = classificar_obras(dre, cfg)
    drv = driver_pessoal(dre, obras, cfg, mcol)
    esc = obras.set_index("Departamento")["Escopo"] if not obras.empty else pd.Series(dtype=str)
    adm_deptos = [cfg["depto_admin_matriz"], cfg["depto_admin_filial"]]
    adm = dre[(dre["Tipo"] == PAG) & (dre["Departamento"].isin(adm_deptos))].copy()
    adm["Grupo"] = adm["Grupo"].astype(str).str.strip()
    adm["Categoria"] = adm["Categoria"].astype(str).str.strip()

    alocs, sobras = [], []
    captado = {}  # (depto, mes) -> valor ja capturado pelas regras

    def _alocar(pool_mes, escopo, origem):
        for mes, v in pool_mes.items():
            if abs(v) <= 0.005:
                continue
            if mes == SEM_DATA or drv.empty or mes not in drv.columns:
                sobras.append({"Origem": origem, "AnoMes": mes, "Valor": v,
                               "Motivo": "sem data" if mes == SEM_DATA else "sem driver no mes"})
                continue
            if escopo == "AMBAS":
                dest = esc.index
            else:
                dest = esc[esc == escopo].index
            d = drv.loc[drv.index.intersection(dest), mes]
            tot = d.sum()
            if tot <= 0.005:
                sobras.append({"Origem": origem, "AnoMes": mes, "Valor": v,
                               "Motivo": "driver zero no escopo " + escopo})
                continue
            for depto, peso in d.items():
                if peso <= 0:
                    continue
                alocs.append({"Departamento": depto, "AnoMes": mes,
                              "Origem": origem, "Valor": v * peso / tot})

    if not adm.empty:
        for _, r in regras.iterrows():
            if not int(r["ativo"]):
                continue
            sel = adm[adm["Departamento"] == r["depto"]]
            if not int(r["todas"]):
                gs = set(json.loads(r["grupos"] or "[]"))
                cs = set(json.loads(r["categorias"] or "[]"))
                sel = sel[sel["Grupo"].isin(gs) | sel["Categoria"].isin(cs)]
            if sel.empty:
                continue
            meses_ok = set(_meses_na_vigencia(sel["AnoMes"].unique(), r["mes_ini"], r["mes_fim"]))
            sel = sel[sel["AnoMes"].isin(meses_ok)]
            if sel.empty:
                continue
            pool = sel.groupby("AnoMes")[mcol].sum() * float(r["pct"]) / 100.0
            pool = pool[pool.abs() > 0.005]
            for mes, v in pool.items():
                captado[(r["depto"], mes)] = captado.get((r["depto"], mes), 0.0) + v
            _alocar(pool, r["escopo"], "Regra: " + str(r["nome"]))

        # Residuo: o que as regras nao capturaram fica 100% no proprio lado.
        if str(cfg.get("residual", "1")) == "1":
            tot_depto = adm.groupby(["Departamento", "AnoMes"])[mcol].sum()
            for (depto, mes), total in tot_depto.items():
                resto = total - captado.get((depto, mes), 0.0)
                if abs(resto) <= 0.005:
                    continue
                escopo = "MATRIZ" if depto == cfg["depto_admin_matriz"] else "FILIAL"
                _alocar(pd.Series({mes: resto}), escopo, "Res\u00edduo: " + str(depto))

    aloc = pd.DataFrame(alocs, columns=["Departamento", "AnoMes", "Origem", "Valor"])
    sob = pd.DataFrame(sobras, columns=["Origem", "AnoMes", "Valor", "Motivo"])
    return aloc, sob


def apuracao_mensal(dre, cfg, aloc, mcol):
    """Por (Projeto, Departamento, AnoMes): RB, Retencoes, RL, Despesas diretas,
    Rateio recebido, ResultadoDireto (sem rateio) e Resultado (com rateio)."""
    obras = classificar_obras(dre, cfg)
    if obras.empty:
        return pd.DataFrame()
    ob = dre[dre["Departamento"].isin(obras["Departamento"])].copy()
    rec, ret = (ob["Tipo"] == REC) & (~ob["IsRetido"]), (ob["Tipo"] == REC) & (ob["IsRetido"])
    ob["RL"] = ob[mcol].where(rec, 0.0)
    ob["RET"] = ob[mcol].where(ret, 0.0)
    ob["DESP"] = ob[mcol].where(ob["Tipo"] == PAG, 0.0)
    g = ob.groupby(["Projeto", "Departamento", "AnoMes"])[["RL", "RET", "DESP"]].sum().reset_index()
    g["RB"] = g["RL"] + g["RET"]
    if aloc is not None and not aloc.empty:
        ra = aloc.groupby(["Departamento", "AnoMes"])["Valor"].sum().reset_index()
        ra.columns = ["Departamento", "AnoMes", "Rateio"]
        g = g.merge(ra, on=["Departamento", "AnoMes"], how="outer")
        # rateio em obra/mes sem movimento direto: completa as chaves
        mapa = obras.set_index("Departamento")["Projeto"]
        g["Projeto"] = g["Projeto"].fillna(g["Departamento"].map(mapa))
        for c in ["RL", "RET", "DESP", "RB", "Rateio"]:
            g[c] = pd.to_numeric(g[c], errors="coerce").fillna(0.0)
    else:
        g["Rateio"] = 0.0
    g["ResultadoDireto"] = g["RL"] + g["DESP"]
    g["Resultado"] = g["ResultadoDireto"] + g["Rateio"]
    return g


def quotas_mensais(apur, parts, cfg):
    """Quota mensal de cada participacao.

    Projeto SEM socio externo:
        quota = pct x Resultado (com rateio administrativo).
    Projeto COM socio externo (visao da parceria):
        base comum = ResultadoDireto - taxa_adm% x RB  (a taxa e custo da parceria);
        quota base = pct x base comum, para TODOS os participantes;
        os socios INTERNOS recebem ainda, na proporcao interna normalizada:
          (+) a taxa adm cobrada (receita dos socios da BWS) e
          (+) o rateio administrativo da obra (custo que nao cabe ao externo).
        Assim a soma das quotas fecha com o Resultado (com rateio) do projeto.
    """
    if apur.empty or parts.empty:
        return pd.DataFrame()
    taxa = float(str(cfg.get("taxa_adm_pct", "1.5")).replace(",", ".")) / 100.0
    pm = apur.groupby(["Projeto", "AnoMes"])[
        ["RB", "RL", "DESP", "Rateio", "ResultadoDireto", "Resultado"]].sum().reset_index()
    pm["TaxaAdm"] = taxa * pm["RB"]  # receita dos socios internos / custo da parceria
    pm["BaseParceria"] = pm["ResultadoDireto"] - pm["TaxaAdm"]

    p = parts.copy()
    p["_ext"] = p["tipo"].astype(str).str.lower().eq("externo")
    proj_ext = set(p.loc[p["_ext"], "projeto"].unique())
    soma_int = p[~p["_ext"]].groupby("projeto")["pct"].sum()

    out = p.merge(pm, left_on="projeto", right_on="Projeto", how="inner")
    if out.empty:
        return pd.DataFrame()
    has_ext = out["Projeto"].isin(proj_ext)
    out["Base"] = out["BaseParceria"].where(has_ext, out["Resultado"])
    out["Quota"] = out["Base"] * out["pct"] / 100.0
    # credito BWS (taxa + rateio) p/ internos em projetos com externo
    si = out["projeto"].map(soma_int).fillna(0.0)
    pct_norm = (out["pct"] / si).where(si > 0, 0.0)
    out["CreditoBWS"] = ((out["TaxaAdm"] + out["Rateio"]) * pct_norm).where(
        has_ext & ~out["_ext"], 0.0)
    out["Quota"] = out["Quota"] + out["CreditoBWS"]
    out["Visao"] = "Interna (c/ rateio)"
    out.loc[has_ext & out["_ext"], "Visao"] = "Parceria (direto \u2212 taxa adm)"
    out.loc[has_ext & ~out["_ext"], "Visao"] = "Parceria + taxa adm + rateio (lado BWS)"
    return out[["socio", "tipo", "Projeto", "AnoMes", "pct", "Base", "Quota", "CreditoBWS",
                "Visao", "RB", "Rateio", "ResultadoDireto", "Resultado", "TaxaAdm"]]


def efeito_ajuste(tipo, valor):
    try:
        v = float(valor)
    except (TypeError, ValueError):
        return 0.0
    if tipo.startswith("Valor Percebido"):
        return -abs(v)
    if tipo.startswith("D\u00edvida Assumida"):
        return abs(v)
    return v


# ------------------------------------------------------------------ Excel
_MONEY = {"Valor", "Quota", "Base", "RB", "RL", "DESP", "Rateio", "Resultado",
          "ResultadoDireto", "BaseParceria", "TaxaAdm", "CreditoBWS", "Receita Bruta", "Retencoes",
          "Receita Liquida", "Despesas diretas", "Rateio admin", "Resultado direto",
          "Resultado final", "Quotas", "Ajustes", "Saldo", "Taxa adm", "Efeito"}


def _fmt_sheet(xw, name, df):
    ws = xw.sheets[name]
    wb = xw.book
    f_money = wb.add_format({"num_format": '_-"R$" * #,##0.00_-;-"R$" * #,##0.00_-;_-"R$" * "-"??_-;_-@_-'})
    f_pct = wb.add_format({"num_format": '0.00"%"'})
    for i, c in enumerate(df.columns):
        cl = str(c)
        if cl in _MONEY:
            ws.set_column(i, i, 17, f_money)
        elif cl in ("pct", "% Part."):
            ws.set_column(i, i, 10, f_pct)
        else:
            try:
                m = int(df[c].astype(str).str.len().head(3000).max() or 10)
            except Exception:
                m = 10
            ws.set_column(i, i, min(max(len(cl) + 2, m + 2), 55))
    ws.freeze_panes(1, 0)


def montar_xlsx(posicao, quotas_proj, apur_view, aloc, sobras, parts, regras, ajus, cfg, medida):
    buf = io.BytesIO()
    par_rows = [{"Par\u00e2metro": k, "Valor": v} for k, v in cfg.items()]
    par_rows.append({"Par\u00e2metro": "medida_da_apuracao", "Valor": medida})
    params = pd.DataFrame(par_rows)
    rg = regras.copy()
    if not rg.empty:
        rg["grupos"] = rg["grupos"].apply(lambda s: ", ".join(json.loads(s or "[]")))
        rg["categorias"] = rg["categorias"].apply(lambda s: ", ".join(json.loads(s or "[]")))
    sheets = [
        ("Posicao Socios", posicao),
        ("Quotas por Projeto", quotas_proj),
        ("Apuracao Mensal", apur_view),
        ("Rateio Mensal", aloc if not aloc.empty else pd.DataFrame({"info": ["sem rateio"]})),
        ("Nao Rateado", sobras if not sobras.empty else pd.DataFrame({"info": ["nada pendente"]})),
        ("Participacoes", parts.drop(columns=["id", "socio_id"], errors="ignore")),
        ("Regras de Rateio", rg),
        ("Ajustes", ajus.drop(columns=["socio_id"], errors="ignore")),
        ("Parametros", params),
    ]
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
        for name, d in sheets:
            d = d if isinstance(d, pd.DataFrame) and not d.empty else pd.DataFrame({"info": ["(vazio)"]})
            d.to_excel(xw, sheet_name=name, index=False)
            _fmt_sheet(xw, name, d)
    buf.seek(0)
    return buf.getvalue()


# ------------------------------------------------------------------ UI: abas
def _tab_cadastros(conn, dre):
    st.subheader("S\u00f3cios")
    st.caption("Cadastre os s\u00f3cios (internos e externos). S\u00f3cio EXTERNO recebe a vis\u00e3o "
               "sem rateio administrativo, com taxa de administra\u00e7\u00e3o sobre a Receita Bruta.")
    sdf = socios_df(conn)
    ed = st.data_editor(
        sdf.drop(columns=["id"]).assign(_id=sdf["id"]),
        column_config={
            "nome": st.column_config.TextColumn("Nome", required=True),
            "tipo": st.column_config.SelectboxColumn("Tipo", options=["Interno", "Externo"]),
            "ativo": st.column_config.CheckboxColumn("Ativo"),
            "_id": None,
        },
        num_rows="dynamic", hide_index=True, use_container_width=True, key="ed_socios")
    if st.button("Salvar s\u00f3cios", key="bt_socios"):
        try:
            ids_mantidos = []
            for _, r in ed.iterrows():
                nome = str(r.get("nome") or "").strip()
                if not nome:
                    continue
                tipo = r.get("tipo") if r.get("tipo") in ("Interno", "Externo") else "Interno"
                ativo = 1 if bool(r.get("ativo", True)) else 0
                rid = r.get("_id")
                if pd.notna(rid):
                    conn.execute("UPDATE socios SET nome=?, tipo=?, ativo=? WHERE id=?",
                                 (nome, tipo, ativo, int(rid)))
                    ids_mantidos.append(int(rid))
                else:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO socios (nome, tipo, ativo) VALUES (?,?,?)",
                        (nome, tipo, ativo))
                    if cur.lastrowid:
                        ids_mantidos.append(cur.lastrowid)
            removidos = set(sdf["id"]) - set(ids_mantidos)
            for rid in removidos:
                conn.execute("DELETE FROM participacoes WHERE socio_id=?", (rid,))
                conn.execute("DELETE FROM ajustes WHERE socio_id=?", (rid,))
                conn.execute("DELETE FROM socios WHERE id=?", (rid,))
            conn.commit()
            st.success("S\u00f3cios salvos.")
            st.rerun()
        except sqlite3.Error as e:
            st.error(f"Falha ao salvar: {e}")

    st.markdown("---")
    st.subheader("Participa\u00e7\u00f5es por Projeto")
    st.caption("Defina, projeto a projeto, o % de cada s\u00f3cio. A soma precisa fechar 100%. "
               "Ex.: projeto com s\u00f3cio externo \u2014 50% para o externo e os outros 50% "
               "divididos entre os internos na propor\u00e7\u00e3o da Filial.")
    socs = socios_df(conn, apenas_ativos=True)
    if socs.empty:
        st.info("Cadastre os s\u00f3cios acima primeiro.")
        return
    projetos = sorted(p for p in dre["Projeto"].dropna().astype(str).unique()
                      if p.strip() and p.lower() != "nan")
    pdfc = participacoes_df(conn)
    ja = sorted(set(pdfc["projeto"]) - set(projetos))
    proj = st.selectbox("Projeto", projetos + ja, key="part_proj")
    atuais = pdfc[pdfc["projeto"] == proj].set_index("socio_id")["pct"] if not pdfc.empty else pd.Series(dtype=float)
    base = socs[["id", "nome", "tipo"]].copy()
    base["pct"] = base["id"].map(atuais).fillna(0.0)
    edp = st.data_editor(
        base, column_config={
            "id": None,
            "nome": st.column_config.TextColumn("S\u00f3cio", disabled=True),
            "tipo": st.column_config.TextColumn("Tipo", disabled=True),
            "pct": st.column_config.NumberColumn("% Participa\u00e7\u00e3o", min_value=0.0,
                                                 max_value=100.0, step=0.01, format="%.2f"),
        }, hide_index=True, use_container_width=True, key=f"ed_part_{proj}")
    soma = float(pd.to_numeric(edp["pct"], errors="coerce").fillna(0.0).sum())
    (st.success if abs(soma - 100.0) < 0.01 else st.warning)(
        f"Soma atual: {pct_fmt(soma)} " +
        ("\u2014 ok." if abs(soma - 100.0) < 0.01 else "\u2014 ATEN\u00c7\u00c3O: deveria fechar 100,00%."))
    c1, c2 = st.columns(2)
    if c1.button("Salvar participa\u00e7\u00f5es deste projeto", key="bt_part"):
        conn.execute("DELETE FROM participacoes WHERE projeto=?", (proj,))
        for _, r in edp.iterrows():
            pct = float(pd.to_numeric(pd.Series([r["pct"]]), errors="coerce").fillna(0.0).iloc[0])
            if pct > 0:
                conn.execute("INSERT INTO participacoes (projeto, socio_id, pct) VALUES (?,?,?)",
                             (proj, int(r["id"]), pct))
        conn.commit()
        st.success(f"Participa\u00e7\u00f5es de {proj} salvas.")
        st.rerun()
    if c2.button("Limpar participa\u00e7\u00f5es deste projeto", key="bt_part_del"):
        conn.execute("DELETE FROM participacoes WHERE projeto=?", (proj,))
        conn.commit()
        st.rerun()
    if not pdfc.empty:
        st.markdown("**Vis\u00e3o geral dos projetos cadastrados**")
        vis = pdfc.pivot_table(index="projeto", columns="socio", values="pct",
                               aggfunc="sum", fill_value=0.0)
        vis["TOTAL"] = vis.sum(axis=1)
        st.dataframe(vis.map(pct_fmt), use_container_width=True)
        falta = vis.index[(vis["TOTAL"] - 100.0).abs() > 0.01].tolist()
        if falta:
            st.warning("Projetos com soma \u2260 100%: " + ", ".join(falta))


def _tab_regras(conn, dre, cfg):
    st.subheader("Par\u00e2metros gerais")
    c1, c2, c3 = st.columns(3)
    taxa = c1.number_input("Taxa de administra\u00e7\u00e3o p/ s\u00f3cio externo (% s/ Receita Bruta)",
                           min_value=0.0, max_value=100.0, step=0.1,
                           value=float(str(cfg["taxa_adm_pct"]).replace(",", ".")), key="cfg_taxa")
    residual = c2.checkbox("Alocar res\u00edduo do admin no pr\u00f3prio lado (Matriz\u2192obras CE; "
                           "BWSNE\u2192obras Filial)", value=str(cfg["residual"]) == "1", key="cfg_res")
    pm = c3.text_input("Projeto da Matriz", value=cfg["projeto_matriz"], key="cfg_pm")
    c4, c5, c6 = st.columns(3)
    dam = c4.text_input("Depto admin da Matriz", value=cfg["depto_admin_matriz"], key="cfg_dam")
    daf = c5.text_input("Depto admin da Filial", value=cfg["depto_admin_filial"], key="cfg_daf")
    gp = c6.text_input("Grupo do driver (pessoal)", value=cfg["grupo_pessoal"], key="cfg_gp")
    if st.button("Salvar par\u00e2metros", key="bt_cfg"):
        cfg_set(conn, "taxa_adm_pct", taxa)
        cfg_set(conn, "residual", "1" if residual else "0")
        cfg_set(conn, "projeto_matriz", pm.strip())
        cfg_set(conn, "depto_admin_matriz", dam.strip())
        cfg_set(conn, "depto_admin_filial", daf.strip())
        cfg_set(conn, "grupo_pessoal", gp.strip())
        st.success("Par\u00e2metros salvos.")
        st.rerun()
    for nome, chave in [("Depto admin Matriz", "depto_admin_matriz"),
                        ("Depto admin Filial", "depto_admin_filial")]:
        if cfg[chave] not in set(dre["Departamento"].astype(str).unique()):
            st.warning(f"{nome} '{cfg[chave]}' n\u00e3o encontrado na base \u2014 confira a grafia.")
    if cfg["grupo_pessoal"] not in set(dre["Grupo"].astype(str).str.strip().unique()):
        st.warning(f"Grupo '{cfg['grupo_pessoal']}' n\u00e3o encontrado na base.")

    st.markdown("---")
    st.subheader("Regras de rateio")
    st.caption("Cada regra pega uma fatia (%) dos custos selecionados do depto administrativo, "
               "na vig\u00eancia indicada, e distribui mensalmente pelas obras do escopo, "
               "proporcionalmente ao custo de pessoal de cada obra no m\u00eas. "
               "Meses no formato AAAA-MM; vazio = sem limite.")
    rg = regras_df(conn)
    adm_deptos = [cfg["depto_admin_matriz"], cfg["depto_admin_filial"]]
    for _, r in rg.iterrows():
        rid = int(r["id"])
        rotulo = ("\u2705 " if int(r["ativo"]) else "\u23f8\ufe0f ") + str(r["nome"])
        with st.expander(rotulo, expanded=False):
            nome = st.text_input("Nome", value=r["nome"], key=f"rg_nome_{rid}")
            ca, cb, cc = st.columns(3)
            depto = ca.selectbox("Depto de origem", adm_deptos,
                                 index=adm_deptos.index(r["depto"]) if r["depto"] in adm_deptos else 0,
                                 key=f"rg_depto_{rid}")
            escopo = cb.selectbox("Obras de destino", ["AMBAS", "FILIAL", "MATRIZ"],
                                  index=["AMBAS", "FILIAL", "MATRIZ"].index(r["escopo"]),
                                  key=f"rg_escopo_{rid}")
            pct = cc.number_input("% aplic\u00e1vel", 0.0, 100.0, float(r["pct"]), 1.0,
                                  key=f"rg_pct_{rid}",
                                  help="Ex.: % do pessoal admin que \u00e9 de setores compartilhados.")
            todas = st.checkbox("Todas as categorias do depto", value=bool(int(r["todas"])),
                                key=f"rg_todas_{rid}")
            sub = dre[(dre["Tipo"] == PAG) & (dre["Departamento"] == depto)]
            gops = sorted(sub["Grupo"].astype(str).str.strip().dropna().unique())
            cops = sorted(sub["Categoria"].astype(str).str.strip().dropna().unique())
            gsel = json.loads(r["grupos"] or "[]")
            csel = json.loads(r["categorias"] or "[]")
            if not todas:
                gsel = st.multiselect("Grupos inclu\u00eddos", gops,
                                      default=[g for g in gsel if g in gops], key=f"rg_g_{rid}")
                csel = st.multiselect("Categorias inclu\u00eddas", cops,
                                      default=[c for c in csel if c in cops], key=f"rg_c_{rid}")
            cv1, cv2, cv3 = st.columns(3)
            mi = cv1.text_input("M\u00eas inicial (AAAA-MM)", value=r["mes_ini"], key=f"rg_mi_{rid}")
            mf = cv2.text_input("M\u00eas final (AAAA-MM)", value=r["mes_fim"], key=f"rg_mf_{rid}")
            ativo = cv3.checkbox("Ativa", value=bool(int(r["ativo"])), key=f"rg_at_{rid}")
            cs, cd = st.columns(2)
            if cs.button("Salvar regra", key=f"rg_save_{rid}"):
                if not (_mes_valido(mi) and _mes_valido(mf)):
                    st.error("Vig\u00eancia inv\u00e1lida: use AAAA-MM ou deixe vazio.")
                elif not todas and not gsel and not csel:
                    st.error("Selecione ao menos um Grupo/Categoria ou marque 'todas'.")
                else:
                    conn.execute(
                        "UPDATE regras SET nome=?, depto=?, todas=?, grupos=?, categorias=?, "
                        "pct=?, escopo=?, mes_ini=?, mes_fim=?, ativo=? WHERE id=?",
                        (nome.strip(), depto, 1 if todas else 0, json.dumps(gsel),
                         json.dumps(csel), pct, escopo, mi.strip(), mf.strip(),
                         1 if ativo else 0, rid))
                    conn.commit()
                    st.success("Regra salva.")
                    st.rerun()
            if cd.button("Excluir regra", key=f"rg_del_{rid}"):
                conn.execute("DELETE FROM regras WHERE id=?", (rid,))
                conn.commit()
                st.rerun()
    if st.button("\u2795 Nova regra", key="rg_new"):
        conn.execute("INSERT INTO regras (nome, depto, todas, pct, escopo) VALUES (?,?,1,100,'AMBAS')",
                     ("Nova regra", cfg["depto_admin_matriz"]))
        conn.commit()
        st.rerun()


def _tab_rateio_preview(aloc, sobras, dre, cfg, mcol):
    st.subheader("Rateio administrativo \u2014 resultado mensal")
    st.caption("Quanto de custo administrativo foi canalizado para cada obra, m\u00eas a m\u00eas, "
               "com a medida e o per\u00edodo selecionados acima. Valores negativos = custo recebido.")
    if aloc.empty:
        st.info("Nenhum valor rateado (verifique regras, deptos admin e o driver de pessoal).")
    else:
        tot_aloc = aloc["Valor"].sum()
        adm_deptos = [cfg["depto_admin_matriz"], cfg["depto_admin_filial"]]
        tot_adm = dre.loc[(dre["Tipo"] == PAG) & (dre["Departamento"].isin(adm_deptos)), mcol].sum()
        tot_sob = sobras["Valor"].sum() if not sobras.empty else 0.0
        k1, k2, k3 = st.columns(3)
        k1.metric("Custo administrativo total", brl(tot_adm))
        k2.metric("Rateado para as obras", brl(tot_aloc))
        k3.metric("N\u00e3o rateado", brl(tot_sob))
        if abs(tot_adm - tot_aloc - tot_sob) > 1.0:
            st.warning("Diferen\u00e7a entre admin total e (rateado + n\u00e3o rateado): " +
                       brl(tot_adm - tot_aloc - tot_sob) +
                       " \u2014 regras sobrepostas podem estar contando o mesmo custo duas vezes.")
        pv = aloc.pivot_table(index="Departamento", columns="AnoMes", values="Valor",
                              aggfunc="sum", fill_value=0.0)
        pv["TOTAL"] = pv.sum(axis=1)
        pv.loc["TOTAL"] = pv.sum()
        st.dataframe(pv.map(brl), use_container_width=True)
        with st.expander("Detalhe por regra/origem"):
            det = aloc.groupby(["Origem", "AnoMes"])["Valor"].sum().reset_index()
            det["Valor"] = det["Valor"].apply(brl)
            st.dataframe(det, use_container_width=True, hide_index=True)
    if not sobras.empty:
        st.markdown("**Valores n\u00e3o rateados** (sem driver no m\u00eas / sem data):")
        s = sobras.copy()
        s["Valor"] = s["Valor"].apply(brl)
        st.dataframe(s, use_container_width=True, hide_index=True)


def _tab_apuracao(apur, cfg):
    st.subheader("Apura\u00e7\u00e3o do resultado por projeto (com rateio)")
    if apur.empty:
        st.info("Sem dados para os filtros atuais.")
        return
    nivel = st.radio("Agrupar por", ["Projeto", "Departamento"], horizontal=True, key="ap_nivel")
    g = apur.groupby(nivel)[["RB", "RET", "RL", "DESP", "Rateio",
                             "ResultadoDireto", "Resultado"]].sum().reset_index()
    g = g.sort_values("Resultado", ascending=False)
    show = g.copy()
    show.columns = [nivel, "Receita Bruta", "Retencoes", "Receita Liquida", "Despesas diretas",
                    "Rateio admin", "Resultado direto", "Resultado final"]
    tot = show.drop(columns=[nivel]).sum()
    show = pd.concat([show, pd.DataFrame([{nivel: "TOTAL", **tot.to_dict()}])], ignore_index=True)
    for c in show.columns[1:]:
        show[c] = show[c].apply(brl)
    st.dataframe(show, use_container_width=True, hide_index=True,
                 height=min(560, 60 + 36 * len(show)))
    st.caption("Resultado final = Receita L\u00edquida + Despesas diretas + Rateio admin. "
               "O TOTAL do Resultado final equivale ao resultado da empresa (obras + admin) "
               "quando todo o admin \u00e9 rateado.")
    fig = go.Figure()
    fig.add_bar(y=g[nivel], x=g["ResultadoDireto"], name="Resultado direto",
                orientation="h", marker_color="#1565c0")
    fig.add_bar(y=g[nivel], x=g["Rateio"], name="Rateio admin",
                orientation="h", marker_color="#ef6c00")
    fig.update_layout(barmode="relative", height=min(620, 110 + 30 * len(g)), separators=",.",
                      xaxis=dict(title="R$", tickprefix="R$ "), yaxis_title="",
                      legend=dict(orientation="h", y=1.1, x=0))
    fig.update_yaxes(autorange="reversed")
    fig.update_traces(hovertemplate="R$ %{x:,.2f}<extra></extra>")
    st.plotly_chart(fig, use_container_width=True)
    mensal = apur.groupby("AnoMes")[["Resultado"]].sum().reset_index()
    mensal = mensal[mensal["AnoMes"] != SEM_DATA].sort_values("AnoMes")
    if len(mensal) > 1:
        mensal["Acumulado"] = mensal["Resultado"].cumsum()
        mensal["MesData"] = pd.to_datetime(mensal["AnoMes"] + "-01", errors="coerce")
        f2 = go.Figure()
        f2.add_bar(x=mensal["MesData"], y=mensal["Resultado"], name="Resultado no m\u00eas",
                   marker_color="#2e7d32")
        f2.add_scatter(x=mensal["MesData"], y=mensal["Acumulado"], name="Acumulado",
                       mode="lines+markers", line=dict(color="#1565c0", width=3))
        f2.update_layout(height=380, separators=",.", hovermode="x unified",
                         legend=dict(orientation="h", y=1.12, x=0), margin=dict(t=30, b=0),
                         yaxis=dict(tickprefix="R$ "))
        f2.update_xaxes(tickformat="%m/%Y")
        f2.update_traces(hovertemplate="R$ %{y:,.2f}<extra></extra>")
        st.plotly_chart(f2, use_container_width=True)


def _tab_posicao(conn, quotas, apur, cfg, medida):
    st.subheader("Posi\u00e7\u00e3o dos s\u00f3cios")

    # ---- Concilia\u00e7\u00e3o global: tudo que tem resultado precisa estar distribu\u00eddo
    if not apur.empty:
        res_total = apur["Resultado"].sum()
        res_proj_all = apur.groupby("Projeto")["Resultado"].sum()
        proj_cadastrados = set(quotas["Projeto"].unique()) if not quotas.empty else set()
        soma_quotas = quotas["Quota"].sum() if not quotas.empty else 0.0
        nao_dist = res_total - soma_quotas
        k1, k2, k3 = st.columns(3)
        k1.metric("Resultado total (c/ rateio)", brl(res_total))
        k2.metric("Distribu\u00eddo aos s\u00f3cios", brl(soma_quotas))
        k3.metric("N\u00c3O distribu\u00eddo", brl(nao_dist))
        faltantes = res_proj_all[(res_proj_all.abs() > 0.01) &
                                 (~res_proj_all.index.isin(proj_cadastrados))]
        if not faltantes.empty:
            ft = faltantes.sort_values().reset_index()
            ft.columns = ["Projeto", "Resultado n\u00e3o distribu\u00eddo"]
            ft["Resultado n\u00e3o distribu\u00eddo"] = ft["Resultado n\u00e3o distribu\u00eddo"].apply(brl)
            st.error("Projetos com resultado e SEM participa\u00e7\u00f5es cadastradas \u2014 "
                     "o resultado deles n\u00e3o est\u00e1 sendo distribu\u00eddo a ningu\u00e9m. "
                     "Cadastre os s\u00f3cios destes projetos na aba Cadastros:")
            st.dataframe(ft, use_container_width=True, hide_index=True)
        elif abs(nao_dist) > 1.0:
            st.warning("H\u00e1 " + brl(nao_dist) + " n\u00e3o distribu\u00eddos mesmo com todos os "
                       "projetos cadastrados \u2014 verifique se as participa\u00e7\u00f5es fecham "
                       "100% em cada projeto (aba Cadastros) e se projetos com s\u00f3cio "
                       "externo t\u00eam ao menos um s\u00f3cio interno cadastrado.")

    if quotas.empty:
        st.info("Cadastre s\u00f3cios e participa\u00e7\u00f5es por projeto na aba Cadastros.")
        return
    ajus = ajustes_df(conn)
    ajus["Efeito"] = ajus.apply(lambda r: efeito_ajuste(r["tipo"], r["valor"]), axis=1) \
        if not ajus.empty else pd.Series(dtype=float)

    qs = quotas.groupby("socio")["Quota"].sum()
    ae = ajus.groupby("socio")["Efeito"].sum() if not ajus.empty else pd.Series(dtype=float)
    pos = pd.DataFrame({"Quotas": qs}).join(pd.DataFrame({"Ajustes": ae}), how="outer").fillna(0.0)
    pos["Saldo"] = pos["Quotas"] + pos["Ajustes"]
    pos = pos.sort_values("Saldo", ascending=False).reset_index().rename(columns={"index": "S\u00f3cio",
                                                                                  "socio": "S\u00f3cio"})
    tot_row = {"S\u00f3cio": "TOTAL", "Quotas": pos["Quotas"].sum(),
               "Ajustes": pos["Ajustes"].sum(), "Saldo": pos["Saldo"].sum()}
    pos = pd.concat([pos, pd.DataFrame([tot_row])], ignore_index=True)
    show = pos.copy()
    for c in ["Quotas", "Ajustes", "Saldo"]:
        show[c] = show[c].apply(brl)
    st.dataframe(show, use_container_width=True, hide_index=True)
    st.caption(f"Medida: **{medida}**. Quotas = soma das fatias do s\u00f3cio em cada projeto. "
               "Projeto sem externo: % x resultado com rateio. Projeto com s\u00f3cio externo: "
               "todos recebem % x (resultado direto \u2212 taxa adm de "
               f"{pct_fmt(cfg['taxa_adm_pct'])} s/ Receita Bruta); a taxa \u00e9 receita dos "
               "s\u00f3cios da BWS e o rateio admin da obra fica s\u00f3 com eles (lado interno, "
               "na propor\u00e7\u00e3o normalizada). Ajustes = percebidos (\u2212), d\u00edvidas "
               "assumidas (+) e outros. Saldo positivo = a receber; negativo = a pagar/devolver.")

    # ---- Quotas e concilia\u00e7\u00e3o por projeto
    qp = quotas.groupby(["Projeto", "socio", "tipo", "Visao"]).agg(
        pct=("pct", "first"), Quota=("Quota", "sum"),
        CreditoBWS=("CreditoBWS", "sum")).reset_index()
    res_proj = apur.groupby("Projeto")[["Resultado", "ResultadoDireto", "RB", "Rateio"]].sum()
    with st.expander("Quotas por projeto e concilia\u00e7\u00e3o (soma das quotas x resultado)"):
        taxa = float(str(cfg.get("taxa_adm_pct", "1.5")).replace(",", ".")) / 100.0
        for proj, gg in qp.groupby("Projeto"):
            r = res_proj.loc[proj] if proj in res_proj.index else None
            st.markdown(f"**{proj}**")
            t = gg[["socio", "tipo", "pct", "Visao", "CreditoBWS", "Quota"]].copy()
            t.columns = ["S\u00f3cio", "Tipo", "% Part.", "Vis\u00e3o",
                         "Taxa adm + rateio (BWS)", "Quota"]
            t["% Part."] = t["% Part."].apply(pct_fmt)
            t["Taxa adm + rateio (BWS)"] = t["Taxa adm + rateio (BWS)"].apply(brl)
            t["Quota"] = t["Quota"].apply(brl)
            st.dataframe(t, use_container_width=True, hide_index=True)
            if r is not None:
                soma_q = gg["Quota"].sum()
                dif = r["Resultado"] - soma_q
                st.caption(f"Resultado c/ rateio: {brl(r['Resultado'])} | direto: "
                           f"{brl(r['ResultadoDireto'])} | rateio admin: {brl(r['Rateio'])} | "
                           f"taxa adm: {brl(taxa * r['RB'])} | soma das quotas: {brl(soma_q)} | "
                           f"diferen\u00e7a: {brl(dif)}" +
                           (" \u2014 deveria ser zero; verifique se as participa\u00e7\u00f5es "
                            "fecham 100% e se h\u00e1 s\u00f3cio interno cadastrado no projeto."
                            if abs(dif) > 1.0 else ""))

    # ---- Gr\u00e1fico temporal por s\u00f3cio (estilo Fluxo de Caixa)
    st.markdown("---")
    st.subheader("Gera\u00e7\u00e3o de resultado no tempo, por s\u00f3cio")
    socios_opts = ["(Todos)"] + sorted(quotas["socio"].unique())
    ssel = st.selectbox("S\u00f3cio", socios_opts, key="pos_socio")
    qq = quotas if ssel == "(Todos)" else quotas[quotas["socio"] == ssel]
    mensal = qq[qq["AnoMes"] != SEM_DATA].groupby("AnoMes")["Quota"].sum().reset_index()
    mensal = mensal.sort_values("AnoMes")
    sem_data = qq.loc[qq["AnoMes"] == SEM_DATA, "Quota"].sum()
    if mensal.empty:
        st.info("Sem quotas mensais para o s\u00f3cio/per\u00edodo selecionado.")
    else:
        mensal["Acumulado"] = mensal["Quota"].cumsum()
        mensal["MesData"] = pd.to_datetime(mensal["AnoMes"] + "-01", errors="coerce")
        fig = go.Figure()
        cores = ["#2e7d32" if v >= 0 else "#c62828" for v in mensal["Quota"]]
        fig.add_bar(x=mensal["MesData"], y=mensal["Quota"], name="Quota no m\u00eas",
                    marker_color=cores)
        fig.add_scatter(x=mensal["MesData"], y=mensal["Acumulado"], name="Acumulado",
                        mode="lines+markers", line=dict(color="#1565c0", width=3))
        fig.update_layout(barmode="relative", height=430, separators=",.",
                          hovermode="x unified", legend=dict(orientation="h", y=1.1, x=0),
                          margin=dict(t=30, b=0), yaxis=dict(tickprefix="R$ "))
        fig.update_xaxes(tickformat="%m/%Y")
        fig.update_traces(hovertemplate="R$ %{y:,.2f}<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)
        if abs(sem_data) > 0.005:
            st.caption(f"+ {brl(sem_data)} em t\u00edtulos sem data (fora do gr\u00e1fico, "
                       "inclu\u00eddos nos totais).")
        tb = mensal[["AnoMes", "Quota", "Acumulado"]].copy()
        tb["Quota"] = tb["Quota"].apply(brl)
        tb["Acumulado"] = tb["Acumulado"].apply(brl)
        tb.columns = ["M\u00eas", "Quota", "Acumulado"]
        st.dataframe(tb, use_container_width=True, hide_index=True, height=300)


def _tab_ajustes(conn, dre):
    st.subheader("Ajustes manuais (valores percebidos, d\u00edvidas assumidas, outros)")
    st.caption("Valor Percebido: o que o s\u00f3cio j\u00e1 recebeu \u2014 DEDUZ do saldo dele. "
               "D\u00edvida Assumida: d\u00edvida da empresa que o s\u00f3cio assumiu (cis\u00e3o) \u2014 "
               "SOMA ao saldo dele. Outro: informe o valor com sinal (+/\u2212).")
    socs = socios_df(conn, apenas_ativos=True)
    if socs.empty:
        st.info("Cadastre os s\u00f3cios primeiro.")
        return
    projetos = [""] + sorted(p for p in dre["Projeto"].dropna().astype(str).unique()
                             if p.strip() and p.lower() != "nan")
    with st.form("form_ajuste", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        socio = c1.selectbox("S\u00f3cio", socs["nome"].tolist())
        tipo = c2.selectbox("Tipo", TIPOS_AJUSTE)
        data = c3.date_input("Data", value=_dt.date.today(), format="DD/MM/YYYY")
        c4, c5 = st.columns([1, 2])
        valor = c4.number_input("Valor (R$)", step=100.0, format="%.2f")
        proj = c5.selectbox("Projeto (opcional, s\u00f3 refer\u00eancia)", projetos)
        desc = st.text_input("Descri\u00e7\u00e3o")
        if st.form_submit_button("Registrar ajuste"):
            if abs(valor) < 0.005:
                st.error("Informe um valor diferente de zero.")
            else:
                sid = int(socs.loc[socs["nome"] == socio, "id"].iloc[0])
                conn.execute(
                    "INSERT INTO ajustes (socio_id, projeto, data, tipo, valor, descricao) "
                    "VALUES (?,?,?,?,?,?)",
                    (sid, proj, data.isoformat(), tipo, float(valor), desc.strip()))
                conn.commit()
                st.success("Ajuste registrado.")
                st.rerun()
    aj = ajustes_df(conn)
    if aj.empty:
        st.info("Nenhum ajuste registrado.")
        return
    aj["Efeito"] = aj.apply(lambda r: efeito_ajuste(r["tipo"], r["valor"]), axis=1)
    show = aj[["id", "socio", "projeto", "data", "tipo", "valor", "Efeito", "descricao"]].copy()
    show["data"] = pd.to_datetime(show["data"], errors="coerce").dt.strftime("%d/%m/%Y").fillna("")
    show["valor"] = show["valor"].apply(brl)
    show["Efeito"] = show["Efeito"].apply(brl)
    show.columns = ["ID", "S\u00f3cio", "Projeto", "Data", "Tipo", "Valor", "Efeito no saldo",
                    "Descri\u00e7\u00e3o"]
    st.dataframe(show, use_container_width=True, hide_index=True)
    cdel1, cdel2 = st.columns([1, 3])
    rid = cdel1.selectbox("Excluir ajuste (ID)", [""] + aj["id"].astype(str).tolist(),
                          key="aj_del_id")
    if cdel2.button("Excluir ajuste selecionado", key="aj_del_bt") and rid:
        conn.execute("DELETE FROM ajustes WHERE id=?", (int(rid),))
        conn.commit()
        st.rerun()


# ------------------------------------------------------------------ pagina
# -----------------------------------------------------------------------------
# CENARIOS DE RATEIO: edite as regras EM MEMORIA (sem gravar), veja o efeito por
# obra lado a lado com o cenario oficial (regras gravadas) e so grave se quiser.
# -----------------------------------------------------------------------------
_CEN_COLS = ["id", "nome", "depto", "todas", "grupos", "categorias", "pct", "escopo",
             "mes_ini", "mes_fim", "ativo"]


def _cen_normalizar(df):
    """Garante tipos/defaults nas regras do cenario (o data_editor devolve object)."""
    d = df.copy()
    for c in _CEN_COLS:
        if c not in d.columns:
            d[c] = None
    d["nome"] = d["nome"].fillna("").astype(str)
    d["depto"] = d["depto"].fillna("").astype(str)
    d["todas"] = d["todas"].map(lambda v: 1 if v in (1, True, "1", "True", "true") else 0)
    d["grupos"] = d["grupos"].fillna("[]").astype(str).replace("", "[]")
    d["categorias"] = d["categorias"].fillna("[]").astype(str).replace("", "[]")
    d["pct"] = pd.to_numeric(d["pct"], errors="coerce").fillna(100.0).clip(0, 100)
    d["escopo"] = d["escopo"].fillna("AMBAS").astype(str).str.upper()
    d.loc[~d["escopo"].isin(["AMBAS", "MATRIZ", "FILIAL"]), "escopo"] = "AMBAS"
    d["mes_ini"] = d["mes_ini"].fillna("").astype(str).str.strip()
    d["mes_fim"] = d["mes_fim"].fillna("").astype(str).str.strip()
    d["ativo"] = d["ativo"].map(lambda v: 1 if v in (1, True, "1", "True", "true") else 0)
    d = d[d["nome"].str.strip() != ""]
    return d[_CEN_COLS]


def _cen_por_obra(apur):
    if apur is None or apur.empty:
        return pd.DataFrame(columns=["Departamento", "Rateio", "Resultado"])
    return apur.groupby("Departamento")[["Rateio", "Resultado"]].sum().reset_index()


def _tab_cenarios(conn, dre, cfg, regras_base, aloc_base, sob_base, apur_base, mcol):
    st.subheader("Cen\u00e1rios de rateio \u2014 ajuste e compare antes de gravar")
    st.caption("Edite os par\u00e2metros das regras aqui (**%**, **escopo**, **vig\u00eancia**, **ativa**), "
               "sem gravar nada. O efeito por obra aparece na hora, lado a lado com o cen\u00e1rio "
               "**oficial** (as regras gravadas). Grupos/categorias das regras se editam na aba Regras.")

    # estado do cenario (copia das regras gravadas)
    if "pc_cen_regras" not in st.session_state or st.session_state.get("pc_cen_base_len") != len(regras_base):
        st.session_state["pc_cen_regras"] = regras_base.copy()
        st.session_state["pc_cen_base_len"] = len(regras_base)
    b1, b2 = st.columns([1, 3])
    if b1.button("\u21a9 Resetar para as regras gravadas", key="pc_cen_reset"):
        st.session_state["pc_cen_regras"] = regras_base.copy()
        st.rerun()
    residual_cen = b2.checkbox("Aplicar res\u00edduo (o que as regras n\u00e3o capturam fica 100% no pr\u00f3prio lado)",
                               value=str(cfg.get("residual", "1")) == "1", key="pc_cen_residual")

    adm_opts = [cfg["depto_admin_matriz"], cfg["depto_admin_filial"]]
    ed = st.data_editor(
        st.session_state["pc_cen_regras"],
        num_rows="dynamic", use_container_width=True, hide_index=True, key="pc_cen_editor",
        column_config={
            "id": st.column_config.NumberColumn("id", disabled=True, width="small"),
            "nome": st.column_config.TextColumn("Regra"),
            "depto": st.column_config.SelectboxColumn("Depto admin", options=adm_opts),
            "todas": st.column_config.CheckboxColumn("Todas as categorias"),
            "grupos": st.column_config.TextColumn("Grupos (JSON)", disabled=True),
            "categorias": st.column_config.TextColumn("Categorias (JSON)", disabled=True),
            "pct": st.column_config.NumberColumn("% rateado", min_value=0.0, max_value=100.0, step=5.0),
            "escopo": st.column_config.SelectboxColumn("Escopo", options=["AMBAS", "MATRIZ", "FILIAL"]),
            "mes_ini": st.column_config.TextColumn("De (AAAA-MM)"),
            "mes_fim": st.column_config.TextColumn("At\u00e9 (AAAA-MM)"),
            "ativo": st.column_config.CheckboxColumn("Ativa"),
        },
    )
    regras_cen = _cen_normalizar(ed)
    st.session_state["pc_cen_regras"] = regras_cen

    cfg_cen = dict(cfg)
    cfg_cen["residual"] = "1" if residual_cen else "0"
    aloc_cen, sob_cen = calcular_rateio(dre, cfg_cen, regras_cen, mcol)
    apur_cen = apuracao_mensal(dre, cfg_cen, aloc_cen, mcol)

    # --- totais lado a lado ---
    t_b = aloc_base["Valor"].sum() if not aloc_base.empty else 0.0
    t_c = aloc_cen["Valor"].sum() if not aloc_cen.empty else 0.0
    s_b = sob_base["Valor"].sum() if not sob_base.empty else 0.0
    s_c = sob_cen["Valor"].sum() if not sob_cen.empty else 0.0
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Rateado \u2014 oficial", brl(t_b))
    k2.metric("Rateado \u2014 cen\u00e1rio", brl(t_c), brl(t_c - t_b))
    k3.metric("N\u00e3o rateado \u2014 oficial", brl(s_b))
    k4.metric("N\u00e3o rateado \u2014 cen\u00e1rio", brl(s_c), brl(s_c - s_b))

    # --- por obra: rateio e resultado, oficial x cenario ---
    ob_b = _cen_por_obra(apur_base).rename(columns={"Rateio": "Rateio oficial", "Resultado": "Resultado oficial"})
    ob_c = _cen_por_obra(apur_cen).rename(columns={"Rateio": "Rateio cen\u00e1rio", "Resultado": "Resultado cen\u00e1rio"})
    comp = ob_b.merge(ob_c, on="Departamento", how="outer").fillna(0.0)
    comp["\u0394 Rateio"] = comp["Rateio cen\u00e1rio"] - comp["Rateio oficial"]
    comp["\u0394 Resultado"] = comp["Resultado cen\u00e1rio"] - comp["Resultado oficial"]
    comp = comp[(comp[["Rateio oficial", "Rateio cen\u00e1rio", "Resultado oficial", "Resultado cen\u00e1rio"]].abs().sum(axis=1)) > 0.5]
    comp = comp.sort_values("\u0394 Resultado")

    st.markdown("**Efeito por obra** (\u0394 = cen\u00e1rio \u2212 oficial; \u0394 Resultado negativo = a obra passa a receber mais custo)")
    if comp.empty:
        st.info("Sem diferen\u00e7as entre o cen\u00e1rio e o oficial.")
    else:
        mostra = comp.copy()
        for c in ["Rateio oficial", "Rateio cen\u00e1rio", "\u0394 Rateio", "Resultado oficial", "Resultado cen\u00e1rio", "\u0394 Resultado"]:
            mostra[c] = mostra[c].map(brl)
        st.dataframe(mostra, use_container_width=True, hide_index=True)
        mud = comp[comp["\u0394 Resultado"].abs() > 0.5]
        if not mud.empty:
            fig = go.Figure(go.Bar(x=mud["Departamento"], y=mud["\u0394 Resultado"],
                                   marker_color=["#c0392b" if v < 0 else "#27ae60" for v in mud["\u0394 Resultado"]]))
            fig.add_hline(y=0, line_color="#7f8c8d", line_width=1)
            fig.update_layout(title="\u0394 Resultado por obra (cen\u00e1rio \u2212 oficial)", height=360,
                              margin=dict(l=10, r=10, t=40, b=10), yaxis_tickformat=",.0f")
            st.plotly_chart(fig, use_container_width=True)

    # --- gravar ---
    st.markdown("---")
    g1, g2 = st.columns([2, 1])
    conf = g1.checkbox("Entendo que gravar SUBSTITUI as regras oficiais pelas do cen\u00e1rio (e o res\u00edduo).",
                       key="pc_cen_confirma")
    if g2.button("\U0001f4be Gravar cen\u00e1rio como regras oficiais", disabled=not conf, key="pc_cen_gravar"):
        cur = conn.cursor()
        cur.execute("DELETE FROM regras")
        cur.executemany(
            "INSERT INTO regras (nome, depto, todas, grupos, categorias, pct, escopo, mes_ini, mes_fim, ativo) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(r["nome"], r["depto"], int(r["todas"]), r["grupos"], r["categorias"], float(r["pct"]),
              r["escopo"], r["mes_ini"], r["mes_fim"], int(r["ativo"])) for _, r in regras_cen.iterrows()])
        conn.commit()
        cfg_set(conn, "residual", cfg_cen["residual"])
        for k in ("pc_cen_regras", "pc_cen_base_len", "pc_cen_confirma"):
            st.session_state.pop(k, None)
        st.success("Regras oficiais atualizadas com o cen\u00e1rio.")
        st.rerun()


def pagina():
    st.title("Presta\u00e7\u00e3o de Contas entre S\u00f3cios")
    st.caption("Resultado das obras (mesma base da DRE) com rateio administrativo "
               "parametriz\u00e1vel, distribu\u00eddo entre os s\u00f3cios conforme o cadastro "
               "Projeto x S\u00f3cio x %. Tudo recalculado na hora \u2014 ajuste os par\u00e2metros "
               "e compare cen\u00e1rios at\u00e9 o consenso.")
    if not os.path.exists(PARQUET):
        st.error("dados_omie.parquet n\u00e3o encontrado. Rode a atualiza\u00e7\u00e3o (atualiza_omie.bat).")
        return
    conn = get_conn()
    cfg = cfg_all(conn)
    df = carregar_base()
    dre_full = base_dre(df)
    if dre_full.empty:
        st.error("A base n\u00e3o tem lan\u00e7amentos com An\u00e1lise = 'DRE'.")
        return

    # ---------- filtros de c\u00e1lculo ----------
    st.markdown("---")
    c1, c2 = st.columns([1, 2])
    medida = c1.radio("Medida", ["Executado", "Comprometido"], horizontal=True, key="pc_medida",
                      help="Executado = s\u00f3 pago/recebido. Comprometido = realizado + em aberto "
                           "(\u00fatil p/ enxergar d\u00edvidas a assumir na cis\u00e3o).")
    meses = meses_da_base(dre_full)
    if meses:
        if len(meses) > 1:
            m_ini, m_fim = c2.select_slider("Per\u00edodo (m\u00eas inicial \u2192 final)",
                                            options=meses, value=(meses[0], meses[-1]),
                                            key="pc_periodo")
        else:
            m_ini = m_fim = meses[0]
            c2.caption(f"Per\u00edodo dispon\u00edvel: {m_ini}")
    else:
        m_ini = m_fim = None
    c3, c4 = st.columns(2)
    incluir_sd = c3.checkbox("Incluir t\u00edtulos sem data (entram nos totais, fora dos "
                             "gr\u00e1ficos mensais)", value=True, key="pc_semdata")
    proj_all = sorted(p for p in dre_full["Projeto"].dropna().astype(str).unique()
                      if p.strip() and p.lower() != "nan")
    excl = c4.multiselect("Excluir projetos do C\u00c1LCULO (isola cen\u00e1rios; recalcula driver "
                          "e rateio)", proj_all, default=[], key="pc_excl")

    dre = dre_full
    if m_ini and m_fim:
        ok = dre["AnoMes"].between(m_ini, m_fim)
        if incluir_sd:
            ok = ok | (dre["AnoMes"] == SEM_DATA)
        dre = dre[ok]
    elif not incluir_sd:
        dre = dre[dre["AnoMes"] != SEM_DATA]
    if excl:
        dre = dre[~dre["Projeto"].isin(excl)]
    if dre.empty:
        st.info("Sem lan\u00e7amentos para os filtros atuais.")
        return

    mcol = medida
    regras = regras_df(conn)
    aloc, sobras = calcular_rateio(dre, cfg, regras, mcol)
    apur = apuracao_mensal(dre, cfg, aloc, mcol)
    parts = participacoes_df(conn)
    if not parts.empty and excl:
        parts = parts[~parts["projeto"].isin(excl)]
    quotas = quotas_mensais(apur, parts, cfg)

    # filtros de VISUALIZA\u00c7\u00c3O (p\u00f3s-c\u00e1lculo)
    cv1, cv2 = st.columns(2)
    pv = cv1.multiselect("Ver apenas estes projetos (n\u00e3o recalcula)",
                         sorted(apur["Projeto"].dropna().unique()) if not apur.empty else [],
                         default=[], key="pc_verproj")
    dv = cv2.multiselect("Ver apenas estes departamentos (n\u00e3o recalcula)",
                         sorted(apur["Departamento"].dropna().unique()) if not apur.empty else [],
                         default=[], key="pc_verdep")
    apur_view = apur
    quotas_view = quotas
    if not apur.empty and pv:
        apur_view = apur_view[apur_view["Projeto"].isin(pv)]
        if not quotas_view.empty:
            quotas_view = quotas_view[quotas_view["Projeto"].isin(pv)]
    if not apur.empty and dv:
        apur_view = apur_view[apur_view["Departamento"].isin(dv)]

    tabs = st.tabs(["Posi\u00e7\u00e3o dos S\u00f3cios", "Apura\u00e7\u00e3o por Projeto",
                    "Rateio Administrativo", "Cen\u00e1rios", "Cadastros", "Ajustes/Percebidos", "Exportar"])
    with tabs[0]:
        _tab_posicao(conn, quotas_view, apur_view, cfg, medida)
    with tabs[1]:
        _tab_apuracao(apur_view, cfg)
    with tabs[2]:
        _tab_regras(conn, dre_full, cfg)
        st.markdown("---")
        _tab_rateio_preview(aloc, sobras, dre, cfg, mcol)
    with tabs[3]:
        _tab_cenarios(conn, dre, cfg, regras, aloc, sobras, apur, mcol)
    with tabs[4]:
        _tab_cadastros(conn, dre_full)
    with tabs[5]:
        _tab_ajustes(conn, dre_full)
    with tabs[6]:
        st.subheader("Exportar Excel")
        st.caption("Gera o pacote completo com os par\u00e2metros e filtros ATUAIS: posi\u00e7\u00e3o "
                   "dos s\u00f3cios, quotas por projeto, apura\u00e7\u00e3o mensal, rateio, regras e ajustes.")
        if st.button("Gerar Excel", key="pc_xlsx_btn"):
            ajus = ajustes_df(conn)
            if not ajus.empty:
                ajus["Efeito"] = ajus.apply(lambda r: efeito_ajuste(r["tipo"], r["valor"]), axis=1)
            qs = quotas_view.groupby("socio")["Quota"].sum() if not quotas_view.empty \
                else pd.Series(dtype=float)
            ae = ajus.groupby("socio")["Efeito"].sum() if not ajus.empty else pd.Series(dtype=float)
            posx = pd.DataFrame({"Quotas": qs}).join(pd.DataFrame({"Ajustes": ae}),
                                                     how="outer").fillna(0.0)
            posx["Saldo"] = posx["Quotas"] + posx["Ajustes"]
            posx = posx.reset_index().rename(columns={"index": "Socio", "socio": "Socio"})
            qproj = (quotas_view.groupby(["Projeto", "socio", "tipo", "Visao"])
                     .agg(pct=("pct", "first"), Base=("Base", "sum"),
                          CreditoBWS=("CreditoBWS", "sum"), Quota=("Quota", "sum"))
                     .reset_index()) if not quotas_view.empty else pd.DataFrame()
            with st.spinner("Montando Excel..."):
                st.session_state["pc_xlsx"] = montar_xlsx(
                    posx, qproj, apur_view, aloc, sobras, parts, regras, ajus, cfg, medida)
        if st.session_state.get("pc_xlsx"):
            st.download_button("Baixar Excel", st.session_state["pc_xlsx"],
                               file_name="prestacao_contas_bws.xlsx",
                               mime="application/vnd.openxmlformats-officedocument."
                                    "spreadsheetml.sheet", key="pc_xlsx_dl")