# -*- coding: utf-8 -*-
"""
auditoria.py — análises de auditoria de lançamentos (funções puras, testáveis).

  - pontualidade(df, min_lanc)   -> ranking por Responsável (antecedência registro)
  - barras_duplicadas(df)        -> códigos de barras (AI) repetidos
  - codigos_invalidos(df)        -> AI com "INVALIDO"
  - risco_ia(df)                 -> linhas "COM RISCO" (AL) + IDs referenciados
  - nf_duplicada(df)             -> mesmo CPF/CNPJ + Nº NF
  - sem_classificacao(df)        -> sem Centro de Custo e/ou Projeto
  - possivel_duplicidade(df, d)  -> mesmo CPF/CNPJ + valor em janela de N dias
"""

import re
import pandas as pd

_ID_RE = re.compile(r"\b(\d{8,})\b")


def _resp(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().replace("", "(sem responsável)")


def pontualidade(df: pd.DataFrame, min_lanc: int = 1) -> pd.DataFrame:
    """
    Antecedência = vencimento − solicitação (dias). Negativo = registrado
    DEPOIS do vencimento (atrasado, gera juros). Agrupa por Responsável (col K).
    """
    cols = ["Responsável", "Qtd", "Média dias", "Mediana", "Atrasados",
            "% Atrasados", "R$ Atrasado", "R$ Total"]
    if df.empty or "solicitacao_dt" not in df.columns or "vencimento_dt" not in df.columns:
        return pd.DataFrame(columns=cols)
    d = df[df["solicitacao_dt"].notna() & df["vencimento_dt"].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=cols)
    d["antec"] = (d["vencimento_dt"] - d["solicitacao_dt"]).dt.days
    d["resp"] = _resp(d["responsavel"])
    d["atraso"] = d["antec"] < 0
    g = d.groupby("resp")
    base = g.agg(Qtd=("antec", "size"),
                 Media=("antec", "mean"),
                 Mediana=("antec", "median"),
                 Atrasados=("atraso", "sum"),
                 RS_Total=("valor_num", "sum")).reset_index()
    rs_atras = (d[d["atraso"]].groupby("resp")["valor_num"].sum()
                .rename("RS_Atrasado").reset_index())
    out = base.merge(rs_atras, on="resp", how="left").fillna({"RS_Atrasado": 0.0})
    out = out[out["Qtd"] >= min_lanc]
    out["Media"] = out["Media"].round(1)
    out["Mediana"] = out["Mediana"].round(1)
    out["% Atrasados"] = (out["Atrasados"] / out["Qtd"] * 100).round(1)
    out = out.rename(columns={"resp": "Responsável", "Media": "Média dias",
                              "RS_Atrasado": "R$ Atrasado", "RS_Total": "R$ Total"})
    out["Atrasados"] = out["Atrasados"].astype(int)
    out = out.sort_values("Média dias").reset_index(drop=True)
    return out[cols]


def _so_boleto(df: pd.DataFrame) -> pd.DataFrame:
    if "forma_pagamento" not in df.columns:
        return df
    forma = df["forma_pagamento"].astype(str).str.lower()
    return df[forma.str.contains("boleto", na=False)]


def barras_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Códigos de barras (AI) repetidos — só para forma de pagamento **Boleto**.
    Retorna o DETALHE (uma linha por lançamento) para o ID virar link.
    Ignora vazios e os 'INVALIDO' (estes vão em codigos_invalidos).
    """
    cols = ["Código de Barras", "id", "credor", "valor_num", "Qtd grupo"]
    if df.empty or "codigo_barras" not in df.columns:
        return pd.DataFrame(columns=cols)
    d = _so_boleto(df).copy()
    if d.empty:
        return pd.DataFrame(columns=cols)
    cb = d["codigo_barras"].astype(str).str.strip()
    d["Código de Barras"] = cb.str.replace(r"[.\s]", "", regex=True)
    d = d[(d["Código de Barras"] != "") & (~cb.str.upper().str.contains("INVALIDO", na=False))]
    if d.empty:
        return pd.DataFrame(columns=cols)
    d["Qtd grupo"] = d.groupby("Código de Barras")["id"].transform("size")
    d = d[d["Qtd grupo"] > 1].sort_values(["Qtd grupo", "Código de Barras"],
                                          ascending=[False, True])
    return d[cols].reset_index(drop=True)


def codigos_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    """Boletos cujo código de barras (AI) contém 'INVALIDO' (precisa recadastrar)."""
    cols = ["id", "credor", "valor_num", "forma_pagamento", "codigo_barras"]
    if df.empty or "codigo_barras" not in df.columns:
        return pd.DataFrame(columns=cols)
    d = _so_boleto(df)
    m = d["codigo_barras"].astype(str).str.upper().str.contains("INVALIDO", na=False)
    return d.loc[m, [c for c in cols if c in d.columns]].reset_index(drop=True)


def risco_ia(df: pd.DataFrame) -> pd.DataFrame:
    """Linhas marcadas 'COM RISCO' na análise da IA (AL), com IDs referenciados."""
    cols = ["id", "credor", "documento", "valor_num", "ref_ids", "analise_ia"]
    if df.empty or "analise_ia" not in df.columns:
        return pd.DataFrame(columns=cols)
    m = df["analise_ia"].astype(str).str.upper().str.contains("COM RISCO", na=False)
    d = df.loc[m].copy()
    if d.empty:
        return pd.DataFrame(columns=cols)

    def _refs(row):
        achados = _ID_RE.findall(str(row["analise_ia"]))
        prop = str(row["id"])
        return ", ".join(dict.fromkeys(i for i in achados if i != prop))

    d["ref_ids"] = d.apply(_refs, axis=1)
    return d[[c for c in cols if c in d.columns]].reset_index(drop=True)


def nf_duplicada(df: pd.DataFrame) -> pd.DataFrame:
    """Mesmo CPF/CNPJ + mesmo Nº NF em lançamentos diferentes (detalhe por ID)."""
    cols = ["CPF/CNPJ", "Nº NF", "id", "credor", "valor_num", "Qtd grupo"]
    if df.empty or "nf" not in df.columns:
        return pd.DataFrame(columns=cols)
    d = df.copy()
    d["Nº NF"] = d["nf"].astype(str).str.strip()
    d["CPF/CNPJ"] = d["documento"].astype(str).str.strip()
    d = d[(d["Nº NF"] != "") & (d["CPF/CNPJ"] != "")]
    if d.empty:
        return pd.DataFrame(columns=cols)
    d["Qtd grupo"] = d.groupby(["CPF/CNPJ", "Nº NF"])["id"].transform("size")
    d = d[d["Qtd grupo"] > 1].sort_values(["Qtd grupo", "CPF/CNPJ", "Nº NF"],
                                          ascending=[False, True, True])
    return d[cols].reset_index(drop=True)


def sem_classificacao(df: pd.DataFrame) -> pd.DataFrame:
    """Lançamentos sem Centro de Custo e/ou sem Projeto (cadastro incompleto)."""
    cols = ["id", "credor", "valor_num", "centro_custo", "projeto", "Faltando"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    cc = df["centro_custo"].astype(str).str.strip() if "centro_custo" in df.columns else ""
    pj = df["projeto"].astype(str).str.strip() if "projeto" in df.columns else ""
    sem_cc = cc == ""
    sem_pj = pj == ""
    m = sem_cc | sem_pj
    d = df.loc[m].copy()
    if d.empty:
        return pd.DataFrame(columns=cols)
    falt = []
    for i in d.index:
        partes = []
        if (cc[i] if hasattr(cc, "__getitem__") else "") == "":
            partes.append("Centro de Custo")
        if (pj[i] if hasattr(pj, "__getitem__") else "") == "":
            partes.append("Projeto")
        falt.append(" + ".join(partes))
    d["Faltando"] = falt
    return d[[c for c in cols if c in d.columns]].reset_index(drop=True)


def sem_integracao_omie(df: pd.DataFrame) -> pd.DataFrame:
    """
    Títulos sem código de integração Omie (col P) que ainda estão ATIVOS
    (não Cancelado e não Pago) — precisam ser integrados ao Omie.
    """
    cols = ["id", "credor", "valor_num", "status_pgt", "codigo_integracao"]
    if df.empty or "codigo_integracao" not in df.columns:
        return pd.DataFrame(columns=cols)
    status = df["status_pgt"].astype(str).str.strip().str.lower()
    ativo = ~status.isin(["cancelado", "pago"])
    sem = df["codigo_integracao"].astype(str).str.strip().eq("")
    d = df.loc[sem & ativo, [c for c in cols if c in df.columns]]
    return d.reset_index(drop=True)


def possivel_duplicidade(df: pd.DataFrame, dias: int = 7) -> pd.DataFrame:
    """
    Checagem determinística (complementa a IA): mesmo CPF/CNPJ + mesmo valor,
    com lançamentos a até N dias um do outro. Retorna o detalhe por ID.
    """
    cols = ["CPF/CNPJ", "Valor", "id", "credor", "Janela (dias)", "Qtd grupo"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    d = df.copy()
    d["_doc"] = d["documento"].astype(str).str.strip()
    d = d[(d["_doc"] != "") & (d["valor_num"] > 0)]
    if d.empty:
        return pd.DataFrame(columns=cols)
    linhas = []
    for (doc, val), sub in d.groupby(["_doc", "valor_num"]):
        if len(sub) < 2:
            continue
        dts = (sub["solicitacao_dt"].dropna().sort_values()
               if "solicitacao_dt" in sub.columns else pd.Series([], dtype="datetime64[ns]"))
        janela = int((dts.iloc[-1] - dts.iloc[0]).days) if len(dts) >= 2 else 0
        if len(dts) >= 2 and janela > dias:
            continue
        for _, r in sub.iterrows():
            linhas.append({"CPF/CNPJ": doc, "Valor": float(val), "id": str(r["id"]),
                           "credor": r.get("credor", ""), "Janela (dias)": janela,
                           "Qtd grupo": int(len(sub))})
    out = pd.DataFrame(linhas, columns=cols)
    if out.empty:
        return out
    return out.sort_values(["Qtd grupo", "Valor", "CPF/CNPJ"],
                           ascending=[False, False, True]).reset_index(drop=True)