# -*- coding: utf-8 -*-
"""
dados.py — Ponte cache(SQLite) -> DataFrame tipado + KPIs.

Converte valores BR ("6.750,00" -> 6750.0) e datas DD/MM/AAAA para uso em filtros,
ordenação e somatórios.
"""
from __future__ import annotations

import re
import numpy as np
import pandas as pd

import cache
from schema import ALL_KEYS, numero_keys, data_keys, COLS


def _br_para_float(v) -> float:
    """Conversão de um único valor BR (uso pontual). O carregamento usa a versão vetorizada."""
    s = str(v or "").strip()
    if not s:
        return 0.0
    s = re.sub(r"[^\d,.-]", "", s)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _serie_para_float(s: pd.Series) -> pd.Series:
    """Vetorizado: '6.750,00' -> 6750.0, '600' -> 600.0. Rápido para muitas linhas."""
    s = s.astype(str).str.strip()
    s = s.str.replace(r"[^\d,.\-]", "", regex=True)
    tem_virgula = s.str.contains(",", regex=False, na=False)
    # só onde há vírgula: ponto é milhar (remove) e vírgula é decimal (vira ponto)
    convertido = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    s = s.mask(tem_virgula, convertido)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _serie_para_data(s: pd.Series) -> pd.Series:
    """Vetorizado: pega os 10 primeiros chars (DD/MM/AAAA) e converte tudo de uma vez."""
    s = s.astype(str).str.strip().str.slice(0, 10)
    return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")


def _status_agend(status_pgt: pd.Series, agendado: pd.Series) -> pd.Series:
    """
    Status Agend NORMALIZADO, exibido SÓ quando Status Pgt = 'Pagar':
      contém 'falha'  -> 'Falha Agendar'   (ex.: 'falhaagendar', 'Falha Agendar')
      == 'verificar'  -> 'Verificar'
      == 'agendado'   -> 'Agendado'
      == 'agendar'    -> 'Agendar'          (exato; 'Desagendar' NÃO entra aqui)
      vazio/'Desagendar'/qualquer outro -> '' (em branco)
    Para Status Pgt diferente de 'Pagar' (Pago, Cancelado, etc.) -> ''.
    """
    st = status_pgt.astype(str).str.strip().str.lower()
    ag = agendado.astype(str).str.strip().str.lower()
    cond = [
        ag.str.contains("falha", na=False),
        ag.eq("verificar"),
        ag.eq("agendado"),
        ag.eq("agendar"),
    ]
    escolhas = ["Falha Agendar", "Verificar", "Agendado", "Agendar"]
    mapeado = np.select(cond, escolhas, default="")   # vazio / Desagendar / outro -> ''
    return pd.Series(np.where(st.eq("pagar"), mapeado, ""), index=status_pgt.index)


def carregar_df() -> pd.DataFrame:
    rows = cache.ler_tudo()
    cols = [c for c in ALL_KEYS] + ["_dirty"]
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    for k in ALL_KEYS:
        if k not in df.columns:
            df[k] = ""
    df = df.fillna("")

    # colunas numéricas/datas paralelas (sufixo _num / _dt) — VETORIZADO
    for k in numero_keys():
        df[k + "_num"] = _serie_para_float(df[k])
    for k in data_keys():
        df[k + "_dt"] = _serie_para_data(df[k])

    # colunas derivadas para exibição/filtros
    # Conta vem DIRETO da coluna U da SPsBD (sem C. Diários). conta_fmt é só o alias
    # usado pelo grid/filtros/KPIs.
    df["conta_fmt"] = df["conta"].astype(str).str.strip()
    df["status_agend"] = _status_agend(df["status_pgt"], df["agendado"])
    # Documentação Fiscal (planilha Lançamentos) — mapeada por ID da SP.
    _mapa_fiscal = cache.get_mapa_sp_fiscal()
    df["sp_fiscal"] = (df["id"].astype(str).str.strip().map(_mapa_fiscal).fillna("")
                       if _mapa_fiscal else "")
    df["pipefy_url"] = "https://app.pipefy.com/open-cards/" + df["id"].astype(str)
    # Risco de duplicidade: a análise da IA (coluna AL) contém "COM RISCO".
    df["risco"] = df["analise_ia"].astype(str).str.upper().str.contains("COM RISCO", na=False)
    # Alerta laranja: cadastro/integração que impede ou atrasa o pagamento.
    #  (a) Pix/BeeVale com a coluna Y só "Chave Pix:" (sem a chave);
    #  (b) Centro de Custo (col H) em branco;
    #  (c) Integração Omie (col P) sem código, com o título ainda ativo
    #      (não Cancelado e não Pago).
    _forma = df["forma_pagamento"].astype(str).str.lower()
    _pix_like = _forma.str.contains("pix", na=False) | _forma.str.contains("beevale", na=False)
    _chave = (df["info_pgt"].astype(str)
              .str.replace(r"(?i)chave\s*pix\s*:?\s*", "", regex=True).str.strip())
    _falta_chave = _pix_like & _chave.eq("")
    _sem_cc = df["centro_custo"].astype(str).str.strip().eq("")
    _status = df["status_pgt"].astype(str).str.strip().str.lower()
    _ativo = ~_status.isin(["cancelado", "pago"])
    _sem_omie = df["codigo_integracao"].astype(str).str.strip().eq("") & _ativo
    df["alerta_laranja"] = _falta_chave | _sem_cc | _sem_omie

    # Lista de pendências por linha (para destaque no detalhe). Ordem fixa.
    df["pendencias"] = [
        " · ".join(
            ([] if not cc else ["Centro de Custo"])
            + ([] if not ch else ["Chave Pix"])
            + ([] if not om else ["Integração Omie"]))
        for cc, ch, om in zip(_sem_cc.values, _falta_chave.values, _sem_omie.values)
    ]
    return df


# ----------------------------------------------------------------------------
# KPIs
# ----------------------------------------------------------------------------

def soma_por_conta(df: pd.DataFrame) -> dict:
    """Σ Valor agrupado por Conta normalizada (ex.: 7011-4)."""
    if df.empty:
        return {}
    col = "conta_fmt" if "conta_fmt" in df.columns else "conta"
    g = df.groupby(df[col].replace("", "(sem conta)"))["valor_num"].sum()
    return {k: float(v) for k, v in g.sort_values(ascending=False).items()}


def soma_por_forma(df: pd.DataFrame) -> dict:
    """Σ Valor + contagem por Forma de Pagamento (coluna J)."""
    if df.empty:
        return {}
    out = {}
    for forma, sub in df.groupby(df["forma_pagamento"].replace("", "(sem forma)")):
        out[forma] = {"qtd": int(len(sub)), "soma": float(sub["valor_num"].sum())}
    return out


def contagem_por_status(df: pd.DataFrame) -> dict:
    """Contagem por Status Pgt (coluna O)."""
    if df.empty:
        return {}
    g = df["status_pgt"].replace("", "(sem status)").value_counts()
    return {k: int(v) for k, v in g.items()}


def contagem_agendamento(df: pd.DataFrame) -> dict:
    """
    Buckets de agendamento espelhando a tela: Agendar / Agendado / Pago / Falha Agendar.
    Deriva da coluna Agendado (AB) + Status Pgt (O). VETORIZADO (rápido p/ muitas linhas).
    """
    if df.empty:
        return {"Agendar": 0, "Agendado": 0, "Pago": 0, "Falha Agendar": 0}
    ag = df["agendado"].astype(str).str.strip().str.lower()
    stp = df["status_pgt"].astype(str).str.strip().str.lower()
    pago = stp.eq("pago")
    falha = ag.str.contains("falha", na=False) & ~pago
    agendado = (ag.eq("agendado") | stp.eq("agendado")) & ~pago & ~falha
    agendar = ~pago & ~falha & ~agendado
    return {
        "Agendar": int(agendar.sum()),
        "Agendado": int(agendado.sum()),
        "Pago": int(pago.sum()),
        "Falha Agendar": int(falha.sum()),
    }


def total_geral(df: pd.DataFrame) -> float:
    return float(df["valor_num"].sum()) if not df.empty else 0.0