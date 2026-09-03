# -*- coding: utf-8 -*-
"""
rateio.py — gera os JSONs de rateio para atualizar título no Omie.

Portado do Apps Script `gerarRateiosJSON`:
  - Situação 1 (Centro de Custo): "distribuicao" com percentuais (nValDep:null).
  - Situação 2 (Categoria de Despesa): "categorias" com percentual E valor,
    distribuído sobre uma base (valor informado ou a soma dos valores).
Ambos usam arredondamento de "menor erro" (maior resto) para fechar 100% / a base.
"""

import math
import re


def _percentuais_min_erro(valores, casas: int = 7):
    total = sum(valores)
    n = len(valores)
    if not (total > 0):
        return [0.0] * n
    scale = 10 ** casas
    raws = [v / total * 100 for v in valores]
    base = [math.floor(r * scale) / scale for r in raws]
    soma_base = sum(base)
    delta = round(100 - soma_base, casas + 3)
    steps = round(delta * scale)
    idxs = [[i, (r * scale) - math.floor(r * scale)] for i, r in enumerate(raws)]
    if steps > 0:
        idxs.sort(key=lambda a: a[1], reverse=True)
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            base[j] = round(base[j] + 1 / scale, casas)
    elif steps < 0:
        steps = -steps
        idxs.sort(key=lambda a: a[1])
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            base[j] = round(base[j] - 1 / scale, casas)
    return [round(x, casas) for x in base]


def _alocar_valores(percentuais, base, casas: int = 2):
    scale = 10 ** casas
    raws = [base * (p / 100) for p in percentuais]
    btrunc = [math.floor(r * scale) / scale for r in raws]
    soma_base = sum(btrunc)
    delta = round(base - soma_base, casas + 3)
    steps = round(delta * scale)
    idxs = [[i, (r * scale) - math.floor(r * scale)] for i, r in enumerate(raws)]
    if steps > 0:
        idxs.sort(key=lambda a: a[1], reverse=True)
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            btrunc[j] = round(btrunc[j] + 1 / scale, casas)
    elif steps < 0:
        steps = -steps
        idxs.sort(key=lambda a: a[1])
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            btrunc[j] = round(btrunc[j] - 1 / scale, casas)
    return [round(x, casas) for x in btrunc]


def _num(x, casas: int) -> str:
    """Número 'enxuto' como o Number(x.toFixed(n)) do JS: sem zeros à toa."""
    x = round(float(x), casas)
    s = ("%.*f" % (casas, x)).rstrip("0").rstrip(".")
    return s if s not in ("", "-0", "-") else "0"


def _to_float(v) -> float:
    """Interpreta valores em padrão CONTÁBIL BR ('1.234,56', 'R$ 994,12', '1.234',
    '(1.000,00)' = negativo) e também colagens em padrão US ('1,234.56')."""
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return 0.0 if (isinstance(v, float) and math.isnan(v)) else float(v)
    s = str(v).strip().replace("\u00a0", "").replace(" ", "")
    s = s.replace("R$", "").replace("r$", "")
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg, s = True, s[1:-1]
    if s.startswith("-"):
        neg, s = True, s[1:]
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):          # BR: 1.234,56
            s = s.replace(".", "").replace(",", ".")
        else:                                     # US: 1,234.56
            s = s.replace(",", "")
    elif "," in s:
        # vírgula única = decimal BR; várias = milhar US (1,234,567)
        s = s.replace(",", ".") if s.count(",") == 1 else s.replace(",", "")
    elif "." in s:
        # só ponto: grupos de 3 = milhar BR (1.234 / 1.234.567); senão decimal
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
            s = s.replace(".", "")
    try:
        x = float(s)
    except ValueError:
        return 0.0
    return -x if neg else x


def gerar_jsons(linhas_cc, linhas_cat, base_cat=None) -> dict:
    """
    linhas_cc:  [{"obra","codigo","valor"}]  (Situação 1)
    linhas_cat: [{"categoria","codigo","valor"}] (Situação 2)
    base_cat:   valor a ratear na categoria (F5); se None/0 usa a soma.
    Retorna {"distribuicao": str|None, "categorias": str|None, "erro": str|None}.
    """
    map_cc = {}
    for l in linhas_cc or []:
        cod = str(l.get("codigo", "")).strip()
        if not cod:
            continue
        obra = str(l.get("obra", "")).strip()
        key = (obra + "||" + cod).upper()
        if key not in map_cc:
            map_cc[key] = {"obra": obra, "codigo": cod, "valor": 0.0}
        map_cc[key]["valor"] += _to_float(l.get("valor"))
    lcc = list(map_cc.values())

    map_cat = {}
    for l in linhas_cat or []:
        cod = str(l.get("codigo", "")).strip()
        if not cod:
            continue
        cat = str(l.get("categoria", "")).strip()
        key = (cat + "||" + cod).upper()
        if key not in map_cat:
            map_cat[key] = {"categoria": cat, "codigo": cod, "valor": 0.0}
        map_cat[key]["valor"] += _to_float(l.get("valor"))
    lcat = list(map_cat.values())

    gerar_s1 = len(lcc) > 0
    gerar_s2 = len(lcat) > 0 and gerar_s1

    if not gerar_s1:
        if lcat:
            return {"distribuicao": None, "categorias": None,
                    "erro": "A Categoria só é gerada junto com o Centro de Custo. "
                            "Preencha ao menos um Centro de Custo (com código)."}
        return {"distribuicao": None, "categorias": None,
                "erro": "Nada a gerar: informe ao menos um Centro de Custo com código."}

    out = {"distribuicao": None, "categorias": None, "erro": None}

    perc1 = _percentuais_min_erro([l["valor"] for l in lcc], 7)
    partes1 = ['{"cCodDep":"%s","cDesDep":"%s","nPerDep":%s,"nValDep":null}'
               % (l["codigo"], l["obra"].replace('"', '\\"'), _num(perc1[i], 7))
               for i, l in enumerate(lcc)]
    out["distribuicao"] = '"distribuicao":\n[' + ",".join(partes1) + ']'

    if gerar_s2:
        perc2 = _percentuais_min_erro([l["valor"] for l in lcat], 7)
        soma_f = sum(l["valor"] for l in lcat)
        base_cat = _to_float(base_cat) if base_cat not in (None, "") else None
        base_val = base_cat if (base_cat and base_cat > 0) else soma_f
        vals = _alocar_valores(perc2, base_val, 2)
        partes2 = ['{"codigo_categoria":"%s","percentual":%s,"valor":%s}'
                   % (l["codigo"], _num(perc2[i], 7), _num(vals[i], 2))
                   for i, l in enumerate(lcat)]
        out["categorias"] = '"categorias":\n[' + ",".join(partes2) + ']'

    return out
