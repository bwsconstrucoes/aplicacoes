# -*- coding: utf-8 -*-
"""
O PERCENTUAL DA HORA EXTRA, POR ESTADO OU POR OBRA (29/10/2026) — o evento que vai no .ps do Fortes.

O dono: *"existe mais de um tipo de evento de hora extra (…) 70%, 100% (…) isso varia em função de quê?
Atualmente a gente tem obra em Pernambuco, em São Paulo e no Ceará (…) a gente tem que ter algum canto para
configurar isso: colocar a UF, ou mesmo especificamente uma obra, e definir qual é o percentual de hora
extra (…) a hora extra da semana é tantos por cento, a do feriado é tanto, a do final de semana é tanto."*

QUATRO TIPOS DE DIA: semana (segunda a sexta), sábado, domingo e feriado — o feriado vence o dia da semana.
TRÊS ALCANCES, do mais específico ao mais geral: a OBRA (pelo código), o ESTADO (a UF da obra, que vem da
coluna AC da C. Diários — `base_obras`) e o PADRÃO da empresa. O dia vai pela obra em que a pessoa bateu
nele (a da primeira batida); sem batida, pela obra do cadastro.

OS PERCENTUAIS são os que o Fortes tem evento (`prompt_geracao_ps.md`): 50% (006), 60% (005), 70% (004) e
100% (003). O padrão nasce como estava: 50% de segunda a sábado, 100% no domingo e no feriado.

Guardado em `ponto.parametros` ("hora_extra.percentuais", JSON) — sem migração.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Optional

from sqlalchemy.engine import Connection

from ..erros import ErroDeValidacao
from . import parametros

PARAMETRO = "hora_extra.percentuais"
EVENTO_DO_PERCENTUAL = {50: "006", 60: "005", 70: "004", 100: "003"}
PERCENTUAIS = tuple(sorted(EVENTO_DO_PERCENTUAL))
TIPOS_DE_DIA = ("semana", "sabado", "domingo", "feriado")
ROTULO_TIPO = {"semana": "Semana (seg a sex)", "sabado": "Sábado", "domingo": "Domingo", "feriado": "Feriado"}
PADRAO = {"semana": 50, "sabado": 50, "domingo": 100, "feriado": 100}
ESCOPOS = ("PADRAO", "UF", "OBRA")


def tipo_do_dia(data: dt.date, feriado) -> str:
    """PURA. O feriado vence o dia da semana."""
    if feriado:
        return "feriado"
    return {5: "sabado", 6: "domingo"}.get(data.weekday(), "semana")


def _regra(bruta) -> dict:
    """PURA. Uma regra completa: o que faltar vem do padrão."""
    r = dict(PADRAO)
    for t in TIPOS_DE_DIA:
        try:
            v = int((bruta or {}).get(t))
        except (TypeError, ValueError):
            continue
        if v in EVENTO_DO_PERCENTUAL:
            r[t] = v
    return r


def ler(conn: Connection) -> dict:
    """{"padrao": {...}, "uf": {"PE": {...}}, "obra": {"AP-01": {...}}}."""
    try:
        bruto = json.loads(parametros.ler(conn, PARAMETRO, "") or "{}")
    except ValueError:
        bruto = {}
    return {"padrao": _regra(bruto.get("padrao")),
            "uf": {str(k).upper(): _regra(v) for k, v in (bruto.get("uf") or {}).items()},
            "obra": {str(k).upper(): _regra(v) for k, v in (bruto.get("obra") or {}).items()}}


def regra_de(cfg: dict, obra: Optional[str], uf: Optional[str]) -> tuple[dict, str]:
    """PURA. (regra, de onde veio) — a da obra, a do estado ou a padrão."""
    if obra and str(obra).upper() in cfg["obra"]:
        return cfg["obra"][str(obra).upper()], f"obra {str(obra).upper()}"
    if uf and str(uf).upper() in cfg["uf"]:
        return cfg["uf"][str(uf).upper()], f"estado {str(uf).upper()}"
    return cfg["padrao"], "padrão da empresa"


def percentual_do_dia(cfg: dict, data: dt.date, feriado, obra: Optional[str], uf: Optional[str]) -> int:
    regra, _ = regra_de(cfg, obra, uf)
    return regra[tipo_do_dia(data, feriado)]


def gravar(conn: Connection, escopo: str, chave: Optional[str], valores: dict, por: str) -> dict:
    escopo = str(escopo or "").upper()
    if escopo not in ESCOPOS:
        raise ErroDeValidacao("diga se a regra é da empresa, de um estado ou de uma obra", campo="escopo")
    regra = {}
    for t in TIPOS_DE_DIA:
        try:
            v = int(valores.get(t))
        except (TypeError, ValueError):
            raise ErroDeValidacao(f"diga o percentual de {ROTULO_TIPO[t].lower()}", campo=t) from None
        if v not in EVENTO_DO_PERCENTUAL:
            raise ErroDeValidacao(f"o Fortes só tem evento para {', '.join(f'{p}%' for p in PERCENTUAIS)}", campo=t)
        regra[t] = v
    cfg = ler(conn)
    if escopo == "PADRAO":
        cfg["padrao"] = regra
    else:
        chave = " ".join(str(chave or "").split()).upper()
        if not chave:
            raise ErroDeValidacao("diga o estado (UF)" if escopo == "UF" else "diga o código da obra", campo="chave")
        if escopo == "UF" and (len(chave) != 2 or not chave.isalpha()):
            raise ErroDeValidacao("a UF tem duas letras (ex.: PE)", campo="chave")
        if escopo == "OBRA":
            from . import cadastros
            o = cadastros.resolver_obra(conn, chave)
            if not o:
                raise ErroDeValidacao(f"obra não encontrada: {chave}", campo="chave")
            chave = str(o["codigo"]).upper()
        cfg["uf" if escopo == "UF" else "obra"][chave] = regra
    parametros.gravar(conn, PARAMETRO, json.dumps(cfg, ensure_ascii=False), por)
    return cfg


def excluir(conn: Connection, escopo: str, chave: str, por: str) -> dict:
    escopo = str(escopo or "").upper()
    if escopo not in ("UF", "OBRA"):
        raise ErroDeValidacao("a regra padrão da empresa não se exclui — ajuste os percentuais", campo="escopo")
    cfg = ler(conn)
    cfg["uf" if escopo == "UF" else "obra"].pop(str(chave or "").upper(), None)
    parametros.gravar(conn, PARAMETRO, json.dumps(cfg, ensure_ascii=False), por)
    return cfg


def para_json(cfg: dict) -> dict:
    linhas = [{"escopo": "PADRAO", "chave": None, "rotulo": "Padrão da empresa", **cfg["padrao"]}]
    linhas += [{"escopo": "UF", "chave": k, "rotulo": f"Estado {k}", **v} for k, v in sorted(cfg["uf"].items())]
    linhas += [{"escopo": "OBRA", "chave": k, "rotulo": f"Obra {k}", **v} for k, v in sorted(cfg["obra"].items())]
    return {"regras": linhas, "percentuais": list(PERCENTUAIS), "tipos": [{"codigo": t, "rotulo": ROTULO_TIPO[t]}
                                                                          for t in TIPOS_DE_DIA]}
