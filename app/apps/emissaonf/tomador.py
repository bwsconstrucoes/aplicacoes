# -*- coding: utf-8 -*-
"""
Resolve os dados do tomador a partir do CNPJ, via BrasilAPI (gratuita, sem token):
    https://brasilapi.com.br/api/cnpj/v1/{cnpj}
Com cache local (tomadores_cnpj.json) — o tomador se repete muito (ex.: SEDUC).
Devolve endereço estruturado + código IBGE do município do tomador.
"""
from __future__ import annotations
import json
import os
import re
from dataclasses import dataclass, field

from municipios_ibge import resolver as resolver_ibge

URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tomadores_cnpj.json")


def _dig(s) -> str:
    return re.sub(r"\D", "", str(s or ""))


@dataclass
class Tomador:
    cnpj: str = ""
    razao_social: str = ""
    logradouro: str = ""
    numero: str = ""
    bairro: str = ""
    municipio: str = ""
    uf: str = ""
    cep: str = ""
    cmun_ibge: str = ""
    fonte: str = ""
    avisos: list = field(default_factory=list)
    ok: bool = False


def _le_cache(p):
    if os.path.exists(p):
        try:
            with open(p, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}


def _grava_cache(c, p):
    try:
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(c, fh, ensure_ascii=False)
    except Exception:
        pass


def _g(d, *ks):
    for k in ks:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return ""


def _via_brasilapi(cnpj, timeout):
    import requests
    r = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
                     timeout=timeout, headers={"Accept": "application/json"})
    if r.status_code == 429:
        raise _Limite("HTTP 429")
    if r.status_code == 404:
        raise LookupError("CNPJ não encontrado")
    r.raise_for_status()
    j = r.json()
    return {"razao_social": _g(j, "razao_social"), "logradouro": _g(j, "logradouro"),
            "numero": _g(j, "numero"), "bairro": _g(j, "bairro"),
            "municipio": _g(j, "municipio"), "uf": _g(j, "uf"), "cep": _g(j, "cep"),
            "codigo_municipio_ibge": _g(j, "codigo_municipio_ibge")}


def _via_minhareceita(cnpj, timeout):
    import requests
    r = requests.get(f"https://minhareceita.org/{cnpj}", timeout=timeout,
                     headers={"Accept": "application/json"})
    if r.status_code == 429:
        raise _Limite("HTTP 429")
    if r.status_code == 404:
        raise LookupError("CNPJ não encontrado")
    r.raise_for_status()
    j = r.json()
    return {"razao_social": _g(j, "razao_social", "nome"), "logradouro": _g(j, "logradouro"),
            "numero": _g(j, "numero"), "bairro": _g(j, "bairro"),
            "municipio": _g(j, "municipio"), "uf": _g(j, "uf"), "cep": _g(j, "cep"),
            "codigo_municipio_ibge": _g(j, "codigo_municipio_ibge")}


def _via_cnpjws(cnpj, timeout):
    import requests
    r = requests.get(f"https://publica.cnpj.ws/cnpj/{cnpj}", timeout=timeout,
                     headers={"Accept": "application/json"})
    if r.status_code == 429:
        raise _Limite("HTTP 429")
    if r.status_code == 404:
        raise LookupError("CNPJ não encontrado")
    r.raise_for_status()
    j = r.json()
    est = j.get("estabelecimento", {}) or {}
    cid = est.get("cidade", {}) or {}
    est_uf = (est.get("estado", {}) or {}).get("sigla", "")
    return {"razao_social": _g(j, "razao_social"), "logradouro": _g(est, "logradouro"),
            "numero": _g(est, "numero"), "bairro": _g(est, "bairro"),
            "municipio": _g(cid, "nome"), "uf": est_uf, "cep": _g(est, "cep"),
            "codigo_municipio_ibge": str(_g(cid, "ibge_id"))}


class _Limite(Exception):
    pass


PROVEDORES = [("BrasilAPI", _via_brasilapi), ("MinhaReceita", _via_minhareceita), ("CNPJ.ws", _via_cnpjws)]


def consultar_cnpj(cnpj, timeout=25, tentativas=4) -> dict:
    """Tenta as fontes em ordem; em 429/erro passa para a próxima. Devolve o 1º sucesso."""
    import time
    cnpj_d = _dig(cnpj)
    erros = []
    for nome, fn in PROVEDORES:
        try:
            dados = fn(cnpj_d, timeout)
            if dados and (dados.get("logradouro") or dados.get("municipio")):
                dados["_fonte"] = nome
                return dados
            erros.append(f"{nome}: resposta sem endereço")
        except _Limite as e:
            erros.append(f"{nome}: {e}")
            time.sleep(2)  # respira e tenta a próxima fonte
        except LookupError as e:
            erros.append(f"{nome}: {e}")
        except Exception as e:
            erros.append(f"{nome}: {type(e).__name__}")
    raise RuntimeError("nenhuma fonte de CNPJ respondeu — " + " | ".join(erros))


def buscar_tomador(cnpj, cache_ibge, cache_path=CACHE, usar_cache=True) -> Tomador:
    cnpj_d = _dig(cnpj)
    av = []
    cache = _le_cache(cache_path) if usar_cache else {}
    dados = cache.get(cnpj_d)
    if dados is None:
        try:
            dados = consultar_cnpj(cnpj_d)
            cache[cnpj_d] = dados
            _grava_cache(cache, cache_path)
        except Exception as e:
            return Tomador(cnpj=cnpj_d, fonte="falha", avisos=[f"BrasilAPI: {e}"], ok=False)

    municipio = (dados.get("municipio") or "").strip()
    uf = (dados.get("uf") or "").strip()
    cmun = str(dados.get("codigo_municipio_ibge") or "").strip()
    if not (cmun.isdigit() and len(cmun) == 7):
        try:
            cmun = resolver_ibge(f"{municipio}-{uf}", cache_ibge)
        except Exception:
            cmun = ""
            av.append(f"IBGE do tomador '{municipio}-{uf}' não resolveu")
    return Tomador(
        cnpj=cnpj_d,
        razao_social=(dados.get("razao_social") or "").strip(),
        logradouro=(dados.get("logradouro") or "").strip(),
        numero=str(dados.get("numero") or "").strip(),
        bairro=(dados.get("bairro") or "").strip(),
        municipio=municipio, uf=uf, cep=_dig(dados.get("cep")),
        cmun_ibge=cmun, fonte=dados.get("_fonte", "API"), avisos=av, ok=True,
    )
