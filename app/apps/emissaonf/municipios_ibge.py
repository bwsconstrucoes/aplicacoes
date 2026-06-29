# -*- coding: utf-8 -*-
"""
Resolve nome de município (como vem na C. Diários, ex.: "Santa Cruz do Capibaribe-PE")
para o código IBGE de 7 dígitos exigido pela NFS-e.

A C. Diários não tem coluna de IBGE; usamos a API pública do IBGE:
    https://servicodados.ibge.gov.br/api/v1/localidades/municipios
O worker baixa uma vez e guarda em cache local (JSON). Depois é tudo offline.
"""

from __future__ import annotations
import json
import os
import re
import unicodedata

URL_IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
CACHE_PADRAO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "municipios_ibge.json")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().upper()


def _extrair_uf(m: dict) -> str | None:
    """A estrutura do IBGE varia; tenta vários caminhos até achar a sigla da UF."""
    caminhos = [
        ("regiao-imediata", "regiao-intermediaria", "UF", "sigla"),
        ("microrregiao", "mesorregiao", "UF", "sigla"),
    ]
    for c in caminhos:
        no = m
        ok = True
        for chave in c:
            if isinstance(no, dict) and no.get(chave) is not None:
                no = no[chave]
            else:
                ok = False
                break
        if ok and isinstance(no, str):
            return no
    return None


def baixar_cache(caminho: str = CACHE_PADRAO) -> dict:
    """Baixa a lista do IBGE e monta o índice {(NOME, UF): codigo}. Salva em JSON."""
    import requests
    r = requests.get(URL_IBGE, timeout=60)
    r.raise_for_status()
    indice = {}
    for m in r.json():
        cod = str(m["id"])                       # 7 dígitos
        nome = _norm(m["nome"])
        uf = _extrair_uf(m)
        if not uf:
            continue
        indice[f"{nome}|{uf}"] = cod
    with open(caminho, "w", encoding="utf-8") as fh:
        json.dump(indice, fh, ensure_ascii=False)
    return indice


def carregar_cache(caminho: str = CACHE_PADRAO) -> dict:
    if not os.path.exists(caminho):
        return baixar_cache(caminho)
    with open(caminho, encoding="utf-8") as fh:
        return json.load(fh)


def resolver(municipio: str, cache: dict, uf: str = None) -> str:
    """Resolve "Nome-UF" (ou nome + uf separado) para o código IBGE. Critica se não achar."""
    nome = municipio.strip()
    if uf is None:
        m = re.search(r"[-/]\s*([A-Za-z]{2})\s*$", nome)
        if m:
            uf = m.group(1).upper()
            nome = nome[: m.start()].strip()
    if not uf:
        raise ValueError(f"Município '{municipio}' sem UF — não dá para resolver o IBGE com segurança.")
    chave = f"{_norm(nome)}|{uf.upper()}"
    cod = cache.get(chave)
    if cod:
        return cod
    # fallback: nome abreviado na C. Diários -> match ÚNICO por aproximação dentro da UF
    alvo, ufx = _norm(nome), uf.upper()
    def _busca(criterio):
        achados = {}
        for k, c in cache.items():
            kn, _, ku = k.partition("|")
            if ku == ufx and criterio(kn):
                achados[kn] = c
        return achados
    for criterio in (lambda kn: kn.startswith(alvo), lambda kn: alvo in kn):
        cand = _busca(criterio)
        if len(cand) == 1:
            kn, c = next(iter(cand.items()))
            print(f"  [aviso IBGE] '{municipio}' casou por aproximação com '{kn.title()}-{ufx}' "
                  f"(cód {c}). CONFIRME se é esse o município da obra.")
            return c
        if len(cand) > 1:
            nomes = ", ".join(sorted(k.title() for k in cand))
            raise ValueError(
                f"Município '{municipio}' ({ufx}) é ambíguo no IBGE: {nomes}. "
                f"Use o nome completo na C. Diários."
            )
    raise ValueError(
        f"Município '{municipio}' (UF {uf}) não encontrado na tabela do IBGE — confira o nome na C. Diários."
    )
