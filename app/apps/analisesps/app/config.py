# -*- coding: utf-8 -*-
"""
config.py — tokens/segredos lidos de uma planilha SEPARADA (só compartilhada com a
service account + admins), com cache local (offline) e prioridade para variável de
ambiente (Render secret / local).

A credencial do Google (service account) NÃO fica aqui — ela é a chave-mestra e
continua local/at env, pois é preciso dela justamente para ler esta planilha.
"""
from __future__ import annotations

import os
import json

import cache

# ID da planilha SÓ de tokens/credenciais. Compartilhe apenas com a service account + você.
PLANILHA_CONFIG = os.environ.get("SPSBD_CONFIG_SHEET",
                                 "1D4aVC7wVHL_t-5QpI6v7vtLJMjJpA7DpDnByTFB9i-U")
ABA_CONFIG = "Credenciais"

_META_KEY = "tokens_cfg"


def atualizar_tokens() -> dict:
    """Lê a planilha de config (Chave|Valor) e guarda no cache local. Precisa de internet."""
    if not PLANILHA_CONFIG:
        raise RuntimeError("Defina PLANILHA_CONFIG (ID da planilha de tokens) "
                           "ou a variável de ambiente SPSBD_CONFIG_SHEET.")
    import gsheets
    vals = gsheets._abrir_aba(ABA_CONFIG, planilha_id=PLANILHA_CONFIG).get_all_values()
    d = {}
    for r in (vals[1:] if vals else []):
        if len(r) >= 2 and str(r[0]).strip():
            d[str(r[0]).strip()] = str(r[1]).strip()
    cache.set_meta(_META_KEY, json.dumps(d, ensure_ascii=False))
    return d


def _tokens_cache() -> dict:
    try:
        return json.loads(cache.get_meta(_META_KEY, "") or "{}")
    except Exception:
        return {}


def get_token(nome: str, default: str = "") -> str:
    """
    Busca um token por nome. Ordem: variável de ambiente > planilha de config (cache).
    Ex.: get_token('PIPEFY_TOKEN'), get_token('OMIE_APP_KEY').
    """
    env = os.environ.get(nome) or os.environ.get(nome.upper())
    if env:
        return env.strip()
    d = _tokens_cache()
    return str(d.get(nome) or d.get(nome.upper()) or default).strip()


def tem_token(nome: str) -> bool:
    return bool(get_token(nome))