# -*- coding: utf-8 -*-
"""
Acesso ao Google (gspread) via service account e leitura de segredos da aba
'Credenciais' (col A = chave, col B = valor).

A service account vem de UMA destas fontes (nesta ordem):
  1) env GOOGLE_CREDENTIALS_BASE64  (JSON da SA em base64) — usado no Render;
  2) arquivo credenciais.json na pasta — usado localmente.
"""
from __future__ import annotations
import os
import json
from base64 import b64decode

import gspread
from google.oauth2.service_account import Credentials

SHEET_CREDENCIAIS = "1D4aVC7wVHL_t-5QpI6v7vtLJMjJpA7DpDnByTFB9i-U"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def cliente_gspread(caminho_json: str = "credenciais.json"):
    b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    if b64:
        info = json.loads(b64decode(b64).decode("utf-8"))
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)
    return gspread.service_account(filename=caminho_json)


def ler_credenciais(gc, sheet_id: str = SHEET_CREDENCIAIS, aba: str = "Credenciais") -> dict:
    """Lê os segredos da aba 'Credenciais' e, por cima, sobrescreve com variáveis
    de ambiente quando existirem (ideal: tudo no Render). Nomes das env vars batem
    com as chaves da planilha; alguns têm apelidos."""
    ws = gc.open_by_key(sheet_id).worksheet(aba)
    valores = ws.get_all_values()
    cred = {r[0].strip(): r[1].strip()
            for r in valores if len(r) >= 2 and r[0].strip()}

    # env var (Render) tem prioridade sobre a planilha
    aliases = {
        "OMIE_KEY": ["OMIE_KEY", "OMIE_BWS_APP_KEY"],
        "OMIE_SECRET": ["OMIE_SECRET", "OMIE_BWS_APP_SECRET"],
        "ZAPI_INSTANCE_ID": ["ZAPI_INSTANCE_ID"],
        "ZAPI_API_TOKEN": ["ZAPI_API_TOKEN"],
        "ZAPI_CLIENT_TOKEN": ["ZAPI_CLIENT_TOKEN"],
        "PIPEFY_TOKEN": ["PIPEFY_TOKEN", "PIPEFY_API_TOKEN"],
        "CERTIFICADO_SENHA": ["EMISSAO_NF_CERTIFICADO_SENHA", "CERTIFICADO_SENHA",
                              "SENHA_CERTIFICADO", "CERT_SENHA"],
        "DROPBOX_APP_KEY": ["DROPBOX_APP_KEY"],
        "DROPBOX_APP_SECRET": ["DROPBOX_APP_SECRET"],
        "DROPBOX_REFRESH_TOKEN": ["DROPBOX_REFRESH_TOKEN"],
    }
    for chave, nomes_env in aliases.items():
        for nome in nomes_env:
            v = os.getenv(nome)
            if v:
                cred[chave] = v
                break
    return cred


def pipefy_token(gc) -> str:
    cred = ler_credenciais(gc)
    tok = cred.get("PIPEFY_TOKEN")
    if not tok:
        raise KeyError("PIPEFY_TOKEN não encontrado na aba 'Credenciais'.")
    return tok
