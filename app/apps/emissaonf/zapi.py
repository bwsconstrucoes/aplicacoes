# -*- coding: utf-8 -*-
"""
Notificações WhatsApp via Z-API (texto + arquivo do recibo).

Destinatários e filtros vêm de uma aba dedicada (recomendado: "Destinatarios WhatsApp"
na mesma planilha de credenciais), com colunas:
   Nome | Telefone | Tipo | Regra | Obras | Ativo
 - Tipo : texto (mensagem) ou arquivo (envia o recibo); grupos usam phone "...-group"
 - Regra: TODAS | EXCETO | APENAS
 - Obras: lista separada por vírgula (use sufixo * para "contém", ex.: IFSP*)
 - Ativo: SIM/NÃO

Credenciais (aba Credenciais): ZAPI_INSTANCE_ID, ZAPI_API_TOKEN, ZAPI_CLIENT_TOKEN.
"""
from __future__ import annotations
import requests

ABA_DESTINATARIOS = "Destinatarios WhatsApp"


def carregar_destinatarios(linhas: list[list[str]]) -> list[dict]:
    if not linhas:
        return []
    hdr = [h.strip().lower() for h in linhas[0]]
    def col(row, nome):
        try:
            return row[hdr.index(nome)].strip()
        except (ValueError, IndexError):
            return ""
    dest = []
    for row in linhas[1:]:
        tel = col(row, "telefone")
        if not tel:
            continue
        if col(row, "ativo").upper() in ("NAO", "NÃO", "N", "FALSE", "0"):
            continue
        dest.append({
            "nome": col(row, "nome"), "telefone": tel,
            "tipo": (col(row, "tipo") or "texto").lower(),
            "regra": (col(row, "regra") or "TODAS").upper(),
            "obras": [o.strip().upper() for o in col(row, "obras").split(",") if o.strip()],
        })
    return dest


def _casa(obra: str, tokens: list[str]) -> bool:
    o = (obra or "").upper()
    for t in tokens:
        if t.endswith("*"):
            if t[:-1] in o:
                return True
        elif t == o:
            return True
    return False


def deve_enviar(dest: dict, obra: str) -> bool:
    regra = dest["regra"]
    if regra == "TODAS":
        return True
    if regra == "EXCETO":
        return not _casa(obra, dest["obras"])
    if regra == "APENAS":
        return _casa(obra, dest["obras"])
    return False


def montar_mensagem(obra_cod, num_medicao, valor_brl, periodo_ini, periodo_fim, numero_nota) -> str:
    return (f"💵💵 *EMISSÃO DE NOTA FISCAL*\n\n"
            f"*Obra:* {obra_cod}\n*Medição:* {num_medicao}\n*Valor:* {valor_brl}\n"
            f"*Período:* {periodo_ini} à {periodo_fim}\n*Nº da NF:* {numero_nota}\n\n"
            f"Atenciosamente,\nBWS Bot 🤖")


def _base(creds):
    return f"https://api.z-api.io/instances/{creds['ZAPI_INSTANCE_ID']}/token/{creds['ZAPI_API_TOKEN']}"


def enviar_texto(creds, telefone, mensagem):
    r = requests.post(f"{_base(creds)}/send-text",
                      json={"phone": telefone, "message": mensagem},
                      headers={"Client-Token": creds["ZAPI_CLIENT_TOKEN"]}, timeout=40)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Z-API send-text HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def enviar_documento(creds, telefone, url_documento, nome_arquivo, extensao="pdf"):
    r = requests.post(f"{_base(creds)}/send-document/{extensao}",
                      json={"phone": telefone, "document": url_documento, "fileName": nome_arquivo},
                      headers={"Client-Token": creds["ZAPI_CLIENT_TOKEN"]}, timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Z-API send-document HTTP {r.status_code}: {r.text[:200]}")
    return r.json()
