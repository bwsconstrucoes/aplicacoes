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
import os
import requests

ABA_DESTINATARIOS = "Destinatarios WhatsApp"

# --- Integração multi-canal (WhatsApp + Telegram) -------------------------
# Espelho Telegram via HTTP na rota /telegram/enviar do próprio monorepo
# (funciona tanto no Render quanto rodando os scripts localmente).
# API key: chave TELEGRAM_API_KEY na aba Credenciais OU env TELEGRAM_SECRET_TOKEN.
# Toggles: env NOTIFICAR_WHATSAPP / NOTIFICAR_TELEGRAM ("0" desativa; padrão ligado).

TELEGRAM_ENVIAR_URL = os.getenv(
    "TELEGRAM_ENVIAR_URL",
    "https://aplicacoes.bwsconstrucoes.com.br/telegram/enviar")

_DESLIGADO = ("0", "false", "nao", "não", "off")


def _ativo(env_nome: str) -> bool:
    return os.getenv(env_nome, "1").strip().lower() not in _DESLIGADO


def _tg_espelho(creds, telefone, mensagem="", arquivo_url=None,
                nome_arquivo=None) -> dict:
    """Espelho Telegram (lookup na aba TelegramID feito pelo servidor)."""
    if not _ativo("NOTIFICAR_TELEGRAM"):
        return {"ok": None, "detalhe": "canal desativado (NOTIFICAR_TELEGRAM=0)"}
    key = (creds or {}).get("TELEGRAM_API_KEY") or os.getenv("TELEGRAM_SECRET_TOKEN", "")
    if not key:
        return {"ok": False,
                "erro": "TELEGRAM_API_KEY ausente (aba Credenciais) e "
                        "TELEGRAM_SECRET_TOKEN não definido"}
    dados = {"telefone": telefone, "mensagem": mensagem or ""}
    if arquivo_url:
        dados["arquivo_url"] = arquivo_url
        if nome_arquivo:
            dados["nome_arquivo"] = nome_arquivo
    try:
        r = requests.post(TELEGRAM_ENVIAR_URL, data=dados,
                          headers={"X-Api-Key": key}, timeout=120)
        try:
            corpo = r.json()
        except Exception:
            corpo = {"raw": r.text[:200]}
        corpo.setdefault("ok", r.status_code == 200)
        return corpo
    except requests.RequestException as e:
        return {"ok": False, "erro": str(e)}
# ---------------------------------------------------------------------------


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


def montar_mensagem_substituicao(obra_cod, num_medicao, valor_brl, periodo_ini, periodo_fim,
                                 numero_nota, numero_substituida) -> str:
    return (f"🔁 *SUBSTITUIÇÃO DE NOTA FISCAL*\n\n"
            f"*Obra:* {obra_cod}\n*Medição:* {num_medicao}\n*Valor:* {valor_brl}\n"
            f"*Período:* {periodo_ini} à {periodo_fim}\n"
            f"*Nº da NF:* {numero_nota}\n*Substitui a NF:* {numero_substituida}\n\n"
            f"Atenciosamente,\nBWS Bot 🤖")


def _base(creds):
    return f"https://api.z-api.io/instances/{creds['ZAPI_INSTANCE_ID']}/token/{creds['ZAPI_API_TOKEN']}"


def enviar_texto(creds, telefone, mensagem):
    """Envia texto por WhatsApp (Z-API) + Telegram (espelho).
    Lança RuntimeError somente se TODOS os canais ativos falharem —
    preserva a contagem de envios dos chamadores (concluir/completar)."""
    # --- WhatsApp (Z-API) ---
    if _ativo("NOTIFICAR_WHATSAPP"):
        try:
            r = requests.post(f"{_base(creds)}/send-text",
                              json={"phone": telefone, "message": mensagem},
                              headers={"Client-Token": creds["ZAPI_CLIENT_TOKEN"]}, timeout=40)
            if r.status_code in (200, 201):
                wa = {"ok": True, "data": r.json()}
            else:
                wa = {"ok": False, "erro": f"HTTP {r.status_code}: {r.text[:200]}"}
        except requests.RequestException as e:
            wa = {"ok": False, "erro": str(e)}
    else:
        wa = {"ok": None, "detalhe": "canal desativado (NOTIFICAR_WHATSAPP=0)"}

    # --- Telegram (espelho) ---
    tg = _tg_espelho(creds, telefone, mensagem=mensagem)

    if not (wa.get("ok") or tg.get("ok")):
        raise RuntimeError(
            f"envio falhou nos 2 canais — WhatsApp: {wa.get('erro') or wa.get('detalhe')} | "
            f"Telegram: {tg.get('erro') or tg.get('detalhe')}")
    return {"whatsapp": wa, "telegram": tg}


def enviar_documento(creds, telefone, url_documento, nome_arquivo, extensao="pdf"):
    """Envia documento por WhatsApp (Z-API) + Telegram (espelho).
    Lança RuntimeError somente se TODOS os canais ativos falharem."""
    # --- WhatsApp (Z-API) ---
    if _ativo("NOTIFICAR_WHATSAPP"):
        try:
            r = requests.post(f"{_base(creds)}/send-document/{extensao}",
                              json={"phone": telefone, "document": url_documento, "fileName": nome_arquivo},
                              headers={"Client-Token": creds["ZAPI_CLIENT_TOKEN"]}, timeout=60)
            if r.status_code in (200, 201):
                wa = {"ok": True, "data": r.json()}
            else:
                wa = {"ok": False, "erro": f"HTTP {r.status_code}: {r.text[:200]}"}
        except requests.RequestException as e:
            wa = {"ok": False, "erro": str(e)}
    else:
        wa = {"ok": None, "detalhe": "canal desativado (NOTIFICAR_WHATSAPP=0)"}

    # --- Telegram (espelho) ---
    nome_tg = nome_arquivo if "." in (nome_arquivo or "") else f"{nome_arquivo}.{extensao}"
    tg = _tg_espelho(creds, telefone, mensagem="",
                     arquivo_url=url_documento, nome_arquivo=nome_tg)

    if not (wa.get("ok") or tg.get("ok")):
        raise RuntimeError(
            f"envio falhou nos 2 canais — WhatsApp: {wa.get('erro') or wa.get('detalhe')} | "
            f"Telegram: {tg.get('erro') or tg.get('detalhe')}")
    return {"whatsapp": wa, "telegram": tg}
