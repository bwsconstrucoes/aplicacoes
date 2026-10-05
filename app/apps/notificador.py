# -*- coding: utf-8 -*-
"""
notificador.py — Envio unificado de notificações (Telegram + WhatsApp)
======================================================================

Módulo compartilhado do monorepo. Os demais blueprints importam e chamam:

    from app.apps.notificador import notificar

    resultado = notificar(
        telefone="5585999999999",
        mensagem="Seu pagamento foi agendado para 20/07.",
    )
    # resultado = {"telegram": {"ok": True, ...}, "whatsapp": {"ok": True, ...}}

Com arquivo:

    notificar(
        telefone="5585999999999",
        mensagem="Segue seu comprovante.",
        arquivo_url="https://.../comprovante.pdf",   # OU arquivo_base64=...
        nome_arquivo="comprovante.pdf",
    )

Escolhendo canais/política:

    notificar(..., canais=("telegram",))                     # só Telegram
    notificar(..., canais=("whatsapp",))                     # só WhatsApp
    notificar(..., canais=("telegram", "whatsapp"))          # ambos (padrão)
    notificar(..., politica="fallback")                      # Telegram primeiro;
                                                             # WhatsApp só se falhar

Canal Telegram: chamada interna direta às funções do blueprint telegram
(mesmo processo — sem overhead de HTTP). O destinatário precisa estar na
aba TelegramID; caso contrário retorna {"ok": False, "erro": "nao_cadastrado"}.

Liga/desliga por canal SEM mexer em código (env vars no Render):
  NOTIFICAR_TELEGRAM = "1" (padrão) | "0" desativa o canal Telegram
  NOTIFICAR_WHATSAPP = "1" (padrão) | "0" desativa o canal WhatsApp
Canal desativado retorna {"ok": None, "detalhe": "canal desativado"} —
não conta como falha, apenas não envia.

Liga/desliga por FINALIDADE (desde 05/10/2026): o chamador pode dizer para
que serve o aviso — notificar(..., finalidade="ponto") — e aí a variável
NOTIFICAR_<CANAL>_<FINALIDADE> (ex.: NOTIFICAR_WHATSAPP_PONTO) decide
sozinha para aquela finalidade. Se ela não existir, vale a geral. É o que
permite deixar o WhatsApp desligado para tudo (o Z-API bloqueia quando o
volume sobe) e ligado só para o ponto, sem publicar código.

Canal WhatsApp: HTTP direto para a API do Z-API (api.z-api.io).
Variáveis de ambiente:
  ZAPI_INSTANCE_ID   -> id da instância (o mesmo dos teus cenários)
  ZAPI_API_TOKEN     -> token da instância. ZAPI_INSTANCE_TOKEN é aceito
                        como apelido (foi o nome usado aqui até 05/10/2026;
                        o Render só tem ZAPI_API_TOKEN)
  ZAPI_CLIENT_TOKEN  -> Account Security Token do painel Z-API
                        (header Client-Token; deixe vazio se a conta não exigir)
"""

import os
import re
import time

import requests

# Funções internas do blueprint do Telegram (mesmo processo)
from app.apps.telegram.telegram_bot import (
    _inferir_tipo,
    _lookup_chat_id,
    _tg_enviar,
    _tg_enviar_arquivo,
)

# ---------------------------------------------------------------------------
# Configuração WhatsApp (Z-API direto)
# ---------------------------------------------------------------------------

WA_BASE = "https://api.z-api.io"


def _wa_credenciais():
    """Lê as credenciais do Z-API a cada chamada (trocar variável no Render
    passa a valer sem reiniciar). Aceita os dois nomes do token."""
    return {
        "instancia": os.environ.get("ZAPI_INSTANCE_ID", "").strip(),
        "token": (os.environ.get("ZAPI_API_TOKEN", "").strip()
                  or os.environ.get("ZAPI_INSTANCE_TOKEN", "").strip()),
        "client_token": os.environ.get("ZAPI_CLIENT_TOKEN", "").strip(),
    }


def _wa_url(sufixo):
    c = _wa_credenciais()
    return f"{WA_BASE}/instances/{c['instancia']}/token/{c['token']}/{sufixo}"


def _wa_configurado():
    c = _wa_credenciais()
    return bool(c["instancia"] and c["token"])


def whatsapp_configurado():
    """Há credenciais do Z-API no ambiente? (não olha o liga/desliga)"""
    return _wa_configurado()


def _wa_post(url, payload, timeout=60):
    """POST ao Z-API com retry e checagem de status."""
    client_token = _wa_credenciais()["client_token"]
    headers = {"Client-Token": client_token} if client_token else {}
    for tentativa in range(3):
        try:
            r = requests.post(url, json=payload, headers=headers,
                              timeout=timeout)
            if r.status_code == 200:
                return True, "ok"
            print(f"[notificador] WhatsApp HTTP {r.status_code}: "
                  f"{r.text[:300]}")
            if 400 <= r.status_code < 500 and r.status_code != 429:
                return False, f"HTTP {r.status_code}: {r.text[:200]}"
            time.sleep(2 * (tentativa + 1))
        except requests.RequestException as e:
            print(f"[notificador] Erro de rede WhatsApp: {e}")
            time.sleep(1 + tentativa)
    return False, "falha após 3 tentativas"


def _wa_enviar_texto(telefone, mensagem):
    if not _wa_configurado():
        return {"ok": False, "erro": "whatsapp_nao_configurado",
                "detalhe": "defina ZAPI_INSTANCE_ID/ZAPI_API_TOKEN"}
    ok, detalhe = _wa_post(_wa_url("send-text"),
                           {"phone": telefone, "message": mensagem})
    return {"ok": ok, "detalhe": detalhe}


def _wa_enviar_arquivo(telefone, tipo, arquivo_url=None, arquivo_base64=None,
                       nome_arquivo=None, legenda=""):
    if not _wa_configurado():
        return {"ok": False, "erro": "whatsapp_nao_configurado",
                "detalhe": "defina ZAPI_INSTANCE_ID/ZAPI_API_TOKEN"}

    if tipo == "imagem":
        payload = {"phone": telefone,
                   "image": arquivo_url or f"data:image/jpeg;base64,{arquivo_base64}"}
        if legenda:
            payload["caption"] = legenda
        ok, detalhe = _wa_post(_wa_url("send-image"), payload)
    else:
        ext = "pdf"
        if nome_arquivo and "." in nome_arquivo:
            ext = nome_arquivo.rsplit(".", 1)[-1].lower()
        payload = {"phone": telefone,
                   "document": (arquivo_url or
                                f"data:application/{ext};base64,{arquivo_base64}"),
                   "fileName": nome_arquivo or f"arquivo.{ext}"}
        if legenda:
            payload["caption"] = legenda
        ok, detalhe = _wa_post(_wa_url(f"send-document/{ext}"), payload)
    return {"ok": ok, "detalhe": detalhe}


# ---------------------------------------------------------------------------
# Canal Telegram (chamadas internas, sem HTTP)
# ---------------------------------------------------------------------------

def _tg_notificar(telefone=None, cpf=None, chat_id=None, mensagem="",
                  arquivo_url=None, arquivo_base64=None, nome_arquivo=None,
                  tipo=None):
    if not chat_id:
        try:
            chat_id = _lookup_chat_id(telefone=telefone or None,
                                      cpf=cpf or None)
        except Exception as e:
            return {"ok": False, "erro": "base_indisponivel",
                    "detalhe": str(e)[:200]}
        if not chat_id:
            return {"ok": False, "erro": "nao_cadastrado",
                    "detalhe": "destinatário sem ID Telegram na aba TelegramID"}

    if arquivo_url or arquivo_base64:
        if not tipo:
            tipo = _inferir_tipo(nome_arquivo or arquivo_url or "")
        ok, detalhe = _tg_enviar_arquivo(
            chat_id, tipo, url=arquivo_url or None,
            conteudo_b64=arquivo_base64 or None,
            nome_arquivo=nome_arquivo or None, legenda=mensagem,
        )
        return {"ok": ok, "chat_id": chat_id, "detalhe": detalhe}

    ok = _tg_enviar(chat_id, mensagem)
    return {"ok": ok, "chat_id": chat_id,
            "detalhe": "ok" if ok else "falha no envio"}


# ---------------------------------------------------------------------------
# Liga/desliga por canal (env vars; lidas a cada chamada)
# ---------------------------------------------------------------------------

_DESLIGADO = ("0", "false", "nao", "não", "off")


def _nome_finalidade(finalidade):
    """'ponto' -> 'PONTO'; 'aviso-cadastro' -> 'AVISO_CADASTRO'."""
    return re.sub(r"[^A-Z0-9]+", "_", str(finalidade or "").strip().upper()).strip("_")


def _canal_ativo(canal, finalidade=None):
    if canal not in ("telegram", "whatsapp"):
        return True
    geral = f"NOTIFICAR_{canal.upper()}"
    nome = _nome_finalidade(finalidade)
    if nome:
        especifico = os.environ.get(f"{geral}_{nome}")
        if especifico is not None and especifico.strip() != "":
            return especifico.strip().lower() not in _DESLIGADO
    return os.environ.get(geral, "1").strip().lower() not in _DESLIGADO


# ---------------------------------------------------------------------------
# Funções públicas auxiliares (usadas pelos zapi.py dos módulos)
# ---------------------------------------------------------------------------

def canal_ativo(canal, finalidade=None):
    """Exposição pública do toggle por canal (env NOTIFICAR_*), com a
    variável por finalidade (NOTIFICAR_<CANAL>_<FINALIDADE>) mandando
    quando existir."""
    return _canal_ativo(canal, finalidade)


def enviar_whatsapp(telefone, mensagem, finalidade=None):
    """Envio SÓ de texto pelo WhatsApp, respeitando o toggle (geral ou da
    finalidade). Para os módulos que não precisam do Telegram junto."""
    if not _canal_ativo("whatsapp", finalidade):
        return {"ok": None, "detalhe": "canal desativado (env NOTIFICAR_*)"}
    tel = re.sub(r"\D", "", telefone or "")
    if not tel:
        return {"ok": False, "erro": "sem_telefone"}
    return _wa_enviar_texto(tel, mensagem)


def enviar_telegram(telefone=None, cpf=None, chat_id=None, mensagem="",
                    arquivo_url=None, arquivo_base64=None, nome_arquivo=None,
                    tipo=None, finalidade=None):
    """Envio SÓ pelo canal Telegram, respeitando o toggle NOTIFICAR_TELEGRAM.
    Usado como espelho pelos módulos que mantêm o envio Z-API próprio."""
    if not _canal_ativo("telegram", finalidade):
        return {"ok": None, "detalhe": "canal desativado (env NOTIFICAR_*)"}
    return _tg_notificar(telefone=telefone, cpf=cpf, chat_id=chat_id,
                         mensagem=mensagem, arquivo_url=arquivo_url,
                         arquivo_base64=arquivo_base64,
                         nome_arquivo=nome_arquivo, tipo=tipo)


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def notificar(telefone=None, cpf=None, chat_id=None, mensagem="",
              arquivo_url=None, arquivo_base64=None, nome_arquivo=None,
              tipo=None, canais=("telegram", "whatsapp"),
              politica="ambos", finalidade=None):
    """
    Envia a notificação pelos canais indicados.

    finalidade: nome curto do que o aviso é ("ponto", "titulo_pago"...). Com
      ele, NOTIFICAR_<CANAL>_<FINALIDADE> decide o liga/desliga daquele uso;
      sem a variável, vale a geral.

    politica:
      "ambos"    -> envia por todos os canais listados (padrão da transição)
      "fallback" -> tenta na ordem de `canais`; para no primeiro que der certo

    Retorna dict por canal, ex.:
      {"telegram": {"ok": True, "chat_id": "701..."},
       "whatsapp": {"ok": True, "detalhe": "ok"}}
    Canais não executados (política fallback) aparecem como
      {"ok": None, "detalhe": "não executado"}.
    """
    if not mensagem and not arquivo_url and not arquivo_base64:
        raise ValueError("informe mensagem e/ou arquivo")
    if not telefone and not cpf and not chat_id:
        raise ValueError("informe telefone, cpf ou chat_id")

    telefone_norm = re.sub(r"\D", "", telefone or "")

    resultados = {}
    for canal in canais:
        if not _canal_ativo(canal, finalidade):
            resultados[canal] = {"ok": None,
                                 "detalhe": "canal desativado "
                                            "(env NOTIFICAR_*)"}
            continue
        if canal == "telegram":
            resultados["telegram"] = _tg_notificar(
                telefone=telefone_norm or None, cpf=cpf, chat_id=chat_id,
                mensagem=mensagem, arquivo_url=arquivo_url,
                arquivo_base64=arquivo_base64, nome_arquivo=nome_arquivo,
                tipo=tipo,
            )
        elif canal == "whatsapp":
            if not telefone_norm:
                resultados["whatsapp"] = {
                    "ok": False, "erro": "sem_telefone",
                    "detalhe": "WhatsApp exige telefone"}
            elif arquivo_url or arquivo_base64:
                t = tipo or _inferir_tipo(nome_arquivo or arquivo_url or "")
                resultados["whatsapp"] = _wa_enviar_arquivo(
                    telefone_norm, t, arquivo_url=arquivo_url,
                    arquivo_base64=arquivo_base64,
                    nome_arquivo=nome_arquivo, legenda=mensagem,
                )
            else:
                resultados["whatsapp"] = _wa_enviar_texto(
                    telefone_norm, mensagem)
        else:
            resultados[canal] = {"ok": False, "erro": "canal_desconhecido"}

        if politica == "fallback" and resultados[canal].get("ok"):
            for restante in canais:
                if restante not in resultados:
                    resultados[restante] = {"ok": None,
                                            "detalhe": "não executado"}
            break

    return resultados
