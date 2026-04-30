# -*- coding: utf-8 -*-
"""
chatbot/zapi_sender.py
Envio de mensagens e documentos via Z-API para o chatbot.
Credenciais via variáveis de ambiente.
"""

import os
import re
import base64
import logging
import requests

logger = logging.getLogger(__name__)

INSTANCE_ID = os.getenv("ZAPI_INSTANCE_ID", "")
API_TOKEN = os.getenv("ZAPI_API_TOKEN", "")
CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN", "")

BASE_URL = "https://api.z-api.io/instances/{instance_id}/token/{api_token}"


def _url(endpoint: str) -> str:
    base = BASE_URL.format(
        instance_id=INSTANCE_ID.strip(),
        api_token=API_TOKEN.strip()
    )
    return f"{base}/{endpoint}"


def _headers() -> dict:
    return {
        "Client-Token": CLIENT_TOKEN.strip(),
        "Content-Type": "application/json",
    }


def _normalizar_telefone(telefone: str) -> str:
    digits = re.sub(r"\D", "", str(telefone or ""))

    if not digits:
        return ""

    if not digits.startswith("55"):
        digits = "55" + digits

    return digits


def _validar_configuracao() -> bool:
    faltando = []

    if not INSTANCE_ID.strip():
        faltando.append("ZAPI_INSTANCE_ID")

    if not API_TOKEN.strip():
        faltando.append("ZAPI_API_TOKEN")

    if not CLIENT_TOKEN.strip():
        faltando.append("ZAPI_CLIENT_TOKEN")

    if faltando:
        logger.error(f"[zapi_sender] Variáveis ausentes no ambiente: {', '.join(faltando)}")
        return False

    return True


def enviar_texto(telefone: str, mensagem: str) -> dict:
    """Envia mensagem de texto pela Z-API."""

    if not _validar_configuracao():
        return {"ok": False, "erro": "Configuração Z-API incompleta"}

    tel = _normalizar_telefone(telefone)

    if not tel:
        return {"ok": False, "erro": "Telefone inválido"}

    try:
        resp = requests.post(
            _url("send-text"),
            json={
                "phone": tel,
                "message": mensagem or "",
            },
            headers=_headers(),
            timeout=30,
        )

        ok = 200 <= resp.status_code < 300

        if not ok:
            logger.error(
                f"[zapi_sender] Erro ao enviar texto para {tel}: "
                f"{resp.status_code} - {resp.text[:500]}"
            )

        return {
            "ok": ok,
            "status": resp.status_code,
            "resposta": resp.text[:500],
        }

    except Exception as e:
        logger.error(f"[zapi_sender] Exceção ao enviar texto: {e}")
        return {"ok": False, "erro": str(e)}


def enviar_documento_bytes(
    telefone: str,
    conteudo_bytes: bytes,
    nome_arquivo: str,
    caption: str = ""
) -> dict:
    """
    Envia documento PDF em Base64 pela Z-API.

    Importante:
    - O endpoint correto é send-document/pdf
    - O conteúdo precisa ir como data:application/pdf;base64,...
    """

    if not _validar_configuracao():
        return {"ok": False, "erro": "Configuração Z-API incompleta"}

    tel = _normalizar_telefone(telefone)

    if not tel:
        return {"ok": False, "erro": "Telefone inválido"}

    if not conteudo_bytes:
        logger.error("[zapi_sender] Conteúdo do documento vazio.")
        return {"ok": False, "erro": "Documento vazio"}

    if not isinstance(conteudo_bytes, bytes):
        logger.error("[zapi_sender] Conteúdo do documento não está em bytes.")
        return {"ok": False, "erro": "Documento não está em bytes"}

    if not conteudo_bytes.startswith(b"%PDF"):
        logger.error("[zapi_sender] Conteúdo não parece ser um PDF válido.")
        return {"ok": False, "erro": "Conteúdo não parece ser PDF válido"}

    try:
        b64 = base64.b64encode(conteudo_bytes).decode("utf-8")
        document_base64 = f"data:application/pdf;base64,{b64}"

        if not nome_arquivo:
            nome_arquivo = "documento.pdf"

        if not nome_arquivo.lower().endswith(".pdf"):
            nome_arquivo += ".pdf"

        payload = {
            "phone": tel,
            "document": document_base64,
            "fileName": nome_arquivo,
            "caption": caption or "",
        }

        logger.info(
            f"[zapi_sender] Enviando PDF para {tel[:6]}*** "
            f"arquivo={nome_arquivo} tamanho={len(conteudo_bytes)} bytes"
        )

        resp = requests.post(
            _url("send-document/pdf"),
            json=payload,
            headers=_headers(),
            timeout=90,
        )

        ok = 200 <= resp.status_code < 300

        if not ok:
            logger.error(
                f"[zapi_sender] Erro ao enviar doc para {tel}: "
                f"{resp.status_code} - {resp.text[:1000]}"
            )

        return {
            "ok": ok,
            "status": resp.status_code,
            "resposta": resp.text[:1000],
        }

    except Exception as e:
        logger.error(f"[zapi_sender] Exceção ao enviar doc: {e}")
        return {"ok": False, "erro": str(e)}