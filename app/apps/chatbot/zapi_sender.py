# -*- coding: utf-8 -*-
"""
chatbot/zapi_sender.py
Envio de mensagens e documentos via Z-API para o chatbot.
Credenciais via variáveis de ambiente.
"""

import os
import logging
import requests

logger = logging.getLogger(__name__)

INSTANCE_ID   = os.getenv('ZAPI_INSTANCE_ID', '')
API_TOKEN     = os.getenv('ZAPI_API_TOKEN', '')
CLIENT_TOKEN  = os.getenv('ZAPI_CLIENT_TOKEN', '')

BASE_URL = "https://api.z-api.io/instances/{instance_id}/token/{api_token}"


def _url(endpoint: str) -> str:
    base = BASE_URL.format(instance_id=INSTANCE_ID, api_token=API_TOKEN)
    return f"{base}/{endpoint}"


def _headers() -> dict:
    return {
        'Client-Token': CLIENT_TOKEN,
        'Content-Type': 'application/json',
    }


def _normalizar_telefone(telefone: str) -> str:
    import re
    digits = re.sub(r'\D', '', str(telefone or ''))
    if not digits.startswith('55'):
        digits = '55' + digits
    return digits


def enviar_texto(telefone: str, mensagem: str) -> dict:
    """Envia mensagem de texto."""
    tel = _normalizar_telefone(telefone)
    try:
        resp = requests.post(
            _url('send-text'),
            json={'phone': tel, 'message': mensagem},
            headers=_headers(),
            timeout=15,
        )
        ok = 200 <= resp.status_code < 300
        if not ok:
            logger.error(f"[zapi_sender] Erro ao enviar texto para {tel}: {resp.text[:200]}")
        return {'ok': ok, 'status': resp.status_code}
    except Exception as e:
        logger.error(f"[zapi_sender] Exceção ao enviar texto: {e}")
        return {'ok': False, 'erro': str(e)}


def enviar_documento_bytes(telefone: str, conteudo_bytes: bytes,
                           nome_arquivo: str, caption: str = '') -> dict:
    """Envia documento como base64."""
    import base64
    tel = _normalizar_telefone(telefone)
    b64 = base64.b64encode(conteudo_bytes).decode('utf-8')
    try:
        resp = requests.post(
            _url('send-document/base64'),
            json={
                'phone':    tel,
                'document': b64,
                'fileName': nome_arquivo,
                'caption':  caption,
            },
            headers=_headers(),
            timeout=60,
        )
        ok = 200 <= resp.status_code < 300
        if not ok:
            logger.error(f"[zapi_sender] Erro ao enviar doc para {tel}: {resp.text[:200]}")
        return {'ok': ok, 'status': resp.status_code}
    except Exception as e:
        logger.error(f"[zapi_sender] Exceção ao enviar doc: {e}")
        return {'ok': False, 'erro': str(e)}
