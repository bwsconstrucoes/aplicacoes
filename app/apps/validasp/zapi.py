# -*- coding: utf-8 -*-
"""
zapi.py — Módulo genérico de envio Z-API
Suporta: texto e arquivo (link)
Reutilizável por qualquer aplicação no Render.

Variáveis de ambiente esperadas:
  ZAPI_INSTANCE_ID    → ID da instância Z-API
  ZAPI_API_TOKEN      → Token da API Z-API
  ZAPI_CLIENT_TOKEN   → Client-Token do header

Ou passados diretamente no payload (override por chamada).
"""

import os
import logging
import requests

logger = logging.getLogger(__name__)

ZAPI_BASE_URL = "https://api.z-api.io/instances/{instance_id}/token/{api_token}"


def _get_config(override: dict = None) -> dict:
    """
    Configuração Z-API vinda exclusivamente do payload.
    Campos esperados: zapi_instance_id, zapi_api_token, zapi_client_token
    """
    cfg = {
        'instance_id':  '',
        'api_token':    '',
        'client_token': '',
    }
    if override:
        cfg.update({k: v for k, v in override.items() if v})
    return cfg


def _base_url(cfg: dict) -> str:
    return ZAPI_BASE_URL.format(
        instance_id=cfg['instance_id'],
        api_token=cfg['api_token'],
    )


def _headers(cfg: dict) -> dict:
    return {
        'Client-Token': cfg['client_token'],
        'Content-Type': 'application/json',
    }


def _normalizar_telefone(telefone: str) -> str:
    """Remove não-dígitos e garante prefixo 55."""
    import re
    digits = re.sub(r'\D', '', str(telefone or ''))
    if not digits.startswith('55'):
        digits = '55' + digits
    return digits


def enviar_texto(telefone: str, mensagem: str, zapi_config: dict = None) -> dict:
    """
    Envia mensagem de texto via Z-API.
    zapi_config: dict opcional com instance_id, api_token, client_token (override do env)
    """
    if not telefone or not mensagem:
        return {'ok': False, 'erro': 'telefone e mensagem são obrigatórios'}

    cfg  = _get_config(zapi_config)
    url  = _base_url(cfg) + '/send-text'
    body = {
        'phone':   _normalizar_telefone(telefone),
        'message': mensagem,
    }

    try:
        resp = requests.post(url, json=body, headers=_headers(cfg), timeout=20)
        ok   = 200 <= resp.status_code < 300
        try:
            data = resp.json()
        except Exception:
            data = {'raw': resp.text}
        return {'ok': ok, 'status': resp.status_code, 'data': data}
    except requests.RequestException as e:
        return {'ok': False, 'erro': str(e)}


def enviar_arquivo(telefone: str, link_arquivo: str, caption: str = '',
                   zapi_config: dict = None) -> dict:
    """
    Envia arquivo (imagem/PDF/doc) via link público Z-API.
    """
    if not telefone or not link_arquivo:
        return {'ok': False, 'erro': 'telefone e link_arquivo são obrigatórios'}

    cfg  = _get_config(zapi_config)
    url  = _base_url(cfg) + '/send-document/link'
    body = {
        'phone':    _normalizar_telefone(telefone),
        'document': link_arquivo,
        'fileName': link_arquivo.split('/')[-1][:50],
        'caption':  caption or '',
    }

    try:
        resp = requests.post(url, json=body, headers=_headers(cfg), timeout=30)
        ok   = 200 <= resp.status_code < 300
        try:
            data = resp.json()
        except Exception:
            data = {'raw': resp.text}
        return {'ok': ok, 'status': resp.status_code, 'data': data}
    except requests.RequestException as e:
        return {'ok': False, 'erro': str(e)}


def enviar_multiplos(mensagens: list, zapi_config: dict = None) -> list:
    """
    Envia múltiplas mensagens em sequência.
    mensagens: lista de dicts com campos:
      - tipo: 'texto' | 'arquivo'
      - telefone: str
      - mensagem: str (para texto)
      - link_arquivo: str (para arquivo)
      - caption: str (opcional, para arquivo)
      - enabled: bool (opcional, padrão True)
    """
    resultados = []
    for msg in mensagens:
        if not msg.get('enabled', True):
            resultados.append({'ok': False, 'ignorado': True, 'motivo': 'enabled=false'})
            continue

        tipo     = msg.get('tipo', 'texto')
        telefone = msg.get('telefone', '')

        if not telefone:
            resultados.append({'ok': False, 'ignorado': True, 'motivo': 'telefone vazio'})
            continue

        if tipo == 'arquivo':
            r = enviar_arquivo(
                telefone      = telefone,
                link_arquivo  = msg.get('link_arquivo', ''),
                caption       = msg.get('caption', ''),
                zapi_config   = zapi_config,
            )
        else:
            r = enviar_texto(
                telefone    = telefone,
                mensagem    = msg.get('mensagem', ''),
                zapi_config = zapi_config,
            )
        resultados.append(r)

    return resultados
