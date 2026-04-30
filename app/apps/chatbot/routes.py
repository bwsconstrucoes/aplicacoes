# -*- coding: utf-8 -*-
"""
chatbot/routes.py
Endpoint do webhook Z-API.
POST /api/chatbot/webhook  ← Z-API envia aqui cada mensagem recebida
GET  /api/chatbot/status   ← verifica saúde do chatbot
POST /api/chatbot/cache/invalidar ← força recarga da planilha
"""

import os
import logging
import threading
from flask import Blueprint, request, jsonify
from .core import processar_mensagem
from . import sheets_cache, session

logger = logging.getLogger(__name__)
bp = Blueprint('chatbot', __name__)

WEBHOOK_SECRET = os.getenv('CHATBOT_WEBHOOK_SECRET', '')

# Limpeza periódica de sessões expiradas a cada 10 minutos
def _iniciar_limpeza_periodica():
    import time
    def loop():
        while True:
            time.sleep(10 * 60)
            try:
                session.limpar_sessoes_expiradas()
            except Exception:
                pass
    threading.Thread(target=loop, daemon=True).start()

_iniciar_limpeza_periodica()


@bp.route('/webhook', methods=['POST'])
def webhook():
    """
    Recebe mensagens da Z-API.
    A Z-API envia um POST com o payload da mensagem recebida.
    Responde 200 imediatamente — processamento em thread separada.
    """
    # Validação do secret (opcional mas recomendado)
    if WEBHOOK_SECRET:
        token = request.headers.get('X-Webhook-Secret') or request.args.get('secret')
        if token != WEBHOOK_SECRET:
            return jsonify({'error': 'Unauthorized'}), 401

    payload = request.get_json(silent=True) or {}

    # Extrai telefone e texto da mensagem Z-API
    telefone, texto = _extrair_mensagem(payload)

    if not telefone or not texto:
        # Ignora eventos sem mensagem (status, acks, etc.)
        return jsonify({'ok': True, 'ignorado': True}), 200

    # Processa em thread para responder 200 imediatamente à Z-API
    threading.Thread(
        target=_processar_seguro,
        args=(telefone, texto),
        daemon=True,
    ).start()

    return jsonify({'ok': True}), 200


def _processar_seguro(telefone: str, texto: str):
    """Processa a mensagem com tratamento de exceção."""
    try:
        processar_mensagem(telefone, texto)
    except Exception as e:
        logger.exception(f"[chatbot] Erro ao processar mensagem de {telefone[:6]}***: {e}")


def _extrair_mensagem(payload: dict) -> tuple[str, str]:
    """
    Extrai telefone e texto do payload da Z-API.
    O formato pode variar — aqui cobre os casos mais comuns.
    """
    # Ignora mensagens enviadas pelo próprio bot (fromMe = True)
    if payload.get('fromMe') or payload.get('isGroup'):
        return '', ''

    # Telefone do remetente
    telefone = (
        payload.get('phone') or
        payload.get('from') or
        (payload.get('senderData') or {}).get('sender') or
        ''
    )
    # Remove sufixo @s.whatsapp.net se presente
    telefone = telefone.split('@')[0].strip()

    # Texto da mensagem
    texto = (
        payload.get('text', {}).get('message') if isinstance(payload.get('text'), dict)
        else payload.get('text') or
        payload.get('body') or
        (payload.get('message') or {}).get('conversation') or
        ''
    )

    return telefone, str(texto).strip()


@bp.route('/status', methods=['GET'])
def status():
    """Health check do chatbot."""
    return jsonify({
        'ok':      True,
        'modulo':  'chatbot',
        'cache':   'carregado' if sheets_cache._cache['dados'] is not None else 'vazio',
    }), 200


@bp.route('/cache/invalidar', methods=['POST'])
def invalidar_cache():
    """Força recarga da planilha de colaboradores."""
    secret = request.headers.get('X-Webhook-Secret') or request.args.get('secret')
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401

    sheets_cache.invalidar_cache()
    return jsonify({'ok': True, 'mensagem': 'Cache invalidado. Será recarregado na próxima consulta.'}), 200