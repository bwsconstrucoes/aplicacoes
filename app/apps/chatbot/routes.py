# -*- coding: utf-8 -*-
"""
chatbot/routes.py
Webhook Z-API com lock por telefone para evitar race conditions.
"""

import os
import time
import logging
import threading
from flask import Blueprint, request, jsonify
from .core import processar_mensagem
from . import sheets_cache, session

logger = logging.getLogger(__name__)
bp = Blueprint('chatbot', __name__)

WEBHOOK_SECRET = os.getenv('CHATBOT_WEBHOOK_SECRET', '')

# Lock por telefone — evita processamento paralelo do mesmo número
_locks_por_telefone: dict = {}
_locks_meta = threading.Lock()

# Deduplicação — evita processar a mesma mensagem duas vezes (Z-API reenvio)
_mensagens_recentes: dict = {}  # {chave: timestamp}
_DEDUP_TTL = 10  # segundos


def _get_lock(telefone: str) -> threading.Lock:
    """Retorna (ou cria) o lock dedicado ao telefone."""
    with _locks_meta:
        if telefone not in _locks_por_telefone:
            _locks_por_telefone[telefone] = threading.Lock()
        return _locks_por_telefone[telefone]


def _ja_processado(chave: str) -> bool:
    """Verifica se essa mensagem já foi processada recentemente (deduplicação)."""
    agora = time.time()
    with _locks_meta:
        # Limpa entradas antigas
        expiradas = [k for k, t in _mensagens_recentes.items() if agora - t > _DEDUP_TTL]
        for k in expiradas:
            del _mensagens_recentes[k]
        # Verifica e registra
        if chave in _mensagens_recentes:
            return True
        _mensagens_recentes[chave] = agora
        return False


# Limpeza periódica de sessões expiradas
def _iniciar_limpeza_periodica():
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
    if WEBHOOK_SECRET:
        token = request.headers.get('X-Webhook-Secret') or request.args.get('secret')
        if token != WEBHOOK_SECRET:
            return jsonify({'error': 'Unauthorized'}), 401

    payload = request.get_json(silent=True) or {}

    telefone, texto = _extrair_mensagem(payload)

    if not telefone or not texto:
        return jsonify({'ok': True, 'ignorado': True}), 200

    # Deduplicação — ignora reenvios da Z-API
    chave_msg = f"{telefone}:{texto}:{int(time.time() // _DEDUP_TTL)}"
    if _ja_processado(chave_msg):
        logger.info(f"[chatbot] Mensagem duplicada ignorada de {telefone[:6]}***")
        return jsonify({'ok': True, 'ignorado': True, 'motivo': 'duplicada'}), 200

    # Processa em thread com lock por telefone
    threading.Thread(
        target=_processar_com_lock,
        args=(telefone, texto),
        daemon=True,
    ).start()

    return jsonify({'ok': True}), 200


def _processar_com_lock(telefone: str, texto: str):
    """Garante que apenas uma thread processa mensagens do mesmo telefone por vez."""
    lock = _get_lock(telefone)
    acquired = lock.acquire(timeout=30)
    if not acquired:
        logger.warning(f"[chatbot] Timeout aguardando lock para {telefone[:6]}***")
        return
    try:
        processar_mensagem(telefone, texto)
    except Exception as e:
        logger.exception(f"[chatbot] Erro ao processar de {telefone[:6]}***: {e}")
    finally:
        lock.release()


def _extrair_mensagem(payload: dict) -> tuple[str, str]:
    if payload.get('fromMe') or payload.get('isGroup'):
        return '', ''

    telefone = (
        payload.get('phone') or
        payload.get('from') or
        (payload.get('senderData') or {}).get('sender') or
        ''
    )
    telefone = telefone.split('@')[0].strip()

    texto = ''
    if isinstance(payload.get('text'), dict):
        texto = payload['text'].get('message', '')
    else:
        texto = (
            payload.get('text') or
            payload.get('body') or
            (payload.get('message') or {}).get('conversation') or
            ''
        )

    return telefone, str(texto).strip()


@bp.route('/status', methods=['GET'])
def status():
    return jsonify({
        'ok':     True,
        'modulo': 'chatbot',
        'cache':  'carregado' if sheets_cache._cache['dados'] is not None else 'vazio',
    }), 200


@bp.route('/cache/invalidar', methods=['POST'])
def invalidar_cache():
    secret = request.headers.get('X-Webhook-Secret') or request.args.get('secret')
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    sheets_cache.invalidar_cache()
    return jsonify({'ok': True, 'mensagem': 'Cache invalidado.'}), 200