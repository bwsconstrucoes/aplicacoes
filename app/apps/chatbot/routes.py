# -*- coding: utf-8 -*-
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

_locks_por_telefone: dict = {}
_locks_meta = threading.Lock()

# Deduplicação por ID de mensagem (não por conteúdo)
_ids_processados: dict = {}  # {msg_id: timestamp}
_DEDUP_TTL = 60  # segundos


def _get_lock(telefone: str) -> threading.Lock:
    with _locks_meta:
        if telefone not in _locks_por_telefone:
            _locks_por_telefone[telefone] = threading.Lock()
        return _locks_por_telefone[telefone]


def _ja_processado(msg_id: str) -> bool:
    """Deduplicação por ID da mensagem — ignora reenvios do load balancer Z-API."""
    agora = time.time()
    with _locks_meta:
        # Limpa IDs expirados
        expirados = [k for k, t in _ids_processados.items() if agora - t > _DEDUP_TTL]
        for k in expirados:
            del _ids_processados[k]
        if msg_id in _ids_processados:
            return True
        _ids_processados[msg_id] = agora
        return False


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

    telefone, texto, msg_id = _extrair_mensagem(payload)

    # LOG DIAGNÓSTICO — captura campos de ID disponíveis
    import json
    logger.info(
        f"[webhook] RAW id_fields: messageId={payload.get('messageId')!r} "
        f"id={payload.get('id')!r} "
        f"msgId={(payload.get('message') or {}).get('id')!r} "
        f"tel={telefone!r} txt={repr(texto)[:20]}"
    )

    if not telefone or not texto:
        return jsonify({'ok': True, 'ignorado': True}), 200

    # Deduplicação por ID — bloqueia reenvios do load balancer Z-API
    if msg_id and _ja_processado(msg_id):
        logger.info(f"[webhook] Duplicata bloqueada id={msg_id} de {telefone[:6]}***")
        return jsonify({'ok': True, 'ignorado': True, 'motivo': 'duplicada'}), 200

    logger.info(f"[webhook] {telefone[:6]}*** id={msg_id} texto={repr(texto)[:30]}")

    threading.Thread(
        target=_processar_com_lock,
        args=(telefone, texto),
        daemon=True,
    ).start()

    return jsonify({'ok': True}), 200


def _processar_com_lock(telefone: str, texto: str):
    lock = _get_lock(telefone)
    acquired = lock.acquire(timeout=30)
    if not acquired:
        logger.warning(f"[chatbot] Timeout lock {telefone[:6]}***")
        return
    try:
        processar_mensagem(telefone, texto)
    except Exception as e:
        logger.exception(f"[chatbot] Erro {telefone[:6]}***: {e}")
    finally:
        lock.release()


def _extrair_mensagem(payload: dict) -> tuple[str, str, str]:
    """Retorna (telefone, texto, msg_id)."""
    if payload.get('fromMe') or payload.get('isGroup'):
        return '', '', ''

    # ID único da mensagem — usado para deduplicação
    msg_id = (
        payload.get('messageId') or
        payload.get('id') or
        (payload.get('message') or {}).get('id') or
        ''
    )

    telefone = (
        payload.get('phone') or
        payload.get('from') or
        (payload.get('senderData') or {}).get('sender') or
        ''
    )
    telefone = telefone.split('@')[0].strip()

    if isinstance(payload.get('text'), dict):
        texto = payload['text'].get('message', '')
    else:
        texto = (
            payload.get('text') or
            payload.get('body') or
            (payload.get('message') or {}).get('conversation') or
            ''
        )

    return telefone, str(texto).strip(), str(msg_id)


@bp.route('/status', methods=['GET'])
def status():
    return jsonify({
        'ok': True,
        'modulo': 'chatbot',
        'cache': 'carregado' if sheets_cache._cache['dados'] is not None else 'vazio',
        'sessoes_ativas': len(session._sessions),
    }), 200


@bp.route('/cache/invalidar', methods=['POST'])
def invalidar_cache():
    secret = request.headers.get('X-Webhook-Secret') or request.args.get('secret')
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    sheets_cache.invalidar_cache()
    return jsonify({'ok': True}), 200