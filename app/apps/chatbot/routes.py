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
_mensagens_recentes: dict = {}
_DEDUP_TTL = 30  # aumentado para 30s


def _get_lock(telefone: str) -> threading.Lock:
    with _locks_meta:
        if telefone not in _locks_por_telefone:
            _locks_por_telefone[telefone] = threading.Lock()
        return _locks_por_telefone[telefone]


def _ja_processado(chave: str) -> bool:
    agora = time.time()
    with _locks_meta:
        expiradas = [k for k, t in _mensagens_recentes.items() if agora - t > _DEDUP_TTL]
        for k in expiradas:
            del _mensagens_recentes[k]
        if chave in _mensagens_recentes:
            return True
        _mensagens_recentes[chave] = agora
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

    # LOG COMPLETO para diagnóstico
    import json
    ts = time.strftime('%H:%M:%S')
    logger.info(f"[webhook] {ts} payload={json.dumps(payload, ensure_ascii=False)[:800]}")

    telefone, texto = _extrair_mensagem(payload)

    if not telefone or not texto:
        logger.info(f"[webhook] {ts} IGNORADO telefone={telefone!r} texto={texto!r}")
        return jsonify({'ok': True, 'ignorado': True}), 200

    # Chave de deduplicação baseada no conteúdo exato
    chave_msg = f"{telefone}:{texto}"
    if _ja_processado(chave_msg):
        logger.warning(f"[webhook] {ts} DUPLICADA de {telefone[:6]}*** texto={texto!r}")
        return jsonify({'ok': True, 'ignorado': True, 'motivo': 'duplicada'}), 200

    logger.info(f"[webhook] {ts} PROCESSANDO {telefone[:6]}*** texto={texto!r}")

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