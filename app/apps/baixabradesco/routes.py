# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import traceback

from flask import request, jsonify

from . import bp
from .core import processar_baixabradesco
from .diagnostico import executar_diagnostico
from .fila import reprocessar_fila
from .fila_tardia import adiar_payload, processar_fila_tardia


def _authorized(payload: dict) -> bool:
    expected = os.getenv('BAIXABRADESCO_SECRET', '')
    if not expected:
        return True
    return (
        payload.get('secret') == expected
        or request.headers.get('X-BaixaBradesco-Secret') == expected
    )


@bp.route('/executar', methods=['POST'])
def executar():
    try:
        payload = request.get_json(force=True, silent=True) or {}
        if not _authorized(payload):
            return jsonify({'ok': False, 'app': 'baixabradesco', 'error': 'Não autorizado.'}), 401

        result = processar_baixabradesco(payload)
        return jsonify(result)

    except ValueError as e:
        return jsonify({'ok': False, 'app': 'baixabradesco', 'error': str(e)}), 400

    except RuntimeError as e:
        # Falha na carga inicial do Sheets por quota: adia em vez de derrubar
        # o cenário do Make — responde 200 e o cron reprocessa depois.
        msg = str(e)
        if 'Falha ao carregar dados Google Sheets' in msg and (
            '429' in msg or 'Quota exceeded' in msg or 'RESOURCE_EXHAUSTED' in msg
        ):
            return jsonify(adiar_payload(payload, msg))
        body = {'ok': False, 'app': 'baixabradesco', 'error': msg}
        debug = os.getenv('BAIXABRADESCO_DEBUG', '').lower() in {'1', 'true', 'sim', 'yes'}
        if debug:
            body['traceback'] = traceback.format_exc()
        return jsonify(body), 500

    except Exception as e:
        body = {'ok': False, 'app': 'baixabradesco', 'error': str(e)}
        debug = os.getenv('BAIXABRADESCO_DEBUG', '').lower() in {'1', 'true', 'sim', 'yes'}
        if debug:
            body['traceback'] = traceback.format_exc()
        return jsonify(body), 500


@bp.route('/health', methods=['GET'])
def health():
    return jsonify({'ok': True, 'app': 'baixabradesco', 'version': '2.0'})


@bp.route('/diagnostico', methods=['POST'])
def diagnostico():
    """Diagnóstico completo — não executa nada, apenas analisa e reporta."""
    try:
        payload = request.get_json(force=True, silent=True) or {}
        if not _authorized(payload):
            return jsonify({'ok': False, 'error': 'Não autorizado.'}), 401
        result = executar_diagnostico(payload)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'ok': False,
            'app': 'baixabradesco',
            'error': str(e),
            'traceback': traceback.format_exc(),
        }), 500


@bp.route('/reprocessar-fila', methods=['POST'])
def reprocessar_fila_route():
    try:
        payload = request.get_json(force=True, silent=True) or {}
        if not _authorized(payload):
            return jsonify({'ok': False, 'app': 'baixabradesco', 'error': 'Não autorizado.'}), 401
        result = reprocessar_fila(payload)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'ok': False,
            'app': 'baixabradesco',
            'error': str(e),
            'traceback': traceback.format_exc(),
        }), 500

@bp.route('/processar-fila-tardia', methods=['POST'])
def processar_fila_tardia_route():
    """Reprocessa payloads adiados por quota. Disparado pelo cron-job.org."""
    try:
        payload = request.get_json(force=True, silent=True) or {}
        if not _authorized(payload):
            return jsonify({'ok': False, 'app': 'baixabradesco', 'error': 'Não autorizado.'}), 401
        return jsonify(processar_fila_tardia())
    except Exception as e:
        return jsonify({
            'ok': False,
            'app': 'baixabradesco',
            'error': str(e),
            'traceback': traceback.format_exc(),
        }), 500