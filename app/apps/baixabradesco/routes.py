# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import traceback

from flask import request, jsonify

from . import bp
from .core import processar_baixabradesco
from .diagnostico import executar_diagnostico


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
