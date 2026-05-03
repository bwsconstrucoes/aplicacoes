# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import traceback
from flask import request, jsonify, current_app
from . import bp
from .core import processar_baixabradesco, normalize_attachments


def _authorized(payload: dict) -> bool:
    expected = os.getenv('BAIXABRADESCO_SECRET', '')
    if not expected:
        return True
    return (payload.get('secret') == expected) or (request.headers.get('X-BaixaBradesco-Secret') == expected)


def _json_error(message: str, status: int = 500, **extra):
    body = {'ok': False, 'app': 'baixabradesco', 'error': message, **extra}
    return jsonify(body), status


@bp.route('/executar', methods=['POST'])
def executar():
    etapa = 'inicio'
    try:
        etapa = 'ler_json'
        payload = request.get_json(force=True, silent=False) or {}

        etapa = 'autorizacao'
        if not _authorized(payload):
            return _json_error('Não autorizado.', 401, etapa=etapa)

        etapa = 'processar_baixabradesco'
        result = processar_baixabradesco(payload)
        return jsonify(result)

    except ValueError as e:
        current_app.logger.warning('baixabradesco ValueError etapa=%s erro=%s', etapa, str(e))
        return _json_error(str(e), 400, etapa=etapa)
    except Exception as e:
        tb = traceback.format_exc()
        current_app.logger.error('baixabradesco ERRO etapa=%s erro=%s\n%s', etapa, str(e), tb)
        body = {
            'ok': False,
            'app': 'baixabradesco',
            'etapa': etapa,
            'error': str(e),
            'tipo_erro': e.__class__.__name__,
        }
        # Enquanto estamos implantando, é melhor sempre devolver traceback. Depois desligamos.
        if os.getenv('BAIXABRADESCO_DEBUG', 'true').lower() in {'1', 'true', 'sim', 'yes'}:
            body['traceback'] = tb
        return jsonify(body), 500


@bp.route('/validar-payload', methods=['POST'])
def validar_payload():
    """Endpoint leve: valida só JSON/autorização/anexos. Não abre Google, Omie, Pipefy nem PDF."""
    try:
        payload = request.get_json(force=True, silent=False) or {}
        if not _authorized(payload):
            return _json_error('Não autorizado.', 401, etapa='autorizacao')
        attachments = normalize_attachments(payload)
        return jsonify({
            'ok': True,
            'app': 'baixabradesco',
            'endpoint': 'validar-payload',
            'quantidade_anexos': len(attachments),
            'anexos': [
                {
                    'filename': a.filename,
                    'tem_base64': bool(a.base64),
                    'base64_len': len(a.base64 or ''),
                    'tem_url': bool(a.url),
                } for a in attachments
            ],
        })
    except Exception as e:
        current_app.logger.error('baixabradesco validar-payload erro=%s\n%s', str(e), traceback.format_exc())
        return _json_error(str(e), 500, etapa='validar_payload', tipo_erro=e.__class__.__name__)


@bp.route('/health', methods=['GET'])
def health():
    deps = {}
    for mod in ['pypdf', 'gspread', 'google.oauth2.service_account', 'requests']:
        try:
            __import__(mod)
            deps[mod] = 'ok'
        except Exception as e:
            deps[mod] = f'erro: {e.__class__.__name__}: {e}'
    return jsonify({'ok': True, 'app': 'baixabradesco', 'deps': deps})
