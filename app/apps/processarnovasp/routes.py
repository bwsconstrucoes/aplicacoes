# -*- coding: utf-8 -*-
"""
processarnovasp/routes.py
POST /api/processarnovasp/executar

Body JSON: payload do webhook Pipefy + parâmetros Omie/Pipefy
Header ou body: campo "secret" para autenticação.

ESTA ROTA É CHAMADA POR SUBCENÁRIO DO MAKE (não diretamente pelo webhook do Pipefy).
Portanto retorna apenas JSON (sem HTML formatado para Webhook Respond).
"""

import logging
from flask import Blueprint, request, jsonify
from .core import validar_payload, executar
from .payload_adapter import adaptar

logger = logging.getLogger(__name__)
bp = Blueprint('processarnovasp', __name__)


@bp.route('/executar', methods=['POST'])
def rota_executar():
    raw_payload = request.get_json(silent=True) or {}
    # Adapta payload nested do Pipefy/Make → estrutura plana interna.
    # (Se já vier plano, o adapter detecta e devolve como está.)
    payload = adaptar(raw_payload)

    try:
        validar_payload(payload)
    except ValueError as e:
        return jsonify({
            'ok':   False,
            'erro': str(e),
        }), 400

    try:
        resultado = executar(payload)
        resultado['ok'] = True
        return jsonify(resultado), 200

    except Exception as e:
        logger.exception('Erro ao executar processarnovasp')
        return jsonify({
            'ok':   False,
            'erro': str(e),
        }), 500