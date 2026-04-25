# -*- coding: utf-8 -*-
"""
atualizaspbotao/routes.py
POST /api/atualizaspbotao/executar
Body JSON: payload completo (mesmo formato enviado ao Apps Script atual)
Header ou body: campo "secret" para autenticação
"""

import logging
from flask import Blueprint, request, jsonify
from .core import validar_payload, executar

logger = logging.getLogger(__name__)
bp = Blueprint('atualizaspbotao', __name__)


@bp.route('/executar', methods=['POST'])
def rota_executar():
    payload = request.get_json(silent=True) or {}

    try:
        validar_payload(payload)
    except ValueError as e:
        return jsonify({'ok': False, 'erro': str(e)}), 400

    try:
        resultado = executar(payload)
        return jsonify({'ok': True, 'result': resultado}), 200

    except Exception as e:
        logger.exception('Erro ao executar atualizaspbotao')
        return jsonify({'ok': False, 'erro': str(e)}), 500
