# -*- coding: utf-8 -*-
"""
sync_logs/routes.py

Endpoints (registrados com url_prefix=/api/sync_logs):
  POST /incremental    — chamado por cron-job.org a cada 30s
  POST /reset          — chamado por cron-job.org às 3h e pelo botão de menu

Auth: ambos exigem campo "secret" no body, comparado com SYNC_LOGS_SECRET.
"""

import logging
import os

from flask import Blueprint, jsonify, request

from .core import reset_completo, sync_incremental

logger = logging.getLogger(__name__)
bp = Blueprint("sync_logs", __name__)


def _validar_secret(payload: dict) -> None:
    """Levanta ValueError se secret inválido. Padrão dos outros módulos."""
    expected = os.getenv("SYNC_LOGS_SECRET", "")
    if not expected:
        raise RuntimeError("SYNC_LOGS_SECRET não configurado.")
    if payload.get("secret") != expected:
        raise ValueError("Secret inválido.")


@bp.route("/incremental", methods=["POST"])
def rota_incremental():
    """Sync incremental: propaga linhas com AG > last_sync."""
    payload = request.get_json(silent=True) or {}

    try:
        _validar_secret(payload)
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

    try:
        resultado = sync_incremental()
        return jsonify(resultado), 200
    except Exception as e:
        logger.exception("Erro no sync incremental")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/reset", methods=["POST"])
def rota_reset():
    """
    Reset completo de uma ou todas as Análises.

    Body:
      {
        "secret": "...",
        "analise_id": "1em1Q..."  (opcional — se ausente, reseta todas)
      }

    O botão de menu envia analise_id pra rodar só na planilha que clicou.
    O cron noturno envia sem analise_id pra resetar todas.
    """
    payload = request.get_json(silent=True) or {}

    try:
        _validar_secret(payload)
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

    analise_id = (payload.get("analise_id") or "").strip() or None

    try:
        resultado = reset_completo(analise_id_solicitada=analise_id)
        return jsonify(resultado), 200
    except Exception as e:
        logger.exception("Erro no reset")
        return jsonify({"ok": False, "erro": str(e)}), 500
