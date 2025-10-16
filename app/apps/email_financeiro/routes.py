# -*- coding: utf-8 -*-
"""
Rotas principais do módulo Email Financeiro
Executa coleta, parsing e retorna status.
"""

from flask import jsonify, request
from datetime import datetime
import threading

from .collector import process_all_mailboxes
from .sheets_utils import get_status_summary

# Endpoint: /api/email_financeiro/run
from . import bp


@bp.route("/run", methods=["GET", "POST"])
def run_collector():
    """
    Executa a coleta de e-mails e anexos financeiros.
    Pode ser acionado manualmente ou por cron no Render.
    """
    def background_job():
        try:
            process_all_mailboxes()
        except Exception as e:
            print(f"[ERRO] Execução do coletor: {e}")

    threading.Thread(target=background_job).start()
    return jsonify({
        "status": "running",
        "message": "Coletor financeiro iniciado em background.",
        "timestamp": datetime.now().isoformat()
    })


@bp.route("/status", methods=["GET"])
def status():
    """
    Retorna resumo da última execução, contagens e valor total encontrado.
    """
    try:
        summary = get_status_summary()
        return jsonify({
            "status": "ok",
            "updated": datetime.now().isoformat(),
            **summary
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
