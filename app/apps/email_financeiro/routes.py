# -*- coding: utf-8 -*-
"""
Rotas principais do módulo Email Financeiro
Executa coleta, parsing e retorna status.
"""

from flask import jsonify, request
from datetime import datetime
import threading

# Blueprint do módulo
from . import bp

# Funções existentes no módulo
from .collector import process_all_mailboxes
from .sheets_utils import get_status_summary

# ===============================
# 🔴 FLAG GLOBAL DE PARADA (NOVA)
# ===============================
# O coletor consulta esta flag periodicamente e encerra de forma limpa
STOP_FLAG = {"active": False}


@bp.route("/run", methods=["GET"])
def run_collector():
    """
    Dispara a execução do coletor em thread separada.
    (Funcionalidade mantida. Nenhuma mudança de comportamento.)
    """
    def _runner():
        try:
            process_all_mailboxes()
        except Exception as e:
            # apenas loga; não altera o fluxo do /run
            print(f"[ERRO] process_all_mailboxes: {e}")

    t = threading.Thread(target=_runner, daemon=True)
    t.start()

    return jsonify({
        "status": "ok",
        "message": "Coletor iniciado",
        "updated": datetime.now().isoformat()
    })


@bp.route("/status", methods=["GET"])
def status():
    """
    Retorna resumo da última execução, contagens e valor total encontrado.
    (Funcionalidade mantida. Nenhuma mudança de comportamento.)
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


# ===========================
# 🔴 ENDPOINT: PARAR EXECUÇÃO
# ===========================
@bp.route("/stop", methods=["GET"])
def stop_execution():
    """
    Marca a execução atual para parar o mais rápido possível.
    O coletor checa STOP_FLAG periodicamente e encerra com status amigável.
    (Nova funcionalidade; não altera as existentes.)
    """
    try:
        STOP_FLAG["active"] = True
        return jsonify({
            "status": "ok",
            "message": "Execução marcada para parar.",
            "updated": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
