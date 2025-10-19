# -*- coding: utf-8 -*-
from flask import jsonify
from datetime import datetime
import threading

from . import bp

# Rotas originais
from .collector import process_all_mailboxes            # (se você ainda usa o antigo)
from .sheets_utils import get_status_summary

# ---------------- ADIÇÕES ----------------
from .collector_v2 import process_all_mailboxes_v2     # Fase 1
from .collector_ai import process_all_mailboxes_ai     # Fase 2 (OCR/IA)

# Flag /stop (se você já tinha; senão mantém aqui)
from .state import STOP_FLAG


@bp.route("/run", methods=["GET"])
def run_collector():
    def _runner():
        try:
            # mantém seu coletor original por compatibilidade
            process_all_mailboxes()
        except Exception as e:
            print(f"[ERRO] process_all_mailboxes: {e}")
    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return jsonify({"status": "ok", "message": "Coletor iniciado", "updated": datetime.now().isoformat()})


@bp.route("/status", methods=["GET"])
def status():
    try:
        summary = get_status_summary()
        return jsonify({"status": "ok", "updated": datetime.now().isoformat(), **summary})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ---------- NOVOS ENDPOINTS SEM QUEBRAR O QUE EXISTE ----------

@bp.route("/run_v2", methods=["GET"])
def run_collector_v2():
    """Fase 1 – IMAP readonly + parser rule-based melhorado"""
    def _runner():
        try:
            process_all_mailboxes_v2()
        except Exception as e:
            print(f"[ERRO] process_all_mailboxes_v2: {e}")
    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return jsonify({"status": "ok", "message": "Coletor v2 iniciado", "updated": datetime.now().isoformat()})


@bp.route("/run_ai", methods=["GET"])
def run_collector_ai():
    """Fase 2 – OCR + IA + fallback"""
    def _runner():
        try:
            process_all_mailboxes_ai()
        except Exception as e:
            print(f"[ERRO] process_all_mailboxes_ai: {e}")
    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return jsonify({"status": "ok", "message": "Coletor IA iniciado", "updated": datetime.now().isoformat()})


@bp.route("/stop", methods=["GET"])
def stop_execution():
    STOP_FLAG["active"] = True
    return jsonify({"status": "ok", "message": "Execução marcada para parar.", "updated": datetime.now().isoformat()})
