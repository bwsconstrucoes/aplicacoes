# -*- coding: utf-8 -*-
"""
sheets_sync/routes.py
Endpoint: POST /api/sheets_sync/sincronizar
Body JSON: { "spreadsheet_id": "ID_DA_PLANILHA_DESTINO" }
"""

from flask import Blueprint, request, jsonify
from .sync import sincronizar

bp = Blueprint("sheets_sync", __name__)


@bp.route("/sincronizar", methods=["POST"])
def rota_sincronizar():
    dados = request.get_json(silent=True) or {}
    destino_id = dados.get("spreadsheet_id", "").strip()

    if not destino_id:
        return jsonify({
            "ok": False,
            "erro": "Campo 'spreadsheet_id' obrigatório no corpo da requisição."
        }), 400

    try:
        resultado = sincronizar(destino_id)
        status_http = 200 if resultado["ok"] else 207  # 207 = parcialmente ok
        return jsonify(resultado), status_http

    except RuntimeError as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

    except Exception as e:
        return jsonify({"ok": False, "erro": f"Erro inesperado: {e}"}), 500
