# -*- coding: utf-8 -*-
"""
sheets_sync/routes.py
Endpoint: POST /api/sheets_sync/sincronizar
Body JSON:
  {
    "spreadsheet_id":   "ID_DA_PLANILHA_DESTINO",
    "spreadsheet_name": "Nome da Planilha"
  }
"""

from flask import Blueprint, request, jsonify
from .sync import sincronizar

bp = Blueprint("sheets_sync", __name__)


@bp.route("/sincronizar", methods=["POST"])
def rota_sincronizar():
    dados = request.get_json(silent=True) or {}

    destino_id    = dados.get("spreadsheet_id", "").strip()
    nome_planilha = dados.get("spreadsheet_name", "").strip()

    if not destino_id:
        return jsonify({"ok": False, "erro": "Campo 'spreadsheet_id' obrigatório."}), 400

    if not nome_planilha:
        return jsonify({"ok": False, "erro": "Campo 'spreadsheet_name' obrigatório."}), 400

    try:
        resultado = sincronizar(destino_id, nome_planilha)
        status_http = 200 if resultado["ok"] else 207
        return jsonify(resultado), status_http

    except RuntimeError as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

    except Exception as e:
        return jsonify({"ok": False, "erro": f"Erro inesperado: {e}"}), 500
