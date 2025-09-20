from flask import Blueprint, request, jsonify
from sheets import buscar_url_por_codigo, adicionar_link
import requests
import csv

encurtador_routes = Blueprint('encurtador', __name__, url_prefix='/encurtador')

@encurtador_routes.route("/<codigo>")
def obter_link(codigo):
    link = buscar_url_por_codigo(codigo)
    if not link:
        return jsonify({"erro": "Link não encontrado"}), 404
    return jsonify(link)

@encurtador_routes.route("/novo", methods=["POST"])
def novo_link():
    codigo = request.form.get("codigo", "").strip()
    url = request.form.get("url", "").strip()
    expira_em = request.form.get("expira_em", "").strip()

    if not codigo or not url or not expira_em:
        return jsonify({"erro": "Campos obrigatórios: codigo, url, expira_em"}), 400

    sucesso = adicionar_link(codigo, url, expira_em)
    if sucesso:
        return jsonify({
            "status": "ok",
            "short_url": f"https://link.bwsconstrucoes.com.br/{codigo}",
            "original_url": url,
            "expira_em": expira_em
        })
    else:
        return jsonify({"erro": "Falha ao gravar na planilha"}), 500

@encurtador_routes.route("/debug")
def debug_linhas():
    SHEET_ID = "1k-ydMq9JEhWGSt7P3D0ucYj2bWNMkhA9uk1kBJiOMb8"
    SHEET_NAME = "Links"
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        content = response.content.decode("utf-8").splitlines()
        reader = csv.DictReader(content)
        linhas = [row for row in reader]
        return jsonify(linhas)
    except Exception as e:
        return jsonify({"erro": f"Falha ao acessar planilha: {str(e)}"}), 500
