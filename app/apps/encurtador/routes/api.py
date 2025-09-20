from flask import Blueprint, request, jsonify, send_file
import json
import os

api_routes = Blueprint('api', __name__)
DATA_FILE = "data/links.json"

def carregar_links():
    try:
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

@api_routes.route("/downloadjson")
def downloadjson():
    return send_file(DATA_FILE, mimetype="application/json", as_attachment=True, download_name="links.json")

@api_routes.route("/admin/download")
def download_admin():
    if request.args.get("token") != "bws123":
        return "Acesso não autorizado", 403
    return send_file(DATA_FILE, mimetype="application/json", as_attachment=True, download_name="links.json")

@api_routes.route("/admin/upload", methods=["GET", "POST"])
def upload_json():
    if request.args.get("token") != "bws123":
        return "Acesso negado", 403
    if request.method == "POST":
        arquivo = request.files.get("arquivo")
        if arquivo and arquivo.filename.endswith(".json"):
            os.makedirs("data", exist_ok=True)
            arquivo.save(DATA_FILE)
            return "Arquivo atualizado com sucesso!"
        return "Envie um arquivo JSON válido.", 400
    return '''
        <h2>Upload de links.json</h2>
        <form method="POST" enctype="multipart/form-data">
            <input type="file" name="arquivo" accept=".json" required><br><br>
            <input type="submit" value="Enviar novo arquivo">
        </form>
    '''
