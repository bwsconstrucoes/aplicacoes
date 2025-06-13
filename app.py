from flask import Flask, request, jsonify
from PyPDF2 import PdfMerger
import os
import uuid
import requests
import dropbox

app = Flask(__name__)

# Token fixo do Dropbox
dbx = dropbox.Dropbox(os.getenv("DROPBOX_TOKEN"))

def upload_dropbox(file_path, dropbox_path):
    with open(file_path, "rb") as f:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
    shared_link_metadata = dbx.sharing_create_shared_link_with_settings(dropbox_path)
    return shared_link_metadata.url.replace("?dl=0", "?dl=1")

@app.route("/")
def index():
    return "API de Processamento de PDFs Online!"

@app.route("/compilar", methods=["POST"])
def compilar_pdf():
    try:
        data = request.get_json()

        links = data.get("links")
        pasta = data.get("pasta", "")
        deletar = data.get("deletar", False)
        salvar = data.get("salvar", False)
        nome_arquivo = data.get("nome_arquivo")

        if not links:
            return jsonify({"erro": "Nenhum link fornecido."}), 400

        if nome_arquivo and not nome_arquivo.lower().endswith(".pdf"):
            nome_arquivo += ".pdf"

        nome_arquivo = nome_arquivo or f"{uuid.uuid4().hex}.pdf"
        output_path = f"/tmp/{nome_arquivo}"

        merger = PdfMerger()
        arquivos_baixados = []

        for url in links:
            response = requests.get(url)
            if response.status_code == 200:
                temp_path = f"/tmp/{uuid.uuid4().hex}.pdf"
                with open(temp_path, "wb") as f:
                    f.write(response.content)
                arquivos_baixados.append(temp_path)
                merger.append(temp_path)
            else:
                return jsonify({"erro": f"Erro ao baixar: {url}"}), 400

        merger.write(output_path)
        merger.close()

        link = None
        if salvar:
            dropbox_path = f"{pasta}/{nome_arquivo}"
            link = upload_dropbox(output_path, dropbox_path)

        if deletar:
            for arquivo in arquivos_baixados:
                os.remove(arquivo)
            os.remove(output_path)

        return jsonify({"link": link or f"Arquivo gerado: {nome_arquivo}"})

    except Exception as e:
        return jsonify({"erro": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
