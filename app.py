from flask import Flask, request, jsonify, send_file
import os
import dropbox
import requests
from io import BytesIO
from PyPDF2 import PdfReader
from fpdf import FPDF
from PIL import Image
import tempfile
import threading
import time

app = Flask(__name__)

DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

def upload_dropbox(file_bytes, dropbox_path):
    dbx.files_upload(file_bytes.getvalue(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
    link = dbx.sharing_create_shared_link_with_settings(dropbox_path).url
    return link.replace("?dl=0", "?dl=1")

def agendar_exclusao(path, delay):
    def apagar():
        time.sleep(delay)
        try:
            dbx.files_delete_v2(path)
        except Exception as e:
            print(f"Erro ao excluir {path}: {e}")
    threading.Thread(target=apagar).start()

@app.route('/compilar', methods=['POST'])
def compilar_pdf():
    data = request.get_json()
    links = data.get("links")
    pasta = data.get("pasta", "/pdf-compilados")
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)

    if not links:
        return jsonify({"erro": "Nenhum link fornecido."}), 400

    pdf = FPDF()
    pdf.set_auto_page_break(0)

    for url in links:
        r = requests.get(url)
        if r.status_code != 200:
            continue
        content_type = r.headers.get("Content-Type", "")
        if "image" in content_type:
            img = Image.open(BytesIO(r.content)).convert('RGB')
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
            img.save(tmp.name)
            pdf.add_page()
            pdf.image(tmp.name, x=0, y=0, w=210, h=297)
            tmp.close()
            os.unlink(tmp.name)
        elif "pdf" in content_type:
            pdf_reader = PdfReader(BytesIO(r.content))
            for page in pdf_reader.pages:
                txt = page.extract_text() or "(Página em branco ou imagem)"
                pdf.add_page()
                pdf.set_font("Arial", size=12)
                pdf.multi_cell(190, 10, txt)

    result = BytesIO()
    pdf.output(result)
    result.seek(0)

    if salvar:
        path = f"{pasta}/compilado.pdf"
        link = upload_dropbox(result, path)
        if deletar:
            agendar_exclusao(path, 300)
        return jsonify({"status": "ok", "link": link})
    else:
        return send_file(result, mimetype='application/pdf', as_attachment=True, download_name="compilado.pdf")

@app.route('/pdf2texto', methods=['POST'])
def pdf_para_texto():
    if 'file' not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado."}), 400

    file = request.files['file']
    pdf_reader = PdfReader(file)
    textos = []
    for i, page in enumerate(pdf_reader.pages):
        texto = page.extract_text()
        textos.append({"pagina": i+1, "texto": texto or "(sem texto extraível)"})

    return jsonify({"status": "ok", "paginas": textos})

@app.route('/')
def home():
    return 'API de Processamento de PDFs Online!'

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

