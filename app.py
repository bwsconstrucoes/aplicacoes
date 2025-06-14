from flask import Flask, request, jsonify, send_file
import os, requests, threading, time, tempfile
import dropbox
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from fpdf import FPDF
from PIL import Image

app = Flask(__name__)

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_TOKEN = None
DROPBOX_TOKEN_EXPIRATION = 0

def get_dropbox_client():
    global DROPBOX_TOKEN, DROPBOX_TOKEN_EXPIRATION
    if not DROPBOX_TOKEN or time.time() > DROPBOX_TOKEN_EXPIRATION:
        refresh_dropbox_token()
    return dropbox.Dropbox(DROPBOX_TOKEN)

def refresh_dropbox_token():
    global DROPBOX_TOKEN, DROPBOX_TOKEN_EXPIRATION
    response = requests.post("https://api.dropbox.com/oauth2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_APP_KEY,
        "client_secret": DROPBOX_APP_SECRET,
    })
    if response.status_code == 200:
        data = response.json()
        DROPBOX_TOKEN = data["access_token"]
        DROPBOX_TOKEN_EXPIRATION = time.time() + data.get("expires_in", 14400) - 60
    else:
        raise Exception("Erro ao renovar token do Dropbox")

def upload_dropbox(bio, path):
    dbx = get_dropbox_client()
    base, ext = os.path.splitext(path)
    contador = 1
    while True:
        try:
            dbx.files_get_metadata(path)
            path = f"{base}({contador}){ext}"
            contador += 1
        except dropbox.exceptions.ApiError as e:
            if isinstance(e.error, dropbox.files.GetMetadataError) and e.error.is_path() and e.error.get_path().is_not_found():
                break
            else:
                raise e

    dbx.files_upload(bio.getvalue(), path, mode=dropbox.files.WriteMode.add)
    try:
        url = dbx.sharing_create_shared_link_with_settings(path).url
    except dropbox.exceptions.ApiError as e:
        if (e.error.is_shared_link_already_exists()):
            links = dbx.sharing_list_shared_links(path=path).links
            if links:
                url = links[0].url
            else:
                raise e
        else:
            raise e
    return url.replace("?dl=0", "?dl=1")

def schedule_delete(path, delay):
    def _del():
        time.sleep(delay)
        try:
            dbx = get_dropbox_client()
            dbx.files_delete_v2(path)
        except:
            pass
    threading.Thread(target=_del).start()

@app.route('/compilar', methods=['POST'])
def compilar_pdf():
    data = request.get_json() or {}
    links = data.get("links", [])
    if not links: return jsonify({"erro": "Nenhum link."}), 400
    pasta = data.get("pasta", "/pdf-compilados")
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)
    nome_arquivo = data.get("nome_arquivo", "compilado.pdf")
    if not nome_arquivo.lower().endswith(".pdf"):
        nome_arquivo += ".pdf"

    writer = PdfWriter()
    for url in links:
        r = requests.get(url)
        if r.status_code != 200: continue
        ct = r.headers.get("Content-Type", "")
        bio = BytesIO(r.content)
        if "pdf" in ct:
            reader = PdfReader(bio)
            for p in reader.pages:
                writer.add_page(p)
        elif "image" in ct:
            img = Image.open(bio).convert("RGB")
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
            img.save(tmp.name); tmp.close()
            f = FPDF(); f.add_page(); f.image(tmp.name, x=0, y=0, w=210, h=297)
            os.unlink(tmp.name)
            tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            f.output(tmp_pdf.name)
            tmp_pdf.close()
            with open(tmp_pdf.name, "rb") as pdf_img:
                reader = PdfReader(pdf_img)
                for p in reader.pages:
                    writer.add_page(p)
            os.unlink(tmp_pdf.name)

    out = BytesIO(); writer.write(out); out.seek(0)

    if not salvar:
        return send_file(out, mimetype='application/pdf', as_attachment=True, download_name=nome_arquivo)

    path = f"{pasta}/{nome_arquivo}"
    link = upload_dropbox(out, path)
    if deletar: schedule_delete(path, int(data.get("auto_delete", 300)))
    return jsonify({"status": "ok", "link": link})

@app.route('/pdf2texto', methods=['POST'])
def pdf2texto():
    data = request.get_json() or {}
    url = data.get("url")
    if not url: return jsonify({"erro": "Informe a URL do PDF."}), 400

    r = requests.get(url)
    if r.status_code != 200:
        return jsonify({"erro": "Não foi possível baixar o PDF."}), 400

    bio = BytesIO(r.content)
    reader = PdfReader(bio)
    texto = "\n".join([page.extract_text() or "" for page in reader.pages])
    return jsonify({"status": "ok", "texto": texto})

@app.route('/token-status', methods=['GET'])
def token_status():
    try:
        dbx = get_dropbox_client()
        account = dbx.users_get_current_account()
        return jsonify({"status": "ok", "account": account.name.display_name})
    except Exception as e:
        return jsonify({"status": "erro", "detalhes": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
