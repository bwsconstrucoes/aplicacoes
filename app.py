from flask import Flask, request, jsonify, send_file
import os, requests, threading, time, tempfile, base64
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
    resp = requests.post("https://api.dropbox.com/oauth2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id": DROPBOX_APP_KEY,
        "client_secret": DROPBOX_APP_SECRET,
    })
    if resp.status_code == 200:
        d = resp.json()
        DROPBOX_TOKEN = d["access_token"]
        DROPBOX_TOKEN_EXPIRATION = time.time() + d.get("expires_in", 14400) - 60
    else:
        raise Exception("Erro ao renovar token Dropbox: " + resp.text)

def upload_dropbox(bio, pasta, nome_arquivo):
    dbx = get_dropbox_client()
    path = f"{pasta.rstrip('/')}/{nome_arquivo}"
    base, ext = os.path.splitext(path)
    contador = 1
    while True:
        try:
            dbx.files_get_metadata(path)
            path = f"{base}({contador}){ext}"
            contador += 1
        except dropbox.exceptions.ApiError as e:
            if e.error.get_path().is_not_found():
                break
            raise
    dbx.files_upload(bio.getvalue(), path, mode=dropbox.files.WriteMode.add)
    try:
        url = dbx.sharing_create_shared_link_with_settings(path).url
    except dropbox.exceptions.ApiError as e:
        if e.error.is_shared_link_already_exists():
            links = dbx.sharing_list_shared_links(path=path).links
            url = links[0].url if links else None
        else:
            raise
    return url.replace("?dl=0", "?dl=1") if url else None

def extract_text_from_pdf(bio):
    reader = PdfReader(bio)
    return "\n".join([page.extract_text() or "" for page in reader.pages])

def convert_image_to_pdf_bytes(img_bio):
    img = Image.open(img_bio).convert("RGB")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
    img.save(tmp.name); tmp.close()
    f = FPDF(); f.add_page(); f.image(tmp.name, x=0, y=0, w=210, h=297)
    os.unlink(tmp.name)
    tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    f.output(tmp_pdf.name); tmp_pdf.close()
    with open(tmp_pdf.name, "rb") as pdf_img:
        content = BytesIO(pdf_img.read())
    os.unlink(tmp_pdf.name)
    return content

@app.route('/compilar', methods=['POST'])
def compilar_pdf():
    data = request.get_json() or {}
    inputs = []

    for link in data.get("links", []):
        inputs.append({"filename": link.split("/")[-1], "link": link})

    for att in data.get("attachments", []):
        if att.get("base64") and att.get("filename"):
            inputs.append({"filename": att["filename"], "base64": att["base64"]})

    if not inputs:
        return jsonify({"erro": "Informe URL ou attachments em base64."}), 400

    pasta = data.get("pasta", "/pdf-compilados")
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)

    results = []

    for item in inputs:
        bio = BytesIO()
        if "link" in item:
            resp = requests.get(item["link"])
            if resp.status_code != 200:
                continue
            bio = BytesIO(resp.content)
        else:
            bio = BytesIO(base64.b64decode(item["base64"]))

        text = ""
        link_url = None

        bio.seek(0)
        header = bio.read(512)
        bio.seek(0)
        if header[:4] == b"%PDF":
            text = extract_text_from_pdf(bio)
        else:
            pdf_bio = convert_image_to_pdf_bytes(bio)
            text = extract_text_from_pdf(pdf_bio)
            bio = pdf_bio

        if salvar:
            nome = item["filename"] if item["filename"].lower().endswith(".pdf") else item["filename"] + ".pdf"
            link_url = upload_dropbox(bio, pasta, nome)
            if deletar:
                # opcional: agendar exclusão
                ...
        results.append({"filename": item["filename"], "texto": text, "link": link_url})

    return jsonify({"status": "ok", "results": results})

@app.route('/token-status', methods=['GET'])
def token_status():
    try:
        acc = get_dropbox_client().users_get_current_account()
        return jsonify({"status": "ok", "account": acc.name.display_name})
    except Exception as e:
        return jsonify({"status": "erro", "detalhes": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
