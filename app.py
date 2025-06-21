
from flask import Flask, request, jsonify
import os, requests, threading, time, base64
import dropbox
import fitz  # PyMuPDF
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import json

app = Flask(__name__)

# === Configurações Dropbox ===
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_TOKEN = None
DROPBOX_TOKEN_EXPIRATION = 0

# === Autenticação Google Drive ===
drive = None
def autenticar_google_drive():
    global drive
    if drive: return drive
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile("token.json")
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
    gauth.SaveCredentialsFile("token.json")
    drive = GoogleDrive(gauth)
    return drive

def upload_google_drive(bio, filename, folder_id):
    drive = autenticar_google_drive()
    file_drive = drive.CreateFile({'title': filename, 'parents': [{'id': folder_id}]})
    bio.seek(0)
    file_drive.SetContentString(bio.read().decode('latin1'))
    file_drive.Upload()
    file_drive.InsertPermission({
        'type': 'anyone',
        'value': 'anyone',
        'role': 'reader'
    })
    return file_drive['alternateLink']

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
    resp.raise_for_status()
    data = resp.json()
    DROPBOX_TOKEN = data["access_token"]
    DROPBOX_TOKEN_EXPIRATION = time.time() + int(data.get("expires_in", 14400)) - 60

def upload_dropbox(bio, path):
    dbx = get_dropbox_client()
    base, ext = os.path.splitext(path)
    cnt = 1
    while True:
        try:
            dbx.files_get_metadata(path)
            path = f"{base}({cnt}){ext}"
            cnt += 1
        except dropbox.exceptions.ApiError as e:
            err = getattr(e, "error", None)
            if hasattr(err, "get_path") and err.get_path().is_not_found():
                break
            raise
    dbx.files_upload(bio.getvalue(), path, mode=dropbox.files.WriteMode.add)
    try:
        url = dbx.sharing_create_shared_link_with_settings(path).url
    except dropbox.exceptions.ApiError as e:
        if e.error.is_shared_link_already_exists():
            links = dbx.sharing_list_shared_links(path=path).links
            url = links[0].url if links else ""
        else:
            raise
    return url.replace("?dl=0", "?dl=1")

def schedule_delete(path, delay):
    def _del():
        time.sleep(delay)
        try:
            dbx = get_dropbox_client()
            dbx.files_delete_v2(path)
        except:
            pass
    threading.Thread(target=_del, daemon=True).start()

def extrair_com_layout(bio):
    doc = fitz.open(stream=bio.getvalue(), filetype="pdf")
    textos = []
    for p in doc:
        blocks = p.get_text("blocks")
        blocks.sort(key=lambda b: (b[1], b[0]))
        textos.append("\n".join(b[4] for b in blocks))
    return textos

def salvar_arquivo(bio, destino, pasta, nome_arquivo):
    if destino == "googledrive":
        return upload_google_drive(bio, nome_arquivo, pasta)
    return upload_dropbox(bio, f"{pasta}/{nome_arquivo}")

@app.route('/compilar', methods=['POST'])
def compilar():
    data = request.get_json(silent=True) or {}
    attachments = data.get("attachments", [])
    links = data.get("links", [])
    pasta = data.get("pasta", "/pdf-compilados")
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)
    nome_arquivo = data.get("nome_arquivo", "compilado.pdf")
    destino = data.get("destino", "dropbox")

    if not nome_arquivo.lower().endswith(".pdf"):
        nome_arquivo += ".pdf"

    items = []
    for url in links or []:
        items.append({"filename": os.path.basename(url), "url": url})
    for att in attachments or []:
        fn = att.get("filename")
        if "base64" in att:
            items.append({"filename": fn, "base64": att["base64"]})
        elif "data" in att:
            items.append({"filename": fn, "hex": att["data"]})

    results = []
    full_writer = PdfWriter()
    for item in items:
        bio = None
        if item.get("url"):
            r = requests.get(item["url"])
            if r.status_code != 200: continue
            bio = BytesIO(r.content)
        elif item.get("base64"):
            try:
                bio = BytesIO(base64.b64decode(item["base64"]))
            except: continue
        elif item.get("hex"):
            try:
                bio = BytesIO(bytes.fromhex(item["hex"]))
            except: continue
        if not bio: continue

        try:
            reader = PdfReader(bio)
            for p in reader.pages:
                full_writer.add_page(p)
            texto = [pg.extract_text() or "" for pg in reader.pages]
            results.append({"filename": item["filename"], "texto": texto})
        except:
            continue

    out = BytesIO()
    full_writer.write(out)
    out.seek(0)

    link = None
    if salvar:
        link = salvar_arquivo(out, destino, pasta, nome_arquivo)
        if deletar and destino == "dropbox":
            schedule_delete(f"{pasta}/{nome_arquivo}", int(data.get("auto_delete", 300)))

    return jsonify({"status": "ok", "file": nome_arquivo, "link": link, "results": results})

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
