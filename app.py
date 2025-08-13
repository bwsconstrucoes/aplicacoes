
from flask import Flask, request, jsonify
import os, requests, threading, time, base64, binascii, json
import dropbox
import fitz  # PyMuPDF
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
import tempfile
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from mimetypes import guess_type

app = Flask(__name__)

# Dropbox configs
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_TOKEN = None
DROPBOX_TOKEN_EXPIRATION = 0

# Google Drive configs
GOOGLE_FOLDER_ID = os.getenv("GOOGLE_FOLDER_ID")

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

def upload_gdrive(bio, filename, folder_id):
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    creds_dict = json.loads(base64.b64decode(creds_b64).decode())
    creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/drive"])
    service = build("drive", "v3", credentials=creds)
    media = MediaIoBaseUpload(bio, mimetype="application/pdf", resumable=True)
    file_metadata = {"name": filename, "parents": [folder_id]}
    file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    file_id = file.get("id")
    service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
    return f"https://drive.google.com/uc?id={file_id}&export=download"

def schedule_delete(path, delay):
    def _del():
        time.sleep(delay)
        try:
            dbx = get_dropbox_client()
            dbx.files_delete_v2(path)
        except:
            pass
    threading.Thread(target=_del, daemon=True).start()

@app.route('/compilar', methods=['POST'])
def compilar():
    data = request.get_json(silent=True) or {}
    attachments = data.get("attachments", [])
    links = data.get("links", [])
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)
    nome_arquivo = data.get("nome_arquivo", "compilado.pdf")
    if not nome_arquivo.lower().endswith(".pdf"):
        nome_arquivo += ".pdf"
    destino = data.get("destino", "dropbox").lower()
    pasta = data.get("pasta", "/pdf-compilados")

    items = []
    for url in links or []:
        items.append({"filename": os.path.basename(url), "url": url})
    for att in attachments or []:
        fn = att.get("filename", "anexo.pdf")
        if "base64" in att:
            items.append({"filename": fn, "base64": att["base64"]})
        elif "data" in att:
            items.append({"filename": fn, "hex": att["data"]})

    results = []
    full_writer = PdfWriter()
    for item in items:
        bio = None
        filename = item.get("filename", "arquivo")
        if item.get("url"):
            r = requests.get(item["url"])
            if r.status_code != 200:
                print(f"❌ Ignorado (erro ao baixar): {filename}")
                continue
            content_type = r.headers.get("Content-Type", "")
            if not ("pdf" in content_type or "image" in content_type or "octet-stream" in content_type):
                continue
                print(f"❌ Ignorado (tipo não suportado): {filename}")
                continue
            bio = BytesIO(r.content)
        elif item.get("base64"):
            try:
                bio = BytesIO(base64.b64decode(item["base64"]))
            except:
                print(f"❌ Ignorado (base64 inválido): {filename}")
                continue
        elif item.get("hex"):
            try:
                bio = BytesIO(bytes.fromhex(item["hex"]))
            except:
                print(f"❌ Ignorado (hex inválido): {filename}")
                continue

        if not bio:
            print(f"❌ Ignorado (conteúdo vazio): {filename}")
            continue

        try:
            reader = PdfReader(bio)
            for p in reader.pages:
                full_writer.add_page(p)
            texto = [pg.extract_text() or "" for pg in reader.pages]
        except:
            try:
                bio.seek(0)
                doc = fitz.open(stream=bio.getvalue(), filetype="pdf")
                texto = []
                pdf_temp = PdfWriter()
                for p in doc:
                    img = p.get_pixmap()
                    img_bytes = img.tobytes("png")
                    img_pdf = fitz.open()
                    rect = fitz.Rect(0, 0, img.width, img.height)
                    page = img_pdf.new_page(width=img.width, height=img.height)
                    page.insert_image(rect, stream=img_bytes)
                    temp = BytesIO()
                    img_pdf.save(temp)
                    img_pdf.close()
                    temp.seek(0)
                    sub_reader = PdfReader(temp)
                    for p in sub_reader.pages:
                        full_writer.add_page(p)
                    texto.append("")
            except:
                print(f"❌ Ignorado (não é PDF nem imagem): {filename}")
                continue

        results.append({"filename": filename, "texto": texto})

    out = BytesIO()
    full_writer.write(out)
    out.seek(0)

    link = None
    if salvar:
        if destino == "googledrive":
            link = upload_gdrive(out, nome_arquivo, pasta)
        else:
            path = f"{pasta}/{nome_arquivo}"
            link = upload_dropbox(out, path)
            if deletar:
                schedule_delete(path, int(data.get("auto_delete", 300)))

    return jsonify({"status": "ok", "file": nome_arquivo, "link": link, "results": results})

@app.route('/pdf2texto', methods=['POST'])
def pdf2texto():
    data = request.get_json(silent=True) or {}
    attachments = data.get("attachments", [])
    pasta = data.get("pasta", "/pdf2texto-files")
    salvar = data.get("salvar", False)

    if not attachments:
        return jsonify({"erro": "Informe ao menos um anexo em attachments."}), 400

    results = []
    for att in attachments:
        filename = att.get("filename", "anexo.pdf")
        raw = att.get("base64") or att.get("data") or att.get("hex")
        if not raw:
            continue

        try:
            import re
            if re.fullmatch(r"[0-9A-Fa-f]+", raw.strip()):
                bio = BytesIO(bytes.fromhex(raw.strip()))
            else:
                bio = BytesIO(base64.b64decode(raw))
        except:
            continue

        if bio.getvalue()[:4] != b"%PDF":
            continue

        reader = PdfReader(bio)
        page_texts = []
        page_links = []

        for idx, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            page_texts.append(text)

            if salvar:
                writer = PdfWriter()
                writer.add_page(page)
                out = BytesIO()
                writer.write(out)
                out.seek(0)
                link = upload_dropbox(out, f"{pasta}/{filename[:-4]}_page{idx}.pdf")
                page_links.append(link)
            else:
                page_links.append(None)

        results.append({
            "filename": filename,
            "paginas": [
                {"numero": i+1, "texto": t.strip(), "link": page_links[i]}
                for i, t in enumerate(page_texts)
            ]
        })

    return jsonify({"status": "ok", "results": results})

@app.route('/token-status', methods=['GET'])
def token_status():
    try:
        account = get_dropbox_client().users_get_current_account()
        return jsonify({"status": "ok", "account": account.name.display_name})
    except Exception as e:
        return jsonify({"status": "erro", "detalhes": str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
