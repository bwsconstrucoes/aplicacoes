from flask import Flask, request, jsonify, send_file
import os, requests, threading, time, tempfile, base64, binascii
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
        data = resp.json()
        DROPBOX_TOKEN = data["access_token"]
        DROPBOX_TOKEN_EXPIRATION = time.time() + int(data.get("expires_in",14400)) - 60
    else:
        raise Exception(f"Erro ao renovar token: {resp.text}")

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
            if hasattr(err, 'get_path') and err.get_path().is_not_found():
                break
            else:
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

@app.route('/compilar', methods=['POST'])
def compilar():
    data = request.get_json(silent=True) or {}
    attachments = data.get("attachments", [])
    links = data.get("links", [])
    pasta = data.get("pasta", "/pdf-compilados")
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)
    nome_arquivo = data.get("nome_arquivo", "compilado.pdf")
    if not nome_arquivo.lower().endswith(".pdf"):
        nome_arquivo += ".pdf"

    print("DEBUG attachments:", attachments)
    items = []
    for url in links or []:
        items.append({"filename": os.path.basename(url), "url": url})
    for att in attachments or []:
        print("DEBUG att keys:", att.keys())
        filename = att.get("filename")
        if "base64" in att:
            items.append({"filename": filename, "base64": att.get("base64")})
        elif "data" in att:
            items.append({"filename": filename, "hex": att.get("data")})

    results = []
    for item in items:
        filename = item.get("filename")
        bio = None

        if item.get("url"):
            r = requests.get(item["url"])
            if r.status_code != 200:
                print("DEBUG url failed:", item["url"])
                continue
            bio = BytesIO(r.content)
        elif item.get("base64"):
            try:
                bio = BytesIO(base64.b64decode(item["base64"]))
            except Exception as e:
                print("DEBUG base64 decode failed:", e)
                continue
        elif item.get("hex"):
            try:
                bio = BytesIO(binascii.unhexlify(item["hex"]))
            except Exception as e:
                print("DEBUG hex decode failed:", e)
                continue

        if not bio:
            continue

        reader = PdfReader(bio)
        texto_paginas = [page.extract_text() or "" for page in reader.pages]

        full_writer = PdfWriter()
        bio.seek(0)
        for p in reader.pages:
            full_writer.add_page(p)
        out = BytesIO()
        full_writer.write(out)
        out.seek(0)

        link = None
        if salvar:
            path = f"{pasta}/{filename}"
            link = upload_dropbox(out, path)
            if deletar:
                schedule_delete(path, int(data.get("auto_delete", 300)))

        results.append({
            "filename": filename,
            "texto": texto_paginas,
            "link": link
        })

    return jsonify({"status": "ok", "results": results})

@app.route('/pdf2texto', methods=['POST'])
def pdf2texto():
    data = request.get_json(silent=True) or {}
    print("📥 DEBUG /pdf2texto received keys:", data.keys())
    print("📥 DEBUG full payload:", data)

    url = data.get("url")
    b64 = data.get("base64") or data.get("data") or data.get("hex")
    if not url and not b64:
        return jsonify({"erro":"Informe a URL, base64 ou hex do PDF."}), 400


    url = data.get("url")
    b64 = data.get("base64")
    hexstr = data.get("hex")
    if not (url or b64 or hexstr):
        return jsonify({"erro": "Informe a URL, base64 ou hex do PDF."}), 400

    if url:
        r = requests.get(url)
        if r.status_code != 200:
            return jsonify({"erro": "Não foi possível baixar o PDF."}), 400
        bio = BytesIO(r.content)
    elif b64:
        try:
            bio = BytesIO(base64.b64decode(b64))
        except:
            return jsonify({"erro": "Base64 inválida."}), 400
    else:
        try:
            bio = BytesIO(binascii.unhexlify(hexstr))
        except:
            return jsonify({"erro": "Hex inválido."}), 400

    reader = PdfReader(bio)
    texto_paginas = [page.extract_text() or "" for page in reader.pages]
    return jsonify({"status": "ok", "texto": texto_paginas})

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
