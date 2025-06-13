from flask import Flask, request, jsonify, send_file
import os, requests, threading, time, tempfile
import dropbox
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from fpdf import FPDF
from PIL import Image

app = Flask(__name__)
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

def upload_dropbox(bio, path):
    dbx.files_upload(bio.getvalue(), path, mode=dropbox.files.WriteMode.overwrite)
    url = dbx.sharing_create_shared_link_with_settings(path).url
    return url.replace("?dl=0", "?dl=1")

def schedule_delete(path, delay):
    def _del():
        time.sleep(delay)
        try: dbx.files_delete_v2(path)
        except: pass
    threading.Thread(target=_del).start()

@app.route('/compilar', methods=['POST'])
def compilar_pdf():
    data = request.get_json() or {}
    links = data.get("links", [])
    if not links: return jsonify({"erro": "Nenhum link."}), 400
    pasta = data.get("pasta", "/pdf-compilados")
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)

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
            buf = BytesIO(); f.output(buf); buf.seek(0)
            reader = PdfReader(buf)
            for p in reader.pages: writer.add_page(p)

    out = BytesIO(); writer.write(out); out.seek(0)

    if not salvar:
        return send_file(out, mimetype='application/pdf', as_attachment=True, download_name="compilado.pdf")

    path = f"{pasta}/compilado.pdf"
    link = upload_dropbox(out, path)
    if deletar: schedule_delete(path, int(data.get("auto_delete", 300)))
    return jsonify({"status": "ok", "link": link})

# pdf2texto continua igual...

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
