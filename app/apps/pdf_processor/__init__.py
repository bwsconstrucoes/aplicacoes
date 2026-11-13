from flask import Blueprint, request, jsonify
import os, requests, threading, time, base64, json, gc
import dropbox
import fitz  # PyMuPDF
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from urllib.parse import urlparse

bp = Blueprint("pdf_processor", __name__)

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_TOKEN = None
DROPBOX_TOKEN_EXPIRATION = 0

# Teto de tamanho por arquivo baixado via URL. Evita que um download gigante
# entre 100% na RAM e estoure a memória do worker. Ajuste se precisar compilar
# arquivos maiores (ou transforme em envvar no futuro).
MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024  # 50 MB


# Prazo TOTAL de um download. O timeout do requests vale por trecho recebido, não
# pela soma: um servidor que manda devagar segurava a linha de atendimento sem
# limite (o serviço tem 1 processo e 4 linhas — ver app/lentidao.py).
PRAZO_DOWNLOAD_S = 120


def _baixar(url, limite=MAX_DOWNLOAD_BYTES, prazo=PRAZO_DOWNLOAD_S):
    """Baixa uma URL em streaming, com teto de tamanho e prazo total.
    Retorna (bio, content_type, motivo): em caso de falha, bio é None e motivo
    diz, em português, por que o arquivo ficou de fora."""
    inicio = time.monotonic()
    try:
        with requests.get(url, stream=True, timeout=(10, 60)) as r:
            if r.status_code != 200:
                return None, None, f"o endereço respondeu {r.status_code} ao baixar"
            content_type = r.headers.get("Content-Type", "")
            buf = BytesIO()
            total = 0
            for chunk in r.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                total += len(chunk)
                if total > limite:
                    print(f"❌ Ignorado (excede {limite} bytes): {url}")
                    return None, None, f"arquivo maior que {limite // (1024 * 1024)} MB"
                if time.monotonic() - inicio > prazo:
                    print(f"❌ Ignorado (download passou de {prazo} s): {url}")
                    return None, None, f"o download passou de {prazo} segundos"
                buf.write(chunk)
            buf.seek(0)
            return buf, content_type, None
    except Exception as e:
        print(f"❌ Erro ao baixar {url}: {e}")
        return None, None, "não foi possível baixar o arquivo"


def _baixar_para_bio(url, limite=MAX_DOWNLOAD_BYTES):
    """Como _baixar, sem o motivo. Retorna (bio, content_type) ou (None, None)."""
    bio, content_type, _motivo = _baixar(url, limite)
    return bio, content_type


def _e_foto(bio):
    """True se o conteúdo começa como JPEG ou PNG (olha os primeiros bytes)."""
    bio.seek(0)
    inicio = bio.read(8)
    bio.seek(0)
    return inicio[:3] == b"\xff\xd8\xff" or inicio == b"\x89PNG\r\n\x1a\n"


def _foto_para_pdf(dados):
    """Põe a foto numa página de PDF SEM redesenhá-la: a imagem entra como veio
    (mesma qualidade, mesmo tamanho em bytes), numa página que cabe em A4.
    Antes a foto era desenhada de novo e regravada em PNG: ~2 s e ~100 MB por
    foto, arquivo 3x maior e página do tamanho de um cartaz.
    A rotação marcada pelo celular é respeitada (conferido contra o caminho antigo)."""
    foto = origem = saida = None
    try:
        foto = fitz.open(stream=dados, filetype="png" if dados[:4] == b"\x89PNG" else "jpeg")
        origem = fitz.open("pdf", foto.convert_to_pdf())
        r = origem[0].rect
        lado_a, lado_b = (595, 842) if r.height >= r.width else (842, 595)  # A4 em pé ou deitado
        escala = min(lado_a / r.width, lado_b / r.height)
        saida = fitz.open()
        pagina = saida.new_page(width=r.width * escala, height=r.height * escala)
        pagina.show_pdf_page(pagina.rect, origem, 0)
        return saida.tobytes()
    finally:
        for doc in (foto, origem, saida):
            if doc is not None:
                doc.close()


class _PedeSenha(Exception):
    """O PDF só abre com senha: não há o que fazer com ele."""


def _reabrir_pelo_fitz(dados):
    """Segunda tentativa para o PDF que o PyPDF2 não abre ou que vem protegido.
    O caso comum é o comprovante de banco: abre sem senha, mas tem trava de
    edição. Devolve o mesmo PDF regravado sem a trava, com as páginas ORIGINAIS
    (texto e nitidez preservados). Antes, esse arquivo era desenhado em baixa
    resolução: ficava ~90x maior, sem texto e pior de ler.
    Levanta _PedeSenha se ele pede senha para abrir, e ValueError se não é PDF."""
    doc = None
    try:
        doc = fitz.open(stream=dados, filetype="pdf")
        if not doc.is_pdf:
            raise ValueError("não é PDF")
        if doc.needs_pass:
            raise _PedeSenha()
        return doc.tobytes(encryption=fitz.PDF_ENCRYPT_NONE)
    finally:
        if doc is not None:
            doc.close()


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
    }, timeout=30)  # sem prazo, um Dropbox travado prendia a linha de atendimento para sempre
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

@bp.route('/compilar', methods=['POST'])
def compilar():
    data = request.get_json(silent=True) or {}
    attachments = data.get("attachments", [])
    links = data.get("links", [])
    deletar = data.get("deletar", False)
    salvar = data.get("salvar", True)
    incluir_texto = data.get("incluir_texto", True)  # permite desligar extração p/ poupar memória
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
    ignorados = []  # o que ficou de fora e por quê — campo NOVO na resposta; os antigos não mudam
    full_writer = PdfWriter()
    for item in items:
        bio = None
        filename = item.get("filename", "arquivo")
        if item.get("url"):
            bio, content_type, motivo = _baixar(item["url"])
            if not bio:
                ignorados.append({"filename": filename, "motivo": motivo})
                continue
            if not ('pdf' in content_type or 'image' in content_type or 'octet-stream' in content_type):
                bio = None
                ignorados.append({"filename": filename, "motivo": "o endereço não devolveu PDF nem imagem"})
                continue
        elif item.get("base64"):
            try:
                bio = BytesIO(base64.b64decode(item["base64"]))
                item["base64"] = None  # solta a string base64 original (dobrava a memória)
            except Exception:
                print(f"❌ Ignorado (base64 inválido): {filename}")
                ignorados.append({"filename": filename, "motivo": "conteúdo base64 inválido"})
                continue
        elif item.get("hex"):
            try:
                bio = BytesIO(bytes.fromhex(item["hex"]))
                item["hex"] = None
            except Exception:
                print(f"❌ Ignorado (hex inválido): {filename}")
                ignorados.append({"filename": filename, "motivo": "conteúdo hexadecimal inválido"})
                continue

        if not bio:
            print(f"❌ Ignorado (conteúdo vazio): {filename}")
            ignorados.append({"filename": filename, "motivo": "conteúdo vazio"})
            continue

        adicionadas = 0  # páginas deste arquivo que já entraram no PDF final
        try:
            if _e_foto(bio):
                reader = PdfReader(BytesIO(_foto_para_pdf(bio.getvalue())))
                ler_texto = False  # foto não tem texto para ler
            else:
                ler_texto = incluir_texto
                try:
                    reader = PdfReader(bio)
                    protegido = reader.is_encrypted
                except Exception:
                    reader, protegido = None, True  # o PyPDF2 nem abriu (ex.: proteção AES)
                if protegido:
                    reader = PdfReader(BytesIO(_reabrir_pelo_fitz(bio.getvalue())))
            paginas = list(reader.pages)  # lê a lista inteira ANTES de copiar a primeira
            for p in paginas:
                full_writer.add_page(p)
                adicionadas += 1
            texto = []
            if ler_texto:
                for pg in paginas:
                    # Falha ao ler o texto de uma página não derruba o arquivo: as páginas já
                    # entraram. Antes, essa falha mandava o arquivo para o caminho de baixo e
                    # as mesmas páginas entravam de novo, como imagem.
                    try:
                        texto.append(pg.extract_text() or "")
                    except Exception:
                        texto.append("")
            elif incluir_texto:
                texto = [""] * len(paginas)
        except Exception as erro:
            if isinstance(erro, _PedeSenha):
                print(f"❌ Ignorado (PDF pede senha): {filename}")
                ignorados.append({"filename": filename, "motivo": "o PDF pede senha para abrir"})
                continue
            if adicionadas:
                # parte das páginas já entrou; redesenhar o arquivo aqui as duplicaria
                print(f"⚠️ Arquivo entrou pela metade ({adicionadas} página(s)): {filename}")
                results.append({"filename": filename, "texto": [""] * adicionadas if incluir_texto else []})
                ignorados.append({"filename": filename, "motivo": f"só {adicionadas} página(s) puderam ser lidas"})
                continue
            # Último recurso, igual ao que sempre existiu: desenha cada página como imagem.
            # Serve para o PDF com defeito que o PyPDF2 não abre e para imagem que não é JPEG/PNG.
            doc = None
            try:
                bio.seek(0)
                doc = fitz.open(stream=bio.getvalue(), filetype="pdf")
                texto = []
                for pagina in doc:
                    pix = pagina.get_pixmap()
                    w, h = pix.width, pix.height
                    img_bytes = pix.tobytes("png")
                    pix = None  # libera o pixmap (grande) imediatamente
                    img_pdf = fitz.open()
                    page = img_pdf.new_page(width=w, height=h)
                    page.insert_image(fitz.Rect(0, 0, w, h), stream=img_bytes)
                    temp = BytesIO()
                    img_pdf.save(temp)
                    img_pdf.close()
                    del img_bytes
                    temp.seek(0)
                    for sp in PdfReader(temp).pages:
                        full_writer.add_page(sp)
                    texto.append("")
            except Exception:
                print(f"❌ Ignorado (não é PDF nem imagem): {filename}")
                ignorados.append({"filename": filename, "motivo": "não é PDF nem imagem (ou está corrompido)"})
                continue
            finally:
                if doc is not None:
                    doc.close()  # fecha o documento fitz (antes vazava a cada arquivo)

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

    # libera os buffers grandes e força devolução ao alocador antes de responder
    del full_writer, out
    gc.collect()

    # "ignorados" é campo acrescentado em 13/11/2026; status, file, link e results seguem como sempre foram.
    return jsonify({"status": "ok", "file": nome_arquivo, "link": link, "results": results, "ignorados": ignorados})

@bp.route('/pdf2texto', methods=['POST'])
def pdf2texto():
    data = request.get_json(silent=True) or {}
    attachments = data.get("attachments", [])
    links = data.get("links", [])  # ⇦ também aceita links
    pasta = data.get("pasta", "/pdf2texto-files")
    salvar = data.get("salvar", False)

    if links:
        attachments += [{"url": u} for u in links]

    if not attachments:
        return jsonify({"erro": "Informe ao menos um anexo em attachments ou um link em links."}), 400

    results = []
    for att in attachments:
        url = att.get("url")
        bio = None
        filename = att.get("filename") or ""

        if url:
            bio, _ = _baixar_para_bio(url)  # download seguro (streaming + timeout + teto)
            if not bio:
                continue
        else:
            raw = att.get("base64") or att.get("data") or att.get("hex")
            if not raw:
                continue
            try:
                import re
                sraw = str(raw).strip()
                if re.fullmatch(r"[0-9A-Fa-f]+", sraw):
                    bio = BytesIO(bytes.fromhex(sraw))
                else:
                    bio = BytesIO(base64.b64decode(raw))
            except Exception:
                continue

        if not bio or bio.getvalue()[:4] != b"%PDF":
            continue

        try:
            reader = PdfReader(bio)
        except Exception:
            continue

        page_texts, page_links = [], []
        if filename.lower().endswith(".pdf"):
            base_name = filename[:-4]
        elif filename:
            base_name = filename
        else:
            deduzido = ""
            if url:
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    deduzido = os.path.basename(parsed.path).strip("/")
                except Exception:
                    deduzido = ""
            if deduzido.lower().endswith(".pdf"):
                deduzido = deduzido[:-4]
            base_name = deduzido or "arquivo"

        for idx, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            page_texts.append(text)

            if salvar:
                try:
                    writer = PdfWriter()
                    writer.add_page(page)
                    out = BytesIO()
                    writer.write(out)
                    out.seek(0)
                    link = upload_dropbox(out, f"{pasta}/{base_name}_page{idx}.pdf")
                    page_links.append(link)
                except Exception:
                    page_links.append(None)

        paginas = []
        for i, t in enumerate(page_texts):
            item = {"numero": i + 1, "texto": t.strip()}
            if salvar and i < len(page_links) and page_links[i]:
                item["link"] = page_links[i]
            paginas.append(item)

        results.append({"filename": filename, "paginas": paginas})

    gc.collect()
    return jsonify({"status": "ok", "results": results})

@bp.route('/token-status', methods=['GET'])
def token_status():
    try:
        account = get_dropbox_client().users_get_current_account()
        return jsonify({"status": "ok", "account": account.name.display_name})
    except Exception as e:
        return jsonify({"status": "erro", "detalhes": str(e)}), 500