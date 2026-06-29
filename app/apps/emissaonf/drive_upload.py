# -*- coding: utf-8 -*-
"""
Upload de arquivos para o Google Drive usando a service account (mesmo credenciais.json
do gspread). Salva na pasta de notas e devolve um link compartilhável.

A pasta precisa estar compartilhada com o e-mail da service account (client_email),
como Editor.

Requisitos: google-api-python-client, google-auth.
"""
from __future__ import annotations

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]
PASTA_NOTAS = "1-NxQ1Q35vC-RzwbMekZZI8JyNhmQtZyh"   # PDFs da NFS-e + Recibo

# Service account não tem cota de armazenamento própria, então NÃO pode ser dona
# de arquivos no "Meu Drive". Com delegação em todo o domínio (Admin > Controles de
# API > Delegação), a service account age COMO este usuário (que tem cota) e os
# arquivos ficam na pasta normal. Deixe "" para não personificar (ex.: se a pasta
# estiver num Drive Compartilhado, que dispensa isso).
EMAIL_IMPERSONAR = "contato@bwsconstrucoes.com.br"

MIME = {"pdf": "application/pdf", "xml": "application/xml", "txt": "text/plain"}


def _servico(caminho_json: str = "credenciais.json"):
    import os, json
    from base64 import b64decode
    b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    if b64:
        info = json.loads(b64decode(b64).decode("utf-8"))
        cred = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        cred = Credentials.from_service_account_file(caminho_json, scopes=SCOPES)
    # Quem personificar (delegação domain-wide). Configurável por env:
    #   EMISSAO_NF_DRIVE_IMPERSONAR="" (vazio) => NÃO personifica
    #   (use quando a PASTA_NOTAS estiver num Drive Compartilhado, que dispensa DWD).
    imp = os.getenv("EMISSAO_NF_DRIVE_IMPERSONAR", EMAIL_IMPERSONAR)
    if imp:
        cred = cred.with_subject(imp)
    return build("drive", "v3", credentials=cred, cache_discovery=False)


def _achar_existente(svc, nome: str, pasta: str):
    """Retorna o id de um arquivo com esse nome na pasta (ou None)."""
    nome_q = nome.replace("'", "\\'")
    q = f"name = '{nome_q}' and '{pasta}' in parents and trashed = false"
    res = svc.files().list(q=q, fields="files(id)", pageSize=1,
                           supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    arquivos = res.get("files", [])
    return arquivos[0]["id"] if arquivos else None


def enviar(nome: str, conteudo: bytes, extensao: str = "pdf",
           pasta: str = PASTA_NOTAS, publico: bool = True,
           substituir: bool = True, caminho_json: str = "credenciais.json"):
    """Sobe o arquivo na pasta e devolve (file_id, link). Se já existir um com o
    mesmo nome e substituir=True, atualiza o conteúdo em vez de duplicar."""
    svc = _servico(caminho_json)
    mime = MIME.get(extensao.lower(), "application/octet-stream")
    media = MediaInMemoryUpload(conteudo, mimetype=mime, resumable=False)

    existente = _achar_existente(svc, nome, pasta) if substituir else None
    if existente:
        f = svc.files().update(fileId=existente, media_body=media,
                               fields="id, webViewLink", supportsAllDrives=True).execute()
    else:
        meta = {"name": nome, "parents": [pasta]}
        f = svc.files().create(body=meta, media_body=media, fields="id, webViewLink",
                               supportsAllDrives=True).execute()

    fid = f["id"]
    if publico:
        try:
            svc.permissions().create(fileId=fid, body={"type": "anyone", "role": "reader"},
                                     supportsAllDrives=True).execute()
        except Exception:
            pass   # já pode estar compartilhado
    link = f.get("webViewLink") or f"https://drive.google.com/file/d/{fid}/view"
    return fid, link


def baixar(file_id: str, caminho_json: str = "credenciais.json") -> bytes:
    """Baixa o conteúdo de um arquivo do Drive pelo id."""
    import io
    from googleapiclient.http import MediaIoBaseDownload
    svc = _servico(caminho_json)
    req = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue()
