# -*- coding: utf-8 -*-
"""
Upload de anexos para o Google Drive com link público.
"""

import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.service_account import Credentials
from io import BytesIO
from base64 import b64decode


def get_drive_service():
    creds_json = b64decode(os.getenv("GOOGLE_CREDENTIALS_BASE64")).decode("utf-8")
    creds = Credentials.from_service_account_info(eval(creds_json))
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_to_drive(filename, file_bytes):
    try:
        folder_id = os.getenv("GDRIVE_FOLDER_ID")
        service = get_drive_service()

        file_metadata = {
            "name": filename,
            "parents": [folder_id]
        }

        media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype="application/octet-stream")
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        file_id = file.get("id")

        # tornar público
        service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader"},
        ).execute()

        return f"https://drive.google.com/file/d/{file_id}/view"
    except Exception as e:
        print(f"[Erro upload_to_drive] {e}")
        return ""
