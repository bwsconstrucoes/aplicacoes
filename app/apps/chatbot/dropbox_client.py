# -*- coding: utf-8 -*-

import os
import json
import logging
import requests
from functools import lru_cache

logger = logging.getLogger(__name__)

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY", "")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET", "")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN", "")

DROPBOX_BASE_PATH = "/BWS DP/FOLHA DE PAGAMENTO/CONTRACHEQUES"
DROPBOX_API_CONTENT = "https://content.dropboxapi.com/2/files/download"


@lru_cache(maxsize=1)
def obter_access_token() -> str:
    url = "https://api.dropboxapi.com/oauth2/token"

    resp = requests.post(
        url,
        data={
            "grant_type": "refresh_token",
            "refresh_token": DROPBOX_REFRESH_TOKEN,
        },
        auth=(DROPBOX_APP_KEY, DROPBOX_APP_SECRET),
        timeout=30,
    )

    if resp.status_code != 200:
        logger.error(f"[dropbox] Erro ao renovar token: {resp.status_code} - {resp.text[:300]}")
        raise Exception("Falha ao renovar access token do Dropbox")

    return resp.json()["access_token"]


def _headers_download(caminho: str) -> dict:
    token = obter_access_token()

    return {
        "Authorization": f"Bearer {token}",
        "Dropbox-API-Arg": json.dumps({"path": caminho}),
    }


def baixar_pdf(ano: int, mes: int) -> bytes | None:
    competencia = f"{ano:04d}.{mes:02d}"
    caminho = f"{DROPBOX_BASE_PATH}/{competencia}.pdf"

    logger.info(f"[dropbox] Buscando: {caminho}")

    try:
        resp = requests.post(
            DROPBOX_API_CONTENT,
            headers=_headers_download(caminho),
            timeout=30,
        )

        if resp.status_code == 200:
            logger.info(f"[dropbox] PDF encontrado: {caminho} ({len(resp.content)} bytes)")
            return resp.content

        elif resp.status_code == 409:
            logger.warning(f"[dropbox] PDF não encontrado: {caminho}")
            return None

        else:
            logger.error(f"[dropbox] Erro {resp.status_code}: {resp.text[:300]}")
            return None

    except Exception as e:
        logger.error(f"[dropbox] Exceção ao baixar PDF: {e}")
        return None


def listar_competencias_disponiveis() -> list[str]:
    url = "https://api.dropboxapi.com/2/files/list_folder"

    try:
        token = obter_access_token()

        resp = requests.post(
            url,
            json={"path": DROPBOX_BASE_PATH},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

        if resp.status_code != 200:
            logger.error(f"[dropbox] Erro ao listar competências: {resp.status_code} - {resp.text[:300]}")
            return []

        data = resp.json()

        arquivos = [
            e["name"].replace(".pdf", "")
            for e in data.get("entries", [])
            if e.get("name", "").endswith(".pdf")
        ]

        return sorted(arquivos, reverse=True)

    except Exception as e:
        logger.error(f"[dropbox] Erro ao listar competências: {e}")
        return []