# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import requests

logger = logging.getLogger(__name__)

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY", "")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET", "")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN", "")

DROPBOX_BASE_PATH = "/BWS DP/FOLHA DE PAGAMENTO/CONTRACHEQUES"
DROPBOX_API_CONTENT = "https://content.dropboxapi.com/2/files/download"
DROPBOX_API_LIST = "https://api.dropboxapi.com/2/files/list_folder"
DROPBOX_API_TOKEN = "https://api.dropboxapi.com/oauth2/token"

# Cache em memória COM expiração.
# (Substitui o antigo @lru_cache, que guardava o token para sempre e causava
#  o erro 401 expired_access_token quando o processo ficava de pé por mais de ~4h.)
_DROPBOX_TOKEN = None
_DROPBOX_TOKEN_EXPIRATION = 0.0


def obter_access_token(forcar: bool = False) -> str:
    """Retorna um access token válido, renovando via refresh_token quando necessário.

    forcar=True ignora o cache e força uma renovação (usado no retry de 401).
    """
    global _DROPBOX_TOKEN, _DROPBOX_TOKEN_EXPIRATION

    if not forcar and _DROPBOX_TOKEN and time.time() < _DROPBOX_TOKEN_EXPIRATION:
        return _DROPBOX_TOKEN

    if not (DROPBOX_APP_KEY and DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN):
        raise RuntimeError(
            "Dropbox não configurado. Defina DROPBOX_APP_KEY, "
            "DROPBOX_APP_SECRET e DROPBOX_REFRESH_TOKEN."
        )

    resp = requests.post(
        DROPBOX_API_TOKEN,
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

    data = resp.json()
    _DROPBOX_TOKEN = data["access_token"]
    # Renova 60s antes do vencimento real (expires_in ~ 14400s / 4h).
    _DROPBOX_TOKEN_EXPIRATION = time.time() + int(data.get("expires_in", 14400)) - 60
    return _DROPBOX_TOKEN


def _headers_download(caminho: str, forcar_token: bool = False) -> dict:
    token = obter_access_token(forcar=forcar_token)
    return {
        "Authorization": f"Bearer {token}",
        "Dropbox-API-Arg": json.dumps({"path": caminho}),
    }


def baixar_pdf(ano: int, mes: int) -> bytes | None:
    competencia = f"{ano:04d}.{mes:02d}"
    caminho = f"{DROPBOX_BASE_PATH}/{competencia}.pdf"

    logger.info(f"[dropbox] Buscando: {caminho}")

    # Tenta 2x: na 2ª, força renovação do token (autocorreção caso o Dropbox
    # revogue o access token antes da expiração prevista).
    for tentativa in (1, 2):
        try:
            resp = requests.post(
                DROPBOX_API_CONTENT,
                headers=_headers_download(caminho, forcar_token=(tentativa == 2)),
                timeout=30,
            )

            if resp.status_code == 200:
                logger.info(f"[dropbox] PDF encontrado: {caminho} ({len(resp.content)} bytes)")
                return resp.content

            if resp.status_code == 401 and tentativa == 1:
                logger.warning("[dropbox] 401 — forçando renovação do token e tentando novamente.")
                continue

            if resp.status_code == 409:
                logger.warning(f"[dropbox] PDF não encontrado: {caminho}")
                return None

            logger.error(f"[dropbox] Erro {resp.status_code}: {resp.text[:300]}")
            return None

        except Exception as e:
            logger.error(f"[dropbox] Exceção ao baixar PDF: {e}")
            return None

    return None


def listar_competencias_disponiveis() -> list[str]:
    # Tenta 2x: na 2ª, força renovação do token.
    for tentativa in (1, 2):
        try:
            token = obter_access_token(forcar=(tentativa == 2))

            resp = requests.post(
                DROPBOX_API_LIST,
                json={"path": DROPBOX_BASE_PATH},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=30,
            )

            if resp.status_code == 401 and tentativa == 1:
                logger.warning("[dropbox] 401 ao listar — forçando renovação do token e tentando novamente.")
                continue

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

    return []