# -*- coding: utf-8 -*-
"""
chatbot/dropbox_client.py
Acesso ao Dropbox para busca de PDFs de contracheques.
Padrão de caminho: /BWS DP/FOLHA DE PAGAMENTO/CONTRACHEQUES/YYYY.MM.pdf
"""

import os
import logging
import requests

logger = logging.getLogger(__name__)

DROPBOX_TOKEN       = os.getenv('DROPBOX_TOKEN', '')
DROPBOX_BASE_PATH   = '/BWS DP/FOLHA DE PAGAMENTO/CONTRACHEQUES'
DROPBOX_API_CONTENT = 'https://content.dropboxapi.com/2/files/download'


def _headers_download(caminho: str) -> dict:
    import json
    return {
        'Authorization':   f'Bearer {DROPBOX_TOKEN}',
        'Dropbox-API-Arg': json.dumps({'path': caminho}),
    }


def baixar_pdf(ano: int, mes: int) -> bytes | None:
    """
    Baixa o PDF do Dropbox para a competência YYYY.MM.
    Retorna bytes do PDF ou None se não encontrado.
    """
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
            logger.error(f"[dropbox] Erro {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"[dropbox] Exceção ao baixar PDF: {e}")
        return None


def listar_competencias_disponiveis() -> list[str]:
    """
    Lista PDFs disponíveis no Dropbox.
    Útil para sugerir competências ao usuário.
    """
    import json
    url = 'https://api.dropboxapi.com/2/files/list_folder'
    try:
        resp = requests.post(
            url,
            json={'path': DROPBOX_BASE_PATH},
            headers={
                'Authorization': f'Bearer {DROPBOX_TOKEN}',
                'Content-Type':  'application/json',
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        arquivos = [
            e['name'].replace('.pdf', '')
            for e in data.get('entries', [])
            if e.get('name', '').endswith('.pdf')
        ]
        return sorted(arquivos, reverse=True)
    except Exception as e:
        logger.error(f"[dropbox] Erro ao listar competências: {e}")
        return []
