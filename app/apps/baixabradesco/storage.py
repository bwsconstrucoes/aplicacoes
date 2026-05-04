# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import requests
import dropbox
from typing import Optional

_DROPBOX_TOKEN: Optional[str] = None
_DROPBOX_TOKEN_EXPIRATION = 0.0


def normalize_shared_link(url: str) -> str:
    """Padroniza link compartilhado do Dropbox para download direto (dl=1)."""
    url = str(url or '').strip()
    if not url:
        return ''
    if '?dl=0' in url:
        url = url.replace('?dl=0', '?dl=1')
    if '&dl=0' in url:
        url = url.replace('&dl=0', '&dl=1')
    if 'dl=1' not in url:
        sep = '&' if '?' in url else '?'
        url = f'{url}{sep}dl=1'
    return url


def get_dropbox_client():
    """Mesmo padrão do módulo pdf_processor: token por refresh_token e cache em memória."""
    global _DROPBOX_TOKEN, _DROPBOX_TOKEN_EXPIRATION
    if not _DROPBOX_TOKEN or time.time() > _DROPBOX_TOKEN_EXPIRATION:
        refresh_dropbox_token()
    return dropbox.Dropbox(_DROPBOX_TOKEN)


def refresh_dropbox_token():
    global _DROPBOX_TOKEN, _DROPBOX_TOKEN_EXPIRATION
    app_key = os.getenv('DROPBOX_APP_KEY', '')
    app_secret = os.getenv('DROPBOX_APP_SECRET', '')
    refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN', '')
    if not (app_key and app_secret and refresh_token):
        raise RuntimeError('Dropbox não configurado. Configure DROPBOX_APP_KEY, DROPBOX_APP_SECRET e DROPBOX_REFRESH_TOKEN.')
    resp = requests.post('https://api.dropbox.com/oauth2/token', data={
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': app_key,
        'client_secret': app_secret,
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    _DROPBOX_TOKEN = data['access_token']
    _DROPBOX_TOKEN_EXPIRATION = time.time() + int(data.get('expires_in', 14400)) - 60


def upload_dropbox_bytes(file_bytes: bytes, path: str) -> dict:
    """Sobe bytes no Dropbox, evitando sobrescrever, e retorna link dl=1.

    Replica a lógica encontrada no pdf_processor:
    - se o arquivo existir, acrescenta (1), (2), ...
    - cria/reaproveita shared link
    - troca dl=0 por dl=1, inclusive quando vier como &dl=0
    """
    dbx = get_dropbox_client()
    path = _normalize_dropbox_path(path)
    base, ext = os.path.splitext(path)
    final_path = path
    cnt = 1
    while True:
        try:
            dbx.files_get_metadata(final_path)
            final_path = f'{base}({cnt}){ext}'
            cnt += 1
        except dropbox.exceptions.ApiError as e:
            err = getattr(e, 'error', None)
            if hasattr(err, 'get_path') and err.get_path().is_not_found():
                break
            raise

    dbx.files_upload(file_bytes, final_path, mode=dropbox.files.WriteMode.add)
    try:
        url = dbx.sharing_create_shared_link_with_settings(final_path).url
    except dropbox.exceptions.ApiError as e:
        if e.error.is_shared_link_already_exists():
            links = dbx.sharing_list_shared_links(path=final_path).links
            url = links[0].url if links else ''
        else:
            raise
    return {
        'storage': 'dropbox',
        'path': final_path,
        'url': normalize_shared_link(url),
    }


def build_receipt_page_filename(original_filename: str, page: int, detected_id: str = '') -> str:
    name = os.path.basename(original_filename or 'comprovante.pdf')
    if name.lower().endswith('.pdf'):
        name = name[:-4]
    safe = _safe_filename(name)[:120] or 'comprovante'
    suffix = f'_{detected_id}' if detected_id else ''
    return f'{safe}{suffix}_page{page}.pdf'


def _safe_filename(value: str) -> str:
    value = str(value or '').strip()
    for ch in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        value = value.replace(ch, '-')
    return ' '.join(value.split())


def _normalize_dropbox_path(path: str) -> str:
    path = str(path or '').strip()
    if not path.startswith('/'):
        path = '/' + path
    while '//' in path:
        path = path.replace('//', '/')
    return path
