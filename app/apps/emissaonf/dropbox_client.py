# -*- coding: utf-8 -*-
"""
Cliente Dropbox: autentica por refresh token (app key/secret), sobe arquivos na
pasta das notas e gera link de compartilhamento.

Credenciais (aba Credenciais): DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN.

Nomenclatura (confirmada): "NOTA FISCAL {nº} - {medição} ª Medição {obra} (NF|Recibo)"
"""
from __future__ import annotations
import json
import requests

PASTA = "/BWS FINANCEIRO/NOTAS FISCAIS DE SAÍDA/Giss Online Eusébio"


def nome_arquivo(numero_nota, num_medicao, obra: str, tipo: str) -> str:
    """tipo: 'NF' ou 'Recibo'."""
    return f"NOTA FISCAL {numero_nota} - {num_medicao} ª Medição {obra} ({tipo})"


def obter_access_token(creds: dict) -> str:
    """Troca o refresh token por um access token válido (curto)."""
    r = requests.post(
        "https://api.dropbox.com/oauth2/token",
        data={"grant_type": "refresh_token", "refresh_token": creds["DROPBOX_REFRESH_TOKEN"]},
        auth=(creds["DROPBOX_APP_KEY"], creds["DROPBOX_APP_SECRET"]),
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Dropbox token HTTP {r.status_code}: {r.text[:200]}")
    return r.json()["access_token"]


def upload(access_token: str, dados: bytes, caminho_dropbox: str, sobrescrever=True):
    arg = {"path": caminho_dropbox,
           "mode": "overwrite" if sobrescrever else "add",
           "autorename": not sobrescrever, "mute": True}
    r = requests.post(
        "https://content.dropboxapi.com/2/files/upload",
        headers={"Authorization": f"Bearer {access_token}",
                 "Dropbox-API-Arg": json.dumps(arg, ensure_ascii=True),
                 "Content-Type": "application/octet-stream"},
        data=dados, timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Dropbox upload HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def criar_link(access_token: str, caminho_dropbox: str) -> str:
    """Cria (ou recupera) link de compartilhamento direto."""
    r = requests.post(
        "https://api.dropboxapi.com/2/sharing/create_shared_link_with_settings",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        data=json.dumps({"path": caminho_dropbox}), timeout=30,
    )
    if r.status_code == 200:
        url = r.json()["url"]
    elif r.status_code == 409:  # link já existe
        r2 = requests.post(
            "https://api.dropboxapi.com/2/sharing/list_shared_links",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            data=json.dumps({"path": caminho_dropbox, "direct_only": True}), timeout=30,
        )
        links = r2.json().get("links", [])
        if not links:
            raise RuntimeError("Dropbox: link existe mas não foi possível recuperá-lo.")
        url = links[0]["url"]
    else:
        raise RuntimeError(f"Dropbox link HTTP {r.status_code}: {r.text[:200]}")
    # ?dl=1 = download direto; troque por raw=1 se quiser visualização
    return url.replace("&dl=0", "").replace("?dl=0", "") + ("&dl=1" if "?" in url else "?dl=1")


def salvar_e_linkar(creds: dict, dados: bytes, numero_nota, num_medicao, obra, tipo,
                    extensao="pdf") -> tuple[str, str]:
    """Sobe o arquivo e devolve (caminho_dropbox, link)."""
    token = obter_access_token(creds)
    nome = nome_arquivo(numero_nota, num_medicao, obra, tipo)
    caminho = f"{PASTA}/{nome}.{extensao}"
    upload(token, dados, caminho)
    link = criar_link(token, caminho)
    return caminho, link