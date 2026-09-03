# -*- coding: utf-8 -*-
"""
drive.py — sobe arquivos no Google Drive usando a MESMA Service Account do app
(credenciais.json) e devolve um link de download público.

Usa a Drive API v3 via REST (sem depender de google-api-python-client), com uma
AuthorizedSession do google-auth. Suporta Drive Compartilhado (Shared Drive).

ATENÇÃO (propriedade/cota): a Service Account só consegue CRIAR arquivos numa
pasta de **Drive Compartilhado** (Shared Drive) onde ela seja membro com permissão
de Gerenciador de Conteúdo/Colaborador. Em pasta de "Meu Drive" comum, a SA não
tem cota de armazenamento próprio e o upload falha com erro de quota. Se isso
ocorrer, mova a pasta para um Shared Drive (ou compartilhe-a com a SA num Shared
Drive).
"""
from __future__ import annotations

import json

_SCOPES = ["https://www.googleapis.com/auth/drive"]
_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _sessao():
    """AuthorizedSession autenticada com a Service Account (escopo Drive)."""
    import gsheets
    from google.oauth2.service_account import Credentials
    from google.auth.transport.requests import AuthorizedSession
    info = gsheets._credenciais()
    if info is None:
        raise RuntimeError("Service Account ausente (credenciais.json não encontrado).")
    creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
    return AuthorizedSession(creds)


def upload_xlsx_publico(conteudo: bytes, nome: str, pasta_id: str) -> dict:
    """Cria o arquivo na pasta, envia o conteúdo (xlsx), deixa público por link e
    devolve {'id': ..., 'link': 'https://drive.google.com/uc?export=download&id=...'}.
    Levanta exceção em qualquer falha."""
    s = _sessao()
    comum = {"supportsAllDrives": "true"}

    # 1) cria os metadados do arquivo dentro da pasta
    r1 = s.post("https://www.googleapis.com/drive/v3/files", params=comum,
                json={"name": nome, "parents": [pasta_id], "mimeType": _XLSX_MIME},
                timeout=60)
    if r1.status_code >= 300:
        raise RuntimeError(f"Drive: falha ao criar arquivo (HTTP {r1.status_code}): "
                           f"{r1.text[:300]}")
    fid = r1.json().get("id")
    if not fid:
        raise RuntimeError(f"Drive: resposta sem id ao criar arquivo: {r1.text[:200]}")

    # 2) envia o conteúdo binário (media upload)
    r2 = s.patch(f"https://www.googleapis.com/upload/drive/v3/files/{fid}",
                 params={"uploadType": "media", **comum},
                 headers={"Content-Type": _XLSX_MIME},
                 data=conteudo, timeout=180)
    if r2.status_code >= 300:
        raise RuntimeError(f"Drive: falha ao enviar conteúdo (HTTP {r2.status_code}): "
                           f"{r2.text[:300]}")

    # 3) permissão pública de leitura (qualquer um com o link)
    r3 = s.post(f"https://www.googleapis.com/drive/v3/files/{fid}/permissions",
                params=comum, json={"role": "reader", "type": "anyone"}, timeout=60)
    if r3.status_code >= 300:
        raise RuntimeError(f"Drive: arquivo subiu, mas falhou a permissão pública "
                           f"(HTTP {r3.status_code}): {r3.text[:200]}")

    return {"id": fid, "link": f"https://drive.google.com/uc?export=download&id={fid}"}
