# -*- coding: utf-8 -*-
"""
VER O ANEXO E O COMPROVANTE SEM BAIXAR — 10/10/2026.

O dono: *"os anexos e comprovantes sempre remetem ao download do arquivo,
sendo que muitas vezes deseja-se apenas dar uma olhada rápida. A abertura num
modal é viável? (…) daí, se quiser, podemos clicar para download. No Pipefy é
assim."*

Cada serviço pede um jeito:

  Google Drive — a página de pré-visualização do próprio Drive, numa moldura
                 (`/file/d/<id>/preview`). Abre com o login Google de quem
                 olha, como abriria clicando no link.
  Dropbox, Pipefy (os anexos ficam num depósito da Amazon) — o servidor busca
                 o arquivo e o devolve "para ver", não "para baixar". Os dois
                 mandam o navegador baixar, e é só por isso que passam por aqui.

⚠️ O SERVIDOR SÓ BUSCA EM ENDEREÇO CONHECIDO (os desses serviços, conferidos
também DEPOIS dos redirecionamentos), e com teto de tamanho: sem a lista,
esta rota viraria um jeito de fazer o servidor buscar qualquer endereço; sem o
teto, um arquivo enorme prenderia memória e uma das quatro linhas do serviço.
O arquivo passa em pedaços, sem ficar inteiro na memória.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

TETO_BYTES = 25 * 1024 * 1024
SEGUNDOS = 30
# O fim do nome do servidor; "dropbox.com" cobre www.dropbox.com.
HOSTS = ("dropbox.com", "dropboxusercontent.com", "pipefy.com", "amazonaws.com")
# O que um navegador mostra sozinho numa moldura.
TIPOS_QUE_SE_VEEM = ("application/pdf", "image/", "text/plain")


class ErroDaPrevia(Exception):
    """A frase vai para a janela."""


def _host_permitido(url: str) -> bool:
    try:
        partes = urlparse(str(url or ""))
    except ValueError:
        return False
    host = (partes.hostname or "").lower()
    return partes.scheme in ("http", "https") and any(
        host == h or host.endswith("." + h) for h in HOSTS)


def id_do_drive(url: str) -> str:
    from .formatos import _DRIVE_ID
    achado = _DRIVE_ID.search(str(url or ""))
    return next((g for g in achado.groups() if g), "") if achado else ""


def como_ver(url: str) -> dict:
    """{"modo": "drive"|"servidor"|"aba", "endereco": …} — como a janela mostra
    este link. "aba" = não dá para mostrar aqui; abre em outra aba."""
    url = str(url or "").strip()
    arquivo = id_do_drive(url)
    if arquivo:
        return {"modo": "drive",
                "endereco": f"https://drive.google.com/file/d/{arquivo}/preview"}
    if _host_permitido(url):
        return {"modo": "servidor", "endereco": url}
    return {"modo": "aba", "endereco": url}


def _para_buscar(url: str) -> str:
    """O Dropbox entrega o arquivo em si com `raw=1` (sem `dl=0`, que abre a
    página dele)."""
    if "dropbox.com" in url:
        url = re.sub(r"([?&])dl=[01]", r"\1raw=1", url)
        if "raw=1" not in url:
            url += ("&" if "?" in url else "?") + "raw=1"
    return url


def buscar(url: str, sessao=None):
    """(pedaços, tipo, nome) do arquivo, para devolver "para ver". Levanta
    `ErroDaPrevia` com a frase da janela."""
    import requests
    if not _host_permitido(url):
        raise ErroDaPrevia("Este endereço não abre aqui — use \"Abrir em outra aba\".")
    sessao = sessao or requests
    try:
        resp = sessao.get(_para_buscar(url), stream=True, timeout=SEGUNDOS,
                          allow_redirects=True)
    except Exception as e:  # noqa: BLE001 — rede: a frase vai para a janela
        raise ErroDaPrevia(f"O arquivo não respondeu: {str(e)[:150]}") from e
    if not _host_permitido(getattr(resp, "url", url)):
        resp.close()
        raise ErroDaPrevia("O arquivo levou a um endereço desconhecido — use "
                           "\"Abrir em outra aba\".")
    if resp.status_code >= 400:
        resp.close()
        raise ErroDaPrevia("O link não abriu (pode ter vencido — o anexo do Pipefy "
                           "vale por um tempo). Abra a SP de novo para um link novo.")
    tipo = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    tamanho = int(resp.headers.get("Content-Length") or 0)
    if tamanho > TETO_BYTES:
        resp.close()
        raise ErroDaPrevia("Arquivo grande demais para ver aqui — use \"Baixar\".")
    nome = _nome_do_arquivo(resp.headers.get("Content-Disposition", ""), url)
    if not tipo or tipo == "application/octet-stream":
        tipo = _tipo_pelo_nome(nome)
    if not any(tipo.startswith(t) for t in TIPOS_QUE_SE_VEEM):
        resp.close()
        raise ErroDaPrevia(f"Este tipo de arquivo ({tipo or 'desconhecido'}) não se "
                           "vê no navegador — use \"Baixar\".")

    def pedacos():
        lidos = 0
        try:
            for pedaco in resp.iter_content(64 * 1024):
                lidos += len(pedaco)
                if lidos > TETO_BYTES:
                    break
                yield pedaco
        finally:
            resp.close()
    return pedacos(), tipo, nome


def _nome_do_arquivo(disposicao: str, url: str) -> str:
    achado = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)', disposicao or "", re.I)
    if achado:
        from urllib.parse import unquote
        return unquote(achado.group(1)).strip()
    caminho = urlparse(url).path.rsplit("/", 1)[-1]
    return caminho or "arquivo"


def _tipo_pelo_nome(nome: str) -> str:
    import mimetypes
    return mimetypes.guess_type(nome)[0] or ""
