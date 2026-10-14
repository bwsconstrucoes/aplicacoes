# -*- coding: utf-8 -*-
"""
SUBSTITUIR O COMPROVANTE DE UMA SP — 14/10/2026.

O dono: *"teve um comprovante que eu tentei baixar e, por algum motivo, está em
erro. Como a gente poderia fazer para eu poder substituir, caso eu entenda
necessário?"*

Mandar o arquivo de novo pela fila de comprovantes NÃO resolveria: o
BaixaBradesco reaproveita o link que a SPsBD já tem e ignora o arquivo novo
(`baixabradesco/core.py`, "SPsBD já possuía link de comprovante"). Por isso a
troca é direta, na ficha da SP:

  - um arquivo (PDF ou foto): sobe para a MESMA pasta do Dropbox onde o
    BaixaBradesco guarda os comprovantes, com o número da SP no nome — nunca
    por cima de outro arquivo (o Dropbox acrescenta "(1)");
  - ou um link colado (quando o comprovante certo já está em algum lugar).

O link novo vai para a coluna AG da SPsBD pelo caminho de toda alteração da
tela (banco, fila da planilha, log). O LINK ANTIGO FICA NO LOG — trocar não
apaga a prova do que havia antes. O card do Pipefy não é tocado.
"""
from __future__ import annotations

import os
import re

# A pasta do BaixaBradesco (`baixabradesco/core.py`).
PASTA_DROPBOX = "/BWS FINANCEIRO/COMPROVANTESTEMP/COMPROVANTESSP"
TETO_BYTES = 10 * 1024 * 1024

# O começo de cada tipo aceito: a extensão do nome mente, o conteúdo não.
ASSINATURAS = ((b"%PDF", ".pdf"), (b"\x89PNG\r\n\x1a\n", ".png"), (b"\xff\xd8\xff", ".jpg"))


class ErroDaSubstituicao(Exception):
    """A frase vai para a ficha."""


def extensao_do_conteudo(conteudo: bytes) -> str:
    for inicio, ext in ASSINATURAS:
        if conteudo.startswith(inicio):
            return ext
    raise ErroDaSubstituicao("O arquivo precisa ser PDF, PNG ou JPG.")


def subir_arquivo(sp_id: str, conteudo: bytes, agora=None, subir=None) -> str:
    """Guarda o arquivo no Dropbox e devolve o link dele."""
    if not conteudo:
        raise ErroDaSubstituicao("O arquivo veio vazio.")
    if len(conteudo) > TETO_BYTES:
        raise ErroDaSubstituicao("Arquivo grande demais (o limite é 10 MB).")
    ext = extensao_do_conteudo(conteudo)
    if subir is None:
        from app.apps.baixabradesco.storage import upload_dropbox_bytes as subir
    if agora is None:
        from .horario import agora as _agora
        agora = _agora()
    sp = re.sub(r"\D", "", str(sp_id))
    caminho = f"{PASTA_DROPBOX}/{sp}_comprovante_substituido_{agora:%Y%m%d_%H%M}{ext}"
    try:
        info = subir(conteudo, caminho)
    except Exception as e:  # noqa: BLE001 — Dropbox fora / credencial: a frase vai
        raise ErroDaSubstituicao(f"Não consegui guardar no Dropbox: {str(e)[:200]}") from e
    url = str((info or {}).get("url") or "").strip()
    if not url:
        raise ErroDaSubstituicao("O Dropbox guardou o arquivo mas não devolveu o link.")
    return url


def link_colado(texto: str) -> str:
    url = str(texto or "").strip()
    if not re.match(r"^https?://\S+$", url):
        raise ErroDaSubstituicao("Cole um link completo, começando com https://")
    return url


def nome_sem_caminho(nome: str) -> str:
    return os.path.basename(str(nome or "").replace("\\", "/"))
