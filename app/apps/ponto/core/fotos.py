# -*- coding: utf-8 -*-
"""
A foto da batida: do base64 do celular ao banco.

Por que redimensionar no servidor: um celular manda 3 a 8 MB por foto; 400
pessoas × 4 batidas × 22 dias seriam dezenas de GB por mês. A 800 px de lado
maior e JPEG 80, cada foto fica em ~60 a 120 KB, e o rosto continua
reconhecível — que é para o que ela serve.

A foto fica no BANCO (`ponto.fotos`), não no disco: o disco do Render é apagado
a cada publicação ou reinício. O Drive vem em fase futura; a coluna
`expurgada_em` já existe para o prazo de guarda.
"""
from __future__ import annotations

import base64
import hashlib
import io
import logging
import re

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao

logger = logging.getLogger("ponto.fotos")

# Teto do que se aceita RECEBER (antes de reduzir) e do que se GUARDA.
MAX_RECEBIDO_BYTES = 8 * 1024 * 1024
MAX_GUARDADO_BYTES = 300 * 1024
LADO_MAIOR_PX = 800
QUALIDADE_JPEG = 80

_PREFIXO_DATA_URL = re.compile(r"^data:image/[a-zA-Z0-9.+-]+;base64,", re.IGNORECASE)


def decodificar_base64(texto: str) -> bytes:
    """Aceita com ou sem o prefixo `data:image/...;base64,`."""
    limpo = _PREFIXO_DATA_URL.sub("", (texto or "").strip())
    limpo = re.sub(r"\s+", "", limpo)
    if not limpo:
        raise ErroDeValidacao("foto vazia", campo="foto_base64")
    # 4 caracteres de base64 = 3 bytes; recusa antes de decodificar um gigante.
    if len(limpo) * 3 // 4 > MAX_RECEBIDO_BYTES:
        raise ErroDeValidacao("foto maior que o limite de 8 MB", campo="foto_base64")
    try:
        return base64.b64decode(limpo, validate=True)
    except Exception as e:  # noqa: BLE001
        raise ErroDeValidacao("foto em base64 ilegível", campo="foto_base64") from e


def reduzir(conteudo: bytes) -> tuple[bytes, int, int]:
    """Abre, corrige a orientação, reduz e regrava como JPEG. Devolve
    (bytes, largura, altura). Levanta ErroDeValidacao se não for imagem."""
    try:
        from PIL import Image, ImageOps
        imagem = Image.open(io.BytesIO(conteudo))
        imagem.load()
    except Exception as e:  # noqa: BLE001
        raise ErroDeValidacao("a foto não é uma imagem legível", campo="foto_base64") from e
    imagem = ImageOps.exif_transpose(imagem)
    if imagem.mode not in ("RGB", "L"):
        imagem = imagem.convert("RGB")
    imagem.thumbnail((LADO_MAIOR_PX, LADO_MAIOR_PX))
    qualidade = QUALIDADE_JPEG
    while True:
        saida = io.BytesIO()
        imagem.save(saida, format="JPEG", quality=qualidade, optimize=True)
        dados = saida.getvalue()
        if len(dados) <= MAX_GUARDADO_BYTES or qualidade <= 40:
            break
        qualidade -= 10
    return dados, imagem.width, imagem.height


def sha256(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


def guardar(conn: Connection, foto_base64: str) -> tuple[int, str]:
    """Processa e grava a foto. Devolve (id, hash)."""
    bruto = decodificar_base64(foto_base64)
    dados, largura, altura = reduzir(bruto)
    digest = sha256(dados)
    linha = db.um(conn, """
        INSERT INTO ponto.fotos (sha256, conteudo, tamanho, mime, largura, altura)
        VALUES (:h, :c, :t, 'image/jpeg', :w, :a) RETURNING id
    """, h=digest, c=dados, t=len(dados), w=largura, a=altura)
    logger.info("Ponto: foto guardada (%d bytes, %dx%d)", len(dados), largura, altura)
    return int(linha["id"]), digest
