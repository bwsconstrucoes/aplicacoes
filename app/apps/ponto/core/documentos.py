# -*- coding: utf-8 -*-
"""
Documentos do ponto — atestado, declaração, acordo de banco de horas.

Mesmo caminho das fotos (`fotos.py`): o arquivo vai para o Google Drive pela
rotina do ERP, e aqui fica a ficha. Se o Drive falhar, o arquivo espera em
`ponto.documentos.conteudo` e a fila de reenvio leva depois — o pedido do
colaborador NÃO falha porque o Drive demorou.

`sigiloso` = documento de saúde (LGPD, art. 11). Quem abre é só quem tem a ação
"aprovar afastamento" (o DP, por padrão) — decisão do dono, 03/10/2026.
"""
from __future__ import annotations

import datetime as dt
import logging
import re

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao
from . import fotos

logger = logging.getLogger("ponto.documentos")

MAX_BYTES = 10 * 1024 * 1024
TIPOS_ACEITOS = {"application/pdf": ".pdf", "image/jpeg": ".jpg", "image/png": ".png",
                 "image/webp": ".webp", "image/heic": ".heic"}


def _tipo_pelo_conteudo(dados: bytes) -> str | None:
    if dados[:4] == b"%PDF":
        return "application/pdf"
    if dados[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if dados[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if dados[:4] == b"RIFF" and dados[8:12] == b"WEBP":
        return "image/webp"
    if dados[4:12] in (b"ftypheic", b"ftypheix", b"ftypmif1", b"ftypmsf1"):
        return "image/heic"
    return None


def preparar(conteudo_base64: str, nome_original: str = "") -> tuple[bytes, str]:
    """Decodifica e confere o tipo pelo CONTEÚDO, não pelo nome (nome mente).
    Foto é reduzida como as da batida, para o atestado fotografado não pesar 8 MB."""
    bruto = fotos.decodificar_base64(conteudo_base64)
    if len(bruto) > MAX_BYTES:
        raise ErroDeValidacao("documento maior que 10 MB", campo="documento")
    tipo = _tipo_pelo_conteudo(bruto)
    if tipo is None:
        raise ErroDeValidacao("o documento precisa ser PDF ou foto (JPG, PNG)", campo="documento")
    if tipo in ("image/jpeg", "image/png", "image/webp"):
        try:
            from PIL import Image, ImageOps
            import io
            imagem = ImageOps.exif_transpose(Image.open(io.BytesIO(bruto)))
            imagem = imagem.convert("RGB")
            imagem.thumbnail((1800, 1800))
            saida = io.BytesIO()
            imagem.save(saida, format="JPEG", quality=85, optimize=True)
            return saida.getvalue(), "image/jpeg"
        except Exception:  # noqa: BLE001 — se não reduzir, guarda como veio
            return bruto, tipo
    return bruto, tipo


def _nome(colaborador_id: int, tipo_ocorrencia: str, mime: str) -> str:
    agora = horario.para_local(horario.agora())
    extensao = TIPOS_ACEITOS.get(mime, "")
    limpo = re.sub(r"[^A-Z_]", "", (tipo_ocorrencia or "DOC").upper())[:20]
    return f"{agora:%Y-%m-%d_%H%M%S}_colab{colaborador_id}_{limpo}{extensao}"


def guardar(conn: Connection, conteudo_base64: str, *, colaborador_id: int,
            tipo_ocorrencia: str, nome_original: str = "", sigiloso: bool = False) -> int:
    """Prepara, sobe ao Drive (ou deixa na fila) e grava a ficha. Devolve o id."""
    dados, mime = preparar(conteudo_base64, nome_original)
    nome = _nome(colaborador_id, tipo_ocorrencia, mime)
    file_id, erro = None, None
    try:
        file_id = fotos._subir_no_drive(conn, dados, nome, horario.agora(), mime)
    except Exception as e:  # noqa: BLE001 — Drive fora: fica na fila
        erro = str(e)[:500]
    linha = db.um(conn, """
        INSERT INTO ponto.documentos (sha256, nome_original, nome_arquivo, mime, tamanho,
                                      sigiloso, drive_file_id, enviada_em, conteudo,
                                      tentativas, ultimo_erro)
        VALUES (:h, :orig, :nome, :mime, :t, :sig, :fid,
                CASE WHEN :fid IS NULL THEN NULL ELSE now() END, :c, 1, :erro)
        RETURNING id
    """, h=fotos.sha256(dados), orig=(nome_original or "")[:200], nome=nome, mime=mime,
         t=len(dados), sig=sigiloso, fid=file_id, c=(None if file_id else dados), erro=erro)
    logger.info("Ponto: documento %s guardado (%s)", nome, "Drive" if file_id else "fila")
    return int(linha["id"])


def ficha(conn: Connection, documento_id: int) -> dict | None:
    return db.um(conn, "SELECT id, sha256, nome_original, nome_arquivo, mime, tamanho, sigiloso, "
                       "drive_file_id, criado_em FROM ponto.documentos WHERE id = :id",
                 id=documento_id)


def baixar(conn: Connection, documento_id: int) -> tuple[bytes, str, str]:
    """(bytes, mime, nome). Levanta LookupError."""
    d = db.um(conn, "SELECT drive_file_id, conteudo, mime, nome_arquivo FROM ponto.documentos "
                    "WHERE id = :id", id=documento_id)
    if not d:
        raise LookupError("documento não encontrado")
    if d["conteudo"] is not None:
        return bytes(d["conteudo"]), d["mime"], d["nome_arquivo"]
    from app.apps.erp.core.documentos import drive as drive_erp
    return (drive_erp.baixar(d["drive_file_id"], impersonar=fotos.quem_personificar()),
            d["mime"], d["nome_arquivo"])


def enviar_pendentes(conn: Connection, *, limite: int = 50) -> dict:
    fila = db.todos(conn, """
        SELECT id, conteudo, sha256, nome_arquivo, mime, criado_em FROM ponto.documentos
         WHERE drive_file_id IS NULL AND conteudo IS NOT NULL ORDER BY id LIMIT :n
    """, n=max(1, min(int(limite), 500)))
    enviados, falhas = 0, []
    for d in fila:
        dados = bytes(d["conteudo"])
        if fotos.sha256(dados) != d["sha256"]:
            falhas.append({"id": d["id"], "erro": "conteúdo não confere com o hash"})
            continue
        try:
            fid = fotos._subir_no_drive(conn, dados, d["nome_arquivo"], d["criado_em"], d["mime"])
        except Exception as e:  # noqa: BLE001
            db.executar(conn, "UPDATE ponto.documentos SET tentativas = tentativas + 1, "
                              "ultimo_erro = :e WHERE id = :id", e=str(e)[:500], id=d["id"])
            falhas.append({"id": d["id"], "erro": str(e)[:200]})
            continue
        db.executar(conn, "UPDATE ponto.documentos SET drive_file_id = :f, enviada_em = now(), "
                          "conteudo = NULL, ultimo_erro = NULL WHERE id = :id", f=fid, id=d["id"])
        enviados += 1
    restantes = db.um(conn, "SELECT count(*) AS n FROM ponto.documentos "
                            "WHERE drive_file_id IS NULL AND conteudo IS NOT NULL")["n"]
    return {"enviados": enviados, "falhas": falhas, "restantes": int(restantes)}


