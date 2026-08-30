# ============================================================================
# ERP — core/documentos/armazenamento.py
# Anexos guardados NO PRÓPRIO BANCO — o ERP não depende de Dropbox nem de
# serviço externo para achar um comprovante.
#
# Antes de gravar, o arquivo é reduzido, porque documento de financeiro é
# consultado muito e ocupado para sempre:
#   IMAGEM — redimensiona para no máximo 1800 px no maior lado e recomprime em
#            JPEG de qualidade 82. Foto de celular de 4 MB vira ~150 KB e
#            continua perfeitamente legível.
#   PDF    — reescrito com limpeza e compressão de fluxos do PyMuPDF; se ainda
#            ficar acima do teto, as páginas são rasterizadas em JPEG e
#            remontadas (última cartada para digitalização pesada).
# O original nunca é jogado fora sem registro: o tamanho antes e depois fica
# gravado, e a compressão é pulada quando não compensa.
# ============================================================================
from __future__ import annotations

import gc
import hashlib
import io
import logging
import os
import re
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import Anexo

logger = logging.getLogger(__name__)

MAX_ENVIO_BYTES = 25 * 1024 * 1024      # teto do upload
LADO_MAX = 1800                          # px no maior lado da imagem
QUALIDADE = 82
ALVO_PDF_BYTES = 900 * 1024              # acima disso, tenta rasterizar
DPI_RASTER = 150

CATEGORIAS = ("COMPROVANTE", "NOTA", "CONTRATO", "ART", "SEGURO", "OS",
              "PRESTACAO_CONTAS", "MEDICAO", "OUTRO")

_MIMES = {
    ".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg", ".webp": "image/webp", ".xml": "application/xml",
    ".ofx": "application/x-ofx", ".csv": "text/csv", ".txt": "text/plain",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


def _mime(nome: str) -> str:
    return _MIMES.get(os.path.splitext(nome or "")[1].lower(), "application/octet-stream")


def _nome_seguro(nome: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", nome or "arquivo")[:120] or "arquivo"


# ---------------------------------------------------------------------------
# Compressão
# ---------------------------------------------------------------------------
def _comprimir_imagem(conteudo: bytes) -> tuple[bytes, str]:
    try:
        from PIL import Image
    except ImportError:
        return conteudo, ""
    try:
        img = Image.open(io.BytesIO(conteudo))
        if img.mode in ("RGBA", "P", "LA"):
            fundo = Image.new("RGB", img.size, (255, 255, 255))
            fundo.paste(img, mask=img.split()[-1] if img.mode in ("RGBA", "LA") else None)
            img = fundo
        else:
            img = img.convert("RGB")
        maior = max(img.size)
        if maior > LADO_MAX:
            fator = LADO_MAX / maior
            img = img.resize((max(1, int(img.width * fator)), max(1, int(img.height * fator))),
                             Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=QUALIDADE, optimize=True, progressive=True)
        img.close()
        return buf.getvalue(), "image/jpeg"
    except Exception as e:
        logger.warning("ERP/anexo: imagem não comprimida (%s)", e)
        return conteudo, ""
    finally:
        gc.collect()


def _comprimir_pdf(conteudo: bytes) -> tuple[bytes, str]:
    try:
        import fitz
    except ImportError:
        return conteudo, ""
    doc = None
    try:
        doc = fitz.open(stream=conteudo, filetype="pdf")
        limpo = doc.tobytes(garbage=4, deflate=True, clean=True)
        if len(limpo) <= ALVO_PDF_BYTES or len(limpo) < len(conteudo) * 0.7:
            return (limpo if len(limpo) < len(conteudo) else conteudo), "application/pdf"

        # ainda pesado: rasteriza as páginas em JPEG e remonta
        escala = DPI_RASTER / 72.0
        novo = fitz.open()
        try:
            for pagina in doc:
                pix = pagina.get_pixmap(matrix=fitz.Matrix(escala, escala))
                img_bytes = pix.tobytes("jpeg", jpg_quality=QUALIDADE)
                pix = None
                p = novo.new_page(width=pagina.rect.width, height=pagina.rect.height)
                p.insert_image(p.rect, stream=img_bytes)
            rasterizado = novo.tobytes(garbage=4, deflate=True)
        finally:
            novo.close()
        menor = min((limpo, rasterizado, conteudo), key=len)
        return menor, "application/pdf"
    except Exception as e:
        logger.warning("ERP/anexo: PDF não comprimido (%s)", e)
        return conteudo, ""
    finally:
        if doc is not None:
            doc.close()
        gc.collect()


def comprimir(conteudo: bytes, nome_arquivo: str) -> tuple[bytes, str, bool]:
    """Devolve (conteúdo final, mime, foi_comprimido)."""
    mime = _mime(nome_arquivo)
    if mime.startswith("image/"):
        novo, novo_mime = _comprimir_imagem(conteudo)
    elif mime == "application/pdf":
        novo, novo_mime = _comprimir_pdf(conteudo)
    else:
        return conteudo, mime, False
    if novo and len(novo) < len(conteudo) * 0.95:      # só vale se compensar
        return novo, (novo_mime or mime), True
    return conteudo, mime, False


# ---------------------------------------------------------------------------
# Gravação e leitura
# ---------------------------------------------------------------------------
def salvar(s: Session, conteudo: bytes, nome_arquivo: str, *,
           entidade_tipo: str, entidade_id: int,
           categoria: str = "OUTRO", descricao: str = "",
           usuario: Optional[Usuario] = None) -> Anexo:
    """Guarda o anexo no banco, comprimido. Reenvio do mesmo arquivo para a
    mesma entidade devolve o anexo já existente."""
    if not conteudo:
        raise ErroValidacao("Arquivo vazio.")
    if len(conteudo) > MAX_ENVIO_BYTES:
        raise ErroValidacao(f"Arquivo acima de {MAX_ENVIO_BYTES // (1024*1024)} MB.")
    categoria = (categoria or "OUTRO").upper()
    if categoria not in CATEGORIAS:
        categoria = "OUTRO"

    tamanho_original = len(conteudo)
    digest = hashlib.sha256(conteudo).hexdigest()
    ja = s.scalars(select(Anexo).where(
        Anexo.hash_sha256 == digest, Anexo.entidade_tipo == entidade_tipo,
        Anexo.entidade_id == entidade_id)).first()
    if ja is not None:
        return ja

    final, mime, comprimido = comprimir(conteudo, nome_arquivo)
    anexo = Anexo(
        entidade_tipo=entidade_tipo, entidade_id=entidade_id,
        nome_arquivo=_nome_seguro(nome_arquivo), dropbox_path=None,
        hash_sha256=digest, tamanho_bytes=len(final),
        tamanho_original=tamanho_original, conteudo=final, mime_type=mime,
        comprimido=comprimido, categoria_anexo=categoria,
        descricao=(descricao or "").strip() or None,
        enviado_por=(usuario.id if usuario else None))
    s.add(anexo)
    s.flush()
    economia = (1 - len(final) / tamanho_original) * 100 if tamanho_original else 0
    logger.info("ERP/anexo: %s (%s) %d KB → %d KB (%.0f%% menor)",
                anexo.nome_arquivo, categoria, tamanho_original // 1024,
                len(final) // 1024, economia)
    registrar_evento(s, entidade_tipo, entidade_id, "ANEXO_GUARDADO", {
        "arquivo": anexo.nome_arquivo, "categoria": categoria,
        "tamanho_kb": len(final) // 1024, "original_kb": tamanho_original // 1024,
        "comprimido": comprimido}, usuario.id if usuario else None)
    return anexo


def obter(s: Session, anexo_id: int) -> Anexo:
    a = s.get(Anexo, anexo_id)
    if a is None:
        raise ErroValidacao("Anexo não encontrado.")
    return a


def listar(s: Session, entidade_tipo: str, entidade_id: int) -> list[dict[str, Any]]:
    linhas = s.scalars(select(Anexo).where(
        Anexo.entidade_tipo == entidade_tipo, Anexo.entidade_id == entidade_id)
        .order_by(Anexo.criado_em.desc())).all()
    return [{
        "id": a.id, "nome": a.nome_arquivo, "categoria": a.categoria_anexo or "OUTRO",
        "descricao": a.descricao, "mime": a.mime_type,
        "tamanho_kb": round((a.tamanho_bytes or 0) / 1024, 1),
        "original_kb": round((a.tamanho_original or a.tamanho_bytes or 0) / 1024, 1),
        "comprimido": a.comprimido,
        "em": a.criado_em.strftime("%d/%m/%Y %H:%M"),
    } for a in linhas]


def excluir(s: Session, anexo_id: int, usuario: Usuario) -> None:
    a = obter(s, anexo_id)
    registrar_evento(s, a.entidade_tipo, a.entidade_id, "ANEXO_EXCLUIDO",
                     {"arquivo": a.nome_arquivo, "categoria": a.categoria_anexo},
                     usuario.id)
    s.delete(a)


def estatisticas(s: Session) -> dict[str, Any]:
    """Quanto os anexos ocupam — para acompanhar o crescimento do banco."""
    from sqlalchemy import func
    total, original, qtd = s.execute(select(
        func.coalesce(func.sum(Anexo.tamanho_bytes), 0),
        func.coalesce(func.sum(func.coalesce(Anexo.tamanho_original, Anexo.tamanho_bytes)), 0),
        func.count(Anexo.id))).first()
    return {"arquivos": qtd, "ocupado_mb": round(total / 1024 / 1024, 2),
            "seria_mb": round(original / 1024 / 1024, 2),
            "economia_pct": round((1 - total / original) * 100, 1) if original else 0.0}
