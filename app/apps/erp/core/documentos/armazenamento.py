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

# BOLETO e GUIA entraram em 10/09/2026, quando o lançamento passou a aceitar
# vários documentos: o par nota + boleto é o caso mais comum de todos, e sem a
# categoria os dois ficavam como "outro" — o que apaga justamente a diferença
# que interessa na hora de procurar.
CATEGORIAS = ("COMPROVANTE", "NOTA", "BOLETO", "GUIA", "CONTRATO", "ART",
              "SEGURO", "OS", "PRESTACAO_CONTAS", "MEDICAO", "PROPOSTA", "OUTRO")

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
# ---------------------------------------------------------------------------
# ONDE OS BYTES FICAM
#
# Até a migração 043 havia uma resposta só: dentro do banco. Agora são duas, e
# a escolha é do dono, na tela de Configurações. O resto do ERP não sabe da
# diferença — salva e lê por aqui, como sempre fez.
#
# A REGRA QUANDO O DRIVE FALHA: guarda no banco assim mesmo. Perder o
# comprovante que a pessoa acabou de anexar seria o pior desfecho possível;
# ocupar um pouco de banco é o menor dos males, e o trabalho de mudança leva
# esse anexo para o Drive depois.
# ---------------------------------------------------------------------------
def _onde_guardar(s: Session, final: bytes, nome: str,
                  mime: str) -> tuple[str, Optional[str], Optional[bytes]]:
    """Devolve (guardado_em, drive_file_id, bytes_para_o_banco)."""
    from app.apps.erp.core.documentos import drive
    try:
        if not drive.ativo(s):
            return "BANCO", None, final
        cfg = drive.configuracao(s)
        file_id = drive.enviar(final, nome, mime, pasta=cfg["pasta"],
                               impersonar=cfg["impersonar"])
        return "DRIVE", file_id, None
    except Exception as e:
        logger.warning("ERP/anexo: Drive indisponível (%s) — guardando no banco", e)
        return "BANCO", None, final


def conteudo_de(s: Session, anexo: Anexo) -> bytes:
    """Os bytes do anexo, venha ele de onde vier.

    É por aqui que a rota de download passa. Nunca devolver link do Drive para
    a tela: quem confere permissão é o ERP, e link do Drive não passa por ele.
    """
    if anexo.guardado_em == "DRIVE" and anexo.drive_file_id:
        from app.apps.erp.core.documentos import drive
        cfg = drive.configuracao(s)
        try:
            return drive.baixar(anexo.drive_file_id, impersonar=cfg["impersonar"])
        except drive.ErroDrive as e:
            raise ErroValidacao(
                "O documento está guardado no Google Drive e o Drive não "
                f"respondeu agora. Tente de novo em instantes. ({e})")
    return anexo.conteudo or b""


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
    onde, file_id, bytes_no_banco = _onde_guardar(s, final, _nome_seguro(nome_arquivo), mime)
    anexo = Anexo(
        entidade_tipo=entidade_tipo, entidade_id=entidade_id,
        nome_arquivo=_nome_seguro(nome_arquivo), dropbox_path=None,
        hash_sha256=digest, tamanho_bytes=len(final),
        tamanho_original=tamanho_original, conteudo=bytes_no_banco, mime_type=mime,
        comprimido=comprimido, categoria_anexo=categoria,
        guardado_em=onde, drive_file_id=file_id,
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
        "comprimido": comprimido, "guardado_em": onde},
        usuario.id if usuario else None)
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
                     {"arquivo": a.nome_arquivo, "categoria": a.categoria_anexo,
                      "guardado_em": a.guardado_em}, usuario.id)
    # O arquivo no Drive sai junto: anexo apagado no ERP que continuasse lá
    # viraria lixo pago, invisível e sem dono.
    if a.guardado_em == "DRIVE" and a.drive_file_id:
        from app.apps.erp.core.documentos import drive
        drive.apagar(a.drive_file_id, impersonar=drive.configuracao(s)["impersonar"])
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


# ---------------------------------------------------------------------------
# A MUDANÇA DOS ANEXOS ANTIGOS PARA O DRIVE
#
# Duas etapas, de propósito, e NUNCA na mesma transação:
#   1. o anexo novo passa a ir para o Drive (basta ligar em Configurações);
#   2. os antigos são levados aos poucos, por aqui.
#
# A regra que não se negocia: NADA sai do banco sem que a cópia no Drive tenha
# sido lida de volta e conferida byte a byte. Um arquivo perdido aqui é um
# comprovante que não existe mais em lugar nenhum.
#
# Vai em lotes pequenos porque o serviço tem 2 GB e divide processo com os
# outros módulos: mover mil anexos de uma vez derrubaria o monorepo inteiro.
# ---------------------------------------------------------------------------
def a_mover(s: Session) -> dict[str, Any]:
    """Quanto ainda está no banco — o que a tela mostra antes de começar."""
    from sqlalchemy import func as _f
    linha = s.execute(select(
        _f.count(Anexo.id), _f.coalesce(_f.sum(Anexo.tamanho_bytes), 0)
    ).where(Anexo.guardado_em == "BANCO")).one()
    no_drive = s.scalar(select(_f.count(Anexo.id)).where(Anexo.guardado_em == "DRIVE")) or 0
    return {"no_banco": int(linha[0]), "bytes_no_banco": int(linha[1]),
            "mb_no_banco": round(int(linha[1]) / (1024 * 1024), 1),
            "no_drive": int(no_drive)}


def mover_para_drive(s: Session, *, limite: int = 25) -> dict[str, Any]:
    """Leva até `limite` anexos do banco para o Drive. Devolve o que aconteceu."""
    from app.apps.erp.core.documentos import drive
    cfg = drive.configuracao(s)
    if not cfg["usavel"]:
        raise ErroValidacao("Configure a pasta do Drive antes de mover os anexos.")

    fila = s.scalars(select(Anexo).where(Anexo.guardado_em == "BANCO")
                     .order_by(Anexo.id).limit(limite)).all()
    movidos, falhas = 0, []
    for a in fila:
        original = bytes(a.conteudo or b"")
        if not original:
            falhas.append({"id": a.id, "erro": "sem conteúdo no banco"})
            continue
        try:
            file_id = drive.enviar(original, a.nome_arquivo,
                                   a.mime_type or "application/octet-stream",
                                   pasta=cfg["pasta"], impersonar=cfg["impersonar"])
        except drive.ErroDrive as e:
            falhas.append({"id": a.id, "erro": str(e)})
            continue
        # CONFERE ANTES DE APAGAR. Sem isto, uma falha silenciosa do Drive
        # apagaria o documento do banco e ninguém saberia.
        try:
            volta = drive.baixar(file_id, impersonar=cfg["impersonar"])
        except drive.ErroDrive as e:
            drive.apagar(file_id, impersonar=cfg["impersonar"])
            falhas.append({"id": a.id, "erro": f"gravou mas não li de volta: {e}"})
            continue
        if volta != original:
            drive.apagar(file_id, impersonar=cfg["impersonar"])
            falhas.append({"id": a.id, "erro": "a cópia no Drive não confere"})
            continue
        a.drive_file_id = file_id
        a.guardado_em = "DRIVE"
        a.conteudo = None
        s.flush()
        movidos += 1

    if movidos:
        logger.info("ERP/anexo: %d anexo(s) movidos para o Drive", movidos)
    resto = a_mover(s)
    return {"movidos": movidos, "falhas": falhas, **resto}
