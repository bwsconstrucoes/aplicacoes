# ============================================================================
# ERP — core/comum/assinatura.py
# A assinatura de quem assina documento pelo sistema.
#
# O dono, em 21/11/2026: "atualmente eu salvo uma imagem escaneada. Isso pode
# ficar no cadastro da pessoa, ou a gente pode ver uma forma de ele cadastrar a
# assinatura dele pela tela do celular. Fica mais fácil desenhar com o dedo."
#
# As duas portas existem: desenhar na tela ou mandar a imagem escaneada. A
# assinatura é DA PESSOA (do operador que está logado) — cada um cadastra a
# sua, e ninguém cadastra a de outro. Ela sai no pedido de compra e na
# solicitação de cotação que aquela pessoa manda.
#
# ONDE FICA: como anexo do próprio operador (`anexos`, entidade
# `usuario_assinatura`). Não é coluna em `usuarios` de propósito — o modelo do
# usuário é lido antes de toda rota, e coluna nova nele derruba o ERP inteiro
# se o código subir antes de a atualização do banco ser aplicada.
# ============================================================================
from __future__ import annotations

import base64
import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

ENTIDADE = "usuario_assinatura"
TETO_BYTES = 3 * 1024 * 1024


def _ultimo(s: Session, usuario_id: int):
    from app.apps.erp.db.models.financeiro import Anexo
    return s.scalars(select(Anexo).where(
        Anexo.entidade_tipo == ENTIDADE, Anexo.entidade_id == int(usuario_id))
        .order_by(Anexo.id.desc())).first()


def obter(s: Session, usuario_id: Optional[int]) -> Optional[bytes]:
    """A imagem da assinatura da pessoa, ou None se ela ainda não cadastrou."""
    if not usuario_id:
        return None
    from app.apps.erp.core.documentos import armazenamento
    anexo = _ultimo(s, usuario_id)
    if anexo is None:
        return None
    try:
        return armazenamento.conteudo_de(s, anexo)
    except Exception:
        logger.exception("ERP/assinatura: não consegui abrir a assinatura do usuário %s",
                         usuario_id)
        return None


def salvar(s: Session, usuario, imagem: Any) -> dict[str, Any]:
    """Guarda a assinatura. Aceita os bytes do arquivo ou o `data:image/...`
    que a tela de desenhar manda."""
    from app.apps.erp.core.documentos import armazenamento
    if isinstance(imagem, str):
        if "," not in imagem or not imagem.startswith("data:image/"):
            raise ErroValidacao("Assinatura em formato inválido.")
        try:
            imagem = base64.b64decode(imagem.split(",", 1)[1])
        except Exception:
            raise ErroValidacao("Assinatura em formato inválido.")
    if not imagem or len(imagem) < 400:
        raise ErroValidacao("Desenhe a assinatura antes de salvar.")
    if len(imagem) > TETO_BYTES:
        raise ErroValidacao("Imagem grande demais. Mande uma foto menor da assinatura.")
    try:
        from PIL import Image
        import io
        Image.open(io.BytesIO(imagem)).verify()
    except Exception:
        raise ErroValidacao("O arquivo não é uma imagem que eu consiga ler.")
    anexo = armazenamento.salvar(
        s, imagem, "assinatura.png", entidade_tipo=ENTIDADE, entidade_id=usuario.id,
        categoria="OUTRO", usuario=usuario, descricao="Assinatura cadastrada pela pessoa")
    return {"id": anexo.id}


def remover(s: Session, usuario) -> None:
    from app.apps.erp.db.models.financeiro import Anexo
    for anexo in s.scalars(select(Anexo).where(
            Anexo.entidade_tipo == ENTIDADE, Anexo.entidade_id == usuario.id)).all():
        s.delete(anexo)
