# ============================================================================
# BWS ERP — core/documentos/service.py
# Registro de documentos fiscais (dedupe por chave) e vínculo com o lançamento.
# ============================================================================
from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.core.documentos.nfe import DadosNFe
from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import DocumentoFiscal, SituacaoNota, TipoDocFiscal, Titulo


def registrar_nfe(s: Session, dados: DadosNFe, usuario: Optional[Usuario],
                  origem: str = "UPLOAD_XML") -> tuple[DocumentoFiscal, bool]:
    """Registra a NFe (ou retorna a existente). Retorna (doc, ja_existia)."""
    existente = s.scalars(select(DocumentoFiscal).where(
        DocumentoFiscal.chave_acesso == dados.chave)).first()
    if existente is not None:
        return existente, True
    doc = DocumentoFiscal(
        tipo=TipoDocFiscal.NFE, chave_acesso=dados.chave,
        numero=dados.numero, serie=dados.serie,
        emitente_doc=dados.emitente_cnpj, emitente_nome=dados.emitente_nome,
        destinatario_doc=dados.destinatario_doc,
        valor_total=dados.valor_total, data_emissao=dados.emissao,
        situacao=SituacaoNota.DESCONHECIDA, origem=origem,
        dados={"natureza_operacao": dados.natureza_operacao,
               "duplicatas": [{"n": d.numero,
                               "venc": d.vencimento.isoformat() if d.vencimento else None,
                               "valor": str(d.valor)} for d in dados.duplicatas]},
    )
    s.add(doc)
    s.flush()
    registrar_evento(s, "documento_fiscal", doc.id, "REGISTRADO",
                     {"chave": dados.chave, "emitente": dados.emitente_nome,
                      "valor": str(dados.valor_total), "origem": origem},
                     usuario.id if usuario else None)
    return doc, False


def titulo_da_nota(s: Session, doc_id: int) -> Optional[Titulo]:
    return s.scalars(select(Titulo).where(Titulo.documento_fiscal_id == doc_id)).first()
