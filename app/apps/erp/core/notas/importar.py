# ============================================================================
# ERP — core/notas/importar.py
# Como as notas entram: XML, um por vez ou dentro de um .zip.
#
# POR QUE COMEÇAR PELO XML, E NÃO PELA SEFAZ
#
# Recomendação registrada em `NOTAS_FISCAIS.md` §5: o valor está no CRUZAMENTO,
# não no download. A BWS já paga um serviço (FSist) que monitora os CNPJs e
# baixa os XMLs; importar o que ele já entrega dá o cruzamento inteiro hoje,
# sem certificado digital, sem sequência da SEFAZ e sem manifestação — três
# assuntos que ainda dependem de decisão do dono.
#
# E trocar a fonte depois NÃO REFAZ o cruzamento: quando o ERP puxar direto da
# SEFAZ, ele vai desembocar exatamente aqui, em `registrar_nfe`.
#
# O PDF NÃO É GUARDADO, de propósito: o DANFE se gera a partir do XML a
# qualquer momento. Guardar os dois é pagar duas vezes pelo mesmo documento.
# ============================================================================
from __future__ import annotations

import io
import logging
import zipfile
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

MAX_ARQUIVOS = 300
MAX_BYTES = 30 * 1024 * 1024


def importar_xml(s: Session, conteudo: bytes, nome: str = "",
                 usuario: Optional[Usuario] = None,
                 origem: str = "IMPORTACAO_XML") -> dict[str, Any]:
    """Registra UMA nota. Reenvio da mesma nota devolve a que já existe."""
    from app.apps.erp.core.documentos.nfe import ErroNFe, parsear_nfe
    from app.apps.erp.core.documentos.service import registrar_nfe
    from app.apps.erp.core.notas import cruzamento

    try:
        dados = parsear_nfe(conteudo)
    except ErroNFe as e:
        raise ErroValidacao(f"{nome or 'arquivo'}: {e}")

    doc, ja_existia = registrar_nfe(s, dados, usuario, origem=origem)
    # De que empresa nossa é esta nota. Sem isso, a tela não consegue separar
    # por CNPJ — e a BWS opera com mais de um.
    if doc.empresa_id is None:
        e = cruzamento.empresa_da_nota(s, doc)
        if e is not None:
            doc.empresa_id = e.id
    s.flush()
    return {"id": doc.id, "chave": doc.chave_acesso, "numero": doc.numero,
            "emitente": doc.emitente_nome,
            "valor": float(doc.valor_total or 0),
            "emissao": doc.data_emissao.isoformat() if doc.data_emissao else None,
            "ja_existia": ja_existia,
            "para_empresa": bool(doc.empresa_id)}


def importar_lote(s: Session, arquivos: list[tuple[str, bytes]],
                  usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Vários XMLs de uma vez, ou um .zip cheio deles.

    O relatório sai por arquivo, e não como um número solto: com trezentas
    notas, "285 importadas" sem dizer quais quinze falharam é inútil.
    """
    if not arquivos:
        raise ErroValidacao("Nenhum arquivo enviado.")

    expandidos: list[tuple[str, bytes]] = []
    for nome, conteudo in arquivos:
        if len(conteudo) > MAX_BYTES:
            raise ErroValidacao(f"{nome}: acima de {MAX_BYTES // (1024*1024)} MB.")
        if nome.lower().endswith(".zip") or conteudo[:2] == b"PK":
            try:
                with zipfile.ZipFile(io.BytesIO(conteudo)) as z:
                    for item in z.infolist():
                        if item.is_dir() or not item.filename.lower().endswith(".xml"):
                            continue
                        if item.file_size > MAX_BYTES:
                            continue
                        expandidos.append((item.filename, z.read(item)))
            except zipfile.BadZipFile:
                raise ErroValidacao(f"{nome}: arquivo compactado ilegível.")
        else:
            expandidos.append((nome, conteudo))

    if len(expandidos) > MAX_ARQUIVOS:
        raise ErroValidacao(
            f"{len(expandidos)} arquivos de uma vez. O limite é {MAX_ARQUIVOS} — "
            f"o serviço divide memória com os outros módulos. Mande em partes.")

    novas, repetidas, falhas = [], [], []
    for nome, conteudo in expandidos:
        try:
            r = importar_xml(s, conteudo, nome, usuario)
        except ErroValidacao as e:
            falhas.append({"arquivo": nome, "erro": str(e)})
            continue
        (repetidas if r["ja_existia"] else novas).append({**r, "arquivo": nome})

    logger.info("ERP/notas: %d nova(s), %d repetida(s), %d falha(s)",
                len(novas), len(repetidas), len(falhas))
    return {"novas": novas, "repetidas": repetidas, "falhas": falhas,
            "total": len(expandidos),
            "sem_empresa": sum(1 for n in novas if not n["para_empresa"])}
