# ============================================================================
# ERP — core/pagamentos/comprovante.py
# Baixa a partir do COMPROVANTE do banco — o fluxo que hoje vive no
# baixabradesco, agora dentro do ERP e sem depender do Make.
#
# O financeiro manda o PDF (ou a foto) do comprovante; o sistema:
#   1. lê o documento (mesmo leitor do lançamento: PDF com texto, digitalizado
#      ou foto de celular);
#   2. procura a parcela correspondente por valor, data e nome do favorecido,
#      além do número da SP quando ele aparece no comprovante;
#   3. dá a baixa e GUARDA O COMPROVANTE anexado ao título, no Dropbox —
#      que é o que permite achar o documento depois sem garimpar e-mail;
#   4. quando não tem certeza, devolve os candidatos ranqueados em vez de
#      chutar. Nada é baixado no escuro.
#
# Também reconhece comprovante de TRANSFERÊNCIA entre contas próprias e de
# TARIFA, propondo a movimentação em vez de procurar título que não existe.
# ============================================================================
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from difflib import SequenceMatcher
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.cadastros.validadores import somente_digitos
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento
from app.apps.erp.core.pagamentos.conciliacao import _normalizar, classificar_extrato
from app.apps.erp.db.models.cadastros import ContaBancaria, Usuario
from app.apps.erp.db.models.financeiro import (
    Anexo, EspecieTitulo, Parcela, StatusParcela, StatusTitulo, Titulo,
)

logger = logging.getLogger(__name__)

JANELA_DIAS = 7
CONFIANCA_AUTOMATICA = 0.80


# ---------------------------------------------------------------------------
# Armazenamento
# ---------------------------------------------------------------------------
def guardar_anexo(s: Session, conteudo: bytes, nome_arquivo: str, *,
                  entidade_tipo: str, entidade_id: int,
                  usuario: Optional[Usuario] = None,
                  categoria: str = "COMPROVANTE") -> Optional[Anexo]:
    """Guarda o comprovante NO BANCO, comprimido — sem Dropbox."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    return salvar(s, conteudo, nome_arquivo, entidade_tipo=entidade_tipo,
                  entidade_id=entidade_id, categoria=categoria, usuario=usuario)


# ---------------------------------------------------------------------------
# Leitura e busca
# ---------------------------------------------------------------------------
def _valor(v: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(v)).quantize(Decimal("0.01")) if str(v or "").strip() else None
    except Exception:
        return None


def _data(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(v)[:10]) if v else None
    except ValueError:
        return None


def _sp_no_texto(dados: dict[str, Any]) -> Optional[str]:
    """O número da SP costuma vir no campo de identificação do pagamento."""
    alvo = " ".join(str(dados.get(c) or "") for c in
                    ("descricao", "observacoes", "numero_documento", "emitente_nome"))
    m = re.search(r"\bSP\s?0*(\d{1,6})\b", alvo, re.IGNORECASE)
    return f"SP{int(m.group(1)):06d}" if m else None


def _semelhanca(a: str, b: str) -> float:
    na, nb = _normalizar(a), _normalizar(b)
    if not na or not nb:
        return 0.0
    if na in nb or nb in na:
        return 0.95
    return SequenceMatcher(None, na, nb).ratio()


def _candidatas(s: Session, *, valor: Decimal, data_pg: Optional[date],
                favorecido: str, documento: str,
                numero_sp: Optional[str]) -> list[dict[str, Any]]:
    """Parcelas em aberto que combinam com o comprovante, ranqueadas."""
    stmt = (select(Parcela).join(Titulo, Parcela.titulo_id == Titulo.id)
            .where(Parcela.status.in_([StatusParcela.ABERTA, StatusParcela.AGENDADA]),
                   Titulo.especie == EspecieTitulo.PAGAR,
                   Titulo.status.in_([StatusTitulo.APROVADO, StatusTitulo.PAGO_PARCIAL]))
            .options(selectinload(Parcela.titulo).selectinload(Titulo.fornecedor)))
    parcelas = list(s.scalars(stmt).all())

    saida = []
    for p in parcelas:
        t = p.titulo
        pontos, motivos = 0.0, []

        if abs(Decimal(p.valor) - valor) <= Decimal("0.01"):
            pontos += 0.55
            motivos.append("valor exato")
        else:
            continue                        # valor diferente não é candidato

        if numero_sp and t.numero_sp == numero_sp:
            pontos += 0.35
            motivos.append(f"nº {numero_sp} no comprovante")

        doc_credor = somente_digitos(t.fornecedor.cnpj_cpf or "")
        if documento and doc_credor and somente_digitos(documento) == doc_credor:
            pontos += 0.25
            motivos.append("CNPJ/CPF confere")

        sim = _semelhanca(favorecido, t.fornecedor.razao_social)
        if sim >= 0.80:
            pontos += 0.20
            motivos.append(f"favorecido {sim:.0%}")
        elif favorecido and sim < 0.40:
            pontos -= 0.15
            motivos.append("favorecido diverge")

        if data_pg:
            dias = abs((p.vencimento - data_pg).days)
            if dias <= 2:
                pontos += 0.12
                motivos.append("vence na data do pagamento")
            elif dias <= JANELA_DIAS:
                pontos += 0.06
                motivos.append(f"vencimento a {dias} dia(s)")

        saida.append({
            "parcela_id": p.id, "titulo_id": t.id, "numero_sp": t.numero_sp,
            "credor": t.fornecedor.razao_social, "documento": t.fornecedor.cnpj_cpf,
            "descricao": t.descricao, "valor": float(p.valor),
            "vencimento": p.vencimento.isoformat(),
            "confianca": round(min(pontos, 1.0), 3), "motivos": motivos,
        })
    saida.sort(key=lambda x: x["confianca"], reverse=True)
    return saida


def processar_comprovante(s: Session, conteudo: bytes, nome_arquivo: str, *,
                          conta_bancaria_id: Optional[int] = None,
                          baixar_automatico: bool = True,
                          usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Lê o comprovante, encontra o título e dá a baixa, anexando o documento."""
    from app.apps.erp.core.comum.ia_custo import contexto
    try:
        with contexto(operacao="comprovante_pagamento",
                      usuario_id=(usuario.id if usuario else None)):
            lido = ler_documento(conteudo, nome_arquivo,
                                 dica_usuario="É um comprovante de pagamento bancário. "
                                              "O emitente é o FAVORECIDO que recebeu.")
    except ErroLeitura as e:
        raise ErroValidacao(f"Não consegui ler o comprovante: {e}")

    valor = _valor(lido.get("valor_total"))
    data_pg = _data(lido.get("data_emissao"))
    favorecido = (lido.get("emitente_nome") or "").strip()
    documento = lido.get("emitente_documento") or ""
    numero_sp = _sp_no_texto(lido)

    resumo = {
        "arquivo": nome_arquivo, "valor": float(valor) if valor else None,
        "data": data_pg.isoformat() if data_pg else None,
        "favorecido": favorecido, "documento": documento,
        "numero_sp_no_comprovante": numero_sp,
        "confianca_leitura": lido.get("confianca"),
        "origem_leitura": lido.get("origem_leitura"),
        "observacoes": lido.get("observacoes"),
    }

    if valor is None:
        return {"situacao": "ILEGIVEL", "leitura": resumo, "candidatas": [],
                "mensagem": "Não foi possível ler o valor do comprovante. "
                            "Envie outra imagem ou registre a baixa manualmente."}

    # comprovante que não é pagamento de título
    tipo = classificar_extrato(
        f"{lido.get('descricao','')} {lido.get('observacoes','')}", favorecido)
    if tipo in ("TARIFA", "IOF", "TRANSFERENCIA_PROPRIA", "RENDIMENTO"):
        return {"situacao": "MOVIMENTACAO", "leitura": resumo, "candidatas": [],
                "classificacao": tipo,
                "mensagem": f"O comprovante parece ser {tipo.replace('_',' ').lower()} — "
                            f"registre como movimentação, não como baixa de título."}

    candidatas = _candidatas(s, valor=valor, data_pg=data_pg, favorecido=favorecido,
                             documento=documento, numero_sp=numero_sp)
    if not candidatas:
        return {"situacao": "SEM_TITULO", "leitura": resumo, "candidatas": [],
                "mensagem": f"Nenhuma parcela em aberto de R$ {valor} para um credor "
                            f"parecido com {favorecido or 'o favorecido do comprovante'}. "
                            f"O pagamento pode não ter sido lançado."}

    melhor = candidatas[0]
    segundo = candidatas[1]["confianca"] if len(candidatas) > 1 else 0.0
    decidido = (melhor["confianca"] >= CONFIANCA_AUTOMATICA
                and melhor["confianca"] - segundo >= 0.15)

    if not (baixar_automatico and decidido):
        return {"situacao": "CONFIRMAR", "leitura": resumo,
                "candidatas": candidatas[:5],
                "mensagem": ("Mais de uma parcela combina — confirme qual."
                             if segundo >= melhor["confianca"] - 0.15
                             else "Confiança insuficiente para baixar sozinho — confirme.")}

    conta = (s.get(ContaBancaria, int(conta_bancaria_id)) if conta_bancaria_id
             else s.scalars(select(ContaBancaria).where(ContaBancaria.ativo.is_(True))).first())
    if conta is None:
        raise ErroValidacao("Nenhuma conta bancária cadastrada para registrar a baixa.")

    from app.apps.erp.core.pagamentos import service as svc_pag
    pg = svc_pag.registrar_pagamento(
        s, parcela_id=melhor["parcela_id"], conta_bancaria_id=conta.id,
        data_pagamento=data_pg or date.today(), usuario=usuario, robo=True)

    anexo = guardar_anexo(s, conteudo, nome_arquivo, entidade_tipo="titulo",
                          entidade_id=melhor["titulo_id"], usuario=usuario)
    if anexo is not None:
        pg.comprovante_anexo_id = anexo.id
    s.flush()
    registrar_evento(s, "titulo", melhor["titulo_id"], "BAIXA_POR_COMPROVANTE", {
        "numero_sp": melhor["numero_sp"], "valor": str(valor),
        "data": (data_pg or date.today()).isoformat(),
        "confianca": melhor["confianca"], "motivos": melhor["motivos"],
        "arquivo": nome_arquivo, "leitura": resumo.get("origem_leitura")},
        usuario.id if usuario else None)
    logger.info("ERP: comprovante baixou %s (confiança %.2f)",
                melhor["numero_sp"], melhor["confianca"])

    return {"situacao": "BAIXADO", "leitura": resumo, "candidatas": candidatas[:3],
            "baixa": {"numero_sp": melhor["numero_sp"], "parcela_id": melhor["parcela_id"],
                      "valor": float(pg.valor_pago),
                      "data": pg.data_pagamento.isoformat(),
                      "conta": conta.descricao, "confianca": melhor["confianca"],
                      "motivos": melhor["motivos"],
                      "anexo_id": anexo.id if anexo else None},
            "mensagem": f"{melhor['numero_sp']} baixado e comprovante anexado."}


def confirmar_baixa(s: Session, *, parcela_id: int, conta_bancaria_id: int,
                    data_pagamento: str, conteudo: Optional[bytes] = None,
                    nome_arquivo: str = "", usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Baixa confirmada pelo humano depois da leitura ambígua."""
    from app.apps.erp.core.pagamentos import service as svc_pag
    p = s.get(Parcela, parcela_id, options=[selectinload(Parcela.titulo)])
    if p is None:
        raise ErroValidacao("Parcela não encontrada.")
    pg = svc_pag.registrar_pagamento(
        s, parcela_id=parcela_id, conta_bancaria_id=conta_bancaria_id,
        data_pagamento=date.fromisoformat(data_pagamento), usuario=usuario)
    anexo = None
    if conteudo:
        anexo = guardar_anexo(s, conteudo, nome_arquivo, entidade_tipo="titulo",
                              entidade_id=p.titulo_id, usuario=usuario)
        if anexo is not None:
            pg.comprovante_anexo_id = anexo.id
    s.flush()
    registrar_evento(s, "titulo", p.titulo_id, "BAIXA_CONFIRMADA_COM_COMPROVANTE",
                     {"numero_sp": p.titulo.numero_sp, "valor": str(pg.valor_pago),
                      "arquivo": nome_arquivo}, usuario.id if usuario else None)
    return {"numero_sp": p.titulo.numero_sp, "valor": float(pg.valor_pago),
            "anexo_id": anexo.id if anexo else None}


def anexos_do_titulo(s: Session, titulo_id: int) -> list[dict[str, Any]]:
    from app.apps.erp.core.documentos.armazenamento import listar
    return listar(s, "titulo", titulo_id)
