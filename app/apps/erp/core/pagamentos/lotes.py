# ============================================================================
# ERP — core/pagamentos/lotes.py
# Lotes de pagamento: organizam as parcelas aprovadas por prioridade e por quem
# solicitou, como o "Lote" da Análise de SPs — porém como tabela, com histórico
# e sem a coluna de texto na planilha.
#
# Inclui a montagem do Pix copia-e-cola (BR Code, padrão EMV do Bacen) para o
# financeiro pagar lendo a tela pelo celular, sem redigitar chave e valor.
# ============================================================================
from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.cadastros.validadores import cnpj_valido, cpf_valido, somente_digitos
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import FormaPagamento, StatusConta, Usuario
from app.apps.erp.db.models.financeiro import (
    Lote, LoteItem, Parcela, StatusParcela, StatusTitulo, Titulo,
)

PRIORIDADES = {1: "Urgente", 2: "Alta", 3: "Normal", 4: "Baixa", 5: "Quando der"}


# ---------------------------------------------------------------------------
# Pix copia-e-cola (BR Code)
# ---------------------------------------------------------------------------
def _tlv(campo: str, valor: str) -> str:
    return f"{campo}{len(valor):02d}{valor}"


def _crc16(payload: str) -> str:
    crc = 0xFFFF
    for ch in payload.encode("utf-8"):
        crc ^= ch << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return f"{crc:04X}"


def _limpar_texto(txt: str, limite: int) -> str:
    t = unicodedata.normalize("NFKD", (txt or "").strip())
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = re.sub(r"[^A-Za-z0-9 .-]", "", t).strip().upper()
    return t[:limite] or "PAGAMENTO"


def montar_pix(chave: str, valor: Decimal, nome_recebedor: str,
               cidade: str = "FORTALEZA", identificador: str = "") -> str:
    """BR Code estático com valor. É o que gera o copia-e-cola e o QR."""
    chave = (chave or "").strip()
    if not chave:
        raise ErroValidacao("Credor sem chave Pix homologada.")
    dig = somente_digitos(chave)
    if len(dig) in (11, 14) and (cpf_valido(dig) or cnpj_valido(dig)):
        chave = dig                      # CPF/CNPJ vão só com dígitos
    elif len(dig) in (12, 13) and dig.startswith("55"):
        chave = "+" + dig                # telefone com país
    elif len(dig) in (10, 11) and "@" not in chave:
        chave = "+55" + dig

    mai = _tlv("00", "br.gov.bcb.pix") + _tlv("01", chave)
    payload = (
        _tlv("00", "01") +
        _tlv("26", mai) +
        _tlv("52", "0000") +
        _tlv("53", "986") +
        _tlv("54", f"{Decimal(valor):.2f}") +
        _tlv("58", "BR") +
        _tlv("59", _limpar_texto(nome_recebedor, 25)) +
        _tlv("60", _limpar_texto(cidade, 15)) +
        _tlv("62", _tlv("05", _limpar_texto(identificador or "***", 25)))
    )
    return payload + "6304" + _crc16(payload + "6304")


def dados_pagamento(s: Session, parcela_id: int) -> dict[str, Any]:
    """Tudo que o financeiro precisa para pagar a parcela: boleto ou Pix
    copia-e-cola pronto, mais a conferência do credor."""
    p = s.get(Parcela, parcela_id, options=[
        selectinload(Parcela.titulo).selectinload(Titulo.fornecedor)])
    if p is None:
        raise ErroValidacao("Parcela não encontrada.")
    t = p.titulo
    dados: dict[str, Any] = {
        "parcela_id": p.id, "numero_sp": t.numero_sp, "parcela": p.numero,
        "credor": t.fornecedor.razao_social, "documento_credor": t.fornecedor.cnpj_cpf,
        "valor": float(p.valor), "vencimento": p.vencimento.isoformat(),
        "forma": t.forma_pagamento.value, "descricao": t.descricao,
        "boleto": p.linha_digitavel, "pix": None, "aviso": None,
    }
    if t.forma_pagamento in (FormaPagamento.PIX, FormaPagamento.TED):
        conta = next((c for c in t.fornecedor.contas
                      if c.id == t.fornecedor_conta_id), None)
        if conta is None or conta.status != StatusConta.HOMOLOGADA:
            dados["aviso"] = "Conta do credor não está homologada — não pague."
        elif conta.forma == FormaPagamento.PIX:
            try:
                dados["pix"] = {
                    "chave": conta.pix_chave, "tipo": conta.pix_tipo,
                    "titular": conta.titular_nome or t.fornecedor.razao_social,
                    "copia_e_cola": montar_pix(
                        conta.pix_chave, Decimal(p.valor),
                        conta.titular_nome or t.fornecedor.razao_social,
                        identificador=t.numero_sp),
                }
            except ErroValidacao as e:
                dados["aviso"] = str(e)
        else:
            dados["transferencia"] = {
                "banco": conta.banco_codigo, "agencia": conta.agencia,
                "conta": f"{conta.conta}-{conta.conta_digito or ''}".rstrip("-"),
                "titular": conta.titular_nome or t.fornecedor.razao_social,
                "documento": conta.titular_doc or t.fornecedor.cnpj_cpf,
            }
    return dados


# ---------------------------------------------------------------------------
# Lotes
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> Lote:
    nome = (dados.get("nome") or "").strip()
    if len(nome) < 3:
        raise ErroValidacao("Dê um nome ao lote (ex.: 'Pagamentos 05/09 — Bradesco').")
    prioridade = int(dados.get("prioridade") or 3)
    if prioridade not in PRIORIDADES:
        raise ErroValidacao("Prioridade deve ser de 1 (urgente) a 5.")
    lote = Lote(nome=nome, descricao=(dados.get("descricao") or "").strip() or None,
                prioridade=prioridade,
                conta_bancaria_id=dados.get("conta_bancaria_id") or None,
                data_prevista=(date.fromisoformat(dados["data_prevista"])
                               if dados.get("data_prevista") else None),
                criado_por=usuario.id)
    s.add(lote)
    s.flush()
    registrar_evento(s, "lote", lote.id, "CRIADO", {"nome": nome, "prioridade": prioridade},
                     usuario.id)
    return lote


def adicionar_parcelas(s: Session, lote_id: int, parcela_ids: list[int],
                       usuario: Usuario) -> dict[str, Any]:
    lote = s.get(Lote, lote_id)
    if lote is None:
        raise ErroValidacao("Lote não encontrado.")
    if lote.status not in ("ABERTO",):
        raise ErroValidacao(f"Lote {lote.nome} está {lote.status} — abra outro lote.")
    incluidas, recusadas = [], []
    ordem = s.scalar(select(func.count()).select_from(LoteItem)
                     .where(LoteItem.lote_id == lote_id)) or 0
    for pid in parcela_ids:
        p = s.get(Parcela, int(pid), options=[selectinload(Parcela.titulo)])
        if p is None:
            recusadas.append({"parcela_id": pid, "motivo": "parcela inexistente"})
            continue
        if p.status == StatusParcela.PAGA:
            recusadas.append({"parcela_id": pid, "motivo": f"{p.titulo.numero_sp} já está paga"})
            continue
        if p.titulo.status not in (StatusTitulo.APROVADO, StatusTitulo.PAGO_PARCIAL):
            recusadas.append({"parcela_id": pid,
                              "motivo": f"{p.titulo.numero_sp} está {p.titulo.status.value} — "
                                        f"só entra em lote o que foi liberado"})
            continue
        ja = s.scalars(select(LoteItem).where(LoteItem.parcela_id == p.id)).first()
        if ja is not None:
            recusadas.append({"parcela_id": pid,
                              "motivo": f"{p.titulo.numero_sp} já está no lote #{ja.lote_id}"})
            continue
        ordem += 1
        s.add(LoteItem(lote_id=lote.id, parcela_id=p.id, ordem=ordem))
        incluidas.append(p.titulo.numero_sp)
    s.flush()
    registrar_evento(s, "lote", lote.id, "PARCELAS_ADICIONADAS",
                     {"incluidas": incluidas, "recusadas": len(recusadas)}, usuario.id)
    return {"incluidas": incluidas, "recusadas": recusadas}


def remover_parcela(s: Session, lote_id: int, parcela_id: int, usuario: Usuario) -> None:
    item = s.scalars(select(LoteItem).where(
        LoteItem.lote_id == lote_id, LoteItem.parcela_id == parcela_id)).first()
    if item is None:
        raise ErroValidacao("Parcela não está neste lote.")
    s.delete(item)
    registrar_evento(s, "lote", lote_id, "PARCELA_REMOVIDA", {"parcela_id": parcela_id},
                     usuario.id)


def detalhar(s: Session, lote_id: int) -> dict[str, Any]:
    lote = s.get(Lote, lote_id)
    if lote is None:
        raise ErroValidacao("Lote não encontrado.")
    # consulta direta: relationship em cache não enxerga item recém-adicionado
    linhas = s.scalars(select(LoteItem).where(LoteItem.lote_id == lote_id)
                       .options(selectinload(LoteItem.parcela).selectinload(Parcela.titulo)
                                .selectinload(Titulo.fornecedor))
                       .order_by(LoteItem.ordem)).all()
    itens = []
    for it in linhas:
        p, t = it.parcela, it.parcela.titulo
        itens.append({
            "parcela_id": p.id, "titulo_id": t.id, "numero_sp": t.numero_sp,
            "credor": t.fornecedor.razao_social, "descricao": t.descricao,
            "valor": float(p.valor), "vencimento": p.vencimento.isoformat(),
            "forma": t.forma_pagamento.value, "status_parcela": p.status.value,
            "boleto": p.linha_digitavel, "ordem": it.ordem,
        })
    total = sum(i["valor"] for i in itens)
    pagas = [i for i in itens if i["status_parcela"] == "PAGA"]
    return {
        "id": lote.id, "nome": lote.nome, "descricao": lote.descricao,
        "prioridade": lote.prioridade, "prioridade_rotulo": PRIORIDADES[lote.prioridade],
        "status": lote.status, "conta_bancaria_id": lote.conta_bancaria_id,
        "data_prevista": lote.data_prevista.isoformat() if lote.data_prevista else None,
        "criado_em": lote.criado_em.strftime("%d/%m/%Y %H:%M"),
        "itens": itens, "total": round(total, 2),
        "pagas": len(pagas), "total_pago": round(sum(i["valor"] for i in pagas), 2),
    }


def listar(s: Session, apenas_abertos: bool = False) -> list[dict[str, Any]]:
    stmt = select(Lote)
    if apenas_abertos:
        stmt = stmt.where(Lote.status == "ABERTO")
    lotes = s.scalars(stmt.order_by(Lote.prioridade, Lote.id.desc())).all()
    todos_itens = s.scalars(select(LoteItem).options(
        selectinload(LoteItem.parcela)).where(
        LoteItem.lote_id.in_([l.id for l in lotes] or [0]))).all()
    por_lote: dict[int, list[LoteItem]] = {}
    for it in todos_itens:
        por_lote.setdefault(it.lote_id, []).append(it)
    saida = []
    for l in lotes:
        itens_l = por_lote.get(l.id, [])
        valores = [float(i.parcela.valor) for i in itens_l]
        pagas = [i for i in itens_l if i.parcela.status == StatusParcela.PAGA]
        saida.append({
            "id": l.id, "nome": l.nome, "prioridade": l.prioridade,
            "prioridade_rotulo": PRIORIDADES[l.prioridade], "status": l.status,
            "itens": len(itens_l), "total": round(sum(valores), 2),
            "pagas": len(pagas),
            "total_pago": round(sum(float(i.parcela.valor) for i in pagas), 2),
            "data_prevista": l.data_prevista.isoformat() if l.data_prevista else None,
            "criado_em": l.criado_em.strftime("%d/%m/%Y"),
        })
    return saida


def mudar_status(s: Session, lote_id: int, novo: str, usuario: Usuario) -> Lote:
    lote = s.get(Lote, lote_id)
    if lote is None:
        raise ErroValidacao("Lote não encontrado.")
    if novo not in ("ABERTO", "ENVIADO", "PAGO", "CANCELADO"):
        raise ErroValidacao(f"Status inválido: {novo}")
    lote.status = novo
    if novo in ("PAGO", "CANCELADO"):
        lote.fechado_em = datetime.now(timezone.utc)
    registrar_evento(s, "lote", lote.id, f"STATUS_{novo}", {"nome": lote.nome}, usuario.id)
    return lote


def extrair_ids_sp(texto: str) -> list[str]:
    """Extrai números de SP colados de qualquer jeito (a mensagem que volta
    pelo WhatsApp, uma lista, uma tabela). Aceita 'SP000123' e '123'."""
    achados = re.findall(r"\bSP\s?0*(\d{1,6})\b", texto or "", re.IGNORECASE)
    achados += re.findall(r"\b(\d{4,6})\b", texto or "")
    vistos, saida = set(), []
    for a in achados:
        n = a.lstrip("0") or "0"
        if n not in vistos:
            vistos.add(n)
            saida.append(f"SP{int(n):06d}")
    return saida


def parcelas_por_sp(s: Session, numeros_sp: list[str]) -> dict[str, Any]:
    """Dado um punhado de números de SP, devolve as parcelas em aberto —
    é como o lote nasce a partir da mensagem que o solicitante devolve."""
    encontrados, nao_encontrados = [], []
    for numero in numeros_sp:
        t = s.scalars(select(Titulo).where(Titulo.numero_sp == numero)
                      .options(selectinload(Titulo.parcelas),
                               selectinload(Titulo.fornecedor))).first()
        if t is None:
            nao_encontrados.append(numero)
            continue
        for p in t.parcelas:
            if p.status in (StatusParcela.ABERTA, StatusParcela.AGENDADA):
                encontrados.append({
                    "parcela_id": p.id, "numero_sp": t.numero_sp,
                    "credor": t.fornecedor.razao_social, "valor": float(p.valor),
                    "vencimento": p.vencimento.isoformat(),
                    "status_titulo": t.status.value,
                    "elegivel": t.status in (StatusTitulo.APROVADO, StatusTitulo.PAGO_PARCIAL),
                })
    return {"parcelas": encontrados, "nao_encontrados": nao_encontrados}
