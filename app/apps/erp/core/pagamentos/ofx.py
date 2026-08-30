# ============================================================================
# BWS ERP — core/pagamentos/ofx.py
# Parser de extrato OFX (Open Financial Exchange) sem dependências externas.
# Suporta o formato SGML clássico dos bancos brasileiros (Bradesco, BB, Itaú,
# Caixa, Santander) e o XML do OFX 2.x. Funções puras.
#
# Saída: lista de LancamentoOFX com hash determinístico por transação —
# o hash usa FITID quando presente (identificador único do banco), o que torna
# a reimportação do mesmo arquivo idempotente (constraint UNIQUE em extratos).
# ============================================================================
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional


@dataclass
class LancamentoOFX:
    data: date
    valor: Decimal                 # negativo = débito (saída)
    tipo: str                      # DEBIT/CREDIT/PAYMENT/...
    memo: str
    nome: Optional[str]
    documento: Optional[str]       # CHECKNUM/REFNUM
    fitid: Optional[str]
    hash_linha: str


class ErroOFX(Exception):
    """Arquivo OFX ilegível ou sem transações."""


_RE_TRN = re.compile(r"<STMTTRN>(.*?)(?:</STMTTRN>|(?=<STMTTRN>)|\Z)",
                     re.DOTALL | re.IGNORECASE)


def _campo(bloco: str, tag: str) -> Optional[str]:
    m = re.search(rf"<{tag}>([^<\r\n]*)", bloco, re.IGNORECASE)
    if not m:
        return None
    v = m.group(1).strip()
    return v or None


def _data_ofx(valor: str) -> date:
    dig = re.sub(r"\D", "", (valor or "")[:14])
    if len(dig) < 8:
        raise ErroOFX(f"Data OFX inválida: {valor!r}")
    return datetime.strptime(dig[:8], "%Y%m%d").date()


def _valor_ofx(valor: str) -> Decimal:
    v = (valor or "").strip().replace(",", ".")
    try:
        return Decimal(v).quantize(Decimal("0.01"))
    except InvalidOperation:
        raise ErroOFX(f"Valor OFX inválido: {valor!r}")


def _decodificar(conteudo: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return conteudo.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return conteudo.decode("latin-1", errors="replace")


def parsear_ofx(conteudo: bytes, conta_bancaria_id: int) -> list[LancamentoOFX]:
    """Extrai as transações de um arquivo OFX. Levanta ErroOFX se nada for
    reconhecido — arquivo errado nunca passa em silêncio."""
    texto = _decodificar(conteudo)
    if "<OFX" not in texto.upper():
        raise ErroOFX("Arquivo não parece ser OFX (tag <OFX> ausente).")

    lancamentos: list[LancamentoOFX] = []
    vistos: set[str] = set()
    for m in _RE_TRN.finditer(texto):
        bloco = m.group(1)
        dt_raw = _campo(bloco, "DTPOSTED")
        val_raw = _campo(bloco, "TRNAMT")
        if not dt_raw or not val_raw:
            continue
        data = _data_ofx(dt_raw)
        valor = _valor_ofx(val_raw)
        fitid = _campo(bloco, "FITID")
        memo = _campo(bloco, "MEMO") or ""
        nome = _campo(bloco, "NAME") or _campo(bloco, "PAYEE")
        doc = _campo(bloco, "CHECKNUM") or _campo(bloco, "REFNUM")
        tipo = (_campo(bloco, "TRNTYPE") or "").upper() or ("DEBIT" if valor < 0 else "CREDIT")

        base = fitid if fitid else f"{data.isoformat()}|{valor}|{memo}|{doc or ''}"
        h = hashlib.sha256(f"cta{conta_bancaria_id}|{base}".encode("utf-8")).hexdigest()
        if h in vistos:            # FITID repetido dentro do mesmo arquivo
            continue
        vistos.add(h)
        lancamentos.append(LancamentoOFX(
            data=data, valor=valor, tipo=tipo, memo=memo.strip(),
            nome=(nome or "").strip() or None, documento=doc, fitid=fitid,
            hash_linha=h))

    if not lancamentos:
        raise ErroOFX("Nenhuma transação (<STMTTRN>) encontrada no arquivo.")
    return lancamentos


_RE_NOME_MEMO = re.compile(
    r"(?:PIX(?:\s+(?:DES|ENV|QRS|TRANSF))?|TED|DOC|TRANSF(?:ERENCIA)?)"
    r"[\s:_-]*(?:PARA|P/|FAVORECIDO)?[\s:_-]*([A-ZÀ-Ú][A-ZÀ-Ú0-9 .&-]{4,})",
    re.IGNORECASE)


def extrair_nome_contraparte(lanc: LancamentoOFX) -> Optional[str]:
    """Melhor esforço para obter o nome do favorecido: campo NAME quando o
    banco preenche; senão, heurística sobre o MEMO (padrão dos extratos
    Bradesco: 'PIX DES: FULANO DE TAL')."""
    if lanc.nome and len(lanc.nome) >= 5:
        return lanc.nome.upper().strip()
    m = _RE_NOME_MEMO.search(lanc.memo or "")
    if m:
        cand = m.group(1).strip().upper()
        cand = re.sub(r"\s{2,}", " ", cand)
        if len(cand) >= 5:
            return cand
    return None
