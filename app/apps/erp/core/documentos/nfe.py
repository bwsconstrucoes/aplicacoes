# ============================================================================
# BWS ERP — core/documentos/nfe.py
# Leitura de XML de NFe (modelo 55) para o lançamento iniciado pelo documento.
# Parser puro (xml.etree, sem dependências); extrai o que pré-preenche o
# lançamento: chave, número/série, emissão, emitente, destinatário, valor
# total e DUPLICATAS (parcelas com vencimento e valor).
# ============================================================================
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

_NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}


class ErroNFe(Exception):
    """XML ilegível ou que não é uma NFe."""


@dataclass
class DuplicataNFe:
    numero: str
    vencimento: Optional[date]
    valor: Decimal


@dataclass
class DadosNFe:
    chave: str
    numero: str
    serie: str
    emissao: Optional[date]
    emitente_cnpj: str
    emitente_nome: str
    destinatario_doc: Optional[str]
    valor_total: Decimal
    duplicatas: list[DuplicataNFe] = field(default_factory=list)
    natureza_operacao: Optional[str] = None


def _texto(no, caminho: str) -> Optional[str]:
    achado = no.find(caminho, _NS)
    return achado.text.strip() if achado is not None and achado.text else None


def _data(v: Optional[str]) -> Optional[date]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(v[:10]).date()
    except ValueError:
        return None


def parsear_nfe(conteudo: bytes) -> DadosNFe:
    try:
        raiz = ET.fromstring(conteudo)
    except ET.ParseError as e:
        raise ErroNFe(f"XML mal-formado: {e}")

    inf = raiz.find(".//nfe:infNFe", _NS)
    if inf is None:
        raise ErroNFe("Não é um XML de NFe (tag infNFe ausente). "
                      "Envie o XML da nota, não o DANFE em PDF.")
    chave = re.sub(r"\D", "", inf.get("Id") or "")
    if len(chave) != 44:
        raise ErroNFe(f"Chave de acesso inválida no XML ({len(chave)} dígitos).")

    emit = inf.find("nfe:emit", _NS)
    if emit is None:
        raise ErroNFe("Emitente ausente no XML.")
    total = _texto(inf, "nfe:total/nfe:ICMSTot/nfe:vNF")
    if not total:
        raise ErroNFe("Valor total (vNF) ausente no XML.")

    duplicatas = []
    for dup in inf.findall("nfe:cobr/nfe:dup", _NS):
        duplicatas.append(DuplicataNFe(
            numero=_texto(dup, "nfe:nDup") or str(len(duplicatas) + 1),
            vencimento=_data(_texto(dup, "nfe:dVenc")),
            valor=Decimal(_texto(dup, "nfe:vDup") or "0"),
        ))

    dest = inf.find("nfe:dest", _NS)
    dest_doc = None
    if dest is not None:
        dest_doc = _texto(dest, "nfe:CNPJ") or _texto(dest, "nfe:CPF")

    return DadosNFe(
        chave=chave,
        numero=_texto(inf, "nfe:ide/nfe:nNF") or "",
        serie=_texto(inf, "nfe:ide/nfe:serie") or "",
        emissao=_data(_texto(inf, "nfe:ide/nfe:dhEmi") or _texto(inf, "nfe:ide/nfe:dEmi")),
        emitente_cnpj=re.sub(r"\D", "", _texto(emit, "nfe:CNPJ") or ""),
        emitente_nome=(_texto(emit, "nfe:xNome") or "").upper(),
        destinatario_doc=dest_doc,
        valor_total=Decimal(total),
        duplicatas=duplicatas,
        natureza_operacao=_texto(inf, "nfe:ide/nfe:natOp"),
    )
