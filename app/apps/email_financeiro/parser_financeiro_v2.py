# -*- coding: utf-8 -*-
"""
Parser rule-based melhorado (Fase 1):
- PDF: tenta extrair texto com PyMuPDF / pdfplumber; busca linha digitável/código barras (47/48 dígitos)
- XML NF-e: parser robusto com lxml + namespaces (emitente = fornecedor)
- Retorna dict padronizado + ValorNum (float) para somatórios
"""

import io
import re
from typing import Dict, Any
from decimal import Decimal, InvalidOperation

# PDF
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None

# XML
try:
    from lxml import etree
except Exception:
    etree = None


# --------- util ---------

def _to_float_brl(s: str) -> float:
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        # tenta capturar números "1234.56"
        try:
            return float(s)
        except Exception:
            return 0.0

def _extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    # tenta PyMuPDF primeiro (rápido e bom)
    if fitz:
        try:
            text_all = []
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                for page in doc:
                    text_all.append(page.get_text())
            return "\n".join(text_all)
        except Exception:
            pass
    # fallback: pdfplumber
    if pdfplumber:
        try:
            text_all = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text_all.append(page.extract_text() or "")
            return "\n".join(text_all)
        except Exception:
            pass
    return ""

def _pick_first(groups):
    for g in groups:
        if g:
            return g
    return ""


# --------- PDF (boleto, DANFE simples) ---------

_REGEX_LINHA_DIGITAVEL = re.compile(r"(\d[\s\.\-]?){47,48}")
_REGEX_COD_BARRAS_44 = re.compile(r"\b(\d{44})\b")
_REGEX_VALOR = re.compile(r"(?:Valor\s*(?:do\s*Título|Total)?\s*[:\-]?\s*)(\d{1,3}(\.\d{3})*,\d{2}|\d+\.\d{2})", re.IGNORECASE)
_REGEX_VENC = re.compile(r"(Vencimento|Vcto\.?|Venc\.)\s*[:\-]?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})", re.IGNORECASE)
_REGEX_NF_NUM = re.compile(r"(?:N[ºo]\s*|No\.\s*|Número\s*da\s*Nota\s*[:\-]?\s*)(\d{3,10})", re.IGNORECASE)
_REGEX_SERIE = re.compile(r"(?:S[eé]rie\s*[:\-]?\s*)(\w{1,5})", re.IGNORECASE)
_REGEX_CNPJ = re.compile(r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b")
_REGEX_FORN = re.compile(r"(?:Favorecido|Beneficiário|Cedente|Fornecedor)[:\-]?\s*(.+)", re.IGNORECASE)

def _parse_pdf_financial(filename: str, b: bytes) -> Dict[str, Any]:
    text = _extract_text_from_pdf_bytes(b)

    linha_dig = ""
    m = _REGEX_LINHA_DIGITAVEL.search(text.replace(" ", ""))
    if m:
        # normaliza removendo pontuação/espaços
        linha_dig = re.sub(r"[^\d]", "", m.group(0))
    if not linha_dig:
        m2 = _REGEX_COD_BARRAS_44.search(text)
        if m2:
            linha_dig = m2.group(1)

    valor = ""
    m = _REGEX_VALOR.search(text)
    if m:
        valor = m.group(1)

    venc = ""
    m = _REGEX_VENC.search(text)
    if m:
        venc = m.group(2)

    nf_num = ""
    m = _REGEX_NF_NUM.search(text)
    if m:
        nf_num = m.group(1)

    serie = ""
    m = _REGEX_SERIE.search(text)
    if m:
        serie = m.group(1)

    cnpjs = list(set([re.sub(r"[^\d]", "", x[0]) for x in _REGEX_CNPJ.findall(text)]))
    cnpj_fornecedor = cnpjs[0] if cnpjs else ""

    fornecedor = ""
    for line in text.splitlines():
        fm = _REGEX_FORN.search(line)
        if fm:
            fornecedor = fm.group(1).strip()
            break

    valor_num = _to_float_brl(valor)

    return {
        "Fornecedor": fornecedor,
        "CNPJ": cnpj_fornecedor,
        "Nº NF": nf_num,
        "Série": serie,
        "Valor (R$)": valor,
        "ValorNum": valor_num,
        "Vencimento": venc,
        "Código de Barras": linha_dig,
        "Tipo": "PDF",
        "Status": "OK" if (valor or linha_dig or nf_num) else "INCOMPLETO",
        # opcionais
        "Banco": "",
        "Linha Digitável": linha_dig,
        "Chave de Acesso": "",
        "Data Emissão": "",
    }


# --------- XML NF-e ---------

def _get_xml_root(b: bytes):
    if etree is None:
        return None, None
    try:
        parser = etree.XMLParser(remove_blank_text=True, recover=True)
        root = etree.fromstring(b, parser=parser)
        ns = root.nsmap.copy()
        # normaliza namespace padrão para 'nfe'
        if None in ns:
            ns['nfe'] = ns.pop(None)
        return root, ns
    except Exception:
        return None, None

def _text(x):
    return (x.text or "").strip() if x is not None else ""

def _find(root, path, ns):
    try:
        return root.find(path, namespaces=ns)
    except Exception:
        return None

def _findall(root, path, ns):
    try:
        return root.findall(path, namespaces=ns)
    except Exception:
        return []

def _parse_xml_nfe(filename: str, b: bytes) -> Dict[str, Any]:
    root, ns = _get_xml_root(b)
    if root is None:
        return {"Tipo": "XML", "Status": "ERRO_XML", "Fornecedor": "", "CNPJ": "", "Nº NF": "", "Série": "", "Valor (R$)": "", "ValorNum": 0.0, "Vencimento": "", "Código de Barras": ""}

    # Padrões (versões variadas)
    ide = _find(root, ".//nfe:ide", ns)
    emit = _find(root, ".//nfe:emit", ns)  # fornecedor
    dest = _find(root, ".//nfe:dest", ns)  # comprador (nós)
    total = _find(root, ".//nfe:total/nfe:ICMSTot", ns)
    cobr = _find(root, ".//nfe:cobr", ns)
    dup_list = _findall(root, ".//nfe:cobr/nfe:dup", ns)

    nr_nf = _text(_find(ide, "nfe:nNF", ns))
    serie = _text(_find(ide, "nfe:serie", ns))
    dhEmi = _text(_find(ide, "nfe:dhEmi", ns)) or _text(_find(ide, "nfe:dEmi", ns))
    chave = _text(_find(root, ".//nfe:infNFe", ns))
    if chave:
        chave = (root.find(".//{*}infNFe").attrib.get("Id", "")).replace("NFe", "")

    # Emitente (fornecedor)
    xNome_emit = _text(_find(emit, "nfe:xNome", ns))
    cnpj_emit = _text(_find(emit, "nfe:CNPJ", ns)) or _text(_find(emit, "nfe:CPF", ns))

    # Total
    vNF = _text(_find(total, "nfe:vNF", ns))
    valor_num = _to_float_brl(vNF)

    # Boletos/duplicatas (se presentes)
    venc = ""
    if dup_list:
        # pega o mais próximo/primeiro
        venc = _text(_find(dup_list[0], "nfe:dVenc", ns))

    # fallback vencimento via cobr/dup ou cobr/fat
    if not venc:
        venc = _text(_find(cobr, "nfe:dup/nfe:dVenc", ns)) or _text(_find(cobr, "nfe:fat/nfe:dVenc", ns))

    return {
        "Fornecedor": xNome_emit,
        "CNPJ": re.sub(r"[^\d]", "", cnpj_emit),
        "Nº NF": nr_nf,
        "Série": serie,
        "Valor (R$)": vNF,
        "ValorNum": valor_num,
        "Vencimento": venc,
        "Código de Barras": "",  # XML não tem; fica para boleto/ DANFE se houver
        "Tipo": "XML",
        "Status": "OK" if (xNome_emit and nr_nf and vNF) else "INCOMPLETO",
        "Chave de Acesso": chave,
        "Data Emissão": dhEmi,
        "Banco": "",
        "Linha Digitável": "",
    }


# --------- entrada única ---------

def extract_financial_data_v2(filename: str, b: bytes) -> Dict[str, Any]:
    name = (filename or "").lower()
    if name.endswith(".xml"):
        return _parse_xml_nfe(filename, b)
    else:
        return _parse_pdf_financial(filename, b)
