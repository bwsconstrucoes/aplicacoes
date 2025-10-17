# -*- coding: utf-8 -*-
"""
Parser financeiro: leitura e extração de dados de PDFs, XMLs e OCR opcional.
"""

import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO

# --- pdfplumber (texto estruturado) ---
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except Exception:
    pdfplumber = None
    HAS_PDFPLUMBER = False

# --- PyMuPDF fallback ---
import fitz  # PyMuPDF

# --- OCR (opcional) ---
OCR_ENABLED = os.getenv("OCR_ENABLED", "FALSE").strip().upper() == "TRUE"
try:
    from pdf2image import convert_from_bytes
    import pytesseract
    HAS_OCR = True
except Exception:
    HAS_OCR = False

def clean_text(txt):
    return re.sub(r"\s+", " ", txt or "").strip()

def extract_text_from_pdf(file_bytes):
    text = ""
    # 1) Tenta pdfplumber (melhor para texto)
    if HAS_PDFPLUMBER:
        try:
            with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                for page in pdf.pages:
                    t = page.extract_text() or ""
                    if t:
                        text += "\n" + t
        except Exception:
            pass

    # 2) Fallback com PyMuPDF
    if not text:
        try:
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            for page in doc:
                text += page.get_text("text")
        except Exception:
            pass

    text = clean_text(text)

    # 3) OCR (só se habilitado e libs presentes)
    if OCR_ENABLED and HAS_OCR and len(text) < 80:
        try:
            images = convert_from_bytes(file_bytes)
            for img in images:
                text += " " + pytesseract.image_to_string(img, lang="por")
            text = clean_text(text)
        except Exception:
            pass

    return text

def extract_from_xml(file_bytes):
    try:
        xml_root = ET.fromstring(file_bytes.decode("utf-8"))
        ns = {"ns": "http://www.portalfiscal.inf.br/nfe"}
        cnpj = xml_root.find(".//ns:emit/ns:CNPJ", ns)
        emit = xml_root.find(".//ns:emit/ns:xNome", ns)
        valor = xml_root.find(".//ns:ICMSTot/ns:vNF", ns)
        nf = xml_root.find(".//ns:ide/ns:nNF", ns)
        data = xml_root.find(".//ns:dup/ns:dVenc", ns)
        return {
            "Fornecedor": emit.text if emit is not None else "",
            "CNPJ": cnpj.text if cnpj is not None else "",
            "Nº NF": nf.text if nf is not None else "",
            "Valor (R$)": valor.text if valor is not None else "",
            "Vencimento": data.text if data is not None else "",
            "Tipo": "NF",
        }
    except Exception:
        return {}

def extract_financial_data_from_attachment(filename, file_bytes):
    name = filename.lower()
    result = {}

    if name.endswith(".xml"):
        result = extract_from_xml(file_bytes)
    else:
        text = extract_text_from_pdf(file_bytes)
        regex_valor = [
            r"Valor\s+Total[: ]*R\$ ?(\d{1,3}(?:\.\d{3})*,\d{2})",
            r"R\$ ?(\d{1,3}(?:\.\d{3})*,\d{2})\s*(Total|NF|Nota)",
        ]
        regex_nf = [r"NF[\sº:]*([0-9]{2,})", r"Nota\s+Fiscal[:\s]*([0-9]+)"]
        regex_venc = [r"Vencimento[: ]*(\d{1,2}/\d{1,2}/\d{4})", r"Data\s+de\s+Vencimento[: ]*(\d{1,2}/\d{1,2}/\d{4})"]
        regex_cnpj = [r"CNPJ[: ]*([\d./-]{14,18})"]
        regex_cod_barras = [r"(\d{47}|\d{48})"]

        def find_first(patterns):
            for r in patterns:
                m = re.search(r, text, flags=re.IGNORECASE)
                if m:
                    return clean_text(m.group(1))
            return ""

        valor = find_first(regex_valor)
        nf = find_first(regex_nf)
        venc = find_first(regex_venc)
        cnpj = find_first(regex_cnpj)
        cod = find_first(regex_cod_barras)

        fornecedor = ""
        for label in ["Emitente", "Fornecedor", "Razão Social", "Empresa"]:
            m = re.search(rf"{label}[: ]*(.+?)\s+CNPJ", text, flags=re.IGNORECASE)
            if m:
                fornecedor = clean_text(m.group(1))
                break

        tipo = "NF" if ("nf" in name or "nota" in name) else ("Boleto" if "boleto" in name else "Financeiro")

        result = {
            "Fornecedor": fornecedor,
            "CNPJ": cnpj,
            "Nº NF": nf,
            "Valor (R$)": valor,
            "Vencimento": venc,
            "Código de Barras": cod,
            "Tipo": tipo,
        }

    # valor numérico
    valornum = 0.0
    try:
        valornum = float(result.get("Valor (R$)", "").replace(".", "").replace(",", "."))
    except Exception:
        pass
    result["ValorNum"] = valornum

    # status
    status = ""
    try:
        v = result.get("Vencimento", "")
        if re.match(r"\d{2}/\d{2}/\d{4}", v):
            from datetime import datetime as _dt
            venc_dt = _dt.strptime(v, "%d/%m/%Y").date()
            hoje = _dt.now().date()
            if venc_dt < hoje:
                status = "🔴 Atrasado"
            elif venc_dt == hoje:
                status = "🟡 Hoje"
            else:
                status = "🟢 A vencer"
    except Exception:
        pass
    result["Status"] = status
    return result
