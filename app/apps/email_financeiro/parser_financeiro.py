# -*- coding: utf-8 -*-
"""
Parser financeiro: leitura e extração de dados de PDFs, XMLs e OCR.
"""

import re
import tempfile
import pdfplumber
import fitz  # PyMuPDF
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from pdf2image import convert_from_bytes
import pytesseract


def clean_text(txt):
    return re.sub(r"\s+", " ", txt or "").strip()


def extract_text_from_pdf(file_bytes):
    text = ""
    try:
        with pdfplumber.open(BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                text += "\n" + page.extract_text() or ""
    except Exception:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        for page in doc:
            text += page.get_text("text")
    text = clean_text(text)
    if len(text) < 100:
        # fallback OCR
        images = convert_from_bytes(file_bytes)
        for img in images:
            text += " " + pytesseract.image_to_string(img, lang="por")
    return clean_text(text)


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
        regex_venc = [r"Vencimento[: ]*(\d{1,2}/\d{1,2}/\d{4})"]
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

        # tentar fornecedor pelo contexto
        fornecedor = ""
        for label in ["Emitente", "Fornecedor", "Razão Social", "Empresa"]:
            m = re.search(rf"{label}[: ]*(.+?)\s+CNPJ", text, flags=re.IGNORECASE)
            if m:
                fornecedor = clean_text(m.group(1))
                break

        tipo = "NF" if "nf" in name or "nota" in name else "Boleto" if "boleto" in name else "Financeiro"

        result = {
            "Fornecedor": fornecedor,
            "CNPJ": cnpj,
            "Nº NF": nf,
            "Valor (R$)": valor,
            "Vencimento": venc,
            "Código de Barras": cod,
            "Tipo": tipo,
        }

    # limpeza e formatação
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
            venc_dt = datetime.strptime(v, "%d/%m/%Y").date()
            hoje = datetime.now().date()
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
