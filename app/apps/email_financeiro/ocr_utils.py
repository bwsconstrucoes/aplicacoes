# -*- coding: utf-8 -*-
"""
Ferramentas de OCR:
- pdf → texto via PyMuPDF (se tiver), senão via pdf2image + pytesseract
- tolerante: se OCR não estiver instalado, retorna texto vazio e não quebra
"""

import io

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    from pdf2image import convert_from_bytes
    import pytesseract
except Exception:
    convert_from_bytes = None
    pytesseract = None


def extract_text_with_ocr(pdf_bytes: bytes, filename: str = "") -> str:
    # 1) tenta extrair via PyMuPDF text do próprio PDF (às vezes resolve)
    if fitz:
        try:
            out = []
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                for page in doc:
                    out.append(page.get_text())
            text = "\n".join(out).strip()
            if text:
                return text
        except Exception:
            pass

    # 2) se não deu, tenta OCR real (imagens) se disponível
    if convert_from_bytes and pytesseract:
        try:
            imgs = convert_from_bytes(pdf_bytes, dpi=300, fmt="png")
            chunks = []
            for im in imgs:
                txt = pytesseract.image_to_string(im)
                if txt:
                    chunks.append(txt)
            return "\n".join(chunks).strip()
        except Exception:
            pass

    # 3) sem OCR
    return ""
