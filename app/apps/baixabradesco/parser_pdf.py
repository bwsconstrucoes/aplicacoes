# -*- coding: utf-8 -*-
from __future__ import annotations

from io import BytesIO
from typing import List, Tuple

try:
    from pypdf import PdfReader, PdfWriter
except Exception:  # pragma: no cover
    PdfReader = None
    PdfWriter = None


def extract_pdf_pages(pdf_bytes: bytes) -> List[Tuple[int, str]]:
    """Retorna [(pagina_1based, texto)]. Não usa OCR nesta versão inicial."""
    if PdfReader is None:
        raise RuntimeError('Dependência pypdf não instalada. Adicione pypdf ao requirements.txt.')
    reader = PdfReader(BytesIO(pdf_bytes))
    if getattr(reader, 'is_encrypted', False):
        try:
            reader.decrypt('')
        except Exception:
            pass
    pages = []
    for i, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ''
        except Exception:
            text = ''
        pages.append((i, text))
    return pages


def extract_single_page_pdf(pdf_bytes: bytes, page_number: int) -> bytes:
    """Retorna um PDF contendo apenas a página informada (1-based)."""
    if PdfReader is None or PdfWriter is None:
        raise RuntimeError('Dependências pypdf/PdfWriter não instaladas.')
    reader = PdfReader(BytesIO(pdf_bytes))
    if getattr(reader, 'is_encrypted', False):
        try:
            reader.decrypt('')
        except Exception:
            pass
    idx = max(0, int(page_number) - 1)
    writer = PdfWriter()
    writer.add_page(reader.pages[idx])
    out = BytesIO()
    writer.write(out)
    return out.getvalue()
