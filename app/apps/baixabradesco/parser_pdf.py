# -*- coding: utf-8 -*-
from __future__ import annotations

from io import BytesIO
from typing import List, Tuple

try:
    import pdfplumber
    _PDFPLUMBER_OK = True
except ImportError:
    _PDFPLUMBER_OK = False

try:
    from pypdf import PdfReader, PdfWriter
    _PYPDF_OK = True
except ImportError:
    _PYPDF_OK = False


def extract_pdf_pages(pdf_bytes: bytes) -> List[Tuple[int, str]]:
    """Extrai texto de cada página do PDF.

    Usa pdfplumber como primeira opção (melhor para comprovantes bancários
    com tabelas e layouts complexos). Fallback para pypdf se necessário.

    Retorna lista de (numero_pagina_1based, texto).
    """
    if _PDFPLUMBER_OK:
        return _extract_pdfplumber(pdf_bytes)
    if _PYPDF_OK:
        return _extract_pypdf(pdf_bytes)
    raise RuntimeError(
        'Nenhuma biblioteca de PDF disponível. '
        'Adicione pdfplumber ao requirements.txt.'
    )


def _extract_pdfplumber(pdf_bytes: bytes) -> List[Tuple[int, str]]:
    pages = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            try:
                # Texto nativo com tolerâncias ajustadas para comprovantes
                text = page.extract_text(x_tolerance=2, y_tolerance=2) or ''

                # Se vazio, tenta via tabelas (alguns comprovantes usam tabelas invisíveis)
                if not text.strip():
                    tables = page.extract_tables()
                    if tables:
                        linhas = []
                        for table in tables:
                            for row in table:
                                linha = ' '.join(str(c or '') for c in row if c)
                                if linha.strip():
                                    linhas.append(linha)
                        text = '\n'.join(linhas)
            except Exception:
                text = ''
            pages.append((i, text))
    return pages


def _extract_pypdf(pdf_bytes: bytes) -> List[Tuple[int, str]]:
    from pypdf import PdfReader
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
    """Retorna PDF com apenas a página informada (1-based).
    Usa pypdf para escrita. Se indisponível, retorna o PDF completo.
    """
    if _PYPDF_OK:
        from pypdf import PdfReader, PdfWriter
        reader = PdfReader(BytesIO(pdf_bytes))
        if getattr(reader, 'is_encrypted', False):
            try:
                reader.decrypt('')
            except Exception:
                pass
        idx = max(0, int(page_number) - 1)
        writer = PdfWriter()
        if idx < len(reader.pages):
            writer.add_page(reader.pages[idx])
        out = BytesIO()
        writer.write(out)
        return out.getvalue()
    return pdf_bytes
