# -*- coding: utf-8 -*-
"""
chatbot/paystub.py
Extração do contracheque individual do PDF geral.
Porta do paystub_parser.py do script anexado.

O PDF de competência contém múltiplos contracheques por página (2 por página).
Este módulo divide e identifica o contracheque do colaborador por CPF.
"""

import re
import io
import logging
import unicodedata

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

CPF_RE  = re.compile(r'(\d{3}\.?\d{3}\.?\d{3}-?\d{2})')
NOME_RE = re.compile(r'EMPREGADO\s+(\d{1,6})\s+([A-ZÀ-Ú0-9\s\'.-]+)')


def _normalizar_cpf(cpf: str) -> str:
    return re.sub(r'\D', '', str(cpf or ''))


def _strip_accents(text: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )


def _dividir_em_metades(pdf_bytes: bytes) -> list[dict]:
    """
    Divide cada página ao meio (2 contracheques por página)
    e extrai o texto de cada metade.
    """
    src = fitz.open(stream=pdf_bytes, filetype='pdf')
    resultados = []

    for page_idx in range(src.page_count):
        page = src[page_idx]
        rect = page.rect
        meio = rect.height / 2

        for parte, clip in enumerate([
            fitz.Rect(rect.x0, rect.y0, rect.x1, rect.y0 + meio),
            fitz.Rect(rect.x0, rect.y0 + meio, rect.x1, rect.y1),
        ]):
            texto = page.get_text('text', clip=clip) or ''
            resultados.append({
                'page_idx':  page_idx,
                'parte':     parte,
                'texto':     texto,
                'clip':      clip,
            })

    src.close()
    return resultados


def _extrair_cpf_do_texto(texto: str) -> str | None:
    m = CPF_RE.search(texto)
    if m:
        return _normalizar_cpf(m.group(1))
    return None


def extrair_contracheque_por_cpf(pdf_bytes: bytes, cpf: str) -> bytes | None:
    """
    Busca o contracheque do colaborador pelo CPF dentro do PDF geral.
    Retorna bytes de um PDF individual ou None se não encontrado.
    """
    cpf_norm = _normalizar_cpf(cpf)
    if not cpf_norm:
        return None

    src = fitz.open(stream=pdf_bytes, filetype='pdf')
    metades = _dividir_em_metades(pdf_bytes)

    for item in metades:
        cpf_encontrado = _extrair_cpf_do_texto(item['texto'])
        if cpf_encontrado == cpf_norm:
            # Gera PDF individual desta metade
            page_idx = item['page_idx']
            clip     = item['clip']
            page     = src[page_idx]

            out_doc  = fitz.open()
            new_page = out_doc.new_page(width=clip.width, height=clip.height)
            new_page.show_pdf_page(
                fitz.Rect(0, 0, clip.width, clip.height),
                src, page_idx, clip=clip,
            )

            buf = io.BytesIO()
            out_doc.save(buf, garbage=4, deflate=True)
            out_doc.close()
            src.close()
            logger.info(f"[paystub] Contracheque encontrado para CPF {cpf_norm[:3]}***")
            return buf.getvalue()

    src.close()
    logger.warning(f"[paystub] Contracheque não encontrado para CPF {cpf_norm[:3]}***")
    return None


def parsear_competencia(texto: str) -> tuple[int, int] | None:
    """
    Tenta parsear uma competência informada pelo usuário.
    Aceita formatos: MM/YYYY, MM/YY, YYYY.MM, MES/ANO por extenso.
    Retorna (ano, mes) ou None.
    """
    import datetime

    texto = texto.strip().upper()

    # MM/YYYY ou MM/YY
    m = re.match(r'^(\d{1,2})[/\-.](\d{2,4})$', texto)
    if m:
        mes, ano = int(m.group(1)), int(m.group(2))
        if ano < 100:
            ano += 2000
        if 1 <= mes <= 12 and ano >= 2020:
            return ano, mes

    # YYYY.MM
    m = re.match(r'^(\d{4})[/\-.](\d{1,2})$', texto)
    if m:
        ano, mes = int(m.group(1)), int(m.group(2))
        if 1 <= mes <= 12 and ano >= 2020:
            return ano, mes

    # Mês por extenso + ano: "JANEIRO 2025", "JAN/2025"
    meses = {
        'JAN': 1, 'JANEIRO': 1, 'FEV': 2, 'FEVEREIRO': 2,
        'MAR': 3, 'MARCO': 3, 'MARÇO': 3,
        'ABR': 4, 'ABRIL': 4, 'MAI': 5, 'MAIO': 5,
        'JUN': 6, 'JUNHO': 6, 'JUL': 7, 'JULHO': 7,
        'AGO': 8, 'AGOSTO': 8, 'SET': 9, 'SETEMBRO': 9,
        'OUT': 10, 'OUTUBRO': 10, 'NOV': 11, 'NOVEMBRO': 11,
        'DEZ': 12, 'DEZEMBRO': 12,
    }
    texto_sem_acento = _strip_accents(texto)
    for nome, num in meses.items():
        if nome in texto_sem_acento:
            ano_m = re.search(r'(\d{4})', texto_sem_acento)
            if ano_m:
                return int(ano_m.group(1)), num

    return None
