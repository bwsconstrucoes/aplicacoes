# -*- coding: utf-8 -*-
"""
Gerador do recibo/ofício da BWS em HTML (réplica do template Google Docs
1GSggPSy3LeWsTab23sKhvcZWpDn1JoijBvs9L6L1h7c), com logo e assinatura embutidas.
Depois converte para PDF (via Playwright, que você já usa) para salvar no Dropbox.

Campos do template: cidade, datadodocumento, enderecamento, assunto, contrato, objeto, info1.
Rodapé é fixo (Nilo Sérgio Viana Bezerra / Administrador / CNPJ).
"""
from __future__ import annotations
import base64
import os

_AQUI = os.path.dirname(os.path.abspath(__file__))
LOGO_PADRAO = os.path.join(_AQUI, "recibo_logo.jpg")
ASSINATURA_PADRAO = os.path.join(_AQUI, "recibo_assinatura.png")


def brl(v) -> str:
    """Formata número no padrão R$ brasileiro: 1234.5 -> '1.234,50'."""
    try:
        n = float(v)
    except Exception:
        return str(v)
    return f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _data_uri(caminho: str, mime: str) -> str:
    if not os.path.exists(caminho):
        return ""
    with open(caminho, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode()
    return f"data:{mime};base64,{b64}"


def montar_recibo_html(dados: dict, logo=LOGO_PADRAO, assinatura=ASSINATURA_PADRAO) -> str:
    logo_uri = _data_uri(logo, "image/jpeg")
    assin_uri = _data_uri(assinatura, "image/png")
    g = lambda k: dados.get(k, "")
    return f"""<!DOCTYPE html><html lang="pt-br"><head><meta charset="utf-8"><style>
 @page {{ size: A4; margin: 2.5cm 2.5cm; }}
 body {{ font-family: Arial, Helvetica, sans-serif; color:#111; font-size:12pt; line-height:1.5; }}
 .logo {{ text-align:center; margin-bottom:28px; }}
 .logo img {{ height:90px; }}
 .topo {{ margin-bottom:22px; }}
 .ref {{ margin:18px 0; }}
 .corpo {{ text-align:justify; margin:10px 0; white-space:pre-wrap; }}
 .fecho {{ margin-top:26px; }}
 .assin {{ margin-top:34px; }}
 .assin img {{ height:80px; display:block; }}
 .nome b {{ display:block; }}
</style></head><body>
 <div class="logo">{('<img src="'+logo_uri+'">') if logo_uri else ''}</div>
 <div class="topo">{g('cidade')}, {g('datadodocumento')}</div>
 <div>À(o)</div>
 <div>{g('enderecamento')}</div>
 <div class="ref">REF.: <b>{g('assunto')}</b></div>
 <div class="corpo">{g('contrato')}</div>
 <div class="corpo">{g('objeto')}</div>
 <div class="corpo">{g('info1')}</div>
 <div class="fecho">Atenciosamente,</div>
 <div class="assin">{('<img src="'+assin_uri+'">') if assin_uri else ''}</div>
 <div class="nome">
   <b>Nilo Sérgio Viana Bezerra</b>
   <b>Administrador</b>
   <b>CPF: 013.567.983-49</b>
   <b>BWS Construções LTDA</b>
   <b>CNPJ: 00.079.526/0001-09</b>
 </div>
</body></html>"""


def html_para_pdf(html: str, saida_pdf: str) -> str:
    """[fallback local] Converte HTML em PDF via Playwright (Chromium headless).
    No Render, prefira gerar_recibo_pdf (fpdf2), que não precisa de navegador."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        navegador = p.chromium.launch()
        pagina = navegador.new_page()
        pagina.set_content(html, wait_until="networkidle")
        pagina.pdf(path=saida_pdf, format="A4", print_background=True)
        navegador.close()
    return saida_pdf


# ---- gerador de PDF sem navegador (fpdf2) — recomendado para Render ----
_SUBST = {"–": "-", "—": "-", "“": '"', "”": '"', "‘": "'", "’": "'", "\u00a0": " ", "•": "-"}


def _l1(texto: str) -> str:
    """Deixa o texto compatível com as fontes core do fpdf (latin-1)."""
    s = str(texto or "")
    for a, b in _SUBST.items():
        s = s.replace(a, b)
    return s.encode("latin-1", "replace").decode("latin-1")


def _extenso(valor) -> str:
    from num2words import num2words
    return num2words(float(valor), lang="pt_BR", to="currency").replace(", ", " ")


def gerar_recibo_pdf(dados: dict, saida_pdf: str, logo=LOGO_PADRAO, assinatura=ASSINATURA_PADRAO) -> str:
    """Gera o RECIBO DE MEDIÇÃO em PDF (padrão BWS), sem navegador. Pronto p/ Render."""
    from fpdf import FPDF
    g = lambda k: _l1(dados.get(k, ""))
    OURO = (216, 162, 36)

    pdf = FPDF(format="A4")
    pdf.set_auto_page_break(False)
    pdf.add_page()
    ME, MD = 20, 20
    larg = pdf.w - ME - MD

    # logo à esquerda + linha dourada
    if os.path.exists(logo):
        pdf.image(logo, x=ME, y=12, w=42)
    pdf.set_draw_color(*OURO)
    pdf.set_line_width(0.6)
    pdf.line(ME, 34, pdf.w - MD, 34)

    # data à direita
    pdf.set_xy(ME, 37)
    pdf.set_font("Helvetica", size=10)
    pdf.cell(larg, 6, _l1(f"{dados.get('cidade', 'Eusébio')}, {g('data_extenso')}"),
             align="R", ln=1)
    pdf.ln(3)

    pdf.set_x(ME)
    pdf.cell(0, 5.5, "À(o)", ln=1)
    pdf.set_x(ME)
    pdf.multi_cell(larg, 5.5, g("tomador"))

    pdf.set_x(ME)
    pdf.write(5.5, "REF.: ")
    pdf.set_font("Helvetica", style="B", size=10)
    pdf.write(5.5, _l1("RECIBO DE MEDIÇÃO"))
    pdf.ln(7)
    pdf.set_font("Helvetica", size=10)
    pdf.set_x(ME)
    pdf.cell(0, 5.5, _l1(f"CONTRATO: {g('contrato')}"), ln=1)
    pdf.set_x(ME)
    pdf.multi_cell(larg, 5.5, _l1(f"OBJETO: {g('objeto')}"))
    pdf.ln(5)
    pdf.set_x(ME)
    pdf.cell(0, 5.5, "Prezados,", ln=1)
    pdf.ln(3)

    # corpo: "Recebi do(a) ..."
    valor_brl = brl(dados.get("valor", 0))
    ext = _l1(_extenso(dados.get("valor", 0)))
    emp = ""
    if str(dados.get("empenho", "")).strip() and str(dados.get("empenho")).strip().strip("-"):
        emp = f" Nº do Empenho: {g('empenho')}."
    corpo = _l1(
        f"        Recebi do(a) {g('tomador')}, a quantia de R$ {valor_brl} ({ext}) "
        f"referente ao pagamento da {g('medicao')}ª medição do serviço de {g('objeto')}, "
        f"conforme Contrato {g('contrato')}, celebrado com a BWS CONSTRUÇÕES LTDA.{emp}"
    )
    pdf.set_x(ME)
    pdf.multi_cell(larg, 5.5, corpo, align="J")
    pdf.ln(6)
    pdf.set_x(ME)
    pdf.cell(0, 5.5, "        Atenciosamente,", ln=1)
    pdf.ln(2)

    # assinatura + nome centralizados
    if os.path.exists(assinatura):
        aw = 42
        pdf.image(assinatura, x=(pdf.w - aw) / 2, y=pdf.get_y(), w=aw)
        pdf.ln(22)
    else:
        pdf.ln(14)
    pdf.set_font("Helvetica", style="B", size=10)
    for ln in ("Nilo Sérgio Viana Bezerra", "Administrador", "CPF: 013.567.983-49",
               "BWS Construções LTDA", "CNPJ: 00.079.526/0001-09"):
        pdf.cell(0, 5, _l1(ln), align="C", ln=1)

    # rodapé fixo no pé da página
    yb = pdf.h - 28
    pdf.set_draw_color(*OURO)
    pdf.line(ME, yb, pdf.w - MD, yb)
    pdf.set_xy(ME, yb + 1.5)
    pdf.set_font("Helvetica", style="B", size=7.5)
    pdf.cell(larg, 3.6, _l1("BWS CONSTRUÇÕES LTDA - CNPJ: 00.079.526/0001-09"),
             align="R", ln=1)
    pdf.set_font("Helvetica", size=7.5)
    for ln in ("Escritório: Rua Luís Moreira Gomes, 11, Pq. Jabuti",
               "Eusébio-CE - CEP: 61766-680 - BR 116, Km 19",
               "Fone/Fax: (85) 99832-2004 - email: contato@bwsconstrucoes.com.br"):
        pdf.set_x(ME)
        pdf.cell(larg, 3.6, _l1(ln), align="R", ln=1)
    if str(dados.get("id_documento", "")).strip():
        pdf.set_x(ME)
        pdf.cell(larg, 3.6, _l1(f"ID {dados.get('id_documento')}"), align="R", ln=1)

    pdf.output(saida_pdf)
    return saida_pdf
