# ============================================================================
# ERP — core/comum/exportar.py
# O que está na tela vira planilha ou PDF.
#
# UM LUGAR SÓ, E NÃO UM POR TELA. Toda tela de lista do ERP manda daqui:
# título, as colunas, as linhas e os filtros que estavam ligados. Fazendo uma
# vez, vale para Solicitações, Títulos, Pedidos, Banco de preços, Insumos,
# Fornecedores, Locações — e para a próxima tela que alguém escrever.
#
# POR QUE OS FILTROS VÃO IMPRESSOS NO CABEÇALHO
# Um relatório sem os filtros é um número sem origem. Três meses depois,
# ninguém sabe se aquela planilha era do mês passado, de uma obra só, ou de
# tudo — e aí ela não serve para decidir nada. O cabeçalho diz o que estava
# ligado quando o arquivo nasceu, mais quem exportou e quando.
#
# NENHUMA DEPENDÊNCIA NOVA: `openpyxl` e `fpdf2` já estão no serviço.
#
# A ARMADILHA DO PDF, que custou tempo em outros módulos deste repositório:
# com as fontes embutidas, o `fpdf2` só escreve o que couber em latin-1 — e
# não avisa, estoura no meio. Latin-1 cobre o português (ç, ã, é), mas não os
# sinais tipográficos que entram sem ninguém perceber: o travessão, as aspas
# curvas, as reticências de um caractere, o "·" que o ERP usa em toda parte.
# Por isso TODO texto passa por `_ascii_seguro()` antes de ir para o papel.
# ============================================================================
from __future__ import annotations

import io
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)

# Teto de linhas. Um PDF de vinte mil linhas ninguém abre, e a memória do
# serviço é de 2 GB dividida com outros treze módulos. Quem precisa da lista
# inteira baixa a planilha, que aguenta muito mais.
LIMITE_PDF = 3000
LIMITE_EXCEL = 50000

# Sinais que não existem em latin-1, e o equivalente legível. Vira o parecido,
# nunca "?" — travessão virando interrogação no meio da frase é pior que hífen.
TROCAS = {
    "—": "-", "–": "-", "−": "-", "·": "-", "•": "-",
    "'": "'", "'": "'", """: '"', """: '"',
    "…": "...", " ": " ", "​": "", "﻿": "",
    "→": "->", "←": "<-", "×": "x", "≥": ">=", "≤": "<=",
    "⤓": "", "✓": "ok", "✗": "x", "⚠": "!", "₂": "2", "²": "2", "³": "3",
}

TIPOS_NUMERICOS = {"numero", "dinheiro", "quantidade", "inteiro", "percentual"}


def _ascii_seguro(valor: Any) -> str:
    texto = "" if valor is None else str(valor)
    for de, para in TROCAS.items():
        texto = texto.replace(de, para)
    # o que sobrou fora do latin-1 sai, em vez de derrubar a geração
    return texto.encode("latin-1", "ignore").decode("latin-1")


def _texto(valor: Any) -> str:
    return "" if valor is None else str(valor)


def _numero(valor: Any) -> Optional[Decimal]:
    """Converte para número aceitando o que a tela manda: 1.234,56 e 1234.56.

    Devolve None quando não é número — e aí a célula fica como texto, em vez
    de virar zero. Zero mentiria: "sem preço" e "preço zero" são coisas
    diferentes num relatório de compras.
    """
    if valor is None or valor == "":
        return None
    if isinstance(valor, (int, float, Decimal)):
        try:
            return Decimal(str(valor))
        except InvalidOperation:
            return None
    texto = re.sub(r"[R$\s]", "", str(valor)).strip()
    if not texto:
        return None
    if "," in texto and "." in texto:
        texto = (texto.replace(".", "").replace(",", ".")
                 if texto.rfind(",") > texto.rfind(".")
                 else texto.replace(",", ""))
    elif "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "." in texto:
        # SÓ PONTO é ambíguo: "1.000" é mil em português e é um vírgula zero
        # em inglês. A regra é a MESMA que a tela usa (`paraNumero`, em
        # erp_base.html): o ponto só separa milhar quando sobram exatamente
        # três dígitos depois dele. Tem de ser a mesma regra dos dois lados,
        # senão o arquivo sai com número diferente do que está na tela.
        partes = texto.split(".")
        e_milhar = (len(partes) > 2 or
                    (len(partes) == 2 and len(partes[1]) == 3
                     and len(partes[0].lstrip("-+")) <= 3))
        if e_milhar:
            texto = texto.replace(".", "")
    try:
        return Decimal(texto)
    except InvalidOperation:
        return None


def normalizar_colunas(colunas: Sequence[Any]) -> list[dict[str, str]]:
    """Aceita ["A", "B"] ou [{"chave": "valor", "rotulo": "A", "tipo": "dinheiro"}].

    A CHAVE é guardada quando vem, e é ela que casa a coluna com o campo da
    linha — ver `_linhas_como_listas`.
    """
    saida = []
    for c in colunas or []:
        if isinstance(c, dict):
            saida.append({"rotulo": _texto(c.get("rotulo") or c.get("chave") or ""),
                          "chave": str(c.get("chave") or ""),
                          "tipo": (c.get("tipo") or "texto").lower()})
        else:
            saida.append({"rotulo": _texto(c), "chave": "", "tipo": "texto"})
    return saida


def _linhas_como_listas(linhas: Sequence[Any],
                        cols: Sequence[dict[str, str]]) -> list[list[Any]]:
    """Cada linha vira uma lista NA ORDEM DAS COLUNAS.

    A ARMADILHA QUE ISTO EVITA, achada em 12/09/2026 ao fazer a resposta do
    assistente virar planilha: a linha costuma ser um dicionário, e pegar
    `linha.values()` entrega os valores na ordem em que o dicionário foi
    montado — que NÃO é necessariamente a ordem das colunas. Pior: linha à
    qual falta um campo empurra todos os seguintes uma casa para a esquerda.
    Nos dois casos o arquivo sai com o valor debaixo do cabeçalho errado, sem
    erro nenhum e sem ninguém perceber. Número errado com cara de certo é o
    pior defeito que um relatório pode ter.

    Quando as colunas trazem CHAVE, cada valor é buscado pela chave. Sem
    chave (a tela que manda só os rótulos), a ordem do dicionário é tudo o que
    existe, e aí ela é respeitada como antes.
    """
    chaves = [c.get("chave") or "" for c in cols]
    tem_chaves = any(chaves)
    saida = []
    for linha in linhas or []:
        if isinstance(linha, dict):
            if tem_chaves:
                saida.append([linha.get(k) if k else None for k in chaves])
                continue
            linha = list(linha.values())
        linha = list(linha)
        linha += [None] * (len(cols) - len(linha))
        saida.append(linha[:len(cols)])
    return saida


def _rodape_de_filtros(filtros: Any) -> list[str]:
    """Os filtros ligados, em linhas curtas. Aceita dict ou lista de textos."""
    if not filtros:
        return []
    if isinstance(filtros, dict):
        return [f"{k}: {v}" for k, v in filtros.items() if v not in (None, "", [], {})]
    return [_texto(f) for f in filtros if _texto(f).strip()]


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------
def para_excel(titulo: str, colunas: Sequence[Any], linhas: Sequence[Any], *,
               subtitulo: str = "", filtros: Any = None,
               quem: str = "", aba: str = "Dados") -> bytes:
    """A planilha, com cabeçalho de origem e a primeira linha congelada."""
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    cols = normalizar_colunas(colunas)
    dados = _linhas_como_listas(linhas, cols)[:LIMITE_EXCEL]

    wb = Workbook()
    ws = wb.active
    ws.title = re.sub(r"[\\/*?:\[\]]", "-", (aba or "Dados"))[:31] or "Dados"

    azul = "0B2C5C"
    ws["A1"] = titulo
    ws["A1"].font = Font(bold=True, size=14, color=azul)
    linha = 2
    if subtitulo:
        ws[f"A{linha}"] = subtitulo
        ws[f"A{linha}"].font = Font(size=10, color="555555")
        linha += 1
    carimbo = f"Gerado em {datetime.now():%d/%m/%Y %H:%M}"
    if quem:
        carimbo += f" por {quem}"
    ws[f"A{linha}"] = carimbo
    ws[f"A{linha}"].font = Font(size=9, color="777777")
    linha += 1
    for f in _rodape_de_filtros(filtros):
        ws[f"A{linha}"] = f
        ws[f"A{linha}"].font = Font(size=9, color="777777", italic=True)
        linha += 1
    linha += 1

    cabecalho = linha
    fundo = PatternFill("solid", fgColor=azul)
    borda = Border(bottom=Side(style="thin", color="CCCCCC"))
    for i, c in enumerate(cols, start=1):
        cel = ws.cell(row=cabecalho, column=i, value=c["rotulo"])
        cel.font = Font(bold=True, color="FFFFFF", size=10)
        cel.fill = fundo
        cel.alignment = Alignment(vertical="center", wrap_text=True)

    for r, valores in enumerate(dados, start=cabecalho + 1):
        for i, (c, v) in enumerate(zip(cols, valores), start=1):
            cel = ws.cell(row=r, column=i)
            if c["tipo"] in TIPOS_NUMERICOS:
                n = _numero(v)
                if n is not None:
                    # número DE VERDADE na célula: é o que permite somar,
                    # ordenar e fazer tabela dinâmica. Texto que parece número
                    # é a reclamação nº 1 de quem recebe planilha de sistema.
                    cel.value = float(n)
                    cel.number_format = ('#,##0.00' if c["tipo"] in ("dinheiro", "numero")
                                         else ('#,##0' if c["tipo"] == "inteiro"
                                               else '#,##0.###'))
                else:
                    cel.value = _texto(v)
                cel.alignment = Alignment(horizontal="right")
            else:
                cel.value = _texto(v)
                cel.alignment = Alignment(vertical="top", wrap_text=len(_texto(v)) > 40)
            cel.border = borda

    for i, c in enumerate(cols, start=1):
        maior = max([len(c["rotulo"])] +
                    [len(_texto(l[i - 1])) for l in dados[:400]] or [10])
        ws.column_dimensions[get_column_letter(i)].width = min(52, max(10, maior + 2))
    ws.freeze_panes = ws.cell(row=cabecalho + 1, column=1)
    ws.auto_filter.ref = (f"A{cabecalho}:"
                          f"{get_column_letter(max(1, len(cols)))}{cabecalho + len(dados)}")

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# PDF
# ---------------------------------------------------------------------------
def _deve_deitar(cols: Sequence[dict], paisagem: Optional[bool]) -> bool:
    """Deitado quando não cabe em pé.

    O dono pediu isso olhando o mapa de cotação com muitos fornecedores: em pé
    as colunas ficam tão estreitas que não se lê nada. A partir de seis
    colunas, deitar é quase sempre melhor.
    """
    if paisagem is not None:
        return bool(paisagem)
    return len(cols) > 5


def para_pdf(titulo: str, colunas: Sequence[Any], linhas: Sequence[Any], *,
             subtitulo: str = "", filtros: Any = None, quem: str = "",
             paisagem: Optional[bool] = None) -> bytes:
    """O relatório em PDF, com cabeçalho de origem e número de página."""
    from fpdf import FPDF

    cols = normalizar_colunas(colunas)
    todas = _linhas_como_listas(linhas, cols)
    dados = todas[:LIMITE_PDF]
    cortou = len(todas) - len(dados)

    deitado = _deve_deitar(cols, paisagem)
    pdf = FPDF(orientation="L" if deitado else "P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.set_margins(10, 10, 10)
    pdf.add_page()
    largura = (297 if deitado else 210) - 20

    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(11, 44, 92)
    pdf.multi_cell(largura, 7, _ascii_seguro(titulo),
               new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(90, 90, 90)
    if subtitulo:
        pdf.set_font("Helvetica", "", 9)
        pdf.multi_cell(largura, 4.5, _ascii_seguro(subtitulo),
                       new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    carimbo = f"Gerado em {datetime.now():%d/%m/%Y %H:%M}"
    if quem:
        carimbo += f" por {quem}"
    pdf.multi_cell(largura, 4, _ascii_seguro(carimbo),
               new_x="LMARGIN", new_y="NEXT")
    for f in _rodape_de_filtros(filtros):
        pdf.multi_cell(largura, 4, _ascii_seguro(f),
                       new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # largura de cada coluna: proporcional ao conteúdo, com piso e teto para
    # nenhuma sumir nem uma engolir a página
    amostra = dados[:200]
    pesos = []
    for i, c in enumerate(cols):
        # o cabeçalho conta pela METADE: "MADEIREIRA E COBERTURAS EXEMPLO LTDA"
        # não pode roubar a largura das colunas que têm conteúdo de verdade.
        maior = max([len(c["rotulo"]) // 2] +
                    [len(_texto(l[i])) for l in amostra] or [8])
        pesos.append(min(40, max(7, maior)))
    total = sum(pesos) or 1
    larguras = [max(14.0, largura * p / total) for p in pesos]
    excesso = sum(larguras) - largura
    if excesso > 0:                       # devolve o que passou, proporcional
        for i in range(len(larguras)):
            larguras[i] -= excesso * (larguras[i] / sum(larguras))

    def _cabe(texto: str, largura: float) -> str:
        """Corta o texto até caber na coluna. Sem isso, um nome comprido
        invade a coluna vizinha e as duas ficam ilegíveis — foi o que
        aconteceu com os nomes dos fornecedores no mapa."""
        texto = _ascii_seguro(texto)
        if pdf.get_string_width(texto) <= largura - 2:
            return texto
        while texto and pdf.get_string_width(texto + ".") > largura - 2:
            texto = texto[:-1]
        return (texto + ".") if texto else ""

    def cabecalho():
        pdf.set_font("Helvetica", "B", 7.5)
        pdf.set_fill_color(11, 44, 92)
        pdf.set_text_color(255, 255, 255)
        for c, w in zip(cols, larguras):
            pdf.cell(w, 6, _cabe(c["rotulo"], w), border=0, fill=True,
                     align="R" if c["tipo"] in TIPOS_NUMERICOS else "L")
        pdf.ln(6)
        pdf.set_text_color(30, 30, 30)

    cabecalho()
    pdf.set_font("Helvetica", "", 7.5)
    limite_pagina = (210 if deitado else 297) - 16
    for n, valores in enumerate(dados):
        if pdf.get_y() > limite_pagina:
            pdf.add_page()
            cabecalho()
            pdf.set_font("Helvetica", "", 7.5)
        if n % 2:
            pdf.set_fill_color(248, 249, 252)
        else:
            pdf.set_fill_color(255, 255, 255)
        for c, w, v in zip(cols, larguras, valores):
            # sem quebra de linha dentro da célula: com muitas colunas a
            # tabela vira um borrão. Corta e avisa com um ponto.
            pdf.cell(w, 4.6, _cabe(_texto(v), w), border=0, fill=True,
                     align="R" if c["tipo"] in TIPOS_NUMERICOS else "L")
        pdf.ln(4.6)

    pdf.ln(2)
    pdf.set_font("Helvetica", "", 7.5)
    pdf.set_text_color(110, 110, 110)
    resumo = f"{len(todas)} linha(s)."
    if cortou:
        resumo += (f" Mostrando as {len(dados)} primeiras; as outras {cortou} "
                   f"estao na versao em Excel.")
    pdf.multi_cell(largura, 4, _ascii_seguro(resumo),
               new_x="LMARGIN", new_y="NEXT")

    saida = pdf.output()
    return bytes(saida)


def nome_de_arquivo(titulo: str, extensao: str) -> str:
    """"Banco de preços" → "banco-de-precos-06-09-2026.xlsx"."""
    base = _ascii_seguro(titulo).lower()
    base = re.sub(r"[àáâãä]", "a", base)
    base = re.sub(r"[èéêë]", "e", base)
    base = re.sub(r"[ìíîï]", "i", base)
    base = re.sub(r"[òóôõö]", "o", base)
    base = re.sub(r"[ùúûü]", "u", base)
    base = base.replace("ç", "c")
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-") or "relatorio"
    return f"{base[:60]}-{datetime.now():%d-%m-%Y}.{extensao}"
