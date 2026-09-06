"""A exportação em Excel e PDF, que agora existe em toda tela de lista.

O que estes testes seguram:

  - o NÚMERO chega como número na planilha. Texto que parece número é a
    reclamação nº 1 de quem recebe arquivo de sistema: não soma, não ordena,
    não vira tabela dinâmica;
  - "sem valor" continua VAZIO, e não vira zero. Num relatório de compras,
    "sem preço" e "preço zero" são coisas diferentes;
  - o PDF sai DEITADO quando há muitas colunas — foi o pedido do dono olhando
    o mapa de cotação;
  - o acento não derruba a geração do PDF. Com as fontes embutidas, o `fpdf2`
    só escreve latin-1, e o travessão, as aspas curvas e o "·" que o ERP usa
    em toda parte estouram no meio do arquivo se não forem trocados antes;
  - os FILTROS aparecem no arquivo. Relatório sem a origem é número sem
    procedência.
"""
from __future__ import annotations

import io

import pytest

from app.apps.erp.core.comum import exportar as svc

COLUNAS = [{"rotulo": "Insumo"}, {"rotulo": "Obra"},
           {"rotulo": "Qtd", "tipo": "quantidade"},
           {"rotulo": "Valor", "tipo": "dinheiro"}]
LINHAS = [["Cimento CP-II — 50kg", "CREPETRIUNFO", "120", "4.620,00"],
          ["Vergalhão CA50 · 12,5mm", "ESCPLANALTO", "430", "R$ 2.967,00"],
          ["Tarucel p/ Junta", "CREPETERRA", "180", ""]]


def planilha(conteudo: bytes):
    import openpyxl
    return openpyxl.load_workbook(io.BytesIO(conteudo)).active


def texto_do_pdf(conteudo: bytes) -> str:
    import fitz
    with fitz.open(stream=conteudo, filetype="pdf") as d:
        return "\n".join(p.get_text() for p in d)


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------
def test_numero_chega_como_numero_na_planilha():
    ws = planilha(svc.para_excel("Compras", COLUNAS, LINHAS))
    valores = {ws.cell(row=r, column=4).value for r in range(1, ws.max_row + 1)}
    assert 4620.0 in valores, "4.620,00 tinha de virar o número 4620"
    assert 2967.0 in valores, '"R$ 2.967,00" também é número'


def test_sem_valor_fica_vazio_e_nao_vira_zero():
    """No relatório de compras, "sem preço" e "preço zero" são coisas
    diferentes — e zero mentiria."""
    ws = planilha(svc.para_excel("Compras", COLUNAS, LINHAS))
    coluna = [ws.cell(row=r, column=4).value for r in range(1, ws.max_row + 1)]
    assert 0 not in coluna and 0.0 not in coluna


def test_a_planilha_congela_o_cabecalho_e_liga_o_filtro():
    ws = planilha(svc.para_excel("Compras", COLUNAS, LINHAS))
    assert ws.freeze_panes, "sem congelar, rolar mil linhas perde o cabeçalho"
    assert ws.auto_filter.ref


def test_os_filtros_vao_no_cabecalho_da_planilha():
    ws = planilha(svc.para_excel("Compras", COLUNAS, LINHAS,
                                 filtros={"Obra": "CREPETRIUNFO"}, quem="Marcelo"))
    tudo = " ".join(str(ws.cell(row=r, column=1).value or "") for r in range(1, 8))
    assert "CREPETRIUNFO" in tudo, "sem os filtros, o arquivo não diz de onde veio"
    assert "Marcelo" in tudo


def test_o_nome_da_aba_aguenta_titulo_com_barra():
    """O Excel recusa / \\ * ? : [ ] no nome da aba, e recusa o arquivo inteiro."""
    ws = planilha(svc.para_excel("Compras 2026/2027", COLUNAS, LINHAS,
                                 aba="Compras 2026/2027"))
    assert "/" not in ws.title


# ---------------------------------------------------------------------------
# PDF
# ---------------------------------------------------------------------------
def test_o_acento_e_os_sinais_nao_derrubam_o_pdf():
    """`fpdf2` com fonte embutida só escreve latin-1 e ESTOURA no meio, sem
    avisar, quando aparece travessão, aspa curva ou o "·" do ERP."""
    linhas = [["Tarucel p/ Junta — 6mm “especial”… 100% ✓", "OBRA", "1", "1,00"]]
    conteudo = svc.para_pdf("Relatório", COLUNAS, linhas)
    assert conteudo[:4] == b"%PDF"
    assert "Tarucel" in texto_do_pdf(conteudo)


def test_o_pdf_deita_quando_tem_muitas_colunas():
    """O dono pediu isso olhando um mapa com muitos fornecedores: em pé, as
    colunas ficam tão estreitas que não se lê nada."""
    import fitz
    muitas = [{"rotulo": f"F{i}"} for i in range(9)]
    with fitz.open(stream=svc.para_pdf("Mapa", muitas, [["x"] * 9]),
                   filetype="pdf") as d:
        assert d[0].rect.width > d[0].rect.height

    with fitz.open(stream=svc.para_pdf("Curto", COLUNAS[:3], [["a", "b", "1"]]),
                   filetype="pdf") as d:
        assert d[0].rect.width < d[0].rect.height, "com 3 colunas, em pé"


def test_o_pedido_manda_deitar_ou_ficar_em_pe():
    import fitz
    with fitz.open(stream=svc.para_pdf("X", COLUNAS[:2], [["a", "b"]], paisagem=True),
                   filetype="pdf") as d:
        assert d[0].rect.width > d[0].rect.height


def test_o_pdf_diz_quando_cortou_linhas():
    """Um PDF de vinte mil linhas ninguém abre. O que ele NÃO pode fazer é
    cortar em silêncio: quem lê tem de saber que falta gente ali."""
    linhas = [["item", "obra", "1", "1,00"]] * (svc.LIMITE_PDF + 40)
    texto = texto_do_pdf(svc.para_pdf("Grande", COLUNAS, linhas))
    assert f"{len(linhas)} linha" in texto
    assert "Excel" in texto, "tem de dizer onde estão as outras"


def test_os_filtros_vao_no_pdf():
    texto = texto_do_pdf(svc.para_pdf("Compras", COLUNAS, LINHAS,
                                      filtros=["Obra: CREPETRIUNFO",
                                               "Situação: em cotação"]))
    assert "CREPETRIUNFO" in texto
    # o "ç" e o "ã" sobrevivem: latin-1 cobre o português
    assert "em cota" in texto


# ---------------------------------------------------------------------------
# Detalhes que já quebraram alguma coisa
# ---------------------------------------------------------------------------
def test_linha_mais_curta_que_o_cabecalho_nao_derruba():
    """A tela pode mandar uma linha a menos. Isso não pode virar erro 500."""
    ws = planilha(svc.para_excel("X", COLUNAS, [["só o insumo"]]))
    assert ws.max_row > 1


def test_coluna_como_texto_simples_tambem_serve():
    ws = planilha(svc.para_excel("X", ["A", "B"], [["1", "2"]]))
    assert ws.max_row > 1


def test_o_nome_do_arquivo_nao_tem_acento_nem_espaco():
    nome = svc.nome_de_arquivo("Banco de preços — Suprimentos", "xlsx")
    assert nome.isascii() and " " not in nome and nome.endswith(".xlsx")


@pytest.mark.parametrize("bruto,esperado", [
    ("1.234,56", 1234.56), ("1234.56", 1234.56), ("R$ 59,50", 59.5),
    ("1.000", 1000.0), ("", None), ("—", None), (None, None), ("12", 12.0)])
def test_o_leitor_de_numero_entende_os_dois_formatos(bruto, esperado):
    """A tela manda "1.234,56"; a API manda "1234.56". Os dois têm de virar o
    mesmo número — e o que não é número tem de virar None, não zero."""
    achado = svc._numero(bruto)
    assert (float(achado) if achado is not None else None) == esperado
