# -*- coding: utf-8 -*-
"""O relatório completo do lote, para a parceria (15/11/2026) —
`lote_relatorio_completo.py`."""
import datetime as dt
import time
import warnings
from decimal import Decimal as D

import pytest

from tests.test_analisesps_telas import (SENHA_CONSULTA, app, app_lote, como,  # noqa: F401
                                         linha_falsa)

warnings.filterwarnings("ignore", category=FutureWarning)

LINK_DO_RELATORIO = "https://drive.google.com/uc?export=download&id=1FoFMMGdZ9qlIoAnGOur2OOJjI7WpksLa"
DESCRICAO_DA_FOLHA = f"""Conta Origem: 92945-8
Pagamento avulso — competência 11/2026
Colaboradores: 94
Planilha de pagamento: https://drive.google.com/uc?export=download&id=1fuePgs_e-QTvnm4dWorAn1OeHavU2MC7
Relatório (PDF): {LINK_DO_RELATORIO}
Cadastro de colaboradores: https://drive.google.com/uc?export=download&id=1WeNF1__q2ZiNjmIcvD4ZIcenJ9GSMagu"""


def _pdf_falso(paginas=1, deitado=False) -> bytes:
    from fpdf import FPDF
    p = FPDF(orientation="L" if deitado else "P")
    for n in range(paginas):
        p.add_page()
        p.set_font("Helvetica", size=12)
        p.cell(0, 10, f"documento pagina {n + 1}")
    return bytes(p.output())


def _png_falso() -> bytes:
    import io

    from PIL import Image
    saida = io.BytesIO()
    Image.new("RGB", (300, 200), "white").save(saida, "PNG")
    return saida.getvalue()


def _sp(i, obra="ESCPE18", valor="100.00", **campos):
    base = {"id": str(1384831000 + i), "credor": f"CREDOR {i}", "documento": "",
            "valor_num": D(valor), "centro_custo": obra, "tipo_despesa": "Concreto",
            "forma_pagamento": "PIX", "descricao": f"compra {i}", "anexo_link": "",
            "vencimento_d": dt.date(2026, 11, 16), "status_pgt": "Pagar",
            "status_agend": "Agendar", "vencido": False, "vence_hoje": False}
    base.update(campos)
    return base


def _montado(*grupos):
    return {"grupos": [{"titulo_exibido": t, "linhas": ls} for t, ls in grupos]}


# --- os anexos ------------------------------------------------------------
def test_o_anexo_da_sp_vem_primeiro_e_pode_ser_mais_de_um():
    from app.apps.analisesps import lote_relatorio_completo as rc
    anexos = rc.anexos_da_sp({"anexo_link": "https://x/a.pdf, https://x/b.pdf",
                              "descricao": DESCRICAO_DA_FOLHA})
    assert [a["url"] for a in anexos] == ["https://x/a.pdf", "https://x/b.pdf"]
    assert [a["rotulo"] for a in anexos] == ["Anexo 1", "Anexo 2"]


def test_sp_da_folha_sem_anexo_usa_o_RELATORIO_PDF_da_descricao():
    """O dono: *"esses registros não têm anexo. Só que, dentro da descrição (…) tem
    lá o relatório PDF (…) esse seria o anexo a colocar."* Só o relatório: a planilha
    de pagamento e o cadastro de colaboradores ficam de fora."""
    from app.apps.analisesps import lote_relatorio_completo as rc
    anexos = rc.anexos_da_sp({"anexo_link": "", "descricao": DESCRICAO_DA_FOLHA})
    assert [a["url"] for a in anexos] == [LINK_DO_RELATORIO]
    assert anexos[0]["origem"] == "descricao"


def test_a_descricao_impressa_nao_leva_os_links_internos():
    from app.apps.analisesps import lote_relatorio_completo as rc
    texto = rc.descricao_para_o_parceiro(DESCRICAO_DA_FOLHA, {LINK_DO_RELATORIO})
    assert "http" not in texto
    assert "Relatório (PDF): (anexado neste relatório)" in texto
    assert "Planilha de pagamento: (link interno, não incluído)" in texto


def test_o_tipo_do_arquivo_baixado():
    from app.apps.analisesps import lote_relatorio_completo as rc
    assert rc._tipo_do_arquivo(_pdf_falso()) == "pdf"
    assert rc._tipo_do_arquivo(_png_falso()) == "png"
    assert rc._tipo_do_arquivo(b"<!DOCTYPE html><html>") == "html"
    assert rc._tipo_do_arquivo(b"PK\x03\x04resto") == "zip"
    with pytest.raises(RuntimeError, match="página da internet"):
        rc._preparar_anexo(b"<html>entre</html>")


# --- os dados -------------------------------------------------------------
def test_so_as_obras_escolhidas_entram_e_os_numeros_acompanham():
    from app.apps.analisesps import lote_relatorio_completo as rc
    montado = _montado(("Pagar amanhã", [_sp(1, valor="100.00"), _sp(2, obra="OUTRA", valor="50.00")]),
                       ("Folha", [_sp(3, valor="30.00", descricao=DESCRICAO_DA_FOLHA)]),
                       ("Só da outra", [_sp(4, obra="OUTRA")]))
    d = rc.montar_dados(montado, obras=["ESCPE18"], data_pagamento=dt.date(2026, 11, 16),
                        extras={})
    assert [l["id"] for l in d["linhas"]] == ["1384831001", "1384831003"]
    assert [g["titulo"] for g in d["grupos"]] == ["ESCPE18"], \
        "a relação é por obra, não pelos grupos do lote"
    assert d["total"] == D("130.00") and d["quantidade"] == 2
    assert [o["nome"] for o in d["obras"]] == ["ESCPE18"]
    assert [l["_n"] for l in d["linhas"]] == [1, 2]
    assert [l["id"] for l in d["sem_anexo"]] == ["1384831001"]


def test_a_relacao_sai_por_obra_da_maior_para_a_menor_e_sem_repetir_a_sp():
    """O dono, vendo a prévia: os títulos do lote ("Pagar amanhã", "Folha e locação")
    não fazem sentido para a parceria."""
    from app.apps.analisesps import lote_relatorio_completo as rc
    montado = _montado(("Pagar amanhã", [_sp(1, obra="A", valor="10"), _sp(2, obra="B", valor="90")]),
                       ("WhatsApp1", [_sp(3, obra="A", valor="5"), _sp(1, obra="A", valor="10")]))
    d = rc.montar_dados(montado, extras={})
    assert [(g["titulo"], [l["id"][-1] for l in g["linhas"]]) for g in d["grupos"]] == [
        ("B", ["2"]), ("A", ["1", "3"])]
    assert [l["_n"] for l in d["linhas"]] == [1, 2, 3]
    assert d["total"] == D("105") and "por_grupo" not in d


def test_as_obras_do_lote_para_a_janela():
    from app.apps.analisesps import lote_relatorio_completo as rc
    obras = rc.obras_do_lote(_montado(("G", [_sp(1, valor="10"), _sp(2, obra="B", valor="90"),
                                             _sp(3, valor="5")])))
    assert obras == [("B", 1, D("90")), ("ESCPE18", 2, D("15"))]


# --- o PDF ----------------------------------------------------------------
def test_o_pdf_traz_os_anexos_logo_depois_de_cada_sp_e_o_sumario():
    import fitz
    from app.apps.analisesps import lote_relatorio_completo as rc
    arquivos = {"https://x/nf.pdf": _pdf_falso(2), "https://x/foto.png": _png_falso(),
                LINK_DO_RELATORIO: _pdf_falso(1, deitado=True)}

    def baixar(url):
        if url in arquivos:
            return arquivos[url]
        raise RuntimeError("o servidor do anexo respondeu 404")

    montado = _montado(
        ("Pagar amanhã", [_sp(1, anexo_link="https://x/nf.pdf https://x/foto.png"),
                          _sp(2, anexo_link="https://x/sumiu.pdf")]),
        ("Folha", [_sp(3, descricao=DESCRICAO_DA_FOLHA, tipo_despesa="Salários e Ordenados")]))
    d = rc.montar_dados(montado, data_pagamento=dt.date(2026, 11, 16), extras={},
                        para="Parceria")
    andamento = []
    conteudo = rc.montar_pdf(d, baixar=baixar, progresso=lambda *a: andamento.append(a))
    doc = fitz.open(stream=conteudo, filetype="pdf")
    textos = [p.get_text() for p in doc]
    toc = doc.get_toc()
    assert [t[1] for t in toc if t[0] == 1][:2] == ["Resumo", "Relação dos pagamentos"]
    # SP 1: detalhe, depois as 2 páginas do PDF e a imagem — todas com a faixa.
    pagina_da_sp1 = next(t[2] for t in toc if t[1].startswith("1. CREDOR 1"))
    faixas = [t for t in textos if "PAGAMENTO 1  ·  SP 1384831001" in t]
    assert len(faixas) == 3
    assert "documento pagina 1" in textos[pagina_da_sp1]      # logo depois do detalhe
    # SP 2: o anexo que não baixou fica dito no detalhe, sem derrubar o resto.
    assert any("não deu para embutir" in t and "404" in t for t in textos)
    # SP 3 (folha): o relatório da descrição entra como documento.
    assert any("Relatório da folha (PDF)" in t and "SP 1384831003" in t and "página 1 de 1" in t
               for t in textos)
    assert all("http" not in t for t in textos), "nenhum link interno no PDF"
    assert textos[-1].count("página") >= 1 and f"página {len(doc)} de {len(doc)}" in textos[-1]
    assert andamento[-1][0] == andamento[-1][1] == 4
    # A linha da relação leva ao detalhe da SP.
    relacao = next(t[2] for t in toc if t[1] == "Relação dos pagamentos") - 1
    destinos = {l["page"] for l in doc[relacao].get_links() if l["kind"] == fitz.LINK_GOTO}
    assert pagina_da_sp1 - 1 in destinos


def test_sem_anexos_o_relatorio_sai_e_diz_por_que():
    import fitz
    from app.apps.analisesps import lote_relatorio_completo as rc
    d = rc.montar_dados(_montado(("G", [_sp(1, anexo_link="https://x/nf.pdf")])), extras={})
    conteudo = rc.montar_pdf(d, baixar=lambda u: 1 / 0, com_anexos=False)
    texto = "".join(p.get_text() for p in fitz.open(stream=conteudo, filetype="pdf"))
    assert "gerado sem os documentos" in texto


# --- a tela ---------------------------------------------------------------
def test_a_tela_do_lote_tem_o_botao_e_gera_pelo_caminho_de_fora(app_lote, monkeypatch):
    from app.apps.analisesps import lote_relatorio_completo as rc
    monkeypatch.setattr(rc, "_extras", lambda ids: {})
    monkeypatch.setattr(rc, "baixar_anexo", lambda url: _pdf_falso())
    cliente = como(app_lote, SENHA_CONSULTA)
    html = cliente.get("/analisesps/lote").get_data(as_text=True)
    assert "Relatório completo (para a parceria)" in html
    assert "Relatório completo do grupo" in html

    r = cliente.post("/analisesps/lote/relatorio-completo",
                     json={"data": "2026-11-16", "para": "Parceria", "grupo": 1})
    assert r.status_code == 200, r.get_data(as_text=True)
    estado_url = r.get_json()["estado"]
    for _ in range(100):
        e = cliente.get(estado_url).get_json()
        if e["pronto"] or e["erro"]:
            break
        time.sleep(0.1)
    assert e["pronto"], e
    assert e["nome"] == "Pagamentos - 2026-11-16.pdf"
    arquivo = cliente.get(e["arquivo"])
    assert arquivo.status_code == 200 and arquivo.get_data().startswith(b"%PDF")


def test_obra_que_nao_esta_no_lote_responde_que_nao_ha_o_que_por(app_lote, monkeypatch):
    from app.apps.analisesps import lote_relatorio_completo as rc
    monkeypatch.setattr(rc, "_extras", lambda ids: {})
    r = como(app_lote, SENHA_CONSULTA).post("/analisesps/lote/relatorio-completo",
                                            json={"obras": ["NAO EXISTE"]})
    assert r.status_code == 400 and "Não há SPs" in r.get_json()["erro"]


def test_relatorio_que_nao_existe_responde_404(app_lote):
    cliente = como(app_lote, SENHA_CONSULTA)
    assert cliente.get("/analisesps/lote/relatorio-completo/abc").status_code == 404
    assert cliente.get("/analisesps/lote/relatorio-completo/abc/arquivo").status_code == 404


# --- a otimização dos anexos (dono, 16/11/2026) -----------------------------
def _foto_pesada(w=3000, h=4000) -> bytes:
    """Uma "foto de celular": ruído em JPEG de alta qualidade, que não comprime."""
    import io
    import os

    from PIL import Image
    imagem = Image.frombytes("RGB", (w, h), os.urandom(w * h * 3))
    saida = io.BytesIO()
    imagem.save(saida, "JPEG", quality=95)
    return saida.getvalue()


def test_a_foto_grande_e_reduzida_antes_de_entrar():
    from PIL import Image
    import io
    from app.apps.analisesps import lote_relatorio_completo as rc
    original = _foto_pesada()
    reduzida, tipo = rc._otimizar_foto(original, "jpg")
    assert tipo == "jpg" and len(reduzida) < len(original) / 3
    assert max(Image.open(io.BytesIO(reduzida)).size) == rc.LADO_MAXIMO_DA_FOTO


def test_foto_pequena_fica_como_esta():
    from app.apps.analisesps import lote_relatorio_completo as rc
    pequena = _png_falso()
    assert rc._otimizar_foto(pequena, "png") == (pequena, "png")


def test_o_pdf_digitalizado_pesado_sai_leve_no_relatorio():
    import fitz
    from app.apps.analisesps import lote_relatorio_completo as rc
    if not hasattr(fitz.Document, "rewrite_images"):
        pytest.skip("PyMuPDF sem rewrite_images")
    scan = fitz.open()
    for _ in range(2):
        pagina = scan.new_page()
        pagina.insert_image(pagina.rect, stream=_foto_pesada())
    pesado = scan.tobytes()
    d = rc.montar_dados(_montado(("G", [_sp(1, anexo_link="https://x/scan.pdf")])), extras={})
    conteudo = rc.montar_pdf(d, baixar=lambda u: pesado)
    assert len(conteudo) < len(pesado) / 4, (len(conteudo), len(pesado))
    assert sum("PAGAMENTO 1  ·  SP" in p.get_text()
               for p in fitz.open(stream=conteudo, filetype="pdf")) == 2


def _pdf_com_defeito(como: str) -> bytes:
    """Um PDF que ABRE e só estoura ao copiar as páginas — o "code=7" do dono."""
    import fitz
    d = fitz.open()
    for i in range(3):
        d.new_page().insert_text((72, 72), f"documento pagina {i + 1}")
    bruto = d.tobytes(garbage=0, deflate=False, use_objstms=0)
    if como == "ciclo":         # a árvore de páginas aponta para ela mesma
        return bruto.replace(b"/Kids[4 0 R 8 0 R 11 0 R]", b"/Kids[4 0 R 2 0 R 11 0 R]")
    return bruto.replace(b"/Count 3", b"/Count 9")      # diz ter 9 páginas e tem 3


@pytest.mark.parametrize("como", ["ciclo", "contagem"])
def test_PDF_COM_DEFEITO_nao_derruba_o_relatorio(como):
    """26/11/2026: *"Não consegui gerar: code=7: malformed page tree"* — um anexo
    quebrado derrubava o relatório inteiro, lá no fim."""
    import fitz
    from app.apps.analisesps import lote_relatorio_completo as rc
    arquivos = {"https://x/quebrado.pdf": _pdf_com_defeito(como),
                "https://x/bom.pdf": _pdf_falso(1)}
    montado = _montado(("G", [_sp(1, anexo_link="https://x/quebrado.pdf"),
                              _sp(2, anexo_link="https://x/bom.pdf")]))
    d = rc.montar_dados(montado, data_pagamento=dt.date(2026, 11, 26), extras={},
                        para="Parceria")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        conteudo = rc.montar_pdf(d, baixar=lambda u: arquivos[u])
    textos = [p.get_text() for p in fitz.open(stream=conteudo, filetype="pdf")]
    assert any("SP 1384831002" in t and "PAGAMENTO 2" in t for t in textos), "o anexo bom entrou"
    da_sp1 = [t for t in textos if "PAGAMENTO 1  ·  SP 1384831001" in t]
    assert da_sp1, "as páginas legíveis do PDF com defeito entraram"
    assert any("tinha defeito" in t for t in textos) or len(da_sp1) >= 2


def _zip(**arquivos) -> bytes:
    import io
    import zipfile
    saida = io.BytesIO()
    with zipfile.ZipFile(saida, "w", compression=zipfile.ZIP_STORED) as z:
        for nome, dado in arquivos.items():
            z.writestr(nome if nome.startswith("[") else nome.replace("_", "."), dado)
    return saida.getvalue()


def test_o_ZIP_com_imagens_entra_no_relatorio():
    """26/11/2026: *"tem um dos anexos que é um arquivo .zip com duas imagens"*. E o
    .zip com um PDF guardado sem compressão era lido COMO PDF ("malformed page tree")."""
    import fitz
    from app.apps.analisesps import lote_relatorio_completo as rc
    duas_fotos = _zip(foto1_png=_png_falso(), foto2_png=_png_falso(), leia_txt=b"oi")
    com_pdf = _zip(nota_pdf=_pdf_falso(2))
    assert rc._tipo_do_arquivo(com_pdf) == "zip", "o PDF dentro do .zip não engana mais"
    arquivos = {"https://x/fotos.zip": duas_fotos, "https://x/nota.zip": com_pdf,
                "https://x/planilha.xlsx": _zip(**{"[Content_Types].xml": b"<x/>"})}
    montado = _montado(("G", [_sp(1, anexo_link="https://x/fotos.zip"),
                              _sp(2, anexo_link="https://x/nota.zip"),
                              _sp(3, anexo_link="https://x/planilha.xlsx")]))
    d = rc.montar_dados(montado, data_pagamento=dt.date(2026, 11, 26), extras={},
                        para="Parceria")
    textos = [p.get_text() for p in fitz.open(stream=rc.montar_pdf(d, baixar=lambda u: arquivos[u]),
                                              filetype="pdf")]
    assert len([t for t in textos if "PAGAMENTO 1  ·  SP 1384831001" in t]) == 2, "as duas fotos"
    assert any("leia.txt" in t for t in textos), "o que ficou de fora do .zip é dito"
    assert len([t for t in textos if "PAGAMENTO 2  ·  SP 1384831002" in t]) == 2, "o PDF de dentro"
    assert any("Office" in t for t in textos), "planilha continua de fora, dizendo por quê"
