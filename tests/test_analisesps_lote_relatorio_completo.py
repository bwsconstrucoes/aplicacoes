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
    assert rc._tipo_do_arquivo(b"PK\x03\x04resto") == "office"
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
    assert [g["titulo"] for g in d["grupos"]] == ["Pagar amanhã", "Folha"], \
        "grupo sem SP da obra escolhida não aparece"
    assert d["total"] == D("130.00") and d["quantidade"] == 2
    assert [o["nome"] for o in d["obras"]] == ["ESCPE18"]
    assert [l["_n"] for l in d["linhas"]] == [1, 2]
    assert [l["id"] for l in d["sem_anexo"]] == ["1384831001"]


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
