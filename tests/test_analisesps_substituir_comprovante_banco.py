# -*- coding: utf-8 -*-
"""
SUBSTITUIR O COMPROVANTE DE UMA SP — 14/10/2026.

O dono: *"teve um comprovante que eu tentei baixar e está em erro. Como a
gente poderia fazer para eu poder substituir, caso eu entenda necessário?"*
"""
import datetime as dt
import io

import pytest

from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco

ANTIGO = "https://www.dropbox.com/s/velho/comp.pdf?dl=1"


def semear_paga():
    from tests.test_analisesps_banco import semear, sp
    semear([sp("1000000801", forma_pagamento="Pix", status_pgt="Pago", valor="10,00",
               vencimento="10/10/2026", centro_custo="OBRA", comprovante=ANTIGO)])


def comprovante_no_banco():
    from app.apps.analisesps.db import consultar_um
    return consultar_um("SELECT comprovante FROM analisesps.sps WHERE id = ?", ("1000000801",))[0]


def test_so_aceita_pdf_ou_imagem_e_com_teto():
    from app.apps.analisesps import substituir_comprovante as sc
    assert sc.extensao_do_conteudo(b"%PDF-1.4 ...") == ".pdf"
    assert sc.extensao_do_conteudo(b"\xff\xd8\xff\xe0...") == ".jpg"
    with pytest.raises(sc.ErroDaSubstituicao, match="PDF, PNG ou JPG"):
        sc.extensao_do_conteudo(b"PK\x03\x04 planilha")
    with pytest.raises(sc.ErroDaSubstituicao, match="grande demais"):
        sc.subir_arquivo("1", b"%PDF" + b"0" * sc.TETO_BYTES)
    chamadas = []
    url = sc.subir_arquivo("1000000801", b"%PDF-1.4", agora=dt.datetime(2026, 10, 14, 9, 30),
                           subir=lambda b, p: chamadas.append(p) or {"url": "https://www.dropbox.com/s/novo.pdf?dl=1"})
    assert url.endswith("novo.pdf?dl=1")
    assert chamadas == [sc.PASTA_DROPBOX + "/1000000801_comprovante_substituido_20261014_0930.pdf"]
    with pytest.raises(sc.ErroDaSubstituicao, match="https://"):
        sc.link_colado("comprovante.pdf")


def test_TROCA_por_link_colado_e_o_antigo_fica_no_log(app):
    from app.apps.analisesps.db import consultar
    semear_paga()
    with app.test_client() as c:
        c.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        r = c.post("/analisesps/api/sp/1000000801/comprovante",
                   data={"link": "https://drive.google.com/file/d/CERTO/view"}).get_json()
        ficha = c.get("/analisesps/sp/1000000801?modal=1").get_data(as_text=True)
    assert r["ok"] and comprovante_no_banco() == "https://drive.google.com/file/d/CERTO/view"
    assert "Substituir comprovante" in ficha
    log = " ".join(str(l) for l in consultar("SELECT * FROM analisesps.log_alteracoes"))
    assert ANTIGO in log, "o comprovante de antes fica registrado"


def test_TROCA_por_arquivo_vai_para_o_dropbox(app, monkeypatch):
    from app.apps.analisesps import substituir_comprovante as sc
    import app.apps.baixabradesco.storage as storage
    monkeypatch.setattr(storage, "upload_dropbox_bytes",
                        lambda b, p: {"url": "https://www.dropbox.com/s/novo/comp.pdf?dl=1"})
    semear_paga()
    with app.test_client() as c:
        c.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        ok = c.post("/analisesps/api/sp/1000000801/comprovante",
                    data={"arquivo": (io.BytesIO(b"%PDF-1.4 certo"), "comp.pdf")},
                    content_type="multipart/form-data").get_json()
        ruim = c.post("/analisesps/api/sp/1000000801/comprovante",
                      data={"arquivo": (io.BytesIO(b"PK\x03\x04"), "planilha.xlsx")},
                      content_type="multipart/form-data")
    assert ok["ok"] and comprovante_no_banco() == "https://www.dropbox.com/s/novo/comp.pdf?dl=1"
    assert ruim.status_code == 400 and "PDF, PNG ou JPG" in ruim.get_json()["erro"]
    assert sc.TETO_BYTES == 10 * 1024 * 1024
