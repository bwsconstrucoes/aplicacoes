# -*- coding: utf-8 -*-
"""Ponto — os achados do dono de 13/10/2026, sem banco: o documento em PDF que
dava "foto em base64 ilegível" e o aparelho que não expira."""
from __future__ import annotations

import base64
import datetime as dt

import pytest

from app.apps.ponto.core import dispositivos, documentos, fotos
from app.apps.ponto.erros import ErroDeValidacao

PDF = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"


@pytest.mark.parametrize("prefixo", ["data:application/pdf;base64,", "data:application/octet-stream;base64,",
                                     "data:;base64,", ""])
def test_documento_pdf_com_qualquer_prefixo(prefixo):
    dados, mime = documentos.preparar(prefixo + base64.b64encode(PDF).decode(), "atestado.pdf")
    assert mime == "application/pdf" and dados == PDF


def test_documento_ilegivel_diz_documento_nao_foto():
    with pytest.raises(ErroDeValidacao) as e:
        documentos.preparar("data:application/pdf;base64,@@@", "x.pdf")
    assert "documento" in e.value.mensagem and "foto" not in e.value.mensagem
    assert fotos.decodificar_base64("data:image/png;base64," + base64.b64encode(b"x").decode()) == b"x"


def test_aparelho_que_nao_expira():
    hoje = dt.date(2026, 10, 13)
    assert dispositivos.validade_do_grupo("NAO_EXPIRA", hoje, perfil="INDIVIDUAL") is None
    assert dispositivos.validade_do_grupo("NAO_EXPIRA", hoje, perfil="COMPARTILHADO") is None
    with pytest.raises(ErroDeValidacao):                      # o de equipe continua passageiro
        dispositivos.validade_do_grupo("NAO_EXPIRA", hoje, perfil="LISTA")
    assert dispositivos.validade_do_grupo(None, hoje, perfil="INDIVIDUAL") == hoje + dt.timedelta(days=90)
    assert dispositivos.vencimento({"valido_ate": None}) is None
