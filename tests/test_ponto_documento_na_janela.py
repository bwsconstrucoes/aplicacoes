# -*- coding: utf-8 -*-
"""Ponto — o documento abre na janela, não baixa (26/10/2026): o tipo sai do próprio
arquivo quando o gravado é genérico."""
from app.apps.ponto.gestao import _tipo_do_arquivo


def test_o_tipo_vem_do_arquivo():
    assert _tipo_do_arquivo(b"%PDF-1.7 ...", "application/octet-stream") == "application/pdf"
    assert _tipo_do_arquivo(b"\xff\xd8\xff\xe0....", None) == "image/jpeg"
    assert _tipo_do_arquivo(b"\x89PNG\r\n\x1a\n....", "") == "image/png"
    assert _tipo_do_arquivo(b"RIFF\x00\x00\x00\x00WEBPVP8 ", None) == "image/webp"
    assert _tipo_do_arquivo(b"qualquer coisa", "text/plain") == "text/plain"
    assert _tipo_do_arquivo(b"", None) == "application/octet-stream"
