"""Foto de iPhone (HEIC) vira JPG na chegada — 25/11/2026, pedido do dono:
*"foto de iPhone no portal, sim, tem que aceitar também."*"""
import io

import pytest

from app.apps.erp.core.comum import fotos

pillow_heif = pytest.importorskip("pillow_heif")


def _heic(largura=40, altura=20) -> bytes:
    from PIL import Image
    fotos.registrar_heic()
    buf = io.BytesIO()
    Image.new("RGB", (largura, altura), (200, 30, 30)).save(buf, format="HEIF")
    return buf.getvalue()


def test_heic_vira_jpg_com_o_nome_trocado():
    from PIL import Image
    conteudo, nome = fotos.heic_para_jpeg(_heic(), "IMG_0042.HEIC")
    assert nome == "IMG_0042.jpg"
    assert conteudo[:3] == b"\xff\xd8\xff", "tem de sair JPG"
    with Image.open(io.BytesIO(conteudo)) as img:
        assert img.size == (40, 20)


def test_reconhece_pelo_conteudo_mesmo_com_nome_de_jpg():
    """O nome pode mentir: foto repassada chega como 'foto.jpg' e é HEIC."""
    conteudo, nome = fotos.heic_para_jpeg(_heic(), "foto.jpg")
    assert conteudo[:3] == b"\xff\xd8\xff" and nome == "foto.jpg"


def test_o_que_nao_e_heic_passa_igual():
    pdf = b"%PDF-1.4 qualquer coisa"
    assert fotos.heic_para_jpeg(pdf, "carta.pdf") == (pdf, "carta.pdf")


def test_heic_quebrado_e_guardado_como_veio():
    lixo = b"\x00\x00\x00\x18ftypheic" + b"\x00" * 40
    assert fotos.heic_para_jpeg(lixo, "x.heic") == (lixo, "x.heic")


def test_a_leitura_por_ia_abre_o_heic():
    from app.apps.erp.core.documentos import leitor
    import base64
    dado, mime = leitor._preparar_imagem(_heic())
    assert mime == "image/jpeg"
    assert base64.b64decode(dado)[:3] == b"\xff\xd8\xff"
