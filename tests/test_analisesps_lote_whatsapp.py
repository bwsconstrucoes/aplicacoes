# -*- coding: utf-8 -*-
"""O grupo "WhatsApp" do lote (08/10/2026): um só, sempre no topo, sem repetir."""
from app.apps.analisesps.lote import juntar_no_grupo_whatsapp, separar_grupos


def test_cria_o_grupo_no_topo_e_preserva_o_resto_como_estava():
    texto = "Pagar amanhã\n1384831053 1384844943\n\nOutro\n2222222222"
    novo, entraram = juntar_no_grupo_whatsapp(texto, ["3333333333"])
    assert entraram == ["3333333333"]
    assert novo == "WhatsApp\n3333333333\n\n" + texto


def test_soma_no_grupo_existente_e_o_traz_para_o_topo():
    texto = "Pagar amanhã\n1111111111\n\nwhatsapp\n4444444444\n\nOutro\n2222222222"
    novo, entraram = juntar_no_grupo_whatsapp(
        texto, ["5555555555", "1111111111", "4444444444", "5555555555"])
    assert entraram == ["5555555555"], "o que já está em QUALQUER grupo não repete"
    grupos = separar_grupos(novo)
    assert grupos[0] == {"titulo": "WhatsApp", "ids": ["4444444444", "5555555555"]}
    assert [g["titulo"] for g in grupos] == ["WhatsApp", "Pagar amanhã", "Outro"]


def test_lote_vazio():
    assert juntar_no_grupo_whatsapp("", ["1234567890"]) == (
        "WhatsApp\n1234567890", ["1234567890"])
