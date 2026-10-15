# -*- coding: utf-8 -*-
"""
CONFIGURAÇÕES › ESPAÇO NO BANCO — 14/10/2026, com banco de verdade.

O dono viu o banco do Render em 73% e perguntou "o que é isso tanto?". A medida
é do banco INTEIRO, área por área — a tela de saúde do ERP só via o ERP.
"""
import pytest

from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco


def test_a_medida_cobre_as_areas_e_traz_as_maiores_tabelas(app):
    from app.apps.analisesps import espaco_banco
    m = espaco_banco.medir()
    assert "erro" not in m, m
    assert m["total_mb"] > 0
    areas = {a["area"] for a in m["areas"]}
    assert "Análise de SPs" in areas
    assert m["tabelas"] and all("." in t["tabela"] for t in m["tabelas"])
    assert m["tabelas"][0]["mb"] >= m["tabelas"][-1]["mb"], "da maior para a menor"


def test_a_TELA_mostra_so_para_o_mestre_na_aba_banco(app):
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        tela = cliente.get("/analisesps/configuracoes?aba=sistema").get_data(as_text=True)
        assert "Espaço no banco" in tela and "Análise de SPs" in tela
        outra = cliente.get("/analisesps/configuracoes?aba=acesso").get_data(as_text=True)
        assert "Espaço no banco" not in outra
