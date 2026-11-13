# -*- coding: utf-8 -*-
"""A FICHA ÚNICA DA PESSOA (13/11/2026): a janela do ponto, sozinha na página, é a que a tela de Colaboradores abre —
com as abas do cadastro e dos pagamentos do ERP para quem pode vê-las. Contra um Postgres de verdade."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_a_ficha_unica_abre_sozinha_e_a_tela_de_colaboradores_a_usa(app, mundo):
    dp = como(app, mundo["dp"])
    r = dp.get(f"/erp/ponto/pessoa/{mundo['joao']}")
    html = r.get_data(as_text=True)
    assert r.status_code == 200
    assert f"const FICHA_ID = {mundo['joao']};" in html and "sem-filtros embutido" in html
    assert "painelPessoaErp" in html and "Configuração do ponto" in html
    # os dados que as abas novas leem são os do ERP, com a trava dele
    assert dp.get(f"/erp/api/colaboradores/{mundo['joao']}/historico").status_code == 200
    # a tela de Colaboradores abre a ficha única (quem enxerga o ponto) e atende os pedidos dela
    col = dp.get("/erp/colaboradores").get_data(as_text=True)
    assert 'id="dlg-unica"' in col and "/erp/ponto/pessoa/" in col and '"bws-ficha"' in col
    # o menu: o Ponto de volta, logo depois de Pessoal
    from app.apps.erp import routes as R
    chaves = [m["chave"] for m in R.MODULOS]
    assert chaves.index("ponto") == chaves.index("pessoal") + 1
