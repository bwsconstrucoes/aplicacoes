# -*- coding: utf-8 -*-
"""Ponto — "não bate ponto" na janela da pessoa e em lote na tela Pessoas (29/10/2026, migração 018): com o
motivo, quem marcou e quando; a pessoa sai das listas e não entra no aplicativo; e volta."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import CPF_JOAO, _schema_ponto2, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco

URL = "/erp/api/ponto/pessoas/bate-ponto"


def _nomes(c, todos=False):
    return {p["nome"] for p in c.get("/erp/api/ponto/pessoas" + ("?todos=1" if todos else "")).get_json()["pessoas"]}


def test_marcar_que_nao_bate_ponto_com_o_motivo_e_voltar(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import marcacoes
    from app.apps.ponto.erros import Recusada
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    joao, maria = mundo["joao"], mundo["maria"]
    nome_joao = next(p["nome"] for p in dp.get("/erp/api/ponto/pessoas").get_json()["pessoas"] if p["id"] == joao)
    # sem motivo não marca; sem ninguém marcado, também não
    assert dp.post(URL, json={"colaboradores": [joao], "bate": False}).status_code == 400
    assert dp.post(URL, json={"colaboradores": [], "bate": False, "motivo": "diretor da empresa"}).status_code == 400
    # o supervisor preso à obra A não alcança quem é de fora dela: "não encontrada", e ninguém muda
    assert sup.post(URL, json={"colaboradores": [joao, maria], "bate": False, "motivo": "teste de alcance"}).status_code == 404
    assert nome_joao in _nomes(dp)
    r = dp.post(URL, json={"colaboradores": [joao, maria], "bate": False, "motivo": "diretor da empresa"})
    assert r.status_code == 200 and r.get_json()["marcadas"] == 2
    assert nome_joao not in _nomes(dp) and nome_joao in _nomes(dp, todos=True)
    ficha = dp.get(f"/erp/api/ponto/pessoas/{joao}").get_json()["pessoa"]
    assert ficha["ativo_no_ponto"] is False and ficha["marcado_nao_bate"]["motivo"] == "diretor da empresa"
    assert ficha["marcado_nao_bate"]["por"] and ficha["marcado_nao_bate"]["em"]
    with pytest.raises(Recusada):
        with db.conexao() as conn:
            marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True)
    # a busca diz por quê
    d = dp.get("/erp/api/ponto/pessoas", query_string={"busca": nome_joao.lower()}).get_json()
    assert "diretor da empresa" in d["fora_da_lista"][0]["motivo"]
    # e volta
    assert dp.post(URL, json={"colaboradores": [joao], "bate": True}).status_code == 200
    ficha = dp.get(f"/erp/api/ponto/pessoas/{joao}").get_json()["pessoa"]
    assert ficha["ativo_no_ponto"] is True and ficha["marcado_nao_bate"] is None and nome_joao in _nomes(dp)
