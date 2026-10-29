# -*- coding: utf-8 -*-
"""Ponto — o espelho do ponto no aplicativo (28/10/2026), com Postgres de verdade: o meu,
com o CPF só pelo final na tela e inteiro na folha para imprimir; e o de uma pessoa da
obra, com o MESMO alcance da consulta (fora dele, "não encontrado", também na folha)."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _entrar_no_app, _schema_ponto2,  # noqa: F401
                                           app, como, mundo)
from tests.test_ponto_papeis_banco import CPF_CARLOS, LONGE, NA_OBRA_A, _bater, _papeis, carlos  # noqa: F401

pytestmark = pytest.mark.banco


def _cpf(c: str) -> str:
    return f"{c[0:3]}.{c[3:6]}.{c[6:9]}-{c[9:11]}"


def test_o_meu_espelho_com_o_cpf_pelo_final_e_a_folha_com_ele_inteiro(app, mundo, monkeypatch):
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    r = c.get("/ponto/app/api/mes")
    d = r.get_json()
    assert d["pessoa"]["nome"] == "João Obra A" and d["pessoa"]["cpf_final"] == CPF_JOAO[-3:]
    assert d["pessoa"]["obra"] == "PG-A" and "funcao" in d["pessoa"]
    assert CPF_JOAO not in r.get_data(as_text=True)              # na tela, só o final
    folha = c.get("/ponto/app/espelho/imprimir")
    html = folha.get_data(as_text=True)
    assert folha.status_code == 200 and folha.mimetype == "text/html"
    assert "João Obra A" in html and _cpf(CPF_JOAO) in html     # na folha, o CPF inteiro
    assert "‹ Voltar" in html and "window.print(); }, 300" not in html
    assert "window.print(); }, 300" in c.get("/ponto/app/espelho/imprimir?imprimir=1").get_data(as_text=True)
    # sem entrar, nada
    assert app.test_client().get("/ponto/app/espelho/imprimir").status_code == 401


def test_o_espelho_de_outra_pessoa_segue_o_alcance_da_consulta(app, mundo, carlos, monkeypatch):
    _papeis(app, mundo, carlos, administrativo_obra=True)
    _bater(CPF_MARIA, "PG-A", 30)                        # a Maria é da obra B, mas bateu na A
    c = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    url = f"/ponto/app/equipe/{mundo['maria']}/espelho/imprimir"
    folha = c.get(f"{url}?{NA_OBRA_A}")
    assert folha.status_code == 200 and "Maria Obra B" in folha.get_data(as_text=True)
    assert _cpf(CPF_MARIA) in folha.get_data(as_text=True)
    # longe da obra: fora do alcance — "não encontrada", numa página (não um JSON)
    fora = c.get(f"{url}?{LONGE}")
    assert fora.status_code == 404 and fora.mimetype == "text/html"
    assert "Não foi possível abrir o espelho" in fora.get_data(as_text=True)
    assert "Maria Obra B" not in fora.get_data(as_text=True)
    # quem não é responsável nem administrativo não abre o espelho de ninguém
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    assert joao.get(f"{url}?{NA_OBRA_A}").status_code == 404
    assert CPF_CARLOS not in folha.get_data(as_text=True)


def test_a_folha_de_um_periodo_escolhido(app, mundo, monkeypatch):
    """29/10/2026: além do mês, um período de até 62 dias (o corte da folha, de 21 a 20)."""
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    folha = c.get("/ponto/app/espelho/imprimir?de=2026-09-21&ate=2026-10-20")
    html = folha.get_data(as_text=True)
    assert folha.status_code == 200 and "21/09/2026 a 20/10/2026" in html and "Totais do período" in html
    longe = c.get("/ponto/app/espelho/imprimir?de=2026-07-01&ate=2026-10-20")
    assert longe.status_code == 400 and "62 dias" in longe.get_data(as_text=True)
    invertido = c.get("/ponto/app/espelho/imprimir?de=2026-10-20&ate=2026-10-01")
    assert invertido.status_code == 400 and "antes do primeiro" in invertido.get_data(as_text=True)
