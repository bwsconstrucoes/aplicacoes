# -*- coding: utf-8 -*-
"""AGRUPAR A LISTA DA FOLHA (27/11/2026). O dono: *"eu queria poder agrupar pela obra
da contabilidade, ou pela obra do ponto (…) ou obra do cadastro, ou conta de
pagamento."*"""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_gestao as fg
from tests.test_analisesps_telas import app  # noqa: F401 — o fixture da tela


def _p(nome, valor, **campos):
    return dict({"nome_na_tela": nome, "valor": D(valor), "setor_curto": "", "setor": "",
                 "filial": "", "obra_principal": "", "obra_do_cadastro": "", "contas": []},
                **campos)


PESSOAS = [
    _p("ANA", "100", filial="001 - MATRIZ", setor_curto="ESCRITORIO", obra_principal="CREPEOLINDA",
       obra_do_cadastro="CREPEOLINDA", contas=["1001"]),
    _p("BETO", "200", filial="002 - OLINDA", setor_curto="OBRA OLINDA", obra_principal="CREPEOLINDA",
       obra_do_cadastro="ESCVARZEA", contas=["1001", "2002"]),
    _p("CAIO", "300", filial="001 - MATRIZ", setor_curto="ESCRITORIO", obra_principal="", obra_do_cadastro="",
       contas=[]),
]


def test_sem_agrupar_e_um_grupo_so_sem_titulo():
    grupos = fg.agrupar(PESSOAS, "")
    assert len(grupos) == 1 and grupos[0]["titulo"] == "" and grupos[0]["quantas"] == 3


@pytest.mark.parametrize("por,esperado", [
    ("contabilidade", [("001 - MATRIZ", ["ANA", "CAIO"], D("400")), ("002 - OLINDA", ["BETO"], D("200"))]),
    ("setor", [("ESCRITORIO", ["ANA", "CAIO"], D("400")), ("OBRA OLINDA", ["BETO"], D("200"))]),
    ("ponto", [("CREPEOLINDA", ["ANA", "BETO"], D("300")), ("(sem obra no ponto)", ["CAIO"], D("300"))]),
    ("cadastro", [("CREPEOLINDA", ["ANA"], D("100")), ("ESCVARZEA", ["BETO"], D("200")),
                  ("(sem obra no cadastro)", ["CAIO"], D("300"))]),
    ("conta", [("1001", ["ANA"], D("100")), ("Mais de uma conta (1001, 2002)", ["BETO"], D("200")),
               ("(sem conta de pagamento)", ["CAIO"], D("300"))]),
])
def test_os_quatro_agrupamentos(por, esperado):
    grupos = fg.agrupar(PESSOAS, por)
    assert [(g["titulo"], [p["nome_na_tela"] for p in g["pessoas"]], g["total"])
            for g in grupos] == esperado


@pytest.mark.banco
def test_a_TELA_da_folha_agrupada(banco_analisesps, app, monkeypatch):  # noqa: F811
    from tests.test_analisesps_folha_arquivo import guardar
    from tests.test_analisesps_telas import _como_mestre
    feito = guardar(monkeypatch, titulo="Folha Sintética - Folha de Pagamento", mes=10)
    cliente = _como_mestre(app)
    normal = cliente.get(f"/analisesps/folha/{feito['id']}").get_data(as_text=True)
    assert "Agrupar por:" in normal and 'class="linha-grupo"' not in normal
    assert "<th>Admissão</th>" in normal, "a coluna da admissão (27/11/2026)"
    agrupada = cliente.get(f"/analisesps/folha/{feito['id']}?agrupar=ponto").get_data(as_text=True)
    assert 'class="linha-grupo"' in agrupada and "(sem obra no ponto)" in agrupada
    assert agrupada.count('class="linha-pessoa') == normal.count('class="linha-pessoa'), \
        "agrupar não tira nem põe ninguém"
