# -*- coding: utf-8 -*-
"""Ponto — AS ESCALAS DAS PESSOAS EM LOTE (26/10/2026): o dono, "selecionar os vigias
todos e aplicar a escala para eles (…) e cada funcionário novo já entrasse na escala de
produção". Postgres de verdade; reusa o mundo de test_ponto_gestao_banco."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_quem_esta_em_cada_escala_e_aplicar_em_lote(app, mundo):
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    r = dp.get("/erp/api/ponto/escalas/pessoas").get_json()
    por_id = {p["id"]: p for p in r["pessoas"]}
    assert por_id[mundo["joao"]]["escala"] == "Obra 44h" and r["inicio_do_ponto"]
    # uma escala nova (a dos vigias) para os dois, de uma vez
    e = dp.post("/erp/api/ponto/escalas", json={"nome": "Vigia noite", "tipo": "CICLO_12X36",
                                                 "ciclo_entrada": "19:00", "ciclo_saida": "07:00"})
    assert e.status_code == 201, e.get_json()
    vigia = e.get_json()["escala"]["id"]
    corpo = {"escala_id": vigia, "vigencia_inicio": r["inicio_do_ponto"], "ciclo_data_base": r["inicio_do_ponto"],
             "colaboradores": [mundo["joao"], mundo["maria"]]}
    assert sup.post("/erp/api/ponto/escalas/aplicar", json=corpo).status_code == 403   # só quem configura
    assert dp.post("/erp/api/ponto/escalas/aplicar", json={**corpo, "colaboradores": []}).status_code == 400
    assert dp.post("/erp/api/ponto/escalas/aplicar", json=corpo).get_json()["aplicadas"] == 2
    por_id = {p["id"]: p for p in dp.get("/erp/api/ponto/escalas/pessoas").get_json()["pessoas"]}
    assert por_id[mundo["joao"]]["escala"] == por_id[mundo["maria"]]["escala"] == "Vigia noite"
    # pessoa fora do cadastro: nada entra (a transação é uma só)
    assert dp.post("/erp/api/ponto/escalas/aplicar", json={**corpo, "escala_id": mundo["escala"],
                   "colaboradores": [mundo["joao"], 999999]}).status_code == 404
    por_id = {p["id"]: p for p in dp.get("/erp/api/ponto/escalas/pessoas").get_json()["pessoas"]}
    assert por_id[mundo["joao"]]["escala"] == "Vigia noite"


def test_feriado_em_varias_obras_de_uma_vez(app, mundo):
    """26/10/2026, o dono: "uma forma simples de selecionar múltiplas obras (…) e também todas"."""
    dp = como(app, mundo["dp"])
    corpo = {"data": "2026-12-08", "nome": "Padroeira", "obra_ids": [mundo["obra_a"], mundo["obra_b"]]}
    r = dp.post("/erp/api/ponto/feriados", json=corpo)
    assert r.status_code == 201 and r.get_json()["criados"] == 2, r.get_json()
    r = dp.post("/erp/api/ponto/feriados", json=corpo).get_json()
    assert r["criados"] == 0 and r["ja_tinham"] == 2
    assert dp.post("/erp/api/ponto/feriados", json={**corpo, "obra_ids": []}).status_code == 400
    lista = dp.get("/erp/api/ponto/feriados?ano=2026").get_json()["feriados"]
    assert sorted(f["obra_id"] for f in lista if f["nome"] == "Padroeira") == sorted([mundo["obra_a"], mundo["obra_b"]])
    assert como(app, mundo["sup"]).post("/erp/api/ponto/feriados", json=corpo).status_code == 403


def test_fotos_de_cadastro_suspeita_pura():
    from app.apps.ponto.core.fotos_cadastro import suspeita
    assert suspeita(["OUTRA_PESSOA", "OUTRA_PESSOA", "MESMA_PESSOA"])
    assert not suspeita(["OUTRA_PESSOA", "MESMA_PESSOA", "MESMA_PESSOA"])
    assert not suspeita(["OUTRA_PESSOA", None, "SEM_ROSTO"])                      # uma só não basta
    assert suspeita(["OUTRA_PESSOA", "SEM_ROSTO", "OUTRA_PESSOA", None, "MESMA_PESSOA"])
    # depois de trocada a foto, as novas conferem: a suspeita some
    assert not suspeita(["MESMA_PESSOA", "MESMA_PESSOA", "MESMA_PESSOA", "OUTRA_PESSOA", "OUTRA_PESSOA"])


def test_fotos_de_cadastro_tela_e_tirar(app, mundo):
    dp = como(app, mundo["dp"])
    r = dp.get("/erp/api/ponto/fotos-cadastro?filtro=TODAS").get_json()
    assert set(r["contagem"]) == {"SUSPEITA", "AUTOMATICA", "SEM_FOTO", "TODAS"} and isinstance(r["pessoas"], list)
    assert dp.get("/erp/ponto/fotos-cadastro").status_code == 200
    assert dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/foto-cadastral/tirar").status_code == 404
    assert como(app, mundo["fin"]).get("/erp/api/ponto/fotos-cadastro").status_code == 403
