# -*- coding: utf-8 -*-
"""Ponto — pedidos do dono de 09/10/2026, com Postgres de verdade: a obra que só
bate entrada e saída (intervalo pré-assinalado) não gera pendência; e o ajuste
pedido aparece no dia, no "Meu mês", com o horário."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, OBRA_A, _entrar_no_app, _schema_ponto2,  # noqa: F401
                                           _segunda_passada, app, bater_via_chave, como, local, mundo)

pytestmark = pytest.mark.banco


def test_obra_de_so_entrada_e_saida_nao_gera_pendencia(app, mundo):
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    bater_via_chave(app, CPF_JOAO, local(seg, 7, 0))
    bater_via_chave(app, CPF_JOAO, local(seg, 17, 0))

    def o_dia():
        esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
        return next(d for d in esp["dias"] if d["data"] == seg.isoformat())
    antes = o_dia()
    # A hora é a de Fortaleza: 07:00 em ponto não é atraso (o banco devolve UTC —
    # em 09/10/2026 o atraso saía com 3 h a mais)
    assert antes["atraso"] == 0 and "ATRASO" not in antes["alertas"]
    assert antes["extra"] == 60 and "INTERVALO_CURTO" in antes["alertas"] and len(antes["faltando"]) == 2
    cercas = dp.get("/erp/api/ponto/cercas").get_json()
    assert cercas["batidas_disponivel"] is True
    r = dp.post(f"/erp/api/ponto/cercas/{mundo['obra_a']}", json={"raio_metros": 200, "so_entrada_e_saida": True})
    assert r.status_code == 200 and r.get_json()["obra"]["so_entrada_e_saida"] is True
    assert next(o for o in dp.get("/erp/api/ponto/cercas").get_json()["obras"]
                if o["id"] == mundo["obra_a"])["so_entrada_e_saida"] is True
    depois = o_dia()
    assert depois["situacao"] == "OK" and depois["extra"] == 0 and depois["trabalhado"] == 540
    assert depois["faltando"] == [] and [p["rotulo"] for p in depois["previstas"]] == ["Entrada", "Saída"]
    assert depois["so_entrada_e_saida"] is True and depois["alertas"] == []


def test_ajuste_pedido_aparece_no_dia_com_o_horario(app, mundo, monkeypatch):
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0)):                    # esqueceu a saída
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)
    r = cel.post("/ponto/app/api/pedidos/ajuste-do-dia", json={
        "horarios": [f"{seg.isoformat()}T17:00:00"], "motivo": "ESQUECI", "descricao": "esqueci de bater a saída",
        "obra": "PG-A"})
    assert r.status_code == 201, r.get_json()
    mes = cel.get(f"/ponto/app/api/mes?competencia={seg:%Y-%m}").get_json()
    dia = next(d for d in mes["dias"] if d["data"] == seg.isoformat())
    assert [(p["tipo"], p["hora"]) for p in dia["pendentes"]] == [("AJUSTE_BATIDA", "17:00")]
    assert dia["faltando"] == []                              # o horário pedido não "falta" mais
