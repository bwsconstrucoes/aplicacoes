# -*- coding: utf-8 -*-
"""Ponto — o ranking guardado (29/10/2026, core/ranking.py): obras com 10 funcionários ou mais, os 25
primeiros funcionários com 10 dias ou mais, a posição de cada um, e a conta que só refaz depois do prazo."""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.ponto.core import ranking
from tests.test_ponto_gestao_banco import CPF_JOAO, _entrar_no_app, _schema_ponto2, app, como, mundo  # noqa: F401

AGORA = dt.datetime(2026, 10, 29, 14, 0, tzinfo=dt.timezone.utc)


def _pessoa(i, pontos, dias, obra="AP-01", ajustes=0):
    return {"id": i, "nome": f"FULANO {i} DE TAL", "pontos": pontos, "dias_trabalho": dias, "ajustes": ajustes,
            "faltas": 0, "entra_no_ranking": dias >= 10, "obra_do_periodo": obra, "selo": None}


def test_montar_so_entra_quem_tem_o_minimo():
    r = {"pessoas": [_pessoa(1, 100, 3), _pessoa(2, 90, 20), _pessoa(3, 80, 15, ajustes=2)],
         "por_obra": [{"obra": "AP-01", "pessoas": 1, "pontos": 100, "entra_no_ranking": False},
                      {"obra": "BR-02", "pessoas": 12, "pontos": 85, "entra_no_ranking": True}]}
    d = ranking.montar(r, {2: {"funcao": "PEDREIRO"}}, {"BR-02": "Obra Grande"}, "2026-10", AGORA)
    assert [p["nome"] for p in d["pessoas"]] == ["FULANO TAL", "FULANO TAL"]       # só o primeiro e o último nome
    assert [p["posicao"] for p in d["pessoas"]] == [1, 2] and d["pessoas"][0]["funcao"] == "PEDREIRO"
    assert d["posicoes"]["2"]["posicao"] == 1 and d["posicoes"]["1"]["posicao"] is None   # a de 3 dias não entra
    assert "10 dias" in d["posicoes"]["1"]["por_que"]
    assert [(o["obra"], o["posicao"]) for o in d["obras"]] == [("BR-02", 1), ("AP-01", None)]   # 1 pessoa: fora
    assert "cpf" not in str(d).lower()


def test_so_refaz_depois_do_prazo():
    assert ranking.vencido(None, 2, AGORA)
    assert not ranking.vencido({"calculado_em": (AGORA - dt.timedelta(hours=1)).isoformat()}, 2, AGORA)
    assert ranking.vencido({"calculado_em": (AGORA - dt.timedelta(hours=3)).isoformat()}, 2, AGORA)
    # a conta só roda das 6h às 20h, na hora de Brasília (14h UTC = 11h; 01h UTC = 22h da véspera)
    assert ranking.na_janela(AGORA) and not ranking.na_janela(AGORA.replace(hour=1))


@pytest.mark.banco
def test_o_aplicativo_le_o_ranking_guardado(app, mundo, monkeypatch):
    from app.apps.ponto import db, horario
    from tests.test_ponto_gestao_banco import bater_via_chave, local
    ontem = horario.hoje() - dt.timedelta(days=1)
    bater_via_chave(app, CPF_JOAO, local(ontem, 7, 0))
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    comp = f"{horario.hoje():%Y-%m}"
    if ontem.month != horario.hoje().month:
        pytest.skip("o primeiro dia do mês: ontem é do mês passado")
    assert c.get("/ponto/app/api/ranking").get_json()["pronto"] is False       # ainda sem conta guardada
    with db.conexao() as conn:
        d = ranking.calcular(conn, comp, esperar=5)
    with db.conexao() as conn:
        assert d["competencia"] == comp and ranking.ler(conn, comp)
    r = c.get("/ponto/app/api/ranking").get_json()
    assert r["pronto"] is True and "posicoes" not in r                       # a posição dos outros não vai
    # a posição de quem está vendo (as faltas também são dias de trabalho previstos: ele entra, com poucos pontos)
    assert r["eu"] is not None and "pontos" in r["eu"]
    assert app.test_client().get("/ponto/app/api/ranking").status_code == 401
