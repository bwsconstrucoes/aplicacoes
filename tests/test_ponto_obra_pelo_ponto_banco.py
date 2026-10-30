# -*- coding: utf-8 -*-
"""Ponto — a obra da pessoa é a do PONTO (30/10/2026, o dono, vendo "sem obra" no espelho de um administrativo que
bate todo dia na BWSNE: "o sistema precisa inferir do próprio ponto qual a obra dessa pessoa. Na falta do ponto
que usamos o cadastro")."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401


def test_a_obra_mais_batida():
    from app.apps.ponto.core.cadastros import obra_mais_batida
    dias = [{"data": "2026-10-20", "batidas": [{"obra": "A"}, {"obra": "A"}]},
            {"data": "2026-10-21", "batidas": [{"obra": "B"}, {"obra": "B"}]},
            {"data": "2026-10-22", "batidas": [{"obra": "B"}]}, {"data": "2026-10-23", "batidas": []}]
    assert obra_mais_batida(dias) == "B"
    assert obra_mais_batida(dias[:2]) == "B"            # empate: a mais recente
    assert obra_mais_batida([{"data": "2026-10-23", "batidas": []}]) is None


@pytest.mark.banco
def test_o_espelho_e_a_ficha_dizem_a_obra_do_ponto(app, mundo):
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    # sem batida: a do cadastro
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    assert esp["pessoa"]["obra"] == "PG-A" == esp["pessoa"]["obra_cadastro"]
    # bateu a semana na obra B: a obra do ponto é a B; o cadastro continua dizendo A
    for n in range(3):
        d = seg + dt.timedelta(days=n)
        for h in (7, 11, 12, 16):
            bater_via_chave(app, CPF_JOAO, local(d, h), obra="PG-B")
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    assert esp["pessoa"]["obra"] == "PG-B" and esp["pessoa"]["obra_cadastro"] == "PG-A"
    ficha = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]
    if (dt.date.today() - seg).days <= 60:
        assert ficha["obra_do_ponto"]["codigo"] == "PG-B"
    assert ficha["obra_principal"]["codigo"] == "PG-A"


def test_a_obra_vizinha():
    from app.apps.ponto.core.espelho import obra_vizinha
    d = lambda n: dt.date(2026, 10, n)                                   # noqa: E731
    por_dia = {d(20): [{"obra_id": 1}], d(23): [{"obra_id": 2}]}
    dias = sorted(por_dia)
    assert obra_vizinha(dias, por_dia, d(21)) == 1                         # o último dia com batida antes
    assert obra_vizinha(dias, por_dia, d(25)) == 2
    assert obra_vizinha(dias, por_dia, d(18)) == 1                         # nenhum antes: o primeiro depois
    assert obra_vizinha([], {}, d(18)) is None


@pytest.mark.banco
def test_o_feriado_vale_pela_obra_onde_a_pessoa_bate(app, mundo):
    """31/10/2026, o dono: "para efeito de feriado, a obra onde a pessoa está trabalhando funciona?" — no dia do
    feriado a pessoa não bate; antes, valia a obra do cadastro, e o feriado da obra em que ela trabalha virava falta."""
    from app.apps.ponto import db
    from app.apps.ponto.core import feriados
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    ter, qua = seg + dt.timedelta(days=1), seg + dt.timedelta(days=2)
    for d in (seg, ter):                                 # o João (cadastro na A) trabalha na B
        for h in (7, 11, 12, 16):
            bater_via_chave(app, CPF_JOAO, local(d, h), obra="PG-B")
    with db.conexao() as conn:
        feriados.gravar(conn, {"data": qua.isoformat(), "nome": "Feriado da obra B", "abrangencia": "OBRA",
                               "obra_id": mundo["obra_b"]})
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={qua:%Y-%m}").get_json()
    d = next(x for x in esp["dias"] if x["data"] == qua.isoformat())
    assert d["feriado"] == "Feriado da obra B" and d["situacao"] != "FALTA", d
    assert d["obra_do_dia"] == "PG-B"
