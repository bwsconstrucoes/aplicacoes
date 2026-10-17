# -*- coding: utf-8 -*-
"""Ponto — DE QUANDO O PONTO VALE (17/10/2026, core/vigencia.py): o dono, "os pontos
deverão ser considerados a partir do dia 16/10/2026; desconsiderar pra todos os
efeitos qualquer lançamento antes disso". Nada é apagado; o que vem antes some de
espelho, filtros, Validações, mosaico e qualidade, e não aceita pedido novo."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import CPF_JOAO, _schema_ponto2, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_o_que_vem_antes_do_inicio_nao_conta(app, mundo, banco):
    from sqlalchemy import text
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import marcacoes, vigencia
    hoje = horario.hoje()
    inicio = hoje - dt.timedelta(days=3)
    antes, depois = hoje - dt.timedelta(days=6), hoje - dt.timedelta(days=1)

    def bater(dia, h):
        with db.conexao() as conn:
            m, _ = marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True,
                                       agora=dt.datetime(dia.year, dia.month, dia.day, h, 0,
                                                         tzinfo=horario.FUSO).astimezone(dt.timezone.utc))
        return m
    velha, nova = bater(antes, 7), bater(depois, 7)
    with banco.connect() as conn:                      # as duas em conferência
        conn.execute(text("UPDATE ponto.marcacoes SET status = 'EM_ANALISE', motivo_analise = 'teste' "
                          "WHERE id = ANY(:ids)"), {"ids": [velha["id"], nova["id"]]})
        conn.commit()
    dp = como(app, mundo["dp"])
    try:
        assert como(app, mundo["sup"]).post("/erp/api/ponto/vigencia", json={"inicio": inicio.isoformat()}).status_code in (403, 404)
        assert dp.post("/erp/api/ponto/vigencia", json={"inicio": inicio.isoformat()}).get_json()["inicio"] == inicio.isoformat()
        assert dp.get("/erp/api/ponto/vigencia").get_json()["inicio"] == inicio.isoformat()
        # o espelho: o dia de antes é "antes do ponto", sem batida e sem falta
        esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={antes:%Y-%m}").get_json()
        dia = next(d for d in esp["dias"] if d["data"] == antes.isoformat())
        assert dia["situacao"] == "ANTES_DO_PONTO" and dia["batidas"] == [] and not dia["falta"]
        # Validações: só a batida de depois espera decisão
        itens = dp.get("/erp/api/ponto/validacoes").get_json()["itens"]
        ids = {i["id"] for i in itens if i["tipo"] == "BATIDA_EM_ANALISE"}
        assert nova["id"] in ids and velha["id"] not in ids
        # os filtros só veem o depois
        obras = dp.get(f"/erp/api/ponto/obras-com-batida?de={antes.isoformat()}&ate={antes.isoformat()}").get_json()["obras"]
        assert obras == []
        # pedido de ajuste para o dia de antes: não entra
        r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"],
                    "data_inicio": antes.isoformat(), "horario": f"{antes.isoformat()}T11:00:00", "obra": "PG-A",
                    "descricao": "esqueceu de bater"})
        assert r.status_code == 400 and "vale a partir" in r.get_json()["erro"]
        # nada foi apagado
        with banco.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM ponto.marcacoes WHERE id = :i"), {"i": velha["id"]}).scalar() == 1
    finally:
        with banco.connect() as conn:                  # volta ao que o conftest pôs
            conn.execute(text("DELETE FROM ponto.parametros WHERE chave = :c"), {"c": vigencia.PARAMETRO})
            conn.commit()
        vigencia._guardado.clear()


def test_sem_nada_gravado_vale_16_de_outubro(monkeypatch):
    from app.apps.ponto.core import vigencia
    monkeypatch.delenv("PONTO_VALE_A_PARTIR_DE", raising=False)
    monkeypatch.setattr(vigencia, "_guardado", {})

    class Sem:
        pass
    monkeypatch.setattr(vigencia.db, "tem_coluna", lambda conn, t, c: False)
    assert vigencia.inicio(Sem()) == dt.date(2026, 10, 16)
