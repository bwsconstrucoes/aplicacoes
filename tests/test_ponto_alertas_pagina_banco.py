# -*- coding: utf-8 -*-
"""Ponto — a tela de Alertas com período, obra, pessoa, ordem e páginas (10/11/2026, o dono: "está exibindo 500 por
página, só que não tem paginação (…) não posso filtrar por data, período (…) só coisa do mês anterior, para tratar")."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_alertas_por_periodo_obra_pessoa_e_pagina(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import vigencia
    with db.conexao() as conn:
        db.executar(conn, "DELETE FROM ponto.alertas")
        vale = max(vigencia.inicio(conn), dt.date.today() - dt.timedelta(days=40))
        antigo, recente = vale + dt.timedelta(days=1), vale + dt.timedelta(days=20)
        for i in range(130):                                           # 130 do dia antigo, na obra A, do João
            db.executar(conn, """INSERT INTO ponto.alertas (chave, codigo, gravidade, colaborador_id, obra_id,
                                        data_referencia, mensagem) VALUES (:k, 'ATRASO', 'INFO', :c, :o, :d, 'atraso')""",
                        k=f"T:{i}", c=mundo["joao"], o=mundo["obra_a"], d=antigo)
        for i in range(5):                                             # 5 do dia recente, na obra B, da Maria
            db.executar(conn, """INSERT INTO ponto.alertas (chave, codigo, gravidade, colaborador_id, obra_id,
                                        data_referencia, mensagem) VALUES (:k, 'FALTA', 'ATENCAO', :c, :o, :d, 'falta')""",
                        k=f"R:{i}", c=mundo["maria"], o=mundo["obra_b"], d=recente)
        db.executar(conn, """INSERT INTO ponto.alertas (chave, codigo, gravidade, mensagem)
                             VALUES ('SEM_DIA', 'APARELHO_PENDENTE', 'INFO', 'aparelho esperando')""")
    dp = como(app, mundo["dp"])
    api = lambda q="": dp.get(f"/erp/api/ponto/alertas?{q}").get_json()  # noqa: E731
    # tudo: 136, em páginas de 100
    d = api()
    assert d["total"] == 136 and d["paginas"] == 2 and len(d["alertas"]) == 100 and d["pagina"] == 1
    assert len(api("pagina=2")["alertas"]) == 36
    assert api("pagina=99")["pagina"] == 2                              # página além do fim: a última
    assert api("por_pagina=50")["paginas"] == 3
    assert api("por_pagina=7")["por_pagina"] == 100                    # só 50, 100 ou 200
    # o período é o DIA do alerta; com período, o alerta sem dia não entra
    d = api(f"de={recente}&ate={recente}")
    assert d["total"] == 5 and {a["codigo"] for a in d["alertas"]} == {"FALTA"}
    d = api(f"de={antigo}&ate={antigo}")
    assert d["total"] == 130 and {t["codigo"]: t["n"] for t in d["tipos"]} == {"ATRASO": 130}
    assert api(f"de={recente}&ate={antigo}")["total"] == 135          # de e até trocados: o sistema desvira
    assert dp.get("/erp/api/ponto/alertas?de=ontem").status_code == 400
    # a obra e a pessoa; as contagens por obra seguem os outros filtros
    d = api(f"obra={mundo['obra_b']}")
    assert d["total"] == 5
    assert {o["codigo"]: o["n"] for o in d["obras"]} == {"PG-A": 130, "PG-B": 5, "sem obra": 1}
    assert api("busca=maria")["total"] == 5
    # a ordem: os mais antigos primeiro (dentro da gravidade)
    d = api("ordem=ANTIGOS&gravidade=INFO&por_pagina=200")
    assert d["alertas"][0]["data_referencia"] == antigo.isoformat() and d["alertas"][-1]["codigo"] == "APARELHO_PENDENTE"
