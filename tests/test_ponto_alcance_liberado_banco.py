# -*- coding: utf-8 -*-
"""Ponto — o alcance liberado pelo DP no aplicativo (31/10/2026, migração 023): obras e pessoas específicas que uma
pessoa consulta e por quem pede, "mesmo que não sejam elas que batem o ponto dessas pessoas"."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import CPF_JOAO, CPF_MARIA, _schema_ponto2, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_liberar_obras_e_pessoas_para_alguem(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros, papeis
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])

    def alcance():
        with db.conexao() as conn:
            return papeis.alcance(conn, cadastros.colaborador_por_id(conn, mundo["joao"]), None, None)
    a = alcance()
    assert a.vazio and not a.pode_pedir                    # sem papel nenhum: só o próprio ponto
    r = dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/forma-de-bater",
                json={"liberado_obras": ["PG-B"], "liberado_pessoas": [CPF_MARIA]})
    assert r.status_code == 200, r.get_json()
    lib = r.get_json()["liberado"]
    assert [o["codigo"] for o in lib["obras"]] == ["PG-B"] and [x["id"] for x in lib["pessoas"]] == [mundo["maria"]]
    a = alcance()
    assert mundo["obra_b"] in a.obras and mundo["maria"] in a.equipe and a.pode_pedir and a.papel == "LIBERADO"
    assert any("liberada pelo DP" in x for x in a.de_onde)
    with db.conexao() as conn:
        assert papeis.no_alcance(conn, a, mundo["maria"])
    # a lista de quem ela alcança, no ERP; e a obra de cada um
    d = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/alcance-no-app").get_json()
    assert d["pode_pedir"] is True and mundo["maria"] in {x["id"] for x in d["pessoas"]}
    assert all("obra_aqui" in x for x in d["pessoas"])
    # aparece na ficha
    ficha = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]
    assert ficha["liberado"]["obras"][0]["codigo"] == "PG-B"
    # o supervisor da obra A não libera a obra B (fora do alcance dele)
    assert sup.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/forma-de-bater",
                    json={"liberado_obras": ["PG-B"]}).status_code in (400, 403, 404)
    # CPF que não existe
    assert dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/forma-de-bater",
                   json={"liberado_pessoas": ["11122233396"]}).status_code == 400
    # tirar: lista vazia
    dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/forma-de-bater", json={"liberado_obras": [], "liberado_pessoas": []})
    assert alcance().vazio
