# -*- coding: utf-8 -*-
"""Ponto — o "esqueci de bater" entregue no PONTO DA OBRA (10/10/2026): o dia da
pessoa abre com os horários da escala já postos, e ela pede um, dois, o dia
inteiro, ou intercalado — e outro dia na mesma identificação. Postgres de verdade."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, _segunda_passada, app,  # noqa: F401
                                           bater_via_chave, como, dia_util, local, mundo)

pytestmark = pytest.mark.banco


def _tablet_da_obra(app, mundo, uuid="tablet-ajuste-do-dia-0123456789"):
    from app.apps.ponto import db
    from app.apps.ponto.core import dispositivos
    c = app.test_client()
    token = c.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    dp = como(app, mundo["dp"])
    aid = next(a["id"] for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"]
               if a["device_uuid"] == uuid)
    with db.conexao() as conn:
        dispositivos.aprovar(conn, aid, perfil="COMPARTILHADO", aprovado_por="teste", obras=[mundo["obra_a"]])
    return c, h


def test_ajuste_no_ponto_da_obra_abre_o_dia_e_pede_varios_horarios(app, mundo, banco):
    seg = _segunda_passada()
    ter = dia_util(seg, 1)
    for hm in ((7, 0), (11, 0)):                                  # segunda: só a manhã
        bater_via_chave(app, CPF_JOAO, local(seg, *hm))
    tab, h = _tablet_da_obra(app, mundo)
    ident = tab.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "para_pedido": True},
                     headers=h).get_json()
    b = ident["bilhete"]

    # o dia vem com o que bateu, o que a escala esperava e o que falta (já sugerido)
    r = tab.post("/ponto/app/api/tablet/dia", json={"bilhete": b, "data": seg.isoformat()}, headers=h)
    assert r.status_code == 200, r.get_json()
    x = r.get_json()["dia"]
    assert [p["hora"] for p in x["previstas"]] == ["07:00", "11:00", "12:00", "17:00"]
    assert [f["sugestao"] for f in x["faltando"]] == ["12:00", "17:00"]
    assert r.get_json()["competencia_fechada"] is False
    futuro = tab.post("/ponto/app/api/tablet/dia",
                      json={"bilhete": b, "data": (dt.date.today() + dt.timedelta(days=3)).isoformat()}, headers=h)
    assert futuro.status_code == 400

    # os dois que faltaram, de uma vez
    p = tab.post("/ponto/app/api/tablet/ajuste-do-dia", headers=h, json={
        "bilhete": b, "horarios": [f"{seg}T12:00:00", f"{seg}T17:00:00"], "motivo": "ESQUECI",
        "descricao": "esqueci de bater a tarde", "obra_do_aparelho": "PG-A"})
    assert p.status_code == 201, p.get_json()
    assert p.get_json()["quantidade"] == 2
    depois = tab.post("/ponto/app/api/tablet/dia", json={"bilhete": b, "data": seg.isoformat()},
                      headers=h).get_json()["dia"]
    assert depois["faltando"] == [] and len(depois["pendentes"]) == 2

    # outro dia, na mesma identificação, intercalado (a entrada e a saída)
    q = tab.post("/ponto/app/api/tablet/ajuste-do-dia", headers=h, json={
        "bilhete": b, "horarios": [f"{ter}T07:00:00", f"{ter}T17:00:00"], "motivo": "ESQUECI",
        "descricao": "celular ficou em casa", "obra_do_aparelho": "PG-A"})
    assert q.status_code == 201, q.get_json()
    with banco.connect() as conn:
        origens = conn.execute(text("SELECT origem, count(*) FROM ponto.ocorrencias "
                                    "WHERE tipo = 'AJUSTE_BATIDA' GROUP BY origem")).all()
    assert origens == [("APARELHO", 4)]

    # sem identificação válida, nada
    assert tab.post("/ponto/app/api/tablet/dia", json={"bilhete": "x", "data": seg.isoformat()},
                    headers=h).status_code in (400, 401, 403)
