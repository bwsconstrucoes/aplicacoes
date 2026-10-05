# -*- coding: utf-8 -*-
"""Ponto — a tela de VALIDAÇÕES (tudo o que espera alguém, numa lista só, com
filtros) e a regra de QUEM VALIDA cada tipo, configurável. Postgres de verdade;
reusa o mundo de test_ponto_gestao_banco."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _entrar_no_app, _schema_ponto2,  # noqa: F401
                                           _segunda_passada, app, bater_via_chave, como, dia_util,
                                           local, mundo)

pytestmark = pytest.mark.banco


def _pedir_ajuste(cel, dia: dt.date, hora: str):
    r = cel.post("/ponto/app/api/pedidos/ajuste-do-dia",
                 json={"horarios": [f"{dia}T{hora}:00"], "motivo": "CELULAR", "obra": "PG-A"})
    assert r.status_code == 201, r.get_json()
    return r.get_json()["pedidos"][0]


def test_a_lista_junta_tudo_filtra_e_diz_quem_valida(app, mundo, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import marcacoes
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0)):
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    pedido = _pedir_ajuste(cel, seg, "17:00")
    with db.conexao() as conn:                     # Maria (obra B, sem coordenada): batida em conferência
        m, _ = marcacoes.registrar(conn, cpf=CPF_MARIA, obra="PG-A", origem="IDFACE", via_chave=True)
    assert m["status"] == "EM_ANALISE"

    dp, sup, fin = como(app, mundo["dp"]), como(app, mundo["sup"]), como(app, mundo["fin"])
    v = dp.get("/erp/api/ponto/validacoes").get_json()
    tipos = {i["tipo"] for i in v["itens"]}
    assert {"AJUSTE_BATIDA", "BATIDA_EM_ANALISE"} <= tipos
    aj = next(i for i in v["itens"] if i["tipo"] == "AJUSTE_BATIDA")
    assert aj["pessoa"] == "João Obra A" and aj["etapa"] == "DP" and aj["pode_decidir"] is True
    assert aj["decidir_em"].endswith(f"/afastamentos/{pedido['id']}/etapa-dp")
    assert v["regra"]["AJUSTE_BATIDA"] == "DP"
    # o supervisor da obra A vê o ajuste, mas o padrão é o DP: não decide
    vs = sup.get("/erp/api/ponto/validacoes").get_json()
    aj_s = next(i for i in vs["itens"] if i["tipo"] == "AJUSTE_BATIDA")
    assert aj_s["pode_decidir"] is False
    assert sup.get("/erp/api/ponto/validacoes?so_minhas=1").get_json()["quantidade"] == 0
    assert fin.get("/erp/api/ponto/validacoes").status_code == 403

    # filtros
    so_ajuste = dp.get("/erp/api/ponto/validacoes?tipos=AJUSTE_BATIDA").get_json()
    assert {i["tipo"] for i in so_ajuste["itens"]} == {"AJUSTE_BATIDA"}
    assert dp.get(f"/erp/api/ponto/validacoes?busca=maria").get_json()["por_tipo"] == {"BATIDA_EM_ANALISE": 1}
    assert dp.get(f"/erp/api/ponto/validacoes?busca={CPF_JOAO[:5]}").get_json()["por_tipo"] == {"AJUSTE_BATIDA": 1}
    longe = (seg - dt.timedelta(days=40)).isoformat()
    assert dp.get(f"/erp/api/ponto/validacoes?ate={longe}").get_json()["quantidade"] == 0
    so_b = dp.get(f"/erp/api/ponto/validacoes?obra=PG-B").get_json()
    assert all(i["obra"] == "PG-B" for i in so_b["itens"] if i["obra"])
    assert sup.get(f"/erp/api/ponto/validacoes?obra=PG-B").status_code == 404      # fora do alcance

    # o DP decide pelo endereço que a lista deu
    r = dp.post(aj["decidir_em"], json={"aprovar": True})
    assert r.status_code == 200 and r.get_json()["ocorrencia"]["status"] == "APROVADA"
    batida = next(i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"]
                  if i["tipo"] == "BATIDA_EM_ANALISE")
    assert batida["decidir_em"].endswith("/decidir-dp")
    assert dp.post(batida["decidir_em"], json={"para": "VALIDA"}).status_code == 200


def test_trocar_quem_valida_realinha_a_fila(app, mundo, monkeypatch):
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0)):
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    pedido = _pedir_ajuste(cel, seg, "17:00")
    assert pedido["status"] == "AGUARDANDO_DP"
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    tela = dp.get("/erp/api/ponto/quem-valida").get_json()
    assert {c["tipo"] for c in tela["configuraveis"]} >= {"AJUSTE_BATIDA", "BATIDA_EM_ANALISE"}
    assert any(f["tipo"] == "ATESTADO" for f in tela["fixos"])
    assert sup.post("/erp/api/ponto/quem-valida", json={"regra": {"AJUSTE_BATIDA": "ENCARREGADO"}}).status_code == 403
    assert dp.post("/erp/api/ponto/quem-valida", json={"regra": {"ATESTADO": "ENCARREGADO"}}).status_code == 400
    assert dp.post("/erp/api/ponto/quem-valida", json={"regra": {"BATIDA_EM_ANALISE": "ENCARREGADO_E_DP"}}).status_code == 400
    r = dp.post("/erp/api/ponto/quem-valida", json={"regra": {"AJUSTE_BATIDA": "ENCARREGADO"}})
    assert r.status_code == 200 and r.get_json()["pedidos_realinhados"] == 1
    item = next(i for i in sup.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["id"] == pedido["id"])
    assert item["pode_decidir"] is True and item["etapa"] == "Encarregado"
    # encarregado e depois o DP: o encarregado aprova e o pedido vai para o DP
    dp.post("/erp/api/ponto/quem-valida", json={"regra": {"AJUSTE_BATIDA": "ENCARREGADO_E_DP"}})
    passo = sup.post(item["decidir_em"], json={"aprovar": True})
    assert passo.get_json()["ocorrencia"]["status"] == "AGUARDANDO_DP"
    fim = dp.post(f"/erp/api/ponto/afastamentos/{pedido['id']}/etapa-dp", json={"aprovar": True})
    assert fim.get_json()["ocorrencia"]["status"] == "APROVADA"
