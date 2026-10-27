# -*- coding: utf-8 -*-
"""Ponto — a negativa avisada a quem pediu (28/10/2026, core/negativas.py, migração 014): o
aviso na tela inicial do aplicativo, para a pessoa e para quem pediu por ela; o negado no dia
do mês; o WhatsApp por padrão, que quem nega pode desligar; e os negados do mesmo dia numa
mensagem só."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.ponto.core import negativas as ng
from tests.test_ponto_gestao_banco import (CPF_JOAO, OBRA_A, _entrar_no_app, _schema_ponto2,  # noqa: F401
                                           _segunda_passada, app, como, mundo)
from tests.test_ponto_papeis_banco import CPF_CARLOS, _papeis, carlos  # noqa: F401


# ---------------------------------------------------------------------------
# As partes puras
# ---------------------------------------------------------------------------
def test_a_mensagem_diz_o_que_foi_negado_e_o_motivo():
    um = ng.texto_da_mensagem(destinatario="JOÃO DA SILVA", pessoa="JOÃO DA SILVA", para_quem_pediu=False,
                              negados=[{"rotulo": "Ajuste de batida", "quando": "21/10 às 17:00",
                                        "motivo": "não estava na obra"}])
    assert um.startswith("BWS Ponto — João, um pedido seu foi NEGADO.")
    assert "21/10 às 17:00" in um and "Motivo: não estava na obra" in um and "peça de novo" in um
    dois = ng.texto_da_mensagem(destinatario="CARLOS ADM", pessoa="JOÃO DA SILVA", para_quem_pediu=True,
                                negados=[{"rotulo": "Ajuste de batida", "quando": "21/10 às 07:00", "motivo": "x" * 5},
                                         {"rotulo": "Ajuste de batida", "quando": "21/10 às 11:00", "motivo": "x" * 5}])
    assert "2 pedidos que você fez para JOÃO DA SILVA foram NEGADOS" in dois
    assert "21/10 às 07:00 e 11:00" in dois and dois.count("Motivo:") == 1      # o mesmo motivo vai uma vez


def test_motivos_diferentes_ficam_em_linhas_diferentes():
    j = ng.juntar([{"rotulo": "Ajuste de batida", "quando": "21/10 às 07:00", "motivo": "a"},
                   {"rotulo": "Ajuste de batida", "quando": "21/10 às 11:00", "motivo": "b"},
                   {"rotulo": "Atestado", "quando": "21/10", "motivo": "a"}])
    assert [x["quando"] for x in j] == ["21/10 às 07:00", "21/10 às 11:00", "21/10"]


def test_o_negado_sai_do_dia_quando_o_assunto_se_resolve():
    n = {"tipo": "AJUSTE_BATIDA", "hora": "17:00"}
    assert ng.ainda_em_aberto(n, {"batidas": [{"hora": "07:00"}], "pendentes": []})
    assert not ng.ainda_em_aberto(n, {"batidas": [{"hora": "17:10"}], "pendentes": []})      # a batida apareceu
    assert not ng.ainda_em_aberto(n, {"batidas": [], "pendentes": [{"hora": "17:00", "tipo": "AJUSTE_BATIDA"}]})
    atestado = {"tipo": "ATESTADO", "hora": None}
    assert ng.ainda_em_aberto(atestado, {"batidas": [], "pendentes": [], "ocorrencia": None})
    assert not ng.ainda_em_aberto(atestado, {"batidas": [], "pendentes": [], "ocorrencia": {"id": 1}})


# ---------------------------------------------------------------------------
# Com Postgres
# ---------------------------------------------------------------------------
def _negar(app, mundo, ids, motivo="não estava na obra nesse dia", **extra):
    dp = como(app, mundo["dp"])
    itens = {i["id"]: i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["categoria"] == "PEDIDO"}
    saidas = []
    for i in ids:
        r = dp.post(itens[i]["decidir_em"], json={"aprovar": False, "motivo": motivo, **extra})
        assert r.status_code == 200, r.get_json()
        saidas.append(r.get_json()["ocorrencia"])
    return saidas


def _pedir_pelo_administrativo(app, mundo, monkeypatch, dia, horas):
    _papeis(app, mundo, mundo["carlos"], administrativo_obra=True)
    c = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    r = c.post(f"/ponto/app/api/equipe/{mundo['joao']}/ajuste-do-dia",
               json={"horarios": [f"{dia.isoformat()}T{h}:00" for h in horas], "motivo": "ESQUECI",
                     "descricao": "esqueceu de bater", "obra": "PG-A",
                     "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10})
    assert r.status_code == 201, r.get_json()
    return c, [p["id"] for p in r.get_json()["pedidos"]]


@pytest.mark.banco
def test_a_pessoa_e_quem_pediu_sao_avisados_no_app_e_numa_mensagem_so(app, mundo, carlos, monkeypatch, banco):
    mundo = {**mundo, "carlos": carlos}
    dia = _segunda_passada()
    c, ids = _pedir_pelo_administrativo(app, mundo, monkeypatch, dia, ["07:00", "11:00"])
    with banco.connect() as conn:
        sol = conn.execute(text("SELECT DISTINCT solicitado_colaborador_id FROM ponto.ocorrencias WHERE id = ANY(:i)"),
                           {"i": ids}).scalars().all()
    assert sol == [carlos]                                     # C: quem pediu ficou guardado
    saidas = _negar(app, mundo, ids)
    assert set(saidas[0]["aviso_da_negativa"]["whatsapp"]) == {"João Obra A", "Carlos Administrativo"}
    # D: UMA mensagem por destinatário, com os dois horários (negar o dia não manda 2 WhatsApps)
    with banco.connect() as conn:
        envios = conn.execute(text("SELECT colaborador_id, texto, agendado_para > now() AS espera FROM ponto.envios "
                                   "WHERE tipo = 'NEGATIVA' ORDER BY colaborador_id")).all()
    assert sorted(e.colaborador_id for e in envios) == sorted([mundo["joao"], carlos])
    for e in envios:
        assert "07:00" in e.texto and "11:00" in e.texto and "não estava na obra" in e.texto and e.espera
    assert any("que você fez para João Obra A" in e.texto for e in envios)
    # A: o aviso na tela inicial — para o Carlos, "o que você pediu para o João"
    avisos = c.get("/ponto/app/api/inicio").get_json()["negativas"]
    assert len(avisos) == 2 and all(a["para_outro"] and a["pessoa"] == "João Obra A" for a in avisos)
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    meus = joao.get("/ponto/app/api/inicio").get_json()["negativas"]
    assert len(meus) == 2 and not meus[0]["para_outro"] and meus[0]["motivo"] == "não estava na obra nesse dia"
    # tocado, some só para quem tocou
    assert c.post(f"/ponto/app/api/negativas/{ids[0]}/vista", json={}).status_code == 200
    assert len(c.get("/ponto/app/api/inicio").get_json()["negativas"]) == 1
    assert len(joao.get("/ponto/app/api/inicio").get_json()["negativas"]) == 2
    assert joao.post("/ponto/app/api/negativas/999999/vista", json={}).status_code == 404
    # B: no Meu mês, o dia diz que foi negado e por quê
    mes = joao.get(f"/ponto/app/api/mes?competencia={dia:%Y-%m}").get_json()
    d = next(x for x in mes["dias"] if x["data"] == dia.isoformat())
    assert {n["hora"] for n in d["negados"]} == {"07:00", "11:00"}
    assert all(n["motivo"] == "não estava na obra nesse dia" for n in d["negados"])


@pytest.mark.banco
def test_quem_nega_pode_nao_mandar_o_whatsapp(app, mundo, banco):
    dia = _segunda_passada()
    dp = como(app, mundo["dp"])
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"], "data_inicio": dia.isoformat(),
        "horario": f"{dia.isoformat()}T17:00:00", "obra": "PG-A", "descricao": "esqueceu de bater"})
    assert r.status_code == 201, r.get_json()
    pid = r.get_json()["ocorrencia"]["id"]
    saida = _negar(app, mundo, [pid], avisar=False)[0]
    assert saida["aviso_da_negativa"] == {"whatsapp": [], "sem_telefone": []}
    with banco.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM ponto.envios WHERE tipo = 'NEGATIVA'")).scalar() == 0
        assert conn.execute(text("SELECT negativa_whatsapp FROM ponto.ocorrencias WHERE id = :i"), {"i": pid}).scalar() is False
    # o aviso no aplicativo aparece de qualquer jeito (o pedido da gestão não tem "quem pediu" no app)
    from app.apps.ponto import db
    with db.conexao() as conn:
        assert [a["id"] for a in ng.avisos(conn, mundo["joao"])] == [pid]
