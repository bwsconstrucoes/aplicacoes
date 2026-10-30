# -*- coding: utf-8 -*-
"""Ponto — a troca de plantão identificada sozinha (31/10/2026, core/trocas.py): dois vigias da mesma obra no 12x36
automático, um trabalha a noite do outro e o outro devolve; o sistema propõe, o DP confirma, e o espelho mostra."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import CPF_JOAO, CPF_MARIA, _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401


def test_a_regra_pura():
    from app.apps.ponto.core.trocas import detectar
    dia = lambda n: f"2026-10-{n:02d}"                                       # noqa: E731
    adelson = {"pessoa": {"nome": "ADELSON"}, "dias": [
        {"data": dia(17), "obra_do_dia": "CF2", "alertas": ["TRABALHO_EM_DESCANSO", "FORA_DO_CICLO"]},
        {"data": dia(18), "obra_do_dia": "CF2", "situacao": "FALTA", "alertas": ["FALTA"]}]}
    adriano = {"pessoa": {"nome": "ADRIANO"}, "dias": [
        {"data": dia(17), "obra_do_dia": "CF2", "situacao": "FALTA", "alertas": ["FALTA"]},
        {"data": dia(18), "obra_do_dia": "CF2", "alertas": ["FORA_DO_CICLO"]}]}
    t = detectar({1: adelson, 2: adriano})
    assert len(t) == 1 and t[0]["a"] == {"id": 1, "nome": "ADELSON", "cobriu": dia(17), "deixou": dia(18)}
    assert t[0]["b"] == {"id": 2, "nome": "ADRIANO", "cobriu": dia(18), "deixou": dia(17)}
    # só cobriu (sem a volta): não é troca — fica a extra e a falta, para o DP
    assert detectar({1: adelson, 2: {**adriano, "dias": adriano["dias"][:1]}}) == []
    # obra diferente: não é troca
    outra = {**adriano, "dias": [{**x, "obra_do_dia": "OUTRA"} for x in adriano["dias"]]}
    assert detectar({1: adelson, 2: outra}) == []


@pytest.mark.banco
def test_a_troca_proposta_confirmada_e_no_espelho(app, mundo):
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    from tests.test_ponto_gestao_banco import bater_via_chave, local
    dp = como(app, mundo["dp"])
    base = _segunda_passada() - dt.timedelta(days=7)
    d = [base + dt.timedelta(days=n) for n in range(10)]
    if d[0].month != d[-1].month:
        pytest.skip("o período do teste atravessa o mês")
    with db.conexao() as conn:
        conn.execute(text("DELETE FROM ponto.colaborador_escalas WHERE colaborador_id = ANY(:ids)"),
                     {"ids": [mundo["joao"], mundo["maria"]]})
        noite = escalas.gravar(conn, {"nome": "Vigia 12x36 troca", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "05:00"})
        for cid in (mundo["joao"], mundo["maria"]):
            escalas.atribuir(conn, cid, noite["id"], d[0].isoformat(), por="teste")

    def noite_de(cpf, n):
        bater_via_chave(app, cpf, local(d[n], 17, 0))
        bater_via_chave(app, cpf, local(d[n] + dt.timedelta(days=1), 5, 0))
    for n in (0, 2, 3, 6, 8):          # o João, das noites pares, cobriu a 3 (da Maria)…
        noite_de(CPF_JOAO, n)
    for n in (1, 4, 5, 7, 9):          # … e a Maria, das ímpares, a 4 (dele)
        noite_de(CPF_MARIA, n)

    def espelho(cid):
        return dp.get(f"/erp/api/ponto/espelho/{cid}?competencia={base:%Y-%m}").get_json()
    e = espelho(mundo["joao"])
    assert len(e["trocas"]) == 1, e["trocas"]
    t = e["trocas"][0]
    eu = t["a"] if t["a"]["id"] == mundo["joao"] else t["b"]
    assert (eu["cobriu"], eu["deixou"]) == (d[3].isoformat(), d[4].isoformat())
    dia = {x["data"]: x for x in e["dias"]}
    assert dia[d[4].isoformat()]["situacao"] == "FALTA" and "FORA_DO_CICLO" in dia[d[3].isoformat()]["alertas"]
    # confirmada pelo DP: as duas compensações; a falta e a extra saem; o espelho diz com quem
    r = dp.post("/erp/api/ponto/trocas/confirmar", json={"colaborador_id": mundo["joao"], "cobriu": d[3].isoformat()})
    assert r.status_code == 200, r.get_json()
    assert len(r.get_json()["ocorrencias"]) == 2
    for cid, cobriu, deixou in ((mundo["joao"], d[3], d[4]), (mundo["maria"], d[4], d[3])):
        e = espelho(cid)
        dia = {x["data"]: x for x in e["dias"]}
        assert dia[deixou.isoformat()]["situacao"] == "ABONADO", dia[deixou.isoformat()]
        assert dia[deixou.isoformat()]["ocorrencia"]["rotulo"].startswith("Permuta com")
        assert dia[cobriu.isoformat()]["extra"] == 0 and dia[cobriu.isoformat()]["compensacao"]["rotulo"].startswith(
            "Permuta com")
        assert e["trocas"] == []
    # não se confirma duas vezes
    assert dp.post("/erp/api/ponto/trocas/confirmar",
                   json={"colaborador_id": mundo["joao"], "cobriu": d[3].isoformat()}).status_code == 400


@pytest.mark.banco
def test_a_permuta_lancada_a_mao_e_a_sugerida_nas_validacoes(app, mundo):
    """A permuta como pedido (31/10/2026, migração 024; o dono: "um tipo de ajuste chamado de permuta, que valerá
    somente pra vigias (…) o DP visualizasse isso e pudesse aplicar"): só entre dois do 12x36; aprovada, o lado do
    colega entra junto; o colega não pede a mesma de novo; e a sugestão das Validações some depois de aplicada."""
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas, trocas
    from tests.test_ponto_gestao_banco import bater_via_chave, local
    dp = como(app, mundo["dp"])
    base = _segunda_passada() - dt.timedelta(days=7)
    d = [base + dt.timedelta(days=n) for n in range(10)]
    if d[0].month != d[-1].month:
        pytest.skip("o período do teste atravessa o mês")

    def permuta(**extra):
        return dp.post("/erp/api/ponto/ocorrencias", json={
            "tipo": "PERMUTA", "colaborador_id": mundo["joao"], "colega_cpf": CPF_MARIA,
            "data_inicio": d[4].isoformat(), "dia_trabalhado": d[3].isoformat(), **extra})
    # sem 12x36: recusada, dizendo por quê
    r = permuta()
    assert r.status_code == 400 and "12x36" in r.get_json()["erro"], r.get_json()
    with db.conexao() as conn:
        conn.execute(text("DELETE FROM ponto.colaborador_escalas WHERE colaborador_id = ANY(:ids)"),
                     {"ids": [mundo["joao"], mundo["maria"]]})
        noite = escalas.gravar(conn, {"nome": "Vigia 12x36 permuta", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "05:00"})
        for cid in (mundo["joao"], mundo["maria"]):
            escalas.atribuir(conn, cid, noite["id"], d[0].isoformat(), por="teste")

    def noite_de(cpf, n):
        bater_via_chave(app, cpf, local(d[n], 17, 0))
        bater_via_chave(app, cpf, local(d[n] + dt.timedelta(days=1), 5, 0))
    for n in (0, 2, 3, 6, 8):
        noite_de(CPF_JOAO, n)
    for n in (1, 4, 5, 7, 9):
        noite_de(CPF_MARIA, n)
    # a sugestão das Validações acha a troca
    trocas.esquecer()
    hoje = d[-1] + dt.timedelta(days=2)
    with db.conexao() as conn:
        sug = trocas.sugeridas(conn, None, hoje=hoje)
    assert any({t["a"]["id"], t["b"]["id"]} == {mundo["joao"], mundo["maria"]} for t in sug), sug
    assert dp.get("/erp/api/ponto/trocas/sugeridas").status_code == 200
    # consigo mesmo: não
    r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "PERMUTA", "colaborador_id": mundo["joao"],
                "colega_cpf": CPF_JOAO, "data_inicio": d[4].isoformat(), "dia_trabalhado": d[3].isoformat()})
    assert r.status_code == 400
    # lançada à mão: esperando o DP; o pedido pendente tira a sugestão
    r = permuta(descricao="combinado entre eles")
    assert r.status_code == 201, r.get_json()
    o = r.get_json()["ocorrencia"]
    assert o["rotulo"] == "Permuta de plantão" and o["colega_id"] == mundo["maria"] and o["status"].startswith("AGUARDANDO")
    trocas.esquecer()
    with db.conexao() as conn:
        assert not any({t["a"]["id"], t["b"]["id"]} == {mundo["joao"], mundo["maria"]}
                       for t in trocas.sugeridas(conn, None, hoje=hoje))
    # o colega não pede a mesma troca pelo lado dele
    r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "PERMUTA", "colaborador_id": mundo["maria"],
                "colega_cpf": CPF_JOAO, "data_inicio": d[3].isoformat(), "dia_trabalhado": d[4].isoformat()})
    assert r.status_code == 400 and "colega" in r.get_json()["erro"], r.get_json()
    # aprovada pelo DP: o lado da Maria entra aprovado; a falta e a extra saem para os dois
    r = dp.post(f"/erp/api/ponto/afastamentos/{o['id']}/etapa-dp", json={"aprovar": True})
    assert r.status_code == 200, r.get_json()
    with db.conexao() as conn:
        lado = db.um(conn, "SELECT status, subtipo, colega_id, data_inicio, dia_trabalhado FROM ponto.ocorrencias "
                           "WHERE colaborador_id = :c AND subtipo = 'PERMUTA'", c=mundo["maria"])
    assert lado and lado["status"] == "APROVADA" and lado["colega_id"] == mundo["joao"]
    assert (lado["data_inicio"], lado["dia_trabalhado"]) == (d[3], d[4])
    for cid, cobriu, deixou in ((mundo["joao"], d[3], d[4]), (mundo["maria"], d[4], d[3])):
        e = dp.get(f"/erp/api/ponto/espelho/{cid}?competencia={base:%Y-%m}").get_json()
        dia = {x["data"]: x for x in e["dias"]}
        assert dia[deixou.isoformat()]["situacao"] == "ABONADO", dia[deixou.isoformat()]
        assert dia[deixou.isoformat()]["ocorrencia"]["rotulo"].startswith("Permuta com")
        assert dia[cobriu.isoformat()]["extra"] == 0
        assert e["trocas"] == []
