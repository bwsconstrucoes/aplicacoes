# -*- coding: utf-8 -*-
"""Ponto — MENOS TELAS E UM FILTRO SÓ (27/10/2026). O dono: "tem tela demais para pouca
utilidade (…) precisa ser facilitado o filtro dos funcionários de uma maneira geral". O
filtro de funcionários é o mesmo no Quadro, nas Validações e nos Relatórios; Alertas,
Qualidade e Rondas viraram partes do Quadro. Postgres de verdade."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, _segunda_passada, app,  # noqa: F401
                                           bater_via_chave, como, dia_util, local, mundo)


def test_o_menu_tem_quatro_telas_e_as_outras_continuam_abrindo():
    from app.apps.ponto import gestao
    assert [a[0] for a in gestao.ABAS] == ["ponto_quadro", "ponto_pendencias", "ponto_relatorios", "ponto_config"]
    assert {"ponto_mosaico", "ponto_banco", "ponto_alertas", "ponto_qualidade",
            "ponto_rondas"} <= set(gestao.ABAS_FORA_DO_MENU)
    # o espelho e o cadastro moram na janela da pessoa (28/10/2026)
    assert not {"ponto_espelho", "ponto_pessoas"} & set(gestao.ABAS_FORA_DO_MENU)


def test_filtro_pessoas_ativo_e_argumentos():
    from app.apps.ponto.core import filtro_pessoas as fp
    assert not fp.ativo({}) and fp.ativo({"funcoes": ["PEDREIRO"]})
    f = fp.de_argumentos({"pessoa": "joão", "funcoes": "PEDREIRO,SERVENTE", "tipos_pessoa": "VIGIA"})
    assert f["busca"] == "joão" and f["funcoes"] == ["PEDREIRO", "SERVENTE"] and f["tipos_pessoa"] == ["VIGIA"]


@pytest.mark.banco
def test_o_mesmo_filtro_em_todas_as_telas(app, mundo):
    from app.apps.ponto import db
    seg = _segunda_passada()
    dp = como(app, mundo["dp"])
    op = dp.get("/erp/api/ponto/filtros/pessoas").get_json()
    assert op["pessoas"] >= 2 and {t["codigo"] for t in op["tipos"]} >= {"VIGIA", "SEM_PIN"}
    assert any(e["nome"] == "Obra 44h" for e in op["escalas"])
    # um pedido de cada pessoa: o filtro de funcionário recorta as Validações
    for cid in (mundo["joao"], mundo["maria"]):
        r = dp.post("/erp/api/ponto/ocorrencias", json={
            "tipo": "AJUSTE_BATIDA", "colaborador_id": cid, "data_inicio": seg.isoformat(),
            "horario": f"{seg.isoformat()}T07:00:00", "obra": "PG-A", "descricao": "esqueceu de bater a entrada"})
        assert r.status_code == 201, r.get_json()
    todos = dp.get("/erp/api/ponto/validacoes").get_json()["itens"]
    assert {mundo["joao"], mundo["maria"]} <= {i.get("colaborador_id") for i in todos}
    so_joao = dp.get("/erp/api/ponto/validacoes?pessoa=joão").get_json()["itens"]
    assert {i["colaborador_id"] for i in so_joao} == {mundo["joao"]}
    pela_escala = dp.get("/erp/api/ponto/validacoes?escalas=Obra 44h").get_json()["itens"]
    assert {mundo["joao"], mundo["maria"]} <= {i["colaborador_id"] for i in pela_escala}
    assert dp.get("/erp/api/ponto/validacoes?escalas=Outra").get_json()["itens"] == []
    # o alerta aberto aparece na linha do Quadro e tem filtro
    with db.conexao() as conn:
        db.executar(conn, """INSERT INTO ponto.alertas (chave, codigo, gravidade, colaborador_id, data_referencia, mensagem, situacao)
                             VALUES ('teste-1', 'FALTA', 'ATENCAO', :c, :d, 'faltou sem aviso', 'ABERTO')""",
                    c=mundo["maria"], d=seg)
    q = dp.post("/erp/api/ponto/quadro", json={"de": seg.isoformat(), "filtros": {"situacoes": ["COM_ALERTA"]}}).get_json()
    assert [l["colaborador_id"] for l in q["linhas"]] == [mundo["maria"]] and q["linhas"][0]["alertas"][0]["mensagem"]
    # a qualidade e as rondas do recorte, com o mesmo filtro
    bater_via_chave(app, CPF_JOAO, local(seg, 7, 0))
    r = dp.post("/erp/api/ponto/quadro/qualidade", json={"de": seg.isoformat(), "ate": seg.isoformat(),
                                                         "filtros": {"busca": "joão"}})
    assert r.status_code in (200, 503), r.get_json()
    if r.status_code == 200:
        assert {p["id"] for p in r.get_json()["pessoas"]} <= {mundo["joao"]}
    r = dp.post("/erp/api/ponto/quadro/rondas", json={"de": seg.isoformat(), "ate": dia_util(seg, 1).isoformat()})
    assert r.status_code == 200 and "turnos" in r.get_json()
    assert como(app, mundo["fin"]).get("/erp/api/ponto/filtros/pessoas").status_code == 403
    # os relatórios também filtram por escala
    rel = dp.post("/erp/api/ponto/relatorios", json={"tipo": "faltas", "de": seg.isoformat(), "ate": seg.isoformat(),
                                                    "filtros": {"escalas": ["Outra"]}}).get_json()
    assert rel["pessoas_no_recorte"] == 0
