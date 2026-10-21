# -*- coding: utf-8 -*-
"""Ponto — O QUADRO DO PONTO (21/10/2026, core/quadro.py): todas as pessoas, dia a
dia, com as batidas e os buracos, e a INCLUSÃO EM LOTE da batida que faltou. O
dono: "para evitar uma enxurrada de pedidos, é melhor que a gestão faça isso em
lote". Postgres de verdade; reusa o mundo de test_ponto_gestao_banco."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _schema_ponto2, _segunda_passada,  # noqa: F401
                                           app, bater_via_chave, como, dia_util, local, mundo)

pytestmark = pytest.mark.banco


def _quadro(cliente, de, ate=None, **filtros):
    r = cliente.post("/erp/api/ponto/quadro", json={"de": de.isoformat(), "ate": (ate or de).isoformat(),
                                                    "filtros": filtros})
    assert r.status_code == 200, r.get_json()
    return r.get_json()


def test_o_quadro_mostra_cada_dia_e_filtra_os_buracos(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import espelho
    seg = _segunda_passada()
    ter = dia_util(seg, 1)
    for h in ((7, 0), (11, 0), (12, 0)):                # João: esqueceu a saída na segunda
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    for h in ((7, 0), (11, 0), (12, 0), (17, 0)):       # e na terça bateu tudo
        bater_via_chave(app, CPF_JOAO, local(ter, *h))
    dp = como(app, mundo["dp"])
    q = _quadro(dp, seg, ter)
    joao = {l["data"]: l for l in q["linhas"] if l["colaborador_id"] == mundo["joao"]}
    assert [b["hora"] for b in joao[seg.isoformat()]["batidas"]] == ["07:00", "11:00", "12:00"]
    assert [f["rotulo"] for f in joao[seg.isoformat()]["faltando"]] == ["Saída"]
    assert "COM_BURACO" in joao[seg.isoformat()]["etiquetas"]
    assert "COMPLETO" in joao[ter.isoformat()]["etiquetas"]
    maria = {l["data"]: l for l in q["linhas"] if l["colaborador_id"] == mundo["maria"]}
    assert maria and all("SEM_BATIDA" in l["etiquetas"] for l in maria.values())
    # o quadro e o espelho dizem a mesma coisa, dia a dia
    with db.conexao() as conn:
        esp = {d["data"]: d for d in espelho.montar(conn, mundo["joao"], seg, ter)["dias"]}
    for data, l in joao.items():
        assert l["situacao"] == esp[data]["situacao"] and l["faltando"] == esp[data]["faltando"]
    # filtros do dia
    so_buraco = _quadro(dp, seg, ter, situacoes=["COM_BURACO"])
    assert {(l["colaborador_id"], l["data"]) for l in so_buraco["linhas"]} >= {(mundo["joao"], seg.isoformat())}
    assert all("COM_BURACO" in l["etiquetas"] for l in so_buraco["linhas"])
    tres = _quadro(dp, seg, ter, batidas=[3])
    assert {(l["colaborador_id"], l["data"]) for l in tres["linhas"]} == {(mundo["joao"], seg.isoformat())}
    sem_saida = _quadro(dp, seg, ter, faltando=["Saída"], busca="joão")
    assert [l["data"] for l in sem_saida["linhas"]] == [seg.isoformat()]
    assert q["contagem"]["COM_BATIDA"] >= 2 and q["por_dia"][0]["data"] == seg.isoformat()
    # quem não é do ponto não vê; o supervisor só a obra dele
    assert como(app, mundo["fin"]).post("/erp/api/ponto/quadro", json={"de": seg.isoformat()}).status_code == 403
    sup = _quadro(como(app, mundo["sup"]), seg, ter)
    assert {l["colaborador_id"] for l in sup["linhas"]} <= {mundo["joao"], mundo["maria"]}


def test_incluir_em_lote_a_batida_que_faltou(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import qualidade
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0)):
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    dp = como(app, mundo["dp"])
    item = {"colaborador_id": mundo["joao"], "data": seg.isoformat()}
    assert dp.post("/erp/api/ponto/quadro/incluir", json={"itens": [item], "rotulo": "Saída",
                                                          "motivo": "curto"}).status_code == 400
    assert como(app, mundo["fin"]).post("/erp/api/ponto/quadro/incluir", json={}).status_code == 403
    r = dp.post("/erp/api/ponto/quadro/incluir", json={
        "itens": [item, {"colaborador_id": mundo["maria"], "data": seg.isoformat()}], "rotulo": "Saída",
        "motivo": "falha do sistema em 21/10/2026 — o aplicativo não abriu na saída"}).get_json()
    joao, maria = r["resultados"]
    assert joao["ok"] and joao["hora"] == "17:00" and joao["aprovado"] is True, joao
    assert maria["ok"] is False and "nenhuma batida" in maria["erro"]  # Maria faltou: não se inventa meio dia
    # o dia ficou completo, a batida diz que foi em lote, e não pesa na qualidade
    q = _quadro(dp, seg, busca="joão")
    linha = q["linhas"][0]
    assert "COMPLETO" in linha["etiquetas"] and [b["hora"] for b in linha["batidas"]][-1] == "17:00"
    with db.conexao() as conn:
        b = db.um(conn, "SELECT status, motivo_analise FROM ponto.marcacoes WHERE colaborador_id = :c "
                        "ORDER BY timestamp_servidor DESC LIMIT 1", c=mundo["joao"])
        assert b["status"] == "AJUSTADA" and b["motivo_analise"].startswith("incluída em lote")
        qualidade.esquecer()
        nota = qualidade.da_pessoa(conn, mundo["joao"], seg, seg)
    assert nota["ajustes"] == 0 and nota["dias_em_dia"] == 1
    # de novo: já não falta
    r2 = dp.post("/erp/api/ponto/quadro/incluir", json={"itens": [item], "rotulo": "Saída",
                 "motivo": "falha do sistema em 21/10/2026 — de novo"}).get_json()
    assert r2["resultados"][0]["ok"] is False and r2["feitas"] == 0


def test_horario_unico_para_todos_e_o_lote_tem_teto(app, mundo):
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0)):
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    dp = como(app, mundo["dp"])
    r = dp.post("/erp/api/ponto/quadro/incluir", json={
        "itens": [{"colaborador_id": mundo["joao"], "data": seg.isoformat()}], "rotulo": "Saída", "hora": "17:10",
        "motivo": "falha do sistema na saída — horário informado pelo encarregado"}).get_json()
    assert r["resultados"][0]["hora"] == "17:10"
    muitos = [{"colaborador_id": mundo["joao"], "data": seg.isoformat()}] * 41
    assert dp.post("/erp/api/ponto/quadro/incluir", json={"itens": muitos, "rotulo": "Saída",
                   "motivo": "falha do sistema na saída"}).status_code == 400


def test_etiquetas_do_dia_puras():
    from app.apps.ponto.core.quadro import etiquetas_do_dia, passa_no_filtro
    falta = {"batidas": [], "faltando": [{"rotulo": "Entrada"}, {"rotulo": "Saída"}], "situacao": "FALTA",
             "previstas": [{"hora": "07:00"}, {"hora": "17:00"}]}
    assert etiquetas_do_dia(falta) == {"SEM_BATIDA", "FALTA"}                 # faltou não é "batida faltando"
    assert not passa_no_filtro(falta, etiquetas_do_dia(falta), {"faltando": ["Saída"]})
    meio = {"batidas": [{"hora": "07:00"}], "faltando": [{"rotulo": "Saída"}], "situacao": "INCOMPLETO",
            "previstas": [{"hora": "07:00"}, {"hora": "17:00"}]}
    assert etiquetas_do_dia(meio) == {"COM_BATIDA", "COM_BURACO"}
    assert passa_no_filtro(meio, etiquetas_do_dia(meio), {"faltando": ["Saída"], "batidas": [1]})
    assert not passa_no_filtro(meio, etiquetas_do_dia(meio), {"batidas": [4]})
    cheio = {"batidas": [{}] * 4, "faltando": [], "situacao": "OK", "previstas": [{}] * 4}
    assert "COMPLETO" in etiquetas_do_dia(cheio)
    folga = {"batidas": [], "faltando": [], "situacao": "DESCANSO", "previstas": []}
    assert etiquetas_do_dia(folga) == {"SEM_BATIDA", "NAO_TRABALHA"}


def test_etiquetas_da_pessoa_puras():
    from app.apps.ponto.core import etiquetas
    vigia = {"tipo_jornada": "VIGIA_NOTURNO_2", "faz_ronda": False}
    assert etiquetas.calcular(vigia, [], True) == ["VIGIA", "VIGIA_SEM_RONDA", "VIGIA_SEM_CELULAR"]
    certo = {"tipo_jornada": "VIGIA_NOTURNO_2", "faz_ronda": True}
    assert etiquetas.calcular(certo, [{"perfil": "INDIVIDUAL", "status": "APROVADO"}], True) == ["VIGIA", "RONDA", "CELULAR"]
    resp = {"tipo_jornada": "PADRAO_4", "administrativo_obra": True}
    assert etiquetas.calcular(resp, [{"perfil": "COMPARTILHADO", "status": "APROVADO"},
                                     {"perfil": "INDIVIDUAL", "status": "PENDENTE"}], False) == \
        ["CELULAR_PENDENTE", "PONTO_OBRA", "ADMINISTRATIVO", "SEM_PIN"]
    assert etiquetas.casa_com_filtro(["VIGIA", "VIGIA_SEM_RONDA"], {"VIGIA_PENDENTE"})
    assert not etiquetas.casa_com_filtro(["VIGIA", "RONDA", "CELULAR"], {"VIGIA_PENDENTE"})


def test_painel_da_pessoa_e_local_da_batida(app, mundo, banco):
    from sqlalchemy import text
    seg = _segunda_passada()
    m = bater_via_chave(app, CPF_JOAO, local(seg, 7, 0))
    with banco.connect() as conn:                       # o celular estava a uns 300 m da obra
        conn.execute(text("UPDATE ponto.marcacoes SET latitude = -3.7300, longitude = -38.5300 WHERE id = :i"),
                     {"i": m["id"]})
        conn.commit()
    dp = como(app, mundo["dp"])
    r = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/painel?data={seg.isoformat()}").get_json()
    assert r["pessoa"]["id"] == mundo["joao"] and isinstance(r["pessoa"]["etiquetas"], list)
    assert [b["hora"] for b in r["dia"]["batidas"]] == ["07:00"] and r["data"] == seg.isoformat()
    assert r["mes"]["competencia"] == seg.strftime("%Y-%m") and any(d["data"] == seg.isoformat() for d in r["mes"]["dias"])
    assert "pedidos" in r and "em_conferencia" in r and r["no_aparelho_dela"] == []
    loc = dp.get(f"/erp/api/ponto/marcacoes/{m['id']}/local").get_json()
    assert loc["obra"]["codigo"] == "PG-A" and loc["latitude"] == -3.73 and loc["distancia_metros"] > 0
    # fora do alcance: não encontrada (nunca "sem permissão")
    assert como(app, mundo["fin"]).get(f"/erp/api/ponto/marcacoes/{m['id']}/local").status_code == 403
    q = dp.post("/erp/api/ponto/quadro", json={"de": seg.isoformat(), "filtros": {"tipos_pessoa": ["SEM_PIN"]}}).get_json()
    assert all(any(x["codigo"] == "SEM_PIN" for x in l["marcas"]) for l in q["linhas"])
