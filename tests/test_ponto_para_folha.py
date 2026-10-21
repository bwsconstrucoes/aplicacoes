# -*- coding: utf-8 -*-
"""Ponto — A BASE MISTA DA FOLHA (21/10/2026, core/para_folha.py e o encaixe em
analisesps/ponto.py): o dono, "até 15 do 10 é MobPonto; do 16 em diante, a base do
ponto já vai ser esta". O dia do ponto eletrônico sai no formato do dia que a folha
já lê, e a folha troca o do MobPonto pelo daqui a partir do início do ponto — só com
a chave ligada."""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.ponto.core import para_folha


def test_as_quatro_posicoes_da_folha():
    b = lambda h, o="AP-01": {"hora": h, "obra": o}  # noqa: E731
    assert para_folha.quatro([b("07:00"), b("11:00"), b("12:00"), b("17:00", "br-02")]) == (
        ["AP-01", "AP-01", "AP-01", "BR-02"], ["07:00", "11:00", "12:00", "17:00"])
    assert para_folha.quatro([b("19:00"), b("07:00")]) == (["AP-01", "", "", "AP-01"], ["19:00", "", "", "07:00"])
    assert para_folha.quatro([b("07:00")]) == (["AP-01", "", "", ""], ["07:00", "", "", ""])
    assert para_folha.quatro([b(f"{h:02d}:00") for h in range(6, 12)])[1] == ["06:00", "07:00", "08:00", "09:00"]


def test_presenca_e_falta_como_a_folha_le():
    assert para_folha.presenca_e_falta({"batidas": [{}], "situacao": "FERIADO", "feriado": "Natal"}) == ("Presença", "")
    assert para_folha.presenca_e_falta({"batidas": [], "situacao": "FALTA"}) == ("Falta", "Falta sem justificativa")
    assert para_folha.presenca_e_falta({"batidas": [], "situacao": "ABONADO",
                                        "ocorrencia": {"rotulo": "Atestado"}}) == ("Atestado", "")
    assert para_folha.presenca_e_falta({"batidas": [], "situacao": "DESCANSO"}) == ("Descanso", "")
    assert para_folha.presenca_e_falta({"batidas": [], "situacao": "OK", "feriado": "Natal"}) == ("Feriado", "")
    assert para_folha.presenca_e_falta({"batidas": [], "situacao": "SEM_ESCALA"}) == ("", "")
    d = para_folha.dia_da_folha({"data": "2026-10-20", "batidas": [{"hora": "07:00", "obra": "AP-01"}],
                                 "situacao": "INCOMPLETO", "trabalhado": 125, "dia_semana": "terça"})
    assert d["data"] == dt.date(2026, 10, 20) and d["total_de_horas"] == "02:05" and d["presenca"] == "Presença"
    # o formato é o de analisesps.ponto._dia_lido (mais a fonte)
    from app.apps.analisesps import ponto as ponto_da_folha
    assert set(ponto_da_folha._dia_lido(None, {})) <= set(d)


def test_juntar_dias_e_a_carga_da_folha(monkeypatch):
    from app.apps.analisesps import ponto as ponto_da_folha
    corte = dt.date(2026, 10, 16)
    mob = [{"data": dt.date(2026, 10, 15), "x": "mob"}, {"data": dt.date(2026, 10, 16), "x": "mob"}]
    novo = [{"data": dt.date(2026, 10, 16), "x": "novo"}, {"data": dt.date(2026, 10, 17), "x": "novo"}]
    assert [(d["data"].day, d["x"]) for d in ponto_da_folha.juntar_dias(mob, novo, corte)] == \
        [(15, "mob"), (16, "novo"), (17, "novo")]
    # desligada: a carga é a do MobPonto, sem mudar nada
    monkeypatch.setattr(ponto_da_folha, "carga_do_mes", lambda a, m: None)
    monkeypatch.setattr(ponto_da_folha, "corte_do_ponto_novo", lambda a, m: None)
    assert ponto_da_folha.carga_para_a_folha(2026, 11) is None
    # ligada, mês sem MobPonto: a carga do ponto eletrônico
    monkeypatch.setattr(ponto_da_folha, "corte_do_ponto_novo", lambda a, m: corte)
    c = ponto_da_folha.carga_para_a_folha(2026, 11)
    assert c["id"] is None and c["fonte"] == "PONTO_ELETRONICO" and c["completa"] and c["ponto_eletronico_desde"] == dt.date(2026, 11, 1)
    monkeypatch.setattr(ponto_da_folha, "carga_do_mes", lambda a, m: {"id": 9, "ano": a, "mes": m})
    assert ponto_da_folha.carga_para_a_folha(2026, 10)["id"] == 9


# ---------------------------------------------------------------------------
# Com Postgres de verdade
# ---------------------------------------------------------------------------
from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, _segunda_passada, app,  # noqa: E402,F401
                                           bater_via_chave, como, local, mundo)


@pytest.mark.banco
def test_o_ponto_no_formato_da_folha_e_a_chave(app, mundo):
    from app.apps.ponto import db
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0), (17, 0)):
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    with db.conexao() as conn:
        r = para_folha.dias_por_cpf(conn, seg, seg + dt.timedelta(days=1))
    joao = r[para_folha.cpf11(CPF_JOAO)]
    dia = next(d for d in joao["dias"] if d["data"] == seg)
    assert dia["marcacoes"] == ["PG-A"] * 4 and dia["horas"] == ["07:00", "11:00", "12:00", "17:00"]
    assert dia["presenca"] == "Presença" and dia["total_de_horas"]
    falta = next(d for d in joao["dias"] if d["data"] == seg + dt.timedelta(days=1))
    assert falta["marcacoes"] == ["", "", "", ""] and falta["falta"] == "Falta sem justificativa"
    # a chave nasce desligada; só quem configura liga
    dp = como(app, mundo["dp"])
    assert dp.get("/erp/api/ponto/folha").get_json()["ligado"] is False
    assert como(app, mundo["sup"]).post("/erp/api/ponto/folha", json={"ligado": True}).status_code in (403, 404)
    assert dp.post("/erp/api/ponto/folha", json={"ligado": True}).get_json()["ligado"] is True
    assert dp.post("/erp/api/ponto/folha", json={"ligado": False}).get_json()["ligado"] is False


@pytest.mark.banco
def test_a_folha_le_o_mobponto_ate_o_corte_e_o_ponto_depois(banco_analisesps, monkeypatch):
    """A ponta da folha (analisesps/ponto.py), com a carga do MobPonto de verdade e o
    ponto eletrônico dublado: até o corte, o MobPonto; dali em diante, o ponto."""
    from tests.test_analisesps_ponto import dublar, pessoa_no_ponto, resposta
    from app.apps.analisesps import ponto
    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic de-mentira")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave-de-mentira")
    monkeypatch.setattr(ponto, "PAUSA_ENTRE_PAGINAS", 0)
    from app.apps.analisesps import db as db_da_folha
    db_da_folha.esquecer_colunas()
    dublar(monkeypatch, [resposta([pessoa_no_ponto(cpf="997.133.493-34", dias=20)])])
    ponto.carregar(2026, 8, quem="teste")
    cpf = "99713349334"
    assert len(ponto.dias_por_cpf(2026, 8)[cpf]) == 20                    # desligada: o de sempre

    corte = dt.date(2026, 8, 16)
    novo = lambda d, obra="AP-01": {"data": d, "marcacoes": [obra, obra, obra, obra],  # noqa: E731
                                    "horas": ["07:00", "11:00", "12:00", "17:00"], "presenca": "Presença",
                                    "falta": "", "total_de_horas": "08:00", "dia_da_semana": "", "fonte": "PONTO"}
    do_ponto = {cpf: {"nome": "GERLANIO GOMES LIMA", "dias": [novo(dt.date(2026, 8, 17)), novo(dt.date(2026, 8, 18))]},
                "12345678909": {"nome": "SÓ NO PONTO NOVO", "dias": [novo(dt.date(2026, 8, 17), "BR-02")]}}
    monkeypatch.setattr(ponto, "corte_do_ponto_novo", lambda a, m: corte)
    monkeypatch.setattr(ponto, "_ponto_novo", lambda a, m, cpfs=None: (
        corte, {c: v for c, v in do_ponto.items() if not cpfs or c in cpfs}))
    dias = ponto.dias_por_cpf(2026, 8)
    assert [d["data"].day for d in dias[cpf]] == list(range(1, 16)) + [17, 18]
    assert dias["12345678909"][0]["marcacoes"] == ["BR-02"] * 4
    ficha = ponto.dias_da_pessoa(cpf, 2026, 8)
    assert ficha["tem_carga"] and [d["data"].day for d in ficha["dias"]][-2:] == [17, 18]
    assert ficha["dias"][-1]["obra"] == "AP-01" and ficha["dias"][-1]["fonte"] == "PONTO"
    assert [d["data"].day for d in ponto.dias_de_um_cpf(2026, 8, cpf)][-1] == 18
    nomes = {p["cpf"]: p["nome"] for p in ponto.resumo_por_pessoa(2026, 8)}
    assert nomes["12345678909"] == "SÓ NO PONTO NOVO"
    # um mês sem carga do MobPonto, todo depois do corte: a folha ainda lê o ponto
    monkeypatch.setattr(ponto, "corte_do_ponto_novo", lambda a, m: dt.date(2026, 8, 16))
    assert ponto.carga_do_mes(2026, 9) is None and ponto.carga_para_a_folha(2026, 9)["fonte"] == "PONTO_ELETRONICO"
    assert set(ponto.dias_por_cpf(2026, 9)) == {cpf, "12345678909"}
