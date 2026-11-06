# -*- coding: utf-8 -*-
"""Ponto — a COMPENSAÇÃO DE HORAS (06/11/2026), com Postgres de verdade. O dono: "é uma compensação, não é de um dia,
é de horas" — a turma ficou até as 20h17 na quinta e sai mais cedo na sexta. As horas a mais de um dia pagam as a
menos de outro: o que sobra continua hora extra, o que falta continua devendo. E o fechamento e o relatório de extras
acusam quem tem hora extra E horas devendo no mesmo período ("o DP não deve pagar horas extras quando a pessoa está
devendo")."""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.ponto.core import fechamento as fe
from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, _segunda_passada, app, bater_via_chave,  # noqa: F401
                                           como, local, mundo)
from tests.test_ponto_tipos_ausencia_banco import _dia

pytestmark = pytest.mark.banco


def _quinta_e_sexta():
    """Uma quinta e a sexta seguinte, passadas, no MESMO pedaço da folha (o corte não cai entre as duas)."""
    seg = _segunda_passada()
    while True:
        qui, sex = seg + dt.timedelta(days=3), seg + dt.timedelta(days=4)
        if (qui.day <= fe.CORTE_PADRAO) == (sex.day <= fe.CORTE_PADRAO) and qui.month == sex.month:
            return qui, sex
        seg -= dt.timedelta(days=7)


def _competencia(dia: dt.date) -> str:
    if dia.day <= fe.CORTE_PADRAO:
        return f"{dia:%Y-%m}"
    proximo = (dia.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return f"{proximo:%Y-%m}"


def _bater(app, dia, *horas):
    for h, m in horas:
        bater_via_chave(app, CPF_JOAO, local(dia, h, m))


def _avisos_de_extra_com_debito(dp, dia):
    f = dp.post("/erp/api/ponto/fechamento", json={"competencia": _competencia(dia)}).get_json()
    return [x for x in f["conferencia"] if x.get("tipo") == "EXTRA_COM_DEBITO" and x["cpf"] == CPF_JOAO]


def test_as_horas_a_mais_da_quinta_pagam_a_saida_cedo_da_sexta(app, mundo):
    dp = como(app, mundo["dp"])
    qui, sex = _quinta_e_sexta()
    _bater(app, qui, (7, 0), (11, 0), (12, 0), (20, 17))       # a escala vai até as 17h: 3h17 a mais
    _bater(app, sex, (7, 0), (11, 0), (12, 0), (12, 30))       # sexta até 16h: saiu 3h30 antes
    antes_qui, antes_sex = _dia(dp, mundo["joao"], qui), _dia(dp, mundo["joao"], sex)
    assert antes_qui["extra"] == 197 and antes_sex["debito"] == 210
    # sem a compensação: o fechamento acusa a hora extra de quem está devendo horas
    aviso = _avisos_de_extra_com_debito(dp, qui)
    assert len(aviso) == 1 and aviso[0]["extra"] == 197 and aviso[0]["devendo"] == 210
    assert "Compensação de horas" in aviso[0]["aviso"]
    # e o relatório de extras mostra o que a pessoa deve no mesmo período
    rel = dp.post("/erp/api/ponto/relatorios", json={"tipo": "extras", "de": qui.isoformat(), "ate": sex.isoformat()}
                  ).get_json()
    pessoa = next(x for x in rel["por_pessoa"] if x["colaborador_id"] == mundo["joao"])
    assert pessoa["devendo"] == 210 and rel["totais"]["devendo"] == 1
    # lançada: nada de dia inteiro — as 3h17 pagam 3h17 da sexta; ficam devendo 13 minutos
    r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "COMPENSACAO_HORAS", "colaborador_id": mundo["joao"],
                "data_inicio": sex.isoformat(), "dia_trabalhado": qui.isoformat(), "aprovar_ja": True})
    assert r.status_code == 201, r.get_json()
    o = r.get_json()["ocorrencia"]
    assert o["status"] == "APROVADA" and o["rotulo"] == "Compensação de horas" and o["subtipo"] == "HORAS"
    d_qui, d_sex = _dia(dp, mundo["joao"], qui), _dia(dp, mundo["joao"], sex)
    assert d_qui["extra"] == 0 and "3h17 pagam" in d_qui["compensacao"]["rotulo"]
    assert d_sex["debito"] == 13 and d_sex["ocorrencia"] is None and d_sex["trabalhado"] == 270    # a manhã conta
    assert any("3h17 cobertas" in x["rotulo"] for x in d_sex["declaracoes"])
    assert "COMPENSACAO_NAO_CUMPRIDA" not in d_sex["alertas"]
    assert not _avisos_de_extra_com_debito(dp, qui)            # sem extra sobrando, nada a criticar


def test_o_que_sobra_continua_hora_extra_e_o_dia_futuro_fica_a_compensar(app, mundo):
    dp = como(app, mundo["dp"])
    qui, sex = _quinta_e_sexta()
    _bater(app, qui, (7, 0), (11, 0), (12, 0), (21, 0))        # 4h a mais
    _bater(app, sex, (7, 0), (11, 0), (12, 0), (12, 30))       # 3h30 a menos
    assert dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "COMPENSACAO", "subtipo": "HORAS",
                   "colaborador_id": mundo["joao"], "data_inicio": sex.isoformat(), "dia_trabalhado": qui.isoformat(),
                   "aprovar_ja": True}).status_code == 201
    d_qui, d_sex = _dia(dp, mundo["joao"], qui), _dia(dp, mundo["joao"], sex)
    assert d_qui["extra"] == 30                                # a meia hora que sobrou é hora extra
    assert d_sex["debito"] == 0 and "SAIDA_ANTECIPADA" not in d_sex["alertas"]
    # o dia das horas a mais ainda não chegou: a sexta continua devendo, "a compensar"
    futuro = dt.date.today() + dt.timedelta(days=5)
    outra = sex - dt.timedelta(days=7)
    _bater(app, outra, (7, 0), (11, 0))
    assert dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "COMPENSACAO_HORAS", "colaborador_id": mundo["joao"],
                   "data_inicio": outra.isoformat(), "dia_trabalhado": futuro.isoformat(),
                   "aprovar_ja": True}).status_code == 201
    d = _dia(dp, mundo["joao"], outra)
    assert d["debito"] > 0 and any("a compensar" in x["rotulo"] for x in d["declaracoes"])
    # longe demais um do outro: recusada
    r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "COMPENSACAO_HORAS", "colaborador_id": mundo["joao"],
                "data_inicio": (outra - dt.timedelta(days=1)).isoformat(),
                "dia_trabalhado": (outra + dt.timedelta(days=60)).isoformat()})
    assert r.status_code == 400 and "31" in r.get_json()["erro"]


def test_a_turma_inteira_pelo_quadro(app, mundo):
    dp = como(app, mundo["dp"])
    qui, sex = _quinta_e_sexta()
    _bater(app, qui, (7, 0), (11, 0), (12, 0), (20, 17))
    _bater(app, sex, (7, 0), (11, 0), (12, 0), (12, 30))
    itens = [{"colaborador_id": mundo["joao"], "data": sex.isoformat()},
             {"colaborador_id": mundo["maria"], "data": sex.isoformat()}]          # a Maria não bateu: falta
    assert como(app, mundo["sup"]).post("/erp/api/ponto/quadro/compensar-horas", json={
        "itens": itens, "dia_das_horas_a_mais": qui.isoformat()}).status_code in (403, 404)
    r = dp.post("/erp/api/ponto/quadro/compensar-horas", json={"itens": itens, "dia_das_horas_a_mais": qui.isoformat(),
                "motivo": "concretagem até 20h17; sexta ao meio-dia"}).get_json()
    rj, rm = r["resultados"]
    assert rj["ok"] and rj["aprovado"], rj
    assert rm["ok"] is False and "não ficou além do horário" in rm["erro"]      # a Maria não fez hora a mais
    assert _dia(dp, mundo["joao"], qui)["extra"] == 0 and _dia(dp, mundo["joao"], sex)["debito"] == 13
    assert _dia(dp, mundo["maria"], sex)["situacao"] == "FALTA"
