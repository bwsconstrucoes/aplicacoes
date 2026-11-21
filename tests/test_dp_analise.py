# -*- coding: utf-8 -*-
"""As contas trabalhistas da análise do agente (`core/dp_analise.py`) — puras,
sem banco. Cada caso é uma regra de lei com o número que ela dá."""
from __future__ import annotations

from datetime import date

from app.apps.erp.core import dp_analise as a


def test_aviso_proporcional_30_mais_3_por_ano_ate_90():
    adm = date(2020, 3, 10)
    assert a.dias_de_aviso(adm, date(2020, 12, 1), "AVISO_TRABALHADO") == 30
    assert a.dias_de_aviso(adm, date(2021, 3, 10), "AVISO_TRABALHADO") == 33
    assert a.dias_de_aviso(adm, date(2026, 3, 9), "AVISO_INDENIZADO") == 45
    assert a.dias_de_aviso(date(1990, 1, 1), date(2026, 1, 1), "AVISO_TRABALHADO") == 90
    # no pedido de demissão a pessoa deve 30, não o proporcional
    assert a.dias_de_aviso(adm, date(2026, 3, 9), "PEDIDO_COLABORADOR") == 30


def test_datas_do_aviso_pela_forma():
    adm, aviso = date(2022, 1, 10), date(2026, 11, 20)        # 4 anos → 42 dias
    t = a.datas_do_aviso(adm, aviso, "AVISO_TRABALHADO")
    assert (t["dias"], t["ultimo_dia"], t["indenizados"]) == (42, date(2026, 12, 20), 12)
    assert t["data_saida"] == date(2027, 1, 1)
    i = a.datas_do_aviso(adm, aviso, "AVISO_INDENIZADO")
    assert i["ultimo_dia"] == aviso and i["data_saida"] == date(2027, 1, 1)
    p = a.datas_do_aviso(adm, aviso, "PEDIDO_COLABORADOR", cumpre=False)
    assert p["ultimo_dia"] == aviso == p["data_saida"]


def test_os_30_dias_antes_da_data_base_dao_indenizacao_adicional():
    # data-base em maio: saída projetada em 15/04 cai nos 30 dias antes
    r = a.risco_da_data_base(5, date(2027, 4, 15))
    assert r and r["data_base"] == date(2027, 5, 1)
    assert a.risco_da_data_base(5, date(2027, 3, 20)) is None
    assert a.risco_da_data_base(5, date(2027, 5, 2)) is None
    assert a.risco_da_data_base(None, date(2027, 4, 15)) is None


def test_periodos_de_ferias_e_o_prazo_para_gozar():
    ps = a.periodos_de_ferias(date(2024, 3, 10), date(2026, 11, 20))
    assert [p["inicio"] for p in ps] == [date(2024, 3, 10), date(2025, 3, 10), date(2026, 3, 10)]
    assert ps[0]["fim"] == date(2025, 3, 9) and ps[0]["limite"] == date(2026, 3, 9)
