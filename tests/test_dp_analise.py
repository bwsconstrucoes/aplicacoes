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


# ---------------------------------------------------------------------------
# A janela de atendimento do desligamento chega preenchida (23/11/2026)
# ---------------------------------------------------------------------------
def test_faltas_contam_do_dia_seguinte_ao_corte():
    assert a.inicio_das_faltas(date(2026, 12, 10), 25) == date(2026, 11, 26)
    assert a.inicio_das_faltas(date(2026, 12, 25), 25) == date(2026, 11, 26)
    assert a.inicio_das_faltas(date(2026, 12, 28), 25) == date(2026, 12, 26)
    assert a.inicio_das_faltas(date(2027, 1, 5), 25) == date(2026, 12, 26)


def _sugere(monkeypatch, forma, aviso, admissao=date(2022, 1, 10),
            contrato=None, cumpre=None, faltas=None):
    monkeypatch.setattr(a, "_pessoa", lambda s, i: {
        "vinculo": "CTPS", "admissao": admissao, "contrato_experiencia": contrato})
    monkeypatch.setattr(a, "faltas_no_ponto", lambda cid, ultimo, hoje=None: faltas or {
        "disponivel": True, "inicio": "2026-11-26", "fim": "2026-12-04",
        "corte": 25, "parcial_ate_hoje": True, "integrais": 2,
        "dias": ["2026-11-27", "2026-12-02"],
        "parciais": [{"data": "2026-12-01", "minutos": 90, "rotulo": "x"}]})
    dados = {"forma_desligamento": forma, "aviso_previo": aviso.isoformat()}
    if cumpre is not None:
        dados["cumpre_aviso"] = cumpre
    return a.sugestoes_do_desligamento(
        None, {"colaborador_id": 7, "dados": dados}, hoje=date(2026, 12, 5))


def test_sugestao_do_aviso_trabalhado_projeta_a_saida(monkeypatch):
    r = _sugere(monkeypatch, "AVISO_TRABALHADO", date(2026, 11, 20))
    assert r["valores"]["ultimo_dia"] == "2026-12-20"
    assert r["valores"]["data_saida"] == "2027-01-01"        # 42 dias
    assert r["valores"]["faltas_a_descontar"] == "2"
    assert "1h30" in r["explicacoes"]["faltas_a_descontar"]


def test_sugestao_do_pedido_de_demissao(monkeypatch):
    r = _sugere(monkeypatch, "PEDIDO_COLABORADOR", date(2026, 12, 1))
    assert r["valores"]["ultimo_dia"] == r["valores"]["data_saida"] == "2026-12-31"
    assert "art. 488" in r["explicacoes"]["reducao_jornada"]
    d = _sugere(monkeypatch, "PEDIDO_COLABORADOR", date(2026, 12, 1), cumpre="0")
    assert d["valores"]["ultimo_dia"] == d["valores"]["data_saida"] == "2026-12-01"


def test_sugestao_no_fim_da_experiencia_e_o_fim_do_contrato(monkeypatch):
    r = _sugere(monkeypatch, "FIM_EXPERIENCIA", date(2026, 12, 1),
                admissao=date(2026, 11, 1), contrato="45+45")
    assert r["valores"]["ultimo_dia"] == r["valores"]["data_saida"] == "2026-12-15"


def test_sem_ponto_a_sugestao_de_faltas_fica_vazia_e_diz_por_que(monkeypatch):
    r = _sugere(monkeypatch, "AVISO_INDENIZADO", date(2026, 11, 20),
                faltas={"disponivel": False, "motivo": "fora do ar"})
    assert "faltas_a_descontar" not in r["valores"]
    assert "fora do ar" in r["explicacoes"]["faltas_a_descontar"]
