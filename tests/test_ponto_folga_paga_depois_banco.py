# -*- coding: utf-8 -*-
"""Ponto — a folga que se paga DEPOIS (21/11/2026, o dono, no recesso de fim de ano pago com sábados do ano que vem:
"o dia que não foi trabalhado fica como folga, compensação a ser paga no futuro (…) quando aquele sábado não
acontecer, o sistema lança uma falta automática dizendo o motivo (…) e na folha de ponto uma observação, e a pessoa
assina também junto"). A folga fica abonada no mês dela; o que o sábado não pagar vira falta NO SÁBADO."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def _espelho(dp, colaborador_id, dia):
    return dp.get(f"/erp/api/ponto/espelho/{colaborador_id}?competencia={dia:%Y-%m}").get_json()


def _dia(dp, colaborador_id, dia):
    return next(d for d in _espelho(dp, colaborador_id, dia)["dias"] if d["data"] == dia.isoformat())


def test_a_folga_fica_e_a_falta_vai_para_o_sabado_que_nao_pagou(app, mundo):
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    ter, sab = seg + dt.timedelta(days=1), seg + dt.timedelta(days=5)        # o sábado DEPOIS da folga, já passado
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "COMPENSACAO", "colaborador_id": mundo["joao"], "data_inicio": ter.isoformat(),
        "dia_trabalhado": sab.isoformat(), "descricao": "recesso pago com sábado", "aprovar_ja": True})
    assert r.status_code == 201 and r.get_json()["ocorrencia"]["status"] == "APROVADA", r.get_json()

    # o sábado passou sem batida: a FOLGA continua folga; a falta é do SÁBADO, com o motivo
    d = _dia(dp, mundo["joao"], ter)
    assert d["situacao"] == "ABONADO" and "COMPENSACAO_NAO_CUMPRIDA" not in d["alertas"], d
    assert d["folga_paga_em"] == sab.isoformat()
    s = _dia(dp, mundo["joao"], sab)
    assert s["situacao"] == "FALTA" and s["falta"] and s["previsto"] == 540 and s["debito"] == 540, s
    assert "COMPENSACAO_NAO_CUMPRIDA" in s["alertas"]
    assert any(x["rotulo"] == f"Compensação não cumprida — este dia pagaria a folga de {ter:%d/%m/%Y}; não houve "
               "trabalho" for x in s["declaracoes"]), s["declaracoes"]
    # o relatório de faltas tem a falta do sábado, com o motivo
    rel = dp.post("/erp/api/ponto/relatorios", json={"tipo": "faltas", "de": seg.isoformat(),
                                                    "ate": sab.isoformat()}).get_json()
    do_joao = [x for x in rel["linhas"] if x["colaborador_id"] == mundo["joao"]]
    linha = next(x for x in do_joao if x["data"] == sab.isoformat())
    assert linha["tipo"] == "Falta" and linha["faltou"] == 540 and "pagaria a folga" in linha["motivo"], linha
    assert not any(x["data"] == ter.isoformat() for x in do_joao)

    # as observações: na folha da folga, o combinado; na do sábado, o que aconteceu
    obs_folga = _espelho(dp, mundo["joao"], ter)["compensacoes"]
    assert f"Folga compensatória em {ter:%d/%m/%Y}, paga com o trabalho em {sab:%d/%m/%Y} (sábado). Se esse dia " \
           "não for trabalhado, a falta é lançada nele." in obs_folga
    obs_sab = _espelho(dp, mundo["joao"], sab)["compensacoes"]
    assert any(t.startswith(f"{sab:%d/%m/%Y} (sábado): compensação não cumprida") and t.endswith(
        "falta lançada neste dia.") for t in obs_sab), obs_sab
    folha = dp.get(f"/erp/ponto/espelho/{mundo['joao']}/imprimir?competencia={ter:%Y-%m}")
    assert folha.status_code == 200 and "Observações — compensações" in folha.get_data(as_text=True)

    # trabalhou 4 h no sábado: falta parcial do resto, no sábado
    for h in (7, 11):
        bater_via_chave(app, CPF_JOAO, local(sab, h, 0))
    s = _dia(dp, mundo["joao"], sab)
    assert s["situacao"] == "FALTA_PARCIAL" and s["debito"] == 300 and "COMPENSACAO_INCOMPLETA" in s["alertas"], s
    assert _dia(dp, mundo["joao"], ter)["situacao"] == "ABONADO"
    # trabalhou as 9 h: pagou; nem falta, nem hora extra
    for h in (12, 17):
        bater_via_chave(app, CPF_JOAO, local(sab, h, 0))
    s = _dia(dp, mundo["joao"], sab)
    assert s["situacao"] == "COMPENSADO" and not s["debito"] and not s["extra"], s
    assert any(t.endswith("compensação cumprida.") for t in _espelho(dp, mundo["joao"], sab)["compensacoes"])


def test_a_folga_paga_depois_e_um_dia_por_pedido(app, mundo):
    """O sábado que não pagar vira UMA falta: por isso a folga paga depois é de um dia por pedido."""
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "COMPENSACAO", "colaborador_id": mundo["joao"], "data_inicio": (seg + dt.timedelta(days=1)).isoformat(),
        "data_fim": (seg + dt.timedelta(days=3)).isoformat(), "dia_trabalhado": (seg + dt.timedelta(days=12)).isoformat(),
        "descricao": "três dias de recesso", "aprovar_ja": True})
    assert r.status_code == 400 and "um pedido para cada dia de folga" in r.get_json()["erro"], r.get_json()


def test_o_lacre_da_folha_assinada_antes_das_observacoes_continua_valendo():
    from app.apps.ponto.core import assinaturas
    esp = {"pessoa": {"cpf": "1", "nome": "x"}, "competencia": "2026-12", "dias": [], "resumo": {},
           "compensacoes": ["Folga compensatória em 29/12/2026, paga com o trabalho em 09/01/2027 (sábado)."]}
    assert assinaturas.lacre(esp) != assinaturas.lacre(esp, com_observacoes=False)
    assert assinaturas.lacre({**esp, "compensacoes": []}) == assinaturas.lacre(esp, com_observacoes=False)
