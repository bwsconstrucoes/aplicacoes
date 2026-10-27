# -*- coding: utf-8 -*-
"""Ponto — o quanto um ajuste de batida é crível (28/10/2026, core/credibilidade.py): sugestão para a
fila das Validações, com os motivos à vista. As partes puras e, com Postgres, a lista das Validações."""
from __future__ import annotations

import pytest

from app.apps.ponto.core import credibilidade as cr
from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, app,  # noqa: F401
                                           bater_via_chave, como, local, mundo)

PROPRIA = {"nome": "João", "como": "pelo celular", "propria": True}
OUTRA = {"nome": "Carlos (administrativo)", "como": "no próprio aparelho", "propria": False}
BATIDAS = [{"hora": "07:00", "obra": "AP-01"}, {"hora": "11:00", "obra": "AP-01"}, {"hora": "12:00", "obra": "AP-01"}]
PREVISTAS = ["07:00", "11:00", "12:00", "17:00"]


def _s(**kw):
    base = dict(pedido_por=PROPRIA, origem="APP", hora="17:00", obra="AP-01", batidas=BATIDAS, pedidos_no_dia=1,
                previstas=PREVISTAS, situacao="INCOMPLETO", feriado=None, fotos_ruins=[], em_conferencia=[],
                fora_da_area=[], dias_depois=1, dias_com_ajuste=1, negados_recentes=0)
    return cr.sinais(**{**base, **kw})


def test_uma_batida_faltando_no_horario_da_escala_e_crivel():
    r = _s()
    assert r["nivel"] == cr.CRIVEL and not r["atencao"]
    assert any("só esta batida falta" in t for t in r["a_favor"]) and any("17:00" in t for t in r["a_favor"])


def test_pedido_pelo_administrativo_e_crivel_mesmo_faltando_mais_de_uma():
    assert _s(pedidos_no_dia=2)["nivel"] == cr.NORMAL
    r = _s(pedidos_no_dia=2, pedido_por=OUTRA, origem="APARELHO")
    assert r["nivel"] == cr.CRIVEL and any("Carlos" in t for t in r["a_favor"])


def test_o_que_pede_atencao():
    assert _s(batidas=[], pedidos_no_dia=4)["nivel"] == cr.ATENCAO                    # o dia inteiro
    assert _s(fotos_ruins=["07:00 — o rosto não confere"])["nivel"] == cr.ATENCAO
    assert _s(fora_da_area=["às 06:58"])["nivel"] == cr.ATENCAO
    assert _s(obra="BR-02")["nivel"] == cr.ATENCAO                                    # obra das batidas é AP-01
    assert _s(hora="21:30")["nivel"] == cr.ATENCAO                                    # longe da escala
    assert _s(situacao="DESCANSO", previstas=[])["nivel"] == cr.ATENCAO
    assert _s(dias_depois=10)["nivel"] == cr.ATENCAO
    assert _s(dias_com_ajuste=cr.MUITOS_AJUSTES_DIAS)["nivel"] == cr.ATENCAO
    assert _s(negados_recentes=1)["nivel"] == cr.ATENCAO
    # a hora no meio do caminho (nem perto, nem longe da escala) não é crível nem atenção
    assert _s(hora="17:50")["nivel"] == cr.NORMAL


@pytest.mark.banco
def test_as_validacoes_trazem_o_nivel_de_cada_ajuste(app, mundo):
    import datetime as dt
    from app.apps.ponto import horario
    hoje = horario.hoje()
    # a segunda mais recente que já passou (no máximo 7 dias atrás: pedido tardio é atenção)
    seg = hoje - dt.timedelta(days=hoje.weekday() or 7)
    ter = seg - dt.timedelta(days=3)                 # a sexta anterior: o dia sem batida
    for h in ((7, 0), (11, 0), (12, 0)):
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    dp = como(app, mundo["dp"])
    for dia, hora in ((seg, "17:00"), (ter, "07:00"), (ter, "11:00")):
        r = dp.post("/erp/api/ponto/ocorrencias", json={
            "tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"], "data_inicio": dia.isoformat(),
            "horario": f"{dia.isoformat()}T{hora}:00", "obra": "PG-A", "descricao": "esqueceu de bater"})
        assert r.status_code == 201, r.get_json()
    v = dp.get("/erp/api/ponto/validacoes").get_json()
    ajustes = {(i["data"], i["quando"][-5:]): i for i in v["itens"] if i["tipo"] == "AJUSTE_BATIDA"}
    assert ajustes[(seg.isoformat(), "17:00")]["credibilidade"]["nivel"] == cr.CRIVEL
    assert ajustes[(ter.isoformat(), "07:00")]["credibilidade"]["nivel"] == cr.ATENCAO       # dia sem batida
    assert v["por_nivel"].get(cr.CRIVEL) == 1
