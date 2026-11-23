# -*- coding: utf-8 -*-
"""Ponto — o termo de acordo de compensação nasce sozinho (23/11/2026, o dono: "o sistema permite ser lançado, o DP
toma ciência (…) e o sistema em seguida cobra a assinatura do termo (…) se houver alguma discordância, o DP vai
poder cancelar aquele lançamento"). A compensação vale já; o termo é a pendência de assinatura que vem depois."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import _schema_ponto2, app, como, mundo  # noqa: F401
from tests.test_ponto_recesso_lote_banco import _folga_e_sabado

pytestmark = pytest.mark.banco


def _termos(colaborador_id):
    from app.apps.ponto import db
    with db.conexao() as conn:
        return [dict(l) for l in conn.execute(text(
            "SELECT id, tipo, titulo, texto, situacao, origem, origem_ids, prazo FROM dp_termos "
            "WHERE colaborador_id = :c ORDER BY id"), {"c": colaborador_id}).mappings().all()]


def test_o_recesso_vira_um_termo_por_pessoa_e_cai_com_a_compensacao(app, mundo):
    from app.apps.ponto import db
    dp = como(app, mundo["dp"])
    folga, sab = _folga_e_sabado()
    folga2 = folga + dt.timedelta(days=7)
    pares = [{"folga": folga.isoformat(), "pagar": sab.isoformat()},
             {"folga": folga2.isoformat(), "pagar": (sab + dt.timedelta(days=7)).isoformat()}]
    r = dp.post("/erp/api/ponto/quadro/recesso", json={"pessoas": [mundo["joao"]], "pares": pares,
                                                       "simular": False}).get_json()
    assert r["entram"] == 2, r
    t = _termos(mundo["joao"])
    assert len(t) == 1, t                                        # as duas folgas num termo só
    t = t[0]
    assert t["tipo"] == "TERMO-COMPENSACAO" and t["situacao"] == "ABERTO" and t["origem"] == "PONTO_COMPENSACAO"
    assert len(t["origem_ids"]) == 2
    assert "Cláusula 5ª - Compensação não cumprida" in t["texto"] and "João Obra A" in t["texto"]
    assert f"Folga em {folga:%d/%m/%Y}" in t["texto"] and "de 9h00: compensada com o trabalho em" in t["texto"]
    assert t["prazo"] <= folga                                   # cobra antes da primeira folga
    # a compensação vale já, sem esperar a assinatura
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={folga:%Y-%m}").get_json()
    assert next(d for d in esp["dias"] if d["data"] == folga.isoformat())["folga_paga_em"] == sab.isoformat()
    # o DP cancela uma: o termo continua (a outra vale); cancela a outra: o termo cai junto
    ids = sorted(t["origem_ids"])
    url = "/erp/api/ponto/ocorrencias/{}/cancelar-compensacao"
    assert dp.post(url.format(ids[0]), json={"motivo": "x"}).status_code == 400        # sem o porquê, não
    assert dp.post(url.format(ids[0]), json={"motivo": "a pessoa não concordou com o sábado"}).status_code == 200
    assert _termos(mundo["joao"])[0]["situacao"] == "ABERTO"
    assert dp.post(url.format(ids[1]), json={"motivo": "a pessoa não concordou com o sábado"}).status_code == 200
    assert _termos(mundo["joao"])[0]["situacao"] == "CANCELADO"
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={folga:%Y-%m}").get_json()
    assert not next(d for d in esp["dias"] if d["data"] == folga.isoformat())["folga_paga_em"]
    # cancelada não se cancela de novo; e quem não é do DP não cancela
    assert dp.post(url.format(ids[1]), json={"motivo": "de novo, por engano"}).status_code == 400
    with db.conexao() as conn:
        st = {l["status"] for l in db.todos(conn, "SELECT status FROM ponto.ocorrencias WHERE id = ANY(:i)", i=ids)}
    assert st == {"CANCELADA"}


def test_a_compensacao_lancada_uma_a_uma_tambem_gera_o_termo(app, mundo):
    dp = como(app, mundo["dp"])
    folga, sab = _folga_e_sabado()
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "COMPENSACAO", "colaborador_id": mundo["maria"], "data_inicio": folga.isoformat(),
        "dia_trabalhado": sab.isoformat(), "descricao": "folga combinada", "aprovar_ja": True})
    assert r.status_code == 201, r.get_json()
    t = _termos(mundo["maria"])
    assert len(t) == 1 and t[0]["situacao"] == "ABERTO" and "Maria Obra B" in t[0]["texto"]
    # a compensação de HORAS não gera termo (é no mesmo mês, sem banco)
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "COMPENSACAO_HORAS", "colaborador_id": mundo["joao"], "data_inicio": folga.isoformat(),
        "dia_trabalhado": (folga + dt.timedelta(days=1)).isoformat(), "aprovar_ja": True})
    assert r.status_code == 201, r.get_json()
    assert _termos(mundo["joao"]) == []
