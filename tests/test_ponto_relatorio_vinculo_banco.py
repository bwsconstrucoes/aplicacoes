# -*- coding: utf-8 -*-
"""Ponto — quem entra nas contas de horas com o cadastro do ERP como base (25/11/2026, o dono: "só aparece falta de 8
pessoas (…) horas extras, pouquíssimas"). Desde 17/11 a base é o ERP, e o "tipo" deixou de vir "CTPS" (planilha) para
vir o tipo de contrato ("CLT_INDETERMINADO"…): o relatório tirava todo mundo de carteira. Agora vale o vínculo."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_quem_tem_carteira_entra_e_o_prestador_nao():
    from app.apps.ponto.core.relatorios import conta_horas
    assert conta_horas({"tipo_cadastro": "CTPS"}) and conta_horas({"tipo_cadastro": "CLT_INDETERMINADO"})
    assert conta_horas({"tipo_cadastro": "clt_determinado"}) and conta_horas({"tipo_cadastro": "JOVEM_APRENDIZ"})
    assert conta_horas({"tipo_cadastro": ""}) and conta_horas({})             # vazio entra (a Conferência avisa)
    assert not conta_horas({"tipo_cadastro": "PRESTADOR"}) and not conta_horas({"tipo_cadastro": "AUTONOMO_RPA"})
    assert not conta_horas({"tipo_cadastro": "Prestador de Serviço"})          # a palavra da planilha


def test_o_relatorio_de_faltas_com_a_base_erp(app, mundo, banco):
    with banco.connect() as c:
        c.execute(text("UPDATE colaboradores SET vinculo = 'CTPS', tipo_contrato = 'CLT_INDETERMINADO' WHERE id = :i"),
                  {"i": mundo["joao"]})
        c.execute(text("UPDATE colaboradores SET vinculo = 'PRESTADOR', tipo_contrato = 'AUTONOMO_RPA' WHERE id = :i"),
                  {"i": mundo["maria"]})
        c.commit()
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    with db.conexao() as conn:
        assert cadastros.colaborador_por_id(conn, mundo["joao"])["tipo_cadastro"] == "CTPS"
    dp = como(app, mundo["dp"])
    dia = _segunda_passada() + dt.timedelta(days=1)                      # sem batida: falta
    rel = dp.post("/erp/api/ponto/relatorios", json={"tipo": "faltas", "de": dia.isoformat(),
                                                    "ate": dia.isoformat()}).get_json()
    quem = {x["colaborador_id"] for x in rel["linhas"]}
    assert mundo["joao"] in quem, rel                                     # de carteira: entra
    assert mundo["maria"] not in quem                                    # prestador: fora das contas de horas
