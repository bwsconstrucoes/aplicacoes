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


def test_o_relatorio_dos_prestadores_de_uma_empresa_e_as_folhas(app, mundo, banco):
    """25/11/2026, o dono: "colocar aqui ponto dos celetistas e prestadores de serviço. E ainda poder definir qual
    empresa (…) gerar o relatório do prestador de serviço de uma empresa"."""
    with banco.connect() as c:
        e1 = c.execute(text("INSERT INTO empresas (razao_social, cnpj) VALUES ('Empresa Um Teste', '11222333000181') "
                            "ON CONFLICT DO NOTHING RETURNING id")).scalar()
        if e1 is None:
            e1 = c.execute(text("SELECT id FROM empresas WHERE cnpj = '11222333000181'")).scalar()
        c.execute(text("UPDATE colaboradores SET vinculo = 'CTPS', tipo_contrato = 'CLT_INDETERMINADO', empresa_id = :e "
                       "WHERE id = :i"), {"i": mundo["joao"], "e": e1})
        c.execute(text("UPDATE colaboradores SET vinculo = 'PRESTADOR', tipo_contrato = 'AUTONOMO_RPA', empresa_id = :e "
                       "WHERE id = :i"), {"i": mundo["maria"], "e": e1})
        c.commit()
    dp = como(app, mundo["dp"])
    dia = _segunda_passada() + dt.timedelta(days=1)
    corpo = {"tipo": "faltas", "de": dia.isoformat(), "ate": dia.isoformat()}
    quem = lambda f: {x["colaborador_id"] for x in dp.post("/erp/api/ponto/relatorios",  # noqa: E731
                                                          json={**corpo, "filtros": f}).get_json()["linhas"]}
    assert quem({}) & {mundo["joao"], mundo["maria"]} == {mundo["joao"]}                  # o padrão: só CTPS
    assert quem({"vinculo": "PRESTADOR"}) & {mundo["joao"], mundo["maria"]} == {mundo["maria"]}
    assert quem({"vinculo": "TODOS"}) & {mundo["joao"], mundo["maria"]} == {mundo["joao"], mundo["maria"]}
    assert quem({"vinculo": "TODOS", "empresa": e1}) == {mundo["joao"], mundo["maria"]}
    assert quem({"vinculo": "TODOS", "empresa": 999999}) == set()
    r = dp.post("/erp/api/ponto/relatorios", json={**corpo, "filtros": {"vinculo": "PRESTADOR", "empresa": e1}}).get_json()
    assert r["vinculo_rotulo"] == "Prestadores de serviço" and r["empresa"]["razao_social"] == "Empresa Um Teste"
    assert dp.post("/erp/api/ponto/relatorios", json={**corpo, "filtros": {"vinculo": "X"}}).status_code == 400
    assert any(e["id"] == e1 for e in dp.get("/erp/api/ponto/relatorios/empresas").get_json()["empresas"])
    # as folhas de ponto do recorte: uma por pessoa
    f = dp.get(f"/erp/ponto/relatorios/folhas?de={dia}&ate={dia}&vinculo=PRESTADOR&empresa={e1}")
    html = f.get_data(as_text=True)
    assert f.status_code == 200 and "Maria Obra B" in html and "João Obra A" not in html
    f = dp.get(f"/erp/ponto/relatorios/folhas?de={dia}&ate={dia}&vinculo=TODOS&empresa={e1}")
    assert "2 folha(s), uma por pessoa" in f.get_data(as_text=True)
    with banco.connect() as c:
        c.execute(text("UPDATE colaboradores SET empresa_id = NULL WHERE id IN (:a, :b)"),
                  {"a": mundo["joao"], "b": mundo["maria"]})
        c.execute(text("DELETE FROM empresas WHERE id = :e AND NOT EXISTS (SELECT 1 FROM obras WHERE empresa_id = :e)"),
                  {"e": e1})
        c.commit()
