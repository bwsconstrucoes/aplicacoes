# -*- coding: utf-8 -*-
"""Ponto — o calendário do fechamento da folha (25/11/2026, o dono): a inicial do dia da semana e o feriado no
cabeçalho; "CLT" ou "Prestador" depois da função; com uma empresa escolhida, primeiro as obras dela e, no fim, as de
outras empresas em que o pessoal dela trabalhou; e o dia com batida incluída por ajuste, marcado."""
from __future__ import annotations

import datetime as dt
import io

import pytest
from sqlalchemy import text

from app.apps.ponto.core import fechamento as fe
from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, app, bater_via_chave, como,  # noqa: F401
                                           dias_de_trabalho_antes, local, mundo)


def _f(**kw):
    base = {"anterior": {"de": "2026-10-26", "ate": "2026-10-31"}, "corrente": {"de": "2026-11-01", "ate": "2026-11-25"},
            "competencia": "2026-11", "gerado_em": "agora", "he_minima": 0, "abater": False,
            "he_dias": [], "neg_dias": [], "trab_dias": [], "calendario_todos": True}
    return {**base, **kw}


def _x(obra, data, cid=1, nome="Ana", **kw):
    return {"colaborador_id": cid, "nome": nome, "funcao": "Pedreiro", "cpf": "1", "obra_do_dia": obra,
            "obra_nome": "", "data": data, "minutos": 540, "banco": False, "vinculo": "CTPS", **kw}


def test_a_obra_de_outra_empresa_vai_para_o_fim():
    f = _f(trab_dias=[_x("AA", "2026-11-03"), _x("BB", "2026-11-04"), _x("CC", "2026-11-05")],
           obra_empresa={"AA": 7, "BB": 8, "CC": None}, empresas_nomes={"7": "Sete Ltda", "8": "Oito SA"}, empresa=7)
    s = fe.supervisao(f)
    assert [(g["obra"], bool(g.get("outra_empresa"))) for g in s["obras"]] == [("AA", False), ("CC", False), ("BB", True)]
    assert s["empresa_nome"] == "Sete Ltda" and s["obras"][-1]["empresa_nome"] == "Oito SA"
    # sem empresa escolhida, a ordem é a de sempre
    assert [g["obra"] for g in fe.supervisao({**f, "empresa": None})["obras"]] == ["AA", "BB", "CC"]


def test_o_vinculo_o_feriado_e_o_ajuste_no_excel():
    from openpyxl import load_workbook
    f = _f(trab_dias=[_x("AA", "2026-11-03", ajustado=True), _x("AA", "2026-11-04", cid=2, nome="Bia", vinculo="Prestador")],
           feriados_por_obra={"AA": {"2026-11-20": "Consciência Negra"}},
           feriados_nacionais={"2026-11-15": "Proclamação da República"})
    s = fe.supervisao(f)
    assert s["obras"][0]["feriados"] == {"2026-11-15": "Proclamação da República", "2026-11-20": "Consciência Negra"}
    assert [p["vinculo"] for p in s["obras"][0]["pessoas"]] == ["CLT", "Prestador"]
    ws = load_workbook(io.BytesIO(fe.excel_supervisao(f)))["Horas extras por obra"]
    linhas = [[c.value for c in r] for r in ws.iter_rows()]
    cab = next(r for r in linhas if r[0] == "Funcionário")
    assert cab[2] == "26/10\nS" and cab[3] == "27/10\nT"                 # segunda, terça
    ana = next(r for r in linhas if r[0] == "Ana")
    assert ana[1] == "Pedreiro - CLT" and "✓*" in ana
    assert next(r for r in linhas if r[0] == "Bia")[1] == "Pedreiro - Prestador"
    # a coluna do feriado em laranja, o domingo em cinza
    col = {c.value: c.column for c in next(r for r in ws.iter_rows() if r[0].value == "Funcionário")}
    linha_cab = next(r for r in ws.iter_rows() if r[0].value == "Funcionário")
    assert linha_cab[col["20/11\nS"] - 1].fill.fgColor.rgb.endswith("FDE7C2")
    assert linha_cab[col["22/11\nD"] - 1].fill.fgColor.rgb.endswith("F3F4F6")


@pytest.mark.banco
def test_o_fechamento_marca_o_ajuste_o_feriado_e_a_empresa(app, mundo, banco):
    from app.apps.ponto import db, horario
    hoje = horario.hoje()
    uteis = [d for d in dias_de_trabalho_antes(banco, hoje, quantos=12) if d.day <= 25 and d.month == hoje.month] \
        or [d for d in dias_de_trabalho_antes(banco, hoje, quantos=12)]
    if len(uteis) < 4:
        pytest.skip("poucos dias de trabalho na folha corrente")
    d0, d1, d2, d3 = uteis[-4:]
    comp = f"{d1:%Y-%m}" if d1.day <= 25 else f"{(d1.replace(day=28) + dt.timedelta(days=4)):%Y-%m}"
    with banco.connect() as c:
        e1 = c.execute(text("INSERT INTO empresas (razao_social, cnpj) VALUES ('Empresa Um Teste', '11222333000181') "
                            "ON CONFLICT DO NOTHING RETURNING id")).scalar() \
            or c.execute(text("SELECT id FROM empresas WHERE cnpj = '11222333000181'")).scalar()
        e2 = c.execute(text("INSERT INTO empresas (razao_social, cnpj) VALUES ('Empresa Dois Teste', '11444777000161') "
                            "ON CONFLICT DO NOTHING RETURNING id")).scalar() \
            or c.execute(text("SELECT id FROM empresas WHERE cnpj = '11444777000161'")).scalar()
        c.execute(text("UPDATE obras SET empresa_id = :e WHERE codigo = 'PG-A'"), {"e": e1})
        c.execute(text("UPDATE obras SET empresa_id = :e WHERE codigo = 'PG-B'"), {"e": e2})
        c.execute(text("UPDATE colaboradores SET vinculo = 'CTPS', tipo_contrato = 'CLT_INDETERMINADO', empresa_id = :e "
                       "WHERE id = :i"), {"i": mundo["joao"], "e": e1})
        obra_a = c.execute(text("SELECT id FROM obras WHERE codigo = 'PG-A'")).scalar()
        c.commit()
    try:
        with db.conexao() as conn:
            db.executar(conn, "INSERT INTO ponto.feriados (data, nome, abrangencia, obra_id) VALUES (:d, 'Padroeira', 'OBRA', :o)",
                        d=d0, o=obra_a)
        dp = como(app, mundo["dp"])
        for h, m in ((7, 0), (11, 0), (12, 0), (17, 40)):              # d1: com hora extra (entra no calendário)
            bater_via_chave(app, CPF_JOAO, local(d1, h, m))
        for h in (7, 11, 12, 17):                                       # d2: o dia inteiro por ajuste aprovado
            r = dp.post("/erp/api/ponto/ocorrencias", json={
                "tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"], "data_inicio": d2.isoformat(),
                "horario": f"{d2.isoformat()}T{h:02d}:00:00", "obra": "PG-A", "descricao": "esqueceu de bater",
                "aprovar_ja": True})
            assert r.status_code == 201, r.get_json()
        for h in (7, 11, 12, 17):                                       # d3: numa obra da outra empresa
            bater_via_chave(app, CPF_JOAO, local(d3, h, 0), obra="PG-B")
        f = dp.post("/erp/api/ponto/fechamento", json={"competencia": comp, "calendario_todos": "1",
                                                      "filtros": {"vinculo": "TODOS", "empresa": e1}}).get_json()
        assert f["empresa"] == e1 and f["empresas_nomes"][str(e1)] == "Empresa Um Teste"
        assert f["obra_empresa"]["PG-A"] == e1 and f["obra_empresa"]["PG-B"] == e2
        assert f["feriados_por_obra"]["PG-A"][d0.isoformat()] == "Padroeira"
        assert d0.isoformat() not in f["feriados_nacionais"]                         # o da obra não vale nas outras
        s0 = fe.supervisao(f)
        assert all(d0.isoformat() not in g["feriados"] for g in s0["obras"] if g["obra"] == "PG-B")
        meus = lambda k: {x["data"]: x for x in f[k] if x["colaborador_id"] == mundo["joao"]}  # noqa: E731
        assert meus("trab_dias")[d2.isoformat()]["ajustado"] is True               # o dia do ajuste: ✓, marcado
        assert meus("trab_dias")[d2.isoformat()]["obra_do_dia"] == "PG-A"
        assert meus("trab_dias")[d3.isoformat()]["obra_do_dia"] == "PG-B"
        assert meus("he_dias")[d1.isoformat()]["ajustado"] is False
        assert meus("trab_dias")[d2.isoformat()]["vinculo"] == "CTPS"
        s = fe.supervisao(f)
        assert [g["obra"] for g in s["obras"]][-1] == "PG-B" and s["obras"][-1].get("outra_empresa")
        assert dp.get("/erp/ponto/relatorios").status_code == 200
    finally:
        with db.conexao() as conn:
            db.executar(conn, "DELETE FROM ponto.feriados WHERE nome = 'Padroeira'")
        with banco.connect() as c:
            c.execute(text("UPDATE obras SET empresa_id = NULL WHERE codigo IN ('PG-A', 'PG-B')"))
            c.execute(text("UPDATE colaboradores SET empresa_id = NULL WHERE id = :i"), {"i": mundo["joao"]})
            c.commit()
