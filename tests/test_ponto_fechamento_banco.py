# -*- coding: utf-8 -*-
"""Ponto — o fechamento da folha para a contabilidade (29/10/2026, core/fechamento.py): do dia 26 do mês
anterior ao 25 do mês da folha; o mês anterior em planilha, o corrente no .ps do Fortes (o inteiro) e em
planilha (o quebrado)."""
from __future__ import annotations

import datetime as dt
import io
import zipfile

import pytest
from sqlalchemy import text

from app.apps.ponto.core import fechamento as fe
from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, app, bater_via_chave, como,  # noqa: F401
                                           local, mundo)


def test_os_periodos_da_folha():
    p = fe.periodos("2026-11")
    assert p["anterior"] == (dt.date(2026, 10, 26), dt.date(2026, 10, 31))
    assert p["corrente"] == (dt.date(2026, 11, 1), dt.date(2026, 11, 25))
    assert fe.periodos("2026-03")["anterior"] == (dt.date(2026, 2, 26), dt.date(2026, 2, 28))
    assert fe.periodos("2027-01")["anterior"] == (dt.date(2026, 12, 26), dt.date(2026, 12, 31))


def test_o_arquivo_ps_do_fortes():
    """O exemplo do documento do dono (`prompt_geracao_ps.md`), linha por linha."""
    linhas = [fe.linha_ps("001234", "001", dt.date(2025, 5, 15), 1),
              fe.linha_ps("005678", "004", dt.date(2025, 5, 15), 1),
              fe.linha_ps("009999", "003", dt.date(2025, 5, 16), 1)]
    assert linhas == ["3001234001150520250001", "3005678004150520250001", "3009999003160520250001"]
    texto = fe.arquivo_ps(linhas).decode("ascii").split("\r\n")
    assert texto[0] == "07AC        PONTO     0013                                        "
    assert texto[4] == "Z000030000000000000000000000000000000.000000000000000.00" and texto[5] == ""
    assert fe.id_fortes_ok("1234") == ("001234", None) and fe.id_fortes_ok("000013") == ("000013", None)
    assert fe.id_fortes_ok("")[1] == "ID Fortes ausente"
    assert "mais de 6 dígitos" in fe.id_fortes_ok("1234567")[1]          # não trunca calado
    with pytest.raises(ValueError):
        fe.linha_ps("000001", "006", dt.date(2025, 5, 1), 0)


def test_as_regras_do_dia():
    assert fe.evento_da_hora_extra({"data": "2026-11-01"}) == "003"           # domingo
    assert fe.evento_da_hora_extra({"data": "2026-11-02", "feriado": "Finados"}) == "003"
    assert fe.evento_da_hora_extra({"data": "2026-11-07"}) == "006"           # sábado: 50%
    assert fe.falta_do_dia({"situacao": "FALTA", "previsto": 540})["tipo"] == "INTEGRAL"
    assert fe.falta_do_dia({"situacao": "FALTA_PARCIAL", "debito": 300}) == {"tipo": "PARCIAL", "rotulo": "Falta parcial",
                                                                            "minutos": 300}
    assert fe.falta_do_dia({"situacao": "OK", "debito": 60})["tipo"] == "ATRASO"
    assert fe.falta_do_dia({"situacao": "FALTA", "previsto": 540, "ocorrencia": {"tipo": "ATESTADO"}}) is None
    assert fe.falta_do_dia({"situacao": "OK", "debito": 0}) is None


def _dia_do_corrente(hoje: dt.date) -> dt.date | None:
    """Uma segunda a quinta já passada, entre o dia 1 e o 25, depois do início do ponto dos testes."""
    d = hoje - dt.timedelta(days=1)
    for _ in range(40):
        if d.weekday() < 3 and 2 <= d.day <= 24:
            return d
        d -= dt.timedelta(days=1)
    return None


@pytest.mark.banco
def test_o_fechamento_separa_o_inteiro_do_quebrado(app, mundo, banco, banco_analisesps):
    from app.apps.ponto import horario
    dia = _dia_do_corrente(horario.hoje())
    if not dia:
        pytest.skip("nenhum dia útil entre 1 e 25 nos últimos 40 dias")
    with banco.connect() as conn:                       # o ID Fortes vem do cadastro da Análise de SPs
        conn.execute(text("INSERT INTO analisesps.colaborador (cpf, nome, celular, cargo, fase, obra_codigo, id_fortes) "
                          "VALUES (:c, 'João Obra A', '85999990000', 'Pedreiro', 'Colaboradores Ativos', 'PG-A', '000123')"),
                     {"c": CPF_JOAO})
        conn.commit()
    from app.apps.ponto.core import registro
    registro.esquecer()
    # um dia com 2h30 de extra (dia útil: 50%), e o seguinte só com a manhã (falta parcial de 5 h)
    for h, m in ((7, 0), (11, 0), (12, 0), (19, 30)):
        bater_via_chave(app, CPF_JOAO, local(dia, h, m))
    seguinte = dia + dt.timedelta(days=1)
    for h, m in ((7, 0), (11, 0)):
        bater_via_chave(app, CPF_JOAO, local(seguinte, h, m))
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    corpo = {"competencia": f"{dia:%Y-%m}"}
    assert sup.post("/erp/api/ponto/fechamento", json=corpo).status_code == 403     # é do DP
    r = dp.post("/erp/api/ponto/fechamento", json=corpo)
    assert r.status_code == 200, r.get_json()
    f = r.get_json()
    joao = lambda chave: [x for x in f[chave] if x["cpf"] == CPF_JOAO]      # noqa: E731
    assert [x["data"] for x in joao("cor_faltas_parciais") if x["tipo"] == "Falta parcial"] == [seguinte.isoformat()]
    assert next(x for x in joao("cor_faltas_parciais") if x["data"] == seguinte.isoformat())["minutos"] == 300
    he = joao("cor_extras_integrais")
    assert [(x["evento"], x["horas"]) for x in he] == [("006", 2)]
    assert [x["minutos"] for x in joao("cor_extras_quebradas")] == [30]
    integrais = joao("cor_faltas_integrais")
    assert all(x["id_fortes"] == "000123" for x in integrais)
    assert len([l for l in f["ps_faltas"] if l[1:7] == "000123"]) == len(integrais)
    fim = dt.date.fromisoformat(f["corrente"]["ate"])
    assert f"3000123006{fim:%d%m%Y}0002" in f["ps_extras"]
    # a Maria não tem ID Fortes: fica fora do .ps e aparece na conferência (se tiver falta)
    if any(x["nome"] == "Maria Obra B" for x in f["cor_faltas_integrais"]):
        assert any("ID Fortes ausente" in x["aviso"] for x in f["conferencia"] if x["nome"] == "Maria Obra B")
    # os arquivos: o .ps, o Excel com as abas e os três num zip
    ps = dp.post("/erp/api/ponto/fechamento/arquivo", json={**corpo, "arquivo": "ps_extras"})
    assert ps.status_code == 200 and ps.data.startswith(b"07AC") and b"\r\nZ0000" in ps.data
    z = zipfile.ZipFile(io.BytesIO(dp.post("/erp/api/ponto/fechamento/arquivo", json={**corpo, "arquivo": "zip"}).data))
    assert sorted(z.namelist()) == sorted([f"fechamento_folha_{dia:%Y%m}.xlsx", f"faltas_{dia:%Y%m}.ps",
                                           f"horas_extras_{dia:%Y%m}.ps"])
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(z.read(f"fechamento_folha_{dia:%Y%m}.xlsx")))
    assert wb.sheetnames == ["Resumo", "Ant - Faltas", "Ant - Horas extras", "Ant - HE por dia", "Faltas integrais",
                             "Faltas parciais", "HE integrais", "HE quebradas", "Conferência"]
    uma = load_workbook(io.BytesIO(dp.post("/erp/api/ponto/fechamento/arquivo",
                                           json={**corpo, "arquivo": "cor_extras_quebradas"}).data))
    assert uma.sheetnames == ["HE quebradas"]
    assert dp.post("/erp/api/ponto/fechamento/arquivo", json={**corpo, "arquivo": "outro"}).status_code == 400
