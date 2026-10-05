# -*- coding: utf-8 -*-
"""
A "Listagem de Férias" do Fortes — importar, sem duplicar, e acusar mudança
(dono, 05/10/2026).

As linhas imitam o arquivo da contabilidade (o real tem dado pessoal e não
entra no repositório).
"""
import datetime as dt
from decimal import Decimal as D

import pytest

from app.apps.analisesps import ferias_fortes as ff
from tests.test_analisesps_telas import app  # noqa: F401 — a fixture

COD_A, COD_B, COD_C = "000705", "002350", "009999"
CPF_A, CPF_B = "99713349334", "03513441363"


def _bloco(codigo, nome, gozo, retorno, abono="0 dia", liquido=1948.85):
    return [
        [codigo, nome, "", "", "", "", "", ""],
        ["Cargo: SERVENTE", "", "", "", "", "", "", ""],
        ["", "", "110 Remuneração de Férias", "", "30 dia(s)", "", 1794.0, ""],
        ["", "", "111 1/3 de Férias", "", "", "", 598.0, ""],
        ["", "", "310 INSS", "", "9%", "", "", 190.96],
        ["", "", "", "", "", "", 2392.0, 443.15],
        ["", "", "", "FGTS: 191,36", "", "", "Líquido a receber:", liquido],
        ["Período Aquisitivo: 01/10/2024 a 30/09/2025", "", "", f"Gozo: {gozo}",
         f"Retorno: {retorno}", "", f"Abono: {abono}", ""],
    ]


def _arquivo(mes="09", blocos=()):
    linhas = [
        ["Listagem de Férias", "", "", "", "", "", "", "Pag.: 1"],
        ["Empresa:", "BWS CONSTRUCOES LTDA", "", "", "", "", "", "Fortes Pessoal"],
        [f"Iniciadas entre 01/{mes}/2026 a 30/{mes}/2026", "", "", "", "", "", "", ""],
        ["Código", "Empregado", "Evento", "", "Referência", "", "Provento", "Desconto"],
    ]
    for b in blocos:
        linhas += b
    linhas += [["Total Geral", f"({len(blocos)} empregados)", "", "", "", "", "", ""]]
    return linhas


def _xlsx(linhas) -> bytes:
    import io
    import openpyxl
    livro = openpyxl.Workbook()
    for l in linhas:
        livro.active.append(l)
    memoria = io.BytesIO()
    livro.save(memoria)
    return memoria.getvalue()


def test_le_cada_colaborador_com_o_GOZO_o_retorno_e_o_abono():
    lido = ff.interpretar(_arquivo(blocos=[
        _bloco(COD_A, "FULANO A", "01/09/2026 a 30/09/2026", "01/10/2026"),
        _bloco(COD_B, "FULANA B", "01/09/2026 a 20/09/2026", "21/09/2026", "10 dias")]))
    assert lido["periodo"] == (dt.date(2026, 9, 1), dt.date(2026, 9, 30))
    a, b = lido["registros"]
    assert (a["codigo"], a["inicio"], a["fim"], a["retorno"]) == (
        COD_A, dt.date(2026, 9, 1), dt.date(2026, 9, 30), dt.date(2026, 10, 1))
    assert a["aquisitivo"] == "01/10/2024 a 30/09/2025" and a["liquido"] == D("1948.85")
    assert [e["codigo"] for e in a["eventos"]] == ["110", "111", "310"]
    assert b["abono_dias"] == 10 and b["fim"] == dt.date(2026, 9, 20)


def test_arquivo_que_NAO_e_a_listagem_e_recusado():
    with pytest.raises(ff.ErroDasFerias):
        ff.interpretar([["Folha Sintética"], ["000705", "FULANO"]])


@pytest.fixture
def banco_ferias(banco_analisesps):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        for cpf, nome, cod in [(CPF_A, "FULANO A", COD_A), (CPF_B, "FULANA B", "")]:
            conn.execute("INSERT INTO analisesps.colaborador (cpf, nome, id_fortes) "
                         " VALUES (?,?,?)", (cpf, nome, cod))
        conn.commit()
    return banco_analisesps


@pytest.mark.banco
def test_IMPORTAR_dois_arquivos_nao_duplica_e_acusa_a_MUDANCA(banco_ferias):
    from app.apps.analisesps import folha_calendario as fc
    setembro = _xlsx(_arquivo("09", [
        _bloco(COD_A, "FULANO A", "01/09/2026 a 30/09/2026", "01/10/2026"),
        _bloco(COD_B, "FULANA B", "28/09/2026 a 12/10/2026", "13/10/2026"),
        _bloco(COD_C, "NINGUEM", "01/09/2026 a 30/09/2026", "01/10/2026")]))
    # O mesmo arquivo duas vezes (mês anterior e atual se repetem): conta uma.
    analise = ff.analisar([("set.xlsx", setembro), ("set-de-novo.xlsx", setembro)])
    assert [r["nome"] for r in analise["novas"]] == ["FULANA B", "FULANO A"]
    assert [r["casou_por"] for r in analise["novas"]] == ["nome", "código"]
    assert [r["nome"] for r in analise["sem_cadastro"]] == ["NINGUEM"]

    feito = ff.importar([("set.xlsx", setembro)], quem="MARCELO")
    assert feito["gravadas"] == 2
    fer = {f["cpf"]: f for f in fc.listar_ferias()}
    assert fer[CPF_A]["origem"].startswith("Fortes") and fer[CPF_A]["liquido"] == D("1948.85")
    # E o cálculo do auxílio já enxerga: 22 dias úteis de setembro.
    assert fc.dias_de_ferias_no_periodo(CPF_A, dt.date(2026, 9, 1), dt.date(2026, 9, 30)) == 22

    # Reimportar: nada novo.
    de_novo = ff.analisar([("set.xlsx", setembro)])
    assert de_novo["a_gravar"] == 0 and len(de_novo["iguais"]) == 2

    # A contabilidade mudou as datas da FULANA B, e tirou o FULANO A.
    mudou = _xlsx(_arquivo("09", [
        _bloco(COD_B, "FULANA B", "29/09/2026 a 13/10/2026", "14/10/2026")]))
    analise = ff.analisar([("set-v2.xlsx", mudou)])
    assert [r["nome"] for r in analise["alteradas"]] == ["FULANA B"]
    assert analise["alteradas"][0]["antes"][0]["inicio"] == dt.date(2026, 9, 28)
    assert [p["cpf"] for p in analise["sumiram"]] == [CPF_A], "aviso, não apaga"
    feito = ff.importar([("set-v2.xlsx", mudou)], quem="MARCELO")
    assert feito["gravadas"] == 1 and feito["substituidas"] == 1
    da_b = [f for f in fc.listar_ferias() if f["cpf"] == CPF_B]
    assert [(f["inicio"], f["fim"]) for f in da_b] == [
        (dt.date(2026, 9, 29), dt.date(2026, 10, 13))]
    assert any(f["cpf"] == CPF_A for f in fc.listar_ferias()), "o que sumiu não é apagado"


def test_a_rota_RESPONDE_a_analise_e_grava_so_com_confirmar(monkeypatch, app):
    from app.apps.analisesps import ferias_fortes
    from tests.test_analisesps_telas import _como_mestre
    chamadas = []
    monkeypatch.setattr(ferias_fortes, "analisar", lambda arqs: chamadas.append(
        ("analisar", [n for n, _ in arqs])) or _analise_vazia())
    monkeypatch.setattr(ferias_fortes, "importar", lambda arqs, quem="": chamadas.append(
        ("importar", [n for n, _ in arqs])) or dict(_analise_vazia(), gravadas=0,
                                                    substituidas=0))
    import io
    cliente = _como_mestre(app)
    r = cliente.post("/analisesps/api/folha/ferias/importar", data={
        "arquivos": [(io.BytesIO(b"x"), "a.xls"), (io.BytesIO(b"y"), "b.xls")]},
        content_type="multipart/form-data")
    assert r.get_json()["ok"] and chamadas == [("analisar", ["a.xls", "b.xls"])]
    r = cliente.post("/analisesps/api/folha/ferias/importar", data={
        "arquivos": [(io.BytesIO(b"x"), "a.xls")], "confirmar": "1"},
        content_type="multipart/form-data")
    assert r.get_json()["ok"] and chamadas[-1] == ("importar", ["a.xls"])


def _analise_vazia():
    return {"novas": [], "iguais": [], "alteradas": [], "sem_cadastro": [],
            "sumiram": [], "conflitos": [], "arquivos": [], "a_gravar": 0}
