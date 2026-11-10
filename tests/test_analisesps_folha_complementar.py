"""A FOLHA COMPLEMENTAR — só a diferença de uma folha já paga (10/11/2026).

O dono: *"gerar um arquivo de pagamento com a diferença (…) quem foi a mais, quem
foi a menos — o que foi a menos vai ser só ilustrativo (…) quem não foi pago,
exibir desmarcado."* Ver `app/apps/analisesps/folha_complementar.py`.
"""
from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_complementar as fc

ZE, MARIA, RAI, LUIZ, PEDRO = ("99713349334", "03513441363", "11144477735",
                              "39053344705", "52998224725")


def _pessoa(cpf, nome, *obras, fora=False):
    return {"cpf": cpf, "nome": nome, "fora": fora,
            "valor": sum((D(v) for _o, v in obras), D("0")),
            "por_obra": [{"obra": o, "dias": 10, "valor": D(v), "origem": "ponto"} for o, v in obras]}


# ---------------------------------------------------------------------------
# PURAS
# ---------------------------------------------------------------------------
def test_a_diferenca_se_divide_nas_obras_e_o_centavo_vai_para_a_ultima():
    partes = fc.dividir(D("100.00"), [{"obra": "A", "valor": D("1")}, {"obra": "B", "valor": D("1")},
                                      {"obra": "C", "valor": D("1")}])
    assert [p["valor"] for p in partes] == [D("33.33"), D("33.33"), D("33.34")]
    assert sum(p["valor"] for p in partes) == D("100.00")
    assert fc.dividir(D("-5"), [{"obra": "A", "valor": D("1")}]) == []


def test_os_quatro_grupos():
    novas = [_pessoa(ZE, "ZE", ("OLINDA", "2152.80")),
             _pessoa(MARIA, "MARIA", ("SOBRAL", "1174.44"), ("OLINDA", "782.96")),
             _pessoa(RAI, "RAI", ("SOBRAL", "1480.00")),
             _pessoa(LUIZ, "LUIZ", ("OLINDA", "2844.40")),
             _pessoa("12345678909", "IGUAL", ("OLINDA", "1000.00"))]
    pagos = {ZE: {"valor": D("1640.00")}, MARIA: {"valor": D("1312.00")},
             LUIZ: {"valor": D("3100.00")}, PEDRO: {"valor": D("180.00"), "nome": "PEDRO"},
             "12345678909": {"valor": D("1000.00")}}
    ajustes = {RAI: {"fora": True, "motivo": "sem CPF no cadastro", "alterado_por": "MARCELO"}}
    por = {l["cpf"]: l for l in fc.comparar(novas, pagos, ajustes)}

    assert (por[ZE]["grupo"], por[ZE]["diferenca"]) == (fc.A_RECEBER, D("512.80"))
    assert por[MARIA]["grupo"] == fc.A_RECEBER
    assert sum(p["valor"] for p in por[MARIA]["obras_da_diferenca"]) == D("645.40")
    assert (por[RAI]["grupo"], por[RAI]["diferenca"]) == (fc.NAO_PAGO, D("1480.00"))
    assert "sem CPF no cadastro" in por[RAI]["motivos"][0], "diz por que ficou de fora"
    assert (por[LUIZ]["grupo"], por[LUIZ]["diferenca"]) == (fc.PAGO_A_MAIS, D("-255.60"))
    assert por[LUIZ]["obras_da_diferenca"] == [], "pago a mais não gera linha de pagamento"
    assert (por[PEDRO]["grupo"], por[PEDRO]["diferenca"], por[PEDRO]["saiu"]) == (fc.PAGO_A_MAIS, D("-180.00"), True)
    assert por["12345678909"]["grupo"] == fc.IGUAL


def test_o_rotulo_e_a_natureza_da_verba_complementar():
    from app.apps.analisesps import folha_geracao as g
    assert g.rotulo_da_verba("complementar2") == "Folha complementar 2"
    assert g.natureza("complementar1") == g.natureza("folha")
    assert fc.proxima_verba([{"verba": "folha"}]) == "complementar1"
    assert fc.proxima_verba([{"verba": "folha"}, {"verba": "complementar1"}]) == "complementar2"


# ---------------------------------------------------------------------------
# COM BANCO: o que foi pago, a montagem, o arquivo, "a descontar" e a trava
# ---------------------------------------------------------------------------
ANO, MES, TIPO = 2026, 10, "fim_de_mes"


@pytest.fixture
def paga(banco_analisesps, monkeypatch):
    """Outubro (fim do mês) PAGO: fechamento da folha + arquivo no log. A folha
    importada de novo é dublada (`abrir` e a apropriação), com a correção."""
    from app.apps.analisesps import folha_apropriacao_guardada as guardada, folha_arquivo
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) VALUES "
                     "('OLINDA', '50024'), ('SOBRAL', '50024')")
        for cpf, nome in ((ZE, "ZE"), (MARIA, "MARIA"), (RAI, "RAI"), (LUIZ, "LUIZ")):
            conn.execute("INSERT INTO analisesps.colaborador (cpf, nome, fase, cargo) VALUES (?,?,?,?)",
                         (cpf, nome, "Colaboradores Ativos", "PEDREIRO"))
        conn.commit()
    guardada.fechar(ANO, MES, TIPO, {"pessoas": [
        _pessoa(ZE, "ZE", ("OLINDA", "1640.00")),
        _pessoa(LUIZ, "LUIZ", ("OLINDA", "3100.00")),
        _pessoa(PEDRO, "PEDRO", ("OLINDA", "180.00")),
        _pessoa(RAI, "RAI", ("SOBRAL", "1480.00"), fora=True)],
        "total_da_folha": D("4920"), "total_apropriado": D("4920"), "fecha": True})
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.folha_arquivo_gerado (ano, mes, tipo, destino, verbas, "
                     "pessoas, total, criado_por) VALUES (?,?,?,?,?,?,?,?)",
                     (ANO, MES, TIPO, "analise", "folha", 3, D("4920"), "MARCELO"))
        conn.commit()
    folha = {"id": 7, "ano": ANO, "mes": MES, "tipo": TIPO, "linhas": [],
             "competencia": "10/2026", "rotulo_do_tipo": "fim do mês"}
    monkeypatch.setattr(folha_arquivo, "abrir", lambda i: dict(folha) if i == 7 else None)
    novas = [_pessoa(ZE, "ZE", ("OLINDA", "2152.80")),
             _pessoa(LUIZ, "LUIZ", ("OLINDA", "2844.40")),
             _pessoa(RAI, "RAI", ("SOBRAL", "1480.00"))]
    monkeypatch.setattr(fc, "_apropriado_sem_fora", lambda f: (
        {"pessoas": novas}, {RAI: {"fora": True, "motivo": "sem CPF", "alterado_por": "MARCELO"}}))
    return folha


@pytest.mark.banco
def test_o_pago_vem_do_FECHAMENTO_e_quem_estava_fora_nao_conta(paga):
    pagos = fc.pago_por_cpf(ANO, MES, TIPO)
    assert pagos[ZE]["valor"] == D("1640.00") and RAI not in pagos
    assert fc.ja_paga(ANO, MES, TIPO)


@pytest.mark.banco
def test_a_montagem_a_selecao_e_o_padrao_de_cada_grupo(paga):
    m = fc.montar(7)
    assert m["verba"] == "complementar1"
    grupos = {g: [l["cpf"] for l in ls] for g, ls in m["grupos"].items()}
    assert grupos[fc.A_RECEBER] == [ZE] and grupos[fc.NAO_PAGO] == [RAI]
    assert set(grupos[fc.PAGO_A_MAIS]) == {LUIZ, PEDRO}
    assert [l["cpf"] for l in m["a_pagar"]] == [ZE], "a receber vem marcado; não pago, desmarcado"
    assert m["total_a_pagar"] == D("512.80")
    fc.salvar_selecao(ANO, MES, TIPO, [{"cpf": RAI, "pagar": True}])
    m = fc.montar(7)
    assert [l["cpf"] for l in m["a_pagar"]] == [RAI, ZE] or {l["cpf"] for l in m["a_pagar"]} == {ZE, RAI}
    assert m["total_a_pagar"] == D("1992.80")
    linhas = fc.linhas_a_pagar(m)
    assert {(l["cpf"], l["valor"], l["conta"], l["verba"]) for l in linhas} == {
        (ZE, D("512.80"), "50024", "complementar1"), (RAI, D("1480.00"), "50024", "complementar1")}


@pytest.mark.banco
def test_GERAR_a_complementar_paga_so_a_diferenca_e_a_proxima_compara_com_tudo(paga, monkeypatch):
    from app.apps.analisesps import beevale, drive, folha_pagamento as fp
    monkeypatch.setattr(drive, "substituir_conteudo", lambda *a, **k: None)
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        {"id": "x", "link": "https://drive/x"})
    saida = fp.gerar_direto("complementar", {"folha_id": 7}, "beevale", quem="MARCELO", forcar=True)
    pagamento = [a for a in saida["arquivos"] if a["destino"] == "beevale"]
    assert pagamento and pagamento[0]["total"] == D("512.80"), "o arquivo leva só a diferença"
    assert [p["verba"] for p in fc.pagamentos_feitos(ANO, MES, TIPO)] == ["folha", "complementar1"]
    # Depois de paga a diferença, a próxima complementar já não vê nada a receber de ZE…
    m = fc.montar(7)
    assert m["verba"] == "complementar2"
    assert not m["grupos"][fc.A_RECEBER]
    assert fc.pago_por_cpf(ANO, MES, TIPO)[ZE]["valor"] == D("2152.80")
    # …e o pago a mais foi para "a descontar".
    a_descontar = {x["cpf"]: x for x in fc.a_descontar()}
    assert a_descontar[LUIZ]["valor"] == "255.60" and a_descontar[PEDRO]["valor"] == "180.00"
    assert fc.marcar_descontado(LUIZ, "2026-10-fim_de_mes", quem="MARCELO")
    assert LUIZ not in {x["cpf"] for x in fc.a_descontar()}


@pytest.mark.banco
def test_folha_JA_PAGA_nao_se_refecha(paga):
    """Refechar apagava o registro do que foi pago, e o arquivo seguinte pagaria a
    folha inteira de novo."""
    from app.apps.analisesps import folha_gestao
    with pytest.raises(folha_gestao.ErroDaGestao) as erro:
        folha_gestao.fechar(7, quem="MARCELO")
    assert "Complementar" in str(erro.value)


from tests.test_analisesps_telas import _como_mestre, app  # noqa: E402,F401


@pytest.mark.banco
def test_a_TELA_e_o_relatorio_da_complementar(paga, app):
    import io

    from openpyxl import load_workbook
    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/folha/7/complementar").get_data(as_text=True)
    assert "Este mês já foi pago" in html and "Folha complementar 1" in html
    assert "A receber" in html and "Não foi pago antes" in html and "Pago a mais" in html
    assert "saiu da folha nova" in html and "sem CPF" in html
    assert 'id="gd-botao"' in html, "o mestre gera daqui"
    planilha = load_workbook(io.BytesIO(cliente.get(
        "/analisesps/folha/7/complementar/relatorio.xlsx").data))
    situacoes = [r[0].value for r in planilha["Complementar"].iter_rows(min_row=2)]
    assert {"A receber", "Não foi pago antes", "Pago a mais"} <= set(situacoes)
