# -*- coding: utf-8 -*-
"""
GRATIFICADOS E PAGAMENTO VIA SP — 17/10/2026.

O dono: "a tela de gratificados e a tela de PJ e Pró-labore (…) pagamento via SP
(…) um SP por pessoa (…) cada um com o seu rateio (…) desconto ou acréscimo (…) a
visão do todo e do que está acontecendo naquela quinzena e na outra". A regra
de quem entra é a QUERY da aba GM; a de quanto, metade na quinzena e metade no
fim do mês, salvo parcela única.
"""
import datetime as dt
import json
from decimal import Decimal as D

import pytest

from tests.test_analisesps_telas import _como_mestre, app  # noqa: F401 — a fixture

GRAT, UNICA, PJ1, PJ2, CLT, SAIU = ("99713349334", "03513441363", "11122233396",
                                    "52998224725", "11144477735", "12345678909")
ANO, MES = 2026, 10


# ---------------------------------------------------------------------------
# AS REGRAS DE DINHEIRO — sem banco
# ---------------------------------------------------------------------------
def test_METADE_na_quinzena_e_o_resto_no_fim_salvo_parcela_unica():
    from app.apps.analisesps.gratificados import partes_do_mes
    assert partes_do_mes(D("1001.00"), False) == (D("500.50"), D("500.50"))
    assert partes_do_mes(D("1000.01"), False) == (D("500.00"), D("500.01")), \
        "o centavo da divisão vai para o fim do mês — as duas somam o mês"
    assert partes_do_mes(D("800.00"), True) == (D("0.00"), D("800.00"))


def test_o_valor_do_mes_e_a_GRATIFICACAO_e_sem_ela_o_valor_do_mes():
    from app.apps.analisesps.gratificados import valor_do_mes
    assert valor_do_mes("1.500,00", "") == (D("1500.00"), "gratificacao", "")
    assert valor_do_mes(None, "5000") == (D("5000.00"), "mensal", "")
    valor, _, alerta = valor_do_mes(D("100"), "200")
    assert valor == D("100.00") and "diferentes" in alerta, "os dois preenchidos: alerta"
    assert valor_do_mes("", "")[0] == D("0.00")


def test_o_PERIODO_que_a_tela_abre():
    from app.apps.analisesps.gratificados import periodo_padrao
    assert periodo_padrao(dt.date(2026, 10, 5)) == (2026, 9, "fim_de_mes")
    assert periodo_padrao(dt.date(2026, 1, 3)) == (2025, 12, "fim_de_mes")
    assert periodo_padrao(dt.date(2026, 10, 17)) == (2026, 10, "quinzena")
    assert periodo_padrao(dt.date(2026, 10, 28)) == (2026, 10, "fim_de_mes")


def test_QUEM_ENTRA_e_a_query_da_aba_GM():
    from app.apps.analisesps.gratificados import SP, GM, entra_na_lista, via_da_pessoa
    ativo = {"fase": "Colaboradores Ativos"}
    for tipo in ("PJ", "Pró-labore", "Autônomo Mensalista", "Estágio"):
        assert entra_na_lista(dict(ativo, tipo_contrato=tipo), {}), tipo
    assert not entra_na_lista(dict(ativo, tipo_contrato="CLT"), {"recebe_gratificacao": ""})
    assert entra_na_lista(dict(ativo, tipo_contrato="CLT"), {"recebe_gratificacao": "Sim"})
    assert not entra_na_lista({"fase": "Colaboradores Desligados", "tipo_contrato": "PJ"}, {})
    # Sem a coluna da marcação na carga, vale ter gratificação.
    assert entra_na_lista(dict(ativo, tipo_contrato="CLT", valor_gratificacao=D("10")), {})
    assert via_da_pessoa({"recebe_via_sp": "Sim"}) == SP
    assert via_da_pessoa({"recebe_via_sp": "Não"}) == GM == via_da_pessoa({})


def test_a_CARGA_traz_as_colunas_da_GM_no_json_e_guarda_o_vazio():
    from app.apps.analisesps import colaboradores as col
    cabecalho = ["CPF (Cadastro de Pessoa Física)", "Nome Completo",
                 "Recebe Gratificação Mensal", "Valor do Mês", "Recebe via SP",
                 "Nome Credor", "Pix CNPJ", "Chave Pix Tipo", "Chave Pix"]
    posicoes, _ = col._achar_colunas(cabecalho)
    assert posicoes["pg_recebe_gratificacao"] == 2 and posicoes["pg_recebe_via_sp"] == 4
    linha = {0: "997.133.493-34", 1: "FULANO", 2: "", 3: "5.000,00", 4: "Sim",
             5: "FULANO LTDA", 6: "12.345.678/0001-95", 7: "CNPJ", 8: ""}
    reg = col._registro(linha, posicoes)
    assert reg["_documentos"]["recebe_via_sp"] == "Sim"
    assert reg["_documentos"]["recebe_gratificacao"] == "", "o vazio é guardado"
    # Sem os nomes, a posição — só se o cabeçalho dali tiver a palavra esperada.
    cab = ["CPF (Cadastro de Pessoa Física)", "Nome Completo"] + ["x"] * 58 + ["Pgto via SP?"]
    posicoes, _ = col._achar_colunas(cab)
    assert posicoes.get("pg_recebe_via_sp") == 60
    cab[60] = "Paga por BeeVale"
    posicoes, _ = col._achar_colunas(cab)
    assert "pg_recebe_via_sp" not in posicoes


# ---------------------------------------------------------------------------
# COM BANCO
# ---------------------------------------------------------------------------
@pytest.fixture
def banco_gm(banco_analisesps, monkeypatch):
    from app.apps.analisesps import folha_cards, gratificados as gm
    from app.apps.analisesps.db import conexao

    def docs(**k):
        base = {"recebe_gratificacao": "", "recebe_via_sp": "", "valor_mensal": ""}
        base.update(k)
        return json.dumps(base)

    with conexao() as conn:
        for codigo, conta in (("CREPEOLINDA", "50024"), ("IFSPSAOJOSE", "7011")):
            conn.execute("INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                         " VALUES ('obra', ?, ?)", (codigo, codigo[:3]))
            conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                         " VALUES (?, ?)", (codigo, conta))
        pessoas = [
            (GRAT, "GRATIFICADO UM", "Colaboradores Ativos", "CLT", D("1001.00"), "",
             "IFSPSAOJOSE", docs(recebe_gratificacao="Sim")),
            (UNICA, "ESTAGIARIA DOIS", "Colaboradores Ativos", "Estágio", None, "Sim",
             "IFSPSAOJOSE", docs(valor_mensal="800,00")),
            (PJ1, "PJ UM", "Colaboradores Ativos", "PJ", None, "", "IFSPSAOJOSE",
             docs(recebe_via_sp="Sim", valor_mensal="5.000,00", nome_credor="PJ UM LTDA",
                  pix_cnpj="12.345.678/0001-95", pix_tipo="CNPJ",
                  pix_chave="12345678000195")),
            (PJ2, "PRO LABORE DOIS", "Colaboradores Ativos", "Pró-labore", None, "Sim",
             "IFSPSAOJOSE", docs(recebe_via_sp="Sim", valor_mensal="3000")),
            (CLT, "CLT SEM MARCA", "Colaboradores Ativos", "CLT", D("300.00"), "",
             "IFSPSAOJOSE", docs(recebe_gratificacao="Não")),
            (SAIU, "PJ DESLIGADO", "Colaboradores Desligados", "PJ", None, "",
             "IFSPSAOJOSE", docs(valor_mensal="9000")),
        ]
        for cpf, nome, fase, tipo, grat, unica, obra, documentos in pessoas:
            conn.execute(
                "INSERT INTO analisesps.colaborador (cpf, nome, fase, tipo_contrato, "
                " valor_gratificacao, parcela_unica, obra_codigo, documentos) "
                " VALUES (?,?,?,?,?,?,?,?)",
                (cpf, nome, fase, tipo, grat, unica, obra, documentos))
        conn.commit()
    monkeypatch.setattr(folha_cards, "_codigos_omie", lambda: {
        folha_cards._chave("CREPEOLINDA"): "111", folha_cards._chave("IFSPSAOJOSE"): "222"})
    monkeypatch.setattr(folha_cards, "_plano_financeiro", lambda: ({
        folha_cards._chave("Gratificações e Extras"): [
            {"nome": "Gratificações e Extras", "record_id": "R-GRA", "codigo_omie": "2.03.01"}],
    }, ""))
    # O ponto: só o gratificado bateu ponto, na CREPEOLINDA.
    monkeypatch.setattr(gm, "_obras_do_ponto", lambda ano, mes: (
        {GRAT: {"obra": "CREPEOLINDA", "dias": 9}}, {GRAT: {}}))
    return banco_analisesps


def _por(calculado):
    return {p["cpf"]: p for p in calculado["pessoas"]}




@pytest.mark.banco
def test_GRATIFICADOS_na_quinzena_metade_e_parcela_unica_so_no_fim(banco_gm):
    from app.apps.analisesps import gratificados as gm
    c = gm.calcular(ANO, MES, "quinzena", gm.GM)
    p = _por(c)
    assert set(p) == {GRAT, UNICA}, "PJ via SP, CLT sem marca e desligado ficam fora"
    assert c["na_outra_tela"] == 2
    g = p[GRAT]
    assert (g["valor_quinzena"], g["valor_fim"], g["valor"]) == (
        D("500.50"), D("500.50"), D("500.50"))
    assert g["obra"] == "CREPEOLINDA" and g["obra_de_onde"] == "ponto" and g["conta"] == "50024"
    assert g["pagar"]
    u = p[UNICA]
    assert u["no_periodo"] == "so_fim" and u["valor"] == D("0.00") and not u["pagar"]
    assert any("parcela única" in m for m in u["motivos"])
    assert u["obra_de_onde"] == "cadastro", "sem ponto nem rateio: a do cadastro, marcada"
    assert c["total_quinzena"] == D("500.50") and c["total_fim"] == D("1300.50")
    fim = _por(gm.calcular(ANO, MES, "fim_de_mes", gm.GM))
    assert fim[UNICA]["valor"] == D("800.00") and fim[UNICA]["pagar"]


@pytest.mark.banco
def test_DESCONTO_e_ACRESCIMO_valem_so_no_periodo_e_a_OBRA_DIVIDIDA_vale_sempre(banco_gm):
    from app.apps.analisesps import gratificados as gm
    with pytest.raises(gm.ErroDosGratificados):
        gm.ajustar(ANO, MES, "quinzena", GRAT, "-100", "")      # sem motivo, não
    gm.ajustar(ANO, MES, "quinzena", GRAT, "-100,50", "adiantamento", quem="MARCELO")
    g = _por(gm.calcular(ANO, MES, "quinzena", gm.GM))[GRAT]
    assert g["valor"] == D("400.00") and g["ajuste_motivo"] == "adiantamento"
    assert _por(gm.calcular(ANO, MES, "fim_de_mes", gm.GM))[GRAT]["valor"] == D("500.50")
    gm.escolher_obra(GRAT, "RATEIO:CREPEOLINDA=60;IFSPSAOJOSE=40")
    g = _por(gm.calcular(ANO, MES, "quinzena", gm.GM))[GRAT]
    assert [(x["obra"], x["valor"], x["conta"]) for x in g["rateio"]] == [
        ("CREPEOLINDA", D("240.00"), "50024"), ("IFSPSAOJOSE", D("160.00"), "7011")]
    assert g["rotulo_obra"] == "dividida por você"
    linhas = gm.linhas_a_pagar(gm.calcular(ANO, MES, "quinzena", gm.GM))
    assert sorted((l["obra"], l["valor"]) for l in linhas) == [
        ("CREPEOLINDA", D("240.00")), ("IFSPSAOJOSE", D("160.00"))]
    gm.salvar_selecao(ANO, MES, "quinzena", [{"cpf": GRAT, "pagar": False}])
    assert not _por(gm.calcular(ANO, MES, "quinzena", gm.GM))[GRAT]["pagar"]


def _subir(monkeypatch):
    from app.apps.analisesps import beevale, drive
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        subidos.append((nome, k.get("mime"), conteudo))
                        or {"id": f"d{len(subidos)}", "link": f"https://drive/{len(subidos)}"})
    return subidos


@pytest.mark.banco
def test_GERAR_os_gratificados_guarda_o_lote_e_quem_saiu_nao_sai_de_novo(banco_gm, monkeypatch):
    from app.apps.analisesps import folha_cards as fcd, folha_pagamento as fp
    from app.apps.analisesps import gratificados as gm
    from tests.test_analisesps_folha_cards import PipefyFalso
    pipe = PipefyFalso(monkeypatch)
    _subir(monkeypatch)
    dados = {"ano": ANO, "mes": MES, "periodo": "fim_de_mes"}
    saida = fp.gerar_direto("gratificacao", dados, "somapay", quem="MARCELO")
    pagamento = {a["conta"]: a["total"] for a in saida["arquivos"] if a["destino"] == "somapay"}
    assert pagamento == {"50024": D("500.50"), "7011": D("800.00")}
    analise = next(a["id"] for a in saida["arquivos"] if a["destino"] == fp.ANALISE)
    assert {p["cpf"] for p in gm.lote_da_analise(analise)["pessoas"]} == {GRAT, UNICA}
    c = gm.calcular(ANO, MES, "fim_de_mes", gm.GM)
    assert all(p["gerada"] and not p["pagar"] for p in c["pessoas"])
    with pytest.raises(fp.ErroDoPagamento):
        fp.gerar_direto("gratificacao", dados, "somapay", quem="MARCELO")
    vista = fcd.previa(analise)
    assert vista["bloqueios"] == [], vista["bloqueios"]
    assert vista["grupos"][0]["grupo"] == "Gratificados e Mensalistas"
    fcd.lancar(analise, quem="MARCELO")
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 2, "uma SP por conta"
    fp.excluir_arquivos([a["id"] for a in saida["arquivos"]], quem="MARCELO")
    assert gm.lote_da_analise(analise) is None
    assert all(not p["gerada"] for p in gm.calcular(ANO, MES, "fim_de_mes", gm.GM)["pessoas"])


@pytest.mark.banco
def test_VIA_SP_uma_SP_POR_PESSOA_com_o_favorecido_e_o_rateio_dela(banco_gm, monkeypatch):
    from app.apps.analisesps import folha_cards as fcd, folha_pagamento as fp
    from app.apps.analisesps import gratificados as gm
    from tests.test_analisesps_folha_cards import PipefyFalso
    pipe = PipefyFalso(monkeypatch)
    subidos = _subir(monkeypatch)
    c = gm.calcular(ANO, MES, "quinzena", gm.SP)
    p = _por(c)
    assert set(p) == {PJ1, PJ2}
    assert p[PJ1]["valor"] == D("2500.00") and p[PJ1]["favorecido"]["pj"]
    assert p[PJ1]["favorecido"]["documento"] == "12.345.678/0001-95"
    assert p[PJ2]["valor"] == D("0.00"), "pró-labore com parcela única: só no fim"
    gm.escolher_obra(PJ2, "RATEIO:CREPEOLINDA=50;IFSPSAOJOSE=50")
    dados = {"ano": ANO, "mes": MES, "periodo": "fim_de_mes"}
    resumo = fp.resumo_direto("viasp", dados, "beevale")
    assert resumo["resumo"]["total"] == D("5500.00")
    saida = fp.gerar_direto("viasp", dados, "beevale", quem="MARCELO")
    destinos = sorted(a["destino"] for a in saida["arquivos"])
    assert "viasp" in destinos and fp.ANALISE in destinos
    assert not any(a["destino"] in ("beevale", "somapay") for a in saida["arquivos"]), \
        "via SP não gera arquivo de banco"
    assert any(n.startswith("Pagamento via SP") for n, _, _ in subidos)
    analise = next(a["id"] for a in saida["arquivos"] if a["destino"] == fp.ANALISE)
    vista = fcd.previa(analise)
    assert vista["bloqueios"] == [], vista["bloqueios"]
    [grupo] = vista["grupos"]
    assert grupo["verba"] == "viasp" and grupo["uma_por_pessoa"]
    assert sorted(s["nome"] for s in grupo["sps"]) == ["PJ UM", "PRO LABORE DOIS"]
    dois = next(s for s in grupo["sps"] if s["cpf"] == PJ2)
    assert len(dois["obras"]) == 2 and "Rateio por obra:" in dois["descricao"]
    assert dois["descricao"].splitlines()[0] == "Conta Origem: 50024" or \
        dois["descricao"].splitlines()[0] == "Conta Origem: 7011"
    fcd.lancar(analise, quem="MARCELO")
    criados = pipe.do_pipe(fcd.PIPE_SP)
    assert len(criados) == 2, "uma SP por pessoa, mesmo na mesma conta"
    um = next(x for x in criados if x["titulo"] == "Pagamento via SP - PJ UM")
    sp_um = next(s for s in grupo["sps"] if s["cpf"] == PJ1)
    campos = {v["campo"]: v["valor"]
              for v in fcd.campos_da_sp(grupo, sp_um, "", dt.datetime(2026, 10, 17))}
    assert campos["local"] == "PJ UM LTDA", "o credor é o favorecido, não a BWS"
    sp_dois = next(s for s in grupo["sps"] if s["cpf"] == PJ2)
    campos = {v["campo"]: v["valor"]
              for v in fcd.campos_da_sp(grupo, sp_dois, "", dt.datetime(2026, 10, 17))}
    assert campos["radio_horizontal_t_tulo"] == "Pessoa Física"
    assert campos["cpf"] == "529.982.247-25" and "cnpj" not in campos
    assert um["valores"]["radio_horizontal_t_tulo"] == "Pessoa Jurídica"
    assert um["valores"]["cnpj"] == "12.345.678/0001-95"
    assert "Chave Pix (CNPJ): 12345678000195" in um["valores"]["descri_o"]
    # Lançar de novo não cria nada (o andamento guarda as duas).
    with pytest.raises(fcd.ErroDosCards):
        fcd.lancar(analise, quem="MARCELO")
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 2


@pytest.mark.banco
def test_as_DUAS_TELAS_abrem_e_ajustam(banco_gm, app):  # noqa: F811
    cliente = _como_mestre(app)
    tela = cliente.get(f"/analisesps/folha/gratificados?ano={ANO}&mes={MES}&periodo=quinzena")
    html = tela.get_data(as_text=True)
    assert tela.status_code == 200 and "GRATIFICADO UM" in html
    assert "só no fim do mês" in html and "PJ UM" not in html
    assert "2 pessoa(s) em Pagamento via SP" in html
    sp = cliente.get(f"/analisesps/folha/via-sp?ano={ANO}&mes={MES}&periodo=quinzena")
    html = sp.get_data(as_text=True)
    assert sp.status_code == 200 and "PJ UM LTDA" in html and "GRATIFICADO UM" not in html
    r = cliente.post("/analisesps/api/folha/gratificados/ajuste", json={
        "ano": ANO, "mes": MES, "periodo": "quinzena", "cpf": GRAT, "valor": "50",
        "motivo": "bônus"}).get_json()
    assert r["ok"]
    html = cliente.get(f"/analisesps/folha/gratificados?ano={ANO}&mes={MES}&periodo=quinzena"
                       ).get_data(as_text=True)
    assert "bônus" in html
    r = cliente.post("/analisesps/api/folha/gratificados/obra",
                     json={"cpf": GRAT, "obra": "IFSPSAOJOSE"}).get_json()
    assert r["ok"]
    pdf = cliente.get(f"/analisesps/folha/gratificados/relatorio.pdf?via=sp&ano={ANO}"
                      f"&mes={MES}&periodo=fim_de_mes")
    assert pdf.data.startswith(b"%PDF")
