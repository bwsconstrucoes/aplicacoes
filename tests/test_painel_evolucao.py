# -*- coding: utf-8 -*-
"""
A evolução do rateio — a conta do cenário aberta mês a mês (16/11/2026).

O dono, depois de mostrar a prestação a um dos sócios: "os sócios não vão se
conter apenas com o cálculo final, sem compreender na minúcia como é que
aqueles valores estão chegando e sendo apropriados (…) precisa poder observar
por obra, por projeto, por estrutura".

O que se prova aqui, e por que importa: a tela NÃO faz conta nova — ela
reorganiza a do cenário. Então o teste que vale é o que confere que cada
número dela FECHA com o do Resultado. Uma tela de "como chegamos aqui" que
somasse diferente do resultado seria pior do que tela nenhuma.

A primeira metade roda sem banco (a conta é função pura); a segunda, com banco
de verdade, abre a tela e baixa o relatório.
"""
from __future__ import annotations

import io

import pytest

from tests.test_painel_cenario import (  # noqa: F401  (fixtures)
    MATRIZ, _calcular, _cliente, _cliente_preso, base, cenario, preso)


# ===========================================================================
# 1. Sem banco: a conta guarda as parcelas, e a evolução fecha com ela
# ===========================================================================
def _calculo_de_exemplo(nome_da_obra_b="OBRA B"):
    """Três obras em dois lados, dois meses — montado com as MESMAS funções que
    o cenário usa, para o teste não ter uma conta paralela.

    Matriz: 1.000 por mês, 20% fica no principal e 10% é da filial. Filial: 300
    por mês. Juros: 100 por mês. Pessoal: A 600, B 300, C 100 (A é do principal).
    """
    from app.apps.painel import prestacao
    a, b, c = "OBRA A", nome_da_obra_b, "OBRA C"
    obras = {a: {"projeto": "CE"}, b: {"projeto": "PE"}, c: {"projeto": "PE"}}
    lado_de = {a: "principal", b: "demais", c: "demais"}
    meses = ("2025-01", "2025-02")
    driver = [(m, o, v) for m in meses for o, v in ((a, 600), (b, 300), (c, 100))]
    pesos = {"periodos": {"grupo": {"Administrativas": [("1900-01", 20.0, 10.0, "", 0.0)]}}}
    rateio = prestacao.calcular_rateio_do_cenario(
        [{"mes": m, "grupo": "Administrativas", "categoria": "Aluguel", "valor": -1000.0}
         for m in meses],
        [], driver, obras, pesos, lado_de=lado_de,
        so_dos_demais=[{"mes": m, "grupo": "Pessoal", "categoria": "Salários",
                        "valor": -300.0} for m in meses])
    # B consome caixa, A e C geram: só B fica no vermelho
    caixa = [(m, o, v) for m in meses for o, v in ((a, 2000.0), (b, -5000.0), (c, 500.0))]
    juros_por_mes = {m: -100.0 for m in meses}
    juros = prestacao.alocar_juros_por_deficit(
        caixa, juros_por_mes, obras, rateio_recebido=rateio["alocacoes"],
        lado_de=lado_de)
    apurado = prestacao.apurar([], obras, rateio["alocacoes"], juros["alocacoes"])
    return {
        "obras": obras, "lado_de": lado_de, "rateio": rateio, "juros": juros,
        "driver": driver, "caixa": caixa, "juros_por_mes": juros_por_mes,
        "por_obra": prestacao.totalizar_por_obra(apurado),
        "sobras": rateio["sobras"] + juros["sobras"],
        "juros_fora_do_resultado": 0.0, "principal": "CE",
    }


def test_o_rateio_guarda_as_parcelas_sem_mudar_a_soma():
    """`detalhe` é novo; `alocacoes` é o que sempre foi. A soma das três
    parcelas de cada obra em cada mês tem de dar a alocação — ao centavo."""
    calculo = _calculo_de_exemplo()
    rateio = calculo["rateio"]
    detalhe = rateio["detalhe"]["por_obra_mes"]
    assert set(detalhe) == set(rateio["alocacoes"])
    for chave, alocado in rateio["alocacoes"].items():
        d = detalhe[chave]
        assert d["livre"] + d["lado"] + d["filial"] == pytest.approx(alocado, abs=0.005)
    # janeiro: 1.000 da matriz = 200 ficam + 100 da filial + 700 rateados
    partes = rateio["detalhe"]["partes_do_mes"]["2025-01"]
    assert (partes["principal"], partes["demais"], partes["livre"], partes["filial"]) == \
        pytest.approx((-200.0, -100.0, -700.0, -300.0))
    # a obra A tem 60% da régua e é a única do principal: 60% de 700 + os 200
    a = detalhe[("OBRA A", "2025-01")]
    assert (a["fracao"], a["fracao_do_lado"]) == pytest.approx((0.6, 1.0))
    assert (a["livre"], a["lado"], a["filial"]) == pytest.approx((-420.0, -200.0, 0.0))
    # B tem 30% entre todas e 75% dentro do lado dela (300 de 400)
    b = detalhe[("OBRA B", "2025-01")]
    assert (b["fracao"], b["fracao_do_lado"]) == pytest.approx((0.3, 0.75))
    assert (b["livre"], b["lado"], b["filial"]) == pytest.approx((-210.0, -75.0, -225.0))
    # por grupo: o que a tela de "matriz por grupo" mostra
    grupo = rateio["detalhe"]["por_grupo_mes"][("2025-01", "Administrativas")]
    assert (grupo["valor"], grupo["fica"], grupo["filial"]) == pytest.approx((-1000.0, -200.0, -100.0))


def test_a_evolucao_fecha_com_a_conta_do_cenario():
    from app.apps.painel import evolucao
    calculo = _calculo_de_exemplo()
    dados = evolucao.montar(calculo, principal="CE")

    # cada obra absorveu exatamente o que o Resultado diz que ela recebeu
    for no in dados["arvores"]["obra"]:
        do_resultado = calculo["por_obra"][no["nome"]]
        assert no["estrutura"] == pytest.approx(-do_resultado["rateio"], abs=0.01)
        assert no["juros"] == pytest.approx(-do_resultado["juros"], abs=0.01)
        assert no["livre"] + no["lado"] + no["filial"] == pytest.approx(no["estrutura"], abs=0.01)

    # as três visões somam o mesmo total
    for visao in ("estrutura", "projeto", "obra"):
        assert sum(n["absorvido"] for n in dados["arvores"][visao]) == \
            pytest.approx(dados["geral"]["absorvido"], abs=0.01)
        assert sum(n["pct_absorvido"] for n in dados["arvores"][visao]) == pytest.approx(1.0)

    c = dados["caminho"]
    assert (c["matriz"], c["fica"], c["da_filial"], c["rateia"], c["filial"], c["juros"]) == \
        pytest.approx((2000.0, 400.0, 200.0, 1400.0, 600.0, 200.0))
    # o que saiu é o que as obras absorveram — e a tela diz que fecha
    assert c["fecha"] and c["diferenca"] == pytest.approx(0.0, abs=0.01)
    assert c["por_lado"]["principal"]["estrutura"] == pytest.approx(2 * (420 + 200))
    # o juro inteiro caiu em B, a única no vermelho
    assert c["por_lado"]["demais"]["juros"] == pytest.approx(200.0)
    assert evolucao.achar(dados, "obra:OBRA B")["juros"] == pytest.approx(200.0)


def test_a_estrutura_principal_abre_direto_nas_obras():
    """O lado principal É um projeto só. Abrir "projeto" dentro dele repetiria
    a mesma linha; as obras vêm direto. O outro lado abre em projetos."""
    from app.apps.painel import evolucao
    dados = evolucao.montar(_calculo_de_exemplo(), principal="CE")
    principal, demais = dados["arvores"]["estrutura"]
    assert (principal["nome"], demais["nome"]) == ("CE", "BWSFILIAL")
    assert [f["tipo"] for f in principal["filhos"]] == ["obra"]
    assert [f["tipo"] for f in demais["filhos"]] == ["projeto"]
    linhas = evolucao.linhas_da_arvore(dados, "estrutura")
    assert [l["nivel"] for l in linhas] == [0, 1, 0, 1, 2, 2]
    assert linhas[4]["pai"] == "lado:demais/projeto:PE"


def test_a_ficha_do_projeto_e_a_soma_das_obras_dele():
    from app.apps.painel import evolucao
    dados = evolucao.montar(_calculo_de_exemplo(), principal="CE")
    projeto = evolucao.ficha(dados, evolucao.achar(dados, "projeto:PE"))
    b = evolucao.ficha(dados, evolucao.achar(dados, "obra:OBRA B"))
    c = evolucao.ficha(dados, evolucao.achar(dados, "obra:OBRA C"))
    for i, linha in enumerate(projeto["linhas"]):
        for campo in ("regua", "estrutura", "caixa", "juros", "fracao"):
            assert linha[campo] == pytest.approx(b["linhas"][i][campo] + c["linhas"][i][campo])
        # o caixa acumulado do conjunto é a soma dos acumulados
        assert linha["acumulado"] == pytest.approx(
            b["linhas"][i]["acumulado"] + c["linhas"][i]["acumulado"])
    # B: -5.000 de caixa e 510 de estrutura em janeiro -> acumulado -5.510
    assert b["linhas"][0]["acumulado"] == pytest.approx(-5000.0 - 510.0)
    assert b["linhas"][0]["buraco"] == pytest.approx(5510.0)
    assert b["linhas"][0]["pct_dos_juros"] == pytest.approx(1.0)


def test_a_conta_do_mes_diz_os_numeros_e_escapa_o_nome():
    """A conta em palavras leva os números da linha — e nome de obra vindo do
    OMIE com sinal de HTML não pode virar HTML na tela."""
    from app.apps.painel import evolucao
    dados = evolucao.montar(_calculo_de_exemplo("OBRA <b>B</b>"), principal="<i>CE</i>")
    ficha = evolucao.ficha(dados, evolucao.achar(dados, "obra:OBRA <b>B</b>"), com_a_conta=True)
    texto = " ".join(str(p) for p in ficha["linhas"][0]["conta"])
    assert "R$ 1.000,00" in texto            # a matriz gastou
    assert "30,0%" in texto                  # a régua
    assert "R$ 210,00" in texto              # 30% de 700
    assert "R$ 510,00" in texto              # estrutura do mês
    assert "-R$ 5.510,00" in texto and "no vermelho" in texto
    assert "desta obra" in texto and "100,0%" in texto     # os juros
    assert "<i>CE</i>" not in texto and "&lt;i&gt;CE&lt;/i&gt;" in texto
    # quem não estava no vermelho não leva juro, e a conta diz por quê
    conta_de_a = evolucao.ficha(dados, evolucao.achar(dados, "obra:OBRA A"),
                                com_a_conta=True)["linhas"][0]["conta"]
    assert "não absorveu juros" in str(conta_de_a[-1])


def test_sem_projeto_principal_nao_ha_visao_por_estrutura():
    from app.apps.painel import evolucao
    calculo = _calculo_de_exemplo()
    calculo["lado_de"] = {o: "demais" for o in calculo["lado_de"]}
    dados = evolucao.montar(calculo, principal="")
    assert not dados["tem_lados"] and dados["arvores"]["estrutura"] == []
    assert evolucao.visao_valida(dados, "estrutura") == "projeto"
    assert evolucao.visao_valida(dados, "obra") == "obra"
    assert evolucao.visao_valida(dados, "qualquer coisa") == "projeto"


def test_as_abas_do_relatorio_fecham_e_cabem_no_excel():
    from app.apps.painel import evolucao
    calculo = _calculo_de_exemplo()
    dados = evolucao.montar(calculo, principal="CE")
    abas = evolucao.abas(dados, calculo)
    nomes = [nome for nome, _c, _l in abas]
    assert nomes == ["Caminho do dinheiro", "Por estrutura", "Por projeto", "Por obra",
                     "Empresa mes a mes", "Matriz por grupo e mes",
                     "Estrutura CE", "Estrutura BWSFILIAL", "Obra mes a mes"]
    # (o projeto PE é a BWSFILIAL inteira neste exemplo, e o CE é o principal:
    # a ficha de cada um já saiu como estrutura — não se repete como projeto)
    assert all(len(n) <= 31 for n in nomes) and len(set(nomes)) == len(nomes)
    por_obra = dict((n, l) for n, _c, l in abas)["Por obra"]
    assert por_obra[-1]["estrutura_nome"] == "TOTAL"
    assert por_obra[-1]["absorvido"] == pytest.approx(dados["geral"]["absorvido"])
    # toda coluna declarada existe em toda linha (a planilha não inventa célula)
    for nome, colunas, linhas in abas:
        for linha in linhas:
            assert all(chave in linha for chave, _t in colunas), nome
    # o recorte de um projeto: o resumo, a ficha dele e a de cada obra
    recorte = [n for n, _c, _l in evolucao.abas(dados, calculo, "projeto:PE")]
    assert recorte == ["Caminho do dinheiro", "Resumo", "Projeto PE", "Obra OBRA B", "Obra OBRA C"]
    # o PDF "com tudo": uma ficha por obra no lugar da lista longa
    tudo = [n for n, _c, _l in evolucao.abas(dados, calculo, ficha_de_cada_obra=True)]
    assert tudo[-3:] == ["Obra OBRA A", "Obra OBRA B", "Obra OBRA C"]
    assert "Obra mes a mes" not in tudo


def test_nome_de_aba_nao_repete_nem_passa_de_31():
    from app.apps.painel import evolucao
    usados: set = set()
    um = evolucao._nome_de_aba("Obra", "ESCOLA MUNICIPAL DE ENSINO FUNDAMENTAL A", usados)
    dois = evolucao._nome_de_aba("Obra", "ESCOLA MUNICIPAL DE ENSINO FUNDAMENTAL B", usados)
    assert um != dois and len(um) <= 31 and len(dois) <= 31
    assert not set("[]:*?/\\") & set(evolucao._nome_de_aba("Obra", "A/B: [x]*?", usados))


def test_o_grafico_marca_o_ano_so_em_janeiro():
    from app.apps.painel import evolucao
    linhas = [{"mes": m, "rotulo": m, "a": v, "b": 1.0}
              for m, v in (("2024-11", 1.0), ("2024-12", 3.0), ("2025-01", -9.0))]
    colunas = evolucao.colunas(linhas, ("a", "b"))
    assert [c["marca"] for c in colunas] == ["2024", "", "2025"]
    assert colunas[1]["altura"] == 100.0 and colunas[1]["partes"] == [75.0, 25.0]
    # estorno não desenha para baixo: a coluna fica só com a outra parte
    assert colunas[2]["partes"] == [0.0, 100.0]


# ===========================================================================
# 2. Com banco: a tela abre, respeita quem olha, e o relatório sai
# ===========================================================================
banco = pytest.mark.banco


def _obra_de_outro_projeto(conn):
    """Uma terceira obra, de outro projeto — para haver dois lados."""
    conn.execute(
        "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
        " situacao_vencimento, categoria, grupo, departamento, projeto,"
        " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
        " VALUES (811,'2. Contas a Pagar','DRE','PAGO','Quitado','Salários',"
        "         'Despesas com Pessoal','OBRA DO NORDESTE','BETA','FORNECEDOR X',"
        "         '2025-03-10',2025,-3000,0,0,0)")


@pytest.fixture()
def dois_lados(cenario):
    from app.apps.painel import cenarios, consultas
    from app.apps.painel import db as painel_db
    with painel_db.conexao() as conn:
        _obra_de_outro_projeto(conn)
        conn.commit()
    consultas.esquecer_listas()
    cenarios.gravar_projetos(cenario, "ALFA", {})
    return cenario


@banco
def test_a_evolucao_do_banco_fecha_com_o_resultado(dois_lados):
    from app.apps.painel import cenarios, evolucao
    from app.apps.painel.web import _evolucao_do_cenario
    calculo, dados = _evolucao_do_cenario(cenarios.completo(dois_lados))
    assert dados["tem_lados"]
    assert {n["nome"] for n in dados["arvores"]["obra"]} == \
        {"MUITA GENTE", "POUCA GENTE", "OBRA DO NORDESTE"}
    for no in dados["arvores"]["obra"]:
        assert no["estrutura"] == pytest.approx(-calculo["por_obra"][no["nome"]]["rateio"], abs=0.02)
        assert no["juros"] == pytest.approx(-calculo["por_obra"][no["nome"]]["juros"], abs=0.02)
    # 900 de aluguel + 300 de patrimônio, sem regra: tudo rateado entre todas
    assert dados["caminho"]["matriz"] == pytest.approx(1200.0, abs=0.02)
    assert dados["caminho"]["juros"] == pytest.approx(600.0, abs=0.02)
    assert dados["caminho"]["fecha"]
    assert evolucao.visao_valida(dados, "") == "estrutura"


@banco
def test_a_tela_de_evolucao_abre_com_a_arvore_e_a_conta(dois_lados, monkeypatch):
    cliente = _cliente(monkeypatch)
    r = cliente.get(f"/painel/prestacao/evolucao?cenario={dois_lados}")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    for trecho in ("O caminho do dinheiro", "Quem absorveu o quê", "Ficha mês a mês",
                   "MUITA GENTE", "OBRA DO NORDESTE", "BWSFILIAL", "A conta fecha"):
        assert trecho in html, trecho
    # a obra pedida vem aberta na ficha, com a conta do mês dela
    html = cliente.get(f"/painel/prestacao/evolucao?cenario={dois_lados}"
                       "&visao=obra&ver=obra:MUITA GENTE").get_data(as_text=True)
    assert "Ficha mês a mês — MUITA GENTE" in html
    assert "A conta de 03/2025 — MUITA GENTE" in html
    assert "A régua é o custo de mão de obra do mês" in html
    # o administrador abre os lançamentos por trás do mês
    assert "Abrir os lançamentos deste mês" in html and "de=2025-03-01" in html
    # o menu das telas da prestação ganhou a porta
    html = cliente.get(f"/painel/prestacao/resultado?cenario={dois_lados}").get_data(as_text=True)
    assert "/painel/prestacao/evolucao" in html


@banco
def test_chave_que_nao_existe_cai_no_primeiro_no_e_nao_da_erro(dois_lados, monkeypatch):
    cliente = _cliente(monkeypatch)
    r = cliente.get(f"/painel/prestacao/evolucao?cenario={dois_lados}"
                    "&visao=inventada&ver=obra:NAO EXISTE")
    assert r.status_code == 200 and "Ficha mês a mês" in r.get_data(as_text=True)


@banco
def test_obra_fora_da_prestacao_nao_aparece_na_evolucao(cenario, monkeypatch):
    """16/11/2026, o dono: "obras fora da prestação não é para se exibir"."""
    from app.apps.painel import cenarios
    cenarios.gravar_fora(cenario, ["obra:POUCA GENTE"])
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/evolucao?cenario={cenario}").get_data(as_text=True)
    assert "MUITA GENTE" in html and "POUCA GENTE" not in html


@banco
def test_quem_le_a_prestacao_abre_a_evolucao_e_baixa_o_relatorio(dois_lados, monkeypatch):
    """16/11/2026, o dono: "quem vê só leitura, vê também". Mas os lançamentos
    da matriz (onde está a folha) não se abrem para ele."""
    from app.apps.painel import usuarios
    r = usuarios.criar("socio", "senha-do-socio", telas=["prestacao_leitura"])
    assert r["ok"], r
    monkeypatch.setenv("PAINEL_SENHA", "senha-do-dono")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"usuario": "socio", "senha": "senha-do-socio"})

    r = cliente.get(f"/painel/prestacao/evolucao?cenario={dois_lados}&ver=obra:MUITA GENTE")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert "Ficha mês a mês — MUITA GENTE" in html and "Modo de leitura" in html
    assert "Abrir os lançamentos deste mês" not in html
    assert cliente.get(f"/painel/baixar/evolucao?cenario={dois_lados}").status_code == 200
    assert cliente.get(f"/painel/baixar/evolucao?cenario={dois_lados}&formato=pdf").status_code == 200


@banco
def test_quem_e_preso_a_obra_nao_abre_a_evolucao_nem_baixa(preso, monkeypatch):
    from app.apps.painel import cenarios
    cenario_id = cenarios.criar("Mão de obra", criterio="pessoal", pct_padrao=100)
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get(f"/painel/prestacao/evolucao?cenario={cenario_id}").status_code == 404
    assert cliente.get(f"/painel/baixar/evolucao?cenario={cenario_id}").status_code == 404


@banco
def test_o_relatorio_sai_em_planilha_e_em_pdf(dois_lados, monkeypatch):
    from openpyxl import load_workbook
    cliente = _cliente(monkeypatch)
    r = cliente.get(f"/painel/baixar/evolucao?cenario={dois_lados}")
    assert r.status_code == 200
    livro = load_workbook(io.BytesIO(r.data))
    assert livro.sheetnames[:4] == ["Caminho do dinheiro", "Por estrutura", "Por projeto", "Por obra"]
    assert "Estrutura ALFA" in livro.sheetnames and "Obra mes a mes" in livro.sheetnames
    # os títulos falam BWSFILIAL e o nome do projeto principal, não "demais"
    titulos = [c.value for c in livro["Empresa mes a mes"][1]]
    assert "Estrutura absorvida — ALFA" in titulos and "Estrutura absorvida — BWSFILIAL" in titulos

    for extra in ("", "&obras=todas", "&ver=obra:MUITA GENTE", "&ver=lado:demais"):
        r = cliente.get(f"/painel/baixar/evolucao?cenario={dois_lados}&formato=pdf{extra}")
        assert r.status_code == 200 and r.data[:4] == b"%PDF", extra
    # o recorte de uma obra é só ela
    r = cliente.get(f"/painel/baixar/evolucao?cenario={dois_lados}&ver=obra:MUITA GENTE")
    assert load_workbook(io.BytesIO(r.data)).sheetnames == \
        ["Caminho do dinheiro", "Resumo", "Obra MUITA GENTE"]
