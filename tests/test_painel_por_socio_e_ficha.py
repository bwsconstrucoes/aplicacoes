# -*- coding: utf-8 -*-
"""
O resultado por sócio, a ficha da obra e a conta corrente no DRE (24/11/2026).

Três pedidos do dono no mesmo dia:

- "visualizar o resultado por obra de cada sócio (…) cada sócio bate o olho e já
  vê quanto é que está sendo apropriado para ele em relação àquela obra (…) e
  ao final a apropriação dos custos administrativos e de juros";
- "clicar na obra, abrir um modal (…) as despesas, as receitas (…) quanto das
  despesas de cada obra foram pagas em qual conta corrente (…) até para
  confrontar: o volume que eu vejo na DRE bate com o que eu vejo na prestação";
- "eu queria ver as despesas, os pagamentos por conta corrente", no DRE.

O que se prova: nenhuma das três faz conta nova — cada número delas FECHA com
o que a prestação e o DRE já mostravam. A primeira metade roda sem banco.
"""
from __future__ import annotations

import io

import pytest

from tests.test_painel_cenario import (  # noqa: F401  (fixtures)
    _calcular, _cliente, _cliente_preso, base, cenario, preso)


# ===========================================================================
# 1. Sem banco: as parcelas da quota e a grade
# ===========================================================================
POR_OBRA = {
    "SÓ DA CASA": {"receita_bruta": 1000.0, "resultado_direto": 400.0, "rateio": -100.0,
                   "juros": -20.0, "resultado": 280.0},
    "COM PARCEIRO": {"receita_bruta": 2000.0, "resultado_direto": 600.0, "rateio": -150.0,
                     "juros": -50.0, "resultado": 400.0},
    "SEM NINGUÉM": {"receita_bruta": 0.0, "resultado_direto": -30.0, "rateio": 0.0,
                    "juros": 0.0, "resultado": -30.0},
}
PARTICIPACOES = [
    {"socio": "Ana", "tipo": "Interno", "pct": 60.0, "obra": "SÓ DA CASA"},
    {"socio": "Beto", "tipo": "Interno", "pct": 40.0, "obra": "SÓ DA CASA"},
    {"socio": "Ana", "tipo": "Interno", "pct": 30.0, "obra": "COM PARCEIRO"},
    {"socio": "Beto", "tipo": "Interno", "pct": 30.0, "obra": "COM PARCEIRO"},
    {"socio": "Zeca", "tipo": "Externo", "pct": 40.0, "obra": "COM PARCEIRO"},
]


def _quotas():
    from app.apps.painel import prestacao
    return prestacao.quotas_por_obra(POR_OBRA, PARTICIPACOES, taxa_adm_pct=5.0)


def test_as_quatro_parcelas_somam_a_quota():
    """A quota não mudou; ela só passou a dizer de onde veio. Em obra da casa e
    em obra com parceiro, direto + estrutura + juros + taxa = quota."""
    quotas = _quotas()
    assert len(quotas) == 5
    for q in quotas:
        partes = q["parte_direto"] + q["parte_estrutura"] + q["parte_juros"] + q["parte_taxa"]
        assert partes == pytest.approx(q["quota"], abs=0.01), q["socio"] + " em " + q["obra"]

    ana = next(q for q in quotas if q["socio"] == "Ana" and q["obra"] == "SÓ DA CASA")
    assert (ana["parte_direto"], ana["parte_estrutura"], ana["parte_juros"], ana["parte_taxa"]) == \
        pytest.approx((240.0, -60.0, -12.0, 0.0))
    # o parceiro não carrega a estrutura; paga a taxa (5% de 2.000 = 100; 40% dele)
    zeca = next(q for q in quotas if q["socio"] == "Zeca")
    assert (zeca["parte_direto"], zeca["parte_estrutura"], zeca["parte_juros"], zeca["parte_taxa"]) == \
        pytest.approx((240.0, 0.0, -20.0, -40.0))
    # os dois da casa dividem a estrutura inteira e recebem a taxa de volta
    ana = next(q for q in quotas if q["socio"] == "Ana" and q["obra"] == "COM PARCEIRO")
    assert (ana["parte_estrutura"], ana["parte_taxa"]) == pytest.approx((-75.0, -30.0 + 50.0))
    # a taxa não cria nem some com dinheiro: o que o parceiro paga os outros recebem
    assert sum(q["parte_taxa"] for q in quotas) == pytest.approx(0.0, abs=0.01)


def _grade():
    from app.apps.painel import por_socio
    calculo = {"quotas": _quotas(), "por_obra": POR_OBRA,
               "obras": {"SÓ DA CASA": {"projeto": "CE"}, "COM PARCEIRO": {"projeto": "PE"},
                         "SEM NINGUÉM": {"projeto": "PE"}}}
    return por_socio.montar(calculo, ["Beto", "Ana"])


def test_a_grade_segue_a_ordem_do_cadastro_e_fecha_com_as_quotas():
    g = _grade()
    # a ordem é a do cadastro; quem não está nele vai para o fim
    assert [s["nome"] for s in g["socios"]] == ["Beto", "Ana", "Zeca"]
    assert [(l["projeto"], l["obra"]) for l in g["linhas"]] == \
        [("CE", "SÓ DA CASA"), ("PE", "COM PARCEIRO"), ("PE", "SEM NINGUÉM")]

    casa = g["linhas"][0]
    assert casa["antes"] == 400.0 and casa["final"] == 280.0
    beto, ana, zeca = casa["celulas"]
    assert zeca is None                                   # não participa: "—" na tela
    assert (beto["pct"], beto["antes"], beto["final"]) == pytest.approx((40.0, 160.0, 112.0))
    assert ana["antes"] + beto["antes"] == pytest.approx(casa["antes"])
    assert ana["final"] + beto["final"] == pytest.approx(casa["final"])

    r = g["rodape"]
    quotas = _quotas()
    for i, s in enumerate(g["socios"]):
        do_socio = [q for q in quotas if q["socio"] == s["nome"]]
        # a última linha é a soma das quotas dele — a mesma nas duas visões
        assert r["final"]["por_socio"][i] == pytest.approx(sum(q["quota"] for q in do_socio), abs=0.01)
        assert (r["parte_direto"]["por_socio"][i] + r["parte_estrutura"]["por_socio"][i]
                + r["parte_juros"]["por_socio"][i] + r["parte_taxa"]["por_socio"][i]) == \
            pytest.approx(r["final"]["por_socio"][i], abs=0.01)
    # a estrutura e os juros de todas as obras com quem divide estão nas colunas
    assert r["parte_estrutura"]["total"] == pytest.approx(-250.0)
    assert r["parte_juros"]["total"] == pytest.approx(-70.0)
    assert r["final"]["total"] == pytest.approx(680.0)    # 280 + 400
    assert g["tem_taxa"]


def test_a_grade_diz_qual_obra_nao_tem_quem_divida():
    g = _grade()
    assert g["sem_dono"] == ["SEM NINGUÉM"] and g["nao_fecham"] == []
    orfa = g["linhas"][2]
    assert orfa["sem_dono"] and all(c is None for c in orfa["celulas"])
    # ela conta no total das obras, mas não na coluna de ninguém
    assert g["total_das_obras"]["final"] == pytest.approx(650.0)
    assert g["rodape"]["final"]["total"] == pytest.approx(680.0)


def test_as_abas_da_grade_tem_uma_coluna_por_socio():
    from app.apps.painel import excel, por_socio
    abas = por_socio.abas(_grade())
    assert [n for n, _c, _l in abas] == ["Antes da estrutura e juros", "Resultado final",
                                         "Resumo por socio"]
    _nome, colunas, linhas = abas[0]
    assert [t for _c, t in colunas][3:] == ["Beto — valor", "Ana — valor", "Zeca — valor"]
    # coluna de sócio sai em reais na planilha e no PDF
    assert all(excel._e_dinheiro(t) for _c, t in colunas[2:])
    assert [l["obra"] for l in linhas][-5:] == [
        "SOMA DO RESULTADO DIRETO", "Estrutura apropriada", "Juros apropriados",
        "Taxa de administração das parcerias", "RESULTADO FINAL DE CADA UM"]
    assert linhas[0]["s2"] == ""                          # Zeca não está na obra da casa
    final = abas[1][2]
    assert final[-1]["obra"] == "RESULTADO FINAL DE CADA UM" and len(final) == 4


def test_a_ficha_confere_as_categorias_e_o_dre():
    from app.apps.painel import ficha_obra
    calculo = {
        "por_obra": {"OBRA": {"receita_bruta": 1100.0, "retencoes": 100.0, "receita_liquida": 1000.0,
                              "despesas": -700.0, "resultado_direto": 300.0, "rateio": -80.0,
                              "juros": -20.0, "resultado": 200.0}},
        "obras": {"OBRA": {"projeto": "CE"}},
        "quotas": [{"socio": "Ana", "tipo": "Interno", "obra": "OBRA", "pct": 100.0,
                    "quota": 200.0, "parte_direto": 300.0}],
        "apurado": [
            {"obra": "OBRA", "mes": "2025-02", "receita_liquida": 1000.0, "despesas": -300.0,
             "resultado_direto": 700.0, "rateio": -50.0, "juros": 0.0, "resultado": 650.0},
            {"obra": "OBRA", "mes": "2025-01", "receita_liquida": 0.0, "despesas": -400.0,
             "resultado_direto": -400.0, "rateio": -30.0, "juros": -20.0, "resultado": -450.0},
            {"obra": "OUTRA", "mes": "2025-01", "receita_liquida": 9.0, "despesas": 0.0,
             "resultado_direto": 9.0, "rateio": 0.0, "juros": 0.0, "resultado": 9.0}],
        # o pessoal vem em valor absoluto (é a régua); o caixa, com sinal
        "pessoal": [("2025-01", "OBRA", 250.0), ("2025-02", "OBRA", 200.0), ("2025-01", "OUTRA", 77.0)],
        "caixa": [("2025-01", "OBRA", -400.0), ("2025-02", "OBRA", 700.0), ("2025-03", "OBRA", 40.0),
                  ("2025-01", "OUTRA", 5.0)],
    }
    detalhe = {
        "despesas": [{"grupo": "Pessoal", "categoria": "Salários", "valor": -450.0},
                     {"grupo": "Pessoal", "categoria": "Encargos", "valor": -50.0},
                     {"grupo": "Materiais", "categoria": "Concreto", "valor": -200.0}],
        "receitas": [{"categoria": "Medição", "retido": False, "valor": 1000.0},
                     {"categoria": "ISS Retido", "retido": True, "valor": 100.0}],
        "contas": [{"conta": "Banco A", "pago": -500.0, "recebido": 1000.0, "pagamentos": 4, "recebimentos": 1},
                   {"conta": "Banco B", "pago": -200.0, "recebido": 0.0, "pagamentos": 2, "recebimentos": 0}],
    }
    f = ficha_obra.montar(calculo, "OBRA", detalhe,
                          {"receita": 1000.0, "despesa": -700.0, "resultado": 300.0})
    assert f["despesas_fecham"] and f["bate_com_o_dre"]
    # o maior grupo de despesa primeiro, com as categorias dentro
    assert [g["grupo"] for g in f["despesas"]] == ["Pessoal", "Materiais"]
    assert f["despesas"][0]["valor"] == pytest.approx(-500.0)
    assert [r["categoria"] for r in f["retidas"]] == ["ISS Retido"]
    # mês a mês na ordem do tempo, só desta obra, com o acumulado
    assert [(m["rotulo"], m["acumulado"]) for m in f["meses"]] == \
        [("01/2025", -450.0), ("02/2025", 200.0), ("03/2025", 200.0)]
    # o pessoal do mês (parte da despesa) e o caixa — pedido de 24/11/2026.
    # O caixa acumulado desconta a estrutura: -400-30, +700-50, +40
    assert [(m["pessoal"], m["caixa"], m["caixa_acumulado"]) for m in f["meses"]] == \
        [(-250.0, -400.0, -430.0), (-200.0, 700.0, 220.0), (0.0, 40.0, 260.0)]
    assert (f["total_pessoal"], f["total_caixa"], f["caixa_final"]) == (-450.0, 340.0, 260.0)
    assert [round(c["pct"], 1) for c in f["contas"]] == [71.4, 28.6]
    assert (f["total_pago"], f["total_recebido"]) == (-700.0, 1000.0)

    # se a soma das categorias não der a despesa, ou o DRE divergir, a ficha DIZ
    detalhe["despesas"].pop()
    ruim = ficha_obra.montar(calculo, "OBRA", detalhe,
                             {"receita": 1000.0, "despesa": -760.0, "resultado": 240.0}, "2025-02")
    assert not ruim["despesas_fecham"] and not ruim["bate_com_o_dre"]
    assert [c["bate"] for c in ruim["conferencia"]] == [True, False, False]
    assert ruim["conferencia"][1]["diferenca"] == pytest.approx(60.0)
    assert ruim["corte_rotulo"] == "02/2025"
    # obra que não está na prestação não tem ficha
    assert ficha_obra.montar(calculo, "FANTASMA", detalhe, {}) is None

    abas = ficha_obra.abas(f)
    assert [n for n, _c, _l in abas] == ["Resumo", "Despesas por categoria", "Mes a mes",
                                         "Por conta corrente", "Conferencia com o DRE"]
    for nome, colunas, linhas in abas:
        for linha in linhas:
            assert all(chave in linha for chave, _t in colunas), nome
    mes_a_mes = dict((n, (c, l)) for n, c, l in abas)["Mes a mes"]
    assert [c for c, _t in mes_a_mes[0]][3] == "pessoal" and \
        [c for c, _t in mes_a_mes[0]][-2:] == ["caixa", "caixa_acumulado"]
    assert mes_a_mes[1][-1]["rotulo"] == "TOTAL" and mes_a_mes[1][-1]["caixa_acumulado"] == 260.0

    # com o analítico, o relatório ganha as duas listas — e o total das
    # receitas não conta o imposto que o cliente reteve
    lancamentos = [
        {"data": None, "tipo": "Receita", "quem": "CLIENTE", "grupo": "Receita", "categoria": "Medição",
         "documento": "NF 1", "conta": "Banco A", "situacao": "RECEBIDO", "observacao": "", "codigo": 1, "valor": 1000.0},
        {"data": None, "tipo": "Retido na fonte", "quem": "CLIENTE", "grupo": "Receita", "categoria": "ISS Retido",
         "documento": "NF 1", "conta": "", "situacao": "RECEBIDO", "observacao": "", "codigo": 2, "valor": 100.0},
        {"data": None, "tipo": "Despesa", "quem": "FORNECEDOR", "grupo": "Pessoal", "categoria": "Salários",
         "documento": "", "conta": "Banco A", "situacao": "PAGO", "observacao": "", "codigo": 3, "valor": -700.0},
    ]
    completo = ficha_obra.abas(f, lancamentos)
    assert [n for n, _c, _l in completo][-2:] == ["Receitas - analitico", "Despesas - analitico"]
    receitas, despesas = completo[-2][2], completo[-1][2]
    assert [l["tipo"] for l in receitas] == ["Receita", "Retido na fonte", "TOTAL (sem o retido)"]
    assert receitas[-1]["valor"] == 1000.0 and despesas[-1]["valor"] == -700.0
    for nome, colunas, linhas in completo:
        for linha in linhas:
            assert all(chave in linha for chave, _t in colunas), nome


# ===========================================================================
# 2. Com banco: as telas abrem, fecham com a prestação e respeitam quem olha
# ===========================================================================
banco = pytest.mark.banco


@pytest.fixture()
def com_socios(cenario):
    """Dois sócios em tudo, e as contas correntes no que foi pago."""
    from app.apps.painel import cenarios, consultas, prestacao_dados
    from app.apps.painel import db as painel_db
    prestacao_dados.salvar_socio("Sócio A")
    prestacao_dados.salvar_socio("Sócio B")
    socios = {s["nome"]: s["id"] for s in prestacao_dados.socios()}
    cenarios.salvar_participacao(cenario, socios["Sócio A"], 60)
    cenarios.salvar_participacao(cenario, socios["Sócio B"], 40)
    with painel_db.conexao() as conn:
        conn.execute("UPDATE fato SET conta_corrente = CASE WHEN codigo_lancamento % 2 = 0"
                     " THEN 'Banco Par' ELSE 'Banco Ímpar' END")
        # um título EM ABERTO da obra: não saiu de conta nenhuma
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
            " VALUES (899,'2. Contas a Pagar','DRE','A VENCER','A vencer','Cimento',"
            "         'Materiais','MUITA GENTE','ALFA','FORNECEDOR X','2025-04-10',2025,"
            "         0,-250,0,0)")
        conn.commit()
    consultas.esquecer_listas()
    return cenario


@banco
def test_o_detalhe_da_obra_fecha_com_a_apuracao(com_socios):
    """A soma das categorias da ficha é a despesa que a prestação mostra — nas
    duas medidas. É a mesma régua, agrupada de outro jeito."""
    from app.apps.painel import consultas
    for medida, esperado in (("comprometido", -2250.0), ("executado", -2000.0)):
        apurado = sum(l["despesas"] for l in consultas.apuracao_por_obra_mes(medida)
                      if l["obra"] == "MUITA GENTE")
        detalhe = consultas.detalhe_da_obra("MUITA GENTE", medida)
        assert apurado == pytest.approx(esperado)
        assert sum(d["valor"] for d in detalhe["despesas"]) == pytest.approx(apurado)
        assert sum(r["valor"] for r in detalhe["receitas"]) == pytest.approx(1000.0)
        # a conta corrente é sempre do que foi pago: o título em aberto não entra
        assert sum(c["pago"] for c in detalhe["contas"]) == pytest.approx(-2000.0)
        assert sum(c["recebido"] for c in detalhe["contas"]) == pytest.approx(1000.0)
    # o pagamento 801 é ímpar; o recebimento 803 também
    assert {c["conta"] for c in detalhe["contas"]} == {"Banco Ímpar"}
    # com o corte do cenário antes do movimento, a obra fica vazia
    assert consultas.detalhe_da_obra("MUITA GENTE", "executado", "2025-02")["despesas"] == []

    # o analítico é a mesma régua, lançamento a lançamento: fecha com a ficha
    for medida, despesa in (("comprometido", -2250.0), ("executado", -2000.0)):
        linhas = consultas.lancamentos_da_obra("MUITA GENTE", medida)
        assert sum(l["valor"] for l in linhas if l["tipo"] == "Despesa") == pytest.approx(despesa)
        assert sum(l["valor"] for l in linhas if l["tipo"] == "Receita") == pytest.approx(1000.0)
    assert [(l["tipo"], l["categoria"], l["codigo"]) for l in linhas] == \
        [("Despesa", "Salários", 801), ("Receita", "Medição", 803)]
    assert consultas.lancamentos_da_obra("MUITA GENTE", "executado", "2025-02") == []


@banco
def test_a_tela_por_socio_abre_e_fecha_com_as_quotas(com_socios, monkeypatch):
    conta = _calcular(com_socios)
    cliente = _cliente(monkeypatch)
    r = cliente.get(f"/painel/prestacao/por-socio?cenario={com_socios}")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    for trecho in ("Sócio A", "Sócio B", "MUITA GENTE", "POUCA GENTE", "Soma do resultado direto",
                   "(−) Estrutura apropriada", "(−) Juros apropriados", "= Resultado final de cada um",
                   'data-ficha-da-obra="MUITA GENTE"', "/painel/prestacao/obra"):
        assert trecho in html, trecho
    # sem parceiro de fora, a linha da taxa não aparece
    assert "Taxa de administração das parcerias" not in html
    # a visão que abre é a de antes da estrutura e dos juros (decisão do dono)
    assert "A parte de cada um no resultado direto das obras" in html
    html = cliente.get(f"/painel/prestacao/por-socio?cenario={com_socios}&visao=final").get_data(as_text=True)
    assert "A parte de cada um no resultado final das obras" in html
    assert "Soma do resultado direto" not in html

    from app.apps.painel import por_socio
    g = por_socio.montar(conta)
    assert g["rodape"]["final"]["total"] == pytest.approx(
        sum(n["resultado"] for n in conta["por_obra"].values()), abs=0.05)
    # o menu da prestação ganhou a porta
    assert "/painel/prestacao/por-socio" in cliente.get(
        f"/painel/prestacao/resultado?cenario={com_socios}").get_data(as_text=True)


@banco
def test_a_ficha_da_obra_abre_com_as_quatro_partes(com_socios, monkeypatch):
    cliente = _cliente(monkeypatch)
    r = cliente.get(f"/painel/prestacao/obra?cenario={com_socios}&obra=MUITA GENTE")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    # é um pedaço de página, não a página inteira
    assert "<html" not in html and "Ficha da obra" in html
    for trecho in ("Despesas com Pessoal", "Salários", "Medição", "Resultado direto da obra",
                   "Estrutura apropriada", "Sócio A · 60%", "03/2025", "Banco Ímpar",
                   "Conferir com o DRE", "Na prestação", "No DRE",
                   # o mês a mês mostra o pessoal e o caixa (24/11/2026)
                   "— da qual, pessoal", "Caixa do mês", "Caixa acumulado"):
        assert trecho in html, trecho
    # 2.000 de pessoal em março; recebeu 1.000 e pagou 2.000: caixa de -1.000
    from app.apps.painel import cenarios
    from app.apps.painel.web import _ficha_da_obra
    marco = _ficha_da_obra(cenarios.completo(com_socios), "MUITA GENTE")["meses"][0]
    assert (marco["rotulo"], marco["pessoal"], marco["caixa"]) == ("03/2025", -2000.0, -1000.0)
    assert marco["caixa_acumulado"] == pytest.approx(-1000.0 + marco["estrutura"])
    # o cenário é pelo executado: o título em aberto fica de fora, e a ficha bate com o DRE
    assert "Cimento" not in html and "<b>Bate.</b>" in html
    # obra que não existe, ou sem dizer a obra: "não encontrado"
    assert cliente.get(f"/painel/prestacao/obra?cenario={com_socios}&obra=FANTASMA").status_code == 404
    assert cliente.get(f"/painel/prestacao/obra?cenario={com_socios}").status_code == 404
    # o Resultado e a Evolução abrem a mesma ficha pelo nome da obra
    for tela in ("resultado", "evolucao?visao=obra"):
        sep = "&" if "?" in tela else "?"
        html = cliente.get(f"/painel/prestacao/{tela}{sep}cenario={com_socios}").get_data(as_text=True)
        assert 'data-ficha-da-obra="MUITA GENTE"' in html and 'id="ficha-obra-veu"' in html, tela


@banco
def test_a_ficha_no_comprometido_avisa_que_a_conta_e_so_do_pago(com_socios, monkeypatch):
    from app.apps.painel import cenarios
    comprometido = cenarios.criar("Comprometido", criterio="pessoal", pct_padrao=100,
                                  medida="comprometido")
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/obra?cenario={comprometido}&obra=MUITA GENTE").get_data(as_text=True)
    assert "Cimento" in html and "pelo comprometido" in html
    assert "o total daqui é menor que a despesa do resumo" in html


@banco
def test_quem_le_a_prestacao_ve_as_telas_novas_e_quem_e_preso_nao(com_socios, preso, monkeypatch):
    from app.apps.painel import usuarios
    r = usuarios.criar("socio", "senha-do-socio", telas=["prestacao_leitura"])
    assert r["ok"], r
    monkeypatch.setenv("PAINEL_SENHA", "senha-do-dono")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    leitor = app.test_client()
    leitor.post("/painel/entrar", data={"usuario": "socio", "senha": "senha-do-socio"})
    caminhos = (f"/painel/prestacao/por-socio?cenario={com_socios}",
                f"/painel/prestacao/obra?cenario={com_socios}&obra=MUITA GENTE",
                f"/painel/baixar/por_socio?cenario={com_socios}",
                f"/painel/baixar/obra?cenario={com_socios}&obra=MUITA GENTE&formato=pdf")
    for caminho in caminhos:
        assert leitor.get(caminho).status_code == 200, caminho
    # quem lê não abre os lançamentos por trás da ficha
    html = leitor.get(caminhos[1]).get_data(as_text=True)
    assert "/painel/analitico" not in html and "Salários" in html

    cliente = _cliente_preso(monkeypatch)
    for caminho in caminhos:
        assert cliente.get(caminho).status_code == 404, caminho


@banco
def test_os_relatorios_saem_em_planilha_e_em_pdf(com_socios, monkeypatch):
    from openpyxl import load_workbook
    cliente = _cliente(monkeypatch)
    r = cliente.get(f"/painel/baixar/por_socio?cenario={com_socios}")
    assert r.status_code == 200
    livro = load_workbook(io.BytesIO(r.data))
    assert livro.sheetnames == ["Antes da estrutura e juros", "Resultado final", "Resumo por socio"]
    assert [c.value for c in livro["Resultado final"][1]][3:] == ["Sócio A — valor", "Sócio B — valor"]

    r = cliente.get(f"/painel/baixar/obra?cenario={com_socios}&obra=MUITA GENTE")
    livro = load_workbook(io.BytesIO(r.data))
    assert livro.sheetnames == ["Resumo", "Despesas por categoria", "Mes a mes",
                                "Por conta corrente", "Conferencia com o DRE",
                                "Receitas - analitico", "Despesas - analitico"]
    titulos = [c.value for c in livro["Mes a mes"][1]]
    assert "Despesa com pessoal (dentro da despesa)" in titulos and titulos[-2:] == ["Caixa do mês", "Caixa acumulado"]
    despesas = [[c.value for c in linha] for linha in livro["Despesas - analitico"].iter_rows(min_row=2)]
    assert [(l[1], l[4], l[-1]) for l in despesas] == [("Despesa", "Salários", -2000), ("TOTAL", None, -2000)]
    assert [l[0].value for l in livro["Por conta corrente"].iter_rows(min_row=2)] == ["Banco Ímpar", "TOTAL"]
    for caminho in (f"/painel/baixar/por_socio?cenario={com_socios}&formato=pdf",
                    f"/painel/baixar/obra?cenario={com_socios}&obra=MUITA GENTE&formato=pdf",
                    # obra que não está na prestação: o arquivo sai dizendo isso, sem erro
                    f"/painel/baixar/obra?cenario={com_socios}&obra=FANTASMA&formato=pdf"):
        r = cliente.get(caminho)
        assert r.status_code == 200 and r.data[:4] == b"%PDF", caminho


@banco
def test_o_dre_agrupa_os_pagamentos_por_conta_corrente(com_socios, monkeypatch):
    """O total por conta é o mesmo de "Pagas (executado)" por grupo: o mesmo
    dinheiro, agrupado de outro jeito. Título em aberto não entra."""
    from app.apps.painel import consultas
    f = consultas.Filtros()
    por_conta = consultas.despesas_por(f, quebra="conta", visao="comprometido")
    por_grupo = consultas.despesas_por(f, quebra="grupo", visao="executado")
    assert sum(l["valor"] for l in por_conta) == pytest.approx(sum(l["valor"] for l in por_grupo))
    assert {l["nome"] for l in por_conta} == {"Banco Par", "Banco Ímpar"}
    # ímpares: 801 (2.000), 805 (900), 807 (600); pares: 802 (1.000), 806 (300)
    valores = {l["nome"]: (l["valor"], l["pagamentos"]) for l in por_conta}
    assert valores["Banco Ímpar"] == (pytest.approx(-3500.0), 3)
    assert valores["Banco Par"] == (pytest.approx(-1300.0), 2)
    assert sum(l["pct_total"] for l in por_conta) == pytest.approx(100.0)

    html = _cliente(monkeypatch).get("/painel/dre?quebra=conta&visao=aberto").get_data(as_text=True)
    assert "Pagamentos por Conta corrente" in html and "Banco Ímpar" in html and "Total pago" in html
    assert "Só entra o que já foi pago" in html
    # as outras duas quebras continuam como eram
    html = _cliente(monkeypatch).get("/painel/dre?quebra=categoria").get_data(as_text=True)
    assert "Despesas por Categoria" in html and "Total pago" not in html
