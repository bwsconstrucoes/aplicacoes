# -*- coding: utf-8 -*-
"""
O cenário da prestação de contas — tudo num ambiente só.

Pedido do dono em 22/09/2026: uma tela onde ele nomeia sócios e parceiros,
escolhe a régua (mão de obra ou faturamento), diz quais contas da matriz se
dividem e em quanto, guarda isso com nome e compara com outro jeito de dividir
— com o resultado por obra, a quota de cada um e a conta aberta para conferir.

Estes testes rodam com banco de verdade porque o que se prova aqui vive no
WHERE e no GROUP BY: quais lançamentos entram no bolo, e em que proporção.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.banco

SENHA_MESTRE = "senha-do-dono"
MATRIZ = "BWS Construções"


def _por_pagar(conn, cod, obra, valor, *, grupo="Custo", categoria="Serviços",
               data="2025-03-10"):
    conn.execute(
        "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
        " situacao_vencimento, categoria, grupo, departamento, projeto,"
        " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
        " VALUES (?,'2. Contas a Pagar','DRE','PAGO','Quitado',?,?,?,'ALFA',"
        "         'FORNECEDOR X',?,2025,?,0,0,0)",
        (cod, categoria, grupo, obra, data, valor))


def _por_receber(conn, cod, obra, valor, *, data="2025-03-10"):
    conn.execute(
        "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
        " situacao_vencimento, categoria, grupo, departamento, projeto,"
        " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
        " VALUES (?,'1. Contas a Receber','DRE','RECEBIDO','Quitado',"
        "         'Medição','Receita',?,'ALFA','CLIENTE',?,2025,?,0,0,0)",
        (cod, obra, data, valor))


@pytest.fixture()
def base():
    """Duas obras e uma matriz, com números escolhidos para a conta ser óbvia.

    MUITA GENTE tem o dobro do pessoal de POUCA GENTE — então, pela régua da
    mão de obra, ela carrega o dobro da estrutura. E MUITA GENTE é a única que
    fica com o caixa negativo, então é ela que paga o juro."""
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import consultas
    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), resultado

    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM cenario")
        conn.execute("DELETE FROM socios")
        conn.execute("DELETE FROM usuarios")
        conn.execute("DELETE FROM config WHERE chave IN ('fora_da_analise', 'ordem_dos_socios')")
        # pessoal: 2.000 numa, 1.000 na outra
        _por_pagar(conn, 801, "MUITA GENTE", -2000, grupo="Despesas com Pessoal",
                   categoria="Salários")
        _por_pagar(conn, 802, "POUCA GENTE", -1000, grupo="Despesas com Pessoal",
                   categoria="Salários")
        # receita: o inverso, para faturamento e mão de obra darem resultados
        # DIFERENTES — senão o teste da régua não provaria nada
        _por_receber(conn, 803, "MUITA GENTE", 1000)
        _por_receber(conn, 804, "POUCA GENTE", 5000)
        # a matriz: 900 de aluguel, 300 de patrimônio e 600 de juros
        _por_pagar(conn, 805, MATRIZ, -900, grupo="Administrativas",
                   categoria="Aluguel")
        _por_pagar(conn, 806, MATRIZ, -300, grupo="Patrimônio",
                   categoria="Equipamento")
        _por_pagar(conn, 807, MATRIZ, -600, grupo="Financeiras",
                   categoria="Juros sobre Empréstimos")
        conn.commit()
    consultas.esquecer_listas()
    yield
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM cenario")
        conn.execute("DELETE FROM socios")
        conn.execute("DELETE FROM usuarios")
        conn.execute("DELETE FROM config WHERE chave IN ('fora_da_analise', 'ordem_dos_socios')")
        conn.commit()


@pytest.fixture()
def cenario(base):
    from app.apps.painel import cenarios
    return cenarios.criar("Mão de obra", criterio="pessoal", pct_padrao=100)


def _calcular(cenario_id):
    from app.apps.painel import cenarios
    from app.apps.painel.web import _calcular_cenario
    return _calcular_cenario(cenarios.completo(cenario_id))


def _cliente(monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", SENHA_MESTRE)
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"usuario": "", "senha": SENHA_MESTRE})
    return cliente


@pytest.fixture()
def preso(base):
    from app.apps.painel import usuarios
    r = usuarios.criar("preso", "senha-dele", obras=["MUITA GENTE"],
                       telas=["dre", "prestacao"])
    assert r["ok"], r
    return r["id"]


def _cliente_preso(monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", SENHA_MESTRE)
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"usuario": "preso", "senha": "senha-dele"})
    return cliente


# ===========================================================================
# 1. A régua: quem tem mais gente carrega mais estrutura
# ===========================================================================
def test_a_mao_de_obra_divide_na_proporcao_do_pessoal(cenario):
    """900 de aluguel + 300 de patrimônio = 1.200 a repartir. Com 2.000 e 1.000
    de pessoal, dois terços vão para quem tem mais gente."""
    conta = _calcular(cenario)
    por_obra = conta["por_obra"]
    assert por_obra["MUITA GENTE"]["rateio"] == pytest.approx(-800, abs=0.02)
    assert por_obra["POUCA GENTE"]["rateio"] == pytest.approx(-400, abs=0.02)


def test_trocar_para_faturamento_muda_o_resultado(cenario):
    """É a pergunta que o dono quer poder fazer: "e se fosse faturamento?".

    Pela receita (1.000 contra 5.000), a conta VIRA: quem carregava dois terços
    passa a carregar um sexto."""
    from app.apps.painel import cenarios
    cenarios.atualizar(cenario, criterio="faturamento")
    por_obra = _calcular(cenario)["por_obra"]
    assert por_obra["MUITA GENTE"]["rateio"] == pytest.approx(-200, abs=0.02)
    assert por_obra["POUCA GENTE"]["rateio"] == pytest.approx(-1000, abs=0.02)


# ===========================================================================
# 2. Os percentuais, em hierarquia
# ===========================================================================
def test_grupo_com_zero_sai_do_bolo(cenario):
    """"Grupo de patrimônio, eu vou botar 0% para dividir" — o dono, com todas
    as letras. Sobram 900 a repartir, e não 1.200."""
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "grupo", "Patrimônio", 0)
    conta = _calcular(cenario)
    rateado = sum(conta["rateio"]["alocacoes"].values())
    assert rateado == pytest.approx(-900, abs=0.02)


def test_meio_a_meio_num_grupo(cenario):
    """"Grupo de despesa com o pessoal, eu vou botar 50%." Metade de 900 mais os
    300 inteiros do patrimônio."""
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "grupo", "Administrativas", 50)
    rateado = sum(_calcular(cenario)["rateio"]["alocacoes"].values())
    assert rateado == pytest.approx(-750, abs=0.02)


def test_a_categoria_ganha_do_grupo(cenario):
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "grupo", "Administrativas", 0)
    cenarios.marcar_peso(cenario, "categoria", "Aluguel", 100)
    rateado = sum(_calcular(cenario)["rateio"]["alocacoes"].values())
    assert rateado == pytest.approx(-1200, abs=0.02)


def test_o_lancamento_ganha_da_categoria(cenario):
    """O caso que o dono descreveu: "tem uns lançamentos aqui que eu quero fazer
    divisão" — dentro de um grupo que, no geral, não divide."""
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "grupo", "Patrimônio", 0)
    cenarios.marcar_peso(cenario, "lancamento", "806", 100)
    rateado = sum(_calcular(cenario)["rateio"]["alocacoes"].values())
    assert rateado == pytest.approx(-1200, abs=0.02)


def test_o_lancamento_marcado_nao_conta_duas_vezes(cenario):
    """A armadilha da hierarquia: se o lançamento entrasse com percentual
    próprio SEM sair do balde da categoria, o dinheiro dobraria."""
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "lancamento", "805", 100)
    rateado = sum(_calcular(cenario)["rateio"]["alocacoes"].values())
    assert rateado == pytest.approx(-1200, abs=0.02)


def test_apagar_a_marcacao_volta_a_herdar(cenario):
    """Vazio e zero são coisas diferentes: zero é "não divide", vazio é "segue
    o nível de cima"."""
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "grupo", "Patrimônio", 0)
    cenarios.marcar_peso(cenario, "grupo", "Patrimônio", "")
    rateado = sum(_calcular(cenario)["rateio"]["alocacoes"].values())
    assert rateado == pytest.approx(-1200, abs=0.02)


def test_padrao_zero_so_divide_o_que_foi_marcado(cenario):
    from app.apps.painel import cenarios
    cenarios.atualizar(cenario, pct_padrao=0)
    cenarios.marcar_peso(cenario, "grupo", "Administrativas", 100)
    rateado = sum(_calcular(cenario)["rateio"]["alocacoes"].values())
    assert rateado == pytest.approx(-900, abs=0.02)


# ===========================================================================
# 3. Os juros seguem a OUTRA régua
# ===========================================================================
def _aprofundar_o_buraco():
    """MUITA GENTE gasta mais 5.000: o buraco dela passa a ser maior do que a
    sobra da POUCA GENTE, e o lado inteiro fica devendo."""
    from app.apps.painel import consultas
    from app.apps.painel import db as painel_db
    with painel_db.conexao() as conn:
        _por_pagar(conn, 899, "MUITA GENTE", -5000)
        conn.commit()
    consultas.esquecer_listas()


def test_o_juro_nao_entra_no_bolo_da_estrutura(cenario):
    """Se entrasse, seria repartido pelo pessoal — e cobrado de obra que se
    paga sozinha. Os 600 de juros ficam fora dos 1.200."""
    _aprofundar_o_buraco()
    conta = _calcular(cenario)
    assert sum(conta["rateio"]["alocacoes"].values()) == pytest.approx(-1200, abs=0.02)
    assert sum(conta["juros"]["alocacoes"].values()) == pytest.approx(-600, abs=0.02)


def test_o_juro_cai_em_quem_ficou_negativo(cenario):
    """Com o lado devendo, o juro cai em quem está no vermelho (MUITA GENTE) e
    não em quem está no azul (POUCA GENTE)."""
    _aprofundar_o_buraco()
    conta = _calcular(cenario)
    por_obra = conta["por_obra"]
    assert por_obra["MUITA GENTE"]["juros"] == pytest.approx(-600, abs=0.02)
    assert por_obra["POUCA GENTE"]["juros"] == pytest.approx(0, abs=0.02)
    assert conta["batimento"]["soma_fecha"]


def test_lado_que_se_cobre_sozinho_nao_paga_juro(cenario):
    """10/11/2026, o dono: "o caixa gerado pelas obras serve para cobrir o
    buraco de obra que ainda não recebeu". MUITA GENTE está no vermelho, mas a
    sobra da POUCA GENTE (mesmo lado) cobre: ninguém paga, e os 600 ficam
    visíveis como custo sem dono — o batimento continua somando."""
    conta = _calcular(cenario)
    assert conta["juros"]["alocacoes"] == {}
    assert sum(s["valor"] for s in conta["juros"]["sobras"]) == pytest.approx(-600, abs=0.02)
    assert conta["batimento"]["soma_fecha"]


def test_aporte_abate_o_buraco_da_obra(cenario):
    """10/11/2026, o dono: "aporte abate buraco sim"."""
    from app.apps.painel import consultas
    from app.apps.painel import db as painel_db
    _aprofundar_o_buraco()
    with painel_db.conexao() as conn:
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa,"
            " tipo_aporte)"
            " VALUES (898,'1. Contas a Receber','Fluxo de Caixa','RECEBIDO','Quitado',"
            "         'Aportes Parceiros','Aportes','MUITA GENTE','ALFA','SOCIO Z',"
            "         '2025-03-05',2025,9000,0,0,0,'Aporte de Parceiro')")
        conn.commit()
    consultas.esquecer_listas()
    assert consultas.aporte_mensal_por_obra() == [("2025-03", "MUITA GENTE", 9000.0)]
    conta = _calcular(cenario)
    assert conta["juros"]["alocacoes"] == {}            # o aporte tapou o buraco


def test_desligar_a_regua_do_deficit_devolve_o_juro_ao_bolo(cenario):
    from app.apps.painel import cenarios
    cenarios.atualizar(cenario, juros_por_deficit=0)
    conta = _calcular(cenario)
    assert sum(conta["rateio"]["alocacoes"].values()) == pytest.approx(-1800, abs=0.02)
    assert conta["juros"]["alocacoes"] == {}


# ===========================================================================
# 4. Quem divide — e a soma tem de fechar
# ===========================================================================
def test_dois_socios_dividem_o_resultado_de_todas_as_obras(cenario):
    from app.apps.painel import cenarios, prestacao_dados
    prestacao_dados.salvar_socio("Sócio A")
    prestacao_dados.salvar_socio("Sócio B")
    socios = {s["nome"]: s["id"] for s in prestacao_dados.socios()}
    cenarios.salvar_participacao(cenario, socios["Sócio A"], 60)
    cenarios.salvar_participacao(cenario, socios["Sócio B"], 40)

    conta = _calcular(cenario)
    resultado = sum(n["resultado"] for n in conta["por_obra"].values())
    assert sum(q["quota"] for q in conta["quotas"]) == pytest.approx(resultado, abs=0.05)


def test_o_parceiro_de_uma_obra_substitui_a_lista_geral(cenario):
    """"Quem for nomeado numa obra específica substitui a lista geral naquela
    obra" — é assim que entra o parceiro de uma obra só, sem refazer o resto."""
    from app.apps.painel import cenarios, prestacao_dados
    prestacao_dados.salvar_socio("Sócio A")
    prestacao_dados.salvar_socio("Parceiro", tipo="Externo")
    socios = {s["nome"]: s["id"] for s in prestacao_dados.socios()}
    cenarios.salvar_participacao(cenario, socios["Sócio A"], 100)
    cenarios.salvar_participacao(cenario, socios["Sócio A"], 50, obra="POUCA GENTE")
    cenarios.salvar_participacao(cenario, socios["Parceiro"], 50, obra="POUCA GENTE")

    conta = _calcular(cenario)
    de_pouca = [q for q in conta["quotas"] if q["obra"] == "POUCA GENTE"]
    assert sorted(q["socio"] for q in de_pouca) == ["Parceiro", "Sócio A"]
    de_muita = [q for q in conta["quotas"] if q["obra"] == "MUITA GENTE"]
    assert [q["socio"] for q in de_muita] == ["Sócio A"]


def test_na_obra_com_parceiro_a_soma_continua_fechando(cenario):
    """Com taxa de administração, crédito ao sócio interno e juros na base de
    todos, a soma das quotas ainda tem de bater com o resultado da obra."""
    from app.apps.painel import cenarios, prestacao_dados
    cenarios.atualizar(cenario, taxa_adm_pct=5)
    prestacao_dados.salvar_socio("Sócio A")
    prestacao_dados.salvar_socio("Parceiro", tipo="Externo")
    socios = {s["nome"]: s["id"] for s in prestacao_dados.socios()}
    cenarios.salvar_participacao(cenario, socios["Sócio A"], 50, obra="POUCA GENTE")
    cenarios.salvar_participacao(cenario, socios["Parceiro"], 50, obra="POUCA GENTE")

    conta = _calcular(cenario)
    quotas = sum(q["quota"] for q in conta["quotas"] if q["obra"] == "POUCA GENTE")
    assert quotas == pytest.approx(conta["por_obra"]["POUCA GENTE"]["resultado"],
                                   abs=0.05)


# ===========================================================================
# 5. Guardar, duplicar, e as telas
# ===========================================================================
def test_duplicar_leva_tudo_junto(cenario):
    """É o que torna barato perguntar "e se fosse faturamento?": parte-se do
    pronto e troca-se uma coisa só."""
    from app.apps.painel import cenarios, prestacao_dados
    cenarios.marcar_peso(cenario, "grupo", "Patrimônio", 0)
    prestacao_dados.salvar_socio("Sócio A")
    socios = {s["nome"]: s["id"] for s in prestacao_dados.socios()}
    cenarios.salvar_participacao(cenario, socios["Sócio A"], 100)

    copia = cenarios.duplicar(cenario, "Igual, mas por faturamento")
    cenarios.atualizar(copia, criterio="faturamento")

    novo = cenarios.completo(copia)
    assert novo["pesos"]["grupo"]["Patrimônio"] == 0
    assert len(novo["participacoes"]) == 1
    # e o original não foi tocado
    assert cenarios.buscar(cenario)["criterio"] == "pessoal"


def test_as_duas_telas_abrem(cenario, monkeypatch):
    cliente = _cliente(monkeypatch)
    montagem = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}")
    assert montagem.status_code == 200
    assert "Mão de obra" in montagem.get_data(as_text=True)

    resultado = cliente.get(f"/painel/prestacao/resultado?cenario={cenario}")
    assert resultado.status_code == 200
    html = resultado.get_data(as_text=True)
    assert "MUITA GENTE" in html


def test_marcar_o_percentual_pela_tela(cenario, monkeypatch):
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "peso", "cenario_id": str(cenario), "nivel": "grupo",
        "chave": "Patrimônio", "pct": "0"})
    from app.apps.painel import cenarios
    assert cenarios.pesos(cenario)["grupo"]["Patrimônio"] == 0


def test_marcar_varios_lancamentos_de_uma_vez(cenario, monkeypatch):
    """O dono foi explícito: "não adianta uma coisa que eu tenho que digitar
    coisa por coisa"."""
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "peso_em_lote", "cenario_id": str(cenario),
        "nivel": "lancamento", "marcado": ["805", "806"], "pct": "0"})
    from app.apps.painel import cenarios
    marcados = cenarios.pesos(cenario)["lancamento"]
    assert marcados == {"805": 0, "806": 0}


def test_a_arvore_inteira_se_salva_num_clique(cenario, monkeypatch):
    """05/11/2026, o dono: "da forma que tá, um pouquinho complicado de lançar
    um a um". Grupo com 50%, uma categoria dele com 80%, o resto em branco —
    um envio só. E só grava o que mudou: apagar o campo desmarca."""
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "categoria", "Equipamento", 10)
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "pesos_da_arvore", "cenario_id": str(cenario),
        "item_nivel": ["grupo", "categoria", "categoria", "grupo"],
        "item_chave": ["Administrativas", "Aluguel", "Equipamento", "Patrimônio"],
        "item_pct": ["50", "80", "", ""]})
    pesos = cenarios.pesos(cenario)
    assert pesos["grupo"] == {"Administrativas": 50}
    assert pesos["categoria"] == {"Aluguel": 80}          # Equipamento desmarcada
    # a tela: um formulário só, com o botão de salvar tudo e o grupo que abre ali
    html = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert 'id="pesos-da-arvore"' in html and "Salvar percentuais" in html
    assert "data-abre-grupo" in html and 'data-herda-do-grupo' in html


def test_o_percentual_muda_com_o_tempo():
    """09/11/2026, o dono: "de janeiro a dezembro de 2021, 50-50; em 2022,
    60-40". Cada período vale a partir do mês dele até o próximo; antes do
    primeiro, vale o percentual fixo — ou, sem ele, o nível de cima."""
    from app.apps.painel import prestacao
    pesos = {"grupo": {"Pessoal": 30}, "categoria": {}, "lancamento": {},
             "periodos": {"grupo": {"Pessoal": [("2021-01", 50), ("2022-01", 60)]},
                          "categoria": {"Salários": [("2023-06", 0)]}}}
    pc = lambda cat, mes: prestacao.peso_da_conta(pesos, "Pessoal", cat, "", 100, mes=mes)  # noqa: E731
    assert pc("Férias", "2020-12") == 30           # antes de tudo: o fixo do grupo
    assert pc("Férias", "2021-07") == 50
    assert pc("Férias", "2022-01") == 60 and pc("Férias", "2026-11") == 60
    assert pc("Salários", "2023-05") == 60         # ainda segue o grupo
    assert pc("Salários", "2023-06") == 0          # o período dela vence
    # sem mês (a tela) e sem data: o fixo
    assert prestacao.peso_da_conta(pesos, "Pessoal", "", "", 100) == 30
    assert pc("Férias", prestacao.SEM_DATA) == 30


def test_o_rateio_usa_o_percentual_de_cada_mes():
    from app.apps.painel import prestacao
    pesos = {"grupo": {}, "categoria": {}, "lancamento": {},
             "periodos": {"grupo": {"Pessoal": [("2021-01", 50), ("2022-01", 60)]}}}
    admin = [{"mes": "2021-06", "grupo": "Pessoal", "categoria": "Salários", "valor": -1000.0},
             {"mes": "2022-06", "grupo": "Pessoal", "categoria": "Salários", "valor": -1000.0}]
    driver = [("2021-06", "OBRA", 10.0), ("2022-06", "OBRA", 10.0)]
    r = prestacao.calcular_rateio_do_cenario(admin, [], driver, {"OBRA": {}}, pesos)
    assert round(r["alocacoes"][("OBRA", "2021-06")], 2) == -500.0
    assert round(r["alocacoes"][("OBRA", "2022-06")], 2) == -600.0


def test_os_periodos_se_salvam_pela_arvore(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "pesos_da_arvore", "cenario_id": str(cenario),
        "item_nivel": ["grupo"], "item_chave": ["Administrativas"], "item_pct": [""],
        "periodo_presente": ["grupo|Administrativas", "categoria|Aluguel"],
        "periodo_nivel": ["grupo", "grupo", "grupo", "categoria"],
        "periodo_chave": ["Administrativas", "Administrativas", "Administrativas", "Aluguel"],
        "periodo_desde": ["2021-01", "2022-01", "", "2024-03"],
        "periodo_pct": ["50", "60", "", ""]})
    per = cenarios.periodos(cenario)
    assert per["grupo"] == {"Administrativas": [("2021-01", 50.0, None), ("2022-01", 60.0, None)]}
    assert per["categoria"] == {}                    # sem percentual: não grava
    # duplicar leva os períodos junto
    copia = cenarios.duplicar(cenario, "cópia")
    assert cenarios.periodos(copia)["grupo"]["Administrativas"][1] == ("2022-01", 60.0, None)
    html = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "50% desde 01/2021 · 60% desde 01/2022" in html and "varia" in html
    # apagar o percentual apaga o período
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "pesos_da_arvore", "cenario_id": str(cenario),
        "periodo_presente": ["grupo|Administrativas"],
        "periodo_nivel": ["grupo", "grupo"], "periodo_chave": ["Administrativas"] * 2,
        "periodo_desde": ["2021-01", "2022-01"], "periodo_pct": ["50", ""]})
    assert cenarios.periodos(cenario)["grupo"] == {"Administrativas": [("2021-01", 50.0, None)]}
    cenarios.apagar(copia)


def test_os_dois_lados_e_quem_recebe():
    """09/11/2026, o dono: "num período, 40% ficou para a BWS CE, os 60% para
    os demais projetos que recebem. Nada fica sem dono. Alguns projetos não
    entram, outros entram até determinada data." """
    from app.apps.painel import cenarios, prestacao
    pesos = {"grupo": {}, "categoria": {}, "lancamento": {},
             "periodos": {"grupo": {"Pessoal": [("2021-01", 100, 40)]}}}
    admin = [{"mes": m, "grupo": "Pessoal", "categoria": "Salários", "valor": -1000.0}
             for m in ("2021-06", "2022-06")]
    driver = [(m, o, v) for m in ("2021-06", "2022-06")
              for o, v in (("CE1", 10.0), ("CE2", 30.0), ("SUL", 10.0), ("NORTE", 10.0), ("FORA", 50.0))]
    lado_de = {"CE1": "principal", "CE2": "principal", "SUL": "demais",
               "NORTE": "demais", "FORA": "demais"}
    regras = {"P-NORTE": ("periodo", "", "2021-12"), "P-FORA": ("nao", "", "")}
    projeto = {"CE1": "BWS CE", "CE2": "BWS CE", "SUL": "P-SUL", "NORTE": "P-NORTE", "FORA": "P-FORA"}
    recebe = lambda o, m: cenarios.recebe_no_mes(regras.get(projeto[o]), m)  # noqa: E731
    obras = {o: {} for o in lado_de}
    r = prestacao.calcular_rateio_do_cenario(admin, [], driver, obras, pesos,
                                             lado_de=lado_de, recebe=recebe)
    a = {k: round(v, 2) for k, v in r["alocacoes"].items()}
    # 2021: 400 para a CE (pela régua 10/30), 600 para SUL e NORTE (meio a meio)
    assert a[("CE1", "2021-06")] == -100.0 and a[("CE2", "2021-06")] == -300.0
    assert a[("SUL", "2021-06")] == -300.0 and a[("NORTE", "2021-06")] == -300.0
    assert ("FORA", "2021-06") not in a                       # não recebe
    # 2022: o NORTE já saiu; os 600 ficam todos com o SUL
    assert a[("SUL", "2022-06")] == -600.0 and ("NORTE", "2022-06") not in a
    assert round(sum(a.values()), 2) == -2000.0 and not r["sobras"]   # nada sem dono
    # um lado sem ninguém recebendo passa a parte para o outro
    so_ce = lambda o, m: lado_de[o] == "principal"  # noqa: E731
    r = prestacao.calcular_rateio_do_cenario(admin[:1], [], driver, obras, pesos,
                                             lado_de=lado_de, recebe=so_ce)
    assert round(sum(r["alocacoes"].values()), 2) == -1000.0


def test_quem_recebe_se_salva_pela_tela(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "projetos", "cenario_id": str(cenario), "principal": "ALFA",
        "proj_nome": ["ALFA", "BETA", "GAMA", "DELTA"],
        "proj_papel": ["", "nao", "periodo", "periodo"],
        "proj_desde": ["", "", "", ""], "proj_ate": ["", "", "2023-12", ""]})
    quem = cenarios.projetos(cenario)
    assert quem["principal"] == "ALFA"
    assert quem["regras"] == {"BETA": ("nao", "", ""), "GAMA": ("periodo", "", "2023-12")}
    html = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "Quem recebe o custo da matriz" in html and "Salvar quem recebe" in html


def test_quem_esta_preso_a_uma_obra_nao_alcanca_o_cenario(preso, monkeypatch):
    """As duas telas mostram a empresa inteira e configuram a conta de todos.
    Não passam pelo filtro de escopo — então não são de quem está preso.

    Repare que ele TEM a tela "prestacao" liberada: mesmo assim não entra. É de
    propósito — a prestação antiga é um relatório, o cenário configura a conta
    de todo mundo."""
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get("/painel/prestacao/montagem").status_code == 404
    assert cliente.get("/painel/prestacao/resultado").status_code == 404


def test_quem_esta_preso_nao_abre_nem_grava_os_parametros(preso, monkeypatch):
    """Achado em 05/10/2026, ao pôr o projeto das obras em Parâmetros: a tela e
    o simulador de regras estavam fora da lista do administrador, e quem tinha
    uma obra gravava sócios, participações e regras da empresa inteira — e,
    com o projeto das obras, poderia mudar quem enxerga cada obra."""
    from app.apps.painel import prestacao_dados
    antes = prestacao_dados.config().get("projeto_da_obra")
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get("/painel/prestacao/parametros?aba=projetos").status_code == 404
    assert cliente.get("/painel/prestacao/cenarios").status_code == 404
    resposta = cliente.post("/painel/prestacao/parametros", data={
        "acao": "projeto_da_obra", "aba": "projetos",
        "codigo": "1", "projeto": "DELE"})
    assert resposta.status_code == 404
    assert prestacao_dados.config().get("projeto_da_obra") == antes


# ===========================================================================
# 6. O download passava por fora da proteção das telas
# ===========================================================================
# Achado em 22/09/2026 ao acrescentar o arquivo do cenário: a rota de download
# não conferia assunto nenhum. A maioria dos arquivos se monta a partir do
# filtro da pessoa e estava certa por acidente — mas quotas, posição, rateio da
# administração e o cenário leem a empresa INTEIRA. Quem tinha uma obra podia
# baixar a divisão de lucro entre os sócios.

@pytest.mark.parametrize("assunto", ["cenario", "quotas", "posicao",
                                     "rateio_admin", "completo", "explorador"])
def test_quem_esta_preso_nao_baixa_a_empresa_inteira(preso, monkeypatch, assunto):
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get(f"/painel/baixar/{assunto}").status_code == 404


def test_quem_esta_preso_continua_baixando_a_tela_dele(preso, monkeypatch):
    """O conserto não pode ter fechado o que o dono liberou de propósito."""
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get("/painel/baixar/dre").status_code == 200


def test_assunto_de_tela_que_ele_nao_tem_e_recusado(preso, monkeypatch):
    """Ele tem DRE, não tem o Extrato — e o arquivo segue a tela."""
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get("/painel/baixar/extrato").status_code == 404


def test_o_dono_baixa_o_cenario_inteiro(cenario, monkeypatch):
    cliente = _cliente(monkeypatch)
    r = cliente.get(f"/painel/baixar/cenario?cenario={cenario}")
    assert r.status_code == 200
    assert len(r.data) > 2000, "planilha vazia"


# ===========================================================================
# 7. Fora da análise — 22/09/2026
# ===========================================================================
# O dono: "preciso poder remover projetos ou obras da análise".

def test_obra_fora_da_analise_nao_recebe_estrutura_nem_juros(cenario):
    from app.apps.painel import cenarios
    cenarios.excluir(cenario, "obra:MUITA GENTE")
    conta = _calcular(cenario)
    assert "MUITA GENTE" not in conta["por_obra"]
    assert conta["excluidas"] == ["MUITA GENTE"]
    # toda a estrutura (1.200) vai para quem ficou
    assert conta["por_obra"]["POUCA GENTE"]["rateio"] == pytest.approx(-1200, abs=0.02)
    assert all(obra != "MUITA GENTE" for obra, _m in conta["juros"]["alocacoes"])


def test_projeto_fora_da_analise_tira_todas_as_obras_dele(cenario):
    """As duas obras do teste estão no projeto ALFA: tirar o projeto esvazia
    a análise — e a tela avisa em vez de quebrar."""
    from app.apps.painel import cenarios
    cenarios.excluir(cenario, "projeto:ALFA")
    conta = _calcular(cenario)
    assert conta["por_obra"] == {}
    assert sorted(conta["excluidas"]) == ["MUITA GENTE", "POUCA GENTE"]


def test_projeto_que_nao_recebe_some_da_prestacao(cenario):
    """09/11/2026, o dono: "os projetos que não receberam nenhum percentual
    não precisam aparecer, porque realmente não interessa"."""
    from app.apps.painel import cenarios
    cenarios.gravar_projetos(cenario, "", {"ALFA": ("nao", "", "")})
    conta = _calcular(cenario)
    assert conta["por_obra"] == {}
    assert sorted(conta["excluidas"]) == ["MUITA GENTE", "POUCA GENTE"]
    # recebe só num período: continua na prestação
    cenarios.gravar_projetos(cenario, "", {"ALFA": ("periodo", "", "2020-12")})
    assert _calcular(cenario)["por_obra"]
    cenarios.gravar_projetos(cenario, "", {})


def test_as_categorias_dos_juros_se_marcam_numa_lista(cenario, monkeypatch):
    """09/11/2026, o dono: "quanto à categoria, coloca dentro dos parâmetros
    para eu definir quais seriam". Lista para marcar, gravada no formato que o
    cálculo já lia."""
    from app.apps.painel import prestacao_dados
    cliente = _cliente(monkeypatch)
    html = cliente.get("/painel/prestacao/parametros?aba=geral").get_data(as_text=True)
    assert 'name="categoria_juros_item" value="Juros sobre Empréstimos" checked' in html
    cliente.post("/painel/prestacao/parametros", data={
        "acao": "config", "aba": "geral", "categoria_juros_lista": "1",
        "categoria_juros_item": ["Juros sobre Empréstimos", "IOF", "IOF"]})
    assert prestacao_dados.config()["categoria_juros"] == "Juros sobre Empréstimos; IOF"


def test_tirar_e_voltar_pela_tela(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "excluir", "cenario_id": str(cenario), "item": ["obra:MUITA GENTE"]})
    assert cenarios.excluidas(cenario) == ["obra:MUITA GENTE"]
    html = cliente.get(f"/painel/prestacao/resultado?cenario={cenario}").get_data(as_text=True)
    # desde 11/11/2026 o nome fica recolhido ("ver quais"): a tela é mostrada
    # aos sócios, e obra fora da prestação não aparece à primeira vista
    assert "1 obra(s) fora desta prestação" in html and "MUITA GENTE" in html
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "reincluir", "cenario_id": str(cenario), "item": "obra:MUITA GENTE"})
    assert cenarios.excluidas(cenario) == []


def test_duplicar_leva_as_excluidas(cenario):
    from app.apps.painel import cenarios
    cenarios.excluir(cenario, "obra:MUITA GENTE")
    copia = cenarios.duplicar(cenario, "Cópia")
    assert cenarios.excluidas(copia) == ["obra:MUITA GENTE"]


# ---- a lista de Parâmetros, que vale para tudo — 22/09/2026 ----
# O dono: "na parte de configurações da prestação de conta, pra eu poder
# eliminar projetos e/ou obras dessa análise".

def test_o_que_sai_em_parametros_sai_de_todo_cenario(cenario):
    from app.apps.painel import cenarios, prestacao_dados
    prestacao_dados.tirar_da_analise(["obra:MUITA GENTE"])
    assert cenarios.excluidas(cenario) == []          # o cenário não tem lista própria
    conta = _calcular(cenario)
    assert conta["excluidas"] == ["MUITA GENTE"]
    assert conta["excluidas_gerais"] == ["obra:MUITA GENTE"]
    assert "MUITA GENTE" not in conta["por_obra"]
    assert conta["por_obra"]["POUCA GENTE"]["rateio"] == pytest.approx(-1200, abs=0.02)
    # a lista do cenário SOMA-SE à geral
    cenarios.excluir(cenario, "obra:POUCA GENTE")
    assert _calcular(cenario)["por_obra"] == {}
    # e voltar em Parâmetros devolve a obra
    prestacao_dados.voltar_para_analise("obra:MUITA GENTE")
    assert prestacao_dados.fora_da_analise() == []
    assert "MUITA GENTE" in _calcular(cenario)["por_obra"]


def test_tirar_e_voltar_pela_aba_de_parametros(cenario, monkeypatch):
    from app.apps.painel import prestacao_dados
    cliente = _cliente(monkeypatch)
    html = cliente.get("/painel/prestacao/parametros?aba=fora").get_data(as_text=True)
    # desde 11/11/2026 a aba é uma lista só (considerar / fora / só até)
    assert "Fora da análise" in html and "lista-fora" in html
    resposta = cliente.post("/painel/prestacao/parametros", data={
        "acao": "tirar_da_analise", "aba": "fora",
        "item": ["obra:MUITA GENTE", "projeto:ALFA"]})
    assert resposta.status_code == 302
    assert prestacao_dados.fora_da_analise() == ["obra:MUITA GENTE", "projeto:ALFA"]
    html = cliente.get("/painel/prestacao/parametros?aba=fora").get_data(as_text=True)
    assert "Projeto — ALFA" in html
    cliente.post("/painel/prestacao/parametros", data={
        "acao": "voltar_para_analise", "aba": "fora", "item": "projeto:ALFA"})
    assert prestacao_dados.fora_da_analise() == ["obra:MUITA GENTE"]
    # a lista nova grava tudo de uma vez, com o mês de quem é "só até"
    cliente.post("/painel/prestacao/parametros", data={
        "acao": "fora_em_lista", "aba": "fora",
        "fora_chave": ["obra:MUITA GENTE", "projeto:ALFA"],
        "fora_modo": ["total", "ate"], "fora_ate": ["", "2025-06"]})
    assert prestacao_dados.fora_da_analise() == ["obra:MUITA GENTE",
                                                 "projeto:ALFA@2025-06"]
    html = cliente.get("/painel/prestacao/montagem").get_data(as_text=True)
    # o que foi tirado em Cadastros aparece no quadro do cenário, travado
    assert "só até 06/2025" in html and "em todos os cenários" in html
    cliente.post("/painel/prestacao/parametros", data={
        "acao": "fora_em_lista", "aba": "fora",
        "fora_chave": ["obra:MUITA GENTE"], "fora_modo": ["total"], "fora_ate": [""]})
    # a montagem do cenário avisa o que já está fora por Parâmetros
    html = cliente.get("/painel/prestacao/montagem").get_data(as_text=True)
    assert "em todos os cenários" in html and "MUITA GENTE" in html


# ===========================================================================
# 7b. A árvore diz quanto ENTRA e quanto FICA DE FORA — 23/09/2026
# ===========================================================================
# O dono: "só informar um grupo fica complicado para quem quiser analisar
# depois (...) preciso ir adentrando até o lançamento e estipular o que entra
# e o que não entra".

def test_a_arvore_da_matriz_mostra_o_que_entra_com_as_excecoes(cenario):
    """Aluguel 900 e Patrimônio 300 se dividem; os juros (600) seguem a régua
    própria. Grupo Patrimônio marcado com 0% e o aluguel (805) com 50%:
    entra 450, fica de fora 750, e a lista de marcações tem as duas."""
    from app.apps.painel import cenarios, prestacao_dados
    from app.apps.painel.web import _contas_da_matriz
    cenarios.marcar_peso(cenario, "grupo", "Patrimônio", 0)
    cenarios.marcar_peso(cenario, "lancamento", "805", 50)
    arvore = _contas_da_matriz(prestacao_dados.config(), cenarios.completo(cenario),
                               "comprometido")
    por_grupo = {g["grupo"]: g for g in arvore["contas"]}
    assert por_grupo["Administrativas"]["entra"] == pytest.approx(-450.0)
    assert por_grupo["Administrativas"]["fora"] == pytest.approx(-450.0)
    assert por_grupo["Administrativas"]["excecoes"] == 1
    assert por_grupo["Patrimônio"]["entra"] == pytest.approx(0.0)
    assert por_grupo["Patrimônio"]["fora"] == pytest.approx(-300.0)
    assert por_grupo["Financeiras"]["juros"] == pytest.approx(-600.0)
    assert por_grupo["Financeiras"]["entra"] == 0.0
    resumo = arvore["resumo"]
    assert resumo["total"] == pytest.approx(-1800.0)
    assert resumo["entra"] == pytest.approx(-450.0)
    assert resumo["fora"] == pytest.approx(-750.0)
    assert resumo["juros"] == pytest.approx(-600.0)
    # e o que entra na árvore é o que a conta divide de fato
    conta = _calcular(cenario)
    assert sum(conta["rateio"]["alocacoes"].values()) == pytest.approx(-450.0)
    niveis = [(m["nivel"], m["chave"], m["pct"]) for m in arvore["marcacoes"]]
    assert ("grupo", "Patrimônio", 0.0) in niveis
    assert ("lancamento", "805", 50.0) in niveis
    lanc = next(m for m in arvore["marcacoes"] if m["nivel"] == "lancamento")
    assert "FORNECEDOR X" in lanc["rotulo"]


def test_a_montagem_mostra_entra_fora_e_o_que_foi_marcado(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cenarios.marcar_peso(cenario, "grupo", "Patrimônio", 0)
    cenarios.marcar_peso(cenario, "lancamento", "805", 50)
    cliente = _cliente(monkeypatch)
    html = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}"
                       "&grupo=Administrativas&categoria=Aluguel").get_data(as_text=True)
    assert "Entra no rateio" in html and "−R$ 450,00" in html
    assert "Fica de fora" in html and "−R$ 750,00" in html
    assert "O que você marcou" in html and "2 marcação(ões)" in html
    assert "1 exceção(ões)" in html
    assert "Não entra (0%)" in html and "Entra inteiro (100%)" in html
    assert "próprio" in html                       # o 805 tem marcação própria
    # numa categoria sem marcação, cada linha diz o que herda da categoria
    html = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}"
                       "&grupo=Patrimônio&categoria=Equipamento").get_data(as_text=True)
    assert "herdado" in html and "herda a categoria: <b>0%</b>" in html


# ===========================================================================
# 8. Onde estão os juros — 22/09/2026
# ===========================================================================
def test_a_conferencia_lista_toda_categoria_que_fala_em_juro(base, monkeypatch):
    """"Na controladoria tem 1,6 milhão, no painel só vejo 191 mil." A
    conferência mostra CADA categoria com cara de juro, em que análise está e
    quanto foi pago — e marca a que a prestação conta."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        # uma parcela de empréstimo no Fluxo de Caixa: o painel não a vê como despesa
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
            " VALUES (901,'2. Contas a Pagar','Fluxo de Caixa','PAGO','Quitado',"
            "         'Empréstimos - Amortização','Financeiras',?,'ALFA','BANCO',"
            "         '2025-03-10',2025,-50000,0,0,0)", (MATRIZ,))
        conn.commit()
    conf = consultas.conferencia_dos_juros({"juros sobre empréstimos"})
    por_nome = {l["categoria"]: l for l in conf["linhas"]}
    assert por_nome["Juros sobre Empréstimos"]["configurada"] is True
    assert por_nome["Juros sobre Empréstimos"]["analise"] == "DRE"
    assert por_nome["Empréstimos - Amortização"]["configurada"] is False
    assert por_nome["Empréstimos - Amortização"]["analise"] == "Fluxo de Caixa"
    assert conf["total_configurado_dre"] == pytest.approx(600.0)

    # 11/11/2026, o dono leu o quadro e respondeu "não entendi": o número do
    # topo somava a categoria na empresa inteira. Com os departamentos da
    # estrutura, o quadro separa o que a prestação distribui (lançado na
    # matriz/filial) do que já é despesa da própria obra.
    with conexao() as conn:
        _por_pagar(conn, 902, "MUITA GENTE", -250, grupo="Financeiras",
                   categoria="Juros sobre Empréstimos")
        conn.commit()
    conf = consultas.conferencia_dos_juros({"juros sobre empréstimos"}, [MATRIZ])
    assert conf["distribui"] == pytest.approx(600.0)
    assert conf["ja_nas_obras"] == pytest.approx(250.0)
    assert conf["total_contado"] == pytest.approx(850.0)

    html = _cliente(monkeypatch).get("/painel/configuracoes?conferir=1").get_data(as_text=True)
    assert "Onde estão os juros de empréstimo" in html
    assert "É este que a prestação distribui" in html
    assert "Não é juro de empréstimo" not in html or "por atraso" in html


# ---------------------------------------------------------------------------
# 10/11/2026 — a revisão da conta: dois consertos, o corte dos juros e o batimento
# ---------------------------------------------------------------------------
_OBRAS_REV = {"CE1": {"projeto": "BWSCE"}, "PE1": {"projeto": "PE"}, "PE2": {"projeto": "PE"}}
_LADO_REV = {"CE1": "principal", "PE1": "demais", "PE2": "demais"}
_REGUA_REV = [("2022-01", "CE1", 100), ("2022-01", "PE1", 50), ("2022-01", "PE2", 50)]


def _por_obra_rev(rateio):
    total = {}
    for (obra, _mes), valor in rateio["alocacoes"].items():
        total[obra] = round(total.get(obra, 0.0) + valor, 2)
    return total


def test_lancamento_marcado_nao_e_descontado_uma_vez_por_departamento():
    """Matriz e filial com a mesma categoria no mesmo mês: o lançamento
    marcado saía das DUAS linhas, e 400 sumiam da conta."""
    from app.apps.painel import prestacao
    admin = [{"mes": "2022-01", "depto": "M", "grupo": "G", "categoria": "A", "valor": -1000.0},
             {"mes": "2022-01", "depto": "F", "grupo": "G", "categoria": "A", "valor": -300.0}]
    marcado = [{"mes": "2022-01", "grupo": "G", "categoria": "A", "codigo": "7", "valor": -400.0}]
    r = prestacao.calcular_rateio_do_cenario(admin, marcado, _REGUA_REV, _OBRAS_REV,
                                             {"lancamento": {"7": 100.0}}, lado_de=_LADO_REV)
    assert round(sum(r["alocacoes"].values()), 2) == -1300.0
    r = prestacao.calcular_rateio_do_cenario(admin, marcado, _REGUA_REV, _OBRAS_REV,
                                             {"lancamento": {"7": 0.0}}, lado_de=_LADO_REV)
    assert round(sum(r["alocacoes"].values()), 2) == -900.0
    assert round(sum(r["fora_por_mes"].values()), 2) == -400.0


def test_categoria_afinada_continua_com_os_dois_lados_do_grupo():
    """Grupo "40% para o principal"; a categoria com percentual próprio (fixo,
    ou por período sem a parte do principal) segue o 40/60 do grupo."""
    from app.apps.painel import prestacao
    admin = [{"mes": "2022-01", "grupo": "G", "categoria": c, "valor": -1000.0} for c in "AB"]
    grupo = {"G": [("2021-01", 100.0, 40.0)]}
    esperado = {"CE1": -800.0, "PE1": -600.0, "PE2": -600.0}
    for pesos in ({"categoria": {"B": 100.0}, "periodos": {"grupo": grupo}},
                  {"periodos": {"grupo": grupo, "categoria": {"B": [("2021-06", 100.0, None)]}}}):
        r = prestacao.calcular_rateio_do_cenario(admin, [], _REGUA_REV, _OBRAS_REV, pesos,
                                                 lado_de=_LADO_REV)
        assert _por_obra_rev(r) == esperado
    # e a categoria que diz a própria parte fica com a dela
    pesos = {"periodos": {"grupo": grupo, "categoria": {"B": [("2021-06", 100.0, 100.0)]}}}
    r = prestacao.calcular_rateio_do_cenario(admin, [], _REGUA_REV, _OBRAS_REV, pesos,
                                             lado_de=_LADO_REV)
    assert _por_obra_rev(r)["CE1"] == -1400.0


def test_sem_projeto_principal_nada_some():
    from app.apps.painel import prestacao
    admin = [{"mes": "2022-01", "grupo": "G", "categoria": "A", "valor": -1000.0}]
    pesos = {"periodos": {"grupo": {"G": [("2021-01", 100.0, 40.0)]}}}
    r = prestacao.calcular_rateio_do_cenario(admin, [], _REGUA_REV, _OBRAS_REV, pesos)
    assert round(sum(r["alocacoes"].values()), 2) == -1000.0


def test_juros_param_no_ponto_de_corte_da_obra():
    """O dono: "terminou de gastar com mão de obra, terminou de receber juros"."""
    from app.apps.painel import prestacao
    caixa = [("2021-01", "CE1", -1000.0), ("2023-06", "PE1", -10.0), ("2021-03", "PE2", -5.0)]
    pessoal = [("2021-01", "CE1", 500.0), ("2021-06", "CE1", 500.0), ("2025-12", "PE1", 1.0)]
    corte = prestacao.corte_dos_juros(pessoal, caixa, _OBRAS_REV)
    # PE2 nunca teve pessoal: vale o último mês em que mexeu no caixa
    assert corte == {"CE1": "2021-06", "PE1": "2025-12", "PE2": "2021-03"}
    juros = {"2021-02": -100.0, "2021-06": -100.0, "2021-07": -100.0, "2023-06": -100.0}
    r = prestacao.alocar_juros_por_deficit(caixa, juros, _OBRAS_REV, corte=corte)
    a = {k: round(v, 2) for k, v in r["alocacoes"].items()}
    assert a[("CE1", "2021-02")] == -100.0 and a[("CE1", "2021-06")] == -100.0
    assert ("CE1", "2021-07") not in a and ("CE1", "2023-06") not in a
    assert a[("PE1", "2023-06")] == -100.0          # só quem ainda está em obra
    assert [s["mes"] for s in r["sobras"]] == ["2021-07"]     # e o resto fica visível


def test_batimento_fecha_e_acusa_o_que_ficou_de_fora():
    """O dono: "o próprio sistema fazer esse somatório e identificar que bate
    com o valor global"."""
    from app.apps.painel import prestacao
    admin = [{"mes": "2022-01", "grupo": "G", "categoria": "A", "valor": -1000.0},
             {"mes": "2022-01", "grupo": "G", "categoria": "Juros", "valor": -200.0},
             {"mes": "2022-02", "grupo": "G", "categoria": "A", "valor": -500.0}]
    sem_juros, juros_mes = prestacao.separar_juros(admin, "Juros")

    def rodar(pesos):
        rateio = prestacao.calcular_rateio_do_cenario(sem_juros, [], _REGUA_REV, _OBRAS_REV,
                                                      pesos, lado_de=_LADO_REV)
        juros = prestacao.alocar_juros_por_deficit([("2022-01", "PE1", -50.0)], juros_mes,
                                                   _OBRAS_REV)
        return prestacao.batimento(admin, rateio, juros, _LADO_REV)

    b = rodar({"periodos": {"grupo": {"G": [("2021-01", 100.0, 40.0)]}}})
    # fevereiro não tem régua: os 500 viram "sem dono", e a soma continua batendo
    assert b["fecha"] and b["total"]["diferenca"] == 0.0
    assert b["total"] == {"matriz": -1700.0, "principal": -400.0, "demais": -600.0,
                          "juros": -200.0, "sem_dono": -500.0, "fora": 0.0, "diferenca": 0.0}
    # com a conta entrando 60%, a soma bate mas o batimento NÃO fecha: critica
    b = rodar({"grupo": {"G": 60.0}})
    assert b["soma_fecha"] and not b["tudo_distribuido"] and not b["fecha"]
    assert b["total"]["fora"] == -600.0


def test_critica_de_participacoes_que_nao_somam_cem():
    from app.apps.painel import prestacao
    gente = [{"obra": "", "pct": 60}, {"obra": "", "pct": 40},
             {"obra": "X", "pct": 50}, {"obra": "X", "pct": 30}]
    assert prestacao.criticas_das_participacoes(gente) == [{"obra": "X", "soma": 80.0}]


def test_o_caixa_e_de_cada_lado_e_o_juro_segue_o_buraco_liquido():
    """10/11/2026, o dono: "o caixa gerado em BWSCE dá suporte a obras que
    ainda não receberam de um lado. E do outro lado, todos os demais projetos
    se ajudam"."""
    from app.apps.painel import prestacao
    obras = {o: {} for o in ("CE1", "CE2", "PE1", "PE2", "PE3")}
    lado = {"CE1": "principal", "CE2": "principal",
            "PE1": "demais", "PE2": "demais", "PE3": "demais"}
    # principal: CE1 sobra 1000 e cobre o buraco de 400 da CE2 — não falta nada
    # demais: PE1 sobra 100; PE2 deve 300 e PE3 deve 100 — faltam 300 no lado
    caixa = [("2022-01", "CE1", 1000.0), ("2022-01", "CE2", -400.0),
             ("2022-01", "PE1", 100.0), ("2022-01", "PE2", -300.0),
             ("2022-01", "PE3", -100.0)]
    r = prestacao.alocar_juros_por_deficit(caixa, {"2022-01": -80.0}, obras, lado_de=lado)
    a = {o: round(v, 2) for (o, _m), v in r["alocacoes"].items()}
    assert a == {"PE2": -60.0, "PE3": -20.0}            # a CE2 não paga: o lado a cobre
    assert r["memoria"][0]["falta_principal"] == 0.0
    assert r["memoria"][0]["falta_demais"] == 300.0
    # os dois lados com falta: o juro se reparte pela falta líquida de cada um
    caixa.append(("2022-02", "CE1", -900.0))            # principal passa a faltar 300
    r = prestacao.alocar_juros_por_deficit(caixa, {"2022-02": -120.0}, obras, lado_de=lado)
    a = {o: round(v, 2) for (o, _m), v in r["alocacoes"].items()}
    assert a == {"CE2": -60.0, "PE2": -45.0, "PE3": -15.0}
    # aporte entra como caixa da obra: tapou o buraco, acabou o juro dela
    caixa.append(("2022-02", "PE2", 300.0))
    r = prestacao.alocar_juros_por_deficit(caixa, {"2022-02": -120.0}, obras, lado_de=lado)
    a = {o: round(v, 2) for (o, _m), v in r["alocacoes"].items()}
    assert a == {"CE2": -120.0}                         # os demais agora se cobrem
    # lado que se cobre sozinho, dos dois lados: ninguém paga, e fica visível
    r = prestacao.alocar_juros_por_deficit(caixa[:3], {"2022-01": -80.0}, obras, lado_de=lado)
    assert not r["alocacoes"] and len(r["sobras"]) == 1


def test_dividendo_pago_diminui_o_caixa_e_sem_projeto_nao_fica_em_lado_nenhum(cenario):
    """10/11/2026, o dono: "o dividendo pago diminui caixa" e "não apropriado
    não fica em canto nenhum (…) obra sem projeto não é para ter"."""
    from app.apps.painel import consultas
    from app.apps.painel import db as painel_db
    with painel_db.conexao() as conn:
        # POUCA GENTE distribui 4.000: a sobra dela deixa de cobrir a outra
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa,"
            " tipo_aporte)"
            " VALUES (897,'2. Contas a Pagar','Fluxo de Caixa','PAGO','Quitado',"
            "         'Dividendos','Dividendos','POUCA GENTE','ALFA','SOCIO Z',"
            "         '2025-03-06',2025,-4000,0,0,0,'Dividendos')")
        # e uma obra sem projeto, com gente e com buraco
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
            " VALUES (896,'2. Contas a Pagar','DRE','PAGO','Quitado','Salários',"
            "         'Despesas com Pessoal','SOLTA','','X','2025-03-10',2025,"
            "         -3000,0,0,0)")
        conn.commit()
    consultas.esquecer_listas()
    assert ("2025-03", "POUCA GENTE", -4000.0) in consultas.aporte_mensal_por_obra()
    conta = _calcular(cenario)
    por_obra = conta["por_obra"]
    # com o dividendo, a POUCA GENTE também ficou no vermelho (400) e o lado
    # passou a dever: os 600 se repartem pelos buracos, 1.800 e 400
    assert por_obra["MUITA GENTE"]["juros"] == pytest.approx(-490.91, abs=0.02)
    assert por_obra["POUCA GENTE"]["juros"] == pytest.approx(-109.09, abs=0.02)
    # a obra sem projeto aparece, mas não leva estrutura nem juro
    assert por_obra["SOLTA"]["rateio"] == 0 and por_obra["SOLTA"]["juros"] == 0
    assert sum(conta["rateio"]["alocacoes"].values()) == pytest.approx(-1200, abs=0.02)
    assert conta["batimento"]["soma_fecha"]


# ===========================================================================
# 11/11/2026 — "as participações e os sócios não podem ser editadas" /
# "tá muito difícil e lento de configurar as participações"
# ===========================================================================
def _dois_socios():
    from app.apps.painel import prestacao_dados
    prestacao_dados.salvar_socio("Sócio A")
    prestacao_dados.salvar_socio("Parceiro", "Externo")
    return {s["nome"]: s["id"] for s in prestacao_dados.socios()}


def test_a_grade_do_cenario_grava_tudo_de_uma_vez(cenario):
    from app.apps.painel import cenarios
    s = _dois_socios()
    n = cenarios.gravar_participacoes_em_grade(cenario, {
        ("", s["Sócio A"]): "100", ("", s["Parceiro"]): "",
        ("POUCA GENTE", s["Sócio A"]): "50", ("POUCA GENTE", s["Parceiro"]): "50,0"})
    assert n == 3
    gravadas = {(p["obra"], p["socio"]): p["pct"] for p in cenarios.participacoes(cenario)}
    assert gravadas == {("", "Sócio A"): 100, ("POUCA GENTE", "Sócio A"): 50,
                        ("POUCA GENTE", "Parceiro"): 50}
    # esvaziar a célula tira a participação
    cenarios.gravar_participacoes_em_grade(cenario, {("POUCA GENTE", s["Parceiro"]): ""})
    assert ("POUCA GENTE", "Parceiro") not in {
        (p["obra"], p["socio"]) for p in cenarios.participacoes(cenario)}


def test_um_percentual_errado_nao_grava_a_grade_pela_metade(cenario):
    from app.apps.painel import cenarios
    s = _dois_socios()
    with pytest.raises(ValueError, match="POUCA GENTE"):
        cenarios.gravar_participacoes_em_grade(cenario, {
            ("", s["Sócio A"]): "100", ("POUCA GENTE", s["Parceiro"]): "cinquenta"})
    assert cenarios.participacoes(cenario) == []


def test_a_grade_de_parametros_grava_e_apaga(base):
    from app.apps.painel import prestacao_dados
    s = _dois_socios()
    prestacao_dados.gravar_participacoes_em_grade({
        ("ALFA", s["Sócio A"]): "60", ("ALFA", s["Parceiro"]): "40"})
    prestacao_dados.gravar_participacoes_em_grade({
        ("ALFA", s["Sócio A"]): "70", ("ALFA", s["Parceiro"]): ""})
    assert {(p["projeto"], p["socio"]): p["pct"]
            for p in prestacao_dados.participacoes()} == {("ALFA", "Sócio A"): 70}
    with pytest.raises(ValueError):
        prestacao_dados.gravar_participacoes_em_grade({("ALFA", s["Sócio A"]): "150"})


def test_socio_e_renomeado_e_reativado(base):
    from app.apps.painel import prestacao_dados
    s = _dois_socios()
    prestacao_dados.salvar_socio("Sócio Alfa", "Interno", s["Sócio A"])
    prestacao_dados.desativar_socio(s["Parceiro"])
    prestacao_dados.reativar_socio(s["Parceiro"])
    por_id = {x["id"]: x for x in prestacao_dados.socios()}
    assert por_id[s["Sócio A"]]["nome"] == "Sócio Alfa"
    assert por_id[s["Parceiro"]]["ativo"]
    with pytest.raises(ValueError, match="Já existe"):
        prestacao_dados.salvar_socio("Parceiro", "Interno", s["Sócio A"])


def test_as_grades_aparecem_e_gravam_pela_tela(cenario, monkeypatch):
    from app.apps.painel import cenarios
    s = _dois_socios()
    cliente = _cliente(monkeypatch)
    html = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "grade-participacoes" in html and "Todas as obras" in html
    # a linha 0 é "Todas as obras"; as obras vêm depois
    r = cliente.post("/painel/prestacao/montagem", data={
        "acao": "participacoes_em_grade", "cenario_id": cenario,
        "linha": ["", "POUCA GENTE"],
        f"pct_0_{s['Sócio A']}": "100", f"pct_0_{s['Parceiro']}": "",
        f"pct_1_{s['Sócio A']}": "30", f"pct_1_{s['Parceiro']}": "70"})
    assert r.status_code == 302
    assert {(p["obra"], p["socio"]): p["pct"] for p in cenarios.participacoes(cenario)} == {
        ("", "Sócio A"): 100, ("POUCA GENTE", "Sócio A"): 30,
        ("POUCA GENTE", "Parceiro"): 70}
    errado = cliente.post("/painel/prestacao/montagem", data={
        "acao": "participacoes_em_grade", "cenario_id": cenario,
        "linha": [""], f"pct_0_{s['Sócio A']}": "abc"})
    assert "erro=" in errado.headers["Location"]

    html = cliente.get("/painel/prestacao/parametros?aba=socios").get_data(as_text=True)
    assert 'id="socios-lista"' in html and 'placeholder="novo sócio — nome"' in html


def test_juro_marcado_um_a_um_nao_e_contado_duas_vezes(cenario):
    """11/11/2026, o dono: "acho que o juro de empréstimo está sendo contado
    duas vezes, dentro de rateio recebido e em juros de empréstimo". Estava,
    num caso: o lançamento de juro marcado UM A UM na árvore entrava no bolo
    do rateio como exceção E no rateio dos juros pelo déficit."""
    from app.apps.painel import cenarios
    cenarios.marcar_pesos(cenario, "lancamento", ["807"], 100)
    conta = _calcular(cenario)
    rateado = sum(conta["rateio"]["alocacoes"].values()) + sum(
        s["valor"] for s in conta["rateio"]["sobras"])
    juros = sum(conta["juros"]["alocacoes"].values()) + sum(
        s["valor"] for s in conta["juros"]["sobras"])
    # aluguel 900 + patrimônio 300 no rateio; os 600 de juro só nos juros
    assert rateado == pytest.approx(-1200, abs=0.05)
    assert juros == pytest.approx(-600, abs=0.05)


# ===========================================================================
# 11/11/2026 — "colocar totalmente fora da análise ou colocar até determinado
# período"
# ===========================================================================
def test_o_item_fora_guarda_o_mes():
    from app.apps.painel import prestacao
    assert prestacao.ler_item_fora("obra:CASA") == ("obra", "CASA", None)
    assert prestacao.ler_item_fora("projeto:A / B@2024-06") == ("projeto", "A / B", "2024-06")
    assert prestacao.montar_item_fora("obra", "CASA", "2024-06") == "obra:CASA@2024-06"
    mapa = {"C1": "ALFA", "C2": "ALFA", "C3": "BETA"}
    itens = ["obra:C3", "projeto:ALFA@2024-06", "obra:C1@2024-01"]
    assert prestacao.obras_fora_da_analise(itens, mapa, mapa) == {"C3"}
    assert prestacao.cortes_da_analise(itens, mapa, mapa) == {"C1": "2024-01", "C2": "2024-06"}
    assert not prestacao.depois_do_corte("2024-06", "C2", {"C2": "2024-06"})
    assert prestacao.depois_do_corte("2024-07", "C2", {"C2": "2024-06"})
    assert prestacao.depois_do_corte(prestacao.SEM_DATA, "C2", {"C2": "2024-06"})
    assert not prestacao.depois_do_corte("2030-01", "C9", {"C2": "2024-06"})


def test_obra_so_ate_um_mes_sai_dali_em_diante(cenario):
    """MUITA GENTE só até 02/2025: o movimento dela é de 03/2025, então sai da
    conta inteira — e o custo da matriz vai todo para POUCA GENTE."""
    from app.apps.painel import cenarios
    cenarios.atualizar(cenario, juros_por_deficit=0)
    cenarios.gravar_fora(cenario, ["obra:MUITA GENTE@2025-02"])
    conta = _calcular(cenario)
    assert conta["cortes"] == {"MUITA GENTE": "2025-02"}
    assert "MUITA GENTE" not in conta["por_obra"]
    obras_rateadas = {o for (o, _m) in conta["rateio"]["alocacoes"]}
    assert obras_rateadas == {"POUCA GENTE"}
    # até 03/2025, inclusive: entra de novo
    cenarios.gravar_fora(cenario, ["obra:MUITA GENTE@2025-03"])
    assert "MUITA GENTE" in _calcular(cenario)["por_obra"]


def test_a_lista_fora_grava_pela_tela(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cliente = _cliente(monkeypatch)
    html = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "lista-fora" in html and "Tirar da análise" in html
    r = cliente.post("/painel/prestacao/montagem", data={
        "acao": "fora_em_lista", "cenario_id": cenario,
        "fora_chave": ["obra:MUITA GENTE", "obra:POUCA GENTE", "projeto:ALFA"],
        "fora_total": ["obra:POUCA GENTE"],
        "fora_ate": ["2025-02", "", ""]})
    assert r.status_code == 302
    assert sorted(cenarios.excluidas(cenario)) == ["obra:MUITA GENTE@2025-02",
                                                   "obra:POUCA GENTE"]
    sem_mes = cliente.post("/painel/prestacao/montagem", data={
        "acao": "fora_em_lista", "cenario_id": cenario,
        "fora_chave": ["obra:MUITA GENTE"], "fora_modo": ["ate"], "fora_ate": [""]})
    assert "erro=" in sem_mes.headers["Location"]
    assert len(cenarios.excluidas(cenario)) == 2      # nada mudou
    html = cliente.get("/painel/prestacao/parametros?aba=fora").get_data(as_text=True)
    assert "lista-fora" in html


# ===========================================================================
# 11/11/2026 — "na hora de criar regras de rateio não tá tendo correlação
# entre o grupo e a categoria (…) tá muito complicado de lançar cada ajuste"
# ===========================================================================
def test_o_ajuste_e_corrigido_na_linha(base, monkeypatch):
    from app.apps.painel import prestacao_dados
    s = _dois_socios()
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/parametros", data={
        "acao": "ajuste", "aba": "ajustes", "socio_id": s["Sócio A"],
        "tipo": prestacao_dados.TIPOS_AJUSTE[0], "valor": "1000", "data": "2025-03-01"})
    ajuste = prestacao_dados.ajustes()[0]
    html = cliente.get("/painel/prestacao/parametros?aba=ajustes").get_data(as_text=True)
    assert f'id="aj-{ajuste["id"]}"' in html
    cliente.post("/painel/prestacao/parametros", data={
        "acao": "ajuste", "aba": "ajustes", "ajuste_id": ajuste["id"],
        "socio_id": s["Parceiro"], "tipo": ajuste["tipo"], "valor": "1500.50",
        "data": "2025-04-01", "descricao": "corrigido"})
    ajustes = prestacao_dados.ajustes()
    assert len(ajustes) == 1
    assert (ajustes[0]["socio"], ajustes[0]["valor"], ajustes[0]["descricao"]) == (
        "Parceiro", 1500.50, "corrigido")
    with __import__("app.apps.painel.db", fromlist=["conexao"]).conexao() as conn:
        conn.execute("DELETE FROM ajustes")
        conn.commit()


# ===========================================================================
# 11/11/2026 (noite) — "eu vou incluir um sócio, clica, demora (…) um projeto
# às vezes tem 4 sócios, eu tenho que adicionar de um por um"
# ===========================================================================
def test_varios_socios_de_uma_vez(base, monkeypatch):
    from app.apps.painel import prestacao_dados
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/parametros", data={
        "acao": "socios_em_lista", "aba": "socios",
        "socio_id": ["", "", "", ""], "nome": ["Ana", "Beto", "", "Caio"],
        "tipo": ["Interno", "Externo", "Interno", "Interno"]})
    socios = {s["nome"]: s for s in prestacao_dados.socios()}
    assert set(socios) == {"Ana", "Beto", "Caio"} and socios["Beto"]["tipo"] == "Externo"
    # editar um e cadastrar outro, no mesmo Salvar
    r = prestacao_dados.salvar_socios_em_lista([
        (socios["Ana"]["id"], "Ana Maria", "Interno"), (socios["Beto"]["id"], "Beto", "Externo"),
        ("", "Davi", "Interno")])
    assert r == {"novos": 1, "alterados": 1}
    # nome repetido não grava nada
    with pytest.raises(ValueError, match="Já existe"):
        prestacao_dados.salvar_socios_em_lista([("", "Caio", "Interno"), ("", "Eva", "Interno")])
    assert "Eva" not in {s["nome"] for s in prestacao_dados.socios()}


def test_quem_divide_por_projeto_vale_para_as_obras_dele(cenario):
    """Obra nomeada > projeto > todas as obras. As duas obras do teste são do
    projeto ALFA."""
    from app.apps.painel import cenarios, prestacao
    s = _dois_socios()
    cenarios.gravar_participacoes_em_grade(cenario, {
        ("", s["Sócio A"]): "100",
        ("projeto:ALFA", s["Sócio A"]): "60", ("projeto:ALFA", s["Parceiro"]): "40",
        ("POUCA GENTE", s["Sócio A"]): "50", ("POUCA GENTE", s["Parceiro"]): "50"})
    conta = _calcular(cenario)
    pct = {(q["obra"], q["socio"]): q["pct"] for q in conta["quotas"]}
    assert pct[("MUITA GENTE", "Sócio A")] == 60 and pct[("MUITA GENTE", "Parceiro")] == 40
    assert pct[("POUCA GENTE", "Sócio A")] == 50
    # sem projeto conhecido, vale a linha geral
    geral = prestacao.participantes_da_obra(cenarios.participacoes(cenario), "OUTRA", "BETA")
    assert [(p["socio"], p["pct"]) for p in geral] == [("Sócio A", 100)]


def test_a_parte_do_principal_sem_mes_vale_desde_o_inicio(cenario):
    from app.apps.painel import cenarios, prestacao
    cenarios.gravar_periodos(cenario, "grupo", "Administrativas", [("", 100, 40)])
    pesos = cenarios.pesos(cenario)
    assert pesos["periodos"]["grupo"]["Administrativas"] == [
        (cenarios.DESDE_O_INICIO, 100.0, 40.0)]
    assert prestacao.peso_e_divisao(pesos, "Administrativas", "Aluguel", "", 100,
                                    mes="2015-01") == (100.0, 40.0)
    # linha nova sem mês e sem a parte do principal não é período nenhum
    cenarios.gravar_periodos(cenario, "grupo", "Patrimônio", [("", 100, "")])
    assert "Patrimônio" not in cenarios.pesos(cenario)["periodos"]["grupo"]
    # a tela mostra a coluna, não mais o link pequeno
    from app.apps.painel.web import _calcular_cenario  # noqa: F401 — a conta roda
    assert _calcular(cenario)["rateio"]["alocacoes"]


def test_a_montagem_mostra_a_parte_do_principal_numa_coluna(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cenarios.gravar_projetos(cenario, "ALFA", {})
    cenarios.gravar_periodos(cenario, "grupo", "Administrativas", [("2025-01", 100, 40)])
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "Para o ALFA" in html and "desde 01/2025" in html
    assert "＋ definir" in html and "Projeto — ALFA" in html


# ===========================================================================
# 11/11/2026 (noite) — "eu quero mostrar essa tela para os outros sócios (…)
# eles precisam bater o olho e entender" / "não quero que apareça na tela"
# ===========================================================================
def test_a_pagina_como_e_feita_explica_o_cenario(cenario, monkeypatch):
    from app.apps.painel import cenarios
    s = _dois_socios()
    cenarios.gravar_projetos(cenario, "ALFA", {})
    cenarios.gravar_periodos(cenario, "grupo", "Administrativas",
                             [("2025-01", 100, 50), ("2026-01", 100, 40)])
    cenarios.gravar_participacoes_em_grade(cenario, {
        ("projeto:ALFA", s["Sócio A"]): "60", ("projeto:ALFA", s["Parceiro"]): "40"})
    cenarios.gravar_fora(cenario, ["obra:POUCA GENTE"])
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/como?cenario={cenario}").get_data(as_text=True)
    assert "Como esta prestação é feita" in html
    assert "50% a partir de 01/2025 · 40% a partir de 01/2026" in html
    assert "Para o ALFA" in html and "o restante" in html
    assert "Projeto ALFA" in html and "60%" in html
    assert "custo de mão de obra" in html and "caixa negativo" in html
    assert "taxa de administração" in html          # tem parceiro de fora
    # o que está fora fica recolhido: a contagem aparece, o nome só ao abrir
    assert "1 obra(s) ou projeto(s) estão fora" in html


def test_a_obra_fora_nao_aparece_pelo_nome_no_resultado(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cenarios.gravar_fora(cenario, ["obra:POUCA GENTE"])
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/resultado?cenario={cenario}").get_data(as_text=True)
    assert "1 obra(s) fora desta prestação" in html
    # o nome só existe dentro do bloco recolhido ("ver quais")
    antes, _, depois = html.partition("<details")
    recolhido, _, resto = depois.partition("</details>")
    assert "POUCA GENTE" in recolhido
    assert "POUCA GENTE" not in antes and "POUCA GENTE" not in resto


def test_a_montagem_vem_em_passos(cenario, monkeypatch):
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    ordem = [html.index(t) for t in ('id="passo-fora"', 'id="passo-divide"',
                                     'id="passo-recebe"', 'id="passo-socios"',
                                     'id="passo-regua"')]
    assert ordem == sorted(ordem)
    assert "Como é feita" in html


# ===========================================================================
# 11/11/2026 (noite) — a prestação ANTIGA saiu. O dono: "eu não sei o que é
# essa prestação de conta antiga (…) se não interessa mais, pode tirar".
# ===========================================================================
def test_a_aba_prestacao_de_contas_abre_o_resultado_do_cenario(cenario, monkeypatch):
    cliente = _cliente(monkeypatch)
    r = cliente.get("/painel/prestacao")
    assert r.status_code == 302 and r.headers["Location"].endswith("/painel/prestacao/resultado")
    # os endereços antigos não dão erro: levam à montagem
    for antigo in ("/painel/prestacao/cenarios", "/painel/prestacao/parametros?aba=regras",
                   "/painel/prestacao/parametros?aba=participacoes"):
        r = cliente.get(antigo)
        assert r.status_code == 302 and "/painel/prestacao/montagem" in r.headers["Location"], antigo
    html = cliente.get("/painel/prestacao/parametros").get_data(as_text=True)
    assert "prestação antiga" not in html and "Regras de rateio" not in html


def test_sem_cenario_a_aba_leva_a_montagem(base, monkeypatch):
    r = _cliente(monkeypatch).get("/painel/prestacao")
    assert r.status_code == 302 and r.headers["Location"].endswith("/painel/prestacao/montagem")


def test_quem_tem_login_proprio_nao_ve_mais_a_aba_da_prestacao(preso, monkeypatch):
    """A aba abre o cenário, que mostra a empresa inteira e a parte de cada
    sócio. O `preso` tinha a tela "prestacao" marcada: ela some do menu dele,
    e o endereço não abre."""
    from app.apps.painel import usuarios
    assert "prestacao" not in usuarios.TELAS
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get("/painel/prestacao").status_code == 404
    html = cliente.get("/painel/dre").get_data(as_text=True)
    assert "Prestação de Contas" not in html


# ===========================================================================
# 11/11/2026 (noite) — "isso aqui era para ser uma das primeiras perguntas
# (…) a gente tira num canto, aí ele fica aparecendo no outro (…) por que eu
# não posso (…) gravar tudo de uma vez? (…) o projeto BWS é exatamente os
# custos da matriz — não era para ele aparecer aqui"
# ===========================================================================
def test_o_que_esta_fora_some_dos_outros_quadros(cenario, monkeypatch):
    import re

    from app.apps.painel import cenarios
    s = _dois_socios()
    cenarios.gravar_participacoes_em_grade(cenario, {("POUCA GENTE", s["Sócio A"]): "100"})
    cenarios.gravar_fora(cenario, ["obra:POUCA GENTE"])
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    # o passo 1 é o que entra, e a obra continua NO QUADRO, marcada
    assert html.index('id="passo-fora"') < html.index('id="passo-divide"')
    assert re.search(r'name="fora_total" value="obra:POUCA GENTE" data-fora-total\s+checked', html)
    # ...mas não na grade de quem divide
    grade = html[html.index('id="passo-socios"'):html.index('id="passo-regua"')]
    assert "POUCA GENTE" not in grade
    # e a participação dela não foi apagada: volta se a obra voltar
    assert cenarios.participacoes(cenario)


def test_projeto_que_e_so_a_estrutura_nao_aparece(cenario, monkeypatch):
    """A matriz do teste está no projeto ALFA junto com as obras; pondo-a num
    projeto só dela ("ESTRUTURA"), ele não pode aparecer em quem recebe nem em
    quem divide — não tem obra nenhuma."""
    from app.apps.painel import consultas, prestacao_dados
    from app.apps.painel.db import conexao
    from app.apps.painel.web import _projetos_e_obras_da_montagem
    with conexao() as conn:
        conn.execute("UPDATE fato SET projeto = 'ESTRUTURA' WHERE departamento = ?", (MATRIZ,))
        conn.commit()
    consultas.esquecer_listas()
    nomes = _projetos_e_obras_da_montagem(prestacao_dados.config(), None)
    assert nomes["projetos"] == ["ALFA"] and MATRIZ not in nomes["obras"]
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "Projeto — ESTRUTURA" not in html and "Projeto — ALFA" in html


def test_salvar_tudo_grava_todas_as_secoes_de_uma_vez(cenario, monkeypatch):
    from app.apps.painel import cenarios
    s = _dois_socios()
    cliente = _cliente(monkeypatch)
    r = cliente.post("/painel/prestacao/montagem", data={
        "acao": "tudo", "cenario_id": cenario,
        # passo 1
        "fora_chave": ["projeto:ALFA", "obra:POUCA GENTE"], "fora_ate": ["", "2025-02"],
        # passo 2: dois períodos novos de uma vez
        "item_nivel": ["grupo"], "item_chave": ["Administrativas"], "item_pct": ["80"],
        "periodo_presente": ["grupo|Administrativas"],
        "periodo_nivel": ["grupo", "grupo", "grupo"],
        "periodo_chave": ["Administrativas"] * 3,
        "periodo_desde": ["2025-01", "2026-01", ""],
        "periodo_pct": ["100", "100", "100"], "periodo_principal": ["50", "40", ""],
        # passo 3
        "principal": "", "proj_nome": ["ALFA"], "proj_papel": ["periodo"],
        "proj_desde": [""], "proj_ate": ["2025-12"],
        # passo 4
        "linha": ["", "projeto:ALFA"],
        f"pct_0_{s['Sócio A']}": "100", f"pct_1_{s['Sócio A']}": "60",
        f"pct_1_{s['Parceiro']}": "40",
        # passo 5
        "nome": "Tudo junto", "criterio": "faturamento", "pct_padrao": "90",
        "juros_por_deficit": "0"})
    assert r.status_code == 302 and "erro=" not in r.headers["Location"]
    c = cenarios.completo(cenario)
    assert c["excluidas"] == ["obra:POUCA GENTE@2025-02"]
    assert c["pesos"]["grupo"]["Administrativas"] == 80
    assert c["pesos"]["periodos"]["grupo"]["Administrativas"] == [
        ("2025-01", 100.0, 50.0), ("2026-01", 100.0, 40.0)]
    assert c["projetos"]["regras"]["ALFA"] == ("periodo", "", "2025-12")
    assert len(c["participacoes"]) == 3
    assert (c["nome"], c["criterio"]) == ("Tudo junto", "faturamento")


def test_salvar_tudo_com_erro_nao_grava_pela_metade(cenario, monkeypatch):
    from app.apps.painel import cenarios
    s = _dois_socios()
    r = _cliente(monkeypatch).post("/painel/prestacao/montagem", data={
        "acao": "tudo", "cenario_id": cenario,
        "fora_chave": ["obra:POUCA GENTE"], "fora_total": ["obra:POUCA GENTE"], "fora_ate": [""],
        "linha": [""], f"pct_0_{s['Sócio A']}": "cento e dez",
        "nome": "Não era para mudar", "criterio": "faturamento"})
    assert "erro=" in r.headers["Location"]
    c = cenarios.completo(cenario)
    assert c["excluidas"] == [] and c["nome"] != "Não era para mudar"


def test_tirar_da_analise_nao_apaga_a_regra_de_quem_recebe(cenario):
    from app.apps.painel import cenarios
    from app.apps.painel.web import _gravar_quem_recebe
    from werkzeug.datastructures import MultiDict
    cenarios.gravar_projetos(cenario, "ALFA", {"BETA": ("periodo", "", "2025-12")})
    # BETA está fora da análise: não vem no formulário
    _gravar_quem_recebe(cenario, MultiDict([("principal", "ALFA"), ("proj_nome", "ALFA"),
                                            ("proj_papel", ""), ("proj_desde", ""),
                                            ("proj_ate", "")]))
    assert cenarios.projetos(cenario)["regras"]["BETA"] == ("periodo", "", "2025-12")


def test_a_montagem_diz_de_onde_vem_o_custo_da_matriz(cenario, monkeypatch):
    """11/11/2026, o dono: "a matriz gastou 15,4 milhões (…) de onde é que
    está sendo puxado esse valor?" """
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "De onde vem “a matriz gastou”" in html
    assert f"<b>{MATRIZ}</b>" in html and "Abrir o DRE só desses centros de custo" in html


def test_o_projeto_abre_nas_obras_dele(cenario, monkeypatch):
    """11/11/2026, o dono: "deveria podermos expandir o projeto e ver as obras
    e poder editar cada obra de dentro do projeto"."""
    from app.apps.painel import cenarios, prestacao_dados
    from app.apps.painel.web import _linhas_fora, _projetos_e_obras_da_montagem
    cenarios.gravar_fora(cenario, ["obra:POUCA GENTE@2025-02"])
    nomes = _projetos_e_obras_da_montagem(prestacao_dados.config(), cenarios.completo(cenario))
    linhas = _linhas_fora(cenarios.excluidas(cenario), nomes["projetos"], nomes["obras"],
                          projeto_de=nomes["projeto_de"])
    # o projeto, e logo abaixo as obras dele; já vem aberto, porque uma tem situação
    assert [(l["rotulo"], l["pai"]) for l in linhas] == [
        ("Projeto — ALFA", ""), ("MUITA GENTE", "ALFA"), ("POUCA GENTE", "ALFA")]
    assert linhas[0]["obras"] == 2 and linhas[0]["aberto"] is True
    assert linhas[2]["modo"] == "ate" and linhas[2]["ate"] == "2025-02"
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert 'data-pai="projeto:ALFA"' in html and "data-fora-abre" in html


# ===========================================================================
# 11/11/2026 — "Não salvou: o servidor respondeu 500" no Salvar tudo
# ===========================================================================
@pytest.mark.parametrize("formato", ["multipart/form-data",
                                     "application/x-www-form-urlencoded"])
def test_salvar_tudo_aceita_a_pagina_de_uma_empresa_de_verdade(cenario, monkeypatch, formato):
    """A montagem manda a página inteira num envio. O teto padrão do servidor
    era de 1.000 campos, e uma empresa de verdade passa disso só com as linhas
    de período em branco — o dono recebia "o servidor respondeu 500" e não
    tinha como gravar nada. Aqui vão mais de 6.000 campos."""
    from werkzeug.datastructures import MultiDict
    from app.apps.painel import cenarios
    s = _dois_socios()
    campos = [("acao", "tudo"), ("cenario_id", str(cenario)),
              ("linha", ""), (f"pct_0_{s['Sócio A']}", "100")]
    for i in range(1200):
        conta = f"Categoria número {i} com nome comprido"
        campos += [("periodo_presente", f"categoria|{conta}"),
                   ("periodo_nivel", "categoria"), ("periodo_chave", conta),
                   ("periodo_desde", ""), ("periodo_pct", "100"),
                   ("periodo_principal", "")]
    r = _cliente(monkeypatch).post("/painel/prestacao/montagem",
                                   data=MultiDict(campos), content_type=formato)
    assert r.status_code == 302 and "erro=" not in r.headers["Location"]
    assert {(p["obra"], p["socio"]): p["pct"]
            for p in cenarios.participacoes(cenario)} == {("", "Sócio A"): 100}


def test_formulario_acima_do_teto_diz_o_motivo_e_nao_500(cenario, monkeypatch):
    from werkzeug.datastructures import MultiDict
    from app.apps.painel import web
    monkeypatch.setattr(web, "TETO_DE_CAMPOS", 50)
    campos = [("acao", "tudo"), ("cenario_id", str(cenario))] + [
        ("periodo_desde", "")] * 200
    r = _cliente(monkeypatch).post("/painel/prestacao/montagem", data=MultiDict(campos),
                                   content_type="multipart/form-data")
    assert r.status_code == 413
    assert "mais dados do que o servidor aceita" in r.get_data(as_text=True)


def test_a_grade_e_lida_numa_passada_e_ignora_campo_estranho():
    from werkzeug.datastructures import MultiDict
    from app.apps.painel.web import _celulas_da_grade
    form = MultiDict([("linha", ""), ("linha", "obra com _ no nome"),
                      ("pct_0_7", "100"), ("pct_1_7", "60"), ("pct_1_9", ""),
                      ("pct_5_7", "10"),          # linha que não veio
                      ("pct_padrao", "90"), ("pct_x_7", "1")])
    assert _celulas_da_grade(form) == {
        ("", 7): "100", ("obra com _ no nome", 7): "60", ("obra com _ no nome", 9): ""}


def test_a_tela_salva_sem_mandar_as_linhas_em_branco(cenario, monkeypatch):
    """A tela desliga as linhas de período em branco antes de juntar os dados
    e manda em texto simples — o contrato fica preso aqui."""
    html = _cliente(monkeypatch).get(
        f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "linhasEmBranco()" in html and "new URLSearchParams()" in html
    assert "data-erro-do-servidor" in html     # mostra o motivo, não só o número


# ===========================================================================
# 11/11/2026 — "Não tem onde editar ou excluir os sócios"
# ===========================================================================
def test_socio_e_excluido_com_os_percentuais_dele(cenario):
    from app.apps.painel import cenarios, prestacao_dados
    s = _dois_socios()
    cenarios.gravar_participacoes_em_grade(cenario, {
        ("", s["Sócio A"]): "60", ("", s["Parceiro"]): "40"})
    assert prestacao_dados.excluir_socio(s["Parceiro"]) == "Parceiro"
    assert [x["nome"] for x in prestacao_dados.socios()] == ["Sócio A"]
    assert {p["socio"] for p in cenarios.participacoes(cenario)} == {"Sócio A"}
    with pytest.raises(ValueError, match="não existe"):
        prestacao_dados.excluir_socio(s["Parceiro"])


def test_socio_com_ajuste_lancado_nao_e_excluido(base):
    from app.apps.painel import prestacao_dados
    s = _dois_socios()
    prestacao_dados.salvar_ajuste(s["Parceiro"], "Aporte", 1000, "2025-01-10")
    with pytest.raises(ValueError, match="1 ajuste"):
        prestacao_dados.excluir_socio(s["Parceiro"])
    assert len(prestacao_dados.socios()) == 2


def test_a_tela_de_socios_tem_excluir_e_a_montagem_leva_ate_ela(cenario, monkeypatch):
    s = _dois_socios()
    cliente = _cliente(monkeypatch)
    html = cliente.get("/painel/prestacao/parametros?aba=socios").get_data(as_text=True)
    assert 'value="excluir_socio"' in html and "Salvar tudo" in html
    montagem = cliente.get(f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    assert "Cadastrar, renomear ou excluir sócio" in montagem
    r = cliente.post("/painel/prestacao/parametros",
                     data={"acao": "excluir_socio", "aba": "socios", "socio_id": s["Parceiro"]})
    assert r.status_code == 302 and "erro=" not in r.headers["Location"]
    assert "Parceiro" not in cliente.get(
        "/painel/prestacao/parametros?aba=socios").get_data(as_text=True)


def test_quem_divide_abre_o_projeto_nas_obras_dele():
    """11/11/2026, o dono: "Quem divide o resultado deveria expandir os centros
    de custo similar ao Fora da análise". As obras vêm logo abaixo do projeto
    delas; o projeto com obra de percentual próprio já vem aberto."""
    from app.apps.painel.web import _grade_quem_divide
    cenario = {"participacoes": [
        {"obra": "", "socio_id": 1, "pct": 100},
        {"obra": "OBRA B2", "socio_id": 2, "pct": 50},
        {"obra": "obra:FORA", "socio_id": 2, "pct": 50}]}
    r = _grade_quem_divide(
        cenario, ["OBRA A1", "OBRA B1", "OBRA B2", "SOLTA"], ["PROJ A", "PROJ B"],
        esconder={"obra:FORA"},
        projeto_de={"OBRA A1": "PROJ A", "OBRA B1": "PROJ B", "OBRA B2": "PROJ B"})
    linhas = r["linhas_quem_divide"]
    assert [(l["chave"], l.get("pai", "-")) for l in linhas] == [
        ("", "-"),
        ("projeto:PROJ A", "-"), ("OBRA A1", "projeto:PROJ A"),
        ("projeto:PROJ B", "-"), ("OBRA B1", "projeto:PROJ B"), ("OBRA B2", "projeto:PROJ B"),
        ("SOLTA", "")]
    por_chave = {l["chave"]: l for l in linhas}
    assert por_chave["projeto:PROJ B"]["aberto"] and por_chave["projeto:PROJ B"]["obras"] == 2
    assert not por_chave["projeto:PROJ A"]["aberto"]
    assert "obra:FORA" not in r["grade_quem_divide"]


def test_a_posicao_dos_socios_e_a_ordem_da_lista_salva(cenario):
    """11/11/2026, o dono: "não consigo (…) mudar a posição". A ordem em que a
    lista é salva vira a posição de cada um — nas colunas e no resultado."""
    from app.apps.painel import cenarios, prestacao_dados
    s = _dois_socios()
    assert [x["nome"] for x in prestacao_dados.socios()] == ["Parceiro", "Sócio A"]   # por nome
    prestacao_dados.salvar_socios_em_lista([
        (s["Sócio A"], "Sócio A", "Interno"), (s["Parceiro"], "Parceiro", "Externo"),
        ("", "Zé Novo", "Interno"), ("", "", "Interno")])
    assert [x["nome"] for x in prestacao_dados.socios()] == ["Sócio A", "Parceiro", "Zé Novo"]
    # cadastrado por fora da lista: vai para o fim, sem quebrar nada
    prestacao_dados.salvar_socio("Ana")
    assert [x["nome"] for x in prestacao_dados.socios(apenas_ativos=True)] == [
        "Sócio A", "Parceiro", "Zé Novo", "Ana"]
    # e o resultado segue a posição escolhida
    cenarios.gravar_participacoes_em_grade(cenario, {
        ("", s["Sócio A"]): "50", ("", s["Parceiro"]): "50"})
    posicao = [p["socio"] for p in _calcular(cenario)["posicao"]]
    assert posicao == ["Sócio A", "Parceiro"]
    # excluir quem está na ordem não atrapalha
    prestacao_dados.excluir_socio(s["Sócio A"])
    assert [x["nome"] for x in prestacao_dados.socios()] == ["Parceiro", "Zé Novo", "Ana"]


def test_a_tela_de_socios_tem_as_setas_de_posicao(base, monkeypatch):
    _dois_socios()
    html = _cliente(monkeypatch).get("/painel/prestacao/parametros?aba=socios").get_data(as_text=True)
    assert html.count("data-socio-sobe") >= 2 and "data-socio-desce" in html



# ===========================================================================
# 11/11/2026 — "veja que não bate os juros e nem o que a matriz gastou"
# ===========================================================================
def test_a_estrutura_e_conferida_contra_o_dre_do_projeto_dela(cenario, monkeypatch):
    """O dono colou "a matriz gastou" (15,5 mi) e o DRE do projeto BWS (14,7
    mi). Os dois somam igual; o que muda é a lista de centros de custo. A
    conferência mostra, com nome e valor, o centro de custo que é do projeto
    e não está marcado como estrutura — e o marcado que é de outro projeto."""
    from app.apps.painel import consultas, prestacao_dados
    from app.apps.painel.db import conexao
    from app.apps.painel.web import _contas_da_matriz
    from app.apps.painel import cenarios
    with conexao() as conn:
        conn.execute("UPDATE fato SET projeto = 'BWS' WHERE departamento = ?", (MATRIZ,))
        conn.execute("UPDATE fato SET projeto = 'OBRAS' WHERE departamento <> ?", (MATRIZ,))
        # o segundo centro de custo do projeto, que NÃO está marcado como estrutura
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
            " VALUES (950,'2. Contas a Pagar','DRE','PAGO','Quitado','Aluguel',"
            "         'Administrativas','Escritório Central','BWS','X','2025-03-10',2025,-400,0,-10,0)")
        conn.commit()
    consultas.esquecer_listas()
    config = dict(prestacao_dados.config(), depto_admin_matriz=MATRIZ, depto_admin_filial="")
    cp = _contas_da_matriz(config, cenarios.completo(cenario), "comprometido")[
        "origem"]["contra_o_projeto"]
    assert not cp["bate"]
    assert cp["estrutura"] == pytest.approx(-1800.0)
    assert cp["projetos"] == [("BWS", pytest.approx(-2210.0))]      # com o encargo de 10
    assert cp["diferenca"] == pytest.approx(410.0)
    assert [(c["depto"], c["estrutura"]) for c in cp["centros"]] == [
        (MATRIZ, True), ("Escritório Central", False)]

    # marcado o segundo centro de custo, bate — e a tela diz isso
    config["depto_admin_filial"] = "Escritório Central"
    cp = _contas_da_matriz(config, cenarios.completo(cenario), "comprometido")[
        "origem"]["contra_o_projeto"]
    assert cp["bate"] and cp["estrutura"] == pytest.approx(-2210.0)


def test_a_montagem_acusa_a_diferenca_e_mostra_os_juros_numa_linha(cenario, monkeypatch):
    from app.apps.painel import consultas, prestacao_dados
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET projeto = 'BWS' WHERE departamento = ?", (MATRIZ,))
        conn.execute("UPDATE fato SET projeto = 'OBRAS' WHERE departamento <> ?", (MATRIZ,))
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
            " VALUES (951,'2. Contas a Pagar','DRE','PAGO','Quitado','Aluguel',"
            "         'Administrativas','Escritório Central','BWS','X','2025-03-10',2025,-400,0,0,0)")
        conn.commit()
    consultas.esquecer_listas()
    antes = prestacao_dados.config()
    prestacao_dados.salvar_config("depto_admin_matriz", MATRIZ)
    prestacao_dados.salvar_config("depto_admin_filial", "")
    try:
        html = _cliente(monkeypatch).get(
            f"/painel/prestacao/montagem?cenario={cenario}").get_data(as_text=True)
    finally:      # a configuração é do banco inteiro: devolve como estava
        for chave in ("depto_admin_matriz", "depto_admin_filial"):
            prestacao_dados.salvar_config(chave, antes.get(chave, ""))
    assert "não bate com o DRE" in html and "Escritório Central" in html
    assert "não está marcado como estrutura" in html
    # os juros: só o valor e a categoria — o quadro grande saiu da montagem
    assert "Onde estão os juros de empréstimo" not in html
    assert "Juros de empréstimo" in html and "Juros Sobre Empréstimos" in html


def test_o_quadro_dos_juros_soma_o_encargo_como_a_conta(base):
    """R$ 3.039.343,02 num quadro e R$ 3.030.809,67 no outro, na mesma tela:
    a conta distribui o juro COM os juros e multas pagos por atraso; o quadro
    de conferência somava sem. Agora os dois dão o mesmo número."""
    from app.apps.painel import consultas, cenarios
    from app.apps.painel.db import conexao
    from app.apps.painel.web import _contas_da_matriz
    from app.apps.painel import prestacao_dados
    with conexao() as conn:
        conn.execute("UPDATE fato SET juros = -7, multa = -3 WHERE codigo_lancamento = 807")
        conn.commit()
    consultas.esquecer_listas()
    conf = consultas.conferencia_dos_juros({"juros sobre empréstimos"}, [MATRIZ])
    assert conf["distribui"] == pytest.approx(610.0)
    c = cenarios.completo(cenarios.criar("Juros", criterio="pessoal", pct_padrao=100))
    config = dict(prestacao_dados.config(), depto_admin_matriz=MATRIZ, depto_admin_filial="")
    assert _contas_da_matriz(config, c, "comprometido")["resumo"]["juros"] == pytest.approx(-610.0)
