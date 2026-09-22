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
def test_o_juro_nao_entra_no_bolo_da_estrutura(cenario):
    """Se entrasse, seria repartido pelo pessoal — e cobrado de obra que se
    paga sozinha. Os 600 de juros ficam fora dos 1.200."""
    conta = _calcular(cenario)
    assert sum(conta["rateio"]["alocacoes"].values()) == pytest.approx(-1200, abs=0.02)
    assert sum(conta["juros"]["alocacoes"].values()) == pytest.approx(-600, abs=0.02)


def test_o_juro_cai_em_quem_ficou_negativo(cenario):
    """MUITA GENTE recebeu 1.000 e gastou 2.000, mais 800 de estrutura: está no
    vermelho. POUCA GENTE recebeu 5.000 e gastou 1.400: está no azul, e não
    paga juro nenhum."""
    conta = _calcular(cenario)
    por_obra = conta["por_obra"]
    assert por_obra["MUITA GENTE"]["juros"] == pytest.approx(-600, abs=0.02)
    assert por_obra["POUCA GENTE"]["juros"] == pytest.approx(0, abs=0.02)


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


def test_quem_esta_preso_a_uma_obra_nao_alcanca_o_cenario(preso, monkeypatch):
    """As duas telas mostram a empresa inteira e configuram a conta de todos.
    Não passam pelo filtro de escopo — então não são de quem está preso.

    Repare que ele TEM a tela "prestacao" liberada: mesmo assim não entra. É de
    propósito — a prestação antiga é um relatório, o cenário configura a conta
    de todo mundo."""
    cliente = _cliente_preso(monkeypatch)
    assert cliente.get("/painel/prestacao/montagem").status_code == 404
    assert cliente.get("/painel/prestacao/resultado").status_code == 404


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


def test_tirar_e_voltar_pela_tela(cenario, monkeypatch):
    from app.apps.painel import cenarios
    cliente = _cliente(monkeypatch)
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "excluir", "cenario_id": str(cenario), "item": ["obra:MUITA GENTE"]})
    assert cenarios.excluidas(cenario) == ["obra:MUITA GENTE"]
    html = cliente.get(f"/painel/prestacao/resultado?cenario={cenario}").get_data(as_text=True)
    assert "Fora da análise neste cenário" in html and "MUITA GENTE" in html
    cliente.post("/painel/prestacao/montagem", data={
        "acao": "reincluir", "cenario_id": str(cenario), "item": "obra:MUITA GENTE"})
    assert cenarios.excluidas(cenario) == []


def test_duplicar_leva_as_excluidas(cenario):
    from app.apps.painel import cenarios
    cenarios.excluir(cenario, "obra:MUITA GENTE")
    copia = cenarios.duplicar(cenario, "Cópia")
    assert cenarios.excluidas(copia) == ["obra:MUITA GENTE"]


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

    html = _cliente(monkeypatch).get("/painel/configuracoes?conferir=1").get_data(as_text=True)
    assert "Onde estão os juros de empréstimo" in html
