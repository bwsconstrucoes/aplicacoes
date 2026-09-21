# -*- coding: utf-8 -*-
"""
As consultas das telas novas, provadas com banco de verdade.

Continuação de `test_painel_banco.py` — mesmo cenário, mesmo Postgres
descartável. Aqui entram Receita de Obra, Necessidade de Caixa e a Prestação de
Contas, que é a tela onde um erro custa mais caro: ela decide quanto do lucro
cabe a cada sócio.
"""
from __future__ import annotations

import pytest

from tests.test_painel_banco import painel_no_banco, reais  # noqa: F401

pytestmark = pytest.mark.banco


# ---------------------------------------------------------------------------
# Receita de Obra
# ---------------------------------------------------------------------------
def test_medicoes_somam_o_bruto_certo(painel_no_banco):
    """O cenário é inserido direto no fato, sem passar pela reconstrução — então
    a coluna da medição está vazia e tudo cai em "(sem medição)". A consulta
    tem de continuar de pé e somar certo: é o que acontece de verdade entre
    aplicar a migração e rodar a próxima atualização."""
    from app.apps.painel import consultas
    total = consultas.total_das_medicoes(consultas.Filtros())
    assert reais(total["recebido"]) == 1000.00
    assert reais(total["retido"]) == 100.00
    assert reais(total["a_receber"]) == 2300.00      # 2.000 + os 300 sem data
    assert reais(total["bruto"]) == 3400.00


def test_visao_das_medicoes_separa_quitadas_de_pendentes(painel_no_banco):
    from app.apps.painel import consultas
    f = consultas.Filtros()
    com_saldo = consultas.total_das_medicoes(f, visao="a_receber")
    quitadas = consultas.total_das_medicoes(f, visao="quitadas")
    todas = consultas.total_das_medicoes(f)
    assert reais(com_saldo["a_receber"]) == 2300.00
    assert reais(quitadas["a_receber"]) == 0.00
    assert com_saldo["quantas"] + quitadas["quantas"] == todas["quantas"]


def test_outras_receitas_nao_repetem_a_receita_de_obra(painel_no_banco):
    """Se a separação falhar, a receita de obra aparece duas vezes na tela."""
    from app.apps.painel import consultas
    outras = consultas.outras_receitas(consultas.Filtros())
    assert all(o["categoria"] != "Receita de Obras" for o in outras)
    assert all("Retido" not in o["categoria"] for o in outras)


# ---------------------------------------------------------------------------
# Necessidade de Caixa
# ---------------------------------------------------------------------------
def test_caixa_por_obra_traz_so_o_que_virou_dinheiro(painel_no_banco):
    from app.apps.painel import consultas
    por_obra = {}
    for _mes, obra, valor in consultas.caixa_mensal_por_obra():
        por_obra[obra] = por_obra.get(obra, 0.0) + valor
    # CASA recebeu 1.000 e pagou 400 (06/2025) e 100 (03/2024), MAIS 25 de juros
    # e multa pagos junto com os 400 — juros sai da conta como qualquer
    # pagamento. Os 999 de juros PREVISTOS num título em aberto não entram: só
    # conta o que foi pago de fato.
    assert reais(por_obra["CASA"]) == 475.00
    assert reais(por_obra["PONTE"]) == -900.00
    # a transferência entre contas não é caixa da operação
    assert reais(sum(por_obra.values())) == -425.00


def test_obra_para_projeto_resolve_o_dominante(painel_no_banco):
    from app.apps.painel import consultas
    mapa = consultas.obra_para_projeto()
    assert mapa["CASA"] == "ALFA"
    assert mapa["PONTE"] == "BETA"


def test_financeiro_mensal_nao_confunde_as_fontes(painel_no_banco):
    """Empréstimo, aporte e dividendo são coisas diferentes e entram em linhas
    diferentes da simulação. O cenário não tem nenhum — o teste garante que a
    consulta devolve isso, e não um erro."""
    from app.apps.painel import consultas
    assert consultas.financeiro_mensal() == []


# ---------------------------------------------------------------------------
# Prestação de Contas
# ---------------------------------------------------------------------------
def test_a_base_da_prestacao_separa_receita_de_despesa(painel_no_banco):
    from app.apps.painel import consultas
    linhas = consultas.apuracao_por_obra_mes("comprometido")
    casa = [l for l in linhas if l["obra"] == "CASA"]
    assert reais(sum(l["receita_liquida"] for l in casa)) == 1300.00
    assert reais(sum(l["retencoes"] for l in casa)) == 100.00
    # 400 + 250 + 100 de principal MAIS os 25 de juros e multa pagos: desde
    # 08/09/2026 os encargos são despesa em todo o painel, e a prestação de
    # contas divide o resultado DEPOIS deles
    assert reais(sum(l["despesas"] for l in casa)) == -775.00


@pytest.fixture()
def cadastro_limpo(painel_no_banco):
    """Sócios e participações só deste teste, apagados no fim."""
    from app.apps.painel.db import conexao

    def _limpar():
        with conexao() as conn:
            conn.execute("DELETE FROM participacoes")
            conn.execute("DELETE FROM socios")
            conn.commit()

    _limpar()
    yield
    _limpar()


def test_a_prestacao_roda_de_ponta_a_ponta(cadastro_limpo):
    """Junta tudo — base do banco, rateio, apuração e divisão — e confere a
    invariante: a soma das quotas fecha com o resultado do projeto."""
    from app.apps.painel import consultas, prestacao, prestacao_dados

    prestacao_dados.salvar_socio("ANA", "Interno")
    prestacao_dados.salvar_socio("BENTO", "Interno")
    socios = {s["nome"]: s["id"] for s in prestacao_dados.socios()}
    prestacao_dados.salvar_participacao("ALFA", socios["ANA"], 70)
    prestacao_dados.salvar_participacao("ALFA", socios["BENTO"], 30)

    config = prestacao_dados.config()
    apuracao = consultas.apuracao_por_obra_mes("comprometido")
    obras = prestacao.classificar_obras(apuracao, config)
    pessoal = consultas.custo_de_pessoal_por_obra_mes(config["grupo_pessoal"])
    admin = consultas.despesa_administrativa(
        [config["depto_admin_matriz"], config["depto_admin_filial"]])
    rateio = prestacao.calcular_rateio(admin, pessoal, obras,
                                       prestacao_dados.regras(), config)
    apurado = prestacao.apurar(apuracao, obras, rateio["alocacoes"])
    por_projeto = prestacao.totalizar_por_projeto(apurado)
    quotas = prestacao.quotas_por_socio(por_projeto,
                                        prestacao_dados.participacoes(), config)

    # ALFA é a obra CASA: receita líquida 1.300, despesas 775 (750 de principal
    # mais 25 de juros e multa pagos) -> resultado 525
    assert reais(por_projeto["ALFA"]["resultado"]) == 525.00
    assert reais(sum(q["quota"] for q in quotas)) == 525.00
    por_socio = {q["socio"]: q["quota"] for q in quotas}
    assert reais(por_socio["ANA"]) == 367.50       # 70%
    assert reais(por_socio["BENTO"]) == 157.50     # 30%


def test_socio_desativado_sai_da_divisao(cadastro_limpo):
    """Desativar não apaga — mas tira da conta daqui para a frente."""
    from app.apps.painel import prestacao_dados
    prestacao_dados.salvar_socio("ANA", "Interno")
    prestacao_dados.salvar_socio("SAIU", "Interno")
    socios = {s["nome"]: s["id"] for s in prestacao_dados.socios()}
    prestacao_dados.salvar_participacao("ALFA", socios["ANA"], 50)
    prestacao_dados.salvar_participacao("ALFA", socios["SAIU"], 50)
    assert len(prestacao_dados.participacoes()) == 2

    prestacao_dados.desativar_socio(socios["SAIU"])
    assert len(prestacao_dados.participacoes()) == 1
    assert len(prestacao_dados.socios()) == 2               # o cadastro fica


# ---------------------------------------------------------------------------
# A importação do arquivo do computador
# ---------------------------------------------------------------------------
def test_importar_a_configuracao_do_arquivo_local(cadastro_limpo, tmp_path):
    """O caminho que traz sócios, percentuais, regras e ajustes do
    `prestacao_contas.db` que rodava no PC. É a única configuração do painel
    que ninguém consegue refazer — e por isso não foi para o Git."""
    import sqlite3
    from app.apps.painel import prestacao_dados
    from app.apps.painel.db import conexao

    arquivo = tmp_path / "prestacao_contas.db"
    origem = sqlite3.connect(str(arquivo))
    origem.executescript("""
        CREATE TABLE socios (id INTEGER PRIMARY KEY, nome TEXT, tipo TEXT, ativo INTEGER);
        CREATE TABLE participacoes (id INTEGER PRIMARY KEY, projeto TEXT,
                                    socio_id INTEGER, pct REAL);
        CREATE TABLE regras (id INTEGER PRIMARY KEY, nome TEXT, depto TEXT,
            todas INTEGER, grupos TEXT, categorias TEXT, pct REAL, escopo TEXT,
            mes_ini TEXT, mes_fim TEXT, ativo INTEGER);
        CREATE TABLE ajustes (id INTEGER PRIMARY KEY, socio_id INTEGER, projeto TEXT,
            data TEXT, tipo TEXT, valor REAL, descricao TEXT);
        CREATE TABLE config (chave TEXT PRIMARY KEY, valor TEXT);
        INSERT INTO socios VALUES (1,'CARLA','Interno',1), (2,'PARCEIRO X','Externo',1);
        INSERT INTO participacoes VALUES (1,'ALFA',1,60), (2,'ALFA',2,40);
        INSERT INTO regras VALUES (1,'tudo da matriz','ADM',1,'[]','[]',100,
                                   'AMBAS','','',1);
        INSERT INTO ajustes VALUES (1,1,'ALFA','2025-05-01','Valor Percebido (-)',
                                    1000.0,'retirada');
        INSERT INTO config VALUES ('taxa_adm_pct','2.5');
    """)
    origem.commit()
    origem.close()

    contagem = prestacao_dados.importar_do_arquivo_local(str(arquivo))
    assert contagem == {"socios": 2, "participacoes": 2, "regras": 1,
                        "ajustes": 1, "config": 1}
    assert {s["nome"] for s in prestacao_dados.socios()} == {"CARLA", "PARCEIRO X"}
    assert prestacao_dados.config()["taxa_adm_pct"] == "2.5"

    # Importar de novo não pode duplicar. O dono vai clicar duas vezes: alguém
    # sempre clica duas vezes.
    prestacao_dados.importar_do_arquivo_local(str(arquivo))
    assert len(prestacao_dados.socios()) == 2
    assert len(prestacao_dados.participacoes()) == 2
    assert len(prestacao_dados.ajustes()) == 1

    with conexao() as conn:
        for tabela in ("ajustes", "regras"):
            conn.execute(f"DELETE FROM {tabela}")
        conn.execute("UPDATE config SET valor = '1.5' WHERE chave = 'taxa_adm_pct'")
        conn.commit()


# ---------------------------------------------------------------------------
# Cenários de rateio, de ponta a ponta
# ---------------------------------------------------------------------------
# O cenário base de `test_painel_banco.py` não tem departamento administrativo,
# então não haveria o que ratear e a comparação sairia vazia sem provar nada.
# Aqui a base é montada de propósito: duas obras da matriz, uma da filial,
# pessoal em cada uma e despesa administrativa dos dois lados.
DESP, REC = "2. Contas a Pagar", "1. Contas a Receber"


def _linha_de_fato(**mudancas):
    base = dict(codigo_lancamento=0, tipo=DESP, analise="DRE", situacao="Pago",
                situacao_vencimento="Quitado", categoria="Serviços",
                grupo="Obra", projeto="BWSCE", departamento="CASA",
                razao_social="F", pago_recebido=0.0, a_pagar_receber=0.0,
                juros=0.0, multa=0.0, data="2025-07-10", ano=2025, mes=7)
    base.update(mudancas)
    return base


# pessoal: CASA 300, PREDIO 100 (matriz) e PONTE 400 (filial)
# administrativo: matriz 2.000, filial 1.000
BASE_COM_ADMINISTRATIVO = [
    _linha_de_fato(codigo_lancamento=1, tipo=REC, departamento="CASA", pago_recebido=10000),
    _linha_de_fato(codigo_lancamento=2, departamento="CASA", pago_recebido=-4000),
    _linha_de_fato(codigo_lancamento=3, grupo="Despesas com Pessoal",
                   departamento="CASA", pago_recebido=-300),
    _linha_de_fato(codigo_lancamento=4, tipo=REC, departamento="PREDIO", pago_recebido=6000),
    _linha_de_fato(codigo_lancamento=5, grupo="Despesas com Pessoal",
                   departamento="PREDIO", pago_recebido=-100),
    _linha_de_fato(codigo_lancamento=6, tipo=REC, projeto="BWSNE",
                   departamento="PONTE", pago_recebido=8000),
    _linha_de_fato(codigo_lancamento=7, projeto="BWSNE", grupo="Despesas com Pessoal",
                   departamento="PONTE", pago_recebido=-400),
    _linha_de_fato(codigo_lancamento=8, departamento="BWS Construções",
                   grupo="Despesas Administrativas", categoria="Aluguéis",
                   pago_recebido=-2000),
    _linha_de_fato(codigo_lancamento=9, projeto="BWSNE", departamento="BWSNE",
                   grupo="Despesas Administrativas", categoria="Aluguéis",
                   pago_recebido=-1000),
]


@pytest.fixture()
def base_para_cenario(painel_no_banco):
    """Substitui o fato pela base com administrativo, e devolve o id da regra."""
    from app.apps.painel import consultas, prestacao_dados
    from app.apps.painel.db import conexao

    colunas = list(BASE_COM_ADMINISTRATIVO[0].keys())
    marcas = ",".join(["?"] * len(colunas))
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({marcas})",
            [tuple(l[c] for c in colunas) for l in BASE_COM_ADMINISTRATIVO])
        conn.execute("DELETE FROM regras")
        conn.execute(
            "INSERT INTO regras (nome, depto, todas, grupos, categorias, pct,"
            " escopo, mes_ini, mes_fim, ativo) VALUES"
            " ('Administrativo matriz','BWS Construções',1,'[]','[]',100,"
            "  'AMBAS','','',1)")
        conn.commit()
    consultas.esquecer_listas()
    yield prestacao_dados.regras()[0]["id"]
    with conexao() as conn:
        conn.execute("DELETE FROM regras")
        conn.commit()


def _apuracao(regras):
    """Roda a conta que a tela de cenários roda, com as regras dadas."""
    from app.apps.painel import consultas, prestacao, prestacao_dados

    config = prestacao_dados.config()
    apuracao = consultas.apuracao_por_obra_mes("comprometido")
    obras = prestacao.classificar_obras(apuracao, config)
    rateio = prestacao.calcular_rateio(
        consultas.despesa_administrativa(
            [config["depto_admin_matriz"], config["depto_admin_filial"]]),
        consultas.custo_de_pessoal_por_obra_mes(config["grupo_pessoal"]),
        obras, regras, config)
    return prestacao.apurar(apuracao, obras, rateio["alocacoes"])


def test_estreitar_o_escopo_da_regra_muda_quem_paga_a_estrutura(base_para_cenario):
    """A conta conferida na mão, que é o ponto do teste.

    GRAVADO (escopo AMBAS): os 2.000 da matriz se dividem pelo pessoal das TRÊS
    obras (300/100/400) — CASA −750, PREDIO −250, PONTE −1.000; e os 1.000 da
    filial, que nenhuma regra pega, caem no resíduo do lado da filial: PONTE
    −1.000. Total em PONTE: −2.000.

    CENÁRIO (escopo MATRIZ): os mesmos 2.000 se dividem só entre as obras da
    matriz (300/100) — CASA −1.500, PREDIO −500 — e PONTE fica só com o
    resíduo da filial, −1.000.

    Então CASA e PREDIO pioram e PONTE melhora. É exatamente a pergunta que a
    tela existe para responder antes de gravar."""
    from app.apps.painel import prestacao, prestacao_dados

    gravadas = prestacao_dados.regras()
    cenario = prestacao.regras_do_cenario(
        gravadas, {str(base_para_cenario): {"escopo": "MATRIZ"}})

    comparacao = prestacao.comparar_por_obra(_apuracao(gravadas),
                                             _apuracao(cenario))
    por_obra = {l["obra"]: l for l in comparacao}
    assert set(por_obra) == {"CASA", "PREDIO", "PONTE"}

    assert reais(por_obra["CASA"]["rateio_oficial"]) == -750.00
    assert reais(por_obra["CASA"]["rateio_cenario"]) == -1500.00
    assert reais(por_obra["CASA"]["delta_resultado"]) == -750.00

    assert reais(por_obra["PREDIO"]["delta_resultado"]) == -250.00

    assert reais(por_obra["PONTE"]["rateio_oficial"]) == -2000.00
    assert reais(por_obra["PONTE"]["rateio_cenario"]) == -1000.00
    assert reais(por_obra["PONTE"]["delta_resultado"]) == 1000.00

    # pior primeiro: quem passa a receber mais custo aparece no topo
    assert comparacao[0]["obra"] == "CASA"


def test_o_cenario_nao_faz_custo_sumir(base_para_cenario):
    """A invariante que mais importa: rateio move custo, não cria nem apaga.
    Um cenário em que todas as obras melhoram seria dinheiro evaporando."""
    from app.apps.painel import prestacao, prestacao_dados

    gravadas = prestacao_dados.regras()
    for mudanca in ({"escopo": "MATRIZ"}, {"pct": "40"}, {"ativo": 0}):
        cenario = prestacao.regras_do_cenario(
            gravadas, {str(base_para_cenario): mudanca})
        comparacao = prestacao.comparar_por_obra(_apuracao(gravadas),
                                                 _apuracao(cenario))
        movido = sum(l["delta_rateio"] for l in comparacao)
        assert abs(movido) < 0.01, f"{mudanca} fez custo aparecer ou sumir: {movido}"


def test_a_tela_de_cenarios_desenha_a_comparacao_com_banco_de_verdade(
        base_para_cenario, monkeypatch):
    """A tela inteira, com o banco por trás: tabela, gráfico e botão de gravar.
    É o teste que pega um erro de SQL — que este módulo já mandou três vezes
    para a produção."""
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "segredo-de-teste"})

    rid = base_para_cenario
    html = cliente.get(
        f"/painel/prestacao/cenarios?viu_{rid}=1&ativo_{rid}=on"
        f"&pct_{rid}=100&escopo_{rid}=MATRIZ").get_data(as_text=True)

    assert "Δ Resultado" in html, "a tabela de comparação apareceu"
    assert "CASA" in html and "PONTE" in html
    # o gráfico colore pelo sinal: há obra que piora e obra que melhora
    assert 'class="b-despesa"' in html and 'class="b-receita"' in html
    assert "Gravar cenário como oficial" in html
    # e nada foi gravado só por abrir a tela
    from app.apps.painel import prestacao_dados
    assert prestacao_dados.regras()[0]["escopo"] == "AMBAS"


# ---------------------------------------------------------------------------
# A Visão do Analítico tem de FILTRAR
# ---------------------------------------------------------------------------
# Defeito achado pelo dono em 04/09/2026: escolher "Só a pagar" e clicar em
# Aplicar não mudava nada. A visão só decidia por qual coluna ordenar; as contas
# já quitadas continuavam na lista, mostrando zero na coluna "A pagar".
#
# Precisa de banco de verdade: a diferença está no WHERE, e o dublê de sessão
# ignora WHERE — ele devolveria as mesmas linhas nos três casos e o teste
# passaria sem provar nada.
@pytest.fixture()
def base_paga_e_em_aberto(painel_no_banco):
    """Três despesas: uma quitada, uma inteiramente em aberto e uma em que só
    os juros foram pagos."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao

    linhas = [
        _linha_de_fato(codigo_lancamento=901, razao_social="QUITADA",
                       situacao="Pago", pago_recebido=-1000, a_pagar_receber=0),
        _linha_de_fato(codigo_lancamento=902, razao_social="EM ABERTO",
                       situacao="A pagar", pago_recebido=0, a_pagar_receber=-700),
        _linha_de_fato(codigo_lancamento=903, razao_social="SO OS JUROS",
                       situacao="Pago", pago_recebido=0, a_pagar_receber=0,
                       juros=-30),
    ]
    colunas = list(linhas[0].keys())
    marcas = ",".join(["?"] * len(colunas))
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({marcas})",
            [tuple(l[c] for c in colunas) for l in linhas])
        conn.commit()
    consultas.esquecer_listas()
    yield


def _credores(visao):
    from app.apps.painel import consultas
    dados = consultas.analitico_despesas(consultas.Filtros(), visao=visao)
    return {l["credor"] for l in dados["linhas"]}, dados["quantos"]


def test_so_a_pagar_tira_o_que_ja_foi_quitado(base_paga_e_em_aberto):
    credores, quantos = _credores("aberto")
    assert credores == {"EM ABERTO"}
    assert quantos == 1, "o contador da tela conta o mesmo que a lista mostra"


def test_so_pagas_tira_o_que_ainda_falta_pagar(base_paga_e_em_aberto):
    credores, quantos = _credores("executado")
    assert "EM ABERTO" not in credores
    assert "QUITADA" in credores
    assert "SO OS JUROS" in credores, (
        "juros pagos são dinheiro que saiu: a linha conta como paga")
    assert quantos == 2


def test_comprometido_continua_mostrando_tudo(base_paga_e_em_aberto):
    credores, quantos = _credores("comprometido")
    assert credores == {"QUITADA", "EM ABERTO", "SO OS JUROS"}
    assert quantos == 3


def test_o_contador_da_tela_bate_com_o_que_o_arquivo_leva(base_paga_e_em_aberto):
    """O defeito irmão, também achado pelo dono: a tela dizia 481 lançamentos e
    o arquivo trazia 316. O número do topo e as linhas têm de vir da MESMA
    seleção, em qualquer visão."""
    from app.apps.painel import consultas

    for visao in ("comprometido", "aberto", "executado"):
        dados = consultas.analitico_despesas(
            consultas.Filtros(), visao=visao, por_pagina=20000)
        assert dados["quantos"] == len(dados["linhas"]), visao


def test_a_medicao_nao_repete_o_numero_do_documento(painel_no_banco):
    """Defeito achado pelo dono: a coluna Documento aparecia com o dado
    duplicado — o número em cima e o mesmo número embaixo, como "medição".

    Causa: quando a observação não traz medição nenhuma, a chave de agrupamento
    cai no PRÓPRIO documento (`DOC:<numero>`), e o rótulo dela vira o número.
    Uma despesa comum não é medição, e a coluna passou a dizer isso ficando
    vazia."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao

    linhas = [
        _linha_de_fato(codigo_lancamento=910, razao_social="SEM MEDICAO",
                       numero_documento="NF 1234", medicao_rotulo="NF 1234",
                       pago_recebido=-100),
        _linha_de_fato(codigo_lancamento=911, razao_social="COM MEDICAO",
                       numero_documento="NF 5678",
                       medicao_rotulo="OBRA X | Medição 3", pago_recebido=-200),
    ]
    colunas = list(linhas[0].keys())
    marcas = ",".join(["?"] * len(colunas))
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({marcas})",
            [tuple(l[c] for c in colunas) for l in linhas])
        conn.commit()
    consultas.esquecer_listas()

    por_credor = {l["credor"]: l for l in
                  consultas.analitico_despesas(consultas.Filtros())["linhas"]}

    assert por_credor["SEM MEDICAO"]["documento"] == "NF 1234"
    assert por_credor["SEM MEDICAO"]["medicao"] == "", (
        "medição igual ao documento é eco, não informação")
    # e a medição de verdade continua aparecendo
    assert por_credor["COM MEDICAO"]["medicao"] == "OBRA X | Medição 3"


# ---------------------------------------------------------------------------
# As telas têm de concordar sobre quanto foi gasto
# ---------------------------------------------------------------------------
# Defeito achado pelo dono em 08/09/2026: numa obra, a Visão Geral mostrava
# resultado de R$ 931.718,04 e o DRE, R$ 888.419,91. A diferença era exatamente
# R$ 43.298,13 — os juros e multas pagos, que só o DRE e o Analítico somavam.
#
# Não era defeito da conversão: o Streamlit original fazia igual. O dono decidiu
# que juros e multa são despesa em TODO o painel.
#
# Estes testes vigiam a CLASSE do problema: qualquer tela que volte a somar só o
# principal passa a discordar do DRE, e aqui isso quebra.
BASE_COM_JUROS = [
    # receita de 10.000
    _linha_de_fato(codigo_lancamento=801, tipo=REC, razao_social="CLIENTE",
                   pago_recebido=10000, a_pagar_receber=0),
    # despesa de 4.000 de principal MAIS 300 de juros e 200 de multa
    _linha_de_fato(codigo_lancamento=802, razao_social="FORNECEDOR",
                   pago_recebido=-4000, a_pagar_receber=0,
                   juros=-300, multa=-200),
]


@pytest.fixture()
def base_com_juros(painel_no_banco):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao

    colunas = list(BASE_COM_JUROS[0].keys())
    marcas = ",".join(["?"] * len(colunas))
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({marcas})",
            [tuple(l[c] for c in colunas) for l in BASE_COM_JUROS])
        conn.commit()
    consultas.esquecer_listas()
    yield


# 10.000 de receita − (4.000 + 300 + 200) = 5.500
RESULTADO_CERTO = 5500.00
DESPESA_CERTA = -4500.00


def test_a_visao_geral_conta_os_juros_como_despesa(base_com_juros):
    """O caso exato que o dono reportou."""
    from app.apps.painel import consultas
    dre = consultas.resultado_dre(consultas.Filtros())
    assert reais(dre["despesa"]) == DESPESA_CERTA
    assert reais(dre["resultado"]) == RESULTADO_CERTO
    assert reais(dre["resultado_exec"]) == RESULTADO_CERTO


def test_a_visao_geral_e_o_dre_dao_o_mesmo_resultado(base_com_juros):
    """A invariante que o dono cobrou: as duas telas olham a mesma base e não
    podem responder coisas diferentes."""
    from app.apps.painel import consultas
    f = consultas.Filtros()
    visao = consultas.resultado_dre(f)
    linhas = {l["linha"]: l for l in consultas.dre_linhas(f)["linhas"]}
    resultado_do_dre = linhas["= RESULTADO"]["comprometido"]
    assert reais(visao["resultado"]) == reais(resultado_do_dre)


def test_o_grafico_da_visao_geral_bate_com_os_seus_proprios_numeros(base_com_juros):
    """O gráfico não pode contar uma história diferente do cartão em cima dele."""
    from app.apps.painel import consultas
    f = consultas.Filtros()
    anos = consultas.dre_por_ano(f)
    assert reais(sum(a["despesa"] for a in anos)) == DESPESA_CERTA
    assert reais(sum(a["receita"] + a["despesa"] for a in anos)) == RESULTADO_CERTO


def test_o_resultado_por_obra_conta_os_juros(base_com_juros):
    """"Não pode dar uma visão de resultado de obras sem essa informação." """
    from app.apps.painel import consultas
    linhas = consultas.resultado_por(consultas.Filtros(), nivel="obra")
    assert reais(sum(l["resultado"] for l in linhas)) == RESULTADO_CERTO


def test_o_comprometido_x_executado_conta_os_juros(base_com_juros):
    from app.apps.painel import consultas
    linhas = consultas.comprometido_vs_executado(consultas.Filtros(), tipo="pagar")
    assert reais(sum(l["executado"] for l in linhas)) == DESPESA_CERTA


def test_o_top_credores_mostra_o_que_foi_pago_de_fato(base_com_juros):
    """Um ranking que subconta o que se pagou ao fornecedor engana quem lê."""
    from app.apps.painel import consultas
    credores = {c["nome"]: c for c in consultas.top_credores(consultas.Filtros())}
    assert reais(credores["FORNECEDOR"]["pago"]) == DESPESA_CERTA


def test_o_caixa_conta_o_juros_como_dinheiro_que_saiu(base_com_juros):
    """Juros pago sai da conta corrente como qualquer outro pagamento."""
    from app.apps.painel import consultas
    caixa = consultas.caixa(consultas.Filtros())
    assert reais(caixa["saidas"]) == DESPESA_CERTA
    assert reais(caixa["geracao"]) == RESULTADO_CERTO


def test_a_prestacao_de_contas_parte_do_resultado_certo(base_com_juros):
    """A mais sensível: é ela que decide quanto cabe a cada sócio. Com os juros
    de fora, o resultado dividido seria maior do que o que a obra deu."""
    from app.apps.painel import consultas
    apuracao = consultas.apuracao_por_obra_mes("comprometido")
    despesas = sum(l["despesas"] for l in apuracao)
    receita = sum(l["receita_liquida"] for l in apuracao)
    assert reais(despesas) == DESPESA_CERTA
    assert reais(receita + despesas) == RESULTADO_CERTO


def test_o_analitico_ja_contava_e_continua_contando(base_com_juros):
    """Ele e o DRE já estavam certos — a correção não podia contar duas vezes."""
    from app.apps.painel import consultas
    dados = consultas.analitico_despesas(consultas.Filtros())
    assert reais(dados["total"]) == DESPESA_CERTA


# ---------------------------------------------------------------------------
# Explorador de lançamentos
# ---------------------------------------------------------------------------
# Vindo do painel Streamlit. É a tela de SANEAMENTO: ela olha a base inteira,
# não só o DRE, porque o erro que se procura quase sempre é o lançamento estar
# na análise errada.
BASE_PARA_EXPLORAR = [
    _linha_de_fato(codigo_lancamento=701, analise="DRE", categoria="Serviços",
                   codigo_categoria="1.01.01", departamento="CASA",
                   razao_social="FORNECEDOR X", pago_recebido=-100),
    # aporte lançado fora do DRE — é o tipo de coisa que se procura aqui
    _linha_de_fato(codigo_lancamento=702, analise="Fluxo de Caixa",
                   categoria="Aporte de Sócio", codigo_categoria="2.02.02",
                   departamento="", projeto="", razao_social="SOCIO A",
                   pago_recebido=5000),
    # transferência entre contas: fica de fora até alguém pedir
    _linha_de_fato(codigo_lancamento=703, analise="TRF",
                   categoria="Transferência", codigo_categoria="9.99",
                   razao_social="BANCO", pago_recebido=-7000),
]


@pytest.fixture()
def base_para_explorar(painel_no_banco):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    colunas = list(BASE_PARA_EXPLORAR[0].keys())
    marcas = ",".join(["?"] * len(colunas))
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({marcas})",
            [tuple(l[c] for c in colunas) for l in BASE_PARA_EXPLORAR])
        conn.commit()
    consultas.esquecer_listas()
    yield


def _pedido_padrao(**extra):
    base = {"tipo": "", "analises": [], "grupos": [], "categorias": [],
            "obras": [], "fornecedores": [], "projetos": [], "contas": [],
            "situacoes": [], "busca": "", "com_trf": False, "de": "", "ate": ""}
    base.update(extra)
    return base


def _codigos(pedido):
    from app.apps.painel import consultas
    base = {"tipo": "", "analises": [], "grupos": [], "categorias": [],
            "obras": [], "projetos": [], "contas": [], "situacoes": [],
            "busca": "", "com_trf": False, "de": "", "ate": ""}
    base.update(pedido)
    return {l["codigo_lancamento"] for l in consultas.explorar(base)["linhas"]}


def test_o_explorador_enxerga_fora_do_dre(base_para_explorar):
    """É a diferença desta tela para todas as outras: ela vê o Fluxo de Caixa.
    Sem isso não há como achar o aporte lançado no lugar errado."""
    assert _codigos({"busca": "SOCIO"}) == {702}


def test_a_transferencia_aparece_sem_precisar_pedir(base_para_explorar):
    """A regra era o contrário até 13/09/2026, e estava errada.

    Transferência é dinheiro trocando de conta da própria empresa: nas telas de
    análise ela fica fora, senão o mesmo valor conta duas vezes. Mas ESTA tela
    existe para achar classificação errada, e "está numa categoria marcada como
    transferência no OMIE sem ser uma" é exatamente um desses erros. Esconder o
    que se procura é o oposto do trabalho.

    O dono topou com isso procurando uma devolução de aporte de 24/12/2025:
    "aqui era pra aparecer todos os lançamentos igual como aparece no relatório
    de conta corrente do OMIE"."""
    assert 703 in _codigos({"busca": "BANCO"})


def test_quem_quiser_so_a_transferencia_marca_a_analise(base_para_explorar):
    """Mostrar tudo por padrão não pode custar o corte: a lista Análise da barra
    lateral continua separando DRE, Fluxo de Caixa e TRF."""
    assert _codigos({"busca": "BANCO", "analises": ["TRF"]}) == {703}
    assert 703 not in _codigos({"analises": ["DRE"]})


def test_procurar_pelo_titulo_sem_apropriacao(base_para_explorar):
    """O caso de uso que a tela existe para resolver: achar o que ficou sem
    obra. É por isso que o rótulo tem de ser um só em todo o painel."""
    from app.apps.painel import consultas
    achados = _codigos({"obras": [consultas.SEM_OBRA], "com_trf": True})
    assert achados == {702}


def test_a_faixa_de_data_deixa_passar_quem_nao_tem_data(base_para_explorar):
    """Ao contrário do Analítico: aqui lançamento sem data é justamente um dos
    que se procura, e escondê-lo esconderia o problema."""
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET data = NULL WHERE codigo_lancamento = 701")
        conn.commit()
    assert 701 in _codigos({"de": "2025-01-01", "ate": "2025-12-31"})


def test_os_totais_separam_linha_de_titulo(base_para_explorar):
    """Um título rateado em três obras vira três linhas: dizer que são três
    títulos enganaria quem for alterar."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO fato (codigo_lancamento, tipo, analise,"
                     " situacao, departamento, pago_recebido, a_pagar_receber,"
                     " juros, multa) VALUES (701,'2. Contas a Pagar','DRE',"
                     " 'Pago','PREDIO',-50,0,0,0)")
        conn.commit()
    consultas.esquecer_listas()
    dados = consultas.explorar({"busca": "", "analises": ["DRE"], "tipo": "",
                                "grupos": [], "categorias": [], "obras": [],
                                "projetos": [], "contas": [], "situacoes": [],
                                "com_trf": False, "de": "", "ate": ""})
    assert dados["quantos"] == 2 and dados["titulos"] == 1


def test_a_tela_do_explorador_abre_e_procura(base_para_explorar, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "segredo-de-teste"})

    # sem filtro nenhum, ela não varre a base: pede um filtro
    vazia = cliente.get("/painel/explorador").get_data(as_text=True)
    assert "Escolha ao menos um filtro" in vazia

    html = cliente.get("/painel/explorador?busca=SOCIO").get_data(as_text=True)
    assert "SOCIO A" in html
    assert "Fluxo de Caixa" in html


def test_o_explorador_nao_esta_no_menu_principal(base_para_explorar, monkeypatch):
    """O dono pediu explicitamente: tela de manutenção não fica exposta ao lado
    dos relatórios. Chega-se a ela por Configurações."""
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.apps.painel.web import ABAS
    assert not any("explorador" in rota for _, _, rota in ABAS)

    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "segredo-de-teste"})
    config = cliente.get("/painel/configuracoes").get_data(as_text=True)
    assert "/painel/explorador" in config, "mas tem de dar para chegar nela"
    # o mesmo vale para o Rateio da Administracao, pelo mesmo motivo
    assert "/painel/rateio-administracao" in config


def test_o_explorador_exige_login(base_para_explorar, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    assert app.test_client().get("/painel/explorador").status_code == 302


def test_procurar_pelo_numero_do_titulo_do_omie(base_para_explorar):
    """A pergunta mais básica que se faz a esta tela — "o título 12345 está no
    painel?" — não tinha resposta até 13/09/2026: a busca só olhava fornecedor,
    documento e observação. Quem procurava um lançamento cujo fornecedor não
    está preenchido não tinha como perguntar."""
    assert _codigos({"busca": "703"}) == {703}


def test_procurar_por_numero_nao_perde_o_documento_que_e_numero(base_para_explorar):
    """Documento também costuma ser só dígitos. Somar a busca por código não
    pode custar a busca por documento."""
    from app.apps.painel.db import conexao
    from app.apps.painel import consultas
    with conexao() as conn:
        conn.execute("UPDATE fato SET numero_documento = '703' "
                     " WHERE codigo_lancamento = 701")
        conn.commit()
    consultas.esquecer_listas()
    # tem de achar os dois: o título 703 e o título 701, cujo documento é "703"
    assert _codigos({"busca": "703"}) == {701, 703}


# ===========================================================================
# Fornecedor: filtro de lista, e nunca uma linha sem nome
# ===========================================================================
# 13/09/2026. O dono quis analisar as devoluções de aporte de uma empresa,
# digitou o nome no Buscar e achou menos lançamentos do que existiam. Procurar
# empresa digitando o nome obriga a acertar a grafia do cadastro do OMIE — e o
# painel ainda deixava a linha SEM NOME quando o cadastro não estava na base,
# tornando-a invisível para qualquer busca por nome.

def test_fornecedor_e_filtro_de_lista_nao_so_texto(base_para_explorar):
    """Marcar na lista não depende de acertar a grafia do cadastro do OMIE."""
    from app.apps.painel import consultas
    r = consultas.fornecedores_do_recorte(consultas.explorar(_pedido_padrao()))
    assert r["itens"], "a barra lateral tem de oferecer os nomes para marcar"


def test_filtrar_por_fornecedor_traz_so_os_dele(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET razao_social = 'CONSTRUTORA X LTDA'"
                     " WHERE codigo_lancamento = 701")
        conn.commit()
    consultas.esquecer_listas()
    assert _codigos({"fornecedores": ["CONSTRUTORA X LTDA"]}) == {701}


def test_quem_esta_sem_fornecedor_pode_ser_achado(base_para_explorar):
    """Antes, linha sem nome não tinha como ser encontrada — nem por busca nem
    por filtro. Agora ela tem um rótulo próprio, como o "(não apropriado)" da
    obra, e aparece na lista para ser marcada."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET razao_social = '' WHERE codigo_lancamento = 702")
        conn.commit()
    consultas.esquecer_listas()
    r = consultas.fornecedores_do_recorte(consultas.explorar(_pedido_padrao()))
    assert consultas.SEM_FORNECEDOR in r["itens"]
    assert _codigos({"fornecedores": [consultas.SEM_FORNECEDOR]}) == {702}


# ===========================================================================
# A conferência das duas regras de "foi pago"
# ===========================================================================
# 13/09/2026, investigando valores errados no bloco de Aportes do DRE: a CARGA
# considera pago o título cujo status diga pago/recebido/conciliado OU cuja
# baixa do OMIE diga liquidado. As TELAS só olham o texto do status. Um título
# liquidado com outra palavra tem valor gravado como realizado e não é contado
# por nenhuma tela — o dinheiro existe na base e não aparece em lugar nenhum.
#
# Esta conferência MEDE o estrago, sem corrigir: o dono decide com o número.

@pytest.fixture()
def base_com_pago_invisivel(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        # a carga deu por QUITADO (a baixa do OMIE disse liquidado), mas o texto
        # do status usa outra palavra — e e so o texto que as telas olham
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, departamento, razao_social,"
            " data, pago_recebido, a_pagar_receber, juros, multa)"
            " VALUES (901,'2. Contas a Pagar','DRE','Baixado','Quitado',"
            "         'Devolução de Aportes','CASA','SOCIO','2025-12-24',"
            "         -320000,0,0,0)")
        conn.commit()
    consultas.esquecer_listas()
    yield


def test_o_titulo_baixado_com_outra_palavra_agora_e_contado(base_com_pago_invisivel):
    """O caso que escondia R$ 96.750,00 na base do dono.

    Este título foi baixado no OMIE — a carga o deu por quitado —, mas o texto
    do status é "Baixado", que não é nenhuma das palavras que as telas olhavam.
    Até a migração 011 ele existia na base e não aparecia em tela nenhuma.

    Agora "foi pago?" tem UMA resposta só: a que a carga gravou. Então este
    título conta, e a conferência das duas regras não acha mais nada — ela
    continua existindo como guarda: se um dia voltar a acusar alguma coisa, é
    porque alguém criou uma segunda regra de novo."""
    from app.apps.painel import consultas
    r = consultas.conferencia_do_pago()
    assert r["titulos"] == 0 and r["valor"] == 0, \
        f"as duas regras divergiram de novo: {r['situacoes']}"
    devolvido = consultas.aportes(consultas.Filtros())["devolvido"]
    assert devolvido == pytest.approx(320000), \
        "e o dinheiro dele tem de aparecer no bloco, que é onde sumia"


def test_a_conferencia_nao_altera_nada(base_com_pago_invisivel):
    """Ela mede. Se corrigisse por conta própria, os números do DRE mudariam
    sem ninguém decidir — e o dono pediu para medir antes."""
    from app.apps.painel import consultas
    from app.apps.painel.db import consultar
    antes = consultar("SELECT situacao, situacao_vencimento, pago_recebido"
                      "  FROM fato WHERE codigo_lancamento = 901")
    consultas.conferencia_do_pago()
    depois = consultar("SELECT situacao, situacao_vencimento, pago_recebido"
                       "  FROM fato WHERE codigo_lancamento = 901")
    assert antes == depois


def test_quando_as_duas_regras_concordam_a_conferencia_fica_limpa(base_para_explorar):
    """Sem o caso ruim na base, ela não pode inventar alarme."""
    from app.apps.painel import consultas
    assert consultas.conferencia_do_pago()["titulos"] == 0


def test_a_lista_de_fornecedores_nao_custa_consulta_nem_pagina(base_para_explorar):
    """13/09/2026: a primeira versão deste filtro desenhava TODOS os
    fornecedores da base. Com milhares deles a página foi a 1,16 MB — 86% do
    peso dela — e o dono sentiu na hora: "o painel tá super lento agora".

    A lista sai das linhas já buscadas: nenhuma consulta a mais, e ela oferece
    exatamente o que está à vista."""
    from app.apps.painel import consultas

    dados = consultas.explorar(_pedido_padrao())
    r = consultas.fornecedores_do_recorte(dados)
    nomes_na_tela = {(l["razao_social"] or "").strip() or consultas.SEM_FORNECEDOR
                     for l in dados["linhas"]}
    assert set(r["itens"]) == nomes_na_tela
    assert len(r["itens"]) <= consultas.TETO_DE_FORNECEDORES


def test_sem_recorte_a_lista_de_fornecedores_nem_e_desenhada(base_para_explorar):
    """Sem filtro a tela não lista lançamento nenhum. Desenhar milhares de
    nomes ali seria peso pago à toa."""
    from app.apps.painel import consultas
    assert consultas.fornecedores_do_recorte(None)["sem_recorte"] is True
    assert consultas.fornecedores_do_recorte({"linhas": []})["itens"] == []


def test_a_lista_tem_teto_de_desenho(base_para_explorar):
    """O teto não é de busca, é de HTML: cada nome vira uma caixa de marcar no
    navegador."""
    from app.apps.painel import consultas
    falso = {"linhas": [{"razao_social": f"FORNECEDOR {i:05d}"} for i in range(5000)],
             "cortou": False}
    r = consultas.fornecedores_do_recorte(falso)
    assert len(r["itens"]) == consultas.TETO_DE_FORNECEDORES
    assert r["cortou"] is True, "e tem de AVISAR que cortou, senão some nome em silêncio"


# ===========================================================================
# Procurar por VALOR
# ===========================================================================
# 13/09/2026. O dono passou a tarde conferindo o painel contra a tela do OMIE
# lado a lado, com os valores na mão — e era justamente o único jeito de
# perguntar "este lançamento está aqui?" que a tela não aceitava. Sobrava
# procurar por nome (que falha quando o nome está vazio) ou pelo número do
# título (que ele não tem à mão no OMIE).

def test_procurar_pelo_valor_do_lancamento(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET pago_recebido = -784647.07"
                     "  WHERE codigo_lancamento = 701")
        conn.commit()
    consultas.esquecer_listas()
    # os formatos que se copia da tela do OMIE têm de achar o mesmo lançamento
    for digitado in ("784.647,07", "784647,07", "784647.07", "R$ 784.647,07"):
        assert _codigos({"busca": digitado}) == {701}, digitado


def test_o_valor_acha_tanto_o_pago_quanto_o_em_aberto(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET pago_recebido = 0, a_pagar_receber = -400000"
                     "  WHERE codigo_lancamento = 702")
        conn.commit()
    consultas.esquecer_listas()
    assert 702 in _codigos({"busca": "400.000,00"})


def test_texto_com_letra_nao_vira_busca_por_valor(base_para_explorar):
    """Procurar "NF 100" não pode virar uma busca por cem reais."""
    from app.apps.painel import consultas
    assert consultas._valor_procurado("NF 100") is None
    assert consultas._valor_procurado("CONSTRUTORA") is None


def test_o_valor_acha_o_titulo_rateado_entre_obras(base_para_explorar):
    """O painel quebra o título por obra: um título rateado entre três obras
    vira três linhas, cada uma com uma FRAÇÃO do valor. Nenhuma delas tem o
    número que está na tela do OMIE.

    Foi assim que três devoluções de aporte pareceram sumidas em 13/09/2026 —
    estavam na base o tempo todo, partidas entre obras. Procurar pelo valor
    cheio não achava nada, e passar o olho na lista também não."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM fato WHERE codigo_lancamento = 801")
        for obra, parte in (("CASA", -300000.00), ("PREDIO", -250000.00),
                            ("PONTE", -234647.07)):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " categoria, departamento, razao_social, data, pago_recebido,"
                " a_pagar_receber, juros, multa)"
                " VALUES (801,'2. Contas a Pagar','Fluxo de Caixa','PAGO',"
                "         'Devolução de Aportes',?,'CONSTRUTORA','2025-12-24',"
                "         ?,0,0,0)", (obra, parte))
        conn.commit()
    consultas.esquecer_listas()
    # 300.000 + 250.000 + 234.647,07 = 784.647,07 — o valor que está no OMIE
    assert 801 in _codigos({"busca": "784.647,07"}), \
        "procurar pelo valor do OMIE tem de achar o título, mesmo partido entre obras"
    # e as três linhas dele vêm juntas: é um título só
    assert len([l for l in consultas.explorar(
        _pedido_padrao(busca="784.647,07"))["linhas"]
        if l["codigo_lancamento"] == 801]) == 3


# ===========================================================================
# "Onde foi parar este número?" — a base crua, antes de virar linha
# ===========================================================================
# 13/09/2026: uma tarde inteira de hipóteses minhas derrubadas uma a uma pelo
# dono, sobre um lançamento que existia no OMIE e não aparecia em tela nenhuma.
# Se isso acontece, só há dois caminhos — a carga nunca baixou, ou baixou e
# descartou ao montar as linhas. Perguntar ao banco é mais barato que adivinhar.

def test_diz_quando_o_titulo_foi_baixado_e_descartado(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM titulos WHERE codigo_lancamento_omie = 9001")
        conn.execute(
            "INSERT INTO titulos (codigo_lancamento_omie, natureza,"
            " valor_documento, status_titulo, numero_documento)"
            " VALUES (9001,'P',784647.07,'CANCELADO','NF X')")
        conn.commit()
    r = consultas.titulos_que_sumiram(784647.07)
    assert r["achados"], "tem de achar o título na base crua"
    achado = r["achados"][0]
    assert achado["codigo"] == 9001
    assert achado["nas_telas"] is False, "e dizer que ele NÃO virou linha"
    assert achado["situacao"] == "CANCELADO", "e por quê"


def test_o_centavo_nao_se_perde_mais_ao_gravar(base_para_explorar):
    """O erro que custou uma semana: R$ 784.647,07 virava R$ 784.647,06.

    As colunas de dinheiro do espelho nasceram REAL (float de 4 bytes), que
    guarda ~7 algarismos significativos. Acima de R$ 131.072,00 o centavo NÃO
    CABE. A migração 010 passou tudo para NUMERIC, que guarda o decimal como
    ele é escrito. Sem este teste, trocar o tipo de volta passaria despercebido
    — e a busca por valor exato voltaria a não achar nada grande."""
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM titulos WHERE codigo_lancamento_omie = 9002")
        conn.execute(
            "INSERT INTO titulos (codigo_lancamento_omie, natureza,"
            " valor_documento, status_titulo) VALUES (9002,'P',784647.07,'PAGO')")
        conn.commit()
        (gravado,) = conn.execute(
            "SELECT valor_documento FROM titulos"
            " WHERE codigo_lancamento_omie = 9002").fetchone()
    assert float(gravado) == 784647.07, \
        f"o banco devolveu {gravado} — o centavo se perdeu ao gravar"


def test_dinheiro_e_guardado_em_numeric_nao_em_float(base_para_explorar):
    """A regra por trás do teste acima, dita direto ao banco.

    Vale para toda coluna de dinheiro do espelho, não só a que o dono procurou:
    se uma voltar a ser `real`, é centavo perdido esperando para acontecer."""
    from app.apps.painel.db import conexao
    esperado = {
        "titulos": ["valor_documento", "valor_ir", "valor_iss", "valor_inss",
                    "valor_pis", "valor_cofins", "valor_csll"],
        "movimentos": ["nvalortitulo", "nvalpago", "nvalliquido", "nvalaberto",
                       "njuros", "nmulta", "ndesconto"],
        "rateio": ["nvaldep"],
        "ajustes": ["valor"],
    }
    with conexao() as conn:
        for tabela, colunas in esperado.items():
            tipos = dict(conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns"
                " WHERE table_schema = 'painel' AND table_name = ?",
                (tabela,)).fetchall())
            for coluna in colunas:
                assert tipos.get(coluna) == "numeric", \
                    f"painel.{tabela}.{coluna} está como {tipos.get(coluna)}"


def test_a_tela_avisa_que_falta_rebaixar_para_os_centavos_voltarem(cliente_config):
    """A migração 010 arruma o TIPO da coluna, não o valor já gravado.

    Sem este aviso, ela pareceria ter resolvido e os números continuariam
    errados em silêncio — que é pior do que não ter consertado. O aviso some
    sozinho quando uma carga inicial terminar bem depois da migração."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM execucoes WHERE tipo = 'carga_inicial'")
        conn.commit()
    assert consultas.recarga_total_pendente()["pendente"] is True
    html = cliente_config.get("/painel/configuracoes").get_data(as_text=True)
    assert "centavo errado" in html
    assert "Primeira carga" in html, \
        "e o botão que resolve tem de estar na mesma tela, senão o aviso é inútil"

    # e some sozinho quando a carga inicial acontecer
    with conexao() as conn:
        conn.execute(
            "INSERT INTO execucoes (tipo, disparo, inicio, fim, ok)"
            " VALUES ('carga_inicial','manual', now(), now(), true)")
        conn.commit()
    assert consultas.recarga_total_pendente()["pendente"] is False
    with conexao() as conn:
        conn.execute("DELETE FROM execucoes WHERE tipo = 'carga_inicial'")
        conn.commit()


# ===========================================================================
# O dinheiro que andou na conta e não está em tela nenhuma — 20/09/2026
# ===========================================================================
# O dono: "esse movimento sem titulo, eu não sei exatamente quem sao e de qual
# forma afeta. como saber?" — e não dava para saber, porque a carga descartava
# esses movimentos antes de gravar, contando quantos foram só no log.

@pytest.fixture()
def base_com_movimento_sem_titulo(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM movimentos_sem_titulo")
        conn.execute(
            "INSERT INTO movimentos_sem_titulo (cnatureza, ccodcateg, ncodcc,"
            " ddtpagamento, cstatus, nvalpago)"
            " VALUES ('P','1.01.02', 7011, '24/12/2025','LIQUIDADO', 50000),"
            "        ('R','1.01.02', 7011, '10/01/2026','LIQUIDADO', 12000)")
        conn.commit()
    consultas.esquecer_listas()
    yield
    with conexao() as conn:
        conn.execute("DELETE FROM movimentos_sem_titulo")
        conn.commit()


def test_a_tela_diz_quanto_dinheiro_andou_fora_do_painel(base_com_movimento_sem_titulo):
    from app.apps.painel import consultas
    r = consultas.movimentos_fora_do_painel()
    assert r["linhas"] == 2
    assert reais(r["valor"]) == 62000.00
    assert reais(r["entrou"]) == 12000.00, "entrada e saída separadas"
    assert reais(r["saiu"]) == 50000.00
    assert [l["ano"] for l in r["por_ano"]] == ["2026", "2025"]
    assert r["maiores"][0]["valor"] == 50000.00, "do maior para o menor"
    assert r["maiores"][0]["natureza"] == "Saída"


def test_esse_dinheiro_nao_entra_em_numero_de_tela_nenhum(base_com_movimento_sem_titulo):
    """A tabela existe para ser OLHADA, não para somar. Se um dia ela começar a
    entrar no DRE sem ninguém decidir, os números mudam sozinhos — que é
    exatamente o que o dono não pode ter."""
    from app.apps.painel import consultas
    from app.apps.painel.db import consultar
    (linhas_fato,) = consultar("SELECT COUNT(*) FROM fato")[0]
    consultas.movimentos_fora_do_painel()
    assert consultar("SELECT COUNT(*) FROM fato")[0][0] == linhas_fato
    dre = consultas.aportes(consultas.Filtros())
    assert dre["aportado"] >= 0  # só para exercitar; o que importa é o de baixo
    (no_fato,) = consultar(
        "SELECT COUNT(*) FROM fato WHERE ABS(pago_recebido) = 50000")[0]
    assert no_fato == 0, "o movimento sem título não pode ter virado linha"


def test_a_carga_guarda_o_movimento_sem_titulo_em_vez_de_jogar_fora():
    """Antes ele era contado e descartado na mesma linha de código."""
    import inspect

    from app.apps.painel.sync import espelho
    fonte = inspect.getsource(espelho.gravar_movimentos)
    assert "movimentos_sem_titulo" in fonte, \
        "voltou a descartar o movimento sem título — e aí não dá nem para contar"


def test_diz_quando_a_carga_nunca_trouxe(base_para_explorar):
    """A outra metade da resposta, e a mais importante: se nem na base crua
    está, o problema é a carga, não a montagem das linhas."""
    from app.apps.painel import consultas
    assert consultas.titulos_que_sumiram(99999999.99)["achados"] == []


def test_titulo_que_virou_linha_aparece_como_presente(base_para_explorar):
    """Sem isso a conferência acusaria todo mundo e não serviria para nada."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM titulos WHERE codigo_lancamento_omie = 701")
        conn.execute(
            "INSERT INTO titulos (codigo_lancamento_omie, natureza,"
            " valor_documento, status_titulo) VALUES (701,'P',12345.67,'PAGO')")
        conn.commit()
    achados = consultas.titulos_que_sumiram(12345.67)["achados"]
    assert achados and achados[0]["nas_telas"] is True


def test_a_busca_por_valor_perdoa_o_centavo_que_o_omie_perdeu(base_para_explorar):
    """O caso que custou a tarde de 13/09/2026.

    O valor chega do OMIE numa coluna de ponto flutuante de 4 bytes, que acima
    de uns R$ 131 mil não guarda centavo: os R$ 784.647,07 da tela do OMIE ficam
    gravados como 784.647,06. Comparar exato errava por um centavo e dizia que o
    lançamento não existia — quando ele estava lá o tempo todo."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET pago_recebido = -784647.06"
                     "  WHERE codigo_lancamento = 701")
        conn.commit()
    consultas.esquecer_listas()
    # o que a pessoa digita é o que ela LÊ no OMIE, com o centavo certo
    assert _codigos({"busca": "784.647,07"}) == {701}


def test_a_folga_nao_confunde_titulos_diferentes(base_para_explorar):
    """Meio real de folga não pode virar uma busca que traz o que não foi
    pedido — senão a tela mente de outro jeito."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET pago_recebido = -50000 WHERE codigo_lancamento = 701")
        conn.execute("UPDATE fato SET pago_recebido = -50002 WHERE codigo_lancamento = 702")
        conn.commit()
    consultas.esquecer_listas()
    assert _codigos({"busca": "50.000,00"}) == {701}


# ===========================================================================
# A cascata dos aportes: onde os valores se perdem
# ===========================================================================
# 14/09/2026, o dono: o bloco de Aportes do DRE mostra R$ 567 mil de devolvido
# para uma empresa, e um unico titulo dela e de R$ 784 mil. O bloco esta comendo
# lancamentos, e a tela nao dizia quais. Em vez de apostar em qual corte e o
# culpado, a cascata mostra quanto CADA UM leva.

@pytest.fixture()
def base_de_aportes(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        # 1) devolucao normal: tem de chegar ate o fim da cascata
        # 2) devolucao marcada como transferencia: cortada no degrau do TRF
        # 3) devolucao com status que o PAGO nao reconhece: cortada no ultimo
        # 4) dividendo: sai no degrau do saldo
        for cod, analise, situacao, cat, valor in (
                (601, "Fluxo de Caixa", "PAGO", "Devolução de Aportes", -100000),
                (602, "TRF", "PAGO", "Devolução de Aportes", -200000),
                (603, "Fluxo de Caixa", "Baixado", "Devolução de Aportes", -400000),
                (604, "Fluxo de Caixa", "PAGO", "Dividendos", -50000)):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " situacao_vencimento, categoria, departamento, razao_social,"
                " data, pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar',?,?,'Quitado',?,'CASA','MORAIS',"
                "         '2025-12-24',?,0,0,0)", (cod, analise, situacao, cat, valor))
        conn.commit()
    consultas.esquecer_listas()
    yield


@pytest.fixture()
def base_de_dois_cadastros(base_para_explorar):
    """A MESMA empresa com dois cadastros no OMIE — mesmo CNPJ, nome diferente.

    20/09/2026. A cascata provou que corte nenhum estava comendo a devolução de
    R$ 784.647,07 do dono: ela está na soma geral. O que a escondia era o
    agrupamento — o bloco somava por NOME, então a empresa virava duas linhas,
    cada uma parecendo menor do que a empresa é."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        for cod, razao, cnpj, valor in (
                (701, "MORAIS VASCONCELOS LTDA", "12.345.678/0001-90", -784647.07),
                (702, "MORAIS VASCONCELOS", "12345678000190", -320000),
                (703, "OUTRA EMPRESA", "99.999.999/0001-99", -100000)):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " situacao_vencimento, categoria, departamento, razao_social,"
                " cnpj_cpf, data, pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar','Fluxo de Caixa','PAGO','Quitado',"
                "         'Devolução de Aportes','CASA',?,?,'2025-12-24',?,0,0,0)",
                (cod, razao, cnpj, valor))
        conn.commit()
    consultas.esquecer_listas()
    yield


def test_a_mesma_empresa_com_dois_cadastros_vira_uma_linha_so(base_de_dois_cadastros):
    """O documento manda, o nome é só rótulo."""
    from app.apps.painel import consultas
    por_socio = consultas.aportes(consultas.Filtros())["por_socio"]
    linhas = {l["socio"]: l["devolvido"] for l in por_socio}
    assert len(por_socio) == 2, f"deviam sobrar duas empresas, vieram {linhas}"
    morais = [v for nome, v in linhas.items() if "MORAIS" in nome]
    assert len(morais) == 1, "as duas grafias da MORAIS têm de virar uma linha só"
    assert reais(morais[0]) == reais(784647.07 + 320000), \
        "e a linha tem de somar os dois cadastros"


def test_a_conferencia_denuncia_o_cadastro_repetido(base_de_dois_cadastros):
    """Juntar sozinho não basta: o dono tem de conseguir VER que havia duas
    grafias, senão a correção no OMIE nunca acontece."""
    from app.apps.painel import consultas
    linhas = consultas.conferencia_dos_aportes()["por_contraparte"]
    repetidas = [l for l in linhas if l["nomes"] > 1]
    assert len(repetidas) == 1, "a tela tem de apontar o cadastro repetido"
    assert "MORAIS VASCONCELOS LTDA" in repetidas[0]["todos_os_nomes"]
    assert "12345678000190" == repetidas[0]["documento"], \
        "o documento entra só com os dígitos, senão as duas grafias não juntam"
    assert reais(repetidas[0]["devolvido"]) == reais(784647.07 + 320000)


def test_a_cascata_mostra_quanto_cada_corte_leva(base_de_aportes):
    from app.apps.painel import consultas
    passos = dict(consultas.conferencia_dos_aportes()["passos"])

    # 100 + 200 + 400 = 700 mil. O dividendo NAO entra aqui, e isso mudou em
    # 17/09/2026: desde que o bloco passou a decidir pelo SENTIDO do dinheiro
    # junto com o TIPO, "devolvido" só soma o que é devolução de aporte de
    # verdade. Dividendo é distribuição de lucro — tem quadro próprio, e não
    # aparece nem como aporte nem como devolução.
    assert passos["Tudo o que é aporte (sem o lado provedor)"]["devolvido"] == \
        pytest.approx(700000)
    # por isso este degrau não tem mais o que levar do devolvido
    saldo = passos["Só o que entra no saldo (tira Dividendos)"]
    assert saldo["devolvido"] == pytest.approx(700000)
    assert saldo["comeu_devolvido"] == pytest.approx(0)
    # O DEGRAU DA TRANSFERÊNCIA SAIU em 21/09/2026: aporte entre contas da
    # própria empresa É transferência, e o corte engolia o bloco inteiro. Por
    # isso o lançamento 602, marcado TRF, chega ao fim agora.
    # O DEGRAU DO "PAGO" NAO LEVA MAIS NADA, e isso é o conserto da migração
    # 011. O lançamento 603 foi baixado no OMIE com o status escrito "Baixado":
    # até 20/09/2026 ele era cortado aqui e sumia do bloco. Hoje "foi pago?"
    # tem uma resposta só — a que a carga gravou —, então ele chega ao fim.
    # O degrau continua na tela como guarda: se um dia voltar a comer alguma
    # coisa, é porque as duas regras divergiram de novo.
    pago = passos["Tirando o que o painel não reconhece como pago"]
    assert pago["devolvido"] == pytest.approx(700000)
    assert pago["comeu_devolvido"] == pytest.approx(0)


def test_a_cascata_bate_com_o_que_o_bloco_do_dre_mostra(base_de_aportes):
    """Se o último degrau não for exatamente o número do DRE, a conferência
    mente — e uma conferência que mente é pior que nenhuma."""
    from app.apps.painel import consultas
    passos = dict(consultas.conferencia_dos_aportes()["passos"])
    ultimo = passos["Tirando o que o painel não reconhece como pago"]
    assert ultimo["devolvido"] == pytest.approx(
        consultas.aportes(consultas.Filtros())["devolvido"])


def test_a_cascata_nomeia_os_lancamentos_cortados(base_de_aportes):
    """Número sem nome não ajuda ninguém a corrigir: tem de dizer QUAIS."""
    from app.apps.painel import consultas
    r = consultas.conferencia_dos_aportes()
    assert r["comidos_trf"] == [], \
        "o corte por transferência não existe mais — ver a nota da cascata"
    assert [l["codigo"] for l in r["comidos_pago"]] == [], \
        "desde a migração 011 nada é cortado por 'não reconhecido como pago'"


# ===========================================================================
# Configurações tem de abrir rápido
# ===========================================================================
# 14/09/2026: empilhei três conferências pesadas nessa tela e deixei as três
# ligadas por padrão. São ~12 varreduras na base de 186 mil linhas. A tela
# parou de abrir para o dono no mesmo dia — e é JUSTAMENTE a tela onde se
# aperta o botão de atualizar a base.

@pytest.fixture()
def cliente_config(base_para_explorar, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    c = app.test_client()
    c.post("/painel/entrar", data={"senha": "segredo-de-teste"})
    return c


def test_configuracoes_nao_roda_conferencia_sozinha(cliente_config, monkeypatch):
    """Abrir a tela não pode disparar varredura nenhuma."""
    from app.apps.painel import consultas
    rodou = []
    for nome in ("conferencia_do_pago", "titulos_que_sumiram",
                 "conferencia_dos_aportes"):
        monkeypatch.setattr(consultas, nome,
                            lambda *a, _n=nome, **k: rodou.append(_n) or {})
    r = cliente_config.get("/painel/configuracoes")
    assert r.status_code == 200
    assert rodou == [], f"abriu a tela e rodou {rodou}"
    assert "Rodar as conferências" in r.get_data(as_text=True)


def test_quem_pede_a_conferencia_recebe(cliente_config, monkeypatch):
    """E o botão tem de funcionar, senão a conferência vira enfeite."""
    from app.apps.painel import consultas
    rodou = []
    for nome in ("conferencia_do_pago", "titulos_que_sumiram",
                 "conferencia_dos_aportes"):
        monkeypatch.setattr(consultas, nome,
                            lambda *a, _n=nome, **k: rodou.append(_n) or {})
    r = cliente_config.get("/painel/configuracoes?conferir=1")
    assert r.status_code == 200
    assert sorted(rodou) == ["conferencia_do_pago", "conferencia_dos_aportes",
                             "titulos_que_sumiram"]


def test_as_conferencias_de_verdade_aparecem_na_tela(cliente_config):
    """SEM dublê: aperta o botão e os quadros têm de vir.

    20/09/2026, o dono: "EU JÁ havia tentado rodar mas não tava apresentando
    resultado." Os testes acima trocavam as quatro conferências por dublê, então
    provavam que o botão CHAMA — nunca que o resultado CHEGA. Um erro de SQL em
    qualquer uma delas passava batido aqui e só aparecia na tela dele."""
    html = cliente_config.get(
        "/painel/configuracoes?conferir=1").get_data(as_text=True)
    assert "Aportes do DRE — onde os valores se perdem" in html
    assert "dinheiro que as telas não contam" in html
    assert "Movimentos que não estão no painel" in html
    assert "falhou" not in html, "alguma conferência quebrou — o quadro diz qual"
    assert "Rodar de novo" in html, \
        "depois de rodar, o botão tem de continuar lá para repetir"


def test_uma_conferencia_que_quebra_nao_leva_a_tela_junto(cliente_config,
                                                          monkeypatch):
    """A falha de uma tem de aparecer NOMEADA, e as outras têm de seguir.

    Antes, qualquer erro numa delas jogava a tela inteira na página de erro do
    painel — e não dava para saber qual das quatro tinha sido."""
    from app.apps.painel import consultas
    monkeypatch.setattr(consultas, "conferencia_dos_aportes",
                        lambda *a, **k: (_ for _ in ()).throw(
                            RuntimeError("coluna pago não existe")))
    r = cliente_config.get("/painel/configuracoes?conferir=1")
    assert r.status_code == 200, "a tela não pode cair por causa de uma conferência"
    html = r.get_data(as_text=True)
    assert "Aportes do DRE — falhou" in html
    assert "coluna pago não existe" in html, "o erro tem de aparecer, não só o log"
    assert "dinheiro que as telas não contam" in html, \
        "as outras conferências seguem — a falha é de uma só"


def test_procurar_um_valor_tambem_roda(cliente_config, monkeypatch):
    """Quem cola um valor no campo está pedindo a conferência, sem clicar no
    botão. Exigir os dois seria pegadinha."""
    from app.apps.painel import consultas
    rodou = []
    monkeypatch.setattr(consultas, "titulos_que_sumiram",
                        lambda *a, **k: rodou.append(1) or {})
    monkeypatch.setattr(consultas, "conferencia_do_pago", lambda *a, **k: {})
    monkeypatch.setattr(consultas, "conferencia_dos_aportes", lambda *a, **k: {})
    r = cliente_config.get("/painel/configuracoes?procurar=784.647,07")
    assert r.status_code == 200 and rodou


# ===========================================================================
# O filtro não pode sumir ao entrar num detalhe e voltar
# ===========================================================================
# 17/09/2026, o dono: estava filtrado na obra Mercado Barbalha, na Receita de
# Obra; entrou numa medição; clicou em "Voltar às medições"; a obra tinha
# sumido. "Ela tem que ser mantida, porque eu estou analisando ela."
#
# A causa: os dois links — o de entrar e o de voltar — montavam o endereço sem
# levar os filtros da barra lateral. O ajudante que faz isso já existia e as
# abas do topo já o usavam; esses links, não.

def test_o_link_da_medicao_leva_o_filtro_junto(cliente_config):
    """Entrar numa medição não pode jogar fora a obra que se está analisando."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET tipo = ?, medicao_rotulo = 'MEDICAO 1',"
                     " pago_recebido = 5000 WHERE codigo_lancamento = 701", (REC,))
        conn.commit()
    consultas.esquecer_listas()
    html = cliente_config.get("/painel/receita?obra=CASA").get_data(as_text=True)
    links = [l for l in html.splitlines() if "/painel/receita/" in l and "href" in l]
    assert links, "a tela não trouxe medição nenhuma para conferir"
    for linha in links:
        assert "obra=CASA" in linha, linha


def test_voltar_da_medicao_traz_o_filtro_de_volta(cliente_config):
    """O caminho exato que o dono percorreu."""
    html = cliente_config.get(
        "/painel/receita/(sem medição)?obra=CASA").get_data(as_text=True)
    assert 'href="/painel/receita?obra=CASA"' in html, \
        "o Voltar tem de devolver a pessoa à MESMA obra que ela estava vendo"


def test_a_tela_da_medicao_mostra_que_o_filtro_continua_valendo(cliente_config):
    """Essa tela esconde a barra lateral. Sem dizer que o filtro sobreviveu,
    quem entrou filtrado não tem como saber para onde vai voltar."""
    html = cliente_config.get(
        "/painel/receita/(sem medição)?obra=CASA").get_data(as_text=True)
    assert "Filtro mantido" in html and "Obra: CASA" in html


def test_sem_filtro_a_tela_da_medicao_nao_inventa_aviso(cliente_config):
    html = cliente_config.get("/painel/receita/(sem medição)").get_data(as_text=True)
    assert "Filtro mantido" not in html


# ===========================================================================
# Achar o que está sem classificação
# ===========================================================================
# O dono, 17/09/2026: "eu queria poder visualizar com facilidade tudo que não
# está apropriado em nenhum centro de custo e tudo que não tem nenhuma
# categoria, para eu facilmente identificar e corrigir".

def test_da_para_pedir_o_que_esta_sem_categoria(base_para_explorar):
    """Antes só dava para pedir o que está sem OBRA. Sem categoria, não havia
    como perguntar — e é metade do trabalho de saneamento."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE fato SET categoria = '' WHERE codigo_lancamento = 701")
        conn.commit()
    consultas.esquecer_listas()
    assert consultas.SEM_CATEGORIA in consultas.opcoes_do_explorador()["categorias"]
    assert _codigos({"categorias": [consultas.SEM_CATEGORIA]}) == {701}


def test_configuracoes_tem_os_atalhos_do_saneamento(cliente_config):
    html = cliente_config.get("/painel/configuracoes").get_data(as_text=True)
    assert "Achar o que está sem classificação" in html
    assert "Sem obra" in html and "Sem categoria" in html


# ===========================================================================
# O Explorador tem de dar para achar o lançamento no OMIE
# ===========================================================================
# "Tem que ter a conta que foi baixada, o dia, tudo certinho, se foi despesa ou
# entrada — para eu ir em paralelo no OMIE e identificar com clareza."

def test_a_lista_mostra_conta_corrente_e_as_duas_datas(cliente_config):
    html = cliente_config.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    for coluna in ("Conta corrente", "Vencimento", "Pagamento"):
        assert f">{coluna}</th>" in html, coluna


def test_a_observacao_aparece_inteira(cliente_config):
    """Cortar em 60 letras escondia justamente o que identifica o lançamento."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    longa = "OBSERVACAO MUITO LONGA " * 6   # bem mais que 60 letras
    with conexao() as conn:
        conn.execute("UPDATE fato SET observacao = ? WHERE codigo_lancamento = 701",
                     (longa,))
        conn.commit()
    consultas.esquecer_listas()
    html = cliente_config.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    assert longa.strip() in html, "a observação tem de aparecer inteira"


# ===========================================================================
# O "Aporte BWS" não é devolução — é a origem do dinheiro
# ===========================================================================
# 17/09/2026, explicado pelo dono: quando a BWS põe dinheiro numa obra, ele sai
# da conta da MATRIZ e entra na conta da OBRA. As duas contas são da BWS. Para
# não virar "mera transferência", são lançados dois registros no OMIE: na conta
# de origem, "Aporte BWS" (negativo); na conta da obra, "Aporte de Parceiro"
# (positivo) — porque para aquela obra a BWS é parceira como qualquer outra.
#
# O bloco contava os dois. Resultado na tela dele: BWS com aportado
# R$ 1.677.455,70 e devolvido O MESMO VALOR, ao centavo. Saldo zero. O painel
# dizia que a BWS não tinha nada aplicado na obra, quando tinha 1,67 milhão.

@pytest.fixture()
def base_com_aporte_bws(painel_no_banco):
    """Os quatro movimentos do plano financeiro, com os DOIS lados de cada um.

    Desenho lançado pelo dono no OMIE em 17/09/2026, com os nomes exatos das
    categorias que ele cadastrou lá."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        for cod, categoria, conta, quem, valor in (
            # 1) BWS manda 100 mil para a obra — MESMO nome dos dois lados
            (801, "Aportes BWS", "MATRIZ 7011", "BWS (MATRIZ)", -100000),
            (802, "Aportes BWS", "PARCERIA 22069", "BWS (MATRIZ)", 100000),
            # 2) 30 mil da BWS voltam — nomes diferentes de cada lado
            (803, "Devolução de Aportes", "PARCERIA 22069", "BWS (MATRIZ)", -30000),
            (804, "Devolução de Aportes BWS", "MATRIZ 7011", "BWS (MATRIZ)", 30000),
            # 3) o parceiro de fora aporta 300 mil (só na conta da parceria)
            (805, "Aportes Parceiros", "PARCERIA 22069", "MORAIS", 300000),
            # 4) e recebe 50 mil de volta
            (806, "Devolução de Aportes", "PARCERIA 22069", "MORAIS", -50000),
        ):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " situacao_vencimento, categoria, conta_corrente, departamento,"
                " razao_social, data, ano, mes, pago_recebido, a_pagar_receber,"
                " juros, multa)"
                " VALUES (?,'2. Contas a Pagar','Fluxo de Caixa','PAGO','Quitado',"
                "         ?,?,'CASA',?,'2025-07-10',2025,7,?,0,0,0)",
                (cod, categoria, conta, quem, valor))
        conn.commit()
    consultas.esquecer_listas()
    yield


def test_devolucao_de_aportes_bws_nao_e_aporte(base_com_aporte_bws):
    """O defeito silencioso: "Devolução de Aportes BWS" contém "aportes bws"
    dentro dele. Com o aporte testado antes, a DEVOLUÇÃO virava APORTE — e
    entrava no bloco com o sinal trocado, sem ninguém perceber."""
    from app.apps.painel.sync.fato import classificar_aporte
    assert classificar_aporte("Devolução de Aportes BWS") == "Devolução de Aporte"
    assert classificar_aporte("Devolução de Aportes") == "Devolução de Aporte"


def test_quem_aportou_sai_da_contraparte_e_nao_do_rotulo(base_com_aporte_bws):
    """21/09/2026, o dono: "A diferença de Aportes BWS e Aportes Parceiros é
    somente a nomenclatura (…) e há várias situações que o pessoal do financeiro
    fez o lançamento trocado e não colocou Aportes BWS. Veja pra isso não
    prejudicar a análise."

    O rótulo erra; a contraparte não."""
    from app.apps.painel.sync.fato import classificar_aporte
    assert classificar_aporte("Aportes BWS",
                              razao_social="BWS CONSTRUCOES LTDA") == "Aporte BWS"
    assert classificar_aporte("Aportes Parceiros",
                              razao_social="BWS CONSTRUCOES LTDA") == "Aporte BWS", \
        "rótulo trocado pelo financeiro não pode virar aporte de parceiro"
    assert classificar_aporte("Aportes BWS",
                              razao_social="MORAIS VASCONCELOS") == "Aporte de Parceiro"
    assert classificar_aporte("Aportes Parceiros",
                              razao_social="MORAIS VASCONCELOS") == "Aporte de Parceiro"


def test_o_lado_provedor_nao_e_aporte(base_com_aporte_bws):
    """Ele é o ESPELHO da operação, na conta que mandou o dinheiro. Contá-lo faz
    o mesmo R$ 10,00 aparecer duas vezes na lista — foi o caso que o dono mandou
    em 21/09/2026, entrando na 22069 e saindo da 7011 no mesmo dia."""
    from app.apps.painel.sync.fato import classificar_aporte
    assert classificar_aporte("Aportes BWS", codigo="2.08.97") is None
    assert classificar_aporte("Devolução de Aportes BWS", codigo="1.02.95") is None
    # e o lado da parceria continua contando, pelos códigos dele
    assert classificar_aporte("qualquer coisa", codigo="2.08.02") == "Devolução de Aporte"
    assert classificar_aporte("nome errado", codigo="1.02.94",
                              razao_social="BWS CONSTRUCOES") == "Aporte BWS"
    assert classificar_aporte("nome errado", codigo="1.02.02",
                              razao_social="MORAIS") == "Aporte de Parceiro"


def test_o_aporte_so_conta_quando_entra_na_obra(base_com_aporte_bws):
    """Os dois lados do aporte da BWS usam o MESMO nome. O que distingue é para
    onde o dinheiro foi: o que entra na obra é aporte; o que sai da matriz é o
    registro de onde ele veio."""
    from app.apps.painel import consultas
    bws = {l["socio"]: l for l in
           consultas.aportes(consultas.Filtros())["por_socio"]}["BWS (MATRIZ)"]
    assert bws["aportado"] == pytest.approx(100000), "conta o lado que ENTRA, uma vez só"
    assert bws["devolvido"] == pytest.approx(30000), "a devolução é a que SAI da obra"
    assert bws["saldo"] == pytest.approx(70000)


def test_a_devolucao_so_conta_quando_sai_da_obra(base_com_aporte_bws):
    """"Devolução de Aportes BWS" entrando na matriz é positiva: é o espelho da
    saída, e contá-la dobraria a devolução."""
    from app.apps.painel import consultas
    assert consultas.aportes(consultas.Filtros())["devolvido"] == \
        pytest.approx(80000), "30 da BWS + 50 do parceiro, sem o espelho"


def test_o_parceiro_de_fora_continua_certo(base_com_aporte_bws):
    from app.apps.painel import consultas
    morais = {l["socio"]: l for l in
              consultas.aportes(consultas.Filtros())["por_socio"]}["MORAIS"]
    assert morais["aportado"] == pytest.approx(300000)
    assert morais["devolvido"] == pytest.approx(50000)
    assert morais["saldo"] == pytest.approx(250000)


def test_os_totais_do_topo_param_de_dobrar(base_com_aporte_bws):
    from app.apps.painel import consultas
    bloco = consultas.aportes(consultas.Filtros())
    assert bloco["aportado"] == pytest.approx(400000)   # 100 da BWS + 300 do parceiro
    assert bloco["devolvido"] == pytest.approx(80000)   # 30 + 50
    assert bloco["saldo"] == pytest.approx(320000)


def test_o_lancamento_da_matriz_continua_a_vista(base_com_aporte_bws):
    """Ele deixa de CONTAR, não de existir — senão some rastro de dinheiro."""
    from app.apps.painel import consultas
    contas = {l["conta"] for l in
              consultas.lancamentos_de_aporte(consultas.Filtros())["linhas"]}
    assert "MATRIZ 7011" in contas, "os lançamentos da matriz continuam listados"


def test_dividendo_nao_vira_devolucao_de_aporte(base_com_aporte_bws):
    """Consequência da regra do sentido do dinheiro, e ela é desejada.

    Antes o bloco decidia só pelo sinal: qualquer coisa negativa com categoria
    de aporte virava "devolvido" — e o dividendo pago, que é negativo, entrava
    ali. Dividendo é distribuição de LUCRO, não devolução de capital: abatê-lo
    do saldo faria parecer que o sócio retirou o que colocou.

    Ele tem quadro próprio na tela e continua aparecendo lá."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, conta_corrente, departamento,"
            " razao_social, data, ano, mes, pago_recebido, a_pagar_receber,"
            " juros, multa)"
            " VALUES (900,'2. Contas a Pagar','Fluxo de Caixa','PAGO','Quitado',"
            "         'Dividendos','PARCERIA 22069','CASA','MORAIS',"
            "         '2025-07-10',2025,7,-70000,0,0,0)")
        conn.commit()
    consultas.esquecer_listas()
    bloco = consultas.aportes(consultas.Filtros())
    assert bloco["devolvido"] == pytest.approx(80000), \
        "o dividendo de 70 mil não pode entrar no devolvido"
    morais = {l["socio"]: l for l in bloco["por_socio"]}["MORAIS"]
    assert morais["saldo"] == pytest.approx(250000), \
        "nem abater o saldo de quem aportou"
    # e continua visível no quadro próprio
    divs = {l["socio"]: l for l in consultas.dividendos_por_socio(consultas.Filtros())}
    assert divs["MORAIS"]["pago"] == pytest.approx(70000)


def test_a_janela_do_incremental_apaga_nas_duas_tabelas():
    """Senão a atualização do dia reinsere os sem título e o número cresce
    sozinho a cada madrugada — um erro que só apareceria semanas depois."""
    import inspect

    from app.apps.painel.sync import espelho
    fonte = inspect.getsource(espelho._apagar_movimentos_janela)
    assert fonte.count("DELETE FROM") == 2
    assert "movimentos_sem_titulo" in fonte


# ===========================================================================
# O bloco de aportes tem de dizer o que o FILTRO está escondendo — 21/09/2026
# ===========================================================================
# O dono: "a parte dos aportes continua sem apresentar todos os números."
#
# Eu vinha comparando a conferência das Configurações — que roda SEM filtro —
# com o bloco do DRE, que roda COM os filtros da barra lateral, e dizendo que
# tinham de bater. Não tinham por que bater: um ano selecionado na lateral já
# explica uma devolução "sumida". A tela nunca dizia isso; agora diz.

@pytest.fixture()
def base_de_dois_anos(base_para_explorar):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        for cod, data, ano, valor in ((801, "2025-12-24", 2025, -784647.07),
                                      (802, "2026-03-10", 2026, -100000)):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " situacao_vencimento, categoria, departamento, razao_social,"
                " cnpj_cpf, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar','Fluxo de Caixa','PAGO','Quitado',"
                "         'Devolução de Aportes','CASA','MORAIS','12345678000190',"
                "         ?,?,?,0,0,0)", (cod, data, ano, valor))
        conn.commit()
    consultas.esquecer_listas()
    yield


def test_o_bloco_avisa_quanto_o_filtro_esta_escondendo(cliente_config,
                                                       base_de_dois_anos):
    """Filtrado em 2026, o bloco mostra R$ 100 mil — e tem de dizer que na base
    inteira são R$ 884 mil, senão o dono procura um dinheiro que está lá."""
    from app.apps.painel import consultas
    base = consultas.aportes_na_base_inteira()
    assert reais(base["devolvido"]) == reais(884647.07)

    html = cliente_config.get(
        "/painel/dre?bloco=aportes&ano=2026").get_data(as_text=True)
    assert "estão fora do que você está vendo" in html
    assert "por causa do filtro" in html
    assert "Tirar todos os filtros" in html, \
        "e tem de dar o caminho de ver tudo, não só avisar"


def test_sem_filtro_o_bloco_diz_que_nao_esconde_nada(cliente_config,
                                                     base_de_dois_anos):
    """O outro lado, que é o que mata a dúvida: quando não há filtro, a tela
    afirma isso. Silêncio aqui deixaria a suspeita de pé para sempre."""
    html = cliente_config.get("/painel/dre?bloco=aportes").get_data(as_text=True)
    assert "Sem filtro escondendo nada" in html
    assert "estão fora do que você está vendo" not in html


def test_o_diagnostico_do_bloco_usa_os_filtros_da_propria_tela(cliente_config,
                                                               base_de_dois_anos):
    """A cascata das Configurações roda sem filtro. Esta, dentro do bloco, tem
    de rodar com os MESMOS filtros da tela — senão repete o erro que me custou
    uma semana: comparar dois recortes diferentes e concluir que falta dinheiro."""
    html = cliente_config.get(
        "/painel/dre?bloco=aportes&ano=2026&diagnostico=1").get_data(as_text=True)
    assert "De onde vem cada número" in html
    # só o quadro do diagnóstico; o aviso lá em cima fala da base inteira de
    # propósito, e é ele que cita os 784 mil
    quadro = html[html.index("De onde vem cada número"):]
    assert "100.000" in quadro
    assert "784.647" not in quadro, \
        "o diagnóstico trouxe lançamento de fora do filtro da tela"


def test_o_diagnostico_so_roda_quando_alguem_pede(cliente_config,
                                                  base_de_dois_anos, monkeypatch):
    """São várias varreduras na base inteira. Abrir o DRE não pode disparar."""
    from app.apps.painel import consultas
    rodou = []
    monkeypatch.setattr(consultas, "conferencia_dos_aportes",
                        lambda *a, **k: rodou.append(1) or {})
    cliente_config.get("/painel/dre?bloco=aportes")
    assert rodou == []
    cliente_config.get("/painel/dre?bloco=aportes&diagnostico=1")
    assert rodou, "e com o pedido, tem de rodar"


def test_o_bloco_diz_ONDE_esta_o_resto_obra_por_obra(cliente_config):
    """21/09/2026. Filtrado na obra, o dono via R$ 887 mil de devolução e
    esperava R$ 3,3 milhões. Na base inteira havia R$ 5,4 milhões — ou seja, o
    dinheiro estava lá, em lançamentos NÃO APROPRIADOS àquela obra.

    Dizer "o filtro esconde X" não resolve: ele precisa saber ONDE está o resto
    para poder apropriar. A linha "(não apropriado)" é a que ele conserta."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        for cod, obra, valor in ((901, "MERCADOBARBALHA", -320000),
                                 (902, "", -784647.07)):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " situacao_vencimento, categoria, codigo_categoria, departamento,"
                " razao_social, cnpj_cpf, data, ano, pago_recebido,"
                " a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar','Fluxo de Caixa','PAGO','Quitado',"
                "         'Devolução de Aportes','2.08.02',?,'MORAIS',"
                "         '09426420000109','2025-12-24',2025,?,0,0,0)",
                (cod, obra, valor))
        conn.commit()
    consultas.esquecer_listas()

    base = consultas.aportes_na_base_inteira()
    por_obra = {l["obra"]: l["devolvido"] for l in base["por_obra"]}
    assert reais(por_obra[consultas.SEM_OBRA]) == reais(784647.07), \
        "o lançamento sem obra tem de aparecer com nome próprio"
    assert reais(por_obra["MERCADOBARBALHA"]) == reais(320000)

    html = cliente_config.get(
        "/painel/dre?bloco=aportes&obra=MERCADOBARBALHA").get_data(as_text=True)
    assert "E onde está o resto" in html
    assert consultas.SEM_OBRA in html, \
        "a linha que ele conserta tem de estar na tela, não no meu raciocínio"
    assert "784.647,07" in html
