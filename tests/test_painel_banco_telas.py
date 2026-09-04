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
    # CASA recebeu 1.000 e pagou 400 (06/2025) e 100 (03/2024)
    assert reais(por_obra["CASA"]) == 500.00
    assert reais(por_obra["PONTE"]) == -900.00
    # a transferência entre contas não é caixa da operação
    assert reais(sum(por_obra.values())) == -400.00


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
    assert reais(sum(l["despesas"] for l in casa)) == -750.00


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

    # ALFA é a obra CASA: receita líquida 1.300, despesas 750 -> resultado 550
    assert reais(por_projeto["ALFA"]["resultado"]) == 550.00
    assert reais(sum(q["quota"] for q in quotas)) == 550.00
    por_socio = {q["socio"]: q["quota"] for q in quotas}
    assert reais(por_socio["ANA"]) == 385.00       # 70%
    assert reais(por_socio["BENTO"]) == 165.00     # 30%


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
