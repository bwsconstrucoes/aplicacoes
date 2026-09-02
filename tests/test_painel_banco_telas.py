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
