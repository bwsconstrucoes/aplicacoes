# -*- coding: utf-8 -*-
"""
Uma medição que junta vários títulos tem de dizer isso — e listá-los.

22/09/2026, o dono: "CREPEBELEM | Medição 1, doc. PM1339984827: R$ 664.875,43.
No OMIE tenho para o mesmo título o valor bruto de 86.828,71."

A linha da Receita de Obra é o grupo de todos os títulos cuja observação diz
"CREPEBELEM|Medição No: 1" — de propósito, porque uma medição é faturada em
várias notas. Mas a tela mostrava UM documento para o grupo inteiro, e ele foi
conferir esse documento no OMIE. Sem dizer quantos títulos a linha junta, e
quais, o número não tem como ser conferido.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.banco

SENHA_MESTRE = "senha-do-dono"
ROTULO = "CREPEBELEM | Medição 1"


def _receita(conn, cod, doc, valor, data="2025-03-10"):
    conn.execute(
        "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
        " situacao_vencimento, categoria, grupo, departamento, projeto,"
        " razao_social, numero_documento, observacao, medicao, medicao_rotulo,"
        " data, ano, pago_recebido, a_pagar_receber, juros, multa)"
        " VALUES (?,'1. Contas a Receber','DRE','RECEBIDO','Quitado',"
        "         'Receita de Obras','Receita','CREPEBELEM','ALFA',"
        "         'SECRETARIA DE EDUCAÇÃO',?,'CREPEBELEM|Medição No: 1',"
        "         'MED:CREPEBELEM|1',?,?,2025,?,0,0,0)",
        (cod, doc, ROTULO, data, valor))


@pytest.fixture()
def base():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura
    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)
    from app.apps.painel import consultas
    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    painel_db._engine = None
    assert not migracoes_runner.aplicar_pendentes().get("erro")
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        _receita(conn, 1001, "PM1339984827", 86828.71)
        _receita(conn, 1002, "PM1339984900", 300000.00, "2025-04-10")
        _receita(conn, 1003, "PM1339985001", 278046.72, "2025-05-10")
        conn.commit()
    consultas.esquecer_listas()
    yield
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.commit()


def _cliente(monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", SENHA_MESTRE)
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"usuario": "", "senha": SENHA_MESTRE})
    return cliente


def test_a_lista_tem_um_titulo_por_linha(base, monkeypatch):
    """"Não fica legal agrupado, confunde, tem que separar mesmo os títulos."
    Cada título com o documento dele e o valor dele — o que o dono confere no
    OMIE é exatamente o que está na linha."""
    html = _cliente(monkeypatch).get("/painel/receita").get_data(as_text=True)
    for doc in ("PM1339984827", "PM1339984900", "PM1339985001"):
        assert doc in html
    assert "86.828,71" in html           # o título que ele conferiu, no valor dele
    assert "<b>R$ 664.875,43</b>" not in html   # nenhuma LINHA com a soma que confundiu
    assert "3 títulos" in html           # o rodapé conta títulos


def test_o_detalhe_e_de_um_titulo_so(base, monkeypatch):
    html = _cliente(monkeypatch).get("/painel/receita/titulo/1001").get_data(as_text=True)
    assert "Título 1001" in html and "PM1339984827" in html
    assert "86.828,71" in html
    assert "PM1339984900" not in html    # o vizinho de medição não entra
    assert ROTULO in html                # mas a medição continua escrita


def test_titulo_fora_do_recorte_nao_existe(base, monkeypatch):
    """Quem só vê uma obra não abre o título de outra pelo número."""
    r = _cliente(monkeypatch).get("/painel/receita/titulo/1001?obra=OUTRA")
    assert r.status_code == 404


def test_a_funcao_devolve_um_titulo_por_linha(base):
    from app.apps.painel import consultas
    titulos = consultas.titulos_da_medicao(ROTULO)
    assert [t["codigo"] for t in titulos] == [1001, 1002, 1003]
    assert sum(t["bruto"] for t in titulos) == pytest.approx(664875.43)


def test_a_receita_de_obra_sai_por_data_e_nao_por_valor(base, monkeypatch):
    """"Essas receitas estão aparecendo de forma aleatória, deveria ser
    ordenada por data." Eram por valor. Mais recente primeiro."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        # uma medição pequena e mais nova, e uma grande e mais velha
        conn.execute("UPDATE fato SET medicao_rotulo = 'CREPEBELEM | Medição 2',"
                     " medicao = 'MED:CREPEBELEM|2', data = '2025-06-01'"
                     " WHERE codigo_lancamento = 1001")
        conn.commit()
    itens = consultas.medicoes(consultas.Filtros())
    assert [m["codigo"] for m in itens] == [1001, 1003, 1002]


def test_outras_receitas_nao_entram_na_lista_de_medicoes(base, monkeypatch):
    """"Essas outras receitas não deveriam ser exibidas junto com as medições.
    Deixar somente embaixo, como outras receitas, separado." Um rendimento com
    observação de medição continua sendo rendimento."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, medicao_rotulo, data, ano, pago_recebido,"
            " a_pagar_receber, juros, multa)"
            " VALUES (2001,'1. Contas a Receber','DRE','RECEBIDO','Quitado',"
            "         'Rendimento de Aplicação','Receita','CREPEBELEM','ALFA',"
            "         'BANCO','CREPEBELEM | Medição 9','2025-07-01',2025,"
            "         5000,0,0,0)")
        # e o imposto retido de uma medição de verdade CONTINUA na medição
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, medicao_rotulo, data, ano, pago_recebido,"
            " a_pagar_receber, juros, multa)"
            " VALUES (1001,'1. Contas a Receber','DRE','RECEBIDO','Quitado',"
            "         'IRRF Retido','Receita','CREPEBELEM','ALFA',"
            "         'SECRETARIA DE EDUCAÇÃO',?,'2025-03-10',2025,"
            "         1000,0,0,0)", (ROTULO,))
        conn.commit()
    consultas.esquecer_listas()

    itens = consultas.medicoes(consultas.Filtros())
    assert {m["medicao"] for m in itens} == {ROTULO}
    assert next(m for m in itens if m["codigo"] == 1001)["retido"] == pytest.approx(1000.0)
    total = consultas.total_das_medicoes(consultas.Filtros())
    assert total["quantas"] == 3            # três títulos; o rendimento não conta

    html = _cliente(monkeypatch).get("/painel/receita").get_data(as_text=True)
    assert "Medição 9" not in html
    assert "Rendimento de Aplicação" in html      # no bloco de outras receitas
