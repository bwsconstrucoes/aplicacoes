# -*- coding: utf-8 -*-
"""
O Extrato de Conta Corrente.

Pedido do dono em 21/09/2026: *"seria até similar com o relatório analítico, só
que ao invés de ser o da obra, seria o da conta corrente (…) e ali só iriam
poder ser vistos os lançamentos que aconteceram na conta corrente."*

E, no mesmo dia, a dor que motivou: ele foi ver as tarifas do Mercado Barbalha e
elas não apareciam no Analítico. **Não apareciam porque o plano financeiro do
OMIE não dá conta de DRE a elas** — então o painel as classifica como
movimentação de caixa e o Analítico, que só mostra DRE, as deixa de fora.

No extrato elas APARECEM, e é o ponto: dinheiro que saiu da conta é extrato,
tenha ou não conta de DRE.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture()
def base_de_extrato():
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
        conn.execute("DELETE FROM usuarios")
        # (codigo, analise, categoria, conta, valor, data, situacao)
        linhas = (
            # uma despesa normal, que o Analítico mostra
            (801, "DRE", "Serviços", "Bradesco 22069-8", -5000, "2025-09-08", "PAGO"),
            # uma TARIFA: fora do DRE porque a categoria não tem conta de DRE
            (802, "Fluxo de Caixa", "Tarifas Bancárias", "Bradesco 22069-8",
             -38.90, "2025-09-08", "PAGO"),
            # um RENDIMENTO: mesma situação, do outro lado
            (803, "Fluxo de Caixa", "Rendimento de Aplicação", "Bradesco 22069-8",
             120.55, "2025-09-08", "PAGO"),
            # uma transferência: sai do resultado, não sai do extrato
            (804, "TRF", "Transferência entre contas", "Bradesco 22069-8",
             -1000, "2025-09-09", "PAGO"),
            # outra conta, para provar o filtro
            (805, "DRE", "Serviços", "Itaú 7011-4", -777, "2025-09-08", "PAGO"),
            # em ABERTO: extrato é caixa, isto não pode aparecer
            (806, "DRE", "Serviços", "Bradesco 22069-8", 0, "2025-10-01", "A PAGAR"),
        )
        for cod, analise, cat, conta, valor, data, sit in linhas:
            quitado = "Quitado" if sit != "A PAGAR" else "A vencer"
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " situacao_vencimento, categoria, departamento, razao_social,"
                " cnpj_cpf, conta_corrente, numero_documento, data, ano,"
                " pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar',?,?,?,?,'MERCADOBARBALHA',"
                "         'FORNECEDOR X','11222333000144',?,'NF 1',?,2025,?,"
                "         ?,0,0)",
                (cod, analise, sit, quitado, cat, conta, data, valor,
                 500 if sit == "A PAGAR" else 0))
        conn.commit()
    consultas.esquecer_listas()
    yield
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM usuarios")
        conn.commit()


def _cliente(monkeypatch, senha="senha-do-dono"):
    monkeypatch.setenv("PAINEL_SENHA", senha)
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    c = app.test_client()
    c.post("/painel/entrar", data={"usuario": "", "senha": senha})
    return c


# ===========================================================================
# 1. O que o extrato mostra — e por que é diferente do Analítico
# ===========================================================================
def test_a_tarifa_aparece_no_extrato_mesmo_ficando_fora_do_dre(base_de_extrato):
    """A dor que motivou a tela. No Analítico a tarifa não aparece porque o
    plano do OMIE não lhe dá conta de DRE. No extrato ela aparece: o dinheiro
    saiu da conta."""
    from app.apps.painel import consultas
    f = consultas.Filtros(contas=["Bradesco 22069-8"])
    codigos = [l["codigo"] for l in consultas.extrato_da_conta(f)["linhas"]]
    assert 802 in codigos, "a tarifa sumiu do extrato"
    assert 803 in codigos, "o rendimento sumiu do extrato"

    # e no Analítico ela continua de fora, que é o comportamento de hoje
    no_analitico = [l["lancamento"] for l in
                    consultas.analitico_despesas(f)["linhas"]]
    assert 802 not in no_analitico


def test_a_transferencia_aparece_no_extrato(base_de_extrato):
    """Ela sai do resultado — não é receita nem despesa —, mas não sai do
    extrato: aconteceu na conta."""
    from app.apps.painel import consultas
    f = consultas.Filtros(contas=["Bradesco 22069-8"])
    codigos = [l["codigo"] for l in consultas.extrato_da_conta(f)["linhas"]]
    assert 804 in codigos


def test_titulo_em_aberto_nao_entra(base_de_extrato):
    """Extrato é caixa, não compromisso."""
    from app.apps.painel import consultas
    f = consultas.Filtros(contas=["Bradesco 22069-8"])
    codigos = [l["codigo"] for l in consultas.extrato_da_conta(f)["linhas"]]
    assert 806 not in codigos


def test_o_filtro_de_conta_separa_as_contas(base_de_extrato):
    from app.apps.painel import consultas
    bradesco = consultas.extrato_da_conta(
        consultas.Filtros(contas=["Bradesco 22069-8"]))
    itau = consultas.extrato_da_conta(consultas.Filtros(contas=["Itaú 7011-4"]))
    assert 805 not in [l["codigo"] for l in bradesco["linhas"]]
    assert [l["codigo"] for l in itau["linhas"]] == [805]


def test_entrou_saiu_e_a_diferenca(base_de_extrato):
    from app.apps.painel import consultas
    r = consultas.extrato_da_conta(consultas.Filtros(contas=["Bradesco 22069-8"]))
    assert round(r["entradas"], 2) == 120.55
    assert round(r["saidas"], 2) == round(5000 + 38.90 + 1000, 2)
    assert round(r["liquido"], 2) == round(120.55 - 6038.90, 2)


def test_a_ordem_e_a_data(base_de_extrato):
    """É assim que se confere lado a lado com o extrato do banco."""
    from app.apps.painel import consultas
    datas = [l["data"] for l in consultas.extrato_da_conta(
        consultas.Filtros(contas=["Bradesco 22069-8"]))["linhas"]]
    assert datas == sorted(datas)


def test_a_tela_abre_e_traz_as_colunas_que_o_dono_pediu(base_de_extrato,
                                                        monkeypatch):
    cliente = _cliente(monkeypatch)
    html = cliente.get("/painel/extrato?conta=Bradesco+22069-8").get_data(as_text=True)
    assert "Extrato de Conta Corrente" in html
    for coluna in ("Data", "Cliente ou Fornecedor", "CNPJ/CPF", "Conta",
                   "Categoria", "Documento", "Observação", "Valor"):
        assert coluna in html, coluna
    assert "11222333000144" in html, "o CNPJ tem de sair na linha"
    assert "Tarifas Bancárias" in html
    assert "Vencimento" not in html, \
        "extrato não tem vencimento — o dono disse isso com todas as letras"


def test_o_download_do_extrato_sai(base_de_extrato, monkeypatch):
    cliente = _cliente(monkeypatch)
    r = cliente.get("/painel/baixar/extrato?conta=Bradesco+22069-8")
    assert r.status_code == 200
    assert len(r.get_data()) > 500


# ===========================================================================
# 2. O acesso por conta
# ===========================================================================
def test_quem_tem_uma_conta_liberada_so_ve_ela(base_de_extrato, monkeypatch):
    """O pedido: "definir qual conta e quais contas ele poderia visualizar"."""
    from app.apps.painel import usuarios
    usuarios.criar("socio", "senha-dele", obras=["MERCADOBARBALHA"],
                   telas=["extrato"], contas=["Bradesco 22069-8"])
    cliente = _cliente(monkeypatch)
    cliente.get("/painel/sair")
    cliente.post("/painel/entrar", data={"usuario": "socio", "senha": "senha-dele"})

    html = cliente.get("/painel/extrato").get_data(as_text=True)
    assert "Bradesco 22069-8" in html
    assert "Itaú 7011-4" not in html, "a conta do outro VAZOU"
    assert "777,00" not in html


def test_pedir_a_conta_de_outro_no_endereco_nao_funciona(base_de_extrato,
                                                         monkeypatch):
    from app.apps.painel import usuarios
    usuarios.criar("socio", "senha-dele", obras=["MERCADOBARBALHA"],
                   telas=["extrato"], contas=["Bradesco 22069-8"])
    cliente = _cliente(monkeypatch)
    cliente.get("/painel/sair")
    cliente.post("/painel/entrar", data={"usuario": "socio", "senha": "senha-dele"})
    html = cliente.get(
        "/painel/extrato?conta=Ita%C3%BA+7011-4").get_data(as_text=True)
    assert "777,00" not in html


def test_sem_a_tela_liberada_o_extrato_nao_abre(base_de_extrato, monkeypatch):
    from app.apps.painel import usuarios
    usuarios.criar("socio", "senha-dele", obras=["MERCADOBARBALHA"],
                   telas=["dre"], contas=["Bradesco 22069-8"])
    cliente = _cliente(monkeypatch)
    cliente.get("/painel/sair")
    cliente.post("/painel/entrar", data={"usuario": "socio", "senha": "senha-dele"})
    assert cliente.get("/painel/extrato").status_code == 404


def test_o_dono_ve_todas_as_contas(base_de_extrato, monkeypatch):
    cliente = _cliente(monkeypatch)
    html = cliente.get("/painel/extrato").get_data(as_text=True)
    assert "Bradesco 22069-8" in html and "Itaú 7011-4" in html
