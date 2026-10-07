# -*- coding: utf-8 -*-
"""
Conferir o saldo de cada conta, painel × OMIE, mês a mês — 07/10/2026.

A rede que pega o que ninguém previu: se o painel deixa de contar um tipo de
lançamento (como o de conta corrente da Sicredi), a conta não bate no mês.
"""
from __future__ import annotations

import datetime as dt
import os

import pytest

from app.apps.painel import conferencia_saldo as cs


def test_o_mes_vira_o_primeiro_e_o_ultimo_dia():
    assert cs._periodo("2026-02") == (dt.date(2026, 2, 1), dt.date(2026, 2, 28))
    assert cs._periodo("2025-12") == (dt.date(2025, 12, 1), dt.date(2025, 12, 31))


def test_os_saldos_sao_achados_pelo_nome_do_campo():
    """O formato do extrato não foi confirmado contra o OMIE: procura-se pelo
    nome, no primeiro nível ou um abaixo, em número ou em texto."""
    assert cs.saldos_da_resposta({"nSaldoAnterior": 100, "nSaldoAtual": 250.5}) == \
        {"anterior": 100.0, "final": 250.5}
    assert cs.saldos_da_resposta({"cabecalho": {"saldoInicial": "1.000,00",
                                                "saldoFinal": "900,10"}}) == \
        {"anterior": 1000.0, "final": 900.1}
    # o saldo "disponível" não é o final do período
    assert cs.saldos_da_resposta({"nSaldoAnterior": 1, "nSaldoDisponivel": 9}) == \
        {"anterior": 1.0, "final": None}
    assert cs.saldos_da_resposta("lixo") == {"anterior": None, "final": None}


@pytest.fixture()
def base():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura
    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)
    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    painel_db._engine = None
    assert not migracoes_runner.aplicar_pendentes().get("erro")
    cs._LIDOS.clear()
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("TRUNCATE TABLE contas_correntes")
        conn.executemany("INSERT INTO contas_correntes (codigo, descricao, inativa)"
                         " VALUES (?,?,?)", [(77, "Sicredi LC", ""), (88, "Bradesco", ""),
                                             (99, "Fechada", "S")])
        colunas = ("tipo", "situacao_vencimento", "categoria", "conta_corrente", "data",
                   "pago_recebido", "juros", "multa")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({','.join('?' * len(colunas))})",
            [("2. Contas a Pagar", "Quitado", "Aluguel", "Bradesco", dt.date(2026, 1, 9), -100, -2, 0),  # juros de despesa: negativo
             ("1. Contas a Receber", "Quitado", "Receita", "Bradesco", dt.date(2026, 1, 20), 500, 0, 0),
             ("1. Contas a Receber", "Quitado", "Impostos Retidos na Fonte", "Bradesco",
              dt.date(2026, 1, 20), 50, 0, 0),                       # nunca passou pela conta
             ("2. Contas a Pagar", "A vencer", "Aluguel", "Bradesco", dt.date(2026, 1, 25), 0, 0, 0)])
        conn.commit()
    yield painel_db
    cs._LIDOS.clear()
    painel_db._engine = None


class Omie:
    def __init__(self, por_conta):
        self.por_conta, self.chamadas = por_conta, []

    def extrato(self, conta, de, ate):
        self.chamadas.append((conta, de, ate))
        r = self.por_conta[conta]
        if isinstance(r, Exception):
            raise r
        return r


@pytest.mark.banco
def test_a_conta_que_nao_bate_aparece_com_a_diferenca(base):
    """A Sicredi andou 100 mil no OMIE e zero no painel: é o lançamento de conta
    corrente que o painel não contava. O Bradesco bate."""
    omie = Omie({77: {"nSaldoAnterior": 0, "nSaldoAtual": -100000.01},
                 88: {"nSaldoAnterior": 1000, "nSaldoAtual": 1398}})
    r = cs.conferir_mes("2026-01", cliente=omie)
    por = {l["conta"]: l for l in r["linhas"]}
    assert set(por) == {"Sicredi LC", "Bradesco"}            # a fechada não entra
    assert por["Bradesco"]["painel"] == 398.0 and por["Bradesco"]["diferenca"] == 0.0
    assert por["Sicredi LC"]["omie"] == -100000.01 and por["Sicredi LC"]["diferenca"] == 100000.01
    assert r["com_diferenca"] == 1
    assert (77, "01/01/2026", "31/01/2026") in omie.chamadas


@pytest.mark.banco
def test_uma_conta_que_falha_nao_derruba_as_outras(base):
    omie = Omie({77: RuntimeError("OMIE fora"), 88: {"campo": "desconhecido"}})
    r = cs.conferir_mes("2026-01", cliente=omie)
    por = {l["conta"]: l for l in r["linhas"]}
    assert "OMIE fora" in por["Sicredi LC"]["problema"]
    assert "baixe a resposta crua" in por["Bradesco"]["problema"]
    assert r["sem_leitura"] == 2


@pytest.mark.banco
def test_so_o_dono_confere_o_saldo(base, monkeypatch):
    from app.apps.painel import usuarios
    monkeypatch.setattr(cs, "_cliente", lambda: Omie({77: {"nSaldoAnterior": 0, "nSaldoAtual": 0},
                                                      88: {"nSaldoAnterior": 0, "nSaldoAtual": 398}}))
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    dono = app.test_client()
    dono.post("/painel/entrar", data={"senha": "segredo-de-teste"})
    r = dono.get("/painel/conferir/saldo?mes=2026-01").get_json()
    assert r["ok"] and r["com_diferenca"] == 0
    assert dono.get("/painel/conferir/saldo?mes=jan").status_code == 400
    cru = dono.get("/painel/conferir/saldo/json?mes=2026-01&conta=88")
    assert cru.status_code == 200 and cru.get_json() == {"nSaldoAnterior": 0, "nSaldoAtual": 398}
    assert "Conferir o saldo das contas com o OMIE" in \
        dono.get("/painel/configuracoes").get_data(as_text=True)

    with base.conexao() as conn:
        conn.execute("DELETE FROM usuarios WHERE usuario = 'preso-saldo'")
        conn.commit()
    usuarios.criar("preso-saldo", "senha-dele", obras=["CASA"], telas=["calendario"])
    preso = app.test_client()
    preso.post("/painel/entrar", data={"usuario": "preso-saldo", "senha": "senha-dele"})
    assert preso.get("/painel/conferir/saldo?mes=2026-01").status_code == 404
    assert preso.get("/painel/conferir/saldo/json?mes=2026-01&conta=88").status_code == 404
    with base.conexao() as conn:
        conn.execute("DELETE FROM usuarios WHERE usuario = 'preso-saldo'")
        conn.commit()
