# -*- coding: utf-8 -*-
"""
Os lançamentos feitos direto na conta corrente entram no painel, rateados nas
obras pela apropriação do OMIE.

07/10/2026, o dono: *"na conta Sicredi tem um lançamento com apropriação em
duas obras e ele não está sendo exibido nem contabilizado no painel. No OMIE
existe baixa, conciliação, favorecido, mas o painel não tá lendo isso."* E:
*"precisamos ver uma forma de atualizar os dados de um dia específico ou um
mês apenas, pra ser rápido no teste."*
"""
from __future__ import annotations

import datetime as dt
import os

import pytest

from app.apps.painel.sync import fato


def test_a_apropriacao_vem_em_percentual_ou_em_valor():
    por_pct = '{"departamentos": [{"cCodDepartamento": "10", "nDistrPercentual": 50},' \
              ' {"cCodDepartamento": "20", "nDistrPercentual": 50}]}'
    assert fato._departamentos_do_bruto(por_pct) == [("10", 0.5), ("20", 0.5)]
    por_valor = '{"departamentos": [{"cCodDep": "10", "nValDep": 30}, {"cCodDep": "20", "nValDep": 70}]}'
    assert fato._departamentos_do_bruto(por_valor) == [("10", 0.3), ("20", 0.7)]
    assert fato._departamentos_do_bruto(None) == []
    assert fato._departamentos_do_bruto("lixo") == []
    assert fato._codigo_do_bruto('{"detalhes": {"nCodLanc": 555}}') == 555


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
    with painel_db.conexao() as conn:
        for t in ("fato", "fato_recebimentos", "titulos", "movimentos",
                  "movimentos_sem_titulo", "rateio", "cat", "clientes",
                  "contas_correntes", "depto_projeto"):
            conn.execute(f"TRUNCATE TABLE {t}")
        conn.execute("DELETE FROM config WHERE chave IN ('projeto_da_obra',"
                     " 'releitura_pagamentos_anos', 'atualizacao_periodo')")
        conn.execute("INSERT INTO cat (codigo, descricao, grupo, codigo_dre)"
                     " VALUES ('2.01.01', 'Aluguel', 'Administrativas', '1')")
        conn.execute("INSERT INTO clientes (codigo, razao_social, cnpj_cpf)"
                     " VALUES (900, 'IMOBILIARIA SUL', '12345678000199')")
        conn.execute("INSERT INTO contas_correntes (codigo, descricao) VALUES (77, 'Sicredi LC')")
        conn.executemany("INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep,"
                         " cdesdep, nperdep, nvaldep) VALUES (?,?,?,?,100,1)",
                         [(1, 1, "10", "CRECHESUAPE"), (2, 1, "20", "ESCPE18")])
        conn.execute("INSERT INTO depto_projeto (ccoddep, projeto) VALUES ('10', 'PROJ-A')")
        conn.commit()
    yield painel_db
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("TRUNCATE TABLE movimentos_sem_titulo")
        conn.execute("TRUNCATE TABLE movimentos")
        conn.execute("DELETE FROM config WHERE chave = 'atualizacao_periodo'")
        conn.commit()
    painel_db._engine = None


def _mov(codigo, dia, valor, *, liquidado="", deps=None, natureza="P"):
    mv = {"detalhes": {"nCodLanc": codigo, "cNatureza": natureza, "cCodCateg": "2.01.01",
                       "nCodCC": 77, "nCodCliente": 900, "dDtPagamento": dia},
          "resumo": {"cLiquidado": liquidado, "nValPago": valor}}
    if deps is not None:
        mv["departamentos"] = deps
    return mv


@pytest.mark.banco
def test_o_lancamento_da_sicredi_entra_rateado_nas_duas_obras(base):
    from app.apps.painel.sync import espelho
    with base.conexao() as conn:
        espelho.gravar_movimentos(conn, [
            _mov(555, "09/01/2026", 100000.01,
                 deps=[{"cCodDepartamento": "10", "nDistrPercentual": 50},
                       {"cCodDepartamento": "20", "nDistrPercentual": 50}]),
            _mov(556, "09/01/2026", 300.0, liquidado="N"),           # previsão
            _mov(557, "12/01/2026", 80.0),                            # sem apropriação
        ])
        fato.reconstruir_fato(conn)
        linhas = conn.execute(
            "SELECT codigo_lancamento, departamento, projeto, pago_recebido::float8,"
            "       conta_corrente, razao_social, analise, pago"
            "  FROM fato WHERE observacao = ? ORDER BY codigo_lancamento, departamento",
            (fato.MARCA_LANCAMENTO_CC,)).fetchall()
    assert [(c, d, p, v) for c, d, p, v, *_ in linhas] == [
        (555, "CRECHESUAPE", "PROJ-A", -50000.0),
        (555, "ESCPE18", "", -50000.01),          # o centavo, na última parte
        (557, "(não apropriado)", "(não apropriado)", -80.0)]
    _c, _d, _p, _v, conta, quem, analise, pago = linhas[0]
    assert (conta, quem, analise, pago) == ("Sicredi LC", "IMOBILIARIA SUL", "DRE", True)


@pytest.mark.banco
def test_nao_conta_duas_vezes_o_que_ja_e_baixa_de_titulo(base):
    """Se o OMIE mandar o mesmo dinheiro como perna de título e como movimento
    solto (mesma conta, dia e valor), só a do título conta."""
    from app.apps.painel.sync import espelho
    with base.conexao() as conn:
        conn.execute("INSERT INTO movimentos (ncodtitulo, cnatureza, ncodcc, ddtpagamento,"
                     " nvalpago, cliquidado, sync_em) VALUES (1, 'P', 77, '10/01/2026', 50, '', 'x')")
        espelho.gravar_movimentos(conn, [_mov(558, "10/01/2026", 50.0)])
        fato.reconstruir_fato(conn)
        quantos = conn.execute("SELECT COUNT(*) FROM fato WHERE observacao = ?",
                               (fato.MARCA_LANCAMENTO_CC,)).fetchone()[0]
    assert quantos == 0


@pytest.mark.banco
def test_atualizar_um_periodo_rele_so_ele_e_de_uma_vez(base):
    from app.apps.painel.sync import espelho

    class Cliente:
        def __init__(self):
            self.pedidos = []

        def listar_movimentos(self, *, param_extra=None, **_k):
            self.pedidos.append(param_extra)
            yield 1, 1, 1, [_mov(560, "09/01/2026", 10.0)]

    with base.conexao() as conn:
        espelho.gravar_movimentos(conn, [_mov(559, "09/01/2026", 999.0),
                                         _mov(561, "20/02/2026", 7.0)])
    cli = Cliente()
    assert espelho.reler_periodo(dt.date(2026, 1, 9), dt.date(2026, 1, 9), cli=cli) == 1
    assert cli.pedidos == [{"dDtPagtoDe": "09/01/2026", "dDtPagtoAte": "09/01/2026"}]
    with base.conexao() as conn:
        dias = conn.execute("SELECT ddtpagamento, nvalpago::float8 FROM movimentos_sem_titulo"
                            " ORDER BY 1").fetchall()
    assert dias == [("09/01/2026", 10.0), ("20/02/2026", 7.0)]


@pytest.mark.banco
def test_o_periodo_e_validado_e_guardado(base, monkeypatch):
    from app.apps.painel import tarefas
    with pytest.raises(ValueError):
        tarefas.guardar_periodo("", "")
    with pytest.raises(ValueError):
        tarefas.guardar_periodo("2026-01-01", "2026-12-31")      # mais de 3 meses
    assert tarefas.guardar_periodo("2026-01-31", "2026-01-01") == (
        dt.date(2026, 1, 1), dt.date(2026, 1, 31))               # invertido, arruma
    assert tarefas._periodo_escolhido() == (dt.date(2026, 1, 1), dt.date(2026, 1, 31))
    monkeypatch.setattr(tarefas, "_iniciar_processo", lambda modo, eid: None)
    with base.conexao() as conn:
        conn.execute("TRUNCATE TABLE execucoes")
        conn.commit()
    r = tarefas.disparar("periodo", periodo={"de": "2026-01-09", "ate": "2026-01-09"})
    assert r["ok"] and "09/01/2026 a 09/01/2026" in r["descricao"]
    with base.conexao() as conn:
        conn.execute("TRUNCATE TABLE execucoes")
        conn.commit()
