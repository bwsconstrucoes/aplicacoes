# -*- coding: utf-8 -*-
"""
A apropriação dos lançamentos de conta corrente vem da consulta do lançamento
no OMIE, não do movimento financeiro.

07/10/2026: o arquivo cru de 09/01/2026 que o dono mandou mostrou os 193
movimentos do dia só com "detalhes" e "resumo" — nenhum com departamento. O
lançamento da Sicredi (100.000,01, metade CRECHESUAPE, metade ESCPE18 na tela
do OMIE) chegava ao painel sem obra.
"""
from __future__ import annotations

import pytest

from app.apps.painel.sync import fato
from app.apps.painel.sync.omie_client import OmieAPIError, OmieClient

# O movimento da Sicredi como veio no arquivo cru de 09/01/2026
SICREDI = {"detalhes": {"cCPFCNPJCliente": "38.240.209/0001-03", "cCodCateg": "2.01.01",
                        "cGrupo": "CONTA_CORRENTE_REC", "cNatureza": "P", "cOrigem": "EXTR",
                        "cStatus": "RECEBIDO", "cTipo": "99999", "dDtPagamento": "09/01/2026",
                        "nCodCC": 77, "nCodCliente": 900, "nCodMovCC": 11309733447,
                        "nCodTitulo": 0, "nValorMovCC": 100000.01},
           "resumo": {"nValPago": 100000.01}}
TRANSFERENCIA = {"detalhes": {"cCodCateg": "0.01.02", "cGrupo": "CONTA_CORRENTE_PAG",
                              "cNatureza": "P", "cOrigem": "TRAP", "cTipo": "TRA",
                              "dDtPagamento": "09/01/2026", "nCodCC": 77,
                              "nCodMovCC": 11100599458, "nCodTitulo": 0,
                              "nValorMovCC": 23972.14},
                 "resumo": {"nValPago": 23972.14}}


def test_acha_a_apropriacao_em_qualquer_nivel_da_resposta():
    # o formato provável da consulta do lançamento: como o IncluirLancCC manda
    direto = {"cabecalho": {"nCodCC": 77}, "departamentos": [
        {"cCodDep": "10", "nValDep": 50000.0}, {"cCodDep": "20", "nValDep": 50000.01}]}
    assert [c for c, _f in fato._departamentos_do_bruto(direto)] == ["10", "20"]
    # aninhada em outro bloco, e em percentual com outro nome
    aninhada = {"lancamento": {"detalhes": {"distribuicao": [
        {"cCodDep": "10", "nPerc": 30}, {"cCodDep": "20", "nPerc": 70}]}}}
    assert [c for c, _f in fato._departamentos_do_bruto(aninhada)] == ["10", "20"]
    assert fato._departamentos_do_bruto({"departamentos": []}) == []


def test_transferencia_entre_contas_vai_para_trf():
    import json
    assert fato._e_transferencia(json.dumps(TRANSFERENCIA), "0.01.02")
    assert fato._e_transferencia(None, "0.01.01")
    assert not fato._e_transferencia(json.dumps(SICREDI), "1.02.01")


def test_a_consulta_tenta_o_segundo_nome_se_o_omie_nao_conhecer_o_primeiro(monkeypatch):
    chamados = []

    def _call(self, url, call, param):
        chamados.append((call, param))
        if call == "ConsultaLancCC":
            raise OmieAPIError("SOAP-ENV:Client", "Method ConsultaLancCC not exists")
        return {"nCodLanc": param["nCodLanc"]}

    monkeypatch.setattr(OmieClient, "_call", _call)
    monkeypatch.setattr(OmieClient, "_consulta_lanc_cc", None)
    cli = OmieClient("k", "s")
    assert cli.consultar_lancamento_cc(11309733447) == {"nCodLanc": 11309733447}
    assert cli.consultar_lancamento_cc(1) == {"nCodLanc": 1}
    # da segunda vez já vai direto no nome que funcionou
    assert [c for c, _p in chamados] == ["ConsultaLancCC", "ConsultarLancCC", "ConsultarLancCC"]


def test_erro_de_negocio_nao_troca_de_nome(monkeypatch):
    def _call(self, url, call, param):
        raise OmieAPIError("SOAP-ENV:Client", "Lançamento não cadastrado")

    monkeypatch.setattr(OmieClient, "_call", _call)
    monkeypatch.setattr(OmieClient, "_consulta_lanc_cc", None)
    with pytest.raises(OmieAPIError, match="não cadastrado"):
        OmieClient("k", "s").consultar_lancamento_cc(5)


@pytest.fixture()
def base():
    import os

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
                  "contas_correntes", "depto_projeto", "apropriacao_lancamentos_cc"):
            conn.execute(f"TRUNCATE TABLE {t}")
        conn.execute("DELETE FROM config WHERE chave = 'projeto_da_obra'")
        conn.execute("INSERT INTO cat (codigo, descricao, grupo, codigo_dre)"
                     " VALUES ('2.01.01', 'Aluguel', 'Administrativas', '1')")
        conn.execute("INSERT INTO clientes (codigo, razao_social, cnpj_cpf)"
                     " VALUES (900, 'IMOBILIARIA SUL', '12345678000199')")
        conn.execute("INSERT INTO contas_correntes (codigo, descricao) VALUES (77, 'Sicredi LC')")
        conn.executemany("INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep,"
                         " cdesdep, nperdep, nvaldep) VALUES (?,?,?,?,100,1)",
                         [(1, 1, "10", "CRECHESUAPE"), (2, 1, "20", "ESCPE18")])
        conn.commit()
    yield painel_db
    with painel_db.conexao() as conn:
        for t in ("fato", "movimentos_sem_titulo", "apropriacao_lancamentos_cc"):
            conn.execute(f"TRUNCATE TABLE {t}")
        conn.commit()
    painel_db._engine = None


class _Omie:
    """O OMIE de mentira: responde a consulta do lançamento como o IncluirLancCC
    recebe — departamentos em valor."""
    pausa = 0

    def __init__(self, falhar=False):
        self.perguntados = []
        self.falhar = falhar

    def consultar_lancamento_cc(self, codigo):
        self.perguntados.append(codigo)
        if self.falhar:
            raise OmieAPIError("SOAP-ENV:Client", "Lançamento não cadastrado")
        return {"cabecalho": {"nCodCC": 77, "nValorLanc": 100000.01},
                "departamentos": [{"cCodDep": "10", "nValDep": 50000.0},
                                  {"cCodDep": "20", "nValDep": 50000.01}]}


@pytest.mark.banco
def test_o_lancamento_da_sicredi_entra_nas_duas_obras(base):
    from app.apps.painel.sync import apropriacao_cc, espelho
    omie = _Omie()
    with base.conexao() as conn:
        espelho.gravar_movimentos(conn, [SICREDI, TRANSFERENCIA])
        r = apropriacao_cc.buscar(conn, omie)
        # transferência entre contas não tem obra: nem é perguntada
        assert omie.perguntados == [11309733447]
        assert (r["lidos"], r["falhas"], r["parou"]) == (1, 0, "")
        # o que já foi respondido não é perguntado de novo
        assert apropriacao_cc.pendentes(conn) == []
        fato.reconstruir_fato(conn)
        linhas = conn.execute(
            "SELECT departamento, pago_recebido::float8, analise FROM fato"
            " WHERE observacao = ?",
            (fato.MARCA_LANCAMENTO_CC,)).fetchall()
    # ordenado aqui, não no banco: a ordem do "(" muda com o idioma do servidor
    assert sorted(linhas) == [("(não apropriado)", -23972.14, "TRF"),
                      ("CRECHESUAPE", -50000.0, "DRE"),
                      ("ESCPE18", -50000.01, "DRE")]


@pytest.mark.banco
def test_para_sozinho_se_o_omie_nao_responder_as_primeiras(base):
    import copy

    from app.apps.painel.sync import apropriacao_cc, espelho
    movimentos = []
    for i in range(8):
        mv = copy.deepcopy(SICREDI)
        mv["detalhes"]["nCodMovCC"] = 1000 + i
        movimentos.append(mv)
    omie = _Omie(falhar=True)
    with base.conexao() as conn:
        espelho.gravar_movimentos(conn, movimentos)
        r = apropriacao_cc.buscar(conn, omie)
        assert len(omie.perguntados) == apropriacao_cc.FALHAS_SEGUIDAS_NO_COMECO
        assert "não respondeu" in r["parou"]
        # quem falhou volta a ser perguntado, até o limite de tentativas
        assert len(apropriacao_cc.pendentes(conn)) == 8
        conn.execute("UPDATE apropriacao_lancamentos_cc SET tentativas = ?",
                     (apropriacao_cc.TENTATIVAS_POR_LANCAMENTO,))
        conn.commit()
        assert len(apropriacao_cc.pendentes(conn)) == 3


@pytest.mark.banco
def test_o_periodo_so_pergunta_pelos_dias_dele(base):
    import copy
    import datetime as dt

    from app.apps.painel.sync import apropriacao_cc, espelho
    outro = copy.deepcopy(SICREDI)
    outro["detalhes"].update(nCodMovCC=42, dDtPagamento="15/02/2026")
    with base.conexao() as conn:
        espelho.gravar_movimentos(conn, [SICREDI, outro])
        assert apropriacao_cc.pendentes(conn) == [42, 11309733447]   # mais recente antes
        assert apropriacao_cc.pendentes(conn, dt.date(2026, 1, 9),
                                        dt.date(2026, 1, 9)) == [11309733447]


def test_falha_na_apropriacao_vira_aviso_e_nao_derruba(monkeypatch):
    import contextlib

    from app.apps.painel import db as painel_db
    from app.apps.painel import tarefas
    from app.apps.painel.sync import apropriacao_cc
    monkeypatch.setattr(painel_db, "conexao", lambda: contextlib.nullcontext(object()))
    respostas = iter([{"lidos": 3, "falhas": 0, "pendentes": 3, "parou": ""},
                      {"lidos": 2, "falhas": 1, "pendentes": 3, "parou": ""},
                      {"lidos": 0, "falhas": 5, "pendentes": 9, "parou": "o OMIE não respondeu"}])
    monkeypatch.setattr(apropriacao_cc, "buscar", lambda *a, **k: next(respostas))
    anotar = lambda *a: None  # noqa: E731
    assert tarefas._ler_apropriacoes("rapida", anotar) == ""
    assert "1 lançamento(s)" in tarefas._ler_apropriacoes("rapida", anotar)
    assert "não respondeu" in tarefas._ler_apropriacoes("rapida", anotar)

    def explode(*a, **k):
        raise RuntimeError("OMIE fora")
    monkeypatch.setattr(apropriacao_cc, "buscar", explode)
    assert "OMIE fora" in tarefas._ler_apropriacoes("periodo", anotar)


def test_reler_um_ano_le_as_obras_de_todos_os_lancamentos_dele(monkeypatch):
    """07/10/2026, o dono: "são dados apenas deste ano que preciso hoje". A
    releitura de UM ano lê a apropriação de todos os lançamentos dele, sem o
    teto de 600 por atualização."""
    import contextlib
    import datetime as dt

    from app.apps.painel import db as painel_db
    from app.apps.painel import tarefas
    from app.apps.painel.sync import espelho
    from app.apps.painel.sync import fato as fato_mod

    monkeypatch.setattr(espelho, "definir_progresso", lambda *a, **k: None)
    monkeypatch.setattr(espelho, "sync_incremental", lambda *a, **k: None)
    monkeypatch.setattr(espelho, "atualizar_projetos", lambda *a, **k: 0)
    monkeypatch.setattr(tarefas, "_carimbar", lambda *a, **k: None)
    monkeypatch.setattr(tarefas, "_releitura_pendente", lambda *a: False)
    monkeypatch.setattr(painel_db, "conexao", lambda: contextlib.nullcontext(object()))
    monkeypatch.setattr(fato_mod, "reconstruir", lambda conn: (1, 1))
    monkeypatch.setattr(tarefas, "_fechar_execucao", lambda *a, **k: None)
    pedidos = []
    monkeypatch.setattr(tarefas, "_ler_apropriacoes",
                        lambda modo, anotar, *args: pedidos.append(args) or "")
    monkeypatch.setattr(espelho, "reler_pagamentos_por_ano",
                        lambda *a, **k: {"relidos": [2026], "pulados": [], "movimentos": 0})
    assert tarefas.executar_trabalho("pagamentos", 1)
    assert pedidos == [(dt.date(2026, 1, 1), dt.date(2026, 12, 31), True)]

    # todos os anos: fica o teto (seriam milhares de consultas numa rodada só)
    pedidos.clear()
    monkeypatch.setattr(espelho, "reler_pagamentos_por_ano",
                        lambda *a, **k: {"relidos": list(range(2015, 2027)), "pulados": [],
                                         "movimentos": 0})
    assert tarefas.executar_trabalho("pagamentos", 2)
    assert pedidos == [(None, None, False)]
