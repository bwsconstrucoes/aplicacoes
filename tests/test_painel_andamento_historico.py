# -*- coding: utf-8 -*-
"""
A história das atualizações — o que rodou, até onde foi, onde parou, o que fazer.

Pedido do dono em 06/10/2026: *"essa tela de atualizar os dados era para ter
mais informativo (…) até onde atualizou, onde é que interrompeu, foram quantas
páginas, quantas linhas, o que ficou pendente, qual foi a última tentativa (…)
e qual ação eu preciso fazer se der um erro. Ninguém entende direito."*
"""
from __future__ import annotations

import os

import pytest

from app.apps.painel import andamento as a


def _e(**k):
    base = {"tipo": "pagamentos", "fim": None, "ok": None, "viva": True,
            "mensagem": "", "etapa": "", "progresso": ""}
    base.update(k)
    return base


def test_a_situacao_sai_do_que_a_execucao_guardou():
    assert a.situacao(_e()) == a.RODANDO
    assert a.situacao(_e(viva=False)) == a.PARADA
    assert a.situacao(_e(fim=1, ok=True)) == a.CONCLUIDA
    assert a.situacao(_e(fim=1, ok=True, mensagem="… ATENÇÃO: planilha")) == a.COM_AVISO
    assert a.situacao(_e(fim=1, ok=False, etapa="lendo")) == a.INTERROMPIDA
    assert a.situacao(_e(fim=1, ok=False)) == a.FALHOU


def test_os_passos_dizem_o_que_fez_onde_parou_e_o_que_nao_chegou():
    gravados = [{"etapa": a.TITULOS_A_PAGAR, "detalhe": "página 3 de 3"},
                {"etapa": a.TITULOS_A_RECEBER, "detalhe": "página 1 de 1"},
                {"etapa": a.PAGAMENTOS, "detalhe": "desde 01/01/2015: página 37 de 420"}]
    passos = a.passos_da_execucao(_e(viva=False, etapa=a.PAGAMENTOS), gravados)
    estados = {p["etapa"]: p["estado"] for p in passos}
    assert estados[a.TITULOS_A_PAGAR] == "feito"
    assert estados[a.PAGAMENTOS] == "parou_aqui"
    assert estados[a.RECALCULO] == "nao_chegou"
    parou = next(p for p in passos if p["estado"] == "parou_aqui")
    assert "37 de 420" in parou["detalhe"]


def test_sem_passos_gravados_ainda_mostra_onde_parou():
    """Execução antiga ou migração 020 pendente: sabe-se só a etapa."""
    passos = a.passos_da_execucao(_e(viva=False, etapa=a.PAGAMENTOS,
                                     progresso="página 9 de 40"), [])
    parou = [p for p in passos if p["estado"] == "parou_aqui"]
    assert parou and parou[0]["etapa"] == a.PAGAMENTOS
    assert parou[0]["detalhe"] == "página 9 de 40"


def test_o_que_fazer_muda_com_a_situacao():
    assert "retoma sozinho" in a.o_que_fazer(_e(viva=False), "Reler")
    assert "Já foi retomada 2 vezes" in a.o_que_fazer(_e(viva=False), "Reler", 2, 2)
    assert "Nada: está rodando" in a.o_que_fazer(_e(), "Reler")
    assert "me mande a mensagem" in a.o_que_fazer(_e(fim=1, ok=False), "Reler")


# ---------------------------------------------------------------------------
# Com banco de verdade
# ---------------------------------------------------------------------------
@pytest.fixture()
def banco():
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
        conn.execute("TRUNCATE TABLE execucoes")
        conn.execute("TRUNCATE TABLE execucao_passos")
        conn.commit()
    yield painel_db
    painel_db._engine = None


@pytest.mark.banco
def test_o_carimbo_grava_os_passos_e_o_historico_os_conta(banco, monkeypatch):
    from app.apps.painel import tarefas
    monkeypatch.setattr(tarefas, "_iniciar_processo", lambda modo, eid: None)
    eid = tarefas.disparar("pagamentos")["execucao"]
    tarefas._carimbar(eid, a.TITULOS_A_PAGAR, "página 1 de 2")
    tarefas._carimbar(eid, a.TITULOS_A_PAGAR, "página 2 de 2")
    tarefas._carimbar(eid, a.PAGAMENTOS, "desde 01/01/2015: página 37 de 420")
    with banco.conexao() as conn:
        conn.execute("UPDATE execucoes SET visto_em = now() - interval '30 minutes'")
        conn.commit()

    h = a.historico()
    (e,) = h["execucoes"]
    assert h["tem_passos"]
    assert e["rotulo"] == "Reler todos os pagamentos"
    assert e["situacao"] == a.PARADA
    estados = {p["etapa"]: (p["estado"], p["detalhe"]) for p in e["passos"]}
    assert estados[a.TITULOS_A_PAGAR] == ("feito", "página 2 de 2")
    assert estados[a.PAGAMENTOS][0] == "parou_aqui"
    assert "retoma sozinho" in e["o_que_fazer"]
    modo = next(m for m in h["por_modo"] if m["tipo"] == "pagamentos")
    assert modo["ultima_concluida"] is None and modo["ultima_tentativa"]["id"] == eid


@pytest.mark.banco
def test_a_tela_de_configuracoes_mostra_a_historia(banco, monkeypatch):
    from app.apps.painel import tarefas
    monkeypatch.setattr(tarefas, "_iniciar_processo", lambda modo, eid: None)
    eid = tarefas.disparar("completa")["execucao"]
    tarefas._carimbar(eid, a.EXCLUIDOS, "contapagar: página 5 de 80")
    with banco.conexao() as conn:
        conn.execute("UPDATE execucoes SET visto_em = now() - interval '30 minutes'")
        conn.commit()
    monkeypatch.setenv("PAINEL_SENHA", "senha-do-dono-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "senha-do-dono-teste"})
    html = cliente.get("/painel/configuracoes").get_data(as_text=True)
    assert "O que aconteceu nas atualizações" in html
    assert "Atualização completa" in html and "Parou de dar sinal" in html
    assert "contapagar: página 5 de 80" in html
    assert "O que fazer:" in html
