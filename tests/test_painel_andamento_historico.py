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


def test_sem_passos_gravados_mostra_so_onde_esta_e_nao_inventa_o_resto():
    """Execução antiga ou migração 020 pendente: sabe-se só a etapa. Em
    06/10/2026 a tela marcou seis passos como "feitos" que ninguém viu."""
    passos = a.passos_da_execucao(_e(viva=False, etapa="baixando o que mudou no OMIE",
                                     progresso="página 9 de 40"), [])
    assert passos == [{"etapa": "baixando o que mudou no OMIE", "estado": "parou_aqui",
                       "detalhe": "página 9 de 40", "inicio": None, "visto_em": None}]
    rodando = a.passos_da_execucao(_e(etapa="x", progresso="p"), [])
    assert [p["estado"] for p in rodando] == ["andando"]


def test_a_espera_pedida_pelo_omie_avisa_e_mantem_a_carga_viva(monkeypatch):
    """06/10/2026: a página "parada" na 247 era o OMIE mandando esperar — sem
    aviso, e sem sinal de vida por até 10 minutos."""
    from app.apps.painel.sync import espelho, omie_client
    avisos, dormidas = [], []
    monkeypatch.setattr(omie_client.time, "sleep", dormidas.append)
    espelho.definir_progresso(lambda etapa, detalhe: avisos.append((etapa, detalhe)))
    try:
        espelho._progresso(a.PAGAMENTOS, "desde 01/01/2015: página 247 de 2716")
        omie_client._esperar(75, "limite de consultas")
    finally:
        espelho.definir_progresso(None)
    assert dormidas == [30.0, 30.0, 15.0]
    esperas = [d for e, d in avisos if "esperando o OMIE" in d]
    assert len(esperas) == 3 and all(e == a.PAGAMENTOS for e, d in avisos)
    assert esperas[0] == ("desde 01/01/2015: página 247 de 2716 — esperando o OMIE "
                          "liberar (limite de consultas) — faltam 75 s")
    assert omie_client._aviso_de_espera is None     # desligado no fim


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
