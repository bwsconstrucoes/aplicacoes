# -*- coding: utf-8 -*-
"""
O andamento da atualização: o que a tela mostra enquanto a carga roda, e o que
ela mostra quando a carga morre no meio.

Este arquivo nasce de um episódio concreto. A primeira carga de verdade levou
horas e a tela dizia apenas "baixando" — sem página, sem contagem, sem nada.
Pior: uma publicação de código reiniciou o serviço no meio, a carga morreu, e a
tela voltou a mostrar **a falha anterior**, de horas antes, como se fosse a
atual. O dono passou um tempo diagnosticando um erro que já estava resolvido.

O que se prova aqui:
  - o andamento é gravado no banco, não só na memória — sobrevive a recarregar
    a página, a outro aparelho e ao processo morrer;
  - execução que parou de dar sinal é reconhecida como interrompida, com
    quantos minutos de silêncio;
  - uma execução nova encerra a órfã, para o histórico não ficar preso;
  - a atualização não cai se o registro do andamento falhar. Perder o andamento
    é chato; perder a carga é caro.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture()
def sem_execucoes():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner

    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), f"migração falhou: {resultado}"

    def _limpar():
        with painel_db.conexao() as conn:
            conn.execute("TRUNCATE TABLE execucoes")
            conn.commit()

    _limpar()
    yield
    _limpar()
    painel_db._engine = None


def _abrir(modo="carga_inicial", disparo="manual"):
    from app.apps.painel import tarefas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        return tarefas._abrir_execucao(conn, modo, disparo)


def _envelhecer(execucao_id, minutos):
    """Empurra o último sinal para trás no tempo, para simular silêncio."""
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute(
            "UPDATE execucoes SET visto_em = now() - (? || ' minutes')::interval "
            " WHERE id = ?", (str(minutos), execucao_id))
        conn.commit()


# ---------------------------------------------------------------------------
# 1. O andamento aparece
# ---------------------------------------------------------------------------
def test_o_andamento_e_gravado_e_pode_ser_lido_de_fora(sem_execucoes):
    """Gravado no banco é o que faz o andamento sobreviver a recarregar a
    página — e ficar visível de outro aparelho."""
    from app.apps.painel import consultas, tarefas

    execucao_id = _abrir()
    tarefas._carimbar(execucao_id, "baixando contas a receber do OMIE",
                      "página 340 de 1.200 — 81.000 títulos")

    andamento = consultas.execucao_em_andamento()
    assert andamento["id"] == execucao_id
    assert andamento["etapa"] == "baixando contas a receber do OMIE"
    assert "página 340 de 1.200" in andamento["progresso"]
    assert andamento["viva"] is True
    assert andamento["silencio_minutos"] < 1


def test_sem_execucao_aberta_nao_ha_andamento(sem_execucoes):
    from app.apps.painel import consultas
    assert consultas.execucao_em_andamento() is None


def test_o_estado_da_tela_reflete_o_banco(sem_execucoes):
    """A tela pergunta ao `tarefas.estado()`. Mesmo sem thread neste processo,
    ele tem de dizer "rodando" se o banco disser que há execução viva — é o
    caso de a página ser aberta noutro navegador."""
    from app.apps.painel import tarefas

    execucao_id = _abrir()
    tarefas._carimbar(execucao_id, "baixando a base inteira do OMIE", "página 2 de 900")

    estado = tarefas.estado()
    assert estado["rodando"] is True
    assert estado["detalhe"]["etapa"] == "baixando a base inteira do OMIE"
    assert estado["detalhe"]["detalhe_progresso"] == "página 2 de 900"
    assert estado["interrompida"] is None


# ---------------------------------------------------------------------------
# 2. A carga que morreu no meio
# ---------------------------------------------------------------------------
def test_execucao_que_parou_de_dar_sinal_e_dada_por_interrompida(sem_execucoes):
    """Exatamente o que aconteceu: uma publicação reiniciou o serviço e a carga
    morreu. Antes, isso ficava invisível."""
    from app.apps.painel import consultas, tarefas

    execucao_id = _abrir()
    tarefas._carimbar(execucao_id, "baixando a base inteira do OMIE", "página 40 de 900")
    _envelhecer(execucao_id, 25)

    andamento = consultas.execucao_em_andamento()
    assert andamento["viva"] is False
    assert andamento["silencio_minutos"] >= 24

    estado = tarefas.estado()
    assert estado["rodando"] is False
    assert estado["interrompida"] is not None
    assert estado["interrompida"]["etapa"] == "baixando a base inteira do OMIE"


def test_uma_pagina_lenta_nao_e_confundida_com_servidor_caido(sem_execucoes):
    """O limite é generoso de propósito: o OMIE tem páginas lentas, e dizer
    "interrompida" numa carga que está viva mandaria o dono rodar tudo de novo
    à toa."""
    from app.apps.painel import consultas

    execucao_id = _abrir()
    _envelhecer(execucao_id, 3)
    assert consultas.execucao_em_andamento()["viva"] is True


def test_execucao_nova_encerra_a_orfa(sem_execucoes):
    """Sem isto, a interrompida ficaria "em aberto" para sempre e a tela nunca
    mais mostraria o resultado de nenhuma atualização."""
    from app.apps.painel import consultas, tarefas
    from app.apps.painel.db import consultar

    orfa = _abrir()
    _envelhecer(orfa, 30)
    nova = _abrir()

    assert nova != orfa
    (fim, ok, mensagem) = consultar(
        "SELECT fim, ok, mensagem FROM execucoes WHERE id = ?", (orfa,))[0]
    assert fim is not None
    assert ok is False
    assert "Interrompida" in mensagem
    assert "rodar de novo" in mensagem

    # e a que está aberta agora é a nova
    assert consultas.execucao_em_andamento()["id"] == nova


def test_a_interrompida_nao_se_disfarca_de_ultima_atualizacao(sem_execucoes):
    """`atualizado_em` só olha execuções terminadas. Enquanto a órfã não for
    encerrada, ela não pode aparecer como se fosse o último resultado."""
    from app.apps.painel import consultas

    _abrir()
    assert consultas.atualizado_em() is None


# ---------------------------------------------------------------------------
# 3. Registrar o andamento nunca derruba a carga
# ---------------------------------------------------------------------------
def test_falha_ao_gravar_o_andamento_nao_interrompe_nada(sem_execucoes, monkeypatch):
    """Perder o andamento é chato; perder uma carga de horas é caro."""
    from app.apps.painel import tarefas

    def _explodir(*_a, **_k):
        raise RuntimeError("banco fora do ar")

    monkeypatch.setattr(tarefas, "conexao", _explodir, raising=False)
    import app.apps.painel.db as painel_db
    monkeypatch.setattr(painel_db, "conexao", _explodir)

    tarefas._carimbar(1, "etapa qualquer", "detalhe")   # não pode levantar


def test_texto_longo_no_andamento_e_cortado(sem_execucoes):
    """A coluna tem limite prático; um detalhe enorme não pode quebrar o
    UPDATE bem no meio de uma carga."""
    from app.apps.painel import consultas, tarefas

    execucao_id = _abrir()
    tarefas._carimbar(execucao_id, "e" * 500, "d" * 500)
    andamento = consultas.execucao_em_andamento()
    assert len(andamento["etapa"]) == 200
    assert len(andamento["progresso"]) == 200


# ---------------------------------------------------------------------------
# 4. A mesma interrupção não é contada duas vezes
# ---------------------------------------------------------------------------
# A tela de Configurações tem dois lugares que falam de atualização: a linha
# "Última atualização" e a caixa vermelha "A atualização anterior foi
# interrompida". Quando duas cargas seguidas morrem — e morrem em série, porque
# a causa é o serviço reiniciar — as duas mostravam a MESMA frase, sobre duas
# execuções diferentes. Quem lia via dois erros e ia procurar dois problemas.
def test_execucao_que_morreu_e_reconhecivel_depois_de_encerrada(sem_execucoes):
    """Quem chega ao fim sozinho zera a etapa; quem foi encerrado pelo faxineiro
    de órfãs deixa a etapa preenchida. É esse o sinal que distingue as duas —
    sem ele não dá para saber, depois, se a execução terminou ou morreu."""
    from app.apps.painel import tarefas
    from app.apps.painel.db import conexao

    morreu = _abrir()
    tarefas._carimbar(morreu, "baixando títulos", "página 12")
    with conexao() as conn:
        tarefas._fechar_execucoes_orfas(conn)

    concluiu = _abrir()
    with conexao() as conn:
        tarefas._fechar_execucao(conn, concluiu, True, "tudo certo", 10)

    with conexao() as conn:
        cur = conn.execute("SELECT id, etapa FROM execucoes ORDER BY id")
        etapas = dict(cur.fetchall())
        cur.close()
    assert etapas[morreu] is not None, "a que morreu tem de guardar a etapa"
    assert etapas[concluiu] is None, "a que terminou sozinha zera a etapa"


def test_a_linha_de_cima_ignora_interrompida_quando_a_caixa_ja_avisa(sem_execucoes):
    """Com `so_concluidas`, "Última atualização" volta a responder a pergunta
    útil — quando a base foi atualizada de verdade —, em vez de repetir a
    interrupção que a caixa vermelha já está contando."""
    from app.apps.painel import consultas, tarefas
    from app.apps.painel.db import conexao

    # uma que deu certo, ontem
    boa = _abrir(modo="rapida")
    with conexao() as conn:
        tarefas._fechar_execucao(conn, boa, True, "27 mil linhas", 27000)

    # e depois uma que morreu no meio
    morreu = _abrir(modo="rapida")
    tarefas._carimbar(morreu, "baixando títulos", "página 12")
    with conexao() as conn:
        tarefas._fechar_execucoes_orfas(conn)

    # sem o filtro, a linha de cima mostra a interrupção — que a caixa também
    # vai mostrar. É a duplicidade que o dono viu.
    assert "Interrompida" in consultas.atualizado_em()["mensagem"]

    # com o filtro, ela volta a falar da última atualização que de fato terminou
    concluida = consultas.atualizado_em(so_concluidas=True)
    assert concluida["mensagem"] == "27 mil linhas"
    assert concluida["ok"] is True


def test_a_tela_de_configuracoes_conta_a_interrupcao_uma_vez_so(sem_execucoes, monkeypatch):
    """O teste que fecha o caso: a tela inteira, montada, com uma carga morta
    aberta e uma interrupção já encerrada antes dela."""
    from app.apps.painel import tarefas
    from app.apps.painel.db import conexao

    antiga = _abrir(modo="rapida")
    tarefas._carimbar(antiga, "baixando títulos", "página 3")
    with conexao() as conn:
        tarefas._fechar_execucoes_orfas(conn)

    # a atual: aberta e sem dar sinal há muito tempo
    atual = _abrir(modo="rapida")
    tarefas._carimbar(atual, "refazendo os números", "bloco 4")
    _envelhecer(atual, 120)

    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "segredo-de-teste"})

    html = cliente.get("/painel/configuracoes").get_data(as_text=True)
    assert html.count("foi interrompida") == 1, "a interrupção aparece uma vez só"
    assert "Interrompida: o serviço reiniciou" not in html, \
        "a frase do faxineiro de órfãs não pode aparecer junto com a caixa"
    assert "Nenhuma atualização feita ainda" not in html, \
        "houve atualização — ela é que morreu; dizer que não houve é falso"


# ---------------------------------------------------------------------------
# 5. Gravar um cenário de rateio
# ---------------------------------------------------------------------------
# Precisa de banco de verdade: o que se prova é o UPDATE — que ele altera só os
# parâmetros e que os ids sobrevivem. O dublê de sessão ignora WHERE e não veria
# nem uma coisa nem outra.
@pytest.fixture()
def com_regras(sem_execucoes):
    """Duas regras gravadas, com grupos e categorias escolhidos."""
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM regras")
        conn.execute(
            "INSERT INTO regras (nome, depto, todas, grupos, categorias, pct,"
            " escopo, mes_ini, mes_fim, ativo) VALUES"
            " ('Matriz','ADM MATRIZ',0,'[\"Despesas Administrativas\"]','[]',"
            "  100,'AMBAS','','',1),"
            " ('Filial','ADM FILIAL',1,'[]','[]',80,'FILIAL','2025-01','',1)")
        conn.commit()
    yield
    with conexao() as conn:
        conn.execute("DELETE FROM regras")
        conn.commit()


def test_gravar_o_cenario_mexe_nos_parametros_e_preserva_o_resto(com_regras):
    """Grupos e categorias são escolhidos na tela de Regras, onde existe a lista.
    Gravar um cenário não pode encostar neles — nem trocar os ids.

    A tela antiga fazia isso com um DELETE de todas as regras seguido de um
    INSERT de todas: os ids mudavam a cada gravação."""
    from app.apps.painel import prestacao, prestacao_dados

    antes = prestacao_dados.regras()
    ids_antes = [r["id"] for r in antes]
    grupos_antes = [r["grupos"] for r in antes]

    cenario = prestacao.regras_do_cenario(antes, {
        str(antes[0]["id"]): {"pct": "35", "escopo": "MATRIZ"},
        str(antes[1]["id"]): {"ativo": 0},
    })
    assert prestacao_dados.salvar_parametros_das_regras(cenario) == 2

    depois = prestacao_dados.regras()
    assert [r["id"] for r in depois] == ids_antes, "os ids têm de sobreviver"
    assert [r["grupos"] for r in depois] == grupos_antes, \
        "grupos são da tela de Regras; o cenário não os toca"
    assert float(depois[0]["pct"]) == 35.0
    assert depois[0]["escopo"] == "MATRIZ"
    assert int(depois[1]["ativo"]) == 0
    # e a que ninguém desligou continua ligada
    assert int(depois[0]["ativo"]) == 1


def test_gravar_cenario_vazio_nao_apaga_regra_nenhuma(com_regras):
    """Um POST sem regra nenhuma (formulário perdido, sessão estranha) não pode
    virar um DELETE silencioso das regras que dividem o resultado."""
    from app.apps.painel import prestacao_dados

    assert prestacao_dados.salvar_parametros_das_regras([]) == 0
    assert len(prestacao_dados.regras()) == 2
