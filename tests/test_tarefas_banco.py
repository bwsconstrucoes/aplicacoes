"""A fila de trabalho em segundo plano — com banco de verdade.

Nasceu da pergunta do dono em 08/09/2026: *"e quando essa base de dados for
crescendo? Como é que é a estratégia de manter isso rápido?"*. A resposta que
mais rende não é máquina maior — é parar de fazer trabalho pesado enquanto
alguém espera a tela.

Com banco porque tudo que importa aqui é do banco: tomar a próxima da fila sem
que dois peguem a mesma, sobreviver ao reinício do serviço, e não deixar um
trabalho preso em "executando" para sempre.

O que se prova:

  1. Enfileirar volta na hora, e trabalho que ninguém sabe fazer é recusado na
     entrada — não vira linha eterna esperando um executor que não existe.
  2. O trabalho executa, grava o resultado e o andamento que a tela mostra.
  3. Falha volta para a fila enquanto houver tentativa; depois do teto para e
     explica. Repetir para sempre esconderia o defeito.
  4. Quem morreu no meio (o serviço reiniciou) volta para a fila. Sem isso
     ficaria "executando" para sempre e ninguém saberia que precisa refazer.
  5. Trabalho que uma tela dispara ao abrir não duplica: dez pessoas abrindo a
     agenda não criam dez recálculos iguais.
  6. Cancelar só vale para o que ainda não começou.
  7. Cada um vê o que pediu; quem já enxerga o sistema inteiro vê a fila toda.
  8. A importação do Pipefy e a agenda passaram MESMO a usar a fila — que é a
     razão de tudo isto existir.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum import tarefas
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Usuario
from app.apps.erp.db.models.financeiro import Tarefa
from tests.conftest import como

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    admin = Usuario(nome="Admin da fila", email="fila.admin@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    obreiro = Usuario(nome="Administrativo de obra", email="fila.obra@teste.local",
                      ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                      perfil=P.ADMINISTRATIVO_OBRA)
    s.add_all([admin, obreiro])
    s.flush()
    return {"s": s, "admin": admin, "obreiro": obreiro}


@pytest.fixture(autouse=True)
def _executores_de_teste():
    """Executores só deste arquivo, tirados do registro no fim.

    Sem a limpeza eles vazariam para os outros testes — e um registro global
    sujo é o tipo de coisa que faz um teste passar sozinho e falhar em conjunto.
    """
    guardados = dict(tarefas._EXECUTORES)
    yield
    tarefas._EXECUTORES.clear()
    tarefas._EXECUTORES.update(guardados)


# ---------------------------------------------------------------------------
# 1. ENFILEIRAR
# ---------------------------------------------------------------------------
def test_enfileirar_volta_na_hora_com_o_trabalho_pendente(cenario):
    tarefas.registrar("somar", lambda s, p, a: {"total": p["a"] + p["b"]})
    t = tarefas.enfileirar(cenario["s"], "somar", {"a": 2, "b": 3},
                           rotulo="Somar dois números", usuario=cenario["admin"])
    assert t.situacao == "PENDENTE"
    assert t.rotulo == "Somar dois números"
    assert t.usuario_id == cenario["admin"].id


def test_trabalho_que_ninguem_sabe_fazer_e_recusado_na_entrada(cenario):
    """Aceitar criaria uma linha que nunca sai de PENDENTE, e a pessoa ficaria
    olhando para ela sem entender."""
    with pytest.raises(ErroValidacao) as e:
        tarefas.enfileirar(cenario["s"], "inventado", {}, rotulo="Qualquer coisa")
    assert "desconhecido" in str(e.value).lower()


def test_todo_trabalho_precisa_de_um_nome_que_a_pessoa_leia(cenario):
    tarefas.registrar("somar", lambda s, p, a: {})
    with pytest.raises(ErroValidacao):
        tarefas.enfileirar(cenario["s"], "somar", {}, rotulo="   ")


# ---------------------------------------------------------------------------
# 2. EXECUTAR
# ---------------------------------------------------------------------------
def test_o_trabalho_executa_e_guarda_o_resultado(cenario):
    tarefas.registrar("somar", lambda s, p, a: {"total": p["a"] + p["b"]})
    t = tarefas.enfileirar(cenario["s"], "somar", {"a": 2, "b": 40},
                           rotulo="Somar", usuario=cenario["admin"])
    pronto = tarefas.executar_agora(cenario["s"], t.id)
    assert pronto.situacao == "CONCLUIDA"
    assert pronto.resultado == {"total": 42}
    assert pronto.concluido_em is not None


def test_o_andamento_fica_gravado_para_a_tela_mostrar(cenario):
    def devagar(s, p, andamento):
        andamento(7, 10, "Card 7 de 10")
        return {"ok": True}

    tarefas.registrar("devagar", devagar)
    t = tarefas.enfileirar(cenario["s"], "devagar", {}, rotulo="Devagar",
                           usuario=cenario["admin"])
    pronto = tarefas.executar_agora(cenario["s"], t.id)
    assert (pronto.passo, pronto.total) == (7, 10)
    assert pronto.mensagem == "Card 7 de 10"


def test_executar_duas_vezes_o_mesmo_trabalho_e_recusado(cenario):
    tarefas.registrar("somar", lambda s, p, a: {"total": 1})
    t = tarefas.enfileirar(cenario["s"], "somar", {}, rotulo="Somar",
                           usuario=cenario["admin"])
    tarefas.executar_agora(cenario["s"], t.id)
    with pytest.raises(ErroValidacao):
        tarefas.executar_agora(cenario["s"], t.id)


# ---------------------------------------------------------------------------
# 3. QUANDO FALHA
# ---------------------------------------------------------------------------
def test_falha_volta_para_a_fila_ate_o_teto_e_depois_para_e_explica(cenario):
    s = cenario["s"]
    tentativas = {"n": 0}

    def quebra(sessao, p, andamento):
        tentativas["n"] += 1
        raise RuntimeError("o Pipefy não respondeu")

    tarefas.registrar("quebra", quebra)
    t = tarefas.enfileirar(s, "quebra", {}, rotulo="Vai falhar",
                           usuario=cenario["admin"])

    for volta in range(1, tarefas.MAX_TENTATIVAS):
        depois = tarefas.executar_agora(s, t.id)
        assert depois.situacao == "PENDENTE", "ainda há tentativa — tem de voltar para a fila"
        assert "Pipefy" in (depois.erro or "")

    final = tarefas.executar_agora(s, t.id)
    assert final.situacao == "FALHADA"
    assert final.tentativas == tarefas.MAX_TENTATIVAS
    assert "Pipefy" in (final.erro or ""), "o motivo tem de sobreviver à desistência"
    assert tentativas["n"] == tarefas.MAX_TENTATIVAS


def test_tipo_sem_executor_registrado_falha_dizendo_isso(cenario):
    """Acontece quando um trabalho ficou na fila e o serviço voltou sem aquele
    executor. Falhar com recado é melhor do que ficar rodando em falso."""
    s = cenario["s"]
    tarefas.registrar("some", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "some", {}, rotulo="Some", usuario=cenario["admin"])
    del tarefas._EXECUTORES["some"]
    final = tarefas.executar_agora(s, t.id)
    assert final.situacao == "FALHADA"
    assert "executor" in (final.erro or "").lower()


# ---------------------------------------------------------------------------
# 4. QUEM MORREU NO MEIO
# ---------------------------------------------------------------------------
def test_trabalho_interrompido_pelo_reinicio_volta_para_a_fila(cenario):
    s = cenario["s"]
    tarefas.registrar("longo", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "longo", {}, rotulo="Longo", usuario=cenario["admin"])
    t.situacao = "EXECUTANDO"
    t.tentativas = 1
    t.batida_em = datetime.now(timezone.utc) - timedelta(
        minutes=tarefas.SEM_SINAL_MINUTOS + 5)
    s.flush()

    assert tarefas.recuperar_orfas(s) == 1
    s.flush()
    assert s.get(Tarefa, t.id).situacao == "PENDENTE"


def test_quem_esta_dando_sinal_de_vida_nao_e_recuperado(cenario):
    """Leitura de IA demorada é lenta, não morta — recuperar seria refazer o
    trabalho por cima de quem ainda está fazendo."""
    s = cenario["s"]
    tarefas.registrar("longo", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "longo", {}, rotulo="Longo", usuario=cenario["admin"])
    t.situacao = "EXECUTANDO"
    t.batida_em = datetime.now(timezone.utc) - timedelta(minutes=1)
    s.flush()
    assert tarefas.recuperar_orfas(s) == 0


def test_reiniciar_demais_derruba_o_trabalho_em_vez_de_repetir_para_sempre(cenario):
    s = cenario["s"]
    tarefas.registrar("longo", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "longo", {}, rotulo="Longo", usuario=cenario["admin"])
    t.situacao = "EXECUTANDO"
    t.tentativas = tarefas.MAX_TENTATIVAS
    t.batida_em = datetime.now(timezone.utc) - timedelta(hours=2)
    s.flush()
    tarefas.recuperar_orfas(s)
    s.flush()
    depois = s.get(Tarefa, t.id)
    assert depois.situacao == "FALHADA"
    assert "reiniciou" in (depois.erro or "")


# ---------------------------------------------------------------------------
# 5. NÃO DUPLICAR O QUE A TELA DISPARA AO ABRIR
# ---------------------------------------------------------------------------
def test_dez_pessoas_abrindo_a_agenda_nao_criam_dez_recalculos(cenario):
    s = cenario["s"]
    tarefas.registrar("recalcular", lambda sessao, p, a: {})
    primeira = tarefas.enfileirar_unico(s, "recalcular", {}, rotulo="Recalcular")
    assert primeira is not None
    for _ in range(9):
        assert tarefas.enfileirar_unico(s, "recalcular", {}, rotulo="Recalcular") is None


def test_o_que_acabou_de_ser_calculado_nao_e_recalculado(cenario):
    s = cenario["s"]
    tarefas.registrar("recalcular", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "recalcular", {}, rotulo="Recalcular")
    tarefas.executar_agora(s, t.id)
    assert tarefas.enfileirar_unico(s, "recalcular", {}, rotulo="Recalcular",
                                    frescor_minutos=10) is None


def test_calculo_velho_e_refeito(cenario):
    s = cenario["s"]
    tarefas.registrar("recalcular", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "recalcular", {}, rotulo="Recalcular")
    pronta = tarefas.executar_agora(s, t.id)
    pronta.concluido_em = datetime.now(timezone.utc) - timedelta(hours=3)
    s.flush()
    assert tarefas.enfileirar_unico(s, "recalcular", {}, rotulo="Recalcular",
                                    frescor_minutos=10) is not None


# ---------------------------------------------------------------------------
# 6. CANCELAR E REPETIR
# ---------------------------------------------------------------------------
def test_cancelar_tira_da_fila_o_que_nao_comecou(cenario):
    s = cenario["s"]
    tarefas.registrar("somar", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "somar", {}, rotulo="Somar", usuario=cenario["admin"])
    tarefas.cancelar(s, t.id, usuario=cenario["admin"])
    assert s.get(Tarefa, t.id).situacao == "CANCELADA"


def test_o_que_ja_comecou_nao_e_interrompido(cenario):
    """Matar no meio deixaria parte do serviço feita sem ninguém saber qual."""
    s = cenario["s"]
    tarefas.registrar("somar", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "somar", {}, rotulo="Somar", usuario=cenario["admin"])
    t.situacao = "EXECUTANDO"
    s.flush()
    with pytest.raises(ErroValidacao) as e:
        tarefas.cancelar(s, t.id, usuario=cenario["admin"])
    assert "já começou" in str(e.value)


def test_repetir_so_vale_para_o_que_falhou(cenario):
    s = cenario["s"]
    tarefas.registrar("somar", lambda sessao, p, a: {"ok": True})
    t = tarefas.enfileirar(s, "somar", {}, rotulo="Somar", usuario=cenario["admin"])
    tarefas.executar_agora(s, t.id)
    with pytest.raises(ErroValidacao):
        tarefas.tentar_de_novo(s, t.id, usuario=cenario["admin"])


def test_repetir_zera_as_tentativas(cenario):
    s = cenario["s"]
    tarefas.registrar("quebra", lambda sessao, p, a: (_ for _ in ()).throw(RuntimeError("x")))
    t = tarefas.enfileirar(s, "quebra", {}, rotulo="Quebra", usuario=cenario["admin"])
    for _ in range(tarefas.MAX_TENTATIVAS):
        tarefas.executar_agora(s, t.id)
    assert s.get(Tarefa, t.id).situacao == "FALHADA"
    tarefas.tentar_de_novo(s, t.id, usuario=cenario["admin"])
    depois = s.get(Tarefa, t.id)
    assert (depois.situacao, depois.tentativas, depois.erro) == ("PENDENTE", 0, None)


# ---------------------------------------------------------------------------
# 7. QUEM VÊ O QUÊ
# ---------------------------------------------------------------------------
def test_cada_um_ve_o_que_pediu(app_real, cenario):
    s = cenario["s"]
    tarefas.registrar("somar", lambda sessao, p, a: {})
    tarefas.enfileirar(s, "somar", {}, rotulo="Do admin", usuario=cenario["admin"])
    tarefas.enfileirar(s, "somar", {}, rotulo="Do obreiro", usuario=cenario["obreiro"])
    s.flush()

    rotulos = [t["rotulo"] for t in
               como(app_real, cenario["obreiro"].id).get("/erp/api/tarefas")
               .get_json()["tarefas"]]
    assert rotulos == ["Do obreiro"]


def test_quem_enxerga_o_sistema_inteiro_ve_a_fila_toda(app_real, cenario):
    s = cenario["s"]
    tarefas.registrar("somar", lambda sessao, p, a: {})
    tarefas.enfileirar(s, "somar", {}, rotulo="Do admin", usuario=cenario["admin"])
    tarefas.enfileirar(s, "somar", {}, rotulo="Do obreiro", usuario=cenario["obreiro"])
    s.flush()

    d = como(app_real, cenario["admin"].id).get("/erp/api/tarefas?todas=1").get_json()
    assert {t["rotulo"] for t in d["tarefas"]} == {"Do admin", "Do obreiro"}


def test_o_trabalho_de_outro_responde_nao_encontrado(app_real, cenario):
    """Fora do escopo é 404, nunca 403: dizer 'sem permissão' confirmaria que
    o número existe."""
    s = cenario["s"]
    tarefas.registrar("somar", lambda sessao, p, a: {})
    t = tarefas.enfileirar(s, "somar", {}, rotulo="Do admin", usuario=cenario["admin"])
    s.flush()
    r = como(app_real, cenario["obreiro"].id).get(f"/erp/api/tarefas/{t.id}")
    assert r.status_code == 404


def test_o_obreiro_nao_ve_a_fila_dos_outros_nem_pedindo(app_real, cenario):
    s = cenario["s"]
    tarefas.registrar("somar", lambda sessao, p, a: {})
    tarefas.enfileirar(s, "somar", {}, rotulo="Do admin", usuario=cenario["admin"])
    s.flush()
    d = (como(app_real, cenario["obreiro"].id)
         .get("/erp/api/tarefas?todas=1").get_json())
    assert d["tarefas"] == []


# ---------------------------------------------------------------------------
# 8. AS DUAS TELAS QUE DEIXARAM DE ESPERAR
# ---------------------------------------------------------------------------
def test_a_importacao_do_pipefy_agora_e_enfileirada(app_real, cenario, monkeypatch):
    """Antes, cem cards rodavam com a pessoa esperando — e o sistema inteiro
    ficava pesado para todo mundo. Agora o clique só põe na fila."""
    chamou = {"buscar": False}

    def nao_deve_buscar(ids):
        chamou["buscar"] = True
        return []

    monkeypatch.setattr(
        "app.apps.erp.core.importadores.pipefy_cards.buscar_cards", nao_deve_buscar)

    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/importar/pipefy", json={"texto": "111111 222222 333333"})
    assert r.status_code == 200
    t = r.get_json()["tarefa"]
    assert t["situacao"] == "PENDENTE"
    assert "3 card(s)" in t["rotulo"]
    assert not chamou["buscar"], "buscar no Pipefy tem de acontecer NA FILA, não no clique"


def test_a_agenda_abre_sem_recalcular_na_frente_de_quem_abriu(app_real, cenario):
    d = como(app_real, cenario["admin"].id).get("/erp/api/agenda").get_json()
    assert d["ok"] is True
    assert "eventos" in d
    assert d["tarefa"] is not None, "o recálculo tem de ter ido para a fila"
    assert d["tarefa"]["situacao"] == "PENDENTE"


def test_abrir_a_agenda_de_novo_nao_empilha_recalculo(app_real, cenario):
    c = como(app_real, cenario["admin"].id)
    c.get("/erp/api/agenda")
    d = c.get("/erp/api/agenda").get_json()
    assert d["tarefa"] is None, "o segundo acesso não pode criar outro recálculo igual"
