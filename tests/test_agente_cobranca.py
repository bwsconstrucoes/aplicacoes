"""O agente que vai atrás de quem tem pendência.

O que se prova aqui, e por que cada coisa importa:

  1. A ESCADA. Dia 5 lembra, dia 10 cobra, dia 15 sobe para o dono — foi a
     escada que o dono aprovou, e quem está com 20 dias de atraso está em
     ESCALADA, não em LEMBRETE de novo.
  2. NINGUÉM RECEBE DUAS VEZES o mesmo degrau. A rotina roda todo dia; sem
     isso, o agente vira spam na primeira semana e a obra passa a ignorar.
  3. QUEM NÃO TEM TELEFONE não trava a varredura — as outras pessoas
     continuam sendo avisadas, e o motivo do pulo fica dito.
  4. A ESCALADA vai para quem manda e quem paga, não para o responsável de
     novo: o sentido do dia 15 é justamente sair das mãos de quem não
     respondeu.
  5. A MENSAGEM não leva valor de contrato nem dado bancário. Ela é lida em
     ônibus, em obra, e por quem pega o telefone emprestado.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core import agente as ag
from app.apps.erp.db.models.cadastros import PerfilUsuario as P
from conftest import SessaoFalsa, novo_usuario


# ---------------------------------------------------------------------------
# 1. A escada
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dias,esperado", [
    (0, None), (4, None),
    (5, "LEMBRETE"), (9, "LEMBRETE"),
    (10, "COBRANCA"), (14, "COBRANCA"),
    (15, "ESCALADA"), (60, "ESCALADA"),
])
def test_a_escada_e_a_que_o_dono_aprovou(dias, esperado):
    assert ag.degrau_de(dias) == esperado


def test_quem_esta_muito_atrasado_nao_volta_para_o_lembrete():
    """O degrau é o mais alto alcançado, não um por dia."""
    assert ag.degrau_de(40) == "ESCALADA"


# ---------------------------------------------------------------------------
# 2. O texto da mensagem
# ---------------------------------------------------------------------------
def _pendencia(dias=12, responsavel_id=7):
    return ag.Pendencia(
        assunto="locacao_conferencia", referencia_id=3,
        responsavel_id=responsavel_id,
        resumo="A conferência dos equipamentos locados de 09/2026 da obra "
               "ESCPLANALTO (contrato LOC00001) ainda não foi respondida.",
        link="/erp/suprimentos/locacoes?conferencia=3",
        dias_de_atraso=dias)


def test_a_mensagem_diz_o_que_falta_e_traz_o_link():
    texto = ag.montar_texto(_pendencia(), "COBRANCA", "Ruan do administrativo")
    assert "conferência" in texto
    assert "/erp/suprimentos/locacoes?conferencia=3" in texto
    assert "Ruan" in texto


def test_o_tom_muda_com_o_degrau_mas_o_assunto_continua_o_mesmo():
    p = _pendencia()
    lembrete = ag.montar_texto(p, "LEMBRETE", "Ruan")
    cobranca = ag.montar_texto(p, "COBRANCA", "Ruan")
    escalada = ag.montar_texto(p, "ESCALADA", "Ruan")
    assert lembrete != cobranca != escalada
    for texto in (lembrete, cobranca, escalada):
        assert "ESCPLANALTO" in texto, "o assunto tem de estar nos três"
    assert "dia(s) depois do prazo" in cobranca
    assert "sem resposta" in escalada.lower()


def test_a_mensagem_nao_leva_dinheiro_nem_dado_bancario():
    """Mensagem de WhatsApp é lida em qualquer lugar, por qualquer um que
    pegue o telefone. O que ela precisa dizer é O QUE FALTA e ONDE resolver."""
    texto = ag.montar_texto(_pendencia(), "COBRANCA", "Ruan")
    for proibido in ("R$", "agência", "conta ", "pix", "cnpj"):
        assert proibido.lower() not in texto.lower(), \
            f"a mensagem levou '{proibido}' — isso não pode sair por WhatsApp"


# ---------------------------------------------------------------------------
# 3. A varredura
# ---------------------------------------------------------------------------
# `ativo` é NOT NULL no banco, com padrão TRUE — mas um objeto criado em
# memória nasce com ele vazio. A regra da escalada lê esse campo, então o
# dublê precisa preenchê-lo: objeto pela metade falha por motivo errado.
def _agente_com(pendencias, *objetos):
    """Registra um assunto de mentira e devolve a sessão dublada."""
    ag.ASSUNTOS.clear()
    ag.registrar_assunto("teste", lambda s: pendencias)
    return SessaoFalsa(*objetos)


@pytest.fixture(autouse=True)
def _restaurar_assuntos():
    originais = dict(ag.ASSUNTOS)
    yield
    ag.ASSUNTOS.clear()
    ag.ASSUNTOS.update(originais)


def test_quem_ainda_esta_no_prazo_nao_recebe_nada():
    ruan = novo_usuario(7, P.ADMINISTRATIVO_OBRA, nome="Ruan", ativo=True)
    ruan.telefone = "5585999990000"
    s = _agente_com([_pendencia(dias=2)], ruan)
    r = ag.varrer(s, simular=True)
    assert r["enviadas"] == []
    assert any("ainda no prazo" in p["motivo"] for p in r["puladas"])


def test_responsavel_sem_telefone_e_pulado_com_o_motivo_dito():
    """E não derruba a varredura: é cadastro incompleto, não defeito.

    O motivo tem de trazer o NOME. "responsável sem telefone" não diz a quem
    ir pedir; "Ruan não tem telefone" diz."""
    ruan = novo_usuario(7, P.ADMINISTRATIVO_OBRA, nome="Ruan", ativo=True)
    ruan.telefone = None
    s = _agente_com([_pendencia(dias=12)], ruan)
    r = ag.varrer(s, simular=True)
    assert r["enviadas"] == []
    assert any("Ruan não tem telefone" in p["motivo"] for p in r["puladas"])


def test_quando_dois_respondem_pela_obra_os_dois_recebem():
    """Decisão do dono: "se por acaso tiverem dois, os dois recebem"."""
    um = novo_usuario(7, P.ADMINISTRATIVO_OBRA, nome="Ruan", ativo=True)
    um.telefone = "5585999990000"
    outro = novo_usuario(8, P.ADMINISTRATIVO_OBRA, nome="Cleide", ativo=True)
    outro.telefone = "5585988880000"
    p = _pendencia(dias=12)
    p.responsaveis = [7, 8]
    s = _agente_com([p], um, outro)
    r = ag.varrer(s, simular=True)
    assert {e["para"] for e in r["enviadas"]} == {"Ruan", "Cleide"}


def test_um_sem_telefone_nao_impede_o_outro_de_receber():
    """Cadastro incompleto de uma pessoa não pode calar a cobrança da outra."""
    um = novo_usuario(7, P.ADMINISTRATIVO_OBRA, nome="Ruan", ativo=True)
    um.telefone = None
    outro = novo_usuario(8, P.ADMINISTRATIVO_OBRA, nome="Cleide", ativo=True)
    outro.telefone = "5585988880000"
    p = _pendencia(dias=12)
    p.responsaveis = [7, 8]
    s = _agente_com([p], um, outro)
    r = ag.varrer(s, simular=True)
    assert [e["para"] for e in r["enviadas"]] == ["Cleide"]
    assert any("Ruan não tem telefone" in x["motivo"] for x in r["puladas"])


def test_pendencia_sem_ninguem_marcado_diz_isso():
    """Silêncio aqui é o pior caso: a conferência fica aberta e ninguém sabe."""
    s = _agente_com([_pendencia(dias=12, responsavel_id=None)])
    r = ag.varrer(s, simular=True)
    assert r["enviadas"] == []
    assert any("ninguém marcado" in p["motivo"] for p in r["puladas"])


def test_o_lembrete_vai_para_quem_tem_de_responder():
    ruan = novo_usuario(7, P.ADMINISTRATIVO_OBRA, nome="Ruan do administrativo", ativo=True)
    ruan.telefone = "5585999990000"
    s = _agente_com([_pendencia(dias=6)], ruan)
    r = ag.varrer(s, simular=True)
    assert len(r["enviadas"]) == 1
    assert r["enviadas"][0]["degrau"] == "LEMBRETE"
    assert r["enviadas"][0]["para"] == "Ruan do administrativo"


def test_a_escalada_sai_das_maos_de_quem_nao_respondeu():
    """Dia 15 é para quem manda e quem paga — esse é o sentido do degrau."""
    ruan = novo_usuario(7, P.ADMINISTRATIVO_OBRA, nome="Ruan", ativo=True)
    ruan.telefone = "5585999990000"
    dono = novo_usuario(1, P.ADMIN, nome="Marcelo", ativo=True)
    dono.telefone = "5585988880000"
    fin = novo_usuario(2, P.FINANCEIRO, nome="Financeiro", ativo=True)
    fin.telefone = "5585977770000"
    s = _agente_com([_pendencia(dias=20)], ruan, dono, fin)
    r = ag.varrer(s, simular=True)
    para = {e["para"] for e in r["enviadas"]}
    assert para == {"Marcelo", "Financeiro"}, \
        "a escalada tem de subir, não voltar para quem não respondeu"
    assert all(e["degrau"] == "ESCALADA" for e in r["enviadas"])


def test_simular_nao_manda_nem_grava_nada():
    ruan = novo_usuario(7, P.ADMINISTRATIVO_OBRA, nome="Ruan", ativo=True)
    ruan.telefone = "5585999990000"
    s = _agente_com([_pendencia(dias=12)], ruan)
    r = ag.varrer(s, simular=True)
    assert r["simulado"] is True
    assert all(e.get("simulado") for e in r["enviadas"])
    assert r["enviadas"][0]["texto"], "simulação mostra o texto que sairia"
