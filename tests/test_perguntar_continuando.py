"""Continuar a conversa — "e da obra Triunfo?".

O dono pediu poder **interagir**, não só disparar perguntas soltas: *"a ideia é
poder perguntar e interagir"*. A forma mais comum disso, em qualquer conversa
de verdade, é a frase curta que só troca um filtro da anterior.

A REGRA QUE MANTÉM ISSO HONESTO, e que este arquivo guarda: **a tela DIZ que
repetiu.** Responder outra pergunta em silêncio só porque a frase era curta
seria o pior tipo de erro — o número sai certo, só que de outra pergunta, e
ninguém tem como perceber.

E continua **sem IA**: é trocar um parâmetro numa pergunta que já existe, não
inventar pergunta nova.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.perguntas import catalogo
from app.apps.erp.core.perguntas.entender import entender

CATALOGO = catalogo.para_a_tela()


def _chave(texto, anterior=""):
    return entender(texto, CATALOGO, chave_anterior=anterior)


# ---------------------------------------------------------------------------
# O caso que o dono vai usar
# ---------------------------------------------------------------------------
def test_dizendo_o_nome_do_filtro():
    base = _chave("o que tem a pagar hoje")
    seguindo = _chave("e da obra Triunfo?", base["chave"])
    assert seguindo["continuacao"] is True
    assert seguindo["chave"] == base["chave"]
    assert seguindo["parametros"] == {"obra": "triunfo"}


def test_sem_dizer_o_nome_do_filtro():
    """"e a elétrica?" — ninguém repete "categoria" na segunda pergunta."""
    base = _chave("insumos da categoria hidraulico")
    seguindo = _chave("e a eletrica?", base["chave"])
    assert seguindo["continuacao"] is True
    assert seguindo["parametros"] == {"categoria": "eletrica"}


def test_a_resposta_AVISA_que_repetiu_a_anterior():
    """Sem esta frase, a pessoa acha que ele entendeu a pergunta nova."""
    base = _chave("insumos da categoria hidraulico")
    seguindo = _chave("e agregados", base["chave"])
    assert "Repeti a pergunta anterior" in seguindo["aviso"]
    assert "categoria = agregados" in seguindo["aviso"]
    assert base["pergunta"] in seguindo["aviso"]


# ---------------------------------------------------------------------------
# O que a continuação NÃO pode sequestrar
# ---------------------------------------------------------------------------
def test_pergunta_nova_reconhecivel_vence_a_continuacao():
    """Se a frase vale por si, ela vale por si — a anterior não entra."""
    base = _chave("o que tem a pagar hoje")
    nova = _chave("quais insumos da categoria hidraulico", base["chave"])
    assert nova.get("continuacao") is None
    assert nova["chave"] == "insumos_da_categoria"


def test_frase_com_assunto_proprio_nao_e_continuacao():
    """"o que está sem nota da obra X" tem a palavra "nota", que é de outra
    pergunta. Repetir a anterior seria trocar a pergunta em silêncio."""
    base = _chave("o que tem a pagar hoje")
    outra = _chave("o que esta sem nota da obra Triunfo", base["chave"])
    assert outra.get("continuacao") is None


def test_empate_continua_virando_pergunta_de_volta():
    """"e o que está vencido" começa com "e" e empata entre duas perguntas.
    Tratá-la como o valor de um filtro viraria "categoria = vencido" — chute
    puro. Empate legítimo tem de virar pergunta de volta."""
    base = _chave("insumos da categoria hidraulico")
    r = _chave("e o que esta vencido", base["chave"])
    assert r["entendi"] is False
    assert r.get("continuacao") is None


def test_sem_pergunta_anterior_nao_ha_o_que_continuar():
    r = _chave("e da obra Triunfo?")
    assert r["entendi"] is False


def test_sem_o_e_do_comeco_nao_e_continuacao():
    """O "e" é o marcador de continuação em português falado. Sem ele, a
    palavra solta é pergunta malfeita — e aí o certo é dizer que não entendeu,
    não chutar um filtro."""
    base = _chave("insumos da categoria hidraulico")
    assert _chave("eletrica", base["chave"])["entendi"] is False


def test_pergunta_com_mais_de_um_filtro_exige_dizer_qual():
    """"e amanhã", depois de "o que tem a pagar", virava obra = "amanhã" na
    primeira versão. Com mais de um filtro, adivinhar qual é chute — e a
    continuação que DIZ o nome continua funcionando."""
    base = _chave("o que tem a pagar hoje")
    assert _chave("e amanha", base["chave"])["entendi"] is False
    assert _chave("e da obra Triunfo", base["chave"])["continuacao"] is True


def test_chave_anterior_desconhecida_nao_derruba_nada():
    """A tela guarda a última pergunta; publicação nova pode renomear uma
    chave. Isso não pode virar erro."""
    r = _chave("e da obra Triunfo?", "pergunta_que_nao_existe_mais")
    assert r["entendi"] is False


# ---------------------------------------------------------------------------
# O contrato: recebe a CHAVE, não a pergunta inteira
# ---------------------------------------------------------------------------
def test_recebe_a_chave_e_nao_um_dicionario():
    """Na primeira versão isto recebia um dicionário, e passar o resultado de
    `entender` em vez da entrada do catálogo fazia a continuação falhar EM
    SILÊNCIO — os dois têm o campo "parametros", com significados diferentes.
    Com a chave não há como passar a coisa errada."""
    import inspect
    assinatura = inspect.signature(entender)
    assert "chave_anterior" in assinatura.parameters
    # `from __future__ import annotations` deixa a anotação como texto.
    assert assinatura.parameters["chave_anterior"].annotation in (str, "str")
