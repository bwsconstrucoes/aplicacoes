"""Perguntar ESCREVENDO — o catálogo é um norte, não um limite.

O dono corrigiu o rumo em 11/09/2026: *"o assistant não pode ficar somente
focado nessas perguntas, né? Isso é só um norte"*. Ele quer escrever a
pergunta, como disse desde o começo — *"eu poder fazer qualquer pergunta ao
sistema"*.

Este arquivo prova a primeira metade disso: da frase escrita para a pergunta
que o sistema sabe responder. Roda SEM BANCO porque é operação de texto pura —
e ela ser pura é justamente o que resolve o problema de permissão: entender não
toca em dado nenhum, então pode ser aberto a todo operador; quem responde
continua sendo a rota do grupo, com a ação dela.

O que se prova:

  1. A frase do dia a dia chega na pergunta certa — inclusive sem acento, com
     plural e com as palavras que ele usou nos exemplos dele.
  2. **Dizer "não sei" é resposta legítima.** Chutar seria pior: a pessoa
     receberia um número certo de uma pergunta que ela não fez, e não teria
     como perceber.
  3. As palavras de ligação não decidem nada — senão toda pergunta que começa
     com "o que" empataria.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.perguntas.catalogo import para_a_tela
from app.apps.erp.core.perguntas.entender import (
    CONFIANCA_MINIMA, entender, palavras,
)

CATALOGO = para_a_tela()


def _chave(frase: str):
    leitura = entender(frase, CATALOGO)
    return leitura["chave"] if leitura["entendi"] else None


# ---------------------------------------------------------------------------
# 1. A frase do dia a dia chega na pergunta certa
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("frase, esperado", [
    # as palavras do próprio dono, tiradas das conversas
    ("o que tem a pagar hoje", "a_pagar_no_periodo"),
    ("me manda aí um relatório do que tem a pagar hoje na obra tal",
     "a_pagar_no_periodo"),
    ("quanto falta receber da obra tal", "falta_receber"),
    ("me manda uma lista dos insumos cadastrados na categoria tal",
     "insumos_da_categoria"),
    ("quantos títulos não estão conciliados", None),   # ainda não existe
    # o jeito curto de perguntar
    ("o que está atrasado", "vencidos_sem_pagar"),
    ("o que está esperando aprovação", "esperando_decisao"),
    ("tem equipamento alugado tempo demais", "locacao_que_ja_pagou_a_compra"),
    ("qual o preço do cimento", "preco_do_insumo"),
])
def test_a_frase_do_dia_a_dia_chega_na_pergunta_certa(frase, esperado):
    assert _chave(frase) == esperado


def test_acento_e_plural_nao_atrapalham():
    """Ninguém digita acento, e todo mundo troca singular por plural."""
    assert _chave("quais insumos da categoria hidraulico") == "insumos_da_categoria"
    assert _chave("quais insumo da categoria hidráulico") == "insumos_da_categoria"


def test_os_exemplos_do_catalogo_sao_entendidos():
    """Os exemplos são as palavras do dia a dia que o enunciado formal não
    tem. Se um deles não voltar para a própria pergunta, ele está escrito de
    um jeito que não ajuda ninguém."""
    erros = []
    for pergunta in CATALOGO:
        for exemplo in pergunta.get("exemplos") or []:
            achou = _chave(exemplo)
            if achou != pergunta["chave"]:
                erros.append(f"{exemplo!r} → {achou} (esperado {pergunta['chave']})")
    assert erros == [], "\n".join(erros)


def test_o_enunciado_da_propria_pergunta_e_entendido():
    for pergunta in CATALOGO:
        assert _chave(pergunta["pergunta"]) == pergunta["chave"], pergunta["chave"]


# ---------------------------------------------------------------------------
# 2. "Não sei" é resposta
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("frase", [
    "qual a cor do cavalo branco",
    "bom dia",
    "asdf",
    "",
])
def test_o_que_nao_e_pergunta_do_sistema_recebe_um_nao_sei(frase):
    """Chutar seria pior que não responder: a pessoa receberia um número certo
    de uma pergunta que ela não fez, e não teria como perceber."""
    leitura = entender(frase, CATALOGO)
    assert leitura["entendi"] is False
    assert leitura["motivo"]


def test_o_nao_sei_ainda_oferece_as_parecidas():
    """"Não achei" sem oferecer nada obriga a pessoa a adivinhar a palavra
    certa."""
    leitura = entender("quanto gastei de cimento na obra", CATALOGO)
    if not leitura["entendi"]:
        assert "parecidas" in leitura


# ---------------------------------------------------------------------------
# EMPATE NO TOPO: pergunta de volta, não escolhe
#
# É a regra que o próprio dono deu, sobre o "quanto falta receber": *"talvez
# valesse a pena questionar, se não tivesse sido bem específica"*.
# ---------------------------------------------------------------------------
def test_frase_ambigua_pergunta_de_volta_em_vez_de_escolher(): 
    """"nota" quer dizer DUAS coisas no ERP: a nota que recebemos, anexada ao
    título, e a nota que emitimos ao cliente. Responder uma delas com ar de
    certeza é o erro que não tem como ser percebido — o número sai certo, só
    que de outra pergunta."""
    leitura = entender("o que está sem nota", CATALOGO)

    assert leitura["entendi"] is False
    assert leitura["ambigua"] is True
    assert len(leitura["parecidas"]) >= 2
    assert "Qual delas" in leitura["motivo"]


def test_o_ambiguo_e_diferente_do_nao_sei():
    """São duas conversas diferentes: numa a pessoa escolhe entre opções, na
    outra ela descobre que o sistema ainda não sabe aquilo."""
    ambigua = entender("o que está sem nota", CATALOGO)
    nao_sei = entender("qual a cor do cavalo branco", CATALOGO)

    assert ambigua["ambigua"] is True
    assert nao_sei["ambigua"] is False


def test_pergunta_que_o_sistema_nao_sabe_nao_e_respondida_por_engano():
    """"quantos títulos não estão conciliados" casava pela METADE com duas
    perguntas erradas, e uma delas era escolhida com ar de certeza."""
    leitura = entender("quantos títulos não estão conciliados", CATALOGO)
    assert leitura["entendi"] is False


def test_a_confianca_minima_existe_e_e_respeitada():
    leitura = entender("o que tem a pagar hoje", CATALOGO)
    assert leitura["entendi"] and leitura["confianca"] >= CONFIANCA_MINIMA


# ---------------------------------------------------------------------------
# 3. As palavras de ligação não decidem nada
# ---------------------------------------------------------------------------
def test_palavras_de_ligacao_sao_descartadas():
    """Sem isto, "o que tem a pagar" e "o que está vencido" empatariam pelo
    "o que" e pelo "está"."""
    assert palavras("o que é que eu tenho a pagar") == {"pagar"}
    assert palavras("me manda aí") == set()


def test_a_frase_generica_demais_pergunta_de_volta():
    """"o que tem a pagar", sem dizer quando, é honestamente ambíguo: pode ser
    o que vence hoje ou o que já venceu. Perguntar de volta é a resposta
    certa — e é melhor que escolher uma das duas com ar de certeza."""
    generica = entender("o que tem a pagar", CATALOGO)
    especifica = entender("o que tem a pagar hoje", CATALOGO)

    assert generica["ambigua"] is True
    assert especifica["entendi"] is True
    assert especifica["chave"] == "a_pagar_no_periodo"


def test_uma_palavra_distintiva_ja_resolve():
    """Uma palavra que só aparece numa pergunta não deixa dúvida."""
    leitura = entender("o que está sem documento anexado", CATALOGO)
    assert leitura["entendi"] is True
    assert leitura["chave"] == "sem_documento"


def test_palavra_que_DEIXOU_de_ser_distintiva_passa_a_perguntar_de_volta():
    """"vencido" era de uma pergunta só. Hoje é de três.

    Até 11/09/2026 só o título vencia. Depois o grupo de Obras trouxe o seguro
    garantia vencido e a vigência de contrato vencida — e "o que está vencido"
    deixou de ter uma resposta só. O sistema NÃO foi ajustado para isso: ele
    percebe o empate sozinho e devolve a pergunta.

    É esse o comportamento que interessa guardar. O catálogo vai crescer, e
    cada pergunta nova pode roubar a exclusividade de uma palavra de outra. Se
    em vez de perguntar ele escolhesse a primeira, o dono receberia a lista de
    apólices quando queria a de contas atrasadas — sem nada na tela avisando.
    """
    leitura = entender("o que está vencido", CATALOGO)
    assert leitura["ambigua"] is True
    oferecidas = {p["chave"] for p in leitura["parecidas"]}
    assert "vencidos_sem_pagar" in oferecidas
    assert "garantia_vencendo" in oferecidas

    # E quem disser mais uma palavra é atendido na hora.
    assert entender("o que está vencido e não foi pago",
                    CATALOGO)["chave"] == "vencidos_sem_pagar"
    assert entender("tem seguro garantia vencendo",
                    CATALOGO)["chave"] == "garantia_vencendo"


# ---------------------------------------------------------------------------
# A frase já diz os filtros — e ignorá-los é ignorar metade do que a pessoa
# falou
# ---------------------------------------------------------------------------
def test_a_categoria_escrita_na_frase_e_usada():
    """"me manda a lista dos insumos da categoria hidráulico" respondido com o
    catálogo INTEIRO é pior que não responder: parece que funcionou. Foi o que
    a tela mostrou na primeira versão — 3.285 insumos em vez de 305."""
    leitura = entender("me manda a lista dos insumos da categoria hidraulico",
                       CATALOGO)
    assert leitura["chave"] == "insumos_da_categoria"
    assert leitura["parametros"] == {"categoria": "hidraulico"}


@pytest.mark.parametrize("frase, esperado", [
    ("o que tem a pagar hoje na obra CREPETRIUNFO", {"obra": "crepetriunfo"}),
    ("quanto falta receber da obra Creche", {"obra": "creche"}),
    ("qual o preço do insumo cimento", {"insumo": "cimento"}),
])
def test_o_filtro_escrito_na_frase_e_lido(frase, esperado):
    leitura = entender(frase, CATALOGO)
    assert leitura["entendi"] is True
    assert leitura["parametros"] == esperado


def test_o_generico_nao_vira_filtro():
    """"da categoria tal" é o jeito de falar, não o nome de uma categoria.
    Virar filtro faria a resposta vir vazia e parecer que não há nada."""
    leitura = entender("quais insumos da categoria tal", CATALOGO)
    assert leitura["parametros"] == {}


def test_pergunta_sem_filtro_nao_inventa_parametro():
    leitura = entender("o que está parado esperando decisão", CATALOGO)
    assert leitura["entendi"] is True
    assert leitura["parametros"] == {}
