# -*- coding: utf-8 -*-
"""
A numeração das notas, e o número que não volta a ser usado.

Veio de um defeito real, visto em 07/10/2026: uma declaração que a prefeitura
aceitou mas que ainda não virou nota **não entra na planilha**, porque a planilha
só recebe nota pronta. O número dela ficava "livre" para a emissão seguinte —
enquanto a prefeitura o mantinha RESERVADO para a declaração travada.

O estrago que isso faria: a nota seguinte sairia pedindo o mesmo número, e a
prefeitura leria o pedido como **reenvio da declaração anterior** (é o que o
manual dela prevê para a mesma identificação), não como nota nova. Dois serviços
diferentes colapsados num documento só — e documento fiscal não se desfaz.

**08/10/2026 — a regra ficou mais dura, por evidência.** A versão de 07/10 ainda
reaproveitava o número para o MESMO card, confiando no manual: declaração
recusada pode ser reenviada com a mesma identificação. Em Eusébio não é assim. A
declaração da nota 3281 foi aceita, transmitida, recusada no nacional, teve o
número liberado e reusado — e a prefeitura respondeu **EL99: "chave informada
para a DPS não existe no repositório municipal"**. Número já enviado é número
gasto, e a exceção do mesmo card saiu.
"""
import os
import sys

import pytest

_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

import declaracoes          # noqa: E402
import worker               # noqa: E402


class AbaFalsa:
    def __init__(self, numeros):
        self.numeros = numeros

    def col_values(self, _c):
        return ["Nº Nota"] + [str(n) for n in self.numeros]


class PlanilhaFalsa:
    def __init__(self, numeros):
        self.aba = AbaFalsa(numeros)

    def worksheets(self):
        return []


class GoogleFalso:
    def __init__(self, numeros):
        self.planilha = PlanilhaFalsa(numeros)

    def open_by_key(self, _k):
        return self.planilha


@pytest.fixture
def numeros_na_planilha(monkeypatch):
    def preparar(emitidos, abertas):
        gc = GoogleFalso(emitidos)
        monkeypatch.setattr(worker, "abrir_aba", lambda planilha, cand: planilha.aba)
        monkeypatch.setattr(declaracoes, "numeros_registrados",
                            lambda planilha: [int(d["numero"]) for d in abertas])
        return gc
    return preparar


def _aberta(numero, card_id="999", horas=0.1):
    return {"numero": str(numero), "card_id": card_id, "horas_aberta": horas,
            "travada": False, "ambiente": "producao", "enviada_em": ""}


# --------------------------------------------------------------------------- #
def test_sem_declaracao_em_aberto_o_proximo_e_o_seguinte_da_planilha(numeros_na_planilha):
    gc = numeros_na_planilha([3278, 3279, 3280], [])
    assert worker.proximo_numero(gc) == (3281, 3280)


def test_numero_de_declaracao_ja_enviada_nao_e_reusado(numeros_na_planilha, capsys):
    """O caso da 3281: aceita pela prefeitura, sem nota, fora da planilha. A nota
    seguinte tem de sair como 3282."""
    gc = numeros_na_planilha([3278, 3279, 3280], [_aberta(3281, card_id="111")])
    prox, ultimo = worker.proximo_numero(gc, card_id="222")
    assert prox == 3282
    assert ultimo == 3280, "o último EMITIDO continua sendo o da planilha"
    assert "já foi usado numa declaração enviada" in capsys.readouterr().out


def test_nem_o_mesmo_card_reusa_o_numero_da_sua_declaracao(numeros_na_planilha):
    """Esta é a regra que mudou em 08/10/2026, e mudou por evidência.

    Antes, o mesmo card reaproveitava o número: o manual da prefeitura diz que
    declaração recusada pode ser reenviada com a MESMA identificação. Foi feito
    exatamente isso com a 3281 e a resposta foi **EL99** — a prefeitura não
    reconhece mais aquela identificação. Número enviado é número gasto, para
    qualquer card."""
    gc = numeros_na_planilha([3278, 3279, 3280], [_aberta(3281, card_id="111")])
    assert worker.proximo_numero(gc, card_id="111")[0] == 3282


def test_varias_declaracoes_presas_empurram_o_numero_para_depois_da_ultima(numeros_na_planilha):
    gc = numeros_na_planilha([3280], [_aberta(3281, "1"), _aberta(3282, "2"), _aberta(3283, "3")])
    assert worker.proximo_numero(gc, card_id="9")[0] == 3284


def test_se_as_declaracoes_nao_puderem_ser_lidas_a_emissao_avisa(monkeypatch, capsys):
    """Falhar em silêncio aqui devolveria o defeito: melhor numerar como antes e
    dizer que o número pode colidir."""
    gc = GoogleFalso([3280])
    monkeypatch.setattr(worker, "abrir_aba", lambda planilha, cand: planilha.aba)

    def explode(_p):
        raise RuntimeError("planilha fora do ar")

    monkeypatch.setattr(declaracoes, "numeros_registrados", explode)
    assert worker.proximo_numero(gc, card_id="1") == (3281, 3280)
    assert "pode colidir" in capsys.readouterr().out


def test_planilha_vazia_comeca_do_um(numeros_na_planilha):
    gc = numeros_na_planilha([], [])
    assert worker.proximo_numero(gc) == (1, 0)


# --------------------------------------------------------------------------- #
# Declaração parada há horas não é fila
# --------------------------------------------------------------------------- #
def test_declaracao_parada_ha_horas_e_marcada_como_travada(monkeypatch):
    """A fila do nacional leva segundos. Horas param de ser espera e passam a ser
    assunto para a prefeitura — e a tela tem de parar de dizer "espere"."""
    import datetime

    agora = datetime.datetime.now(declaracoes.FUSO_BRASILIA)
    recente = (agora - datetime.timedelta(minutes=5)).strftime("%d/%m/%Y %H:%M")
    antiga = (agora - datetime.timedelta(hours=5)).strftime("%d/%m/%Y %H:%M")

    class WS:
        def get_all_values(self):
            return [declaracoes.CAB,
                    ["DPS1", "3281", "111", "OBRA", "10", "producao", antiga,
                     "aguardando", "", "", ""],
                    ["DPS2", "3282", "222", "OBRA", "11", "producao", recente,
                     "aguardando", "", "", ""]]

    monkeypatch.setattr(declaracoes, "_ws", lambda p: WS())
    abertas = {d["numero"]: d for d in declaracoes.listar_abertas(None)}
    assert abertas["3281"]["travada"] is True
    assert abertas["3282"]["travada"] is False
    assert abertas["3281"]["horas_aberta"] >= 4


def test_data_ilegivel_nao_vira_travada_nem_estoura(monkeypatch):
    class WS:
        def get_all_values(self):
            return [declaracoes.CAB,
                    ["DPS1", "3281", "111", "O", "10", "producao", "sei lá quando",
                     "aguardando", "", "", ""]]

    monkeypatch.setattr(declaracoes, "_ws", lambda p: WS())
    d = declaracoes.listar_abertas(None)[0]
    assert d["travada"] is False and d["horas_aberta"] is None


# --------------------------------------------------------------------------- #
# Toda declaração enviada conta — qualquer que seja o desfecho
# --------------------------------------------------------------------------- #
def test_numeros_registrados_conta_todo_status(monkeypatch):
    """A aba só recebe declaração DEPOIS de a prefeitura aceitar. Então todo
    número que está nela já foi enviado, e número enviado é número gasto — valha
    a declaração ter virado nota, ter sido recusada ou estar aguardando."""
    class WS:
        def get_all_values(self):
            return [declaracoes.CAB,
                    ["DPS1", "3281", "111", "O", "10", "producao", "", "recusada", "", "", ""],
                    ["DPS2", "3282", "222", "O", "11", "producao", "", "aguardando", "", "", ""],
                    ["DPS3", "3283", "333", "O", "12", "producao", "", "concluida", "", "3283", ""]]

    monkeypatch.setattr(declaracoes, "_ws", lambda p: WS())
    assert sorted(declaracoes.numeros_registrados(None)) == [3281, 3282, 3283]


def test_numero_recusado_tambem_empurra_a_numeracao(numeros_na_planilha, monkeypatch):
    """O caso exato de 08/10/2026: a 3281 foi recusada, o número foi liberado e
    reusado, e a prefeitura respondeu EL99. Agora ela continua contando."""
    gc = GoogleFalso([3280])
    monkeypatch.setattr(worker, "abrir_aba", lambda planilha, cand: planilha.aba)
    monkeypatch.setattr(declaracoes, "numeros_registrados", lambda p: [3281])
    assert worker.proximo_numero(gc, card_id="111")[0] == 3282


def test_linha_sem_numero_nao_estoura(monkeypatch):
    class WS:
        def get_all_values(self):
            return [declaracoes.CAB,
                    ["DPS1", "", "111", "O", "10", "producao", "", "recusada", "", "", ""]]

    monkeypatch.setattr(declaracoes, "_ws", lambda p: WS())
    assert declaracoes.numeros_registrados(None) == []
