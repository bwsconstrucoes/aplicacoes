# -*- coding: utf-8 -*-
"""
Quem entra, e onde o trabalho de cada um fica guardado.

DOIS PROBLEMAS DE VERDADE ESTÃO COBERTOS AQUI, e os dois vieram do dono:

  1. "basta colocar o nome idêntico?" — com campo livre, digitar "Marcelo"
     hoje e "Marcelo Leitão" amanhã dava DUAS pessoas, e a segunda encontrava
     o lote vazio sem entender por quê. Virou lista de seleção.

  2. "faça de alguma forma que os filtros e o lote fiquem salvos" — eles
     dependiam de uma tabela que só nasce quando alguém aperta "Aplicar
     atualizações do banco". Enquanto o botão não era apertado, NADA era
     guardado, em silêncio. Agora há um segundo lugar, que existe desde o
     primeiro dia, e o que for guardado nele é trazido para a tabela boa
     quando ela aparecer.
"""
from __future__ import annotations

import json

import pytest

from app.apps.analisesps import pessoas, preferencias


# ---------------------------------------------------------------------------
# A LISTA DE QUEM ENTRA
# ---------------------------------------------------------------------------
def test_a_lista_nunca_vem_vazia_mesmo_sem_banco(monkeypatch):
    """Uma tela de entrada sem nenhuma opção trancaria todo mundo do lado de
    fora — inclusive quem consertaria o problema."""
    def sem_banco(*a, **k):
        raise RuntimeError("banco fora do ar")

    monkeypatch.setattr("app.apps.analisesps.db.consultar_um", sem_banco)
    assert pessoas.listar() == pessoas.PADRAO


def test_o_mesmo_nome_de_dois_jeitos_vira_um_so():
    """"Karla" e "KARLA" cairiam no mesmo lote, mas apareceriam como duas
    opções — uma lista que confunde em vez de resolver."""
    assert pessoas._limpar(["KARLA", "Karla", "  karla "]) == ["KARLA"]
    assert pessoas._limpar(["João", "Joao"]) == ["João"]


def test_linha_em_branco_nao_vira_pessoa():
    assert pessoas._limpar(["MARCELO", "", "   ", "THIAGO"]) == ["MARCELO",
                                                                "THIAGO"]


def test_lista_vazia_e_recusada(monkeypatch):
    """Aceitar e deixar a entrada sem opção seria pior do que recusar."""
    with pytest.raises(ValueError):
        pessoas.gravar([])
    with pytest.raises(ValueError):
        pessoas.gravar(["   ", ""])


def test_o_nome_da_tela_e_conferido_contra_a_lista(monkeypatch):
    monkeypatch.setattr(pessoas, "listar", lambda: ["MARCELO", "THIAGO"])
    assert pessoas.da_lista("marcelo") == "MARCELO"
    assert pessoas.da_lista("THIAGO") == "THIAGO"
    assert pessoas.da_lista("Fulano") == ""
    assert pessoas.da_lista("") == ""


def test_a_lista_guardada_ganha_da_padrao(monkeypatch):
    monkeypatch.setattr("app.apps.analisesps.db.consultar_um",
                        lambda *a, **k: (json.dumps(["ANA", "BRUNO"]),))
    assert pessoas.listar() == ["ANA", "BRUNO"]


def test_lista_guardada_estragada_nao_tranca_a_entrada(monkeypatch):
    """Se o que está no banco não for legível, vale a lista padrão — nunca
    uma tela de entrada sem opção nenhuma."""
    monkeypatch.setattr("app.apps.analisesps.db.consultar_um",
                        lambda *a, **k: ("isto não é json",))
    assert pessoas.listar() == pessoas.PADRAO


# ---------------------------------------------------------------------------
# O ARMÁRIO DE RESERVA — o filtro e o lote guardados SEM esperar o botão
# ---------------------------------------------------------------------------
class BancoDeMentira:
    """Um `meta` de brinquedo: (chave -> valor), que é o que a tabela é."""

    def __init__(self):
        self.linhas: dict[str, str] = {}

    def consultar_um(self, sql, params=()):
        if "analisesps.meta" in sql:
            valor = self.linhas.get(params[0])
            return (valor,) if valor is not None else None
        return None

    def conexao(self):
        banco = self

        class Conexao:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params=()):
                if "analisesps.meta" in sql:
                    banco.linhas[params[0]] = params[1]

            def commit(self):
                pass

        return Conexao()


@pytest.fixture
def sem_a_migracao(monkeypatch):
    """O estado real em que o módulo passou dias: código novo, banco velho."""
    banco = BancoDeMentira()
    monkeypatch.setattr("app.apps.analisesps.db.consultar_um", banco.consultar_um)
    monkeypatch.setattr("app.apps.analisesps.db.conexao", banco.conexao)
    monkeypatch.setattr("app.apps.analisesps.db.tem_coluna",
                        lambda tabela, coluna: False)
    return banco


def test_o_filtro_e_guardado_mesmo_sem_a_tabela_da_migracao(sem_a_migracao):
    """O defeito que o dono sentiu: ele digitava o nome todo dia achando que
    estava separando o trabalho dele, e nada era guardado."""
    preferencias.gravar("marcelo", preferencias.FILTRO,
                        {"status_pgt": ["Pagar"]})
    assert preferencias.ler("marcelo", preferencias.FILTRO) == {
        "status_pgt": ["Pagar"]}


def test_cada_pessoa_tem_o_seu_mesmo_sem_a_tabela(sem_a_migracao):
    preferencias.gravar("marcelo", preferencias.FILTRO, {"conta": ["A"]})
    preferencias.gravar("thiago", preferencias.FILTRO, {"conta": ["B"]})
    assert preferencias.ler("marcelo", preferencias.FILTRO) == {"conta": ["A"]}
    assert preferencias.ler("thiago", preferencias.FILTRO) == {"conta": ["B"]}


def test_o_lote_de_cada_um_tambem_e_separado_sem_a_tabela(sem_a_migracao):
    """Antes daqui, sem a coluna `pessoa` todo mundo voltava a dividir o mesmo
    lote — e quem salvasse depois apagava o trabalho do outro sem aviso."""
    from app.apps.analisesps import lote

    lote.salvar("111111111\n222222222", "MARCELO", "marcelo")
    lote.salvar("999999999", "THIAGO", "thiago")

    assert "111111111" in lote.ler("marcelo")["conteudo"]
    assert lote.ler("thiago")["conteudo"] == "999999999"
    assert lote.ler("marcelo")["salvo_por"] == "MARCELO"


def test_quem_nunca_salvou_nao_recebe_o_lote_de_outro(sem_a_migracao):
    from app.apps.analisesps import lote

    lote.salvar("111111111", "MARCELO", "marcelo")
    assert lote.ler("karla")["conteudo"] == ""


def test_o_que_ficou_no_armario_e_trazido_quando_a_tabela_aparece(monkeypatch):
    """O momento em que o botão finalmente é apertado. Sem esta passagem,
    apertar o botão pareceria APAGAR os filtros de todo mundo."""
    banco = BancoDeMentira()
    monkeypatch.setattr("app.apps.analisesps.db.conexao", banco.conexao)
    monkeypatch.setattr("app.apps.analisesps.db.tem_coluna",
                        lambda tabela, coluna: False)
    monkeypatch.setattr("app.apps.analisesps.db.consultar_um", banco.consultar_um)

    preferencias.gravar("marcelo", preferencias.FILTRO, {"conta": ["A"]})

    # Agora o botão foi apertado: a tabela boa existe, e está vazia.
    gravadas = []

    def consultar_um(sql, params=()):
        if "analisesps.preferencias" in sql:
            return None                      # a tabela nova ainda sem linhas
        return banco.consultar_um(sql, params)

    class Conexao:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=()):
            if "analisesps.preferencias" in sql:
                gravadas.append(params)
            else:
                banco.conexao().execute(sql, params)

        def commit(self):
            pass

    monkeypatch.setattr("app.apps.analisesps.db.consultar_um", consultar_um)
    monkeypatch.setattr("app.apps.analisesps.db.conexao", lambda: Conexao())
    monkeypatch.setattr("app.apps.analisesps.db.tem_coluna",
                        lambda tabela, coluna: True)

    assert preferencias.ler("marcelo", preferencias.FILTRO) == {"conta": ["A"]}
    assert gravadas, "o filtro guardado não foi trazido para a tabela nova"
