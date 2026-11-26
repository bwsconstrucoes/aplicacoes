# -*- coding: utf-8 -*-
"""CANCELAR SP PELO SISTEMA (26/11/2026) — `cancelar_sp.py`.

O dono: *"um botão tanto na parte de solicitações quanto em lote, que vai ser
cancelar SP. E também no modal (…) Você vai criar um novo card no Pipefy (…) pipe
301426645, (…) em mutation, se tiver mais de um, para economizar chamadas de API (…)
antes de cancelar, abre um modal listando o que vai ser cancelado."*
"""
from __future__ import annotations

import pytest


def test_os_campos_do_card_sao_os_que_o_dono_ditou():
    from app.apps.analisesps import cancelar_sp
    assert cancelar_sp.PIPE_DE_PROCEDIMENTOS == "301426645"
    assert {v["campo"]: v["valor"] for v in cancelar_sp.valores_do_card("1384831001")} == {
        "selecione_o_procedimento": "Cancelar SP",
        "n_da_solicita_o": "1384831001",
        "motivo": "Cancelado pelo Sistema AnaliseSPs",
        "colaborador_solicitante": ["620439304"],
    }


def test_VARIOS_cards_numa_mutation_so_e_um_recusado_nao_esconde_os_outros(monkeypatch):
    from app.apps.analisesps import pipefy
    chamadas = []

    def falso(consulta, token=None):
        chamadas.append(consulta)
        n = consulta.count("createCard(")
        dados = {f"c{i}": {"card": {"id": str(900 + i), "url": f"https://p/{900 + i}"}}
                 for i in range(n) if i != 1}
        return dados, ({"c1": "Campo inválido"} if n > 1 else {})
    monkeypatch.setattr(pipefy, "graphql_parcial", falso)
    cards = [{"chave": str(sp), "titulo": f"Cancelar SP {sp}",
              "valores": [{"campo": "n_da_solicita_o", "valor": str(sp)},
                          {"campo": "colaborador_solicitante", "valor": ["620439304"]}]}
             for sp in range(1, 13)]
    feito = pipefy.criar_cards("301426645", cards, token="t")
    assert len(chamadas) == 2, "12 cards = 2 chamadas (10 por vez)"
    assert chamadas[0].startswith("mutation {") and chamadas[0].count("createCard(") == 10
    assert 'pipe_id: 301426645' in chamadas[0]
    assert 'field_value: ["620439304"]' in chamadas[0]
    assert feito["1"] == {"id": "900", "link": "https://p/900"}
    assert feito["2"] == {"erro": "Campo inválido"}
    assert feito["11"]["id"] == "900" and feito["12"] == {"erro": "Campo inválido"}


@pytest.fixture
def banco_cancelar(banco_analisesps):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        for sp in [("1400000001", "CREDOR A", "100.00", "Pagar"),
                   ("1400000002", "CREDOR B", "200.00", "Pagar"),
                   ("1400000003", "CREDOR C", "300.00", "Cancelado")]:
            conn.execute("INSERT INTO analisesps.sps (id, credor, valor_num, status_pgt) "
                         "VALUES (?, ?, ?, ?)", sp)
        conn.commit()


@pytest.mark.banco
def test_a_previa_diz_o_que_VAI_e_o_que_NAO_vai(banco_cancelar):
    from app.apps.analisesps import cancelar_sp
    vista = cancelar_sp.previa(["1400000001", "1400000003", "999"])
    assert vista["vao"] == ["1400000001"]
    por = {s["id"]: s for s in vista["sps"]}
    assert por["1400000003"]["cancelada"] and not por["999"]["achada"]


@pytest.mark.banco
def test_confirmar_cria_os_cards_e_registra_no_LOG(banco_cancelar):
    from app.apps.analisesps import cancelar_sp
    from app.apps.analisesps.db import consultar
    pedidos = []

    def criar(pipe, cards):
        pedidos.append((pipe, [c["chave"] for c in cards]))
        return {c["chave"]: ({"id": "77" + c["chave"][-1], "link": "https://p"}
                             if c["chave"] != "1400000002" else {"erro": "recusado"})
                for c in cards}
    feito = cancelar_sp.pedir(["1400000001", "1400000002", "1400000003"], "MARCELO",
                              "Operador", criar=criar)
    assert pedidos == [("301426645", ["1400000001", "1400000002"])], "a cancelada não vai"
    assert [c["sp"] for c in feito["criados"]] == ["1400000001"]
    assert feito["erros"] == {"1400000002": "recusado"} and feito["pulados"] == ["1400000003"]
    log = consultar("SELECT sp_id, acao, pessoa, status FROM analisesps.log_alteracoes")
    assert [tuple(l) for l in log] == [("1400000001", "Cancelar SP", "MARCELO", "enviado")]
    # a segunda vez a janela avisa que já foi pedido
    vista = cancelar_sp.previa(["1400000001"])
    assert vista["sps"][0]["pedido"]["por"] == "MARCELO"
    # e nada muda na SP: quem cancela é o Make
    assert consultar("SELECT status_pgt FROM analisesps.sps WHERE id = '1400000001'")[0][0] == "Pagar"


@pytest.mark.banco
def test_tudo_ja_cancelado_e_recusado_sem_ir_ao_Pipefy(banco_cancelar):
    from app.apps.analisesps import cancelar_sp
    with pytest.raises(cancelar_sp.ErroDoCancelamento):
        cancelar_sp.pedir(["1400000003"], criar=lambda *a: 1 / 0)
