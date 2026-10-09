# -*- coding: utf-8 -*-
"""Consultar no Omie e equalizar a SPsBD (08/10/2026) — as regras, sem banco."""
import pytest

from app.apps.analisesps import pagamento_omie as po
from app.apps.analisesps import pipefy


def test_o_codigo_e_o_da_coluna_P_ou_Int_mais_a_SP():
    assert po.codigo_de_integracao("1426036778", " Int1426036778 ") == "Int1426036778"
    assert po.codigo_de_integracao("1426036778", "") == "Int1426036778"


def test_a_conta_vai_so_com_o_numero_como_o_BaixaBradesco_grava():
    assert po.conta_da_planilha("Bradesco - 50024-0") == "50024-0"
    assert po.conta_da_planilha("BD 7011-4 (Ag 3311)") == "7011-4"
    assert po.conta_da_planilha("Caixa da obra") == "Caixa da obra"
    assert po.conta_da_planilha("") == ""


def test_campo_vazio_no_card_NAO_apaga_o_que_a_planilha_sabe():
    assert po.valores_do_card({"data": "2026-10-07", "comprovante": "",
                               "banco": ""}) == {"data_pagamento": "07/10/2026"}
    assert po.valores_do_card({"data": "07/10/2026 14:30",
                               "comprovante": "https://x/y.html",
                               "banco": "Bradesco 92945-8"}) == {
        "data_pagamento": "07/10/2026", "comprovante": "https://x/y.html",
        "_ak": "92945-8"}
    assert po.valores_do_card({"data": "lixo"}) == {}


def test_o_que_FALTA_na_planilha():
    assert po.falta_na_planilha({"status_pgt": "Pagar"}) == ["status", "data", "comprovante"]
    assert po.falta_na_planilha({"status_pgt": "Pago", "data_pagamento": "01/10/2026",
                                 "comprovante": "x"}) == []


def test_conector_e_anexo_chegam_como_lista_e_fica_o_primeiro():
    assert pipefy._primeiro_valor('["Bradesco - 50024-0"]') == "Bradesco - 50024-0"
    assert pipefy._primeiro_valor("[]") == ""
    assert pipefy._primeiro_valor(None) == ""
    assert pipefy._primeiro_valor("texto [x]") == "texto [x]"


def test_ler_os_cards_e_UMA_ida_por_leva_e_um_card_ruim_nao_esconde_os_outros(monkeypatch):
    idas = []

    def postar(consulta, token=None):
        idas.append(consulta)
        return {"data": {"c0": {"id": "11", "current_phase": {"id": "1", "name": "Pagar"},
                                "fields": [
                                    {"name": "x", "field": {"label": "Data do Pagamento"}, "value": "07/10/2026"},
                                    {"name": "x", "field": {"label": "Comprovante HTML/Email (Integração)"}, "value": "https://c"},
                                    {"name": "x", "field": {"label": "Banco do Pagamento"}, "value": '["BD 7011-4"]'}]},
                         "c1": None},
                "errors": [{"message": "card não encontrado", "path": ["c1"]}]}
    monkeypatch.setattr(pipefy, "_postar", postar)
    lidos = pipefy.ler_pagamentos(["11", "22"], token="t")
    assert len(idas) == 1
    assert lidos["11"] == {"fase_id": "1", "fase": "Pagar", "data": "07/10/2026",
                           "comprovante": "https://c", "banco": "BD 7011-4"}
    assert lidos["_erros"] == {"22": "card não encontrado"}


def test_mover_os_cards_em_LOTE_e_por_mutation(monkeypatch):
    idas = []

    def postar(consulta, token=None):
        idas.append(consulta)
        return {"data": {"c0": {"clientMutationId": None}, "c1": None},
                "errors": [{"message": "sem permissão", "path": ["c1"]}]}
    monkeypatch.setattr(pipefy, "_postar", postar)
    r = pipefy.mover_cards(["11", "22"], pipefy.FASE_PAGO_ALIMENTAR_OMIE, token="t")
    assert len(idas) == 1 and idas[0].startswith("mutation {")
    assert "destination_phase_id: 309521694" in idas[0]
    assert r == {"movidos": ["11"], "erros": {"22": "sem permissão"}}


def test_id_de_card_que_nao_e_numero_e_recusado_antes_de_ir_ao_pipefy():
    with pytest.raises(pipefy.ErroDoPipefy):
        pipefy.mover_cards(["11) { x }"], "1", token="t")


def test_CHAVE_PIX_A_ATUALIZAR_e_reconhecida():
    """09/10/2026: BeeVale/Pix com a chave "Atualizar Chave" não se agenda."""
    from app.apps.analisesps.pagamentos import chave_a_atualizar
    assert chave_a_atualizar("BeeVale", "Chave Pix: Atualizar Chave")
    assert chave_a_atualizar("Pix", "Chave Pix: ATUALIZAR CHAVE PIX")
    assert chave_a_atualizar("BEEVALE", "atualizar chave")
    assert not chave_a_atualizar("BeeVale", "Chave Pix: 123.456.789-01")
    assert not chave_a_atualizar("Boleto", "Atualizar Chave"), "boleto não tem chave Pix"
    assert not chave_a_atualizar("BeeVale", "")


def test_SEM_NF_destaca_menos_BeeVale_rescisao_ferias_e_salarios():
    """09/10/2026: *"tudo que não tiver número de nota (…) um destaque. Se for
    BeeVale, não precisa (…) rescisão, férias, salários e ordenados não."*"""
    from app.apps.analisesps.pagamentos import falta_nota, sem_validacao
    assert falta_nota("Boleto", "Material", "", "Pagar")
    assert falta_nota("Pix", "Serviço", " ", "Pago"), "paga sem nota também se destaca"
    assert not falta_nota("Boleto", "Material", "1234", "Pagar")
    assert not falta_nota("BeeVale", "Material", "", "Pagar")
    for tipo in ("Rescisão", "RESCISOES", "Férias", "Salários e Ordenados"):
        assert not falta_nota("Pix", tipo, "", "Pagar"), tipo
    assert not falta_nota("Boleto", "Material", "", "Cancelado")
    assert sem_validacao("", "Pagar") and sem_validacao(None, "")
    assert not sem_validacao("Sim", "Pagar")
    assert not sem_validacao("", "Pago") and not sem_validacao("", "Cancelado")
