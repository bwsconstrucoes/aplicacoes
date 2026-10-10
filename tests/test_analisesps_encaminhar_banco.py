# -*- coding: utf-8 -*-
"""
ENCAMINHAR SPs PELO WHATSAPP — 10/10/2026, com banco de verdade.

O dono: *"selecionar, encaminhar informações pelo WhatsApp (…) uma mini base
de nome e telefone (…) três opções: informações, anexo, comprovante (…) a
mensagem padrão com número da SP, datas, descrição, valor, tipo de despesa,
centro de custo, credor, CPF/CNPJ, responsável, forma de pagamento — e o
boleto ou a chave Pix (…) por padrão manda tudo."*
"""
from urllib.parse import unquote

import pytest

from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco


def semear_duas():
    from tests.test_analisesps_banco import semear, sp
    semear([sp("1000000501", solicitacao="01/10/2026", vencimento="10/10/2026",
               credor="AÇO CEARENSE", documento="12.345.678/0001-90",
               descricao="Vergalhão", valor="1.234,50", tipo_despesa="Material",
               centro_custo="OBRA X", responsavel="Fulano", forma_pagamento="Boleto",
               codigo_barras="34191.79001 01043.510047", status_pgt="Pago",
               data_pagamento="09/10/2026",
               anexo_link="https://drive.google.com/file/d/ANEXO9/view",
               comprovante="https://www.dropbox.com/s/c/comp.pdf?dl=0"),
            sp("1000000502", credor="JOÃO", forma_pagamento="Pix", valor="50,00",
               info_pgt="joao@exemplo.com", status_pgt="Pagar")])


def test_a_MENSAGEM_traz_tudo_por_padrao_e_o_que_paga_conforme_a_forma(app):
    from app.apps.analisesps import encaminhar
    semear_duas()
    texto = encaminhar.montar_mensagem(["1000000501", "1000000502"])
    boleto, pix = texto.split("———")
    for trecho in ("*Nº da SP:* 1000000501", "*Data da solicitação:* 01/10/2026",
                   "*Vencimento:* 10/10/2026", "*Descrição:* Vergalhão",
                   "*Valor:* R$ 1.234,50", "*Tipo de despesa:* Material",
                   "*Centro de custo:* OBRA X", "*Credor:* AÇO CEARENSE",
                   "*CPF/CNPJ:* 12.345.678/0001-90", "*Responsável:* Fulano",
                   "*Forma de pagamento:* Boleto",
                   "*Código do boleto:* 34191.79001 01043.510047",
                   "*Situação do pagamento:* Pago em 09/10/2026",
                   "uc?export=download&id=ANEXO9", "comp.pdf?dl=1"):
        assert trecho in boleto, trecho
    assert "*Chave Pix:* joao@exemplo.com" in pix
    assert "Comprovante" not in pix and "Vencimento" not in pix, "campo vazio não sai"


def test_DESMARCAR_tira_da_mensagem(app):
    from app.apps.analisesps import encaminhar
    semear_duas()
    texto = encaminhar.montar_mensagem(["1000000501"], campos=["id", "valor"],
                                       anexo=False, comprovante=True)
    assert "*Nº da SP:* 1000000501" in texto and "*Valor:*" in texto
    assert "Credor" not in texto and "Anexo" not in texto and "Comprovante" in texto


def test_CONTATOS_guardam_nome_e_telefone_e_o_link_abre_o_whatsapp(app):
    from app.apps.analisesps import encaminhar
    assert encaminhar.gravar_contato("Fornecedor A", "(85) 99999-1234")["ok"]
    assert not encaminhar.gravar_contato("Sem número", "123")["ok"]
    assert encaminhar.gravar_contato("Fornecedor A de novo", "5585999991234")["ok"]
    contatos = encaminhar.listar_contatos()
    assert [c["telefone"] for c in contatos] == ["5585999991234"], "o mesmo número não duplica"
    link = encaminhar.link_whatsapp("85 99999-1234", "*SP* 1")
    assert link.startswith("https://wa.me/5585999991234?text=") and "*SP* 1" in unquote(link)
    assert encaminhar.link_whatsapp("", "x").startswith("https://wa.me/?text="), "sem número: escolher no WhatsApp"
    assert encaminhar.remover_contato("85999991234")["contatos"] == []


def test_a_JANELA_pelas_rotas(app):
    semear_duas()
    with app.test_client() as c:
        c.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        lista = c.get("/analisesps/solicitacoes?f=1&busca=10000005",
                      follow_redirects=True).get_data(as_text=True)
        previa = c.post("/analisesps/api/encaminhar/previa",
                        json={"ids": ["1000000501", "1000000502"]}).get_json()
        msg = c.post("/analisesps/api/encaminhar/mensagem",
                     json={"ids": ["1000000502"], "campos": ["id"], "anexo": True,
                           "comprovante": True, "telefone": "85988887777"}).get_json()
        contato = c.post("/analisesps/api/encaminhar/contatos",
                         json={"nome": "Maria", "telefone": "85 98888-7777"}).get_json()
    assert 'id="ba-encaminhar"' in lista and 'id="dialogo-enc"' in lista
    assert [s["id"] for s in previa["sps"]] == ["1000000501", "1000000502"]
    assert previa["sps"][0]["anexos"] == 1 and previa["sps"][1]["comprovantes"] == 0
    assert msg["texto"] == "*Solicitação de Pagamento*\n*Nº da SP:* 1000000502"
    assert msg["link"].startswith("https://wa.me/5585988887777?text=")
    assert contato["ok"] and contato["contatos"][0]["nome"] == "Maria"


def test_ARQUIVOS_GERADOS_mandam_cada_geracao_ao_lote_com_titulo(app):
    """10/10/2026: *"seleciono e envio para o lote aquelas SPs; o cabeçalho do
    lote já com competência e tipo de arquivo; selecionar vários, ele cria
    vários lotes."*"""
    import json
    from app.apps.analisesps import lote
    texto, titulo = lote.acrescentar_grupo("", ["1000000501"], "Folha 09/2026 · quinzena")
    assert titulo == "Folha 09/2026 · quinzena"
    _t, numero = lote.acrescentar_grupo(texto, ["1"], "1000000501")
    assert numero == "Novo Lote 1", "título que parece SP não vira título"
    semear_duas()
    grupos = [{"titulo": "Folha 09/2026 · quinzena · BeeVale", "ids": ["1000000501"]},
              {"titulo": "Folha 09/2026 · fim de mês", "ids": ["1000000502"]}]
    with app.test_client() as c:
        c.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        tela = c.post("/analisesps/lote", data={"acao": "receber_grupos",
                                                 "grupos": json.dumps(grupos)},
                      follow_redirects=True).get_data(as_text=True)
    assert "2 grupo(s) entraram no lote" in tela
    caixa = tela[tela.index('id="lote-conteudo"'):]
    caixa = caixa[:caixa.index("</textarea>")]
    assert (caixa.index("Folha 09/2026 · quinzena · BeeVale") < caixa.index("1000000501")
            < caixa.index("Folha 09/2026 · fim de mês") < caixa.index("1000000502")), \
        "um grupo por geração, a primeira marcada no topo"


def test_ENVIO_EM_LOTE_cada_SP_com_as_suas_caixinhas(app):
    """10/10/2026: *"tem que ser uma coisa para envio em lote: dados resumidos de
    cada uma e as caixinhas para envio."*"""
    semear_duas()
    itens = [{"id": "1000000501", "info": False, "anexo": False, "comprovante": True},
             {"id": "1000000502", "info": False, "anexo": False, "comprovante": False}]
    with app.test_client() as c:
        c.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        previa = c.post("/analisesps/api/encaminhar/previa", json={"ids": ["1000000501"]}).get_json()
        msg = c.post("/analisesps/api/encaminhar/mensagem",
                     json={"ids": ["1000000501", "1000000502"], "itens": itens}).get_json()
        nada = c.post("/analisesps/api/encaminhar/mensagem",
                      json={"ids": ["1000000502"], "itens": itens[1:]})
    assert previa["sps"][0]["vencimento"] == "10/10/2026"
    # sem as informações, vai só o número junto do comprovante; a 502 fica fora
    assert msg["texto"].startswith("*Solicitação de Pagamento*\n*Nº da SP:* 1000000501\n🧾")
    assert "Credor" not in msg["texto"] and "1000000502" not in msg["texto"]
    assert nada.status_code == 400
