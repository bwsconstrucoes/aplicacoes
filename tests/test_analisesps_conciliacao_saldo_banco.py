# -*- coding: utf-8 -*-
"""
CONCILIAÇÃO › O OFX DO BRADESCO, O SALDO ANTES E DEPOIS, A DATA DE INÍCIO —
14/10/2026, com banco de verdade.

O dono gerou o extrato de 01 a 05/10 e o arquivo trouxe seis linhas de 13/10
que ele não pediu; o cabeçalho diz o dia do download (14/10) nos dois campos do
período, e o saldo vem sem data ("00000000"). O corte manual ("Importar só
até") e a janela "Onde o saldo não bate" saíram a pedido dele, no mesmo dia
("não serviu pra nada"). O arquivo daqui reproduz o do Bradesco com valores
inventados.
"""
import datetime as dt
from decimal import Decimal as D

import pytest

from tests.test_analisesps_conciliacao_banco import (  # noqa: F401 — fixtures
    app, banco_acesso, banco_conc, conta_de_teste, ofx)

pytestmark = pytest.mark.banco


def ofx_bradesco(transacoes, saldo="488.68"):
    """Como o Bradesco manda: período = dia do download, saldo sem data."""
    return ofx(transacoes, ini="20261014120000", fim="20261014120000",
               saldo=saldo).replace(b"<DTASOF>20261014120000", b"<DTASOF>00000000")


EXTRATO = [("20261001", "-144.76", "N1012D", "PAGTO ELETRON COBRANCA 1"),
           ("20261002", "0.02", "N1016F", "RENTAB.INVEST FACILCRED*"),
           ("20261005", "7000.00", "N10199", "PIX RECEBIDO"),
           ("20261005", "-6899.39", "N101D9", "PAGTO ELETRON COBRANCA 2"),
           # os "últimos lançamentos", oito dias depois
           ("20261013", "-1832.12", "N10269", "DEBITO AUTOMATICO"),
           ("20261013", "-285.74", "N102AB", "CONTA DE TELEFONE")]


def test_o_saldo_sem_data_e_do_dia_do_download(banco_conc):
    from app.apps.analisesps import conciliacao_ofx
    lido = conciliacao_ofx.ler(ofx_bradesco(EXTRATO))
    assert lido.baixado_em == dt.date(2026, 10, 14)
    assert lido.saldo_em == dt.date(2026, 10, 14), "sem DTASOF, vale o dia do download"


def _entrar(cliente):
    from tests.test_analisesps_usuarios_banco import SENHA_MESTRE_OPERADOR
    cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})


def _mandar(cliente, rota, conteudo, **campos):
    import io
    dados = {"extrato": (io.BytesIO(conteudo), "Bradesco.OFX"), **campos}
    return cliente.post(f"/analisesps/api/conciliacao/{rota}", data=dados,
                        content_type="multipart/form-data").get_json()


def test_a_IMPORTACAO_mostra_o_saldo_da_conta_antes_e_depois_e_confere_com_o_banco(banco_conc, app):
    """14/10/2026: "na importação do extrato, exibir o novo saldo após a
    importação dos novos lançamentos"."""
    from app.apps.analisesps import conciliacao
    # saldo de partida que faz o arquivo bater: 488,68 − movimento inteiro
    conta_id = conta_de_teste(saldo_inicial="2.650,67", saldo_inicial_em="2026-09-30")
    with app.test_client() as cliente:
        _entrar(cliente)
        d = _mandar(cliente, "conferir", ofx_bradesco(EXTRATO), conta_id=str(conta_id))
        assert d["ok"] and d["novas"] == 6 and d["saldo_bate"]
        assert d["saldo_conta_antes"] == "2650.67"
        assert d["saldo_conta_depois"] == "488.68" and d["soma_das_novas"] == "-2161.99"
        assert "ate" not in d and "cortadas_total" not in d, "o corte manual saiu"
        f = _mandar(cliente, "importar", ofx_bradesco(EXTRATO), conta_id=str(conta_id),
                    ate="2026-10-05")
        assert f["gravadas"] == 6, "data de corte mandada é ignorada"
        assert f["saldo_conta_antes"] == "2650.67" and f["saldo_conta_depois"] == "488.68"
    assert conciliacao.saldo_da_conta(conta_id) == D("488.68")
    tela = app.test_client()
    _entrar(tela)
    html = tela.get(f"/analisesps/conciliacao?conta_id={conta_id}").get_data(as_text=True)
    assert "Onde o saldo não bate" not in html and "Importar só até" not in html


# ---------------------------------------------------------------------------
# A conciliação vale a partir de um dia (14/10/2026): "o que está muito para
# trás não me importa mais (…) uma única informação de saldo".
# ---------------------------------------------------------------------------
def test_o_que_e_ANTERIOR_AO_INICIO_fica_guardado_mas_fora_da_lista_e_das_contas(banco_conc, app):
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(saldo_inicial="1.000,00", saldo_inicial_em="2026-08-31")
    velha = conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 8, 10), "VELHA", D("-70.00"), quem="T")
    nova = conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 9, 2), "NOVA", D("-30.00"), quem="T")

    f = {"conta_id": conta_id}
    assert [l["id"] for l in conciliacao.listar(f)] == [nova]
    assert conciliacao.resumo(f)["pendentes"] == 1
    assert conciliacao.saldo_da_conta(conta_id) == D("970.00")
    assert conciliacao.anteriores_ao_inicio(conta_id) == 1
    todas = conciliacao.listar(dict(f, anteriores=True))
    assert {l["id"] for l in todas} == {velha, nova}
    saldos = {l["id"]: l["saldo"] for l in todas}
    assert saldos[nova] == D("970.00") and saldos[velha] == D("1000.00"), \
        "a anterior não mexe no saldo corrido"
    painel = conciliacao.panorama(2026)
    [linha] = [c for c in painel["contas"] if c["id"] == conta_id]
    assert linha["pendentes"] == 1 and linha["pendentes_total"] == 1

    with app.test_client() as cliente:
        _entrar(cliente)
        tela = cliente.get(f"/analisesps/conciliacao?conta_id={conta_id}").get_data(as_text=True)
        assert "ver o que é anterior" in tela and "VELHA" not in tela
        assert "O filtro está escondendo" not in tela
        tela = cliente.get(f"/analisesps/conciliacao?conta_id={conta_id}&anteriores=1").get_data(as_text=True)
        assert "VELHA" in tela


def test_o_OFX_nao_traz_o_que_e_anterior_ao_inicio_e_lista_tudo_o_que_sobra(banco_conc, app):
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(saldo_inicial="0", saldo_inicial_em="2026-10-01")
    # duas que o extrato não traz: uma digitada, uma repetida à mão
    for dia in (2, 2):
        conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 10, dia), "SOBRA", D("-9.00"), quem="T")
    with app.test_client() as cliente:
        _entrar(cliente)
        d = _mandar(cliente, "conferir", ofx_bradesco(EXTRATO), conta_id=str(conta_id))
        assert d["anteriores_total"] == 1 and d["inicio"] == "2026-10-01"
        assert d["novas"] == 5, "o lançamento de 01/10 é do dia do início: fica de fora"
        assert d["so_aqui_total"] == 2 and len(d["so_aqui"]) == 2
        assert all(l["id"] for l in d["so_aqui"]), "cada sobra vem com o número, para excluir"


def test_extrato_ANTIGO_baixado_hoje_nao_acusa_como_sobra_o_que_e_depois_dele(banco_conc, app):
    """14/10/2026 — "lançamentos de outra conta entraram nesta; se eu importar o
    OFX, vai ter lançamento a mais". A lista de sobras tem de pegar ESSES, e só
    dentro do que o arquivo cobre: o dia do download não é período."""
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(saldo_inicial="0", saldo_inicial_em="2026-09-30")
    outra = conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 10, 2), "DE OUTRA CONTA",
                                          D("-500.00"), quem="T")
    conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 10, 10), "CERTO, DEPOIS DO EXTRATO",
                                  D("-1.00"), quem="T")
    setembro_a_05 = [l for l in EXTRATO if l[0] <= "20261005"]
    with app.test_client() as cliente:
        _entrar(cliente)
        d = _mandar(cliente, "conferir", ofx_bradesco(setembro_a_05), conta_id=str(conta_id))
    assert d["periodo_fim"] == "2026-10-05"
    assert [l["id"] for l in d["so_aqui"]] == [outra]
    assert d["so_aqui"][0]["dia_vazio"] is False


# ---------------------------------------------------------------------------
# O extrato "por arquivo" do Bradesco traz as aplicações e resgates automáticos
# do Invest Fácil, e o saldo da conta corrente fica perto de zero (14/10/2026).
# ---------------------------------------------------------------------------
POR_ARQUIVO = [("20261001", "-500.00", "N1", "PAGTO ELETRON COBRANCA 1"),
               ("20261001", "500.00", "N2", "RESGATE INVEST FACIL"),
               ("20261002", "0.40", "N3", "RENTAB.INVEST FACILCRED*"),
               ("20261002", "1000.00", "N4", "PIX RECEBIDO"),
               ("20261002", "-1000.40", "N5", "APLIC.INVEST FACIL")]


def test_o_INVEST_FACIL_sai_por_padrao_e_o_saldo_do_arquivo_deixa_de_ser_comparado(banco_conc, app):
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(saldo_inicial="10.000,00", saldo_inicial_em="2026-09-30")
    arquivo = ofx_bradesco(POR_ARQUIVO, saldo="0.00")
    with app.test_client() as cliente:
        _entrar(cliente)
        d = _mandar(cliente, "conferir", arquivo, conta_id=str(conta_id))
        assert d["aplicacoes_no_arquivo"] == 2 and d["aplicacoes_total"] == 2
        assert d["novas"] == 3, "a rentabilidade fica; resgate e aplicação saem"
        assert d["saldo_banco"] == "", "o saldo da conta corrente (0,00) não é comparado"
        tudo = _mandar(cliente, "conferir", arquivo, conta_id=str(conta_id), aplicacoes="incluir")
        assert tudo["novas"] == 5 and tudo["aplicacoes_incluidas"] and tudo["saldo_banco"] == "0.00"
        f = _mandar(cliente, "importar", arquivo, conta_id=str(conta_id))
        assert f["gravadas"] == 3
    assert conciliacao.saldo_da_conta(conta_id) == D("10500.40")


def test_EXPORTAR_o_extrato_em_planilha_e_pdf_com_o_filtro_da_tela(banco_conc, app):
    """14/10/2026: "quero poder exportar o extrato conciliado em pdf ou em
    planilha". Sai o filtro da tela, todas as páginas, em ordem de data."""
    import io
    from openpyxl import load_workbook
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(saldo_inicial="1.000,00", saldo_inicial_em="2026-08-31")
    a = conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 9, 2), "PRIMEIRA", D("-100.00"), quem="T")
    conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 9, 5), "SEGUNDA", D("50.00"), quem="T")
    conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 8, 5), "ANTERIOR", D("7.00"), quem="T")
    conciliacao.marcar([a], True, "MARCELO")
    with app.test_client() as cliente:
        _entrar(cliente)
        tela = cliente.get(f"/analisesps/conciliacao?conta_id={conta_id}").get_data(as_text=True)
        assert "formato=pdf" in tela and "formato=xlsx" in tela
        r = cliente.get(f"/analisesps/conciliacao/exportar?formato=xlsx&conta_id={conta_id}")
        assert r.status_code == 200 and "spreadsheet" in r.mimetype
        aba = load_workbook(io.BytesIO(r.data)).active
        linhas = [[c.value for c in l] for l in aba.iter_rows(min_row=5, max_row=6)]
        assert [l[1] for l in linhas] == ["PRIMEIRA", "SEGUNDA"], "em ordem de data, sem a anterior"
        assert linhas[0][6] == "Sim" and linhas[0][7] == "MARCELO"
        assert float(linhas[0][5]) == 900.0 and float(linhas[1][5]) == 950.0
        so = cliente.get(f"/analisesps/conciliacao/exportar?formato=xlsx&conta_id={conta_id}"
                         "&situacao=pendentes")
        aba = load_workbook(io.BytesIO(so.data)).active
        assert aba.cell(5, 2).value == "SEGUNDA" and aba.cell(6, 2).value is None
        p = cliente.get(f"/analisesps/conciliacao/exportar?formato=pdf&conta_id={conta_id}")
        assert p.status_code == 200 and p.data.startswith(b"%PDF")


def test_as_NOVAS_vem_marcadas_a_PARECIDA_e_apontada_e_a_DESMARCADA_nao_entra(banco_conc, app):
    """21/10/2026: "queria que eles estivessem todos pré-selecionados, mas que
    pudessem ser desmarcados (…) aparece um item que já estava incluso e o
    sistema tenta colocar novamente" — RENTAB.INVEST FACILCRED* 0,01, que o
    banco ora manda com documento (3617438), ora sem."""
    import json
    from app.apps.analisesps import conciliacao
    conta_id = conta_de_teste(saldo_inicial="2.650,67", saldo_inicial_em="2026-09-30")
    conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 10, 2), "RENTAB.INVEST FACILCRED*",
                                  "0.02", documento="3617438", quem="MARCELO")
    with app.test_client() as cliente:
        _entrar(cliente)
        d = _mandar(cliente, "conferir", ofx_bradesco(EXTRATO), conta_id=str(conta_id))
        assert d["novas"] == 6 and all(l["marca"] for l in d["amostra"])
        [parecida] = [l for l in d["amostra"] if l["parecida"]]
        assert parecida["descricao"] == "RENTAB.INVEST FACILCRED*"
        assert "documento 3617438" in parecida["parecida"]
        assert d["data_do_saldo"] == "2026-10-14"
        f = _mandar(cliente, "importar", ofx_bradesco(EXTRATO), conta_id=str(conta_id),
                    pular=json.dumps([parecida["marca"]]))
        assert f["ok"] and f["gravadas"] == 5, "a desmarcada não entra"
    # 2.650,67 + 0,02 (a que já estava) + o movimento sem a repetida = o banco
    assert conciliacao.saldo_da_conta(conta_id) == D("488.68")
