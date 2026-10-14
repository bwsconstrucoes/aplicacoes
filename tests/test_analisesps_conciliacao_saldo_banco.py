# -*- coding: utf-8 -*-
"""
CONCILIAÇÃO › OS "ÚLTIMOS LANÇAMENTOS" DO BRADESCO E ONDE O SALDO NÃO BATE —
14/10/2026, com banco de verdade.

O dono gerou o extrato de 01 a 05/10 e o arquivo trouxe seis linhas de 13/10
que ele não pediu; o cabeçalho diz o dia do download (14/10) nos dois campos do
período, e o saldo vem sem data ("00000000"). Nada as distingue, então o corte é
só onde a pessoa disser ("Importar só até"). O arquivo daqui reproduz isso com
valores inventados.
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


def test_o_saldo_sem_data_e_do_dia_do_download_e_o_corte_e_onde_a_pessoa_diz(banco_conc):
    from app.apps.analisesps import conciliacao_ofx
    lido = conciliacao_ofx.ler(ofx_bradesco(EXTRATO))
    assert lido.baixado_em == dt.date(2026, 10, 14)
    assert lido.saldo_em == dt.date(2026, 10, 14), "sem DTASOF, vale o dia do download"
    cortado = conciliacao_ofx.cortar(lido, dt.date(2026, 10, 5))
    assert len(cortado.lancamentos) == 4 and len(cortado.cortadas) == 2
    assert cortado.periodo_fim == dt.date(2026, 10, 5)
    assert cortado.saldo == lido.saldo and cortado.saldo_em == lido.saldo_em
    assert cortado.transacoes_no_arquivo == 4, "as cortadas não são 'perdidas na leitura'"


def _entrar(cliente):
    from tests.test_analisesps_usuarios_banco import SENHA_MESTRE_OPERADOR
    cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})


def _mandar(cliente, rota, conteudo, **campos):
    import io
    dados = {"extrato": (io.BytesIO(conteudo), "Bradesco.OFX"), **campos}
    return cliente.post(f"/analisesps/api/conciliacao/{rota}", data=dados,
                        content_type="multipart/form-data").get_json()


def test_a_IMPORTACAO_so_corta_quando_a_pessoa_pede_e_o_saldo_confere(banco_conc, app):
    """14/10/2026: nada no arquivo distingue os "últimos lançamentos" — o dono
    recusou o corte automático. Sem data, entra tudo."""
    from app.apps.analisesps import conciliacao
    # saldo de partida que faz o arquivo bater: 488,68 − movimento inteiro
    conta_id = conta_de_teste(saldo_inicial="2.650,67", saldo_inicial_em="2026-09-30")
    with app.test_client() as cliente:
        _entrar(cliente)
        tudo = _mandar(cliente, "conferir", ofx_bradesco(EXTRATO), conta_id=str(conta_id))
        assert tudo["ok"] and tudo["ate"] == "" and tudo["novas"] == 6 and tudo["saldo_bate"]
        assert "ultimos" not in tudo

        d = _mandar(cliente, "conferir", ofx_bradesco(EXTRATO), conta_id=str(conta_id),
                    ate="2026-10-05")
        assert d["ate"] == "2026-10-05" and d["cortadas_total"] == 2 and d["novas"] == 4
        assert d["saldo_em"] == "2026-10-14"
        assert d["saldo_bate"], d      # as 4 novas + as 2 deixadas de fora

        f = _mandar(cliente, "importar", ofx_bradesco(EXTRATO), conta_id=str(conta_id),
                    ate="2026-10-05")
        assert f["ok"] and f["gravadas"] == 4
        assert conciliacao.quantas_na_conta(conta_id) == 4

        # o extrato seguinte traz as duas de 13/10: entram, sem repetir
        f = _mandar(cliente, "importar", ofx_bradesco(EXTRATO), conta_id=str(conta_id))
        assert f["gravadas"] == 2 and conciliacao.quantas_na_conta(conta_id) == 6
        assert f["saldo_bate"]


def test_ONDE_O_SALDO_NAO_BATE_acha_o_intervalo_e_a_linha_repetida(banco_conc, app):
    from app.apps.analisesps import conciliacao, conciliacao_saldo as cs
    conta_id = conta_de_teste(saldo_inicial="1.000,00", saldo_inicial_em="2026-09-01")
    for dia, valor, texto in [(2, "-100.00", "A"), (5, "-250.00", "BOLETO X"),
                              (6, "-250.00", "BOLETO X"),   # a repetida
                              (10, "500.00", "PIX"), (20, "-50.00", "TARIFA")]:
        conciliacao.acrescentar_a_mao(conta_id, dt.date(2026, 9, dia), texto, D(valor), quem="T")
    # O banco: em 03/09 tinha 900 (bate); em 15/09, 1.150 (aqui dá 900: −250)
    cs.gravar_ponto(conta_id, "03/09/2026", "900,00", "T")
    cs.gravar_ponto(conta_id, "15/09/2026", "1.150,00", "T")
    cs.gravar_ponto(conta_id, "30/09/2026", "1.100,00", "T")

    d = cs.diagnostico(conta_id)
    assert [p["diferenca"] for p in d["pontos"]] == [D("250.00"), D("250.00"), D("0"), D("0")]
    assert not d["bate_hoje"]
    [j] = d["janelas"]
    assert (j["de"], j["ate"]) == (dt.date(2026, 9, 3), dt.date(2026, 9, 15))
    assert j["mudanca"] == D("250.00")
    assert {l["data"].day for l in j["mesmo_valor"]} == {5, 6}
    assert {l["data"].day for l in j["repetidas"]} == {5, 6}
    assert j["dia_do_meio"] == dt.date(2026, 9, 9)

    with app.test_client() as cliente:
        _entrar(cliente)
        r = cliente.get(f"/analisesps/api/conciliacao/saldo/diagnostico?conta_id={conta_id}").get_json()
        assert r["ok"] and r["janelas"][0]["dia_do_meio"] == "2026-09-09"
        r = cliente.post("/analisesps/api/conciliacao/saldo/ponto",
                         json={"conta_id": conta_id, "data": "2026-09-15", "apagar": True}).get_json()
        assert r["ok"] and len(r["pontos"]) == 3
        r = cliente.post("/analisesps/api/conciliacao/saldo/ponto",
                         json={"conta_id": conta_id, "data": "2026-09-09", "saldo": ""}).get_json()
        assert not r["ok"] and "saldo" in r["erro"]
        tela = cliente.get(f"/analisesps/conciliacao?conta_id={conta_id}").get_data(as_text=True)
        assert 'id="btn-onde-saldo"' in tela and 'id="cartao-saldo"' in tela


def test_o_SALDO_DO_OFX_importado_vira_ponto(banco_conc):
    from app.apps.analisesps import conciliacao, conciliacao_ofx, conciliacao_saldo as cs
    conta_id = conta_de_teste(saldo_inicial="0", saldo_inicial_em="2026-09-30")
    lido = conciliacao_ofx.cortar(conciliacao_ofx.ler(ofx_bradesco(EXTRATO, saldo="0")),
                                  dt.date(2026, 10, 5))
    conciliacao.importar(conta_id, lido, "Bradesco.OFX", "T")
    [ponto, inicial] = cs.diagnostico(conta_id)["pontos"]
    assert ponto["data"] == dt.date(2026, 10, 14) and ponto["origem"] == "arquivo"
    assert inicial["origem"] == "inicial"
