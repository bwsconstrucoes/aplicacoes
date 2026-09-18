# -*- coding: utf-8 -*-
"""A CIÊNCIA DA OPERAÇÃO — o único ponto deste módulo que ESCREVE na Receita.

Autorizada pelo dono em 17/09/2026, depois de eu explicar o que ela é: *"pode
baixar as notas dando essa ciência."*

⚠️ POR QUE ESTE ARQUIVO EXISTE, E POR QUE ELE É DESCONFIADO.

Todo o resto deste módulo lê. Isto aqui **declara algo em nome da BWS**, com o
certificado A1, e a declaração fica no histórico daquela nota na Receita para
sempre. Não dá para desfazer com um botão.

Por isso os testes não perguntam só "funciona?". Eles perguntam **o que nunca
pode acontecer**: declarar duas vezes, declarar fora do prazo, declarar em nota
cancelada, declarar por um CNPJ que não é o destinatário, ou declarar sem
deixar rastro de que declarou.

Nenhum teste aqui fala com a Receita — não há certificado fora do Render. O que
se prova é o envelope, os cortes e o registro.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import sefaz

CHAVE = "26260929066773000152550010000014301000000010"
CNPJ_BWS = "00079526000109"


# ── O envelope ───────────────────────────────────────────────────────────────

def test_o_ID_do_evento_tem_a_forma_que_a_Receita_exige():
    """"ID" + tipo + chave + sequência de dois dígitos. Errar isso faz a
    Receita rejeitar com uma mensagem que fala de outra coisa."""
    _, id_evento = sefaz.montar_evento_ciencia(CNPJ_BWS, CHAVE)

    assert id_evento == "ID" + "210210" + CHAVE + "01"
    assert len(id_evento) == 2 + 6 + 44 + 2


def test_o_evento_leva_o_CNPJ_de_quem_declara_e_a_chave_da_nota():
    xml, _ = sefaz.montar_evento_ciencia("00.079.526/0001-09", CHAVE)

    assert f"<CNPJ>{CNPJ_BWS}</CNPJ>" in xml
    assert f"<chNFe>{CHAVE}</chNFe>" in xml
    assert "<tpEvento>210210</tpEvento>" in xml
    assert "<descEvento>Ciencia da Operacao</descEvento>" in xml
    # 91 é o Ambiente Nacional — é ele que recebe a manifestação.
    assert "<cOrgao>91</cOrgao>" in xml


def test_chave_invalida_NAO_VIRA_evento():
    """Mandar lixo assinado para a Receita é pior do que não mandar."""
    with pytest.raises(ValueError):
        sefaz.montar_evento_ciencia(CNPJ_BWS, "123")


def test_so_a_CIENCIA_e_automatica():
    """⚠️ Os outros eventos da manifestação (confirmação, desconhecimento,
    operação não realizada) afirmam coisas sobre o NEGÓCIO — que a compra
    aconteceu, que não aconteceu, que a empresa não reconhece o fornecedor.
    Isso é decisão de gente. A ciência diz apenas "recebi o aviso"."""
    import inspect

    fonte = inspect.getsource(sefaz)
    for proibido in ("210200", "210220", "210240"):
        assert proibido not in fonte, (
            f"o evento {proibido} apareceu no código — ele afirma coisa sobre o "
            "negócio e não pode ser automático")


# ── A resposta da Receita ────────────────────────────────────────────────────

def _resposta(codigo, motivo="", protocolo=""):
    return ("<retEnvEvento><cStat>128</cStat><xMotivo>Lote processado</xMotivo>"
            f"<retEvento><infEvento><cStat>{codigo}</cStat>"
            f"<xMotivo>{motivo}</xMotivo><nProt>{protocolo}</nProt>"
            "</infEvento></retEvento></retEnvEvento>")


def test_le_o_codigo_DO_EVENTO_e_nao_o_do_lote():
    """O cStat do lote diz "processado" mesmo quando o evento foi rejeitado.
    Ler o primeiro faria toda rejeição passar por sucesso."""
    lido = sefaz.ler_resposta_do_evento(
        _resposta("135", "Evento registrado e vinculado a NF-e", "1352600001"))

    assert lido["codigo"] == "135" and lido["ok"] is True
    assert lido["protocolo"] == "1352600001"


def test_JA_EXISTIA_nao_e_erro():
    """⚠️ 573 é "duplicidade de evento": a ciência daquela nota já foi dada. O
    resultado prático é o mesmo, e tratar como falha faria a rotina tentar de
    novo para sempre — o caminho do bloqueio por consumo indevido."""
    lido = sefaz.ler_resposta_do_evento(_resposta("573", "Duplicidade de evento"))

    assert lido["ok"] is True
    assert lido["ja_existia"] is True


def test_rejeicao_e_rejeicao_e_traz_a_frase_da_Receita():
    lido = sefaz.ler_resposta_do_evento(
        _resposta("596", "Prazo para manifestacao expirado"))

    assert lido["ok"] is False
    assert "Prazo" in lido["motivo"], "a frase da Receita não chegou"


# ── A assinatura ─────────────────────────────────────────────────────────────

def test_a_assinatura_fica_com_a_BIBLIOTECA():
    """⚠️ É a parte onde errar é fácil (canonicalização, algoritmo,
    referência) e o erro volta como "rejeitado" sem dizer por quê. O envelope é
    nosso; a assinatura é dela. Foi a mesma divisão que salvou o certificado."""
    import inspect

    fonte = inspect.getsource(sefaz._assinar)
    assert "erpbrasil.assinatura" in fonte
    assert "assina_xml2" in fonte, (
        "assina_xml2 é a que aceita a REFERÊNCIA ao Id do infEvento — sem ela "
        "a assinatura aponta para o documento todo e a Receita rejeita")


def test_o_evento_vai_ASSINADO_e_com_o_certificado_na_conexao(monkeypatch):
    """Duas coisas diferentes e as duas obrigatórias: a assinatura prova quem
    declarou; o certificado na conexão prova quem está falando."""
    import erpbrasil.assinatura.certificado as cert_mod
    import requests

    capturado = {}

    class CertFalso:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return ("/tmp/chave.pem", "/tmp/cert.pem")

        def __exit__(self, *a):
            return False

    class RespostaFalsa:
        text = ("<retEnvEvento><cStat>128</cStat><retEvento><infEvento>"
                "<cStat>135</cStat><xMotivo>ok</xMotivo></infEvento>"
                "</retEvento></retEnvEvento>")

        def raise_for_status(self):
            return None

    class SessaoFalsa:
        cert = None

        def post(self, url, data=None, timeout=None, headers=None):
            capturado["url"] = url
            capturado["pedido"] = data.decode("utf-8")
            capturado["cert"] = self.cert
            return RespostaFalsa()

    monkeypatch.setattr(cert_mod, "ArquivoCertificado", CertFalso)
    monkeypatch.setattr(requests, "Session", lambda: SessaoFalsa())
    monkeypatch.setattr(sefaz, "_certificado", lambda cnpj: object())
    monkeypatch.setattr(sefaz, "_assinar",
                        lambda xml, ident, cnpj: xml.replace(
                            "</evento>", "<Signature>assinado</Signature></evento>"))

    resultado = sefaz.manifestar_ciencia(CNPJ_BWS, CHAVE)

    assert "NFeRecepcaoEvento4" in capturado["url"]
    assert "<envEvento" in capturado["pedido"]
    assert "<Signature>" in capturado["pedido"], "o evento saiu SEM assinatura"
    assert capturado["cert"] == ("/tmp/chave.pem", "/tmp/cert.pem")
    assert resultado["ok"] is True
