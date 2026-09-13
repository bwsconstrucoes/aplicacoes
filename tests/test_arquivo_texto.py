"""O texto de dentro do documento — o que destrava perguntar sobre um contrato.

Até 12/09/2026 o texto do documento só era guardado quando a LEITURA POR IA
tinha rodado, e só das SEIS primeiras páginas (teto da leitura por IA, onde
página custa dinheiro). Um contrato de quarenta páginas ficava 85% invisível, e
documento arrastado para a tela sem passar pela IA não tinha texto nenhum.

Aqui não há IA: é a camada de texto do próprio PDF, de graça. Estes testes
provam as três coisas que sustentam a pergunta sobre documento:

  1. o texto sai INTEIRO, não só o começo;
  2. documento que é IMAGEM sai vazio — e quem pergunta ouve isso, em vez de
     receber resposta inventada sobre um documento que ninguém leu;
  3. o trecho citado é CONFERIDO contra o documento antes de ir para a tela.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.arquivo import texto as svc


def _pdf(paginas: list[str]) -> bytes:
    """Um PDF de verdade, com camada de texto, feito na hora."""
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    for conteudo in paginas:
        p = doc.new_page()
        p.insert_text((72, 100), conteudo, fontsize=11)
    dados = doc.tobytes()
    doc.close()
    return dados


# ---------------------------------------------------------------------------
# Extrair
# ---------------------------------------------------------------------------
def test_le_o_documento_inteiro_e_nao_so_as_primeiras_paginas():
    """A falha que isto tapa: o teto de SEIS páginas da leitura por IA valia
    também para o texto guardado, então contrato longo ficava mudo."""
    paginas = [f"Clausula {i} deste contrato de empreitada." for i in range(1, 31)]
    saiu = svc.extrair(_pdf(paginas), "contrato.pdf")
    assert "Clausula 1 " in saiu
    assert "Clausula 30 " in saiu, (
        "o texto parou antes do fim — é exatamente o defeito que impedia "
        "perguntar sobre um contrato longo")


def test_documento_que_e_imagem_sai_vazio():
    """Decisão do dono em 12/09/2026: *"não ler escaneados por hora"*.

    Vazio é a resposta CERTA aqui: quem pergunta ouve "este documento é uma
    imagem, não consigo ler o texto dele". Devolver qualquer outra coisa faria
    a IA opinar sobre um documento que ninguém leu.
    """
    assert svc.extrair(b"\x89PNG\r\n\x1a\n" + b"0" * 500, "foto.png") == ""
    assert svc.extrair(b"", "vazio.pdf") == ""


def test_arquivo_quebrado_nao_derruba_nada():
    """Documento ilegível é documento sem texto — nunca um erro que impeça
    alguém de arquivar o que precisa arquivar."""
    assert svc.extrair(b"%PDF-1.4 isto nao e um PDF de verdade", "torto.pdf") == ""


def test_texto_puro_tambem_vale():
    assert "reajuste anual" in svc.extrair(
        "Contrato com reajuste anual pelo INCC.".encode("utf-8"), "nota.txt")


# ---------------------------------------------------------------------------
# A CONFERÊNCIA DO TRECHO — a única defesa real contra citação inventada
# ---------------------------------------------------------------------------
DOCUMENTO = ("O prazo de garantia dos serviços executados é de 5 (cinco) anos, "
             "contados do recebimento definitivo da obra pela contratante.")


def test_trecho_que_esta_no_documento_passa():
    bons = svc.conferir_trechos(
        ["O prazo de garantia dos serviços executados é de 5 (cinco) anos"],
        DOCUMENTO)
    assert len(bons) == 1


def test_trecho_inventado_e_descartado():
    """O caso que importa: a IA escreve uma citação plausível que NÃO está no
    documento. Se ela chegasse à tela como citação, seria indistinguível de uma
    verdadeira — e é justamente o que ninguém tem como conferir."""
    bons = svc.conferir_trechos(
        ["O prazo de garantia dos serviços executados é de 10 (dez) anos"],
        DOCUMENTO)
    assert bons == []


def test_trecho_confere_mesmo_com_espaco_e_quebra_de_linha_diferentes():
    """O PDF quebra linha onde quer. Exigir espaçamento idêntico faria a
    conferência reprovar citação verdadeira — e aí ninguém confiaria nela."""
    bons = svc.conferir_trechos(
        ["O prazo de garantia dos serviços\n   executados   é de 5 (cinco) anos"],
        DOCUMENTO)
    assert len(bons) == 1


def test_trecho_curto_demais_nao_prova_nada():
    """"o prazo" aparece em qualquer contrato: passaria na conferência sem
    significar coisa nenhuma."""
    assert svc.conferir_trechos(["o prazo"], DOCUMENTO) == []


# ---------------------------------------------------------------------------
# Escolher o que sobe para a IA num documento longo
# ---------------------------------------------------------------------------
def test_documento_curto_sobe_inteiro():
    assert svc.pedacos_para_a_pergunta(DOCUMENTO, "qual a garantia") == DOCUMENTO


def test_documento_longo_sobe_pela_parte_que_fala_do_assunto():
    enchimento = "Disposições gerais sobre placas de sinalização. " * 2000
    doc = enchimento + "\nA multa por atraso é de 2% sobre o valor da parcela.\n" + enchimento
    escolhido = svc.pedacos_para_a_pergunta(doc, "qual a multa por atraso?",
                                            teto=8000)
    assert "multa por atraso" in escolhido
    assert len(escolhido) <= 8000 + svc.TAMANHO_DO_PEDACO
