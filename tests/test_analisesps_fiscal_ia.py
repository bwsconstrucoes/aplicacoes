# -*- coding: utf-8 -*-
"""Análise de SPs — a IA lendo o anexo.

O QUE ESTES TESTES GUARDAM. A IA é o passo mais caro e o mais perigoso desta
frente: caro porque cada leitura é cobrada, perigoso porque uma nota lida
errado de um PDF torto vira dedução indevida com cara de decisão tomada.

Por isso os testes aqui olham três coisas: que a IA **propõe e não decide**,
que ela **não roda sozinha**, e que o que ela lê **é conferido contra a SP**.

Nenhum teste chama a OpenAI: o leitor do ERP é dublado em todos.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import fiscal, fiscal_ia


CREDOR = "29066773000152"


def chave(cnpj=CREDOR, resto="550010000123456789012345"):
    return ("26" + "2609" + cnpj + resto + "0" * 44)[:44]


def sp(**campos):
    base = {"id": "1409289353", "credor": "SERTAO CASA E CONSTRUCAO",
            "documento": "29.066.773/0001-52", "valor": "269,00",
            "anexo_link": "https://exemplo/anexo.pdf"}
    base.update(campos)
    return base


def lido(**campos):
    base = {"tipo_documento": "NFE", "chave_acesso": chave(),
            "emitente_documento": CREDOR, "confianca": "ALTA",
            "numero_documento": "1430", "valor_total": "269.00"}
    base.update(campos)
    return base


# ---------------------------------------------------------------------------
# A CHAVE MANDA SOBRE O QUE A IA ACHOU QUE ERA
# ---------------------------------------------------------------------------
def test_a_categoria_sai_de_dentro_da_chave_e_nao_do_palpite_da_IA():
    """Se a IA leu 44 dígitos, os dígitos 21-22 são o modelo do documento por
    definição da Receita. Aí é certeza, não interpretação — e é isso que
    autoriza propor."""
    proposta = fiscal_ia.proposta_da_leitura(sp(), lido(
        chave_acesso=chave(resto="570010000999888777666555"),
        tipo_documento="NFE"))          # a IA disse NFe; a chave diz CT-e
    assert proposta["documentacao"] == "CT-e (Frete)", (
        "o palpite da IA venceu a chave")
    assert "de dentro dela" in proposta["motivo"]


def test_sem_chave_vale_o_tipo_que_a_IA_identificou_mas_com_menos_confianca():
    """Uma guia de imposto não tem chave de acesso. O tipo lido serve — só não
    vale tanto quanto um número que veio da própria Receita."""
    proposta = fiscal_ia.proposta_da_leitura(
        sp(), lido(tipo_documento="GUIA", chave_acesso="", emitente_documento=""))
    assert proposta["documentacao"] == "Guia de Tributo"
    assert proposta["confianca"] <= 60


def test_documento_que_NAO_e_fiscal_nao_ganha_categoria_chutada():
    """Um orçamento ou um comprovante bancário não são documento fiscal de
    despesa. Chutar uma categoria para eles é pior do que dizer "não sei"."""
    for tipo in ("ORCAMENTO", "COMPROVANTE", "OUTRO"):
        proposta = fiscal_ia.proposta_da_leitura(
            sp(), lido(tipo_documento=tipo, chave_acesso="",
                       emitente_documento=""))
        assert proposta["documentacao"] == "", tipo
        assert proposta["propoe"] is False, tipo


def test_confianca_BAIXA_da_IA_nao_vira_proposta_marcada():
    """O leitor devolve ALTA/MEDIA/BAIXA, e "BAIXA" quer dizer que ele mesmo
    desconfia. Marcar isso para aprovação em lote seria transformar a dúvida
    dele em decisão nossa."""
    proposta = fiscal_ia.proposta_da_leitura(
        sp(), lido(confianca="BAIXA", chave_acesso="", tipo_documento="GUIA",
                   emitente_documento=""))
    assert proposta["propoe"] is False


# ---------------------------------------------------------------------------
# O QUE A IA LÊ TEM DE BATER COM A SP
# ---------------------------------------------------------------------------
def test_anexo_de_OUTRO_fornecedor_e_apontado_e_nunca_proposto():
    """O erro mais caro possível, e ele acontece — o dono descreveu: "colocar
    uma nota de um registro para outro". Se o anexo foi parar no card errado, a
    IA lê uma nota perfeitamente válida do fornecedor errado."""
    proposta = fiscal_ia.proposta_da_leitura(
        sp(), lido(chave_acesso=chave("11222333000181"),
                   emitente_documento="11222333000181"))
    assert proposta["propoe"] is False
    assert proposta["confianca"] == 0
    assert "outro lançamento" in proposta["motivo"]


def test_a_nota_da_FILIAL_do_mesmo_credor_continua_valendo():
    """Matriz e filial têm CNPJ diferente e são o mesmo fornecedor. Recusar
    aqui perderia justamente o caso comum."""
    filial = "29066773000899"
    proposta = fiscal_ia.proposta_da_leitura(
        sp(), lido(chave_acesso=chave(filial), emitente_documento=filial))
    assert proposta["documentacao"] == "NF-e (Mercadoria)"
    assert proposta["propoe"] is True


# ---------------------------------------------------------------------------
# ELA NÃO RODA SOZINHA, E NÃO LÊ O QUE NÃO PRECISA
# ---------------------------------------------------------------------------
def test_SP_sem_anexo_nao_gasta_leitura():
    """Cada leitura é cobrada. Baixar o que não existe para descobrir que não
    existe seria pagar por nada."""
    with pytest.raises(fiscal_ia.SemAnexo):
        fiscal_ia.ler_anexo(sp(anexo_link=""))
    with pytest.raises(fiscal_ia.SemAnexo):
        fiscal_ia.ler_anexo(sp(anexo_link="—"))


def test_o_anexo_e_baixado_com_TETO_de_tamanho():
    """A instância tem 2 GB divididos com 17 módulos e já morreu de memória em
    julho de 2026. `resposta.content` traria o arquivo inteiro para a memória
    ANTES de qualquer conferência."""
    import inspect
    # Só o CÓDIGO, sem a docstring: ela cita `resposta.content` justamente para
    # explicar por que ele não é usado, e procurar no texto inteiro faria o
    # teste acusar a explicação em vez do defeito.
    codigo = inspect.getsource(fiscal_ia._baixar)
    corpo = codigo.split('"""')[-1]
    assert "stream=True" in corpo
    assert "MAXIMO_ANEXO" in corpo
    assert ".content" not in corpo, (
        "voltou a trazer o arquivo inteiro antes de conferir o tamanho")


def test_a_leitura_reusa_o_leitor_do_ERP_em_vez_de_ter_um_proprio():
    """O ERP já tem um leitor rodando em produção — XML por parser exato, PDF
    com texto, e foto por leitura visual. Um segundo leitor seria duas verdades
    sobre o mesmo PDF."""
    import inspect
    assert "erp.core.documentos.leitor" in inspect.getsource(fiscal_ia.ler_anexo)


def test_a_dica_manda_o_que_se_espera_do_documento():
    """Dizer de quem é a despesa e quanto ela custa é de graça e deixa o leitor
    conferir o que extraiu contra o que se esperava."""
    import inspect
    codigo = inspect.getsource(fiscal_ia.ler_anexo)
    assert "credor" in codigo and "valor" in codigo and "chave de acesso" in codigo


def test_toda_categoria_que_a_IA_pode_propor_existe_no_campo_do_Pipefy():
    """O Pipefy RECUSA O CARD INTEIRO quando o texto não é uma das 22 opções.
    Uma tradução errada aqui derrubaria a gravação do lote."""
    for categoria in fiscal_ia.CATEGORIA_DO_TIPO.values():
        assert categoria in fiscal.CATEGORIAS, categoria
