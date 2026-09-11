# -*- coding: utf-8 -*-
"""Análise de SPs — a conciliação fiscal.

POR QUE ESTE ARQUIVO É DETALHADO. Casar a nota errada com a SP errada vira
dedução indevida, e o erro NÃO aparece na tela: aparece na contabilidade,
meses depois. É o tipo de defeito que passa despercebido por muito tempo,
então a regra fica presa aqui, caso a caso.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import fiscal


# A chave de acesso tem posição fixa: 2 dígitos de UF, 4 de ano/mês, 14 do CNPJ
# do emitente, e o resto. Estas são montadas para os testes, mas com a mesma
# estrutura das de verdade.
def chave(cnpj_emitente: str, resto: str = "550010000123456789012345") -> str:
    montada = "26" + "2609" + fiscal.so_digitos(cnpj_emitente) + resto
    montada = (montada + "0" * 44)[:44]
    assert len(montada) == 44
    return montada


CREDOR = "29066773000152"          # o fornecedor: quem emite
BWS = "10656452007869"             # a BWS: quem recebe, em TODAS as notas


def nota(**campos):
    base = {"chave": chave(CREDOR), "emitente_doc": CREDOR,
            "emitente": "SERTAO CASA E CONSTRUCAO",
            "destinatario_doc": BWS, "destinatario": "BWS CONSTRUCOES LTDA",
            "numero": "1430", "valor": "269,00", "status": "Autorizada",
            "emissao": "18/06/2026"}
    base.update(campos)
    return base


def sp(**campos):
    base = {"id": "1409289353", "credor": "SERTAO CASA E CONSTRUCAO",
            "documento": "29.066.773/0001-52", "valor": "269,00",
            "nf": "1430", "vencimento": "10/07/2026",
            "tipo_despesa": "Ferramentas"}
    base.update(campos)
    return base


# ---------------------------------------------------------------------------
# O ERRO DE CONCEITO QUE ESTE MÓDULO CORRIGE
# ---------------------------------------------------------------------------
def test_o_credor_do_lancamento_e_quem_EMITIU_a_nota():
    """É O TESTE MAIS IMPORTANTE DO ARQUIVO.

    A análise que rodava na planilha até 11/09/2026 dava 30 pontos quando o
    CNPJ do credor batia com o DESTINATÁRIO, e 8 quando batia com o EMITENTE.
    Está de cabeça para baixo: o relatório é de notas emitidas CONTRA a BWS,
    então o destinatário é a BWS em todas as linhas — não distingue nada."""
    pontos, porques = fiscal.pontuar(sp(), nota())
    assert pontos >= fiscal.PONTOS_EMITENTE
    assert any("emitiu" in p for p in porques)


def test_bater_com_o_destinatario_nao_vale_ponto_nenhum():
    """O destinatário é a BWS em TODAS as notas. Pontuar por ele seria dar a
    mesma nota a todas as candidatas — ruído puro."""
    lancamento = sp(documento=BWS, credor="BWS CONSTRUCOES LTDA", nf="",
                    valor="0,01", vencimento="")
    pontos, _ = fiscal.pontuar(lancamento, nota())
    assert pontos == 0


def test_o_cnpj_de_quem_emitiu_e_lido_de_dentro_da_chave():
    """Os dígitos 7 a 20 da chave SÃO o CNPJ do emitente, por definição da
    Receita. Ler dali salva a conciliação quando a coluna do relatório vem
    suja ou vazia."""
    assert fiscal.emitente_da_chave(chave(CREDOR)) == CREDOR
    sem_coluna = nota(emitente_doc="", emitente="")
    pontos, porques = fiscal.pontuar(sp(), sem_coluna)
    assert any("emitiu" in p for p in porques), "não leu o CNPJ de dentro da chave"


def test_chave_com_tamanho_errado_nao_inventa_emitente():
    assert fiscal.emitente_da_chave("123") == ""
    assert fiscal.emitente_da_chave("") == ""


# ---------------------------------------------------------------------------
# MATRIZ E FILIAL
# ---------------------------------------------------------------------------
def test_a_filial_que_entregou_casa_com_a_matriz_cadastrada():
    """A nota sai da filial; o cadastro do credor quase sempre tem a matriz.
    Exigir os catorze dígitos perderia justamente os casos comuns."""
    assert fiscal.mesmo_documento("29.066.773/0001-52", "29066773000899")


def test_empresas_diferentes_nao_casam():
    assert not fiscal.mesmo_documento("29066773000152", "54063528000724")


def test_cpf_casa_inteiro_e_nao_pela_raiz():
    """CPF não tem matriz e filial. Comparar só o começo casaria pessoas
    diferentes."""
    assert fiscal.mesmo_documento("879.993.713-00", "87999371300")
    assert not fiscal.mesmo_documento("87999371300", "87999371399")


# ---------------------------------------------------------------------------
# O NÚMERO DA NOTA
# ---------------------------------------------------------------------------
def test_o_numero_ignora_zeros_a_esquerda():
    assert fiscal.mesmo_numero_de_nota("000123", "123")
    assert fiscal.mesmo_numero_de_nota("1.430", "1430")


def test_numero_vazio_nunca_casa():
    """Senão dois lançamentos sem número casariam entre si."""
    assert not fiscal.mesmo_numero_de_nota("", "")
    assert not fiscal.mesmo_numero_de_nota("", "123")


# ---------------------------------------------------------------------------
# A ESCOLHA ENTRE CANDIDATAS
# ---------------------------------------------------------------------------
def test_nao_propoe_quando_duas_notas_estao_empatadas():
    """Duas notas do mesmo fornecedor no mesmo dia, com valores próximos.
    Escolher uma no par ou ímpar é o erro que este módulo existe para não
    cometer: a tela diz que há dúvida."""
    a = nota(chave=chave(CREDOR, "550010000000000000000001"), numero="1430")
    b = nota(chave=chave(CREDOR, "550010000000000000000002"), numero="1431")
    escolha = fiscal.melhor_nota(sp(nf=""), [a, b])
    assert escolha["empate"] is True
    assert escolha["propoe"] is False
    assert any("confira" in p for p in escolha["porques"])


def test_propoe_quando_a_melhor_esta_sozinha_na_frente():
    certa = nota()
    outra = nota(chave=chave("11222333000181"), emitente_doc="11222333000181",
                 emitente="OUTRA EMPRESA", numero="99", valor="10.000,00")
    escolha = fiscal.melhor_nota(sp(), [certa, outra])
    assert escolha["propoe"] is True
    assert escolha["nota"]["chave"] == certa["chave"]
    assert escolha["pontos"] >= fiscal.CONFIANCA_PARA_PROPOR


def test_sem_candidata_nenhuma_responde_que_procurou():
    """"Procurei e não achei" é diferente de "não procurei", e a tela precisa
    poder dizer os dois."""
    escolha = fiscal.melhor_nota(sp(), [])
    assert escolha["nota"] is None and escolha["propoe"] is False


def test_a_nota_cancelada_aparece_com_o_aviso_em_vez_de_sumir():
    """Se ela É a nota daquele lançamento, quem analisa PRECISA ver — pagar
    com nota cancelada é problema fiscal. Esconder seria pior."""
    escolha = fiscal.melhor_nota(sp(), [nota(status="Cancelada")])
    assert escolha["nota"] is not None
    assert any("CANCELADA" in p for p in escolha["porques"])


# ---------------------------------------------------------------------------
# A DATA
# ---------------------------------------------------------------------------
def test_a_data_confirma_mas_nao_escolhe_sozinha():
    """Vale poucos pontos de propósito: em qualquer mês há dezenas de notas
    com data compatível."""
    lancamento = sp(documento="", credor="", nf="", valor="")
    pontos, _ = fiscal.pontuar(lancamento, nota())
    assert 0 < pontos < fiscal.CONFIANCA_PARA_PROPOR


def test_nota_emitida_muito_depois_do_vencimento_nao_ganha_ponto_de_data():
    lancamento = sp(documento="", credor="", nf="", valor="",
                    vencimento="01/01/2026")
    pontos, _ = fiscal.pontuar(lancamento, nota(emissao="31/12/2026"))
    assert pontos == 0


# ---------------------------------------------------------------------------
# A DEDUTIBILIDADE — a tabela é do dono
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("categoria", sorted(fiscal.NAO_DEDUTIVEIS))
def test_as_cinco_que_nao_dao_deducao(categoria):
    assert fiscal.dedutivel(categoria) is False


@pytest.mark.parametrize("categoria", [
    "NF-e (Mercadoria)", "NFS-e (Serviço)", "CT-e (Frete)", "Seguros",
    "Contrato", "Fundo Fixo", "Presente",
    # As três que faltavam na tabela da planilha e o dono respondeu em 11/09:
    "BeeVale", "Férias ou PL", "Rescisões (TRCT e Multa)",
])
def test_as_que_dao_deducao(categoria):
    assert fiscal.dedutivel(categoria) is True


def test_todas_as_opcoes_do_pipefy_tem_dedutibilidade_definida():
    """Uma opção sem resposta viraria uma SP que ninguém sabe classificar."""
    assert len(fiscal.CATEGORIAS) == 22
    for c in fiscal.CATEGORIAS:
        assert isinstance(fiscal.dedutivel(c), bool)
    assert fiscal.NAO_DEDUTIVEIS <= set(fiscal.CATEGORIAS)


# ---------------------------------------------------------------------------
# A CATEGORIA SUGERIDA PELO TIPO DE DESPESA
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tipo,esperado", [
    ("Veículos (Taxas, Impostos, Multas)", "Seguros"),
    ("Água e Energia", "Nota de Débito/Fatura"),
    ("Locação de Equipamentos", "Nota de Débito/Fatura"),
    ("Internet, Telefonia e Sistemas", "Nota de Débito/Fatura"),
    ("Aluguéis e Condomínios", "Contrato"),
    ("Multas e Processos Trabalhistas", "Rescisões (TRCT e Multa)"),
    ("Cartórios, Crea, Taxas", "Taxas Diversas"),
    ("Material Elétrico", "NF-e (Mercadoria)"),
    ("Parafusos, Ferragens e Acessórios", "NF-e (Mercadoria)"),
    ("Ferramentas", "NF-e (Mercadoria)"),
])
def test_a_sugestao_vem_do_historico_do_dono(tipo, esperado):
    """Não é invenção: na planilha dele, 18 dos 19 tipos de despesa tiveram
    SEMPRE a mesma categoria. A regra apenas repete o que ele já fazia."""
    assert fiscal.categoria_sugerida(tipo) == esperado


def test_tipo_de_despesa_desconhecido_nao_sugere_nada():
    """Chutar uma categoria é pior do que deixar em branco: o chute entra no
    card como se fosse decisão."""
    assert fiscal.categoria_sugerida("Alguma Coisa Nova") == ""
    assert fiscal.categoria_sugerida("") == ""


def test_seguros_e_contrato_nao_entram_na_conciliacao():
    """Uma apólice e um contrato de aluguel não têm nota eletrônica para
    casar. Procurar par para elas só produziria ruído."""
    assert "Seguros" in fiscal.NAO_CONCILIA
    assert "Contrato" in fiscal.NAO_CONCILIA
    assert "Fundo Fixo" in fiscal.NAO_CONCILIA
    # E as que PRECISAM ser conciliadas continuam de fora dessa lista.
    for c in ("NF-e (Mercadoria)", "NFS-e (Serviço)", "CT-e (Frete)",
              "Emissão Futura", "Ausente", "Reanalisar"):
        assert c not in fiscal.NAO_CONCILIA
