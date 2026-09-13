# -*- coding: utf-8 -*-
"""Análise de SPs — o nome do credor escrito de cinco jeitos.

POR QUE OS CASOS AQUI SÃO OS DE VERDADE. Esta regra reescreve o nome do credor
em SPs já pagas. Errar junta fornecedores diferentes ou troca o nome certo pelo
errado — e o estrago só aparece quando alguém procura por um fornecedor e não
acha. Por isso os testes usam os casos REAIS da planilha do dono, tirados da
aba Lançamentos em 11/09/2026, e não exemplos inventados.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import credores


def repetir(*pares):
    """(nome, quantas vezes) -> a lista como ela sai da planilha."""
    saida = []
    for nome, vezes in pares:
        saida.extend([nome] * vezes)
    return saida


# ---------------------------------------------------------------------------
# A CHAVE DE COMPARAÇÃO
# ---------------------------------------------------------------------------
def test_acento_cedilha_e_pontuacao_nao_distinguem_nome():
    assert credores.chave("ESPERANÇA NORDESTE LTDA") \
        == credores.chave("ESPERANCA NORDESTE LTDA")
    assert credores.chave("MASSA PRONTA PRODUTOS E SERVIÇOS LTDA") \
        == credores.chave("MASSA PRONTA PRODUTOS E SERVICOS LTDA")


def test_o_ESPACO_tambem_sai_da_chave():
    """Não é descuido: é o que faz "MBP ISOBLOCK" e "MBP ISO BLOCK" — o mesmo
    fornecedor digitado por duas pessoas — virarem a mesma coisa."""
    assert credores.chave("MBP ISOBLOCK") == credores.chave("MBP ISO BLOCK")


def test_nomes_de_verdade_diferentes_nao_viram_a_mesma_chave():
    assert credores.chave("CELPE CIA ENERGETICA") != credores.chave("NEOENERGIA")
    assert credores.chave("ESPERANÇA NORDESTE") != credores.chave("ESPERAÇA NORDESTE")


@pytest.mark.parametrize("documento,vale", [
    ("29.066.773/0001-52", True),      # CNPJ
    ("040.586.364-08", True),          # CPF
    ("29066773", False),               # raiz de CNPJ, pela metade
    ("", False), (None, False), ("—", False),
])
def test_documento_pela_metade_nao_agrupa_nada(documento, vale):
    """Campo incompleto juntaria fornecedores diferentes debaixo do mesmo
    "documento", e o nome de um entraria nas SPs do outro."""
    assert credores.documento_valido(documento) is vale


# ---------------------------------------------------------------------------
# O QUE O SISTEMA RESOLVE SOZINHO
# ---------------------------------------------------------------------------
def test_so_o_acento_diferente_e_resolvido_sozinho():
    """Caso real: MASSA PRONTA, com e sem cedilha. É o mais seguro de todos —
    é a mesma palavra."""
    r = credores.escolher(repetir(
        ("MASSA PRONTA PRODUTOS E SERVIÇOS LTDA", 2),
        ("MASSA PRONTA PRODUTOS E SERVICOS LTDA", 1)))
    assert r["automatico"] is True
    assert r["tipo"] == credores.IGUAL
    assert r["nome"] == "MASSA PRONTA PRODUTOS E SERVIÇOS LTDA", (
        "ficou a grafia menos usada")


def test_a_abreviacao_cede_para_o_nome_inteiro():
    """Caso real: "TRI" e "TRIBUNAL DE JUSTIÇA DO CEARÁ". Aqui o mais longo é
    seguro justamente porque o curto está inteiro dentro dele."""
    r = credores.escolher(repetir(("TRIBUNAL DE JUSTIÇA DO CEARÁ", 3), ("TRI", 1)))
    assert r["automatico"] is True
    assert r["tipo"] == credores.COMECO
    assert r["nome"] == "TRIBUNAL DE JUSTIÇA DO CEARÁ"


def test_a_grafia_MAIS_USADA_ganha_e_nao_a_mais_longa():
    """A equipe reconhece o nome que ela escreve. Trocá-lo pelo que alguém
    digitou uma vez seria piorar, não melhorar."""
    r = credores.escolher(repetir(("MBP ISOBLOCK", 6), ("MBP ISO BLOCK", 4)))
    assert r["nome"] == "MBP ISOBLOCK"


def test_um_nome_so_nao_e_divergencia():
    assert credores.divergencias([("29066773000152", "SERTAO CASA")] * 5) == []


# ---------------------------------------------------------------------------
# O QUE O SISTEMA **NÃO** RESOLVE — e é o coração desta regra
# ---------------------------------------------------------------------------
def test_o_ERRO_DE_DIGITACAO_MAIS_LONGO_nao_pode_ganhar():
    """O CASO QUE DERRUBOU A REGRA ÓBVIA, e é real:

        MAGNA LOCAÇÕES LTDA      3x
        MAGNA LOCAÇÃOES LTDA     1x   <- o MAIS LONGO é o digitado errado

    "Fica o nome mais completo" trocaria o certo pelo errado em todas as SPs
    daquele fornecedor. Se este teste começar a falhar porque alguém pôs a
    regra do "mais completo" de volta, é a regra que está errada."""
    r = credores.escolher(repetir(("MAGNA LOCAÇÕES LTDA", 3),
                                  ("MAGNA LOCAÇÃOES LTDA", 1)))
    assert r["automatico"] is False, "escreveu sozinho onde precisava perguntar"
    assert r["nome"] == "MAGNA LOCAÇÕES LTDA", "a sugestão saiu errada"


def test_empresa_que_MUDOU_DE_NOME_e_decisao_de_gente():
    """Caso real: CELPE virou NEOENERGIA. Nenhum dos dois é erro, e nenhuma
    regra automática sabe qual deve valer."""
    r = credores.escolher(["CELPE CIA ENERGETICA", "NEOENERGIA"])
    assert r["automatico"] is False


def test_nome_de_fantasia_contra_razao_social_e_decisao_de_gente():
    """Caso real: MBP ISOBLOCK e METALURGICA BARRA DO PIRAI S/A. O mesmo CNPJ,
    os dois nomes certos."""
    r = credores.escolher(repetir(("MBP ISOBLOCK", 10),
                                  ("METALURGICA BARRA DO PIRAI S/A", 4)))
    assert r["automatico"] is False


def test_palavra_A_MAIS_no_meio_nao_e_abreviacao():
    """Caso real: FRIGELAR COMERCIO LTDA e FRIGELAR COMERCIO E INDUSTRIA LTDA.
    Um não é o começo do outro — a diferença está no meio. Pode ser a razão
    social completa, pode ser outra empresa do grupo. Na dúvida, pergunta."""
    r = credores.escolher(repetir(("FRIGELAR COMERCIO E INDUSTRIA LTDA", 5),
                                  ("FRIGELAR COMERCIO LTDA", 3)))
    assert r["automatico"] is False


def test_erro_de_digitacao_no_meio_da_palavra_espera_decisao():
    """ESPERANÇA / ESPERAÇA. O acento a gente resolve; a letra que sumiu, não —
    e adivinhar ali seria adivinhar em cima do nome de um fornecedor."""
    r = credores.escolher(repetir(("ESPERANÇA NORDESTE LTDA", 2),
                                  ("ESPERANCA NORDESTE LTDA", 1),
                                  ("ESPERAÇA NORDESTE LTDA", 1)))
    assert r["automatico"] is False
    assert r["nome"] == "ESPERANÇA NORDESTE LTDA"


def test_toda_decisao_vem_com_o_motivo_em_portugues():
    """Quem lê a tela não é programador, e vai decidir olhando o motivo."""
    for tipo in (credores.IGUAL, credores.COMECO, credores.DECIDIR):
        assert credores.MOTIVOS[tipo]
        assert "_" not in credores.MOTIVOS[tipo]


# ---------------------------------------------------------------------------
# A LISTA QUE A TELA MOSTRA — montada com OS OITO CASOS REAIS
# ---------------------------------------------------------------------------
REAIS = {
    "92660406000623": [("FRIGELAR COMERCIO E INDUSTRIA LTDA", 5),
                       ("FRIGELAR COMERCIO LTDA", 3)],
    "28566933001727": [("MBP ISOBLOCK", 6), ("MBP ISO BLOCK", 4),
                       ("METALURGICA BARRA DO PIRAI S/A", 4)],
    "03666136000123": [("ESPERANÇA NORDESTE LTDA", 2),
                       ("ESPERAÇA NORDESTE LTDA", 1),
                       ("ESPERANCA NORDESTE LTDA", 1)],
    "24091522000104": [("MAFEMA MATERIAIS ELÉTRICOS", 1), ("MAFEMA LIMITADA", 1)],
    "10835932000108": [("CELPE CIA ENERGETICA", 1), ("NEOENERGIA", 1)],
    "04100718000100": [("MASSA PRONTA PRODUTOS E SERVIÇOS LTDA", 2),
                       ("MASSA PRONTA PRODUTOS E SERVICOS LTDA", 1)],
    "09444530000101": [("TRIBUNAL DE JUSTIÇA DO CEARÁ", 3), ("TRI", 1)],
    "01519852000829": [("MAGNA LOCAÇÕES LTDA", 3), ("MAGNA LOCAÇÃOES LTDA", 1)],
}


def linhas_reais():
    return [(documento, nome)
            for documento, pares in REAIS.items()
            for nome in repetir(*pares)]


def test_os_oito_casos_reais_se_separam_em_dois_e_seis():
    """A medição inteira, presa aqui: dos 8 CNPJs com mais de um nome na
    planilha do dono, DOIS o sistema resolve sozinho e SEIS ele devolve para
    ele decidir — uma vez cada."""
    achadas = credores.divergencias(linhas_reais())
    assert len(achadas) == 8
    automaticas = [d["nome"] for d in achadas if d["automatico"]]
    assert sorted(automaticas) == [
        "MASSA PRONTA PRODUTOS E SERVIÇOS LTDA", "TRIBUNAL DE JUSTIÇA DO CEARÁ"]
    assert len(achadas) - len(automaticas) == 6


def test_a_mesma_grafia_repetida_nao_vira_divergencia():
    """O caso normal — o mesmo nome em cem SPs — não pode encher a lista."""
    linhas = [("29066773000152", "SERTAO CASA E CONSTRUCAO")] * 100
    assert credores.divergencias(linhas) == []


def test_o_que_espera_decisao_aparece_ANTES_do_automatico():
    """A lista existe para o dono decidir. O que já está resolvido é recado,
    não tarefa."""
    achadas = credores.divergencias(linhas_reais())
    primeiro_automatico = next(n for n, d in enumerate(achadas) if d["automatico"])
    assert all(not d["automatico"] for d in achadas[:primeiro_automatico])


def test_o_que_afeta_mais_SPs_aparece_primeiro():
    """Entre duas decisões, a que muda quatorze SPs importa mais que a que
    muda duas."""
    esperando = [d for d in credores.divergencias(linhas_reais())
                 if not d["automatico"]]
    assert esperando == sorted(esperando, key=lambda d: -d["sps"])
    assert esperando[0]["sps"] == 14


def test_documento_vazio_nao_junta_credores_diferentes():
    """SP sem CPF/CNPJ preenchido existe. Agrupar por "vazio" juntaria
    fornecedores que não têm nada a ver um com o outro."""
    assert credores.divergencias([("", "UM"), ("", "OUTRO")]) == []


# ---------------------------------------------------------------------------
# O QUE VAI SER REESCRITO NA PLANILHA
# ---------------------------------------------------------------------------
def test_so_as_SPs_com_nome_diferente_sao_reescritas():
    """Cada reescrita é uma célula na fila e uma ida ao Google. Regravar o que
    já está certo seria pagar o preço sem mudar nada."""
    escolhido = {"09444530000101": "TRIBUNAL DE JUSTIÇA DO CEARÁ"}
    sps = [("1", "09444530000101", "TRI"),
           ("2", "09444530000101", "TRIBUNAL DE JUSTIÇA DO CEARÁ"),
           ("3", "09444530000101", "")]
    assert credores.a_corrigir(sps, escolhido) == [
        ("1", "TRIBUNAL DE JUSTIÇA DO CEARÁ"),
        ("3", "TRIBUNAL DE JUSTIÇA DO CEARÁ")]


def test_credor_de_documento_sem_escolha_nao_e_tocado():
    """Enquanto o dono não decidir, aquele fornecedor fica exatamente como
    está. Silêncio é melhor que chute."""
    sps = [("1", "10835932000108", "CELPE CIA ENERGETICA")]
    assert credores.a_corrigir(sps, {}) == []
