# -*- coding: utf-8 -*-
"""
As regras de rateio da folha — a conta que divide o salário de uma pessoa entre
obras.

⚠️ AQUI NÃO PODE SOBRAR NEM FALTAR CENTAVO. A soma das partes tem de ser igual ao
valor, sempre: qualquer diferença vira uma obra com custo errado e um arquivo de
pagamento que não bate com a folha. E nada disso estoura — só aparece num total
que ninguém consegue explicar.
"""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_rateio as fr


# ---------------------------------------------------------------------------
# CPF — a identidade que liga a folha ao ponto e ao pagamento
# ---------------------------------------------------------------------------
def test_o_cpf_e_conferido_pelo_digito_verificador():
    """Um dígito trocado faz a regra nunca encontrar a pessoa — e ela volta a ser
    apropriada pelo ponto da matriz, sem ninguém perceber que a regra estava lá,
    escrita, e não pegou."""
    assert fr.cpf_valido("997.133.493-34") is True
    assert fr.cpf_valido("99713349334") is True
    assert fr.cpf_valido("997.133.493-35") is False     # dígito trocado
    assert fr.cpf_valido("111.111.111-11") is False     # todos iguais
    assert fr.cpf_valido("123") is False
    assert fr.cpf_valido("") is False


def test_o_cpf_sai_formatado_para_a_tela_e_guardado_em_digitos():
    assert fr.so_digitos("997.133.493-34") == "99713349334"
    assert fr.cpf_bonito("99713349334") == "997.133.493-34"


# ---------------------------------------------------------------------------
# A regra: o que fecha e o que não fecha
# ---------------------------------------------------------------------------
def test_percentuais_que_somam_100_passam():
    obras = fr.conferir_obras([
        {"obra": "crepeareias", "percentual": "60"},
        {"obra": "CREPEOLINDA", "percentual": "40"}])
    assert [o["obra"] for o in obras] == ["CREPEAREIAS", "CREPEOLINDA"]
    assert obras[0]["percentual"] == D("60.0000")


@pytest.mark.parametrize("soma,percentuais", [
    ("99.00", ["60", "39"]),
    ("101.00", ["60", "41"]),
])
def test_percentual_que_NAO_soma_100_e_recusado_e_nao_arredondado(soma, percentuais):
    """⚠️ Rateio que não fecha 100% esconde ou inventa dinheiro. Arredondar por
    conta própria faria a diferença aparecer depois, num total de obra que
    ninguém consegue explicar."""
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.conferir_obras([{"obra": f"OBRA{i}", "percentual": p}
                           for i, p in enumerate(percentuais)])
    assert soma in str(erro.value)
    assert "100%" in str(erro.value)


def test_uma_obra_com_50_e_O_RESTO_dividido_entre_as_outras():
    """É o caso que o dono descreveu com essas palavras: *"uma obra é 50% e o
    restante dividido entre outras"*. Ninguém precisa fazer a conta de cabeça."""
    efetivos = fr.percentuais_efetivos([
        {"obra": "CREPEAREIAS", "percentual": "50"},
        {"obra": "CREPEOLINDA", "resto": True},
        {"obra": "CREPETRIUNFO", "resto": True}])

    por_obra = {o["obra"]: o["percentual"] for o in efetivos}
    assert por_obra["CREPEAREIAS"] == D("50.0000")
    assert por_obra["CREPEOLINDA"] == D("25.0000")
    assert por_obra["CREPETRIUNFO"] == D("25.0000")
    assert sum(por_obra.values()) == D("100")


def test_o_resto_que_nao_divide_exato_ainda_fecha_100():
    """50% numa obra e o resto entre TRÊS: 16,6667 três vezes dá 50,0001. A sobra
    do percentual vai para a maior fatia, pelo mesmo motivo da sobra do centavo."""
    efetivos = fr.percentuais_efetivos([
        {"obra": "A", "percentual": "50"},
        {"obra": "B", "resto": True},
        {"obra": "C", "resto": True},
        {"obra": "D", "resto": True}])
    assert sum(o["percentual"] for o in efetivos) == D("100")


def test_tudo_como_RESTO_divide_igual():
    efetivos = fr.percentuais_efetivos([
        {"obra": "A", "resto": True}, {"obra": "B", "resto": True},
        {"obra": "C", "resto": True}, {"obra": "D", "resto": True}])
    assert all(o["percentual"] == D("25.0000") for o in efetivos)


def test_resto_sem_nada_sobrando_e_recusado_com_frase():
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.conferir_obras([{"obra": "A", "percentual": "100"},
                           {"obra": "B", "resto": True}])
    assert "não há saldo" in str(erro.value)


def test_obra_repetida_percentual_zerado_e_regra_vazia_sao_recusados():
    for obras, pedaco in (
            ([{"obra": "A", "percentual": "50"}, {"obra": " a ", "percentual": "50"}],
             "duas vezes"),
            ([{"obra": "A", "percentual": "0"}, {"obra": "B", "percentual": "100"}],
             "maior que zero"),
            ([{"obra": "A", "percentual": "abc"}], "não é um número"),
            ([{"obra": "A"}], 'marque-a como "o resto"'),
            ([], "ao menos uma obra")):
        with pytest.raises(fr.ErroDoRateio) as erro:
            fr.conferir_obras(obras)
        assert pedaco in str(erro.value), f"{obras} → {erro.value}"


# ---------------------------------------------------------------------------
# A DISTRIBUIÇÃO — o centavo
# ---------------------------------------------------------------------------
def test_a_soma_das_partes_e_IGUAL_ao_valor():
    partes = fr.distribuir("1000.00", [
        {"obra": "A", "percentual": "60"}, {"obra": "B", "percentual": "40"}])
    assert [p["valor"] for p in partes] == [D("600.00"), D("400.00")]
    assert sum(p["valor"] for p in partes) == D("1000.00")


def test_o_centavo_que_sobra_vai_para_a_MAIOR_fatia():
    """⚠️ A mesma regra que o dono escolheu para o rateio por dias em 26/09/2026
    ("a obra com mais dias"): a maior fatia é a que menos sente.

    1.000,00 dividido em 50% / 16,6667 x3 dá 500,00 + 166,67 x3 = 1.000,01 se cada
    parte for arredondada solta."""
    partes = fr.distribuir("1000.00", [
        {"obra": "GRANDE", "percentual": "50"},
        {"obra": "B", "resto": True}, {"obra": "C", "resto": True},
        {"obra": "D", "resto": True}])

    assert sum(p["valor"] for p in partes) == D("1000.00")
    por_obra = {p["obra"]: p["valor"] for p in partes}
    # A sobra (ou a falta) foi absorvida pela maior.
    assert por_obra["B"] == por_obra["C"] == por_obra["D"]


@pytest.mark.parametrize("valor", ["958.90", "1126.60", "0.01", "3526.00",
                                   "1198.84", "12345.67"])
def test_nenhum_valor_perde_nem_inventa_centavo(valor):
    """Os valores são de gente de verdade, tirados da folha de 08/2026."""
    for obras in (
            [{"obra": "A", "percentual": "33.33"},
             {"obra": "B", "percentual": "33.33"},
             {"obra": "C", "percentual": "33.34"}],
            [{"obra": "A", "resto": True}, {"obra": "B", "resto": True},
             {"obra": "C", "resto": True}],
            [{"obra": "A", "percentual": "70"}, {"obra": "B", "resto": True},
             {"obra": "C", "resto": True}],
            [{"obra": "SOZINHA", "percentual": "100"}]):
        partes = fr.distribuir(valor, obras)
        assert sum(p["valor"] for p in partes) == D(valor), (valor, obras)


def test_valor_zero_nao_estoura():
    partes = fr.distribuir("0", [{"obra": "A", "percentual": "100"}])
    assert partes[0]["valor"] == D("0.00")


# ---------------------------------------------------------------------------
# Sem a migração, nada estoura
# ---------------------------------------------------------------------------
def test_sem_a_migracao_a_lista_vem_VAZIA_e_gravar_recusa_com_frase(monkeypatch):
    """O código sobe antes do botão "Aplicar atualizações do banco" ser apertado.
    Nessa janela a tela tem de avisar, não estourar."""
    monkeypatch.setattr(fr, "_pronto", lambda: False)
    assert fr.listar() == []
    assert fr.regra_da_pessoa("99713349334") is None
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.gravar({"nome": "X", "obras": [{"obra": "A", "percentual": "100"}],
                   "pessoas": [{"cpf": "99713349334"}]})
    assert "Aplicar atualizações do banco" in str(erro.value)


# ---------------------------------------------------------------------------
# REPETIR A OBRA É O PESO DELA — 27/09/2026
#
# ⚠️ Pedido do dono, e ele está certo sobre o erro de desenho que eu tinha
# feito: *"imagina preencher 10 funcionários 10 vezes cada campozinho. 10 vezes
# 10 dá 100. Imagina preencher 100 campos. É absurdo. (…) Imagina que eu coloco
# código da obra 1, código da obra 2 e código da obra 2 de novo. O que é que o
# sistema teria que entender? 66% para uma e os 33% para outra."*
#
# Eu havia resolvido o CASO de uma regra e ignorado o VOLUME de dez.
# ---------------------------------------------------------------------------
def test_a_obra_repetida_DUAS_vezes_vale_o_dobro():
    obras = fr.obras_por_repeticao(["A", "B", "B"])
    por_nome = {o["obra"]: o["percentual"] for o in obras}
    assert por_nome["B"] == D("66.6667")
    assert por_nome["A"] == D("33.3333")


def test_a_soma_fecha_EXATAMENTE_cem_em_todos_os_casos():
    """⚠️ Rateio que soma 99,9999 esconde dinheiro, e 100,0001 inventa. A
    diferença apareceria depois num total de obra que ninguém explica."""
    for entrada in (["A"], ["A", "B"], ["A", "B", "C"], ["A", "B", "B"],
                    ["A", "A", "A", "B"], ["A", "B", "C", "D", "E", "F", "G"],
                    ["A"] * 7 + ["B"] * 3, ["A", "B", "C"] * 11):
        obras = fr.obras_por_repeticao(entrada)
        soma = sum((o["percentual"] for o in obras), D("0"))
        assert soma == D("100.0000"), f"{entrada} somou {soma}"


def test_a_sobra_do_arredondamento_vai_para_a_obra_de_MAIOR_peso():
    """Mesma regra da sobra de centavo, decidida pelo dono em 26/09/2026 —
    "pode botar na obra com mais dias".

    ⚠️ ESTE TESTE PEGOU UM DEFEITO DE VERDADE: na primeira versão a sobra ia
    para a obra de MENOR peso, porque era a última da lista que fechava a conta.
    O resultado era 66,6666 / 33,3334 em vez de 66,6667 / 33,3333."""
    obras = {o["obra"]: o["percentual"]
             for o in fr.obras_por_repeticao(["A", "B", "B"])}
    assert obras["B"] > D("66.6666")
    assert obras["A"] == D("33.3333")


def test_a_ordem_em_que_foi_digitado_nao_muda_o_resultado():
    """Duas colagens iguais têm de dar o mesmo rateio. Rateio que muda sozinho
    entre duas rodadas é impossível de conferir."""
    um = {o["obra"]: o["percentual"] for o in fr.obras_por_repeticao(["A", "B", "B"])}
    outro = {o["obra"]: o["percentual"] for o in fr.obras_por_repeticao(["B", "A", "B"])}
    assert um == outro


def test_uma_obra_sozinha_leva_cem_por_cento():
    assert fr.obras_por_repeticao(["A"])[0]["percentual"] == D("100.0000")


def test_a_obra_entra_em_MAIUSCULA_e_sem_espaco_sobrando():
    obras = fr.obras_por_repeticao(["  crepeareias ", "CrepeAreias"])
    assert len(obras) == 1
    assert obras[0]["obra"] == "CREPEAREIAS"
    assert obras[0]["percentual"] == D("100.0000")


def test_lista_vazia_de_obras_e_recusada():
    with pytest.raises(fr.ErroDoRateio):
        fr.obras_por_repeticao([])
    with pytest.raises(fr.ErroDoRateio):
        fr.obras_por_repeticao(["", "  "])


def test_o_que_sai_daqui_passa_pela_CONFERENCIA_de_sempre():
    """O modelo no banco não muda: a repetição é só a forma de ENTRAR. Uma
    segunda forma de guardar rateio divergiria da primeira."""
    obras = fr.obras_por_repeticao(["A", "B", "B"])
    assert fr.conferir_obras(obras) == obras


# ---------------------------------------------------------------------------
# A TABELA COLADA
# ---------------------------------------------------------------------------
def test_a_tabela_colada_vira_regra_pronta():
    regras = fr.ler_tabela_de_rateio(
        "997.133.493-34 ; GERLANIO GOMES LIMA ; CREPEAREIAS, CREPEOLINDA, CREPEOLINDA")
    assert len(regras) == 1
    assert regras[0]["pessoas"] == [{"cpf": "99713349334",
                                     "nome": "GERLANIO GOMES LIMA"}]
    por_nome = {o["obra"]: o["percentual"] for o in regras[0]["obras"]}
    assert por_nome["CREPEOLINDA"] == D("66.6667")


def test_quem_tem_a_MESMA_distribuicao_vira_UMA_regra():
    """Dez pessoas com o mesmo rateio não são dez regras: são uma com dez
    pessoas. É o que faz a tela continuar legível depois de colar trinta
    linhas."""
    regras = fr.ler_tabela_de_rateio(
        "997.133.493-34 ; UM ; A, B\n"
        "035.134.413-63 ; DOIS ; A, B\n"
        "111.444.777-35 ; TRES ; A, A, B\n")
    assert len(regras) == 2
    primeira = [r for r in regras if len(r["pessoas"]) == 2][0]
    assert {p["nome"] for p in primeira["pessoas"]} == {"UM", "DOIS"}


def test_a_ordem_das_obras_na_linha_nao_separa_em_duas_regras():
    """Quem escreveu "A, B, B" e quem escreveu "B, A, B" tem o MESMO rateio."""
    regras = fr.ler_tabela_de_rateio(
        "997.133.493-34 ; UM ; A, B, B\n"
        "035.134.413-63 ; DOIS ; B, B, A\n")
    assert len(regras) == 1
    assert len(regras[0]["pessoas"]) == 2


def test_o_nome_da_pessoa_pode_faltar():
    """Quem manda é o CPF; o nome de verdade vem do cadastro. Duas colunas em
    vez de três é o que acontece ao copiar de uma planilha mais enxuta."""
    regras = fr.ler_tabela_de_rateio("997.133.493-34 ; A, B")
    assert regras[0]["pessoas"] == [{"cpf": "99713349334", "nome": ""}]
    assert len(regras[0]["obras"]) == 2


def test_a_tabela_aceita_TABULACAO_que_e_o_que_sai_da_planilha():
    regras = fr.ler_tabela_de_rateio("997.133.493-34\tGERLANIO\tA, B, B")
    assert regras[0]["pessoas"][0]["nome"] == "GERLANIO"
    assert len(regras[0]["obras"]) == 2


def test_linha_em_branco_e_comentario_sao_ignorados():
    regras = fr.ler_tabela_de_rateio(
        "# as obras de setembro\n\n997.133.493-34 ; A\n\n")
    assert len(regras) == 1


def test_o_erro_diz_o_NUMERO_DA_LINHA():
    """⚠️ "Não entendi a tabela" mandaria a pessoa procurar agulha em trinta
    linhas."""
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.ler_tabela_de_rateio("997.133.493-34 ; A\n123 ; NOME ; B\n")
    assert "Linha 2" in str(erro.value)


def test_cpf_com_digito_errado_e_recusado_dizendo_o_que_conferir():
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.ler_tabela_de_rateio("111.111.111-11 ; NOME ; A")
    assert "dígito verificador" in str(erro.value)


def test_linha_sem_obra_nenhuma_e_recusada():
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.ler_tabela_de_rateio("997.133.493-34 ;  ; ")
    assert "Linha 1" in str(erro.value)


def test_a_MESMA_pessoa_em_duas_linhas_e_recusada_dizendo_onde():
    """⚠️ Só uma regra pode valer para alguém — é a trava que já existe na
    gravação. Duas linhas do mesmo CPF fariam a segunda derrubar a primeira sem
    avisar, e o rateio da pessoa mudaria em silêncio."""
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.ler_tabela_de_rateio(
            "997.133.493-34 ; UM ; A\n"
            "035.134.413-63 ; DOIS ; B\n"
            "997.133.493-34 ; UM DE NOVO ; C\n")
    frase = str(erro.value)
    assert "997.133.493-34" in frase
    assert "1" in frase and "3" in frase, "tem de dizer em quais linhas"
    assert "a repetição da obra define o peso" in frase, "tem de dizer o que fazer"


def test_tabela_vazia_e_recusada_com_o_formato_explicado():
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.ler_tabela_de_rateio("   \n\n")
    assert "CPF ; nome ; obra" in str(erro.value)


def test_colar_trinta_linhas_com_dez_obras_nao_e_absurdo():
    """O caso que ele descreveu: dez pessoas, dez obras. Antes eram 100 campos;
    agora são dez linhas."""
    linhas = []
    cpfs = ["99713349334", "03513441363", "11144477735", "52998224725",
            "11122233396"]
    for i, cpf in enumerate(cpfs):
        obras = ", ".join(f"OBRA{n}" for n in range(1, 11))
        linhas.append(f"{cpf} ; PESSOA {i} ; {obras}")
    regras = fr.ler_tabela_de_rateio("\n".join(linhas))
    assert len(regras) == 1, "todos com a mesma distribuição = uma regra"
    assert len(regras[0]["pessoas"]) == 5
    assert len(regras[0]["obras"]) == 10
    soma = sum((o["percentual"] for o in regras[0]["obras"]), D("0"))
    assert soma == D("100.0000")
