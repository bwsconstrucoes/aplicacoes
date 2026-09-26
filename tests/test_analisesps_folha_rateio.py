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
    assert "não sobra nada" in str(erro.value)


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
