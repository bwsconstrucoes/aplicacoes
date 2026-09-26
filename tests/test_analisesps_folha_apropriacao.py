# -*- coding: utf-8 -*-
"""
A APROPRIAÇÃO da folha: para qual obra vai cada real.

⚠️ É A CONTA MAIS PERIGOSA DO MÓDULO. Ela decide de qual conta corrente sai o
dinheiro de ~500 pessoas por quinzena, e qual obra carrega o custo. Erro aqui não
estoura: some centavo, ou põe custo na obra errada, e a divergência só aparece
meses depois num total que ninguém consegue explicar.

Todas as regras aqui foram decididas pelo dono em 26/09/2026 e estão citadas nos
testes com as palavras dele.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_apropriacao as ap


# ---------------------------------------------------------------------------
# O PERÍODO — quinzena é 1 a 15; fim de mês é 16 até o ÚLTIMO dia
# ---------------------------------------------------------------------------
def test_a_quinzena_le_do_dia_1_ao_15():
    assert ap.periodo_do_pagamento(2026, 8, "quinzena") == (
        dt.date(2026, 8, 1), dt.date(2026, 8, 15))


@pytest.mark.parametrize("ano,mes,ultimo", [
    (2026, 8, 31), (2026, 4, 30), (2026, 2, 28), (2024, 2, 29),
])
def test_fim_de_mes_vai_ate_o_ULTIMO_dia_e_nao_ate_31(ano, mes, ultimo):
    """⚠️ Escrever 31 faria a leitura do ponto procurar dias que não existem em
    fevereiro — e, pior, daria a impressão de que o período está certo."""
    assert ap.periodo_do_pagamento(ano, mes, "fim_de_mes") == (
        dt.date(ano, mes, 16), dt.date(ano, mes, ultimo))


# ---------------------------------------------------------------------------
# A OBRA DO DIA — quatro marcações, vale a que mais aparece
# ---------------------------------------------------------------------------
def test_tres_marcacoes_numa_obra_e_uma_noutra_vale_as_tres():
    """A regra que o dono descreveu: *"se ele tem três pontos numa obra e um ponto
    noutra, então vai valer os três pontos"*."""
    d = ap.obra_do_dia(["CREPETERRA", "CREPETERRA", "CREPETERRA", "CREPEOLINDA"],
                       "PRESENÇA")
    assert d["obra"] == "CREPETERRA"
    assert d["empate"] is False


def test_no_empate_2x2_vale_a_obra_em_que_o_dia_COMECOU():
    """*"Se for equilibrado, dois pontos numa obra, dois pontos na outra, utiliza
    o ponto das duas primeiras marcações."*

    ⚠️ E A ORDEM IMPORTA: ordenar as marcações antes de contar faria o desempate
    virar sorteio alfabético, e ninguém notaria."""
    d = ap.obra_do_dia(["ZULU", "ZULU", "ALFA", "ALFA"], "PRESENÇA")
    assert d["obra"] == "ZULU"
    assert d["empate"] is True, "o empate tem de ficar anotado para a crítica"

    # E o contrário, para provar que não é o alfabeto decidindo.
    d = ap.obra_do_dia(["ALFA", "ALFA", "ZULU", "ZULU"], "PRESENÇA")
    assert d["obra"] == "ALFA"


def test_uma_marcacao_so_ainda_define_a_obra():
    d = ap.obra_do_dia(["CREPETERRA", "", "", ""], "PRESENÇA")
    assert d["obra"] == "CREPETERRA"
    assert d["marcou"] == 1


def test_feriado_e_compensacao_NAO_sao_dia_de_obra():
    """É uma escolha, não um fato: o dia vale pela obra em que a pessoa bateu
    ponto, e em feriado ela não bateu em obra nenhuma."""
    for situacao in ("FERIADO", "COMPENSAÇÃO"):
        d = ap.obra_do_dia(["CREPETERRA"] * 4, situacao)
        assert d["obra"] is None, situacao


def test_dia_SEM_falta_e_SEM_marcacao_e_separado_de_falta_declarada():
    """⚠️ O furo que o dono apontou: *"vai ter dias que a pessoa não tem falta mas
    também não bateu o ponto"* — ou ela esqueceu, ou quem devia lançar a falta não
    lançou. Não é presença nem falta."""
    silencio = ap.obra_do_dia(["", "", "", ""], "")
    assert silencio["obra"] is None
    assert silencio["motivo"] == "sem marcação e sem falta"

    falta = ap.obra_do_dia([], "", "FALTA JUSTIFICADA")
    assert falta["motivo"] == "falta"


# ---------------------------------------------------------------------------
# O RATEIO PELOS DIAS — e o centavo
# ---------------------------------------------------------------------------
def dia(numero, obra, presenca="PRESENÇA"):
    return {"data": dt.date(2026, 8, numero),
            "marcacoes": [obra, obra, obra, obra], "presenca": presenca}


def uteis(*dias):
    return ap._dias_uteis_do_ponto(list(dias), dt.date(2026, 8, 1),
                                   dt.date(2026, 8, 15))


def test_dez_dias_numa_obra_so_vao_inteiros_para_ela():
    feito = ap.apropriar_pelo_ponto("958.90", uteis(
        *[dia(n, "CREPETERRA") for n in range(1, 11)]))
    assert len(feito["por_obra"]) == 1
    assert feito["por_obra"][0] == {"obra": "CREPETERRA", "dias": 10,
                                    "valor": D("958.90"), "origem": "ponto"}
    assert sum(d["valor"] for d in feito["por_dia"]) == D("958.90")


def test_dias_em_obras_diferentes_dividem_na_PROPORCAO_dos_dias():
    feito = ap.apropriar_pelo_ponto("1000.00", uteis(
        dia(1, "A"), dia(2, "A"), dia(3, "A"), dia(4, "B")))
    por_obra = {o["obra"]: o["valor"] for o in feito["por_obra"]}
    assert por_obra == {"A": D("750.00"), "B": D("250.00")}


def test_a_sobra_do_centavo_vai_para_a_obra_com_MAIS_DIAS():
    """Decisão do dono: *"pode botar na obra que com mais dias"*.

    958,90 em 3 dias (2 numa obra, 1 na outra): 639,266... e 319,633...
    Arredondando solto, sobra ou falta centavo."""
    feito = ap.apropriar_pelo_ponto("958.90", uteis(
        dia(1, "GRANDE"), dia(2, "GRANDE"), dia(3, "PEQUENA")))
    por_obra = {o["obra"]: o["valor"] for o in feito["por_obra"]}

    assert sum(por_obra.values()) == D("958.90")
    assert por_obra["GRANDE"] > por_obra["PEQUENA"] * 2 - D("0.02")
    # A sobra ficou na maior, não na menor.
    assert por_obra["PEQUENA"] == D("319.63")
    assert por_obra["GRANDE"] == D("639.27")


@pytest.mark.parametrize("valor", ["958.90", "1126.60", "3526.00", "0.01",
                                   "1198.84", "12345.67", "570.94"])
@pytest.mark.parametrize("reparticao", [
    ["A"], ["A", "B"], ["A", "A", "B"], ["A", "B", "C"],
    ["A", "A", "A", "B", "C", "C", "D"],
])
def test_nenhum_nivel_perde_nem_inventa_centavo(valor, reparticao):
    """⚠️ CADA NÍVEL FECHA: os dias de uma obra somam o total da obra, e as obras
    somam o valor. Um nível que não fecha faz o relatório analítico contradizer o
    resumido — e aí nenhum dos dois serve de prova."""
    dias = uteis(*[dia(i + 1, o) for i, o in enumerate(reparticao)])
    feito = ap.apropriar_pelo_ponto(valor, dias)

    assert sum(o["valor"] for o in feito["por_obra"]) == D(valor)
    assert sum(d["valor"] for d in feito["por_dia"]) == D(valor)
    for obra in feito["por_obra"]:
        dos_dias = sum(d["valor"] for d in feito["por_dia"]
                       if d["obra"] == obra["obra"])
        assert dos_dias == obra["valor"], (obra["obra"], valor)


def test_o_periodo_CORTA_os_dias_de_fora():
    """Se estou pagando a quinzena, só leio os dias 1 a 15 — palavras dele."""
    dias = ap._dias_uteis_do_ponto(
        [dia(10, "A"), dia(20, "B")], dt.date(2026, 8, 1), dt.date(2026, 8, 15))
    assert [d["data"].day for d in dias] == [10]


# ---------------------------------------------------------------------------
# QUEM MANDA: mão > regra > ponto
# ---------------------------------------------------------------------------
class Linha:
    def __init__(self, valor="1000.00", id_fortes="000013", nome="FULANO",
                 filial_nome="CONSTRUTORA"):
        self.valor = D(valor)
        self.id_fortes = id_fortes
        self.nome = nome
        self.filial_nome = filial_nome


REGRA = {"nome": "Supervisores PE",
         "obras": [{"obra": "A", "percentual": "50"},
                   {"obra": "B", "resto": True}]}


def test_a_REGRA_vence_o_ponto():
    """⚠️ DE PROPÓSITO, e é o ponto mais delicado do arquivo: quem bate ponto na
    MATRIZ tem dias no ponto, e são justamente os dias que não servem — apontam
    para a matriz. Se o ponto viesse primeiro, a regra dessa pessoa nunca pegaria,
    e o erro seria invisível: a apropriação existiria, só estaria na obra errada."""
    feito = ap.apropriar_pessoa(Linha(), uteis(dia(1, "CONS"), dia(2, "CONS")),
                                regra=REGRA)
    assert feito["origem"] == "regra"
    assert feito["regra"] == "Supervisores PE"
    assert {o["obra"] for o in feito["por_obra"]} == {"A", "B"}
    assert sum(o["valor"] for o in feito["por_obra"]) == D("1000.00")


def test_o_ajuste_de_MAO_vence_a_regra_e_o_ponto():
    feito = ap.apropriar_pessoa(
        Linha(), uteis(dia(1, "CONS")), regra=REGRA,
        ajuste={"obra_unica": "escvarzea"})
    assert feito["origem"] == "mao"
    assert feito["por_obra"] == [{"obra": "ESCVARZEA", "dias": 1,
                                 "valor": D("1000.00"), "origem": "mao"}]


def test_desmarcar_a_pessoa_a_tira_da_apropriacao_com_o_motivo():
    feito = ap.apropriar_pessoa(Linha(), ajuste={"fora": True,
                                                 "motivo": "já foi desligada"})
    assert feito["fora"] is True
    assert feito["por_obra"] == []
    assert feito["motivo_fora"] == "já foi desligada"


def test_ajuste_dia_a_dia_com_valor_por_obra_e_conferido():
    """Ajuste que não soma o valor da pessoa esconde ou inventa dinheiro."""
    feito = ap.apropriar_pessoa(Linha(), ajuste={"por_obra": [
        {"obra": "A", "valor": "600.00", "dias": 3},
        {"obra": "B", "valor": "400.00", "dias": 2}]})
    assert feito["criticas"] == []
    assert sum(o["valor"] for o in feito["por_obra"]) == D("1000.00")

    torto = ap.apropriar_pessoa(Linha(), ajuste={"por_obra": [
        {"obra": "A", "valor": "600.00"}]})
    assert any("soma" in c for c in torto["criticas"])


def test_sem_dia_e_sem_regra_a_pessoa_fica_SEM_apropriacao_e_com_critica():
    """⚠️ NÃO INVENTA OBRA. Chutar a obra da contabilidade pareceria funcionar e
    poria o custo na obra errada em silêncio. Esta pessoa vai para a faixa
    "precisa da sua mão"."""
    feito = ap.apropriar_pessoa(Linha(), [])
    assert feito["por_obra"] == []
    assert feito["origem"] == ""
    assert any("sem regra de rateio" in c for c in feito["criticas"])


def test_o_empate_do_ponto_vira_critica_na_pessoa():
    dias = ap._dias_uteis_do_ponto([{
        "data": dt.date(2026, 8, 3),
        "marcacoes": ["ZULU", "ZULU", "ALFA", "ALFA"],
        "presenca": "PRESENÇA"}], dt.date(2026, 8, 1), dt.date(2026, 8, 15))
    feito = ap.apropriar_pessoa(Linha(), dias)
    assert any("empatada" in c for c in feito["criticas"])
    assert feito["por_obra"][0]["obra"] == "ZULU"


# ---------------------------------------------------------------------------
# A FOLHA INTEIRA
# ---------------------------------------------------------------------------
def test_a_folha_inteira_fecha_e_soma_por_obra():
    cadastro = {"000013": {"cpf": "99713349334", "nome": "GERLANIO G. LIMA"},
                "000387": {"cpf": "03513441363", "nome": "LUELIA M. G. TOMAS"}}
    linhas = [Linha("1198.84", "000013", "GERLANIO GOMES LIMA"),
              Linha("1198.84", "000387", "LUELIA MADIDA GOMES TOMAS")]
    dias = {
        "99713349334": [dia(1, "A"), dia(2, "A"), dia(3, "B")],
        "03513441363": [dia(1, "B"), dia(2, "B")],
    }
    feito = ap.apropriar(linhas, dias, cadastro_por_id=cadastro,
                         periodo=(dt.date(2026, 8, 1), dt.date(2026, 8, 15)))

    assert feito["fecha"] is True
    assert feito["total_a_pagar"] == D("2397.68")
    assert feito["total_apropriado"] == D("2397.68")
    por_obra = {o["obra"]: o["valor"] for o in feito["por_obra"]}
    assert sum(por_obra.values()) == D("2397.68")
    assert set(por_obra) == {"A", "B"}
    # O nome do CADASTRO é o que vai para o arquivo de pagamento.
    assert feito["pessoas"][0]["nome_cadastro"] == "GERLANIO G. LIMA"


def test_quem_NAO_esta_no_cadastro_CONTINUA_na_lista_como_pendente():
    """⚠️ CORREÇÃO DO DONO EM 26/09/2026, sobre uma decisão minha que estava
    errada:

        *"Pessoas sem ID Fortes no cadastro não entram. Na verdade, ela vai entrar
        após tratamento. Vamos tratar para poder entrar. Então não pode ficar
        oculto, escondido."*

    Eu havia tirado essas pessoas da lista e posto numa lista à parte. Lista à
    parte é lista que alguém esquece de abrir — e aí a pessoa DESAPARECE da folha:
    trabalhou e não recebeu, sem nada na tela gritando."""
    feito = ap.apropriar([Linha("500.00", "009999", "NINGUEM CONHECE")],
                         cadastro_por_id={})

    assert len(feito["pessoas"]) == 1, "a pessoa sumiu da lista"
    pendente = feito["pessoas"][0]
    assert pendente["pendente_cadastro"] is True
    assert pendente["id_fortes"] == "009999"
    assert pendente["nome"] == "NINGUEM CONHECE"
    assert pendente["valor"] == D("500.00")
    assert pendente["cpf"] == ""
    assert pendente["por_obra"] == []
    assert any("não está no cadastro" in c for c in pendente["criticas"])
    # E o atalho para a faixa "precisa da sua mão" aponta para o MESMO objeto.
    assert feito["sem_cadastro"][0] is pendente


def test_a_folha_NAO_FECHA_enquanto_houver_pendente_de_cadastro():
    """⚠️ Tirar o pendente da soma faria a tela dizer "fecha" com gente de fora —
    o pior resultado possível, porque é o que convence alguém a apertar o botão."""
    cadastro = {"000013": {"cpf": "99713349334", "nome": "X"}}
    feito = ap.apropriar(
        [Linha("100.00", "000013"), Linha("500.00", "009999", "SEM ID")],
        {"99713349334": [dia(1, "A")]}, cadastro_por_id=cadastro,
        periodo=(dt.date(2026, 8, 1), dt.date(2026, 8, 15)))

    assert feito["total_da_folha"] == D("600.00")
    assert feito["total_a_pagar"] == D("600.00")
    assert feito["total_apropriado"] == D("100.00")
    assert feito["fecha"] is False
    assert len(feito["sem_apropriacao"]) == 1


def test_depois_de_tratado_o_pendente_entra_normalmente():
    """"Vamos tratar para poder entrar" — o que muda é só o cadastro ter o ID."""
    linhas = [Linha("500.00", "009999", "AGORA CONHECE")]
    antes = ap.apropriar(linhas, cadastro_por_id={})
    assert antes["pessoas"][0]["pendente_cadastro"] is True

    depois = ap.apropriar(
        linhas, {"99713349334": [dia(1, "A"), dia(2, "B")]},
        cadastro_por_id={"009999": {"cpf": "99713349334", "nome": "AGORA CONHECE"}},
        periodo=(dt.date(2026, 8, 1), dt.date(2026, 8, 15)))

    pessoa = depois["pessoas"][0]
    assert pessoa["pendente_cadastro"] is False
    assert pessoa["cpf"] == "99713349334"
    assert sum(o["valor"] for o in pessoa["por_obra"]) == D("500.00")
    assert depois["fecha"] is True
    assert depois["sem_cadastro"] == []


def test_quem_ficou_sem_apropriacao_aparece_na_lista_e_a_folha_NAO_fecha():
    cadastro = {"000013": {"cpf": "99713349334", "nome": "X"}}
    feito = ap.apropriar([Linha("100.00", "000013")], {}, cadastro_por_id=cadastro)

    assert feito["fecha"] is False, "fechou com gente sem obra nenhuma"
    assert len(feito["sem_apropriacao"]) == 1
    assert feito["total_apropriado"] == D("0")


def test_quem_foi_desmarcado_NAO_conta_no_total():
    cadastro = {"000013": {"cpf": "99713349334", "nome": "X"},
                "000387": {"cpf": "03513441363", "nome": "Y"}}
    feito = ap.apropriar(
        [Linha("100.00", "000013"), Linha("200.00", "000387")],
        {"99713349334": [dia(1, "A")], "03513441363": [dia(1, "A")]},
        ajustes_por_cpf={"03513441363": {"fora": True, "motivo": "desligado"}},
        cadastro_por_id=cadastro,
        periodo=(dt.date(2026, 8, 1), dt.date(2026, 8, 15)))

    assert feito["total_a_pagar"] == D("100.00")
    assert feito["fecha"] is True
    assert len(feito["fora"]) == 1
