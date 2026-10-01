# -*- coding: utf-8 -*-
"""
A LEITURA DA PLANILHA DE CONCILIAÇÃO — com os exemplos REAIS do dono.

⚠️ AS AMOSTRAS ABAIXO FORAM COLADAS POR ELE EM 24/09/2026, direto das abas que
usa. Não são inventadas, e é isso que dá valor a estes testes: cada banco
escreve de um jeito, e as diferenças que importam só aparecem no dado de
verdade. Se um formato mudar, é aqui que se acrescenta a amostra nova.
"""
from datetime import date
from decimal import Decimal

import pytest

from app.apps.analisesps import conciliacao_planilha as planilha


# ---------------------------------------------------------------------------
# BRADESCO — crédito e débito em colunas separadas, o débito JÁ NEGATIVO,
# e a coluna "TIPO" trazendo o NÚMERO do documento.
# ---------------------------------------------------------------------------
# ⚠️ AS ONZE LINHAS SEGUIDAS que ele colou, sem pular nenhuma — e de propósito:
# a conferência do saldo só funciona com o extrato CONTÍNUO. Com linhas
# salteadas ela reprova (e reprovou, na primeira versão deste teste), porque o
# saldo de fato não fecha. Isso é bom sinal: a conferência pega o que deveria.
BRADESCO_50302 = [
    ["DATA", "DESCRIÇÃO", "", "TIPO", "CRÉDITO", "DEBITO", "SALDO",
     "", "", "", "", "Conciliado"],
    ["08/10/2024", "RENTAB.INVEST FACILCRED*", "", "1798917", "3,87", "",
     "637.425,90", "", "", "", "", "Conciliado"],
    ["08/10/2024", "PAGTO ELETRON COBRANCA\nALPE FUNDO DE INVESTIMENTO EM DI",
     "", "938", "", "-675,87", "636.750,03", "", "", "", "", "Conciliado"],
    ["08/10/2024", "PAGTO ELETRON COBRANCA\nLOJAO DUFERRO", "", "939", "",
     "-481,36", "636.268,67", "", "", "", "", "Conciliado"],
    ["08/10/2024", "PAGTO ELETRON COBRANCA\nVOTORANTIM CIMENTOS NNE SA", "",
     "940", "", "-3.374,40", "632.894,27", "", "", "", "", "Conciliado"],
    ["08/10/2024", "PAGTO ELETRON COBRANCA\nPOSTO BR COMERCIO DE COMBUSTIVEI",
     "", "941", "", "-1.316,08", "631.578,19", "", "", "", "", "Conciliado"],
    ["08/10/2024", "TED DIF.TITUL.CC H.BANK\nDEST. DAVID ANTONIO A SANT", "",
     "4396521", "", "-3.500,00", "628.078,19", "", "", "", "", "Conciliado"],
    ["08/10/2024", "TED DIF.TITUL.CC H.BANK\nDEST. Leonardo Arruda Cost", "",
     "4418581", "", "-10.000,00", "618.078,19", "", "", "", "", "Conciliado"],
    ["08/10/2024", "TARIFA BANCARIA\nTRANSF PGTO PIX", "", "41024", "",
     "-9,00", "618.069,19", "", "", "", "", "Conciliado"],
    ["08/10/2024", "DOC/TED INTERNET\nTED INTERNET", "", "4396521", "",
     "-5,79", "618.063,40", "", "", "", "", "Conciliado"],
    ["08/10/2024", "DOC/TED INTERNET\nTED INTERNET", "", "4418581", "",
     "-5,79", "618.057,61", "", "", "", "", "Conciliado"],
    ["08/10/2024", "TRANSFERENCIA PIX\nDES: CARLOS ALEXANDRE RIBE 08/10", "",
     "946047", "", "-2.450,00", "615.607,61", "", "", "", "", "Conciliado"],
]

BRADESCO_IFPE = [
    ["Data", "Histórico", "Documento", "Tipo", "Crédito", "Débito", "Saldo",
     "Obs. 1", "Obs. 2", "Status Tarifa Omie", "", "Status Conciliação"],
    ["21/10/2025", "TED-TRANSF ELET DISPON\nREMET.L R SANTOS CONSTRUC", "",
     "1244152", "30.000,00", "", "30.000,00", "", "", "", "", "Conciliado"],
    ["04/11/2025", "TRANSFERENCIA PIX\nDES: SIQUEIRA & VASCONCELO 04/11", "",
     "1011136", "", "-850,00", "29.150,86", "conferir", "", "tarifa ok", "",
     "Conciliado"],
]

# ---------------------------------------------------------------------------
# BANCO DO BRASIL — UMA coluna de valor, o sentido numa coluna à parte, e as
# linhas "Saldo Anterior"/"Saldo do dia", que o dono mandou ignorar.
# ---------------------------------------------------------------------------
BB = [
    ["Data", "Lançamento", "Detalhes", "N° documento", "Valor",
     "Tipo Lançamento", "", "", "", "", "", "Conciliado"],
    ["29/08/2025", "Saldo Anterior", "", "", "0,00", "", "", "", "", "", "", ""],
    ["10/09/2025", "Tarifa Pacote de Serviços", "Cobrança referente 10/09/2025",
     "842531101710834", "-343,60", "Saída", "", "", "", "", "", "Conciliado"],
    ["10/09/2025", "BB Rende Fácil", "Rende Facil", "9903", "343,60",
     "Entrada", "", "", "", "", "", "Conciliado"],
    ["10/09/2025", "Saldo do dia", "", "", "0,00", "", "", "", "", "", "", ""],
    ["26/09/2025", "TED-Crédito em Conta",
     "237 0624 00079526000109 BWS CONSTRUCOE", "395810808", "35.000,00",
     "Entrada", "", "", "", "", "", "Conciliado"],
]

# ---------------------------------------------------------------------------
# SANTANDER — o bloco de colunas REPETIDO na mesma linha de cabeçalho, e as
# datas do mais novo para o mais antigo.
# ---------------------------------------------------------------------------
SANTANDER = [
    ["Data", "Histórico", "Documento", "Valor (R$)", "Saldo (R$)", "",
     "Data", "Histórico", "Documento", "Valor (R$)", "Saldo (R$)", "Conciliado"],
    ["22/01/2026", "Resgate contamax automatico", "", "27.000,00", "0,00",
     "", "", "", "", "", "", "Conciliado"],
    ["22/01/2026", "Pix enviado", "", "-27.000,00", "-27.000,00", "", "", "",
     "", "", "", ""],
    ["19/01/2026", "Tarifa mensalidade pacote servicos", "", "-339,00",
     "8.421,15", "", "", "", "", "", "", "Conciliado"],
]

# ---------------------------------------------------------------------------
# SICREDI — "Tipo" traz o TIPO (PIX_DEB), e não o documento.
# ---------------------------------------------------------------------------
SICREDI = [
    ["Data", "Descrição", "Tipo", "Valor", "Saldo", "", "", "", "", "", "",
     "Conciliado"],
    ["02/12/2024", "PAGAMENTO PIX 94162786372 Francisco Isaque Sátir",
     "PIX_DEB", "-152,00", "95.737,44", "", "", "", "", "", "", "Conciliado"],
    ["02/12/2024", "PAGAMENTO PIX SICREDI 25024063000109 WERBBET CAR",
     "CX685054", "-130,00", "87.807,44", "", "", "", "", "", "", "Conciliado"],
]


# ---------------------------------------------------------------------------
# O básico, aba por aba
# ---------------------------------------------------------------------------
def test_bradesco_le_credito_e_debito_com_o_sinal_certo():
    """⚠️ O DÉBITO JÁ VEM NEGATIVO na planilha dele (-675,87). Subtrair de
    novo inverteria metade do extrato — e o saldo dobraria sem nada estourar."""
    lido = planilha.interpretar(BRADESCO_50302, "BD 50302")

    valores = [l["valor"] for l in lido["linhas"]]
    assert valores[:3] == [Decimal("3.87"), Decimal("-675.87"),
                           Decimal("-481.36")]
    assert valores[-1] == Decimal("-2450.00")
    assert len(valores) == 11


def test_bradesco_usa_a_coluna_TIPO_como_documento():
    """No Bradesco "Tipo" é 1798917 — o número do documento."""
    lido = planilha.interpretar(BRADESCO_50302, "BD 50302")
    assert lido["tipo_e_documento"] is True
    assert lido["linhas"][0]["documento"] == "1798917"


def test_sicredi_NAO_usa_TIPO_como_documento():
    """⚠️ No Sicredi "Tipo" é PIX_DEB — o tipo do lançamento. A mesma coluna,
    o mesmo nome, outro conteúdo. Decidir pelo nome poria "PIX_DEB" no campo
    de documento."""
    lido = planilha.interpretar(SICREDI, "Sicredi 30008")
    assert lido["tipo_e_documento"] is False
    assert lido["linhas"][0]["documento"] == ""


def test_o_BB_ignora_saldo_anterior_e_saldo_do_dia():
    """Ele avisou: *"pode ser uma informação de saldo do dia, isso aí é
    ignorável, nem para entrar"*. Elas entrariam caladas, com valor zero — e
    ninguém notaria olhando o saldo."""
    lido = planilha.interpretar(BB, "BB")

    assert len(lido["linhas"]) == 3
    assert all("saldo" not in l["descricao"].lower() for l in lido["linhas"])
    porques = [d["porque"] for d in lido["descartadas"]]
    assert porques.count("é saldo, não lançamento") == 2


def test_o_BB_usa_a_coluna_de_SENTIDO_quando_o_valor_vem_sem_sinal():
    lido = planilha.interpretar(BB, "BB")
    por_descricao = {l["descricao"]: l["valor"] for l in lido["linhas"]}
    assert por_descricao["Tarifa Pacote de Serviços — Cobrança referente "
                         "10/09/2025"] == Decimal("-343.60")
    assert por_descricao["BB Rende Fácil — Rende Facil"] == Decimal("343.60")


def test_o_BB_junta_lancamento_e_detalhes_na_descricao():
    """"Tarifa Pacote de Serviços" sozinho não diz de que mês é."""
    lido = planilha.interpretar(BB, "BB")
    assert "Cobrança referente 10/09/2025" in lido["linhas"][0]["descricao"]


def test_o_santander_le_SO_o_primeiro_bloco_de_colunas():
    """⚠️ O cabeçalho dele repete Data|Histórico|Documento|Valor|Saldo. Ler os
    dois blocos como um duplicaria o extrato inteiro."""
    lido = planilha.interpretar(SANTANDER, "STDER 13006542-0")

    assert lido["colunas"]["data"] == 0
    assert lido["colunas"]["valor"] == 3
    assert len(lido["linhas"]) == 3


def test_a_palavra_conciliado_vira_a_marca(  ):
    """Ele padronizou a palavra em texto justamente para isto — antes era uma
    célula pintada de amarelo, que exigiria outra API do Google."""
    lido = planilha.interpretar(SANTANDER, "STDER")
    marcas = [l["conciliado"] for l in lido["linhas"]]
    assert marcas == [True, False, True]
    assert lido["conciliadas"] == 2


def test_a_celula_vazia_de_conciliado_e_NAO_conciliado():
    """No Santander dele há linha sem nada na coluna — o "Pix enviado"."""
    lido = planilha.interpretar(SANTANDER, "STDER")
    assert lido["linhas"][1]["conciliado"] is False


def test_as_colunas_extras_viram_OBSERVACAO_com_o_nome_delas():
    """Pedido dele: *"algum dado que tenha entre as colunas da parte numérica
    e a coluna L, a gente vai colocar como observação do lançamento"*. Sem o
    nome da coluna junto, "conferir" sozinho não diria nada daqui a um ano."""
    lido = planilha.interpretar(BRADESCO_IFPE, "BD IFPE 2541")

    # A coluna K não tem cabeçalho e entra assim mesmo, chamada pela letra —
    # é justamente o caso que a primeira versão deixava de fora.
    assert lido["observacao_veio_de"] == ["Obs. 1", "Obs. 2",
                                          "Status Tarifa Omie", "coluna K"]
    assert lido["linhas"][0]["observacao"] == ""
    assert lido["linhas"][1]["observacao"] == ("Obs. 1: conferir · "
                                               "Status Tarifa Omie: tarifa ok")


def test_o_periodo_e_a_soma_saem_da_aba():
    lido = planilha.interpretar(BRADESCO_50302, "BD 50302")
    assert lido["periodo_ini"] == date(2024, 10, 8)
    assert lido["periodo_fim"] == date(2024, 10, 8)
    assert lido["soma"] == Decimal("-21814.42")


# ---------------------------------------------------------------------------
# A prova de que os sinais foram entendidos
# ---------------------------------------------------------------------------
def test_o_saldo_da_planilha_confere_a_leitura():
    """⚠️ É A ÚNICA PROVA DE QUE OS SINAIS ESTÃO CERTOS. Ler o débito como
    positivo deixaria todas as linhas lá, com as datas e os históricos certos.
    Só o saldo denuncia."""
    lido = planilha.interpretar(BRADESCO_50302, "BD 50302")
    conferencia = lido["conferencia_do_saldo"]

    assert conferencia["deu"] is True
    assert conferencia["bate"] is True


def test_a_coluna_DEBITO_vale_com_ou_sem_o_sinal_de_menos():
    """⚠️ NEM TODO BANCO ESCREVE O DÉBITO NEGATIVO. O Bradesco dele escreve
    (-675,87); outro pode escrever 675,87 na coluna "Débito", e as duas coisas
    querem dizer a mesma. O leitor aceita as duas — foi escrito assim de
    propósito, e é por isso que este caso NÃO depende do saldo para acertar."""
    sem_sinal = [list(l) for l in BRADESCO_50302]
    for linha in sem_sinal[1:]:
        linha[5] = linha[5].replace("-", "")

    com = planilha.interpretar(BRADESCO_50302, "com")
    sem = planilha.interpretar(sem_sinal, "sem")
    assert [l["valor"] for l in com["linhas"]] == [l["valor"] for l in sem["linhas"]]


def test_o_saldo_acusa_quando_a_leitura_erra_o_sinal():
    """A prova de que a prova funciona, onde ela é de fato necessária: a coluna
    ÚNICA de valor. Ali o sinal vem como está escrito — não há crédito e débito
    separados para deduzir o sentido. Um banco que escrevesse a saída sem o
    menos passaria batido, com todas as linhas certas, e só o saldo denuncia."""
    certo = [
        ["Data", "Histórico", "Valor", "Saldo", "Conciliado"],
        ["01/09/2025", "SALDO INICIAL DEPOSITO", "1.000,00", "1.000,00", "Conciliado"],
        ["02/09/2025", "PAGAMENTO FORNECEDOR", "-300,00", "700,00", "Conciliado"],
        ["03/09/2025", "PAGAMENTO ALUGUEL", "-200,00", "500,00", "Conciliado"],
    ]
    assert planilha.interpretar(certo, "certo")["conferencia_do_saldo"]["bate"] is True

    torto = [list(l) for l in certo]
    for linha in torto[2:]:
        linha[2] = linha[2].replace("-", "")

    conferencia = planilha.interpretar(torto, "torto")["conferencia_do_saldo"]
    assert conferencia["deu"] is True
    assert conferencia["bate"] is False


# ---------------------------------------------------------------------------
# As recusas
# ---------------------------------------------------------------------------
def test_aba_sem_cabecalho_reconhecivel_e_recusada_com_frase():
    with pytest.raises(planilha.ErroDaPlanilha) as erro:
        planilha.interpretar([["uma", "coisa"], ["outra", "coisa"]], "x")
    assert "cabeçalho" in str(erro.value).lower()


def test_aba_vazia_e_recusada():
    with pytest.raises(planilha.ErroDaPlanilha):
        planilha.interpretar([], "x")


def test_aba_so_com_cabecalho_e_recusada_em_vez_de_importar_nada():
    """Importar zero linha calada faria parecer que deu certo."""
    with pytest.raises(planilha.ErroDaPlanilha) as erro:
        planilha.interpretar([SICREDI[0]], "vazia")
    assert "lançamento" in str(erro.value).lower()


@pytest.mark.parametrize("bruto,esperado", [
    ("-3.374,40", Decimal("-3374.40")),
    ("637.425,90", Decimal("637425.90")),
    ("3,87", Decimal("3.87")),
    ("R$ 1.000,00", Decimal("1000.00")),
    ("", None),
    ("   ", None),
    ("não é número", None),
])
def test_o_numero_do_brasil_e_lido_certo(bruto, esperado):
    assert planilha._numero(bruto) == esperado


# ---------------------------------------------------------------------------
# O endereço da planilha
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bruto,esperado", [
    ("https://docs.google.com/spreadsheets/d/1dqkadY5ix3E5E1hOcJro57-gFBa8uk1Ym26LLNcERws/edit",
     "1dqkadY5ix3E5E1hOcJro57-gFBa8uk1Ym26LLNcERws"),
    ("https://docs.google.com/spreadsheets/d/1dqkadY5ix3E5E1hOcJro57-gFBa8uk1Ym26LLNcERws",
     "1dqkadY5ix3E5E1hOcJro57-gFBa8uk1Ym26LLNcERws"),
    ("1dqkadY5ix3E5E1hOcJro57-gFBa8uk1Ym26LLNcERws",
     "1dqkadY5ix3E5E1hOcJro57-gFBa8uk1Ym26LLNcERws"),
])
def test_o_endereco_inteiro_da_barra_e_aceito(bruto, esperado, monkeypatch):
    """Exigir que a pessoa recorte o pedaço certo de uma URL é pedir para
    errar — a mesma regra do campo da pasta do Drive."""
    gravado = []
    from app.apps.analisesps import db as db_

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=()): gravado.append(params); return self
        def commit(self): pass

    monkeypatch.setattr(db_, "conexao", lambda: ConexaoFalsa())
    assert planilha.guardar_planilha(bruto) == esperado
    assert gravado[0][1] == esperado


def test_endereco_que_nao_e_planilha_e_recusado(monkeypatch):
    with pytest.raises(planilha.ErroDaPlanilha) as erro:
        planilha.guardar_planilha("isso aqui não é planilha nenhuma")
    assert "planilha do Google" in str(erro.value)


# ---------------------------------------------------------------------------
# ⚠️ O DEFEITO QUE PASSOU EM SILÊNCIO — 24/09/2026
#
# Ele importou de verdade e voltou: *"tenho a impressão que não foi importada
# as observações (…) confirmo, nenhuma observação foi importada."*
#
# A regra antiga só olhava colunas com CABEÇALHO preenchido. Nas abas do
# Bradesco dele, as colunas de anotação NÃO TÊM CABEÇALHO NENHUM. O sistema
# importou tudo, disse que deu certo, e deixou dois anos de anotação para trás
# sem avisar — o pior tipo de defeito, o que parece sucesso.
#
# A regra dele: *"a coluna subsequente ao último dado (…) porque tem uns que o
# último dado, o saldo, fica numa coluna e outra coluna. Então seria a
# informação subsequente."*
# ---------------------------------------------------------------------------
BRADESCO_SEM_CABECALHO_NAS_SOBRAS = [
    # DATA  DESCRIÇÃO  (vazia) TIPO  CRÉDITO DEBITO SALDO  H  I  J  K  Conciliado
    ["DATA", "DESCRIÇÃO", "", "TIPO", "CRÉDITO", "DEBITO", "SALDO",
     "", "", "", "", "Conciliado"],
    ["08/10/2024", "PAGTO ELETRON COBRANCA", "", "938", "", "-675,87",
     "636.750,03", "falta nota", "", "ver com o Nilo", "urgente", "Conciliado"],
    ["09/10/2024", "TARIFA BANCARIA", "", "41024", "", "-9,00",
     "636.741,03", "", "", "", "", "Conciliado"],
]


def test_coluna_SEM_CABECALHO_depois_do_saldo_vira_observacao():
    """⚠️ O defeito que fez o dono perder dois anos de anotação na primeira
    importação. Nenhuma dessas colunas tem nome no cabeçalho."""
    lido = planilha.interpretar(BRADESCO_SEM_CABECALHO_NAS_SOBRAS, "BD 50302")

    primeira = lido["linhas"][0]
    assert primeira["observacao"] == ("coluna H: falta nota · "
                                      "coluna J: ver com o Nilo · "
                                      "coluna K: urgente")
    # A linha sem nada anotado continua sem observação — e não com os rótulos
    # das colunas vazias pendurados nela.
    assert lido["linhas"][1]["observacao"] == ""


def test_a_coluna_de_conciliado_NAO_entra_na_observacao():
    """Ela é a marca, não uma anotação. Entrando, toda linha conciliada teria
    a palavra "Conciliado" escrita na observação, para sempre."""
    lido = planilha.interpretar(BRADESCO_SEM_CABECALHO_NAS_SOBRAS, "BD 50302")
    assert "Conciliado" not in lido["linhas"][0]["observacao"]
    assert lido["linhas"][0]["conciliado"] is True


def test_a_letra_da_coluna_e_a_da_planilha():
    """"coluna H" tem de ser a coluna H que ele vê no Google, senão o rótulo
    atrapalha em vez de ajudar."""
    assert planilha.letra_da_coluna(0) == "A"
    assert planilha.letra_da_coluna(7) == "H"
    assert planilha.letra_da_coluna(10) == "K"
    assert planilha.letra_da_coluna(26) == "AA"
