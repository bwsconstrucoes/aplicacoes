# -*- coding: utf-8 -*-
"""O espelho do cadastro de colaboradores.

O QUE ESTES TESTES PROTEGEM, e por que cada um existe:

  1. A LEITURA POR NOME DE COLUNA. A regra da casa é procurar coluna pelo nome,
     nunca pela posição — e ela tem preço pago: em 26/09/2026 eu afirmei ao dono
     que um carregamento lia a coluna errada porque olhei o cabeçalho de uma
     CÓPIA da planilha. Aqui o cabeçalho é dado de teste, e uma coluna que muda
     de lugar não pode quebrar nada.

  2. O ALINHAMENTO DAS FAIXAS. A leitura pede só as colunas que interessam, em
     faixas separadas, e o Sheets devolve uma matriz por faixa COM TAMANHOS
     DIFERENTES. Juntar isso por posição sem cuidado desloca dado de uma pessoa
     para outra — um auxílio no CPF errado. É o defeito mais caro possível aqui,
     e é o que o teste do alinhamento persegue.

  3. A LINHA 2 NÃO É PESSOA. A planilha guarda o número de cada coluna na linha
     2 (1, 2, 3…), para uma fórmula com INDIRECT. Lê-la como dado criaria um
     colaborador chamado "2".
"""
from decimal import Decimal

import datetime as dt
import pytest

from app.apps.analisesps import colaboradores as col


# ---------------------------------------------------------------------------
# O link para o card do Pipefy
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bruto,esperado", [
    ("123456", "https://app.pipefy.com/open-cards/123456"),
    (123456, "https://app.pipefy.com/open-cards/123456"),
    # Chega da planilha como texto, e às vezes com ".0" pendurado de quem o
    # tratou como número — o link tem de sair certo do mesmo jeito.
    ("123456.0", "https://app.pipefy.com/open-cards/1234560"),
    ("  123456 ", "https://app.pipefy.com/open-cards/123456"),
    ("", ""),
    (None, ""),
    ("sem número", ""),
])
def test_o_link_do_card_sai_como_a_planilha_monta(bruto, esperado):
    """A planilha monta ="https://app.pipefy.com/open-cards/"&B na coluna BX.
    Dois jeitos de montar o mesmo link divergiriam no dia em que o Pipefy
    mudasse o endereço."""
    assert col.link_do_card(bruto) == esperado


def test_o_ponto_zero_do_numero_vira_digito_e_isso_esta_assumido():
    """⚠️ CASO CONHECIDO E ACEITO: "123456.0" vira "1234560", não "123456".

    Fica registrado porque é decisão, não descuido. Tirar só o ".0" final
    exigiria adivinhar quando o ponto é sujeira e quando é parte do número — e
    o número do card do Pipefy é inteiro, então a planilha não deveria mandar
    ".0" nunca. Se aparecer na prática, o conserto é aqui e o teste muda."""
    assert col.link_do_card("123456.0").endswith("1234560")


# ---------------------------------------------------------------------------
# Agrupar colunas em faixas
# ---------------------------------------------------------------------------
def test_colunas_vizinhas_viram_UMA_faixa():
    assert col._faixas_das_colunas([0, 1, 2]) == [[0, 1, 2]]


def test_colunas_separadas_viram_faixas_separadas():
    """É isto que faz a leitura trazer 30 colunas em vez de 78 — e deixar na
    planilha o endereço, o nome da mãe, o RG, o PIS e o salário, que este
    módulo não tem por que conhecer."""
    assert col._faixas_das_colunas([0, 1, 4, 22, 23]) == [[0, 1], [4], [22, 23]]


def test_a_ordem_e_a_repeticao_da_entrada_nao_importam():
    assert col._faixas_das_colunas([4, 0, 1, 4]) == [[0, 1], [4]]


def test_lista_vazia_nao_gera_faixa():
    assert col._faixas_das_colunas([]) == []


@pytest.mark.parametrize("indice,letra", [
    (0, "A"), (1, "B"), (25, "Z"), (26, "AA"), (69, "BR"), (51, "AZ")])
def test_a_letra_da_coluna(indice, letra):
    assert col.letra_da_coluna(indice) == letra


# ---------------------------------------------------------------------------
# ⚠️ O ALINHAMENTO — o teste mais importante deste arquivo
# ---------------------------------------------------------------------------
def test_faixas_de_tamanhos_DIFERENTES_nao_deslocam_dado():
    """O Sheets corta o fim vazio: uma faixa cujas últimas linhas estão em
    branco volta MAIS CURTA que as outras.

    Se a junção não repusesse essas linhas, o dado da linha 3 de uma faixa
    encostaria na linha 2 de outra — e o auxílio de uma pessoa iria para o CPF
    de outra. É o defeito mais caro que este módulo pode ter."""
    grupos = [[0, 1], [4]]
    blocos = [
        [["111", "Ana"], ["222", "Bia"], ["333", "Caio"]],   # 3 linhas
        [["10,00"], ["20,00"]],                              # 2 linhas: a 3ª vazia
    ]
    linhas = col._juntar(blocos, grupos, 3)

    assert linhas[0] == {0: "111", 1: "Ana", 4: "10,00"}
    assert linhas[1] == {0: "222", 1: "Bia", 4: "20,00"}
    # A terceira pessoa existe, e o valor dela é vazio — não é o valor da Bia.
    assert linhas[2] == {0: "333", 1: "Caio", 4: ""}


def test_linha_com_MENOS_celulas_que_a_faixa_nao_estoura():
    """Uma linha cujo fim está vazio volta com menos células. Ler pela posição
    sem conferir o tamanho levantaria IndexError no meio da carga."""
    linhas = col._juntar([[["111"]]], [[0, 1, 2]], 1)
    assert linhas[0] == {0: "111", 1: "", 2: ""}


def test_faixa_que_voltou_vazia_nao_derruba_a_juncao():
    linhas = col._juntar([[], []], [[0], [4]], 2)
    assert linhas == [{0: "", 4: ""}, {0: "", 4: ""}]


def test_a_juncao_devolve_sempre_o_numero_de_linhas_PEDIDO():
    """Quem chama conta as linhas pelo bloco que pediu. Devolver menos faria a
    carga pular gente sem avisar."""
    assert len(col._juntar([[["x"]]], [[0]], 5)) == 5


# ---------------------------------------------------------------------------
# Achar as colunas pelo nome
# ---------------------------------------------------------------------------
CABECALHO = [
    "CPF (Cadastro de Pessoa Física)",   # A
    "Nº Registro Pipefy",                # B
    "Matrícula",                         # C
    "algo que não interessa",            # D
    "Nome Completo",                     # E
]


def test_acha_as_colunas_pelo_nome_que_a_planilha_usa():
    posicoes, _ = col._achar_colunas(CABECALHO)
    assert posicoes["cpf"] == 0
    assert posicoes["card_pipefy"] == 1
    assert posicoes["matricula"] == 2
    assert posicoes["nome"] == 4


def test_coluna_que_MUDOU_DE_LUGAR_continua_sendo_achada():
    """É o ponto todo de procurar pelo nome: alguém insere uma coluna na
    planilha e nada quebra."""
    embaralhado = ["nova coluna", "Nome Completo", "CPF (Cadastro de Pessoa Física)"]
    posicoes, _ = col._achar_colunas(embaralhado)
    assert posicoes["nome"] == 1
    assert posicoes["cpf"] == 2


def test_cabecalho_com_espaco_e_caixa_diferente_ainda_casa():
    posicoes, _ = col._achar_colunas(["  cpf (CADASTRO de Pessoa Física)  "])
    assert posicoes["cpf"] == 0


def test_coluna_de_auxilio_que_FALTA_vira_AVISO_escrito():
    """⚠️ O nome das colunas de auxílio é o único que NÃO está confirmado: elas
    chegam por IMPORTRANGE de uma faixa, sem título.

    Então, quando a carga não acha uma delas, ela tem de AVISAR — porque campo
    de auxílio em branco vira pagamento a MENOS, e pagamento a menos ninguém
    nota tão rápido quanto um a mais."""
    _, avisos = col._achar_colunas(CABECALHO)
    juntos = " ".join(avisos)
    assert "Alimenta" in juntos
    assert "Transporte" in juntos
    assert "em branco" in juntos


def test_o_aviso_da_coluna_que_falta_PEDE_o_nome_certo():
    """Aviso que só diz "não achei" deixa o dono sem saber o que fazer."""
    _, avisos = col._achar_colunas(CABECALHO)
    assert any("nome exato" in a for a in avisos)


# ---------------------------------------------------------------------------
# Montar a pessoa
# ---------------------------------------------------------------------------
def _posicoes():
    return {"cpf": 0, "nome": 1, "card_pipefy": 2, "valor_alimentacao": 3,
            "modo_alimentacao": 4, "data_saida": 5, "valor_transporte": 6}


def test_a_pessoa_sai_com_dinheiro_e_data_convertidos():
    linha = {0: "111.444.777-35", 1: "Ana Souza", 2: "9911",
             3: "1.198,84", 4: "Segunda à Sexta", 5: "31/08/2026", 6: ""}
    r = col._registro(linha, _posicoes())
    assert r["cpf"] == "11144477735"
    assert r["nome"] == "Ana Souza"
    assert r["valor_alimentacao"] == Decimal("1198.84")
    assert r["data_saida"] == dt.date(2026, 8, 31)
    # ⚠️ VAZIO É None, NÃO ZERO. "o cadastro não diz" e "o cadastro diz zero"
    # são respostas diferentes, e somá-las como zero esconderia o
    # preenchimento faltando.
    assert r["valor_transporte"] is None


def test_o_valor_com_PONTO_de_milhar_nao_e_multiplicado_por_cem():
    """A armadilha que quase passou no leitor da Folha Sintética: apagar todos
    os pontos transforma 1198.84 em 119884."""
    r = col._registro({0: "11144477735", 1: "Ana", 3: "1.198,84"}, _posicoes())
    assert r["valor_alimentacao"] == Decimal("1198.84")


def test_linha_SEM_CPF_nao_vira_pessoa():
    """O CPF é a chave em tudo — no ponto, na folha da contabilidade, no
    rateio. Sem ele a linha não casa com nada e só ocuparia lugar na lista
    fingindo que existe."""
    assert col._registro({0: "", 1: "Alguém"}, _posicoes()) is None


def test_a_LINHA_2_DA_PLANILHA_nao_vira_pessoa():
    """A linha 2 guarda o número de cada coluna (1, 2, 3…), para uma fórmula
    com INDIRECT da aba "Dados Gerais". Lida como dado, criaria um colaborador
    chamado "2" com CPF "1"."""
    assert col._registro({0: "1", 1: "2", 2: "3"}, _posicoes()) is None


def test_cpf_com_MENOS_de_onze_digitos_nao_passa():
    assert col._registro({0: "123456", 1: "Ana"}, _posicoes()) is None


def test_todos_os_campos_do_banco_saem_preenchidos_ou_None():
    """A gravação monta a tupla na ordem de CAMPOS. Um campo que não saísse do
    registro viraria KeyError no meio da carga — ou, pior, deslocaria a tupla."""
    r = col._registro({0: "11144477735", 1: "Ana"}, _posicoes())
    for campo in col.CAMPOS:
        assert campo in r, f"falta o campo {campo}"


def test_a_primeira_linha_dos_dados_e_a_TRES():
    """Cabeçalho na 1, números das colunas na 2, dados da 3 em diante — é o que
    a própria planilha faz: QUERY('Dados Documentos'!A3:AX; …)."""
    assert col.PRIMEIRA_LINHA_DADOS == 3
    assert col.LINHA_DO_CABECALHO == 1


def test_o_sql_de_gravar_tem_um_marcador_por_campo():
    """Um marcador a mais ou a menos grava valor na coluna errada — e aqui as
    colunas vizinhas são dinheiro."""
    assert col.SQL_GRAVAR.count("?") == len(col.CAMPOS)
    assert "ON CONFLICT (cpf) DO UPDATE" in col.SQL_GRAVAR
    # ATUALIZA, NÃO SUBSTITUI: a tabela nunca fica vazia no meio do caminho.
    assert "DELETE" not in col.SQL_GRAVAR.upper()


def test_o_cpf_nao_e_sobrescrito_pelo_proprio_update():
    """`cpf = EXCLUDED.cpf` seria inócuo, mas é a chave do conflito: mexer nela
    no UPDATE é o tipo de coisa que passa e depois assombra."""
    assert "cpf = EXCLUDED.cpf" not in col.SQL_GRAVAR


# ---------------------------------------------------------------------------
# O teto de segurança
# ---------------------------------------------------------------------------
def test_ha_teto_de_linhas_e_ele_cabe_o_cadastro_de_verdade():
    """O cadastro tem ~3.500 pessoas. O teto existe para a aba que vier com um
    número de linhas absurdo (fórmula esticada) não fazer a carga tentar ler
    milhões de células numa instância de 2 GB dividida com 17 módulos."""
    assert col.MAXIMO_DE_LINHAS >= 10_000
    assert col.LINHAS_POR_BLOCO <= 1000


def test_sem_a_tabela_a_tela_recebe_vazio_em_vez_de_estourar():
    """O código sobe para o Render ANTES de alguém apertar "Aplicar
    atualizações do banco". No intervalo, a tela tem de abrir."""
    assert col.por_cpf("11144477735") is None
    assert col.muitos_por_cpf(["11144477735"]) == {}
    assert col.buscar("ana") == []
    assert col.quando_atualizou()["pronto"] is False


# ---------------------------------------------------------------------------
# O AVISO NÃO PODE VIRAR RUÍDO
#
# ⚠️ Correção do dono em 26/09/2026, sobre outra crítica que eu ia criar: avisar
# sobre o que é normal transforma o aviso em ruído, e ruído faz ignorar o aviso
# que importa. Então a lista de avisos tem de ser CURTA e só do que decide
# dinheiro ou elegibilidade.
# ---------------------------------------------------------------------------
def test_coluna_de_ENFEITE_faltando_nao_vira_aviso():
    """Matrícula, celular, convenção: a falta delas não faz ninguém receber
    errado. Aparecer na lista de avisos só afogaria o aviso do auxílio."""
    cabecalho = ["CPF (Cadastro de Pessoa Física)", "Nome Completo"]
    _, avisos = col._achar_colunas(cabecalho)
    juntos = " ".join(avisos)
    for enfeite in ("Matrícula", "Celular", "Convenção", "Cargo"):
        assert enfeite not in juntos, \
            f'"{enfeite}" faltando não devia virar aviso'


def test_coluna_que_decide_DINHEIRO_faltando_vira_aviso():
    cabecalho = ["CPF (Cadastro de Pessoa Física)", "Nome Completo"]
    _, avisos = col._achar_colunas(cabecalho)
    juntos = " ".join(avisos)
    for importa in ("Alimenta", "Transporte", "Gratifica", "Registro Pipefy",
                    "Tipo de Contrato", "Data de Saída"):
        assert importa in juntos, f'"{importa}" faltando TEM de virar aviso'


def test_a_lista_de_avisos_cabe_na_tela():
    """Com o cabeçalho só de CPF e nome — o pior caso — a lista tem de continuar
    legível. Mais de uma dúzia de linhas ninguém lê."""
    _, avisos = col._achar_colunas(
        ["CPF (Cadastro de Pessoa Física)", "Nome Completo"])
    assert len(avisos) <= 12, f"{len(avisos)} avisos é muita coisa para uma tela"


def test_o_cabecalho_COMPLETO_nao_gera_aviso_nenhum():
    """A prova de que os nomes que o módulo procura são os que a planilha usa:
    com o cabeçalho de verdade, silêncio."""
    cabecalho = [aceitos[0] for aceitos in col.COLUNAS.values()] + \
                [aceitos[0] for aceitos in col.COLUNAS_DOS_AUXILIOS.values()]
    posicoes, avisos = col._achar_colunas(cabecalho)
    assert avisos == [], avisos
    assert set(posicoes) == set(col.COLUNAS) | set(col.COLUNAS_DOS_AUXILIOS)


def test_todo_campo_do_banco_tem_uma_coluna_procurada():
    """Um campo em CAMPOS que ninguém procura na planilha ficaria vazio para
    sempre, sem ninguém notar."""
    procurados = set(col.COLUNAS) | set(col.COLUNAS_DOS_AUXILIOS)
    assert set(col.CAMPOS) == procurados, (
        "sobrando no banco: " + str(set(col.CAMPOS) - procurados) +
        " / procurado e sem lugar no banco: " + str(procurados - set(col.CAMPOS)))
