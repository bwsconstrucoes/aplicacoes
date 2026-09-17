# -*- coding: utf-8 -*-
"""O relatório do FSist subido como ARQUIVO — 13/09/2026.

Reclamação do dono, e ela é justa: *"Importar relatório FSist — e ele diz que
vai rodar no sistema? E cadê a opção de incluir o arquivo? Como é que ele vai
rodar? De onde vai tirar essa informação, se eu não estou nem colocando?"*

O botão lia a aba "Relatório FSIST" da planilha de apoio — o fluxo antigo, de
colar o relatório lá. Funciona, e não era o que o nome prometia: "importar
relatório" pede um arquivo, e não havia onde pôr.

AS DUAS PORTAS FICAM, e passam pelo MESMO mapeamento de colunas e pela MESMA
gravação. Um segundo caminho de leitura divergiria no dia em que o FSist
mudasse uma coluna de nome — e só um dos dois seria corrigido.
"""
from __future__ import annotations

import io

import pytest

CHAVE_A = "26260929066773000152550010000014301000000010"
CHAVE_B = "26260911222333000181570010000000771000000010"

CABECALHO = ["Emissão", "Chave", "Número", "Série", "Valor", "Situação",
             "Emitente CNPJ", "Emitente", "Emitente UF", "Destinatário CNPJ"]
LINHA_A = ["01/09/2026", CHAVE_A, "1430", "1", "269,00", "Autorizada",
           "29.066.773/0001-52", "ACME MATERIAIS", "BA", "10.656.452/0078-69"]
LINHA_B = ["02/09/2026", CHAVE_B, "77", "1", "1.250,50", "Cancelada",
           "11.222.333/0001-81", "TRANSPORTES BETA", "SE", "10.656.452/0078-69"]


def _csv(linhas, separador=";", codificacao="utf-8"):
    texto = "\r\n".join(separador.join(c for c in linha) for linha in linhas)
    return texto.encode(codificacao)


def _xlsx(linhas):
    from openpyxl import Workbook
    livro = Workbook()
    aba = livro.active
    for linha in linhas:
        aba.append(linha)
    saco = io.BytesIO()
    livro.save(saco)
    return saco.getvalue()


# ---------------------------------------------------------------------------
# A LEITURA DO ARQUIVO — os três formatos que o FSist exporta
# ---------------------------------------------------------------------------
def test_le_CSV_com_ponto_e_virgula():
    """É como o Excel brasileiro salva."""
    from app.apps.analisesps import sincronizacao

    linhas = sincronizacao._linhas_do_arquivo(
        _csv([CABECALHO, LINHA_A]), "relatorio.csv")
    assert linhas[0][1] == "Chave"
    assert linhas[1][1] == CHAVE_A


def test_le_CSV_com_virgula():
    """O separador é DESCOBERTO, não presumido. Presumir um dos dois faria o
    arquivo inteiro virar uma coluna só, e o recado seria "não achei a coluna
    Chave" — que manda procurar defeito no lugar errado."""
    from app.apps.analisesps import sincronizacao

    linhas = sincronizacao._linhas_do_arquivo(
        _csv([CABECALHO, LINHA_A], separador=","), "relatorio.csv")
    assert linhas[1][1] == CHAVE_A


def test_le_arquivo_com_acento_que_NAO_e_UTF8():
    """O relatório é em português e o arquivo salvo pelo Excel brasileiro não
    é UTF-8. Recusar por isso obrigaria a converter antes — o trabalho manual
    que esta tela existe para tirar."""
    from app.apps.analisesps import sincronizacao

    linhas = sincronizacao._linhas_do_arquivo(
        _csv([CABECALHO, LINHA_A], codificacao="cp1252"), "relatorio.csv")
    assert linhas[0][0] == "Emissão"


def test_le_XLSX():
    from app.apps.analisesps import sincronizacao

    linhas = sincronizacao._linhas_do_arquivo(
        _xlsx([CABECALHO, LINHA_A]), "relatorio.xlsx")
    assert linhas[1][1] == CHAVE_A


def test_o_XLS_antigo_e_recusado_DIZENDO_o_que_fazer():
    """Recusa que não diz o que fazer é recusa que vira chamado."""
    from app.apps.analisesps import sincronizacao

    with pytest.raises(sincronizacao.ErroDeRelatorio) as e:
        sincronizacao._linhas_do_arquivo(b"qualquer coisa", "relatorio.xls")
    assert ".xlsx" in str(e.value)


def test_arquivo_grande_demais_e_recusado():
    """A instância divide 2 GB com 17 módulos e já morreu de falta de memória
    em julho de 2026."""
    from app.apps.analisesps import sincronizacao

    with pytest.raises(sincronizacao.ErroDeRelatorio) as e:
        sincronizacao.importar_notas_de_arquivo(
            b"x" * (sincronizacao.MAXIMO_RELATORIO + 1), "gordo.csv")
    assert "MB" in str(e.value)


def test_arquivo_vazio_e_recusado():
    from app.apps.analisesps import sincronizacao

    with pytest.raises(sincronizacao.ErroDeRelatorio):
        sincronizacao.importar_notas_de_arquivo(b"", "vazio.csv")


def test_arquivo_SEM_a_coluna_chave_diz_o_que_encontrou(monkeypatch):
    """*"Confira se subiu o relatório certo"* — e mostrar o que veio no lugar é
    o que permite a ele descobrir que subiu o extrato do banco por engano."""
    from app.apps.analisesps import sincronizacao

    with pytest.raises(sincronizacao.ErroDeRelatorio) as e:
        sincronizacao.importar_notas_de_arquivo(
            _csv([["Data", "Histórico", "Valor"], ["01/09", "PIX", "10,00"]]),
            "extrato.csv")
    assert "Chave" in str(e.value)
    assert "Histórico" in str(e.value), "não disse o que encontrou no lugar"


def test_o_cabecalho_NAO_precisa_estar_na_primeira_linha():
    """No relatório do FSist a primeira linha é o título. Procurar a linha que
    TEM a coluna "Chave" é mais robusto do que fixar o número."""
    from app.apps.analisesps import sincronizacao

    arquivo = _csv([["Relatório de Notas de Compras"], [""],
                    CABECALHO, LINHA_A])
    linhas = sincronizacao._linhas_do_arquivo(arquivo, "r.csv")
    # A procura em si é exercitada pelo teste com banco; aqui basta que as
    # linhas de título tenham sobrevivido à leitura.
    assert linhas[2][1] == "Chave"


# ---------------------------------------------------------------------------
# AS DUAS PORTAS NÃO PODEM DIVERGIR
# ---------------------------------------------------------------------------
def test_o_arquivo_usa_o_MESMO_mapeamento_de_colunas_da_aba():
    """Dois mapeamentos divergiriam no dia em que o FSist mudasse uma coluna de
    nome — e só um dos dois seria corrigido."""
    import inspect

    from app.apps.analisesps import sincronizacao

    fonte = inspect.getsource(sincronizacao.importar_notas_de_arquivo)
    assert "COLUNAS_DAS_NOTAS" in fonte
    assert "COLUNAS_OBRIGATORIAS_DAS_NOTAS" in fonte
    assert "_gravar_notas" in fonte, (
        "a gravação tem de ser a mesma das notas vindas da Receita e da aba")


def test_o_botao_da_aba_do_FSIST_SAIU_do_trabalho_fiscal():
    """⚠️ Decisão do dono em 16/09/2026: *"pra que diabo serve o botão 'Ler a
    aba do FSist na planilha'? Não tem sentido isso. Vou importar o relatório
    no sistema."*

    O teste anterior exigia o contrário — que o botão existisse e dissesse que
    lê uma aba. Ele nasceu de uma cobrança ANTERIOR dele (*"cadê a opção de
    incluir o arquivo?"*), respondida na época renomeando o botão. Agora a
    resposta é melhor: o arquivo sobe direto, e a porta duplicada saiu.

    A função continua existindo — em Configurações e na sincronização
    automática. O que saiu foi o atalho no meio do trabalho fiscal."""
    from app.apps.analisesps import web

    assert not [a for a in web.ACOES_FISCAIS if a["modo"] == "apoios"], (
        "o botão da aba voltou para o meio do trabalho fiscal")


def test_a_planilha_e_lida_EM_FLUXO_e_com_teto():
    """A liberação do `openpyxl` neste arquivo depende das duas coisas: ler em
    fluxo (`read_only`) e recusar arquivo grande ANTES de abrir. Sem elas, um
    relatório gordo abriria inteiro na memória de uma instância que já morreu
    disso em julho de 2026."""
    import inspect

    from app.apps.analisesps import sincronizacao

    fonte = inspect.getsource(sincronizacao._linhas_do_arquivo)
    assert "read_only=True" in fonte
    assert sincronizacao.MAXIMO_RELATORIO <= 25 * 1024 * 1024

    # E o teto é conferido antes de a leitura começar.
    entrada = inspect.getsource(sincronizacao.importar_notas_de_arquivo)
    assert entrada.index("MAXIMO_RELATORIO") < entrada.index("_linhas_do_arquivo")
