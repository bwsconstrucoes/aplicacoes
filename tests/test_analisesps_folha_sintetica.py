# -*- coding: utf-8 -*-
"""
A leitura da Folha Sintética do Fortes — a entrada da folha de pagamento.

⚠️ ESTE É O ARQUIVO QUE DIZ QUANTO CADA PESSOA RECEBE. Um erro aqui não estoura:
ele paga a pessoa errada, paga o valor errado, ou perde uma pessoa em silêncio.
E o formato não ajuda — é um relatório impresso, com cabeçalho repetido a cada
página e grupo de filial atravessando a quebra.

Os casos foram montados com o arquivo REAL de 08/2026 na mão, inclusive a
divergência do rodapé (491 pessoas / 430.129,75 no corpo contra 507 /
445.199,96 declarado).
"""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_sintetica as fs


def cab(pagina=1):
    return [
        ["Folha Sintética - Adiantamento de Folha", "", "", "", f": {pagina}"],
        ["Empresa:", "BWS CONSTRUCOES LTDA  - CNPJ: 00.079.526/0001-09",
         "Fortes Pessoal 8.26.0", "", ""],
        ["Código", "Empregado", "", "", "Líquido"],
    ]


FOLHA_SIMPLES = (
    cab()[:2]
    + [["Mês/Ano: 08/2026", "", "", "", ""]]
    + [cab()[2]]
    + [["001 - CONSTRUTORA", "", "", "", ""],
       ["000013", "GERLANIO GOMES LIMA", "", "", 1198.84],
       ["000387", "LUELIA MADIDA GOMES TOMAS", "", "", 1198.84],
       ["Total: 001 - CONSTRUTORA  ", "", "", "", 2397.68],
       ["090 - OBRA ESTADIOITA CONST ESTADIO ITAITINGA", "", "", "", ""],
       ["000901", "FRANCISCO EDUARDO SILVA COELHO", "", "", 771.42],
       ["Total: 090 - OBRA ESTADIOI", "", "", "", 771.42],
       ["Total: Geral (3 Empregado(s))", "", "", "", 3169.10],
       ["", "", "", "", "Fim"]]
)


def test_le_as_pessoas_o_valor_e_a_filial_de_cada_uma():
    lida = fs.interpretar(FOLHA_SIMPLES)

    assert lida.mes == 8 and lida.ano == 2026
    assert lida.cnpj == "00.079.526/0001-09"
    assert [l.id_fortes for l in lida.linhas] == ["000013", "000387", "000901"]
    assert lida.linhas[0].valor == D("1198.84")
    assert lida.linhas[2].filial_codigo == "090"
    assert lida.linhas[2].filial_nome.startswith("OBRA ESTADIOITA")
    assert lida.total == D("3169.10")


def test_o_ID_FORTES_guarda_os_ZEROS_a_esquerda():
    """⚠️ Os zeros fazem parte do código. Guardar 13 em vez de 000013 obriga todo
    cruzamento a lembrar de completar com zeros — e no dia em que alguém
    esquecer, a pessoa não é achada no cadastro e o pagamento dela SOME, sem
    erro nenhum na tela."""
    lida = fs.interpretar(FOLHA_SIMPLES)
    assert lida.linhas[0].id_fortes == "000013"
    assert all(len(l.id_fortes) == 6 for l in lida.linhas)


def test_o_subtotal_por_filial_e_o_que_o_RELATORIO_declara():
    lida = fs.interpretar(FOLHA_SIMPLES)
    assert set(lida.filiais) == {"001", "090"}
    assert lida.filiais["001"]["total"] == D("2397.68")
    assert lida.total_das_filiais == D("3169.10")
    assert lida.fecha is True


def test_o_rodape_e_guardado_mas_NAO_e_a_verdade():
    """⚠️ DECISÃO DO DONO EM 26/09/2026: *"vamos nos ocupar com as pessoas que
    aparecem no relatório e o valor de cada uma. Se o somatório total final seja
    maior, ignore isso."*

    No arquivo real de 08/2026 o corpo soma 430.129,75 para 491 pessoas e o
    rodapé declara 445.199,96 para 507. O que se paga é o corpo."""
    divergente = list(FOLHA_SIMPLES)
    divergente[-2] = ["Total: Geral (9 Empregado(s))", "", "", "", 9999.99]
    lida = fs.interpretar(divergente)

    assert lida.total == D("3169.10"), "o rodapé virou a verdade"
    assert lida.pessoas_declaradas == 9
    assert lida.total_declarado == D("9999.99")
    # E não conta como "não fecha": quem fecha é linhas x subtotais.
    assert lida.fecha is True


def test_o_que_NAO_fecha_e_linha_contra_subtotal_de_filial():
    """Essa divergência é grave: as duas somas saem do mesmo relatório sobre as
    mesmas linhas. Se diferem, o arquivo chegou truncado ou a leitura errou — e
    nada pode ser pago com ele."""
    quebrado = list(FOLHA_SIMPLES)
    quebrado[7] = ["Total: 001 - CONSTRUTORA  ", "", "", "", 9999.00]
    lida = fs.interpretar(quebrado)
    assert lida.fecha is False


def test_o_cabecalho_REPETIDO_a_cada_pagina_nao_vira_linha():
    """O relatório repete "Folha Sintética", "Empresa:" e "Código|Empregado" em
    cada página. Tratar isso como dado criaria lixo no meio da folha."""
    duas_paginas = (
        FOLHA_SIMPLES[:-2]
        + cab(pagina=2)
        + [["113 - OBRA ESCCAETES", "", "", "", ""],
           ["003337", "JOSE DE CARVALHO DOURADO", "", "", 1024.26],
           ["Total: 113 - OBRA ESCCAETES", "", "", "", 1024.26],
           ["Total: Geral (4 Empregado(s))", "", "", "", 4193.36]]
    )
    lida = fs.interpretar(duas_paginas)
    assert len(lida.linhas) == 4
    assert lida.avisos == [], f"sobrou aviso: {lida.avisos}"
    assert lida.fecha is True


def test_a_filial_que_ATRAVESSA_a_quebra_de_pagina_nao_duplica():
    """⚠️ O caso do arquivo real: as filiais 139, 142, 152, 155, 158, 161, 162,
    164 e 169 aparecem DUAS vezes, porque o grupo atravessou a página. Se cada
    aparição virasse um grupo, o relatório mostraria a mesma obra duas vezes e o
    subtotal de uma delas seria zero."""
    atravessa = (
        FOLHA_SIMPLES[:4]          # só o cabeçalho, sem a filial 001
        + [["139 - OBRA CREPECAMARAGIBE1 CONST CRECHES CAMARAGIBE", "", "", "", ""],
           ["001001", "PRIMEIRA PESSOA", "", "", 100.00]]
        + cab(pagina=2)
        + [["139 - OBRA CREPECAMARAGIBE1 CONST", "", "", "", ""],
           ["001002", "SEGUNDA PESSOA", "", "", 200.00],
           ["Total: 139 - OBRA CREPECAMARAGIBE1", "", "", "", 300.00]]
    )
    lida = fs.interpretar(atravessa)

    assert list(lida.filiais) == ["139"], f"duplicou a filial: {list(lida.filiais)}"
    assert lida.filiais["139"]["total"] == D("300.00")
    # O nome COMPRIDO ganha: a repetição depois da quebra vem cortada.
    assert lida.filiais["139"]["nome"].endswith("CAMARAGIBE")
    assert len(lida.linhas) == 2
    assert all(l.filial_codigo == "139" for l in lida.linhas)
    assert lida.fecha is True


def test_linha_de_empregado_SEM_valor_vira_aviso_e_nao_entra():
    """Entrar com zero seria pior: a pessoa apareceria no relatório como se
    tivesse sido paga.

    ⚠️ ESTE TESTE ACHOU UM DEFEITO DE VERDADE: a procura do valor começava na
    PRIMEIRA célula, e o código do empregado ("000999") lido como número é 999.
    A linha sem valor virava uma pessoa de R$ 999,00 — um número plausível, que
    ninguém questionaria no relatório."""
    sem_valor = list(FOLHA_SIMPLES)
    sem_valor.insert(6, ["000999", "SEM VALOR NENHUM", "", "", ""])
    lida = fs.interpretar(sem_valor)

    assert "000999" not in [l.id_fortes for l in lida.linhas]
    assert any("000999" in a for a in lida.avisos)


def test_o_rodape_de_pagina_do_fortes_nao_gera_aviso():
    """⚠️ CONFERIDO NO ARQUIVO REAL: o Fortes põe "Continua..." no fim de cada
    página e "Fim" na última, na ÚLTIMA célula, com a primeira vazia. Sem
    reconhecer os dois, o arquivo de 08/2026 gerava ONZE avisos de "linha que
    não reconheci" — e aviso que aparece sempre é aviso que ninguém lê, o que
    estraga os avisos de verdade."""
    com_rodape = list(FOLHA_SIMPLES)
    com_rodape.insert(7, ["", "", "", "", "Continua..."])
    lida = fs.interpretar(com_rodape)
    assert lida.avisos == [], f"sobrou aviso: {lida.avisos}"
    assert len(lida.linhas) == 3


@pytest.mark.parametrize("arquivo", [
    "/root/.claude/uploads/aacbc884-22a3-5149-a9c7-9fa4ed1dc760/"
    "a68c9072-Folha_Sint_tica_-_Adiantamento_de_Folha.xls"])
def test_o_arquivo_REAL_de_08_2026_e_lido_inteiro(arquivo):
    """⚠️ A ÚNICA prova que vale: o arquivo que a contabilidade mandou de
    verdade. Os números foram conferidos à mão antes de entrar aqui.

    Pulado quando o arquivo não está na máquina (ele veio por anexo, não fica no
    repositório: é folha de pagamento, com nome e valor de 491 pessoas)."""
    import pathlib
    if not pathlib.Path(arquivo).exists():
        pytest.skip("o arquivo real não está nesta máquina")

    lida = fs.ler(pathlib.Path(arquivo).read_bytes())

    assert (lida.mes, lida.ano) == (8, 2026)
    assert lida.cnpj == "00.079.526/0001-09"
    assert len(lida.linhas) == 491
    assert lida.total == D("430129.75")
    assert len(lida.filiais) == 47
    assert lida.total_das_filiais == D("430129.75")
    assert lida.fecha is True
    # O rodapé divergente, guardado e ignorado — ver a decisão do dono.
    assert lida.pessoas_declaradas == 507
    assert lida.total_declarado == D("445199.96")
    assert lida.avisos == [], f"avisos no arquivo real: {lida.avisos[:5]}"
    assert lida.linhas[0].id_fortes == "000013"
    assert lida.linhas[-1].id_fortes == "004001"
    # Nenhuma filial sem nome, mesmo as nove que atravessam quebra de página.
    assert all(f["nome"] for f in lida.filiais.values())


def test_linha_estranha_NAO_e_engolida_em_silencio():
    """Formato novo do Fortes, linha de aviso, rodapé diferente — qualquer coisa
    que eu não reconheça tem de aparecer na tela. Engolir é como se perde
    dinheiro sem ninguém ver."""
    com_lixo = list(FOLHA_SIMPLES)
    com_lixo.insert(6, ["OBSERVAÇÃO: conferir com o RH", "", "", "", ""])
    lida = fs.interpretar(com_lixo)
    assert any("OBSERVAÇÃO" in a for a in lida.avisos)


def test_arquivo_vazio_e_arquivo_que_nao_e_folha_respondem_frase():
    with pytest.raises(fs.ErroDaFolha) as erro:
        fs.ler(b"")
    assert "vazio" in str(erro.value)

    with pytest.raises(fs.ErroDaFolha) as erro:
        fs.ler(b"isto nao e um xls")
    assert "Excel antigo" in str(erro.value)


def test_sem_MES_ANO_o_arquivo_e_recusado():
    """Sem competência não dá para saber a qual folha ele pertence — e gravar
    numa competência chutada mistura duas quinzenas."""
    sem_competencia = [l for l in FOLHA_SIMPLES
                       if not str(l[0]).startswith("Mês/Ano")]
    lida = fs.interpretar(sem_competencia)
    assert lida.mes is None
    # `ler` é quem recusa, porque é ele que fala com a tela.
    import app.apps.analisesps.folha_sintetica as mod
    original = mod.interpretar
    try:
        mod.interpretar = lambda linhas: lida
        with pytest.raises(fs.ErroDaFolha) as erro:
            mod.ler(b"\xd0\xcf\x11\xe0qualquer-coisa")
        assert "Mês/Ano" in str(erro.value) or "Excel antigo" in str(erro.value)
    finally:
        mod.interpretar = original


def test_o_valor_aceita_numero_de_celula_e_texto_em_portugues():
    """O Fortes grava número de verdade, mas um arquivo reexportado pode vir com
    texto. `1.198,84` não pode virar 1,19."""
    assert fs._numero(1198.84) == D("1198.84")
    assert fs._numero("1.198,84") == D("1198.84")
    assert fs._numero("") is None
    assert fs._numero(None) is None


# ---------------------------------------------------------------------------
# QUINZENA OU FIM DE MÊS — 26/09/2026
#
# Pergunta do dono: *"como é que a gente vai saber se a gente está tratando de
# quinzena, se está tratando de fim de mês (…) de qual arquivo é aquele dali que
# a gente está tratando."*
# ---------------------------------------------------------------------------
def test_o_titulo_ADIANTAMENTO_sugere_quinzena():
    """O arquivo real de 08/2026 se chama "Folha Sintética - Adiantamento de
    Folha", e adiantamento é a quinzena (dias 1 a 15)."""
    assert fs.tipo_sugerido("Folha Sintética - Adiantamento de Folha") == "quinzena"
    assert fs.interpretar(FOLHA_SIMPLES).tipo_sugerido == "quinzena"


def test_titulo_que_NAO_deixa_claro_devolve_VAZIO_para_a_tela_perguntar():
    """⚠️ A RESPOSTA MAIS IMPORTANTE DAS TRÊS. Eu não conheço o título do
    relatório de fim de mês — nunca vi um. Chutar "fim de mês" só porque não é
    adiantamento faria ler o pedaço errado do ponto (16 ao fim em vez de 1 a 15),
    e o erro sairia como um valor plausível, na obra errada."""
    assert fs.tipo_sugerido("Folha Sintética") == ""
    assert fs.tipo_sugerido("Relatório de Pagamento") == ""
    assert fs.tipo_sugerido("") == ""


@pytest.mark.parametrize("titulo", [
    # ⚠️ O TÍTULO DE VERDADE, colado pelo dono em 26/09/2026.
    "Folha Sintética - Folha de Pagamento",
    "Folha Sintética - Fim de Mês", "Folha Sintética - Fechamento",
    "Folha Mensal Sintética",
])
def test_titulos_de_FECHAMENTO_sugerem_fim_de_mes(titulo):
    """Os dois títulos reais são:

        quinzena   → "Folha Sintética - Adiantamento de Folha"
        fim de mês → "Folha Sintética - Folha de Pagamento"
    """
    assert fs.tipo_sugerido(titulo) == "fim_de_mes"


def test_o_ADIANTAMENTO_ganha_do_titulo_generico_de_fim_de_mes():
    """O título de fim de mês é o mais genérico dos dois ("Folha de Pagamento"),
    então a ordem da conferência importa: adiantamento é testado primeiro."""
    assert fs.tipo_sugerido(
        "Folha Sintética - Adiantamento - Folha de Pagamento") == "quinzena"


# ---------------------------------------------------------------------------
# O VALOR — o mesmo relatório manda em formatos diferentes
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bruto,esperado", [
    # Os dois formatos que o dono colou da folha de FIM DE MÊS (26/09/2026).
    ("1.074,64", "1074.64"),
    ("1362,56", "1362.56"),
    # O que vem do arquivo de adiantamento: número de verdade na célula.
    (1198.84, "1198.84"),
    # ⚠️ A ARMADILHA DE CEM VEZES: texto com ponto DECIMAL, que sai de uma
    # reexportação. A leitura antiga apagava todo ponto e isto virava 119884 —
    # e a conferência de fechamento NÃO pegaria, porque o total da filial vem no
    # mesmo formato e inflaria igual. A tela diria "fecha" com todo mundo
    # recebendo cem vezes mais.
    ("1198.84", "1198.84"),
    # Ponto como milhar, sem centavo.
    ("1.074", "1074.00"),
    ("1.234.567,89", "1234567.89"),
    ("119.884,00", "119884.00"),
    ("R$ 958,90", "958.90"),
    # Negativo, nos dois jeitos que relatório usa.
    ("-50,00", "-50.00"),
    ("(100,00)", "-100.00"),
])
def test_o_valor_e_lido_em_qualquer_formato_do_fortes(bruto, esperado):
    from decimal import Decimal
    assert fs._numero(bruto) == Decimal(esperado), bruto


def test_a_folha_de_FIM_DE_MES_do_dono_e_lida_certo():
    """As linhas que ele colou, com os dois formatos na mesma folha."""
    from decimal import Decimal
    linhas = [
        ["Folha Sintética - Folha de Pagamento", "", "", "", ": 1"],
        ["Empresa:", "BWS CONSTRUCOES LTDA  - CNPJ: 00.079.526/0001-09",
         "Fortes Pessoal 8.27.1", "", ""],
        ["Mês/Ano: 08/2026", "", "", "", ""],
        ["Código", "Empregado", "", "", "Líquido"],
        ["001 - CONSTRUTORA", "", "", "", ""],
        ["000013", "GERLANIO GOMES LIMA", "", "", "1.074,64"],
        ["000387", "LUELIA MADIDA GOMES TOMAS", "", "", "1362,56"],
    ]
    lida = fs.interpretar(linhas)

    assert lida.tipo_sugerido == "fim_de_mes"
    assert lida.competencia == "08/2026"
    assert [l.valor for l in lida.linhas] == [Decimal("1074.64"),
                                              Decimal("1362.56")]
    assert lida.total == Decimal("2437.20")
    assert lida.avisos == [], f"avisos: {lida.avisos}"


def test_a_competencia_sai_pronta_para_a_tela():
    assert fs.interpretar(FOLHA_SIMPLES).competencia == "08/2026"
    sem = fs.interpretar([l for l in FOLHA_SIMPLES
                          if not str(l[0]).startswith("Mês/Ano")])
    assert sem.competencia == ""
