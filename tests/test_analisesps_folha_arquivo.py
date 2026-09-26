# -*- coding: utf-8 -*-
"""
A folha importada, guardada no banco — 27/09/2026, com banco de verdade.

⚠️ POR QUE COM BANCO: tudo o que importa aqui é constraint e transação, e o
dublê da suíte não alcança nenhum dos dois.

  - a unicidade por competência + tipo é o que faz REIMPORTAR SUBSTITUIR em vez
    de acumular. Duas folhas de 08/2026 quinzena deixariam qualquer total
    ambíguo, e ninguém saberia qual é a boa;
  - o `NUMERIC(14,2)` é o que impede o arredondamento binário do float — folha
    errada em centavo é folha errada;
  - o `ON DELETE CASCADE` é o que leva as pessoas junto quando a folha sai;
  - e a transação é o que impede uma folha existir com a antiga já apagada e a
    nova não gravada — que mostraria total zero como se fosse verdade.
"""
from decimal import Decimal

import pathlib

import pytest

pytestmark = pytest.mark.banco

ARQUIVO_REAL = (
    "/root/.claude/uploads/aacbc884-22a3-5149-a9c7-9fa4ed1dc760/"
    "a68c9072-Folha_Sint_tica_-_Adiantamento_de_Folha.xls")


@pytest.fixture
def banco_folha_arquivo(banco, monkeypatch):
    """Sobe o schema pelas migrações de verdade — as mesmas que o botão aplica."""
    from sqlalchemy import text

    from app.apps.analisesps import db as db_analisesps

    url = str(banco.url.render_as_string(hide_password=False))
    monkeypatch.setenv("DATABASE_URL", url)
    db_analisesps._engine = None

    pasta = pathlib.Path(db_analisesps.__file__).parent / "migracoes"
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        for caminho in sorted(pasta.glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        conn.commit()
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.commit()
    db_analisesps._engine = None


# ---------------------------------------------------------------------------
# Uma folha de mentira, montada linha a linha — para não depender do anexo
# ---------------------------------------------------------------------------
def folha_falsa(titulo="Folha Sintética - Folha de Pagamento", mes=8, ano=2026,
                pessoas=(("000013", "GERLANIO GOMES LIMA", "1.074,64"),
                         ("000387", "LUELIA MADIDA GOMES TOMAS", "1362,56"))):
    """As linhas cruas, como o `xlrd` as entrega. Ver `folha_sintetica`."""
    total = 0
    for _c, _n, v in pessoas:
        total += float(v.replace(".", "").replace(",", "."))
    linhas = [
        [titulo],
        # ⚠️ O FORMATO É O DO FORTES, conferido em `folha_sintetica`: "Empresa:"
        # é a PRIMEIRA célula, e o nome com o CNPJ vêm nas seguintes — o leitor
        # junta `celulas[1:]` e procura o CNPJ dentro. Montar o dublê com uma
        # célula só faria o teste medir a minha suposição, não o leitor.
        ["Empresa:", "BWS CONSTRUCOES LTDA", "CNPJ: 00.079.526/0001-09"],
        [f"Mês/Ano: {mes:02d}/{ano}"],
        ["001 - MATRIZ"],
        ["Código", "Nome", "", "", "Líquido"],
    ]
    for codigo, nome, valor in pessoas:
        linhas.append([codigo, nome, "", "", valor])
    linhas.append(["", "", "", "Total:", f"{total:.2f}".replace(".", ",")])
    return linhas


def guardar(monkeypatch, **extra):
    """Importa uma folha de mentira, driblando a leitura do `.xls`."""
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs

    linhas = extra.pop("linhas", None) or folha_falsa(**extra)
    monkeypatch.setattr(fs, "ler", lambda conteudo: fs.interpretar(linhas))
    return fa.importar(b"nao-e-um-xls-de-verdade",
                       nome_do_arquivo="folha.xls", quem="MARCELO")


def test_a_migracao_029_roda_no_postgres(banco_folha_arquivo):
    from app.apps.analisesps import folha_arquivo as fa
    assert fa._pronto() is True
    assert fa.listar() == []


def test_importar_guarda_a_folha_e_as_pessoas(banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa

    resultado = guardar(monkeypatch)

    assert resultado["ano"] == 2026
    assert resultado["mes"] == 8
    assert resultado["tipo"] == "fim_de_mes"
    assert resultado["pessoas"] == 2
    assert resultado["total"] == Decimal("2437.20")
    assert resultado["substituiu"] is False

    folha = fa.abrir(resultado["id"])
    assert folha["competencia"] == "08/2026"
    # `empresa` guarda a linha inteira do cabeçalho, com o CNPJ dentro — é como
    # `folha_sintetica` já entregava antes desta tabela existir. Quem precisa do
    # CNPJ limpo usa o campo próprio, que é extraído dali.
    assert folha["empresa"].startswith("BWS CONSTRUCOES LTDA")
    assert folha["cnpj"] == "00.079.526/0001-09"
    assert [l["nome"] for l in folha["linhas"]] == [
        "GERLANIO GOMES LIMA", "LUELIA MADIDA GOMES TOMAS"]


def test_o_valor_volta_EXATO_do_banco(banco_folha_arquivo, monkeypatch):
    """NUMERIC, não float. 1.074,64 em float não é 1074.64, e a folha fecharia
    com centavo de diferença sem ninguém saber de onde veio."""
    from app.apps.analisesps import folha_arquivo as fa

    resultado = guardar(monkeypatch)
    linhas = fa.abrir(resultado["id"])["linhas"]
    por_nome = {l["nome"]: l["valor"] for l in linhas}
    assert por_nome["GERLANIO GOMES LIMA"] == Decimal("1074.64")
    assert por_nome["LUELIA MADIDA GOMES TOMAS"] == Decimal("1362.56")


def test_o_ID_FORTES_guarda_os_ZEROS_da_frente(banco_folha_arquivo, monkeypatch):
    """⚠️ "000013" como número viraria 13, e o casamento com o cadastro deixaria
    de funcionar. É o mesmo cuidado que já custou um defeito na leitura."""
    from app.apps.analisesps import folha_arquivo as fa

    resultado = guardar(monkeypatch)
    codigos = {l["id_fortes"] for l in fa.abrir(resultado["id"])["linhas"]}
    assert codigos == {"000013", "000387"}


def test_reimportar_a_MESMA_competencia_SUBSTITUI(banco_folha_arquivo, monkeypatch):
    """⚠️ É a constraint que o dublê da suíte não alcança. Duas folhas de 08/2026
    quinzena deixariam qualquer total ambíguo."""
    from app.apps.analisesps import folha_arquivo as fa

    guardar(monkeypatch)
    segunda = guardar(monkeypatch, pessoas=(
        ("000013", "GERLANIO GOMES LIMA", "2.000,00"),))

    assert segunda["substituiu"] is True
    assert len(fa.listar()) == 1
    folha = fa.abrir(segunda["id"])
    assert folha["total"] == Decimal("2000.00")
    assert len(folha["linhas"]) == 1, "as linhas antigas foram junto"


def test_quinzena_e_fim_de_mes_do_MESMO_mes_convivem(banco_folha_arquivo, monkeypatch):
    """São duas folhas diferentes do mesmo mês — a unicidade é por competência
    E tipo."""
    from app.apps.analisesps import folha_arquivo as fa

    guardar(monkeypatch, titulo="Folha Sintética - Adiantamento de Folha")
    guardar(monkeypatch, titulo="Folha Sintética - Folha de Pagamento")

    tipos = {f["tipo"] for f in fa.listar()}
    assert tipos == {"quinzena", "fim_de_mes"}
    assert len(fa.listar()) == 2


def test_o_titulo_decide_o_tipo_e_ADIANTAMENTO_e_quinzena(banco_folha_arquivo,
                                                          monkeypatch):
    resultado = guardar(monkeypatch,
                        titulo="Folha Sintética - Adiantamento de Folha")
    assert resultado["tipo"] == "quinzena"


def test_titulo_que_NAO_DIZ_o_tipo_e_recusado_pedindo_para_escolher(
        banco_folha_arquivo, monkeypatch):
    """⚠️ Adivinhar o tipo erraria o PERÍODO DO PONTO, e o período errado
    apropria os dias errados nas obras. Perguntar é a resposta certa."""
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs

    linhas = folha_falsa(titulo="Relatório qualquer")
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))
    with pytest.raises(fa.ErroDaImportacao) as erro:
        fa.importar(b"x", tipo="")
    frase = str(erro.value)
    assert "QUINZENA" in frase and "FIM DE MÊS" in frase
    assert "Escolha na tela" in frase


def test_o_tipo_ESCOLHIDO_NA_TELA_vale_quando_o_titulo_nao_diz(
        banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs

    linhas = folha_falsa(titulo="Relatório qualquer")
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))
    resultado = fa.importar(b"x", tipo="quinzena", quem="EU")
    assert resultado["tipo"] == "quinzena"


def test_folha_que_NAO_FECHA_e_importada_COM_AVISO(banco_folha_arquivo, monkeypatch):
    """⚠️ Não fechar é aviso, não é recusa. O dono precisa importar a folha que
    não fecha para DESCOBRIR por que não fecha — recusar deixaria o arquivo do
    lado de fora, onde ninguém investiga."""
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs

    linhas = folha_falsa()
    # Mexe no subtotal da filial para as somas discordarem.
    linhas[-1] = ["", "", "", "Total:", "9.999,99"]
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))

    resultado = fa.importar(b"x", quem="EU")
    assert resultado["fecha"] is False
    assert resultado["avisos"], "tem de vir aviso"
    folha = fa.abrir(resultado["id"])
    assert folha["lista_de_avisos"]
    assert folha["fecha"] is False


def test_arquivo_SEM_NINGUEM_e_recusado(banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs

    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar([["Só o título"]]))
    with pytest.raises(fa.ErroDaImportacao) as erro:
        fa.importar(b"x")
    assert "Folha Sintética" in str(erro.value)


def test_arquivo_VAZIO_e_arquivo_GRANDE_demais_sao_recusados(
        banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa

    with pytest.raises(fa.ErroDaImportacao) as erro:
        fa.importar(b"")
    assert "vazio" in str(erro.value)

    monkeypatch.setattr(fa, "MAXIMO_DO_ARQUIVO", 10)
    with pytest.raises(fa.ErroDaImportacao) as erro:
        fa.importar(b"x" * 100)
    assert "teto" in str(erro.value)


def test_os_totais_por_filial_saem_do_MAIOR_para_o_menor(banco_folha_arquivo,
                                                         monkeypatch):
    """É o primeiro corte do painel que ele pediu: *"saber qual é o total por
    obra, porque isso já ajuda nessa questão do rateio"*. Por obra depende da
    apropriação; por filial é o que o arquivo traz."""
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs

    linhas = [
        ["Folha Sintética - Folha de Pagamento"],
        ["Empresa:", "BWS", "CNPJ: 00.079.526/0001-09"], ["Mês/Ano: 08/2026"],
        ["001 - MATRIZ"],
        ["Código", "Nome", "", "", "Líquido"],
        ["000001", "PEQUENA", "", "", "100,00"],
        ["", "", "", "Total:", "100,00"],
        ["002 - FILIAL GRANDE"],
        ["Código", "Nome", "", "", "Líquido"],
        ["000002", "GRANDE UM", "", "", "900,00"],
        ["000003", "GRANDE DOIS", "", "", "500,00"],
        ["", "", "", "Total:", "1.400,00"],
    ]
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))
    resultado = fa.importar(b"x", quem="EU")

    totais = fa.totais_por_filial(resultado["id"])
    assert [t["codigo"] for t in totais] == ["002", "001"]
    assert totais[0]["total"] == Decimal("1400.00")
    assert totais[0]["pessoas"] == 2
    assert totais[1]["total"] == Decimal("100.00")


def test_apagar_leva_as_pessoas_junto(banco_folha_arquivo, monkeypatch):
    """O CASCADE. Sem ele sobrariam linhas órfãs, que somariam num total de
    ninguém."""
    from app.apps.analisesps import folha_arquivo as fa
    from app.apps.analisesps.db import consultar_um

    resultado = guardar(monkeypatch)
    assert fa.apagar(resultado["id"], quem="EU") is True
    assert fa.abrir(resultado["id"]) is None
    assert fa.listar() == []
    sobrou = consultar_um("SELECT count(*) FROM analisesps.folha_linha")
    assert sobrou[0] == 0


def test_apagar_o_que_nao_existe_devolve_falso_sem_estourar(banco_folha_arquivo):
    from app.apps.analisesps import folha_arquivo as fa
    assert fa.apagar(99999, quem="EU") is False


def test_a_lista_vem_da_mais_RECENTE_para_a_mais_antiga(banco_folha_arquivo,
                                                        monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa

    guardar(monkeypatch, mes=7, ano=2026)
    guardar(monkeypatch, mes=12, ano=2025)
    guardar(monkeypatch, mes=9, ano=2026)

    assert [(f["mes"], f["ano"]) for f in fa.listar()] == [
        (9, 2026), (7, 2026), (12, 2025)]


def test_a_IMPRESSAO_do_arquivo_distingue_conteudo_de_competencia(
        banco_folha_arquivo, monkeypatch):
    """"É o mesmo mês" e "é o mesmo arquivo" são perguntas diferentes: o dono
    pode mandar o arquivo CORRIGIDO da mesma competência."""
    from app.apps.analisesps import folha_arquivo as fa

    assert fa.impressao_do_arquivo(b"um") != fa.impressao_do_arquivo(b"outro")
    assert fa.impressao_do_arquivo(b"um") == fa.impressao_do_arquivo(b"um")

    resultado = guardar(monkeypatch)
    assert fa.abrir(resultado["id"])["impressao"]


def test_quem_importou_fica_registrado(banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa
    resultado = guardar(monkeypatch)
    assert fa.abrir(resultado["id"])["importado_por"] == "MARCELO"


def test_o_arquivo_REAL_da_contabilidade_e_guardado_inteiro(banco_folha_arquivo):
    """⚠️ A única prova que vale: o arquivo que a contabilidade mandou de
    verdade, do começo ao banco. Pulado quando o anexo não está nesta máquina —
    ele não fica no repositório, porque é folha de pagamento com nome e valor de
    491 pessoas."""
    from app.apps.analisesps import folha_arquivo as fa

    if not pathlib.Path(ARQUIVO_REAL).exists():
        pytest.skip("o arquivo real não está nesta máquina")

    resultado = fa.importar(pathlib.Path(ARQUIVO_REAL).read_bytes(),
                            nome_do_arquivo="Folha Sintética.xls", quem="MARCELO")

    assert resultado["pessoas"] == 491
    assert resultado["total"] == Decimal("430129.75")
    assert resultado["tipo"] == "quinzena", "é o Adiantamento de Folha"
    assert (resultado["mes"], resultado["ano"]) == (8, 2026)

    folha = fa.abrir(resultado["id"])
    assert len(folha["linhas"]) == 491
    # A soma do que foi GRAVADO tem de bater com o total — se o banco perdesse
    # uma linha, é aqui que apareceria.
    assert sum((l["valor"] for l in folha["linhas"]), Decimal("0")) == \
        Decimal("430129.75")

    filiais = fa.totais_por_filial(resultado["id"])
    assert len(filiais) == 47
    assert sum((f["total"] for f in filiais), Decimal("0")) == Decimal("430129.75")
