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
import datetime as dt
import os
import pathlib
from decimal import Decimal

import pytest

pytestmark = pytest.mark.banco

ARQUIVO_REAL = (
    "/root/.claude/uploads/aacbc884-22a3-5149-a9c7-9fa4ed1dc760/"
    "a68c9072-Folha_Sint_tica_-_Adiantamento_de_Folha.xls")


@pytest.fixture
def banco_folha_arquivo(banco_analisesps):
    """O schema `analisesps` limpo para este teste.

    ⚠️ ERA UM REFAZ-TUDO: `DROP SCHEMA` mais os 36 arquivos de migração, A CADA
    TESTE. Com 727 testes de banco na suíte, isso dava ~26 mil execuções de
    arquivo SQL por rodada para construir sempre a mesma coisa — e era a maior
    conta do tempo que o dono cobrou em 29/09/2026.

    Agora o schema nasce uma vez por sessão e as tabelas são esvaziadas entre os
    testes. O isolamento é o mesmo: tabelas vazias e contadores de `id` zerados.
    Ver `banco_analisesps` no `conftest.py`."""

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

    # `os.path.isfile`, e não `Path.exists()`: no GitHub a pasta do anexo nem
    # pode ser lida, e `exists()` levanta PermissionError em vez de dizer não.
    if not os.path.isfile(ARQUIVO_REAL):
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


# ---------------------------------------------------------------------------
# CASAR A FOLHA COM AS PESSOAS, E AS CRÍTICAS — 27/09/2026
#
# ⚠️ É O PASSO QUE FAZ A FOLHA CONVERSAR COM O RESTO. A Folha Sintética traz
# código e nome; o ponto, o cadastro, o rateio e o pagamento são por CPF.
# ---------------------------------------------------------------------------
def cadastrar(*pessoas):
    """Põe gente no cadastro, já com o código do Fortes."""
    from app.apps.analisesps import colaboradores as col
    from app.apps.analisesps.db import conexao

    registros = []
    for cpf, nome, id_fortes, extra in pessoas:
        r = {c: "" for c in col.CAMPOS}
        r.update({"cpf": cpf, "nome": nome})
        # Toda data e todo número vazio é None, e a lista vem do módulo — ver o
        # comentário igual em test_analisesps_colaboradores_banco.py: um ajudante
        # que enumera os campos à mão quebra na próxima coluna de data que entrar.
        for campo in col.DATAS + col.NUMEROS:
            r[campo] = None
        r.update(extra or {})
        registros.append((r, id_fortes))
    with conexao() as conn:
        col._gravar(conn, [r for r, _ in registros])
        for r, id_fortes in registros:
            if id_fortes:
                conn.execute(
                    "UPDATE analisesps.colaborador SET id_fortes = ? "
                    " WHERE cpf = ?", (id_fortes, r["cpf"]))
        conn.commit()


def test_a_folha_casa_com_o_cadastro_PELO_CODIGO(banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO DO CADASTRO", "000013", None),
              ("03513441363", "LUELIA DO CADASTRO", "000387", None))
    resultado = guardar(monkeypatch)

    assert resultado["casadas"] == 2
    assert resultado["pendentes"] == 0
    cpfs = {l["cpf"] for l in fa.abrir(resultado["id"])["linhas"]}
    assert cpfs == {"99713349334", "03513441363"}


def test_quem_NAO_tem_codigo_no_cadastro_fica_PENDENTE_e_VISIVEL(
        banco_folha_arquivo, monkeypatch):
    """⚠️ Correção do dono em 26/09/2026: *"pessoas sem ID Fortes no cadastro não
    entram. Na verdade ela vai entrar após tratamento (…) não pode ficar oculto,
    escondido."*

    Lista à parte é lista que alguém esquece de abrir — e aí a pessoa desaparece
    da folha: trabalhou e não recebeu, sem nada na tela gritando."""
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013", None))   # só um dos dois
    resultado = guardar(monkeypatch)

    assert resultado["casadas"] == 1
    assert resultado["pendentes"] == 1

    # A pessoa CONTINUA na folha, com o valor dela.
    folha = fa.abrir(resultado["id"])
    assert len(folha["linhas"]) == 2
    assert folha["total"] == Decimal("2437.20")

    criticas = fa.criticas(resultado["id"])
    assert [p["nome"] for p in criticas["pendentes"]] == \
        ["LUELIA MADIDA GOMES TOMAS"]
    assert criticas["total_pendente"] == Decimal("1362.56")


def test_a_folha_NAO_casa_por_NOME(banco_folha_arquivo, monkeypatch):
    """⚠️ As planilhas cruzam por nome hoje, e é frágil: dois "JOSE DA SILVA", um
    acento diferente, um nome do meio abreviado — e o salário vai para a pessoa
    errada. Aqui, sem código, fica pendente em vez de casar com um parecido."""
    from app.apps.analisesps import folha_arquivo as fa

    # Mesmo nome exato da folha, mas SEM o código do Fortes.
    cadastrar(("99713349334", "GERLANIO GOMES LIMA", "", None))
    resultado = guardar(monkeypatch)

    assert resultado["casadas"] == 0
    assert resultado["pendentes"] == 2
    assert all(not l["cpf"] for l in fa.abrir(resultado["id"])["linhas"])


def test_casar_DE_NOVO_resolve_quem_passou_a_ter_codigo(banco_folha_arquivo,
                                                        monkeypatch):
    """O cadastro pode ser atualizado depois da importação. Aí gente que estava
    pendente passa a casar, sem ninguém reimportar nada."""
    from app.apps.analisesps import folha_arquivo as fa

    resultado = guardar(monkeypatch)
    assert resultado["pendentes"] == 2

    cadastrar(("99713349334", "GERLANIO", "000013", None),
              ("03513441363", "LUELIA", "000387", None))
    assert fa.casar_com_o_cadastro(resultado["id"]) == {"casadas": 2,
                                                       "pendentes": 0}


def test_sem_de_para_nenhum_o_casamento_NAO_apaga_o_que_ja_casou(
        banco_folha_arquivo, monkeypatch):
    """Perder o CPF já resolvido por causa de uma planilha fora do ar faria a
    folha inteira virar pendente de uma hora para outra."""
    from app.apps.analisesps import colaboradores as col, folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013", None))
    resultado = guardar(monkeypatch)
    assert resultado["casadas"] == 1

    monkeypatch.setattr(col, "de_para_do_fortes", lambda: {})
    fa.casar_com_o_cadastro(resultado["id"])
    cpfs = [l["cpf"] for l in fa.abrir(resultado["id"])["linhas"] if l["cpf"]]
    assert cpfs == ["99713349334"], "o que já estava casado tem de continuar"


def test_quem_JA_SAIU_aparece_na_critica_da_folha(banco_folha_arquivo, monkeypatch):
    """⚠️ *"Não podemos pagar salário ou diárias pra quem saiu."* Esta é a
    crítica aplicada à folha de verdade."""
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013",
               {"data_saida": dt.date(2026, 7, 31)}),
              ("03513441363", "LUELIA", "000387", None))
    resultado = guardar(monkeypatch)      # folha de 08/2026, fim de mês

    criticas = fa.criticas(resultado["id"])
    assert [p["nome"] for p in criticas["sairam"]] == ["GERLANIO"]
    assert criticas["total_de_quem_saiu"] == Decimal("1074.64")
    assert "Não pague" in criticas["sairam"][0]["motivo"]
    assert criticas["saindo"] == []


def test_quem_esta_SAINDO_e_aviso_e_nao_entra_como_quem_saiu(banco_folha_arquivo,
                                                            monkeypatch):
    """Pode haver valor devido até o último dia: é conferência, não trava."""
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013",
               {"aviso_previo": dt.date(2026, 8, 20)}),
              ("03513441363", "LUELIA", "000387", None))
    resultado = guardar(monkeypatch)

    criticas = fa.criticas(resultado["id"])
    assert [p["nome"] for p in criticas["saindo"]] == ["GERLANIO"]
    assert criticas["sairam"] == []


def test_o_PERIODO_DA_FOLHA_decide_quem_saiu_e_quem_esta_saindo(
        banco_folha_arquivo, monkeypatch):
    """⚠️ A data que manda é o FIM DO PERÍODO da folha, não hoje. Quem saiu no
    dia 20 trabalhou a quinzena (1 a 15) inteira e RECEBE; na folha de fim de mês
    (16 ao último dia) do mesmo mês, não."""
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013",
               {"data_saida": dt.date(2026, 8, 20)}))

    quinzena = guardar(monkeypatch,
                       titulo="Folha Sintética - Adiantamento de Folha",
                       pessoas=(("000013", "GERLANIO GOMES LIMA", "500,00"),))
    fim = guardar(monkeypatch, titulo="Folha Sintética - Folha de Pagamento",
                  pessoas=(("000013", "GERLANIO GOMES LIMA", "500,00"),))

    # Na quinzena (até 15/08) ela ainda não tinha saído: é "está saindo".
    assert [p["nome"] for p in fa.criticas(quinzena["id"])["saindo"]] == ["GERLANIO"]
    assert fa.criticas(quinzena["id"])["sairam"] == []
    # No fim do mês (até 31/08) ela já saiu.
    assert [p["nome"] for p in fa.criticas(fim["id"])["sairam"]] == ["GERLANIO"]


def test_folha_toda_certa_nao_gera_critica_nenhuma(banco_folha_arquivo, monkeypatch):
    """⚠️ Se o caso normal gerasse crítica, a crítica viraria ruído — e ruído faz
    ignorar o aviso que importa."""
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013",
               {"fase": "Colaboradores Ativos"}),
              ("03513441363", "LUELIA", "000387",
               {"fase": "Colaboradores Ativos"}))
    resultado = guardar(monkeypatch)

    criticas = fa.criticas(resultado["id"])
    assert criticas["pendentes"] == []
    assert criticas["sairam"] == []
    assert criticas["saindo"] == []
    assert criticas["total_pendente"] == 0


def test_a_critica_traz_o_link_do_card_para_resolver(banco_folha_arquivo,
                                                     monkeypatch):
    """O lugar de tratar é o card do Pipefy. A crítica que não leva até lá deixa
    a pessoa procurando."""
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013",
               {"data_saida": dt.date(2026, 7, 31), "card_pipefy": "778899"}))
    resultado = guardar(monkeypatch,
                        pessoas=(("000013", "GERLANIO GOMES LIMA", "100,00"),))
    achado = fa.criticas(resultado["id"])["sairam"][0]
    assert achado["link_pipefy"].endswith("778899")


def test_criticas_de_folha_que_nao_existe_nao_estoura(banco_folha_arquivo):
    from app.apps.analisesps import folha_arquivo as fa
    assert fa.criticas(99999)["pendentes"] == []


# ---------------------------------------------------------------------------
# O PANORAMA, com banco de verdade
# ---------------------------------------------------------------------------
def test_o_panorama_soma_TODAS_as_folhas_importadas(banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa

    guardar(monkeypatch, mes=8, titulo="Folha Sintética - Adiantamento de Folha")
    guardar(monkeypatch, mes=8, titulo="Folha Sintética - Folha de Pagamento")

    panorama = fa.panorama()
    assert panorama["pronto"] is True
    assert len(panorama["folhas"]) == 2
    assert panorama["total"] == Decimal("4874.40")      # 2.437,20 × 2
    assert panorama["pessoas"] == 4
    assert panorama["competencias"] == 1, "as duas são de 08/2026"


def test_o_panorama_conta_quem_NAO_casou(banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa

    cadastrar(("99713349334", "GERLANIO", "000013", None))
    guardar(monkeypatch)
    assert fa.panorama()["pendentes"] == 1


def test_o_panorama_por_filial_vem_do_MAIOR_para_o_menor(banco_folha_arquivo,
                                                         monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa, folha_sintetica as fs

    linhas = [
        ["Folha Sintética - Folha de Pagamento"],
        ["Empresa:", "BWS", "CNPJ: 00.079.526/0001-09"], ["Mês/Ano: 08/2026"],
        ["001 - MATRIZ"], ["Código", "Nome", "", "", "Líquido"],
        ["000001", "PEQUENA", "", "", "100,00"],
        ["", "", "", "Total:", "100,00"],
        ["002 - GRANDE"], ["Código", "Nome", "", "", "Líquido"],
        ["000002", "GRANDE UM", "", "", "900,00"],
        ["", "", "", "Total:", "900,00"],
    ]
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))
    fa.importar(b"x", quem="EU")

    filiais = fa.panorama()["por_filial"]
    assert [f["codigo"] for f in filiais] == ["002", "001"]


def test_o_panorama_vazio_nao_estoura(banco_folha_arquivo):
    from app.apps.analisesps import folha_arquivo as fa
    panorama = fa.panorama()
    assert panorama["pronto"] is True
    assert panorama["folhas"] == []
    assert panorama["total"] == Decimal("0")


# ---------------------------------------------------------------------------
# 30/09/2026 — O SETOR DO FORTES, GUARDADO (migração 038)
#
# Ele: *"vamos guardar essa informação e expor ela em tela"* — o setor dentro da
# filial ("001.08 - CONSTRUTORA/AFASTADO INSS") era jogado fora na leitura.
# ---------------------------------------------------------------------------
def _folha_com_setores():
    return [
        ["Folha Sintética - Adiantamento de Folha", "", "", "", ": 1"],
        ["Empresa:", "BWS CONSTRUCOES LTDA - CNPJ: 00.079.526/0001-09", "", "", ""],
        ["Mês/Ano: 08/2026", "", "", "", ""],
        ["Código", "Empregado", "", "", "Líquido"],
        ["001 - CONSTRUTORA", "", "", "", ""],
        ["001.01 - CONSTRUTORA/ESCRITORIO", "", "", "", ""],
        ["000013", "GERLANIO GOMES LIMA", "", "", 1198.84],
        ["Total: 001.01 - CONSTRUTORA/ESCRITORIO", "", "", "", 1198.84],
        ["001.08 - CONSTRUTORA/AFASTADO INSS", "", "", "", ""],
        ["000387", "LUELIA MADIDA GOMES TOMAS", "", "", 1000.00],
        ["Total: 001 - CONSTRUTORA", "", "", "", 2198.84],
        ["090 - OBRA X", "", "", "", ""],
        ["000901", "FRANCISCO", "", "", 771.42],
        ["Total: 090 - OBRA X", "", "", "", 771.42],
        ["Total: Geral (3 Empregado(s))", "", "", "", 2970.26],
        ["", "", "", "", "Fim"]]


def test_o_SETOR_de_cada_pessoa_e_guardado_e_lido_de_volta(banco_folha_arquivo, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa
    from app.apps.analisesps import folha_sintetica as fs
    linhas = _folha_com_setores()
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))

    feito = fa.importar(b"x", "folha.xls", tipo="quinzena", quem="T")
    lidas = {l["id_fortes"]: l for l in fa.abrir(feito["id"])["linhas"]}

    assert lidas["000013"]["setor_nome"] == "CONSTRUTORA/ESCRITORIO"
    assert lidas["000387"]["setor_codigo"] == "001.08"
    # Filial nova sem setor: não herda o setor da anterior.
    assert lidas["000901"]["setor_nome"] == ""
    assert feito["avisos"] == [], feito["avisos"]


def test_o_total_por_SETOR_e_o_destaque_de_quem_pede_atencao(banco_folha_arquivo, monkeypatch):
    from decimal import Decimal as D
    from app.apps.analisesps import folha_arquivo as fa
    from app.apps.analisesps import folha_sintetica as fs
    linhas = _folha_com_setores()
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))
    feito = fa.importar(b"x", "folha.xls", tipo="quinzena", quem="T")

    setores = {s["codigo"]: s for s in fa.totais_por_setor(feito["id"])}
    assert setores["001.08"]["curto"] == "AFASTADO INSS"
    assert setores["001.08"]["atencao"] is True
    assert setores["001.01"]["atencao"] is False
    assert setores["001.01"]["total"] == D("1198.84")


def test_SEM_a_migracao_038_a_folha_entra_sem_o_setor(banco_analisesps_mutilado, banco_folha_arquivo, monkeypatch):
    """O intervalo entre o código subir e o botão ser apertado."""
    from sqlalchemy import text
    from app.apps.analisesps import db
    from app.apps.analisesps import folha_arquivo as fa
    from app.apps.analisesps import folha_sintetica as fs
    with db.obter_engine().connect() as conn:
        conn.execute(text("ALTER TABLE analisesps.folha_linha DROP COLUMN setor_nome"))
        conn.execute(text("ALTER TABLE analisesps.folha_linha DROP COLUMN setor_codigo"))
        conn.commit()
    db.esquecer_colunas()
    linhas = _folha_com_setores()
    monkeypatch.setattr(fs, "ler", lambda c: fs.interpretar(linhas))

    feito = fa.importar(b"x", "folha.xls", tipo="quinzena", quem="T")
    folha = fa.abrir(feito["id"])
    assert len(folha["linhas"]) == 3
    assert folha["tem_setor"] is False
    assert fa.totais_por_setor(feito["id"]) == []
    db.esquecer_colunas()
