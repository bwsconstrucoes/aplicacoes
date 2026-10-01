# -*- coding: utf-8 -*-
"""
Feriados e férias — 27/09/2026, com banco de verdade.

⚠️ POR QUE COM BANCO: tudo o que importa aqui é constraint e `WHERE` de
sobreposição de datas, e o dublê da suíte ignora os dois.

  - o índice único do feriado é o que impede um 7 de setembro cadastrado duas
    vezes descontar DOIS dias do auxílio de todo mundo;
  - o CHECK do período impede férias com o fim antes do início, que daria
    contagem negativa e auxílio a MAIS;
  - e a regra de sobreposição é `inicio <= fim_pedido AND fim >= inicio_pedido`
    — escrita ao contrário é o erro clássico, e passaria sem estourar.
"""
import datetime as dt
import pathlib

import pytest

pytestmark = pytest.mark.banco

D = dt.date


@pytest.fixture
def banco_calendario(banco_analisesps):
    """O schema `analisesps` limpo para este teste.

    ⚠️ ERA UM REFAZ-TUDO: `DROP SCHEMA` mais os 36 arquivos de migração, A CADA
    TESTE. Com 727 testes de banco na suíte, isso dava ~26 mil execuções de
    arquivo SQL por rodada para construir sempre a mesma coisa — e era a maior
    conta do tempo que o dono cobrou em 29/09/2026.

    Agora o schema nasce uma vez por sessão e as tabelas são esvaziadas entre os
    testes. O isolamento é o mesmo: tabelas vazias e contadores de `id` zerados.
    Ver `banco_analisesps` no `conftest.py`."""

GERLANIO = "99713349334"
LUELIA = "03513441363"


def test_a_migracao_032_roda_no_postgres(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    assert fc._pronto() is True
    assert fc.listar_feriados() == []
    assert fc.listar_ferias() == []


# ---------------------------------------------------------------------------
# FERIADOS
# ---------------------------------------------------------------------------
def test_feriado_nacional_e_de_obra_convivem(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("07/09/2026", fc.NACIONAL, descricao="Independência",
                      quem="MARCELO")
    fc.gravar_feriado("12/09/2026", fc.POR_OBRA, obra="CREPEOLINDA",
                      descricao="Aniversário da cidade", quem="MARCELO")

    lista = fc.listar_feriados()
    assert len(lista) == 2
    por_data = {f["data"]: f for f in lista}
    assert por_data[D(2026, 9, 7)]["abrangencia"] == "nacional"
    assert por_data[D(2026, 9, 7)]["obra"] == ""
    assert por_data[D(2026, 9, 12)]["obra"] == "CREPEOLINDA"


def test_o_MESMO_feriado_DUAS_vezes_e_recusado_com_frase(banco_calendario):
    """⚠️ Um 7 de setembro em duplicidade descontaria DOIS dias do auxílio de
    todo mundo."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    with pytest.raises(fc.ErroDoCalendario) as erro:
        fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    assert "já está cadastrado" in str(erro.value)
    assert len(fc.listar_feriados()) == 1


def test_o_mesmo_dia_em_DUAS_obras_diferentes_pode(banco_calendario):
    """Cada município tem o seu — o dia pode coincidir."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("12/09/2026", fc.POR_OBRA, obra="CREPEOLINDA")
    fc.gravar_feriado("12/09/2026", fc.POR_OBRA, obra="CREPEAREIAS")
    assert len(fc.listar_feriados()) == 2


def test_feriado_de_obra_SEM_obra_e_recusado(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    with pytest.raises(fc.ErroDoCalendario) as erro:
        fc.gravar_feriado("12/09/2026", fc.POR_OBRA, obra="  ")
    assert "selecione a obra" in str(erro.value).lower()


def test_feriado_nacional_IGNORA_a_obra_que_vier(banco_calendario):
    """Nacional com obra escrita deixaria a consulta ambígua. O banco também
    recusa; o código limpa antes para a frase não precisar existir."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("07/09/2026", fc.NACIONAL, obra="CREPEOLINDA")
    assert fc.listar_feriados()[0]["obra"] == ""


def test_data_ilegivel_no_feriado_e_recusada(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    with pytest.raises(fc.ErroDoCalendario) as erro:
        fc.gravar_feriado("não é data", fc.NACIONAL)
    assert "dia/mês/ano" in str(erro.value)


def test_os_feriados_do_periodo_respeitam_a_OBRA(banco_calendario):
    """⚠️ O nacional vale sempre; o da obra só na obra dela. Somar os dois sem
    distinguir descontaria o feriado de Olinda do auxílio de quem trabalha em
    Sobral."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    fc.gravar_feriado("12/09/2026", fc.POR_OBRA, obra="CREPEOLINDA")
    fc.gravar_feriado("20/09/2026", fc.POR_OBRA, obra="CREPEAREIAS")

    em_olinda = fc.feriados_no_periodo(D(2026, 9, 1), D(2026, 9, 30),
                                       "CREPEOLINDA")
    assert [f["data"] for f in em_olinda] == [D(2026, 9, 7), D(2026, 9, 12)]

    # Sem obra, só os nacionais.
    assert [f["data"] for f in
            fc.feriados_no_periodo(D(2026, 9, 1), D(2026, 9, 30))] == \
        [D(2026, 9, 7)]


def test_feriado_fora_do_periodo_nao_entra(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    assert fc.feriados_no_periodo(D(2026, 8, 1), D(2026, 8, 31)) == []


def test_apagar_feriado(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    novo = fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    assert fc.apagar_feriado(novo) is True
    assert fc.listar_feriados() == []
    assert fc.apagar_feriado(99999) is False


def test_listar_feriados_filtra_por_ANO(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    fc.gravar_feriado("07/09/2025", fc.NACIONAL)
    assert len(fc.listar_feriados(ano=2026)) == 1
    assert len(fc.listar_feriados()) == 2


# ---------------------------------------------------------------------------
# FÉRIAS
# ---------------------------------------------------------------------------
def test_gravar_e_listar_ferias(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_ferias(GERLANIO, "01/09/2026", "20/09/2026",
                     nome="GERLANIO GOMES LIMA", quem="MARCELO")
    lista = fc.listar_ferias()
    assert len(lista) == 1
    assert lista[0]["nome"] == "GERLANIO GOMES LIMA"
    assert lista[0]["cpf_bonito"] == "997.133.493-34"
    assert lista[0]["inicio"] == D(2026, 9, 1)
    assert lista[0]["dias"] == 20, "conta os dois extremos"


def test_a_mesma_pessoa_pode_ter_DOIS_periodos_no_ano(banco_calendario):
    """Férias fracionadas são a regra, não a exceção."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_ferias(GERLANIO, "01/03/2026", "15/03/2026")
    fc.gravar_ferias(GERLANIO, "01/09/2026", "15/09/2026")
    assert len(fc.listar_ferias()) == 2


def test_periodos_que_SE_CRUZAM_sao_recusados_dizendo_qual(banco_calendario):
    """⚠️ Dois períodos que se cruzam descontariam o mesmo dia duas vezes, e o
    auxílio sairia a menos."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_ferias(GERLANIO, "01/09/2026", "20/09/2026")
    with pytest.raises(fc.ErroDoCalendario) as erro:
        fc.gravar_ferias(GERLANIO, "15/09/2026", "30/09/2026")
    frase = str(erro.value)
    assert "01/09/2026" in frase and "20/09/2026" in frase
    assert "se cruzam" in frase
    assert len(fc.listar_ferias()) == 1


def test_periodo_que_ENGLOBA_o_outro_tambem_e_recusado(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    fc.gravar_ferias(GERLANIO, "10/09/2026", "15/09/2026")
    with pytest.raises(fc.ErroDoCalendario):
        fc.gravar_ferias(GERLANIO, "01/09/2026", "30/09/2026")


def test_periodos_ENCOSTADOS_mas_sem_cruzar_podem(banco_calendario):
    """Um acaba no dia 15, o outro começa no 16: não se cruzam."""
    from app.apps.analisesps import folha_calendario as fc
    fc.gravar_ferias(GERLANIO, "01/09/2026", "15/09/2026")
    fc.gravar_ferias(GERLANIO, "16/09/2026", "30/09/2026")
    assert len(fc.listar_ferias()) == 2


def test_periodos_de_PESSOAS_DIFERENTES_nao_conflitam(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    fc.gravar_ferias(GERLANIO, "01/09/2026", "20/09/2026")
    fc.gravar_ferias(LUELIA, "01/09/2026", "20/09/2026")
    assert len(fc.listar_ferias()) == 2


def test_fim_ANTES_do_inicio_e_recusado(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    with pytest.raises(fc.ErroDoCalendario) as erro:
        fc.gravar_ferias(GERLANIO, "20/09/2026", "01/09/2026")
    assert "anterior à data inicial" in str(erro.value)


def test_periodo_ABSURDO_e_recusado_porque_e_ano_digitado_errado(banco_calendario):
    """Quase sempre é o ano trocado — e descontaria meses de auxílio."""
    from app.apps.analisesps import folha_calendario as fc
    with pytest.raises(fc.ErroDoCalendario) as erro:
        fc.gravar_ferias(GERLANIO, "01/09/2026", "01/09/2029")
    assert "Verifique o ano" in str(erro.value)


def test_cpf_com_digito_errado_e_recusado(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    with pytest.raises(fc.ErroDoCalendario) as erro:
        fc.gravar_ferias("111.111.111-11", "01/09/2026", "10/09/2026")
    assert "dígito verificador" in str(erro.value)


def test_a_busca_das_ferias_acha_por_nome_e_por_cpf(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_ferias(GERLANIO, "01/09/2026", "10/09/2026",
                     nome="GERLANIO GOMES LIMA")
    fc.gravar_ferias(LUELIA, "01/09/2026", "10/09/2026",
                     nome="LUELIA MADIDA GOMES TOMAS")

    assert len(fc.listar_ferias("gomes")) == 2
    assert [f["nome"] for f in fc.listar_ferias("gerlanio")] == \
        ["GERLANIO GOMES LIMA"]
    assert len(fc.listar_ferias("997.133.493-34")) == 1


def test_apagar_ferias(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    novo = fc.gravar_ferias(GERLANIO, "01/09/2026", "10/09/2026")
    assert fc.apagar_ferias(novo) is True
    assert fc.listar_ferias() == []


# ---------------------------------------------------------------------------
# O QUE O CÁLCULO DO AUXÍLIO PERGUNTA
# ---------------------------------------------------------------------------
def test_dias_uteis_de_um_mes_inteiro():
    """Setembro de 2026: 30 dias, começando numa terça."""
    from app.apps.analisesps import folha_calendario as fc

    assert fc.dias_uteis(D(2026, 9, 1), D(2026, 9, 30)) == 22
    # Segunda à quinta: tira as sextas (4 no mês).
    assert fc.dias_uteis(D(2026, 9, 1), D(2026, 9, 30), sexta=False) == 18
    # Com sábado: 22 + 4 sábados.
    assert fc.dias_uteis(D(2026, 9, 1), D(2026, 9, 30), sabado=True) == 26


def test_dias_uteis_de_periodo_invertido_ou_vazio_e_zero():
    from app.apps.analisesps import folha_calendario as fc
    assert fc.dias_uteis(D(2026, 9, 30), D(2026, 9, 1)) == 0
    assert fc.dias_uteis(None, D(2026, 9, 1)) == 0


def test_dias_de_ferias_conta_so_o_que_cai_DENTRO_do_periodo(banco_calendario):
    """As férias podem começar antes do mês e acabar depois. Só o pedaço de
    dentro desconta."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_ferias(GERLANIO, "25/08/2026", "10/09/2026")
    # De 1 a 10 de setembro há 8 dias úteis (1 a 4, 7 a 10).
    assert fc.dias_de_ferias_no_periodo(
        GERLANIO, D(2026, 9, 1), D(2026, 9, 30)) == 8


def test_dias_de_ferias_soma_os_DOIS_periodos(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_ferias(GERLANIO, "01/09/2026", "04/09/2026")   # 4 úteis
    fc.gravar_ferias(GERLANIO, "21/09/2026", "25/09/2026")   # 5 úteis
    assert fc.dias_de_ferias_no_periodo(
        GERLANIO, D(2026, 9, 1), D(2026, 9, 30)) == 9


def test_quem_nao_tem_ferias_desconta_zero(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc
    assert fc.dias_de_ferias_no_periodo(
        GERLANIO, D(2026, 9, 1), D(2026, 9, 30)) == 0


def test_feriado_no_FIM_DE_SEMANA_nao_desconta_nada(banco_calendario):
    """⚠️ Aquele dia já não contava. É a mesma sutileza que a planilha resolve
    contando os feriados de sexta à parte — aqui sai de graça, porque a conta é
    dia a dia."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("05/09/2026", fc.NACIONAL)   # sábado
    fc.gravar_feriado("07/09/2026", fc.NACIONAL)   # segunda
    assert fc.dias_de_feriado_no_periodo(D(2026, 9, 1), D(2026, 9, 30)) == 1


def test_feriado_na_SEXTA_nao_desconta_de_quem_e_segunda_a_quinta(banco_calendario):
    """A sexta já não contava para essa modalidade. Descontar seria tirar um dia
    que ninguém ia pagar."""
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("11/09/2026", fc.NACIONAL)   # sexta
    assert fc.dias_de_feriado_no_periodo(D(2026, 9, 1), D(2026, 9, 30)) == 1
    assert fc.dias_de_feriado_no_periodo(
        D(2026, 9, 1), D(2026, 9, 30), sexta=False) == 0


def test_feriado_de_obra_desconta_so_na_obra_dele(banco_calendario):
    from app.apps.analisesps import folha_calendario as fc

    fc.gravar_feriado("14/09/2026", fc.POR_OBRA, obra="CREPEOLINDA")
    assert fc.dias_de_feriado_no_periodo(
        D(2026, 9, 1), D(2026, 9, 30), "CREPEOLINDA") == 1
    assert fc.dias_de_feriado_no_periodo(
        D(2026, 9, 1), D(2026, 9, 30), "CREPEAREIAS") == 0
