# -*- coding: utf-8 -*-
"""
Feriados e férias — 27/09/2026, com banco de verdade.

⚠️ POR QUE COM BANCO: tudo o que importa aqui é constraint e `WHERE` de
sobreposição de datas, e o dublê da suíte ignora os dois.

  - os feriados vêm do PONTO desde 04/11/2026 (uma base só) — os testes deles
    gravam no ponto, pela fixture de `tests/_feriados_do_ponto.py`;
  - o CHECK do período impede férias com o fim antes do início, que daria
    contagem negativa e auxílio a MAIS;
  - e a regra de sobreposição é `inicio <= fim_pedido AND fim >= inicio_pedido`
    — escrita ao contrário é o erro clássico, e passaria sem estourar.
"""
import datetime as dt
import pathlib

import pytest

from tests._feriados_do_ponto import _schema_ponto2, feriados_ponto  # noqa: F401

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
    assert fc.listar_ferias() == []


# ---------------------------------------------------------------------------
# FERIADOS — VÊM DO PONTO (04/11/2026)
#
# O dono: *"a parte de feriados deve vir do ponto. Dessa forma ficaremos somente
# com uma base de feriados."* A regra de qual feriado vale onde é a DO PONTO.
# ---------------------------------------------------------------------------
AGO_INI, AGO_FIM = D(2026, 8, 1), D(2026, 8, 31)     # agosto: sem feriado nacional


def test_a_lista_da_tela_e_a_do_ponto(feriados_ponto):
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.obra("FC-OLINDA", "CRECHE OLINDA", uf="PE", municipio="Olinda")
    feriados_ponto.nacional(D(2026, 9, 7), "Independência")
    feriados_ponto.estadual(D(2026, 3, 6), "PE", "Revolução Pernambucana")
    feriados_ponto.municipal(D(2026, 3, 12), "PE", "Olinda", "Aniversário de Olinda")
    feriados_ponto.da_obra(D(2026, 8, 14), "FC-OLINDA", "Padroeira da obra")

    lista = fc.listar_feriados(2026)
    assert [(f["data"], f["rotulo"], f["onde"], f["descricao"]) for f in lista] == [
        (D(2026, 9, 7), "Nacional", "", "Independência"),
        (D(2026, 8, 14), "Só nesta obra", "FC-OLINDA", "Padroeira da obra"),
        (D(2026, 3, 12), "Municipal", "Olinda / PE", "Aniversário de Olinda"),
        (D(2026, 3, 6), "Estadual", "PE", "Revolução Pernambucana"),
    ]
    assert fc.listar_feriados(2025) == []


def test_o_nacional_vale_para_todo_mundo(feriados_ponto):
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.nacional(D(2026, 8, 10))
    assert len(fc.feriados_no_periodo(AGO_INI, AGO_FIM)) == 1
    assert len(fc.feriados_no_periodo(AGO_INI, AGO_FIM, "QUALQUER OBRA")) == 1


def test_estadual_e_municipal_valem_pela_UF_e_pelo_MUNICIPIO_da_obra(feriados_ponto):
    """O que a tabela antiga desta tela não tinha: feriado de Olinda vale para
    toda obra de Olinda, sem cadastrar obra por obra."""
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.obra("FC-OLINDA", "CRECHE OLINDA", uf="PE", municipio="Olinda")
    feriados_ponto.obra("FC-RECIFE", "POSTO RECIFE", uf="PE", municipio="Recife")
    feriados_ponto.obra("FC-SOBRAL", "ESCOLA SOBRAL", uf="CE", municipio="Sobral")
    feriados_ponto.estadual(D(2026, 8, 6), "PE")
    feriados_ponto.municipal(D(2026, 8, 12), "PE", "Olinda")

    def dias(obra):
        return [f["data"] for f in fc.feriados_no_periodo(AGO_INI, AGO_FIM, obra)]

    assert dias("FC-OLINDA") == [D(2026, 8, 6), D(2026, 8, 12)]
    assert dias("FC-RECIFE") == [D(2026, 8, 6)]
    assert dias("FC-SOBRAL") == []


def test_a_obra_casa_pelo_codigo_e_pelo_NOME_do_cadastro_do_rateio(feriados_ponto):
    """O auxílio pergunta pelo NOME da obra (o do cadastro); o diarista, pelo
    CÓDIGO. Os dois têm de achar o mesmo feriado."""
    from app.apps.analisesps import folha_calendario as fc
    from app.apps.analisesps.db import conexao
    feriados_ponto.obra("FC-OLINDA", "Creche de Olinda (ERP)", uf="PE", municipio="Olinda")
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                     "VALUES ('obra', 'CREPEOLINDA', 'FC-OLINDA')")
        conn.commit()
    feriados_ponto.da_obra(D(2026, 8, 14), "FC-OLINDA")
    for obra in ("FC-OLINDA", "fc-olinda", "CREPEOLINDA", "Creche de Olinda (ERP)"):
        fc.esquecer_leituras()
        assert len(fc.feriados_no_periodo(AGO_INI, AGO_FIM, obra)) == 1, obra


def test_obra_que_NAO_casa_conta_so_o_nacional(feriados_ponto):
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.obra("FC-OLINDA", "CRECHE OLINDA", uf="PE", municipio="Olinda")
    feriados_ponto.nacional(D(2026, 8, 10))
    feriados_ponto.municipal(D(2026, 8, 12), "PE", "Olinda")
    assert [f["data"] for f in fc.feriados_no_periodo(AGO_INI, AGO_FIM, "OBRA FANTASMA")] \
        == [D(2026, 8, 10)]


def test_feriado_fora_do_periodo_nao_entra(feriados_ponto):
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.nacional(D(2026, 9, 7))
    assert fc.feriados_no_periodo(AGO_INI, AGO_FIM) == []


def test_sem_o_ponto_a_folha_segue_sem_feriado(banco_calendario, monkeypatch):
    """Ponto fora do ar não derruba o auxílio: segue sem desconto de feriado, e o
    erro aparece na tela de Feriados."""
    from app.apps.analisesps import folha_calendario as fc

    def quebra(*a, **k):
        raise RuntimeError("ponto fora do ar")
    monkeypatch.setattr(fc, "FeriadosDoPonto", quebra)
    fc.esquecer_leituras()
    assert fc.feriados_no_periodo(AGO_INI, AGO_FIM) == []


# ---------------------------------------------------------------------------
# OS FERIADOS ANTIGOS DESTA TELA → o ponto
# ---------------------------------------------------------------------------
def _antigo(data, abrangencia, obra="", descricao=""):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.feriado (data, abrangencia, obra, descricao) "
                     "VALUES (?,?,?,?)", (data, abrangencia, obra, descricao))
        conn.commit()


def test_o_antigo_que_o_ponto_ja_cobre_nao_aparece(feriados_ponto):
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.obra("FC-OLINDA", "CRECHE OLINDA", uf="PE", municipio="Olinda")
    feriados_ponto.nacional(D(2026, 9, 7))
    feriados_ponto.municipal(D(2026, 8, 12), "PE", "Olinda")
    _antigo(D(2026, 9, 7), "nacional")
    _antigo(D(2026, 8, 12), "obra", "FC-OLINDA")     # coberto pelo municipal
    _antigo(D(2026, 8, 20), "obra", "FC-OLINDA", "Padroeira")
    faltam = fc.antigos_fora_do_ponto()
    assert [(f["data"], f["obra"]) for f in faltam] == [(D(2026, 8, 20), "FC-OLINDA")]


def test_levar_ao_ponto_grava_la_e_o_auxilio_passa_a_contar(feriados_ponto):
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.obra("FC-OLINDA", "CRECHE OLINDA", uf="PE", municipio="Olinda")
    _antigo(D(2026, 8, 10), "nacional", descricao="Feriado antigo")
    _antigo(D(2026, 8, 20), "obra", "FC-OLINDA", "Padroeira")
    _antigo(D(2026, 8, 21), "obra", "OBRA QUE O PONTO NAO TEM")
    assert fc.feriados_no_periodo(AGO_INI, AGO_FIM, "FC-OLINDA") == []

    r = fc.levar_ao_ponto(quem="teste")

    assert r == {"levados": 2, "sem_obra": ["OBRA QUE O PONTO NAO TEM"]}
    assert [f["data"] for f in fc.feriados_no_periodo(AGO_INI, AGO_FIM, "FC-OLINDA")] \
        == [D(2026, 8, 10), D(2026, 8, 20)]
    # Rodar de novo não duplica nada no ponto.
    antes = feriados_ponto.contar()
    assert fc.levar_ao_ponto()["levados"] == 0
    assert feriados_ponto.contar() == antes


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


def test_feriado_no_FIM_DE_SEMANA_nao_desconta_nada(feriados_ponto):
    """⚠️ Aquele dia já não contava. É a mesma sutileza que a planilha resolve
    contando os feriados de sexta à parte — aqui sai de graça, porque a conta é
    dia a dia."""
    from app.apps.analisesps import folha_calendario as fc

    feriados_ponto.nacional(D(2026, 9, 5))   # sábado
    feriados_ponto.nacional(D(2026, 9, 7))   # segunda
    assert fc.dias_de_feriado_no_periodo(D(2026, 9, 1), D(2026, 9, 30)) == 1


def test_feriado_na_SEXTA_nao_desconta_de_quem_e_segunda_a_quinta(feriados_ponto):
    """A sexta já não contava para essa modalidade. Descontar seria tirar um dia
    que ninguém ia pagar."""
    from app.apps.analisesps import folha_calendario as fc

    feriados_ponto.nacional(D(2026, 9, 11))   # sexta
    assert fc.dias_de_feriado_no_periodo(D(2026, 9, 1), D(2026, 9, 30)) == 1
    assert fc.dias_de_feriado_no_periodo(
        D(2026, 9, 1), D(2026, 9, 30), sexta=False) == 0


def test_feriado_de_obra_desconta_so_na_obra_dele(feriados_ponto):
    from app.apps.analisesps import folha_calendario as fc

    feriados_ponto.obra("FC-OLINDA", "CREPEOLINDA")
    feriados_ponto.obra("FC-AREIAS", "CREPEAREIAS")
    feriados_ponto.da_obra(D(2026, 9, 14), "FC-OLINDA")
    assert fc.dias_de_feriado_no_periodo(
        D(2026, 9, 1), D(2026, 9, 30), "CREPEOLINDA") == 1
    assert fc.dias_de_feriado_no_periodo(
        D(2026, 9, 1), D(2026, 9, 30), "CREPEAREIAS") == 0
