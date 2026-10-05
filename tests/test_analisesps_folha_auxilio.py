# -*- coding: utf-8 -*-
"""
Auxílio alimentação e transporte — 27/09/2026.

⚠️ A DIFERENÇA ENTRE AS DUAS VERBAS É DINHEIRO, e é decisão do dono: a alimentação
desconta feriado e férias; o transporte desconta férias e **não** feriado de um dia.
Trocar isso pagaria refeição de feriado, ou tiraria transporte que era devido — e em
~500 pessoas, todo mês.

E a regra de contagem por modalidade veio das fórmulas (§7.14.6), não de suposição:
Mês = valor fechado; Mensal = dias do mês − 3; Segunda à Sexta e Segunda à Quinta =
dias úteis, com e sem as sextas.
"""
import datetime as dt
import pathlib
from decimal import Decimal as D

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_auxilio(banco_analisesps):
    """O schema `analisesps` limpo para este teste.

    ⚠️ ERA UM REFAZ-TUDO: `DROP SCHEMA` mais os 36 arquivos de migração, A CADA
    TESTE. Com 727 testes de banco na suíte, isso dava ~26 mil execuções de
    arquivo SQL por rodada para construir sempre a mesma coisa — e era a maior
    conta do tempo que o dono cobrou em 29/09/2026.

    Agora o schema nasce uma vez por sessão e as tabelas são esvaziadas entre os
    testes. O isolamento é o mesmo: tabelas vazias e contadores de `id` zerados.
    Ver `banco_analisesps` no `conftest.py`."""

GERLANIO = "99713349334"

# Setembro de 2026: 30 dias, 22 dias úteis, 4 sextas.
INICIO, FIM = dt.date(2026, 9, 1), dt.date(2026, 9, 30)


def ficha(**extra):
    """Uma ficha do cadastro como `colaboradores.buscar` devolve."""
    base = {"cpf": GERLANIO, "nome": "GERLANIO GOMES LIMA", "cargo": "AJUDANTE",
            "link_pipefy": "", "obra_cadastro": "CREPEOLINDA",
            "situacao": "ativo", "motivo": "",
            "modo_alimentacao": "Segunda à Sexta", "valor_alimentacao": D("15.00"),
            "modo_transporte": "Segunda à Sexta", "valor_transporte": D("10.00")}
    base.update(extra)
    return base


# ---------------------------------------------------------------------------
# A contagem de dias por modalidade
# ---------------------------------------------------------------------------
def test_segunda_a_sexta_conta_os_dias_uteis():
    from app.apps.analisesps import folha_auxilio as fx
    dias, sabado, sexta = fx.dias_da_modalidade("Segunda à Sexta", INICIO, FIM)
    assert (dias, sabado, sexta) == (22, False, True)


def test_segunda_a_quinta_tira_as_sextas():
    from app.apps.analisesps import folha_auxilio as fx
    dias, sabado, sexta = fx.dias_da_modalidade("Segunda à Quinta", INICIO, FIM)
    assert (dias, sabado, sexta) == (18, False, False)


def test_mensal_e_todos_os_dias_MENOS_TRES():
    """O "−3" é da fórmula da planilha, não meu."""
    from app.apps.analisesps import folha_auxilio as fx
    dias, sabado, sexta = fx.dias_da_modalidade("Mensal", INICIO, FIM)
    assert (dias, sabado, sexta) == (27, True, True)


def test_MENSAL_no_TRANSPORTE_e_valor_do_mes_e_conta_UM_dia():
    """O dono, 03/10/2026: *"Mensal é mensal. Aquele valor que está lá já é o
    valor mensal. Aí você está multiplicando a base, quantidade de dias, pelo
    valor que é mensal."* Na alimentação, a regra da planilha continua."""
    from app.apps.analisesps import folha_auxilio as fx
    assert fx.dias_da_modalidade("Mensal", INICIO, FIM, fx.TRANSPORTE)[0] == 1
    assert fx.dias_da_modalidade("Mensal", INICIO, FIM, fx.ALIMENTACAO)[0] == 27
    assert fx.valor_fechado("Mensal", fx.TRANSPORTE) is True
    assert fx.valor_fechado("Mensal", fx.ALIMENTACAO) is False
    assert fx.valor_fechado("Mês", fx.ALIMENTACAO) is True


def test_mes_e_valor_fechado_e_conta_UM_dia():
    from app.apps.analisesps import folha_auxilio as fx
    assert fx.dias_da_modalidade("Mês", INICIO, FIM)[0] == 1


def test_modalidade_sem_acento_e_em_caixa_baixa_ainda_casa():
    """O cadastro é digitado por gente."""
    from app.apps.analisesps import folha_auxilio as fx
    assert fx.dias_da_modalidade("segunda a quinta", INICIO, FIM)[0] == 18
    assert fx.dias_da_modalidade("  MÊS  ", INICIO, FIM)[0] == 1


def test_modalidade_DESCONHECIDA_nao_chuta():
    """⚠️ Pagar por uma régua inventada é pior que não pagar e alguém reclamar."""
    from app.apps.analisesps import folha_auxilio as fx
    assert fx.dias_da_modalidade("quando der", INICIO, FIM)[0] == 0


def test_o_periodo_do_auxilio_e_o_MES_INTEIRO():
    """⚠️ Não é quinzena — é o que a planilha faz. Usar o período da quinzena
    pagaria metade do transporte de alguém duas vezes."""
    from app.apps.analisesps import folha_auxilio as fx
    assert fx.periodo_do_mes(2026, 9) == (dt.date(2026, 9, 1), dt.date(2026, 9, 30))
    assert fx.periodo_do_mes(2026, 2) == (dt.date(2026, 2, 1), dt.date(2026, 2, 28))


# ---------------------------------------------------------------------------
# A conta de uma pessoa
# ---------------------------------------------------------------------------
def test_o_caminho_INTEIRO_da_conta_vem_na_resposta(banco_auxilio):
    """⚠️ Ele pediu para saber "de onde veio aquela informação". Um valor sozinho
    não se audita."""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM)
    assert r["dias_base"] == 22
    assert r["feriados"] == 0
    assert r["ferias"] == 0
    assert r["dias"] == 22
    assert r["valor"] == D("330.00")
    assert r["pagar"] is True


def test_a_ALIMENTACAO_desconta_feriado(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx, folha_calendario as fc

    fc.gravar_feriado("07/09/2026", fc.NACIONAL)      # segunda
    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM)
    assert r["feriados"] == 1
    assert r["dias"] == 21
    assert r["valor"] == D("315.00")


def test_o_TRANSPORTE_NAO_desconta_feriado(banco_auxilio):
    """⚠️ Decisão do dono: *"um único dia não precisaria"*. Descontar aqui tiraria
    transporte que era devido, em ~500 pessoas, todo mês."""
    from app.apps.analisesps import folha_auxilio as fx, folha_calendario as fc

    fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    r = fx.calcular_pessoa(fx.TRANSPORTE, ficha(), INICIO, FIM)
    assert r["feriados"] == 0, "o transporte não desconta feriado"
    assert r["dias"] == 22
    assert r["valor"] == D("220.00")


def test_TRANSPORTE_MENSAL_paga_o_valor_do_cadastro_CHEIO(banco_auxilio):
    """Era 27 × o valor do mês. Agora é o valor do mês, e a base é 1."""
    from app.apps.analisesps import folha_auxilio as fx
    r = fx.calcular_pessoa(fx.TRANSPORTE, ficha(modo_transporte="Mensal",
                                                  valor_transporte=D("180.00")),
                           INICIO, FIM)
    assert r["dias_base"] == 1 and r["dias"] == 1
    assert r["valor"] == D("180.00")
    assert r["valor_fechado"] is True
    assert r["pagar"] is True


def test_as_DUAS_verbas_descontam_FERIAS(banco_auxilio):
    """*"Férias, como são mais dias, sim, deveríamos proporcionalizar."*"""
    from app.apps.analisesps import folha_auxilio as fx, folha_calendario as fc

    fc.gravar_ferias(GERLANIO, "01/09/2026", "04/09/2026")   # 4 dias úteis
    alimentacao = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM)
    transporte = fx.calcular_pessoa(fx.TRANSPORTE, ficha(), INICIO, FIM)

    assert alimentacao["ferias"] == 4
    assert alimentacao["dias"] == 18
    assert transporte["ferias"] == 4
    assert transporte["dias"] == 18


def test_o_feriado_da_OBRA_desconta_so_de_quem_e_da_obra(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx, folha_calendario as fc

    fc.gravar_feriado("14/09/2026", fc.POR_OBRA, obra="CREPEOLINDA")
    daqui = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM)
    dalhures = fx.calcular_pessoa(fx.ALIMENTACAO,
                                  ficha(obra_cadastro="CREPEAREIAS"),
                                  INICIO, FIM)
    assert daqui["feriados"] == 1
    assert dalhures["feriados"] == 0


def test_o_feriado_na_SEXTA_nao_desconta_de_quem_e_segunda_a_quinta(banco_auxilio):
    """A sexta já não contava para essa modalidade — descontar tiraria um dia que
    ninguém ia pagar. A régua do desconto é a MESMA da modalidade."""
    from app.apps.analisesps import folha_auxilio as fx, folha_calendario as fc

    fc.gravar_feriado("11/09/2026", fc.NACIONAL)     # sexta
    seg_qui = fx.calcular_pessoa(
        fx.ALIMENTACAO, ficha(modo_alimentacao="Segunda à Quinta"), INICIO, FIM)
    seg_sex = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM)
    assert seg_qui["feriados"] == 0
    assert seg_sex["feriados"] == 1


def test_valor_FECHADO_nao_desconta_dia_nenhum(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx, folha_calendario as fc

    fc.gravar_feriado("07/09/2026", fc.NACIONAL)
    fc.gravar_ferias(GERLANIO, "01/09/2026", "10/09/2026")
    r = fx.calcular_pessoa(
        fx.ALIMENTACAO,
        ficha(modo_alimentacao="Mês", valor_alimentacao=D("400.00")),
        INICIO, FIM)
    assert r["dias"] == 1
    assert r["valor"] == D("400.00")


def test_quem_SAIU_nao_recebe(banco_auxilio):
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx

    r = fx.calcular_pessoa(
        fx.ALIMENTACAO,
        ficha(situacao=col.SITUACAO_SAIU, motivo="saiu em 31/08/2026."),
        INICIO, FIM)
    assert r["pagar"] is False
    assert "31/08/2026" in " ".join(r["motivos"])
    # ⚠️ O VALOR CONTINUA CALCULADO, e é de propósito: a tela mostra quanto SERIA,
    # e é o que permite a ele marcar para pagar o valor certo se decidir que é
    # devido. O que decide é `pagar`, e o total só soma quem paga.
    assert r["valor"] == D("330.00")
    assert r["impossivel"] is False


def test_quem_esta_AFASTADO_nao_recebe_auxilio(banco_auxilio):
    """A mesma exclusão que as abas da planilha já fazem pela fase."""
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO,
                           ficha(situacao=col.SITUACAO_AFASTADO), INICIO, FIM)
    assert r["pagar"] is False


def test_quem_esta_SAINDO_recebe_mas_com_aviso(banco_auxilio):
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx

    r = fx.calcular_pessoa(
        fx.ALIMENTACAO,
        ficha(situacao=col.SITUACAO_SAINDO, motivo="sai em 30/09/2026."),
        INICIO, FIM)
    assert r["pagar"] is True
    assert "30/09/2026" in " ".join(r["motivos"])


def test_CARTAO_no_transporte_nao_paga_em_dinheiro(banco_auxilio):
    """Vem da planilha (`AA != 'Cartão'`), e é só do transporte."""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.TRANSPORTE, ficha(modo_transporte="Cartão"),
                           INICIO, FIM)
    assert r["pagar"] is False
    assert "Cartão" in " ".join(r["motivos"])


def test_sem_VALOR_no_cadastro_nao_paga_e_diz_onde_consertar(banco_auxilio):
    """⚠️ Campo de auxílio em branco vira pagamento a menos — e a frase tem de
    dizer que o conserto é no card do Pipefy."""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(valor_alimentacao=None),
                           INICIO, FIM)
    assert r["pagar"] is False
    assert "card do Pipefy" in " ".join(r["motivos"])


def test_sem_MODALIDADE_no_cadastro_nao_paga_e_lista_as_conhecidas(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(modo_alimentacao=""),
                           INICIO, FIM)
    assert r["pagar"] is False
    assert "Segunda à Sexta" in " ".join(r["motivos"])


def test_modalidade_que_eu_nao_conheco_diz_as_que_conheco(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO,
                           ficha(modo_alimentacao="quinzenal talvez"), INICIO, FIM)
    assert r["pagar"] is False
    assert "quinzenal talvez" in " ".join(r["motivos"])
    assert "Mensal" in " ".join(r["motivos"])


# ---------------------------------------------------------------------------
# Os ajustes dele
# ---------------------------------------------------------------------------
def test_o_ajuste_de_DIAS_entra_na_conta(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           ajuste={"dias": -2})
    assert r["dias"] == 20
    assert r["valor"] == D("300.00")


def test_o_ajuste_NUNCA_deixa_os_dias_negativos(banco_auxilio):
    """Um ajuste torto não pode virar valor a devolver."""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           ajuste={"dias": -99})
    assert r["dias"] == 0
    assert r["valor"] == D("0.00")


def test_DESMARCAR_a_pessoa_manda_mais_que_o_calculo(banco_auxilio):
    """*"Eu preciso ter a liberdade de desmarcar uma pessoa para gerar o pagamento
    ou não."*"""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           ajuste={"pagar": False})
    assert r["pagar"] is False
    assert "desmarcado manualmente" in " ".join(r["motivos"])


def test_MARCAR_quem_o_calculo_recusou_POR_POLITICA_vale(banco_auxilio):
    """A última palavra é dele: quem saiu não recebe por decisão, e se ele decidir
    que é devido, paga — pelo valor calculado, não por zero."""
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx

    r = fx.calcular_pessoa(
        fx.ALIMENTACAO, ficha(situacao=col.SITUACAO_SAIU), INICIO, FIM,
        ajuste={"pagar": True})
    assert r["pagar"] is True
    assert "apesar da pendência" in " ".join(r["motivos"])
    assert r["valor"] == D("330.00")


def test_MARCAR_nao_resolve_falta_de_dado_no_cadastro(banco_auxilio):
    """⚠️ A distinção que um teste me obrigou a fazer. Sem valor no cadastro não há
    o que pagar: marcar "pagar" pagaria ZERO em silêncio, que é pior do que não
    pagar. Então aqui a marcação NÃO vale, e o recado diz o que consertar."""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(modo_alimentacao=""),
                           INICIO, FIM, ajuste={"pagar": True})
    assert r["pagar"] is False
    assert r["impossivel"] is True
    assert "o cadastro está incompleto" in " ".join(r["motivos"])


def test_ajuste_NULO_e_diferente_de_FALSO(banco_auxilio):
    """⚠️ Nulo é "não mexi" — vale o cálculo. Falso é "decidi não pagar".
    Confundir os dois faria o padrão virar decisão."""
    from app.apps.analisesps import folha_auxilio as fx

    assert fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                              ajuste={"pagar": None})["pagar"] is True


def test_a_OBRA_e_o_CODIGO_e_vem_do_cadastro(banco_auxilio):
    """⚠️ MUDOU EM 28/09/2026, por correção do dono: *"em obra tem que colocar o
    CÓDIGO da obra e não a obra por extenso"* e *"eu não sei por que você colocou
    um campo editável; essa informação vem do cadastro."*

    Obra digitada na tela divergiria do cadastro e do rateio, e ninguém saberia
    qual das duas manda."""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           codigo_da_obra="1042")
    assert r["obra"] == "1042", "a tela mostra o CÓDIGO"
    assert "obra_ajustada" not in r, "não existe mais obra editável"

    # E o nome por extenso continua vindo, porque é por ele que o feriado
    # municipal é cadastrado na tela de Feriados.
    assert "obra_nome" in r


def test_o_ajuste_SOBREVIVE_ao_recalculo(banco_auxilio):
    """⚠️ É O PONTO TODO da tabela: sem guardar, cada recálculo apagaria o que ele
    mexeu, e ele teria de refazer todo mês."""
    from app.apps.analisesps import folha_auxilio as fx

    fx.gravar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO, pagar=False,
                     dias=-3, obra="CREPEAREIAS", observacao="entrou depois",
                     quem="MARCELO")
    guardado = fx.ajustes_do_mes(fx.ALIMENTACAO, 2026, 9)[GERLANIO]
    assert guardado["pagar"] is False
    assert guardado["dias"] == -3
    assert guardado["obra"] == "CREPEAREIAS"
    assert guardado["observacao"] == "entrou depois"
    assert guardado["alterado_por"] == "MARCELO"


def test_gravar_o_ajuste_DUAS_vezes_atualiza(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx

    fx.gravar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO, dias=-3)
    fx.gravar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO, dias=+2)
    assert fx.ajustes_do_mes(fx.ALIMENTACAO, 2026, 9)[GERLANIO]["dias"] == 2
    assert len(fx.ajustes_do_mes(fx.ALIMENTACAO, 2026, 9)) == 1


def test_o_ajuste_de_uma_VERBA_nao_vale_para_a_outra(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx

    fx.gravar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO, pagar=False)
    assert fx.ajustes_do_mes(fx.TRANSPORTE, 2026, 9) == {}


def test_o_ajuste_de_um_MES_nao_vale_para_o_outro(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx

    fx.gravar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO, pagar=False)
    assert fx.ajustes_do_mes(fx.ALIMENTACAO, 2026, 10) == {}


def test_ajuste_de_dias_ABSURDO_e_recusado(banco_auxilio):
    """É quase sempre digitação, e mudaria o valor em centenas de reais."""
    from app.apps.analisesps import folha_auxilio as fx

    with pytest.raises(fx.ErroDoAuxilio) as erro:
        fx.gravar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO, dias=300)
    assert "dois meses" in str(erro.value)


def test_limpar_o_ajuste_devolve_a_pessoa_ao_calculo(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx

    fx.gravar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO, pagar=False)
    assert fx.limpar_ajuste(fx.ALIMENTACAO, 2026, 9, GERLANIO) is True
    assert fx.ajustes_do_mes(fx.ALIMENTACAO, 2026, 9) == {}


# ---------------------------------------------------------------------------
# A verba inteira
# ---------------------------------------------------------------------------
def cadastrar(*pessoas):
    from app.apps.analisesps import colaboradores as col
    from app.apps.analisesps.db import conexao

    registros = []
    for cpf, nome, extra in pessoas:
        r = {c: "" for c in col.CAMPOS}
        r.update({"cpf": cpf, "nome": nome, "fase": "Colaboradores Ativos"})
        # Toda data e todo número vazio é None, e a lista vem do módulo — ver o
        # comentário igual em test_analisesps_colaboradores_banco.py.
        for campo in col.DATAS + col.NUMEROS:
            r[campo] = None
        r.update(extra or {})
        registros.append(r)
    with conexao() as conn:
        col._gravar(conn, registros)
        conn.commit()


def test_a_verba_inteira_lista_SO_quem_a_recebe(banco_auxilio):
    """É o que a planilha faz: quem não recebe a verba não é problema a resolver,
    é gente que não recebe."""
    from app.apps.analisesps import folha_auxilio as fx

    cadastrar(
        (GERLANIO, "COM ALIMENTACAO",
         {"modo_alimentacao": "Segunda à Sexta", "valor_alimentacao": D("15.00")}),
        ("03513441363", "SEM NADA", {}),
    )
    resultado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    assert [p["nome"] for p in resultado["pessoas"]] == ["COM ALIMENTACAO"]
    assert resultado["total"] == D("330.00")
    assert resultado["quantos_a_pagar"] == 1


def test_a_verba_inteira_soma_POR_OBRA(banco_auxilio):
    """É o corte que ele pediu para o painel, aplicado à verba."""
    from app.apps.analisesps import folha_auxilio as fx

    # ⚠️ AGRUPA PELO CÓDIGO da obra, não pelo nome (correção de 28/09/2026).
    cadastrar(
        (GERLANIO, "UM", {"modo_alimentacao": "Mês",
                          "valor_alimentacao": D("100.00"),
                          "obra_codigo": "OBRAA"}),
        ("03513441363", "DOIS", {"modo_alimentacao": "Mês",
                                 "valor_alimentacao": D("300.00"),
                                 "obra_codigo": "OBRAB"}),
        ("11144477735", "TRES", {"modo_alimentacao": "Mês",
                                 "valor_alimentacao": D("50.00"),
                                 "obra_codigo": "OBRAA"}),
    )
    # Sem ponto, a obra do cadastro NÃO entra sozinha (03/10/2026): é pendência,
    # e quem escolhe é ele ("usar esta obra").
    resultado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    assert len(resultado["sem_obra"]) == 3
    assert [o["obra"] for o in resultado["por_obra"]] == ["(sem obra)"]
    for cpf, obra in ((GERLANIO, "OBRAA"), ("03513441363", "OBRAB"),
                      ("11144477735", "OBRAA")):
        fx.gravar_extras(fx.ALIMENTACAO, 2026, 9, cpf, obra=obra)
    resultado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    assert resultado["total"] == D("450.00")
    assert resultado["sem_obra"] == []
    # Da maior para a menor.
    assert [o["obra"] for o in resultado["por_obra"]] == ["OBRAB", "OBRAA"]
    assert resultado["por_obra"][1]["total"] == D("150.00")
    assert resultado["por_obra"][1]["pessoas"] == 2


def test_quem_tem_PROBLEMA_aparece_na_lista_de_problemas(banco_auxilio):
    """E continua na lista principal, marcado — não numa lista à parte que alguém
    esquece de abrir."""
    from app.apps.analisesps import folha_auxilio as fx

    cadastrar(
        (GERLANIO, "SEM MODALIDADE", {"valor_alimentacao": D("15.00")}),
        ("03513441363", "OK", {"modo_alimentacao": "Mês",
                               "valor_alimentacao": D("100.00")}),
    )
    resultado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    assert len(resultado["pessoas"]) == 2
    assert [p["nome"] for p in resultado["com_problema"]] == ["SEM MODALIDADE"]
    assert resultado["total"] == D("100.00")


def test_os_com_problema_vem_PRIMEIRO_na_lista(banco_auxilio):
    """Quem precisa de mão tem de ser visto antes de quem está certo."""
    from app.apps.analisesps import folha_auxilio as fx

    cadastrar(
        (GERLANIO, "AAA OK", {"modo_alimentacao": "Mês",
                              "valor_alimentacao": D("100.00")}),
        ("03513441363", "ZZZ PROBLEMA", {"valor_alimentacao": D("15.00")}),
    )
    nomes = [p["nome"] for p in fx.calcular(fx.ALIMENTACAO, 2026, 9)["pessoas"]]
    assert nomes[0] == "ZZZ PROBLEMA"


def test_verba_desconhecida_e_recusada(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx
    with pytest.raises(fx.ErroDoAuxilio):
        fx.calcular("cesta", 2026, 9)


# ---------------------------------------------------------------------------
# A OBRA QUE PAGA VEM DO PONTO — 29/09/2026
#
# *"Em Alimentação a informação de obra deveria ser a do Ponto. Caso não tenha,
# usar a de cadastro."* E, no mesmo dia: *"a questão da obra que paga é
# fundamental."*
# ---------------------------------------------------------------------------
def test_a_obra_do_ponto_e_lida_do_mes_e_recortada_pelo_PERIODO(monkeypatch):
    """⚠️ RECORTA PELO PERÍODO: o auxílio conta os dias do mês trabalhado, e um dia
    fora dele não pode decidir a obra que paga."""
    import datetime as dt

    from app.apps.analisesps import folha_auxilio as fx, ponto

    monkeypatch.setattr(ponto, "dias_por_cpf", lambda a, m: {
        "99713349334": [
            {"data": dt.date(2026, 9, 2), "marcacoes": ["AAA"] * 4,
             "presenca": "Presença", "falta": ""},
            {"data": dt.date(2026, 9, 3), "marcacoes": ["AAA"] * 4,
             "presenca": "Presença", "falta": ""},
            # FORA do período: se contasse, "BBB" empataria e poderia ganhar.
            {"data": dt.date(2026, 10, 5), "marcacoes": ["BBB"] * 4,
             "presenca": "Presença", "falta": ""},
            {"data": dt.date(2026, 10, 6), "marcacoes": ["BBB"] * 4,
             "presenca": "Presença", "falta": ""},
            {"data": dt.date(2026, 10, 7), "marcacoes": ["BBB"] * 4,
             "presenca": "Presença", "falta": ""}]})

    achado = fx._obra_do_ponto_por_cpf(
        2026, 9, dt.date(2026, 9, 1), dt.date(2026, 9, 30))
    assert achado.pop("_janela") == (dt.date(2026, 9, 1), dt.date(2026, 9, 3))
    assert achado == {"99713349334": {"obra": "AAA", "dias": 2}}


def test_a_obra_que_paga_e_a_dos_ULTIMOS_15_DIAS_do_ponto(monkeypatch):
    """Dono, 03/10/2026: *"considerar aí os últimos 15 dias, a obra que a pessoa
    mais trabalhou, é a obra que vai pagar"*. No mês inteiro AAA tem mais dias;
    nos últimos 15, BBB."""
    import datetime as dt

    from app.apps.analisesps import folha_auxilio as fx, ponto

    def dia(d, obra):
        return {"data": dt.date(2026, 9, d), "marcacoes": [obra] * 4,
                "presenca": "Presença", "falta": ""}
    monkeypatch.setattr(ponto, "dias_por_cpf", lambda a, m: {
        "1": [dia(d, "AAA") for d in (1, 2, 3, 4, 7, 8, 9)]
             + [dia(d, "BBB") for d in (21, 22, 23)]})
    achado = fx._obra_do_ponto_por_cpf(2026, 9, dt.date(2026, 9, 1),
                                       dt.date(2026, 9, 30))
    assert achado["_janela"] == (dt.date(2026, 9, 9), dt.date(2026, 9, 23))
    assert achado["1"]["obra"] == "BBB"


def test_sem_ponto_da_competencia_vale_o_ponto_do_MES_ANTERIOR(monkeypatch):
    import datetime as dt

    from app.apps.analisesps import folha_auxilio as fx, ponto
    monkeypatch.setattr(ponto, "dias_por_cpf", lambda a, m: {} if m == 10 else {
        "1": [{"data": dt.date(2026, 9, 30), "marcacoes": ["CCC"] * 4,
               "presenca": "Presença", "falta": ""}]})
    achado = fx._obra_do_ponto_por_cpf(2026, 10, dt.date(2026, 10, 1),
                                       dt.date(2026, 10, 31))
    assert achado["1"]["obra"] == "CCC"


def test_sem_ponto_do_mes_o_auxilio_NAO_ESTOURA_e_cai_no_cadastro(monkeypatch):
    """⚠️ O auxílio é calculado todo mês, inclusive antes de alguém trazer o ponto.
    Uma tela de erro aqui esconderia a lista inteira por causa de uma coluna."""
    import datetime as dt

    from app.apps.analisesps import folha_auxilio as fx, ponto

    monkeypatch.setattr(ponto, "dias_por_cpf", lambda a, m: {})
    assert fx._obra_do_ponto_por_cpf(
        2026, 9, dt.date(2026, 9, 1), dt.date(2026, 9, 30)) == {}


def test_ponto_que_falha_nao_derruba_o_calculo_do_auxilio(monkeypatch):
    """O ponto vinha falhando por certificado no Render. Se isso derrubasse o
    auxílio, ele perderia as duas telas por causa de uma."""
    import datetime as dt

    from app.apps.analisesps import folha_auxilio as fx, ponto

    def explode(a, m):
        raise RuntimeError("certificate verify failed")

    monkeypatch.setattr(ponto, "dias_por_cpf", explode)
    assert fx._obra_do_ponto_por_cpf(
        2026, 9, dt.date(2026, 9, 1), dt.date(2026, 9, 30)) == {}


def test_a_pessoa_guarda_DE_ONDE_veio_a_obra():
    """⚠️ Sem a origem, obra de cadastro desatualizado paga pela conta errada e
    ninguém tem como desconfiar."""
    import datetime as dt

    from app.apps.analisesps import folha_auxilio as fx

    ficha = {"cpf": "99713349334", "nome": "GERLANIO", "fase": "Ativos",
             "valor_alimentacao": "15.00", "modo_alimentacao": "Segunda à Sexta",
             "obra_cadastro": "CREPEOLINDA"}
    do_ponto = fx.calcular_pessoa(
        fx.ALIMENTACAO, ficha, dt.date(2026, 9, 1), dt.date(2026, 9, 30),
        codigo_da_obra="AAA", obra_do_ponto="AAA", dias_na_obra=18)
    assert do_ponto["obra_de_onde"] == "ponto"
    assert do_ponto["dias_na_obra"] == 18
    assert do_ponto["fase"] == "Ativos"

    do_cadastro = fx.calcular_pessoa(
        fx.ALIMENTACAO, ficha, dt.date(2026, 9, 1), dt.date(2026, 9, 30),
        codigo_da_obra="CRE1")
    assert do_cadastro["obra_de_onde"] == "cadastro"
    assert do_cadastro["dias_na_obra"] == 0

    sem_nada = fx.calcular_pessoa(
        fx.ALIMENTACAO, ficha, dt.date(2026, 9, 1), dt.date(2026, 9, 30))
    assert sem_nada["obra_de_onde"] == ""


# ---------------------------------------------------------------------------
# 03/10/2026 — SAÍDA PROPORCIONAL, AUSÊNCIAS NO PONTO E VALOR ACRESCENTADO
# ---------------------------------------------------------------------------
def _dia(data, presenca="", falta="", obras=()):
    return {"data": data, "marcacoes": list(obras) or ["", "", "", ""],
            "presenca": presenca, "falta": falta}


def test_quem_SAIU_durante_a_competencia_NAO_recebe(banco_auxilio):
    """Dono, 03/10/2026, vendo alguém desligado em 19/09 na alimentação de
    09/2026: *"Se ele já saiu, ele não recebe mais."* O auxílio da competência é
    pago no mês seguinte e é o benefício daquele mês."""
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx
    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(
        situacao=col.SITUACAO_SAIU, data_saida=dt.date(2026, 9, 19)), INICIO, FIM)
    assert r["pagar"] is False
    assert r["saida_no_mes"] is None


def test_quem_SAI_no_MES_DO_PAGAMENTO_recebe_proporcional(banco_auxilio):
    """*"Proporcionalize o pagamento considerando data de saída"* — a parte do
    mês do pagamento (10/2026) até a saída."""
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx
    # Outubro/2026: 22 dias úteis; até 15/10 (quinta), 11.
    r = fx.calcular_pessoa(fx.TRANSPORTE, ficha(
        situacao=col.SITUACAO_SAINDO, data_saida=dt.date(2026, 10, 15)), INICIO, FIM)
    assert r["pagar"] is True
    assert r["valor"] == D("110.00"), "220 (22 dias × 10) × 11/22"
    assert r["proporcao"] == "11/22 dias úteis até 15/10"
    assert any("mês do pagamento" in m for m in r["motivos"])

    fixo = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(
        situacao=col.SITUACAO_SAINDO, data_saida=dt.date(2026, 10, 15),
        modo_alimentacao="Mês", valor_alimentacao=D("310.00")), INICIO, FIM)
    assert fixo["valor"] == D("150.00"), "310 × 15/31 dias corridos"

    depois = fx.calcular_pessoa(fx.TRANSPORTE, ficha(
        situacao=col.SITUACAO_SAINDO, data_saida=dt.date(2026, 11, 20)), INICIO, FIM)
    assert depois["valor"] == D("220.00"), "sai depois do mês do pagamento: inteiro"

def test_ULTIMO_DIA_trabalhado_na_competencia_sem_saida_NAO_recebe():
    """05/10/2026: sem data de saída, mas com o último dia trabalhado dentro da
    competência, a pessoa já saiu — *"se ele já saiu, ele não recebe mais"*. Ela
    ficava "em desligamento", recebendo, e aparecia entre os "sem obra"."""
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx
    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(
        situacao=col.SITUACAO_SAINDO, ultimo_dia=dt.date(2026, 9, 18)), INICIO, FIM)
    assert r["pagar"] is False and r["desligado"]
    assert "18/09/2026" in " ".join(r["motivos"])
    # Último dia DEPOIS da competência: segue recebendo, com o aviso.
    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(
        situacao=col.SITUACAO_SAINDO, ultimo_dia=dt.date(2026, 10, 3)), INICIO, FIM)
    assert r["pagar"] is True


def test_o_filtro_SEM_OBRA_nao_traz_os_DESLIGADOS():
    """05/10/2026: *"quero tratar somente os que devem receber, mas estão sem obra.
    O filtro que tem exibe os que estão desligados."* O escondido continua
    escondido quando se filtra outra situação; aparece se ele for marcado."""
    from werkzeug.datastructures import MultiDict
    from app.apps.analisesps import folha_lista
    vai_sem_obra = {"cpf": "1", "nome": "A", "pagar": True, "sem_obra": True,
                    "pagar_calculado": True}
    saiu_sem_obra = {"cpf": "2", "nome": "B", "pagar": True, "sem_obra": True,
                     "desligado": True, "pagar_calculado": False}
    pessoas = [vai_sem_obra, saiu_sem_obra]
    lista = folha_lista.filtrar(pessoas, MultiDict([("situacao", "sem_obra")]),
                                campo_da_obra="obra",
                                escondidas=folha_lista.ESCONDIDAS_NOS_AUXILIOS)
    assert [p["cpf"] for p in lista["pessoas"]] == ["1"]
    lista = folha_lista.filtrar(pessoas, MultiDict([("situacao", "sem_obra"),
                                                    ("situacao", "saiu")]),
                                campo_da_obra="obra",
                                escondidas=folha_lista.ESCONDIDAS_NOS_AUXILIOS)
    assert [p["cpf"] for p in lista["pessoas"]] == ["1", "2"]


def test_AUSENCIA_e_falta_ou_atestado_sem_marcacao_e_nao_folga():
    from app.apps.analisesps import folha_auxilio as fx
    d = dt.date(2026, 9, 8)
    assert fx.ausencia_do_dia(_dia(d, "FALTA NÃO JUSTIFICADA")) == "FALTA NÃO JUSTIFICADA"
    assert fx.ausencia_do_dia(_dia(d, "", "ATESTADO MÉDICO")) == "ATESTADO MÉDICO"
    assert fx.ausencia_do_dia(_dia(d, "FÉRIAS")) == "", "férias já descontam à parte"
    assert fx.ausencia_do_dia(_dia(d, "FOLGA")) == ""
    assert fx.ausencia_do_dia(_dia(d, "")) == "", "sem nada escrito não se desconta"
    assert fx.ausencia_do_dia(_dia(d, "PRESENÇA", obras=["1042", "1042", "", ""])) == ""


def test_as_ausencias_contam_so_nos_dias_da_modalidade():
    from app.apps.analisesps import folha_auxilio as fx
    sexta, segunda = dt.date(2026, 9, 11), dt.date(2026, 9, 14)
    dias = [_dia(sexta, "FALTA NÃO JUSTIFICADA"), _dia(segunda, "ATESTADO")]
    assert len(fx.ausencias_do_mes(dias, INICIO, FIM, "Segunda à Sexta")) == 2
    assert len(fx.ausencias_do_mes(dias, INICIO, FIM, "Segunda à Quinta")) == 1


def test_o_desconto_das_ausencias_e_PROPOSTO_e_so_vale_APLICADO(banco_auxilio):
    """*"que isso fosse uma opção de aplicar ou não o desconto"* — e só no
    transporte."""
    from app.apps.analisesps import folha_auxilio as fx
    ponto = [_dia(dt.date(2026, 9, 8), "FALTA NÃO JUSTIFICADA"),
             _dia(dt.date(2026, 9, 9), "ATESTADO")]
    proposto = fx.calcular_pessoa(fx.TRANSPORTE, ficha(), INICIO, FIM,
                                  ausencias=ponto)
    assert proposto["valor"] == D("220.00"), "sem aplicar, não desconta"
    assert proposto["desconto_proposto"] == D("20.00")
    assert len(proposto["ausencias"]) == 2

    aplicado = fx.calcular_pessoa(fx.TRANSPORTE, ficha(), INICIO, FIM,
                                  {"desconto_ausencias": True}, ausencias=ponto)
    assert aplicado["valor"] == D("200.00")
    assert aplicado["desconto_aplicado"] is True

    # NAS DUAS VERBAS desde 05/10/2026 (dono: *"é meio que espelho uma coisa da
    # outra"*): a alimentação também propõe e só desconta aplicado.
    alimentacao = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                                     ausencias=ponto)
    assert len(alimentacao["ausencias"]) == 2 and alimentacao["valor"] == D("330.00")
    assert alimentacao["desconto_proposto"] == D("30.00")
    aplicada = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                                  {"desconto_ausencias": True}, ausencias=ponto)
    assert aplicada["valor"] == D("300.00") and aplicada["desconto_aplicado"]


def test_no_MENSAL_o_dia_ausente_vale_o_mes_pelos_dias_uteis(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx
    r = fx.calcular_pessoa(fx.TRANSPORTE, ficha(
        modo_transporte="Mensal", valor_transporte=D("220.00")), INICIO, FIM,
        {"desconto_ausencias": True},
        ausencias=[_dia(dt.date(2026, 9, 8), "FALTA")])
    assert r["desconto_proposto"] == D("10.00"), "220 ÷ 22 dias úteis"
    assert r["valor"] == D("210.00")


def test_o_VALOR_ACRESCENTADO_soma_e_diz_o_motivo(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx
    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           {"valor_extra": D("50.00"), "motivo_extra": "agosto"})
    assert r["valor"] == D("380.00") and r["valor_calculado"] == D("330.00")
    assert r["motivo_extra"] == "agosto"


def test_gravar_extras_NAO_apaga_a_selecao_e_a_selecao_nao_apaga_o_extra(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx
    fx.gravar_ajuste(fx.TRANSPORTE, 2026, 9, GERLANIO, pagar=False)
    fx.gravar_extras(fx.TRANSPORTE, 2026, 9, [GERLANIO], valor_extra="1.234,50",
                     motivo_extra="esquecido em agosto")
    fx.gravar_extras(fx.TRANSPORTE, 2026, 9, GERLANIO, desconto_ausencias=True)
    g = fx.ajustes_do_mes(fx.TRANSPORTE, 2026, 9)[GERLANIO]
    assert g["pagar"] is False
    assert g["valor_extra"] == D("1234.50") and g["motivo_extra"] == "esquecido em agosto"
    assert g["desconto_ausencias"] is True

    fx.limpar_ajuste(fx.TRANSPORTE, 2026, 9, GERLANIO)
    g = fx.ajustes_do_mes(fx.TRANSPORTE, 2026, 9)[GERLANIO]
    assert g["pagar"] is None, "a escolha de pagar volta ao cálculo"
    assert g["valor_extra"] == D("1234.50"), "o valor acrescentado fica"

    fx.gravar_extras(fx.TRANSPORTE, 2026, 9, GERLANIO, valor_extra=None,
                     desconto_ausencias=None)
    fx.limpar_ajuste(fx.TRANSPORTE, 2026, 9, GERLANIO)
    assert fx.ajustes_do_mes(fx.TRANSPORTE, 2026, 9) == {}


def test_DESCONTO_PARCIAL_releva_os_dias_justificados(banco_auxilio):
    """Dono, 05/10/2026: *"pode ser que de 5 dias, um tenha justificativa e vamos
    descontar somente 4"*."""
    from app.apps.analisesps import folha_auxilio as fx
    ponto = [_dia(dt.date(2026, 9, 8), "FALTA NÃO JUSTIFICADA"),
             _dia(dt.date(2026, 9, 9), "ATESTADO"),
             _dia(dt.date(2026, 9, 10), "FALTA")]
    r = fx.calcular_pessoa(fx.TRANSPORTE, ficha(), INICIO, FIM,
                           {"desconto_ausencias": True,
                            "ausencias_relevadas": "2026-09-09",
                            "motivo_relevadas": "atestado entregue"}, ausencias=ponto)
    assert r["desconto_proposto"] == D("30.00"), "o proposto é de todos os dias"
    assert r["dias_descontados"] == 2 and r["desconto_valor"] == D("20.00")
    assert r["valor"] == D("200.00")
    assert [a["descontar"] for a in r["ausencias"]] == [True, False, True]
    assert r["motivo_relevadas"] == "atestado entregue"

    # Gravado pela tela: os dias e a justificativa; sem justificativa, recusa.
    with pytest.raises(fx.ErroDoAuxilio):
        fx.gravar_extras(fx.TRANSPORTE, 2026, 9, GERLANIO, desconto_ausencias=True,
                         ausencias_relevadas=["2026-09-09"])
    fx.gravar_extras(fx.TRANSPORTE, 2026, 9, GERLANIO, desconto_ausencias=True,
                     ausencias_relevadas=["2026-09-09"], motivo_relevadas="atestado")
    g = fx.ajustes_do_mes(fx.TRANSPORTE, 2026, 9)[GERLANIO]
    assert g["ausencias_relevadas"] == "2026-09-09" and g["motivo_relevadas"] == "atestado"
    # Desfazer o desconto limpa a escolha dos dias.
    fx.gravar_extras(fx.TRANSPORTE, 2026, 9, GERLANIO, desconto_ausencias=None)
    g = fx.ajustes_do_mes(fx.TRANSPORTE, 2026, 9)[GERLANIO]
    assert g["ausencias_relevadas"] == "" and g["desconto_ausencias"] is None


def test_o_RELATORIO_detalha_cada_ausencia_e_o_que_se_decidiu(banco_auxilio):
    """Dono, 05/10/2026: *"é importante que as informações e detalhamento estejam
    nos relatórios, visto que talvez precisemos encaminhar a alguém para
    analisar"*."""
    import io
    import openpyxl
    from app.apps.analisesps import folha_auxilio as fx, folha_relatorio as fr
    ponto = [_dia(dt.date(2026, 9, 8), "FALTA NÃO JUSTIFICADA"),
             _dia(dt.date(2026, 9, 9), "ATESTADO")]
    p = fx.calcular_pessoa(fx.TRANSPORTE, ficha(), INICIO, FIM,
                           {"desconto_ausencias": True, "ausencias_relevadas": "2026-09-09",
                            "motivo_relevadas": "atestado entregue"}, ausencias=ponto)
    p["obra"] = "CREPEOLINDA"
    montado = fr.montado_do_auxilio({"pessoas": [p]}, [p], {}, {}, "Auxílio transporte",
                                    "09/2026")
    dados = fr.montar(montado, {})
    detalhe = dados["pessoas"][0]["ausencias_detalhe"]
    assert [a["situacao"] for a in detalhe] == [
        "descontada", "não descontada — atestado entregue"]
    assert "desconto de 1 de 2 ausência(s)" in dados["pessoas"][0]["situacao_rotulo"]
    aba = openpyxl.load_workbook(io.BytesIO(fr.excel(dados)))["Ausências"]
    assert aba["D2"].value == "08/09/2026" and aba["F3"].value.startswith("não descontada")
    assert fr.pdf(dados)[:5] == b"%PDF-"

    # Sem aplicar: "a confirmar", e o rótulo diz o proposto.
    p2 = fx.calcular_pessoa(fx.TRANSPORTE, ficha(), INICIO, FIM, ausencias=ponto)
    p2["obra"] = "CREPEOLINDA"
    m2 = fr.montado_do_auxilio({"pessoas": [p2]}, [p2], {}, {}, "Auxílio transporte", "09/2026")
    assert {a["situacao"] for a in m2["pessoas"][0]["ausencias_detalhe"]} == {"a confirmar"}
    assert "desconto a confirmar" in m2["pessoas"][0]["situacao_rotulo"]


def test_desconto_de_ausencia_na_ALIMENTACAO_tambem_grava(banco_auxilio):
    """Era recusado até 05/10/2026 (*"só para o transporte"*); agora as duas
    verbas são espelho."""
    from app.apps.analisesps import folha_auxilio as fx
    fx.gravar_extras(fx.ALIMENTACAO, 2026, 9, GERLANIO, desconto_ausencias=True)
    assert fx.ajustes_do_mes(fx.ALIMENTACAO, 2026, 9)[GERLANIO]["desconto_ausencias"] is True


def test_SEM_PONTO_e_PENDENCIA_e_a_obra_do_cadastro_e_so_SUGESTAO(banco_auxilio):
    """Dono, 03/10/2026: *"Não utilizar obra de cadastro automático, precisa ser
    ajustado (…) vamos selecionar e isso precisa ter destaque, já que é
    pendência."*"""
    from app.apps.analisesps import folha_auxilio as fx
    cadastrar((GERLANIO, "UM", {"modo_alimentacao": "Mês",
                                "valor_alimentacao": D("100.00"),
                                "obra_codigo": "OBRAA"}))
    p = fx.calcular(fx.ALIMENTACAO, 2026, 9)["pessoas"][0]
    assert p["obra"] == "" and p["sem_obra"] is True
    assert p["obra_do_cadastro"] == "OBRAA", "vai como sugestão"
    with pytest.raises(fx.ErroDoAuxilio, match="sem obra do ponto"):
        fx.fechar(fx.ALIMENTACAO, 2026, 9, "fim_de_mes")

    fx.gravar_extras(fx.ALIMENTACAO, 2026, 9, GERLANIO, obra="obrab")
    p = fx.calcular(fx.ALIMENTACAO, 2026, 9)["pessoas"][0]
    assert p["obra"] == "OBRAB" and p["obra_de_onde"] == "mao" and not p["sem_obra"]
    # Salvar a seleção (voltar ao cálculo) NÃO apaga a obra escolhida.
    fx.salvar_selecao(fx.ALIMENTACAO, 2026, 9, [{"cpf": GERLANIO, "pagar": True}])
    assert fx.calcular(fx.ALIMENTACAO, 2026, 9)["pessoas"][0]["obra"] == "OBRAB"


def test_VALOR_NEGATIVO_reduz_e_o_total_nunca_fica_negativo(banco_auxilio):
    """*"O botão mais valor deve aceitar também número negativo pra reduzir."*"""
    from app.apps.analisesps import folha_auxilio as fx
    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           {"valor_extra": D("-30.00"), "motivo_extra": "devolução"})
    assert r["valor"] == D("300.00")
    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           {"valor_extra": D("-999.00")})
    assert r["valor"] == D("0.00")
    fx.gravar_extras(fx.ALIMENTACAO, 2026, 9, GERLANIO, valor_extra="-50,00",
                     motivo_extra="pago a mais em agosto")
    assert fx.ajustes_do_mes(fx.ALIMENTACAO, 2026, 9)[GERLANIO]["valor_extra"] == D("-50.00")
