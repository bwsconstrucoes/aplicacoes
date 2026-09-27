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
def banco_auxilio(banco, monkeypatch):
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
    assert "desmarcou" in " ".join(r["motivos"])


def test_MARCAR_quem_o_calculo_recusou_POR_POLITICA_vale(banco_auxilio):
    """A última palavra é dele: quem saiu não recebe por decisão, e se ele decidir
    que é devido, paga — pelo valor calculado, não por zero."""
    from app.apps.analisesps import colaboradores as col, folha_auxilio as fx

    r = fx.calcular_pessoa(
        fx.ALIMENTACAO, ficha(situacao=col.SITUACAO_SAIU), INICIO, FIM,
        ajuste={"pagar": True})
    assert r["pagar"] is True
    assert "mesmo assim" in " ".join(r["motivos"])
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
    assert "falta o dado no cadastro" in " ".join(r["motivos"])


def test_ajuste_NULO_e_diferente_de_FALSO(banco_auxilio):
    """⚠️ Nulo é "não mexi" — vale o cálculo. Falso é "decidi não pagar".
    Confundir os dois faria o padrão virar decisão."""
    from app.apps.analisesps import folha_auxilio as fx

    assert fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                              ajuste={"pagar": None})["pagar"] is True


def test_a_OBRA_ajustada_ganha_da_do_cadastro(banco_auxilio):
    """*"Caso eu queira alterar a obra que aquela pessoa vai ficar apropriada."*"""
    from app.apps.analisesps import folha_auxilio as fx

    r = fx.calcular_pessoa(fx.ALIMENTACAO, ficha(), INICIO, FIM,
                           ajuste={"obra": "CREPEAREIAS"})
    assert r["obra"] == "CREPEAREIAS"
    assert r["obra_ajustada"] is True


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
        r.update({"cpf": cpf, "nome": nome, "valor_alimentacao": None,
                  "valor_transporte": None, "valor_gratificacao": None,
                  "aviso_previo": None, "ultimo_dia": None, "data_saida": None,
                  "fase": "Colaboradores Ativos"})
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

    cadastrar(
        (GERLANIO, "UM", {"modo_alimentacao": "Mês",
                          "valor_alimentacao": D("100.00"),
                          "obra_cadastro": "OBRAA"}),
        ("03513441363", "DOIS", {"modo_alimentacao": "Mês",
                                 "valor_alimentacao": D("300.00"),
                                 "obra_cadastro": "OBRAB"}),
        ("11144477735", "TRES", {"modo_alimentacao": "Mês",
                                 "valor_alimentacao": D("50.00"),
                                 "obra_cadastro": "OBRAA"}),
    )
    resultado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    assert resultado["total"] == D("450.00")
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
