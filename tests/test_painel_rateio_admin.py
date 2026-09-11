# -*- coding: utf-8 -*-
"""
Rateio da Administração — a conta.

Duas repartições diferentes acontecem aqui, e confundi-las seria o erro caro:

  - o **custo da matriz** vai pelo CRITÉRIO (faturamento ou pessoal): quem
    produziu mais carrega mais estrutura;
  - os **juros do banco** vão pelo DÉFICIT: quem estava com o caixa negativo
    naquele mês. Juros não é estrutura, é o preço de faltar dinheiro.

Nenhum teste aqui abre banco: a função recebe listas e devolve listas.
"""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.painel import rateio_admin


M1, M2, M3 = dt.date(2025, 1, 1), dt.date(2025, 2, 1), dt.date(2025, 3, 1)
MATRIZ = "ADM"
OBRAS = ["CASA", "PONTE", MATRIZ]
PROJETOS = {"CASA": "ALFA", "PONTE": "BETA", MATRIZ: "ADM"}
SO_A = [("obra:CASA", 100)]


def _simular(**extra):
    base = dict(
        operacional=[(M1, "CASA", 1000.0), (M1, "PONTE", -400.0)],
        receita=[(M1, "CASA", 3000.0), (M1, "PONTE", 1000.0)],
        pessoal=[(M1, "CASA", 100.0), (M1, "PONTE", 300.0)],
        matriz=[(M1, -1000.0, 0.0)],
        juros=[],
        escolhas_a=SO_A, mapa_projeto=PROJETOS, obras=OBRAS,
        depto_matriz=MATRIZ,
    )
    base.update(extra)
    return rateio_admin.simular(**base)


# ===========================================================================
# 1. Os dois lados, e a matriz fora deles
# ===========================================================================
def test_a_matriz_nao_entra_em_nenhum_dos_lados():
    """Ela é o bolo a repartir. Se entrasse num lado, receberia rateio de si
    mesma e o número deixaria de significar qualquer coisa."""
    r = _simular()
    assert MATRIZ not in r["pesos_a"]
    # e o que sobrou para B é só a outra obra
    assert set(r["pesos_a"]) == {"CASA"}


def test_o_lado_b_e_o_complemento_de_a():
    """Meia obra em A significa a outra meia em B — não a obra inteira nos
    dois."""
    r = _simular(escolhas_a=[("obra:CASA", 50)])
    linha = r["linhas"][0]
    # operacional de CASA (1.000) dividido meio a meio; PONTE inteira em B
    assert linha["operacional_a"] == pytest.approx(500.0)
    assert linha["operacional_b"] == pytest.approx(500.0 - 400.0)


def test_projeto_se_abre_nas_obras_dele():
    r = _simular(escolhas_a=[("projeto:ALFA", 100)])
    assert set(r["pesos_a"]) == {"CASA"}


# ===========================================================================
# 2. O bolo da matriz
# ===========================================================================
def test_o_bolo_nunca_vira_positivo():
    """Matriz que num mês recebeu mais do que gastou não distribui lucro —
    isso viraria uma sobra fantasma no caixa das obras."""
    r = _simular(matriz=[(M1, -100.0, 500.0)])
    assert r["linhas"][0]["pool"] == 0.0


def test_a_receita_da_matriz_abate_o_bolo_quando_pedido():
    com = _simular(matriz=[(M1, -1000.0, 300.0)],
                   abater_receita_da_matriz=True)["linhas"][0]["pool"]
    sem = _simular(matriz=[(M1, -1000.0, 300.0)],
                   abater_receita_da_matriz=False)["linhas"][0]["pool"]
    assert com == pytest.approx(-700.0)
    assert sem == pytest.approx(-1000.0)


def test_o_valor_fixo_por_mes_SAI_do_bolo():
    """É a parte que NÃO deve ser rateada — tipicamente a fatia dos salários da
    matriz que pertence a um lado só. Entra em módulo: informar 200 tira 200 do
    bolo, qualquer que seja o sinal digitado."""
    assert _simular(valor_fixo_por_mes=200.0)["linhas"][0]["pool"] == pytest.approx(-800.0)
    assert _simular(valor_fixo_por_mes=-200.0)["linhas"][0]["pool"] == pytest.approx(-800.0)


def test_o_bolo_e_repartido_inteiro_entre_os_dois_lados():
    """A invariante: rateio move custo, não cria nem apaga."""
    r = _simular()
    for linha in r["linhas"]:
        assert linha["matriz_a"] + linha["matriz_b"] == pytest.approx(linha["pool"])


# ===========================================================================
# 3. O critério — quem carrega mais estrutura
# ===========================================================================
def test_por_faturamento_quem_recebeu_mais_carrega_mais():
    """CASA recebeu 3.000 e PONTE 1.000: 75% da matriz vai para A."""
    r = _simular(criterio="faturamento", janela="1")
    linha = r["linhas"][0]
    assert linha["pct_matriz_a"] == pytest.approx(75.0)
    assert linha["matriz_a"] == pytest.approx(-750.0)


def test_por_pessoal_a_conta_inverte():
    """CASA tem 100 de pessoal e PONTE 300: agora A carrega 25%."""
    r = _simular(criterio="pessoal", janela="1")
    assert r["linhas"][0]["pct_matriz_a"] == pytest.approx(25.0)


def test_percentual_fixo_ignora_os_dois_criterios():
    r = _simular(criterio="fixo", pct_fixo=30.0)
    assert r["linhas"][0]["pct_matriz_a"] == pytest.approx(30.0)
    assert r["linhas"][0]["matriz_a"] == pytest.approx(-300.0)


def test_a_janela_soma_os_meses_anteriores():
    """Janela de 3 meses: um mês fraco não vira uma virada de participação."""
    tres = [(M1, -300.0, 0.0), (M2, -300.0, 0.0), (M3, -300.0, 0.0)]
    receita = [(M1, "CASA", 1000.0), (M2, "PONTE", 1000.0), (M3, "PONTE", 1000.0)]
    curta = _simular(matriz=tres, receita=receita, janela="1")["linhas"]
    longa = _simular(matriz=tres, receita=receita, janela="3")["linhas"]
    # no 3º mês, a janela curta olha só março (só PONTE recebeu) -> A com 0%
    assert curta[2]["pct_matriz_a"] == pytest.approx(0.0)
    # a janela de 3 ainda vê os 1.000 de CASA em janeiro -> A com 1/3
    assert longa[2]["pct_matriz_a"] == pytest.approx(33.33, abs=0.1)


def test_mes_sem_movimento_repete_a_ultima_participacao():
    """Não se inventa meio a meio: a empresa não mudou de perfil só porque um
    mês foi fraco."""
    r = _simular(matriz=[(M1, -100.0, 0.0), (M2, -100.0, 0.0)],
                 receita=[(M1, "CASA", 1000.0)], janela="1")
    assert r["linhas"][0]["pct_matriz_a"] == pytest.approx(100.0)
    assert r["linhas"][1]["pct_matriz_a"] == pytest.approx(100.0)


def test_sem_nenhum_mes_valido_divide_meio_a_meio():
    r = _simular(receita=[], pessoal=[], janela="1")
    assert r["linhas"][0]["pct_matriz_a"] == pytest.approx(50.0)


# ===========================================================================
# 4. Os juros — a outra repartição, e a que mais importa acertar
# ===========================================================================
def test_os_juros_vao_para_quem_estava_negativo():
    """O ponto da tela. A carrega o operacional positivo, B o negativo: os
    juros do mês são de B, mesmo que o critério mandasse o contrário."""
    r = _simular(
        operacional=[(M1, "CASA", 5000.0), (M1, "PONTE", -5000.0)],
        matriz=[(M1, 0.0, 0.0)],
        juros=[(M1, -100.0)],
        criterio="fixo", pct_fixo=90.0)
    linha = r["linhas"][0]
    assert linha["deficit_a"] == pytest.approx(0.0)
    assert linha["deficit_b"] > 0
    assert linha["juros_a"] == pytest.approx(0.0)
    assert linha["juros_b"] == pytest.approx(-100.0)


def test_os_juros_se_dividem_na_proporcao_do_buraco():
    """Dois lados negativos: quem cavou mais fundo paga mais."""
    r = _simular(
        operacional=[(M1, "CASA", -1000.0), (M1, "PONTE", -3000.0)],
        matriz=[(M1, 0.0, 0.0)], juros=[(M1, -400.0)])
    linha = r["linhas"][0]
    assert linha["juros_a"] == pytest.approx(-100.0)   # 1/4 do buraco
    assert linha["juros_b"] == pytest.approx(-300.0)


def test_sem_ninguem_negativo_os_juros_seguem_o_criterio():
    r = _simular(operacional=[(M1, "CASA", 5000.0), (M1, "PONTE", 5000.0)],
                 matriz=[(M1, 0.0, 0.0)], juros=[(M1, -100.0)],
                 criterio="fixo", pct_fixo=70.0,
                 juros_sem_deficit="criterio")
    assert r["linhas"][0]["juros_a"] == pytest.approx(-70.0)


def test_ou_nao_sao_alocados_a_ninguem():
    r = _simular(operacional=[(M1, "CASA", 5000.0), (M1, "PONTE", 5000.0)],
                 matriz=[(M1, 0.0, 0.0)], juros=[(M1, -100.0)],
                 juros_sem_deficit="nao_alocar")
    linha = r["linhas"][0]
    assert linha["juros_a"] == 0.0 and linha["juros_b"] == 0.0


def test_o_deficit_dos_juros_e_medido_ANTES_dos_juros():
    """Sem isso a conta seria circular: os juros mudariam o déficit que decide
    quem paga os juros."""
    r = _simular(operacional=[(M1, "CASA", -100.0)], matriz=[(M1, 0.0, 0.0)],
                 juros=[(M1, -1000.0)])
    linha = r["linhas"][0]
    # o déficit é 100 (o operacional), não 1.100
    assert linha["deficit_a"] == pytest.approx(100.0)
    # e a posição final é que carrega os juros
    assert linha["final_a"] == pytest.approx(-1100.0)


# ===========================================================================
# 5. Ajustes e fechamento
# ===========================================================================
def test_o_ajuste_de_caixa_entra_no_mes_e_fica_no_acumulado():
    """Recurso que existe mas não está na base — caixa gerado antes da série."""
    r = _simular(matriz=[(M1, -100.0, 0.0), (M2, -100.0, 0.0)],
                 ajustes=[("A", M1, 5000.0)])
    assert r["linhas"][0]["ajuste_a"] == pytest.approx(5000.0)
    assert r["linhas"][1]["caixa_a"] > 4000

def test_a_posicao_final_e_o_caixa_mais_os_juros_acumulados():
    r = _simular(operacional=[(M1, "CASA", -1000.0)], matriz=[(M1, 0.0, 0.0)],
                 juros=[(M1, -200.0)])
    linha = r["linhas"][0]
    assert linha["final_a"] == pytest.approx(linha["caixa_a"] + linha["juros_a"])


def test_sem_dado_nenhum_nao_quebra():
    r = rateio_admin.simular([], [], [], [], [], SO_A, PROJETOS, OBRAS,
                             depto_matriz=MATRIZ)
    assert r["vazio"] is True and r["linhas"] == []


def test_o_resumo_fecha_com_a_soma_das_linhas():
    r = _simular(matriz=[(M1, -100.0, 0.0), (M2, -300.0, 0.0)],
                 juros=[(M1, -50.0)])
    resumo = r["resumo"]
    assert resumo["matriz_a"] + resumo["matriz_b"] == pytest.approx(
        sum(l["pool"] for l in r["linhas"]))
    assert resumo["juros_a"] + resumo["juros_b"] == pytest.approx(
        sum(l["juros_mes"] for l in r["linhas"]))
    assert resumo["meses"] == 2


# ===========================================================================
# 6. A tela
# ===========================================================================
@pytest.fixture()
def painel_rateio(monkeypatch):
    """A tela com o banco dublado — aqui só se confere que ela monta."""
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    import tests.test_painel as base
    from app.apps.painel import consultas, prestacao_dados
    monkeypatch.setattr(consultas, "consultar", base._consultar_falso)
    monkeypatch.setattr(prestacao_dados, "config", lambda: dict(base.CONFIG_FALSA))
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    c = app.test_client()
    c.post("/painel/entrar", data={"senha": "segredo-de-teste"})
    return c


def test_a_tela_pede_o_lado_a_antes_de_calcular(painel_rateio):
    html = painel_rateio.get("/painel/rateio-administracao").get_data(as_text=True)
    assert "Monte o Lado A" in html


def test_a_tela_explica_as_duas_reparticoes(painel_rateio):
    """É a distinção que a tela existe para ensinar: estrutura vai pelo
    critério, juros vão pelo déficit."""
    html = painel_rateio.get("/painel/rateio-administracao").get_data(as_text=True)
    assert "custo da matriz" in html and "critério" in html
    assert "juros do banco" in html and "déficit" in html


def test_a_tela_do_rateio_nao_esta_no_menu_principal():
    """Como o explorador: é ferramenta de simulação, não relatório. Que o link
    EXISTE em Configurações é conferido em `test_painel_banco_telas.py`, onde a
    tela de Configurações tem banco de verdade para montar."""
    from app.apps.painel.web import ABAS
    assert not any("rateio" in rota for _, _, rota in ABAS)


def test_o_rateio_exige_login(painel_rateio):
    painel_rateio.get("/painel/sair")
    assert painel_rateio.get("/painel/rateio-administracao").status_code == 302


def test_zero_por_cento_nao_aparece_como_menos_zero():
    """Detalhe de tela, mas confunde: 0 dividido por um total negativo dá
    "-0,0%", e quem lê acha que perdeu alguma coisa."""
    r = _simular(operacional=[(M1, "CASA", 5000.0), (M1, "PONTE", -5000.0)],
                 matriz=[(M1, -100.0, 0.0)], juros=[(M1, -100.0)])
    assert str(r["resumo"]["pct_juros_a"])[0] != "-"
