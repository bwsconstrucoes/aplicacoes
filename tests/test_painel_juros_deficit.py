# -*- coding: utf-8 -*-
"""
Os juros de empréstimo na Prestação de Contas: paga quem demandou o caixa.

A régua do custo da estrutura é uma — quem tem mais gente consome mais
administração. A régua do juro é OUTRA, e é o ponto destes testes: juro não é
estrutura, é o preço de ter faltado dinheiro. Obra que se paga sozinha não
deveria pagar juro nenhum; obra que só andou com dinheiro emprestado paga o
juro do mês em que estava no vermelho.

Nenhum teste aqui abre banco: as funções recebem listas e devolvem listas.
"""
from __future__ import annotations

import pytest

from app.apps.painel import prestacao

JAN, FEV, MAR = "2025-01", "2025-02", "2025-03"
OBRAS = {"CASA": {"projeto": "ALFA", "lado": prestacao.FILIAL},
         "PONTE": {"projeto": "ALFA", "lado": prestacao.FILIAL}}


def _alocar(caixa, juros, **extra):
    return prestacao.alocar_juros_por_deficit(caixa, juros, OBRAS, **extra)


def test_obra_no_azul_nao_paga_juro():
    """A regra em uma frase: se a obra não demandou caixa, o juro não é dela."""
    conta = _alocar([(JAN, "CASA", -1000.0), (JAN, "PONTE", 500.0)],
                    {JAN: -100.0})
    assert conta["alocacoes"] == {("CASA", JAN): -100.0}
    assert "PONTE" not in {obra for obra, _mes in conta["alocacoes"]}


def test_quem_deve_o_dobro_paga_o_dobro():
    conta = _alocar([(JAN, "CASA", -1000.0), (JAN, "PONTE", -3000.0)],
                    {JAN: -400.0})
    assert conta["alocacoes"][("CASA", JAN)] == pytest.approx(-100.0)
    assert conta["alocacoes"][("PONTE", JAN)] == pytest.approx(-300.0)


def test_o_buraco_do_mes_passado_continua_valendo():
    """Mês sem movimento não apaga a dívida de caixa: ela só some quando a obra
    recupera o dinheiro. Sem isto, bastaria a obra ficar parada um mês para
    deixar de pagar o juro do buraco que ela mesma cavou."""
    conta = _alocar([(JAN, "CASA", -1000.0), (JAN, "PONTE", 2000.0)],
                    {FEV: -80.0})
    assert conta["alocacoes"] == {("CASA", FEV): -80.0}


def test_obra_que_se_recupera_deixa_de_pagar():
    conta = _alocar([(JAN, "CASA", -1000.0), (FEV, "CASA", 1200.0)],
                    {FEV: -80.0})
    assert conta["alocacoes"] == {}
    assert conta["sobras"][0]["motivo"].startswith("nenhuma obra")


def test_o_rateio_da_estrutura_conta_como_caixa_demandado():
    """A parte da administração que coube à obra é dinheiro que a obra fez a
    empresa gastar. Ignorá-la faria a obra parecer equilibrada às custas da
    matriz — e o juro cairia em quem não o provocou."""
    conta = _alocar([(JAN, "CASA", 0.0), (JAN, "PONTE", 1000.0)],
                    {JAN: -50.0},
                    rateio_recebido={("CASA", JAN): -400.0})
    assert conta["alocacoes"] == {("CASA", JAN): -50.0}


def test_mes_sem_ninguem_no_vermelho_vira_sobra_visivel():
    """O juro existiu e tem de aparecer. Some-lo seria mudar o resultado da
    empresa por causa de uma régua de rateio."""
    conta = _alocar([(JAN, "CASA", 500.0)], {JAN: -70.0})
    assert conta["alocacoes"] == {}
    assert conta["sobras"] == [{"origem": "Juros de empréstimo", "mes": JAN,
                                "valor": -70.0,
                                "motivo": "nenhuma obra estava com o caixa "
                                          "negativo neste mês"}]


def test_sem_deficit_pode_seguir_a_regua_da_estrutura():
    conta = _alocar([(JAN, "CASA", 500.0), (JAN, "PONTE", 500.0)],
                    {JAN: -60.0},
                    rateio_recebido={("CASA", JAN): -300.0,
                                     ("PONTE", JAN): -100.0},
                    sem_deficit="estrutura")
    # O rateio entra no caixa das duas, e as duas continuam positivas: aí sim
    # o juro segue a proporção da estrutura — 3 para 1.
    assert conta["alocacoes"][("CASA", JAN)] == pytest.approx(-45.0)
    assert conta["alocacoes"][("PONTE", JAN)] == pytest.approx(-15.0)


def test_juro_sem_data_nao_e_alocado_e_diz_por_que():
    conta = _alocar([(JAN, "CASA", -1000.0)],
                    {JAN: -100.0, prestacao.SEM_DATA: -30.0})
    assert conta["alocacoes"] == {("CASA", JAN): -100.0}
    assert any(s["mes"] == prestacao.SEM_DATA for s in conta["sobras"])


def test_nada_some_e_nada_nasce():
    """A soma do que foi alocado com o que sobrou é o juro que a empresa pagou.
    Rateio move custo — não o cria nem o apaga."""
    juros = {JAN: -100.0, FEV: -200.0, MAR: -300.0}
    conta = _alocar([(JAN, "CASA", -1000.0), (FEV, "CASA", 3000.0),
                     (FEV, "PONTE", -500.0), (MAR, "PONTE", 900.0)], juros)
    total = (sum(conta["alocacoes"].values())
             + sum(s["valor"] for s in conta["sobras"]))
    assert total == pytest.approx(sum(juros.values()))


def test_departamento_que_nao_e_obra_nao_recebe_juro():
    """A matriz é o bolo a repartir, não destino de rateio. Se ela recebesse
    juro, o custo voltaria para ela e sairia da conta das obras."""
    conta = _alocar([(JAN, "ADM MATRIZ", -5000.0), (JAN, "CASA", -1000.0)],
                    {JAN: -100.0})
    assert conta["alocacoes"] == {("CASA", JAN): -100.0}


# --------------------------------------------------------------------- pool
def _admin(mes, categoria, valor):
    return {"mes": mes, "depto": "ADM MATRIZ", "grupo": "Financeiras",
            "categoria": categoria, "valor": valor}


def test_o_juro_sai_do_bolo_da_estrutura():
    """Deixá-lo no bolo faria o mesmo dinheiro ser repartido duas vezes: uma
    pelo pessoal, junto da administração, e outra pelo déficit."""
    resto, juros = prestacao.separar_juros(
        [_admin(JAN, "Juros sobre Empréstimos", -900.0),
         _admin(JAN, "Aluguel", -2000.0)],
        "Juros sobre Empréstimos")
    assert juros == {JAN: -900.0}
    assert [l["categoria"] for l in resto] == ["Aluguel"]


def test_sem_categoria_configurada_nada_e_separado():
    resto, juros = prestacao.separar_juros([_admin(JAN, "Aluguel", -10.0)], "")
    assert juros == {} and len(resto) == 1


# ------------------------------------------------------------------ apuração
def test_a_apuracao_mostra_estrutura_e_juro_em_colunas_diferentes():
    """São duas réguas: juntá-las numa coluna só esconderia justamente o que o
    dono quer enxergar — obra que carrega estrutura mas não carrega juro."""
    apurado = prestacao.apurar(
        [{"mes": JAN, "obra": "CASA", "projeto": "ALFA",
          "receita_liquida": 1000.0, "retencoes": 0.0, "despesas": -600.0}],
        OBRAS, {("CASA", JAN): -100.0}, {("CASA", JAN): -50.0})
    linha = apurado[0]
    assert linha["rateio"] == -100.0 and linha["juros"] == -50.0
    assert linha["resultado_direto"] == pytest.approx(400.0)
    assert linha["resultado"] == pytest.approx(250.0)


def test_na_parceria_o_juro_entra_na_base_de_todos():
    """O juro é o preço do dinheiro que financiou AQUELA obra — não é estrutura
    da construtora. Quem participa do resultado participa do custo de bancá-la.
    E a soma das quotas continua fechando com o resultado do projeto."""
    por_projeto = {"ALFA": {"receita_bruta": 0.0, "receita_liquida": 0.0,
                            "retencoes": 0.0, "despesas": 0.0,
                            "rateio": -100.0, "juros": -60.0,
                            "resultado_direto": 1000.0, "resultado": 840.0}}
    participacoes = [{"projeto": "ALFA", "socio": "BWS", "tipo": "Interno", "pct": 50},
                     {"projeto": "ALFA", "socio": "Parceiro", "tipo": "Externo", "pct": 50}]
    quotas = prestacao.quotas_por_socio(por_projeto, participacoes,
                                        {"taxa_adm_pct": "0"})
    externo = next(q for q in quotas if q["tipo"] == "Externo")
    assert externo["base"] == pytest.approx(940.0)     # 1000 direto − 60 de juro
    assert sum(q["quota"] for q in quotas) == pytest.approx(840.0)
