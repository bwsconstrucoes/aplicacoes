"""Análise de SPs — o lote, o rateio e o calendário da agenda.

Três coisas que são Python puro e não precisam de banco nem de tela. São
também as três em que um erro passa despercebido por muito tempo: o lote
agrupando errado, o rateio fechando 99,99%, a agenda pondo um imposto num
feriado. Nenhuma delas dá erro na tela — só dá um número errado.
"""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.analisesps import agenda, lote, rateio


# ---------------------------------------------------------------------------
# O lote: separar grupos
# ---------------------------------------------------------------------------
def test_linha_de_texto_vira_titulo_e_numeros_viram_sps():
    grupos = lote.separar_grupos(
        "Pagar amanhã\n1384831053\n1384844943\n\nAguardando\n1384852359")
    assert [g["titulo"] for g in grupos] == ["Pagar amanhã", "Aguardando"]
    assert grupos[0]["ids"] == ["1384831053", "1384844943"]
    assert grupos[1]["ids"] == ["1384852359"]


def test_varios_numeros_na_mesma_linha():
    """Colar de uma planilha traz os números separados por espaço ou vírgula."""
    for separador in (" ", ", ", "; ", "\t"):
        grupos = lote.separar_grupos(separador.join(["111", "222", "333"]))
        assert grupos[0]["ids"] == ["111", "222", "333"]


def test_ids_sem_titulo_ficam_num_grupo_sem_nome():
    grupos = lote.separar_grupos("1384831053\n1384844943")
    assert len(grupos) == 1
    assert grupos[0]["titulo"] is None


def test_titulo_com_numero_no_meio_continua_titulo():
    """"Pagar 15/03" tem número, mas não é uma lista de SPs. A regra é: só é
    lista se TODOS os pedaços forem números."""
    grupos = lote.separar_grupos("Pagar 15/03\n111")
    assert grupos[0]["titulo"] == "Pagar 15/03"
    assert grupos[0]["ids"] == ["111"]


def test_lote_vazio_nao_gera_grupo():
    assert lote.separar_grupos("") == []
    assert lote.separar_grupos("\n\n  \n") == []


# ---------------------------------------------------------------------------
# O lote: extrair SPs de mensagens
# ---------------------------------------------------------------------------
def test_extrai_as_sps_de_uma_mensagem_de_whatsapp():
    texto = ("✅ Solicitação de Pagamento Validada\n"
             "Nº da SP: 1426036778\n"
             "Valor: R$ 1.234,56\n"
             "Outra SP: 1426036779")
    assert lote.extrair_ids(texto) == ["1426036778", "1426036779"]


def test_nao_confunde_telefone_cnpj_valor_nem_data_com_sp():
    """Uma SP tem exatamente 10 dígitos. Telefone tem 11, CNPJ vem em blocos
    menores, e valor e data trazem pontuação no meio."""
    texto = ("telefone 11987654321, CNPJ 01.637.895/0001-32, "
             "valor 1.234,56, data 31/12/2026, CPF 12345678901")
    assert lote.extrair_ids(texto) == []


def test_a_mesma_sp_repetida_entra_uma_vez_so():
    """A mesma SP citada em duas mensagens não pode virar duas linhas do lote."""
    assert (lote.extrair_ids("1426036778 1426036778 1426036779")
            == ["1426036778", "1426036779"])


def test_acrescentar_grupo_poe_no_topo_e_numera():
    novo, titulo = lote.acrescentar_grupo("Antigo\n111", ["222", "333"])
    assert titulo == "Novo Lote 1"
    assert novo.startswith("Novo Lote 1\n222\n333")
    assert "Antigo" in novo                  # o que havia continua lá
    mais_novo, titulo2 = lote.acrescentar_grupo(novo, ["444"])
    assert titulo2 == "Novo Lote 2"          # numera a partir do que existe
    assert mais_novo.index("Novo Lote 2") < mais_novo.index("Novo Lote 1")


# ---------------------------------------------------------------------------
# O lote: tirar as que já saíram
# ---------------------------------------------------------------------------
def test_remover_por_status_tira_so_o_alvo_e_mantem_os_titulos():
    texto = "Pagar amanhã\n111 222\n\nDepois\n333"
    status = {"111": "Pago", "222": "Pagar", "333": "Pago"}
    novo, quantos = lote.remover_por_status(texto, {"pago"}, status)
    assert quantos == 2
    assert "111" not in novo and "333" not in novo
    assert "222" in novo
    # Os títulos ficam, mesmo com o grupo esvaziado — quem montou o lote
    # organizou por algum motivo, e apagar a organização seria pior.
    assert "Pagar amanhã" in novo and "Depois" in novo


def test_remover_por_status_ignora_maiuscula():
    novo, quantos = lote.remover_por_status("111", {"pago"}, {"111": "PAGO"})
    assert quantos == 1 and novo == ""


def test_sp_desconhecida_nao_e_removida():
    """Se a SP não está na base, não dá para saber o status dela. Na dúvida,
    fica — tirar do lote o que ninguém conferiu seria perder trabalho."""
    novo, quantos = lote.remover_por_status("999", {"pago"}, {})
    assert quantos == 0 and "999" in novo


# ---------------------------------------------------------------------------
# Rateio: os percentuais têm de fechar
# ---------------------------------------------------------------------------
def test_o_rateio_fecha_cem_por_cento():
    """Três valores iguais dão 33,333...% cada. Arredondar os três para baixo
    daria 99,99% e o Omie recusaria o lançamento."""
    percentuais = rateio._percentuais_min_erro([100, 100, 100])
    assert sum(percentuais) == pytest.approx(100.0, abs=1e-9)


@pytest.mark.parametrize("valores", [
    [1, 1, 1], [1, 2, 3], [7000, 3000], [0.01, 0.01, 0.01],
    [1234.56, 7890.12, 3456.78], [1] * 7, [1] * 11,
])
def test_o_rateio_fecha_para_qualquer_combinacao(valores):
    assert sum(rateio._percentuais_min_erro(valores)) == pytest.approx(100.0, abs=1e-9)


def test_a_alocacao_fecha_a_base_informada():
    """A soma dos pedaços tem de dar exatamente o valor do título. Um centavo
    sobrando vira uma diferença que alguém vai ter de caçar depois."""
    percentuais = rateio._percentuais_min_erro([1, 1, 1])
    alocado = rateio._alocar_valores(percentuais, 1000.00)
    assert sum(alocado) == pytest.approx(1000.00, abs=1e-9)


@pytest.mark.parametrize("base", [1000.00, 999.99, 0.03, 12345.67])
def test_a_alocacao_fecha_para_qualquer_base(base):
    percentuais = rateio._percentuais_min_erro([1, 1, 1])
    assert sum(rateio._alocar_valores(percentuais, base)) == pytest.approx(base, abs=1e-9)


@pytest.mark.parametrize("texto,esperado", [
    ("1.234,56", 1234.56), ("R$ 1.234,56", 1234.56), ("1234", 1234.0),
    ("1.000", 1000.0), ("(1.000,00)", -1000.0), ("", 0.0), ("abc", 0.0),
])
def test_o_rateio_le_valor_em_padrao_brasileiro(texto, esperado):
    assert rateio._to_float(texto) == pytest.approx(esperado)


# ---------------------------------------------------------------------------
# Agenda: feriados
# ---------------------------------------------------------------------------
def test_calcula_os_feriados_que_dependem_da_pascoa():
    """Carnaval, Sexta-feira Santa e Corpus Christi mudam de data todo ano.
    Errar isso põe um imposto para vencer num feriado."""
    # Páscoa de 2026: 5 de abril.
    feriados = agenda.feriados_nacionais(2026)
    assert dt.date(2026, 4, 3) in feriados      # Sexta-feira Santa
    assert dt.date(2026, 2, 16) in feriados     # Segunda de Carnaval
    assert dt.date(2026, 2, 17) in feriados     # Terça de Carnaval
    assert dt.date(2026, 6, 4) in feriados      # Corpus Christi


def test_os_feriados_fixos_estao_todos_la():
    feriados = agenda.feriados_nacionais(2026)
    for mes, dia in ((1, 1), (4, 21), (5, 1), (9, 7), (10, 12),
                     (11, 2), (11, 15), (11, 20), (12, 25)):
        assert dt.date(2026, mes, dia) in feriados


def test_sabado_e_domingo_nao_sao_dia_util():
    assert not agenda.eh_dia_util(dt.date(2026, 9, 5), set())   # sábado
    assert not agenda.eh_dia_util(dt.date(2026, 9, 6), set())   # domingo
    assert agenda.eh_dia_util(dt.date(2026, 9, 4), set())       # sexta


# ---------------------------------------------------------------------------
# Agenda: o ajuste para dia útil
# ---------------------------------------------------------------------------
def test_posterga_empurra_para_frente():
    # 5/9/2026 é sábado; postergando cai na segunda, dia 7.
    assert agenda.ajustar_dia_util(
        dt.date(2026, 9, 5), "posterga", set()) == dt.date(2026, 9, 7)


def test_antecipa_puxa_para_tras():
    # O mesmo sábado, antecipando, cai na sexta, dia 4.
    assert agenda.ajustar_dia_util(
        dt.date(2026, 9, 5), "antecipa", set()) == dt.date(2026, 9, 4)


def test_o_ajuste_pula_feriado_tambem():
    feriados = {dt.date(2026, 9, 7)}            # Independência, uma segunda
    assert agenda.ajustar_dia_util(
        dt.date(2026, 9, 5), "posterga", feriados) == dt.date(2026, 9, 8)


def test_nenhum_ajuste_deixa_a_data_como_esta():
    sabado = dt.date(2026, 9, 5)
    assert agenda.ajustar_dia_util(sabado, "nenhum", set()) == sabado


def test_imposto_antecipa_e_conta_posterga():
    """Não é preferência: imposto pago depois do vencimento tem multa, então
    quando cai em dia não útil ele anda para trás. O resto anda para frente."""
    for categoria in ("Imposto", "FGTS", "Parcelamento"):
        assert agenda.ajuste_sugerido(categoria) == "antecipa"
    for categoria in ("Conta", "Empréstimo", "Transferência", "Outro"):
        assert agenda.ajuste_sugerido(categoria) == "posterga"


# ---------------------------------------------------------------------------
# Agenda: as ocorrências
# ---------------------------------------------------------------------------
def test_mensal_gera_uma_ocorrencia_por_mes():
    compromisso = {"data_base": "07/01/2026", "recorrencia": "mensal",
                   "dia_mes": "7", "ajuste_dia_util": "nenhum"}
    datas = agenda.ocorrencias(compromisso, dt.date(2026, 1, 1),
                               dt.date(2026, 6, 30), set())
    assert len(datas) == 6
    assert all(d.day == 7 for d in datas)


def test_dia_31_quer_dizer_ultimo_dia_do_mes():
    """Se fosse o dia 31 literal, o compromisso sumiria em fevereiro, abril,
    junho, setembro e novembro — cinco meses por ano sem cobrança."""
    compromisso = {"data_base": "31/01/2026", "recorrencia": "mensal",
                   "dia_mes": "31", "ajuste_dia_util": "nenhum"}
    datas = agenda.ocorrencias(compromisso, dt.date(2026, 1, 1),
                               dt.date(2026, 4, 30), set())
    assert dt.date(2026, 2, 28) in datas      # fevereiro de 2026 tem 28 dias
    assert dt.date(2026, 4, 30) in datas
    assert len(datas) == 4                    # nenhum mês ficou de fora


def test_anual_gera_uma_por_ano():
    compromisso = {"data_base": "10/03/2025", "recorrencia": "anual",
                   "ajuste_dia_util": "nenhum"}
    datas = agenda.ocorrencias(compromisso, dt.date(2025, 1, 1),
                               dt.date(2027, 12, 31), set())
    assert len(datas) == 3


def test_sem_recorrencia_aparece_uma_vez_so_e_no_periodo():
    compromisso = {"data_base": "10/03/2026", "recorrencia": "nenhuma",
                   "ajuste_dia_util": "nenhum"}
    assert len(agenda.ocorrencias(compromisso, dt.date(2026, 1, 1),
                                  dt.date(2026, 12, 31), set())) == 1
    assert agenda.ocorrencias(compromisso, dt.date(2027, 1, 1),
                              dt.date(2027, 12, 31), set()) == []


def test_sem_data_base_nao_gera_nada():
    """Dado incompleto na planilha não pode estourar a tela."""
    assert agenda.ocorrencias({"data_base": "", "recorrencia": "mensal"},
                              dt.date(2026, 1, 1), dt.date(2026, 12, 31),
                              set()) == []


def test_a_tela_guarda_a_data_original_quando_a_ocorrencia_foi_movida(monkeypatch):
    """Quem vê a data mudada precisa saber de onde ela veio, senão acha que o
    sistema errou.

    `ocorrencias()` devolve só a data já ajustada — a original se perde ali
    dentro. Quem monta a tela refaz os dois passos para as duas sobreviverem, e
    é isso que este teste fixa."""
    # 5/9/2026 é um sábado. Postergando cairia na segunda, dia 7 — mas 7 de
    # setembro é a Independência. Então anda mais um e cai na terça, dia 8.
    # É justamente o encadeamento (fim de semana + feriado) que erra fácil.
    compromisso = {"id": "1", "titulo": "Aluguel", "categoria": "Conta",
                   "data_base": "05/09/2026", "recorrencia": "nenhuma",
                   "ajuste_dia_util": "posterga", "status": "ativo"}
    monkeypatch.setattr(agenda, "listar", lambda: [compromisso])
    monkeypatch.setattr(agenda, "feriados_extra", set)

    class RelogioFalso:
        @staticmethod
        def date():
            return dt.date(2026, 9, 1)

    monkeypatch.setattr(agenda, "agora", lambda: RelogioFalso(), raising=False)
    import app.apps.analisesps.horario as horario
    monkeypatch.setattr(horario, "agora",
                        lambda: dt.datetime(2026, 9, 1, 10, 0))

    linha = [o for o in agenda.proximos(60)
             if o["compromisso"]["id"] == "1"][0]
    assert linha["data"] == dt.date(2026, 9, 8)
    assert linha["data_original"] == dt.date(2026, 9, 5)


def test_ocorrencia_ja_vem_ajustada_ao_dia_util():
    """O contrato de `ocorrencias()`: uma lista de datas, já ajustadas."""
    compromisso = {"data_base": "05/09/2026", "recorrencia": "nenhuma",
                   "ajuste_dia_util": "posterga"}
    datas = agenda.ocorrencias(compromisso, dt.date(2026, 9, 1),
                               dt.date(2026, 9, 30), set())
    assert datas == [dt.date(2026, 9, 7)]
