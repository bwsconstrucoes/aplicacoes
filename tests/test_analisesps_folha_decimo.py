# -*- coding: utf-8 -*-
"""O 13º SALÁRIO NA FOLHA (25/11/2026).

O dono: *"vou lançar a folha do 13º. Pagamos o adiantamento dele em novembro e o
restante em dezembro. Ela é igual à folha da contabilidade (…) gostaria de dividir
cada funcionário de acordo com o ponto do ano."* Título real: "Folha Sintética -
Adiantamento de 13º Salário".
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_apropriacao as ap
from app.apps.analisesps import folha_sintetica as fs


@pytest.mark.parametrize("titulo,tipo", [
    ("Folha Sintética - Adiantamento de 13º Salário", "decimo_1"),
    ("Folha Sintética - 13º Salário", "decimo_2"),
    ("Folha Sintética - Décimo Terceiro Salário", "decimo_2"),
    # as de sempre, intactas
    ("Folha Sintética - Adiantamento de Folha", "quinzena"),
    ("Folha Sintética - Folha de Pagamento", "fim_de_mes"),
])
def test_o_TITULO_separa_o_13o_da_quinzena(titulo, tipo):
    """⚠️ O título do 13º tem "Adiantamento": sem a conferência do 13º antes, ele
    entraria como a quinzena de novembro e SUBSTITUIRIA a quinzena de verdade."""
    assert fs.tipo_sugerido(titulo) == tipo


def test_as_duas_parcelas_leem_o_ponto_de_JANEIRO_a_NOVEMBRO():
    assert ap.periodo_do_pagamento(2026, 11, "decimo_1") == (dt.date(2026, 1, 1),
                                                            dt.date(2026, 11, 30))
    assert ap.periodo_do_pagamento(2026, 12, "decimo_2") == (dt.date(2026, 1, 1),
                                                            dt.date(2026, 11, 30))
    # a 1ª parcela paga antes (com as férias, por exemplo): até o fim daquele mês
    assert ap.periodo_do_pagamento(2026, 7, "decimo_1")[1] == dt.date(2026, 7, 31)


class Linha:
    def __init__(self, valor, id_fortes, nome):
        self.valor, self.id_fortes, self.nome, self.filial_nome = D(valor), id_fortes, nome, "X"


def _dia(data, obra):
    return {"data": data, "marcacoes": [obra] * 4, "presenca": "PRESENÇA"}


def test_o_rateio_e_pela_PROPORCAO_DOS_DIAS_NO_ANO():
    """10 dias na obra A em janeiro e 30 na obra B em novembro: 25% e 75%."""
    dias = ([_dia(dt.date(2026, 1, d), "A") for d in range(5, 15)]
            + [_dia(dt.date(2026, 11, d), "B") for d in range(1, 31)])
    feito = ap.apropriar([Linha("2000.00", "000013", "FULANO")], {"111": dias},
                         cadastro_por_id={"000013": {"cpf": "111", "nome": "FULANO"}},
                         periodo=ap.periodo_do_pagamento(2026, 11, "decimo_1"))
    por_obra = {o["obra"]: o["valor"] for o in feito["por_obra"]}
    assert por_obra == {"A": D("500.00"), "B": D("1500.00")} and feito["fecha"]


def test_quem_SAIU_no_meio_do_ano_nao_vira_alerta_de_fora_do_13o():
    """Saiu em março: bateu ponto no ano, mas recebeu o 13º na rescisão. O alerta
    'bateu ponto e não está na folha' olha só o último mês do período."""
    dias = {"222": [_dia(dt.date(2026, 3, d), "A") for d in range(1, 10)],
            "333": [_dia(dt.date(2026, 11, d), "A") for d in range(1, 10)]}
    cadastro = {"000013": {"cpf": "111", "nome": "FULANO"},
                "000020": {"cpf": "222", "nome": "SAIU", "tipo": "CTPS"},
                "000030": {"cpf": "333", "nome": "ESQUECIDO", "tipo": "CTPS"}}
    feito = ap.apropriar([Linha("100.00", "000013", "FULANO")], dias,
                         cadastro_por_id=cadastro,
                         periodo=ap.periodo_do_pagamento(2026, 11, "decimo_1"))
    fora = feito["fora_da_folha"]
    todos = [q["cpf"] for lista in fora.values() for q in lista]
    assert "222" not in todos and "333" in todos


def test_o_ponto_do_ano_e_lido_MES_A_MES_e_enxuto(monkeypatch):
    from app.apps.analisesps import ponto
    lidos = []

    def do_mes(ano, mes):
        lidos.append(mes)
        if mes == 2:
            return {}                       # fevereiro sem carga
        return {"111": [{"data": dt.date(ano, mes, 3), "marcacoes": ["A", "A", "B", "A"],
                         "presenca": "PRESENÇA", "falta": "", "horas": ["x"] * 4},
                        {"data": dt.date(ano, mes, 4), "marcacoes": [], "presenca": "",
                         "falta": "FALTA"}]}
    monkeypatch.setattr(ponto, "dias_por_cpf", do_mes)
    ponto._ANO_GUARDADO.clear()
    dias = ponto.dias_do_pagamento(2026, 11, "decimo_1")
    assert lidos == list(range(1, 12))
    assert len(dias["111"]) == 20
    assert dias["111"][0] == {"data": dt.date(2026, 1, 3), "marcacoes": ["A"],
                              "presenca": "", "falta": ""}
    assert ponto.dias_do_ano(dt.date(2026, 1, 1), dt.date(2026, 11, 30))["meses_sem_ponto"] == ["02/2026"]
    ponto.dias_do_pagamento(2026, 12, "decimo_2")
    assert len(lidos) == 11, "guardado: a segunda abertura não relê o ano"
    # a folha do mês continua lendo só o mês
    ponto.dias_do_pagamento(2026, 11, "quinzena")
    assert lidos[-1] == 11 and len(lidos) == 12
    ponto._ANO_GUARDADO.clear()


@pytest.mark.banco
def test_o_13o_entra_SEM_substituir_a_quinzena_do_mes(banco_analisesps, monkeypatch):
    from tests.test_analisesps_folha_arquivo import guardar
    from app.apps.analisesps import folha_arquivo as fa
    guardar(monkeypatch, titulo="Folha Sintética - Adiantamento de Folha", mes=11)
    feito = guardar(monkeypatch, titulo="Folha Sintética - Adiantamento de 13º Salário", mes=11)
    assert feito["tipo"] == "decimo_1"
    assert {f["tipo"] for f in fa.listar()} == {"quinzena", "decimo_1"}
    folha = [f for f in fa.listar() if f["tipo"] == "decimo_1"][0]
    assert "13º" in fa.abrir(folha["id"])["rotulo_do_tipo"]


@pytest.mark.banco
def test_competencia_13_vira_o_mes_da_parcela(banco_analisesps, monkeypatch):
    from tests.test_analisesps_folha_arquivo import guardar
    feito = guardar(monkeypatch, titulo="Folha Sintética - 13º Salário", mes=13)
    assert feito["tipo"] == "decimo_2" and feito["mes"] == 12


# --- 27/11/2026: "Campo Mês/Ano não encontrado no arquivo" no 13º ---------------
def _linhas(cabeca):
    """Uma folha mínima com o cabeçalho dado (lista de linhas) no lugar do de sempre."""
    return cabeca + [["001 - MATRIZ"], ["Código", "Nome", "", "", "Líquido"],
                     ["000013", "FULANO", "", "", "1.000,00"],
                     ["", "", "", "Total:", "1000,00"]]


TITULO = "Folha Sintética - Adiantamento de 13º Salário"
EMPRESA = ["Empresa:", "BWS CONSTRUCOES LTDA", "CNPJ: 00.079.526/0001-09"]


@pytest.mark.parametrize("cabeca,mes,ano", [
    # o rótulo numa célula e a competência na seguinte
    ([[TITULO], EMPRESA, ["Mês/Ano:", "11/2026"]], 11, 2026),
    # a competência na MESMA linha do título
    ([[TITULO, "", "Mês/Ano: 11/2026"], EMPRESA], 11, 2026),
    # "13º/2026" e "13/2026" (o Fortes chama o 13º de mês 13): vira o mês da parcela
    ([[TITULO], EMPRESA, ["Mês/Ano: 13º/2026"]], 13, 2026),
    # outro rótulo e o mês por extenso
    ([[TITULO], EMPRESA, ["Competência:", "Novembro/2026"]], 11, 2026),
    # sem o mês: no 13º vale o da parcela, com o ano do arquivo
    ([[TITULO], EMPRESA, ["Ano:", "2026"]], 11, 2026),
])
def test_a_competencia_do_13o_e_achada_em_QUALQUER_celula(cabeca, mes, ano):
    lida = fs.interpretar(_linhas(cabeca))
    if lida.mes is None and lida.tipo_sugerido in fs.DECIMOS:
        # o mesmo caminho do `ler`
        lida.ano = lida.ano or 2026
        lida.mes = 11
    assert (lida.mes, lida.ano) == (mes, ano)
    assert lida.tipo_sugerido == "decimo_1"


def test_sem_mes_no_13o_o_LER_usa_o_da_parcela_e_avisa(monkeypatch):
    import xlrd

    class Aba:
        def __init__(self, linhas):
            self.linhas = linhas
            self.nrows = len(linhas)
            self.ncols = max(len(l) for l in linhas)

        def cell_value(self, r, c):
            return self.linhas[r][c] if c < len(self.linhas[r]) else ""

    linhas = _linhas([[TITULO], EMPRESA])
    monkeypatch.setattr(xlrd, "open_workbook",
                        lambda file_contents: type("L", (), {"sheet_by_index": lambda s, i: Aba(linhas)})())
    lida = fs.ler(b"\xd0\xcf\x11\xe0" + b"x" * 100)
    assert lida.mes == 11 and lida.ano == dt.date.today().year
    assert any("13º" in a for a in lida.avisos)


def test_a_folha_COMUM_sem_competencia_continua_recusada():
    """Só o 13º ganha o mês da parcela; a folha comum sem competência é recusada."""
    lida = fs.interpretar(_linhas([["Folha Sintética - Folha de Pagamento"], EMPRESA]))
    assert lida.mes is None
