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
from tests.test_analisesps_telas import app  # noqa: F401 — o fixture da tela


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
        return {"11122233344": [{"data": dt.date(ano, mes, 3), "marcacoes": ["A", "A", "B", "A"],
                         "presenca": "PRESENÇA", "falta": "", "horas": ["x"] * 4},
                        {"data": dt.date(ano, mes, 4), "marcacoes": [], "presenca": "",
                         "falta": "FALTA"}]}
    monkeypatch.setattr(ponto, "dias_por_cpf", do_mes)
    ponto._ANO_GUARDADO.clear()
    dias = ponto.dias_do_pagamento(2026, 11, "decimo_1")
    assert lidos == list(range(1, 12))
    assert len(dias["11122233344"]) == 20
    primeiro = dict(dias["11122233344"][0])
    assert primeiro.pop("peso") == 1, "1 dia com obra no mês: o dia vale o mês inteiro"
    assert primeiro == {"data": dt.date(2026, 1, 3), "marcacoes": ["A"],
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


def test_o_13o_do_Fortes_traz_DATA_DE_CALCULO_e_nao_Mes_Ano():
    """O arquivo real (27/11/2026): título, empresa e "Data de Cálculo: 30/11/2026" —
    nenhum "Mês/Ano". A data de cálculo dá a competência do 13º."""
    lida = fs.interpretar(_linhas([[TITULO, ": 1"],
                                   ["Empresa:", "BWS CONSTRUCOES LTDA  - CNPJ: 00.079.526/0001-09"],
                                   ["Data de Cálculo: 30/11/2026"]]))
    assert lida.data_de_calculo == (30, 11, 2026)
    assert not any("Data de Cálculo" in a for a in lida.avisos), "não é linha estranha"


ARQUIVO_REAL_DO_13 = ("/root/.claude/uploads/aacbc884-22a3-5149-a9c7-9fa4ed1dc760/"
                      "996c2bef-Folha_Sint_tica_-_Adiantamento_de_13__Sal_rio.xls")


@pytest.mark.banco
def test_o_ARQUIVO_REAL_do_13o_entra(banco_analisesps):
    """Só roda onde o arquivo está — ele tem nome e valor de cada pessoa e NÃO vai
    para o repositório."""
    import pathlib
    if not pathlib.Path(ARQUIVO_REAL_DO_13).exists():
        pytest.skip("arquivo real do 13º não está nesta máquina")
    from app.apps.analisesps import folha_arquivo as fa
    feito = fa.importar(pathlib.Path(ARQUIVO_REAL_DO_13).read_bytes(),
                        nome_do_arquivo="13.xls", quem="TESTE")
    assert feito["tipo"] == "decimo_1" and (feito["ano"], feito["mes"]) == (2026, 11)
    assert feito["pessoas"] == 353


@pytest.mark.banco
def test_a_TELA_da_folha_do_13o_abre_e_diz_o_periodo(banco_analisesps, app, monkeypatch):  # noqa: F811
    from tests.test_analisesps_folha_arquivo import guardar
    from tests.test_analisesps_telas import _como_mestre
    from app.apps.analisesps import ponto
    ponto._ANO_GUARDADO.clear()
    feito = guardar(monkeypatch, titulo="Folha Sintética - Adiantamento de 13º Salário", mes=11)
    html = _como_mestre(app).get(f"/analisesps/folha/{feito['id']}").get_data(as_text=True)
    assert "13º salário" in html and "01/01/2026" in html and "30/11/2026" in html
    assert "Sem ponto carregado em" in html, "o teste não tem ponto: os meses aparecem"
    ponto._ANO_GUARDADO.clear()


# --- 27/11/2026: o relatório anual de presenças do Mobponto ----------------------
# ⚠️ CPF E NOME INVENTADOS: o arquivo real tem dados pessoais e não entra aqui.
def _relatorio_html(pessoas):
    """O formato do Mobponto: tabela HTML com CPF, nome e LOCAL/ETAPA de JAN a DEZ."""
    linhas = []
    for cpf, nome, meses in pessoas:
        tds = [cpf, nome]
        for m in range(1, 13):
            tds += [meses.get(m, ""), ""]
        linhas.append("<tr class=>" + "".join(f"<td style='x'>{t}</td>" for t in tds) + "</tr>")
    return ("<table border='1'><thead><tr><th colspan='26'>Presenças por Locais - ( Anual )"
            "</th></tr></thead><tbody>" + "\n".join(linhas) + "</tbody></table>").encode("latin-1")


def test_o_relatorio_anual_e_lido_pessoa_mes_e_obra():
    from app.apps.analisesps import ponto_anual
    lido = ponto_anual.ler(_relatorio_html([
        ("111.222.333-44", "FULANO DE TAL", {1: "OBRAX", 2: "OBRAX"}),
        ("555.666.777-88", "BELTRANO", {}),                    # sem local: fica de fora
    ]))
    assert lido == {"11122233344": {1: "OBRAX", 2: "OBRAX"}}
    with pytest.raises(ponto_anual.ErroDoRelatorioAnual):
        ponto_anual.ler(b"<table><tr><td>outra coisa</td></tr></table>")


ARQUIVO_REAL_ANUAL = ("/root/.claude/uploads/aacbc884-22a3-5149-a9c7-9fa4ed1dc760/"
                      "88fd79b7-PresencasAnual-11-2026.xls")


def test_o_ARQUIVO_REAL_do_relatorio_anual():
    """Só roda onde o arquivo está — ele tem dados pessoais e não vai para o repositório."""
    import pathlib
    if not pathlib.Path(ARQUIVO_REAL_ANUAL).exists():
        pytest.skip("arquivo real não está nesta máquina")
    from app.apps.analisesps import ponto_anual
    lido = ponto_anual.ler(pathlib.Path(ARQUIVO_REAL_ANUAL).read_bytes())
    assert len(lido) > 1400
    assert sum(1 for m in lido.values() if 1 in m) == 892     # janeiro, conferido à mão


def test_os_AVOS_cada_mes_vale_o_mesmo(monkeypatch):
    """Relatório anual de jan a ago na obra A (8 meses); setembro dia a dia na B (20
    dias); outubro metade B, metade C. 10 meses: A 8/10, B 1,5/10, C 0,5/10."""
    from app.apps.analisesps import ponto, ponto_anual

    def do_mes(ano, mes):
        if mes < 9:
            return {}                                     # sem carga: o relatório cobre
        if mes == 9:
            dias = [(d, "B") for d in range(1, 21)]
        elif mes == 10:
            dias = [(d, "B") for d in range(1, 11)] + [(d, "C") for d in range(11, 21)]
        else:
            return {"99999999999": [{"data": dt.date(ano, mes, 3), "marcacoes": ["Z"] * 4,
                                     "presenca": "PRESENÇA", "falta": ""}]}
        return {"11122233344": [{"data": dt.date(ano, mes, d), "marcacoes": [o] * 4,
                                 "presenca": "PRESENÇA", "falta": ""} for d, o in dias]}
    monkeypatch.setattr(ponto, "dias_por_cpf", do_mes)
    monkeypatch.setattr(ponto_anual, "carregar", lambda ano: {
        "pessoas": {"11122233344": {m: "A" for m in range(1, 11)}}})   # set/out: têm carga
    ponto._ANO_GUARDADO.clear()
    feito = ponto.dias_do_ano(dt.date(2026, 1, 1), dt.date(2026, 11, 30))
    assert feito["meses_sem_ponto"] == []
    assert feito["meses_do_relatorio"] == [f"{m:02d}/2026" for m in range(1, 9)]
    apropriado = ap.apropriar([Linha("1000.00", "000013", "FULANO")], feito["dias"],
                              cadastro_por_id={"000013": {"cpf": "11122233344", "nome": "FULANO"}},
                              periodo=ap.periodo_do_pagamento(2026, 11, "decimo_1"))
    por_obra = {o["obra"]: o["valor"] for o in apropriado["por_obra"]}
    assert por_obra == {"A": D("800.00"), "B": D("150.00"), "C": D("50.00")}
    assert apropriado["fecha"]
    ponto._ANO_GUARDADO.clear()


def test_a_folha_do_MES_continua_pesando_cada_dia_igual():
    """O peso só existe no 13º: na quinzena, dia sem peso vale 1, como sempre."""
    dias = [_dia(dt.date(2026, 11, d), "A") for d in range(1, 4)] + [_dia(dt.date(2026, 11, 4), "B")]
    feito = ap.apropriar([Linha("400.00", "000013", "FULANO")], {"111": dias},
                         cadastro_por_id={"000013": {"cpf": "111", "nome": "FULANO"}},
                         periodo=ap.periodo_do_pagamento(2026, 11, "quinzena"))
    assert {o["obra"]: o["valor"] for o in feito["por_obra"]} == {"A": D("300.00"), "B": D("100.00")}


@pytest.mark.banco
def test_ENVIAR_o_relatorio_anual_pela_tela_do_13o(banco_analisesps, app, monkeypatch):  # noqa: F811
    import io
    from tests.test_analisesps_folha_arquivo import guardar
    from tests.test_analisesps_telas import _como_mestre
    from app.apps.analisesps import ponto, ponto_anual
    ponto._ANO_GUARDADO.clear()
    ponto_anual._GUARDADO.clear()
    feito = guardar(monkeypatch, titulo="Folha Sintética - Adiantamento de 13º Salário", mes=11)
    cliente = _como_mestre(app)
    r = cliente.post(f"/analisesps/folha/{feito['id']}/ponto-anual", data={
        "arquivo": (io.BytesIO(_relatorio_html([("111.222.333-44", "FULANO DE TAL",
                                                 {m: "OBRAX" for m in range(1, 9)})])),
                    "PresencasAnual-11-2026.xls")}, content_type="multipart/form-data")
    assert r.status_code == 302 and "importado" in r.headers["Location"]
    html = cliente.get(r.headers["Location"]).get_data(as_text=True)
    assert "Pelo relatório anual de presenças" in html and "01/2026" in html
    falta = html.split("Sem ponto carregado em")[1][:60]
    assert "09/2026" in falta and "01/2026" not in falta
    ponto._ANO_GUARDADO.clear()
    ponto_anual._GUARDADO.clear()
