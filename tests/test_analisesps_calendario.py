# -*- coding: utf-8 -*-
"""
O CALENDÁRIO DAS SPs — a grade do mês e a tela.

Pedido do dono em 23/09/2026: ver, em formato de calendário, o que cai em cada
dia, com o MESMO filtro das Solicitações e do Relatório.

⚠️ O QUE ESTES TESTES PROTEGEM, e é o que quebra em silêncio:

1. **O filtro é o mesmo, na mesma gaveta.** Se o Calendário guardasse o filtro
   num lugar próprio, quem recortasse nas Solicitações chegaria aqui vendo
   tudo — e não teria como desconfiar, porque a tela desenha igual.
2. **A data que manda depende do recorte.** Contas a pagar pelo vencimento,
   contas pagas pela data do pagamento. Misturar mostraria a mesma SP em dois
   dias e não fecharia com o Relatório.
3. **O que não tem data não some calado.** Uma SP sem vencimento não cai em dia
   nenhum; sumir sem aviso faria a soma daqui não bater com a do Relatório.
4. **O mês sobrevive a mexer no filtro.** Quem está olhando outubro e marca uma
   caixa continua em outubro.
"""
from datetime import date
from decimal import Decimal

import pytest
from flask import Flask

from app.apps.analisesps import consultas, preferencias, web

SENHA = "senha-de-teste-operador"


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA)
    monkeypatch.setattr(consultas, "base_carregada",
                        lambda: {"pronta": True, "quantidade": 59055,
                                 "ultima": "2026-09-23T18:00:00"})
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400: ["Pagar", "Pago"])
    monkeypatch.setattr(consultas, "opcoes_de_filtro",
                        lambda carimbo=None: dict(
                            {a: ["Pagar"] for a in consultas.COLUNAS_DE_FILTRO},
                            status_agend=["Agendar"]))
    monkeypatch.setattr(preferencias, "ler", lambda pessoa, chave: {})
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: None)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def como(app, nome="MARCELO"):
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": SENHA, "nome": nome})
    return cliente


def achado_falso(dias=None, **extra):
    base = {"dias": dias or {}, "total": Decimal("0"), "quantidade": 0,
            "sem_data_qtd": 0, "sem_data_total": Decimal("0"),
            "coluna": "vencimento_d"}
    base.update(extra)
    return base


# ---------------------------------------------------------------------------
# A GRADE — matemática pura, sem banco
# ---------------------------------------------------------------------------
def test_a_grade_comeca_no_domingo_e_fecha_no_sabado():
    """Calendário de parede no Brasil começa no domingo. A grade inclui os
    dias vizinhos que completam a primeira e a última semana."""
    from app.apps.analisesps import calendario

    g = calendario.grade(2026, 9, {})
    primeiro = g["semanas"][0]["dias"][0]["data"]
    ultimo = g["semanas"][-1]["dias"][-1]["data"]

    assert primeiro.weekday() == 6      # domingo
    assert ultimo.weekday() == 5        # sábado
    assert all(len(s["dias"]) == 7 for s in g["semanas"])


def test_os_dias_do_mes_vizinho_vem_marcados():
    """Eles aparecem apagados na tela. Sem a marca, 30 de agosto pareceria
    setembro e o mês somaria errado aos olhos de quem lê."""
    from app.apps.analisesps import calendario

    g = calendario.grade(2026, 9, {})
    de_fora = [d for s in g["semanas"] for d in s["dias"] if not d["do_mes"]]
    assert de_fora
    assert all(d["data"].month != 9 for d in de_fora)


def test_a_semana_traz_o_proprio_total():
    """"Quanto sai nesta semana?" é a pergunta que a soma do mês não responde."""
    from app.apps.analisesps import calendario

    g = calendario.grade(2026, 9, {
        date(2026, 9, 1): {"quantidade": 2, "total": Decimal("1000"),
                           "vencidas": 0},
        date(2026, 9, 2): {"quantidade": 1, "total": Decimal("500"),
                           "vencidas": 1},
    })
    semana = [s for s in g["semanas"]
              if any(d["data"] == date(2026, 9, 1) for d in s["dias"])][0]
    assert semana["total"] == Decimal("1500")
    assert semana["quantidade"] == 3


def test_a_escala_da_cor_sai_do_maior_dia_DO_MES():
    """⚠️ Do mês, não da grade. Um vencimento gordo do mês vizinho, que só
    aparece na grade porque completa a semana, apagaria todo o resto."""
    from app.apps.analisesps import calendario

    g = calendario.grade(2026, 9, {
        date(2026, 8, 31): {"quantidade": 1, "total": Decimal("900000"),
                            "vencidas": 0},
        date(2026, 9, 10): {"quantidade": 1, "total": Decimal("1000"),
                            "vencidas": 0},
    })
    assert g["maior"] == Decimal("1000")


def test_mes_malformado_na_barra_de_endereco_nao_derruba_a_tela():
    """Ano e mês vêm de fora. Mês 13 estouraria dentro do `calendar` e
    derrubaria a tela inteira por causa de um endereço torto."""
    from app.apps.analisesps import calendario

    for ano, mes in [("2026", "13"), ("abc", "9"), (None, None),
                     ("1800", "5"), ("2026", "0")]:
        a, m = calendario.mes_valido(ano, mes)
        assert 1 <= m <= 12
        assert 2000 <= a <= 2100

    assert calendario.mes_valido("2026", "10") == (2026, 10)


def test_os_meses_vizinhos_viram_o_ano():
    from app.apps.analisesps import calendario

    assert calendario.vizinhos(2026, 1)[0] == (2025, 12)
    assert calendario.vizinhos(2026, 12)[1] == (2027, 1)


def test_os_limites_cobrem_a_grade_inteira_e_nao_so_o_mes():
    """A consulta tem de trazer os dias vizinhos também: um vencimento no dia
    30 do mês passado aparece na primeira linha, e mostrá-lo vazio é mentira."""
    from app.apps.analisesps import calendario

    primeiro, ultimo = calendario.limites(2026, 9)
    assert primeiro < date(2026, 9, 1)
    assert ultimo > date(2026, 9, 30)


# ---------------------------------------------------------------------------
# A TELA
# ---------------------------------------------------------------------------
def test_a_tela_monta_e_mostra_o_mes(app, monkeypatch):
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            {date(2026, 9, 10): {"quantidade": 3,
                                                 "total": Decimal("12345.67"),
                                                 "vencidas": 0}},
                            total=Decimal("12345.67"), quantidade=3))
    resposta = como(app).get("/analisesps/calendario?f=1&ano=2026&mes=9")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "setembro de 2026" in html
    assert "12.345,67" in html


def test_o_dia_com_SP_leva_para_as_solicitacoes_daquele_dia(app, monkeypatch):
    """⚠️ É o que faz o calendário servir para trabalhar, e não só para olhar:
    a pergunta seguinte a "tem 12 mil no dia 10" é sempre "de quem?"."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            {date(2026, 9, 10): {"quantidade": 3,
                                                 "total": Decimal("12345.67"),
                                                 "vencidas": 0}}))
    html = como(app).get(
        "/analisesps/calendario?f=1&ano=2026&mes=9").get_data(as_text=True)

    assert "periodo_ini=2026-09-10" in html
    assert "periodo_fim=2026-09-10" in html
    assert "/analisesps/solicitacoes?" in html


def test_no_recorte_das_pagas_o_dia_leva_a_data_do_PAGAMENTO(app, monkeypatch):
    """A data que manda muda com o recorte — e o link do dia tem de mudar
    junto, senão ele abre a lista por vencimento e traz outras SPs."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            {date(2026, 9, 10): {"quantidade": 1,
                                                 "total": Decimal("10"),
                                                 "vencidas": 0}},
                            coluna="data_pagamento_d"))
    html = como(app).get(
        "/analisesps/calendario?f=1&ano=2026&mes=9&tipo=pagas"
    ).get_data(as_text=True)

    assert "pgt_ini=2026-09-10" in html
    assert "pgt_fim=2026-09-10" in html
    assert "periodo_ini=2026-09-10" not in html


def test_o_que_nao_tem_data_e_avisado_na_tela(app, monkeypatch):
    """Sumir em silêncio faria a soma do calendário não bater com a do
    Relatório no mesmo filtro — e essa conferência é o que o dono faz."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            sem_data_qtd=4, sem_data_total=Decimal("900.00")))
    html = como(app).get(
        "/analisesps/calendario?f=1&ano=2026&mes=9").get_data(as_text=True)

    assert "ficam de fora do calendário" in html
    assert "900,00" in html


def test_o_recorte_padrao_e_contas_a_pagar(app, monkeypatch):
    """Calendário se olha para frente. A visão geral fica a um clique."""
    vistos = []
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: vistos.append(tipo)
                        or achado_falso())
    como(app).get("/analisesps/calendario?f=1")
    assert vistos == ["pagar"]


def test_recorte_inventado_na_barra_de_endereco_cai_no_padrao(app, monkeypatch):
    vistos = []
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: vistos.append(tipo)
                        or achado_falso())
    como(app).get("/analisesps/calendario?f=1&tipo=qualquer-coisa")
    assert vistos == ["pagar"]


def test_o_calendario_le_o_filtro_guardado_DAS_OUTRAS_TELAS(app, monkeypatch):
    """⚠️ O CORAÇÃO DO PEDIDO. Quem recortou nas Solicitações e vem para cá
    tem de ver aquele recorte. É a MESMA gaveta — um lugar próprio faria a
    tela mostrar tudo, sem ninguém ter como desconfiar."""
    monkeypatch.setattr(preferencias, "ler",
                        lambda pessoa, chave: {"conta": ["BRADESCO 7011-4"]})
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso())

    resposta = como(app).get("/analisesps/calendario")
    assert resposta.status_code in (301, 302)
    assert "conta=BRADESCO" in resposta.headers["Location"].replace("+", " ")


def test_o_filtro_da_tela_chega_na_consulta(app, monkeypatch):
    vistos = []
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: vistos.append(f)
                        or achado_falso())
    como(app).get("/analisesps/calendario?f=1&conta=BRADESCO+7011-4")
    assert vistos[0]["conta"] == ["BRADESCO 7011-4"]


def test_o_mes_sobrevive_a_mexer_no_filtro(app, monkeypatch):
    """Quem está olhando outubro e marca uma caixa continua em outubro. Sem
    isso, a barra jogaria a pessoa de volta para o mês de hoje — justo quando
    ela estava filtrando outubro."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso())
    html = como(app).get(
        "/analisesps/calendario?f=1&ano=2026&mes=10").get_data(as_text=True)

    assert 'name="mes" value="10"' in html
    assert 'name="ano" value="2026"' in html
    # E a barra de filtros carrega os dois junto, senão marcar uma caixa
    # devolveria a pessoa para o mês de hoje.
    assert "'ano', 'mes'" in html or 'name="ano"' in html


def test_a_tela_aparece_no_menu_do_modulo():
    """A lista de telas alimenta a faixa de abas E o menu do celular. Uma tela
    que não entra nela existe mas não tem como ser achada."""
    chaves = [c for c, _, _ in web.TELAS]
    assert "calendario" in chaves
    assert web.TELAS[chaves.index("calendario")][2] == "analisesps.calendario"


def test_a_base_nao_carregada_nao_vira_calendario_vazio(app, monkeypatch):
    """Calendário vazio parece "não tem nada a pagar". A base não carregada
    tem de dizer que é base não carregada — mesma regra das outras telas."""
    monkeypatch.setattr(consultas, "base_carregada",
                        lambda: {"pronta": False, "quantidade": 0,
                                 "ultima": None})
    html = como(app).get("/analisesps/calendario?f=1").get_data(as_text=True)
    assert "A base ainda não foi carregada" in html
    assert "Semana" not in html


def test_o_link_do_dia_leva_o_status_QUE_A_CELULA_CONTOU(app, monkeypatch):
    """⚠️ A célula conta só o recorte (a pagar, por exemplo); a lista de
    Solicitações, sem o status, mostraria também o que já foi pago naquele
    dia. O dia diria 3 e a lista mostraria 5 — sem nada avisando.

    E o valor sai da LISTA DE OPÇÕES, não de um texto chutado: uma SP gravada
    como "PAGAR" na planilha ficaria de fora de um filtro exato por "Pagar"."""
    monkeypatch.setattr(consultas, "opcoes_de_filtro",
                        lambda carimbo=None: dict(
                            {a: [] for a in consultas.COLUNAS_DE_FILTRO},
                            status_pgt=["PAGAR", "Pago", "Cancelado"],
                            status_agend=[]))
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            {date(2026, 9, 10): {"quantidade": 3,
                                                 "total": Decimal("10"),
                                                 "vencidas": 0}}))
    html = como(app).get(
        "/analisesps/calendario?f=1&ano=2026&mes=9").get_data(as_text=True)

    assert "status_pgt=PAGAR" in html
    assert "status_pgt=Pago" not in html


def test_no_recorte_geral_o_dia_nao_filtra_status(app, monkeypatch):
    """A visão geral conta tudo o que não está cancelado; amarrar um status
    ao link faria a lista mostrar menos do que a célula contou."""
    monkeypatch.setattr(consultas, "opcoes_de_filtro",
                        lambda carimbo=None: dict(
                            {a: [] for a in consultas.COLUNAS_DE_FILTRO},
                            status_pgt=["Pagar", "Pago"], status_agend=[]))
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            {date(2026, 9, 10): {"quantidade": 3,
                                                 "total": Decimal("10"),
                                                 "vencidas": 0}}))
    html = como(app).get(
        "/analisesps/calendario?f=1&ano=2026&mes=9&tipo=geral"
    ).get_data(as_text=True)

    assert "status_pgt=" not in html
