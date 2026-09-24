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


def situacoes(pago=0, vencido=0, a_vencer=0, outros=0):
    """As quatro situações de um dia, no formato que a consulta devolve.

    O valor acompanha a quantidade só para os testes terem número: aqui o que
    importa é qual balde recebeu quanto."""
    return {nome: {"quantidade": q, "total": Decimal(q * 100)}
            for nome, q in (("pago", pago), ("vencido", vencido),
                            ("a_vencer", a_vencer), ("outros", outros))}


def dia_falso(quantidade=1, total="100.00", **baldes):
    achado = situacoes(**baldes) if baldes else situacoes(a_vencer=quantidade)
    return {"quantidade": quantidade, "total": Decimal(total),
            "vencidas": achado["vencido"]["quantidade"],
            "situacoes": achado}


def achado_falso(dias=None, **extra):
    base = {"dias": dias or {}, "total": Decimal("0"), "quantidade": 0,
            "situacoes": situacoes(),
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


def test_o_recorte_padrao_e_a_VISAO_GERAL_por_causa_das_CORES(app, monkeypatch):
    """⚠️ Era "a pagar" até 23/09/2026, e mudou por causa do pedido das cores.

    O dono pediu azul para pago, vermelho para vencido e laranja a vencer. No
    recorte "a pagar", conta paga está fora — o azul NUNCA apareceria. Abrir
    numa visão que esconde uma das três cores é entregar metade do pedido."""
    vistos = []
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: vistos.append(tipo)
                        or achado_falso())
    como(app).get("/analisesps/calendario?f=1")
    assert vistos == ["geral"]


def test_recorte_inventado_na_barra_de_endereco_cai_no_padrao(app, monkeypatch):
    vistos = []
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: vistos.append(tipo)
                        or achado_falso())
    como(app).get("/analisesps/calendario?f=1&tipo=qualquer-coisa")
    assert vistos == ["geral"]


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
        "/analisesps/calendario?f=1&ano=2026&mes=9&tipo=pagar"
    ).get_data(as_text=True)

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


# ---------------------------------------------------------------------------
# AS CORES — 23/09/2026
#
# *"Coloca azul para pago, vermelho para vencido e laranja a vencer."*
#
# ⚠️ O QUE ESTES TESTES PROTEGEM: um dia com conta paga E conta a vencer tem
# de mostrar as DUAS. Pintar o dia de uma cor só esconderia metade do que ele
# tem, e quem olha não teria como saber que está vendo metade.
# ---------------------------------------------------------------------------
def test_o_dia_mostra_UMA_LINHA_POR_SITUACAO():
    from app.apps.analisesps import calendario

    faixas = calendario._faixas(
        {"pago": {"quantidade": 2, "total": Decimal("200")},
         "vencido": {"quantidade": 1, "total": Decimal("900")},
         "a_vencer": {"quantidade": 3, "total": Decimal("300")},
         "outros": {"quantidade": 0, "total": Decimal("0")}})

    assert [f["cor"] for f in faixas] == ["vermelho", "laranja", "azul"]
    assert [f["quantidade"] for f in faixas] == [1, 3, 2]


def test_a_situacao_vazia_nao_vira_linha():
    """Um dia com três faixas zeradas seria ruído dentro de um quadradinho
    que já é pequeno."""
    from app.apps.analisesps import calendario

    faixas = calendario._faixas(
        {"pago": {"quantidade": 0, "total": Decimal("0")},
         "a_vencer": {"quantidade": 2, "total": Decimal("200")}})
    assert [f["cor"] for f in faixas] == ["laranja"]


def test_a_ordem_das_cores_e_de_URGENCIA_e_nao_de_valor():
    """⚠️ Vencido primeiro, sempre: é o que cobra ação. Ordenar por valor
    faria uma conta vencida de R$ 200 sumir embaixo de vinte pagas."""
    from app.apps.analisesps import calendario

    assert [c for _, c, _ in calendario.SITUACOES][:3] == [
        "vermelho", "laranja", "azul"]


def test_a_barra_do_dia_e_a_situacao_MAIS_URGENTE_e_nao_a_maior(app,
                                                                monkeypatch):
    """Um dia com uma conta vencida e vinte pagas continua gritando vermelho."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            {date(2026, 9, 10): dia_falso(
                                21, "2100.00", pago=20, vencido=1)}))
    html = como(app).get(
        "/analisesps/calendario?f=1&ano=2026&mes=9").get_data(as_text=True)

    assert "urgencia-vermelho" in html
    assert "urgencia-azul" not in html
    # E as duas situações aparecem, cada uma na sua linha.
    assert "faixa-vermelho" in html and "faixa-azul" in html


def test_a_tela_traz_a_legenda_das_cores(app, monkeypatch):
    """Cor sem legenda é adivinhação — e são três."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso())
    html = como(app).get("/analisesps/calendario?f=1").get_data(as_text=True)

    assert "vencido e ainda a pagar" in html
    assert "a vencer" in html
    assert "pago" in html


def test_o_mes_mostra_o_total_de_cada_situacao(app, monkeypatch):
    """"Quanto tem vencido?" olhando dia a dia exigiria somar de cabeça."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso(
                            situacoes=situacoes(pago=2, vencido=1, a_vencer=3)))
    html = como(app).get("/analisesps/calendario?f=1").get_data(as_text=True)

    assert "Vencido e ainda a pagar no mês" in html
    assert "A vencer no mês" in html
    assert "Pago no mês" in html
    # A situação sem nada não vira quadro vazio.
    assert "Em outra situação no mês" not in html


# ---------------------------------------------------------------------------
# O FILTRO DE DATA NÃO VALE AQUI — 24/09/2026
#
# *"Como é um calendário, eu não queria que o filtro de data interferisse nele;
# o correto é aparecer tudo. Continua mantendo os outros filtros."*
#
# ⚠️ E é o certo: quem escolhe a data nesta tela é o MÊS aberto. Um recorte de
# vencimento vindo das Solicitações apagaria dias inteiros do calendário sem
# nada explicando — a pessoa veria um mês pela metade e concluiria que não há
# nada a pagar naqueles dias.
# ---------------------------------------------------------------------------
def test_o_filtro_de_data_nao_chega_na_consulta_do_calendario(app, monkeypatch):
    vistos = []
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: vistos.append(f)
                        or achado_falso())
    como(app).get("/analisesps/calendario?f=1&periodo_ini=2026-09-01"
                  "&periodo_fim=2026-09-05&pgt_ini=2026-09-02"
                  "&pgt_fim=2026-09-03")

    usado = vistos[0]
    assert usado["periodo_ini"] is None and usado["periodo_fim"] is None
    assert usado["pgt_ini"] is None and usado["pgt_fim"] is None


def test_os_OUTROS_filtros_continuam_valendo(app, monkeypatch):
    """"Continua mantendo os outros filtros, caso a gente queira." Jogar fora
    o filtro inteiro junto com a data seria perder o que a tela tem de melhor."""
    vistos = []
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: vistos.append(f)
                        or achado_falso())
    como(app).get("/analisesps/calendario?f=1&conta=ITAU&periodo_ini=2026-09-01"
                  "&centro_custo=OBRA+1&busca=cimento")

    usado = vistos[0]
    assert usado["conta"] == ["ITAU"]
    assert usado["centro_custo"] == ["OBRA 1"]
    assert usado["busca"] == "cimento"
    assert usado["periodo_ini"] is None


def test_a_data_marcada_NAO_E_APAGADA_das_outras_telas(app, monkeypatch):
    """⚠️ Ignorar aqui e apagar são coisas diferentes. O recorte foi montado
    nas Solicitações; esta tela não pode mexer nele pelas costas."""
    gravados = []
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: gravados.append(valor))
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso())

    html = como(app).get(
        "/analisesps/calendario?f=1&periodo_ini=2026-09-01").get_data(as_text=True)

    # O que foi guardado continua com a data.
    assert gravados and gravados[0].get("periodo_ini") == ["2026-09-01"]
    # E a tela diz, na própria barra, que ali ela não vale.
    assert "Não vale no calendário" in html


def test_a_barra_avisa_que_a_data_nao_vale_mesmo_sem_data_marcada(app,
                                                                  monkeypatch):
    """O recado não depende de haver data marcada: quem vai marcar precisa
    saber antes que ali não vai adiantar."""
    monkeypatch.setattr(consultas, "calendario_do_mes",
                        lambda f, ini, fim, tipo: achado_falso())
    html = como(app).get("/analisesps/calendario?f=1").get_data(as_text=True)
    assert html.count("Não vale no calendário") == 2   # vencimento e pagamento


def test_nas_solicitacoes_a_data_continua_valendo(app, monkeypatch):
    """A barra é a mesma nas três telas. O recado só pode aparecer numa."""
    monkeypatch.setattr(consultas, "listar", lambda f, **k: [])
    monkeypatch.setattr(consultas, "resumo_e_agendamento", lambda f: (
        {"quantidade": 0, "total": 0, "quantidade_pagar": 0, "total_pagar": 0},
        {}))
    monkeypatch.setattr(consultas, "soma_por", lambda f, coluna, limite=12: [])
    monkeypatch.setattr(preferencias, "ler", lambda pessoa, chave: {})

    html = como(app).get(
        "/analisesps/solicitacoes?f=1&periodo_ini=2026-09-01").get_data(as_text=True)
    assert "Não vale no calendário" not in html
