# -*- coding: utf-8 -*-
"""
O Calendário com banco de verdade — o que vive no WHERE e no GROUP BY.

Pedido do dono em 22/09/2026: um calendário do mês com o resumo de cada dia
(pago e recebido), o detalhe ao clicar, e os mesmos filtros do Analítico. A
régua é a do Fluxo de Caixa: só o que virou dinheiro, pela data em que
aconteceu, com os encargos pagos e sem as retenções.
"""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_painel_banco import painel_no_banco  # noqa: F401  (fixture)

pytestmark = pytest.mark.banco


def test_o_mes_fecha_com_o_fluxo_de_caixa(painel_no_banco):
    """Junho de 2025 no cenário: 1.000 recebidos e 425 pagos (400 de
    principal, 20 de juros e 5 de multa), tudo no dia 10. O retido (100) fica
    de fora, o título em aberto (250) fica de fora, a transferência fica de
    fora — exatamente como na linha de junho do Fluxo de Caixa."""
    from app.apps.painel import consultas
    f = consultas.Filtros()
    mes = consultas.calendario_do_mes(f, 2025, 6)
    assert mes["entradas"] == pytest.approx(1000.0)
    assert mes["saidas"] == pytest.approx(-425.0)
    assert mes["liquido"] == pytest.approx(575.0)
    assert mes["dias_com_movimento"] == 1 and mes["quantos"] == 2
    dia = mes["dias"][dt.date(2025, 6, 10)]
    assert dia["entradas"] == pytest.approx(1000.0)
    assert dia["saidas"] == pytest.approx(-425.0)
    assert mes["maior_entrada"] == (dt.date(2025, 6, 10), pytest.approx(1000.0))
    assert mes["maior_saida"] == (dt.date(2025, 6, 10), pytest.approx(-425.0))

    fluxo = {l["rotulo"]: l for l in consultas.caixa_por_mes(f)}
    assert fluxo["06/2025"]["entradas"] == pytest.approx(mes["entradas"])
    assert fluxo["06/2025"]["saidas"] == pytest.approx(mes["saidas"])

    # mês sem nada: tudo zero, sem quebrar
    vazio = consultas.calendario_do_mes(f, 2023, 1)
    assert vazio["dias"] == {} and vazio["quantos"] == 0
    assert vazio["maior_entrada"] is None


def test_o_terceiro_numero_e_o_a_pagar_pelo_vencimento(painel_no_banco):
    """O título 3 do cenário: 250 em aberto, vence 30/06/2025 (a data de
    sempre é 10/06 — o vencimento manda). Visto de julho, está vencido; visto
    de junho, a vencer. O a receber em aberto (título 4) não entra."""
    from app.apps.painel import consultas
    f = consultas.Filtros()
    mes = consultas.calendario_do_mes(f, 2025, 6, hoje=dt.date(2025, 7, 15))
    dia = mes["dias"][dt.date(2025, 6, 30)]
    assert dia["a_pagar"] == pytest.approx(-250.0) and dia["quantos_a_pagar"] == 1
    assert dia["vencido"] is True and dia["quantos"] == 0
    assert mes["a_pagar"] == pytest.approx(-250.0)
    assert mes["a_pagar_vencido"] == pytest.approx(-250.0)
    assert mes["a_pagar_a_vencer"] == 0.0
    assert mes["maior_a_pagar"] == (dt.date(2025, 6, 30), pytest.approx(-250.0))
    assert mes["dias_com_movimento"] == 1                 # só o dia 10 tem caixa
    antes = consultas.calendario_do_mes(f, 2025, 6, hoje=dt.date(2025, 6, 1))
    assert antes["dias"][dt.date(2025, 6, 30)]["vencido"] is False
    assert antes["a_pagar_a_vencer"] == pytest.approx(-250.0)
    # setembro tem o a receber de 2.000 em aberto — não é a pagar
    assert consultas.calendario_do_mes(f, 2025, 9)["a_pagar"] == 0.0
    # o detalhe do dia traz o título em aberto, marcado
    linhas = consultas.lancamentos_do_dia(f, "2025-06-30", hoje=dt.date(2025, 7, 15))
    assert [(l["natureza"], l["em_aberto"]) for l in linhas] == [("Vencido", True)]
    assert linhas[0]["valor"] == pytest.approx(-250.0)
    # "só a pagar" esconde o caixa; "só pagamentos" esconde o a pagar
    so = consultas.calendario_do_mes(f, 2025, 6, tipo="a_pagar")
    assert so["entradas"] == 0 and so["a_pagar"] == pytest.approx(-250.0)
    assert consultas.calendario_do_mes(f, 2025, 6, tipo="pago")["a_pagar"] == 0.0


def test_o_detalhe_do_dia_fecha_com_o_quadradinho(painel_no_banco):
    from app.apps.painel import consultas
    f = consultas.Filtros()
    linhas = consultas.lancamentos_do_dia(f, "2025-06-10")
    assert [l["natureza"] for l in linhas] == ["Recebimento", "Pagamento"]
    assert sum(l["valor"] for l in linhas) == pytest.approx(575.0)
    pagamento = linhas[1]
    assert pagamento["valor"] == pytest.approx(-425.0)
    assert pagamento["encargo"] == pytest.approx(-25.0)
    assert pagamento["obra"] == "CASA" and pagamento["categoria"] == "Materiais"
    # os filtros próprios da tela valem no mês E no detalhe
    so_pago = consultas.calendario_do_mes(f, 2025, 6, tipo="pago")
    assert so_pago["entradas"] == 0 and so_pago["quantos"] == 1
    assert len(consultas.lancamentos_do_dia(f, "2025-06-10", tipo="pago")) == 1
    assert len(consultas.lancamentos_do_dia(f, "2025-06-10", busca="CLIENTE A")) == 1
    assert consultas.lancamentos_do_dia(f, "2025-06-10", categoria="Salários") == []
    # a obra da barra lateral
    ponte = consultas.Filtros(departamentos=["PONTE"])
    assert consultas.calendario_do_mes(ponte, 2025, 6)["dias"] == {}
    assert consultas.calendario_do_mes(ponte, 2025, 7)["saidas"] == pytest.approx(-900.0)


def test_a_tela_e_o_detalhe_abrem_com_banco_de_verdade(painel_no_banco, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "segredo-de-teste"})

    html = cliente.get("/painel/calendario?mes=2025-06").get_data(as_text=True)
    assert "Junho de 2025" in html
    assert 'data-dia="2025-06-10"' in html
    assert "R$ 1.000,00" in html and "−R$ 425,00" in html
    # o ano da barra lateral não vale aqui: com ano=2024 marcado, junho de
    # 2025 continua aparecendo
    html = cliente.get("/painel/calendario?mes=2025-06&ano=2024").get_data(as_text=True)
    assert 'data-dia="2025-06-10"' in html

    dados = cliente.get("/painel/calendario/dia?dia=2025-06-10&mes=2025-06").get_json()
    assert dados["ok"] and dados["quantos"] == 2
    assert dados["liquido"] == pytest.approx(575.0)

    r = cliente.get("/painel/baixar/calendario?mes=2025-06")
    assert r.status_code == 200 and "spreadsheet" in r.mimetype



def test_dre_ou_fluxo_separa_o_que_entra_no_resultado(painel_no_banco):
    """O dono: "é importante poder ver só os lançamentos de fluxo, ou só os
    de DRE — tudo junto atrapalha". Um empréstimo recebido (fluxo) entra no
    dia 10/06 ao lado dos lançamentos de DRE; cada filtro fica só com o seu."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
            " situacao_vencimento, categoria, grupo, departamento, projeto,"
            " razao_social, data, ano, mes, pago_recebido, a_pagar_receber, juros, multa)"
            " VALUES (950,'1. Contas a Receber','Fluxo de Caixa','Recebido','Quitado',"
            "         'Empréstimos','Financeiras','CASA','ALFA','BANCO',"
            "         '2025-06-10',2025,6,5000,0,0,0)")
        conn.commit()
    f = consultas.Filtros()
    tudo = consultas.calendario_do_mes(f, 2025, 6)
    so_dre = consultas.calendario_do_mes(f, 2025, 6, analise="dre")
    so_fluxo = consultas.calendario_do_mes(f, 2025, 6, analise="fluxo")
    assert tudo["entradas"] == pytest.approx(6000.0)
    assert so_dre["entradas"] == pytest.approx(1000.0)
    assert so_fluxo["entradas"] == pytest.approx(5000.0)
    assert so_fluxo["saidas"] == 0.0 and so_fluxo["a_pagar"] == 0.0
    assert so_dre["entradas"] + so_fluxo["entradas"] == pytest.approx(tudo["entradas"])
    linhas = consultas.lancamentos_do_dia(f, "2025-06-10", analise="fluxo")
    assert [l["analise"] for l in linhas] == ["Fluxo de Caixa"]


# ===========================================================================
# De onde veio a conta — 23/09/2026
# ===========================================================================
# O dono: "no Calendário está dizendo que esse pagamento foi pago numa conta,
# quando no comprovante ele foi pago noutra".

@pytest.fixture()
def espelho_de_um_titulo(painel_no_banco):
    """O título 2 do cenário (pago 425 em 10/06/2025, na obra CASA). No
    espelho: previsto na conta 7011, baixa consolidada repetindo a 7011, e a
    baixa BANCÁRIA na 22069 — que é por onde o dinheiro saiu de verdade."""
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM movimentos WHERE ncodtitulo = 2")
        conn.execute("DELETE FROM titulos WHERE codigo_lancamento_omie = 2")
        conn.execute("DELETE FROM contas_correntes WHERE codigo IN (7011, 22069)")
        conn.execute("INSERT INTO contas_correntes (codigo, descricao) VALUES"
                     " (7011, 'Bradesco 7011-4'), (22069, 'Bradesco 22069-8')")
        conn.execute("INSERT INTO titulos (codigo_lancamento_omie, natureza,"
                     " valor_documento, id_conta_corrente, numero_documento,"
                     " status_titulo) VALUES (2, 'P', 400, 7011, 'SP1', 'PAGO')")
        conn.execute("INSERT INTO movimentos (ncodtitulo, ddtpagamento, nvalpago,"
                     " nvalliquido, ncodcc, cliquidado) VALUES"
                     " (2, '10/06/2025', 425, 425, 7011, 'S'),"
                     " (2, '10/06/2025', 425, 0, 22069, '')")
        conn.commit()
    yield
    with conexao() as conn:
        conn.execute("DELETE FROM movimentos WHERE ncodtitulo = 2")
        conn.execute("DELETE FROM titulos WHERE codigo_lancamento_omie = 2")
        conn.commit()


def test_a_origem_da_conta_mostra_as_pernas_e_qual_valeu(espelho_de_um_titulo):
    from app.apps.painel import consultas
    d = consultas.origem_da_conta(2)
    assert d["conta_prevista"] == "Bradesco 7011-4"
    assert d["regra"] == "baixa bancária" and d["tem_baixa_bancaria"] is True
    por_tipo = {p["tipo"]: p for p in d["pernas"]}
    assert por_tipo["baixa bancária"]["conta"] == "Bradesco 22069-8"
    assert por_tipo["baixa bancária"]["valeu"] is True
    assert por_tipo["baixa consolidada"]["valeu"] is False
    assert consultas.origem_da_conta(999999) is None
    assert consultas.origem_da_conta("abc") is None


def test_sem_baixa_bancaria_a_regra_diz_que_falta_no_omie(espelho_de_um_titulo):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM movimentos WHERE ncodtitulo = 2 AND cliquidado = ''")
        conn.commit()
    d = consultas.origem_da_conta(2)
    assert d["regra"] == "baixa consolidada" and d["tem_baixa_bancaria"] is False


def test_a_origem_da_conta_e_so_do_dono(espelho_de_um_titulo, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.apps.painel import usuarios
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    dono = app.test_client()
    dono.post("/painel/entrar", data={"senha": "segredo-de-teste"})
    r = dono.get("/painel/titulo/2/conta")
    assert r.status_code == 200 and r.get_json()["regra"] == "baixa bancária"
    assert dono.get("/painel/titulo/999999/conta").status_code == 404
    html = dono.get("/painel/calendario?mes=2025-06").get_data(as_text=True)
    assert "const ADMIN = true" in html and "/painel/titulo/0/conta" in html

    from app.apps.painel.db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM usuarios WHERE usuario = 'preso-conta'")
        conn.commit()
    usuarios.criar("preso-conta", "senha-dele", obras=["CASA"], telas=["calendario"])
    preso = app.test_client()
    preso.post("/painel/entrar", data={"usuario": "preso-conta", "senha": "senha-dele"})
    assert preso.get("/painel/titulo/2/conta").status_code == 404
    assert "const ADMIN = false" in preso.get("/painel/calendario?mes=2025-06").get_data(as_text=True)
    with conexao() as conn:
        conn.execute("DELETE FROM usuarios WHERE usuario = 'preso-conta'")
        conn.commit()
