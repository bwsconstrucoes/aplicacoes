# -*- coding: utf-8 -*-
"""
O extrato conferível — uma conta, dia a dia, com saldo (24/11/2026).

O dono: "a gente pegar o extrato da conta corrente, inclusive eu poder comparar
com o extrato aberto, o extrato de um lado e a conta do outro. E conseguir
visualizar o saldo do dia. E naquele extrato do painel, eu poder enxergar cada
título e a que se refere (…) Se for rateado em obras, a gente poder clicar e
expandir." E, sobre o saldo: "vai precisar ser informado o saldo inicial, para
poder bater."

O que se prova: o saldo sai de UM dia informado, para frente e para trás; um
pagamento rateado é UMA linha; o que nunca passou pela conta (imposto retido,
título em aberto) não entra; e só o administrador chega nesta visão.
"""
from __future__ import annotations

import datetime as dt
import io
import os

import pytest

BRADESCO = "Bradesco 22069-8"


# ===========================================================================
# 1. Sem banco: montar o extrato
# ===========================================================================
def _linha(dia, codigo, valor, *, obra="OBRA A", categoria="Serviços", tipo="2. Contas a Pagar",
           analise="DRE", quem="FORNECEDOR X"):
    return {"data": dt.date.fromisoformat(dia), "codigo": codigo, "tipo": tipo, "analise": analise,
            "quem": quem, "cnpj": "11222333000144", "grupo": "Custo", "categoria": categoria,
            "obra": obra, "projeto": "ALFA", "documento": "NF 1", "observacao": "", "link": "",
            "valor": valor}


LINHAS = [
    _linha("2025-09-08", 10, 1000.0, tipo="1. Contas a Receber", quem="CLIENTE"),
    # um pagamento só, rateado em duas obras: UM débito de 300 no banco
    _linha("2025-09-08", 11, -200.0, obra="OBRA A"),
    _linha("2025-09-08", 11, -100.0, obra="OBRA B"),
    _linha("2025-09-09", 12, -50.0, analise="TRF"),
    _linha("2025-09-09", 13, 20.0, analise="Fluxo de Caixa", tipo="1. Contas a Receber"),
    # sem número: não se junta com o vizinho
    _linha("2025-09-09", None, -5.0),
    _linha("2025-09-09", None, -5.0),
]


def test_cada_dia_fecha_com_o_saldo_e_o_rateado_e_uma_linha():
    from app.apps.painel import extrato
    x = extrato.montar(LINHAS, saldo_antes=500.0, conferidos={"2025-09-08"})
    assert x["tem_saldo"] and x["saldo_inicial"] == 500.0
    assert [d["rotulo"] for d in x["dias"]] == ["08/09/2025", "09/09/2025"]
    primeiro, segundo = x["dias"]
    # 2 movimentos no banco (não 3 linhas): o recebimento e o pagamento rateado
    assert primeiro["quantos"] == 2 and primeiro["conferido"] and not segundo["conferido"]
    recebido, rateado = primeiro["movimentos"]
    assert (recebido["tipo"], recebido["valor"], recebido["saldo"]) == ("receita", 1000.0, 1500.0)
    assert rateado["rateado"] and rateado["n_obras"] == 2 and rateado["obra"] == ""
    assert (rateado["valor"], rateado["saldo"]) == (-300.0, 1200.0)
    assert [(p["obra"], p["valor"]) for p in rateado["partes"]] == [("OBRA A", -200.0), ("OBRA B", -100.0)]
    assert (primeiro["entrou"], primeiro["saiu"], primeiro["saldo_final"]) == (1000.0, 300.0, 1200.0)
    # o segundo dia: transferência, fora do resultado e dois lançamentos sem número
    assert [m["tipo"] for m in segundo["movimentos"]] == ["transferencia", "fora", "despesa", "despesa"]
    assert segundo["saldo_final"] == 1160.0
    assert (x["entrou"], x["saiu"], x["saldo_final"], x["quantos"]) == (1020.0, 360.0, 1160.0, 6)
    assert x["dias_conferidos"] == 1


def test_sem_saldo_informado_a_coluna_e_a_variacao():
    from app.apps.painel import extrato
    x = extrato.montar(LINHAS)
    assert not x["tem_saldo"] and x["saldo_inicial"] == 0.0
    assert x["saldo_final"] == 660.0                    # entrou 1.020, saiu 360
    assert extrato.montar([])["dias"] == []


def test_o_saldo_digitado_guarda_o_sinal():
    """Conta no cheque especial tem saldo negativo; e saldo em branco é erro,
    não zero — gravar zero por engano desalinharia o extrato inteiro."""
    from app.apps.painel import extrato
    assert extrato.ler_saldo("184.320,55") == 184320.55
    assert extrato.ler_saldo("-1.250,50") == -1250.50
    assert extrato.ler_saldo("−1.250,50") == -1250.50     # o menos tipográfico
    assert extrato.ler_saldo("(1.250,50)") == -1250.50
    assert extrato.ler_saldo("R$ 0,00") == 0.0
    for ruim in ("", "   ", None, "abc"):
        with pytest.raises(ValueError):
            extrato.ler_saldo(ruim)


def test_o_periodo_que_falta_e_completado():
    from app.apps.painel import extrato
    hoje = dt.date(2026, 11, 24)
    assert extrato.periodo_padrao("", "", hoje) == ("2026-11-01", "2026-11-24")
    assert extrato.periodo_padrao("2026-02-10", "", hoje) == ("2026-02-10", "2026-02-28")
    assert extrato.periodo_padrao("", "2026-03-15", hoje) == ("2026-03-01", "2026-03-15")
    assert extrato.periodo_padrao("2026-01-05", "2026-01-09", hoje) == ("2026-01-05", "2026-01-09")


def test_as_abas_do_arquivo_trazem_o_saldo_e_as_partes():
    from app.apps.painel import extrato
    extrato_, por_dia = extrato.abas(extrato.montar(LINHAS, saldo_antes=500.0, conferidos={"2025-09-08"}))
    assert (extrato_[0], por_dia[0]) == ("Extrato dia a dia", "Saldo por dia")
    linhas = extrato_[2]
    assert linhas[0]["tipo"] == "SALDO ANTERIOR" and linhas[0]["saldo"] == 500.0
    rateado = next(l for l in linhas if l["obra"] == "rateado em 2 obras")
    assert (rateado["saiu"], rateado["saldo"]) == (300.0, 1200.0)
    partes = [l for l in linhas if l["tipo"].strip() == "parte"]
    assert [(p["obra"], p["parte"]) for p in partes] == [("OBRA A", -200.0), ("OBRA B", -100.0)]
    for _nome, colunas, dados in (extrato_, por_dia):
        for linha in dados:
            assert all(chave in linha for chave, _t in colunas)
    assert [(d["dia"], d["saldo"], d["conferido"]) for d in por_dia[2]] == \
        [("08/09/2025", 1200.0, "sim"), ("09/09/2025", 1160.0, "")]
    # sem saldo informado o título da coluna diz que é variação
    sem = extrato.abas(extrato.montar(LINHAS))[0]
    assert sem[1][-1][1] == "Saldo (variação no período)" and sem[2][0]["tipo"] != "SALDO ANTERIOR"


# ===========================================================================
# 2. Com banco
# ===========================================================================
banco = pytest.mark.banco


@pytest.fixture()
def conta_de_teste():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura
    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import consultas
    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    painel_db._engine = None
    assert not migracoes_runner.aplicar_pendentes().get("erro")

    def limpar(conn):
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM usuarios")
        conn.execute("DELETE FROM extrato_saldo_informado")
        conn.execute("DELETE FROM extrato_dia_conferido")

    with painel_db.conexao() as conn:
        limpar(conn)
        # (codigo, tipo, analise, categoria, obra, conta, pago, a_pagar, juros, data, situacao)
        R, P = "1. Contas a Receber", "2. Contas a Pagar"
        for cod, tipo, analise, cat, obra, conta, pago, aberto, juros, data, sit in (
            (700, P, "DRE", "Aluguel", "MATRIZ", BRADESCO, -900, 0, 0, "2025-08-29", "PAGO"),
            (801, R, "DRE", "Receita de Obras", "OBRA A", BRADESCO, 1000, 0, 0, "2025-09-08", "RECEBIDO"),
            # o imposto que o cliente reteve: nunca passou pela conta
            (802, R, "DRE", "ISS Retido", "OBRA A", BRADESCO, 50, 0, 0, "2025-09-08", "RECEBIDO"),
            # um pagamento rateado em duas obras
            (803, P, "DRE", "Concreto", "OBRA A", BRADESCO, -200, 0, 0, "2025-09-08", "PAGO"),
            (803, P, "DRE", "Concreto", "OBRA B", BRADESCO, -100, 0, 0, "2025-09-08", "PAGO"),
            # pago com 4 de juros: saíram 44 da conta
            (804, P, "DRE", "Serviços", "OBRA A", BRADESCO, -40, 0, -4, "2025-09-09", "PAGO"),
            (805, P, "TRF", "Transferência", "MATRIZ", BRADESCO, -500, 0, 0, "2025-09-09", "PAGO"),
            # em aberto: não saiu de conta nenhuma
            (806, P, "DRE", "Serviços", "OBRA A", BRADESCO, 0, -70, 0, "2025-09-09", "A PAGAR"),
            # outra conta
            (807, P, "DRE", "Serviços", "OBRA A", "Itaú 7011-4", -777, 0, 0, "2025-09-08", "PAGO"),
        ):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao, situacao_vencimento,"
                " categoria, grupo, departamento, projeto, razao_social, cnpj_cpf, conta_corrente,"
                " numero_documento, data, ano, pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,?,?,?,?,?,'Grupo',?,'ALFA','FORNECEDOR X','11222333000144',?,'NF 1',?,2025,"
                "         ?,?,?,0)",
                (cod, tipo, analise, sit, "Quitado" if pago else "A vencer", cat, obra, conta,
                 data, pago, aberto, juros))
        conn.commit()
    consultas.esquecer_listas()
    yield
    with painel_db.conexao() as conn:
        limpar(conn)
        conn.commit()


def _admin(monkeypatch, senha="senha-do-dono"):
    monkeypatch.setenv("PAINEL_SENHA", senha)
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    c = app.test_client()
    c.post("/painel/entrar", data={"usuario": "", "senha": senha})
    return c


@banco
def test_so_entra_o_que_passou_pela_conta(conta_de_teste):
    from app.apps.painel import extrato
    x = extrato.do_pedido(BRADESCO, "2025-09-01", "2025-09-30")
    codigos = [m["codigo"] for d in x["dias"] for m in d["movimentos"]]
    assert codigos == [801, 803, 804, 805], \
        "retido (802), em aberto (806) e a outra conta (807) não passaram por esta conta"
    por_codigo = {m["codigo"]: m for d in x["dias"] for m in d["movimentos"]}
    assert por_codigo[803]["rateado"] and por_codigo[803]["valor"] == -300.0
    assert por_codigo[804]["valor"] == -44.0, "os juros pagos saíram da conta junto"
    assert por_codigo[805]["tipo"] == "transferencia"
    assert (x["entrou"], x["saiu"]) == (1000.0, 844.0)
    assert not x["tem_saldo"] and x["informado"] is None


@banco
def test_o_saldo_sai_de_um_dia_informado_para_frente_e_para_tras(conta_de_teste):
    from app.apps.painel import extrato
    # o banco diz: ao fim de 08/09 havia 5.000 na conta
    extrato.gravar_saldo(BRADESCO, "2025-09-08", 5000.0, "teste")
    informado = extrato.saldo_informado(BRADESCO)
    assert (informado["dia"], informado["saldo"]) == (dt.date(2025, 9, 8), 5000.0)

    # para trás: antes de 08/09 entraram 1.000 e saíram 300 naquele dia → 4.300
    x = extrato.do_pedido(BRADESCO, "2025-09-08", "2025-09-09")
    assert x["saldo_inicial"] == 4300.0
    assert [d["saldo_final"] for d in x["dias"]] == [5000.0, 4456.0]     # 5.000 − 44 − 500
    # para frente: começando em 09/09, o saldo anterior é o informado
    assert extrato.do_pedido(BRADESCO, "2025-09-09", "2025-09-30")["saldo_inicial"] == 5000.0
    # bem antes: tira também o aluguel de 29/08 (−900): 4.300 + 900
    antes = extrato.do_pedido(BRADESCO, "2025-08-01", "2025-08-31")
    assert antes["saldo_inicial"] == 5200.0 and antes["saldo_final"] == 4300.0
    # bem depois: nada andou, o saldo é o do fim de 09/09
    depois = extrato.do_pedido(BRADESCO, "2025-10-01", "2025-10-31")
    assert depois["saldo_inicial"] == depois["saldo_final"] == 4456.0 and depois["dias"] == []
    # informar de novo troca o anterior
    extrato.gravar_saldo(BRADESCO, "2025-09-09", -10.0, "teste")
    assert extrato.do_pedido(BRADESCO, "2025-09-09", "2025-09-09")["saldo_final"] == -10.0


@banco
def test_a_tela_do_administrador_abre_no_dia_a_dia_com_uma_conta(conta_de_teste, monkeypatch):
    from app.apps.painel import extrato
    extrato.gravar_saldo(BRADESCO, "2025-09-08", 5000.0, "teste")
    cliente = _admin(monkeypatch)
    r = cliente.get("/painel/extrato?conta=Bradesco+22069-8&de=2025-09-08&ate=2025-09-09")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    for trecho in ("Dia a dia, com saldo", "Lista de lançamentos", "Saldo informado",
                   "Saldo antes de 08/09/2025", "4.300,00", "5.000,00", "4.456,00",
                   "2 obras", "conferi com o banco", "11222333000144", "Transferência"):
        assert trecho in html, trecho
    assert "ISS Retido" not in html and "Sem transferências" not in html
    # a lista de antes continua lá, pelo nome
    html = cliente.get("/painel/extrato?conta=Bradesco+22069-8&visao=lancamentos").get_data(as_text=True)
    assert 'id="lista-extrato"' in html and "conferi com o banco" not in html
    # sem UMA conta escolhida não há saldo para mostrar: a tela pede a conta
    html = cliente.get("/painel/extrato?visao=saldo").get_data(as_text=True)
    assert "Escolha UMA conta" in html
    # e sem conta o administrador continua caindo na lista, como sempre
    assert 'id="lista-extrato"' in cliente.get("/painel/extrato").get_data(as_text=True)


@banco
def test_informar_o_saldo_e_marcar_o_dia_pela_tela(conta_de_teste, monkeypatch):
    from app.apps.painel import extrato
    cliente = _admin(monkeypatch)
    r = cliente.post("/painel/api/extrato/saldo", data={
        "conta": BRADESCO, "dia": "2025-09-08", "saldo": "-1.250,50", "de": "2025-09-08", "ate": "2025-09-09"})
    assert r.status_code == 302 and "visao=saldo" in r.headers["Location"]
    assert extrato.saldo_informado(BRADESCO)["saldo"] == -1250.50
    # valor que não é número, ou em branco: não grava, e diz
    for ruim in ("abc", ""):
        r = cliente.post("/painel/api/extrato/saldo", data={"conta": BRADESCO, "dia": "2025-09-08", "saldo": ruim})
        assert "erro=" in r.headers["Location"]
    assert extrato.saldo_informado(BRADESCO)["saldo"] == -1250.50

    r = cliente.post("/painel/api/extrato/conferido", json={"conta": BRADESCO, "dia": "2025-09-08", "conferido": True})
    assert r.get_json() == {"ok": True, "conferido": True}
    assert extrato.dias_conferidos(BRADESCO, "2025-09-01", "2025-09-30") == {"2025-09-08"}
    cliente.post("/painel/api/extrato/conferido", json={"conta": BRADESCO, "dia": "2025-09-08", "conferido": True})
    cliente.post("/painel/api/extrato/conferido", json={"conta": BRADESCO, "dia": "2025-09-08", "conferido": False})
    assert extrato.dias_conferidos(BRADESCO, "2025-09-01", "2025-09-30") == set()
    assert cliente.post("/painel/api/extrato/conferido", json={"conta": "", "dia": "x"}).status_code == 400


@banco
def test_sem_a_atualizacao_do_banco_a_tela_abre_e_avisa(conta_de_teste, monkeypatch):
    """O código sobe antes de o botão ser apertado: a tela não pode cair por
    causa das tabelas novas (a lição de 11/11, quando Configurações caiu)."""
    from app.apps.painel import extrato
    monkeypatch.setattr(extrato, "tabelas_prontas", lambda: False)
    cliente = _admin(monkeypatch)
    r = cliente.get("/painel/extrato?conta=Bradesco+22069-8&de=2025-09-08&ate=2025-09-09")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert "Falta aplicar as atualizações do banco" in html and "Variação no período" in html
    assert "conferi com o banco" not in html
    r = cliente.post("/painel/api/extrato/saldo", data={"conta": BRADESCO, "dia": "2025-09-08", "saldo": "10"})
    assert "erro=" in r.headers["Location"]
    assert cliente.post("/painel/api/extrato/conferido",
                        json={"conta": BRADESCO, "dia": "2025-09-08", "conferido": True}).status_code == 409


@banco
def test_quem_tem_login_proprio_fica_na_lista_e_nao_grava(conta_de_teste, monkeypatch):
    """Quem tem login próprio vê a conta recortada nas obras dele: saldo de
    conta pela metade não é saldo. A visão é só do administrador."""
    from app.apps.painel import usuarios
    usuarios.criar("financeiro", "senha-dele", obras=["OBRA A"], telas=["extrato"], contas=[BRADESCO])
    cliente = _admin(monkeypatch)
    cliente.get("/painel/sair")
    cliente.post("/painel/entrar", data={"usuario": "financeiro", "senha": "senha-dele"})
    for caminho in ("/painel/extrato", "/painel/extrato?visao=saldo"):
        html = cliente.get(caminho).get_data(as_text=True)
        assert 'id="lista-extrato"' in html, caminho
        assert "Dia a dia, com saldo" not in html and "conferi com o banco" not in html, caminho
    assert cliente.post("/painel/api/extrato/saldo",
                        data={"conta": BRADESCO, "dia": "2025-09-08", "saldo": "1"}).status_code == 404
    assert cliente.post("/painel/api/extrato/conferido",
                        json={"conta": BRADESCO, "dia": "2025-09-08", "conferido": True}).status_code == 404
    # e o arquivo dele é a lista, mesmo pedindo o outro
    from openpyxl import load_workbook
    r = cliente.get("/painel/baixar/extrato?visao=saldo")
    assert load_workbook(io.BytesIO(r.data)).sheetnames == ["Extrato de Conta"]


@banco
def test_o_extrato_dia_a_dia_sai_em_planilha_e_em_pdf(conta_de_teste, monkeypatch):
    from openpyxl import load_workbook
    from app.apps.painel import extrato
    extrato.gravar_saldo(BRADESCO, "2025-09-08", 5000.0, "teste")
    cliente = _admin(monkeypatch)
    base = "/painel/baixar/extrato?conta=Bradesco+22069-8&visao=saldo&de=2025-09-08&ate=2025-09-09"
    livro = load_workbook(io.BytesIO(cliente.get(base).data))
    assert livro.sheetnames == ["Extrato dia a dia", "Saldo por dia"]
    por_dia = [[c.value for c in linha] for linha in livro["Saldo por dia"].iter_rows(min_row=2)]
    assert [(l[0], l[4]) for l in por_dia] == [("08/09/2025", 5000), ("09/09/2025", 4456)]
    r = cliente.get(base + "&formato=pdf")
    assert r.status_code == 200 and r.data[:4] == b"%PDF"
    # a lista de lançamentos continua saindo como antes
    livro = load_workbook(io.BytesIO(cliente.get(
        "/painel/baixar/extrato?conta=Bradesco+22069-8&visao=lancamentos").data))
    assert livro.sheetnames == ["Extrato de Conta"]
