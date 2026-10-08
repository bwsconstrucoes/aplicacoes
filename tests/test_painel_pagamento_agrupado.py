# -*- coding: utf-8 -*-
"""
Um pagamento dividido entre obras é UM débito no banco.

Pedido do dono em 06/10/2026: *"tem um determinado pagamento, que é o mesmo
título, só que ele está dividido para duas obras. Visualmente a gente enxerga
dois lançamentos. Mas se você for verificar na conta corrente, eles somam o
valor. (…) seria interessante que a gente pudesse ver de forma consolidada e
expandido os dois lançamentos."*
"""
from __future__ import annotations

import datetime as dt
import os

import pytest

from app.apps.painel import consultas


def test_a_chave_junta_titulo_dia_e_conta():
    a = consultas.chave_do_movimento(10, dt.date(2026, 5, 11), "Bradesco")
    assert a == consultas.chave_do_movimento("10", "2026-05-11", "Bradesco")
    assert a != consultas.chave_do_movimento(10, dt.date(2026, 5, 11), "Itaú")
    assert a != consultas.chave_do_movimento(10, dt.date(2026, 5, 11), "Bradesco",
                                             em_aberto=True)
    assert consultas.chave_do_movimento(10, None, None).endswith("|(sem conta)")


def test_agrupar_diz_o_que_a_tela_ve_e_o_que_o_banco_ve():
    """Com o filtro numa obra só, a tela vê uma parte; o grupo avisa que há
    outra fora do filtro e quanto é o débito inteiro no extrato."""
    chave = consultas.chave_do_movimento(1, "2026-05-11", "BD")
    outra = consultas.chave_do_movimento(2, "2026-05-11", "BD")
    linhas = [{"movimento": chave, "valor": -600.0, "obra": "CASA"},
              {"movimento": outra, "valor": -50.0, "obra": "LOJA"}]
    partes = {chave: {"partes": 2, "obras": 2, "total": -1000.0}}
    g1, g2 = consultas.agrupar_por_movimento(linhas, partes)
    assert (g1["valor"], g1["no_extrato"], g1["partes"], g1["fora_do_filtro"]) == \
        (-600.0, -1000.0, 2, 1)
    # sem informação da base, o grupo é ele mesmo
    assert (g2["no_extrato"], g2["partes"], g2["fora_do_filtro"]) == (-50.0, 1, 0)


# ---------------------------------------------------------------------------
# Com banco de verdade
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def base():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura
    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)
    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    painel_db._engine = None
    assert not migracoes_runner.aplicar_pendentes().get("erro")
    yield painel_db
    painel_db._engine = None


DIA = dt.date(2026, 5, 11)


@pytest.fixture()
def pagamentos(base):
    """O título 500 pago num dia, numa conta, dividido entre CASA (600) e
    LOJA (400): no banco é um débito de 1.000. O título 600 é de uma obra só."""
    colunas = ("codigo_lancamento", "tipo", "analise", "situacao",
               "situacao_vencimento", "categoria",
               "grupo", "departamento", "projeto", "razao_social", "conta_corrente",
               "data", "data_pagamento", "data_vencimento", "ano", "mes",
               "pago_recebido", "a_pagar_receber")
    linhas = [
        (500, "2. Contas a Pagar", "DRE", "Pago", "Quitado", "Aluguel", "Administrativas", "CASA", "P",
         "LOCADORA", "Bradesco", DIA, DIA, DIA, 2026, 5, -600, 0),
        (500, "2. Contas a Pagar", "DRE", "Pago", "Quitado", "Aluguel", "Administrativas", "LOJA", "P",
         "LOCADORA", "Bradesco", DIA, DIA, DIA, 2026, 5, -400, 0),
        (600, "2. Contas a Pagar", "DRE", "Pago", "Quitado", "Energia", "Administrativas", "CASA", "P",
         "COELCE", "Bradesco", DIA, DIA, DIA, 2026, 5, -80, 0),
    ]
    with base.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({','.join('?' * len(colunas))})",
            linhas)
        conn.commit()
    consultas.esquecer_listas()
    yield
    with base.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.commit()
    consultas.esquecer_listas()


@pytest.mark.banco
def test_as_partes_somam_o_debito_da_conta(pagamentos):
    partes = consultas.partes_dos_movimentos([500, 600])
    assert partes[consultas.chave_do_movimento(500, DIA, "Bradesco")] == \
        {"partes": 2, "obras": 2, "total": -1000.0}
    assert partes[consultas.chave_do_movimento(600, DIA, "Bradesco")]["partes"] == 1


@pytest.mark.banco
def test_quem_esta_preso_so_conta_as_partes_que_ve(pagamentos):
    """Somar a parte da obra de outro revelaria quanto foi para ela."""
    escopo = consultas.Filtros(departamentos=["CASA"], excluir_trf=False)
    partes = consultas.partes_dos_movimentos([500], escopo)
    assert partes[consultas.chave_do_movimento(500, DIA, "Bradesco")] == \
        {"partes": 1, "obras": 1, "total": -600.0}


@pytest.mark.banco
def test_o_analitico_agrupado_pagina_por_pagamento(pagamentos):
    f = consultas.Filtros()
    dados = consultas.analitico_despesas(f, visao="executado", agrupar=True,
                                         por_pagina=1, ordem="valor")
    assert dados["agrupado"] and dados["quantos_pagamentos"] == 2
    assert dados["paginas"] == 2                 # 2 pagamentos, 1 por página
    (g,) = dados["grupos"]
    assert g["partes_aqui"] == 2 and g["obras"] == ["CASA", "LOJA"]
    assert g["pago"] == -1000.0 and g["no_extrato"] == -1000.0
    # a segunda página traz o outro pagamento, inteiro
    (g2,) = consultas.analitico_despesas(f, visao="executado", agrupar=True,
                                         por_pagina=1, pagina=2)["grupos"]
    assert g2["linhas"][0]["lancamento"] == 600


@pytest.mark.banco
def test_filtrado_numa_obra_a_linha_diz_que_e_parte_de_um_debito_maior(pagamentos):
    f = consultas.Filtros(departamentos=["CASA"])
    dados = consultas.analitico_despesas(f, visao="executado", marcar_partes=True)
    casa = next(l for l in dados["linhas"] if l["lancamento"] == 500)
    assert casa["pagamento"]["partes"] == 2
    assert casa["pagamento"]["fora_do_filtro"] == 1
    assert casa["pagamento"]["no_extrato"] == -1000.0


@pytest.mark.banco
def test_as_telas_abrem_agrupadas(pagamentos, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "senha-do-dono-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "senha-do-dono-teste"})

    # SEMPRE agrupado, sem o selo "partes · no extrato" (dono, 08/10/2026:
    # "ninguém que opere esse painel vai entender")
    html = cliente.get("/painel/analitico?visao=executado").get_data(as_text=True)
    assert "2 obras" in html and "no extrato" not in html and "partes ·" not in html
    assert 'name="agrupar"' not in html
    # o filtro numa obra só: o aviso de que o pagamento tem outra obra fica
    html = cliente.get("/painel/analitico?visao=executado&obra=CASA").get_data(as_text=True)
    assert "+1 obra fora do filtro" in html

    r = cliente.get(f"/painel/calendario/dia?dia={DIA.isoformat()}").get_json()
    assert r["ok"]
    grupo = next(g for g in r["grupos"] if len(g["linhas"]) == 2)
    assert grupo["valor"] == -1000.0 and grupo["obras"] == ["CASA", "LOJA"]
