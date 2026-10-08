# -*- coding: utf-8 -*-
"""
Por que um aporte (ou dividendo) não aparece no bloco — conferência por obra.

Pedido do dono em 06/10/2026: *"embora esteja lançado no OMIE, os aportes
associados a uma obra desse projeto não estão aparecendo (…) o que pode estar
acontecendo e o que eu posso fazer para identificar isso"* — e o mesmo com
dividendos pagos. Cada regra do bloco corta calada; a conferência diz qual.
"""
from __future__ import annotations

import datetime as dt
import os

import pytest

from app.apps.painel import consultas


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


D = dt.date(2026, 3, 10)
REC, PAG = "1. Contas a Receber", "2. Contas a Pagar"


@pytest.fixture()
def obra(base):
    colunas = ("codigo_lancamento", "tipo", "analise", "situacao",
               "situacao_vencimento", "categoria", "codigo_categoria",
               "departamento", "projeto", "razao_social", "data", "ano", "mes",
               "pago_recebido", "a_pagar_receber")
    q, a = "Quitado", "A Vencer"
    linhas = [
        # entra: aporte quitado entrando na obra
        (1, REC, "Fluxo de Caixa", "Recebido", q, "Aportes BWS", "1.02.94",
         "PONTE", "", "BWS CONSTRUCOES", D, 2026, 3, 1000, 0),
        # lado provedor: categoria de quem MANDA, lançada na obra
        (2, PAG, "Fluxo de Caixa", "Pago", q, "Aportes BWS", "2.08.97",
         "PONTE", "", "BWS CONSTRUCOES", D, 2026, 3, -500, 0),
        # não quitado
        (3, REC, "Fluxo de Caixa", "A Receber", a, "Aportes Parceiros", "1.02.02",
         "PONTE", "", "SOCIO X", D, 2026, 3, 0, 700),
        # categoria que não é reconhecida
        (4, REC, "Fluxo de Caixa", "Recebido", q, "Capital de sócio", "",
         "PONTE", "", "SOCIO Y", D, 2026, 3, 300, 0),
        # dividendo pago: entra
        (5, PAG, "Fluxo de Caixa", "Pago", q, "Distribuição de Lucros", "",
         "PONTE", "", "SOCIO Y", D, 2026, 3, -200, 0),
        # despesa comum da obra: nem é candidata
        (6, PAG, "DRE", "Pago", q, "Cimento", "", "PONTE", "", "LOJA", D, 2026, 3, -90, 0),
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


def _por_codigo(dados):
    return {l["codigo"]: l for l in dados["linhas"]}


@pytest.mark.banco
def test_cada_candidato_diz_por_que_entra_ou_fica_de_fora(obra):
    d = _por_codigo(consultas.conferir_aportes(consultas.Filtros(), obra="PONTE"))
    assert 6 not in d                                  # cimento não é candidato
    assert d[1]["entra"] and "aporte" in d[1]["motivo"]
    assert not d[2]["entra"] and "2.08.97" in d[2]["motivo"]
    assert not d[3]["entra"] and "quitado" in d[3]["motivo"]
    assert not d[4]["entra"] and "não é reconhecida" in d[4]["motivo"]
    assert d[5]["entra"] and "dividendo" in d[5]["motivo"]


@pytest.mark.banco
def test_o_filtro_de_projeto_da_tela_e_apontado_como_culpado(obra):
    """A obra está sem projeto: com a tela filtrada num projeto, o aporte some
    do bloco — e a conferência diz isso, em vez de dizer só "entra"."""
    f = consultas.Filtros(projetos=["OUTRO"])
    d = _por_codigo(consultas.conferir_aportes(f, obra="PONTE"))
    assert d[1]["entra"] and "SEM PROJETO" in d[1]["motivo"]
    f = consultas.Filtros(anos=[2025])
    d = _por_codigo(consultas.conferir_aportes(f, obra="PONTE"))
    assert "ANO" in d[1]["motivo"]


@pytest.mark.banco
def test_pelo_numero_acha_em_qualquer_obra_e_categoria(obra):
    d = consultas.conferir_aportes(consultas.Filtros(), codigo="6")
    assert [l["codigo"] for l in d["linhas"]] == [6]
    assert not d["linhas"][0]["entra"]


@pytest.mark.banco
def test_a_tela_abre_a_conferencia(obra, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "senha-do-dono-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "senha-do-dono-teste"})
    r = cliente.get("/painel/dre/conferir-aportes?obra_conferida=PONTE").get_json()
    assert r["ok"] and r["quantos"] == 5 and r["entram"] == 2
    assert cliente.get("/painel/dre/conferir-aportes").status_code == 400
    # mudou do DRE para Configurações em 07/10/2026 (o dono: "não é para estar
    # na apresentação")
    html = cliente.get("/painel/dre?bloco=aportes").get_data(as_text=True)
    assert "Não achou um aporte ou dividendo?" not in html
    html = cliente.get("/painel/configuracoes").get_data(as_text=True)
    assert "Não achou um aporte ou dividendo?" in html
    assert '<option value="PONTE">' in html
