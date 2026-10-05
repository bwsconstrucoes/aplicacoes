# -*- coding: utf-8 -*-
"""
Projeto das obras, em Parâmetros — corrigir o "(sem projeto)" pela tela.

Pedido do dono em 05/10/2026: *"quero poder corrigir isso dentro de parâmetros
e regras"*, sobre a lista de "Obras sem projeto". O projeto vem da planilha
"C. Diários"; o que se marca na tela vale por cima dela, NA HORA (os
lançamentos já gravados mudam) e nas cargas seguintes.
"""
from __future__ import annotations

import os

import pytest

from app.apps.painel.sync import projetos


def test_o_guardado_estragado_nao_derruba_a_carga():
    assert projetos.ler_projetos_da_tela("") == {}
    assert projetos.ler_projetos_da_tela(None) == {}
    assert projetos.ler_projetos_da_tela("isto não é json") == {}
    assert projetos.ler_projetos_da_tela("[1, 2]") == {}
    assert projetos.ler_projetos_da_tela(
        '{" 10 ": " ALFA ", "11": "", "": "BETA"}') == {"10": "ALFA"}


# ---------------------------------------------------------------------------
# Com banco de verdade
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def base(request):
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


@pytest.fixture()
def obras(base):
    """Três obras no OMIE: uma com projeto na planilha, uma sem, e uma sem
    que já mudou de nome (dois nomes no rateio, o mesmo código)."""
    from app.apps.painel import consultas
    with base.conexao() as conn:
        for tabela in ("fato", "fato_recebimentos", "rateio", "depto_projeto"):
            conn.execute(f"TRUNCATE TABLE {tabela}")
        conn.execute("DELETE FROM config WHERE chave = 'projeto_da_obra'")
        conn.executemany(
            "INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep, cdesdep,"
            " nperdep, nvaldep) VALUES (?,?,?,?,100,10)",
            [(1, 1, "10", "CASA"), (2, 1, "20", "QUIOSQUES"),
             (3, 1, "30", "PAVFUJITA"), (4, 1, "30", "PAV FUJITA")])
        conn.execute("INSERT INTO depto_projeto (ccoddep, projeto) VALUES ('10', 'ALFA')")
        conn.executemany(
            "INSERT INTO fato (codigo_lancamento, departamento, projeto)"
            " VALUES (?,?,?)",
            [(1, "CASA", "ALFA"), (2, "QUIOSQUES", ""),
             (3, "PAVFUJITA", ""), (4, "PAV FUJITA", "")])
        conn.execute("INSERT INTO fato_recebimentos (codigo_lancamento, departamento,"
                     " projeto) VALUES (2, 'QUIOSQUES', '')")
        conn.commit()
    consultas.esquecer_listas()
    yield
    # ⚠️ LIMPAR NA SAÍDA TAMBÉM: a `config` mora no banco do trabalhador e
    # sobrevive ao arquivo. O projeto dado aqui ficava para o próximo arquivo
    # do mesmo trabalhador — e o teste do cenário que prova que o preso NÃO
    # grava o projeto falhava conforme a ordem (05/10/2026).
    with base.conexao() as conn:
        conn.execute("DELETE FROM config WHERE chave = 'projeto_da_obra'")
        conn.commit()
    consultas.esquecer_listas()


def _projeto_no_fato(base, obra):
    with base.conexao() as conn:
        return {p for (p,) in conn.execute(
            "SELECT projeto FROM fato WHERE departamento = ?", (obra,)).fetchall()}


@pytest.mark.banco
def test_a_tela_lista_quem_esta_sem_projeto(obras):
    from app.apps.painel import prestacao_dados
    lista = {o["codigo"]: o for o in prestacao_dados.obras_e_projetos()}
    assert lista["10"]["efetivo"] == "ALFA"
    assert lista["20"]["efetivo"] == "" and lista["30"]["efetivo"] == ""


@pytest.mark.banco
def test_dar_o_projeto_vale_na_hora_em_todos_os_nomes_da_obra(obras, base):
    from app.apps.painel import consultas, prestacao_dados
    assert consultas.obra_para_projeto()["QUIOSQUES"] == ""   # lembrado antes

    mudou = prestacao_dados.definir_projeto_da_obra("20", "  BETA ")
    prestacao_dados.definir_projeto_da_obra("30", "BETA")

    assert mudou == {"fato": 1, "fato_recebimentos": 1}
    assert _projeto_no_fato(base, "QUIOSQUES") == {"BETA"}
    assert _projeto_no_fato(base, "PAV FUJITA") == {"BETA"}
    assert _projeto_no_fato(base, "PAVFUJITA") == {"BETA"}
    # a lista guardada em memória foi esquecida: o acesso por projeto já vê
    assert consultas.obra_para_projeto()["QUIOSQUES"] == "BETA"
    assert "QUIOSQUES" in consultas.obras_dos_projetos(["BETA"])
    lista = {o["codigo"]: o for o in prestacao_dados.obras_e_projetos()}
    assert lista["20"] == {"codigo": "20", "obra": "QUIOSQUES", "planilha": "",
                           "tela": "BETA", "efetivo": "BETA"}


@pytest.mark.banco
def test_a_tela_vale_por_cima_da_planilha_e_tirar_devolve_a_ela(obras, base):
    from app.apps.painel import prestacao_dados
    prestacao_dados.definir_projeto_da_obra("10", "GAMA")
    assert _projeto_no_fato(base, "CASA") == {"GAMA"}

    prestacao_dados.definir_projeto_da_obra("10", "")
    assert _projeto_no_fato(base, "CASA") == {"ALFA"}       # o da planilha
    assert "10" not in projetos.ler_projetos_da_tela(
        prestacao_dados.config().get("projeto_da_obra"))


@pytest.mark.banco
def test_a_proxima_carga_chega_ao_mesmo_projeto(obras, base):
    """O fato é refeito a cada atualização a partir dos catálogos: o projeto
    dado na tela tem de estar lá, senão a correção sumiria no dia seguinte."""
    from app.apps.painel import prestacao_dados
    from app.apps.painel.sync import fato
    prestacao_dados.definir_projeto_da_obra("20", "BETA")
    prestacao_dados.definir_projeto_da_obra("10", "GAMA")
    with base.conexao() as conn:
        _cat, _cli, proj, _cc = fato.carregar_catalogos(conn)
    assert proj["20"] == "BETA"
    assert proj["10"] == "GAMA"          # por cima da planilha
    assert "30" not in proj


@pytest.mark.banco
def test_a_aba_abre_e_grava_pela_tela(obras, base, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "senha-do-dono-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    cliente = app.test_client()
    cliente.post("/painel/entrar", data={"senha": "senha-do-dono-teste"})

    html = cliente.get("/painel/prestacao/parametros?aba=projetos").get_data(as_text=True)
    assert "Obras sem projeto" in html and "QUIOSQUES" in html
    assert "C. Diários" in html

    r = cliente.post("/painel/prestacao/parametros", data={
        "acao": "projeto_da_obra", "aba": "projetos", "codigo": "20",
        "projeto": "BETA"})
    assert r.status_code == 302
    assert _projeto_no_fato(base, "QUIOSQUES") == {"BETA"}

    cliente.post("/painel/prestacao/parametros", data={
        "acao": "projeto_da_obra", "aba": "projetos", "codigo": "20",
        "tirar": "1"})
    assert _projeto_no_fato(base, "QUIOSQUES") == {""}
