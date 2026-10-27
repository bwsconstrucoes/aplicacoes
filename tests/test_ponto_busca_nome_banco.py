# -*- coding: utf-8 -*-
"""Ponto — a busca pelo NOME no ponto da obra (27/10/2026, core/busca_nome.py): para
quem esqueceu o CPF e não está com o QR. Só com o responsável conectado, 3 letras ou
mais, sem o CPF na resposta; a pessoa escolhida bate com a ESCOLHA assinada, a batida
fica marcada "identificada pelo nome" e sempre passa pela conferência do rosto. O
funcionário novo (admitido há até 30 dias, ou sem obra) aparece mesmo sem ser da obra
— a observação do dono."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _entrar_no_app,  # noqa: F401
                                           _schema_ponto2, app, como, mundo)
from tests.test_ponto_papeis_banco import _limpar_ponto
from tests.test_ponto_qr_banco import _foto, _tablet

CPF_NOVO = "39053344705"
CPF_ANTIGO = "12345678909"
CPF_SEM_OBRA = "98765432100"


# ---------------------------------------------------------------------------
# As partes puras
# ---------------------------------------------------------------------------
def test_cada_palavra_digitada_e_comeco_de_uma_palavra_do_nome():
    from app.apps.ponto.core import busca_nome
    assert busca_nome.casa("jos sous", "JOSÉ RIBAMAR SOUSA")
    assert busca_nome.casa("ribam", "José Ribamar Sousa")
    assert not busca_nome.casa("amar", "José Ribamar Sousa")          # começo da palavra, não o meio
    assert not busca_nome.casa("jos silva", "José Ribamar Sousa")
    assert not busca_nome.casa("", "José")
    assert busca_nome.nome_curto("José Ribamar de Sousa") == "José Sousa"
    assert busca_nome.nome_curto("Pelé") == "Pelé"


def test_a_escolha_vale_so_neste_aparelho_e_por_pouco_tempo():
    from app.apps.ponto.core import qr
    from app.apps.ponto.erros import ErroDeValidacao
    e = qr.emitir_escolha(7, 3, agora=1000)
    assert qr.conferir_escolha(e, 3, agora=1100) == 7
    with pytest.raises(ErroDeValidacao, match="outro aparelho"):
        qr.conferir_escolha(e, 4, agora=1100)
    with pytest.raises(ErroDeValidacao, match="venceu"):
        qr.conferir_escolha(e, 3, agora=1000 + qr.ESCOLHA_S + 1)
    trocada = "8" + e[1:]                                              # outra pessoa, mesma assinatura
    with pytest.raises(ErroDeValidacao, match="ilegível"):
        qr.conferir_escolha(trocada, 3, agora=1100)


def test_a_batida_pelo_nome_confere_sempre_o_rosto():
    from app.apps.ponto.core import marcacoes, rosto
    assert rosto._pelo_nome({"registrado_por": marcacoes.IDENTIFICADA_PELO_NOME + " — Maria"})
    assert not rosto._pelo_nome({"registrado_por": None})
    assert not rosto._pelo_nome({"registrado_por": "DP"})


# ---------------------------------------------------------------------------
# Com o banco
# ---------------------------------------------------------------------------
@pytest.fixture
def pessoas_extras(mundo, banco):
    """Na obra B: um admitido há 10 dias e um há 2 anos; e um sem obra nenhuma."""
    hoje = dt.date.today()
    cpfs = (CPF_NOVO, CPF_ANTIGO, CPF_SEM_OBRA)
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM colaboradores WHERE cpf = ANY(:c)"), {"c": list(cpfs)})
        ids = {}
        for nome, cpf, obra, admissao in (("Pedro Novato Lima", CPF_NOVO, mundo["obra_b"], hoje - dt.timedelta(days=10)),
                                          ("Paulo Antigo Lima", CPF_ANTIGO, mundo["obra_b"], hoje - dt.timedelta(days=700)),
                                          ("Plínio Avulso Lima", CPF_SEM_OBRA, None, hoje - dt.timedelta(days=700))):
            ids[nome.split()[0]] = conn.execute(text(
                "INSERT INTO colaboradores (nome, cpf, obra_id, admissao, telefone) "
                "VALUES (:n, :c, :o, :a, '85911110000') RETURNING id"),
                {"n": nome, "c": cpf, "o": obra, "a": admissao}).scalar_one()
        conn.commit()
    yield ids
    with banco.connect() as conn:
        _limpar_ponto(conn)
        conn.execute(text("DELETE FROM colaboradores WHERE cpf = ANY(:c)"), {"c": list(cpfs)})
        conn.commit()


@pytest.mark.banco
def test_busca_pelo_nome_so_com_o_responsavel_e_sem_cpf(app, mundo, pessoas_extras, monkeypatch, banco):
    _entrar_no_app(app, CPF_MARIA, monkeypatch)                        # a Maria cria o PIN 481927
    t, h, disp_id = _tablet(app, mundo)                                # responsável: a Maria
    busca = lambda nome, **kw: t.post("/ponto/app/api/obra/buscar-nome", json={"nome": nome, **kw}, headers=h)
    # Ninguém conectado: nada de busca, e a escolha de fora não identifica
    assert busca("joão").status_code == 401
    from app.apps.ponto.core import qr
    with app.test_request_context():
        forjada = qr.emitir_escolha(mundo["joao"], disp_id)
    r = t.post("/ponto/app/api/tablet/identificar", json={"escolha": forjada, "obra": "PG-A"}, headers=h)
    assert r.status_code == 403 and "responsável" in r.get_json()["erro"]

    assert t.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"}, headers=h).status_code == 200
    # Menos de 3 letras: não procura
    j = busca("jo").get_json()
    assert j["curta"] and j["pessoas"] == []
    # O João é da obra A: aparece com o nome curto, a função e a obra — sem o CPF
    r = busca("joão")
    j = r.get_json()
    assert [p["nome"] for p in j["pessoas"]] == ["João A"] and j["pessoas"][0]["escolha"]
    assert CPF_JOAO not in r.get_data(as_text=True)
    # "Lima": o novato (admitido há 10 dias) e o sem obra aparecem; o antigo da obra B, não
    nomes = {p["nome"] for p in busca("lima").get_json()["pessoas"]}
    assert nomes == {"Pedro Lima", "Plínio Lima"}
    # "Procurar em todas as obras" acha o antigo
    nomes = {p["nome"] for p in busca("lima", todas=True).get_json()["pessoas"]}
    assert nomes == {"Pedro Lima", "Paulo Lima", "Plínio Lima"}

    # A escolha identifica e bate; a batida fica marcada e vai para a conferência do rosto
    escolha = busca("joão").get_json()["pessoas"][0]["escolha"]
    r = t.post("/ponto/app/api/tablet/identificar", json={"escolha": escolha, "obra": "PG-A"}, headers=h)
    assert r.status_code == 200, r.get_json()
    ident = r.get_json()
    assert ident["nome"] == "João Obra A" and ident["identificacao"] == "NOME"
    b = t.post("/ponto/app/api/bater", json={"bilhete": ident["bilhete"], "obra": "PG-A", "foto_base64": _foto(),
                                             "latitude": OBRA_A[0], "longitude": OBRA_A[1]}, headers=h)
    assert b.status_code == 201, b.get_json()
    with banco.connect() as conn:
        m = conn.execute(text("SELECT identificacao, registrado_por FROM ponto.marcacoes "
                              "WHERE colaborador_id = :c"), {"c": mundo["joao"]}).one()
    from app.apps.ponto.core import marcacoes
    assert m.identificacao == "CPF"
    assert m.registrado_por.startswith(marcacoes.IDENTIFICADA_PELO_NOME) and "Maria" in m.registrado_por

    # Na consulta, a mesma escolha abre a folha (de quem é da obra do aparelho)
    r = t.post("/ponto/app/api/equipe/cpf", json={"escolha": escolha}, headers=h)
    assert r.status_code == 200 and r.get_json()["id"] == mundo["joao"]
    # A escolha de outro aparelho não serve aqui
    with app.test_request_context():
        alheia = qr.emitir_escolha(mundo["joao"], disp_id + 99)
    r = t.post("/ponto/app/api/tablet/identificar", json={"escolha": alheia, "obra": "PG-A"}, headers=h)
    assert r.status_code == 400 and "outro aparelho" in r.get_json()["erro"]


@pytest.mark.banco
def test_busca_pelo_nome_nao_vale_no_celular_pessoal(app, mundo, monkeypatch):
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    r = c.post("/ponto/app/api/obra/buscar-nome", json={"nome": "maria"})
    assert r.status_code in (401, 403)
