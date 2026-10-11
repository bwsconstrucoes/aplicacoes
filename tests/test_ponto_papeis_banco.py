# -*- coding: utf-8 -*-
"""Ponto — a tela inicial e quem enxerga quem (pedido do dono, 08/10/2026), com
Postgres de verdade: o administrativo de obra consulta e pede pelas pessoas da
obra em que ESTÁ (pela cerca) ou em que bateu ponto nos últimos dias (o computador); o
ponto da obra aceita o responsável e o administrativo; o ponto de equipe vê a
equipe; e pedido pelo próprio celular só com a marcação."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _entrar_no_app,  # noqa: F401
                                           _png, _schema_ponto2, app, como, mundo)
from tests.test_ponto_regras_0610_banco import _aparelho, _id_do_aparelho

pytestmark = pytest.mark.banco

CPF_CARLOS = "39053344705"
NA_OBRA_A = f"lat={OBRA_A[0]}&lon={OBRA_A[1]}&precisao=10"
LONGE = "lat=-3.80&lon=-38.40&precisao=10"


def _limpar_ponto(conn):
    tabelas = [r[0] for r in conn.execute(text(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'ponto' "
        "AND tablename NOT IN ('_migracoes', 'feriados', 'tipos_licenca')"))]
    conn.execute(text("TRUNCATE " + ", ".join(f'ponto."{t}"' for t in tabelas) + " RESTART IDENTITY CASCADE"))


@pytest.fixture
def carlos(mundo, banco):
    """O administrativo, cadastrado na obra B."""
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM colaboradores WHERE cpf = :c"), {"c": CPF_CARLOS})
        cid = conn.execute(text("INSERT INTO colaboradores (nome, cpf, obra_id, telefone) "
                                "VALUES ('Carlos Administrativo', :c, :o, '85977770000') RETURNING id"),
                           {"c": CPF_CARLOS, "o": mundo["obra_b"]}).scalar_one()
        conn.commit()
    yield cid
    with banco.connect() as conn:
        _limpar_ponto(conn)
        conn.execute(text("DELETE FROM colaboradores WHERE cpf = :c"), {"c": CPF_CARLOS})
        conn.commit()


def _bater(cpf, obra, minutos_atras):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import marcacoes
    with db.conexao() as conn:
        marcacoes.registrar(conn, cpf=cpf, obra=obra, origem="IDFACE", via_chave=True,
                            agora=horario.agora() - dt.timedelta(minutes=minutos_atras))


def _papeis(app, mundo, colaborador_id, **marcas):
    r = como(app, mundo["dp"]).post(f"/erp/api/ponto/pessoas/{colaborador_id}/forma-de-bater", json=marcas)
    assert r.status_code == 200, r.get_json()
    return r.get_json()


def test_administrativo_ve_a_obra_em_que_esta(app, mundo, carlos, monkeypatch):
    j = _papeis(app, mundo, carlos, administrativo_obra=True)
    assert j["administrativo_obra"] is True and j["bate_no_celular"] is False
    exc = como(app, mundo["dp"]).get("/erp/api/ponto/excecoes").get_json()
    assert [p["id"] for p in exc["administrativos"]] == [carlos]
    _bater(CPF_MARIA, "PG-A", 30)                      # a Maria é da obra B, mas bateu na A este mês
    c = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    # Dentro da cerca da obra A: o João (cadastrado nela) e a Maria (bateu nela)
    d = c.get(f"/ponto/app/api/equipe?{NA_OBRA_A}").get_json()
    nomes = {p["nome"]: p for p in d["pessoas"]}
    assert set(nomes) == {"João Obra A", "Maria Obra B"} and nomes["Maria Obra B"]["batidas_na_obra"] == 1
    assert d["alcance"]["papel"] == "ADMINISTRATIVO" and "PG-A" in d["alcance"]["de_onde"][0]
    assert c.get(f"/ponto/app/api/equipe/{mundo['maria']}/mes?{NA_OBRA_A}").status_code == 200
    # Longe de qualquer obra, e sem batida dele em obra nenhuma: ninguém — e a Maria é "não encontrada"
    d = c.get(f"/ponto/app/api/equipe?{LONGE}").get_json()
    assert d["pessoas"] == [] and "não está dentro" in d["alcance"]["sem_alcance"]
    r = c.get(f"/ponto/app/api/equipe/{mundo['maria']}/mes?{LONGE}")
    assert r.status_code == 404
    # No computador (sem localização) e a qualquer hora: as obras em que ele bateu nos últimos dias
    _bater(CPF_CARLOS, "PG-A", 20)
    _bater(CPF_CARLOS, "PG-A", 1)                       # já bateu a saída: continua valendo
    d = c.get("/ponto/app/api/equipe").get_json()
    assert {p["nome"] for p in d["pessoas"]} >= {"João Obra A", "Maria Obra B"}
    assert "últimos 7 dias" in d["alcance"]["de_onde"][0]
    # Batida de mais de 7 dias atrás não conta: a obra antiga sai sozinha
    from app.apps.ponto import db
    with db.conexao() as conn:
        conn.execute(text("UPDATE ponto.marcacoes SET data_referencia = data_referencia - 8 "
                          "WHERE colaborador_id = :c"), {"c": carlos})
    assert c.get("/ponto/app/api/equipe").get_json()["pessoas"] == []
    # E a tela inicial diz o que ele pode
    ini = c.get(f"/ponto/app/api/inicio?{NA_OBRA_A}").get_json()
    assert ini["pode"]["consultar"] and ini["pode"]["pedir_por_outros"] and ini["pode"]["pedir_o_meu"]
    assert ini["pessoa"]["administrativo"] is True


def test_pedido_por_outra_pessoa_sai_em_nome_do_administrativo(app, mundo, carlos, monkeypatch, banco):
    _papeis(app, mundo, carlos, administrativo_obra=True)
    c = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    hoje = dt.date.today().isoformat()
    corpo = {"tipo": "ATESTADO", "data_inicio": hoje, "data_fim": hoje, "documento_base64": _png(),
             "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10}
    r = c.post(f"/ponto/app/api/equipe/{mundo['joao']}/pedidos", json=corpo)
    assert r.status_code == 201, r.get_json()
    with banco.connect() as conn:
        o = conn.execute(text("SELECT colaborador_id, origem, solicitado_por FROM ponto.ocorrencias WHERE id = :i"),
                         {"i": r.get_json()["pedido"]["id"]}).one()
    assert o.colaborador_id == mundo["joao"] and o.origem == "RESPONSAVEL" and "Carlos" in o.solicitado_por
    # Longe da obra: o João não está ao alcance — "não encontrado", não "sem permissão"
    longe = {**corpo, "latitude": -3.80, "longitude": -38.40}
    assert c.post(f"/ponto/app/api/equipe/{mundo['joao']}/pedidos", json=longe).status_code == 404
    # O próprio pedido é pelo Meu ponto
    assert c.post(f"/ponto/app/api/equipe/{carlos}/pedidos", json=corpo).status_code == 400
    # O ajuste de um dia que faltou, na obra em que ele está
    from app.apps.ponto import horario
    dia = horario.hoje() - dt.timedelta(days=1)
    while dia.weekday() > 3:
        dia -= dt.timedelta(days=1)
    if dia.month != horario.hoje().month:
        pytest.skip("sem dia útil anterior neste mês para o ajuste")
    r = c.post(f"/ponto/app/api/equipe/{mundo['joao']}/ajuste-do-dia",
               json={"horarios": [f"{dia.isoformat()}T07:00:00"], "motivo": "ESQUECI",
                     "descricao": "esqueceu de bater a entrada", "obra": "PG-A",
                     "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10})
    assert r.status_code == 201, r.get_json()


def test_pedido_pelo_proprio_celular_so_com_a_marcacao(app, mundo, monkeypatch):
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    assert c.get("/ponto/app/api/eu").get_json()["pede_no_celular"] is False
    hoje = dt.date.today().isoformat()
    pedido = {"tipo": "ATESTADO", "data_inicio": hoje, "data_fim": hoje, "documento_base64": _png()}
    r = c.post("/ponto/app/api/pedidos", json=pedido)
    assert r.status_code == 400 and "administrativo" in r.get_json()["erro"]
    # Bater no celular (a exceção de 05/10) não dá mais, sozinho, o pedido pelo celular
    _papeis(app, mundo, mundo["joao"], bate_no_celular=True)
    assert c.post("/ponto/app/api/pedidos", json=pedido).status_code == 400
    _papeis(app, mundo, mundo["joao"], pede_no_celular=True)
    assert c.get("/ponto/app/api/eu").get_json()["pede_no_celular"] is True
    assert c.post("/ponto/app/api/pedidos", json=pedido).status_code == 201
    # E sem ser administrativo, não consulta ninguém
    ini = c.get(f"/ponto/app/api/inicio?{NA_OBRA_A}").get_json()
    assert ini["pode"]["consultar"] is False and ini["pode"]["bater_o_meu"] is True
    assert c.get(f"/ponto/app/api/equipe/{mundo['maria']}/mes?{NA_OBRA_A}").status_code == 404


def test_ponto_da_obra_consulta_pelo_cpf_sem_pin(app, mundo, carlos, monkeypatch):
    """Decisões do dono, 10/10/2026: no ponto da obra todo mundo entra com CPF e
    PIN (só o responsável e o administrativo entram nele); dentro, a consulta é
    pelo CPF da pessoa — sem outro PIN e sem depender da localização — e só de
    quem tem relação com as obras do aparelho."""
    cel = {cpf: _entrar_no_app(app, cpf, monkeypatch) for cpf in (CPF_JOAO, CPF_MARIA, CPF_CARLOS)}  # PIN 481927
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "ponto-da-obra-a-com-tela-inicial-01")
    ap = _id_do_aparelho(dp, "ponto-da-obra-a-com-tela-inicial-01")
    d5 = (dt.date.today() + dt.timedelta(days=5)).isoformat()
    proprio = {"tipo": "ATESTADO", "data_inicio": d5, "data_fim": d5, "documento_base64": _png()}
    assert cel[CPF_MARIA].post("/ponto/app/api/pedidos", json=proprio).status_code == 400
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    # Responsável por um ponto da obra pede o PRÓPRIO no "Meu ponto" (10/10/2026: "ela
    # consegue pelo outro modo, é para conseguir aqui também"); quem não é, não.
    assert cel[CPF_MARIA].get("/ponto/app/api/eu").get_json()["pede_no_celular"] is True
    assert cel[CPF_MARIA].post("/ponto/app/api/pedidos", json=proprio).status_code == 201
    assert cel[CPF_JOAO].post("/ponto/app/api/pedidos", json=proprio).status_code == 400
    # Sem ninguém entrar, nada de consulta
    assert t.post("/ponto/app/api/equipe/cpf", json={"cpf": CPF_JOAO}, headers=h).status_code == 401
    assert t.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=h).status_code == 403
    assert t.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"}, headers=h).status_code == 200
    ini = t.get("/ponto/app/api/inicio", headers=h).get_json()          # sem localização
    assert ini["da_obra"] and ini["pessoa"]["primeiro_nome"] == "Maria" and ini["alcance"]["papel"] == "APARELHO_OBRA"
    assert ini["pode"]["bater_aqui"] and ini["pode"]["consultar"] and ini["pode"]["qr_por_cpf"]
    assert ini["pessoa"]["papel_aqui"] == "responsável por este aparelho"     # o papel dela, não o do aparelho
    # Lista de nomes, não: é pelo CPF. E o número da pessoa sozinho não abre nada.
    assert t.get("/ponto/app/api/equipe", headers=h).status_code == 403
    assert t.get(f"/ponto/app/api/equipe/{mundo['joao']}/mes", headers=h).status_code == 404
    # A Maria é da obra B e não bateu na A: "não encontrada"; o João é da A
    assert t.post("/ponto/app/api/equipe/cpf", json={"cpf": CPF_MARIA}, headers=h).status_code == 404
    r = t.post("/ponto/app/api/equipe/cpf", json={"cpf": CPF_JOAO}, headers=h)
    assert r.status_code == 200 and r.get_json()["id"] == mundo["joao"]
    m = t.get(f"/ponto/app/api/equipe/{mundo['joao']}/mes", headers=h).get_json()
    assert m["pelo_aparelho"] and m["pode_pedir"] and m["pessoa"]["nome"] == "João Obra A"
    hoje = dt.date.today().isoformat()
    r = t.post(f"/ponto/app/api/equipe/{mundo['joao']}/pedidos", headers=h,
               json={"tipo": "ATESTADO", "data_inicio": hoje, "data_fim": hoje, "documento_base64": _png()})
    assert r.status_code == 201, r.get_json()
    # O PIN de quem entrou continua conferível (a tela não o pede mais para sair da batida)
    assert t.post("/ponto/app/api/confirmar-pin", json={"pin": "000000"}, headers=h).status_code == 401
    assert t.post("/ponto/app/api/confirmar-pin", json={"pin": "481927"}, headers=h).status_code == 200
    # O administrativo também entra; as opções são as do aparelho
    t.post("/ponto/app/api/sair", json={}, headers=h)
    _papeis(app, mundo, carlos, administrativo_obra=True)
    assert t.post("/ponto/app/api/entrar", json={"cpf": CPF_CARLOS, "pin": "481927"}, headers=h).status_code == 200
    ini = t.get("/ponto/app/api/inicio", headers=h).get_json()
    assert ini["alcance"]["papel"] == "APARELHO_OBRA" and ini["pode"]["consultar"]
    assert ini["pessoa"]["papel_aqui"] == "administrativo da obra"


def test_ponto_de_equipe_consulta_a_equipe_e_pede_so_com_a_marcacao(app, mundo, monkeypatch):
    for cpf in (CPF_JOAO, CPF_MARIA):
        _entrar_no_app(app, cpf, monkeypatch)
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "ponto-de-equipe-da-maria-0123456789")
    ap = _id_do_aparelho(dp, "ponto-de-equipe-da-maria-0123456789")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "LISTA", "autorizados": [CPF_JOAO], "cpf": CPF_MARIA}).status_code == 200
    assert t.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"}, headers=h).status_code == 200
    r = t.post("/ponto/app/api/equipe/cpf", json={"cpf": CPF_JOAO}, headers=h)   # a equipe vale sem localização
    assert r.status_code == 200
    m = t.get(f"/ponto/app/api/equipe/{mundo['joao']}/mes", headers=h).get_json()
    assert m["pessoa"]["nome"] == "João Obra A" and m["pode_pedir"] is False
    hoje = dt.date.today().isoformat()
    pedido = {"tipo": "ATESTADO", "data_inicio": hoje, "data_fim": hoje, "documento_base64": _png()}
    r = t.post(f"/ponto/app/api/equipe/{mundo['joao']}/pedidos", json=pedido, headers=h)
    assert r.status_code == 403 and "não faz pedido" in r.get_json()["erro"]
    _papeis(app, mundo, mundo["maria"], pede_no_celular=True)            # a responsável pelo aparelho
    assert t.post(f"/ponto/app/api/equipe/{mundo['joao']}/pedidos", json=pedido, headers=h).status_code == 201


def test_pedidos_pelo_celular_marcados_no_aparelho_refletem_na_pessoa(app, mundo, monkeypatch):
    """10/10/2026: "facilitaria se ficasse no cadastro do telefone, ou também, e uma
    coisa refletisse na outra" — a tela do aparelho grava a marcação da PESSOA."""
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "celular-do-joao-pedidos-0123456789")
    ap = _id_do_aparelho(dp, "celular-do-joao-pedidos-0123456789")
    aprovar = {"perfil": "INDIVIDUAL", "cpf": CPF_JOAO}
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar", json=aprovar).status_code == 200
    assert dp.get(f"/erp/api/ponto/dispositivos/{ap}").get_json()["dispositivo"]["dono_pede_no_celular"] is False
    # sem o campo, a marcação fica como está; com ele, vai para o cadastro da pessoa
    r = dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar", json={**aprovar, "pede_no_celular": True})
    assert r.status_code == 200, r.get_json()
    assert dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]["pede_no_celular"] is True
    assert cel.get("/ponto/app/api/eu").get_json()["pede_no_celular"] is True
    # e o caminho de volta: desmarcado em Pessoas, o aparelho mostra desmarcado
    assert dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/forma-de-bater",
                   json={"pede_no_celular": False}).status_code == 200
    assert dp.get(f"/erp/api/ponto/dispositivos/{ap}").get_json()["dispositivo"]["dono_pede_no_celular"] is False
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar", json=aprovar).status_code == 200
    assert dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]["pede_no_celular"] is False
