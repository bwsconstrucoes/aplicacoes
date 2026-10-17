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


def test_ponto_da_obra_com_alguem_logado_grava_a_batida_de_quem_se_identificou(app, mundo, monkeypatch):
    """13/10/2026: "bati o ponto de outra pessoa no ponto da obra, diz que bateu, mas
    não aparece no espelho" — a sessão de quem entrou passava na frente do bilhete."""
    for cpf in (CPF_JOAO, CPF_MARIA):
        _entrar_no_app(app, cpf, monkeypatch)
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "ponto-da-obra-com-responsavel-logado")
    ap = _id_do_aparelho(dp, "ponto-da-obra-com-responsavel-logado")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    assert t.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"}, headers=h).status_code == 200
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    r = t.post("/ponto/app/api/bater", headers=h, json={
        "bilhete": ident["bilhete"], "obra": "PG-A", "foto_base64": _png(),
        "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10})
    assert r.status_code == 201, r.get_json()
    assert r.get_json()["comprovante"]["empregado"].startswith("João")
    joao = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}").get_json()
    maria = dp.get(f"/erp/api/ponto/espelho/{mundo['maria']}").get_json()
    assert sum(len(d["batidas"]) for d in joao["dias"]) == 1
    assert sum(len(d["batidas"]) for d in maria["dias"]) == 0
    # sem identificação, a sessão de quem entrou não bate no ponto da obra
    sem = t.post("/ponto/app/api/bater", headers=h, json={"obra": "PG-A", "latitude": OBRA_A[0],
                                                         "longitude": OBRA_A[1]})
    assert sem.status_code == 403 and "se identifica" in sem.get_json()["erro"]


def test_mesmo_celular_em_outro_navegador_leva_a_liberacao(app, mundo, monkeypatch):
    """13/10/2026: "quando abre em novo navegador ou aba anônima, ele entende que é
    um novo celular" — quem já tem aparelho liberado diz "é este", e a liberação vem."""
    _entrar_no_app(app, CPF_JOAO, monkeypatch)
    _entrar_no_app(app, CPF_MARIA, monkeypatch)
    dp = como(app, mundo["dp"])
    velho, hv = _aparelho(app, "navegador-antigo-do-joao-0123456")
    ap = _id_do_aparelho(dp, "navegador-antigo-do-joao-0123456")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO, "valido_ate": "NAO_EXPIRA"}).status_code == 200
    assert dp.get(f"/erp/api/ponto/dispositivos/{ap}").get_json()["dispositivo"]["valido_ate"] is None
    assert velho.get("/ponto/app/api/aparelho", headers=hv).get_json()["aparelho"]["status"] == "APROVADO"
    # navegador novo, sem nada guardado: ao entrar, a resposta oferece o aparelho
    novo = app.test_client()
    hn = {"X-Device-UUID": "navegador-novo-do-joao-0123456789"}
    r = novo.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=hn).get_json()
    assert [a["id"] for a in r["aparelhos_seus"]] == [ap]
    # a Maria não leva o aparelho do João
    outra = app.test_client()
    outra.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"},
               headers={"X-Device-UUID": "navegador-da-maria-0123456789ab"})
    assert outra.post("/ponto/app/api/aparelho/usar-este", json={"dispositivo_id": ap},
                      headers={"X-Device-UUID": "navegador-da-maria-0123456789ab"}).status_code == 404
    # o João leva: o novo vale, o velho para na hora
    t = novo.post("/ponto/app/api/aparelho/usar-este", json={"dispositivo_id": ap}, headers=hn)
    assert t.status_code == 200, t.get_json()
    hn["X-Device-Token"] = t.get_json()["token"]
    a = novo.get("/ponto/app/api/aparelho", headers=hn).get_json()["aparelho"]
    assert a["status"] == "APROVADO" and a["perfil"] == "INDIVIDUAL" and a["vencimento"] is None
    assert velho.get("/ponto/app/api/aparelho", headers=hv).get_json()["aparelho"] is None
    # já liberado aqui: a entrada não oferece de novo
    r2 = novo.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=hn).get_json()
    assert r2["aparelhos_seus"] == []


def test_identificar_avisa_batida_de_ha_pouco(app, mundo, monkeypatch):
    """A batida repetida (13/10/2026): o ponto da obra pergunta "bater de novo?"."""
    _entrar_no_app(app, CPF_MARIA, monkeypatch)
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "ponto-da-obra-batida-repetida-0123")
    ap = _id_do_aparelho(dp, "ponto-da-obra-batida-repetida-0123")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    assert ident["batida_recente"] is None
    assert t.post("/ponto/app/api/bater", headers=h, json={"bilhete": ident["bilhete"], "obra": "PG-A",
                  "foto_base64": _png(), "latitude": OBRA_A[0], "longitude": OBRA_A[1]}).status_code == 201
    de_novo = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    assert de_novo["batida_recente"] and de_novo["batida_recente"]["ha_minutos"] == 0


def test_celular_de_quem_nao_tem_obra_no_cadastro_acha_a_obra_em_que_esta(app, mundo, banco, monkeypatch):
    """13/10/2026: "aparece 'nenhuma obra sua tem localização cadastrada, escolha a
    obra abaixo', não aparece a lista — e outro celular no mesmo lugar bate normal"."""
    from sqlalchemy import text
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET obra_id = NULL WHERE id = :j"), {"j": mundo["joao"]})
        conn.commit()
    _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)          # o PIN, e a exceção de bater no celular
    dp = como(app, mundo["dp"])
    cel, h = _aparelho(app, "celular-do-joao-sem-obra-0123456789")
    ap = _id_do_aparelho(dp, "celular-do-joao-sem-obra-0123456789")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO}).status_code == 200
    assert cel.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=h).status_code == 200
    assert cel.get("/ponto/app/api/eu", headers=h).get_json()["obras"] == []
    aqui = cel.get(f"/ponto/app/api/obra-aqui?{NA_OBRA_A}", headers=h).get_json()
    assert aqui["obra"]["codigo"] == "PG-A"
    assert cel.get("/ponto/app/api/obra-aqui?lat=-3.90&lon=-38.40&precisao=10", headers=h).get_json()["obra"] is None
    r = cel.post("/ponto/app/api/bater", headers=h, json={"obra": "PG-A", "latitude": OBRA_A[0],
                                                          "longitude": OBRA_A[1], "precisao": 10, "foto_base64": _png()})
    assert r.status_code == 201, r.get_json()
    assert "fora da lista da pessoa" not in (r.get_json().get("motivo_analise") or "")   # a obra do cadastro não conta
    assert len(cel.get("/ponto/app/api/eu", headers=h).get_json()["obras_para_escolher"]) >= 2   # sem obra: todas as ativas


def test_aparelho_sem_batida_se_exclui_e_com_batida_se_arquiva(app, mundo, monkeypatch):
    """13/10/2026: "não tem como excluir aparelhos daqui? fica poluído com o tempo"."""
    for cpf in (CPF_JOAO, CPF_MARIA):
        _entrar_no_app(app, cpf, monkeypatch)
    dp = como(app, mundo["dp"])
    _aparelho(app, "aba-anonima-lixo-0123456789abcd")
    lixo = _id_do_aparelho(dp, "aba-anonima-lixo-0123456789abcd")
    r = dp.post(f"/erp/api/ponto/dispositivos/{lixo}/excluir", json={})
    assert r.status_code == 200 and r.get_json()["feito"] == "EXCLUIDO"
    assert all(a["id"] != lixo for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"])
    # o ponto da obra que já registrou batida: arquivado, bloqueado, guardado
    t, h = _aparelho(app, "ponto-da-obra-com-batida-0123456")
    ap = _id_do_aparelho(dp, "ponto-da-obra-com-batida-0123456")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    assert t.post("/ponto/app/api/bater", headers=h, json={"bilhete": ident["bilhete"], "obra": "PG-A",
                  "foto_base64": _png(), "latitude": OBRA_A[0], "longitude": OBRA_A[1]}).status_code == 201
    a = next(x for x in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"] if x["id"] == ap)
    assert a["batidas"] == 1 and a["arquivado"] is False
    r = dp.post(f"/erp/api/ponto/dispositivos/{ap}/excluir", json={})
    assert r.get_json()["feito"] == "ARQUIVADO"
    a = next(x for x in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"] if x["id"] == ap)
    assert a["arquivado"] is True and a["status"] == "BLOQUEADO"
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/desarquivar", json={}).status_code == 200
    a = next(x for x in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"] if x["id"] == ap)
    assert a["arquivado"] is False and a["status"] == "BLOQUEADO"


def test_afastado_nao_bate_mas_entrega_pedido(app, mundo, banco, monkeypatch):
    """14/10/2026: "o cara está afastado, ele não bate ponto (…) se retornou, a gente
    tem que alterar o cadastro dele"."""
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import marcacoes
    from app.apps.ponto.erros import Recusada
    _entrar_no_app(app, CPF_MARIA, monkeypatch)
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET situacao = 'AFASTADO' WHERE id = :j"), {"j": mundo["joao"]})
        conn.commit()
    with pytest.raises(Recusada) as e, db.conexao() as conn:
        marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True)
    assert "afastad" in str(e.value)
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "ponto-da-obra-afastado-0123456789")
    ap = _id_do_aparelho(dp, "ponto-da-obra-afastado-0123456789")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    r = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h)
    assert r.status_code == 403 and "afastad" in r.get_json()["erro"]
    # o atestado ele entrega
    assert t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "para_pedido": True},
                  headers=h).status_code == 200
    # voltou: o cadastro muda, e ele bate
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET situacao = 'ATIVO' WHERE id = :j"), {"j": mundo["joao"]})
        conn.commit()
    with db.conexao() as conn:
        m, _ = marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True)
    assert m["id"]


def test_onde_o_celular_estava_na_batida_e_na_recusa(app, mundo, banco, monkeypatch):
    """14/10/2026: "a geolocalização é coletada? a gente pode checar onde a pessoa
    estava no momento?" — no espelho (aceita) e na recusa (agora também)."""
    from sqlalchemy import text
    _entrar_no_app(app, CPF_MARIA, monkeypatch)
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "ponto-da-obra-mapa-0123456789abc")
    ap = _id_do_aparelho(dp, "ponto-da-obra-mapa-0123456789abc")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    assert t.post("/ponto/app/api/bater", headers=h, json={"bilhete": ident["bilhete"], "obra": "PG-A", "foto_base64": _png(),
                  "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 12}).status_code == 201
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}").get_json()
    b = next(x for d in esp["dias"] for x in d["batidas"])
    assert abs(b["latitude"] - OBRA_A[0]) < 1e-6 and abs(b["longitude"] - OBRA_A[1]) < 1e-6
    # longe da obra: recusada — e o lugar fica guardado
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    longe = t.post("/ponto/app/api/bater", headers=h, json={"bilhete": ident["bilhete"], "obra": "PG-A",
                   "foto_base64": _png(), "latitude": -3.80, "longitude": -38.40, "precisao": 20})
    assert longe.status_code == 403
    with banco.connect() as conn:
        det = conn.execute(text("SELECT detalhes FROM ponto.recusas ORDER BY id DESC LIMIT 1")).scalar()
    assert det["latitude"] == "-3.8" and det["longitude"] == "-38.4"


def test_pedir_a_aprovacao_do_aparelho_so_depois_de_entrar(app, mundo, monkeypatch):
    """14/10/2026: "antes de tudo precisa ser feito o cadastro do telefone e gerado
    um PIN (…) depois desse momento é que eu poderia pedir para que o telefone fosse
    aprovado". O pedido leva o nome de quem pediu e entra na fila de Validações —
    mesmo de quem não está liberado para bater no celular (pode ser o tablet da
    obra); o tipo é de quem aprova."""
    from tests.test_ponto_gestao_banco import CPF_JOAO, CPF_MARIA, _entrar_no_app, como
    from tests.test_ponto_regras_0610_banco import _id_do_aparelho
    uuid = "pedido-depois-do-pin-0123456789"
    anonimo = app.test_client()
    token = anonimo.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    assert anonimo.post("/ponto/app/api/aparelho/pedir", json={}, headers=h).status_code in (401, 403)
    dp = como(app, mundo["dp"])
    def na_fila():
        itens = dp.get("/erp/api/ponto/validacoes").get_json()["itens"]
        return [i for i in itens if i["tipo"] == "APARELHO" and i["id"] == _id_do_aparelho(dp, uuid)]
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)        # João: só consulta (não liberado)
    r = cel.post("/ponto/app/api/aparelho/pedir", json={}, headers=h)
    assert r.status_code == 200 and r.get_json()["pedido"] is True
    ap = dp.get(f"/erp/api/ponto/dispositivos/{_id_do_aparelho(dp, uuid)}").get_json()["dispositivo"]
    assert ap["descricao"].startswith("Pedido por ") and ap["dono"]["nome"].startswith("João")
    assert len(na_fila()) == 1
    # outra pessoa que entra nele antes da aprovação não troca quem pediu
    outra = app.test_client()
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso
    env = []
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, CPF_MARIA, enviar=lambda t, m: env.append(m))
    outra.post("/ponto/app/api/pin", json={"cpf": CPF_MARIA, "codigo": env[-1].split("código é ")[1][:6], "pin": "481927"},
               headers=h)
    outra.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"}, headers=h)
    ap = dp.get(f"/erp/api/ponto/dispositivos/{_id_do_aparelho(dp, uuid)}").get_json()["dispositivo"]
    assert ap["dono"]["nome"].startswith("João")
    # aprovado como ponto da obra: o pedido já não muda nada
    assert dp.post(f"/erp/api/ponto/dispositivos/{_id_do_aparelho(dp, uuid)}/aprovar",
                   json={"perfil": "COMPARTILHADO", "cpf": CPF_JOAO, "obras": [mundo["obra_a"]]}).status_code == 200
    assert cel.post("/ponto/app/api/aparelho/pedir", json={}, headers=h).get_json()["pedido"] is False
    assert na_fila() == []


def test_pedido_de_aparelho_mais_novo_em_cima_com_a_hora(app, mundo, monkeypatch):
    """16/10/2026: "a que informou depois chegou antes da pessoa que informou
    primeiro" — a lista estava em ordem de nome. Agora o pedido traz a hora, e o
    navegador antigo que pede de novo sobe para cima."""
    from tests.test_ponto_gestao_banco import CPF_JOAO, CPF_MARIA, _entrar_no_app, como
    from tests.test_ponto_regras_0610_banco import _id_do_aparelho
    from app.apps.ponto import db
    def registrar(uuid):
        c = app.test_client()
        t = c.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
        return {"X-Device-UUID": uuid, "X-Device-Token": t}
    h_antigo = registrar("navegador-antigo-do-joao-0123456789")
    h_novo = registrar("navegador-novo-da-maria-0123456789")
    dp = como(app, mundo["dp"])
    with db.conexao() as conn:                      # o do João foi registrado há dias
        db.executar(conn, "UPDATE ponto.dispositivos SET criado_em = now() - interval '3 days' WHERE id = :i",
                    i=_id_do_aparelho(dp, "navegador-antigo-do-joao-0123456789"))
    maria = _entrar_no_app(app, CPF_MARIA, monkeypatch)
    assert maria.post("/ponto/app/api/aparelho/pedir", json={}, headers=h_novo).status_code == 200
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)            # pede DEPOIS, no navegador antigo
    assert joao.post("/ponto/app/api/aparelho/pedir", json={}, headers=h_antigo).status_code == 200
    lista = [a for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"] if a["status"] == "PENDENTE"]
    assert all(a["pedido_em"] for a in lista)
    itens = [i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["tipo"] == "APARELHO"]
    nomes = [i["pessoa"] or "" for i in itens]
    assert nomes.index(next(n for n in nomes if n.startswith("João"))) < nomes.index(next(n for n in nomes if n.startswith("Maria")))


def test_codigo_nao_chegou_diagnostico_e_codigo_em_maos(app, mundo, banco):
    """16/10/2026: "a pessoa não recebe o código (…) e não chega nada no aparelhos.
    Consultei o telefone no cadastro, tá normal. E aí eu não sei o que fazer." A
    ficha diz o motivo; o DP gera o código para entregar em mãos."""
    from sqlalchemy import text
    from tests.test_ponto_gestao_banco import CPF_JOAO, como
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET telefone = '85988887777' WHERE id = :i"), {"i": mundo["joao"]})
        conn.commit()
    d = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/acesso").get_json()["acesso"]
    assert d["telefone"] == "(85) 98888-7777" and d["impede"] == [] and d["codigos"] == [] and d["tem_pin"] is False
    # o WhatsApp recusou: a ficha mostra
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda t, m: None)
    with banco.connect() as conn:
        tem = conn.execute(text("SELECT 1 FROM information_schema.tables WHERE table_schema='mensageria' "
                                "AND table_name='envios'")).scalar()
        if tem:
            conn.execute(text("INSERT INTO mensageria.envios (tipo, canal, destinatario, status, detalhe) VALUES "
                              "('ponto.codigo_acesso', 'whatsapp', '5585988887777', 'FALHOU', 'número sem WhatsApp')"))
            conn.commit()
    d = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/acesso").get_json()["acesso"]
    assert len(d["codigos"]) == 1 and d["codigos"][0]["usado"] is False
    if tem:
        assert d["codigos"][0]["envio"]["status"] == "FALHOU" and "sem WhatsApp" in d["codigos"][0]["envio"]["detalhe"]
    # o código em mãos — e um do WhatsApp pedido DEPOIS não o derruba
    assert sup.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/codigo-em-maos", json={}).status_code in (403, 404)
    g = dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/codigo-em-maos", json={}).get_json()
    assert len(g["codigo"]) == 6 and g["validade_min"] == 15
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda t, m: None)
    errado = app.test_client().post("/ponto/app/api/pin", json={"cpf": CPF_JOAO, "codigo": "000000", "pin": "481927"})
    assert errado.status_code == 401
    certo = app.test_client().post("/ponto/app/api/pin", json={"cpf": CPF_JOAO, "codigo": g["codigo"], "pin": "481927"})
    assert certo.status_code == 200, certo.get_json()
    assert dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/acesso").get_json()["acesso"]["tem_pin"] is True
    # celular sem DDD: a ficha diz que o código não sai
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET telefone = '98887777' WHERE id = :i"), {"i": mundo["joao"]})
        conn.commit()
    d = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/acesso").get_json()["acesso"]
    assert any("DDD" in x for x in d["impede"])


def test_pediu_dois_codigos_e_digitou_o_primeiro(app, mundo):
    """16/10/2026, "código inválido ou vencido": o WhatsApp demorou, a pessoa pediu
    de novo e digitou o PRIMEIRO que chegou — só o último valia. Agora vale
    qualquer um ainda no prazo, e cada um uma vez só."""
    from tests.test_ponto_gestao_banco import CPF_JOAO
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso
    msgs = []
    for _ in range(2):
        with db.conexao() as conn:
            acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda t, m: msgs.append(m))
    primeiro, segundo = (m.split("código é ")[1][:6] for m in msgs)
    c = app.test_client()
    assert c.post("/ponto/app/api/pin", json={"cpf": CPF_JOAO, "codigo": primeiro, "pin": "481927"}).status_code == 200
    # usado: não vale de novo
    assert c.post("/ponto/app/api/pin", json={"cpf": CPF_JOAO, "codigo": primeiro, "pin": "481927"}).status_code == 401
    assert c.post("/ponto/app/api/pin", json={"cpf": CPF_JOAO, "codigo": segundo, "pin": "135790"}).status_code == 200


def test_aparelho_registrado_sem_pedido_ganha_o_botao_de_pedir(app, mundo, monkeypatch):
    """16/10/2026: "ela coloca o código e não funciona, não tem aparelho liberado
    ainda". O navegador registrado sem o pedido, de quem não é exceção, ficava
    PENDENTE fora da fila de Validações — e o botão de pedir sumia porque já
    havia aparelho. A tela agora sabe que ele não foi pedido."""
    from tests.test_ponto_gestao_banco import CPF_JOAO, _entrar_no_app, como
    from tests.test_ponto_regras_0610_banco import _id_do_aparelho
    uuid = "registrado-sem-pedido-0123456789"
    c = app.test_client()
    token = c.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    dp = como(app, mundo["dp"])
    def na_fila():
        itens = dp.get("/erp/api/ponto/validacoes").get_json()["itens"]
        return [i for i in itens if i["tipo"] == "APARELHO" and i["id"] == _id_do_aparelho(dp, uuid)]
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    cel.post("/ponto/app/api/aparelho/identificar", json={}, headers=h)
    assert cel.get("/ponto/app/api/aparelho", headers=h).get_json()["aparelho"]["pedido"] is False
    assert na_fila() == []
    assert cel.post("/ponto/app/api/aparelho/pedir", json={}, headers=h).get_json()["pedido"] is True
    assert cel.get("/ponto/app/api/aparelho", headers=h).get_json()["aparelho"]["pedido"] is True
    assert len(na_fila()) == 1


def test_codigo_certo_de_quem_nao_bate_ponto_diz_o_motivo_e_nao_gasta(app, mundo, banco):
    """16/10/2026: o código era aceito e o PIN criado, mas a entrada recusava quem
    está marcado "não bate ponto" — a tela voltava para "entrar" e a pessoa achava
    que o código não funcionava. Agora ela lê o motivo, e a ficha também diz."""
    from sqlalchemy import text
    from tests.test_ponto_gestao_banco import CPF_JOAO, como
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso
    with banco.connect() as conn:
        conn.execute(text("INSERT INTO ponto.colaborador_config (colaborador_id, ativo) VALUES (:c, FALSE) "
                          "ON CONFLICT (colaborador_id) DO UPDATE SET ativo = FALSE"), {"c": mundo["joao"]})
        conn.commit()
    msgs = []
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda t, m: msgs.append(m))
    codigo = msgs[-1].split("código é ")[1][:6]
    r = app.test_client().post("/ponto/app/api/pin", json={"cpf": CPF_JOAO, "codigo": codigo, "pin": "481927"})
    assert r.status_code == 403 and "não bate ponto" in r.get_json()["erro"]
    dp = como(app, mundo["dp"])
    d = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/acesso").get_json()["acesso"]
    assert d["codigos"][0]["usado"] is False and d["tem_pin"] is False
    assert any("não bate ponto" in x for x in d["impede"])
    # religado, o mesmo código ainda vale
    with banco.connect() as conn:
        conn.execute(text("UPDATE ponto.colaborador_config SET ativo = TRUE WHERE colaborador_id = :c"), {"c": mundo["joao"]})
        conn.commit()
    assert app.test_client().post("/ponto/app/api/pin", json={"cpf": CPF_JOAO, "codigo": codigo,
                                                               "pin": "481927"}).status_code == 200


def test_correcao_escolhe_entre_todas_as_obras_cadastradas(app, mundo, monkeypatch):
    """16/10/2026, o dono: "só tem a obra do cadastro. Não é para ser a obra do
    cadastro, é para aparecer a relação de obras cadastradas". João é da obra A
    e trabalhou na B: o aplicativo oferece a B, e o pedido entra."""
    from tests.test_ponto_gestao_banco import CPF_JOAO, _entrar_no_app, como, _segunda_passada
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    eu = cel.get("/ponto/app/api/eu").get_json()
    assert {o["codigo"] for o in eu["obras"]} == {"PG-A"}
    assert {"PG-A", "PG-B"} <= {o["codigo"] for o in eu["obras_cadastradas"]}
    seg = _segunda_passada()
    r = como(app, mundo["dp"]).post("/erp/api/ponto/ocorrencias", json={
        "tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"], "data_inicio": seg.isoformat(),
        "horario": f"{seg.isoformat()}T07:00:00", "obra": "PG-B", "descricao": "trabalhou na obra B nesse dia"})
    assert r.status_code in (200, 201), r.get_json()


def test_a_ficha_diz_por_que_o_celular_foi_bloqueado(app, mundo):
    """16/10/2026: "não aparece dentre os aprovados e está dando bloqueado; já tinha
    sido aprovado e sumiu". A ficha da pessoa mostra o motivo e se foi arquivado."""
    from tests.test_ponto_gestao_banco import CPF_JOAO, como
    from tests.test_ponto_regras_0610_banco import _id_do_aparelho
    uuid = "celular-do-joao-bloqueado-0123456789"
    app.test_client().post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid})
    dp = como(app, mundo["dp"])
    ap = _id_do_aparelho(dp, uuid)
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO}).status_code == 200
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/bloquear", json={"motivo": "celular perdido"}).status_code == 200
    a = next(x for x in dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]["aparelhos"]
             if x["id"] == ap)
    assert a["status"] == "BLOQUEADO" and "celular perdido" in a["motivo_bloqueio"] and a["bloqueado_em"]


def test_filtros_obras_com_batida_e_quem_bateu(app, mundo, banco):
    """16/10/2026: "o padrão de exibição deveria ser as obras que têm ponto" e, no
    espelho, primeiro a obra, depois a lista de quem bateu nela. Quem bateu em duas
    obras aparece nas duas; o supervisor da obra A não vê a B."""
    from tests.test_ponto_gestao_banco import CPF_JOAO, CPF_MARIA, como
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import marcacoes
    with db.conexao() as conn:
        marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True)
    with db.conexao() as conn:
        marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-B", origem="IDFACE", via_chave=True,
                            agora=horario.agora() + dt.timedelta(minutes=5))
    with db.conexao() as conn:
        marcacoes.registrar(conn, cpf=CPF_MARIA, obra="PG-B", origem="IDFACE", via_chave=True)
    hoje = horario.hoje().isoformat()
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    obras = {o["codigo"]: o for o in dp.get(f"/erp/api/ponto/obras-com-batida?de={hoje}&ate={hoje}").get_json()["obras"]}
    assert set(obras) == {"PG-A", "PG-B"} and obras["PG-B"]["pessoas"] == 2
    comp = hoje[:7]
    na_b = dp.get(f"/erp/api/ponto/pessoas-com-batida?obra={mundo['obra_b']}&competencia={comp}").get_json()["pessoas"]
    assert {p["id"] for p in na_b} == {mundo["joao"], mundo["maria"]}
    assert {o["codigo"] for o in sup.get(f"/erp/api/ponto/obras-com-batida?competencia={comp}").get_json()["obras"]} == {"PG-A"}
    assert sup.get(f"/erp/api/ponto/pessoas-com-batida?obra={mundo['obra_b']}&competencia={comp}").status_code == 404
    joao = next(p for p in dp.get("/erp/api/ponto/pessoas").get_json()["pessoas"] if p["id"] == mundo["joao"])
    assert set(joao["obras_batidas"]) == {"PG-A", "PG-B"}
