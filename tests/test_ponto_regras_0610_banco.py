# -*- coding: utf-8 -*-
"""Ponto — os pedidos do dono de 06/10/2026, com Postgres de verdade:
quem sai perde o acesso; a escala padrão faz a falta aparecer no dia seguinte;
o aparelho de grupo é temporário e o sistema sugere cancelá-lo; as licenças da
lei numa lista; e só quem tem permissão de bater faz pedido."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _entrar_no_app,  # noqa: F401
                                           _png, _schema_ponto2, app, como, mundo)

pytestmark = pytest.mark.banco


def _aparelho(app, uuid):
    c = app.test_client()
    token = c.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    return c, {"X-Device-UUID": uuid, "X-Device-Token": token}


def _id_do_aparelho(dp, uuid):
    return next(a["id"] for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"]
                if a["device_uuid"] == uuid)


def test_quem_e_desligado_perde_celular_qr_e_grupo(app, mundo, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import desligamentos, qr
    dp = como(app, mundo["dp"])
    _, _h = _aparelho(app, "celular-do-joao-desligado-012345")
    cel = _id_do_aparelho(dp, "celular-do-joao-desligado-012345")
    dp.post(f"/erp/api/ponto/dispositivos/{cel}/aprovar", json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO})
    _, _h2 = _aparelho(app, "aparelho-de-grupo-desligado-0123")
    grupo = _id_do_aparelho(dp, "aparelho-de-grupo-desligado-0123")
    dp.post(f"/erp/api/ponto/dispositivos/{grupo}/aprovar", json={"perfil": "LISTA", "autorizados": [CPF_JOAO, CPF_MARIA]})
    with db.conexao() as conn:
        qr.gravar_enviado(conn, mundo["joao"], "BWSP1.qr-do-joao-desligado-00000001", "GESTAO")
        assert desligamentos.aplicar(conn)["pessoas"] == 0                      # ninguém saiu ainda
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET situacao = 'DESLIGADO' WHERE id = :j"), {"j": mundo["joao"]})
        conn.commit()
    with db.conexao() as conn:
        r = desligamentos.aplicar(conn)
    assert (r["pessoas"], r["celulares"], r["grupos"], r["qr"]) == (1, 1, 1, 1)
    lista = {a["id"]: a for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"]}
    assert lista[cel]["status"] == "BLOQUEADO" and "desligada" in lista[cel]["motivo_bloqueio"]
    assert lista[grupo]["qtd_autorizados"] == 1 and lista[grupo]["status"] == "APROVADO"


def test_escala_padrao_faz_a_falta_aparecer(app, mundo, banco):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import escalas, espelho
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM ponto.colaborador_escalas WHERE colaborador_id = :j"), {"j": mundo["joao"]})
        conn.commit()
    ontem = horario.hoje() - dt.timedelta(days=1)
    while ontem.weekday() > 3:                      # um dia de segunda a quinta da escala de 44 h
        ontem -= dt.timedelta(days=1)
    with db.conexao() as conn:
        assert espelho.montar(conn, mundo["joao"], ontem, ontem)["dias"][0]["situacao"] == "SEM_ESCALA"
    dp = como(app, mundo["dp"])
    assert dp.post("/erp/api/ponto/escalas/padrao", json={"escala_id": mundo["escala"]}).status_code == 200
    assert dp.get("/erp/api/ponto/escalas").get_json()["padrao_id"] == mundo["escala"]
    with db.conexao() as conn:
        d = espelho.montar(conn, mundo["joao"], ontem, ontem)["dias"][0]
        assert d["situacao"] == "FALTA" and "padrão da empresa" in d["escala"]
        assert escalas.gravar_padrao(conn, None, "teste") is None


def test_grupo_e_temporario_e_o_sistema_sugere_cancelar(app, mundo, banco):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import dispositivos
    dp = como(app, mundo["dp"])
    _aparelho(app, "aparelho-de-grupo-temporario-012")
    g = _id_do_aparelho(dp, "aparelho-de-grupo-temporario-012")
    longe = (horario.hoje() + dt.timedelta(days=120)).isoformat()
    r = dp.post(f"/erp/api/ponto/dispositivos/{g}/aprovar", json={"perfil": "LISTA", "autorizados": [CPF_JOAO],
                                                                  "valido_ate": longe})
    assert r.status_code == 400 and "90 dias" in r.get_json()["erro"]
    j = dp.post(f"/erp/api/ponto/dispositivos/{g}/aprovar",
                json={"perfil": "LISTA", "autorizados": [CPF_JOAO]}).get_json()["dispositivo"]
    assert j["valido_ate"] == (horario.hoje() + dt.timedelta(days=15)).isoformat()
    with banco.connect() as conn:                   # o grupo venceu
        conn.execute(text("UPDATE ponto.dispositivos SET valido_ate = :v WHERE id = :g"),
                     {"v": horario.hoje() - dt.timedelta(days=1), "g": g})
        conn.commit()
    with db.conexao() as conn:
        a = dispositivos.por_id(conn, g)
        assert "venceu" in dispositivos.autorizado_para(a, mundo["joao"], mundo["obra_a"], {mundo["joao"]}, set())
    item = next(i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["tipo"] == "GRUPO")
    assert item["sugestao"] == "CANCELAR" and "venceu" in item["detalhe"]
    assert dp.post(f"/erp/api/ponto/dispositivos/{g}/renovar", json={}).status_code == 200
    assert not [i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["tipo"] == "GRUPO"]


def test_licencas_da_lei_conferem_dias_documento_e_limite(app, mundo):
    from app.apps.ponto import horario
    dp = como(app, mundo["dp"])
    lic = {l["codigo"]: l for l in dp.get("/erp/api/ponto/licencas").get_json()["licencas"]}
    assert lic["CASAMENTO"]["dias"] == 3 and lic["DOACAO_SANGUE"]["limite_ano"] == 1
    d0 = horario.hoje() + dt.timedelta(days=10)
    base = {"tipo": "LICENCA", "colaborador_id": mundo["joao"], "data_inicio": d0.isoformat()}
    r = dp.post("/erp/api/ponto/afastamentos", json={**base, "subtipo": "CASAMENTO",
                                                     "data_fim": (d0 + dt.timedelta(days=3)).isoformat(),
                                                     "documento_base64": _png()})
    assert r.status_code == 400 and "3 dia" in r.get_json()["erro"]
    r = dp.post("/erp/api/ponto/afastamentos", json={**base, "subtipo": "CASAMENTO",
                                                     "data_fim": (d0 + dt.timedelta(days=2)).isoformat()})
    assert r.status_code == 400 and "certidão de casamento" in r.get_json()["erro"]
    ok = dp.post("/erp/api/ponto/afastamentos", json={**base, "subtipo": "DOACAO_SANGUE", "documento_base64": _png()})
    assert ok.status_code == 201 and "doação" in ok.get_json()["ocorrencia"]["rotulo"]
    d1 = (d0 + dt.timedelta(days=20)).isoformat()
    r = dp.post("/erp/api/ponto/afastamentos", json={**base, "data_inicio": d1, "subtipo": "DOACAO_SANGUE",
                                                     "documento_base64": _png()})
    assert r.status_code == 400 and "12 meses" in r.get_json()["erro"]
    assert dp.post("/erp/api/ponto/afastamentos", json={**base, "subtipo": ""}).status_code == 400


def test_so_quem_bate_faz_pedido(app, mundo, monkeypatch):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import dispositivos, forma_de_bater
    d0 = (horario.hoje() + dt.timedelta(days=5)).isoformat()
    atestado = {"tipo": "ATESTADO", "data_inicio": d0, "data_fim": d0, "documento_base64": _png()}
    # 1. No próprio celular, quem não é exceção não pede
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    r = cel.post("/ponto/app/api/pedidos", json=atestado)
    assert r.status_code == 400 and "aparelho da obra" in r.get_json()["erro"]
    with db.conexao() as conn:
        forma_de_bater.definir(conn, mundo["joao"], True, "teste")
    assert cel.post("/ponto/app/api/pedidos", json=atestado).status_code == 201       # exceção pede
    # 2. No aparelho da obra, identificado pelo CPF
    tab, h = _aparelho(app, "tablet-da-obra-pedidos-01234567")
    dp = como(app, mundo["dp"])
    with db.conexao() as conn:
        dispositivos.aprovar(conn, _id_do_aparelho(dp, "tablet-da-obra-pedidos-01234567"), perfil="COMPARTILHADO",
                             aprovado_por="teste", obras=[mundo["obra_b"]])
    ident = tab.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_MARIA, "para_pedido": True},
                     headers=h).get_json()
    assert ident["validade_segundos"] == 600
    assert tab.post("/ponto/app/api/bater", json={"bilhete": ident["bilhete"]}, headers=h).status_code == 403
    p = tab.post("/ponto/app/api/tablet/pedido", json={"bilhete": ident["bilhete"], **atestado}, headers=h)
    assert p.status_code == 201, p.get_json()
    ped = dp.get(f"/erp/api/ponto/ocorrencias?colaborador_id={mundo['maria']}").get_json()["ocorrencias"]
    assert ped[0]["origem"] == "APARELHO" and ped[0]["etapa_atual"] == "DP"
    # 3. O encarregado entrega atestado no ERP (com documento) — o DP valida
    sup = como(app, mundo["sup"])
    d1 = (horario.hoje() + dt.timedelta(days=8)).isoformat()
    sem_doc = sup.post("/erp/api/ponto/ocorrencias", json={"tipo": "ATESTADO", "colaborador_id": mundo["joao"],
                                                          "data_inicio": d1})
    assert sem_doc.status_code == 400
    r = sup.post("/erp/api/ponto/ocorrencias", json={"tipo": "ATESTADO", "colaborador_id": mundo["joao"],
                                                    "data_inicio": d1, "documento_base64": _png()})
    assert r.status_code == 201 and r.get_json()["ocorrencia"]["etapa_atual"] == "DP"
    assert sup.post("/erp/api/ponto/ocorrencias", json={"tipo": "FERIAS", "colaborador_id": mundo["joao"],
                                                       "data_inicio": d1}).status_code == 400


def test_fila_rapida_no_tablet_vai_para_conferencia_na_hora(app, mundo, banco):
    """Cinco pessoas diferentes, uma a cada 3 s, no mesmo tablet: a quinta já
    entra em conferência (antes, só o alerta do dia seguinte via isso)."""
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import dispositivos, marcacoes
    from tests.test_ponto_registro_banco import _cpf
    cpfs = [CPF_JOAO] + [_cpf(b) for b in ("135792468", "246813570", "975318642", "864297531")]
    with banco.connect() as conn:
        for i, c in enumerate(cpfs[1:]):
            conn.execute(text("INSERT INTO colaboradores (nome, cpf, obra_id) VALUES (:n, :c, :o)"),
                         {"n": f"Pessoa {i}", "c": c, "o": mundo["obra_a"]})
        conn.commit()
    try:
        tab, h = _aparelho(app, "tablet-da-fila-rapida-0123456789")
        dp = como(app, mundo["dp"])
        with db.conexao() as conn:
            dispositivos.aprovar(conn, _id_do_aparelho(dp, "tablet-da-fila-rapida-0123456789"),
                                 perfil="COMPARTILHADO", aprovado_por="teste", obras=[mundo["obra_a"]])
        t0 = horario.agora() - dt.timedelta(minutes=5)
        status = []
        for i, c in enumerate(cpfs):
            with db.conexao() as conn:
                m, _ = marcacoes.registrar(conn, cpf=c, obra="PG-A", origem="PWA", device_uuid=h["X-Device-UUID"],
                                           device_token=h["X-Device-Token"], latitude=OBRA_A[0],
                                           longitude=OBRA_A[1], precisao=10, foto_base64=None,
                                           agora=t0 + dt.timedelta(seconds=3 * i))
            status.append((m["status"], m.get("motivo_analise") or ""))
        assert "fila rápida" in status[-1][1] and "fila rápida" not in status[3][1]
    finally:
        with banco.connect() as conn:
            conn.execute(text("DELETE FROM ponto.marcacoes WHERE colaborador_id IN "
                              "(SELECT id FROM colaboradores WHERE cpf = ANY(:c))"), {"c": cpfs[1:]})
            conn.execute(text("DELETE FROM colaboradores WHERE cpf = ANY(:c)"), {"c": cpfs[1:]})
            conn.commit()
