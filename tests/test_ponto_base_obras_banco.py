# -*- coding: utf-8 -*-
"""Ponto — a aba "C. Diários" como base de obras e a forma de bater (pedidos
do dono, 05/10/2026). Postgres de verdade: a planilha esconde a obra concluída,
cria a que falta no ERP, a coordenada dela vence a do ERP; e só o aparelho da
obra bate, salvo a exceção cadastrada."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _entrar_no_app,  # noqa: F401
                                           _schema_ponto2, app, como, mundo)

pytestmark = pytest.mark.banco


def _linha(a, primario, nome, status, coord):
    l = [""] * 39
    l[0], l[1], l[2], l[21], l[38] = a, primario, nome, status, coord
    return l


def _aba():
    cab = _linha("Código", "Código Primário", "Centro de Custo", "Status", "Coordenadas Geográficas")
    return [cab,
            _linha("PG-A", "PG-ESCA", "Escola A (planilha)", "Em andamento", "-3.700000, -38.500000"),
            _linha("", "PG-NOVA", "Obra nova", "Em andamento", "-3.800000, -38.600000"),
            _linha("", "PG-VELHA", "Obra velha", "Concluída", ""),
            _linha("", "PG-C", "Obra C", "Distratada", "")]


@pytest.fixture
def planilha(mundo, banco):
    from app.apps.ponto.core import base_obras
    with banco.connect() as conn:          # obra do ERP que a planilha diz distratada
        conn.execute(text("INSERT INTO obras (codigo, nome, status) VALUES ('PG-C', 'Obra C', 'ATIVA')"))
        conn.commit()
    base_obras.esquecer()
    yield
    base_obras.esquecer()


def _ler(por="teste"):
    from app.apps.ponto import db
    from app.apps.ponto.core import base_obras
    with db.conexao() as conn:
        return base_obras.gravar(conn, base_obras.interpretar(_aba()), por)


def test_a_planilha_manda_nas_obras_do_ponto(app, mundo, planilha):
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    with db.conexao() as conn:
        antes = {o["codigo"] for o in cadastros.listar_obras(conn)}
    assert {"PG-A", "PG-B", "PG-C"} <= antes                 # sem leitura, vale o ERP

    r = _ler()
    assert (r["lidas"], r["ativas"], r["encerradas"], r["quantidade_criadas"]) == (4, 2, 2, 1)
    with db.conexao() as conn:
        obras = {o["codigo"]: o for o in cadastros.listar_obras(conn)}
        assert set(obras) == {"PG-A", "PG-NOVA"}
        a = obras["PG-A"]                                      # casou pelo código da coluna A
        assert a["nome"] == "Escola A (planilha)" and a["origem_coordenada"] == "PLANILHA"
        assert (float(a["latitude"]), float(a["longitude"])) == (-3.7, -38.5)
        assert cadastros.obra_por_codigo(conn, "PG-C")["status"] == "ENCERRADA"
        assert cadastros.obra_por_codigo(conn, "PG-B")["status"] == "FORA_DA_PLANILHA"
        assert db.um(conn, "SELECT status FROM public.obras WHERE codigo = 'PG-NOVA'")["status"] == "ATIVA"
        assert db.um(conn, "SELECT count(*) AS n FROM public.obras WHERE codigo = 'PG-VELHA'")["n"] == 0

    # Ler de novo não duplica nada no ERP
    assert _ler()["quantidade_criadas"] == 0

    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    t = dp.get("/erp/api/ponto/obras-base").get_json()
    assert t["usar"] is True and t["ativas"] == 2 and t["encerradas"] == 2 and t["com_coordenada"] == 2
    assert t["ultima"]["colunas"]["coordenada"]["titulo"] == "Coordenadas Geográficas"
    assert sup.post("/erp/api/ponto/obras-base/fonte", json={"fonte": "ERP"}).status_code == 403
    assert dp.post("/erp/api/ponto/obras-base/fonte", json={"fonte": "ERP"}).get_json()["usar"] is False
    with db.conexao() as conn:
        a = cadastros.obra_por_codigo(conn, "PG-A")
        assert a["nome"] == "Escola A" and float(a["latitude"]) == OBRA_A[0]          # de volta ao ERP


def test_a_cerca_usa_a_coordenada_da_planilha(app, mundo, planilha):
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros, geo
    _ler()
    with db.conexao() as conn:
        candidatas = cadastros.listar_obras(conn)
    situacao, detectada, _ = geo.localizar_obra(-3.7, -38.5, 10, candidatas)
    assert situacao == "DENTRO" and detectada["codigo"] == "PG-A"
    situacao, _, _ = geo.localizar_obra(OBRA_A[0], OBRA_A[1], 10, candidatas)     # o lugar do ERP
    assert situacao == "FORA"


def test_so_o_aparelho_da_obra_bate_salvo_a_excecao(app, mundo, monkeypatch):
    cel = _entrar_no_app(app, CPF_MARIA, monkeypatch)
    uuid = "celular-da-maria-0123456789abcdef"
    token = cel.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    cel.post("/ponto/app/api/aparelho/identificar", json={}, headers=h)
    assert cel.get("/ponto/app/api/eu").get_json()["bate_no_celular"] is False
    r = cel.post("/ponto/app/api/bater", json={"obra": "PG-B"}, headers=h)
    assert r.status_code == 403 and "aparelho da obra" in r.get_json()["erro"]

    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    fila = dp.get("/erp/api/ponto/validacoes").get_json()
    assert not [i for i in fila["itens"] if i["tipo"] == "APARELHO"]          # celular de quem não é exceção
    assert dp.get("/erp/api/ponto/excecoes").get_json()["celular"] == []

    assert sup.post(f"/erp/api/ponto/pessoas/{mundo['maria']}/forma-de-bater",
                    json={"bate_no_celular": True}).status_code == 404       # Maria é da obra B
    assert dp.post(f"/erp/api/ponto/pessoas/{mundo['maria']}/forma-de-bater",
                   json={"bate_no_celular": True}).status_code == 200
    assert cel.get("/ponto/app/api/eu").get_json()["bate_no_celular"] is True
    fila = dp.get("/erp/api/ponto/validacoes").get_json()
    assert [i for i in fila["itens"] if i["tipo"] == "APARELHO"]              # agora ela entra na fila
    assert [p["nome"] for p in dp.get("/erp/api/ponto/excecoes").get_json()["celular"]] == ["Maria Obra B"]

    # Desfazer a exceção faz o celular parar de bater, mesmo aprovado
    from app.apps.ponto import db
    from app.apps.ponto.core import dispositivos, forma_de_bater
    with db.conexao() as conn:
        a = dispositivos.por_uuid(conn, uuid)
        dispositivos.aprovar(conn, a["id"], perfil="INDIVIDUAL", aprovado_por="teste",
                             colaborador_id=mundo["maria"])
        forma_de_bater.definir(conn, mundo["maria"], False, "teste")
    r = cel.post("/ponto/app/api/bater", json={"obra": "PG-B"}, headers=h)
    assert r.status_code == 403 and "aparelho da obra" in r.get_json()["erro"]
    with db.conexao() as conn:                                               # aprovar de novo = exceção
        dispositivos.aprovar(conn, a["id"], perfil="INDIVIDUAL", aprovado_por="teste",
                             colaborador_id=mundo["maria"])
    assert cel.get("/ponto/app/api/eu").get_json()["bate_no_celular"] is True


def test_aparelho_da_obra_mostra_codigo_e_nao_aceita_login(app, mundo, monkeypatch):
    """Duas brechas da revisão de 05/10/2026: o aparelho esperando aprovação
    tem um código que a tela dele mostra (para não aprovar o celular de alguém
    como tablet), e no aparelho da obra ninguém entra com CPF e PIN."""
    from app.apps.ponto import db
    from app.apps.ponto.core import dispositivos
    _entrar_no_app(app, CPF_JOAO, monkeypatch)                  # cria o PIN 481927 do João
    c = app.test_client()
    uuid = "tablet-da-obra-a-0123456789abcd-ef"
    token = c.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    dp = como(app, mundo["dp"])
    item = next(i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["tipo"] == "APARELHO")
    assert item["codigo"] == "ABCDEF" and "ABCDEF" in item["detalhe"]
    assert c.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=h).status_code == 200
    with db.conexao() as conn:
        dispositivos.aprovar(conn, item["id"], perfil="COMPARTILHADO", aprovado_por="teste",
                             obras=[mundo["obra_a"]])
    r = c.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=h)
    assert r.status_code == 403 and "aparelho da obra" in r.get_json()["erro"]
