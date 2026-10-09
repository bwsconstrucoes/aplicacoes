# -*- coding: utf-8 -*-
"""Ponto — pedidos do dono de 09/10/2026, com Postgres de verdade: a obra que só
bate entrada e saída (intervalo pré-assinalado) não gera pendência; e o ajuste
pedido aparece no dia, no "Meu mês", com o horário."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, OBRA_A, _entrar_no_app, _schema_ponto2,  # noqa: F401
                                           _segunda_passada, app, bater_via_chave, como, local, mundo)

pytestmark = pytest.mark.banco


def test_obra_de_so_entrada_e_saida_nao_gera_pendencia(app, mundo):
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    bater_via_chave(app, CPF_JOAO, local(seg, 7, 0))
    bater_via_chave(app, CPF_JOAO, local(seg, 17, 0))

    def o_dia():
        esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
        return next(d for d in esp["dias"] if d["data"] == seg.isoformat())
    antes = o_dia()
    # A hora é a de Fortaleza: 07:00 em ponto não é atraso (o banco devolve UTC —
    # em 09/10/2026 o atraso saía com 3 h a mais)
    assert antes["atraso"] == 0 and "ATRASO" not in antes["alertas"]
    assert antes["extra"] == 60 and "INTERVALO_CURTO" in antes["alertas"] and len(antes["faltando"]) == 2
    cercas = dp.get("/erp/api/ponto/cercas").get_json()
    assert cercas["batidas_disponivel"] is True
    r = dp.post(f"/erp/api/ponto/cercas/{mundo['obra_a']}", json={"raio_metros": 200, "so_entrada_e_saida": True})
    assert r.status_code == 200 and r.get_json()["obra"]["so_entrada_e_saida"] is True
    assert next(o for o in dp.get("/erp/api/ponto/cercas").get_json()["obras"]
                if o["id"] == mundo["obra_a"])["so_entrada_e_saida"] is True
    depois = o_dia()
    assert depois["situacao"] == "OK" and depois["extra"] == 0 and depois["trabalhado"] == 540
    assert depois["faltando"] == [] and [p["rotulo"] for p in depois["previstas"]] == ["Entrada", "Saída"]
    assert depois["so_entrada_e_saida"] is True and depois["alertas"] == []


def test_ajuste_pedido_aparece_no_dia_com_o_horario(app, mundo, monkeypatch):
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0)):                    # esqueceu a saída
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)
    r = cel.post("/ponto/app/api/pedidos/ajuste-do-dia", json={
        "horarios": [f"{seg.isoformat()}T17:00:00"], "motivo": "ESQUECI", "descricao": "esqueci de bater a saída",
        "obra": "PG-A"})
    assert r.status_code == 201, r.get_json()
    mes = cel.get(f"/ponto/app/api/mes?competencia={seg:%Y-%m}").get_json()
    dia = next(d for d in mes["dias"] if d["data"] == seg.isoformat())
    assert [(p["tipo"], p["hora"]) for p in dia["pendentes"]] == [("AJUSTE_BATIDA", "17:00")]
    assert dia["faltando"] == []                              # o horário pedido não "falta" mais


def test_fora_da_obra_explicando_vai_para_conferencia(app, mundo, monkeypatch):
    """Decisão do dono, 09/10/2026: fora da obra, com localização e explicação, a
    batida vai para conferência — e aparece no dia como "em conferência"; sem
    explicação, recusada; sem localização, nunca."""
    from app.apps.ponto import auth as _auth
    dp = como(app, mundo["dp"])
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)
    uuid = "celular-do-joao-na-rua-0123456789"
    token = cel.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, _auth.CABECALHO_TOKEN: token}
    ap = next(a for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"] if a["device_uuid"] == uuid)
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap['id']}/aprovar",
                   json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO}).status_code == 200
    longe = {"obra": "PG-A", "latitude": OBRA_A[0] - 0.05, "longitude": OBRA_A[1], "precisao": 10}
    r = cel.post("/ponto/app/api/bater", json=longe, headers=h)
    assert r.status_code == 403 and "explique o motivo" in r.get_json()["erro"]
    r = cel.post("/ponto/app/api/bater", json={**longe, "justificativa": "fui comprar material na loja"}, headers=h)
    assert r.status_code == 201, r.get_json()
    assert r.get_json()["status"] == "EM_ANALISE" and "fora da área da obra" in r.get_json()["motivo_analise"]
    assert "fui comprar material" in r.get_json()["motivo_analise"]
    hoje = cel.get("/ponto/app/api/eu").get_json()["hoje"]
    assert [b["status"] for b in hoje["batidas"]] == ["EM_ANALISE"]
    r = cel.post("/ponto/app/api/bater", json={"obra": "PG-A", "justificativa": "sem gps"}, headers=h)
    assert r.status_code == 403 and "localização" in r.get_json()["erro"]


def test_aparelho_pendente_ganha_o_nome_de_quem_entra_e_avisa_a_troca(app, mundo, monkeypatch):
    """09/10/2026: "chega aparelho sem nome" e "a pessoa que já tinha o celular
    cadastrado está pedindo de novo o cadastro"."""
    from app.apps.ponto import auth as _auth
    dp = como(app, mundo["dp"])
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)            # PIN 481927
    for uuid, aprovar in (("primeiro-celular-do-joao-01234567", True), ("celular-novo-do-joao-765432100", False)):
        c = app.test_client()
        token = c.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
        h = {"X-Device-UUID": uuid, _auth.CABECALHO_TOKEN: token}
        assert c.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"}, headers=h).status_code == 200
        ap = next(a for a in dp.get("/erp/api/ponto/dispositivos").get_json()["dispositivos"] if a["device_uuid"] == uuid)
        assert ap["descricao"].startswith("Celular de João Obra A") and ap["dono"]["id"] == mundo["joao"]
        if aprovar:
            assert dp.post(f"/erp/api/ponto/dispositivos/{ap['id']}/aprovar",
                           json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO}).status_code == 200
    item = next(i for i in dp.get("/erp/api/ponto/validacoes").get_json()["itens"] if i["tipo"] == "APARELHO")
    assert item["pessoa"] == "João Obra A" and item["troca_de_celular"] is True and "já tem um celular aprovado" in item["detalhe"]
    assert cel is not None
