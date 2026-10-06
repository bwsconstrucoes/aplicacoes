# -*- coding: utf-8 -*-
"""Ponto — "corrigir este dia" no celular e a batida com a obra escolhida na
lista, contra um Postgres DE VERDADE. Reusa o mundo de test_ponto_gestao_banco."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _entrar_no_app, _schema_ponto2,  # noqa: F401
                                           _segunda_passada, app, bater_via_chave, como, dia_util,
                                           local, mundo)

pytestmark = pytest.mark.banco


def _dia(cel, data: dt.date) -> dict:
    mes = cel.get(f"/ponto/app/api/mes?competencia={data:%Y-%m}").get_json()
    return next(d for d in mes["dias"] if d["data"] == data.isoformat())


def test_corrigir_o_dia_mostra_o_que_falta_e_barra_o_pedido_errado(app, mundo, banco, monkeypatch):
    seg = _segunda_passada()
    ter, qua = dia_util(seg, 1), dia_util(seg, 2)
    for h in ((7, 0), (11, 0)):                                   # segunda: só a manhã
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    for h in ((7, 0), (11, 0), (12, 0), (17, 0)):                 # terça: completa
        bater_via_chave(app, CPF_JOAO, local(ter, *h))
    with banco.connect() as conn:                                 # quarta: atestado aprovado
        conn.execute(text("""INSERT INTO ponto.ocorrencias (colaborador_id, tipo, data_inicio, data_fim, status,
                             solicitado_por) VALUES (:c, 'ATESTADO', :d, :d, 'APROVADA', 'DP')"""),
                     {"c": mundo["joao"], "d": qua})
        conn.commit()
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)

    d = _dia(cel, seg)
    assert [f["rotulo"] for f in d["faltando"]] == ["Volta do intervalo", "Saída"]
    assert [p["hora"] for p in d["previstas"]] == ["07:00", "11:00", "12:00", "17:00"]
    assert _dia(cel, ter)["faltando"] == [] and _dia(cel, qua)["faltando"] == []

    r = cel.post("/ponto/app/api/pedidos/ajuste-do-dia", json={
        "horarios": [f"{seg}T12:00:00", f"{seg}T17:05:00"], "motivo": "CELULAR", "descricao": "", "obra": "PG-A"})
    assert r.status_code == 201, r.get_json()
    assert r.get_json()["quantidade"] == 2
    assert r.get_json()["pedidos"][0]["descricao"].startswith("[Celular quebrado")
    assert _dia(cel, seg)["faltando"] == []                       # o pedido esperando já cobre

    def pedir(horarios, motivo="ESQUECI"):
        return cel.post("/ponto/app/api/pedidos/ajuste-do-dia",
                        json={"horarios": horarios, "motivo": motivo, "obra": "PG-A"})
    repetido = pedir([f"{seg}T12:10:00"])
    assert repetido.status_code == 400 and "pedido esperando decisão" in repetido.get_json()["erro"]
    ja_batido = pedir([f"{ter}T07:10:00"])
    assert ja_batido.status_code == 400 and "já existe batida às 07:00" in ja_batido.get_json()["erro"]
    completo = pedir([f"{ter}T14:00:00"])
    assert completo.status_code == 400 and "já tem as 4 batidas" in completo.get_json()["erro"]
    justificado = pedir([f"{qua}T07:00:00"])
    assert justificado.status_code == 400 and "justificado" in justificado.get_json()["erro"]
    futuro = pedir([f"{dt.date.today() + dt.timedelta(days=3)}T07:00:00"])
    assert futuro.status_code == 400
    outro_sem_texto = pedir([f"{dia_util(seg, 3)}T07:00:00"], motivo="OUTRO")
    assert outro_sem_texto.status_code == 400 and "conte o que aconteceu" in outro_sem_texto.get_json()["erro"]
    # nada entrou pela metade: só os 2 primeiros pedidos existem
    with banco.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM ponto.ocorrencias WHERE tipo = 'AJUSTE_BATIDA'")).scalar() == 2

    # o DP (o padrão de quem valida) aprova os dois e o dia fecha
    dp = como(app, mundo["dp"])
    for p in r.get_json()["pedidos"]:
        ok = dp.post(f"/erp/api/ponto/afastamentos/{p['id']}/etapa-dp", json={"aprovar": True})
        assert ok.status_code == 200, ok.get_json()
    d = _dia(cel, seg)
    assert d["situacao"] == "OK" and [b["status"] for b in d["batidas"]][-2:] == ["AJUSTADA", "AJUSTADA"]


def test_obra_escolhida_na_lista_leva_a_justificativa_para_a_conferencia(app, mundo):
    # Maria é da obra B, que não tem coordenada: a obra não é detectada
    cel = _entrar_no_app(app, CPF_MARIA, monkeypatch=None)
    uuid = "celular-da-maria-justifica-0123"
    token = cel.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    cel.post("/ponto/app/api/aparelho/identificar", json={}, headers=h)
    dp = como(app, mundo["dp"])
    aparelho = dp.get("/erp/api/ponto/pendencias").get_json()["aparelhos"][0]
    dp.post(f"/erp/api/ponto/dispositivos/{aparelho['id']}/aprovar", json={"perfil": "INDIVIDUAL", "cpf": CPF_MARIA})
    r = cel.post("/ponto/app/api/bater", json={"obra": "PG-B", "latitude": -3.80, "longitude": -38.60,
                                               "justificativa": "o GPS do meu celular não funciona"}, headers=h)
    assert r.status_code == 201, r.get_json()
    assert r.get_json()["status"] == "EM_ANALISE"
    assert "justificativa da pessoa: o GPS do meu celular não funciona" in r.get_json()["motivo_analise"]
