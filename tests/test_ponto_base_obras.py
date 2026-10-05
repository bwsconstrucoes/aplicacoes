# -*- coding: utf-8 -*-
"""Ponto — a base de obras da aba "C. Diários" (pedido do dono, 05/10/2026):
o formato da coordenada (coluna AM, "Coordenadas Geográficas"), o status da
coluna V que esconde a obra, e o ritmo da leitura. Sem banco."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from app.apps.ponto.core import base_obras as bo


@pytest.mark.parametrize("texto, lat, lon", [
    ("-3.731862, -38.526670", "-3.731862", "-38.526670"),          # o que o Google Maps copia
    ("-3.731862,-38.526670", "-3.731862", "-38.526670"),
    ("-3,731862; -38,526670", "-3.731862", "-38.526670"),          # vírgula decimal
    ("-3,731862, -38,526670", "-3.731862", "-38.526670"),
    ("https://www.google.com/maps/@-3.731862,-38.526670,17z", "-3.731862", "-38.526670"),
    ("https://maps.google.com/?q=-3.731862,-38.526670", "-3.731862", "-38.526670"),
    ("3°43'54.7\"S 38°31'36.0\"W", "-3.731861", "-38.526667"),     # graus, minutos, segundos
])
def test_formatos_aceitos(texto, lat, lon):
    c = bo.ler_coordenada(texto)
    assert c["problema"] is None, c
    assert (c["latitude"], c["longitude"]) == (Decimal(lat), Decimal(lon))


def test_trocada_e_desvirada_com_aviso():
    c = bo.ler_coordenada("-38.526670, -3.731862")
    assert (c["latitude"], c["longitude"]) == (Decimal("-3.731862"), Decimal("-38.526670"))
    assert "trocadas" in c["aviso"]


@pytest.mark.parametrize("texto, pedaco", [
    ("-3.73, -38.52", "poucas casas"),
    ("-3.731862, 38.526670", "sinal de menos"),
    ("48.858370, 2.294481", "fora do Brasil"),
    ("perto da praça", "dois números"),
    ("-3.731862", "dois números"),
])
def test_formatos_recusados_dizem_por_que(texto, pedaco):
    c = bo.ler_coordenada(texto)
    assert c["latitude"] is None and pedaco in c["problema"]


def test_vazio_nao_e_problema():
    assert bo.ler_coordenada("  ") == {"latitude": None, "longitude": None, "aviso": None, "problema": None}


@pytest.mark.parametrize("status, escondida", [
    ("Concluída", True), ("Concluída com Dívida", True), ("Distratada", True), ("CONCLUIDA", True),
    ("Em andamento", False), ("Paralisada", False), ("", False), (None, False)])
def test_status_que_esconde_a_obra(status, escondida):
    assert bo.encerrada(status) is escondida


def _aba():
    cab = [""] * 39
    cab[0], cab[1], cab[2] = "Código", "Código Primário", "Centro de Custo"
    cab[21], cab[38] = "Status", "Coordenadas Geográficas"

    def linha(a, primario, nome, status, coord):
        l = [""] * 39
        l[0], l[1], l[2], l[21], l[38] = a, primario, nome, status, coord
        return l
    return [cab,
            linha("ESC01", "ESCFOR1", "Escola Fortaleza", "Em andamento", "-3.731862, -38.526670"),
            linha("", "POSTO2", "Posto 2", "Concluída com Dívida", ""),
            linha("X9", "", "Só código A", "Em andamento", "-3.7, -38.5"),
            linha("", "ESCFOR1", "Repetida", "Em andamento", ""),
            linha("", "", "sem código", "Em andamento", "")]


def test_interpretar_a_aba():
    r = bo.interpretar(_aba())
    por = {o["codigo"]: o for o in r["obras"]}
    assert set(por) == {"ESCFOR1", "POSTO2", "X9"}
    assert por["ESCFOR1"]["codigo_secundario"] == "ESC01" and por["ESCFOR1"]["nome"] == "Escola Fortaleza"
    assert por["ESCFOR1"]["latitude"] == Decimal("-3.731862")
    assert por["POSTO2"]["encerrada"] is True
    assert por["X9"]["codigo_secundario"] is None and por["X9"]["latitude"] is None
    problemas = {(p["codigo"], p["problema"].split(" ")[0]) for p in r["problemas"]}
    assert ("X9", "coordenada:") in problemas and ("ESCFOR1", "código") in problemas
    assert r["colunas"]["status"] == {"letra": "V", "titulo": "Status"}
    assert r["colunas"]["coordenada"] == {"letra": "AM", "titulo": "Coordenadas Geográficas"}


def test_coordenada_pelo_titulo_mesmo_fora_da_AM():
    aba = _aba()
    for l in aba:
        l.insert(5, "")                 # alguém inseriu uma coluna antes: a AM virou AN
    aba[0][22] = aba[0][21]             # o status continua sendo lido da V (posição)
    r = bo.interpretar(aba)
    assert r["colunas"]["coordenada"]["letra"] == "AN"
    assert next(o for o in r["obras"] if o["codigo"] == "ESCFOR1")["latitude"] == Decimal("-3.731862")


def test_ritmo_da_leitura():
    utc = dt.timezone.utc
    meio_dia = dt.datetime(2026, 10, 5, 15, 0, tzinfo=utc)          # 12h em Fortaleza
    assert bo.precisa_ler(None, meio_dia)
    assert not bo.precisa_ler({"em": "2026-10-05T14:00:00+00:00"}, meio_dia)
    assert bo.precisa_ler({"em": "2026-10-05T12:30:00+00:00"}, meio_dia)
    assert not bo.precisa_ler({"em": "2026-10-05T12:30:00+00:00", "falha_em": "2026-10-05T14:30:00+00:00"},
                              meio_dia)                          # a falha também espera o intervalo
    assert not bo.precisa_ler(None, dt.datetime(2026, 10, 5, 2, 0, tzinfo=utc))   # 23h: fora da janela
