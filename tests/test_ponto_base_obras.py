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
    assert r["colunas"]["status"] == {"letra": "V", "titulo": "Status", "por_nome": True}
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


@pytest.mark.parametrize("texto, uf", [("PE", "PE"), (" pe ", "PE"), ("Pernambuco", "PE"), ("São Paulo", "SP"),
                                       ("ceará", "CE"), ("", None)])
def test_ler_uf_aceita_sigla_e_nome(texto, uf):
    assert bo.ler_uf(texto) == (uf, None)


def test_uf_ilegivel_vira_problema():
    uf, problema = bo.ler_uf("PX")
    assert uf is None and "UF ilegível" in problema

# ---------------------------------------------------------------------------
# O STATUS PELO NOME DO CABEÇALHO (31/10/2026)
#
# Pedido do dono: *"tem obras que a fase está concluída (…) tem como importar
# somente as que não estão concluídas?"*. O filtro existia, mas lia a coluna V
# pela POSIÇÃO — e numa planilha que gente edita, basta inserir uma coluna
# antes dela para o status virar outra coisa. Aí NADA é reconhecido como
# concluído, e obra encerrada volta a aparecer. Calado.
# ---------------------------------------------------------------------------
def _aba_de(cabecalhos, linhas):
    largura = max(len(cabecalhos), *(len(l) for l in linhas)) if linhas else len(cabecalhos)
    encher = lambda l: list(l) + [""] * (largura - len(l))     # noqa: E731
    return [encher(cabecalhos)] + [encher(l) for l in linhas]


def test_o_status_e_achado_pelo_nome_mesmo_fora_da_coluna_V():
    """O caso que levou ao pedido: o status saiu da V e ficou na D."""
    lido = bo.interpretar(_aba_de(
        ["Código", "Código Primário", "Centro de Custo", "Status da Obra"],
        [["A-1", "OBR-1", "Obra em andamento", "Em andamento"],
         ["A-2", "OBR-2", "Obra pronta", "Concluída"],
         ["A-3", "OBR-3", "Obra rompida", "Distratada"]]))

    assert lido["colunas"]["status"] == {"letra": "D", "titulo": "Status da Obra", "por_nome": True}
    assert {o["codigo"]: o["encerrada"] for o in lido["obras"]} == {
        "OBR-1": False, "OBR-2": True, "OBR-3": True}
    assert lido["alerta_status"] is None


@pytest.mark.parametrize("titulo", ["Status", "Fase", "Fase da Obra", "Situação", "Situacao", "Etapa"])
def test_os_nomes_que_valem_como_status(titulo):
    lido = bo.interpretar(_aba_de(["Código", titulo],
                               [["A-1", "Concluída"], ["A-2", "Em andamento"]]))
    assert lido["colunas"]["status"]["por_nome"] is True
    assert [o["encerrada"] for o in lido["obras"]] == [True, False]


def test_sem_coluna_com_nome_a_posicao_V_continua_valendo():
    """Quem não tem cabeçalho reconhecível cai na coluna V, como antes — a
    planilha de hoje é assim, e trocar isso de uma vez quebraria a leitura."""
    linha = [""] * 22
    linha[0], linha[21] = "A-1", "Concluída"
    outra = [""] * 22
    outra[0], outra[21] = "A-2", "Em andamento"
    lido = bo.interpretar([[""] * 22, linha, outra])

    assert lido["colunas"]["status"]["letra"] == "V"
    assert lido["colunas"]["status"]["por_nome"] is False
    assert [o["encerrada"] for o in lido["obras"]] == [True, False]


def test_status_especifico_ganha_do_genericos():
    """Planilha com "Status" (do contrato) e "Status da Obra" — vale a da obra."""
    lido = bo.interpretar(_aba_de(["Código", "Status", "Status da Obra"],
                               [["A-1", "Assinado", "Concluída"]]))
    assert lido["colunas"]["status"]["titulo"] == "Status da Obra"
    assert lido["obras"][0]["encerrada"] is True


# ---------------------------------------------------------------------------
# O ALERTA: "esta coluna não parece ser a do status"
# ---------------------------------------------------------------------------
def test_coluna_errada_e_acusada_quando_a_maioria_vem_sem_status():
    lido = bo.interpretar(_aba_de(["Código", "Nome da Obra", "Engenheiro"],
                               [["A-1", "Obra 1", ""], ["A-2", "Obra 2", ""],
                                ["A-3", "Obra 3", ""], ["A-4", "Obra 4", "Concluída"]]))
    assert lido["alerta_status"] and "SEM status" in lido["alerta_status"]


def test_nenhuma_obra_concluida_tambem_e_acusado():
    """Numa base de obras de anos, zero concluída é sinal de coluna errada —
    e o aviso diz as duas possibilidades, sem afirmar a errada."""
    lido = bo.interpretar(_aba_de(["Código", "Status"],
                               [["A-1", "Em andamento"], ["A-2", "Em execução"]]))
    assert lido["alerta_status"] and "Nenhuma obra concluída" in lido["alerta_status"]


def test_planilha_saudavel_nao_alerta_nada():
    lido = bo.interpretar(_aba_de(["Código", "Status"],
                               [["A-1", "Em andamento"], ["A-2", "Concluída"]]))
    assert lido["alerta_status"] is None


def test_o_alerta_diz_que_a_coluna_veio_pela_posicao():
    """Quando não há cabeçalho de status, o aviso precisa dizer isso — senão o
    dono procura o erro na planilha sem saber que o sistema chutou a coluna."""
    linha = [""] * 22
    linha[0] = "A-1"
    lido = bo.interpretar([[""] * 22, linha])
    assert "achada pela posição" in (lido["alerta_status"] or "")


# ---------------------------------------------------------------------------
# O status da planilha → a fase do ERP
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("status, fase", [
    ("Concluída", "CONCLUIDA"),
    ("concluida", "CONCLUIDA"),
    ("Concluída com Dívida", "CONCLUIDA_COM_DIVIDA"),
    ("Concluida com divida", "CONCLUIDA_COM_DIVIDA"),
    ("Distratada", "DISTRATADA"),
    ("distratado", "DISTRATADA"),
])
def test_a_fase_que_fica_no_erp(status, fase):
    assert bo.fase_no_erp(status) == fase
