# -*- coding: utf-8 -*-
"""Ferramenta de PC que cancela no Omie os títulos de cards cancelados no Pipefy
(ferramentas/cancela_titulos_omie). O Omie é dublado: nenhuma chamada sai daqui."""
import importlib.util
import os

import pytest

CAMINHO = os.path.join(os.path.dirname(__file__), "..", "ferramentas",
                       "cancela_titulos_omie", "cancela_omie.py")


@pytest.fixture
def m(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("cancela_omie_teste", CAMINHO)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "PASTA", str(tmp_path))
    monkeypatch.setattr(mod, "esperar", lambda s, motivo: mod._ESPERAS.append(s))
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    mod._ESPERAS = []
    return mod


class Resp:
    def __init__(self, status, dados):
        self.status_code = status
        self._dados = dados
        self.text = str(dados)

    def json(self):
        return self._dados


class SessaoFalsa:
    def __init__(self, respostas):
        self.respostas = list(respostas)
        self.corpos = []
        self.headers = {}

    def post(self, url, data, timeout):
        self.corpos.append(data)
        return self.respostas.pop(0)


def test_normaliza_id(m):
    assert m.normalizar_id("12345") == "12345"
    assert m.normalizar_id(12345.0) == "12345"
    assert m.normalizar_id(" Int 987 ") == "987"
    assert m.normalizar_id("INT987") == "987"
    assert m.normalizar_id("ID do card") == ""
    assert m.normalizar_id(None) == ""


def test_le_planilha_xlsx_sem_repetir(m, tmp_path):
    from openpyxl import Workbook
    wb = Workbook()
    for v in ("Card", 111, "222", 111, None, "Int333"):
        wb.active.append([v])
    arq = tmp_path / "cards.xlsx"
    wb.save(arq)
    ids, ignorados = m.ler_ids(str(arq))
    assert ids == ["111", "222", "333"]
    assert ignorados == ["Card"]


def test_le_csv_com_ponto_e_virgula(m, tmp_path):
    arq = tmp_path / "cards.csv"
    arq.write_text("id;obs\n10;x\n20;y\n", encoding="utf-8")
    assert m.ler_ids(str(arq))[0] == ["10", "20"]


@pytest.mark.parametrize("titulo,pode", [
    ({"status_titulo": "A VENCER"}, True),
    ({"status_titulo": "ATRASADO"}, True),
    ({"status_titulo": "VENCE HOJE"}, True),
    ({"status_titulo": "CANCELADO"}, False),
    ({"status_titulo": "PAGO"}, False),
    ({"status_titulo": "PARCIAL"}, False),
    ({"status_titulo": "A VENCER", "valor_pago": "50,00"}, False),
    ({"status_titulo": "QUALQUER"}, False),
    ({}, False),
])
def test_so_cancela_em_aberto_sem_pagamento(m, titulo, pode):
    assert m.classificar(titulo)[1] is pode


def test_espera_o_tempo_pedido_e_repete(m):
    omie = m.Omie("k", "s", pausa=0)
    omie.sessao = SessaoFalsa([
        Resp(425, {"faultstring": "API bloqueada por consumo indevido. Tente novamente em 664 segundos."}),
        Resp(200, {"status_titulo": "A VENCER"}),
    ])
    assert omie.consultar("Int1")["status_titulo"] == "A VENCER"
    assert m._ESPERAS == [669]


def test_nao_encontrado_nao_repete(m):
    omie = m.Omie("k", "s", pausa=0)
    omie.sessao = SessaoFalsa([Resp(500, {"faultstring": "ERROR: Lançamento não cadastrado para o Código de Integração [Int1] !"})])
    with pytest.raises(m.ErroOmie) as e:
        omie.consultar("Int1")
    assert e.value.nao_existe
    assert len(omie.sessao.corpos) == 1


def test_espera_acima_do_teto_nao_trava(m):
    omie = m.Omie("k", "s", pausa=0, teto_espera=60)
    omie.sessao = SessaoFalsa([Resp(425, {"faultstring": "Tente novamente em 900 segundos."})])
    with pytest.raises(m.ErroOmie):
        omie.consultar("Int1")
    assert m._ESPERAS == []


def test_execucao_completa(m, tmp_path, monkeypatch):
    (tmp_path / "entrada").mkdir()
    (tmp_path / "entrada" / "c.csv").write_text("card\n1\n2\n3\n4\n", encoding="utf-8")
    (tmp_path / ".env").write_text("OMIE_KEY=k\nOMIE_SECRET=s\nPAUSA_ENTRE_CHAMADAS=0\n", encoding="utf-8")
    monkeypatch.setattr(m.sys, "argv", ["x"])
    monkeypatch.setattr("builtins.input", lambda *_: "SIM")
    for nome in ("SIMULAR", "ARQUIVO_PLANILHA"):
        monkeypatch.delenv(nome, raising=False)
    monkeypatch.setenv("SIMULAR", "nao")
    monkeypatch.setenv("ARQUIVO_PLANILHA", "")
    sessao = SessaoFalsa([
        Resp(200, {"codigo_lancamento_omie": 11, "status_titulo": "A VENCER"}),   # 1 consulta
        Resp(200, {"codigo_lancamento_omie": 22, "status_titulo": "CANCELADO"}),  # 2
        Resp(500, {"faultstring": "Lançamento não cadastrado"}),                  # 3
        Resp(200, {"codigo_lancamento_omie": 44, "status_titulo": "PAGO"}),       # 4
        Resp(200, {"codigo_status": "0", "descricao_status": "Cancelado"}),       # cancela 1
        Resp(200, {"codigo_lancamento_omie": 11, "status_titulo": "CANCELADO"}),  # confere 1
    ])
    monkeypatch.setattr(m.requests, "Session", lambda: sessao)
    assert m.main() == 0
    assert not sessao.respostas
    cancelamento = sessao.corpos[4]
    assert '"CancelarContaPagar"' in cancelamento and '"codigo_lancamento_omie": 11' in cancelamento
    res = {l["Card Pipefy"]: l["Resultado"] for l in m._ESTADO["linhas"]}
    assert res == {"1": "CANCELADO AGORA", "2": "JÁ ESTAVA CANCELADO",
                   "3": "NÃO EXISTE NO OMIE", "4": "PAGO / COM BAIXA"}
    assert len(os.listdir(tmp_path / "relatorios")) == 1
