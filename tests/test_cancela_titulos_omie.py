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


def test_lista_colada_no_txt(m, tmp_path):
    arq = tmp_path / "lista.txt"
    arq.write_text("Card\tObs\n101\tqualquer\n 102 \nInt103;x\n\n101\n", encoding="utf-8")
    assert m.ler_ids(str(arq))[0] == ["101", "102", "103"]


def _preparar(m, tmp_path, monkeypatch, respostas, resposta_sim="SIM"):
    (tmp_path / ".env").write_text("OMIE_KEY=k\nOMIE_SECRET=s\nPAUSA_ENTRE_CHAMADAS=0\n", encoding="utf-8")
    monkeypatch.setattr(m.sys, "argv", ["x"])
    monkeypatch.setattr("builtins.input", lambda *_: resposta_sim)
    monkeypatch.setenv("SIMULAR", "nao")
    monkeypatch.setenv("ARQUIVO_PLANILHA", "")
    sessao = SessaoFalsa(respostas)
    monkeypatch.setattr(m.requests, "Session", lambda: sessao)
    return sessao


def _pagina(pagina, total, titulos):
    return Resp(200, {"pagina": pagina, "total_de_paginas": total,
                      "conta_pagar_cadastro": titulos})


def test_execucao_completa(m, tmp_path, monkeypatch):
    entrada = tmp_path / "entrada"
    entrada.mkdir()
    (entrada / "LEIA-ME.txt").write_text("instruções 999\n", encoding="utf-8")
    (entrada / "a.csv").write_text("card\n1\n2\n3\n", encoding="utf-8")
    (entrada / "b.txt").write_text("4\n5\n6\n1\n", encoding="utf-8")
    sessao = _preparar(m, tmp_path, monkeypatch, [
        _pagina(1, 2, [
            {"codigo_lancamento_omie": 11, "codigo_lancamento_integracao": "Int1", "status_titulo": "A VENCER"},
            {"codigo_lancamento_omie": 22, "codigo_lancamento_integracao": "INT2", "status_titulo": "CANCELADO"},
            {"codigo_lancamento_omie": 99, "codigo_lancamento_integracao": "OUTRO", "status_titulo": "A VENCER"},
        ]),
        _pagina(2, 2, [
            {"codigo_lancamento_omie": 44, "codigo_lancamento_integracao": "Int4", "status_titulo": "PAGO"},
            {"codigo_lancamento_omie": 55, "codigo_lancamento_integracao": "Int5", "status_titulo": "ATRASADO"},
        ]),
        Resp(200, {"codigo_status": "0", "descricao_status": "Cancelado"}),       # cancela 1
        Resp(200, {"codigo_lancamento_omie": 11, "status_titulo": "CANCELADO"}),  # confere 1
        Resp(500, {"faultstring": "Título incluído em remessa de pagamento. Não é permitido cancelar."}),  # cancela 5
        Resp(200, {"codigo_lancamento_omie": 55, "status_titulo": "ATRASADO"}),   # confere 5 após recusa
    ])
    assert m.main() == 0
    assert not sessao.respostas
    # Nenhuma consulta de card ausente: só a lista, os cancelamentos e as conferências.
    chamadas = [c.split('"call": "')[1].split('"')[0] for c in sessao.corpos]
    assert chamadas == ["ListarContasPagar", "ListarContasPagar", "CancelarContaPagar",
                        "ConsultarContaPagar", "CancelarContaPagar", "ConsultarContaPagar"]
    assert '"codigo_lancamento_omie": 11' in sessao.corpos[2]
    res = {l["Card Pipefy"]: l["Resultado"] for l in m._ESTADO["linhas"]}
    assert res == {"1": "CANCELADO AGORA", "2": "JÁ ESTAVA CANCELADO",
                   "3": "NÃO EXISTE NO OMIE", "4": "PAGO / COM BAIXA", "5": "FALHOU",
                   "6": "NÃO EXISTE NO OMIE"}
    assert len(os.listdir(tmp_path / "relatorios")) == 1
    # Na entrada sobra só a lista de pendentes (e o LEIA-ME); os lidos foram para processados.
    sobra = sorted(n for n in os.listdir(entrada) if n != "processados")
    assert sobra[0] == "LEIA-ME.txt" and sobra[1].startswith("pendentes_") and len(sobra) == 2
    assert (entrada / sobra[1]).read_text(encoding="utf-8").split() == ["5"]
    assert len(os.listdir(entrada / "processados")) == 2
    # A lista guardada já sabe do cancelamento.
    assert m.ler_cache()["titulos"]["INT1"]["status_titulo"] == "CANCELADO"


def test_baixa_interrompida_continua_da_pagina(m, tmp_path, monkeypatch):
    omie = m.Omie("k", "s", pausa=0)
    omie.sessao = SessaoFalsa([
        _pagina(1, 3, [{"codigo_lancamento_integracao": "Int1", "status_titulo": "A VENCER"}]),
        Resp(400, {"faultstring": "Erro qualquer"}),
    ])
    with pytest.raises(m.ErroOmie):
        m.baixar_titulos(omie, "Int", 12)
    omie.sessao = SessaoFalsa([
        _pagina(2, 3, [{"codigo_lancamento_integracao": "Int2", "status_titulo": "A VENCER"}]),
        _pagina(3, 3, []),
    ])
    cache = m.baixar_titulos(omie, "Int", 12)
    assert '"pagina": 2' in omie.sessao.corpos[0]
    assert set(cache["titulos"]) == {"INT1", "INT2"} and cache["completo"]
    # Rodada seguinte dentro da validade: nem chama o Omie.
    omie.sessao = SessaoFalsa([])
    assert m.baixar_titulos(omie, "Int", 12)["titulos"].keys() == {"INT1", "INT2"}


def test_segunda_rodada_le_so_os_pendentes(m, tmp_path, monkeypatch):
    entrada = tmp_path / "entrada"
    entrada.mkdir()
    (entrada / "pendentes_x.txt").write_text("5\n", encoding="utf-8")
    sessao = _preparar(m, tmp_path, monkeypatch, [
        _pagina(1, 1, [{"codigo_lancamento_omie": 55, "codigo_lancamento_integracao": "Int5",
                        "status_titulo": "CANCELADO"}])])
    assert m.main() == 0
    assert [l["Card Pipefy"] for l in m._ESTADO["linhas"]] == ["5"]
    assert [n for n in os.listdir(entrada) if n != "processados"] == []


def test_simulacao_nao_mexe_na_entrada(m, tmp_path, monkeypatch):
    entrada = tmp_path / "entrada"
    entrada.mkdir()
    (entrada / "a.txt").write_text("1\n", encoding="utf-8")
    sessao = _preparar(m, tmp_path, monkeypatch, [
        _pagina(1, 1, [{"codigo_lancamento_omie": 11, "codigo_lancamento_integracao": "Int1",
                        "status_titulo": "A VENCER"}])], resposta_sim="")
    assert m.main() == 0
    assert m._ESTADO["linhas"][0]["Resultado"] == "SERIA CANCELADO (simulação)"
    assert os.listdir(entrada) == ["a.txt"]


def test_lista_velha_reconfere_antes_de_cancelar(m, tmp_path, monkeypatch):
    import time as _t
    entrada = tmp_path / "entrada"
    entrada.mkdir()
    (entrada / "a.txt").write_text("1\n", encoding="utf-8")
    duas_horas = _t.time() - 7200
    m.gravar_cache({"prefixo": "Int", "inicio": duas_horas, "fim": duas_horas, "completo": True,
                    "proxima_pagina": 2, "total_paginas": 1,
                    "titulos": {"INT1": {"codigo_lancamento_omie": 11, "status_titulo": "ATRASADO"}}})
    sessao = _preparar(m, tmp_path, monkeypatch, [
        Resp(200, {"codigo_lancamento_omie": 11, "status_titulo": "PAGO"})])   # foi pago depois
    assert m.main() == 0
    assert '"ConsultarContaPagar"' in sessao.corpos[0] and len(sessao.corpos) == 1
    assert m._ESTADO["linhas"][0]["Resultado"] == "PAGO / COM BAIXA"


def test_lista_guardada_fora_da_pasta_e_falha_ao_gravar_nao_derruba(m, tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local"))
    assert m._caminho_cache().startswith(str(tmp_path / "local"))
    assert m.gravar_cache({"titulos": {}}) is True

    def trava(*a):
        raise PermissionError("Acesso negado")
    monkeypatch.setattr(m.os, "replace", trava)
    assert m.gravar_cache({"titulos": {}}) is False


def test_comando_inexistente_nao_repete(m):
    omie = m.Omie("k", "s", pausa=0)
    omie.sessao = SessaoFalsa([Resp(500, {"status": "error",
                                          "message": 'Method "CancelarContaPagar" not exists'})])
    with pytest.raises(m.ErroOmie):
        omie.cancelar(11, "CancelarContaPagar")
    assert len(omie.sessao.corpos) == 1
