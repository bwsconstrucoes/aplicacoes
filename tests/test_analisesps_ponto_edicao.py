# -*- coding: utf-8 -*-
"""
01/10/2026 — corrigir o ponto no Mobponto a partir da janela do funcionário.

*"altera a obra ou adiciona uma obra que não existia, salva e grava as
alterações. Assim fica rápido de corrigir as possíveis distorções do ponto."*

Nenhum teste fala com o Mobponto: a chamada é trocada por um dublê que guarda o
que seria mandado.
"""
import pytest

from tests.test_analisesps_telas import app  # noqa: F401 — a fixture
from tests.test_analisesps_telas import (SENHA_CONSULTA, _como_mestre,
                                         _dias_do_mes, _preparar_folha_aberta,
                                         como)


@pytest.fixture
def configurado(monkeypatch):
    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic teste")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave-de-teste")
    monkeypatch.setenv("MOBPONTO_RESPONSAVEL_CPF", "111.222.333-96")
    monkeypatch.setenv("MOBPONTO_RESPONSAVEL_NOME", "MARCELO")


def _dublar(monkeypatch, respostas):
    """Troca a chamada ao Mobponto. `respostas`: lista de (ok, texto)."""
    from app.apps.analisesps import ponto_edicao
    mandados = []
    fila = list(respostas)

    def falso(payload):
        mandados.append(dict(payload))
        return fila.pop(0) if fila else (True, '{"status": true}')
    monkeypatch.setattr(ponto_edicao, "_mandar", falso)
    monkeypatch.setattr(ponto_edicao, "_registrar", lambda *a, **k: None)
    return mandados


def test_o_payload_e_o_do_script_dele(configurado, monkeypatch):
    from app.apps.analisesps import ponto_edicao
    mandados = _dublar(monkeypatch, [])
    feito = ponto_edicao.incluir_batidas(
        "997.133.493-34", "GERLANIO", "2026-09-03",
        [{"hora": "17:00", "obra": "cre1"}, {"hora": "07:00", "obra": "CRE1"}],
        "esqueceu de bater", quem="MARCELO")
    assert [b["hora"] for b in feito["enviadas"]] == ["07:00", "17:00"]
    assert feito["falhou"] is None
    p = mandados[0]
    assert p["type_data"] == "CAD_EDT_PONTO" and p["acao"] == "C"
    assert p["cpf_funcionario"] == "99713349334"
    assert p["cpf_responsavel"] == "11122233396"
    assert p["nome_responsavel"] == "MARCELO"
    assert p["dt_ponto_new"] == "2026-09-03 07:00"
    assert p["local"] == "CRE1"
    assert p["justificativa"] == "esqueceu de bater"


def test_PARA_na_primeira_que_falha_e_diz_o_que_entrou(configurado, monkeypatch):
    from app.apps.analisesps import ponto_edicao
    mandados = _dublar(monkeypatch, [(True, "ok"), (False, '{"status": false}')])
    feito = ponto_edicao.incluir_batidas(
        "99713349334", "", "2026-09-03",
        [{"hora": "07:00", "obra": "A"}, {"hora": "11:00", "obra": "A"},
         {"hora": "13:00", "obra": "A"}], "justificativa ok")
    assert len(mandados) == 2, "não podia ter mandado a terceira"
    assert [b["hora"] for b in feito["enviadas"]] == ["07:00"]
    assert feito["falhou"]["hora"] == "11:00"


def test_tempo_esgotado_DIZ_QUE_NAO_SABE_se_gravou(configurado, monkeypatch):
    """Gravar de novo depois de um tempo esgotado pode duplicar a batida."""
    from app.apps.analisesps import ponto_edicao
    _dublar(monkeypatch, [(None, "o Mobponto não respondeu a tempo. NÃO SEI SE GRAVOU")])
    feito = ponto_edicao.incluir_batidas(
        "99713349334", "", "2026-09-03", [{"hora": "07:00", "obra": "A"}],
        "justificativa ok")
    assert feito["falhou"]["talvez_gravou"] is True
    assert "NÃO SEI SE GRAVOU" in feito["falhou"]["motivo"]


@pytest.mark.parametrize("batidas,justificativa,trecho", [
    ([{"hora": "7h", "obra": "A"}], "justificativa", "formato"),
    ([{"hora": "07:00", "obra": ""}], "justificativa", "sem obra"),
    ([], "justificativa", "nenhuma batida"),
    ([{"hora": "07:00", "obra": "A"}, {"hora": "07:00", "obra": "B"}], "justificativa", "mesma hora"),
    ([{"hora": "07:00", "obra": "A"}], "", "justificativa"),
    ([{"hora": f"0{i}:00", "obra": "A"} for i in range(5)], "justificativa", "no máximo"),
])
def test_recusa_ANTES_de_mandar_a_primeira(configurado, monkeypatch, batidas,
                                            justificativa, trecho):
    from app.apps.analisesps import ponto_edicao
    mandados = _dublar(monkeypatch, [])
    with pytest.raises(ponto_edicao.ErroDaEdicao, match=trecho):
        ponto_edicao.incluir_batidas("99713349334", "", "2026-09-03", batidas,
                                     justificativa)
    assert mandados == []


def test_sem_o_responsavel_NAO_GRAVA_e_diz_o_que_criar(monkeypatch):
    from app.apps.analisesps import ponto_edicao
    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic teste")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    monkeypatch.delenv("MOBPONTO_RESPONSAVEL_CPF", raising=False)
    monkeypatch.delenv("MOBPONTO_RESPONSAVEL_NOME", raising=False)
    mandados = _dublar(monkeypatch, [])
    with pytest.raises(ponto_edicao.ErroDaEdicao, match="MOBPONTO_RESPONSAVEL_CPF"):
        ponto_edicao.incluir_batidas("99713349334", "", "2026-09-03",
                                     [{"hora": "07:00", "obra": "A"}], "justificativa")
    assert mandados == []


def test_a_rota_grava_e_TRAZ_O_PONTO_DE_NOVO(app, configurado, monkeypatch):
    from app.apps.analisesps import web
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    mandados = _dublar(monkeypatch, [])
    trazidos = []
    monkeypatch.setattr(web, "_trazer_o_ponto_da_pessoa",
                        lambda folha, cpf, nome: trazidos.append(cpf) or {"ok": True})
    r = _como_mestre(app).post("/analisesps/api/folha/ponto/batida", json={
        "folha_id": 1, "cpf": "99713349334", "nome": "GERLANIO",
        "data": "2026-09-12", "batidas": [{"hora": "07:00", "obra": "XYZ9"}],
        "justificativa": "bateu no lugar errado"})
    d = r.get_json()
    assert r.status_code == 200 and d["ok"] and d["atualizando"]
    assert len(mandados) == 1 and trazidos == ["99713349334"]


def test_a_rota_RECUSA_quem_nao_esta_na_folha_e_dia_de_outro_mes(app, configurado, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    mandados = _dublar(monkeypatch, [])
    cliente = _como_mestre(app)
    fora = cliente.post("/analisesps/api/folha/ponto/batida", json={
        "folha_id": 1, "cpf": "52998224725", "data": "2026-09-12",
        "batidas": [{"hora": "07:00", "obra": "A"}], "justificativa": "teste ok"})
    assert fora.status_code == 404
    outro_mes = cliente.post("/analisesps/api/folha/ponto/batida", json={
        "folha_id": 1, "cpf": "99713349334", "data": "2026-08-12",
        "batidas": [{"hora": "07:00", "obra": "A"}], "justificativa": "teste ok"})
    assert outro_mes.status_code == 400
    assert mandados == []


def test_gravar_no_mobponto_e_SO_DO_MESTRE(app):
    from app.apps.analisesps import auth
    assert auth.e_so_do_mestre("analisesps.folha_ponto_incluir_batida") is True
    r = como(app, SENHA_CONSULTA).post("/analisesps/api/folha/ponto/batida", json={})
    assert r.status_code in (302, 403, 404)


def test_o_analitico_oferece_CORRIGIR_por_dia(app, configurado, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert 'class="link-btn corrigir-dia"' in html
    assert 'data-data="2026-09-01"' in html
    assert 'id="corrigir-ponto"' in html and "Gravar no Mobponto" in html
    assert "não se desfaz" in html


def test_sem_configuracao_o_quadro_DIZ_O_QUE_FALTA(app, monkeypatch):
    monkeypatch.delenv("MOBPONTO_RESPONSAVEL_CPF", raising=False)
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert "MOBPONTO_RESPONSAVEL_CPF" in html
    assert "Gravar no Mobponto" not in html


def test_a_pagina_de_imprimir_NAO_tem_o_corrigir(app, configurado, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334").get_data(as_text=True)
    assert "corrigir-dia" not in html
