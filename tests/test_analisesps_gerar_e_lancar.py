"""Gerar e lançar no Pipefy numa tacada só (11/11/2026).

O dono: *"primeiro a gente gera o arquivo e depois lança no Pipefy. Eu queria poder
ter a opção de gerar e lançar ao mesmo tempo (…) poder somente gerar e ficar lá
guardado, ou então gerar tudo uma vez."*
"""
import pytest

from tests.test_analisesps_telas import _como_mestre, app  # noqa: F401


@pytest.fixture
def gerado(monkeypatch):
    from app.apps.analisesps import folha_cards as fcd, folha_pagamento as fpg
    lancados = []
    monkeypatch.setattr(fpg, "gerar_direto", lambda origem, dados, destino, **k: {
        "competencia": "10/2026",
        "arquivos": [{"id": 77, "destino": fpg.ANALISE}, {"id": 78, "destino": "beevale"}]})

    def lancar(analise, quem="", contas=None):
        lancados.append((analise, contas))
        if lancados and getattr(lancar, "trava", False):
            raise fcd.ErroDosCards("nenhum card foi criado: a obra X não tem Código Omie.")
        return {"ok": True, "sps": [{"id": "1"}, {"id": "2"}], "avisos": []}
    monkeypatch.setattr(fcd, "lancar", lancar)
    return lancados, lancar


PEDIDO = {"origem": "diaria", "ano": 2026, "mes": 10, "periodo": "quinzena", "destino": "beevale"}


def test_gerar_e_lancar_lanca_todas_as_contas_da_geracao(app, gerado):
    lancados, _ = gerado
    d = _como_mestre(app).post("/analisesps/api/folha/gerar-direto",
                               json={**PEDIDO, "lancar": True}).get_json()
    assert d["ok"] and d["lancamento"] == {"ok": True, "sps": 2, "avisos": []}
    assert lancados == [(77, None)], "a análise da geração, todas as contas"


def test_lancamento_travado_deixa_os_arquivos_gerados_e_diz_por_que(app, gerado):
    lancados, lancar = gerado
    lancar.trava = True
    d = _como_mestre(app).post("/analisesps/api/folha/gerar-direto",
                               json={**PEDIDO, "lancar": True}).get_json()
    assert d["ok"], "a geração vale mesmo sem o lançamento"
    assert d["lancamento"]["ok"] is False and "Código Omie" in d["lancamento"]["erro"]
    assert "rodada=77" in d["destino_da_tela"]


def test_so_gerar_nao_lanca(app, gerado):
    lancados, _ = gerado
    d = _como_mestre(app).post("/analisesps/api/folha/gerar-direto", json=PEDIDO).get_json()
    assert d["ok"] and d["lancamento"] is None and lancados == []


def test_a_janela_tem_os_dois_botoes():
    import pathlib
    html = (pathlib.Path(__file__).resolve().parents[1] / "app" / "apps" / "analisesps"
            / "templates" / "_gerar_direto.html").read_text(encoding="utf-8")
    assert 'id="gd-confirmar"' in html and 'id="gd-confirmar-lancar"' in html
