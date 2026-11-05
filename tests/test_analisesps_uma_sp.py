"""A rota e o botão do "Atualizar da planilha" da ficha (03/11/2026)."""

from tests.test_analisesps_telas import app, como, SENHA_OPERADOR  # noqa: F401


def _rota(app, monkeypatch, resposta):
    from app.apps.analisesps import sincronizacao
    chamadas = []

    def falsa(sp_id):
        chamadas.append(sp_id)
        if isinstance(resposta, Exception):
            raise resposta
        return resposta

    monkeypatch.setattr(sincronizacao, "atualizar_uma_sp", falsa)
    r = como(app, SENHA_OPERADOR).post("/analisesps/api/sp/123/da-planilha")
    return r, chamadas


def test_a_rota_devolve_o_que_mudou(app, monkeypatch):
    r, chamadas = _rota(app, monkeypatch,
                        {"achada": True, "mudou": ["Status Pgt"], "linha": 4, "devolvidas": 0})
    assert r.status_code == 200 and r.get_json()["mudou"] == ["Status Pgt"]
    assert chamadas == ["123"]


def test_sp_fora_da_planilha_vira_aviso_e_nao_erro_mudo(app, monkeypatch):
    r, _ = _rota(app, monkeypatch, {"achada": False, "mudou": [], "devolvidas": 0})
    assert r.status_code == 404
    assert "não está na planilha" in r.get_json()["erro"]


def test_falha_do_google_diz_o_que_houve(app, monkeypatch):
    r, _ = _rota(app, monkeypatch, RuntimeError("cota excedida"))
    assert r.status_code == 500 and "cota excedida" in r.get_json()["erro"]


def test_o_botao_aparece_na_ficha_e_nao_se_confunde_com_o_do_omie(app, monkeypatch):
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "uma", lambda sp_id: {"id": sp_id, "credor": "X"})
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/sp/123?modal=1").get_data(as_text=True)
    assert "Atualizar da planilha" in html
    assert "/analisesps/api/sp/123/da-planilha" in html
