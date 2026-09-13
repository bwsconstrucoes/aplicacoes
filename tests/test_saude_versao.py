"""Qual versão do ERP está no ar, agora.

Nasceu de uma volta perdida em 10/09/2026. O dono disse que um botão novo *"não
aparece pra mim"*, e não havia como saber se ele estava vendo a versão nova ou
a de antes: o Render leva alguns minutos para trocar o serviço e **nada na tela
dizia isso**. Meia hora de investigação por uma pergunta que o próprio sistema
podia responder.

O carimbo é bobo de propósito — a hora em que ESTE processo começou. Como toda
publicação reinicia o serviço, "no ar desde" é, na prática, "quando a versão
atual subiu".
"""
from __future__ import annotations

from app.apps.erp.core.comum import saude


def test_a_versao_diz_desde_quando_esta_no_ar():
    v = saude.versao()

    assert v["no_ar_desde"], "sem isso a pergunta 'já subiu?' fica sem resposta"
    assert v["minutos_no_ar"] >= 0


def test_sem_as_variaveis_do_render_nao_inventa_commit(monkeypatch):
    """No PC não há commit publicado. Dizer que não sabe é melhor que chutar —
    versão errada na tela é pior que versão nenhuma."""
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.delenv("RENDER_GIT_BRANCH", raising=False)

    v = saude.versao()

    assert v["commit"] == "" and v["ramo"] == ""


def test_o_commit_sai_encurtado_para_caber_na_tela(monkeypatch):
    monkeypatch.setenv("RENDER_GIT_COMMIT",
                       "a100451fd55bd9c78832091257abcdef01234567")

    v = saude.versao()

    assert v["commit"] == "a100451"
    assert v["commit_inteiro"].startswith("a100451fd")


def test_a_versao_entra_no_panorama_que_a_tela_le():
    """A tela lê `panorama`; versão fora dele não chegaria a lugar nenhum."""
    from conftest import SessaoFalsa

    p = saude.panorama(SessaoFalsa())

    assert "versao" in p and p["versao"]["no_ar_desde"]
