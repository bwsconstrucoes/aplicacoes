# -*- coding: utf-8 -*-
"""Ponto — o espelho do ponto no aplicativo (28/10/2026), com Postgres de verdade: o meu,
com o CPF só pelo final na tela e inteiro na folha para imprimir; e o de uma pessoa da
obra, com o MESMO alcance da consulta (fora dele, "não encontrado", também na folha)."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _entrar_no_app, _schema_ponto2,  # noqa: F401
                                           app, como, mundo)
from tests.test_ponto_papeis_banco import CPF_CARLOS, LONGE, NA_OBRA_A, _bater, _papeis, carlos  # noqa: F401

pytestmark = pytest.mark.banco


def _cpf(c: str) -> str:
    return f"{c[0:3]}.{c[3:6]}.{c[6:9]}-{c[9:11]}"


def test_o_meu_espelho_com_o_cpf_pelo_final_e_a_folha_com_ele_inteiro(app, mundo, monkeypatch):
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    r = c.get("/ponto/app/api/mes")
    d = r.get_json()
    assert d["pessoa"]["nome"] == "João Obra A" and d["pessoa"]["cpf_final"] == CPF_JOAO[-3:]
    assert d["pessoa"]["obra"] == "PG-A" and "funcao" in d["pessoa"]
    assert CPF_JOAO not in r.get_data(as_text=True)              # na tela, só o final
    folha = c.get("/ponto/app/espelho/imprimir")
    html = folha.get_data(as_text=True)
    assert folha.status_code == 200 and folha.mimetype == "text/html"
    assert "João Obra A" in html and _cpf(CPF_JOAO) in html     # na folha, o CPF inteiro
    assert "‹ Voltar" in html and "window.print(); }, 300" not in html
    assert "window.print(); }, 300" in c.get("/ponto/app/espelho/imprimir?imprimir=1").get_data(as_text=True)
    # sem entrar, nada
    assert app.test_client().get("/ponto/app/espelho/imprimir").status_code == 401


def test_o_espelho_de_outra_pessoa_segue_o_alcance_da_consulta(app, mundo, carlos, monkeypatch):
    _papeis(app, mundo, carlos, administrativo_obra=True)
    _bater(CPF_MARIA, "PG-A", 30)                        # a Maria é da obra B, mas bateu na A
    c = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    url = f"/ponto/app/equipe/{mundo['maria']}/espelho/imprimir"
    folha = c.get(f"{url}?{NA_OBRA_A}")
    assert folha.status_code == 200 and "Maria Obra B" in folha.get_data(as_text=True)
    assert _cpf(CPF_MARIA) in folha.get_data(as_text=True)
    # longe da obra: fora do alcance — "não encontrada", numa página (não um JSON)
    fora = c.get(f"{url}?{LONGE}")
    assert fora.status_code == 404 and fora.mimetype == "text/html"
    assert "Não foi possível abrir o espelho" in fora.get_data(as_text=True)
    assert "Maria Obra B" not in fora.get_data(as_text=True)
    # quem não é responsável nem administrativo não abre o espelho de ninguém
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    assert joao.get(f"{url}?{NA_OBRA_A}").status_code == 404
    assert CPF_CARLOS not in folha.get_data(as_text=True)


def test_a_folha_de_um_periodo_escolhido(app, mundo, monkeypatch):
    """29/10/2026: além do mês, um período de até 62 dias (o corte da folha, de 21 a 20)."""
    c = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    folha = c.get("/ponto/app/espelho/imprimir?de=2026-09-21&ate=2026-10-20")
    html = folha.get_data(as_text=True)
    assert folha.status_code == 200 and "21/09/2026 a 20/10/2026" in html and "Totais do período" in html
    longe = c.get("/ponto/app/espelho/imprimir?de=2026-07-01&ate=2026-10-20")
    assert longe.status_code == 400 and "62 dias" in longe.get_data(as_text=True)
    invertido = c.get("/ponto/app/espelho/imprimir?de=2026-10-20&ate=2026-10-01")
    assert invertido.status_code == 400 and "antes do primeiro" in invertido.get_data(as_text=True)


def test_no_ponto_da_obra_a_folha_de_outra_pessoa_abre_com_o_bilhete(app, mundo, monkeypatch):
    """05/11/2026 (Weverton, obra ESCADELIA): no ponto da obra, "Baixar o espelho" de um colaborador dava "pessoa não
    encontrada" — a página aberta para imprimir não leva a identificação do aparelho, então o sistema conferia o
    alcance PESSOAL de quem entrou, e não o do aparelho. O bilhete, pedido pela chamada do aplicativo, resolve."""
    from app.apps.ponto.core import qr
    from tests.test_ponto_regras_0610_banco import _aparelho, _id_do_aparelho
    _entrar_no_app(app, CPF_MARIA, monkeypatch)                                 # cria o PIN 481927
    dp = como(app, mundo["dp"])
    t, h = _aparelho(app, "ponto-da-obra-a-espelho-bilhete-01")
    ap = _id_do_aparelho(dp, "ponto-da-obra-a-espelho-bilhete-01")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "cpf": CPF_MARIA}).status_code == 200
    assert t.post("/ponto/app/api/entrar", json={"cpf": CPF_MARIA, "pin": "481927"}, headers=h).status_code == 200
    assert t.post("/ponto/app/api/equipe/cpf", json={"cpf": CPF_JOAO}, headers=h).status_code == 200
    joao, periodo = mundo["joao"], "competencia=2026-10"
    url = f"/ponto/app/equipe/{joao}/espelho/imprimir?{periodo}"
    # o erro do Weverton: a página, sem a identificação do aparelho, não acha a pessoa
    assert t.get(url).status_code == 404
    r = t.post(f"/ponto/app/api/equipe/{joao}/espelho/bilhete?{periodo}", json={}, headers=h)
    assert r.status_code == 200, r.get_json()
    bilhete = r.get_json()["bilhete"]
    folha = t.get(f"{url}&bilhete={bilhete}")
    assert folha.status_code == 200 and "João Obra A" in folha.get_data(as_text=True)
    # o bilhete vale só para aquela pessoa, aquele período e por pouco tempo
    assert t.get(f"/ponto/app/equipe/{mundo['maria']}/espelho/imprimir?{periodo}&bilhete={bilhete}").status_code == 404
    assert t.get(f"/ponto/app/equipe/{joao}/espelho/imprimir?competencia=2026-09&bilhete={bilhete}").status_code == 404
    vencido = qr.emitir_impressao(joao, mundo["maria"], "2026-10||", agora=0)
    assert t.get(f"{url}&bilhete={vencido}").status_code == 404
    # sem ninguém logado, o bilhete sozinho não abre nada
    assert app.test_client().get(f"{url}&bilhete={bilhete}").status_code == 401
    # e quem não alcança a pessoa não ganha bilhete: a Maria (obra B) não foi consultada pelo CPF
    assert t.post(f"/ponto/app/api/equipe/{mundo['maria']}/espelho/bilhete?{periodo}", json={},
                  headers=h).status_code == 404
