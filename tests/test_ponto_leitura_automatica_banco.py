# -*- coding: utf-8 -*-
"""Ponto — o atestado e a declaração lidos sozinhos pela IA (04/11/2026, o dono: "todos os documentos
encaminhados, tipo atestado, declarações, já fossem lidos automaticamente pela IA (…) pra não precisar fazer
manual"). A IA é dublada: o que se testa é QUEM é lido, o que se grava e o que acontece quando falha."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401
from tests.test_ponto_tipos_ausencia_banco import _png

pytestmark = pytest.mark.banco


def _atestado(dp, pessoa, dia, aprovar=False, documento=True):
    corpo = {"tipo": "ATESTADO", "colaborador_id": pessoa, "data_inicio": dia.isoformat(), "dias": 1,
             "descricao": "consulta", "aprovar_ja": aprovar}
    if documento:
        corpo["documento_base64"] = _png()
    r = dp.post("/erp/api/ponto/afastamentos", json=corpo)
    assert r.status_code == 201, r.get_json()
    return r.get_json()["ocorrencia"]["id"]


@pytest.fixture
def ia(monkeypatch):
    from app.apps.ponto.core import leitura_atestado as la
    chamadas = []

    def falsa(dados, mime, oid, operacao="ponto_atestado"):
        chamadas.append((oid, operacao))
        if getattr(falsa, "quebrar", False):
            raise RuntimeError("serviço fora do ar")
        return {"tipo": "ATESTADO", "dias_afastamento": 1, "medico": "Dra. Ana", "crm": "1234", "legivel": True}
    monkeypatch.setattr(la, "_ler_bytes", falsa)
    monkeypatch.setattr(la, "_teto_da_empresa_estourou", lambda: False)
    la._falhas.clear()
    falsa.chamadas = chamadas
    yield falsa
    la._falhas.clear()


def test_le_sozinho_o_atestado_que_espera_decisao_e_so_ele(app, mundo, ia):
    from app.apps.ponto import db
    from app.apps.ponto.core import leitura_atestado as la
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    esperando = _atestado(dp, mundo["joao"], seg)
    aprovado = _atestado(dp, mundo["maria"], seg, aprovar=True)        # já decidido: não gasta IA
    sem_papel = _atestado(dp, mundo["maria"], seg + __import__("datetime").timedelta(days=2), documento=False)
    feito = la.ler_pendentes()
    assert feito == {"lidos": 1, "falhas": 0} and [c[0] for c in ia.chamadas] == [esperando]
    assert ia.chamadas[0][1] == "ponto_atestado_automatico"           # o consumo aparece separado no painel de IA
    with db.conexao() as conn:
        lidas = {r["id"]: r["leitura_ia"] for r in db.todos(conn, "SELECT id, leitura_ia FROM ponto.ocorrencias "
                                                                    "WHERE id = ANY(:i)", i=[esperando, aprovado, sem_papel])}
    assert lidas[esperando]["medico"] == "Dra. Ana" and lidas[esperando]["automatica"] is True
    assert lidas[aprovado] is None and lidas[sem_papel] is None
    assert la.ler_pendentes() == {"lidos": 0, "falhas": 0}             # lido não se lê de novo
    # nas Validações, o DP já vê a leitura e o confronto, sem apertar nada
    lista = dp.get("/erp/api/ponto/validacoes").get_json()["itens"]
    item = next(i for i in lista if i.get("categoria") == "PEDIDO" and i.get("id") == esperando)
    assert item["leitura_ia"]["medico"] == "Dra. Ana" and item["confronto"] is not None


def test_a_leitura_que_falha_tenta_de_novo_uma_vez_e_depois_fica_para_o_botao(app, mundo, ia):
    from app.apps.ponto.core import leitura_atestado as la
    dp = como(app, mundo["dp"])
    _atestado(dp, mundo["joao"], _segunda_passada())
    ia.quebrar = True
    assert la.ler_pendentes()["falhas"] == 1
    assert la.ler_pendentes()["falhas"] == 1
    assert la.ler_pendentes() == {"lidos": 0, "falhas": 0}             # desistiu: o "ler com IA" continua lá
    assert len(ia.chamadas) == la.TENTATIVAS


def test_teto_de_ia_da_empresa_estourado_nao_le(app, mundo, ia, monkeypatch):
    from app.apps.ponto.core import leitura_atestado as la
    monkeypatch.setattr(la, "_teto_da_empresa_estourou", lambda: True)
    _atestado(como(app, mundo["dp"]), mundo["joao"], _segunda_passada())
    assert la.ler_pendentes() == {"lidos": 0, "falhas": 0} and ia.chamadas == []
