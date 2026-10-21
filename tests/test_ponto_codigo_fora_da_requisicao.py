# -*- coding: utf-8 -*-
"""Ponto — o WhatsApp do código de acesso sai FORA da requisição (21/10/2026,
incidente: na hora da saída o Render inteiro parou com CPU em 20 % e memória em
30 %). O envio esperava o Z-API até 60 s × 3 com a requisição e a conexão presas;
quatro pedidos de código tomavam as 4 linhas do serviço."""
from __future__ import annotations

import threading
import time

import pytest

from tests.test_ponto_gestao_banco import CPF_MARIA, _schema_ponto2, app, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_pedir_o_codigo_responde_na_hora_mesmo_com_o_whatsapp_travado(app, mundo, banco, monkeypatch):
    from sqlalchemy import text
    import app.apps.notificador as notificador
    solta, chamado = threading.Event(), threading.Event()

    def whatsapp_travado(**kw):
        chamado.set()
        solta.wait(10)                      # o Z-API que não responde
        return {"ok": False}
    monkeypatch.setattr(notificador, "notificar", whatsapp_travado)
    with banco.connect() as conn:
        antes = conn.execute(text("SELECT count(*) FROM ponto.codigos_acesso")).scalar()
    c = app.test_client()
    comeco = time.time()
    r = c.post("/ponto/app/api/codigo", json={"cpf": CPF_MARIA})
    try:
        assert r.status_code == 200 and time.time() - comeco < 3       # não esperou o WhatsApp
        assert chamado.wait(5)                                         # mas o envio foi feito
        with banco.connect() as conn:                                  # e o código já está gravado
            assert conn.execute(text("SELECT count(*) FROM ponto.codigos_acesso")).scalar() == antes + 1
    finally:
        solta.set()
