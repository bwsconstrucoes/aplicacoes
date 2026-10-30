# -*- coding: utf-8 -*-
"""A medição da lentidão (31/10/2026, app/lentidao.py): o pedido lento e as linhas tomadas vão para o log."""
from __future__ import annotations

import logging
import threading
import time

from flask import Flask


def _app():
    from app import lentidao
    a = Flask(__name__)
    lentidao.ligar(a)

    @a.route("/rapido")
    def rapido():
        return "ok"

    @a.route("/devagar")
    def devagar():
        time.sleep(0.25)
        return "ok"
    return a


def test_o_pedido_lento_vai_para_o_log(caplog, monkeypatch):
    from app import lentidao
    monkeypatch.setattr(lentidao, "LIMITE_S", 0.2)
    c = _app().test_client()
    with caplog.at_level(logging.WARNING, logger="lentidao"):
        assert c.get("/rapido").status_code == 200
        assert c.get("/devagar").status_code == 200
    linhas = [r.getMessage() for r in caplog.records]
    assert any("LENTA: GET /devagar" in x for x in linhas) and not any("/rapido" in x for x in linhas)
    assert lentidao.agora_em_andamento() == []           # o que terminou sai da conta


def test_as_linhas_tomadas_vao_para_o_log(caplog, monkeypatch):
    from app import lentidao
    monkeypatch.setattr(lentidao, "LINHAS", 2)
    monkeypatch.setattr(lentidao, "_ultimo_aviso", [0.0])
    a = _app()
    with caplog.at_level(logging.WARNING, logger="lentidao"):
        t = threading.Thread(target=lambda: a.test_client().get("/devagar"))
        t.start()
        time.sleep(0.05)
        a.test_client().get("/devagar")
        t.join()
    assert any("AS 2 LINHAS OCUPADAS" in r.getMessage() and "GET /devagar" in r.getMessage() for r in caplog.records)
