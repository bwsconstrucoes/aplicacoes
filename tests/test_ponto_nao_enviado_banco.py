# -*- coding: utf-8 -*-
"""Ponto — a mensagem que o ponto não mandou fica na tela Mensagens (31/10/2026, o dono: "preciso que essas
mensagens mesmo que bloqueadas apareçam no mensageria, pq assim tenho como resolver a pane e visualizar a
tentativa")."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import CPF_JOAO, _schema_ponto2, app, mundo  # noqa: F401

pytestmark = pytest.mark.banco


@pytest.fixture
def anotadas(monkeypatch):
    from app.apps import notificador
    lista = []
    monkeypatch.setattr(notificador, "_registrar", lambda m, **campos: lista.append(campos))
    monkeypatch.setattr(notificador, "_mensageria", lambda: object())
    return lista


def test_o_codigo_barrado_fica_anotado_com_o_motivo(app, mundo, anotadas):
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso
    with db.conexao() as conn:
        # CPF que não está no cadastro: quem pediu recebe a resposta de sempre; a tela Mensagens, o motivo
        r = acesso.pedir_codigo(conn, "11122233396", enviar=lambda *a: None)
        assert r["mensagem"] == acesso.RESPOSTA_DO_CODIGO
    assert anotadas[-1]["status"] == "BARRADO" and "CPF não encontrado" in anotadas[-1]["detalhe"]
    assert anotadas[-1]["tipo"] == "ponto.codigo_acesso" and anotadas[-1]["cpf"] == "11122233396"
    # o teto da hora: o quarto pedido não sai, e fica anotado
    enviados = []
    with db.conexao() as conn:
        for _ in range(4):
            acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda tel, txt: enviados.append(tel))
    assert len(enviados) == 3
    assert anotadas[-1]["status"] == "BARRADO" and "códigos na última hora" in anotadas[-1]["detalhe"]
    # sem celular no cadastro: "Sem destinatário"
    with db.conexao() as conn:
        conn.execute(text("DELETE FROM ponto.codigos_acesso"))
        conn.execute(text("UPDATE colaboradores SET telefone = NULL WHERE id = :c"), {"c": mundo["joao"]})
    from app.apps.ponto.core import registro
    registro.esquecer()
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda *a: None)
    assert anotadas[-1]["status"] == "SEM_DESTINO" and "sem celular" in anotadas[-1]["detalhe"]


def test_o_qr_sem_telefone_fica_anotado(app, mundo, anotadas):
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import envios, registro
    with db.conexao() as conn:
        conn.execute(text("UPDATE colaboradores SET telefone = NULL WHERE id = :c"), {"c": mundo["joao"]})
    registro.esquecer()
    with db.conexao() as conn:
        assert envios.pedir_qr(conn, mundo["joao"])["enfileirado"] is False
    assert anotadas[-1]["tipo"] == "ponto.qr" and anotadas[-1]["status"] == "SEM_DESTINO"
    assert "sem celular" in anotadas[-1]["detalhe"] and anotadas[-1]["cpf"] == CPF_JOAO
