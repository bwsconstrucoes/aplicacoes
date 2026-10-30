# -*- coding: utf-8 -*-
"""Ponto — o PIN certo de quem está fora do ponto (31/10/2026, o dono: "tá muito comum o pessoal dizer que o pin
não funciona mais"): antes, "CPF ou PIN incorreto" mesmo com o PIN certo."""
from __future__ import annotations

import re

import pytest

from tests.test_ponto_gestao_banco import CPF_JOAO, _schema_ponto2, app, mundo  # noqa: F401

pytestmark = pytest.mark.banco


def test_o_pin_certo_de_quem_esta_fora_do_ponto_diz_o_motivo(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso, cadastros
    from app.apps.ponto.erros import NaoAutenticado, Recusada
    textos = []
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda tel, txt: textos.append(txt))
    codigo = re.search(r"(\d{6})", textos[-1]).group(1)
    with db.conexao() as conn:
        acesso.criar_pin(conn, CPF_JOAO, codigo, "482915")
    with db.conexao() as conn:
        assert acesso.entrar(conn, CPF_JOAO, "482915")["id"] == mundo["joao"]
    with db.conexao() as conn:
        cadastros.marcar_bate_ponto(conn, mundo["joao"], False, "teste do PIN", "teste")
    with pytest.raises(Recusada, match="não bate ponto"):
        with db.conexao() as conn:
            acesso.entrar(conn, CPF_JOAO, "482915")
    # PIN errado continua com a frase de sempre (não revela nada a quem chuta) — e conta para o DP ver
    with pytest.raises(NaoAutenticado, match="CPF ou PIN incorreto"):
        with db.conexao() as conn:
            acesso.entrar(conn, CPF_JOAO, "111999")
    with db.conexao() as conn:
        assert acesso.diagnostico(conn, mundo["joao"])["pin_falhas"] == 1
    # CPF que não existe: a frase de sempre
    with pytest.raises(NaoAutenticado, match="CPF ou PIN incorreto"):
        with db.conexao() as conn:
            acesso.entrar(conn, "11122233396", "482915")
