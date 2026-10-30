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


def test_quem_nao_bate_ponto_mas_foi_liberado_entra_no_aplicativo(app, mundo):
    """31/10/2026, o dono: "pode ser que eu precise de uma pessoa do cadastro que não bata ponto, mas que eu libere
    pra fazer algo relacionado ao ponto". Sem liberação, recusada; com obra liberada, entra — e a batida continua
    recusada. Na tela de Pessoas, o filtro "quem não bate" mostra ela; o padrão, não."""
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso, cadastros, papeis
    from app.apps.ponto.erros import Recusada
    from tests.test_ponto_gestao_banco import como
    textos = []
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, CPF_JOAO, enviar=lambda tel, txt: textos.append(txt))
    codigo = re.search(r"(\d{6})", textos[-1]).group(1)
    with db.conexao() as conn:
        acesso.criar_pin(conn, CPF_JOAO, codigo, "482915")
        cadastros.marcar_bate_ponto(conn, mundo["joao"], False, "só ajuda o DP", "teste")
    with pytest.raises(Recusada, match="não bate ponto"):
        with db.conexao() as conn:
            acesso.entrar(conn, CPF_JOAO, "482915")
    dp = como(app, mundo["dp"])
    assert dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/forma-de-bater",
                   json={"liberado_obras": ["PG-B"]}).status_code == 200
    with db.conexao() as conn:
        p = acesso.entrar(conn, CPF_JOAO, "482915")
        assert p["id"] == mundo["joao"] and papeis.entra_no_app(conn, p) and not p["ativo_no_ponto"]
    ids = lambda bate: {x["id"] for x in dp.get(f"/erp/api/ponto/pessoas?bate={bate}").get_json()["pessoas"]}  # noqa: E731
    assert mundo["joao"] in ids("NAO") and mundo["joao"] not in ids("SIM") and mundo["joao"] in ids("TODOS")
    assert mundo["maria"] in ids("SIM") and mundo["maria"] not in ids("NAO")
