# -*- coding: utf-8 -*-
"""Ponto — o Registro de Colaboradores como base de pessoas (pedido do dono,
04/10/2026). A cópia que a Análise de SPs guarda (`analisesps.colaborador`)
manda no nome, celular, cargo, obra e situação; quem está só no ERP bate, mas
para conferência; quem falta no ERP entra pelo botão. Postgres de verdade."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _schema_ponto2, app,  # noqa: F401
                                           como, mundo)

pytestmark = pytest.mark.banco


def _cpf(base9: str) -> str:
    d = [int(x) for x in base9]
    for n in (9, 10):
        s = sum(v * (n + 1 - i) for i, v in enumerate(d[:n]))
        r = (s * 10) % 11
        d.append(0 if r == 10 else r)
    return "".join(map(str, d))


CPF_NOVO = _cpf("123456789")          # ativo no Registro, não existe no ERP
CPF_SO_ERP = _cpf("987654321")        # existe no ERP, não está no Registro
CPF_RUIM = "12345678900"              # dígito errado: não entra no ERP


@pytest.fixture
def registro(banco_analisesps, mundo, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import parametros, registro as reg
    with banco.connect() as conn:
        for cpf, nome, celular, cargo, fase, obra in (
                (CPF_JOAO, "JOÃO DO REGISTRO", "(85) 91111-2222", "Pedreiro", "Colaboradores Ativos", "PG-B"),
                (CPF_MARIA, "MARIA DO REGISTRO", "85933334444", "Servente", "Colaboradores Desligados", "PG-B"),
                (CPF_NOVO, "NOVO DO REGISTRO", "85955556666", "Ajudante", "Colaboradores Ativos", "PG-A"),
                (CPF_RUIM, "CPF ERRADO", "", "Ajudante", "Colaboradores Ativos", "PG-A")):
            conn.execute(text("""INSERT INTO analisesps.colaborador (cpf, nome, celular, cargo, fase, obra_codigo)
                                 VALUES (:c, :n, :t, :g, :f, :o)"""),
                         {"c": cpf, "n": nome, "t": celular, "g": cargo, "f": fase, "o": obra})
        conn.execute(text("INSERT INTO colaboradores (nome, cpf, obra_id) VALUES ('SÓ NO ERP', :c, :o)"),
                     {"c": CPF_SO_ERP, "o": mundo["obra_a"]})
        conn.commit()
    with db.conexao() as conn:
        parametros.gravar(conn, reg.PARAMETRO_FONTE, reg.FONTE_REGISTRO, "teste")
    reg.esquecer()
    yield
    reg.esquecer()
    with banco.connect() as conn:
        ids = "SELECT id FROM colaboradores WHERE cpf IN (:a, :b)"
        for tabela in ("ponto.marcacoes", "ponto.colaborador_config"):
            conn.execute(text(f"DELETE FROM {tabela} WHERE colaborador_id IN ({ids})"), {"a": CPF_NOVO, "b": CPF_SO_ERP})
        conn.execute(text("DELETE FROM colaboradores WHERE cpf IN (:a, :b)"), {"a": CPF_NOVO, "b": CPF_SO_ERP})
        conn.commit()


def test_o_registro_manda_no_nome_celular_cargo_obra_e_situacao(app, mundo, registro):
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros, marcacoes
    from app.apps.ponto.erros import Recusada
    with db.conexao() as conn:
        j = cadastros.colaborador_por_cpf(conn, CPF_JOAO)
        assert (j["nome"], j["telefone"], j["funcao"], j["obra_codigo"], j["situacao"], j["no_registro"]) == \
            ("JOÃO DO REGISTRO", "(85) 91111-2222", "Pedreiro", "PG-B", "ATIVO", True)
        assert mundo["obra_b"] in cadastros.obras_da_pessoa(conn, mundo["joao"])
        assert cadastros.colaborador_por_cpf(conn, CPF_MARIA)["situacao"] == "DESLIGADO"
        assert cadastros.colaborador_por_cpf(conn, CPF_SO_ERP)["situacao"] == "FORA_DO_REGISTRO"
        nomes = [p["nome"] for p in cadastros.listar_colaboradores(conn, so_ativos=True)]
        assert "JOÃO DO REGISTRO" in nomes and "MARIA DO REGISTRO" not in nomes
    with pytest.raises(Recusada, match="desligada"):
        with db.conexao() as conn:
            marcacoes.registrar(conn, cpf=CPF_MARIA, obra="PG-B", origem="IDFACE", via_chave=True)
    with db.conexao() as conn:
        m, _ = marcacoes.registrar(conn, cpf=CPF_SO_ERP, obra="PG-A", origem="IDFACE", via_chave=True)
    assert m["status"] == "EM_ANALISE" and "fora do Registro de Colaboradores" in m["motivo_analise"]


def test_configuracao_mostra_a_base_cadastra_quem_falta_e_volta_para_o_erp(app, mundo, registro):
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    r = dp.get("/erp/api/ponto/registro").get_json()
    assert r["disponivel"] is True and r["usando"] is True and r["fonte"] == "REGISTRO"
    assert r["ativos"] == 3 and r["faltam_no_erp"] == 2 and r["so_no_erp"] >= 1
    assert sup.post("/erp/api/ponto/registro/cadastrar").status_code == 403
    c = dp.post("/erp/api/ponto/registro/cadastrar").get_json()
    assert c["criados"] == 1 and c["cpf_invalido"] == 1 and c["retrato"]["faltam_no_erp"] == 1
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    with db.conexao() as conn:
        novo = cadastros.colaborador_por_cpf(conn, CPF_NOVO)
    assert novo["nome"] == "NOVO DO REGISTRO" and novo["obra_codigo"] == "PG-A" and novo["telefone"] == "85955556666"

    assert sup.post("/erp/api/ponto/registro/fonte", json={"fonte": "ERP"}).status_code == 403
    assert dp.post("/erp/api/ponto/registro/fonte", json={"fonte": "PLANILHA"}).status_code == 400
    assert dp.post("/erp/api/ponto/registro/fonte", json={"fonte": "ERP"}).get_json()["usando"] is False
    with db.conexao() as conn:
        assert cadastros.colaborador_por_cpf(conn, CPF_JOAO)["nome"] == "João Obra A"     # o do ERP
