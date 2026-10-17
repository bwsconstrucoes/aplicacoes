# -*- coding: utf-8 -*-
"""Ponto — A CONFERÊNCIA DO ROSTO QUE SE ADAPTA À PESSOA (17/10/2026, core/confianca.py):
o dono, "quanto melhor a pessoa, menos se confere; quanto pior a pessoa, mais se
confere (…) se nas conferências aleatórias aquela pessoa aparecer de novo, a gente
volta ela para insistir na conferência"."""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.ponto.core import confianca
from app.apps.ponto.erros import ErroDeValidacao

D = confianca.config({})


def test_quem_comeca_confere_tudo_e_desce_conforme_acerta():
    assert confianca.nivel(0, 0, D)["codigo"] == "OBSERVACAO" and confianca.nivel(0, 0, D)["pct"] == 100
    assert confianca.nivel(19, 0, D)["proximo"] == {"rotulo": "Boa", "pct": 50, "faltam": 1}
    assert [confianca.nivel(n, 0, D)["pct"] for n in (20, 59, 60, 149, 150, 900)] == [50, 50, 25, 25, 10, 10]
    assert confianca.nivel(150, 0, D)["proximo"] is None


def test_reincidente_precisa_do_dobro_para_descer():
    n = confianca.nivel(30, 2, D)                  # 2 falhas em 60 dias
    assert n["reincidente"] and n["codigo"] == "OBSERVACAO" and n["proximo"]["faltam"] == 10
    assert confianca.nivel(40, 2, D)["codigo"] == "BOA"
    assert confianca.nivel(30, 1, D)["codigo"] == "BOA" and not confianca.nivel(30, 1, D)["reincidente"]


def test_o_sorteio_e_estavel_e_respeita_a_chance():
    assert all(confianca.sorteada(i, 100) for i in range(50))
    assert [confianca.sorteada(i, 25) for i in range(200)] == [confianca.sorteada(i, 25) for i in range(200)]
    for pct in (10, 25, 50):
        taxa = sum(confianca.sorteada(i, pct) for i in range(20_000)) / 20_000
        assert abs(taxa - pct / 100) < 0.02, (pct, taxa)


def test_ninguem_fica_sem_conferencia_mais_que_a_garantia():
    agora = dt.datetime(2026, 10, 20, 12, tzinfo=dt.timezone.utc)
    exemplar = confianca.nivel(500, 0, D)
    nao = next(i for i in range(1000) if not confianca.sorteada(i, exemplar["pct"]))
    assert confianca.decidir(exemplar, nao, agora - dt.timedelta(days=2), agora, D)[0] is False
    confere, porque = confianca.decidir(exemplar, nao, agora - dt.timedelta(days=7), agora, D)
    assert confere and "7 dias" in porque
    desligada = {**D, "ligada": False}
    assert confianca.decidir(exemplar, nao, agora - dt.timedelta(days=2), agora, desligada)[0] is True


def test_a_configuracao_e_conferida():
    d = confianca.validar({"limpas": [5, 10, 30, 90], "pct": [80, 40, 20, 5]}, {})
    assert d["limpas"] == [0, 10, 30, 90] and d["pct"] == [100, 40, 20, 5]   # o 1º nível é sempre "confere tudo"
    for ruim in ({"limpas": [0, 30, 20, 90]}, {"pct": [100, 20, 40, 10]}, {"pct": [100, 50, 25, 2]},
                 {"garantia_dias": 0}, {"reincidente_dias": 2}, {"limpas": ["x", 1, 2, 3]}):
        with pytest.raises(ErroDeValidacao):
            confianca.validar(ruim, {})


def test_resumo_por_nivel():
    lista = [confianca.nivel(n, f, D) for n, f in ((0, 0), (25, 0), (25, 3), (200, 0))]
    r = confianca.resumo(lista)
    assert [x["pessoas"] for x in r["por_nivel"]] == [2, 1, 0, 1] and r["reincidentes"] == 1


# ---------------------------------------------------------------------------
# Com Postgres de verdade
# ---------------------------------------------------------------------------
from tests.test_ponto_gestao_banco import CPF_JOAO, _schema_ponto2, app, como, mundo  # noqa: E402,F401


@pytest.mark.banco
def test_quem_acerta_e_sorteado_e_quem_falha_volta_a_conferir_tudo(app, mundo, banco):
    from sqlalchemy import text
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import marcacoes, mosaico, rosto
    from tests.test_ponto_banco import _foto_base64

    class Aws:
        def __init__(self, semelhanca):
            self.semelhanca, self.chamadas = semelhanca, 0

        def compare_faces(self, **_):
            self.chamadas += 1
            return {"FaceMatches": [{"Similarity": self.semelhanca}]}

    joao = mundo["joao"]
    dp = como(app, mundo["dp"])
    assert dp.post("/erp/api/ponto/rosto", json={"ligado": True, "teto_mensal_usd": "10"}).status_code == 200
    agora = horario.agora()
    with db.conexao() as conn:
        # o passado: 25 conferências limpas (batidas sem foto, de 20 dias atrás)
        antigas = [marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True,
                                       agora=agora - dt.timedelta(days=20) + dt.timedelta(hours=3 * i))[0]["id"]
                   for i in range(25)]
        novas = [marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True,
                                     foto_base64=_foto_base64(),
                                     agora=agora - dt.timedelta(hours=44) + dt.timedelta(hours=2 * i))[0]["id"]
                 for i in range(21)]
        mosaico.definir_foto_cadastral(conn, joao, novas[0])
    with banco.connect() as conn:
        conn.execute(text("""INSERT INTO ponto.conferencias_rosto (marcacao_id, resultado, semelhanca, custo_usd, conferido_em)
                             SELECT unnest(CAST(:ids AS bigint[])), 'MESMA_PESSOA', 99, 0.001, now() - interval '1 day'"""),
                     {"ids": antigas})
        conn.commit()
    ficha = dp.get(f"/erp/api/ponto/pessoas/{joao}").get_json()["pessoa"]["conferencia_rosto"]
    assert ficha["codigo"] == "BOA" and ficha["pct"] == 50 and ficha["limpas"] == 25

    aws = Aws(99.0)
    with db.conexao() as conn:
        r = rosto.conferir(conn, cliente=aws)
    assert r["dispensadas"] > 0 and r["conferidas"] > 0 and r["dispensadas"] + r["conferidas"] == 20, r
    assert aws.chamadas == r["conferidas"]                       # só se paga o sorteado
    with banco.connect() as conn:
        linhas = conn.execute(text("""SELECT resultado, nivel, chance, custo_usd FROM ponto.conferencias_rosto
                                       WHERE marcacao_id = ANY(:ids) AND marcacao_id <> :c"""),
                              {"ids": novas, "c": novas[0]}).mappings().all()
    assert {(l["nivel"], l["chance"]) for l in linhas} == {("BOA", 50)}
    assert all(float(l["custo_usd"]) == 0 for l in linhas if l["resultado"] == "DISPENSADA")
    t = dp.get("/erp/api/ponto/rosto").get_json()
    assert t["mes"]["dispensadas"] == r["dispensadas"] and t["niveis"]["pessoas"] >= 1

    # o sorteio pegou um rosto que não confere: volta a conferir tudo, e o alerta diz
    with db.conexao() as conn:
        falha = marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True,
                                    foto_base64=_foto_base64(), agora=agora - dt.timedelta(minutes=30))[0]["id"]
    with banco.connect() as conn:                 # a garantia força a conferência desta
        conn.execute(text("UPDATE ponto.conferencias_rosto SET conferido_em = now() - interval '8 days' "
                          "WHERE resultado = 'MESMA_PESSOA'"))
        conn.commit()
    with db.conexao() as conn:
        assert rosto.conferir(conn, cliente=Aws(30.0))["suspeitas"] == 1
    ficha = dp.get(f"/erp/api/ponto/pessoas/{joao}").get_json()["pessoa"]["conferencia_rosto"]
    assert ficha["codigo"] == "OBSERVACAO" and ficha["pct"] == 100 and ficha["limpas"] == 0

    # o DP olhou e aprovou a batida: era falso alarme — não conta como falha
    with banco.connect() as conn:
        conn.execute(text("UPDATE ponto.marcacoes SET status = 'VALIDA' WHERE id = :i"), {"i": falha})
        conn.commit()
    ficha = dp.get(f"/erp/api/ponto/pessoas/{joao}").get_json()["pessoa"]["conferencia_rosto"]
    assert ficha["codigo"] != "OBSERVACAO" and ficha["limpas"] >= 25

    lista = dp.get("/erp/api/ponto/rosto/pessoas").get_json()["pessoas"]
    assert any(p["id"] == joao for p in lista)
    assert como(app, mundo["sup"]).get("/erp/api/ponto/rosto/pessoas").status_code in (403, 404)


@pytest.mark.banco
def test_quem_falha_duas_vezes_vira_reincidente_no_alerta(app, mundo, banco):
    from sqlalchemy import text
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import alertas, marcacoes
    agora = horario.agora()
    with db.conexao() as conn:
        ids = [marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True,
                                   agora=agora - dt.timedelta(hours=10 - 3 * i))[0]["id"] for i in range(2)]
    with banco.connect() as conn:
        for n, i in enumerate(ids):
            conn.execute(text("""INSERT INTO ponto.conferencias_rosto (marcacao_id, resultado, semelhanca, custo_usd, conferido_em)
                                 VALUES (:i, 'OUTRA_PESSOA', 30, 0.001, now() - make_interval(hours => :h))"""),
                         {"i": i, "h": 5 - n})
        conn.execute(text("UPDATE ponto.marcacoes SET status = 'EM_ANALISE' WHERE id = ANY(:ids)"), {"ids": ids})
        conn.commit()
    with db.conexao() as conn:
        alertas.gerar(conn, ate=horario.hoje())
        textos = [a["mensagem"] for a in alertas.listar(conn) if a["codigo"] == "ROSTO_NAO_CONFERE"]
    assert any("REINCIDENTE: 2ª vez" in t for t in textos), textos
    assert any("REINCIDENTE" not in t for t in textos)            # a primeira ainda não era
