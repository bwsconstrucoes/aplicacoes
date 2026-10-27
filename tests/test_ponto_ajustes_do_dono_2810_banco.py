# -*- coding: utf-8 -*-
"""Ponto — as anotações do dono de 28/10/2026, com Postgres de verdade: corrigir UMA batida
(a obra errada, ou a hora) sem apagar a original; o espelho e o cadastro na janela da pessoa
(o endereço antigo abre o Quadro com ela); a conferência do rosto que explica o nível; a
batida "sem rosto" validada pelo DP que não conta como falha; as exceções com a função; e o
filtro de funcionários com várias obras."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _schema_ponto2, _segunda_passada,  # noqa: F401
                                           app, bater_via_chave, como, local, mundo)

pytestmark = pytest.mark.banco


def test_o_dp_corrige_a_obra_e_a_hora_de_uma_batida_sem_apagar_a_original(app, mundo, banco):
    seg = _segunda_passada()
    m = bater_via_chave(app, CPF_JOAO, local(seg, 7, 0))
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    url = f"/erp/api/ponto/marcacoes/{m['id']}/corrigir"
    motivo = "bateu na obra errada por engano, estava na PG-B"
    # só o DP corrige; o motivo é obrigatório; e precisa mudar alguma coisa
    assert sup.post(url, json={"obra": "PG-B", "motivo": motivo}).status_code in (403, 404)
    assert dp.post(url, json={"obra": "PG-B", "motivo": "curto"}).status_code == 400
    assert dp.post(url, json={"obra": "PG-A", "hora": "07:00", "motivo": motivo}).status_code == 400
    r = dp.post(url, json={"obra": "PG-B", "hora": "07:05", "motivo": motivo})
    assert r.status_code == 200, r.get_json()
    nova = r.get_json()["batida"]
    assert nova["obra"]["codigo"] == "PG-B" and nova["status"] == "AJUSTADA" and "T07:05:00" in nova["horario"]
    assert nova["marcacao_origem_id"] == m["id"]
    with banco.connect() as conn:
        orig = conn.execute(text("SELECT status FROM ponto.marcacoes WHERE id = :i"), {"i": m["id"]}).scalar()
        dec = conn.execute(text("SELECT motivo, usuario_nome FROM ponto.marcacao_decisoes WHERE marcacao_id = :i"),
                           {"i": m["id"]}).one()
    assert orig == "REJEITADA" and "PG-A às 07:00 → PG-B às 07:05" in dec.motivo and motivo in dec.motivo
    # o espelho conta a corrigida, na obra certa; a janela da batida diz que foi corrigida
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    dia = next(d for d in esp["dias"] if d["data"] == seg.isoformat())
    assert [(b["hora"], b["obra"]) for b in dia["batidas"]] == [("07:05", "PG-B")]
    loc = dp.get(f"/erp/api/ponto/marcacoes/{nova['id']}/local").get_json()
    assert "corrigida pelo DP" in loc["como"]
    # a original não se corrige de novo
    assert dp.post(url, json={"obra": "PG-A", "motivo": motivo}).status_code == 400


def test_o_espelho_abre_o_quadro_com_a_janela_e_pessoas_tem_a_lista(app, mundo):
    dp = como(app, mundo["dp"])
    r = dp.get(f"/erp/ponto/espelho?colaborador_id={mundo['joao']}&competencia=2026-10")
    assert r.status_code == 302 and "/erp/ponto/quadro" in r.headers["Location"]
    assert f"abrir_pessoa={mundo['joao']}" in r.headers["Location"] and "aba=mes" in r.headers["Location"]
    # a tela Pessoas voltou (28/10/2026), com a foto de cadastro de cada um
    assert dp.get(f"/erp/ponto/pessoas?pessoa={mundo['maria']}").status_code == 200
    lista = dp.get("/erp/api/ponto/pessoas").get_json()["pessoas"]
    assert all("tem_foto_cadastral" in p for p in lista)
    so_a = dp.get(f"/erp/api/ponto/pessoas?obra_cadastro={mundo['obra_a']}").get_json()["pessoas"]
    assert {p["nome"] for p in so_a} == {"João Obra A"}
    assert dp.get("/erp/ponto/espelho").headers["Location"].endswith("/erp/ponto/quadro")


def test_a_sem_rosto_validada_e_falso_alarme_e_o_nivel_se_explica(app, mundo, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import confianca, rosto
    seg = _segunda_passada()
    feitas = [bater_via_chave(app, CPF_JOAO, local(seg, *h)) for h in ((7, 0), (11, 0))]
    with banco.connect() as conn:
        for m, st in zip(feitas, ("VALIDA", "EM_ANALISE")):
            conn.execute(text("INSERT INTO ponto.conferencias_rosto (marcacao_id, resultado, detalhe) "
                              "VALUES (:m, 'SEM_ROSTO', 'nenhum rosto encontrado na foto')"), {"m": m["id"]})
            conn.execute(text("UPDATE ponto.marcacoes SET status = :s WHERE id = :m"), {"s": st, "m": m["id"]})
        conn.commit()
    with db.conexao() as conn:
        d = confianca.config(rosto.ler(conn))
        h = confianca.historico(conn, [mundo["joao"]], d)[mundo["joao"]]
    assert h["falhas_recentes"] == 1                    # a validada não conta; a em conferência, sim
    r = como(app, mundo["dp"]).get(f"/erp/api/ponto/pessoas/{mundo['joao']}/rosto").get_json()
    assert r["nivel"]["codigo"] == "OBSERVACAO" and "11:00" in r["porque"] and "foto de CADASTRO" in r["porque"]
    por_id = {c["marcacao_id"]: c for c in r["conferencias"]}
    assert por_id[feitas[0]["id"]]["falso_alarme"] and not por_id[feitas[0]["id"]]["falha"]
    assert por_id[feitas[1]["id"]]["falha"]


def test_excecoes_uma_linha_por_pessoa_e_o_filtro_com_varias_obras(app, mundo, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import forma_de_bater
    with db.conexao() as conn:
        forma_de_bater.definir(conn, mundo["joao"], True, "teste")
    exc = como(app, mundo["dp"]).get("/erp/api/ponto/excecoes").get_json()
    joao = next(p for p in exc["pessoas"] if p["id"] == mundo["joao"])
    assert joao["excecoes"] == ["CELULAR"] and joao["obra"] == "PG-A" and "funcao" in joao
    # o filtro de funcionários aceita várias obras de uma vez (lista ou "1,2")
    seg = _segunda_passada()
    dp = como(app, mundo["dp"])
    nomes = lambda **f: {l["nome"] for l in dp.post("/erp/api/ponto/quadro", json={  # noqa: E731
        "de": seg.isoformat(), "ate": seg.isoformat(), "filtros": f}).get_json()["linhas"]}
    so_a = nomes(obra_cadastro=[mundo["obra_a"]])
    as_duas = nomes(obra_cadastro=[mundo["obra_a"], mundo["obra_b"]])
    assert "João Obra A" in so_a and "Maria Obra B" not in so_a
    assert {"João Obra A", "Maria Obra B"} <= as_duas
    assert nomes(obra_cadastro=f"{mundo['obra_a']},{mundo['obra_b']}") == as_duas


def test_a_gestao_envia_uma_foto_nova_de_cadastro(app, mundo, banco):
    from tests.test_ponto_qr_banco import _foto
    dp, fin = como(app, mundo["dp"]), como(app, mundo["fin"])
    url = f"/erp/api/ponto/pessoas/{mundo['joao']}/foto-cadastral"
    assert fin.post(url + "/enviar", json={"foto_base64": _foto()}).status_code == 403
    assert dp.post(url + "/enviar", json={"foto_base64": "não é foto"}).status_code == 400
    r = dp.post(url + "/enviar", json={"foto_base64": _foto()})
    assert r.status_code == 200, r.get_json()
    with banco.connect() as conn:
        fid = conn.execute(text("SELECT foto_cadastral_id FROM ponto.colaborador_config WHERE colaborador_id = :c"),
                           {"c": mundo["joao"]}).scalar()
    assert fid == r.get_json()["foto_id"]
    foto = dp.get(url)
    assert foto.status_code == 200 and foto.mimetype == "image/jpeg"
    assert dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]["tem_foto_cadastral"] is True
