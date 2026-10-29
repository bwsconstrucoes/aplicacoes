# -*- coding: utf-8 -*-
"""Ponto — os tipos de ausência numa tela (29/10/2026, core/tipos_ausencia.py, migração 017): os abonos da
empresa cadastráveis, "abona as horas?" nos abonos e nas licenças da lei, e excluir (o que já foi usado
só desliga)."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401

pytestmark = pytest.mark.banco


@pytest.fixture(autouse=True)
def catalogo_de_volta(banco):
    """As duas tabelas de tipos não são esvaziadas entre os testes (são catálogo): o que este arquivo
    mudar volta ao que era."""
    yield
    from sqlalchemy import text
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM ponto.tipos_abono WHERE codigo NOT IN ('VIAGEM', 'DISPENSA')"))
        conn.execute(text("UPDATE ponto.tipos_abono SET abona = TRUE, ativo = TRUE"))
        conn.execute(text("UPDATE ponto.tipos_licenca SET abona = TRUE, ativo = TRUE WHERE codigo = 'DOACAO_SANGUE'"))
        conn.commit()


def _dia(dp, colaborador_id, dia):
    esp = dp.get(f"/erp/api/ponto/espelho/{colaborador_id}?competencia={dia:%Y-%m}").get_json()
    return next(d for d in esp["dias"] if d["data"] == dia.isoformat())


def _lancar(dp, colaborador_id, dia, tipo, subtipo):
    return dp.post("/erp/api/ponto/afastamentos", json={
        "tipo": tipo, "subtipo": subtipo, "colaborador_id": colaborador_id, "data_inicio": dia.isoformat(),
        "data_fim": dia.isoformat(), "descricao": "teste", "aprovar_ja": True})


def test_o_abono_cadastrado_que_nao_abona_justifica_mas_desconta(app, mundo):
    dp, fin = como(app, mundo["dp"]), como(app, mundo["fin"])
    seg = _segunda_passada()
    # os dois de sempre já vêm cadastrados
    assert {a["codigo"] for a in dp.get("/erp/api/ponto/abonos").get_json()["abonos"]} >= {"VIAGEM", "DISPENSA"}
    # só o DP cadastra
    assert fin.post("/erp/api/ponto/abonos/novo", json={"nome": "Treinamento externo"}).status_code == 403
    r = dp.post("/erp/api/ponto/abonos/novo", json={"nome": "Treinamento externo", "abona": False})
    assert r.status_code == 200, r.get_json()
    a = r.get_json()["abono"]
    assert a["codigo"] == "TREINAMENTOEXTERNO" and a["abona"] is False
    # o que abona (viagem): o dia não é descontado; o que não abona: justificado, mas descontado
    ter, qua = seg + dt.timedelta(days=1), seg + dt.timedelta(days=2)
    assert _lancar(dp, mundo["joao"], ter, "ABONO", "VIAGEM").status_code == 201
    assert _lancar(dp, mundo["joao"], qua, "ABONO", a["codigo"]).status_code == 201
    d_ter, d_qua = _dia(dp, mundo["joao"], ter), _dia(dp, mundo["joao"], qua)
    assert d_ter["situacao"] == "ABONADO" and not d_ter["debito"]
    assert d_qua["situacao"] != "ABONADO" and d_qua["debito"] > 0
    assert [x["rotulo"] for x in d_qua["declaracoes"]] == ["Treinamento externo"]
    # abono que não existe (ou desligado) é recusado
    assert _lancar(dp, mundo["joao"], seg, "ABONO", "INVENTADO").status_code == 400
    # excluir: o usado só desliga e sai da lista; o nunca usado sai de vez
    r = dp.post(f"/erp/api/ponto/tipos-ausencia/ABONO/{a['codigo']}/excluir", json={})
    assert r.get_json()["resultado"] == "DESLIGADO"
    assert a["codigo"] not in {x["codigo"] for x in dp.get("/erp/api/ponto/abonos").get_json()["abonos"]}
    assert _dia(dp, mundo["joao"], qua)["declaracoes"][0]["rotulo"] == "Treinamento externo"   # o nome fica
    dp.post("/erp/api/ponto/abonos/novo", json={"nome": "Nunca usado"})
    r = dp.post("/erp/api/ponto/tipos-ausencia/ABONO/NUNCAUSADO/excluir", json={})
    assert r.get_json()["resultado"] == "EXCLUIDO"
    todos = {x["codigo"] for x in dp.get("/erp/api/ponto/abonos?todas=1").get_json()["abonos"]}
    assert "NUNCAUSADO" not in todos and a["codigo"] in todos


def test_a_licenca_da_lei_pode_nao_abonar(app, mundo):
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    lic = {l["codigo"]: l for l in dp.get("/erp/api/ponto/licencas?todas=1").get_json()["licencas"]}
    assert lic["CASAMENTO"]["abona"] is True                    # o padrão: toda licença abona
    r = dp.post("/erp/api/ponto/licencas/DOACAO_SANGUE", json={**lic["DOACAO_SANGUE"], "abona": False})
    assert r.status_code == 200 and r.get_json()["licenca"]["abona"] is False
    dia = seg + dt.timedelta(days=3)
    r = dp.post("/erp/api/ponto/afastamentos", json={
        "tipo": "LICENCA", "subtipo": "DOACAO_SANGUE", "colaborador_id": mundo["joao"], "data_inicio": dia.isoformat(),
        "data_fim": dia.isoformat(), "descricao": "doação", "aprovar_ja": True, "documento_base64": _png()})
    assert r.status_code == 201, r.get_json()
    d = _dia(dp, mundo["joao"], dia)
    assert d["situacao"] != "ABONADO" and d["debito"] > 0 and d["declaracoes"]
    # a licença usada não se apaga: desliga
    assert dp.post("/erp/api/ponto/tipos-ausencia/LICENCA/DOACAO_SANGUE/excluir", json={}).get_json()["resultado"] == "DESLIGADO"


def _png():
    from tests.test_ponto_gestao_banco import _png as png
    return png()


def test_o_abono_e_pedido_por_quem_faz_requisicao_e_vai_para_validacao(app, mundo):
    """29/10/2026, o dono: o abono "não é pra ser só o DP. É pra ser pros mesmos que podem fazer requisição"."""
    from app.apps.ponto.core import ocorrencias
    assert "ABONO" in ocorrencias.PEDIDOS_DO_APP                  # o aplicativo também pede
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    dia = _segunda_passada() + dt.timedelta(days=1)
    pedido = {"tipo": "ABONO", "subtipo": "VIAGEM", "colaborador_id": mundo["joao"], "data_inicio": dia.isoformat(),
              "data_fim": dia.isoformat()}
    assert sup.post("/erp/api/ponto/ocorrencias", json={**pedido, "descricao": "curto"}).status_code == 400
    r = sup.post("/erp/api/ponto/ocorrencias", json={**pedido, "descricao": "viagem a Recife a pedido do engenheiro"})
    assert r.status_code == 201, r.get_json()
    o = r.get_json()["ocorrencia"]
    assert o["status"] != "APROVADA"                               # o supervisor pede; quem valida decide
    assert _dia(dp, mundo["joao"], dia)["situacao"] != "ABONADO"
    assert dp.post(f"/erp/api/ponto/afastamentos/{o['id']}/etapa-dp", json={"aprovar": True}).status_code == 200
    assert _dia(dp, mundo["joao"], dia)["situacao"] == "ABONADO"
    # férias e afastamento continuam só do DP
    ferias = {**pedido, "tipo": "FERIAS", "subtipo": "", "descricao": "férias do joão no mês"}
    assert sup.post("/erp/api/ponto/ocorrencias", json=ferias).status_code == 400


def test_a_compensacao_conferida(app, mundo):
    """29/10/2026, o dono: "como o sistema trata uma compensação que ainda não aconteceu, e se ela não
    acontecer?" — "automático": passado o dia trabalhado, a folga só fica abonada no que ele rendeu."""
    from tests.test_ponto_gestao_banco import bater_via_chave, local
    from tests.test_ponto_gestao_banco import CPF_JOAO
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    sabado = seg - dt.timedelta(days=2)                    # descanso na escala: tudo o que trabalhar paga a folga
    ter, qua, qui = (seg + dt.timedelta(days=n) for n in (1, 2, 3))
    futuro = dt.date.today() + dt.timedelta(days=9)

    def compensar(folga, trabalhado):
        r = dp.post("/erp/api/ponto/ocorrencias", json={
            "tipo": "COMPENSACAO", "colaborador_id": mundo["joao"], "data_inicio": folga.isoformat(),
            "dia_trabalhado": trabalhado.isoformat(), "descricao": "troca de dia combinada", "aprovar_ja": True})
        assert r.status_code == 201, r.get_json()
        assert r.get_json()["ocorrencia"]["status"] == "APROVADA"

    # o sábado passou sem batida: a folga da terça vira falta
    compensar(ter, sabado)
    d = _dia(dp, mundo["joao"], ter)
    assert d["situacao"] == "FALTA" and d["debito"] == 540 and "COMPENSACAO_NAO_CUMPRIDA" in d["alertas"]
    # trabalhou 4 h no sábado: cobrem 4 h da folga; as outras 5 h ficam falta parcial
    for h in (7, 11):
        bater_via_chave(app, CPF_JOAO, local(sabado, h, 0))
    d = _dia(dp, mundo["joao"], ter)
    assert d["situacao"] == "FALTA_PARCIAL" and d["debito"] == 300
    assert any("não cumprida" in x["rotulo"] for x in d["declaracoes"])
    # o dia trabalhado ainda não chegou: a folga vale, e fica "a cumprir"
    compensar(qua, futuro)
    d = _dia(dp, mundo["joao"], qua)
    assert d["situacao"] == "ABONADO" and d["compensacao_a_cumprir"] == futuro.isoformat()
    # bateu no dia da folga: as horas contam
    compensar(qui, futuro + dt.timedelta(days=1))
    for h in (7, 11):
        bater_via_chave(app, CPF_JOAO, local(qui, h, 0))
    d = _dia(dp, mundo["joao"], qui)
    assert d["extra"] == 240 and "TRABALHOU_NA_FOLGA" in d["alertas"]
