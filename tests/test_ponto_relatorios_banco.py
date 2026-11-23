# -*- coding: utf-8 -*-
"""Ponto — OS RELATÓRIOS (26/10/2026): faltas, ausências justificadas, horas extras e
banco de horas, com as contas do espelho. Postgres de verdade; o mundo de
test_ponto_gestao_banco."""
from __future__ import annotations

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, _segunda_passada, app,  # noqa: F401
                                           bater_via_chave, como, dia_util, local, mundo)

pytestmark = pytest.mark.banco


def _rel(cliente, tipo, de, ate, **filtros):
    r = cliente.post("/erp/api/ponto/relatorios", json={"tipo": tipo, "de": de.isoformat(), "ate": ate.isoformat(),
                                                        "filtros": filtros})
    assert r.status_code == 200, r.get_json()
    return r.get_json()


def test_faltas_extras_e_justificadas(app, mundo):
    seg = _segunda_passada()
    ter = dia_util(seg, 1)
    for h in ((7, 0), (11, 0), (12, 0), (18, 0)):        # João: uma hora a mais na segunda
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    dp = como(app, mundo["dp"])
    # Maria faltou nos dois dias; João faltou na terça
    f = _rel(dp, "faltas", seg, ter)
    por = {p["colaborador_id"]: p for p in f["por_pessoa"]}
    assert por[mundo["maria"]]["faltas"] == 2 and por[mundo["joao"]]["faltas"] == 1
    assert f["totais"]["faltas"] == len(f["linhas"]) >= 3
    # o relatório e o espelho dizem a mesma coisa
    from app.apps.ponto import db
    from app.apps.ponto.core import espelho
    with db.conexao() as conn:
        esp = espelho.montar(conn, mundo["maria"], seg, ter)
    faltas_esp = [d["data"] for d in esp["dias"] if d["situacao"] == "FALTA"]
    assert sorted(l["data"] for l in f["linhas"] if l["colaborador_id"] == mundo["maria"]) == sorted(faltas_esp)
    e = _rel(dp, "extras", seg, ter)
    joao = next(p for p in e["por_pessoa"] if p["colaborador_id"] == mundo["joao"])
    assert joao["total"] > 0 and joao["util"] == joao["total"] and e["linhas"][0]["tipo_dia"] == "UTIL"
    # o atestado da Maria na terça: sai das faltas e entra nas justificadas
    r = dp.post("/erp/api/ponto/afastamentos", json={"tipo": "ATESTADO", "colaborador_id": mundo["maria"],
                "data_inicio": ter.isoformat(), "data_fim": ter.isoformat(), "aprovar_ja": True, "cid": "J11"})
    assert r.status_code == 201, r.get_json()
    j = _rel(dp, "justificadas", seg, ter)
    assert [(l["colaborador_id"], l["tipo"], l["dias"]) for l in j["linhas"]] == [(mundo["maria"], "ATESTADO", 1)]
    assert j["por_tipo"][0]["dias"] == 1 and j["linhas"][0]["cid"] == "J11"
    assert _rel(dp, "faltas", seg, ter)["por_pessoa"][0]["colaborador_id"] in (mundo["maria"], mundo["joao"])
    assert next(p for p in _rel(dp, "faltas", seg, ter)["por_pessoa"] if p["colaborador_id"] == mundo["maria"])["faltas"] == 1
    # filtro, banco, permissão e o teto
    assert _rel(dp, "faltas", seg, ter, busca="joão")["totais"]["pessoas"] == 1
    assert "linhas" in _rel(dp, "banco", seg, ter)
    assert como(app, mundo["fin"]).post("/erp/api/ponto/relatorios", json={"tipo": "faltas"}).status_code == 403
    assert dp.post("/erp/api/ponto/relatorios", json={"tipo": "faltas", "de": "2026-01-01",
                                                      "ate": "2026-06-30"}).status_code == 400
    assert dp.post("/erp/api/ponto/relatorios", json={"tipo": "xyz", "de": seg.isoformat()}).status_code == 400
    assert dp.get("/erp/ponto/relatorios").status_code == 200


def test_faltas_sugeridas_no_desligamento_vem_do_espelho(app, mundo):
    """23/11/2026: a janela de atendimento do desligamento sugere as faltas do
    ponto, do dia seguinte ao corte até o último dia — a mesma conta do espelho."""
    import datetime as dt
    from app.apps.erp.core import dp_analise
    seg = _segunda_passada()
    ter = dia_util(seg, 1)
    r = dp_analise.faltas_no_ponto(mundo["maria"], ter, hoje=ter + dt.timedelta(days=1))
    assert r["disponivel"], r
    inicio = dt.date.fromisoformat(r["inicio"])
    esperadas = [d.isoformat() for d in (seg, ter) if d >= inicio]
    assert r["dias"] == esperadas and r["integrais"] == len(esperadas)
    assert r["parcial_ate_hoje"] is False


def test_dp_abre_o_espelho_e_as_sugestoes_pelo_pedido_de_desligamento(app, mundo):
    """23/11/2026: a janela de Atender traz as sugestões e abre a ficha única
    do ponto no mês das faltas; quem não é DP não vê as sugestões."""
    import datetime as dt
    from app.apps.erp.core import pessoal_solicitacoes as svc
    from app.apps.erp.db.database import get_session
    from app.apps.erp.db.models.cadastros import Usuario
    seg = _segunda_passada()
    ter = dia_util(seg, 1)
    with get_session() as s:
        p = svc.abrir(s, {"tipo": "DESLIGAMENTO", "colaborador_id": mundo["maria"],
                          "forma_desligamento": "Demissão com Aviso Indenizado",
                          "aviso_previo": ter.isoformat(),
                          "continua_prestador": "Não, está saindo da empresa"},
                      s.get(Usuario, mundo["dp"]))
        sid = p["id"]
        s.commit()
    dp = como(app, mundo["dp"])
    r = dp.get(f"/erp/api/dp/solicitacoes/{sid}/sugestoes")
    assert r.status_code == 200, r.get_json()
    j = r.get_json()
    assert j["valores"]["ultimo_dia"] == ter.isoformat()
    per = j["periodo_faltas"]
    assert per["ate"] == ter.isoformat()
    # a janela abre a FICHA ÚNICA do ponto no espelho do mês (não um arquivo)
    pagina = dp.get(f"/erp/ponto/pessoa/{mundo['maria']}?aba=mes&competencia={per['ate'][:7]}")
    assert pagina.status_code == 200
    assert como(app, mundo["fin"]).get(
        f"/erp/api/dp/solicitacoes/{sid}/sugestoes").status_code in (302, 403, 404)
