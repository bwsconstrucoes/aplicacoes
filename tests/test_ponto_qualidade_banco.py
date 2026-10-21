# -*- coding: utf-8 -*-
"""Ponto — a QUALIDADE do ponto (17/10/2026, core/qualidade.py): o dia em dia, o
índice, os pontos e o selo; o ranking por obra e por responsável; o limite de
ajustes; e quem vê o quê (a pessoa, o dela; o responsável pelo ponto da obra, a
equipe; a gestão, as obras que alcança). Contra um Postgres DE VERDADE."""
from __future__ import annotations

import datetime as dt

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _entrar_no_app,  # noqa: F401
                                           _schema_ponto2, app, como, mundo)
from tests.test_ponto_qr_banco import _foto, _tablet

pytestmark = pytest.mark.banco


@pytest.fixture(autouse=True)
def _conta_nova():
    """A guarda da qualidade não cai mais a cada batida (21/10/2026): cada teste
    começa sem o que o anterior calculou."""
    from app.apps.ponto.core import qualidade
    qualidade.esquecer()
    yield
    qualidade.esquecer()


def _mes_passado():
    from app.apps.ponto import horario
    fim = horario.hoje().replace(day=1) - dt.timedelta(days=1)
    return fim.replace(day=1), fim


def _bater(cpf, dia, hora, minuto, obra="PG-A"):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import marcacoes
    with db.conexao() as conn:
        marcacoes.registrar(conn, cpf=cpf, obra=obra, origem="IDFACE", via_chave=True,
                            agora=dt.datetime(dia.year, dia.month, dia.day, hora, minuto,
                                              tzinfo=horario.FUSO).astimezone(dt.timezone.utc))


def test_regras_puras_do_selo_e_do_dia():
    from app.apps.ponto.core.qualidade import avaliar_dias, pontos_e_selo
    assert pontos_e_selo(None, None, 0, 3) == (None, None)
    assert pontos_e_selo(100, 100, 0, 3)[1]["codigo"] == "OURO"
    assert pontos_e_selo(100, None, 0, 3)[0] == 100                      # sem foto do aplicativo: só o índice
    assert pontos_e_selo(90, 90, 5, 3) == (80, {"codigo": "BRONZE", "rotulo": "Bronze"})   # 2 ajustes a mais: −10
    dias = [
        {"data": "2026-10-05", "situacao": "OK", "faltando": [], "batidas": [
            {"hora": "07:00", "obra": "A", "status": "VALIDA", "origem": "PWA", "tem_foto": True}]},
        {"data": "2026-10-06", "situacao": "FALTA", "faltando": [], "batidas": []},
        {"data": "2026-10-07", "situacao": "OK", "faltando": [{"rotulo": "Saída"}], "batidas": [
            {"hora": "07:00", "obra": "B", "status": "VALIDA", "origem": "PWA", "tem_foto": False}]},
        {"data": "2026-10-08", "situacao": "OK", "faltando": [], "batidas": [
            {"hora": "07:00", "obra": "A", "status": "AJUSTADA", "origem": "MANUAL", "tem_foto": False}]},
        {"data": "2026-10-10", "situacao": "DESCANSO", "faltando": [], "batidas": []},
    ]
    av = avaliar_dias(dias, "A")
    t = av["totais"]
    assert (t["dias_trabalho"], t["dias_em_dia"], t["faltas"], t["incompletos"], t["dias_com_ajuste"]) == (4, 1, 1, 1, 1)
    assert t["sem_foto"] == 1 and t["batidas_app"] == 2
    assert av["por_obra"]["A"]["dias_trabalho"] == 3 and av["por_obra"]["B"]["incompletos"] == 1
    # a falta vai para a obra em que a pessoa mais bateu (A: 2 batidas; B: 1), não a do cadastro
    assert avaliar_dias(dias, "Z")["por_obra"]["A"]["faltas"] == 1
    assert {p["tipo"] for p in av["problemas"]} == {"FALTA", "FALTANDO", "AJUSTE", "SEM_FOTO"}


def test_ranking_da_gestao_com_limite_de_ajustes(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import feriados
    inicio, fim = _mes_passado()
    with db.conexao() as conn:                        # um dia de segunda a quinta que não seja feriado
        cal = feriados.Calendario(conn, inicio, fim)
        segunda = next(d for d in (inicio + dt.timedelta(days=i) for i in range(28))
                       if d.weekday() < 4 and not cal.feriado(d, mundo["obra_a"]))
    for h, m in ((7, 0), (11, 0), (12, 0), (17, 0)):
        _bater(CPF_JOAO, segunda, h, m)
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    dias_de_ajuste = [d for d in (inicio + dt.timedelta(days=i) for i in range((fim - inicio).days + 1))
                      if d.weekday() < 4 and d != segunda][:4]
    for d in dias_de_ajuste:
        r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"],
                    "data_inicio": d.isoformat(), "horario": f"{d.isoformat()}T07:00:00", "obra": "PG-A",
                    "descricao": "esqueceu de bater a entrada"})
        assert r.status_code in (200, 201), r.get_json()
    comp = f"{inicio:%Y-%m}"
    q = dp.get(f"/erp/api/ponto/qualidade?competencia={comp}").get_json()
    joao = next(p for p in q["pessoas"] if p["id"] == mundo["joao"])
    assert joao["dias_em_dia"] >= 1 and joao["faltas"] >= 1 and joao["ajustes"] == 4
    assert joao["muitos_ajustes"] is True and q["limite_ajustes"] == 3
    assert any(x["tipo"] == "FALTA" for x in joao["problemas"]) and joao["selo"]["codigo"] == "ATENCAO"
    assert any(o["obra"] == "PG-A" for o in q["por_obra"])
    # o supervisor da obra A não vê a Maria (obra B)
    assert mundo["maria"] not in {p["id"] for p in sup.get(f"/erp/api/ponto/qualidade?competencia={comp}").get_json()["pessoas"]}
    # o limite é configurável — só por quem configura o ponto
    assert sup.post("/erp/api/ponto/qualidade/limite", json={"limite": 10}).status_code in (403, 404)
    assert dp.post("/erp/api/ponto/qualidade/limite", json={"limite": 5}).get_json()["limite"] == 5
    joao = next(p for p in dp.get(f"/erp/api/ponto/qualidade?competencia={comp}").get_json()["pessoas"]
                if p["id"] == mundo["joao"])
    assert joao["muitos_ajustes"] is False
    assert dp.post("/erp/api/ponto/qualidade/limite", json={"limite": 99}).status_code == 400
    # a tela abre
    assert dp.get("/erp/ponto/qualidade").status_code == 200


def test_a_pessoa_ve_o_dela_e_o_responsavel_a_equipe(app, mundo, monkeypatch):
    from app.apps.ponto import horario
    t, h, _ = _tablet(app, mundo)                     # o ponto da obra A, com a Maria de responsável
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    assert t.post("/ponto/app/api/bater", headers=h, json={"bilhete": ident["bilhete"], "obra": "PG-A",
                  "foto_base64": _foto(), "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10}).status_code == 201
    comp = f"{horario.hoje():%Y-%m}"
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    meu = joao.get(f"/ponto/app/api/qualidade/meu?competencia={comp}")
    assert meu.status_code == 200 and meu.get_json()["id"] == mundo["joao"]
    assert joao.get(f"/ponto/app/api/qualidade/equipe?competencia={comp}").status_code == 404
    maria = _entrar_no_app(app, CPF_MARIA, monkeypatch)
    eq = maria.get(f"/ponto/app/api/qualidade/equipe?competencia={comp}")
    assert eq.status_code == 200, eq.get_json()
    assert mundo["joao"] in {p["id"] for p in eq.get_json()["pessoas"]}
    assert maria.get("/ponto/app/api/inicio").get_json()["pode"]["qualidade_equipe"] is True


def test_nunca_duas_contas_pesadas_ao_mesmo_tempo(app, mundo):
    """21/10/2026, incidente: no pico da saída as contas da qualidade, todas juntas,
    tomaram o servidor. Com uma rodando, a outra ouve "tente em instantes" na hora."""
    import time as _t
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import qualidade
    hoje = horario.hoje()
    assert qualidade._uma_por_vez.acquire(blocking=False)        # outra conta "rodando"
    try:
        with db.conexao() as conn:
            comeco = _t.time()
            with pytest.raises(qualidade.Ocupado):
                qualidade.avaliar(conn, [mundo["joao"]], hoje.replace(day=1), hoje, esperar=0)
            with pytest.raises(qualidade.Ocupado):
                qualidade.da_pessoa_guardada(conn, mundo["joao"], hoje.replace(day=1), hoje)
            assert _t.time() - comeco < 2                            # não fica esperando
    finally:
        qualidade._uma_por_vez.release()
    with db.conexao() as conn:                                       # livre: calcula e guarda
        a = qualidade.da_pessoa_guardada(conn, mundo["joao"], hoje.replace(day=1), hoje)
        assert qualidade._uma_por_vez.acquire(blocking=False)
        try:                                                         # guardado: responde mesmo ocupado
            assert qualidade.da_pessoa_guardada(conn, mundo["joao"], hoje.replace(day=1), hoje) is a
        finally:
            qualidade._uma_por_vez.release()
