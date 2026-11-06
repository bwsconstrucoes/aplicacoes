# -*- coding: utf-8 -*-
"""Ponto — a ronda surpresa dos vigias noturnos (13/10/2026), com Postgres de
verdade: a entrada sorteia as rondas, sem entrada não há ronda, o WhatsApp sai na
hora, o vigia registra com foto e localização, a perdida avisa, a saída cancela."""
from __future__ import annotations

import datetime as dt
import random

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _entrar_no_app, _png,  # noqa: F401
                                           _schema_ponto2, _segunda_passada, app, bater_via_chave, como,
                                           dia_util, local, mundo)

pytestmark = pytest.mark.banco


def test_sorteio_fica_no_miolo_do_turno_e_espalhado():
    from app.apps.ponto.core import rondas
    inicio = dt.datetime(2026, 10, 13, 18, 0, tzinfo=dt.timezone.utc)
    for semente in range(50):
        hs = rondas.sortear_horarios(inicio, dt.timedelta(hours=12), random.Random(semente))
        assert len(hs) == 4 and hs == sorted(hs)
        assert hs[0] >= inicio + dt.timedelta(hours=1) and hs[-1] <= inicio + dt.timedelta(hours=11)
        assert min((b - a) for a, b in zip(hs, hs[1:])) >= dt.timedelta(minutes=45)


def _vigia(app, mundo, colaborador_id, sim=True):
    dp = como(app, mundo["dp"])
    r = dp.post(f"/erp/api/ponto/pessoas/{colaborador_id}/forma-de-bater", json={"faz_ronda": sim})
    assert r.status_code == 200, r.get_json()
    assert dp.get(f"/erp/api/ponto/pessoas/{colaborador_id}").get_json()["pessoa"]["faz_ronda"] is sim
    return dp


def test_ronda_do_vigia_do_comeco_ao_fim(app, mundo, banco, monkeypatch):
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import parametros, rondas
    dp = _vigia(app, mundo, mundo["joao"])
    with db.conexao() as conn:
        parametros.gravar(conn, parametros.TELEFONES_RESUMO, "85988887777", "teste")
    seg = _segunda_passada()
    ter = dia_util(seg, 1)
    # De manhã, sem turno aberto, a batida não abre turno (é a saída de um turno esquecido)
    bater_via_chave(app, CPF_JOAO, local(seg, 7, 0))
    with banco.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM ponto.rondas")).scalar() == 0
    # A Maria não é vigia: a entrada dela à noite não sorteia nada
    bater_via_chave(app, CPF_MARIA, local(seg, 17, 40), obra="PG-B")
    # A entrada do vigia às 17:30 sorteia as 4 rondas da noite
    entrada = bater_via_chave(app, CPF_JOAO, local(seg, 17, 30))
    with db.conexao() as conn:
        lista = db.todos(conn, "SELECT * FROM ponto.rondas ORDER BY numero")
    assert [r["numero"] for r in lista] == [1, 2, 3, 4] and all(r["status"] == "AGENDADA" for r in lista)
    assert all(r["colaborador_id"] == mundo["joao"] and r["entrada_id"] == entrada["id"] for r in lista)
    assert lista[0]["agendada_para"] >= local(seg, 18, 30) and lista[-1]["agendada_para"] <= local(ter, 5, 30)
    # A entrada batida de novo em seguida não fecha o turno
    bater_via_chave(app, CPF_JOAO, local(seg, 17, 33))
    with banco.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM ponto.rondas WHERE status = 'AGENDADA'")).scalar() == 4

    # Chegou a hora da 1ª: sai o WhatsApp com o link
    enviados = []
    def enviar(**kw):
        enviados.append(kw)
        return {"whatsapp": {"ok": True}}
    primeira = lista[0]["agendada_para"]
    r = rondas.rodar(db.conexao, agora=primeira + dt.timedelta(seconds=5), enviar=enviar)
    assert r["enviadas"] == 1 and enviados[0]["finalidade"] == "ponto.ronda"
    assert "ronda=" in enviados[0]["mensagem"] and enviados[0]["telefone"]
    token = enviados[0]["mensagem"].split("ronda=")[1].split()[0]

    # O vigia registra pelo app (foto + localização), dentro do prazo
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    monkeypatch.setattr(horario, "agora", lambda: primeira + dt.timedelta(minutes=3))
    info = cel.get(f"/ponto/app/api/ronda?token={token}").get_json()["ronda"]
    assert info["status"] == "ENVIADA" and info["numero"] == 1 and info["segundos_restantes"] > 0
    sem_foto = cel.post("/ponto/app/api/ronda", json={"token": token, "latitude": OBRA_A[0], "longitude": OBRA_A[1]})
    assert sem_foto.status_code == 400 and "foto" in sem_foto.get_json()["erro"]
    outra = _entrar_no_app(app, CPF_MARIA, monkeypatch)
    assert outra.get(f"/ponto/app/api/ronda?token={token}").status_code == 404     # a ronda é dele
    feita = cel.post("/ponto/app/api/ronda", json={"token": token, "foto_base64": _png(),
                                                   "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10})
    assert feita.status_code == 201, feita.get_json()
    assert feita.get_json()["status"] == "FEITA" and feita.get_json()["dentro_da_cerca"] is True
    assert cel.post("/ponto/app/api/ronda", json={"token": token, "foto_base64": _png(), "latitude": OBRA_A[0],
                                                  "longitude": OBRA_A[1]}).status_code == 400   # já feita
    monkeypatch.undo()

    # A 2ª sai e ninguém responde: passados os 10 min, PERDIDA — e o aviso na hora
    segunda = lista[1]["agendada_para"]
    rondas.rodar(db.conexao, agora=segunda + dt.timedelta(seconds=5), enviar=enviar)
    r = rondas.rodar(db.conexao, agora=segunda + dt.timedelta(minutes=11), enviar=enviar)
    assert r["perdidas"] == 1
    aviso = [e for e in enviados if e["finalidade"] == "ponto.ronda_perdida"]
    assert len(aviso) == 1 and aviso[0]["telefone"] == "5585988887777" and "Ronda perdida" in aviso[0]["mensagem"]

    # A saída às 05:20 cancela o que falta
    bater_via_chave(app, CPF_JOAO, local(ter, 5, 20))
    with db.conexao() as conn:
        st = {x["numero"]: x["status"] for x in db.todos(conn, "SELECT numero, status FROM ponto.rondas")}
    assert st[1] == "FEITA" and st[2] == "PERDIDA"
    assert st[3] == st[4] == "CANCELADA"

    # A tela Rondas e o espelho mostram a noite
    noite = dp.get(f"/erp/api/ponto/rondas?dia={seg.isoformat()}").get_json()
    assert noite["disponivel"] and len(noite["turnos"]) == 1
    t = noite["turnos"][0]
    assert t["nome"].startswith("João") and t["entrada"] == "17:30" and t["saida"] == "05:20"
    assert t["feitas"] == 1 and t["perdidas"] == 1
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    dia = next(d for d in esp["dias"] if d["data"] == seg.isoformat())
    assert [x["status"] for x in dia["rondas"]][:2] == ["FEITA", "PERDIDA"]
    # a foto da ronda feita
    rid = next(x["id"] for x in t["rondas"] if x["status"] == "FEITA")
    assert dp.get(f"/erp/api/ponto/rondas/{rid}/foto").status_code in (200, 404)


def test_sem_entrada_nao_ha_ronda_e_ronda_atrasada_do_servico_fora(app, mundo, banco):
    """Folga ou falta: sem entrada, nada é agendado. E a ronda que passou da hora com
    o serviço fora do ar não sai depois (meia hora atrasada já não é surpresa)."""
    from app.apps.ponto import db
    from app.apps.ponto.core import rondas
    _vigia(app, mundo, mundo["joao"])
    seg = _segunda_passada()
    r = rondas.rodar(db.conexao, agora=dt.datetime.combine(seg, dt.time(23, 0), tzinfo=dt.timezone.utc),
                     enviar=lambda **kw: {"whatsapp": {"ok": True}})
    assert r["enviadas"] == 0
    bater_via_chave(app, CPF_JOAO, local(seg, 18, 0))
    with db.conexao() as conn:
        ultima = db.um(conn, "SELECT max(agendada_para) AS u FROM ponto.rondas")["u"]
    r = rondas.rodar(db.conexao, agora=ultima + dt.timedelta(hours=1), enviar=lambda **kw: {"whatsapp": {"ok": True}})
    assert r["enviadas"] == 0 and r["nao_enviadas"] == 4


def test_aparelho_do_vigia_avisa_e_marca(app, mundo, monkeypatch):
    from tests.test_ponto_regras_0610_banco import _aparelho, _id_do_aparelho
    _entrar_no_app(app, CPF_JOAO, monkeypatch)
    dp = como(app, mundo["dp"])
    _, _h = _aparelho(app, "celular-do-vigia-joao-0123456789")
    ap = _id_do_aparelho(dp, "celular-do-vigia-joao-0123456789")
    assert dp.post(f"/erp/api/ponto/dispositivos/{ap}/aprovar",
                   json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO, "faz_ronda": True}).status_code == 200
    assert dp.get(f"/erp/api/ponto/dispositivos/{ap}").get_json()["dispositivo"]["dono_faz_ronda"] is True
    assert dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]["faz_ronda"] is True


def test_o_ajuste_do_plantao_dobrado_cancela_as_rondas_da_falsa_entrada(app, mundo, banco):
    """05/11/2026, o dono: o vigia dobrou o plantão e bateu só as 18:01 no dia de descanso; o sistema entendeu a
    entrada de uma noite e mandou 4 rondas, todas perdidas. Aprovada a entrada das 6:01, as 18:01 são a saída — as
    rondas que ela abriu são canceladas (as da noite de verdade ficam)."""
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    dp = _vigia(app, mundo, mundo["joao"])
    seg = _segunda_passada()
    ter = seg + dt.timedelta(days=1)
    with db.conexao() as conn:
        noite = escalas.gravar(conn, {"nome": "Vigia noturno 12x36", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "06:00"})
        escalas.atribuir(conn, mundo["joao"], noite["id"], seg.isoformat(), por="teste")
    bater_via_chave(app, CPF_JOAO, local(seg, 17, 0))
    bater_via_chave(app, CPF_JOAO, local(ter, 6, 1))
    bater_via_chave(app, CPF_JOAO, local(ter, 18, 1))           # parece a entrada de uma noite: sorteia rondas

    def rondas_de(dia):
        with banco.connect() as conn:
            return [r[0] for r in conn.execute(text("SELECT status FROM ponto.rondas WHERE colaborador_id = :c "
                                                    "AND data_referencia = :d"), {"c": mundo["joao"], "d": dia})]
    assert rondas_de(ter) and "CANCELADA" not in rondas_de(ter)
    antes_seg = rondas_de(seg)
    r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"],
                "data_inicio": ter.isoformat(), "horario": f"{ter.isoformat()}T06:01:00", "obra": "PG-A",
                "descricao": "dobrou o plantão até as 18h"})
    assert r.status_code == 201, r.get_json()
    sup = como(app, mundo["sup"])
    for _ in range(4):
        p = dp.get(f"/erp/api/ponto/ocorrencias/{r.get_json()['ocorrencia']['id']}").get_json()["pedido"]
        if p["status"] == "APROVADA":
            break
        cli = sup if p["status"] == "AGUARDANDO_SUPERVISOR" else dp
        assert cli.post(p["decidir_em"], json={"aprovar": True, "motivo": ""}).status_code == 200
    assert p["status"] == "APROVADA"
    assert set(rondas_de(ter)) == {"CANCELADA"}
    assert rondas_de(seg) == antes_seg                          # a noite de verdade não muda


def test_a_ronda_nao_sai_depois_da_hora_de_sair(app, mundo, banco):
    """06/11/2026, o dono: "um vigia recebeu a mensagem da ronda às 7 horas da manhã, num horário que ele já tinha
    saído". As rondas ficam dentro do turno da ESCALA (17h–6h), mesmo com a entrada tarde; e a ronda cujo vigia já
    bateu a saída — que chegou por ajuste, ou do ponto da obra sem internet — não sai mais."""
    from app.apps.ponto import db
    from app.apps.ponto.core import rondas
    dp = _vigia(app, mundo, mundo["joao"])
    seg = _segunda_passada()
    ter = dia_util(seg, 1)
    esc = dp.post("/erp/api/ponto/escalas", json={"nome": "Vigia noite 17-06", "tipo": "SEMANAL",
                  "semana": {str(d): [["17:00", "06:00"]] for d in range(7)}}).get_json()
    assert esc.get("ok"), esc
    eid = esc.get("escala", esc).get("id")
    r = dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/escala",
                json={"escala_id": eid, "vigencia_inicio": (seg - dt.timedelta(days=7)).isoformat()})
    assert r.status_code == 200, r.get_json()
    bater_via_chave(app, CPF_JOAO, local(seg, 20, 0))                    # entrou tarde
    with db.conexao() as conn:
        lista = db.todos(conn, "SELECT * FROM ponto.rondas ORDER BY numero")
    assert len(lista) == 4 and lista[-1]["agendada_para"] <= local(ter, 5, 0)       # a última, 1 h antes das 6h
    # a saída chegou sem cancelar as rondas (por ajuste, ou do ponto da obra sem internet): a ronda devida não sai
    import app.apps.ponto.core.rondas as modulo
    original, modulo.ao_bater = modulo.ao_bater, (lambda *a, **kw: "")
    try:
        bater_via_chave(app, CPF_JOAO, local(seg, 23, 0))
    finally:
        modulo.ao_bater = original
    enviados = []
    r = rondas.rodar(db.conexao, agora=lista[-1]["agendada_para"] + dt.timedelta(seconds=5),
                     enviar=lambda **kw: enviados.append(kw) or {"whatsapp": {"ok": True}})
    assert r["enviadas"] == 0 and not [e for e in enviados if e["finalidade"] == "ponto.ronda"]
    with db.conexao() as conn:
        motivos = {x["motivo"] for x in db.todos(conn, "SELECT motivo FROM ponto.rondas WHERE status = 'CANCELADA'")}
    assert "o vigia já tinha batido a saída" in motivos
