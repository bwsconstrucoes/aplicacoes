# -*- coding: utf-8 -*-
"""Ponto — a batida SEM INTERNET no ponto da obra (16/10/2026): a lista que o
aparelho guarda, e a batida guardada que chega depois — com a hora de quando a
pessoa bateu, a de chegada à parte, sem duplicar, e para conferência quando a
hora não está comprovada ou o CPF não foi reconhecido. Contra um Postgres DE
VERDADE, com o mesmo mundo de `test_ponto_gestao_banco.py`."""
from __future__ import annotations

import datetime as dt
import hashlib

import pytest

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _schema_ponto2,  # noqa: F401
                                           app, como, mundo)
from tests.test_ponto_qr_banco import _foto, _tablet

pytestmark = pytest.mark.banco

URL_LISTA = "/ponto/app/api/tablet/lista-sem-internet"
URL_BATER = "/ponto/app/api/tablet/bater-sem-internet"


def _iso(momento: dt.datetime) -> str:
    return momento.astimezone(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _guardada(pessoa_id=None, *, ha_minutos=30, comprovada=True, cpf=None, ident="CPF", n="1"):
    agora = dt.datetime.now(dt.timezone.utc)
    momento = agora - dt.timedelta(minutes=ha_minutos)
    corpo = {"id": f"batida-guardada-{n}-0123456789", "identificacao": ident, "obra": "PG-A",
             "momento": _iso(momento), "hora_comprovada": comprovada, "relogio_aparelho": _iso(momento),
             "sincronizado_em": _iso(momento - dt.timedelta(minutes=5)), "foto_base64": _foto(),
             "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 12}
    if pessoa_id:
        corpo["colaborador_id"] = pessoa_id
    if cpf:
        corpo["cpf"] = cpf
    return corpo, momento


def test_a_lista_tem_quem_bate_ali_com_impressoes_que_conferem(app, mundo, banco):
    """O nome e as impressões digitais do CPF e do QR — e só de quem bate ali:
    João (obra A) entra; Maria (obra B) não, até bater nele."""
    from app.apps.ponto import db
    from app.apps.ponto.core import qr
    t, h, _ = _tablet(app, mundo)
    with db.conexao() as conn:
        qr.gravar_enviado(conn, mundo["joao"], "BWSP1.qr-do-joao-para-o-teste", "INICIAL")
    assert app.test_client().get(URL_LISTA).status_code in (401, 403)       # só o aparelho
    r = t.get(URL_LISTA, headers=h)
    assert r.status_code == 200, r.get_json()
    lista = r.get_json()
    pessoas = {p["id"]: p for p in lista["pessoas"]}
    assert mundo["joao"] in pessoas and mundo["maria"] not in pessoas
    joao = pessoas[mundo["joao"]]
    assert joao["c"] == hashlib.sha256(f"bwsp-cpf|{CPF_JOAO}".encode()).hexdigest()
    assert hashlib.sha256(b"BWSP1.qr-do-joao-para-o-teste").hexdigest() in joao["q"]
    assert CPF_JOAO not in str(lista)                                   # o CPF não vai aberto
    assert dt.datetime.fromisoformat(lista["agora"].replace("Z", "+00:00"))


def test_batida_guardada_chega_com_a_hora_de_quando_bateu_e_nao_duplica(app, mundo, banco):
    t, h, disp_id = _tablet(app, mundo)
    corpo, momento = _guardada(mundo["joao"], ha_minutos=40)
    r = t.post(URL_BATER, json=corpo, headers=h)
    assert r.status_code == 201, r.get_json()
    d = r.get_json()
    assert d["status"] == "VALIDA" and d["ja_existia"] is False, d
    # mandou de novo (a resposta se perdeu): devolve a mesma, sem outra batida
    r2 = t.post(URL_BATER, json=corpo, headers=h)
    assert r2.status_code == 200 and r2.get_json()["ja_existia"] is True
    assert r2.get_json()["comprovante"]["nsr"] == d["comprovante"]["nsr"]
    from sqlalchemy import text
    with banco.connect() as conn:
        linhas = conn.execute(text("SELECT timestamp_servidor, recebida_em, sem_internet, dispositivo_id "
                                   "FROM ponto.marcacoes WHERE colaborador_id = :c"), {"c": mundo["joao"]}).all()
    assert len(linhas) == 1
    ts, recebida, sem, disp = linhas[0]
    assert sem is True and disp == disp_id
    assert abs((ts - momento).total_seconds()) < 1                      # a hora de quando bateu
    assert (recebida - ts).total_seconds() > 30 * 60                    # chegou depois
    # o espelho mostra
    dp = como(app, mundo["dp"])
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}").get_json()
    b = next(x for dia in esp["dias"] for x in dia["batidas"])
    assert b["sem_internet"] is True and b["recebida_em"]


def test_hora_nao_comprovada_ou_cpf_desconhecido_vao_para_conferencia(app, mundo, banco):
    t, h, _ = _tablet(app, mundo)
    corpo, momento = _guardada(mundo["joao"], ha_minutos=90, comprovada=False, n="2")
    corpo["hora_pelo_contador"] = _iso(momento - dt.timedelta(hours=1))
    d = t.post(URL_BATER, json=corpo, headers=h).get_json()
    assert d["status"] == "EM_ANALISE" and "não pôde ser comprovada" in d["motivo_analise"]
    assert "pela contagem interna do aparelho" in d["motivo_analise"]
    # o CPF que o aparelho não reconheceu (Maria não estava na lista): bate, para conferência
    corpo, _ = _guardada(cpf=CPF_MARIA, ident="CPF_NAO_RECONHECIDO", ha_minutos=20, n="3")
    r = t.post(URL_BATER, json=corpo, headers=h)
    assert r.status_code == 201, r.get_json()
    assert r.get_json()["status"] == "EM_ANALISE" and "não reconheceu" in r.get_json()["motivo_analise"]


def test_hora_do_futuro_fica_a_de_chegada(app, mundo, banco):
    t, h, _ = _tablet(app, mundo)
    corpo, _ = _guardada(mundo["joao"], ha_minutos=-120, n="4")       # 2 h "no futuro"
    d = t.post(URL_BATER, json=corpo, headers=h).get_json()
    assert d["status"] == "EM_ANALISE" and "no futuro" in d["motivo_analise"]


def test_guardada_antiga_nao_e_tomada_por_repetida_de_uma_recente(app, mundo, banco):
    """A repetição olha perto da hora da batida: a entrada guardada de 3 h atrás
    não pode virar "já registrada" por causa de uma batida de agora."""
    from app.apps.ponto import db
    from app.apps.ponto.core import marcacoes
    with db.conexao() as conn:
        marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True)
    t, h, _ = _tablet(app, mundo)
    corpo, _ = _guardada(mundo["joao"], ha_minutos=180, n="5")
    r = t.post(URL_BATER, json=corpo, headers=h)
    assert r.status_code == 201 and r.get_json()["ja_existia"] is False


def test_pessoa_afastada_guardada_e_recusada_na_chegada(app, mundo, banco):
    from sqlalchemy import text
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET situacao = 'AFASTADO' WHERE id = :j"), {"j": mundo["joao"]})
        conn.commit()
    t, h, _ = _tablet(app, mundo)
    assert mundo["joao"] not in {p["id"] for p in t.get(URL_LISTA, headers=h).get_json()["pessoas"]}
    corpo, _ = _guardada(cpf=CPF_JOAO, ident="CPF_NAO_RECONHECIDO", n="6")
    r = t.post(URL_BATER, json=corpo, headers=h)
    assert r.status_code == 403 and "afastad" in r.get_json()["erro"]


def test_decidir_hora_pura():
    from app.apps.ponto.core.sem_internet import decidir_hora
    agora = dt.datetime(2026, 10, 16, 12, 0, tzinfo=dt.timezone.utc)
    m = agora - dt.timedelta(hours=1)
    assert decidir_hora(momento=m, comprovada=True, sincronizado_em=m - dt.timedelta(minutes=10),
                        recebida_em=agora) == (m, None)
    hora, motivo = decidir_hora(momento=m, comprovada=True, sincronizado_em=m + dt.timedelta(minutes=30),
                                recebida_em=agora)
    assert hora == m and "anterior à última conexão" in motivo
    hora, motivo = decidir_hora(momento=None, comprovada=True, sincronizado_em=None, recebida_em=agora)
    assert hora == agora and motivo


def test_hora_comprovada_mas_suspeita_vai_para_conferencia(app, mundo, banco):
    """A revisão de 16/10/2026: quem mexe no aparelho forjaria "ontem às 7h". Se o
    aparelho bateu com internet depois, a guardada devia ter subido ali; e a que
    chega mais de um dia depois vai para conferência."""
    t, h, _ = _tablet(app, mundo)
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_MARIA, "obra": "PG-A"}, headers=h).get_json()
    online = t.post("/ponto/app/api/bater", headers=h, json={"bilhete": ident["bilhete"], "obra": "PG-A",
                    "foto_base64": _foto(), "latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10})
    assert online.status_code == 201, online.get_json()
    # a "guardada" de 2 h atrás, com o aparelho tendo batido com internet agora há pouco… mas a online é de AGORA,
    # dentro da folga da reconexão: não acusa
    corpo, _ = _guardada(mundo["joao"], ha_minutos=120, n="7")
    assert t.post(URL_BATER, json=corpo, headers=h).get_json()["status"] == "VALIDA"
    # a online de 10 min atrás (fora da folga) acusa a guardada de 2 h atrás
    from sqlalchemy import text
    with banco.connect() as conn:
        conn.execute(text("UPDATE ponto.marcacoes SET timestamp_servidor = now() - interval '10 minutes' "
                          "WHERE colaborador_id = :m AND NOT sem_internet"), {"m": mundo["maria"]})
        conn.commit()
    corpo, _ = _guardada(mundo["joao"], ha_minutos=125, n="8")
    d = t.post(URL_BATER, json=corpo, headers=h).get_json()
    assert d["status"] == "EM_ANALISE" and "bateu com internet" in d["motivo_analise"], d
    # chegou mais de um dia depois (num aparelho que não bateu com internet desde então)
    t2, h2, _ = _tablet(app, mundo, uuid="tablet-sem-internet-outro-0123456789")
    corpo, _ = _guardada(mundo["joao"], ha_minutos=26 * 60, n="9")
    d = t2.post(URL_BATER, json=corpo, headers=h2).get_json()
    assert d["status"] == "EM_ANALISE" and "dia(s)" in d["motivo_analise"], d
