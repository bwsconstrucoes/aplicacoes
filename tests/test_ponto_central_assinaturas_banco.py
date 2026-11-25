# -*- coding: utf-8 -*-
"""Ponto — a central de assinaturas pelo aplicativo do ponto (23/11/2026, o dono: "vamos concentrar o aplicativo do
ponto para ser o caminho para a coleta dessas assinaturas"). A pessoa vê e assina os dela; o administrativo conduz os
da equipe (na tela, no papel, a recusa); o DP vê tudo, valida o papel e a recusa; a ficha mostra o histórico."""
from __future__ import annotations

import pytest

from tests.test_ponto_assinatura_folha_banco import _png
from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _entrar_no_app, _schema_ponto2, app,  # noqa: F401
                                           bater_via_chave, como, local, mundo)
from tests.test_ponto_papeis_banco import CPF_CARLOS, NA_OBRA_A, OBRA_A, _papeis, carlos  # noqa: F401
from tests.test_ponto_recesso_lote_banco import _folga_e_sabado
from tests.test_ponto_termo_banco import _jpeg

pytestmark = pytest.mark.banco


@pytest.fixture()
def sem_drive(monkeypatch):
    """O Arquivo do ERP guarda o PDF fora (o Drive): aqui só se anota o que iria."""
    from app.apps.erp.core.arquivo import service as svc_arq
    guardado = []

    class Doc:
        id = None

    def arquivar(s_, conteudo, nome, **kw):
        guardado.append({"nome": nome, "tipo": kw.get("tipo_codigo"), "observacao": kw.get("observacao")})
        return Doc()
    monkeypatch.setattr(svc_arq, "arquivar", arquivar)
    return guardado


def _termo_de(dp, mundo, cid):
    """Um documento do DP para a pessoa: o termo de compensação, que nasce da compensação lançada."""
    folga, sab = _folga_e_sabado()
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "COMPENSACAO", "colaborador_id": cid, "data_inicio": folga.isoformat(),
        "dia_trabalhado": sab.isoformat(), "descricao": "folga combinada", "aprovar_ja": True})
    assert r.status_code == 201, r.get_json()


def test_a_pessoa_ve_e_assina_o_dela_pelo_meu_ponto(app, mundo, sem_drive, monkeypatch):
    import datetime as dt
    dp = como(app, mundo["dp"])
    bater_via_chave(app, CPF_JOAO, local(dt.date.today(), 7, 0))
    _termo_de(dp, mundo, mundo["joao"])
    _termo_de(dp, mundo, mundo["maria"])
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    d = joao.get("/ponto/app/api/meus-documentos").get_json()
    assert [x["titulo"] for x in d["documentos"]] == ["Termo de acordo individual de compensação de jornada"], d
    tid = d["documentos"][0]["id"]
    doc = joao.get(f"/ponto/app/api/meus-documentos/{tid}").get_json()["documento"]
    assert "Cláusula 1ª" in doc["texto"] and doc["termo_de_ciencia"]
    # o da Maria não é dele: "não encontrado", nem para ler nem para assinar
    da_maria = dp.get("/erp/api/ponto/assinaturas-central").get_json()["documentos"]["abertos"]
    outro = next(x["id"] for x in da_maria if x["colaborador_id"] == mundo["maria"])
    assert joao.get(f"/ponto/app/api/meus-documentos/{outro}").status_code == 404
    assert joao.post(f"/ponto/app/api/meus-documentos/{outro}", json={"pin": "481927", "assinatura": _png()}
                     ).status_code == 404
    # PIN errado não assina; o certo assina, e o PDF vai ao Arquivo dizendo que foi a própria pessoa
    assert joao.post(f"/ponto/app/api/meus-documentos/{tid}", json={"pin": "000000", "assinatura": _png()}
                     ).status_code == 400
    r = joao.post(f"/ponto/app/api/meus-documentos/{tid}", json={"pin": "481927", "assinatura": _png()})
    assert r.status_code == 201, r.get_json()
    assert sem_drive[-1]["tipo"] == "TERMO-COMPENSACAO" and "própria pessoa" in sem_drive[-1]["observacao"]
    d = joao.get("/ponto/app/api/meus-documentos").get_json()
    assert d["documentos"] == [] and d["assinados"][0]["rotulo"] == "assinado"
    # a ficha e o DP veem
    ficha = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/assinaturas").get_json()
    assert ficha["documentos"][0]["vale"] is True
    feitos = dp.get("/erp/api/ponto/assinaturas-central").get_json()["documentos"]["feitos"]
    assert any(x["id"] == tid and x["forma"] == "NA_TELA" for x in feitos)


def test_o_administrativo_conduz_na_tela_no_papel_e_a_recusa(app, mundo, carlos, sem_drive, monkeypatch):
    import datetime as dt
    dp = como(app, mundo["dp"])
    _papeis(app, mundo, carlos, administrativo_obra=True)
    hoje = dt.date.today()
    bater_via_chave(app, CPF_JOAO, local(hoje, 7, 0))
    bater_via_chave(app, CPF_MARIA, local(hoje, 7, 0))
    _termo_de(dp, mundo, mundo["joao"])
    _termo_de(dp, mundo, mundo["maria"])
    _entrar_no_app(app, CPF_JOAO, monkeypatch)                  # o João cria o PIN dele
    adm = _entrar_no_app(app, CPF_CARLOS, monkeypatch)
    aqui = {"latitude": OBRA_A[0], "longitude": OBRA_A[1], "precisao": 10}
    lista = adm.get(f"/ponto/app/api/equipe/documentos?{NA_OBRA_A}").get_json()["documentos"]
    por_nome = {x["pessoa"]: x["id"] for x in lista}
    assert set(por_nome) == {"João Obra A", "Maria Obra B"}, lista
    url = "/ponto/app/api/equipe/documentos/{}?" + NA_OBRA_A
    assert "Cláusula" in adm.get(url.format(por_nome["João Obra A"])).get_json()["documento"]["texto"]
    # na tela: o PIN DA PESSOA (o do João), não o de quem conduz
    r = adm.post(url.format(por_nome["João Obra A"]), json={"acao": "assinar", "pin": "481927", "assinatura": _png(), **aqui})
    assert r.status_code == 201, r.get_json()
    # no papel: a foto; vale só depois do DP
    r = adm.post(url.format(por_nome["Maria Obra B"]), json={"acao": "papel", "foto": _jpeg(), **aqui})
    assert r.status_code == 201, r.get_json()
    central = dp.get("/erp/api/ponto/assinaturas-central").get_json()["documentos"]
    validar = [x for x in central["validar"] if x["colaborador_id"] == mundo["maria"]]
    assert len(validar) == 1 and validar[0]["forma"] == "PAPEL"
    # o DP devolve (foto ruim): volta a ficar aberto para a obra, dizendo por quê
    v = f"/erp/api/ponto/assinaturas-central/documentos/{validar[0]['id']}/validar"
    assert dp.post(v, json={"aceitar": False, "motivo": "x"}).status_code == 400
    assert dp.post(v, json={"aceitar": False, "motivo": "a foto está cortada"}).status_code == 200
    lista = adm.get(f"/ponto/app/api/equipe/documentos?{NA_OBRA_A}").get_json()["documentos"]
    assert [x["devolvido"] for x in lista if x["pessoa"] == "Maria Obra B"] == [True]
    # a recusa: duas testemunhas; o DP aceita, e vale
    r = adm.post(url.format(por_nome["Maria Obra B"]), json={"acao": "recusa", "motivo": "não quis assinar",
                                                             "testemunhas": [{"nome": "Ana Souza"}], **aqui})
    assert r.status_code == 400
    # 25/11/2026: só o nome não basta mais — cada testemunha tem CPF e assina com o dedo
    r = adm.post(url.format(por_nome["Maria Obra B"]), json={"acao": "recusa", "motivo": "não quis assinar",
                                                             "testemunhas": [{"nome": "Ana Souza"}, {"nome": "Rui Lima"}], **aqui})
    assert r.status_code == 400 and "CPF" in r.get_json()["erro"]
    r = adm.post(url.format(por_nome["Maria Obra B"]), json={"acao": "recusa", "motivo": "não quis assinar",
                                                             "testemunhas": [
                                                                 {"nome": "Ana Souza", "cpf": "714.602.380-01", "assinatura": _png()},
                                                                 {"nome": "Rui Lima", "cpf": "286.255.878-87", "assinatura": _png()}],
                                                             **aqui})
    assert r.status_code == 201, r.get_json()
    assert dp.post(v, json={"aceitar": True}).status_code == 200
    central = dp.get("/erp/api/ponto/assinaturas-central").get_json()["documentos"]
    assert central["abertos"] == [] and central["validar"] == []
    assert {x["rotulo"] for x in central["feitos"]} == {"assinado", "recusa registrada"}
    # quem não alcança não vê; e o supervisor não valida
    sup = como(app, mundo["sup"])
    assert sup.post(v, json={"aceitar": True}).status_code in (400, 403, 404)


def test_a_tela_de_assinaturas_e_a_ficha_abrem(app, mundo):
    dp = como(app, mundo["dp"])
    assert dp.get("/erp/ponto/assinaturas").status_code == 200
    d = dp.get("/erp/api/ponto/assinaturas-central").get_json()
    assert d["ok"] and {"documentos", "folhas", "termo_do_ponto"} <= set(d)
    assert "faltam" in d["folhas"]
    f = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/assinaturas").get_json()
    assert f["ok"] and f["documentos"] == []


def test_a_pessoa_assina_a_propria_folha_do_mes_fechado(app, mundo, monkeypatch):
    from tests.test_ponto_assinatura_folha_banco import _dia_do_mes_passado
    dp = como(app, mundo["dp"])
    dia = _dia_do_mes_passado()
    comp = dia.strftime("%Y-%m")
    for h in (7, 11, 12, 17):
        bater_via_chave(app, CPF_JOAO, local(dia, h, 0))
    joao = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    # o mês aberto: a folha não aparece para assinar
    assert all(x["competencia"] != comp for x in joao.get("/ponto/app/api/meus-documentos").get_json()["folhas"])
    assert dp.post(f"/erp/api/ponto/competencias/{comp}/fechar").status_code == 200
    d = joao.get("/ponto/app/api/meus-documentos").get_json()
    assert [x["competencia"] for x in d["folhas"]] == [comp], d
    f = joao.get(f"/ponto/app/api/minha-folha?competencia={comp}").get_json()
    assert any(x["batidas"] == ["07:00", "11:00", "12:00", "17:00"] for x in f["dias"])
    assert joao.post("/ponto/app/api/minha-folha", json={"competencia": comp, "pin": "000000", "assinatura": _png()}
                     ).status_code == 401
    r = joao.post("/ponto/app/api/minha-folha", json={"competencia": comp, "pin": "481927", "assinatura": _png()})
    assert r.status_code == 201, r.get_json()
    assert joao.get("/ponto/app/api/meus-documentos").get_json()["folhas"] == []
    ficha = dp.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/assinaturas").get_json()
    assert ficha["folhas"][0]["competencia"] == comp and ficha["folhas"][0]["situacao"] == "ASSINADA"
    assert "própria pessoa" in ficha["folhas"][0]["por"] or ficha["folhas"][0]["por"] == "João Obra A"
    folhas = dp.get(f"/erp/api/ponto/assinaturas-central?competencia={comp}").get_json()["folhas"]
    assert folhas["assinadas"] == 1 and all(x["id"] != mundo["joao"] for x in folhas["faltam"])
