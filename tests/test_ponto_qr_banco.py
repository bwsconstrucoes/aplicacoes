# -*- coding: utf-8 -*-
"""Ponto — o tablet da obra identificando por QR Code ou CPF, o QR pessoal pelo
WhatsApp (troca, convivência e QR antigo), a fila com ritmo, o mosaico
obrigatório e os sinais de fraude — contra um Postgres DE VERDADE.

Reusa as fixtures de `test_ponto_gestao_banco.py` (o mesmo mundo: obra A com
João, obra B com Maria, supervisor da A, DP e financeiro)."""
from __future__ import annotations

import base64
import datetime as dt
import io

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, OBRA_A, _schema_ponto2,  # noqa: F401
                                           app, como, local, mundo)

pytestmark = pytest.mark.banco


def _hoje() -> dt.date:
    """O dia de Fortaleza (o do servidor do teste pode já ter virado em UTC)."""
    from app.apps.ponto import horario
    return horario.hoje()


def _foto(cor=(200, 150, 100), listras=True) -> str:
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (120, 160), cor)
    if listras:
        d = ImageDraw.Draw(img)
        for x in range(0, 120, 12):
            d.rectangle([x, 0, x + 5, 160], fill=(30, 30, 30))
    s = io.BytesIO()
    img.save(s, format="JPEG")
    return "data:image/jpeg;base64," + base64.b64encode(s.getvalue()).decode()


def _tablet(app, mundo, uuid="tablet-qr-obra-a-0123456789abcd"):
    t = app.test_client()
    token = t.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    dp = como(app, mundo["dp"])
    disp = next(d for d in dp.get("/erp/api/ponto/dispositivos?status=PENDENTE").get_json()["dispositivos"]
                if d["device_uuid"] == uuid)
    r = dp.post(f"/erp/api/ponto/dispositivos/{disp['id']}/aprovar",
                json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "descricao": "Tablet da Escola A",
                      "cpf": CPF_MARIA})
    assert r.status_code == 200, r.get_json()
    return t, {"X-Device-UUID": uuid, "X-Device-Token": token}, disp["id"]


def _mandar_um(monkeypatch, token=None):
    """Processa um envio da fila com um WhatsApp falso. Devolve o que 'saiu'."""
    from app.apps.ponto.core import envios, qr
    saiu = []
    if token:
        monkeypatch.setattr(qr, "novo_token", lambda: token)
    sabado_10h = dt.datetime.now(dt.timezone.utc)
    situacao = envios.processar_um(enviar=lambda **kw: (saiu.append(kw), {"whatsapp": {"ok": True}})[1],
                                   agora=sabado_10h)
    return situacao, saiu


@pytest.fixture(autouse=True)
def _janela_sempre_aberta(monkeypatch):
    """O teste roda a qualquer hora; a janela do envio não pode depender disso."""
    from app.apps.ponto.core import envios
    monkeypatch.setattr(envios, "dentro_da_janela", lambda tipo, motivo, momento: True)


# ---------------------------------------------------------------------------
# Tablet: CPF e bilhete
# ---------------------------------------------------------------------------
def test_tablet_identifica_por_cpf_e_bate_com_bilhete_e_foto(app, mundo, banco):
    t, h, disp_id = _tablet(app, mundo)
    r = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h)
    assert r.status_code == 200, r.get_json()
    ident = r.get_json()
    assert ident["nome"] == "João Obra A" and ident["identificacao"] == "CPF" and ident["bilhete"]
    b = t.post("/ponto/app/api/bater", json={"bilhete": ident["bilhete"], "obra": "PG-A", "foto_base64": _foto(),
                                             "latitude": OBRA_A[0], "longitude": OBRA_A[1]}, headers=h)
    assert b.status_code == 201, b.get_json()
    assert b.get_json()["status"] == "VALIDA" and b.get_json()["comprovante"]["empregado"] == "João Obra A"
    with banco.connect() as conn:
        m = conn.execute(text("SELECT identificacao, foto_id FROM ponto.marcacoes")).one()
        f = conn.execute(text("SELECT luminancia, contraste, dhash, conteudo IS NOT NULL FROM ponto.fotos")).one()
    assert m[0] == "CPF" and m[1] is not None
    assert f[0] > 30 and f[1] > 10 and len(f[2]) == 64 and f[3] is True     # na sala de espera


def test_tablet_recusa_cpf_desconhecido_celular_pessoal_e_bilhete_alheio(app, mundo, banco):
    t, h, disp_id = _tablet(app, mundo)
    r = t.post("/ponto/app/api/tablet/identificar", json={"cpf": "39053344705", "obra": "PG-A"}, headers=h)
    assert r.status_code == 403 and "não encontrado" in r.get_json()["erro"]
    with banco.connect() as conn:
        assert conn.execute(text("SELECT motivo FROM ponto.recusas")).scalar() == "CPF não identificado no tablet"
    # Maria é da obra B; o tablet só vale na obra A
    r = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_MARIA, "obra": "PG-B"}, headers=h)
    assert r.status_code == 403 and "não vale nesta obra" in r.get_json()["erro"]
    # bilhete emitido para outro aparelho não bate aqui
    from app.apps.ponto.core import qr
    with app.test_request_context():
        alheio = qr.emitir_bilhete(mundo["joao"], disp_id + 99, "CPF")
    r = t.post("/ponto/app/api/bater", json={"bilhete": alheio, "obra": "PG-A"}, headers=h)
    assert r.status_code == 400 and "outro aparelho" in r.get_json()["erro"]
    # sem credencial de aparelho, nem identifica
    assert app.test_client().post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO}).status_code == 401


def test_tablet_sem_foto_vai_para_analise(app, mundo):
    t, h, _ = _tablet(app, mundo)
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    b = t.post("/ponto/app/api/bater", json={"bilhete": ident["bilhete"], "obra": "PG-A",
                                             "latitude": OBRA_A[0], "longitude": OBRA_A[1]}, headers=h)
    assert b.status_code == 201 and b.get_json()["status"] == "EM_ANALISE"
    assert "sem foto no aparelho da obra" in b.get_json()["motivo_analise"]


# ---------------------------------------------------------------------------
# QR pelo WhatsApp: envio, troca e QR antigo
# ---------------------------------------------------------------------------
def test_qr_do_whatsapp_troca_convive_e_o_antigo_e_recusado(app, mundo, banco, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import alertas, envios
    t, h, _ = _tablet(app, mundo)
    dp = como(app, mundo["dp"])
    r = dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/qr/enviar")
    assert r.status_code == 200 and r.get_json()["qr"]["na_fila"]["motivo"] == "GESTAO"
    situacao, saiu = _mandar_um(monkeypatch, token="BWSP1.primeiro-qr-do-joao-0001")
    assert situacao == "ENVIADO" and saiu[0]["telefone"] == "5585999990000"
    assert saiu[0]["tipo"] == "imagem" and base64.b64decode(saiu[0]["arquivo_base64"])[:2] == b"\xff\xd8"
    assert "QR Code" in saiu[0]["mensagem"] and "não passe para ninguém" in saiu[0]["mensagem"]
    with banco.connect() as conn:
        guardado = conn.execute(text("SELECT token_hash FROM ponto.qr_codigos")).scalar()
        troca = conn.execute(text("SELECT qr_proxima_troca FROM ponto.colaborador_config "
                                  "WHERE colaborador_id = :c"), {"c": mundo["joao"]}).scalar()
    assert "primeiro-qr" not in guardado                      # só o hash
    assert 6 <= (troca - dt.datetime.now(dt.timezone.utc)).days <= 15

    r = t.post("/ponto/app/api/tablet/identificar", json={"qr": "BWSP1.primeiro-qr-do-joao-0001", "obra": "PG-A"},
               headers=h)
    assert r.status_code == 200 and r.get_json()["identificacao"] == "QR_WHATSAPP"

    # a troca: o novo chega, o antigo ainda vale até o novo ser usado
    with db.conexao() as conn:
        envios.pedir_qr(conn, mundo["joao"], motivo="PEDIDO", por="o próprio João")
    assert _mandar_um(monkeypatch, token="BWSP1.segundo-qr-do-joao-0002")[0] == "ENVIADO"
    assert t.post("/ponto/app/api/tablet/identificar", json={"qr": "BWSP1.primeiro-qr-do-joao-0001"},
                  headers=h).status_code == 200
    assert t.post("/ponto/app/api/tablet/identificar", json={"qr": "BWSP1.segundo-qr-do-joao-0002"},
                  headers=h).status_code == 200
    velho = t.post("/ponto/app/api/tablet/identificar", json={"qr": "BWSP1.primeiro-qr-do-joao-0001"}, headers=h)
    assert velho.status_code == 403 and "foi trocado" in velho.get_json()["erro"]
    desconhecido = t.post("/ponto/app/api/tablet/identificar", json={"qr": "BWSP1.inventado"}, headers=h)
    assert desconhecido.status_code == 403 and "não reconhecido" in desconhecido.get_json()["erro"]

    with db.conexao() as conn:
        alertas.gerar(conn, ate=_hoje())
        abertos = alertas.listar(conn)
    assert any(a["codigo"] == "QR_ANTIGO_USADO" and a["colaborador_id"] == mundo["joao"] for a in abertos)

    # celular perdido: nenhum QR vale mais
    assert dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/qr/revogar").get_json()["revogados"] == 2
    assert t.post("/ponto/app/api/tablet/identificar", json={"qr": "BWSP1.segundo-qr-do-joao-0002"},
                  headers=h).status_code == 403


def test_esqueci_meu_qr_no_tablet_responde_igual_e_poe_na_fila(app, mundo, banco):
    t, h, _ = _tablet(app, mundo)
    a = t.post("/ponto/app/api/tablet/esqueci-qr", json={"cpf": CPF_JOAO}, headers=h)
    b = t.post("/ponto/app/api/tablet/esqueci-qr", json={"cpf": "39053344705"}, headers=h)
    assert a.status_code == b.status_code == 200 and a.get_json()["mensagem"] == b.get_json()["mensagem"]
    with banco.connect() as conn:
        fila = conn.execute(text("SELECT colaborador_id, motivo, prioridade FROM ponto.envios")).all()
    assert fila == [(mundo["joao"], "PEDIDO", 1)]


def test_qr_do_app_muda_e_identifica(app, mundo, monkeypatch):
    import time
    from app.apps.ponto.core import qr
    from tests.test_ponto_gestao_banco import _entrar_no_app
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    r = cel.get("/ponto/app/api/meu-qr")
    assert r.status_code == 200 and r.get_json()["imagem"].startswith("data:image/png;base64,")
    assert 1 <= r.get_json()["troca_em_segundos"] <= 30
    t, h, _ = _tablet(app, mundo)
    with app.test_request_context():
        agora = qr.conteudo_do_app(mundo["joao"])
        velho = qr.conteudo_do_app(mundo["joao"], agora=time.time() - 300)
    assert t.post("/ponto/app/api/tablet/identificar", json={"qr": agora}, headers=h).get_json()["identificacao"] == "QR_APP"
    assert t.post("/ponto/app/api/tablet/identificar", json={"qr": velho}, headers=h).status_code == 403
    assert app.test_client().get("/ponto/app/api/meu-qr").status_code == 401


# ---------------------------------------------------------------------------
# A fila: ligar, espalhar, teto
# ---------------------------------------------------------------------------
def test_envio_automatico_so_com_a_chave_ligada_e_sem_duplicar(app, mundo, banco, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import envios
    with db.conexao() as conn:
        assert envios.planejar(conn)["ligado"] is False
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    assert sup.post("/erp/api/ponto/configuracao", json={"qr_envio_automatico": True}).status_code == 403
    assert dp.post("/erp/api/ponto/configuracao", json={"qr_envio_automatico": True}).status_code == 200
    assert dp.get("/erp/api/ponto/configuracao").get_json()["qr_envio_automatico"] is True
    with db.conexao() as conn:
        r = envios.planejar(conn)
        de_novo = envios.planejar(conn)
    assert r["iniciais"] == 2 and de_novo["iniciais"] == 0
    with banco.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM ponto.envios WHERE motivo = 'INICIAL'")).scalar() == 2
    # teto por hora: com uma mensagem na última hora e teto 1, nada sai
    monkeypatch.setattr(envios, "POR_HORA", 1)
    with banco.connect() as conn:
        conn.execute(text("UPDATE ponto.envios SET agendado_para = now() - interval '1 minute'"))
        conn.commit()
    assert _mandar_um(monkeypatch, token="BWSP1.lote-1")[0] == "ENVIADO"
    assert _mandar_um(monkeypatch, token="BWSP1.lote-2")[0] is None
    # desligar segura na hora o que já estava na fila
    monkeypatch.setattr(envios, "POR_HORA", 40)
    assert dp.post("/erp/api/ponto/configuracao", json={"qr_envio_automatico": False}).status_code == 200
    assert _mandar_um(monkeypatch, token="BWSP1.lote-3")[0] is None
    painel = dp.get("/erp/api/ponto/envios").get_json()
    assert painel["na_ultima_hora"] == 1 and painel["envios"][0]["telefone"].startswith("***")


def test_falha_no_whatsapp_tenta_de_novo_e_nao_deixa_qr_valendo(app, mundo, banco, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import envios
    with db.conexao() as conn:
        envios.pedir_qr(conn, mundo["joao"], motivo="GESTAO", por="teste")
    agora = dt.datetime.now(dt.timezone.utc)
    for _ in range(envios.TENTATIVAS_MAX):
        with banco.connect() as conn:
            conn.execute(text("UPDATE ponto.envios SET agendado_para = now() - interval '1 minute'"))
            conn.commit()
        envios.processar_um(enviar=lambda **kw: {"whatsapp": {"ok": False, "detalhe": "HTTP 500"}}, agora=agora)
    with banco.connect() as conn:
        e = conn.execute(text("SELECT status, tentativas FROM ponto.envios")).one()
        qrs = conn.execute(text("SELECT count(*) FROM ponto.qr_codigos")).scalar()
    assert e == ("FALHOU", envios.TENTATIVAS_MAX) and qrs == 0


# ---------------------------------------------------------------------------
# Mosaico
# ---------------------------------------------------------------------------
def test_mosaico_obrigatorio_avisa_alerta_e_conferir_manda_suspeita_para_analise(app, mundo, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import alertas, marcacoes, mosaico
    dp, sup, fin = como(app, mundo["dp"]), como(app, mundo["sup"]), como(app, mundo["fin"])
    with banco.connect() as conn:
        conn.execute(text("UPDATE usuarios SET telefone = '85977776666' WHERE id = :u"), {"u": mundo["sup"]})
        conn.commit()
    cfg = dp.get("/erp/api/ponto/mosaico/obras").get_json()
    candidato = next(u for u in cfg["usuarios"] if u["id"] == mundo["sup"])
    assert candidato["pode_conferir"] is True and candidato["tem_telefone"] is True
    assert dp.post(f"/erp/api/ponto/mosaico/obras/{mundo['obra_a']}", json={"obrigatorio": True}).status_code == 400
    assert sup.post(f"/erp/api/ponto/mosaico/obras/{mundo['obra_a']}",
                    json={"obrigatorio": True, "responsavel_id": mundo["sup"]}).status_code == 403
    assert dp.post(f"/erp/api/ponto/mosaico/obras/{mundo['obra_a']}",
                   json={"obrigatorio": True, "responsavel_id": mundo["sup"]}).status_code == 200

    ontem = _hoje() - dt.timedelta(days=1)
    with db.conexao() as conn:
        m1, _ = marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True,
                                    agora=local(ontem, 7, 2).astimezone(dt.timezone.utc))
        marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="IDFACE", via_chave=True,
                            agora=local(ontem, 17, 1).astimezone(dt.timezone.utc))
        r = mosaico.preparar_do_dia(conn, ontem)
        assert mosaico.preparar_do_dia(conn, ontem)["abertas"] == 0          # não duplica
    assert r == {"abertas": 1, "avisos": 1}
    with banco.connect() as conn:
        aviso = conn.execute(text("SELECT tipo, telefone, texto FROM ponto.envios")).one()
    assert aviso[0] == "MOSAICO" and aviso[1] == "5585977776666"
    assert f"/erp/ponto/mosaico?obra={mundo['obra_a']}&data={ontem.isoformat()}" in aviso[2]

    tela = sup.get(f"/erp/api/ponto/mosaico?obra={mundo['obra_a']}&data={ontem.isoformat()}").get_json()
    assert tela["totais"]["pessoas"] == 1 and tela["totais"]["batidas"] == 2
    assert tela["conferencia"]["situacao"] == "PENDENTE" and tela["pode_conferir"] is True
    assert fin.get(f"/erp/api/ponto/mosaico?obra={mundo['obra_a']}&data={ontem.isoformat()}").status_code == 403
    assert sup.get(f"/erp/api/ponto/mosaico?obra={mundo['obra_b']}").status_code == 404      # fora do alcance

    # sem conferência até o dia seguinte: alerta
    with db.conexao() as conn:
        alertas.gerar(conn, ate=_hoje() + dt.timedelta(days=1))
        assert any(a["codigo"] == "MOSAICO_PENDENTE" for a in alertas.listar(conn))
    pend = sup.get("/erp/api/ponto/mosaico/pendentes").get_json()["pendentes"]
    assert [p["obra_id"] for p in pend] == [mundo["obra_a"]]

    # conferir com uma foto suspeita
    curta = sup.post("/erp/api/ponto/mosaico/conferir",
                     json={"obra": mundo["obra_a"], "data": ontem.isoformat(),
                           "suspeitas": [{"marcacao_id": m1["id"], "motivo": "x"}]})
    assert curta.status_code == 400
    ok = sup.post("/erp/api/ponto/mosaico/conferir",
                  json={"obra": mundo["obra_a"], "data": ontem.isoformat(), "nota": "um rosto estranho",
                        "suspeitas": [{"marcacao_id": m1["id"], "motivo": "não é o João na foto"}]})
    assert ok.status_code == 200, ok.get_json()
    assert ok.get_json()["conferencia"]["situacao"] == "CONFERIDO"
    assert ok.get_json()["conferencia"]["suspeitas"] == 1
    with banco.connect() as conn:
        st = conn.execute(text("SELECT status, motivo_analise FROM ponto.marcacoes WHERE id = :i"),
                          {"i": m1["id"]}).one()
        decisao = conn.execute(text("SELECT de_status, para_status, usuario_nome FROM ponto.marcacao_decisoes")).one()
        alerta = conn.execute(text("SELECT situacao FROM ponto.alertas WHERE codigo = 'MOSAICO_PENDENTE'")).scalar()
    assert st[0] == "EM_ANALISE" and "foto suspeita no mosaico" in st[1]
    assert decisao == ("VALIDA", "EM_ANALISE", "Supervisor A") and alerta == "RESOLVIDO"
    # quem só vê não confere
    assert fin.post("/erp/api/ponto/mosaico/conferir", json={"obra": mundo["obra_a"]}).status_code == 403


# ---------------------------------------------------------------------------
# Sinais de fraude: foto escura, foto repetida, sem foto e o aviso à pessoa
# ---------------------------------------------------------------------------
def test_foto_escura_repetida_e_sem_foto_viram_alerta_e_aviso(app, mundo, banco, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import alertas, envios, marcacoes, parametros
    t, h, _ = _tablet(app, mundo)
    uuid, token = h["X-Device-UUID"], h["X-Device-Token"]
    ontem = _hoje() - dt.timedelta(days=1)
    mesma = _foto()
    preta = _foto(cor=(0, 0, 0), listras=False)
    with db.conexao() as conn:
        parametros.gravar(conn, envios.AVISO_SEM_FOTO, "1", "teste")

        def bate(hh, foto):
            marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-A", origem="PWA", device_uuid=uuid,
                                device_token=token, latitude=OBRA_A[0], longitude=OBRA_A[1],
                                foto_base64=foto, agora=local(ontem, hh).astimezone(dt.timezone.utc))
        bate(7, mesma)
        bate(11, mesma)          # a mesma imagem de novo
        bate(12, preta)
        bate(17, None)           # sem foto
        alertas.gerar(conn, ate=ontem)
        abertos = {a["codigo"] for a in alertas.listar(conn)}
    assert {"FOTO_REPETIDA", "FOTO_ESCURA", "SEM_FOTO"} <= abertos
    with banco.connect() as conn:
        aviso = conn.execute(text("SELECT tipo, texto FROM ponto.envios WHERE tipo = 'AVISO_FOTO'")).one()
        analise = conn.execute(text("SELECT count(*) FROM ponto.marcacoes WHERE status = 'EM_ANALISE'")).scalar()
    assert "sem foto" in aviso[1] and "17:00" in aviso[1]
    # A sem foto e, desde 06/10/2026, a foto que não deixa ver ninguém (pedido do
    # dono: "foto que não permite visualização") — esta vai para conferência na hora.
    assert analise == 2


def test_foto_de_cadastro_sai_de_uma_batida_da_propria_pessoa(app, mundo, banco):
    t, h, _ = _tablet(app, mundo)
    ident = t.post("/ponto/app/api/tablet/identificar", json={"cpf": CPF_JOAO, "obra": "PG-A"}, headers=h).get_json()
    b = t.post("/ponto/app/api/bater", json={"bilhete": ident["bilhete"], "obra": "PG-A", "foto_base64": _foto(),
                                             "latitude": OBRA_A[0], "longitude": OBRA_A[1]}, headers=h)
    with banco.connect() as conn:
        mid = conn.execute(text("SELECT id FROM ponto.marcacoes")).scalar()
    sup, fin = como(app, mundo["sup"]), como(app, mundo["fin"])
    assert sup.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/foto-cadastral").status_code == 404
    assert sup.post(f"/erp/api/ponto/pessoas/{mundo['maria']}/foto-cadastral",
                    json={"marcacao_id": mid}).status_code == 404            # Maria é de outra obra
    assert fin.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/foto-cadastral",
                    json={"marcacao_id": mid}).status_code == 403
    assert sup.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/foto-cadastral",
                    json={"marcacao_id": mid}).status_code == 200, b.get_json()
    r = sup.get(f"/erp/api/ponto/pessoas/{mundo['joao']}/foto-cadastral")
    assert r.status_code == 200 and r.data[:2] == b"\xff\xd8"
    from app.apps.ponto import horario
    hoje = sup.get(f"/erp/api/ponto/mosaico?obra={mundo['obra_a']}&data={horario.hoje().isoformat()}")
    assert hoje.get_json()["pessoas"][0]["tem_foto_cadastral"] is True
    mini = sup.get(f"/erp/api/ponto/marcacoes/{mid}/miniatura")
    assert mini.status_code == 200 and "max-age=86400" in mini.headers["Cache-Control"]


# ---------------------------------------------------------------------------
# A cerca que bloqueia e a obra detectada sozinha (04/10/2026)
# ---------------------------------------------------------------------------
def test_celular_a_obra_e_detectada_e_fora_da_area_nao_bate(app, mundo, banco, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import alertas
    from tests.test_ponto_gestao_banco import _entrar_no_app
    with banco.connect() as conn:          # a obra B ganha coordenada, 1,4 km ao sul da A
        conn.execute(text("UPDATE obras SET latitude = -3.7400, longitude = -38.5270 WHERE id = :o"),
                     {"o": mundo["obra_b"]})
        conn.commit()
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    uuid = "celular-do-joao-cerca-0123456789"
    token = cel.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    cel.post("/ponto/app/api/aparelho/identificar", json={}, headers=h)
    dp = como(app, mundo["dp"])
    aparelho = dp.get("/erp/api/ponto/pendencias").get_json()["aparelhos"][0]
    dp.post(f"/erp/api/ponto/dispositivos/{aparelho['id']}/aprovar", json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO})

    # escolheu a obra A, mas está dentro da cerca da B: a batida é da B
    r = cel.post("/ponto/app/api/bater", json={"obra": "PG-A", "latitude": -3.7401, "longitude": -38.5270,
                                               "precisao": 15}, headers=h)
    assert r.status_code == 201, r.get_json()
    assert r.get_json()["comprovante"]["local"].startswith("PG-B")
    assert "fora da lista da pessoa" in r.get_json()["motivo_analise"]     # B não é obra do João

    # longe das duas: recusada, com a distância
    longe = cel.post("/ponto/app/api/bater", json={"obra": "PG-A", "latitude": -3.80, "longitude": -38.5270},
                     headers=h)
    assert longe.status_code == 403 and "fora da área da obra" in longe.get_json()["erro"]
    # localização desligada no celular: recusada
    sem = cel.post("/ponto/app/api/bater", json={"obra": "PG-A"}, headers=h)
    assert sem.status_code == 403 and "ligue a localização" in sem.get_json()["erro"]

    with db.conexao() as conn:
        alertas.gerar(conn, ate=_hoje())
        recusada = [a for a in alertas.listar(conn) if a["codigo"] == "BATIDA_RECUSADA_FORA_DA_OBRA"]
    assert len(recusada) == 1 and "2 tentativa(s)" in recusada[0]["mensagem"]


def test_gestao_ajusta_a_cerca_de_cada_obra(app, mundo):
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    lista = dp.get("/erp/api/ponto/cercas").get_json()
    a = next(o for o in lista["obras"] if o["id"] == mundo["obra_a"])
    b = next(o for o in lista["obras"] if o["id"] == mundo["obra_b"])
    assert lista["modo_disponivel"] is True
    assert a["tem_coordenada"] is True and a["fora_da_cerca"] == "BLOQUEAR" and a["raio_metros"] == 200
    assert b["tem_coordenada"] is False
    assert sup.get("/erp/api/ponto/cercas").status_code == 403
    assert sup.post(f"/erp/api/ponto/cercas/{mundo['obra_a']}", json={"raio_metros": 400}).status_code == 403
    r = dp.post(f"/erp/api/ponto/cercas/{mundo['obra_a']}", json={"raio_metros": 400, "fora_da_cerca": "ANALISAR"})
    assert r.status_code == 200 and r.get_json()["obra"]["raio_metros"] == 400
    assert r.get_json()["obra"]["fora_da_cerca"] == "ANALISAR"
    assert dp.post(f"/erp/api/ponto/cercas/{mundo['obra_a']}", json={"raio_metros": 10}).status_code == 400
    assert dp.post(f"/erp/api/ponto/cercas/{mundo['obra_a']}", json={"fora_da_cerca": "TALVEZ"}).status_code == 400
