# -*- coding: utf-8 -*-
"""O cadastro do Telegram no ERP e o convite pelo WhatsApp — sem banco.

Pedido do dono em 14/11/2026: quem não recebe a mensagem do Telegram por falta
de cadastro recebe, pelo WhatsApp, a orientação de como se cadastrar; o
cadastro sai da planilha e vem para o ERP; só colaborador (ou operador) com
telefone pode se cadastrar. Os casos com banco estão em
`test_mensageria_telegram_banco.py`.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps import notificador
from app.apps.erp import routes
from app.apps.erp.db.models.cadastros import PerfilUsuario as P
from app.apps.mensageria import gestao, telegram_cadastro as tc  # noqa: F401 — pendura as rotas

from conftest import SessaoFalsa, novo_usuario


# ---------------------------------------------------------------------------
# Telefone, texto do convite
# ---------------------------------------------------------------------------
def test_telefone_com_e_sem_nono_digito_e_com_55():
    assert tc.variantes_telefone("+55 (85) 99832-2004") == {"85998322004", "8598322004"}
    assert tc.variantes_telefone("85 9832-2004") == {"8598322004", "85998322004"}
    assert tc.telefone_canonico("(85) 99832-2004") == "5585998322004"
    assert tc.telefone_canonico("5585998322004") == "5585998322004"
    assert tc.normalizar_cpf("123.456.789-0") == "01234567890"


def test_o_link_pessoal_aponta_para_o_robo(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_LINK", raising=False)
    assert tc.link_pessoal("abc") == "https://t.me/bwsconstrucoesbotbot?start=c_abc"
    monkeypatch.setenv("TELEGRAM_BOT_LINK", "https://t.me/outrobot/")
    assert tc.link_pessoal("x") == "https://t.me/outrobot?start=c_x"


def test_o_texto_do_convite_leva_o_primeiro_nome_e_o_link():
    t = tc.montar_texto(tc.TEXTO_PADRAO, "JOÃO DA SILVA", "https://t.me/b?start=c_1")
    assert t.startswith("Olá, João!") and "https://t.me/b?start=c_1" in t
    assert "não chegou" in t and "Telegram" in t and "RH" in t
    # Pela tela não houve mensagem perdida: o texto não diz que "tentou mandar"
    manual = tc.montar_texto(tc.TEXTO_PADRAO, "Ana", "L")
    assert "tentou te mandar" in manual
    assert "tentou te mandar" not in tc.montar_texto(tc.TEXTO_PADRAO, "Ana", "L", manual=True)
    # Quem editou o texto e esqueceu o link: o link vai no fim mesmo assim
    assert tc.montar_texto("Oi {nome}", "Ana", "L").endswith("\n\nL")


def test_texto_editado_precisa_ter_o_link():
    assert tc.validar_modelo("  ") == ""
    assert tc.validar_modelo("Oi {nome}: {link}") == "Oi {nome}: {link}"
    with pytest.raises(ValueError, match="link"):
        tc.validar_modelo("Oi {nome}, cadastre-se")
    with pytest.raises(ValueError, match="2.000"):
        tc.validar_modelo("{link}" + "x" * 2001)


@pytest.mark.parametrize("resultado, esperado", [
    ({"whatsapp": {"ok": True}}, "ENVIADO"),
    ({"mensageria": {"ok": None, "detalhe": "WhatsApp desligado na tela Mensagens"}}, "DESLIGADO"),
    ({"whatsapp": {"ok": False, "erro": "limite", "detalhe": "teto"}}, "LIMITE"),
    ({"whatsapp": {"ok": False, "detalhe": "HTTP 500"}}, "FALHOU"),
])
def test_o_resultado_do_envio_vira_a_situacao_do_convite(resultado, esperado):
    assert tc._status_do_resultado(resultado)[0] == esperado


# ---------------------------------------------------------------------------
# O convite automático: quando dispara e quando não
# ---------------------------------------------------------------------------
def test_convite_nao_convida_nem_sem_banco(monkeypatch):
    assert tc.convidar_se_preciso(telefone="5585", tipo_que_falhou="telegram.aviso_cadastro")["status"] == "IGNORADO"
    assert tc.convidar_se_preciso(telefone="5585", tipo_que_falhou="mensageria.limite")["status"] == "IGNORADO"
    monkeypatch.setattr(tc.db, "disponivel", lambda: False)
    assert tc.convidar_se_preciso(telefone="5585", tipo_que_falhou="erp.titulo_pago")["status"] == "IGNORADO"


@pytest.fixture
def chamadas(monkeypatch):
    feitas = []
    monkeypatch.setattr(tc, "convidar_se_preciso",
                        lambda **kw: (feitas.append(kw), {"status": "AGENDADO", "detalhe": "ok"})[1])
    return feitas


def test_o_notificador_convida_quando_o_telegram_nao_tem_cadastro(chamadas):
    m = object()
    notificador._convidar_se_nao_chegou(m, {"telegram": {"ok": False, "erro": "nao_cadastrado"}},
                                        "5585999990000", "12345678901", "erp.titulo_pago")
    assert chamadas == [{"telefone": "5585999990000", "cpf": "12345678901", "tipo_que_falhou": "erp.titulo_pago"}]


def test_nao_convida_se_outro_canal_entregou_ou_se_o_erro_e_outro(chamadas):
    m = object()
    notificador._convidar_se_nao_chegou(m, {"telegram": {"ok": False, "erro": "nao_cadastrado"},
                                            "whatsapp": {"ok": True}}, "5585", None, "erp.titulo_pago")
    notificador._convidar_se_nao_chegou(m, {"telegram": {"ok": False, "detalhe": "falha no envio"}},
                                        "5585", None, "erp.titulo_pago")
    notificador._convidar_se_nao_chegou(None, {"telegram": {"ok": False, "erro": "nao_cadastrado"}},
                                        "5585", None, "erp.titulo_pago")
    assert chamadas == []


def test_notificar_de_ponta_a_ponta_dispara_o_convite(monkeypatch, chamadas):
    """Mensagem 'Só Telegram' para quem não tem cadastro: registra SEM_DESTINO e convida."""
    from tests.test_mensageria import CoreFalso, DbFalso
    from app.apps.mensageria import core
    c, d = CoreFalso(core.TELEGRAM), DbFalso()
    monkeypatch.setattr(notificador, "_mensageria", lambda: (c, d))
    monkeypatch.setattr(notificador, "_tg_notificar",
                        lambda **kw: {"ok": False, "erro": "nao_cadastrado", "detalhe": "sem TelegramID"})
    r = notificador.notificar(telefone="5585999990000", cpf="12345678901", mensagem="pago",
                              finalidade="erp.titulo_pago")
    assert r["telegram"]["erro"] == "nao_cadastrado"
    assert c.registros[-1]["status"] == "SEM_DESTINO"
    assert chamadas and chamadas[0]["tipo_que_falhou"] == "erp.titulo_pago"


# ---------------------------------------------------------------------------
# O robô
# ---------------------------------------------------------------------------
@pytest.fixture
def robo(monkeypatch):
    from app.apps.telegram import telegram_bot as tb
    monkeypatch.setattr(tb, "TELEGRAM_SECRET", "")
    enviados = []
    monkeypatch.setattr(tb, "_tg_enviar", lambda chat, texto, **kw: enviados.append((chat, texto, kw)) or True)
    a = Flask(__name__)
    a.register_blueprint(tb.telegram_bp)
    return tb, a.test_client(), enviados


def test_o_link_do_convite_e_tratado_antes_do_lote_e_do_cadastro(robo, monkeypatch):
    tb, c, enviados = robo
    abertos = []
    monkeypatch.setattr(tb, "_abrir_convite",
                        lambda chat, codigo, nome: (abertos.append((chat, codigo)), tb.jsonify({"ok": True}))[1])
    monkeypatch.setattr(tb, "_lote_analisesps", lambda *a: pytest.fail("o lote não devia ver o convite"))
    r = c.post("/telegram/webhook", json={"message": {"chat": {"id": 77}, "from": {"id": 77, "first_name": "Ana"},
                                                      "text": "/start c_CODIGO123"}})
    assert r.status_code == 200 and abertos == [(77, "CODIGO123")]


def test_link_vencido_responde_e_oferece_o_cadastro_normal(robo, monkeypatch):
    tb, c, enviados = robo
    monkeypatch.setattr(tb, "_cadastro_erp", lambda: None)
    monkeypatch.setattr(tb, "_buscar_cadastro_por_chat_id", lambda chat: (None, None))
    c.post("/telegram/webhook", json={"message": {"chat": {"id": 5}, "from": {"id": 5}, "text": "/start c_x"}})
    assert "venceu ou já foi usado" in enviados[-1][1] and enviados[-1][2].get("teclado")


def test_o_aviso_do_telegram_enviar_vira_o_convite_da_mensageria(robo, monkeypatch):
    tb, _, _ = robo
    feitos = []

    class TcFalso:
        @staticmethod
        def convidar_se_preciso(**kw):
            feitos.append(kw)
            return {"status": "AGENDADO", "detalhe": "convite para Ana saindo pelo WhatsApp"}
    monkeypatch.setattr(tb, "_cadastro_erp", lambda: (TcFalso, None))
    r = tb._wa_aviso_cadastro("5585999990000", cpf="12345678901")
    assert r["ok"] is True and feitos[0]["cpf"] == "12345678901"
    assert feitos[0]["tipo_que_falhou"] == "externo.telegram_enviar"


def test_o_robo_procura_a_pessoa_no_erp_antes_das_planilhas(robo, monkeypatch):
    tb, _, _ = robo
    pessoa = {"cpf": "12345678901", "nome": "Ana Lima", "telefone": "5585999990000", "tipo": "COLABORADOR"}
    monkeypatch.setattr(tb, "_pessoa_no_erp", lambda **kw: pessoa)
    monkeypatch.setattr(tb, "_ler_valores_com_retry", lambda *a, **k: pytest.fail("não devia ler planilha"))
    achado, ok = tb._consultar_bases(telefone="85999990000")
    assert ok and achado["cpf"] == "12345678901" and achado["base"] == "ERP (colaboradores)"


# ---------------------------------------------------------------------------
# A tela
# ---------------------------------------------------------------------------
@pytest.fixture
def app():
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


def _como(app, monkeypatch, perfil):
    @contextlib.contextmanager
    def _fake():
        yield SessaoFalsa(novo_usuario(1, perfil, nome="Marcelo"))
    monkeypatch.setattr(routes, "get_session", _fake)
    c = app.test_client()
    with c.session_transaction() as s:
        s["erp_usuario_id"] = 1
    return c


def test_as_rotas_novas_declaram_a_acao_certa():
    mapa = routes._REGISTRO_PERMISSOES
    assert mapa["erp.mensagens_pagina_telegram"]["GET"] == "ver_mensagens"
    assert mapa["erp.mensagens_api_telegram"]["GET"] == "ver_mensagens"
    for ep in ("convidar", "configurar", "importar", "desvincular"):
        assert mapa[f"erp.mensagens_api_telegram_{ep}"]["POST"] == "configurar_mensagens"
    assert ("msg_telegram", "Cadastro do Telegram", "erp.mensagens_pagina_telegram") in gestao.ABAS


def test_a_aba_abre_para_quem_ve_e_so_o_admin_convida(app, monkeypatch):
    c = _como(app, monkeypatch, P.DEPARTAMENTO_PESSOAL)
    r = c.get("/erp/mensagens/telegram")
    assert r.status_code == 200 and "Cadastro do Telegram" in r.get_data(as_text=True)
    assert c.post("/erp/api/mensagens/telegram/convidar", json={"chaves": ["C1"]}).status_code == 403
    assert c.post("/erp/api/mensagens/telegram/importar").status_code == 403
    assert _como(app, monkeypatch, P.CONSULTA).get("/erp/mensagens/telegram").status_code == 403


def test_convite_em_lote_tem_teto_e_pede_o_whatsapp_ligado(app, monkeypatch):
    monkeypatch.setattr(tc.db, "disponivel", lambda: True)
    monkeypatch.setattr(tc.db, "conexao", contextlib.nullcontext)
    c = _como(app, monkeypatch, P.ADMIN)
    r = c.post("/erp/api/mensagens/telegram/convidar", json={"chaves": [f"C{i}" for i in range(60)]})
    assert r.status_code == 400 and "50" in r.get_json()["erro"]
    assert c.post("/erp/api/mensagens/telegram/convidar", json={"chaves": []}).status_code == 400
    monkeypatch.setattr(tc.core, "whatsapp_ligado", lambda conn: False)
    r = c.post("/erp/api/mensagens/telegram/convidar", json={"chaves": ["C1"]})
    assert r.status_code == 400 and "desligado" in r.get_json()["erro"]


def test_intervalo_entre_convites_tem_limite(app, monkeypatch):
    monkeypatch.setattr(tc.db, "disponivel", lambda: True)
    monkeypatch.setattr(tc.db, "conexao", contextlib.nullcontext)
    c = _como(app, monkeypatch, P.ADMIN)
    assert c.post("/erp/api/mensagens/telegram/configurar", json={"intervalo_dias": "0"}).status_code == 400
    assert c.post("/erp/api/mensagens/telegram/configurar", json={"texto": "sem o link"}).status_code == 400
