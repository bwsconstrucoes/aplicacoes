# -*- coding: utf-8 -*-
"""O notificador decide sozinho se o WhatsApp sai, e com qual credencial.

Até 05/10/2026 o ponto testava um nome de token (ZAPI_INSTANCE_TOKEN) que não
existia no Render — a fila de QR Code nunca saía do lugar — e o liga/desliga
do WhatsApp era um só para tudo. Estes testes travam as duas correções:
os dois nomes de token valem, e a variável por finalidade
(NOTIFICAR_WHATSAPP_PONTO) manda sobre a geral (NOTIFICAR_WHATSAPP).
Nada aqui fala com a internet: o POST ao Z-API é dublado.
"""
import pytest

from app.apps import notificador

VARS = ("ZAPI_INSTANCE_ID", "ZAPI_API_TOKEN", "ZAPI_INSTANCE_TOKEN", "ZAPI_CLIENT_TOKEN",
        "NOTIFICAR_WHATSAPP", "NOTIFICAR_TELEGRAM", "NOTIFICAR_WHATSAPP_PONTO",
        "NOTIFICAR_WHATSAPP_AVISO_CADASTRO_TELEGRAM")


@pytest.fixture(autouse=True)
def ambiente_limpo(monkeypatch):
    for v in VARS:
        monkeypatch.delenv(v, raising=False)


@pytest.fixture
def zapi_dublado(monkeypatch):
    """Captura o que iria para o Z-API e responde 200."""
    chamadas = []

    class Resp:
        status_code = 200
        text = "ok"

    def post(url, json=None, headers=None, timeout=None):
        chamadas.append({"url": url, "json": json, "headers": headers})
        return Resp()

    monkeypatch.setattr(notificador.requests, "post", post)
    return chamadas


# ── credenciais ───────────────────────────────────────────────────────────────

def test_sem_credencial_nao_esta_configurado():
    assert notificador.whatsapp_configurado() is False
    r = notificador.enviar_whatsapp("5585999990000", "oi")
    assert r["ok"] is False and r["erro"] == "whatsapp_nao_configurado"


def test_token_pelo_nome_do_render(monkeypatch, zapi_dublado):
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_API_TOKEN", "tok-api")
    assert notificador.whatsapp_configurado() is True
    assert notificador.enviar_whatsapp("5585999990000", "oi")["ok"] is True
    assert zapi_dublado[0]["url"] == "https://api.z-api.io/instances/inst/token/tok-api/send-text"


def test_token_pelo_nome_antigo_ainda_vale(monkeypatch, zapi_dublado):
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_INSTANCE_TOKEN", "tok-antigo")
    assert notificador.enviar_whatsapp("5585999990000", "oi")["ok"] is True
    assert "/token/tok-antigo/" in zapi_dublado[0]["url"]


def test_client_token_vai_no_header(monkeypatch, zapi_dublado):
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_API_TOKEN", "t")
    monkeypatch.setenv("ZAPI_CLIENT_TOKEN", "ct")
    notificador.enviar_whatsapp("85999990000", "oi")
    assert zapi_dublado[0]["headers"] == {"Client-Token": "ct"}
    assert zapi_dublado[0]["json"]["phone"] == "85999990000"


# ── liga/desliga ──────────────────────────────────────────────────────────────

def test_geral_desligado_cala_quem_nao_tem_finalidade(monkeypatch, zapi_dublado):
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_API_TOKEN", "t")
    monkeypatch.setenv("NOTIFICAR_WHATSAPP", "0")
    assert notificador.canal_ativo("whatsapp") is False
    assert notificador.enviar_whatsapp("5585999990000", "oi")["ok"] is None
    assert zapi_dublado == []


def test_finalidade_ligada_vence_o_geral_desligado(monkeypatch, zapi_dublado):
    """O caso do dono: WhatsApp desligado para tudo, ligado só para o ponto."""
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_API_TOKEN", "t")
    monkeypatch.setenv("NOTIFICAR_WHATSAPP", "0")
    monkeypatch.setenv("NOTIFICAR_WHATSAPP_PONTO", "1")
    assert notificador.canal_ativo("whatsapp", "ponto") is True
    assert notificador.canal_ativo("whatsapp") is False
    assert notificador.canal_ativo("whatsapp", "titulo_pago") is False
    r = notificador.notificar(telefone="5585999990000", mensagem="QR",
                              canais=("whatsapp",), finalidade="ponto")
    assert r["whatsapp"]["ok"] is True
    assert len(zapi_dublado) == 1


def test_finalidade_desligada_vence_o_geral_ligado(monkeypatch, zapi_dublado):
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_API_TOKEN", "t")
    monkeypatch.setenv("NOTIFICAR_WHATSAPP_PONTO", "0")
    assert notificador.canal_ativo("whatsapp") is True
    assert notificador.canal_ativo("whatsapp", "ponto") is False
    r = notificador.notificar(telefone="5585999990000", mensagem="QR",
                              canais=("whatsapp",), finalidade="ponto")
    assert r["whatsapp"]["ok"] is None and zapi_dublado == []


def test_finalidade_sem_variavel_segue_o_geral(monkeypatch):
    assert notificador.canal_ativo("whatsapp", "ponto") is True
    monkeypatch.setenv("NOTIFICAR_WHATSAPP", "0")
    assert notificador.canal_ativo("whatsapp", "ponto") is False
    monkeypatch.setenv("NOTIFICAR_WHATSAPP_PONTO", "   ")  # vazia conta como ausente
    assert notificador.canal_ativo("whatsapp", "ponto") is False


def test_nome_da_finalidade_vira_nome_de_variavel(monkeypatch):
    monkeypatch.setenv("NOTIFICAR_WHATSAPP", "0")
    monkeypatch.setenv("NOTIFICAR_WHATSAPP_AVISO_CADASTRO_TELEGRAM", "1")
    assert notificador.canal_ativo("whatsapp", "aviso-cadastro telegram") is True
    assert notificador.canal_ativo("whatsapp", "aviso_cadastro_telegram") is True


def test_telegram_tambem_respeita_finalidade(monkeypatch):
    monkeypatch.setenv("NOTIFICAR_TELEGRAM", "0")
    assert notificador.enviar_telegram(telefone="5585999990000", mensagem="x")["ok"] is None
    monkeypatch.setenv("NOTIFICAR_TELEGRAM_PONTO", "1")
    monkeypatch.setattr(notificador, "_tg_notificar", lambda **kw: {"ok": True, "chat_id": "1"})
    assert notificador.enviar_telegram(telefone="5585999990000", mensagem="x",
                                       finalidade="ponto")["ok"] is True


# ── o ponto segue o notificador ───────────────────────────────────────────────

def test_ponto_so_dispara_a_fila_com_credencial_e_finalidade_ligada(monkeypatch):
    from app.apps.ponto.core import envios
    assert envios.whatsapp_pronto() is False                      # sem credencial
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_API_TOKEN", "t")                     # o nome que o Render tem
    assert envios.whatsapp_pronto() is True
    monkeypatch.setenv("NOTIFICAR_WHATSAPP", "0")                 # geral desligado...
    assert envios.whatsapp_pronto() is False
    monkeypatch.setenv("NOTIFICAR_WHATSAPP_PONTO", "1")           # ...mas o ponto ligado
    assert envios.whatsapp_pronto() is True


def test_aviso_de_cadastro_do_telegram_passa_pelo_notificador(monkeypatch, zapi_dublado):
    from app.apps.telegram import telegram_bot as tb
    monkeypatch.setattr(tb, "_AVISOS_WA", {})
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "inst")
    monkeypatch.setenv("ZAPI_API_TOKEN", "t")
    monkeypatch.setenv("NOTIFICAR_WHATSAPP", "0")
    assert tb._wa_aviso_cadastro("5585999990000")["ok"] is None   # desligado: cala
    assert zapi_dublado == []
    monkeypatch.setenv("NOTIFICAR_WHATSAPP_AVISO_CADASTRO_TELEGRAM", "1")
    assert tb._wa_aviso_cadastro("5585999990000")["ok"] is True
    assert "t.me/" in zapi_dublado[0]["json"]["message"]
    assert tb._wa_aviso_cadastro("5585999990000")["ok"] is None   # 6 h de silêncio depois
