# -*- coding: utf-8 -*-
"""
O LOTE "WHATSAPP" PELO ROBÔ DO TELEGRAM — 08/10/2026 (migração 052).

O dono: *"alimentar um lote chamado WhatsApp (…) sempre o primeiro de todos
(…) atrelar isso ao usuário"*. Com banco de verdade: a ligação, o convite de
uso único e o lote vivem em `WHERE` e `DELETE … RETURNING`.
"""
import pytest
from flask import Flask

from tests.test_analisesps_banco import semear, sp
from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso, criar, entrar_como)

pytestmark = pytest.mark.banco

CHAT = 777001
PEDIDO = ("Solicitação de Pagamento Validada\nNº da SP: 1426036778\n"
          "Valor: R$ 1.250,00 — CNPJ 12.345.678/0001-90 — tel 81987654321\n\n"
          "Solicitação de Pagamento Validada\nNº da SP: 1426036999")


def _ligar(uid, chat=CHAT):
    from app.apps.analisesps import telegram_lote
    link = telegram_lote.gerar_convite(uid)
    codigo = link.split("start=", 1)[1]
    return telegram_lote.receber(chat, f"/start {codigo}"), link


def _lote(chave="thiago"):
    from app.apps.analisesps import lote
    return lote.ler(chave)["conteudo"]


def test_o_link_liga_a_conversa_e_vale_UMA_vez(app):
    from app.apps.analisesps import telegram_lote
    uid = criar(telas=("lote",), pode_operar=True)
    resposta, link = _ligar(uid)
    assert link.startswith("https://t.me/") and "start=lote_" in link
    assert "ligada ao usuário THIAGO" in resposta
    assert telegram_lote.ligacao(uid)
    # o mesmo link de novo não liga nada (outra conversa tentando)
    codigo = link.split("start=", 1)[1]
    assert "não vale mais" in telegram_lote.receber(999, f"/start {codigo}")


def test_o_link_VENCIDO_nao_liga(app):
    from app.apps.analisesps import telegram_lote
    from app.apps.analisesps.db import conexao
    uid = criar(telas=("lote",), pode_operar=True)
    link = telegram_lote.gerar_convite(uid)
    with conexao() as conn:
        conn.execute("UPDATE analisesps.telegram_convite "
                     "   SET criado_em = now() - interval '1 hour'")
        conn.commit()
    resposta = telegram_lote.receber(CHAT, "/start " + link.split("start=", 1)[1])
    assert "venceu" in resposta and telegram_lote.ligacao(uid) is None


def test_as_SPs_coladas_entram_no_grupo_WHATSAPP_no_topo_do_lote_dele(app):
    from app.apps.analisesps import lote, telegram_lote
    uid = criar(telas=("lote",), pode_operar=True)
    _ligar(uid)
    semear([sp("1426036778", credor="FORNECEDOR")])
    lote.salvar("Pagar amanhã\n1111111111", "THIAGO", "thiago")

    resposta = telegram_lote.receber(CHAT, PEDIDO)
    assert "2 SP(s) entraram no grupo WhatsApp" in resposta
    assert "Não achei na base" in resposta and "1426036999" in resposta
    assert _lote() == ("WhatsApp\n1426036778\n1426036999\n\n"
                       "Pagar amanhã\n1111111111")
    assert lote.ler("thiago")["salvo_por"] == "THIAGO (Telegram)"

    # o segundo envio SOMA no mesmo grupo, e o repetido não entra de novo
    resposta = telegram_lote.receber(CHAT, "SP 1426036778 e SP 2222222222")
    assert "1 já estava(m) no seu lote: 1426036778" in resposta
    assert _lote().startswith("WhatsApp\n1426036778\n1426036999\n2222222222\n\n")


def test_avisa_quando_a_SP_esta_no_lote_de_OUTRA_pessoa(app):
    from app.apps.analisesps import lote, telegram_lote
    uid = criar(telas=("lote",), pode_operar=True)
    _ligar(uid)
    lote.salvar("Do Joao\n1426036778", "JOAO", "joao")
    assert "também está no lote de JOAO" in telegram_lote.receber(CHAT, PEDIDO)


def test_conversa_NAO_LIGADA_ou_sem_SP(app):
    from app.apps.analisesps import telegram_lote
    assert telegram_lote.receber(CHAT, PEDIDO) == telegram_lote.MSG_NAO_LIGADO
    # sem número de SP, o robô segue o caminho de sempre (contracheque etc.)
    assert telegram_lote.receber(CHAT, "1") is None
    assert telegram_lote.receber(CHAT, "/start") is None
    assert telegram_lote.receber(CHAT, "12345678901") is None, "CPF não é SP"


def test_quem_NAO_PODE_alterar_o_lote_nao_alimenta_pelo_robo(app):
    from app.apps.analisesps import telegram_lote
    uid = criar(telas=("solicitacoes",), pode_operar=True)
    resposta, _ = _ligar(uid)
    assert "ainda não pode alterar o Lote" in resposta
    assert "não pode alterar o Lote" in telegram_lote.receber(CHAT, PEDIDO)
    assert _lote() == ""


def test_DESLIGAR_e_ligar_de_novo_em_outro_celular(app):
    from app.apps.analisesps import telegram_lote
    uid = criar(telas=("lote",), pode_operar=True)
    _ligar(uid)
    _ligar(uid, chat=888)               # celular novo: a conversa antiga sai
    assert telegram_lote.receber(CHAT, PEDIDO) == telegram_lote.MSG_NAO_LIGADO
    assert "entraram" in telegram_lote.receber(888, PEDIDO)
    telegram_lote.desligar(uid)
    assert telegram_lote.receber(888, PEDIDO) == telegram_lote.MSG_NAO_LIGADO


def test_a_TELA_ABERTA_nao_apaga_no_salvar_o_que_chegou_pelo_robo(app):
    """Abriu a janela do Lote, colou SPs no robô, voltou e salvou: o texto que
    a tela manda é o de antes — e sem a proteção as SPs do robô sumiam."""
    from app.apps.analisesps import lote, telegram_lote
    uid = criar(telas=("lote",), pode_operar=True)
    _ligar(uid)
    lote.salvar("Pagar amanhã\n1111111111", "THIAGO", "thiago")
    versao = str(lote.ler("thiago")["salvo_em"])
    telegram_lote.receber(CHAT, PEDIDO)

    with app.test_client() as cliente:
        entrar_como(cliente)
        semear([sp("1111111111")])
        r = cliente.post("/analisesps/lote", data={
            "acao": "salvar", "versao": versao,
            "conteudo": "Pagar amanhã\n1111111111\n3333333333"})
        assert r.status_code == 302 and "Telegram" in r.location
        assert _lote().startswith("WhatsApp\n1426036778\n1426036999\n\n")
        assert "3333333333" in _lote()

        # Agora a tela carregou a versão nova e a pessoa TIRA uma de propósito:
        # ela não volta sozinha.
        versao = str(lote.ler("thiago")["salvo_em"])
        cliente.post("/analisesps/lote", data={
            "acao": "salvar", "versao": versao,
            "conteudo": "WhatsApp\n1426036778\n\nPagar amanhã\n1111111111"})
    assert "1426036999" not in _lote()


def test_a_tela_liga_pelo_botao_e_a_senha_geral_nao_liga(app):
    uid = criar(telas=("lote",), pode_operar=True)
    semear([sp("1111111111")])
    with app.test_client() as cliente:
        entrar_como(cliente)
        tela = cliente.get("/analisesps/lote").get_data(as_text=True)
        assert "Ligar ao Telegram" in tela and 'name="versao"' in tela
        r = cliente.post("/analisesps/lote/telegram", json={"acao": "ligar"})
        assert r.get_json()["ok"] and "start=lote_" in r.get_json()["link"]
    from app.apps.analisesps import telegram_lote
    _ligar(uid)
    with app.test_client() as cliente:
        entrar_como(cliente)
        assert "Desligar o Telegram" in cliente.get("/analisesps/lote").get_data(as_text=True)
        assert cliente.post("/analisesps/lote/telegram",
                            json={"acao": "desligar"}).get_json()["ok"]
    assert telegram_lote.ligacao(uid) is None
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        r = cliente.post("/analisesps/lote/telegram", json={"acao": "ligar"})
        assert r.status_code == 400 and "seu usuário" in r.get_json()["erro"]


def test_o_WEBHOOK_do_robo_entrega_ao_lote_e_responde(app, monkeypatch):
    """Ponta a ponta pelo robô de verdade, com o envio ao Telegram dublado."""
    from app.apps.telegram import telegram_bot
    enviadas = []
    monkeypatch.setattr(telegram_bot, "TELEGRAM_SECRET", "")
    monkeypatch.setattr(telegram_bot, "_tg_enviar",
                        lambda chat, texto, **k: enviadas.append((chat, texto)))
    robo = Flask("robo")
    robo.register_blueprint(telegram_bot.telegram_bp)
    uid = criar(telas=("lote",), pode_operar=True)
    from app.apps.analisesps import telegram_lote
    link = telegram_lote.gerar_convite(uid)
    with robo.test_client() as c:
        def manda(texto):
            c.post("/telegram/webhook", json={"message": {
                "chat": {"id": CHAT}, "from": {"id": 1, "first_name": "T"},
                "text": texto}})
        manda("/start " + link.split("start=", 1)[1])
        manda(PEDIDO)
    assert "ligada ao usuário THIAGO" in enviadas[0][1]
    assert "2 SP(s) entraram" in enviadas[1][1]
    assert _lote().startswith("WhatsApp\n1426036778")


def test_ERRO_do_analisesps_nao_derruba_o_robo(app, monkeypatch):
    from app.apps.analisesps import telegram_lote
    from app.apps.telegram import telegram_bot

    def explode(*a, **k):
        raise RuntimeError("banco fora")
    monkeypatch.setattr(telegram_lote, "receber", explode)
    assert "Não consegui pôr as SPs no lote" in telegram_bot._lote_analisesps(1, "x")
