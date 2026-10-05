# -*- coding: utf-8 -*-
"""A Mensageria: por onde cada mensagem sai, com teto e registro.

Decisão do dono em 05/10/2026: "eu preciso ter gestão sobre quais mensagens
vão para o WhatsApp e quais não vão (…) prioridade é Telegram; por enquanto,
ponto permitido no WhatsApp". Estes testes travam:

  · a política do tipo (tela Mensagens) manda sobre o que o chamador pediu;
  · a chave geral do WhatsApp cala qualquer política de WhatsApp;
  · o teto por hora/dia segura a mensagem e avisa os ADMIN, uma vez por hora;
  · tudo fica no registro, inclusive o que NÃO saiu e por quê;
  · a tela entrou no ERP com as duas ações, as seções e o padrão NEGAR;
  · sem a migração 083 aplicada, nada quebra — valem as variáveis, como antes.

Nenhum teste fala com Telegram, Z-API ou banco: tudo dublado.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps import notificador
from app.apps.erp import routes
from app.apps.erp.db.models.cadastros import PerfilUsuario as P
from app.apps.mensageria import core, gestao  # noqa: F401 — pendura as rotas no ERP

from conftest import SessaoFalsa, novo_usuario


# ---------------------------------------------------------------------------
# Dublês
# ---------------------------------------------------------------------------
class ConnFalsa:
    pass


class DbFalso:
    """A mensageria "no ar": conexão de mentira, registro em lista."""
    def __init__(self):
        self.registros = []

    def disponivel(self):
        return True

    @contextlib.contextmanager
    def conexao(self):
        yield ConnFalsa()


class CoreFalso:
    def __init__(self, politica, wa_ligado=True, cabe=True):
        self.politica, self.wa_ligado, self.cabe = politica, wa_ligado, cabe
        self.registros = []
        self.avisos_limite = 0
        self.ja_avisou = False
        self.POLITICAS = core.POLITICAS

    def decidir(self, conn, tipo):
        # O decidir de verdade, com a leitura do banco dublada
        return _decidir_com(self.politica, self.wa_ligado, tipo)

    def registrar(self, conn, **campos):
        self.registros.append(campos)

    def limpar_se_preciso(self, conn):
        pass

    def cabe_no_teto(self, conn, momento=None):
        return (True, "") if self.cabe else (False, "teto de 40 mensagens de WhatsApp por hora atingido")

    def _deve_avisar_limite(self, conn, momento):
        if self.ja_avisou:
            return False
        self.ja_avisou = True
        return True

    def tipo_que_mais_consome(self, conn, momento):
        return "QR Code do ponto (39 hoje)"

    def nome_do_tipo(self, chave):
        return core.nome_do_tipo(chave)


def _decidir_com(politica, wa_ligado, tipo, monkeypatch=None):
    """Roda o `core.decidir` real trocando só a leitura do banco."""
    original_pol, original_wa = core.politica_do_tipo, core.whatsapp_ligado
    core.politica_do_tipo = lambda conn, t: politica
    core.whatsapp_ligado = lambda conn: wa_ligado
    try:
        return core.decidir(ConnFalsa(), tipo)
    finally:
        core.politica_do_tipo, core.whatsapp_ligado = original_pol, original_wa


@pytest.fixture(autouse=True)
def ambiente_limpo(monkeypatch):
    for v in ("NOTIFICAR_WHATSAPP", "NOTIFICAR_TELEGRAM", "ZAPI_INSTANCE_ID", "ZAPI_API_TOKEN",
              "NOTIFICAR_WHATSAPP_PONTO_QR"):
        monkeypatch.delenv(v, raising=False)


@pytest.fixture
def canais(monkeypatch):
    """Telegram e WhatsApp dublados no notificador; devolve o que cada um recebeu."""
    saiu = {"telegram": [], "whatsapp": [], "sem_telegram": set()}

    def tg(telefone=None, cpf=None, chat_id=None, mensagem="", **kw):
        if (telefone or "") in saiu["sem_telegram"]:
            return {"ok": False, "erro": "nao_cadastrado", "detalhe": "sem TelegramID"}
        saiu["telegram"].append({"telefone": telefone, "cpf": cpf, "mensagem": mensagem, **kw})
        return {"ok": True, "chat_id": "123", "detalhe": "ok"}

    def wa_texto(tel, msg):
        saiu["whatsapp"].append({"telefone": tel, "mensagem": msg})
        return {"ok": True, "detalhe": "ok"}

    def wa_arquivo(tel, tipo, **kw):
        saiu["whatsapp"].append({"telefone": tel, "tipo": tipo, **kw})
        return {"ok": True, "detalhe": "ok"}

    monkeypatch.setattr(notificador, "_tg_notificar", tg)
    monkeypatch.setattr(notificador, "_wa_enviar_texto", wa_texto)
    monkeypatch.setattr(notificador, "_wa_enviar_arquivo", wa_arquivo)
    return saiu


def _mensageria(monkeypatch, politica, wa_ligado=True, cabe=True, admins=()):
    c, d = CoreFalso(politica, wa_ligado, cabe), DbFalso()
    d.todos = lambda conn, sql, **p: [{"telefone": t, "cpf": ""} for t in admins]
    monkeypatch.setattr(notificador, "_mensageria", lambda: (c, d))
    return c


# ---------------------------------------------------------------------------
# A decisão (core.decidir), política × chave geral
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("politica, wa, canais_esperados, fallback", [
    (core.TELEGRAM, True, ("telegram",), False),
    (core.TELEGRAM, False, ("telegram",), False),
    (core.WHATSAPP, True, ("whatsapp",), False),
    (core.WHATSAPP, False, (), False),
    (core.TELEGRAM_OU_WHATSAPP, True, ("telegram", "whatsapp"), True),
    (core.TELEGRAM_OU_WHATSAPP, False, ("telegram",), False),
    (core.DESLIGADO, True, (), False),
])
def test_politica_e_chave_geral_viram_canais(politica, wa, canais_esperados, fallback):
    d = _decidir_com(politica, wa, "ponto.qr")
    assert d.canais == canais_esperados and d.fallback is fallback


def test_tipo_fora_do_catalogo_sai_so_por_telegram():
    d = _decidir_com(None, True, "modulo.novo")
    assert d.canais == ("telegram",) and "fora do catálogo" in d.motivo


def test_todo_tipo_do_catalogo_tem_nome_modulo_e_politica_valida():
    chaves = [t["chave"] for t in core.CATALOGO]
    assert len(chaves) == len(set(chaves))
    for t in core.CATALOGO:
        assert t["nome"] and t["modulo"] and t["descricao"]
        assert t["politica"] in core.POLITICAS
        assert core.normalizar_chave(t["chave"]) == t["chave"]
    # As finalidades que o código passa existem no catálogo
    for chave in ("ponto.qr", "ponto.codigo_acesso", "ponto.mosaico", "ponto.aviso_foto", "ponto.resumo_dia",
                  "erp.titulo_pago", "erp.encaminhamento", "erp.agente_cobranca", "erp.pergunta_agendada",
                  "erp.teto_ia", "erp.insumos", "telegram.aviso_cadastro", "mensageria.limite"):
        assert chave in chaves


def test_ponto_nasce_liberado_no_whatsapp_e_o_resto_so_telegram():
    """A decisão do dono em 05/10/2026, gravada no catálogo."""
    por = {t["chave"]: t["politica"] for t in core.CATALOGO}
    assert all(v == core.WHATSAPP for k, v in por.items() if k.startswith("ponto."))
    assert all(v == core.TELEGRAM for k, v in por.items() if k.startswith("erp."))
    assert por["telegram.aviso_cadastro"] == core.DESLIGADO


# ---------------------------------------------------------------------------
# O notificador obedece à mensageria
# ---------------------------------------------------------------------------
def test_politica_da_tela_manda_sobre_os_canais_pedidos(monkeypatch, canais):
    """O ponto pede WhatsApp; a tela diz 'Só Telegram' → sai por Telegram."""
    m = _mensageria(monkeypatch, core.TELEGRAM)
    r = notificador.notificar(telefone="5585999990000", mensagem="QR", canais=("whatsapp",),
                              finalidade="ponto.qr")
    assert r["telegram"]["ok"] is True and "whatsapp" not in r
    assert canais["whatsapp"] == [] and len(canais["telegram"]) == 1
    assert m.registros[-1]["canal"] == "telegram" and m.registros[-1]["status"] == "ENVIADO"
    assert m.registros[-1]["tipo"] == "ponto.qr"


def test_whatsapp_desligado_na_chave_geral_cala_e_registra(monkeypatch, canais):
    m = _mensageria(monkeypatch, core.WHATSAPP, wa_ligado=False)
    r = notificador.notificar(telefone="5585999990000", mensagem="QR", canais=("whatsapp",),
                              finalidade="ponto.qr")
    assert r == {"mensageria": {"ok": None, "detalhe": "WhatsApp desligado na tela Mensagens"}}
    assert canais["whatsapp"] == [] and canais["telegram"] == []
    assert m.registros[-1]["status"] == "DESLIGADO"


def test_tipo_desligado_nao_sai_nem_por_telegram(monkeypatch, canais):
    m = _mensageria(monkeypatch, core.DESLIGADO)
    r = notificador.notificar(telefone="5585999990000", mensagem="x", finalidade="telegram.aviso_cadastro")
    assert r["mensageria"]["ok"] is None
    assert canais["telegram"] == [] and m.registros[-1]["status"] == "DESLIGADO"


def test_whatsapp_so_para_quem_nao_tem_telegram(monkeypatch, canais):
    _mensageria(monkeypatch, core.TELEGRAM_OU_WHATSAPP)
    canais["sem_telegram"].add("5585999990000")
    r = notificador.notificar(telefone="5585999990000", mensagem="aviso", finalidade="erp.titulo_pago")
    assert r["telegram"]["erro"] == "nao_cadastrado" and r["whatsapp"]["ok"] is True
    # Quem tem Telegram não gasta WhatsApp
    r2 = notificador.notificar(telefone="5585888880000", mensagem="aviso", finalidade="erp.titulo_pago")
    assert r2["telegram"]["ok"] is True and r2["whatsapp"] == {"ok": None, "detalhe": "não executado"}
    assert len(canais["whatsapp"]) == 1


def test_teto_segura_a_mensagem_registra_e_avisa_os_admin_uma_vez(monkeypatch, canais):
    m = _mensageria(monkeypatch, core.WHATSAPP, cabe=False, admins=("5585911110000",))
    r1 = notificador.notificar(telefone="5585999990000", mensagem="QR", finalidade="ponto.qr")
    r2 = notificador.notificar(telefone="5585999990001", mensagem="QR", finalidade="ponto.qr")
    assert r1["whatsapp"]["erro"] == "limite" and r2["whatsapp"]["erro"] == "limite"
    assert canais["whatsapp"] == []
    limites = [x for x in m.registros if x["status"] == "LIMITE"]
    assert len(limites) == 2
    # Os ADMIN foram avisados pelo Telegram, UMA vez (a segunda cai no "já avisei")
    avisos = [t for t in canais["telegram"] if "teto do WhatsApp" in t["mensagem"]]
    assert len(avisos) == 1 and avisos[0]["telefone"] == "5585911110000"
    assert "QR Code do ponto" in avisos[0]["mensagem"]


def test_enviar_telegram_com_finalidade_segue_a_politica(monkeypatch, canais):
    """O ERP chama `enviar_telegram`; se a tela disser 'Telegram; WhatsApp para
    quem não tem', quem não tem Telegram recebe por WhatsApp — e o chamador
    vê ok=True."""
    _mensageria(monkeypatch, core.TELEGRAM_OU_WHATSAPP)
    canais["sem_telegram"].add("5585999990000")
    r = notificador.enviar_telegram(telefone="5585999990000", mensagem="pago", finalidade="erp.titulo_pago")
    assert r["ok"] is True and canais["whatsapp"][0]["mensagem"] == "pago"


def test_sem_mensageria_valem_as_variaveis_como_antes(monkeypatch, canais):
    """Antes da migração 083 (ou sem banco), nada muda de comportamento."""
    monkeypatch.setattr(notificador, "_mensageria", lambda: None)
    monkeypatch.setenv("NOTIFICAR_WHATSAPP", "0")
    r = notificador.notificar(telefone="5585999990000", mensagem="QR", canais=("whatsapp",),
                              finalidade="ponto.qr")
    assert r["whatsapp"]["ok"] is None and canais["whatsapp"] == []
    monkeypatch.setenv("NOTIFICAR_WHATSAPP_PONTO_QR", "1")
    r = notificador.notificar(telefone="5585999990000", mensagem="QR", canais=("whatsapp",),
                              finalidade="ponto.qr")
    assert r["whatsapp"]["ok"] is True


def test_envio_sem_finalidade_tambem_fica_no_registro(monkeypatch, canais):
    """Os módulos antigos não dizem o tipo; mesmo assim o que saiu aparece na
    aba Enviadas, como 'sem_tipo' — sem mudar como eles decidem."""
    m = _mensageria(monkeypatch, core.TELEGRAM)
    notificador.notificar(telefone="5585999990000", mensagem="x", canais=("telegram",))
    assert m.registros[-1]["tipo"] == "sem_tipo" and m.registros[-1]["canal"] == "telegram"


def test_a_fila_do_ponto_so_liga_se_a_mensageria_liberar(monkeypatch):
    from app.apps.ponto.core import envios
    monkeypatch.setenv("ZAPI_INSTANCE_ID", "i"); monkeypatch.setenv("ZAPI_API_TOKEN", "t")
    _mensageria(monkeypatch, core.WHATSAPP, wa_ligado=False)
    assert envios.whatsapp_pronto() is False
    _mensageria(monkeypatch, core.WHATSAPP, wa_ligado=True)
    assert envios.whatsapp_pronto() is True
    _mensageria(monkeypatch, core.TELEGRAM)
    assert envios.whatsapp_pronto() is False



# ---------------------------------------------------------------------------
# A tela dentro do ERP
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


class TestEncaixeNoErp:
    def test_acoes_tem_nome_e_secao(self):
        from app.apps.erp.core.auth import secoes
        from app.apps.erp.core.auth.permissoes import ACAO_ROTULOS, ACOES_IMPLICADAS, PERMISSOES
        assert {"ver_mensagens", "configurar_mensagens"} <= set(PERMISSOES)
        assert {"ver_mensagens", "configurar_mensagens"} <= set(ACAO_ROTULOS)
        assert PERMISSOES["configurar_mensagens"] == {P.ADMIN}
        assert ACOES_IMPLICADAS["ver_mensagens"] == ("configurar_mensagens",)
        secao = secoes.POR_CHAVE["adm_mensagens"]
        assert secao["ler"] == ["ver_mensagens"] and secao["editar"] == ["configurar_mensagens"]

    def test_o_menu_ganhou_o_modulo_e_toda_aba_exige_ver_mensagens(self):
        assert any(m["chave"] == "mensagens" for m in routes.MODULOS)
        for _, _, endpoint in gestao.ABAS:
            assert routes._REGISTRO_PERMISSOES[endpoint]["GET"] == "ver_mensagens"
        mapa = routes._REGISTRO_PERMISSOES
        assert mapa["erp.mensagens_api_politica"]["POST"] == "configurar_mensagens"
        assert mapa["erp.mensagens_api_parametros"]["POST"] == "configurar_mensagens"
        assert mapa["erp.mensagens_api_teste"]["POST"] == "configurar_mensagens"
        assert mapa["erp.mensagens_api_enviadas"]["GET"] == "ver_mensagens"

    @pytest.mark.parametrize("perfil", [P.CONSULTA, P.LANCADOR, P.SUPERVISOR_OBRA, P.FINANCEIRO])
    def test_perfil_sem_alcada_nao_entra(self, app, monkeypatch, perfil):
        c = _como(app, monkeypatch, perfil)
        assert c.get("/erp/mensagens").status_code == 403
        assert c.get("/erp/api/mensagens/enviadas").status_code == 403

    @pytest.mark.parametrize("perfil", [P.DIRETOR_FINANCEIRO, P.DEPARTAMENTO_PESSOAL])
    def test_quem_so_ve_nao_muda_politica(self, app, monkeypatch, perfil):
        c = _como(app, monkeypatch, perfil)
        assert c.get("/erp/mensagens").status_code == 200
        r = c.post("/erp/api/mensagens/politica", json={"chave": "ponto.qr", "politica": "TELEGRAM"})
        assert r.status_code == 403

    def test_as_telas_abrem_para_o_admin(self, app, monkeypatch):
        c = _como(app, monkeypatch, P.ADMIN)
        r = c.get("/erp/mensagens")
        assert r.status_code == 200 and "Por onde cada mensagem sai" in r.get_data(as_text=True)
        r = c.get("/erp/mensagens/enviadas")
        assert r.status_code == 200 and "Mensagens enviadas" in r.get_data(as_text=True)

    def test_sem_a_migracao_a_api_explica_em_vez_de_quebrar(self, app, monkeypatch):
        from app.apps.mensageria import db as mdb
        monkeypatch.setattr(mdb, "disponivel", lambda: False)
        c = _como(app, monkeypatch, P.ADMIN)
        d = c.get("/erp/api/mensagens/tipos").get_json()
        assert d["ok"] is True and d["banco_atrasado"] is True and "083" in d["erro"]

    def test_politica_desconhecida_e_recusada(self, app, monkeypatch):
        from app.apps.mensageria import db as mdb
        monkeypatch.setattr(mdb, "disponivel", lambda: True)
        monkeypatch.setattr(mdb, "conexao", contextlib.nullcontext)
        c = _como(app, monkeypatch, P.ADMIN)
        r = c.post("/erp/api/mensagens/politica", json={"chave": "ponto.qr", "politica": "FAX"})
        assert r.status_code == 400 and "política" in r.get_json()["erro"]

    def test_teste_de_envio_valida_canal_e_telefone(self, app, monkeypatch):
        from app.apps.mensageria import db as mdb
        monkeypatch.setattr(mdb, "disponivel", lambda: True)
        c = _como(app, monkeypatch, P.ADMIN)
        assert c.post("/erp/api/mensagens/teste", json={"canal": "fax", "telefone": "85999990000"}).status_code == 400
        assert c.post("/erp/api/mensagens/teste", json={"canal": "telegram", "telefone": "123"}).status_code == 400


def test_a_fila_do_ponto_diz_o_tipo_de_cada_mensagem():
    from app.apps.ponto.core import envios
    assert envios.FINALIDADE == {"QR": "ponto.qr", "MOSAICO": "ponto.mosaico", "AVISO_FOTO": "ponto.aviso_foto"}
