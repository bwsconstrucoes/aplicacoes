# -*- coding: utf-8 -*-
"""
A tela MENSAGENS, dentro do ERP — decisão do dono em 05/10/2026 ("vamos
criar essa tela de mensageria").

Entra no ERP do mesmo jeito que a gestão do ponto: as rotas são registradas NO
BLUEPRINT DO ERP (`erp.routes.bp`), herdando login, guarda de permissão
(padrão NEGAR) e o menu. No ERP há uma linha só, protegida, no fim de
`erp/routes.py` — se este arquivo falhar ao carregar, o ERP sobe sem o menu
Mensagens.

QUEM PODE O QUÊ (permissoes.py / secoes.py, seção "Mensagens"):
  ver_mensagens         ver as políticas, os números e o registro de envios
  configurar_mensagens  mudar política, ligar/desligar o WhatsApp, mexer no
                        teto e mandar mensagem de teste

Duas abas: "Tipos e canais" (a gestão) e "Enviadas" (o registro, 90 dias).
"""
from __future__ import annotations

import logging
import os
import re
from functools import wraps

from flask import jsonify, render_template_string, request

from app.apps.erp import routes as R
from app.apps.erp.routes import bp, login_obrigatorio, permissao

from . import core, db, painel, telegram_cadastro as tc

logger = logging.getLogger("mensageria.gestao")

ABAS = [
    ("msg_painel", "Painel", "erp.mensagens_pagina_painel"),
    ("msg_tipos", "Tipos e canais", "erp.mensagens_pagina_tipos"),
    ("msg_envios", "Enviadas", "erp.mensagens_pagina_envios"),
    ("msg_telegram", "Cadastro do Telegram", "erp.mensagens_pagina_telegram"),
]
MODULO = {
    "chave": "mensagens", "nome": "Mensagens", "sigla": "MSG",
    "descricao": "Por onde cada aviso sai (Telegram ou WhatsApp), o teto do WhatsApp e o registro do que foi enviado",
    "cor": "var(--ciano)", "abas": ABAS,
}
ACOES_DA_TELA = ("ver_mensagens", "configurar_mensagens")

BANCO_ATRASADO = ("A mensageria ainda não está instalada no banco. Um administrador precisa abrir "
                  "Configurações › Manutenção do banco e apertar “Aplicar atualizações do banco” "
                  "(atualização 083). Até lá os envios seguem as variáveis de ambiente, como antes.")


def _registrar_no_menu() -> None:
    if any(m["chave"] == "mensagens" for m in R.MODULOS):
        return
    posicao = next((i for i, m in enumerate(R.MODULOS) if m["chave"] == "admin"), len(R.MODULOS))
    R.MODULOS.insert(posicao, MODULO)
    R._MODULO_DA_ABA.update({aba[0]: "mensagens" for aba in ABAS})


_PASTA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")


def _render(nome: str, **ctx):
    with open(os.path.join(_PASTA, nome), encoding="utf-8") as f:
        return render_template_string(f.read(), **ctx)


def _pagina(aba: str):
    ctx = R._contexto(aba)
    ctx["pode"] = {**ctx.get("pode", {}), **R._pode_agora(*ACOES_DA_TELA)}
    return _render("mensagens.html", aba_msg=aba, politicas=core.POLITICAS, **ctx)


def _quem() -> str:
    with R.get_session() as s:
        u = R._usuario_logado(s)
        return (u.nome if u else "") or ""


def _api(fn):
    @wraps(fn)
    def _envolta(*a, **kw):
        if not db.disponivel():
            return jsonify({"ok": True, "banco_atrasado": True, "erro": BANCO_ATRASADO}), 200
        try:
            return fn(*a, **kw)
        except ValueError as e:
            return jsonify({"ok": False, "erro": str(e)}), 400
        except Exception:  # noqa: BLE001
            logger.exception("Mensagens: erro em %s", request.path)
            return jsonify({"ok": False, "erro": "erro interno; o detalhe está no log"}), 500
    return _envolta


def _corpo() -> dict:
    d = request.get_json(silent=True)
    return d if isinstance(d, dict) else {}


# ---------------------------------------------------------------------------
# Telas
# ---------------------------------------------------------------------------
@bp.route("/erp/mensagens")
@login_obrigatorio
@permissao("ver_mensagens")
def mensagens_pagina_tipos():
    return _pagina("msg_tipos")


@bp.route("/erp/mensagens/enviadas")
@login_obrigatorio
@permissao("ver_mensagens")
def mensagens_pagina_envios():
    return _pagina("msg_envios")


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def _credenciais():
    from app.apps.notificador import whatsapp_configurado
    return {"whatsapp": whatsapp_configurado(),
            "telegram": bool(os.environ.get("TELEGRAM_BOT_TOKEN", "").strip())}


@bp.route("/erp/api/mensagens/tipos")
@login_obrigatorio
@permissao("ver_mensagens")
@_api
def mensagens_api_tipos():
    with db.conexao() as conn:
        tipos = core.listar_tipos(conn)
        res = core.resumo(conn)
    for t in tipos:
        t["atualizado_em"] = t["atualizado_em"].isoformat() if t.get("atualizado_em") else ""
    return jsonify({"ok": True, "tipos": tipos, "politicas": core.POLITICAS, "resumo": res,
                    "credenciais": _credenciais()})


@bp.route("/erp/api/mensagens/politica", methods=["POST"])
@login_obrigatorio
@permissao("configurar_mensagens")
@_api
def mensagens_api_politica():
    d = _corpo()
    chave = core.normalizar_chave(d.get("chave"))
    politica = str(d.get("politica") or "").strip().upper()
    with db.conexao() as conn:
        core.gravar_politica(conn, chave, politica, _quem())
    logger.info("Mensagens: %s passou a %s por %s", chave, politica, _quem())
    return jsonify({"ok": True})


@bp.route("/erp/api/mensagens/parametros", methods=["POST"])
@login_obrigatorio
@permissao("configurar_mensagens")
@_api
def mensagens_api_parametros():
    d, quem = _corpo(), _quem()
    with db.conexao() as conn:
        if "whatsapp_ligado" in d:
            ligado = d["whatsapp_ligado"] in (True, 1, "1", "true", "sim")
            core.gravar_parametro(conn, core.WHATSAPP_LIGADO, "1" if ligado else "", quem)
            logger.info("Mensagens: WhatsApp %s por %s", "LIGADO" if ligado else "desligado", quem)
        for chave, campo in ((core.POR_HORA, "por_hora"), (core.POR_DIA, "por_dia")):
            if campo in d:
                valor = re.sub(r"\D", "", str(d[campo] or ""))
                if not valor:
                    raise ValueError("o teto precisa ser um número inteiro (0 = sem teto)")
                core.gravar_parametro(conn, chave, valor, quem)
    return jsonify({"ok": True})


@bp.route("/erp/api/mensagens/enviadas")
@login_obrigatorio
@permissao("ver_mensagens")
@_api
def mensagens_api_enviadas():
    a = request.args
    with db.conexao() as conn:
        linhas = core.listar_envios(conn, tipo=core.normalizar_chave(a.get("tipo", "")),
                                    canal=(a.get("canal") or "").strip().lower()[:20],
                                    status=(a.get("status") or "").strip().upper()[:20],
                                    busca=(a.get("busca") or "")[:60],
                                    dias=int(a.get("dias") or 7), limite=int(a.get("limite") or 300))
    for l in linhas:
        l["criado_em"] = l["criado_em"].isoformat() if l.get("criado_em") else ""
        l["tipo_nome"] = core.nome_do_tipo(l["tipo"])
    return jsonify({"ok": True, "envios": linhas, "retencao_dias": core.RETENCAO_DIAS})


@bp.route("/erp/api/mensagens/teste", methods=["POST"])
@login_obrigatorio
@permissao("configurar_mensagens")
@_api
def mensagens_api_teste():
    """Uma mensagem de teste para um telefone, pelo canal escolhido. Passa pela
    chave geral do WhatsApp e pelo teto; fica no registro como "mensageria.teste"."""
    from app.apps import notificador
    d = _corpo()
    canal = str(d.get("canal") or "").strip().lower()
    telefone = re.sub(r"\D", "", str(d.get("telefone") or ""))
    if canal not in ("telegram", "whatsapp"):
        raise ValueError("escolha o canal: telegram ou whatsapp")
    if len(telefone) < 10:
        raise ValueError("informe o telefone com DDD")
    if not telefone.startswith("55"):
        telefone = "55" + telefone
    texto = f"✅ Teste da Mensageria do ERP BWS, pedido por {_quem() or 'um administrador'}."
    if canal == "whatsapp":
        with db.conexao() as conn:
            if not core.whatsapp_ligado(conn):
                raise ValueError("o WhatsApp está desligado na chave geral desta tela — ligue antes de testar")
            cabe, motivo = core.cabe_no_teto(conn)
        if not cabe:
            raise ValueError(motivo)
        r = notificador._wa_enviar_texto(telefone, texto)
        status = "ENVIADO" if r.get("ok") else "FALHOU"
    else:
        r = notificador._tg_notificar(telefone=telefone, mensagem=texto)
        status = notificador._status_tg(r)
    with db.conexao() as conn:
        core.registrar(conn, tipo="mensageria.teste", canal=canal, destinatario=telefone, cpf="",
                       texto=texto, nome_arquivo="", status=status,
                       detalhe=str(r.get("detalhe") or r.get("erro") or ""), origem="tela Mensagens")
    if not r.get("ok"):
        return jsonify({"ok": False, "erro": f"não saiu: {r.get('detalhe') or r.get('erro') or r}"}), 502
    return jsonify({"ok": True, "status": status})


# ---------------------------------------------------------------------------
# CADASTRO DO TELEGRAM (14/11/2026) — quem tem, quem não tem, e o convite
# ---------------------------------------------------------------------------
@bp.route("/erp/mensagens/telegram")
@login_obrigatorio
@permissao("ver_mensagens")
def mensagens_pagina_telegram():
    return _pagina("msg_telegram")


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else (v or "")


@bp.route("/erp/api/mensagens/telegram")
@login_obrigatorio
@permissao("ver_mensagens")
@_api
def mensagens_api_telegram():
    a = request.args
    with db.conexao() as conn:
        d = tc.painel(conn, filtro=(a.get("filtro") or "").strip().lower()[:20], busca=(a.get("busca") or "")[:60])
    for l in d["pessoas"]:
        l["telegram_desde"] = _iso(l.get("telegram_desde"))
        l["ultima_falha"] = _iso(l.get("ultima_falha"))
        if l.get("convite"):
            l["convite"] = {k: _iso(v) for k, v in l["convite"].items()}
    d["a_conferir"] = [{k: _iso(v) for k, v in c.items()} for c in d["a_conferir"]]
    d["tentativas"] = [{k: _iso(v) for k, v in c.items()} for c in d["tentativas"]]
    return jsonify({"ok": True, **d})


def _espaco_entre_convites() -> float:
    import random
    return random.uniform(20, 45)


def _convidar_em_lote(pessoas: list, por: str) -> None:
    """Um convite por vez, com pausa sorteada entre eles: mandar 50 WhatsApps de
    uma vez é o padrão que o Z-API bloqueia."""
    import time
    for i, p in enumerate(pessoas):
        if i:
            time.sleep(_espaco_entre_convites())
        try:
            r = tc.convidar(p, motivo="MANUAL", por=por)
            if r["status"] == "LIMITE":
                logger.warning("Mensagens: lote de convites parou no teto do WhatsApp (%s de %s)",
                               i, len(pessoas))
                return
        except Exception:  # noqa: BLE001
            logger.exception("Mensagens: convite em lote falhou para %s", p.get("nome"))


@bp.route("/erp/api/mensagens/telegram/convidar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_mensagens")
@_api
def mensagens_api_telegram_convidar():
    d, por = _corpo(), _quem()
    chaves = [str(c)[:12] for c in (d.get("chaves") or []) if c][: tc.LOTE_MAXIMO + 1]
    if not chaves:
        raise ValueError("escolha quem vai receber o convite")
    if len(chaves) > tc.LOTE_MAXIMO:
        raise ValueError(f"no máximo {tc.LOTE_MAXIMO} convites por vez — o WhatsApp bloqueia lote grande")
    with db.conexao() as conn:
        if not tc.migrado(conn):
            raise ValueError("migre antes o cadastro da planilha (botão no fim desta aba) — sem isso, "
                             "quem já tem Telegram pela planilha receberia convite à toa")
        if not core.whatsapp_ligado(conn):
            raise ValueError("o WhatsApp está desligado na aba Tipos e canais — ligue antes de convidar")
        elegiveis = {p["chave"]: p for p in tc.pessoas_elegiveis(conn)}
    pessoas = [elegiveis[c] for c in chaves if c in elegiveis]
    if not pessoas:
        raise ValueError("ninguém da lista é colaborador ou operador com telefone no ERP")
    if len(pessoas) == 1:
        r = tc.convidar(pessoas[0], motivo="MANUAL", por=por)
        if r["status"] != "ENVIADO":
            return jsonify({"ok": False, "erro": f"o convite não saiu: {r['detalhe'] or r['status']}"}), 400
        return jsonify({"ok": True, "enviados": 1, "em_fila": 0})
    import threading
    threading.Thread(target=_convidar_em_lote, args=(pessoas, por), name="convites-lote", daemon=True).start()
    logger.info("Mensagens: %s convites do Telegram em fila por %s", len(pessoas), por)
    return jsonify({"ok": True, "enviados": 0, "em_fila": len(pessoas)})


@bp.route("/erp/api/mensagens/telegram/configurar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_mensagens")
@_api
def mensagens_api_telegram_configurar():
    d, por = _corpo(), _quem()
    with db.conexao() as conn:
        if "automatico" in d:
            ligado = d["automatico"] in (True, 1, "1", "true", "sim")
            core.gravar_parametro(conn, tc.CONVITE_AUTOMATICO, "1" if ligado else "0", por)
            logger.info("Mensagens: convite automático do Telegram %s por %s",
                        "LIGADO" if ligado else "desligado", por)
        if "intervalo_dias" in d:
            valor = re.sub(r"\D", "", str(d["intervalo_dias"] or ""))
            if not valor or not 1 <= int(valor) <= 90:
                raise ValueError("o intervalo entre convites é de 1 a 90 dias")
            core.gravar_parametro(conn, tc.CONVITE_INTERVALO, valor, por)
        for campo, chave, maximo, frase in (
                ("falhas_entre", tc.CONVITE_FALHAS_ENTRE, 50, "o número de mensagens entre convites é de 1 a 50"),
                ("maximo_automatico", tc.CONVITE_MAXIMO, 10, "o máximo de convites automáticos é de 1 a 10")):
            if campo in d:
                valor = re.sub(r"\D", "", str(d[campo] or ""))
                if not valor or not 1 <= int(valor) <= maximo:
                    raise ValueError(frase)
                core.gravar_parametro(conn, chave, valor, por)
        if "texto" in d:
            core.gravar_parametro(conn, tc.CONVITE_TEXTO, tc.validar_modelo(str(d["texto"] or "")), por)
    return jsonify({"ok": True})


@bp.route("/erp/api/mensagens/telegram/migrar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_mensagens")
@_api
def mensagens_api_telegram_migrar():
    """Traz a aba TelegramID para o ERP, CRUZANDO cada linha com o cadastro de
    colaboradores, e desliga a planilha: daí em diante o robô e os envios só
    usam o ERP. Pode rodar de novo (antes de alguém editar a planilha à mão,
    por exemplo): quem já está no banco só ganha o que faltar."""
    from app.apps.telegram import telegram_bot as tb
    try:
        linhas = tb._ler_valores_com_retry(tb.PLAN_TGID_ID, tb.ABA_TGID)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"não consegui ler a aba TelegramID da planilha: {str(e)[:200]}")
    with db.conexao() as conn:
        r = tc.migrar_planilha(conn, linhas, _quem())
    logger.info("Mensagens: cadastro do Telegram migrado da planilha por %s: %s", _quem(), r["contagem"])
    return jsonify({"ok": True, "relatorio": r})


@bp.route("/erp/api/mensagens/telegram/desvincular", methods=["POST"])
@login_obrigatorio
@permissao("configurar_mensagens")
@_api
def mensagens_api_telegram_desvincular():
    d = _corpo()
    try:
        cadastro_id = int(d.get("id"))
    except (TypeError, ValueError):
        raise ValueError("cadastro não encontrado")
    with db.conexao() as conn:
        tc.desvincular(conn, cadastro_id, _quem())
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# PAINEL (14/11/2026) — o monitoramento: o que saiu, para quem, por onde, quando
# ---------------------------------------------------------------------------
@bp.route("/erp/mensagens/painel")
@login_obrigatorio
@permissao("ver_mensagens")
def mensagens_pagina_painel():
    return _pagina("msg_painel")


@bp.route("/erp/api/mensagens/painel")
@login_obrigatorio
@permissao("ver_mensagens")
@_api
def mensagens_api_painel():
    a = request.args
    filtros = {k: (a.get(k) or "").strip()[:80] for k in ("de", "ate", "dias", "canal", "tipo", "modulo",
                                                          "situacao", "pessoa")}
    with db.conexao() as conn:
        d = painel.dados(conn, filtros)
    return jsonify({"ok": True, **d})


# ---------------------------------------------------------------------------
# A FICHA DO COLABORADOR (14/11/2026) — o Telegram e as mensagens dele, dentro
# do cadastro de pessoas. Lê; quem convida ou desliga é a aba Cadastro do Telegram.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/mensagens/pessoa/<int:colaborador_id>")
@login_obrigatorio
@permissao("ver_mensagens")
@_api
def mensagens_api_pessoa(colaborador_id: int):
    with db.conexao() as conn:
        try:
            d = tc.resumo_da_pessoa(conn, colaborador_id)
        except ValueError:
            return jsonify({"ok": False, "erro": "não encontrado"}), 404
    if d.get("telegram"):
        d["telegram"]["desde"] = _iso(d["telegram"].get("desde"))
    if d.get("convite"):
        d["convite"] = {k: _iso(v) for k, v in d["convite"].items()}
    for m in d["mensagens"]:
        m["criado_em"] = _iso(m["criado_em"])
        m["texto"] = (m.get("texto") or "")[:200]
    return jsonify({"ok": True, **d})


_registrar_no_menu()
