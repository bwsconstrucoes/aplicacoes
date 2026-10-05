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

from . import core, db

logger = logging.getLogger("mensageria.gestao")

ABAS = [
    ("msg_tipos", "Tipos e canais", "erp.mensagens_pagina_tipos"),
    ("msg_envios", "Enviadas", "erp.mensagens_pagina_envios"),
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


_registrar_no_menu()
