# ============================================================================
# ERP — app/apps/erp/routes.py
# Blueprint do ERP financeiro dentro do monorepo. Serve a interface HTML e a
# API JSON consumida por ela. Segue os padrões da casa: blueprint próprio,
# respostas {ok: true/false}, logging PT-BR, credenciais por envvar.
#
# Envvars: DATABASE_URL (Internal Database URL do erp-db) e ERP_SECRET_KEY
# (assinatura da sessão; usa fallback derivado se ausente).
# ============================================================================
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from decimal import Decimal
from functools import wraps

from flask import (
    Blueprint, jsonify, redirect, render_template, request, session, url_for,
)

from app.apps.erp.core.auth.service import ErroAutenticacao, autenticar
from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao
from app.apps.erp.core.titulos import service as svc_titulos
from app.apps.erp.db.database import get_session
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

bp = Blueprint("erp", __name__, template_folder="templates", static_folder="static",
               static_url_path="/erp/static")

_LIMITE_GRADE = 500
_ABERTOS = ("EM_ANALISE", "AGUARDANDO_APROVACAO", "APROVADO", "BLOQUEADO", "PAGO_PARCIAL")


# ---------------------------------------------------------------------------
# Sessão
# ---------------------------------------------------------------------------
def _usuario_logado(s) -> Usuario | None:
    uid = session.get("erp_usuario_id")
    return s.get(Usuario, uid) if uid else None


def login_obrigatorio(fn):
    @wraps(fn)
    def _wrap(*a, **kw):
        if not session.get("erp_usuario_id"):
            if request.path.startswith("/erp/api/"):
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            return redirect(url_for("erp.pagina_login"))
        return fn(*a, **kw)
    return _wrap


@bp.route("/erp/entrar", methods=["GET", "POST"])
def pagina_login():
    if request.method == "GET":
        return render_template("erp_login.html", erro=None)
    email = (request.form.get("email") or "").strip()
    senha = request.form.get("senha") or ""
    try:
        with get_session() as s:
            usuario = autenticar(s, email, senha)
            session["erp_usuario_id"] = usuario.id
            session["erp_usuario_nome"] = usuario.nome
            session["erp_usuario_perfil"] = usuario.perfil.value
        logger.info("ERP: login de %s", email)
        return redirect(url_for("erp.pagina_titulos"))
    except ErroAutenticacao as e:
        return render_template("erp_login.html", erro=str(e)), 401
    except Exception as e:  # falha de banco/config
        logger.exception("ERP: falha no login")
        return render_template("erp_login.html",
                               erro=f"Não foi possível conectar ao banco: {e}"), 500


@bp.route("/erp/sair")
def sair():
    session.pop("erp_usuario_id", None)
    session.pop("erp_usuario_nome", None)
    session.pop("erp_usuario_perfil", None)
    return redirect(url_for("erp.pagina_login"))


# ---------------------------------------------------------------------------
# Páginas
# ---------------------------------------------------------------------------
@bp.route("/erp/")
@bp.route("/erp/titulos")
@login_obrigatorio
def pagina_titulos():
    return render_template("erp_titulos.html",
                           usuario_nome=session.get("erp_usuario_nome", ""),
                           usuario_perfil=session.get("erp_usuario_perfil", ""))


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def _serializar(t, hoje: date) -> dict:
    venc = min((p.vencimento for p in t.parcelas), default=None)
    return {
        "id": t.id,
        "numero_sp": t.numero_sp,
        "fornecedor": t.fornecedor.razao_social,
        "descricao": t.descricao,
        "categoria": f"{t.categoria.codigo} · {t.categoria.descricao}",
        "valor_liquido": float(t.valor_liquido),
        "competencia": t.competencia.strftime("%m/%Y"),
        "vencimento": venc.isoformat() if venc else None,
        "vencido": bool(venc and venc < hoje and t.status.value in _ABERTOS),
        "parcelas": len(t.parcelas),
        "risco": t.score_risco or 0,
        "status": t.status.value,
    }


@bp.route("/erp/api/titulos")
@login_obrigatorio
def api_titulos():
    busca = (request.args.get("busca") or "").strip()
    status = [s for s in (request.args.get("status") or "").split(",") if s]
    try:
        with get_session() as s:
            itens = svc_titulos.listar(s, busca=busca, limite=_LIMITE_GRADE)
            hoje = date.today()
            linhas = [_serializar(t, hoje) for t in itens
                      if not status or t.status.value in status]
    except Exception as e:
        logger.exception("ERP: falha ao listar títulos")
        return jsonify({"ok": False, "erro": str(e)}), 500

    limite7 = date.today() + timedelta(days=7)
    def _soma(f):
        return round(sum(l["valor_liquido"] for l in linhas if f(l)), 2)
    resumo = {
        "quantidade": len(linhas),
        "total": _soma(lambda l: True),
        "aguardando": _soma(lambda l: l["status"] == "AGUARDANDO_APROVACAO"),
        "qtd_aguardando": sum(1 for l in linhas if l["status"] == "AGUARDANDO_APROVACAO"),
        "bloqueado": _soma(lambda l: l["status"] == "BLOQUEADO"),
        "qtd_bloqueado": sum(1 for l in linhas if l["status"] == "BLOQUEADO"),
        "vencendo": _soma(lambda l: l["vencimento"] and
                          date.fromisoformat(l["vencimento"]) <= limite7 and
                          l["status"] in _ABERTOS),
        "qtd_vencendo": sum(1 for l in linhas if l["vencimento"] and
                            date.fromisoformat(l["vencimento"]) <= limite7 and
                            l["status"] in _ABERTOS),
    }
    return jsonify({"ok": True, "titulos": linhas, "resumo": resumo,
                    "limite_atingido": len(itens) >= _LIMITE_GRADE})


@bp.route("/erp/api/titulos/<int:titulo_id>")
@login_obrigatorio
def api_titulo_detalhe(titulo_id: int):
    from sqlalchemy import select
    from app.apps.erp.db.models.financeiro import Analise, Evento
    try:
        with get_session() as s:
            t = svc_titulos.obter(s, titulo_id)
            analise = s.scalars(select(Analise).where(Analise.titulo_id == t.id)
                                .order_by(Analise.executada_em.desc())).first()
            eventos = s.scalars(select(Evento).where(
                Evento.entidade_tipo == "titulo", Evento.entidade_id == t.id)
                .order_by(Evento.criado_em)).all()
            dados = {
                "cabecalho": _serializar(t, date.today()),
                "bruto": float(t.valor_bruto),
                "retencoes_total": float(t.valor_retencoes),
                "dedutivel": t.dedutivel,
                "forma_pagamento": t.forma_pagamento.value,
                "parcelas": [{"numero": p.numero,
                              "vencimento": p.vencimento.strftime("%d/%m/%Y"),
                              "valor": float(p.valor), "status": p.status.value,
                              "boleto": (p.linha_digitavel or "")[:24]}
                             for p in t.parcelas],
                "rateios": [{"obra": f"{r.obra.codigo} · {r.obra.nome}",
                             "valor": float(r.valor),
                             "percentual": float(r.percentual or 0)} for r in t.rateios],
                "retencoes": [{"tipo": r.tipo.value, "base": float(r.base_calculo),
                               "aliquota": float(r.aliquota), "valor": float(r.valor)}
                              for r in t.retencoes],
                "criticas": (analise.criticas if analise else []) or [],
                "trilha": [{"quando": e.criado_em.strftime("%d/%m/%Y %H:%M"),
                            "acao": e.acao, "detalhe": e.detalhe} for e in eventos],
            }
        return jsonify({"ok": True, "titulo": dados})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except Exception as e:
        logger.exception("ERP: falha no detalhe do título %s", titulo_id)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/titulos/acao", methods=["POST"])
@login_obrigatorio
def api_acao_lote():
    payload = request.get_json(silent=True) or {}
    acao = (payload.get("acao") or "").strip()
    ids = payload.get("ids") or []
    motivo = (payload.get("motivo") or "").strip()
    if acao not in ("aprovar", "devolver", "cancelar"):
        return jsonify({"ok": False, "erro": f"Ação inválida: {acao!r}"}), 400
    if not ids:
        return jsonify({"ok": False, "erro": "Nenhum título selecionado."}), 400

    oks, erros = [], []
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            for tid in ids:
                try:
                    if acao == "aprovar":
                        t = svc_titulos.aprovar(s, int(tid), usuario)
                    elif acao == "devolver":
                        t = svc_titulos.devolver(s, int(tid), motivo, usuario)
                    else:
                        t = svc_titulos.cancelar(s, int(tid), motivo, usuario)
                    oks.append(t.numero_sp)
                except (ErroValidacao, ErroPermissao) as e:
                    erros.append({"id": tid, "erro": str(e)})
            s.commit()
    except Exception as e:
        logger.exception("ERP: falha na ação em lote %s", acao)
        return jsonify({"ok": False, "erro": str(e)}), 500
    logger.info("ERP: %s em lote — %d ok, %d com erro", acao, len(oks), len(erros))
    return jsonify({"ok": True, "processados": oks, "erros": erros})


@bp.route("/erp/health")
def health():
    """Health check do módulo — não exige login."""
    try:
        from sqlalchemy import text
        with get_session() as s:
            s.execute(text("SELECT 1"))
        return jsonify({"ok": True, "modulo": "erp", "banco": "conectado"}), 200
    except Exception as e:
        return jsonify({"ok": False, "modulo": "erp", "erro": str(e)}), 503
