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

# Abas do topo (chave, rótulo, endpoint)
ABAS = [
    ("titulos", "Títulos", "erp.pagina_titulos"),
    ("importar", "Importar", "erp.pagina_importar"),
    ("config", "Configurações", "erp.pagina_config"),
]


def _contexto(aba: str) -> dict:
    return {"abas": ABAS, "aba_ativa": aba,
            "usuario_nome": session.get("erp_usuario_nome", ""),
            "usuario_perfil": session.get("erp_usuario_perfil", "")}
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
    return render_template("erp_titulos.html", **_contexto("titulos"))


@bp.route("/erp/importar")
@login_obrigatorio
def pagina_importar():
    return render_template("erp_importar.html", **_contexto("importar"))


@bp.route("/erp/configuracoes")
@login_obrigatorio
def pagina_config():
    return render_template("erp_config.html", **_contexto("config"))


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def _serializar(t, hoje: date) -> dict:
    venc = min((p.vencimento for p in t.parcelas), default=None)
    obras = sorted({r.obra.codigo for r in t.rateios if r.obra})
    return {
        "obra": " + ".join(obras) if obras else "",
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
        "dedutibilidade": (t.dedutibilidade.value if hasattr(t.dedutibilidade, "value")
                           else str(t.dedutibilidade)),
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


# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
@bp.route("/erp/api/config")
@login_obrigatorio
def api_config():
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import Categoria, ContaBancaria, Obra
    try:
        with get_session() as s:
            cats = s.scalars(select(Categoria).order_by(Categoria.ordem, Categoria.codigo)).all()
            obras = s.scalars(select(Obra).order_by(Obra.codigo)).all()
            contas = s.scalars(select(ContaBancaria).order_by(ContaBancaria.descricao)).all()
            usuarios = s.scalars(select(Usuario).order_by(Usuario.nome)).all()
            dados = {
                "categorias": [{
                    "id": c.id, "codigo": c.codigo, "descricao": c.descricao,
                    "natureza": getattr(c, "natureza", "RESULTADO"),
                    "grupo_codigo": c.grupo_codigo or "0",
                    "grupo_nome": c.grupo_nome or "Sem grupo",
                    "subgrupo_codigo": c.subgrupo_codigo or "",
                    "subgrupo_nome": c.subgrupo_nome or "",
                    "uso": c.descricao_uso or "",
                    "sugestao_dedutivel": c.dedutivel_padrao,
                    "ativo": c.ativo,
                    "substituida_por": c.substituida_por_id,
                    "personalizada": c.personalizada,
                    "tipos": [(t.value if hasattr(t, "value") else str(t)).split("_", 1)[0]
                              for t in (c.tipos_permitidos or [])],
                } for c in cats],
                "obras": [{
                    "id": o.id, "codigo": o.codigo, "nome": o.nome,
                    "objeto": (o.objeto or "")[:120], "cliente": o.cliente,
                    "municipio": o.municipio, "uf": o.uf, "contrato": o.contrato,
                    "status": o.status,
                } for o in obras],
                "contas": [{"id": b.id, "descricao": b.descricao, "banco": b.banco_codigo,
                            "agencia": b.agencia, "conta": b.conta, "ativo": b.ativo}
                           for b in contas],
                "usuarios": [{"nome": u.nome, "email": u.email,
                              "perfil": u.perfil.value, "ativo": u.ativo} for u in usuarios],
            }
        return jsonify({"ok": True, "dados": dados})
    except Exception as e:
        logger.exception("ERP: falha ao carregar configurações")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/categoria", methods=["POST"])
@login_obrigatorio
def api_nova_categoria():
    from app.apps.erp.core.cadastros import categorias as svc_cat
    dados = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_cat.criar(s, {
                "codigo": dados.get("codigo"), "descricao": dados.get("descricao"),
                "natureza": dados.get("natureza") or "RESULTADO",
                "dedutivel_padrao": (dados.get("dedutivel_padrao") or "sim") == "sim",
            }, usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao criar categoria")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/obra", methods=["POST"])
@login_obrigatorio
def api_nova_obra():
    from app.apps.erp.core.cadastros import obras as svc_obra
    dados = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_obra.criar(s, dados, usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao criar obra")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/conta", methods=["POST"])
@login_obrigatorio
def api_nova_conta():
    from app.apps.erp.db.models.cadastros import ContaBancaria
    d = request.get_json(silent=True) or {}
    faltando = [c for c in ("descricao", "banco_codigo", "agencia", "conta") if not (d.get(c) or "").strip()]
    if faltando:
        return jsonify({"ok": False, "erro": f"Preencha: {', '.join(faltando)}."}), 400
    try:
        with get_session() as s:
            s.add(ContaBancaria(descricao=d["descricao"].strip(),
                                banco_codigo=d["banco_codigo"].strip(),
                                agencia=d["agencia"].strip(), conta=d["conta"].strip()))
            s.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("ERP: falha ao criar conta bancária")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Importação
# ---------------------------------------------------------------------------
@bp.route("/erp/api/importar/pipefy", methods=["POST"])
@login_obrigatorio
def api_importar_pipefy():
    from app.apps.erp.core.importadores.pipefy_cards import (
        ErroPipefy, buscar_cards, extrair_ids, importar_cards,
    )
    d = request.get_json(silent=True) or {}
    ids = extrair_ids(d.get("texto") or "")
    if not ids:
        return jsonify({"ok": False, "erro": "Nenhum ID de card reconhecido no texto colado."}), 400
    if len(ids) > 100:
        return jsonify({"ok": False, "erro": f"{len(ids)} cards de uma vez — importe em blocos de até 100."}), 400
    try:
        cards = buscar_cards(ids)
        if not cards:
            return jsonify({"ok": False, "erro": "Nenhum card encontrado com esses IDs."}), 404
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = importar_cards(
                s, cards, usuario,
                categoria_padrao_id=int(d["categoria_padrao_id"]) if d.get("categoria_padrao_id") else None,
                obra_padrao_id=int(d["obra_padrao_id"]) if d.get("obra_padrao_id") else None,
                criar_fornecedor=bool(d.get("criar_fornecedor", True)))
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroPipefy as e:
        return jsonify({"ok": False, "erro": str(e)}), 502
    except Exception as e:
        logger.exception("ERP: falha na importação do Pipefy")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/importar/csv", methods=["POST"])
@login_obrigatorio
def api_importar_csv():
    from app.apps.erp.core.importadores.planilhas import (
        importar_categorias_csv, importar_obras_csv,
    )
    arquivo = request.files.get("arquivo")
    tipo = (request.form.get("tipo") or "").strip()
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Arquivo não enviado."}), 400
    if tipo not in ("obras", "categorias"):
        return jsonify({"ok": False, "erro": f"Tipo inválido: {tipo!r}."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            fn = importar_obras_csv if tipo == "obras" else importar_categorias_csv
            rel = fn(s, conteudo, usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na importação de CSV")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/importar/ofx", methods=["POST"])
@login_obrigatorio
def api_importar_ofx():
    from app.apps.erp.core.pagamentos import service as svc_pag
    from app.apps.erp.core.pagamentos.ofx import ErroOFX
    arquivo = request.files.get("arquivo")
    conta_id = request.form.get("conta_id")
    if arquivo is None or not conta_id:
        return jsonify({"ok": False, "erro": "Envie o arquivo e escolha a conta."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = svc_pag.importar_ofx(s, conteudo, int(conta_id), usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroOFX as e:
        return jsonify({"ok": False, "erro": f"Arquivo OFX inválido: {e}"}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na importação de OFX")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/plano/instalar", methods=["POST"])
@login_obrigatorio
def api_instalar_plano():
    """Grava o plano financeiro padrão da BWS direto no banco (idempotente)."""
    from app.apps.erp.core.cadastros.plano_padrao import aplicar_plano
    from app.apps.erp.db.models.cadastros import PerfilUsuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            rel = aplicar_plano(s, usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except Exception as e:
        logger.exception("ERP: falha ao instalar plano financeiro")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/categoria/substituir", methods=["POST"])
@login_obrigatorio
def api_substituir_categoria():
    """Aposenta uma conta e remaneja seus títulos para outra."""
    from app.apps.erp.core.cadastros.plano_padrao import substituir_categoria
    from app.apps.erp.db.models.cadastros import PerfilUsuario
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            rel = substituir_categoria(s, int(d.get("origem_id")),
                                       int(d.get("destino_id")), usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao substituir categoria")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/titulos/dedutibilidade", methods=["POST"])
@login_obrigatorio
def api_definir_dedutibilidade():
    """Define a dedutibilidade do título — decisão do financeiro (ou da IA),
    tomada a partir do documento, não da categoria."""
    from datetime import datetime, timezone
    from decimal import Decimal
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import StatusDedutibilidade, Titulo
    d = request.get_json(silent=True) or {}
    ids = d.get("ids") or []
    situacao = (d.get("situacao") or "").strip().upper()
    motivo = (d.get("motivo") or "").strip()
    if situacao not in {s.value for s in StatusDedutibilidade}:
        return jsonify({"ok": False, "erro": f"Situação inválida: {situacao!r}"}), 400
    if situacao in ("INDEDUTIVEL", "PARCIAL") and len(motivo) < 10:
        return jsonify({"ok": False, "erro": "Indedutível ou parcial exige motivo "
                                             "(mínimo 10 caracteres)."}), 400
    valor = d.get("valor_dedutivel")
    processados, erros = [], []
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            for tid in ids:
                t = s.get(Titulo, int(tid))
                if t is None:
                    erros.append({"id": tid, "erro": "título não encontrado"})
                    continue
                if situacao == "PARCIAL" and not valor:
                    erros.append({"id": tid, "erro": "informe o valor dedutível"})
                    continue
                t.dedutibilidade = StatusDedutibilidade(situacao)
                t.dedutivel = situacao in ("DEDUTIVEL", "PARCIAL")
                t.dedutibilidade_valor = (Decimal(str(valor).replace(",", "."))
                                          if situacao == "PARCIAL" else None)
                t.dedutibilidade_motivo = motivo or None
                t.dedutibilidade_por = usuario.id
                t.dedutibilidade_em = datetime.now(timezone.utc)
                t.dedutibilidade_origem = d.get("origem") or "HUMANO"
                registrar_evento(s, "titulo", t.id, "DEDUTIBILIDADE_DEFINIDA",
                                 {"situacao": situacao, "motivo": motivo,
                                  "valor": str(valor) if valor else None}, usuario.id)
                processados.append(t.numero_sp)
            s.commit()
        return jsonify({"ok": True, "processados": processados, "erros": erros})
    except Exception as e:
        logger.exception("ERP: falha ao definir dedutibilidade")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/categoria/<int:categoria_id>", methods=["POST"])
@login_obrigatorio
def api_editar_categoria(categoria_id: int):
    """Renomeia/ajusta uma conta. A edição marca a conta como personalizada —
    reinstalar o plano padrão não sobrescreve mais o texto dela."""
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Categoria, PerfilUsuario, TipoTitulo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            cat = s.get(Categoria, categoria_id)
            if cat is None:
                return jsonify({"ok": False, "erro": "Categoria não encontrada."}), 404
            antes = {"codigo": cat.codigo, "descricao": cat.descricao,
                     "natureza": cat.natureza, "ativo": cat.ativo}

            if "descricao" in d:
                nova_desc = (d.get("descricao") or "").strip()
                if len(nova_desc) < 3:
                    return jsonify({"ok": False, "erro": "Descrição muito curta."}), 400
                cat.descricao = nova_desc
            if "codigo" in d and (d.get("codigo") or "").strip():
                novo_cod = d["codigo"].strip()
                if novo_cod != cat.codigo:
                    from sqlalchemy import select as _sel
                    if s.scalars(_sel(Categoria).where(Categoria.codigo == novo_cod)).first():
                        return jsonify({"ok": False, "erro": f"Já existe conta com o código {novo_cod}."}), 400
                    cat.codigo = novo_cod
            if "descricao_uso" in d:
                cat.descricao_uso = (d.get("descricao_uso") or "").strip() or None
            if "natureza" in d:
                nat = (d.get("natureza") or "").strip().upper()
                if nat not in ("RESULTADO", "FLUXO"):
                    return jsonify({"ok": False, "erro": "Natureza deve ser RESULTADO ou FLUXO."}), 400
                cat.natureza = nat
            if "grupo_nome" in d and (d.get("grupo_nome") or "").strip():
                cat.grupo_nome = d["grupo_nome"].strip()
            if "subgrupo_nome" in d and (d.get("subgrupo_nome") or "").strip():
                cat.subgrupo_nome = d["subgrupo_nome"].strip()
            if "tipos" in d and isinstance(d["tipos"], list):
                try:
                    cat.tipos_permitidos = [TipoTitulo(t) for t in d["tipos"]]
                except ValueError as e:
                    return jsonify({"ok": False, "erro": f"Tipo inválido: {e}"}), 400
            if "ativo" in d:
                cat.ativo = bool(d["ativo"])

            cat.personalizada = True
            s.flush()
            registrar_evento(s, "categoria", cat.id, "EDITADA",
                             {"antes": antes, "depois": {
                                 "codigo": cat.codigo, "descricao": cat.descricao,
                                 "natureza": cat.natureza, "ativo": cat.ativo}}, usuario.id)
            s.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("ERP: falha ao editar categoria %s", categoria_id)
        return jsonify({"ok": False, "erro": str(e)}), 500


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
