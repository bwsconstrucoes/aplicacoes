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

# ---------------------------------------------------------------------------
# Navegação em MÓDULOS
# O sistema não é um financeiro com apêndices: cada área é um módulo, com suas
# próprias telas. A barra de abas mostra só o módulo em que a pessoa está.
# ---------------------------------------------------------------------------
MODULOS = [
    {
        "chave": "financeiro", "nome": "Financeiro", "sigla": "FIN",
        "descricao": "Solicitações de pagamento, recebimentos, conciliação e relatórios",
        "cor": "var(--azul-claro)",
        "abas": [
            ("lancar", "Lançar", "erp.pagina_lancar"),
            ("prestacao", "Fundo fixo e cartão", "erp.pagina_prestacao"),
            ("titulos", "Solicitações", "erp.pagina_titulos"),
            ("confirmar", "Confirmar", "erp.pagina_confirmar"),
            ("pagamentos", "Pagamentos", "erp.pagina_pagamentos"),
            ("conciliacao", "Conciliação", "erp.pagina_conciliacao"),
            ("receber", "Receber", "erp.pagina_receber"),
            ("relatorios", "Relatórios", "erp.pagina_relatorios"),
            ("importar", "Importar", "erp.pagina_importar"),
        ],
    },
    {
        "chave": "obras", "nome": "Obras", "sigla": "OBR",
        "descricao": "Contratos, aditivos, vigências, tributação e documentação das obras",
        "cor": "var(--amarelo)",
        "abas": [
            ("obras", "Painel de obras", "erp.pagina_obras"),
        ],
    },
    {
        "chave": "admin", "nome": "Administração", "sigla": "ADM",
        "descricao": "Plano financeiro, operadores, banco de dados e auditoria",
        "cor": "var(--roxo)",
        "abas": [
            ("config", "Configurações", "erp.pagina_config"),
        ],
    },
]

# aba → módulo a que pertence
_MODULO_DA_ABA = {aba[0]: m["chave"] for m in MODULOS for aba in m["abas"]}


def _migracoes_pendentes() -> list[str]:
    """Consulta barata, usada para avisar em toda tela quando o banco está
    atrás do código — evita a tela quebrar com 'column does not exist'."""
    try:
        from app.apps.erp.core.comum.migracoes import listar_estado
        return listar_estado().get("pendentes", [])
    except Exception:
        return []


def _contexto(aba: str) -> dict:
    chave = _MODULO_DA_ABA.get(aba, "financeiro")
    modulo = next(m for m in MODULOS if m["chave"] == chave)
    return {"modulos": MODULOS, "modulo": modulo, "abas": modulo["abas"],
            "aba_ativa": aba,
            "usuario_nome": session.get("erp_usuario_nome", ""),
            "usuario_perfil": session.get("erp_usuario_perfil", ""),
            "migracoes_pendentes": _migracoes_pendentes()}
_ABERTOS = ("EM_ANALISE", "AGUARDANDO_APROVACAO", "APROVADO", "BLOQUEADO", "PAGO_PARCIAL")


# ---------------------------------------------------------------------------
# Sessão
# ---------------------------------------------------------------------------
def _usuario_logado(s) -> Usuario | None:
    uid = session.get("erp_usuario_id")
    return s.get(Usuario, uid) if uid else None


def _perfil_bruto(s) -> str:
    """Perfil por SQL direto, sem passar pelo ORM.

    Necessário no caminho de manutenção: se o banco ainda não tem as colunas
    que o modelo espera, carregar o objeto Usuario quebra — e justamente o
    botão que conserta o banco ficaria inutilizável (impasse circular)."""
    from sqlalchemy import text as _text
    uid = session.get("erp_usuario_id")
    if not uid:
        return ""
    linha = s.execute(_text("SELECT perfil::text FROM usuarios WHERE id = :i"),
                      {"i": uid}).first()
    return linha[0] if linha else ""


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
        return redirect(url_for("erp.pagina_inicio"))
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


@bp.route("/erp/inicio")
@login_obrigatorio
def pagina_inicio():
    """Porta de entrada: escolha do módulo."""
    return render_template("erp_inicio.html", modulos=MODULOS, modulo=None,
                           abas=[], aba_ativa="",
                           usuario_nome=session.get("erp_usuario_nome", ""),
                           usuario_perfil=session.get("erp_usuario_perfil", ""),
                           migracoes_pendentes=_migracoes_pendentes())


@bp.route("/erp/lancar")
@login_obrigatorio
def pagina_lancar():
    return render_template("erp_lancar.html", **_contexto("lancar"))


@bp.route("/erp/confirmar")
@login_obrigatorio
def pagina_confirmar():
    return render_template("erp_confirmar.html", **_contexto("confirmar"))


@bp.route("/erp/pagamentos")
@login_obrigatorio
def pagina_pagamentos():
    return render_template("erp_pagamentos.html", **_contexto("pagamentos"))


@bp.route("/erp/conciliacao")
@login_obrigatorio
def pagina_conciliacao():
    return render_template("erp_conciliacao.html", **_contexto("conciliacao"))


@bp.route("/erp/receber")
@login_obrigatorio
def pagina_receber():
    return render_template("erp_receber.html", **_contexto("receber"))


@bp.route("/erp/obras")
@login_obrigatorio
def pagina_obras():
    return render_template("erp_obras.html", **_contexto("obras"))


@bp.route("/erp/relatorios")
@login_obrigatorio
def pagina_relatorios():
    return render_template("erp_relatorios.html", **_contexto("relatorios"))


@bp.route("/erp/prestacao")
@login_obrigatorio
def pagina_prestacao():
    return render_template("erp_prestacao.html", **_contexto("prestacao"))


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
def _serializar(t, hoje: date, ver_pagamento: bool = True) -> dict:
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
        "exige_aval": bool(getattr(t, "exige_aval", False)),
        "avalizado": bool(getattr(t, "avalizado_em", None)),
        "ver_pagamento": ver_pagamento,
    }


@bp.route("/erp/api/titulos")
@login_obrigatorio
def api_titulos():
    busca = (request.args.get("busca") or "").strip()
    status = [s for s in (request.args.get("status") or "").split(",") if s]
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            from app.apps.erp.core.titulos.aval import pode_ver_dados_pagamento
            ver_pg = pode_ver_dados_pagamento(usuario)
            itens = svc_titulos.listar(s, busca=busca, limite=_LIMITE_GRADE, usuario=usuario)
            hoje = date.today()
            linhas = [_serializar(t, hoje, ver_pg) for t in itens
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
            from app.apps.erp.core.titulos.aval import (
                historico_avais, pode_ver_dados_pagamento,
            )
            ver_pg = pode_ver_dados_pagamento(_usuario_logado(s))
            dados = {
                "cabecalho": _serializar(t, date.today(), ver_pg),
                "avais": historico_avais(s, t.id),
                "bruto": float(t.valor_bruto),
                "retencoes_total": float(t.valor_retencoes),
                "dedutivel": t.dedutivel,
                "forma_pagamento": t.forma_pagamento.value if ver_pg else "—",
                "parcelas": [{"numero": p.numero,
                              "vencimento": p.vencimento.strftime("%d/%m/%Y"),
                              "valor": float(p.valor), "status": p.status.value,
                              "boleto": ((p.linha_digitavel or "")[:24] if ver_pg
                                         else ("informado" if p.linha_digitavel else ""))}
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


@bp.route("/erp/api/manutencao/banco")
@login_obrigatorio
def api_estado_banco():
    from app.apps.erp.core.comum.migracoes import listar_estado
    try:
        return jsonify({"ok": True, "estado": listar_estado()})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/manutencao/banco/aplicar", methods=["POST"])
@login_obrigatorio
def api_aplicar_migracoes():
    """Botão de atualização do banco — restrito a ADMIN."""
    from app.apps.erp.core.comum.migracoes import aplicar_pendentes
    try:
        with get_session() as s:
            perfil = _perfil_bruto(s)      # SQL direto: funciona com o banco atrasado
            if perfil != "ADMIN":
                return jsonify({"ok": False, "erro": "Restrito ao ADMIN."}), 403
        rel = aplicar_pendentes()
        logger.info("ERP: %s migração(ões) aplicada(s) por usuário %s",
                    len(rel["aplicadas"]), session.get("erp_usuario_id"))
        return jsonify({"ok": True, "relatorio": rel})
    except Exception as e:
        logger.exception("ERP: falha ao aplicar migrações")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/depara")
@login_obrigatorio
def api_listar_depara():
    from app.apps.erp.core.cadastros.depara import listar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "depara": listar(s)})
    except Exception as e:
        logger.exception("ERP: falha ao listar de-para")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/depara/instalar", methods=["POST"])
@login_obrigatorio
def api_instalar_depara():
    from app.apps.erp.core.cadastros.depara import instalar_depara_padrao
    from app.apps.erp.db.models.cadastros import PerfilUsuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            rel = instalar_depara_padrao(s, usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except Exception as e:
        logger.exception("ERP: falha ao instalar de-para")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/depara/definir", methods=["POST"])
@login_obrigatorio
def api_definir_depara():
    from app.apps.erp.core.cadastros.depara import definir
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            definir(s, int(d.get("depara_id")), int(d.get("categoria_id")), usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao definir tradução")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Lançamento
# ---------------------------------------------------------------------------
@bp.route("/erp/api/lancamento/dados")
@login_obrigatorio
def api_dados_lancamento():
    """Cadastros necessários ao formulário, já filtrados para uso."""
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import (
        Categoria, Fornecedor, Obra, StatusConta,
    )
    try:
        with get_session() as s:
            forns = s.scalars(select(Fornecedor).where(Fornecedor.ativo.is_(True))
                              .order_by(Fornecedor.razao_social)).all()
            obras = s.scalars(select(Obra).where(Obra.status == "ATIVA")
                              .order_by(Obra.codigo)).all()
            cats = s.scalars(select(Categoria).where(Categoria.ativo.is_(True))
                             .order_by(Categoria.ordem, Categoria.codigo)).all()
            dados = {
                "fornecedores": [{
                    "id": f.id, "nome": f.razao_social, "documento": f.cnpj_cpf,
                    "situacao_rfb": f.situacao_rfb,
                    "contas": [{"id": ct.id, "forma": ct.forma.value,
                                "identificacao": ct.pix_chave or
                                f"{ct.banco_codigo}/{ct.agencia}/{ct.conta}"}
                               for ct in f.contas if ct.status == StatusConta.HOMOLOGADA],
                } for f in forns],
                "obras": [{"id": o.id, "codigo": o.codigo, "nome": o.nome} for o in obras],
                "categorias": [{
                    "id": c.id, "codigo": c.codigo, "descricao": c.descricao,
                    "grupo": c.grupo_nome or "", "subgrupo": c.subgrupo_nome or "",
                    "uso": c.descricao_uso or "", "natureza": c.natureza,
                    "tipos": [(t.value if hasattr(t, "value") else str(t))
                              for t in (c.tipos_permitidos or [])],
                } for c in cats],
            }
        return jsonify({"ok": True, "dados": dados})
    except Exception as e:
        logger.exception("ERP: falha ao carregar dados do lançamento")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lancamento/ler-documento", methods=["POST"])
@login_obrigatorio
def api_ler_documento():
    """Lê o anexo (XML/PDF/imagem) e devolve os campos sugeridos."""
    from sqlalchemy import select
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.db.models.cadastros import Fornecedor, Obra
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Nenhum arquivo enviado."}), 400
    try:
        lido = ler_documento(arquivo.read(), arquivo.filename or "",
                             dica_usuario=(request.form.get("dica") or ""))
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao ler documento")
        return jsonify({"ok": False, "erro": f"Falha ao ler o documento: {e}"}), 500

    # amarra ao que já existe no cadastro
    try:
        with get_session() as s:
            doc = somente_digitos(lido.get("emitente_documento") or "")
            forn = None
            if doc:
                forn = s.scalars(select(Fornecedor).where(Fornecedor.cnpj_cpf == doc)).first()
            lido["fornecedor_id"] = forn.id if forn else None
            lido["fornecedor_nome_cadastro"] = forn.razao_social if forn else None
            obra_txt = (lido.get("obra_mencionada") or "").strip()
            if obra_txt:
                o = s.scalars(select(Obra).where(Obra.codigo == obra_txt.upper())).first()
                lido["obra_id"] = o.id if o else None
    except Exception:
        logger.exception("ERP: falha ao casar documento com cadastros")
    return jsonify({"ok": True, "documento": lido})


@bp.route("/erp/api/lancamento/checar", methods=["POST"])
@login_obrigatorio
def api_checar_duplicidade():
    """Crítica de duplicidade antes de gravar."""
    from app.apps.erp.core.titulos.duplicidade import checar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            return jsonify({"ok": True, "critica": checar(s, d)})
    except Exception as e:
        logger.exception("ERP: falha na crítica de duplicidade")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lancamento", methods=["POST"])
@login_obrigatorio
def api_criar_titulo():
    """Grava o título. A crítica de duplicidade roda de novo aqui — o que
    bloqueia na tela não pode passar por chamada direta."""
    from app.apps.erp.core.titulos import service as svc
    from app.apps.erp.core.titulos.duplicidade import checar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            critica = checar(s, {
                "fornecedor_id": d.get("fornecedor_id"),
                "valor": d.get("valor_bruto"), "parcelas": d.get("parcelas") or [],
                "competencia": d.get("competencia"), "descricao": d.get("descricao"),
                "categoria_id": d.get("categoria_id"),
                "documento_numero": d.get("documento_numero")})
            if critica["bloqueios"] and not d.get("forcar"):
                return jsonify({"ok": False, "erro": "Lançamento bloqueado por duplicidade.",
                                "critica": critica}), 409
            usuario = _usuario_logado(s)
            if not d.get("tipo"):
                from app.apps.erp.core.titulos.derivacao import derivar_por_contexto
                from app.apps.erp.db.models.cadastros import Categoria
                cat = s.get(Categoria, int(d.get("categoria_id") or 0))
                if cat is None:
                    return jsonify({"ok": False,
                                    "erro": "Escolha a conta do plano financeiro."}), 400
                tipo = derivar_por_contexto(cat, d)
                d["tipo"] = tipo.value
                if tipo.value == "T14_EXCECAO_SEM_NOTA" and not d.get("justificativa_excecao"):
                    d["justificativa_excecao"] = (
                        f"Lançamento sem documento fiscal vinculado — "
                        f"registrado por {usuario.email}.")
            titulo = svc.criar_titulo(s, d, usuario)
            if critica["alertas"]:
                from app.apps.erp.core.comum.auditoria import registrar_evento
                registrar_evento(s, "titulo", titulo.id, "ALERTAS_DUPLICIDADE_ACEITOS",
                                 {"alertas": critica["alertas"],
                                  "confirmado_por": usuario.email}, usuario.id)
            s.commit()
            return jsonify({"ok": True, "titulo": {
                "id": titulo.id, "numero_sp": titulo.numero_sp,
                "status": titulo.status.value, "risco": titulo.score_risco}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except Exception as e:
        logger.exception("ERP: falha ao criar título")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Pagamentos e lotes
# ---------------------------------------------------------------------------
@bp.route("/erp/api/pagamentos/agenda")
@login_obrigatorio
def api_agenda():
    """Parcelas liberadas aguardando pagamento."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.apps.erp.db.models.cadastros import ContaBancaria
    from app.apps.erp.db.models.financeiro import (
        LoteItem, Parcela, StatusParcela, StatusTitulo, Titulo,
    )
    try:
        with get_session() as s:
            parcelas = s.scalars(
                select(Parcela).join(Titulo, Parcela.titulo_id == Titulo.id)
                .where(Parcela.status.in_([StatusParcela.ABERTA, StatusParcela.AGENDADA]),
                       Titulo.status.in_([StatusTitulo.APROVADO, StatusTitulo.PAGO_PARCIAL]))
                .options(selectinload(Parcela.titulo).selectinload(Titulo.fornecedor),
                         selectinload(Parcela.titulo).selectinload(Titulo.rateios))
                .order_by(Parcela.vencimento)).all()
            em_lote = {i.parcela_id for i in s.scalars(select(LoteItem)).all()}
            hoje = date.today()
            itens = [{
                "parcela_id": p.id, "titulo_id": p.titulo.id,
                "numero_sp": p.titulo.numero_sp, "parcela": p.numero,
                "credor": p.titulo.fornecedor.razao_social,
                "descricao": p.titulo.descricao,
                "obra": " + ".join(sorted({r.obra.codigo for r in p.titulo.rateios if r.obra})),
                "valor": float(p.valor), "vencimento": p.vencimento.isoformat(),
                "atrasada": p.vencimento < hoje,
                "forma": p.titulo.forma_pagamento.value,
                "tem_boleto": bool(p.linha_digitavel),
                "em_lote": p.id in em_lote,
            } for p in parcelas]
            contas = [{"id": c.id, "descricao": c.descricao}
                      for c in s.scalars(select(ContaBancaria)
                                         .where(ContaBancaria.ativo.is_(True))).all()]
        return jsonify({"ok": True, "parcelas": itens, "contas": contas})
    except Exception as e:
        logger.exception("ERP: falha na agenda de pagamentos")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/detalhe/<int:parcela_id>")
@login_obrigatorio
def api_detalhe_pagamento(parcela_id: int):
    from app.apps.erp.core.pagamentos.lotes import dados_pagamento
    try:
        with get_session() as s:
            return jsonify({"ok": True, "pagamento": dados_pagamento(s, parcela_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except Exception as e:
        logger.exception("ERP: falha no detalhe de pagamento")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/baixar", methods=["POST"])
@login_obrigatorio
def api_baixar():
    """Registra o pagamento de uma ou várias parcelas."""
    from app.apps.erp.core.pagamentos import service as svc_pag
    d = request.get_json(silent=True) or {}
    itens = d.get("itens") or []
    conta_id = d.get("conta_bancaria_id")
    data_pg = d.get("data_pagamento") or date.today().isoformat()
    if not itens or not conta_id:
        return jsonify({"ok": False, "erro": "Informe as parcelas e a conta de saída."}), 400
    ok, erros = [], []
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            for it in itens:
                try:
                    pg = svc_pag.registrar_pagamento(
                        s, parcela_id=int(it["parcela_id"]),
                        conta_bancaria_id=int(conta_id),
                        data_pagamento=date.fromisoformat(data_pg),
                        valor_pago=it.get("valor"), usuario=usuario)
                    ok.append({"parcela_id": pg.parcela_id, "valor": float(pg.valor_pago),
                               "pagamento_id": pg.id})
                except (ErroValidacao, ErroPermissao) as e:
                    erros.append({"parcela_id": it.get("parcela_id"), "erro": str(e)})
            s.commit()
            avisos = []
            if d.get("avisar", True):
                from app.apps.erp.core.notificacoes import avisar_baixa
                for item in ok:
                    try:
                        avisos.append(avisar_baixa(s, item["pagamento_id"]))
                    except Exception as e:      # aviso não derruba a baixa
                        logger.warning("ERP: aviso falhou (%s)", e)
                s.commit()
        return jsonify({"ok": True, "pagas": ok, "erros": erros, "avisos": avisos})
    except Exception as e:
        logger.exception("ERP: falha ao baixar pagamentos")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes")
@login_obrigatorio
def api_lotes():
    from app.apps.erp.core.pagamentos.lotes import listar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "lotes": listar(s)})
    except Exception as e:
        logger.exception("ERP: falha ao listar lotes")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>")
@login_obrigatorio
def api_lote_detalhe(lote_id: int):
    from app.apps.erp.core.pagamentos.lotes import detalhar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "lote": detalhar(s, lote_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/lotes/criar", methods=["POST"])
@login_obrigatorio
def api_criar_lote():
    from app.apps.erp.core.pagamentos.lotes import adicionar_parcelas, criar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            lote = criar(s, d, usuario)
            rel = {"incluidas": [], "recusadas": []}
            if d.get("parcela_ids"):
                rel = adicionar_parcelas(s, lote.id, d["parcela_ids"], usuario)
            s.commit()
            return jsonify({"ok": True, "lote_id": lote.id, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao criar lote")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/parcelas", methods=["POST"])
@login_obrigatorio
def api_lote_parcelas(lote_id: int):
    from app.apps.erp.core.pagamentos.lotes import adicionar_parcelas, remover_parcela
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if d.get("remover"):
                remover_parcela(s, lote_id, int(d["remover"]), usuario)
                s.commit()
                return jsonify({"ok": True})
            rel = adicionar_parcelas(s, lote_id, d.get("parcela_ids") or [], usuario)
            s.commit()
            return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao alterar lote")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/status", methods=["POST"])
@login_obrigatorio
def api_lote_status(lote_id: int):
    from app.apps.erp.core.pagamentos.lotes import mudar_status
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            mudar_status(s, lote_id, (d.get("status") or "").upper(), usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/lotes/por-sp", methods=["POST"])
@login_obrigatorio
def api_lote_por_sp():
    """Recebe o texto colado (a mensagem que volta do solicitante) e devolve
    as parcelas correspondentes."""
    from app.apps.erp.core.pagamentos.lotes import extrair_ids_sp, parcelas_por_sp
    d = request.get_json(silent=True) or {}
    numeros = extrair_ids_sp(d.get("texto") or "")
    if not numeros:
        return jsonify({"ok": False, "erro": "Nenhum número de SP reconhecido no texto."}), 400
    try:
        with get_session() as s:
            return jsonify({"ok": True, "reconhecidos": numeros, **parcelas_por_sp(s, numeros)})
    except Exception as e:
        logger.exception("ERP: falha ao buscar SPs coladas")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Conciliação
# ---------------------------------------------------------------------------
@bp.route("/erp/api/conciliacao/painel")
@login_obrigatorio
def api_conciliacao_painel():
    from sqlalchemy import select
    from app.apps.erp.core.pagamentos.conciliacao import painel
    from app.apps.erp.db.models.cadastros import ContaBancaria
    conta = request.args.get("conta_id", type=int)
    try:
        with get_session() as s:
            contas = [{"id": c.id, "descricao": c.descricao}
                      for c in s.scalars(select(ContaBancaria)
                                         .where(ContaBancaria.ativo.is_(True))).all()]
            return jsonify({"ok": True, "painel": painel(s, conta), "contas": contas})
    except Exception as e:
        logger.exception("ERP: falha no painel de conciliação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/conciliacao/executar", methods=["POST"])
@login_obrigatorio
def api_conciliar():
    from app.apps.erp.core.pagamentos.conciliacao import conciliar_automatico
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = conciliar_automatico(
                s, conta_bancaria_id=d.get("conta_id") or None, usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except Exception as e:
        logger.exception("ERP: falha na conciliação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/conciliacao/manual", methods=["POST"])
@login_obrigatorio
def api_conciliar_manual():
    from app.apps.erp.core.pagamentos.conciliacao import conciliar_manual
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            conciliar_manual(s, int(d["pagamento_id"]), int(d["extrato_id"]), usuario,
                             d.get("observacao", ""))
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na conciliação manual")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Relatórios
# ---------------------------------------------------------------------------
@bp.route("/erp/api/relatorios", methods=["POST"])
@login_obrigatorio
def api_relatorios():
    from app.apps.erp.core.relatorios import analitico, dre_gerencial, resumo
    d = request.get_json(silent=True) or {}
    tipo = (d.get("tipo") or "resumo").strip()
    filtros = d.get("filtros") or {}
    try:
        with get_session() as s:
            if tipo == "dre":
                return jsonify({"ok": True, "dre": dre_gerencial(s, filtros)})
            if tipo == "analitico":
                return jsonify({"ok": True, "linhas": analitico(s, filtros)})
            return jsonify({"ok": True,
                            "resumo": resumo(s, d.get("dimensao") or "grupo", filtros)})
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no relatório")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/relatorios/csv", methods=["POST"])
@login_obrigatorio
def api_relatorios_csv():
    from flask import Response
    from app.apps.erp.core.relatorios import analitico, para_csv, resumo
    d = request.get_json(silent=True) or {}
    filtros = d.get("filtros") or {}
    try:
        with get_session() as s:
            if (d.get("tipo") or "") == "analitico":
                linhas = analitico(s, filtros)
                colunas = [("numero_sp", "SP"), ("competencia", "Competência"),
                           ("credor", "Credor"), ("descricao", "Descrição"),
                           ("grupo", "Grupo"), ("conta", "Conta"), ("obra", "Obra"),
                           ("valor", "Valor"), ("vencimento", "Vencimento"),
                           ("pagamento", "Pagamento"), ("situacao", "Situação"),
                           ("dedutibilidade", "Dedutibilidade")]
                nome = "erp_analitico.csv"
            else:
                r = resumo(s, d.get("dimensao") or "grupo", filtros)
                linhas = r["linhas"]
                colunas = [("chave", r["rotulo"]), ("titulos", "Títulos"),
                           ("total", "Total"), ("pago", "Pago"), ("aberto", "Em aberto"),
                           ("percentual", "%")]
                nome = f"erp_{d.get('dimensao') or 'grupo'}.csv"
            conteudo = para_csv(linhas, colunas)
        return Response(conteudo.encode("utf-8-sig"), mimetype="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="{nome}"'})
    except Exception as e:
        logger.exception("ERP: falha ao exportar CSV")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Extrato, movimentações e recebimentos
# ---------------------------------------------------------------------------
@bp.route("/erp/api/conciliacao/extrato")
@login_obrigatorio
def api_extrato():
    from app.apps.erp.core.pagamentos.conciliacao import extrato_detalhado
    try:
        with get_session() as s:
            linhas = extrato_detalhado(
                s, conta_bancaria_id=request.args.get("conta_id", type=int),
                situacao=request.args.get("situacao", "todos"))
        return jsonify({"ok": True, "linhas": linhas})
    except Exception as e:
        logger.exception("ERP: falha ao listar extrato")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/conciliacao/candidatos/<int:extrato_id>")
@login_obrigatorio
def api_candidatos(extrato_id: int):
    from app.apps.erp.core.pagamentos.conciliacao import candidatos_para_extrato
    try:
        with get_session() as s:
            return jsonify({"ok": True, "candidatos": candidatos_para_extrato(s, extrato_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/movimentacoes", methods=["GET", "POST"])
@login_obrigatorio
def api_movimentacoes():
    from app.apps.erp.core.titulos.receber import (
        TIPOS_MOVIMENTO, criar_movimentacao, listar_movimentacoes,
    )
    if request.method == "GET":
        try:
            with get_session() as s:
                return jsonify({"ok": True, "movimentacoes": listar_movimentacoes(s),
                                "tipos": [{"chave": k, "rotulo": v[0], "conta": v[1],
                                           "exige_origem": v[2], "exige_destino": v[3]}
                                          for k, v in TIPOS_MOVIMENTO.items()]})
        except Exception as e:
            logger.exception("ERP: falha ao listar movimentações")
            return jsonify({"ok": False, "erro": str(e)}), 500
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            mov = criar_movimentacao(s, d, usuario)
            s.commit()
            return jsonify({"ok": True, "movimentacao_id": mov.id,
                            "neutra": mov.neutra, "par_id": mov.par_id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao criar movimentação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/receber", methods=["GET", "POST"])
@login_obrigatorio
def api_receber():
    from app.apps.erp.core.titulos.receber import criar_medicao, listar_receber
    if request.method == "GET":
        try:
            with get_session() as s:
                return jsonify({"ok": True, "titulos": listar_receber(s, {
                    "obra_id": request.args.get("obra_id", type=int),
                    "status": request.args.get("status")})})
        except Exception as e:
            logger.exception("ERP: falha ao listar recebíveis")
            return jsonify({"ok": False, "erro": str(e)}), 500
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            t = criar_medicao(s, d, usuario)
            s.commit()
            return jsonify({"ok": True, "titulo": {"id": t.id, "numero_sp": t.numero_sp,
                                                   "liquido": float(t.valor_liquido)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao lançar medição")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/receber/baixar", methods=["POST"])
@login_obrigatorio
def api_receber_baixar():
    from app.apps.erp.core.titulos.receber import registrar_recebimento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = registrar_recebimento(
                s, parcela_id=int(d["parcela_id"]),
                conta_bancaria_id=int(d["conta_bancaria_id"]),
                data_recebimento=date.fromisoformat(d.get("data") or date.today().isoformat()),
                valor=d.get("valor"), notas_fiscais=d.get("notas_fiscais") or [],
                usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "recebimento": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no recebimento")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Ajustes: reclassificar e desfazer
# ---------------------------------------------------------------------------
@bp.route("/erp/api/titulos/<int:titulo_id>/reclassificar", methods=["POST"])
@login_obrigatorio
def api_reclassificar(titulo_id: int):
    """Troca conta do plano e/ou obra mesmo com o título pago e conciliado."""
    from app.apps.erp.core.titulos.ajustes import reclassificar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = reclassificar(s, titulo_id,
                                categoria_id=d.get("categoria_id"),
                                rateios=d.get("rateios"),
                                motivo=d.get("motivo", ""), usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except Exception as e:
        logger.exception("ERP: falha ao reclassificar")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/titulos/<int:titulo_id>/desfazer", methods=["GET", "POST"])
@login_obrigatorio
def api_desfazer(titulo_id: int):
    """GET diz o que será desfeito; POST desfaz conciliação e baixa de uma vez."""
    from app.apps.erp.core.titulos.ajustes import desfazer_em_cadeia, diagnosticar_desfazer
    try:
        if request.method == "GET":
            with get_session() as s:
                return jsonify({"ok": True, "diagnostico": diagnosticar_desfazer(s, titulo_id)})
        d = request.get_json(silent=True) or {}
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = desfazer_em_cadeia(s, titulo_id, d.get("motivo", ""), usuario,
                                     ate=(d.get("ate") or "APROVADO").upper())
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except Exception as e:
        logger.exception("ERP: falha ao desfazer")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>", methods=["GET", "POST"])
@login_obrigatorio
def api_obra(obra_id: int):
    """Cadastro completo da obra: identificação, endereço, contrato e tributação."""
    from sqlalchemy import select
    from app.apps.erp.core.titulos.tributacao import resumo_tributacao
    from app.apps.erp.db.models.cadastros import Obra, ObraAditivo
    from app.apps.erp.core.comum.auditoria import registrar_evento
    try:
        with get_session() as s:
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            if request.method == "POST":
                usuario = _usuario_logado(s)
                d = request.get_json(silent=True) or {}
                texto = ("nome objeto cliente cnpj_cliente contrato municipio uf cno endereco "
                         "bairro numero_endereco complemento cep responsavel_tecnico art_rrt "
                         "engenheiro_fiscal ordem_servico indice_reajuste regime_obra "
                         "observacoes_fiscais orgao_resumido status").split()
                numeros = ("valor_contrato aliquota_iss aliquota_iss_pct pct_servico_iss "
                           "pct_servico_inss").split()
                datas = ("vigencia_inicio vigencia_fim data_base_orcamento data_ordem_servico "
                         "data_inicio data_termino").split()
                booleanos = "iss_retido inss_retido aceita_deducao_material".split()
                from decimal import Decimal, InvalidOperation
                from datetime import date as _date
                antes = {"aliquota_iss": str(obra.aliquota_iss_pct),
                         "valor_contrato": str(obra.valor_contrato)}
                for campo in texto:
                    if campo in d:
                        setattr(obra, campo, (str(d[campo]).strip() or None))
                for campo in numeros:
                    if campo in d:
                        v = str(d[campo]).replace(".", "").replace(",", ".").strip()
                        try:
                            setattr(obra, campo, Decimal(v) if v else None)
                        except InvalidOperation:
                            return jsonify({"ok": False,
                                            "erro": f"Valor inválido em {campo}."}), 400
                for campo in datas:
                    if campo in d:
                        v = str(d[campo]).strip()
                        setattr(obra, campo, _date.fromisoformat(v) if v else None)
                for campo in booleanos:
                    if campo in d:
                        setattr(obra, campo, bool(d[campo]))
                if "federais_retidos" in d:
                    obra.federais_retidos = [str(x).upper() for x in (d["federais_retidos"] or [])]
                if "prazo_execucao_dias" in d:
                    obra.prazo_execucao_dias = int(d["prazo_execucao_dias"] or 0) or None
                if "conta_recebimento_id" in d:
                    obra.conta_recebimento_id = d["conta_recebimento_id"] or None
                s.flush()
                registrar_evento(s, "obra", obra.id, "ATUALIZADA",
                                 {"codigo": obra.codigo, "antes": antes}, usuario.id)
                s.commit()

            aditivos = s.scalars(select(ObraAditivo)
                                 .where(ObraAditivo.obra_id == obra.id)
                                 .order_by(ObraAditivo.numero)).all()
            acrescimo = sum(float(a.valor) for a in aditivos)
            dias_extra = sum(a.dias for a in aditivos)
            dados = {c: getattr(obra, c) for c in (
                "id codigo nome objeto cliente cnpj_cliente contrato municipio uf cno endereco "
                "bairro numero_endereco complemento cep responsavel_tecnico art_rrt "
                "engenheiro_fiscal ordem_servico indice_reajuste regime_obra status "
                "observacoes_fiscais orgao_resumido codigo_omie_depto ref_pipefy "
                "iss_retido inss_retido aceita_deducao_material prazo_execucao_dias "
                "conta_recebimento_id").split()}
            for campo in ("valor_contrato", "aliquota_iss", "aliquota_iss_pct",
                          "pct_servico_iss", "pct_servico_inss"):
                v = getattr(obra, campo, None)
                dados[campo] = float(v) if v is not None else None
            for campo in ("vigencia_inicio", "vigencia_fim", "data_base_orcamento",
                          "data_ordem_servico", "data_inicio", "data_termino"):
                v = getattr(obra, campo, None)
                dados[campo] = v.isoformat() if v else None
            dados["federais_retidos"] = list(obra.federais_retidos or [])
            dados["resumo_tributacao"] = resumo_tributacao(obra)
            dados["valor_vigente"] = round(float(obra.valor_contrato or 0) + acrescimo, 2)
            dados["aditivos"] = [{
                "id": a.id, "numero": a.numero, "tipo": a.tipo, "valor": float(a.valor),
                "dias": a.dias, "objeto": a.objeto,
                "data_assinatura": a.data_assinatura.isoformat() if a.data_assinatura else None,
                "nova_vigencia_fim": (a.nova_vigencia_fim.isoformat()
                                      if a.nova_vigencia_fim else None)} for a in aditivos]
            dados["dias_aditivados"] = dias_extra
        return jsonify({"ok": True, "obra": dados})
    except Exception as e:
        logger.exception("ERP: falha no cadastro da obra %s", obra_id)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/aditivos", methods=["POST"])
@login_obrigatorio
def api_aditivo(obra_id: int):
    from decimal import Decimal
    from datetime import date as _date
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Obra, ObraAditivo
    d = request.get_json(silent=True) or {}
    numero = (d.get("numero") or "").strip()
    tipo = (d.get("tipo") or "VALOR").upper()
    if not numero:
        return jsonify({"ok": False, "erro": "Informe o número do aditivo."}), 400
    if tipo not in ("VALOR", "PRAZO", "VALOR_E_PRAZO", "REAJUSTE", "SUPRESSAO"):
        return jsonify({"ok": False, "erro": f"Tipo inválido: {tipo}"}), 400
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            valor = Decimal(str(d.get("valor") or 0).replace(".", "").replace(",", "."))
            if tipo == "SUPRESSAO" and valor > 0:
                valor = -valor                      # supressão reduz o contrato
            nova_vig = (_date.fromisoformat(d["nova_vigencia_fim"])
                        if d.get("nova_vigencia_fim") else None)
            s.add(ObraAditivo(
                obra_id=obra.id, numero=numero, tipo=tipo, valor=valor,
                dias=int(d.get("dias") or 0), nova_vigencia_fim=nova_vig,
                data_assinatura=(_date.fromisoformat(d["data_assinatura"])
                                 if d.get("data_assinatura") else None),
                objeto=(d.get("objeto") or "").strip() or None, criado_por=usuario.id))
            if nova_vig:
                obra.vigencia_fim = nova_vig       # o aditivo de prazo move a vigência
            s.flush()
            registrar_evento(s, "obra", obra.id, "ADITIVO_REGISTRADO", {
                "numero": numero, "tipo": tipo, "valor": str(valor),
                "dias": int(d.get("dias") or 0)}, usuario.id)
            s.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("ERP: falha ao registrar aditivo")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/tributacao", methods=["POST"])
@login_obrigatorio
def api_simular_tributacao(obra_id: int):
    """Simula as retenções de uma medição com o cadastro fiscal da obra."""
    from app.apps.erp.core.titulos.tributacao import calcular
    from app.apps.erp.db.models.cadastros import Obra
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            calc = calcular(obra, d.get("valor") or 0,
                            sem_deducao=bool(d.get("sem_deducao")),
                            pct_servico_iss=d.get("pct_servico_iss"),
                            pct_servico_inss=d.get("pct_servico_inss"),
                            aliquota_iss=d.get("aliquota_iss"))
        return jsonify({"ok": True, "calculo": calc.como_dict()})
    except Exception as e:
        logger.exception("ERP: falha ao simular tributação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/comprovante", methods=["POST"])
@login_obrigatorio
def api_comprovante():
    """Baixa por comprovante: lê o PDF/foto do banco, acha o título e baixa."""
    from app.apps.erp.core.pagamentos.comprovante import processar_comprovante
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o comprovante."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = processar_comprovante(
                s, conteudo, arquivo.filename or "comprovante.pdf",
                conta_bancaria_id=request.form.get("conta_id", type=int),
                baixar_automatico=(request.form.get("automatico", "1") != "0"),
                usuario=usuario)
            s.commit()
            if rel.get("situacao") == "BAIXADO" and rel.get("baixa"):
                from app.apps.erp.core.notificacoes import avisar_baixa
                from app.apps.erp.db.models.financeiro import Pagamento
                from sqlalchemy import select as _sel
                pg = s.scalars(_sel(Pagamento).where(
                    Pagamento.parcela_id == rel["baixa"]["parcela_id"])
                    .order_by(Pagamento.id.desc())).first()
                if pg is not None:
                    try:
                        rel["aviso"] = avisar_baixa(s, pg.id)
                        s.commit()
                    except Exception as e:
                        logger.warning("ERP: aviso do comprovante falhou (%s)", e)
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao processar comprovante")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/comprovante/confirmar", methods=["POST"])
@login_obrigatorio
def api_comprovante_confirmar():
    from app.apps.erp.core.pagamentos.comprovante import confirmar_baixa
    arquivo = request.files.get("arquivo")
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = confirmar_baixa(
                s, parcela_id=int(request.form["parcela_id"]),
                conta_bancaria_id=int(request.form["conta_id"]),
                data_pagamento=request.form.get("data") or date.today().isoformat(),
                conteudo=(arquivo.read() if arquivo else None),
                nome_arquivo=(arquivo.filename if arquivo else ""), usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "baixa": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao confirmar baixa por comprovante")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/anexos/<entidade>/<int:entidade_id>", methods=["GET", "POST"])
@login_obrigatorio
def api_anexos(entidade: str, entidade_id: int):
    """Anexos guardados no próprio banco, comprimidos."""
    from app.apps.erp.core.documentos.armazenamento import listar, salvar
    if entidade not in ("titulo", "obra", "movimentacao", "fornecedor"):
        return jsonify({"ok": False, "erro": f"Entidade inválida: {entidade}"}), 400
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, "anexos": listar(s, entidade, entidade_id)})
            arquivo = request.files.get("arquivo")
            if arquivo is None:
                return jsonify({"ok": False, "erro": "Envie o arquivo."}), 400
            usuario = _usuario_logado(s)
            a = salvar(s, arquivo.read(), arquivo.filename or "arquivo",
                       entidade_tipo=entidade, entidade_id=entidade_id,
                       categoria=request.form.get("categoria", "OUTRO"),
                       descricao=request.form.get("descricao", ""), usuario=usuario)
            s.commit()
            return jsonify({"ok": True, "anexo": {
                "id": a.id, "nome": a.nome_arquivo,
                "tamanho_kb": round((a.tamanho_bytes or 0) / 1024, 1),
                "original_kb": round((a.tamanho_original or 0) / 1024, 1)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no anexo")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/anexo/<int:anexo_id>")
@login_obrigatorio
def baixar_anexo(anexo_id: int):
    """Serve o arquivo direto do banco."""
    from flask import Response
    from app.apps.erp.core.documentos.armazenamento import obter
    try:
        with get_session() as s:
            a = obter(s, anexo_id)
            if not a.conteudo:
                return jsonify({"ok": False,
                                "erro": "Anexo antigo sem conteúdo no banco."}), 404
            return Response(bytes(a.conteudo), mimetype=a.mime_type or "application/octet-stream",
                            headers={"Content-Disposition":
                                     f'inline; filename="{a.nome_arquivo}"'})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/anexos/<int:anexo_id>", methods=["DELETE"])
@login_obrigatorio
def api_excluir_anexo(anexo_id: int):
    from app.apps.erp.core.documentos.armazenamento import excluir
    try:
        with get_session() as s:
            excluir(s, anexo_id, _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/usuarios", methods=["GET", "POST"])
@login_obrigatorio
def api_usuarios():
    """Cadastro de operadores. Só o ADMIN mexe."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import ROTULOS, exigir
    from app.apps.erp.core.auth.service import criar_usuario
    from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
    from app.apps.erp.db.models.cadastros import Obra, PerfilUsuario, Usuario, UsuarioObra
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                usuarios = s.scalars(select(Usuario).order_by(Usuario.nome)).all()
                vinculos: dict[int, list[str]] = {}
                for v in s.scalars(select(UsuarioObra)).all():
                    obra = s.get(Obra, v.obra_id)
                    vinculos.setdefault(v.usuario_id, []).append(
                        obra.codigo if obra else str(v.obra_id))
                return jsonify({"ok": True, "usuarios": [{
                    "id": u.id, "nome": u.nome, "email": u.email,
                    "cpf": u.cpf, "telefone": u.telefone,
                    "ff_autorizado": u.ff_autorizado,
                    "ff_teto_item": float(u.ff_teto_item) if u.ff_teto_item else None,
                    "ff_teto_prestacao": (float(u.ff_teto_prestacao)
                                          if u.ff_teto_prestacao else None),
                    "ff_saldo_adiantamento": float(u.ff_saldo_adiantamento or 0),
                    "perfil": u.perfil.value,
                    "perfil_rotulo": ROTULOS.get(u.perfil, u.perfil.value),
                    "ativo": u.ativo, "obras": vinculos.get(u.id, []),
                } for u in usuarios], "perfis": [
                    {"chave": p.value, "rotulo": ROTULOS.get(p, p.value)}
                    for p in PerfilUsuario]})

            exigir(atual, "gerir_usuarios")
            d = request.get_json(silent=True) or {}
            cpf = somente_digitos(d.get("cpf") or "")
            if cpf and not cpf_valido(cpf):
                return jsonify({"ok": False, "erro": "CPF inválido."}), 400
            u = criar_usuario(s, nome=d.get("nome") or "", email=d.get("email") or "",
                              senha=d.get("senha") or "", perfil=d.get("perfil") or "CONSULTA",
                              criado_por=atual)
            u.cpf = cpf or None
            u.telefone = somente_digitos(d.get("telefone") or "") or None
            u.observacoes = (d.get("observacoes") or "").strip() or None
            for obra_id in (d.get("obras") or []):
                s.add(UsuarioObra(usuario_id=u.id, obra_id=int(obra_id)))
            s.commit()
            return jsonify({"ok": True, "usuario_id": u.id})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no cadastro de operador")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/usuarios/<int:usuario_id>", methods=["POST"])
@login_obrigatorio
def api_editar_usuario(usuario_id: int):
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario, UsuarioObra
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            exigir(atual, "gerir_usuarios")
            u = s.get(Usuario, usuario_id)
            if u is None:
                return jsonify({"ok": False, "erro": "Operador não encontrado."}), 404
            for campo in ("nome", "telefone", "observacoes"):
                if campo in d:
                    valor = (d[campo] or "").strip()
                    setattr(u, campo, somente_digitos(valor) if campo == "telefone" else valor or None)
            if d.get("perfil"):
                u.perfil = PerfilUsuario(d["perfil"])
            if "ativo" in d:
                u.ativo = bool(d["ativo"])
            if d.get("senha"):
                u.senha_hash = gerar_hash(d["senha"])
            if "ff_autorizado" in d:
                u.ff_autorizado = bool(d["ff_autorizado"])
            from decimal import Decimal as _D, InvalidOperation as _IE
            for campo in ("ff_teto_item", "ff_teto_prestacao"):
                if campo in d:
                    valor = str(d[campo] or "").replace(".", "").replace(",", ".").strip()
                    try:
                        setattr(u, campo, _D(valor) if valor else None)
                    except _IE:
                        return jsonify({"ok": False,
                                        "erro": f"Valor inválido em {campo}."}), 400
            if "obras" in d:
                for v in s.scalars(select(UsuarioObra).where(
                        UsuarioObra.usuario_id == u.id)).all():
                    s.delete(v)
                s.flush()
                for obra_id in (d.get("obras") or []):
                    s.add(UsuarioObra(usuario_id=u.id, obra_id=int(obra_id)))
            s.commit()
        return jsonify({"ok": True})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except Exception as e:
        logger.exception("ERP: falha ao editar operador")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/permissoes")
@login_obrigatorio
def api_permissoes():
    from app.apps.erp.core.auth.permissoes import contexto_permissoes
    with get_session() as s:
        return jsonify({"ok": True, "contexto": contexto_permissoes(s, _usuario_logado(s))})


@bp.route("/erp/api/titulos/<int:titulo_id>/historico")
@login_obrigatorio
def api_historico(titulo_id: int):
    """Trilha completa do título: tudo que mudou, quando e por quem — mais os
    avisos enviados ao solicitante."""
    from sqlalchemy import select
    from app.apps.erp.core.notificacoes import historico as historico_avisos
    from app.apps.erp.db.models.financeiro import Evento
    try:
        with get_session() as s:
            eventos = s.execute(select(Evento, Usuario.nome)
                                .join(Usuario, Usuario.id == Evento.usuario_id, isouter=True)
                                .where(Evento.entidade_tipo == "titulo",
                                       Evento.entidade_id == titulo_id)
                                .order_by(Evento.criado_em.desc())).all()
            linhas = [{
                "quando": e.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
                "acao": e.acao, "por": nome or "sistema",
                "detalhe": e.detalhe,
            } for e, nome in eventos]
            return jsonify({"ok": True, "eventos": linhas,
                            "avisos": historico_avisos(s, titulo_id)})
    except Exception as e:
        logger.exception("ERP: falha ao montar histórico")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/<int:pagamento_id>/avisar", methods=["POST"])
@login_obrigatorio
def api_reenviar_aviso(pagamento_id: int):
    """Reenvio manual do aviso (quando a pessoa apagou a mensagem, por ex.)."""
    from app.apps.erp.core.notificacoes import avisar_baixa
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            rel = avisar_baixa(s, pagamento_id, forcar=bool(d.get("forcar")))
            s.commit()
        return jsonify({"ok": True, "aviso": rel})
    except Exception as e:
        logger.exception("ERP: falha ao reenviar aviso")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Aval (dupla confirmação)
# ---------------------------------------------------------------------------
@bp.route("/erp/api/avais/pendentes")
@login_obrigatorio
def api_avais_pendentes():
    from app.apps.erp.core.titulos.aval import pendentes
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            return jsonify({"ok": True, "titulos": pendentes(s, usuario),
                            "perfil": usuario.perfil.value, "quem": usuario.nome})
    except Exception as e:
        logger.exception("ERP: falha ao listar avais pendentes")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/avais/<int:titulo_id>", methods=["POST"])
@login_obrigatorio
def api_avalizar(titulo_id: int):
    """Assinatura da segunda pessoa: confirma ou recusa o título."""
    from app.apps.erp.core.titulos.aval import registrar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = registrar(s, titulo_id, usuario,
                            decisao=(d.get("decisao") or "CONFIRMADO"),
                            motivo=d.get("motivo", ""),
                            ip=(request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
                                or request.remote_addr or ""),
                            dispositivo=request.headers.get("User-Agent", ""))
            s.commit()
        return jsonify({"ok": True, "aval": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao registrar aval")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/historico/<entidade>/<int:entidade_id>")
@login_obrigatorio
def api_historico_geral(entidade: str, entidade_id: int):
    """Histórico de QUALQUER cadastro: obra, fornecedor, categoria, título,
    movimentação, lote — tudo que mudou, quando e por quem."""
    from sqlalchemy import select
    from app.apps.erp.db.models.financeiro import Evento
    permitidas = ("titulo", "obra", "fornecedor", "categoria", "movimentacao",
                  "lote", "conciliacao", "categoria_depara", "pagamento", "extrato")
    if entidade not in permitidas:
        return jsonify({"ok": False, "erro": f"Entidade inválida: {entidade}"}), 400
    try:
        with get_session() as s:
            linhas = s.execute(
                select(Evento, Usuario.nome)
                .join(Usuario, Usuario.id == Evento.usuario_id, isouter=True)
                .where(Evento.entidade_tipo == entidade, Evento.entidade_id == entidade_id)
                .order_by(Evento.criado_em.desc()).limit(300)).all()
            return jsonify({"ok": True, "eventos": [{
                "quando": e.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
                "acao": e.acao, "por": nome or "sistema", "detalhe": e.detalhe,
            } for e, nome in linhas]})
    except Exception as e:
        logger.exception("ERP: falha no histórico de %s", entidade)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/auditoria")
@login_obrigatorio
def api_auditoria():
    """Consulta ampla da trilha, para auditoria: por período, usuário, ação."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.db.models.financeiro import Evento
    try:
        with get_session() as s:
            exigir(_usuario_logado(s), "ver_relatorios")
            stmt = (select(Evento, Usuario.nome)
                    .join(Usuario, Usuario.id == Evento.usuario_id, isouter=True)
                    .order_by(Evento.criado_em.desc()).limit(500))
            if request.args.get("entidade"):
                stmt = stmt.where(Evento.entidade_tipo == request.args["entidade"])
            if request.args.get("acao"):
                stmt = stmt.where(Evento.acao.ilike(f"%{request.args['acao']}%"))
            if request.args.get("usuario_id", type=int):
                stmt = stmt.where(Evento.usuario_id == request.args.get("usuario_id", type=int))
            if request.args.get("de"):
                stmt = stmt.where(Evento.criado_em >= f"{request.args['de']} 00:00:00")
            if request.args.get("ate"):
                stmt = stmt.where(Evento.criado_em <= f"{request.args['ate']} 23:59:59")
            linhas = s.execute(stmt).all()
            return jsonify({"ok": True, "eventos": [{
                "quando": e.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
                "entidade": e.entidade_tipo, "entidade_id": e.entidade_id,
                "acao": e.acao, "por": nome or "sistema", "detalhe": e.detalhe,
            } for e, nome in linhas]})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except Exception as e:
        logger.exception("ERP: falha na auditoria")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/interessados/<int:titulo_id>", methods=["GET", "POST", "DELETE"])
@login_obrigatorio
def api_interessados(titulo_id: int):
    """Quem mais acompanha o título e recebe os avisos."""
    from sqlalchemy import select
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import TituloInteressado
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                from app.apps.erp.core.notificacoes import destinatarios
                from app.apps.erp.db.models.financeiro import Titulo as _T
                t = s.get(_T, titulo_id)
                if t is None:
                    return jsonify({"ok": False, "erro": "Título não encontrado."}), 404
                extras = s.scalars(select(TituloInteressado).where(
                    TituloInteressado.titulo_id == titulo_id)).all()
                pessoas = destinatarios(s, t)
                return jsonify({"ok": True,
                    "interessados": [{"usuario_id": i.usuario_id,
                                      "nome": (s.get(Usuario, i.usuario_id).nome
                                               if s.get(Usuario, i.usuario_id) else "—"),
                                      "motivo": i.motivo} for i in extras],
                    "recebem_aviso": [{"id": u.id, "nome": u.nome,
                                       "tem_contato": bool(u.telefone or u.cpf)}
                                      for u in pessoas]})
            d = request.get_json(silent=True) or {}
            if request.method == "DELETE":
                alvo = s.scalars(select(TituloInteressado).where(
                    TituloInteressado.titulo_id == titulo_id,
                    TituloInteressado.usuario_id == int(d.get("usuario_id")))).first()
                if alvo is not None:
                    s.delete(alvo)
                    registrar_evento(s, "titulo", titulo_id, "INTERESSADO_REMOVIDO",
                                     {"usuario_id": d.get("usuario_id")}, atual.id)
                    s.commit()
                return jsonify({"ok": True})
            adicionados = []
            for uid in (d.get("usuarios") or []):
                pessoa = s.get(Usuario, int(uid))
                if pessoa is None or not pessoa.ativo:
                    continue
                ja = s.scalars(select(TituloInteressado).where(
                    TituloInteressado.titulo_id == titulo_id,
                    TituloInteressado.usuario_id == pessoa.id)).first()
                if ja is not None:
                    continue
                s.add(TituloInteressado(titulo_id=titulo_id, usuario_id=pessoa.id,
                                        motivo=(d.get("motivo") or "").strip() or None,
                                        adicionado_por=atual.id))
                adicionados.append(pessoa.nome)
            if adicionados:
                registrar_evento(s, "titulo", titulo_id, "INTERESSADOS_ADICIONADOS",
                                 {"pessoas": adicionados}, atual.id)
            s.commit()
            return jsonify({"ok": True, "adicionados": adicionados})
    except Exception as e:
        logger.exception("ERP: falha nos interessados")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/interessados", methods=["GET", "POST"])
@login_obrigatorio
def api_obra_interessados(obra_id: int):
    """Interessados fixos da obra: entram em todo título dela."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import ObraInteressado
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                linhas = s.scalars(select(ObraInteressado).where(
                    ObraInteressado.obra_id == obra_id)).all()
                return jsonify({"ok": True, "interessados": [
                    {"usuario_id": i.usuario_id,
                     "nome": (s.get(Usuario, i.usuario_id).nome
                              if s.get(Usuario, i.usuario_id) else "—")} for i in linhas]})
            exigir(atual, "configurar")
            d = request.get_json(silent=True) or {}
            for i in s.scalars(select(ObraInteressado).where(
                    ObraInteressado.obra_id == obra_id)).all():
                s.delete(i)
            s.flush()
            nomes = []
            for uid in (d.get("usuarios") or []):
                pessoa = s.get(Usuario, int(uid))
                if pessoa is not None and pessoa.ativo:
                    s.add(ObraInteressado(obra_id=obra_id, usuario_id=pessoa.id))
                    nomes.append(pessoa.nome)
            registrar_evento(s, "obra", obra_id, "INTERESSADOS_DEFINIDOS",
                             {"pessoas": nomes}, atual.id)
            s.commit()
            return jsonify({"ok": True, "interessados": nomes})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except Exception as e:
        logger.exception("ERP: falha nos interessados da obra")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/operadores/contato")
@login_obrigatorio
def api_operadores_contato():
    """Lista enxuta para escolher interessados, dizendo quem consegue receber."""
    from sqlalchemy import select
    try:
        with get_session() as s:
            usuarios = s.scalars(select(Usuario).where(Usuario.ativo.is_(True))
                                 .order_by(Usuario.nome)).all()
            return jsonify({"ok": True, "operadores": [
                {"id": u.id, "nome": u.nome, "perfil": u.perfil.value,
                 "tem_contato": bool(u.telefone or u.cpf)} for u in usuarios]})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Gestão de obras
# ---------------------------------------------------------------------------
FASES_OBRA = [
    ("CRIACAO", "Criação / cadastro"),
    ("AGUARDANDO_OS", "Aguardando ordem de serviço"),
    ("EM_EXECUCAO", "Em execução"),
    ("PARALISADA", "Paralisada"),
    ("CONCLUIDA", "Concluída"),
    ("CONCLUIDA_COM_DIVIDA", "Concluída com dívida"),
    ("RECEBIMENTO_PROVISORIO", "Recebimento provisório"),
    ("RECEBIMENTO_DEFINITIVO", "Recebimento definitivo"),
    ("ACERVO_TECNICO", "Acervo técnico"),
    ("DISTRATADA", "Distratada"),
]


@bp.route("/erp/api/obras")
@login_obrigatorio
def api_listar_obras():
    """Painel de obras: situação, contrato, medições e o que foi gasto."""
    from sqlalchemy import func, select
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    from app.apps.erp.db.models.cadastros import Obra, ObraAditivo
    from app.apps.erp.db.models.financeiro import EspecieTitulo, Rateio, Titulo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            permitidas = obras_do_usuario(s, usuario)
            stmt = select(Obra).order_by(Obra.codigo)
            if permitidas is not None:
                stmt = stmt.where(Obra.id.in_(permitidas or [0]))
            obras = s.scalars(stmt).all()
            ids = [o.id for o in obras] or [0]

            aditivos: dict[int, float] = {}
            for obra_id, total in s.execute(
                    select(ObraAditivo.obra_id, func.sum(ObraAditivo.valor))
                    .where(ObraAditivo.obra_id.in_(ids))
                    .group_by(ObraAditivo.obra_id)):
                aditivos[obra_id] = float(total or 0)

            gastos: dict[int, float] = {}
            recebidos: dict[int, float] = {}
            for obra_id, especie, total in s.execute(
                    select(Rateio.obra_id, Titulo.especie, func.sum(Rateio.valor))
                    .join(Titulo, Titulo.id == Rateio.titulo_id)
                    .where(Rateio.obra_id.in_(ids),
                           Titulo.status.not_in(["CANCELADO", "ESTORNADO"]))
                    .group_by(Rateio.obra_id, Titulo.especie)):
                destino = recebidos if especie == EspecieTitulo.RECEBER else gastos
                destino[obra_id] = float(total or 0)

            fases = dict(FASES_OBRA)
            hoje = date.today()
            linhas = []
            for o in obras:
                contrato = float(o.valor_contrato or 0)
                vigente = contrato + aditivos.get(o.id, 0.0)
                gasto = gastos.get(o.id, 0.0)
                linhas.append({
                    "id": o.id, "codigo": o.codigo, "nome": o.nome,
                    "objeto": (o.objeto or "")[:140], "cliente": o.cliente,
                    "municipio": o.municipio, "uf": o.uf, "contrato": o.contrato,
                    "fase": o.fase, "fase_rotulo": fases.get(o.fase, o.fase),
                    "status": o.status,
                    "valor_contrato": contrato, "aditivos": aditivos.get(o.id, 0.0),
                    "valor_vigente": vigente,
                    "gasto": gasto, "recebido": recebidos.get(o.id, 0.0),
                    "margem": round(recebidos.get(o.id, 0.0) - gasto, 2),
                    "vigencia_fim": o.vigencia_fim.isoformat() if o.vigencia_fim else None,
                    "vence_em_dias": ((o.vigencia_fim - hoje).days
                                      if o.vigencia_fim else None),
                    "data_base": (o.data_base_orcamento.isoformat()
                                  if o.data_base_orcamento else None),
                    "reajuste_em_dias": ((o.data_base_orcamento.replace(
                        year=o.data_base_orcamento.year + 1) - hoje).days
                        if o.data_base_orcamento else None),
                    "seguro_vigencia_fim": (o.seguro_vigencia_fim.isoformat()
                                            if o.seguro_vigencia_fim else None),
                    "cno": o.cno, "art_rrt": o.art_rrt,
                    "aliquota_iss": float(o.aliquota_iss_pct or 0) or None,
                })
            return jsonify({"ok": True, "obras": linhas,
                            "fases": [{"chave": k, "rotulo": v} for k, v in FASES_OBRA]})
    except Exception as e:
        logger.exception("ERP: falha ao listar obras")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/fase", methods=["POST"])
@login_obrigatorio
def api_mudar_fase(obra_id: int):
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Obra, ObraFase
    d = request.get_json(silent=True) or {}
    fase = (d.get("fase") or "").upper()
    if fase not in dict(FASES_OBRA):
        return jsonify({"ok": False, "erro": f"Fase inválida: {fase}"}), 400
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            anterior = obra.fase
            obra.fase = fase
            obra.fase_desde = date.today()
            if fase in ("CONCLUIDA", "CONCLUIDA_COM_DIVIDA") and not obra.data_conclusao:
                obra.data_conclusao = date.today()
            if fase == "RECEBIMENTO_PROVISORIO":
                obra.data_recebimento_provisorio = date.today()
            if fase == "RECEBIMENTO_DEFINITIVO":
                obra.data_recebimento_definitivo = date.today()
            s.add(ObraFase(obra_id=obra.id, fase=fase,
                           observacao=(d.get("observacao") or "").strip() or None,
                           usuario_id=usuario.id))
            registrar_evento(s, "obra", obra.id, "FASE_ALTERADA",
                             {"de": anterior, "para": fase,
                              "observacao": d.get("observacao")}, usuario.id)
            s.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("ERP: falha ao mudar fase")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/fases")
@login_obrigatorio
def api_historico_fases(obra_id: int):
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import ObraFase
    fases = dict(FASES_OBRA)
    try:
        with get_session() as s:
            linhas = s.execute(
                select(ObraFase, Usuario.nome)
                .join(Usuario, Usuario.id == ObraFase.usuario_id, isouter=True)
                .where(ObraFase.obra_id == obra_id)
                .order_by(ObraFase.criado_em.desc())).all()
            return jsonify({"ok": True, "fases": [{
                "fase": f.fase, "rotulo": fases.get(f.fase, f.fase),
                "observacao": f.observacao, "por": nome or "—",
                "quando": f.criado_em.strftime("%d/%m/%Y %H:%M")} for f, nome in linhas]})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/titulos")
@login_obrigatorio
def api_titulos_da_obra(obra_id: int):
    """Tudo que passou pela obra: o que se gastou e o que se recebeu."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.apps.erp.db.models.financeiro import EspecieTitulo, Rateio, Titulo
    try:
        with get_session() as s:
            linhas = s.execute(
                select(Titulo, Rateio.valor)
                .join(Rateio, Rateio.titulo_id == Titulo.id)
                .where(Rateio.obra_id == obra_id,
                       Titulo.status.not_in(["CANCELADO", "ESTORNADO"]))
                .options(selectinload(Titulo.fornecedor), selectinload(Titulo.categoria),
                         selectinload(Titulo.parcelas))
                .order_by(Titulo.competencia.desc(), Titulo.id.desc()).limit(400)).all()
            saida = []
            for t, valor in linhas:
                venc = min((p.vencimento for p in t.parcelas), default=None)
                saida.append({
                    "id": t.id, "numero_sp": t.numero_sp,
                    "especie": (t.especie.value if hasattr(t.especie, "value")
                                else str(t.especie)),
                    "credor": t.fornecedor.razao_social, "descricao": t.descricao,
                    "categoria": f"{t.categoria.codigo} · {t.categoria.descricao}",
                    "grupo": t.categoria.grupo_nome or "",
                    "valor": float(valor), "competencia": t.competencia.strftime("%m/%Y"),
                    "vencimento": venc.isoformat() if venc else None,
                    "status": t.status.value,
                    "medicao": t.numero_medicao,
                })
            return jsonify({"ok": True, "titulos": saida})
    except Exception as e:
        logger.exception("ERP: falha ao listar títulos da obra")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Prestação de contas: fundo fixo e fatura de cartão
# ---------------------------------------------------------------------------
@bp.route("/erp/api/prestacao/comprovante", methods=["POST"])
@login_obrigatorio
def api_prestacao_comprovante():
    """Lê um comprovante e devolve a linha, guardando o arquivo no banco."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.core.titulos.prestacao import ler_comprovante_item
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o comprovante."}), 400
    try:
        conteudo = arquivo.read()
        linha = ler_comprovante_item(conteudo, arquivo.filename or "comprovante")
        with get_session() as s:
            usuario = _usuario_logado(s)
            # guarda solto (entidade 0) e vincula ao título quando ele for criado
            anexo = salvar(s, conteudo, arquivo.filename or "comprovante",
                           entidade_tipo="prestacao_rascunho", entidade_id=usuario.id,
                           categoria="COMPROVANTE", usuario=usuario)
            linha["anexo_id"] = anexo.id
            s.commit()
        return jsonify({"ok": True, "linha": linha})
    except Exception as e:
        logger.exception("ERP: falha ao ler comprovante da prestação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao/fatura", methods=["POST"])
@login_obrigatorio
def api_prestacao_fatura():
    """Lê a fatura do cartão e devolve todas as compras como linhas."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.core.titulos.prestacao import ler_fatura_cartao
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o PDF da fatura."}), 400
    try:
        conteudo = arquivo.read()
        rel = ler_fatura_cartao(conteudo, arquivo.filename or "fatura.pdf")
        with get_session() as s:
            usuario = _usuario_logado(s)
            anexo = salvar(s, conteudo, arquivo.filename or "fatura.pdf",
                           entidade_tipo="prestacao_rascunho", entidade_id=usuario.id,
                           categoria="OUTRO", descricao="Fatura de cartão", usuario=usuario)
            rel["anexo_fatura_id"] = anexo.id
            s.commit()
        return jsonify({"ok": True, "fatura": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao ler fatura")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao/criticar", methods=["POST"])
@login_obrigatorio
def api_prestacao_criticar():
    from app.apps.erp.core.titulos.prestacao import criticar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            from datetime import date as _d
            return jsonify({"ok": True, "critica": criticar(
                s, d.get("itens") or [], solicitante_id=usuario.id,
                modalidade=(d.get("modalidade") or "FUNDO_FIXO").upper(),
                total_declarado=d.get("total_declarado"),
                periodo_inicio=_d.fromisoformat(d["periodo_inicio"]) if d.get("periodo_inicio") else None,
                periodo_fim=_d.fromisoformat(d["periodo_fim"]) if d.get("periodo_fim") else None)})
    except Exception as e:
        logger.exception("ERP: falha na crítica da prestação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao", methods=["POST"])
@login_obrigatorio
def api_criar_prestacao():
    from app.apps.erp.core.documentos.armazenamento import Anexo as _A
    from app.apps.erp.core.titulos.prestacao import criar_prestacao
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            titulo = criar_prestacao(s, d, usuario)
            # vincula os comprovantes ao título criado
            for item in (d.get("itens") or []):
                if item.get("anexo_id"):
                    anexo = s.get(_A, int(item["anexo_id"]))
                    if anexo is not None and anexo.entidade_tipo == "prestacao_rascunho":
                        anexo.entidade_tipo = "titulo"
                        anexo.entidade_id = titulo.id
            if d.get("anexo_fatura_id"):
                anexo = s.get(_A, int(d["anexo_fatura_id"]))
                if anexo is not None:
                    anexo.entidade_tipo = "titulo"
                    anexo.entidade_id = titulo.id
            s.commit()
            return jsonify({"ok": True, "titulo": {
                "id": titulo.id, "numero_sp": titulo.numero_sp,
                "status": titulo.status.value, "total": float(titulo.valor_liquido)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao criar prestação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao/<int:titulo_id>")
@login_obrigatorio
def api_prestacao_detalhe(titulo_id: int):
    from app.apps.erp.core.titulos.prestacao import detalhar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "prestacao": detalhar(s, titulo_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/prestacao/<int:titulo_id>/conferir", methods=["POST"])
@login_obrigatorio
def api_conferir(titulo_id: int):
    from app.apps.erp.core.titulos.prestacao import confirmar_analise
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = confirmar_analise(s, titulo_id, usuario,
                                    item_id=d.get("item_id"),
                                    observacao=d.get("observacao", ""))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/prestacao/historico")
@login_obrigatorio
def api_prestacao_historico():
    from app.apps.erp.core.titulos.prestacao import historico_do_solicitante
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            alvo = request.args.get("usuario_id", type=int) or usuario.id
            return jsonify({"ok": True, "historico": historico_do_solicitante(
                s, alvo, (request.args.get("modalidade") or "FUNDO_FIXO").upper())})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacoes/pendentes")
@login_obrigatorio
def api_prestacoes_pendentes():
    """Prestações com apontamento aguardando análise — a fila do financeiro e
    do diretor, complementar à do aval."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.apps.erp.core.auth.permissoes import aplicar_escopo
    from app.apps.erp.db.models.financeiro import StatusTitulo, Titulo, TituloItem
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            stmt = (select(Titulo).where(
                        Titulo.modalidade.in_(["FUNDO_FIXO", "CARTAO"]),
                        Titulo.status.not_in([StatusTitulo.PAGO, StatusTitulo.CANCELADO,
                                              StatusTitulo.ESTORNADO]))
                    .options(selectinload(Titulo.fornecedor))
                    .order_by(Titulo.id.desc()).limit(200))
            stmt = aplicar_escopo(stmt, s, usuario)
            saida = []
            for t in s.scalars(stmt).all():
                itens = s.scalars(select(TituloItem).where(
                    TituloItem.titulo_id == t.id)).all()
                bloqueios = criticas = pendentes_conf = 0
                for i in itens:
                    lista = i.criticas or []
                    bloqueios += sum(1 for x in lista if x.get("gravidade") == "BLOQUEIA")
                    criticas += sum(1 for x in lista if x.get("gravidade") == "CRITICA")
                    if [x for x in lista if x.get("gravidade") in ("BLOQUEIA", "CRITICA")] \
                            and not i.conferido_em:
                        pendentes_conf += 1
                solicitante = s.get(Usuario, t.solicitante_id)
                saida.append({
                    "id": t.id, "numero_sp": t.numero_sp, "modalidade": t.modalidade,
                    "tipo": t.fundo_fixo_tipo, "status": t.status.value,
                    "solicitante": solicitante.nome if solicitante else "—",
                    "total": float(t.valor_liquido), "itens": len(itens),
                    "bloqueios": bloqueios, "criticas": criticas,
                    "nao_conferidos": pendentes_conf,
                    "analises": len(t.alertas_confirmados or []),
                    "competencia": t.competencia.strftime("%m/%Y"),
                })
            return jsonify({"ok": True, "prestacoes": saida})
    except Exception as e:
        logger.exception("ERP: falha ao listar prestações pendentes")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/movimentacoes/neutras")
@login_obrigatorio
def api_neutras():
    """Pontas soltas: entrou e não foi devolvido, saiu e não foi ressarcido."""
    from app.apps.erp.core.titulos.receber import neutras_sem_par
    try:
        with get_session() as s:
            return jsonify({"ok": True, "pendentes": neutras_sem_par(s)})
    except Exception as e:
        logger.exception("ERP: falha ao listar neutras")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/movimentacoes/vincular", methods=["POST"])
@login_obrigatorio
def api_vincular_neutras():
    """Liga as duas pontas que se anulam."""
    from app.apps.erp.core.titulos.receber import vincular_par
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = vincular_par(s, int(d["movimentacao_id"]), int(d["par_id"]),
                               usuario, motivo=d.get("motivo", ""))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao vincular par neutro")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/health")
def health():
    """Health check do módulo — não exige login."""
    try:
        from sqlalchemy import text
        with get_session() as s:
            s.execute(text("SELECT 1"))
        pendentes = _migracoes_pendentes()
        return jsonify({"ok": not pendentes, "modulo": "erp", "banco": "conectado",
                        "migracoes_pendentes": pendentes,
                        "aviso": ("aplique as atualizações em Configurações"
                                  if pendentes else "estrutura em dia")}), 200
    except Exception as e:
        return jsonify({"ok": False, "modulo": "erp", "erro": str(e)}), 503
