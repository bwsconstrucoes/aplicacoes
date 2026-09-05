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
from decimal import Decimal, InvalidOperation
from functools import wraps

from flask import (
    Blueprint, jsonify, redirect, render_template, request, session, url_for,
)

from sqlalchemy.exc import IntegrityError, ProgrammingError

from app.apps.erp.core.auth.permissoes import (
    ACAO_ROTULOS, PERMISSOES, PROTEGIDAS_DO_ADMIN, ROTULOS, decidir, pode,
)
from app.apps.erp.core.auth.service import ErroAutenticacao, autenticar
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroPermissao, ErroValidacao,
)
from app.apps.erp.core.titulos import service as svc_titulos
from app.apps.erp.db.database import get_session
from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario

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
            ("empreitas", "Empreitas", "erp.pagina_empreitas"),
            ("locacoes", "Locações", "erp.pagina_locacoes"),
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
        "chave": "pessoal", "nome": "Pessoal", "sigla": "PES",
        "descricao": "Colaboradores e despesas com colaboradores (diárias, produção, verbas)",
        "cor": "var(--verde)",
        "abas": [
            ("dc", "Despesas com colaborador", "erp.pagina_dc"),
            ("colaboradores", "Colaboradores", "erp.pagina_colaboradores"),
        ],
    },
    {
        "chave": "suprimentos", "nome": "Suprimentos", "sigla": "SUP",
        "descricao": "Insumos, fornecedores, cotação e pedidos de compra",
        "cor": "var(--ambar)",
        "abas": [
            ("sup_solicitacoes", "Solicitações", "erp.pagina_suprimentos"),
            ("sup_cadastros", "Cadastros", "erp.pagina_suprimentos_cadastros"),
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
    if not uid:
        return None
    u = s.get(Usuario, uid)
    if u is not None:
        u.permissoes_extras = _excecoes_brutas(s, uid)
    return u


def _excecoes_brutas(s, usuario_id: int) -> dict[str, bool]:
    """As marcações de permissão da pessoa, por SQL direto (ação → concedida).

    SQL direto e não ORM pelo mesmo motivo do perfil: esta leitura acontece em
    toda requisição, inclusive na tela que aplica as migrações. Enquanto a
    migração 032 não tiver rodado, a tabela não existe — e aí a resposta certa
    é "nenhuma exceção", que faz valer o cargo, e não derrubar o ERP.
    """
    from sqlalchemy import text as _text
    try:
        linhas = s.execute(
            _text("SELECT acao, concedida FROM usuario_permissoes WHERE usuario_id = :i"),
            {"i": usuario_id}).all()
    except Exception:
        logger.warning("ERP/permissao: usuario_permissoes indisponível "
                       "(migração 032 pendente?) — valendo só o cargo")
        return {}
    return {acao: bool(concedida) for acao, concedida in linhas}


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


# ---------------------------------------------------------------------------
# Autorização: o padrão é NEGAR
#
# Toda rota do ERP declara a ação que exige. O que não declara não passa —
# `_guarda_permissao` recusa endpoint ausente do registro. É a inversão do
# default: esquecer fecha a rota, em vez de deixá-la aberta a todo mundo.
#
# Rota realmente pública declara isso por escrito, com o motivo, em
# `@permissao_publica(...)`. Silêncio nunca vale como liberação.
#
# Isto trata ALÇADA ("este perfil pode esta ação?"). Escopo de objeto ("pode
# NESTE registro?") é a outra metade, e mora em permissoes.exigir_*_no_escopo.
# ---------------------------------------------------------------------------
_REGISTRO_PERMISSOES: dict[str, dict[str, str]] = {}   # endpoint -> {método: ação}
_ENDPOINTS_PUBLICOS: dict[str, str] = {}               # endpoint -> motivo

# o Flask serve o CSS/JS do blueprint por este endpoint; não é rota de negócio
_ISENTOS = {"erp.static"}

_TODOS_METODOS = ("GET", "POST", "PUT", "PATCH", "DELETE")


def permissao(acao: str | None = None, **por_metodo: str):
    """Declara a ação exigida pela rota.

    `@permissao("pagar")` vale para todos os métodos. Quando um mesmo endpoint
    lê e escreve com sensibilidades diferentes, declare por método:
    `@permissao(GET="ver_erp", POST="configurar")`.
    """
    if acao is None and not por_metodo:
        raise ValueError("Declare a ação exigida pela rota.")
    mapa = {m: acao for m in _TODOS_METODOS} if acao else {}
    mapa.update({m.upper(): a for m, a in por_metodo.items()})
    for a in set(mapa.values()):
        if a not in PERMISSOES:
            raise ValueError(f"Ação desconhecida em @permissao: {a!r}")

    def _decorar(fn):
        _REGISTRO_PERMISSOES[f"erp.{fn.__name__}"] = mapa
        fn._permissao = mapa
        return fn
    return _decorar


def permissao_publica(motivo: str):
    """Rota sem autenticação. Exige motivo escrito — para a exceção ser uma
    decisão visível na revisão, e não um descuido."""
    def _decorar(fn):
        _ENDPOINTS_PUBLICOS[f"erp.{fn.__name__}"] = motivo
        fn._permissao_publica = motivo
        return fn
    return _decorar


@bp.errorhandler(ProgrammingError)
def _banco_atrasado(e: ProgrammingError):
    """Coluna ou tabela que o código espera e o banco ainda não tem.

    Acontece quando o código novo sobe antes de alguém apertar "Aplicar
    atualizações do banco". Em vez de "Internal Server Error", diz o que é e
    quem resolve. 503: o serviço está de pé, só o banco está atrasado."""
    logger.error("ERP: banco atrasado em relação ao código (%s) em %s",
                 str(e.orig or e).splitlines()[0], request.path)
    if request.path.startswith("/erp/api/"):
        return jsonify({"ok": False, "banco_atrasado": True,
                        "erro": "O banco está desatualizado em relação ao sistema. "
                                "Um administrador precisa aplicar as atualizações "
                                "em Configurações."}), 503
    return render_template("erp_banco_atrasado.html",
                           detalhe=str(e.orig or e).splitlines()[0],
                           e_admin=(session.get("erp_usuario_perfil") == "ADMIN")), 503


@bp.errorhandler(ErroNaoEncontrado)
def _fora_do_escopo(e: ErroNaoEncontrado):
    """Fora do escopo responde como inexistente — mesma resposta, mesmo status.

    Se aqui saísse 403 "sem permissão", a diferença entre 403 e 404 viraria um
    oráculo: bastaria varrer os ids para saber quais existem.
    """
    return jsonify({"ok": False, "erro": str(e) or "Não encontrado."}), 404


@bp.before_request
def _abrir_contexto_ia():
    """Quem está logado "assina" as chamadas de IA desta requisição. Zerado a
    cada requisição porque as threads do gunicorn são reaproveitadas."""
    from flask import g
    from app.apps.erp.core.comum.ia_custo import iniciar_contexto_requisicao
    g.erp_ia_token = iniciar_contexto_requisicao(session.get("erp_usuario_id"))


@bp.teardown_request
def _fechar_contexto_ia(_exc=None):
    from flask import g
    from app.apps.erp.core.comum.ia_custo import encerrar_contexto_requisicao
    encerrar_contexto_requisicao(g.pop("erp_ia_token", None))


@bp.before_request
def _guarda_permissao():
    endpoint = request.endpoint or ""
    if endpoint in _ISENTOS or endpoint in _ENDPOINTS_PUBLICOS:
        return None

    mapa = _REGISTRO_PERMISSOES.get(endpoint)
    if mapa is None:
        # Rota nova que ninguém declarou. Fecha e grita: é assim que o
        # esquecimento vira erro visível em vez de brecha silenciosa.
        logger.error("ERP/permissao: endpoint %s sem declaração — acesso negado", endpoint)
        return jsonify({"ok": False, "erro": "Rota sem permissão declarada."}), 403

    acao = mapa.get(request.method)
    if acao is None:
        return jsonify({"ok": False, "erro": "Método não liberado nesta rota."}), 405

    if not session.get("erp_usuario_id"):
        return None          # login_obrigatorio responde (401 ou redireciona)

    # O perfil vem por SQL DIRETO, nunca pelo ORM. Esta função roda antes de
    # TODA rota — inclusive da tela que aplica as migrações. Se carregasse o
    # objeto Usuario e o banco estivesse uma migração atrasado (coluna nova no
    # modelo, ainda não criada), quebraria aqui, e o botão que conserta o banco
    # ficaria inalcançável: o impasse circular que derrubou o ERP em 2026-09-02.
    with get_session() as s:
        perfil = _perfil_bruto(s)
        excecoes = _excecoes_brutas(s, session["erp_usuario_id"]) if perfil else {}
    if not perfil:
        return None          # usuário sumiu do banco: a rota responde
    try:
        perfil_enum = PerfilUsuario(perfil)
    except ValueError:
        logger.error("ERP/permissao: perfil desconhecido %r no usuário %s",
                     perfil, session.get("erp_usuario_id"))
        return jsonify({"ok": False, "erro": "Perfil de usuário inválido."}), 403
    if not decidir(perfil_enum, acao, excecoes):
        logger.warning("ERP/permissao: %s negado ao usuário %s (%s) em %s",
                       acao, session.get("erp_usuario_id"), perfil, endpoint)
        return jsonify({"ok": False,
                        "erro": "Seu perfil não tem permissão para esta operação."}), 403
    return None


@bp.route("/erp/entrar", methods=["GET", "POST"])
@permissao_publica("tela de login — porta de entrada do ERP")
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
@permissao_publica("encerrar sessao nao pode exigir sessao valida")
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
@permissao("ver_erp")
def pagina_titulos():
    return render_template("erp_titulos.html", **_contexto("titulos"))


@bp.route("/erp/inicio")
@login_obrigatorio
@permissao("ver_erp")
def pagina_inicio():
    """Porta de entrada: escolha do módulo."""
    return render_template("erp_inicio.html", modulos=MODULOS, modulo=None,
                           abas=[], aba_ativa="",
                           usuario_nome=session.get("erp_usuario_nome", ""),
                           usuario_perfil=session.get("erp_usuario_perfil", ""),
                           migracoes_pendentes=_migracoes_pendentes())


@bp.route("/erp/lancar")
@login_obrigatorio
@permissao("lancar")
def pagina_lancar():
    return render_template("erp_lancar.html", **_contexto("lancar"))


@bp.route("/erp/confirmar")
@login_obrigatorio
@permissao("ver_erp")
def pagina_confirmar():
    return render_template("erp_confirmar.html", **_contexto("confirmar"))


@bp.route("/erp/pagamentos")
@login_obrigatorio
@permissao("pagar")
def pagina_pagamentos():
    return render_template("erp_pagamentos.html", **_contexto("pagamentos"))


@bp.route("/erp/empreitas")
@login_obrigatorio
@permissao("ver_erp")
def pagina_empreitas():
    return render_template("erp_empreitas.html", **_contexto("empreitas"))


@bp.route("/erp/locacoes")
@login_obrigatorio
@permissao("ver_erp")
def pagina_locacoes():
    return render_template("erp_locacoes.html", **_contexto("locacoes"))


@bp.route("/erp/dc")
@login_obrigatorio
@permissao("ver_pessoal")
def pagina_dc():
    return render_template("erp_dc.html", **_contexto("dc"))


@bp.route("/erp/colaboradores")
@login_obrigatorio
@permissao("ver_pessoal")
def pagina_colaboradores():
    return render_template("erp_colaboradores.html", **_contexto("colaboradores"))


@bp.route("/erp/conciliacao")
@login_obrigatorio
@permissao("conciliar")
def pagina_conciliacao():
    return render_template("erp_conciliacao.html", **_contexto("conciliacao"))


@bp.route("/erp/receber")
@login_obrigatorio
@permissao("receber")
def pagina_receber():
    return render_template("erp_receber.html", **_contexto("receber"))


@bp.route("/erp/obras")
@login_obrigatorio
@permissao("ver_erp")
def pagina_obras():
    return render_template("erp_obras.html", **_contexto("obras"))


# ---------------------------------------------------------------------------
# Suprimentos — fase 1: os cadastros. A especificação está em SUPRIMENTOS.md.
# ---------------------------------------------------------------------------
@bp.route("/erp/suprimentos/cadastros")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_cadastros():
    return render_template("erp_suprimentos_cadastros.html", **_contexto("sup_cadastros"))


@bp.route("/erp/api/suprimentos/cadastros")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimentos_cadastros():
    """Tudo que a tela de cadastros precisa, numa consulta só."""
    from sqlalchemy import select
    from app.apps.erp.core.suprimentos.pagamento import descrever
    from app.apps.erp.db.models.cadastros import (
        CondicaoPagamento, Insumo, InsumoCategoria, Obra, UnidadeCompra,
    )
    with get_session() as s:
        unidades = s.scalars(select(UnidadeCompra).order_by(UnidadeCompra.ordem)).all()
        condicoes = s.scalars(select(CondicaoPagamento)
                              .order_by(CondicaoPagamento.ordem)).all()
        categorias = s.scalars(select(InsumoCategoria)
                               .order_by(InsumoCategoria.nome)).all()
        return jsonify({
            "ok": True,
            "unidades": [{"codigo": u.codigo, "descricao": u.descricao,
                          "ativo": u.ativo} for u in unidades],
            "condicoes": [{"id": c.id, "nome": c.nome,
                           "entrada_percentual": float(c.entrada_percentual or 0),
                           "dias": list(c.dias or []),
                           "em_palavras": descrever(c.entrada_percentual, c.dias),
                           "ativo": c.ativo} for c in condicoes],
            "categorias_insumo": [{"id": c.id, "codigo": c.codigo, "nome": c.nome}
                                  for c in categorias],
            # Obras e insumos vêm por aqui, e não pela tela de Configurações:
            # quem pede material não tem — nem deve ter — acesso àquela tela.
            "obras": [{"id": o.id, "codigo": o.codigo, "nome": o.nome}
                      for o in s.scalars(select(Obra).order_by(Obra.codigo)).all()
                      if getattr(o, "status", "ATIVA") != "ENCERRADA"],
            "insumos": [{"id": i.id, "codigo": i.codigo, "descricao": i.descricao,
                         "unidade": i.unidade}
                        for i in s.scalars(select(Insumo).order_by(Insumo.descricao)).all()
                        if i.ativo],
        })


@bp.route("/erp/api/suprimentos/condicoes", methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_condicao_pagamento():
    """Cadastra uma condição de pagamento como REGRA: quanto entra na hora e em
    quantos dias vencem as demais. Arranjo novo é uma linha, não código novo."""
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.suprimentos.pagamento import gerar_parcelas
    from app.apps.erp.db.models.cadastros import CondicaoPagamento
    from datetime import date as _date
    d = request.get_json(silent=True) or {}
    nome = (d.get("nome") or "").strip()
    if not nome:
        return jsonify({"ok": False, "erro": "Dê um nome à condição."}), 400
    try:
        entrada = Decimal(str(d.get("entrada_percentual") or 0))
        dias = sorted({int(x) for x in (d.get("dias") or [])})
    except (ValueError, TypeError, InvalidOperation):
        return jsonify({"ok": False, "erro": "Entrada e prazos têm de ser números."}), 400
    try:
        # Prova a regra antes de gravar: condição que não gera parcela nenhuma
        # só apareceria como defeito no primeiro pedido que a usasse.
        gerar_parcelas("1000.00", _date.today(), entrada, dias)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            c = CondicaoPagamento(nome=nome, entrada_percentual=entrada, dias=dias,
                                  ordem=int(d.get("ordem") or 0))
            s.add(c)
            s.flush()
            registrar_evento(s, "condicao_pagamento", c.id, "CRIADA",
                             {"nome": nome, "entrada": str(entrada), "dias": dias},
                             atual.id if atual else None)
            s.commit()
            return jsonify({"ok": True, "id": c.id})
    except Exception as e:
        logger.exception("ERP: falha ao cadastrar condição de pagamento")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/suprimentos")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos():
    return render_template("erp_suprimentos.html", **_contexto("sup_solicitacoes"))


@bp.route("/erp/api/suprimentos/solicitacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="solicitar_suprimento")
def api_suprimento_solicitacoes():
    """Os itens que a pessoa pode ver, e o registro de um pedido novo."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                itens = svc.listar_itens(
                    s, atual, status=request.args.get("status"),
                    obra_id=request.args.get("obra_id"),
                    busca=(request.args.get("busca") or "").strip())
                return jsonify({"ok": True, "itens": itens})
            sol = svc.criar(s, request.get_json(silent=True) or {}, atual)
            numero = sol.numero
            s.commit()
            return jsonify({"ok": True, "numero": numero})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha na solicitação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/suprimentos/solicitacoes/<int:solicitacao_id>")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimento_solicitacao(solicitacao_id: int):
    """Uma solicitação com seus itens. Fora do alcance responde 404, nunca 403."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    with get_session() as s:
        return jsonify({"ok": True,
                        "solicitacao": svc.obter(s, solicitacao_id, _usuario_logado(s))})


@bp.route("/erp/api/suprimentos/itens/<int:item_id>/situacao", methods=["POST"])
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimento_situacao(item_id: int):
    """Move o item pelo fluxo. Só quem enxerga o item pode mexer nele."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            visiveis = {i["id"] for i in svc.listar_itens(s, atual)}
            if item_id not in visiveis:
                raise ErroNaoEncontrado("Item não encontrado.")
            svc.mudar_situacao(s, item_id, d.get("status") or "", atual,
                               d.get("observacao") or "")
            s.commit()
            return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao mudar situação do item")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/suprimentos/insumos/solicitacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="solicitar_suprimento")
def api_solicitacoes_insumo():
    """Pedir o cadastro de um insumo que ainda não existe, e ver os pedidos."""
    from app.apps.erp.core.suprimentos import insumos as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                pendentes = str(request.args.get("pendentes") or "") in ("1", "true", "sim")
                return jsonify({"ok": True,
                                "solicitacoes": svc.listar(s, atual, pendentes)})
            pedido = svc.solicitar(s, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": pedido.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha no pedido de cadastro de insumo")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/suprimentos/insumos/solicitacoes/<int:solicitacao_id>",
          methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_decidir_solicitacao_insumo(solicitacao_id: int):
    """Cadastrar ou recusar. O nome final, a categoria e a conta do plano são
    de quem decide — é isso que mantém a base de insumos limpa."""
    from app.apps.erp.core.suprimentos import insumos as svc
    d = request.get_json(silent=True) or {}
    acao = (d.get("acao") or "").strip().lower()
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if acao == "cadastrar":
                insumo = svc.cadastrar(s, solicitacao_id, d, atual)
                resposta = {"ok": True, "insumo_id": insumo.id, "codigo": insumo.codigo}
            elif acao == "recusar":
                svc.recusar(s, solicitacao_id, d.get("motivo") or "", atual)
                resposta = {"ok": True}
            else:
                return jsonify({"ok": False,
                                "erro": "Diga se é para cadastrar ou recusar."}), 400
            s.commit()
            return jsonify(resposta)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao decidir cadastro de insumo")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/suprimentos/importar/<tipo>", methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_importar(tipo: str):
    """Carga de fornecedores ou insumos por CSV exportado da planilha.

    Com `?simular=1` só relata o que aconteceria — é a prévia que se confere
    antes de deixar gravar. Rodar duas vezes não duplica: fornecedor casa por
    CNPJ, insumo pela descrição.
    """
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.importadores import suprimentos as imp
    if tipo not in ("fornecedores", "insumos"):
        return jsonify({"ok": False, "erro": "Tipo de carga desconhecido."}), 400
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Anexe o arquivo CSV."}), 400
    conteudo = arquivo.read()
    if not conteudo:
        return jsonify({"ok": False, "erro": "Arquivo vazio."}), 400
    simular = str(request.args.get("simular") or "").strip() in ("1", "true", "sim")
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            funcao = (imp.importar_fornecedores_csv if tipo == "fornecedores"
                      else imp.importar_insumos_csv)
            rel = funcao(s, conteudo, atual, simular=simular)
            if simular:
                s.rollback()
            else:
                registrar_evento(s, "importacao", 0, f"SUPRIMENTOS_{tipo.upper()}",
                                 rel, atual.id if atual else None)
                s.commit()
            logger.info("ERP/suprimentos: carga de %s (%s) — %s",
                        tipo, "prévia" if simular else "gravada", rel)
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na carga de %s", tipo)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/relatorios")
@login_obrigatorio
@permissao("ver_relatorios")
def pagina_relatorios():
    return render_template("erp_relatorios.html", **_contexto("relatorios"))


@bp.route("/erp/prestacao")
@login_obrigatorio
@permissao("ver_erp")
def pagina_prestacao():
    return render_template("erp_prestacao.html", **_contexto("prestacao"))


@bp.route("/erp/importar")
@login_obrigatorio
@permissao("importar")
def pagina_importar():
    return render_template("erp_importar.html", **_contexto("importar"))


@bp.route("/erp/configuracoes")
@login_obrigatorio
@permissao("configurar")
def pagina_config():
    return render_template("erp_config.html", **_contexto("config"))


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def _colaboradores_do_titulo(s, t) -> list[dict]:
    """Quem são as pessoas por trás deste pagamento."""
    from sqlalchemy import select as _sel
    from app.apps.erp.db.models.cadastros import Colaborador
    from app.apps.erp.db.models.financeiro import TituloColaborador
    saida = []
    if getattr(t, "colaborador_id", None):
        c = s.get(Colaborador, t.colaborador_id)
        if c is not None:
            saida.append({"id": c.id, "nome": c.nome, "cpf": c.cpf,
                          "valor": float(t.valor_liquido)})
    for v in s.scalars(_sel(TituloColaborador).where(
            TituloColaborador.titulo_id == t.id)).all():
        c = s.get(Colaborador, v.colaborador_id)
        if c is not None:
            saida.append({"id": c.id, "nome": c.nome, "cpf": c.cpf,
                          "valor": float(v.valor) if v.valor else None,
                          "observacao": v.observacao})
    return saida


def _explicar_status(s, t) -> str:
    """Em português: por que este título está parado onde está."""
    from app.apps.erp.db.models.financeiro import StatusTitulo as _S
    if t.status == _S.AGUARDANDO_AVAL:
        solicitante = s.get(Usuario, t.solicitante_id)
        perfil = solicitante.perfil.value.replace("_", " ").lower() if solicitante else "?"
        return (f"Lançado por {solicitante.nome if solicitante else '—'} ({perfil}), "
                f"que exige confirmação de uma segunda pessoa. Aguarda o aval de um "
                f"supervisor da obra, gestor de obras ou diretor financeiro — na aba Confirmar.")
    if t.status == _S.AGUARDANDO_APROVACAO:
        return ("Passou na análise e aguarda a liberação para pagamento pelo financeiro "
                "(botão 'Liberar p/ pagamento' na lista).")
    if t.status == _S.BLOQUEADO:
        return (f"A análise automática apontou risco {t.score_risco}. Revise os apontamentos "
                f"abaixo; quem tem alçada pode liberar assumindo o registro.")
    if t.status == _S.APROVADO:
        return "Liberado — aparece na aba Pagamentos, pronto para ser pago."
    if t.status == _S.DEVOLVIDO:
        return "Devolvido a quem lançou; veja o motivo no histórico."
    if t.status == _S.PAGO_PARCIAL:
        return "Parte das parcelas já foi paga; o restante segue em Pagamentos."
    return ""


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
@permissao("ver_erp")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
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
@permissao("ver_erp")
def api_titulo_detalhe(titulo_id: int):
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import ContaBancaria
    from app.apps.erp.db.models.financeiro import Analise, Evento, Parcela
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
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
            from app.apps.erp.core.documentos.armazenamento import listar as listar_anexos
            from app.apps.erp.db.models.financeiro import Pagamento as _Pg
            pagamentos = s.execute(
                select(_Pg, ContaBancaria.descricao, Usuario.nome)
                .join(Parcela, Parcela.id == _Pg.parcela_id)
                .join(ContaBancaria, ContaBancaria.id == _Pg.conta_bancaria_id, isouter=True)
                .join(Usuario, Usuario.id == _Pg.executado_por, isouter=True)
                .where(Parcela.titulo_id == t.id)
                .order_by(_Pg.data_pagamento)).all()
            solicitante = s.get(Usuario, t.solicitante_id)
            dados = {
                "cabecalho": _serializar(t, date.today(), ver_pg),
                "pode_editar": ver_pg,
                "avais": historico_avais(s, t.id),
                "solicitante": solicitante.nome if solicitante else "—",
                "modalidade": getattr(t, "modalidade", "NORMAL"),
                "porque_status": _explicar_status(s, t),
                "anexos": listar_anexos(s, "titulo", t.id),
                "colaboradores": _colaboradores_do_titulo(s, t),
                "pagamentos": [{
                    "id": pg.id, "parcela_id": pg.parcela_id,
                    "data": pg.data_pagamento.isoformat(),
                    "valor": float(pg.valor_pago),
                    "meio": pg.meio.value if hasattr(pg.meio, "value") else str(pg.meio),
                    "conta": conta or "—", "por": quem or "sistema",
                    "comprovante_id": pg.comprovante_anexo_id,
                } for pg, conta, quem in pagamentos],
                "bruto": float(t.valor_bruto),
                "retencoes_total": float(t.valor_retencoes),
                "dedutivel": t.dedutivel,
                "forma_pagamento": t.forma_pagamento.value if ver_pg else "—",
                "parcelas": [{"parcela_id": p.id, "numero": p.numero,
                              "vencimento": p.vencimento.strftime("%d/%m/%Y"),
                              "valor": float(p.valor), "status": p.status.value,
                              "boleto": ((p.linha_digitavel or "")[:24] if ver_pg
                                         else ("informado" if p.linha_digitavel else ""))}
                             for p in t.parcelas],
                "rateios": [{"obra": f"{r.obra.codigo} · {r.obra.nome}",
                             "categoria": (f"{r.categoria.codigo} · {r.categoria.descricao}"
                                           if getattr(r, "categoria", None) else None),
                             "descricao": r.descricao,
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no detalhe do título %s", titulo_id)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/titulos/acao", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
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
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao carregar configurações")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/categoria", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar categoria")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/obra", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar obra")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/conta", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar conta bancária")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Importação
# ---------------------------------------------------------------------------
@bp.route("/erp/api/importar/pipefy", methods=["POST"])
@login_obrigatorio
@permissao("importar")
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
                criar_fornecedor=bool(d.get("criar_fornecedor", True)),
                baixar_anexos=bool(d.get("baixar_anexos", True)))
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroPipefy as e:
        return jsonify({"ok": False, "erro": str(e)}), 502
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na importação do Pipefy")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/importar/csv", methods=["POST"])
@login_obrigatorio
@permissao("importar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na importação de CSV")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/importar/ofx", methods=["POST"])
@login_obrigatorio
@permissao("importar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na importação de OFX")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/plano/instalar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao instalar plano financeiro")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/categoria/substituir", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao substituir categoria")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/titulos/dedutibilidade", methods=["POST"])
@login_obrigatorio
@permissao("reclassificar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao definir dedutibilidade")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/categoria/<int:categoria_id>", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao editar categoria %s", categoria_id)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/manutencao/banco")
@login_obrigatorio
@permissao("configurar")
def api_estado_banco():
    from app.apps.erp.core.comum.migracoes import listar_estado
    try:
        return jsonify({"ok": True, "estado": listar_estado()})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/manutencao/banco/aplicar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao aplicar migrações")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/depara")
@login_obrigatorio
@permissao("configurar")
def api_listar_depara():
    from app.apps.erp.core.cadastros.depara import listar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "depara": listar(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar de-para")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/depara/instalar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao instalar de-para")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/config/depara/definir", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao definir tradução")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Lançamento
# ---------------------------------------------------------------------------
@bp.route("/erp/api/lancamento/dados")
@login_obrigatorio
@permissao("lancar")
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
            from app.apps.erp.core.cadastros.sugestao import categorias_do_usuario
            permitidas = categorias_do_usuario(s, _usuario_logado(s))
            stmt_cat = select(Categoria).where(Categoria.ativo.is_(True))
            if permitidas:
                stmt_cat = stmt_cat.where(Categoria.id.in_(permitidas))
            cats = s.scalars(stmt_cat.order_by(Categoria.ordem, Categoria.codigo)).all()
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao carregar dados do lançamento")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lancamento/ler-documento", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_ler_documento():
    """Lê o anexo (XML/PDF/imagem) e devolve os campos sugeridos."""
    from sqlalchemy import select
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.db.models.cadastros import Fornecedor, Obra
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Nenhum arquivo enviado."}), 400
    from app.apps.erp.core.comum.ia_custo import contexto
    try:
        with contexto(operacao="leitura_documento"):
            lido = ler_documento(arquivo.read(), arquivo.filename or "",
                                 dica_usuario=(request.form.get("dica") or ""))
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
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
            from app.apps.erp.core.cadastros.sugestao import sugerir_categoria, sugerir_obra
            usuario = _usuario_logado(s)
            obra_txt = (lido.get("obra_mencionada") or "").strip()
            if obra_txt:
                o = s.scalars(select(Obra).where(Obra.codigo == obra_txt.upper())).first()
                lido["obra_id"] = o.id if o else None
            if not lido.get("obra_id"):
                sug_obra = sugerir_obra(s, lido)
                if sug_obra:
                    lido["obra_id"] = sug_obra["obra_id"]
                    lido["obra_sugerida"] = sug_obra
            lido["categoria_sugerida"] = sugerir_categoria(
                s, lido, usuario, lido.get("fornecedor_id"))
            # nota de débito da locadora: acha o contrato e a parcela sozinho
            from app.apps.erp.core.locacoes import identificar_contrato
            achado = identificar_contrato(s, lido, lido.get("fornecedor_id"))
            if achado:
                lido["locacao_identificada"] = achado
    except Exception:
        logger.exception("ERP: falha ao casar documento com cadastros")
    return jsonify({"ok": True, "documento": lido})


@bp.route("/erp/api/lancamento/checar", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_checar_duplicidade():
    """Crítica de duplicidade antes de gravar."""
    from app.apps.erp.core.titulos.duplicidade import checar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.titulos.enquadramento import avaliar
            critica = checar(s, d)
            critica["enquadramento"] = avaliar(s, d, _usuario_logado(s))
            return jsonify({"ok": True, "critica": critica})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na crítica de duplicidade")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lancamento", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
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
            # credor lido do documento e ainda não cadastrado: cadastra agora,
            # no salvamento — não antes, para não criar fornecedor de rascunho
            if not d.get("fornecedor_id") and d.get("emitente_documento"):
                from app.apps.erp.core.cadastros import fornecedores as svc_forn
                from app.apps.erp.core.cadastros.validadores import somente_digitos
                from app.apps.erp.db.models.cadastros import Fornecedor as _F
                from sqlalchemy import select as _sel
                doc = somente_digitos(d["emitente_documento"])
                existente = s.scalars(_sel(_F).where(_F.cnpj_cpf == doc)).first()
                if existente is not None:
                    d["fornecedor_id"] = existente.id
                else:
                    try:
                        novo = svc_forn.criar(s, {
                            "tipo_pessoa": "PJ" if len(doc) == 14 else "PF",
                            "cnpj_cpf": doc,
                            "razao_social": (d.get("emitente_nome") or "").strip(),
                            "municipio": d.get("municipio_emissao") or None,
                        }, usuario)
                        s.flush()
                        d["fornecedor_id"] = novo.id
                        logger.info("ERP: credor %s cadastrado no salvamento do título", doc)
                    except ErroValidacao as e:
                        return jsonify({"ok": False,
                                        "erro": f"Não foi possível cadastrar o credor "
                                                f"automaticamente: {e}"}), 400
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
            if d.get("chave_acesso"):
                titulo.chave_acesso_nfe = str(d["chave_acesso"])[:44] or None
            if d.get("cno_documento"):
                titulo.cno_documento = str(d["cno_documento"])[:30] or None
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar título")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Pagamentos e lotes
# ---------------------------------------------------------------------------
@bp.route("/erp/api/pagamentos/agenda")
@login_obrigatorio
@permissao("pagar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na agenda de pagamentos")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/detalhe/<int:parcela_id>")
@login_obrigatorio
@permissao("ver_dados_pagamento")
def api_detalhe_pagamento(parcela_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_parcela_no_escopo
    from app.apps.erp.core.pagamentos.lotes import dados_pagamento
    try:
        with get_session() as s:
            exigir_parcela_no_escopo(s, _usuario_logado(s), parcela_id)
            return jsonify({"ok": True, "pagamento": dados_pagamento(s, parcela_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroNaoEncontrado:
        raise            # vira 404 no errorhandler, nao 500
    except Exception as e:
        logger.exception("ERP: falha no detalhe de pagamento")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/baixar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
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
                        valor_pago=it.get("valor_pago") or it.get("valor"),
                        usuario=usuario)
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao baixar pagamentos")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes")
@login_obrigatorio
@permissao("pagar")
def api_lotes():
    from app.apps.erp.core.pagamentos.lotes import listar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "lotes": listar(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar lotes")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>")
@login_obrigatorio
@permissao("pagar")
def api_lote_detalhe(lote_id: int):
    from app.apps.erp.core.pagamentos.lotes import detalhar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "lote": detalhar(s, lote_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/lotes/criar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_criar_lote():
    from app.apps.erp.core.pagamentos.lotes import adicionar_parcelas, criar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            lote = criar(s, d, usuario)
            rel = {"incluidas": [], "recusadas": [], "nao_encontradas": []}
            ids = list(d.get("parcela_ids") or [])
            # o lote nasce com as SPs: coladas como texto ou marcadas na tabela
            if d.get("texto"):
                from app.apps.erp.core.pagamentos.lotes import (
                    extrair_ids_sp, parcelas_por_sp,
                )
                numeros = extrair_ids_sp(d["texto"])
                achado = parcelas_por_sp(s, numeros) if numeros else {}
                ids += [x["parcela_id"] for x in achado.get("parcelas", [])
                        if x.get("status") != "PAGA"]
                rel["nao_encontradas"] = achado.get("nao_encontradas", [])
            if ids:
                achadas = adicionar_parcelas(s, lote.id, sorted(set(ids)), usuario)
                rel.update({k: v for k, v in achadas.items()})
            s.commit()
            return jsonify({"ok": True, "lote_id": lote.id, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar lote")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/parcelas", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao alterar lote")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/status", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
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
@permissao("pagar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao buscar SPs coladas")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Conciliação
# ---------------------------------------------------------------------------
@bp.route("/erp/api/conciliacao/painel")
@login_obrigatorio
@permissao("conciliar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no painel de conciliação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/conciliacao/executar", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
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
    except IntegrityError:
        # outra pessoa executou a conciliação no mesmo instante: a restrição
        # única barrou a repetição. Nada gravado; é só rodar de novo.
        return jsonify({"ok": False, "erro": "Outra conciliação acabou de rodar. "
                        "Recarregue a tela e execute de novo."}), 409
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na conciliação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/conciliacao/manual", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na conciliação manual")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Relatórios
# ---------------------------------------------------------------------------
@bp.route("/erp/api/relatorios", methods=["POST"])
@login_obrigatorio
@permissao("ver_relatorios")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no relatório")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/relatorios/csv", methods=["POST"])
@login_obrigatorio
@permissao("ver_relatorios")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao exportar CSV")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Extrato, movimentações e recebimentos
# ---------------------------------------------------------------------------
@bp.route("/erp/api/conciliacao/extrato")
@login_obrigatorio
@permissao("conciliar")
def api_extrato():
    from app.apps.erp.core.pagamentos.conciliacao import extrato_detalhado
    try:
        with get_session() as s:
            linhas = extrato_detalhado(
                s, conta_bancaria_id=request.args.get("conta_id", type=int),
                situacao=request.args.get("situacao", "todos"))
        return jsonify({"ok": True, "linhas": linhas})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar extrato")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/conciliacao/candidatos/<int:extrato_id>")
@login_obrigatorio
@permissao("conciliar")
def api_candidatos(extrato_id: int):
    from app.apps.erp.core.pagamentos.conciliacao import candidatos_para_extrato
    try:
        with get_session() as s:
            return jsonify({"ok": True, "candidatos": candidatos_para_extrato(s, extrato_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/movimentacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="conciliar", POST="conciliar")
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
        except ErroNaoEncontrado:
            raise        # recusa de escopo vira 404, nunca 500
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar movimentação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/receber", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="receber", POST="receber")
def api_receber():
    from app.apps.erp.core.titulos.receber import criar_medicao, listar_receber
    if request.method == "GET":
        try:
            with get_session() as s:
                return jsonify({"ok": True, "titulos": listar_receber(s, {
                    "obra_id": request.args.get("obra_id", type=int),
                    "status": request.args.get("status")})})
        except ErroNaoEncontrado:
            raise        # recusa de escopo vira 404, nunca 500
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao lançar medição")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/receber/baixar", methods=["POST"])
@login_obrigatorio
@permissao("receber")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no recebimento")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Ajustes: reclassificar e desfazer
# ---------------------------------------------------------------------------
@bp.route("/erp/api/titulos/<int:titulo_id>/reclassificar", methods=["POST"])
@login_obrigatorio
@permissao("reclassificar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao reclassificar")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/titulos/<int:titulo_id>/desfazer", methods=["GET", "POST"])
@login_obrigatorio
@permissao("desfazer")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao desfazer")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_obra(obra_id: int):
    """Cadastro completo da obra: identificação, endereço, contrato e tributação."""
    from sqlalchemy import select
    from app.apps.erp.core.titulos.tributacao import resumo_tributacao
    from app.apps.erp.db.models.cadastros import Obra, ObraAditivo
    from app.apps.erp.core.comum.auditoria import registrar_evento
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
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
                if "conta_bancaria_id" in d:
                    obra.conta_bancaria_id = (int(d["conta_bancaria_id"])
                                              if d["conta_bancaria_id"] else None)
                numeros = ("valor_contrato latitude longitude "
                           "aliquota_iss aliquota_iss_pct pct_servico_iss "
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
                        bruto = str(d[campo]).strip()
                        if campo in ("latitude", "longitude"):
                            # coordenada não é moeda: o ponto é decimal, não milhar
                            v = bruto.replace(",", ".")
                        elif "," in bruto:
                            v = bruto.replace(".", "").replace(",", ".")
                        else:
                            v = bruto
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
            for campo in ("valor_contrato", "latitude", "longitude",
                          "aliquota_iss", "aliquota_iss_pct",
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no cadastro da obra %s", obra_id)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/aditivos", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
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
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao registrar aditivo")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/tributacao", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_simular_tributacao(obra_id: int):
    """Simula as retenções de uma medição com o cadastro fiscal da obra."""
    from app.apps.erp.core.titulos.tributacao import calcular
    from app.apps.erp.db.models.cadastros import Obra
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            calc = calcular(obra, d.get("valor") or 0,
                            sem_deducao=bool(d.get("sem_deducao")),
                            pct_servico_iss=d.get("pct_servico_iss"),
                            pct_servico_inss=d.get("pct_servico_inss"),
                            aliquota_iss=d.get("aliquota_iss"))
        return jsonify({"ok": True, "calculo": calc.como_dict()})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao simular tributação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/comprovante", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
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
                    except ErroNaoEncontrado:
                        raise        # recusa de escopo vira 404, nunca 500
                    except Exception as e:
                        logger.warning("ERP: aviso do comprovante falhou (%s)", e)
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao processar comprovante")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/comprovante/confirmar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao confirmar baixa por comprovante")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/anexos/<entidade>/<int:entidade_id>", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar")
def api_anexos(entidade: str, entidade_id: int):
    """Anexos guardados no próprio banco, comprimidos."""
    from app.apps.erp.core.auth.permissoes import exigir_entidade_no_escopo
    from app.apps.erp.core.documentos.armazenamento import listar, salvar
    if entidade not in ("titulo", "obra", "movimentacao", "fornecedor",
                        "medicao", "contrato_servico"):
        return jsonify({"ok": False, "erro": f"Entidade inválida: {entidade}"}), 400
    try:
        with get_session() as s:
            exigir_entidade_no_escopo(s, _usuario_logado(s), entidade, entidade_id)
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
    except ErroNaoEncontrado:
        raise            # vira 404 no errorhandler, nao 500
    except Exception as e:
        logger.exception("ERP: falha no anexo")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/anexo/<int:anexo_id>")
@login_obrigatorio
@permissao("ver_erp")
def baixar_anexo(anexo_id: int):
    """Serve o arquivo direto do banco."""
    from flask import Response
    from app.apps.erp.core.auth.permissoes import exigir_anexo_no_escopo
    from app.apps.erp.core.documentos.armazenamento import obter
    try:
        with get_session() as s:
            exigir_anexo_no_escopo(s, _usuario_logado(s), anexo_id)
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
@permissao("lancar")
def api_excluir_anexo(anexo_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_anexo_no_escopo
    from app.apps.erp.core.documentos.armazenamento import excluir
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_anexo_no_escopo(s, usuario, anexo_id)
            excluir(s, anexo_id, usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/usuarios", methods=["GET", "POST"])
@login_obrigatorio
@permissao("gerir_usuarios")
def api_usuarios():
    """Cadastro de operadores. Só o ADMIN mexe."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import ROTULOS, escopo_visao, exigir
    from app.apps.erp.core.auth.service import criar_usuario
    from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
    from app.apps.erp.db.models.cadastros import (
        EscopoVisao, Obra, PerfilUsuario, Usuario, UsuarioCategoria, UsuarioObra,
    )
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
                    "categorias": [x.categoria_id for x in s.scalars(
                        select(UsuarioCategoria).where(
                            UsuarioCategoria.usuario_id == u.id)).all()],
                    "perfil": u.perfil.value,
                    "perfil_rotulo": ROTULOS.get(u.perfil, u.perfil.value),
                    "escopo_visao": escopo_visao(u).value,
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
            # Escopo ausente ou desconhecido cai no mais restritivo.
            try:
                u.escopo_visao = EscopoVisao(d.get("escopo_visao"))
            except ValueError:
                u.escopo_visao = EscopoVisao.PROPRIOS
            for obra_id in (d.get("obras") or []):
                s.add(UsuarioObra(usuario_id=u.id, obra_id=int(obra_id)))
            s.commit()
            return jsonify({"ok": True, "usuario_id": u.id})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no cadastro de operador")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/usuarios/<int:usuario_id>", methods=["POST"])
@login_obrigatorio
@permissao("gerir_usuarios")
def api_editar_usuario(usuario_id: int):
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.db.models.cadastros import (
        EscopoVisao, PerfilUsuario, Usuario, UsuarioObra,
    )
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
            if "escopo_visao" in d:
                # Ampliar o alcance é escolha explícita; valor estranho fecha.
                try:
                    u.escopo_visao = EscopoVisao(d["escopo_visao"])
                except ValueError:
                    return jsonify({"ok": False,
                                    "erro": "Escopo de visão inválido."}), 400
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
            if "categorias" in d:
                from app.apps.erp.db.models.cadastros import UsuarioCategoria
                for v in s.scalars(select(UsuarioCategoria).where(
                        UsuarioCategoria.usuario_id == u.id)).all():
                    s.delete(v)
                s.flush()
                for cat_id in (d.get("categorias") or []):
                    s.add(UsuarioCategoria(usuario_id=u.id, categoria_id=int(cat_id)))
            s.commit()
        return jsonify({"ok": True})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao editar operador")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/usuarios/<int:usuario_id>/permissoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="gerir_usuarios", POST="gerir_usuarios")
def api_permissoes_do_usuario(usuario_id: int):
    """As marcações de permissão de UMA pessoa, sobre o que o cargo já dá.

    GET devolve, para cada ação: o que o cargo dá, o que está marcado à mão e o
    resultado. POST grava as marcações — só as que diferem do cargo viram linha,
    para o cadastro não guardar repetição do que o cargo já responde.
    """
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Usuario, UsuarioPermissao
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            exigir(atual, "gerir_usuarios")
            u = s.get(Usuario, usuario_id)
            if u is None:
                return jsonify({"ok": False, "erro": "Operador não encontrado."}), 404

            marcadas = {r.acao: r.concedida for r in s.scalars(
                select(UsuarioPermissao).where(
                    UsuarioPermissao.usuario_id == usuario_id)).all()
                if r.usuario_id == usuario_id}

            if request.method == "GET":
                acoes = []
                for acao in sorted(PERMISSOES):
                    do_cargo = u.perfil in PERMISSOES[acao]
                    marcada = marcadas.get(acao)
                    acoes.append({
                        "acao": acao,
                        "rotulo": ACAO_ROTULOS.get(acao, acao),
                        "do_cargo": do_cargo,
                        "marcada": marcada,
                        "efetiva": decidir(u.perfil, acao, marcadas),
                        "travada": u.perfil is PerfilUsuario.ADMIN and acao in PROTEGIDAS_DO_ADMIN,
                    })
                return jsonify({"ok": True, "usuario": {"id": u.id, "nome": u.nome,
                                                        "perfil": u.perfil.value,
                                                        "perfil_rotulo": ROTULOS.get(u.perfil, u.perfil.value)},
                                "acoes": acoes})

            pedido = (request.get_json(silent=True) or {}).get("permissoes") or {}
            desconhecidas = [a for a in pedido if a not in PERMISSOES]
            if desconhecidas:
                return jsonify({"ok": False,
                                "erro": f"Ação desconhecida: {', '.join(sorted(desconhecidas))}."}), 400

            for linha in s.scalars(select(UsuarioPermissao).where(
                    UsuarioPermissao.usuario_id == usuario_id)).all():
                if linha.usuario_id == usuario_id:
                    s.delete(linha)
            s.flush()

            gravadas = {}
            for acao, valor in pedido.items():
                quer = bool(valor)
                if quer == (u.perfil in PERMISSOES[acao]):
                    continue          # igual ao cargo: não vira exceção
                s.add(UsuarioPermissao(usuario_id=usuario_id, acao=acao,
                                       concedida=quer, definida_por=atual.id))
                gravadas[acao] = quer

            registrar_evento(s, "usuario", usuario_id, "PERMISSOES_AJUSTADAS",
                             {"permissoes": gravadas}, atual.id)
            s.commit()
            logger.info("ERP: permissões de %s ajustadas por %s: %s",
                        usuario_id, atual.id, gravadas)
        return jsonify({"ok": True, "permissoes": gravadas})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao ajustar permissões do operador")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/permissoes")
@login_obrigatorio
@permissao("ver_erp")
def api_permissoes():
    from app.apps.erp.core.auth.permissoes import contexto_permissoes
    with get_session() as s:
        return jsonify({"ok": True, "contexto": contexto_permissoes(s, _usuario_logado(s))})


@bp.route("/erp/api/titulos/<int:titulo_id>/historico")
@login_obrigatorio
@permissao("ver_erp")
def api_historico(titulo_id: int):
    """Trilha completa do título: tudo que mudou, quando e por quem — mais os
    avisos enviados ao solicitante."""
    from sqlalchemy import select
    from app.apps.erp.core.notificacoes import historico as historico_avisos
    from app.apps.erp.db.models.financeiro import Evento
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao montar histórico")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/pagamentos/<int:pagamento_id>/avisar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_reenviar_aviso(pagamento_id: int):
    """Reenvio manual do aviso (quando a pessoa apagou a mensagem, por ex.)."""
    from app.apps.erp.core.notificacoes import avisar_baixa
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            rel = avisar_baixa(s, pagamento_id, forcar=bool(d.get("forcar")))
            s.commit()
        return jsonify({"ok": True, "aviso": rel})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao reenviar aviso")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Aval (dupla confirmação)
# ---------------------------------------------------------------------------
@bp.route("/erp/api/avais/pendentes")
@login_obrigatorio
@permissao("avalizar")
def api_avais_pendentes():
    from app.apps.erp.core.titulos.aval import pendentes
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            return jsonify({"ok": True, "titulos": pendentes(s, usuario),
                            "perfil": usuario.perfil.value, "quem": usuario.nome})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar avais pendentes")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/avais/<int:titulo_id>", methods=["POST"])
@login_obrigatorio
@permissao("avalizar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao registrar aval")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/historico/<entidade>/<int:entidade_id>")
@login_obrigatorio
@permissao("ver_erp")
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
            from app.apps.erp.core.auth.permissoes import exigir_entidade_no_escopo
            exigir_entidade_no_escopo(s, _usuario_logado(s), entidade, entidade_id)
            linhas = s.execute(
                select(Evento, Usuario.nome)
                .join(Usuario, Usuario.id == Evento.usuario_id, isouter=True)
                .where(Evento.entidade_tipo == entidade, Evento.entidade_id == entidade_id)
                .order_by(Evento.criado_em.desc()).limit(300)).all()
            return jsonify({"ok": True, "eventos": [{
                "quando": e.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
                "acao": e.acao, "por": nome or "sistema", "detalhe": e.detalhe,
            } for e, nome in linhas]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no histórico de %s", entidade)
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/auditoria")
@login_obrigatorio
@permissao("configurar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na auditoria")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/interessados/<int:titulo_id>", methods=["GET", "POST", "DELETE"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar", DELETE="lancar")
def api_interessados(titulo_id: int):
    """Quem mais acompanha o título e recebe os avisos."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import TituloInteressado
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            # Sem esta trava, dava para se incluir como interessado num título
            # fora do escopo e passar a receber o aviso de pagamento COM o
            # comprovante — contornando por fora todo o resto do controle.
            exigir_titulo_no_escopo(s, atual, titulo_id)
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
    except ErroNaoEncontrado:
        raise            # vira 404 no errorhandler, nao 500
    except Exception as e:
        logger.exception("ERP: falha nos interessados")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/interessados", methods=["GET", "POST"])
@login_obrigatorio
@permissao("configurar")
def api_obra_interessados(obra_id: int):
    """Interessados fixos da obra: entram em todo título dela."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import ObraInteressado
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha nos interessados da obra")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/operadores/contato")
@login_obrigatorio
@permissao("lancar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
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
@permissao("ver_erp")
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
                    "conta_bancaria_id": o.conta_bancaria_id,
                    "latitude": float(o.latitude) if o.latitude else None,
                    "longitude": float(o.longitude) if o.longitude else None,
                    "aliquota_iss": float(o.aliquota_iss_pct or 0) or None,
                })
            return jsonify({"ok": True, "obras": linhas,
                            "fases": [{"chave": k, "rotulo": v} for k, v in FASES_OBRA]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar obras")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/fase", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_mudar_fase(obra_id: int):
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Obra, ObraFase
    d = request.get_json(silent=True) or {}
    fase = (d.get("fase") or "").upper()
    if fase not in dict(FASES_OBRA):
        return jsonify({"ok": False, "erro": f"Fase inválida: {fase}"}), 400
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao mudar fase")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/fases")
@login_obrigatorio
@permissao("ver_erp")
def api_historico_fases(obra_id: int):
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import ObraFase
    fases = dict(FASES_OBRA)
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            linhas = s.execute(
                select(ObraFase, Usuario.nome)
                .join(Usuario, Usuario.id == ObraFase.usuario_id, isouter=True)
                .where(ObraFase.obra_id == obra_id)
                .order_by(ObraFase.criado_em.desc())).all()
            return jsonify({"ok": True, "fases": [{
                "fase": f.fase, "rotulo": fases.get(f.fase, f.fase),
                "observacao": f.observacao, "por": nome or "—",
                "quando": f.criado_em.strftime("%d/%m/%Y %H:%M")} for f, nome in linhas]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/titulos")
@login_obrigatorio
@permissao("ver_erp")
def api_titulos_da_obra(obra_id: int):
    """Tudo que passou pela obra: o que se gastou e o que se recebeu."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.apps.erp.db.models.financeiro import EspecieTitulo, Rateio, Titulo
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar títulos da obra")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Prestação de contas: fundo fixo e fatura de cartão
# ---------------------------------------------------------------------------
@bp.route("/erp/api/prestacao/comprovante", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_prestacao_comprovante():
    """Lê um comprovante e devolve a linha, guardando o arquivo no banco."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.core.titulos.prestacao import ler_bloco_de_comprovantes
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o comprovante."}), 400
    try:
        conteudo = arquivo.read()
        nome = arquivo.filename or "comprovante"
        # PDF com várias páginas = vários comprovantes, um por página
        linhas = ler_bloco_de_comprovantes(conteudo, nome)
        with get_session() as s:
            usuario = _usuario_logado(s)
            for linha in linhas:
                pagina = linha.pop("_conteudo_pagina", None)
                nome_pagina = linha.pop("_nome_pagina", nome)
                anexo = salvar(s, pagina if pagina else conteudo, nome_pagina,
                               entidade_tipo="prestacao_rascunho", entidade_id=usuario.id,
                               categoria="COMPROVANTE", usuario=usuario)
                linha["anexo_id"] = anexo.id
            s.commit()
        return jsonify({"ok": True, "linhas": linhas,
                        "paginas": len(linhas),
                        "linha": linhas[0] if linhas else None})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ler comprovante da prestação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao/fatura", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ler fatura")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao/criticar", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na crítica da prestação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar prestação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacao/<int:titulo_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_prestacao_detalhe(titulo_id: int):
    from app.apps.erp.core.titulos.prestacao import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            return jsonify({"ok": True, "prestacao": detalhar(s, titulo_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/prestacao/<int:titulo_id>/conferir", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_conferir(titulo_id: int):
    from app.apps.erp.core.titulos.prestacao import confirmar_analise
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
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
@permissao("ver_erp")
def api_prestacao_historico():
    from app.apps.erp.core.titulos.prestacao import historico_do_solicitante
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            alvo = request.args.get("usuario_id", type=int) or usuario.id
            return jsonify({"ok": True, "historico": historico_do_solicitante(
                s, alvo, (request.args.get("modalidade") or "FUNDO_FIXO").upper())})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/prestacoes/pendentes")
@login_obrigatorio
@permissao("aprovar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar prestações pendentes")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/movimentacoes/neutras")
@login_obrigatorio
@permissao("conciliar")
def api_neutras():
    """Pontas soltas: entrou e não foi devolvido, saiu e não foi ressarcido."""
    from app.apps.erp.core.titulos.receber import neutras_sem_par
    try:
        with get_session() as s:
            return jsonify({"ok": True, "pendentes": neutras_sem_par(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar neutras")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/movimentacoes/vincular", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao vincular par neutro")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/conciliacao/par-neutro", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
def api_par_neutro():
    """Marca duas linhas do extrato (a entrada e a saída) como par que se anula."""
    from app.apps.erp.core.titulos.receber import resolver_par_no_extrato
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = resolver_par_no_extrato(
                s, int(d["extrato_entrada_id"]), int(d["extrato_saida_id"]),
                motivo=d.get("motivo", ""), contraparte=d.get("contraparte", ""),
                tipo=(d.get("tipo") or "RECEBIMENTO_INDEVIDO"), usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao resolver par neutro")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/titulos/<int:titulo_id>/parcelas", methods=["POST"])
@login_obrigatorio
@permissao("reclassificar")
def api_editar_parcelas(titulo_id: int):
    """Ajusta vencimento e boleto das parcelas em aberto — sem desfazer nada."""
    from datetime import date as _date
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import Parcela, StatusParcela, Titulo as _T
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir(usuario, "reclassificar")     # financeiro, diretor ou admin
            t = s.get(_T, titulo_id)
            if t is None:
                return jsonify({"ok": False, "erro": "Título não encontrado."}), 404
            motivo = (d.get("motivo") or "").strip()
            if len(motivo) < 5:
                return jsonify({"ok": False, "erro": "Informe o motivo da alteração."}), 400
            mudancas = []
            for alteracao in (d.get("parcelas") or []):
                p = s.get(Parcela, int(alteracao.get("parcela_id", 0)))
                if p is None or p.titulo_id != t.id:
                    continue
                if p.status == StatusParcela.PAGA:
                    return jsonify({"ok": False,
                                    "erro": f"Parcela {p.numero} já está paga — "
                                            f"desfaça a baixa antes de alterar."}), 400
                if alteracao.get("vencimento"):
                    novo = _date.fromisoformat(alteracao["vencimento"])
                    if novo != p.vencimento:
                        mudancas.append(f"parcela {p.numero}: vencimento "
                                        f"{p.vencimento:%d/%m/%Y} → {novo:%d/%m/%Y}")
                        p.vencimento = novo
                if "linha_digitavel" in alteracao:
                    linha = somente_digitos(alteracao["linha_digitavel"] or "")
                    if linha != (p.linha_digitavel or ""):
                        mudancas.append(f"parcela {p.numero}: boleto atualizado")
                        p.linha_digitavel = linha or None
            if not mudancas:
                return jsonify({"ok": False, "erro": "Nada foi alterado."}), 400
            registrar_evento(s, "titulo", t.id, "PARCELAS_ALTERADAS",
                             {"mudancas": mudancas, "motivo": motivo}, usuario.id)
            s.commit()
        return jsonify({"ok": True, "mudancas": mudancas})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao alterar parcelas")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Contratos de empreita
# ---------------------------------------------------------------------------
@bp.route("/erp/api/empreitas", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar")
def api_empreitas():
    from app.apps.erp.core.titulos.empreita import criar_contrato, listar
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "contratos": listar(s, usuario)})
            contrato = criar_contrato(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "contrato": {
                "id": contrato.id, "numero": contrato.numero,
                "valor": float(contrato.valor_total), "status": contrato.status}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em empreitas")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/empreitas/<int:contrato_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_empreita_detalhe(contrato_id: int):
    from app.apps.erp.core.documentos.armazenamento import listar as listar_anexos
    from app.apps.erp.core.titulos.empreita import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_empreita_no_escopo
            exigir_empreita_no_escopo(s, _usuario_logado(s), contrato_id)
            dados = detalhar(s, contrato_id)
            for m in dados["medicoes"]:
                m["anexos"] = listar_anexos(s, "medicao", m["id"])
            dados["anexos"] = listar_anexos(s, "contrato_servico", contrato_id)
            return jsonify({"ok": True, "contrato": dados})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/empreitas/<int:contrato_id>/aprovar", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_aprovar_empreita(contrato_id: int):
    from app.apps.erp.core.titulos.empreita import aprovar_contrato
    try:
        with get_session() as s:
            c = aprovar_contrato(s, contrato_id, _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "status": c.status})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/empreitas/<int:contrato_id>/aditivo", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_aditivar_empreita(contrato_id: int):
    from app.apps.erp.core.titulos.empreita import aditivar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            rel = aditivar(s, contrato_id, d.get("valor"), d.get("motivo", ""),
                           _usuario_logado(s), quantidade=d.get("quantidade"))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/empreitas/<int:contrato_id>/medicoes", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_medir(contrato_id: int):
    """Registra a medição (ainda não é pagamento) e devolve as críticas."""
    from app.apps.erp.core.titulos.empreita import criticar_medicao, registrar_medicao
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_empreita_no_escopo
            exigir_empreita_no_escopo(s, _usuario_logado(s), contrato_id)
            usuario = _usuario_logado(s)
            if d.get("apenas_criticar"):
                return jsonify({"ok": True,
                                "criticas": criticar_medicao(s, contrato_id, d)})
            criticas = criticar_medicao(s, contrato_id, d)
            m = registrar_medicao(s, contrato_id, d, usuario)
            s.commit()
            return jsonify({"ok": True, "medicao": {
                "id": m.id, "numero": m.numero, "valor": float(m.valor_medido),
                "abatido": float(m.valor_adiantamento_abatido),
                "liquido": float(m.valor_liquido)},
                "criticas": [x for x in criticas if x["gravidade"] != "BLOQUEIA"]})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao medir")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/medicoes/<int:medicao_id>/autorizar", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_autorizar_medicao(medicao_id: int):
    from app.apps.erp.core.titulos.empreita import autorizar_medicao
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            rel = autorizar_medicao(s, medicao_id, _usuario_logado(s), d)
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao autorizar medição")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/periodo", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_periodo():
    """Fechamento e destrave do período — só diretor e admin."""
    from app.apps.erp.core.titulos.empreita import (
        definir_bloqueio, destravar, periodo_bloqueado_ate,
    )
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                limite = periodo_bloqueado_ate(s)
                return jsonify({"ok": True,
                                "bloqueado_ate": limite.isoformat() if limite else None})
            d = request.get_json(silent=True) or {}
            if d.get("destravar"):
                rel = destravar(s, d.get("ate"), d.get("motivo", ""), usuario,
                                horas=int(d.get("horas") or 24))
            else:
                rel = definir_bloqueio(s, d.get("ate"), usuario)
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/meu-cadastro")
@login_obrigatorio
@permissao("ver_erp")
def pagina_meu_cadastro():
    return render_template("erp_meu_cadastro.html", **_contexto("prestacao"))


@bp.route("/erp/api/meu-cadastro", methods=["GET", "POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_meu_cadastro():
    """Dados de recebimento de quem está logado.

    No reembolso de fundo fixo o favorecido é a própria pessoa — faz sentido
    ela mesma manter a chave Pix, sem depender do financeiro cadastrar.
    """
    from sqlalchemy import select
    from app.apps.erp.core.cadastros import fornecedores as svc_forn
    from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import (
        FormaPagamento, Fornecedor, FornecedorConta, StatusConta,
    )
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            doc = somente_digitos(usuario.cpf or "")
            pessoa = (s.scalars(select(Fornecedor).where(Fornecedor.cnpj_cpf == doc)).first()
                      if doc else None)

            if request.method == "GET":
                conta = None
                if pessoa is not None:
                    conta = next((c for c in pessoa.contas
                                  if c.status == StatusConta.HOMOLOGADA), None)
                return jsonify({"ok": True, "cadastro": {
                    "nome": usuario.nome, "cpf": usuario.cpf, "telefone": usuario.telefone,
                    "fornecedor_id": pessoa.id if pessoa else None,
                    "conta_descricao": (f"{conta.pix_tipo}: {conta.pix_chave}"
                                        if conta and conta.pix_chave else None),
                    "pix_chave": conta.pix_chave if conta else None,
                    "pix_tipo": conta.pix_tipo if conta else None,
                }})

            d = request.get_json(silent=True) or {}
            if not doc or not cpf_valido(doc):
                return jsonify({"ok": False,
                                "erro": "Seu CPF não está cadastrado ou é inválido. "
                                        "Peça ao administrador para corrigir."}), 400
            chave = (d.get("pix_chave") or "").strip()
            if not chave:
                return jsonify({"ok": False, "erro": "Informe a chave Pix."}), 400
            if pessoa is None:
                pessoa = svc_forn.criar(s, {"tipo_pessoa": "PF", "cnpj_cpf": doc,
                                            "razao_social": usuario.nome}, usuario)
                s.flush()
            for c_ in pessoa.contas:
                if c_.forma == FormaPagamento.PIX:
                    c_.status = StatusConta.INATIVA
            s.add(FornecedorConta(
                fornecedor_id=pessoa.id, forma=FormaPagamento.PIX,
                pix_tipo=(d.get("pix_tipo") or "CPF").upper(), pix_chave=chave,
                titular_nome=usuario.nome, titular_doc=doc,
                status=StatusConta.PENDENTE))
            s.flush()
            registrar_evento(s, "fornecedor", pessoa.id, "CONTA_INFORMADA_PELO_PROPRIO", {
                "usuario": usuario.email, "chave": chave,
                "observacao": "aguarda homologação do financeiro"}, usuario.id)
            s.commit()
        return jsonify({"ok": True, "aviso": "Chave registrada — o financeiro homologa "
                                             "antes do primeiro pagamento."})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no meu cadastro")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Locações
# ---------------------------------------------------------------------------
@bp.route("/erp/api/locacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar")
def api_locacoes():
    from app.apps.erp.core.locacoes import criar, listar, painel_por_obra
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "contratos": listar(s, usuario),
                                "por_obra": painel_por_obra(s)})
            c = criar(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "contrato": {"id": c.id, "numero": c.numero}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em locações")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/locacoes/<int:contrato_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_locacao_detalhe(contrato_id: int):
    from app.apps.erp.core.locacoes import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_locacao_no_escopo
            exigir_locacao_no_escopo(s, _usuario_logado(s), contrato_id)
            return jsonify({"ok": True, "contrato": detalhar(s, contrato_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/locacoes/<int:contrato_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_locacao_acao(contrato_id: int, acao: str):
    from app.apps.erp.core.locacoes import devolver, gerar_previsao, remanejar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_locacao_no_escopo
            exigir_locacao_no_escopo(s, _usuario_logado(s), contrato_id)
            usuario = _usuario_logado(s)
            if acao == "devolver":
                rel = devolver(s, contrato_id, d, usuario)
            elif acao == "remanejar":
                rel = remanejar(s, contrato_id, d, usuario)
            elif acao == "previsao":
                rel = {"parcelas": gerar_previsao(s, contrato_id,
                                                  meses=int(d.get("meses") or 6),
                                                  usuario=usuario)}
            else:
                return jsonify({"ok": False, "erro": f"Ação inválida: {acao}"}), 400
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na ação de locação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/locacoes/parcelas/<int:parcela_id>/lancar", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_lancar_locacao(parcela_id: int):
    from app.apps.erp.core.locacoes import lancar_parcela
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_parcela_locacao_no_escopo
            exigir_parcela_locacao_no_escopo(s, _usuario_logado(s), parcela_id)
            rel = lancar_parcela(s, parcela_id, request.get_json(silent=True) or {},
                                 _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/insumos", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_insumos():
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import Insumo, InsumoCategoria
    try:
        with get_session() as s:
            if request.method == "GET":
                stmt = select(Insumo).where(Insumo.ativo.is_(True))
                if request.args.get("locavel"):
                    stmt = stmt.where(Insumo.locavel.is_(True))
                itens = s.scalars(stmt.order_by(Insumo.descricao)).all()
                return jsonify({"ok": True, "insumos": [{
                    "id": i.id, "codigo": i.codigo, "descricao": i.descricao,
                    "unidade": i.unidade, "locavel": i.locavel,
                    "valor_compra": float(i.valor_referencia_compra or 0) or None,
                    "valor_locacao": float(i.valor_referencia_locacao or 0) or None,
                } for i in itens]})
            d = request.get_json(silent=True) or {}
            from decimal import Decimal as _D
            i = Insumo(codigo=(d.get("codigo") or "").strip().upper(),
                       descricao=(d.get("descricao") or "").strip(),
                       unidade=(d.get("unidade") or "").strip() or None,
                       locavel=bool(d.get("locavel")),
                       categoria_id=d.get("categoria_id") or None,
                       valor_referencia_compra=(_D(str(d["valor_compra"]).replace(",", "."))
                                                if d.get("valor_compra") else None),
                       valor_referencia_locacao=(_D(str(d["valor_locacao"]).replace(",", "."))
                                                 if d.get("valor_locacao") else None))
            if not i.codigo or not i.descricao:
                return jsonify({"ok": False, "erro": "Informe código e descrição."}), 400
            s.add(i)
            s.commit()
            return jsonify({"ok": True, "insumo_id": i.id})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em insumos")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/locacoes/ler-contrato", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_ler_contrato_locacao():
    """Lê o contrato da locadora e devolve o rascunho do cadastro."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.core.locacoes import ler_contrato
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o contrato em PDF ou foto."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rascunho = ler_contrato(s, conteudo, arquivo.filename or "contrato.pdf")
            anexo = salvar(s, conteudo, arquivo.filename or "contrato.pdf",
                           entidade_tipo="locacao_rascunho", entidade_id=usuario.id,
                           categoria="CONTRATO", descricao="Contrato de locação",
                           usuario=usuario)
            rascunho["anexo_id"] = anexo.id
            s.commit()
        return jsonify({"ok": True, "rascunho": rascunho})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ler contrato de locação")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/mapa")
@login_obrigatorio
@permissao("ver_relatorios")
def api_mapa():
    """Obras, equipamentos locados e volume financeiro por lugar."""
    from app.apps.erp.core.locacoes import mapa
    try:
        with get_session() as s:
            return jsonify({"ok": True, **mapa(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no mapa")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/ia/consumo")
@login_obrigatorio
@permissao("configurar")
def api_ia_consumo():
    """Painel de consumo de IA — restrito a quem administra."""
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.ia_custo import painel
    try:
        with get_session() as s:
            exigir(_usuario_logado(s), "configurar")
            return jsonify({"ok": True, "consumo": painel(
                s, dias=request.args.get("dias", type=int) or 90)})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no painel de IA")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/ia/teto", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_ia_teto():
    """Teto mensal de gasto com IA (US$). Só avisa; não bloqueia nada."""
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.comum.ia_custo import definir_teto_mensal, situacao_teto
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir(usuario, "configurar")
            teto = definir_teto_mensal(s, d.get("teto"), usuario.id)
            registrar_evento(s, "parametro", 0, "IA_TETO_ALTERADO",
                             {"teto_usd": (str(teto) if teto is not None else None)},
                             usuario.id)
            s.commit()
            return jsonify({"ok": True, "situacao": situacao_teto(s)})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao definir teto de IA")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ---------------------------------------------------------------------------
# Pessoal
# ---------------------------------------------------------------------------
@bp.route("/erp/api/colaboradores", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="editar_colaboradores")
def api_colaboradores():
    from app.apps.erp.core.pessoal import listar_colaboradores, salvar_colaborador
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "colaboradores": listar_colaboradores(
                    s, request.args.get("obra_id", type=int),
                    ativos=request.args.get("todos") != "1")})
            c = salvar_colaborador(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "colaborador_id": c.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em colaboradores")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/funcoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="editar_colaboradores")
def api_funcoes():
    from sqlalchemy import select
    from decimal import Decimal as _D
    from app.apps.erp.db.models.cadastros import Funcao
    try:
        with get_session() as s:
            if request.method == "POST":
                d = request.get_json(silent=True) or {}
                nome = (d.get("nome") or "").strip()
                if not nome:
                    return jsonify({"ok": False, "erro": "Informe o nome da função."}), 400
                f = Funcao(nome=nome,
                           valor_diaria=(_D(str(d["valor_diaria"]).replace(",", "."))
                                         if d.get("valor_diaria") else None))
                s.add(f)
                s.commit()
                return jsonify({"ok": True, "funcao_id": f.id})
            return jsonify({"ok": True, "funcoes": [
                {"id": f.id, "nome": f.nome,
                 "valor_diaria": float(f.valor_diaria) if f.valor_diaria else None}
                for f in s.scalars(select(Funcao).where(Funcao.ativo.is_(True))
                                   .order_by(Funcao.nome)).all()]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/dc", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="lancar_dc")
def api_dc():
    from app.apps.erp.core.pessoal import VERBAS, criar_despesa, listar
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                from app.apps.erp.core.pessoal import categorias_de_pessoal
                return jsonify({"ok": True, "despesas": listar(s, usuario),
                                "categorias": categorias_de_pessoal(s),
                                "verbas": [{"chave": k, "rotulo": v[0],
                                            "exige_qtd": v[2]}
                                           for k, v in VERBAS.items()]})
            d = criar_despesa(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "despesa": {
                "id": d.id, "numero": d.numero, "status": d.status,
                "total": float(d.valor_total)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em DC")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/dc/criticar", methods=["POST"])
@login_obrigatorio
@permissao("lancar_dc")
def api_dc_criticar():
    from app.apps.erp.core.pessoal import criticar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            return jsonify({"ok": True, "critica": criticar(
                s, d.get("itens") or [], obra_id=d.get("obra_id"),
                despesa_id=d.get("despesa_id"))})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criticar DC")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/dc/<int:despesa_id>")
@login_obrigatorio
@permissao("ver_pessoal")
def api_dc_detalhe(despesa_id: int):
    from app.apps.erp.core.pessoal import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_despesa_no_escopo
            exigir_despesa_no_escopo(s, _usuario_logado(s), despesa_id)
            return jsonify({"ok": True, "despesa": detalhar(s, despesa_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/dc/<int:despesa_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("ver_pessoal")
def api_dc_acao(despesa_id: int, acao: str):
    from app.apps.erp.core.pessoal import aprovar, devolver, gerar_titulo, planilha_pagamento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_despesa_no_escopo
            exigir_despesa_no_escopo(s, _usuario_logado(s), despesa_id)
            usuario = _usuario_logado(s)
            if acao == "aprovar":
                rel = aprovar(s, despesa_id, usuario, d.get("observacao", ""))
            elif acao == "devolver":
                rel = devolver(s, despesa_id, d.get("motivo", ""), usuario)
            elif acao == "gerar-titulo":
                rel = gerar_titulo(s, despesa_id, d, usuario)
            elif acao == "planilha":
                rel = planilha_pagamento(s, despesa_id)
            else:
                return jsonify({"ok": False, "erro": f"Ação inválida: {acao}"}), 400
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na ação da DC")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/historico")
@login_obrigatorio
@permissao("ver_pessoal")
def api_historico_colaborador(colaborador_id: int):
    """Tudo que já se pagou a esta pessoa: DC, títulos diretos e rateados."""
    from app.apps.erp.core.pessoal import historico
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
            exigir_colaborador_no_escopo(s, _usuario_logado(s), colaborador_id)
            return jsonify({"ok": True, "historico": historico(
                s, colaborador_id, meses=request.args.get("meses", type=int) or 24)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no histórico do colaborador")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/adicionar-sps", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_lote_adicionar_sps(lote_id: int):
    """Cola-se a lista de SPs e as parcelas em aberto entram no lote."""
    from app.apps.erp.core.pagamentos.lotes import (
        adicionar_parcelas, extrair_ids_sp, parcelas_por_sp,
    )
    d = request.get_json(silent=True) or {}
    numeros = extrair_ids_sp(d.get("texto") or "")
    if not numeros:
        return jsonify({"ok": False,
                        "erro": "Nenhum número de SP reconhecido no texto."}), 400
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            achado = parcelas_por_sp(s, numeros)
            ids = [p["parcela_id"] for p in achado.get("parcelas", [])
                   if p.get("status") != "PAGA"]
            rel = adicionar_parcelas(s, lote_id, ids, usuario) if ids else {"adicionadas": 0}
            s.commit()
        return jsonify({"ok": True, "resultado": {
            "adicionadas": rel.get("adicionadas", len(ids)),
            "ja_estavam": rel.get("ja_estavam", []),
            "nao_encontradas": achado.get("nao_encontradas", [])}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao adicionar SPs ao lote")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>", methods=["DELETE"])
@login_obrigatorio
@permissao("pagar")
def api_excluir_lote(lote_id: int):
    """Apaga o agrupamento. As SPs continuam intactas — o lote não é um estado
    do título, é só uma forma de olhar para um conjunto delas."""
    from sqlalchemy import select
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import Lote, LoteParcela
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            lote = s.get(Lote, lote_id)
            if lote is None:
                return jsonify({"ok": False, "erro": "Lote não encontrado."}), 404
            nome = lote.nome
            quantas = len(s.scalars(select(LoteParcela).where(
                LoteParcela.lote_id == lote_id)).all())
            for lp in s.scalars(select(LoteParcela).where(
                    LoteParcela.lote_id == lote_id)).all():
                s.delete(lp)
            s.delete(lote)
            registrar_evento(s, "lote", lote_id, "EXCLUIDO",
                             {"nome": nome, "parcelas": quantas}, usuario.id)
            s.commit()
        return jsonify({"ok": True, "nome": nome})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao excluir lote")
        return jsonify({"ok": False, "erro": str(e)}), 500


@bp.route("/erp/health")
@permissao_publica("health check do Render, sem dado de negocio")
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
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "modulo": "erp", "erro": str(e)}), 503
