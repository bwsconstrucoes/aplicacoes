# -*- coding: utf-8 -*-
"""
Painel Financeiro OMIE — rotas.

Blueprint do monorepo, com endereco proprio em /painel. Nada de banco acontece
no import: se o Postgres estiver fora do ar ou a DATABASE_URL faltar, o painel
falha na primeira tela aberta e os outros 14 modulos sobem normalmente.

Envvars: PAINEL_SENHA (entrada), PAINEL_SECRET (chamada do agendador),
DATABASE_URL, OMIE_BWS_APP_KEY, OMIE_BWS_APP_SECRET, GOOGLE_CREDENTIALS_BASE64,
PAINEL_SHEET_PROJETOS.
"""
from __future__ import annotations

import logging

from flask import (
    Blueprint, jsonify, redirect, render_template, request, session, url_for,
)

from . import auth

logger = logging.getLogger("painel.web")

bp = Blueprint("painel", __name__,
               url_prefix="/painel",
               template_folder="templates",
               static_folder="static",
               # relativo ao url_prefix: o arquivo sai em /painel/static/painel.css
               static_url_path="/static")


@bp.before_request
def _porta_de_entrada():
    """Padrao NEGAR: rota que nao esteja na lista de publicas exige login."""
    return auth.exigir_login()


@bp.context_processor
def _ajudantes_de_template():
    """Formatadores disponiveis nas telas do painel. Escopo de blueprint: nao
    vazam para os templates do ERP nem dos outros modulos."""
    def brl(v):
        try:
            v = float(v)
        except (TypeError, ValueError):
            return "—"
        if v != v:                      # NaN
            return "—"
        texto = f"{abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return ("−R$ " if v < 0 else "R$ ") + texto

    def classe_valor(v):
        """Vermelho para negativo, verde para positivo — para nao ser preciso
        procurar o sinal no meio do numero."""
        try:
            v = float(v)
        except (TypeError, ValueError):
            return ""
        if v < -0.005:
            return "v-neg"
        if v > 0.005:
            return "v-pos"
        return ""

    return {"brl": brl, "classe_valor": classe_valor}


@bp.errorhandler(Exception)
def _erro(e):
    """Erro nao previsto vira mensagem legivel, nunca pagina branca."""
    logger.exception("Painel: erro na rota %s", request.path)
    if request.path.startswith("/painel/api/"):
        return jsonify({"ok": False, "erro": str(e)}), 500
    return render_template("painel_erro.html", erro=str(e)), 500


# ---------------------------------------------------------------------------
# Entrada e saida
# ---------------------------------------------------------------------------
@bp.route("/entrar", methods=["GET", "POST"])
def entrar():
    if request.method == "GET":
        if auth.esta_logado():
            return redirect(url_for("painel.visao_geral"))
        return render_template("painel_login.html", erro=None,
                               sem_senha=not auth.senha_configurada())

    if not auth.senha_configurada():
        return render_template("painel_login.html", sem_senha=True,
                               erro="O painel ainda não tem senha configurada."), 403
    if not auth.senha_confere(request.form.get("senha", "")):
        logger.warning("Painel: tentativa de entrada com senha errada.")
        return render_template("painel_login.html", sem_senha=False,
                               erro="Senha incorreta."), 401
    auth.entrar_na_sessao()
    return redirect(url_for("painel.visao_geral"))


@bp.route("/sair")
def sair():
    auth.sair_da_sessao()
    return redirect(url_for("painel.entrar"))


@bp.route("/saude")
def saude():
    """Checagem de servico. Nao devolve dado financeiro nenhum, so diz que o
    modulo esta de pe e se as variaveis essenciais existem."""
    import os
    return jsonify({
        "ok": True,
        "modulo": "painel",
        "senha_configurada": bool(auth.senha_configurada()),
        "segredo_configurado": bool(os.getenv("PAINEL_SECRET", "").strip()),
        "banco_configurado": bool(os.getenv("DATABASE_URL", "").strip()),
    })


# ---------------------------------------------------------------------------
# Filtros da barra lateral
# ---------------------------------------------------------------------------
def _filtros_do_pedido():
    from .consultas import Filtros
    anos = [int(a) for a in request.args.getlist("ano") if str(a).strip().isdigit()]
    return Filtros(anos=anos,
                   projetos=[p for p in request.args.getlist("projeto") if p],
                   departamentos=[o for o in request.args.getlist("obra") if o],
                   excluir_trf=request.args.get("trf") != "1")


def _contexto_comum(aba: str):
    """O que toda tela precisa: abas, filtros disponiveis e a data da base."""
    from . import consultas
    return {
        "aba_ativa": aba,
        "abas": [("visao", "Visão Geral", "painel.visao_geral"),
                 ("dre", "DRE", "painel.dre"),
                 ("config", "Configurações", "painel.configuracoes")],
        "opcoes": consultas.opcoes_de_filtro(),
        "atualizacao": consultas.atualizado_em(),
        "selecao": {
            "anos": request.args.getlist("ano"),
            "projetos": request.args.getlist("projeto"),
            "obras": request.args.getlist("obra"),
            "trf": request.args.get("trf") == "1",
        },
    }


# ---------------------------------------------------------------------------
# Telas
# ---------------------------------------------------------------------------
@bp.route("/")
def visao_geral():
    from . import consultas
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    from . import graficos
    f = _filtros_do_pedido()
    por_ano = consultas.dre_por_ano(f)
    caixa_ano = consultas.caixa_por_ano(f)
    return render_template(
        "painel_visao.html",
        **_contexto_comum("visao"),
        chips=f.resumo(),
        dre=consultas.resultado_dre(f),
        caixa=consultas.caixa(f),
        grafico_dre=graficos.barras_agrupadas(
            por_ano,
            [("receita", "b-receita", "Receita líquida"),
             ("despesa", "b-despesa", "Despesa")],
            campo_linha="resultado"),
        grafico_caixa=graficos.barras_agrupadas(
            caixa_ano,
            [("valor", "b-caixa", "Geração de caixa no ano")],
            campo_linha="acumulado"),
    )


@bp.route("/dre")
def dre():
    from . import consultas, graficos
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    quebra = "categoria" if request.args.get("quebra") == "categoria" else "grupo"
    visao = request.args.get("visao", "comprometido")
    return render_template(
        "painel_dre.html",
        **_contexto_comum("dre"),
        chips=f.resumo(),
        quebra=quebra,
        visao=visao,
        linhas=consultas.dre_linhas(f),
        despesas=graficos.proporcoes(
            consultas.despesas_por(f, quebra=quebra, visao=visao)),
        receita_obra=consultas.receita_por_obra(f),
        credores=consultas.top_credores(f),
    )


@bp.route("/configuracoes")
def configuracoes():
    from . import migracoes_runner, tarefas
    estado_migracoes = migracoes_runner.listar_estado()
    contexto = {"aba_ativa": "config",
                "abas": [("visao", "Visão Geral", "painel.visao_geral"),
                         ("dre", "DRE", "painel.dre"),
                         ("config", "Configurações", "painel.configuracoes")]}
    # Se as tabelas ainda nao existem, nem tenta consultar a base.
    if estado_migracoes["pendentes"]:
        atualizacao, vazia = None, True
    else:
        from . import consultas
        atualizacao, vazia = consultas.atualizado_em(), consultas.base_vazia()
    return render_template(
        "painel_config.html", **contexto,
        migracoes=estado_migracoes,
        atualizacao=atualizacao,
        base_vazia=vazia,
        primeira=request.args.get("primeira") == "1",
        modos=tarefas.MODOS,
        sincronizacao=tarefas.estado(),
    )


# ---------------------------------------------------------------------------
# Acoes
# ---------------------------------------------------------------------------
@bp.route("/api/migracoes/aplicar", methods=["POST"])
def aplicar_migracoes():
    from . import migracoes_runner
    resultado = migracoes_runner.aplicar_pendentes()
    return jsonify({"ok": resultado["erro"] is None, **resultado})


@bp.route("/api/sincronizar", methods=["POST"])
def sincronizar():
    """Dispara a atualizacao da base.

    Aceita duas identidades: a sessao (o botao na tela) ou o segredo do modulo
    no corpo do pedido (o agendador da madrugada). Sem uma das duas, recusa.
    """
    from . import tarefas
    dados = request.get_json(silent=True) or {}
    modo = dados.get("modo") or request.form.get("modo") or "rapida"

    if auth.esta_logado():
        disparo = "manual"
    elif auth.segredo_de_maquina_confere(dados.get("secret", "")):
        disparo = "agendado"
    else:
        return jsonify({"ok": False, "erro": "não autorizado"}), 401

    return jsonify(tarefas.disparar(modo, disparo))


@bp.route("/api/estado")
def estado():
    """A tela de Configuracoes consulta este endereco para acompanhar a
    atualizacao sem recarregar a pagina."""
    from . import tarefas
    from . import consultas
    return jsonify({"ok": True, "sincronizacao": tarefas.estado(),
                    "ultima": _serializar(consultas.atualizado_em())})


def _serializar(d):
    if not d:
        return None
    saida = dict(d)
    for chave in ("inicio", "fim"):
        if saida.get(chave) is not None:
            saida[chave] = saida[chave].strftime("%d/%m/%Y %H:%M")
    return saida
