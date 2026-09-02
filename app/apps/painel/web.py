# -*- coding: utf-8 -*-
"""
Painel Financeiro OMIE — rotas.

Blueprint do monorepo, com endereco proprio em /painel. Nada de banco acontece
no import: se o Postgres estiver fora do ar ou a DATABASE_URL faltar, o painel
falha na primeira tela aberta e os outros 14 modulos sobem normalmente.

Envvars: PAINEL_SENHA (entrada), PAINEL_SECRET (chamada do agendador),
DATABASE_URL, OMIE_KEY, OMIE_SECRET, GOOGLE_CREDENTIALS_BASE64,
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
    """Checagem de servico. Nao devolve dado financeiro nenhum: so diz que o
    modulo esta de pe, se as variaveis essenciais existem e QUAL VERSAO esta
    rodando.

    A versao esta aqui por um motivo pratico: depois de publicar uma correcao,
    a unica forma de saber se ela ja subiu era clicar e ver se o erro se repete.
    O Render entrega o commit publicado em RENDER_GIT_COMMIT; com ele da para
    conferir antes de tentar de novo."""
    import os
    from .horario import agora
    commit = (os.getenv("RENDER_GIT_COMMIT", "")
              or os.getenv("SOURCE_VERSION", "")).strip()
    return jsonify({
        "ok": True,
        "modulo": "painel",
        "versao": commit[:8] if commit else "desconhecida (fora do Render)",
        "agora": agora().strftime("%d/%m/%Y às %H:%M:%S") + " (Brasília)",
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


ABAS = [
    ("visao", "Visão Geral", "painel.visao_geral"),
    ("dre", "DRE", "painel.dre"),
    ("receita", "Receita de Obra", "painel.receita"),
    ("fluxo", "Fluxo de Caixa", "painel.fluxo"),
    ("obras", "Resultado por Obra", "painel.obras"),
    ("execucao", "Comprometido × Executado", "painel.execucao"),
    ("caixa", "Necessidade de Caixa", "painel.necessidade_caixa"),
    ("prestacao", "Prestação de Contas", "painel.prestacao_contas"),
    ("config", "Configurações", "painel.configuracoes"),
]


def _nivel_do_pedido() -> str:
    """Agrupar por projeto (o conjunto) ou por obra (o departamento no OMIE)."""
    return "obra" if request.args.get("nivel") == "obra" else "projeto"


def _contexto_comum(aba: str):
    """O que toda tela precisa: abas, filtros disponiveis e a data da base."""
    from . import consultas
    return {
        "aba_ativa": aba,
        "abas": ABAS,
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


@bp.route("/receita")
def receita():
    """Receita de obra agrupada por medicao."""
    from . import consultas
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    visao = request.args.get("visao", "todas")
    if visao not in ("todas", "a_receber", "quitadas"):
        visao = "todas"
    itens = consultas.medicoes(f, visao=visao)
    return render_template(
        "painel_receita.html",
        **_contexto_comum("receita"),
        chips=f.resumo(), visao=visao, itens=itens,
        total=consultas.total_das_medicoes(f, visao=visao),
        outras=consultas.outras_receitas(f),
        # a coluna da medicao so e preenchida na atualizacao seguinte a migracao
        sem_medicao=bool(itens) and all(i["medicao"] == "(sem medição)" for i in itens),
    )


@bp.route("/receita/<path:medicao>")
def receita_detalhe(medicao):
    """Os recebimentos que quitaram uma medicao, um a um."""
    from . import consultas
    recebimentos = consultas.recebimentos_da_medicao(medicao)
    total = {campo: sum(float(r[campo] or 0) for r in recebimentos)
             for campo in ("valor", "juros", "multa", "desconto")}
    return render_template(
        "painel_medicao.html",
        **_contexto_comum("receita"),
        medicao=medicao, recebimentos=recebimentos, total=total,
    )


@bp.route("/fluxo")
def fluxo():
    """Entradas e saidas mes a mes, e o caixa acumulado."""
    from . import consultas, graficos
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    meses = consultas.caixa_por_mes(f)
    total = {
        "entradas": sum(m["entradas"] for m in meses),
        "saidas": sum(m["saidas"] for m in meses),
        "liquido": sum(m["liquido"] for m in meses),
        "acumulado": meses[-1]["acumulado"] if meses else 0.0,
    }
    # Com muitos meses o grafico vira uma parede de barras finas demais para
    # ler. Os ultimos 36 (tres anos) cobrem a leitura util; a tabela abaixo
    # continua mostrando tudo.
    recentes = meses[-36:]
    return render_template(
        "painel_fluxo.html",
        **_contexto_comum("fluxo"),
        chips=f.resumo(),
        meses=meses,
        total=total,
        grafico=graficos.barras_agrupadas(
            recentes,
            [("entradas", "b-receita", "Entradas"),
             ("saidas", "b-despesa", "Saídas")],
            campo_rotulo="rotulo", campo_linha="acumulado"),
    )


@bp.route("/obras")
def obras():
    """Receita liquida, despesa e resultado por projeto ou por obra."""
    from . import consultas, graficos
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    nivel = _nivel_do_pedido()
    medida = "executado" if request.args.get("medida") == "executado" else "comprometido"
    itens = consultas.resultado_por(f, nivel=nivel, medida=medida)
    return render_template(
        "painel_obras.html",
        **_contexto_comum("obras"),
        chips=f.resumo(), nivel=nivel, medida=medida,
        itens=graficos.proporcoes(itens, campo="resultado"),
    )


@bp.route("/execucao")
def execucao():
    """Quanto de cada obra ja foi executado e quanto ainda falta."""
    from . import consultas
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    nivel = _nivel_do_pedido()
    tipo = "receber" if request.args.get("tipo") == "receber" else "pagar"
    return render_template(
        "painel_execucao.html",
        **_contexto_comum("execucao"),
        chips=f.resumo(), nivel=nivel, tipo=tipo,
        itens=consultas.comprometido_vs_executado(f, nivel=nivel, tipo=tipo),
    )


@bp.route("/necessidade-caixa")
def necessidade_caixa():
    """Simulacao: um conjunto de obras se paga sozinho, ou alguem segurou?

    Nao usa os filtros da barra lateral de proposito — ver o modulo `simulacao`.
    """
    import json
    from . import consultas, graficos, simulacao

    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))

    linhas_obra = consultas.caixa_mensal_por_obra()
    financeiro = consultas.financeiro_mensal()
    mapa = consultas.obra_para_projeto()

    # O conjunto A vem na propria URL, como `a=obra:CASA|100`, para a simulacao
    # poder ser guardada nos favoritos e reaberta exatamente igual.
    escolhas, escolhidos = [], []
    for bruto in request.args.getlist("a"):
        item, _, percentual = bruto.partition("|")
        if not item:
            continue
        escolhas.append((item, percentual or 100))
        escolhidos.append({"item": item, "pct": percentual or "100"})

    try:
        saldo = float(request.args.get("saldo") or 0)
    except ValueError:
        saldo = 0.0
    incluir = request.args.get("aportes", "1") == "1"
    desde = _mes_do_pedido(request.args.get("desde"))

    resultado = simulacao.simular(linhas_obra, financeiro, escolhas, mapa,
                                  saldo_inicial=saldo, incluir_aportes=incluir,
                                  desde=desde)

    opcoes = ([{"valor": f"projeto:{p}", "rotulo": f"Projeto — {p}"}
               for p in sorted({v for v in mapa.values() if v})]
              + [{"valor": f"obra:{o}", "rotulo": f"Obra — {o}"}
                 for o in resultado.get("obras", [])])

    linhas = resultado.get("linhas") or []
    negativos = [l for l in linhas if l["conjunto_a"] < -0.5]
    pior = min(negativos, key=lambda l: l["conjunto_a"]) if negativos else None
    resumo = {
        "meses_negativos": len(negativos),
        "pior_a": pior["conjunto_a"] if pior else 0.0,
        "pior_mes": pior["rotulo"] if pior else "",
        "resto_no_pior": pior["resto"] if pior else 0.0,
        "empresa_no_pior": pior["empresa"] if pior else 0.0,
    }

    return render_template(
        "painel_caixa.html",
        **_contexto_comum("caixa"),
        simulacao=resultado,
        resumo=resumo,
        saldo=int(saldo),
        incluir_aportes=incluir,
        desde=request.args.get("desde", ""),
        meses_disponiveis=[l["rotulo"] for l in linhas],
        opcoes_json=json.dumps(opcoes, ensure_ascii=False),
        escolhidos_json=json.dumps(escolhidos, ensure_ascii=False),
        grafico=graficos.linhas_com_barras(
            linhas,
            [("conjunto_a", "var(--vermelho)", "Conjunto A"),
             ("resto", "var(--verde)", "Resto das obras"),
             ("empresa", "var(--azul)", "Empresa inteira"),
             ("caixa_reconstruido", "var(--ambar)", "Caixa reconstruído")],
            barras=("emprestimo_tomado_no_mes", "b-emprestimo", "Empréstimo tomado")),
    )


def _mes_do_pedido(texto):
    """'09/2025' -> a data do primeiro dia daquele mes."""
    import datetime as dt
    if not texto or "/" not in texto:
        return None
    mes, _, ano = texto.partition("/")
    try:
        return dt.date(int(ano), int(mes), 1)
    except ValueError:
        return None


def _calcular_prestacao(medida: str):
    """Junta a base do banco com a configuracao e roda a conta inteira.

    Fica aqui, e nao dentro da rota, porque a tela de resultado e a de
    configuracao precisam do mesmo calculo — e porque assim da para chamar de
    um teste sem passar por HTTP."""
    from . import consultas, prestacao, prestacao_dados

    config = prestacao_dados.config()
    apuracao = consultas.apuracao_por_obra_mes(medida)
    obras = prestacao.classificar_obras(apuracao, config)
    pessoal = consultas.custo_de_pessoal_por_obra_mes(config["grupo_pessoal"], medida)
    admin = consultas.despesa_administrativa(
        [config["depto_admin_matriz"], config["depto_admin_filial"]], medida)

    rateio = prestacao.calcular_rateio(admin, pessoal, obras,
                                       prestacao_dados.regras(), config)
    apurado = prestacao.apurar(apuracao, obras, rateio["alocacoes"])
    por_projeto = prestacao.totalizar_por_projeto(apurado)
    quotas = prestacao.quotas_por_socio(por_projeto,
                                        prestacao_dados.participacoes(), config)
    ajustes = prestacao_dados.ajustes()
    return {
        "config": config, "obras": obras, "rateio": rateio,
        "apurado": apurado, "por_projeto": por_projeto,
        "quotas": quotas, "ajustes": ajustes,
        "posicao": prestacao.posicao_dos_socios(quotas, ajustes),
    }


@bp.route("/prestacao")
def prestacao_contas():
    """Quanto do resultado de cada obra cabe a cada socio."""
    from . import consultas, prestacao_dados
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    medida = "executado" if request.args.get("medida") == "executado" else "comprometido"
    calculo = _calcular_prestacao(medida)

    projetos = sorted(calculo["por_projeto"].items(),
                      key=lambda kv: -kv[1]["resultado"])
    sobras = calculo["rateio"]["sobras"]
    return render_template(
        "painel_prestacao.html",
        **_contexto_comum("prestacao"),
        medida=medida,
        posicao=calculo["posicao"],
        quotas=calculo["quotas"],
        projetos=projetos,
        obras=calculo["obras"],
        sobras=sorted(sobras, key=lambda s: abs(s["valor"]), reverse=True)[:40],
        total_sobras=sum(s["valor"] for s in sobras),
        rateio_total=sum(calculo["rateio"]["alocacoes"].values()),
        tem_participacoes=bool(prestacao_dados.participacoes()),
    )


@bp.route("/prestacao/parametros", methods=["GET", "POST"])
def prestacao_parametros():
    """Socios, participacoes, regras de rateio e ajustes."""
    from . import consultas, prestacao_dados

    if request.method == "POST":
        _aplicar_mudanca_da_prestacao(prestacao_dados, request.form)
        return redirect(url_for("painel.prestacao_parametros",
                                aba=request.form.get("aba", "socios")))

    listas = consultas.grupos_e_categorias() if not consultas.base_vazia() else         {"grupos": [], "categorias": []}
    return render_template(
        "painel_prestacao_config.html",
        **_contexto_comum("prestacao"),
        aba=request.args.get("aba", "socios"),
        config=prestacao_dados.config(),
        socios=prestacao_dados.socios(),
        socios_ativos=prestacao_dados.socios(apenas_ativos=True),
        participacoes=prestacao_dados.participacoes(),
        regras=prestacao_dados.regras(),
        ajustes=prestacao_dados.ajustes(),
        tipos_ajuste=prestacao_dados.TIPOS_AJUSTE,
        escopos=prestacao_dados.ESCOPOS,
        projetos=consultas.opcoes_de_filtro()["projetos"] if not consultas.base_vazia() else [],
        **listas,
    )


def _aplicar_mudanca_da_prestacao(dados, form):
    """Uma acao por envio; o formulario diz qual em `acao`."""
    acao = form.get("acao", "")
    if acao == "socio":
        dados.salvar_socio(form["nome"], form.get("tipo", "Interno"),
                           form.get("socio_id") or None)
    elif acao == "desativar_socio":
        dados.desativar_socio(form["socio_id"])
    elif acao == "participacao":
        dados.salvar_participacao(form["projeto"], form["socio_id"], form["pct"])
    elif acao == "apagar_participacao":
        dados.apagar_participacao(form["participacao_id"])
    elif acao == "regra":
        dados.salvar_regra({
            "nome": form["nome"], "depto": form["depto"],
            "todas": form.get("todas") == "1",
            "grupos": form.getlist("grupos"),
            "categorias": form.getlist("categorias"),
            "pct": form.get("pct") or 100,
            "escopo": form.get("escopo", "AMBAS"),
            "mes_ini": form.get("mes_ini", ""), "mes_fim": form.get("mes_fim", ""),
            "ativo": form.get("ativo") == "1",
        }, form.get("regra_id") or None)
    elif acao == "apagar_regra":
        dados.apagar_regra(form["regra_id"])
    elif acao == "ajuste":
        dados.salvar_ajuste(form["socio_id"], form["tipo"], form["valor"],
                            form.get("data", ""), form.get("projeto", ""),
                            form.get("descricao", ""))
    elif acao == "apagar_ajuste":
        dados.apagar_ajuste(form["ajuste_id"])
    elif acao == "config":
        for chave in ("projeto_matriz", "depto_admin_matriz", "depto_admin_filial",
                      "grupo_pessoal", "taxa_adm_pct", "residual"):
            if chave in form:
                dados.salvar_config(chave, form[chave])


@bp.route("/configuracoes")
def configuracoes():
    from . import migracoes_runner, tarefas
    estado_migracoes = migracoes_runner.listar_estado()
    contexto = {"aba_ativa": "config", "abas": ABAS}
    # Se as tabelas ainda nao existem, nem tenta consultar a base.
    if estado_migracoes["pendentes"]:
        atualizacao, vazia, etapas = None, True, []
    else:
        from . import consultas
        atualizacao, vazia = consultas.atualizado_em(), consultas.base_vazia()
        etapas = consultas.etapas_da_carga()
    return render_template(
        "painel_config.html", **contexto,
        migracoes=estado_migracoes,
        atualizacao=atualizacao,
        base_vazia=vazia,
        primeira=request.args.get("primeira") == "1",
        etapas=etapas,
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


@bp.route("/api/esquecer-etapas", methods=["POST"])
def esquecer_etapas():
    """Faz a proxima carga comecar do zero, em vez de retomar.

    Botao separado de proposito: retomar e o certo em quase todo caso, e
    refazer horas de download deve ser uma decisao, nao um acidente."""
    from .db import conexao
    from .sync.espelho import limpar_etapas
    with conexao() as conn:
        limpar_etapas(conn)
    logger.info("Painel: etapas da carga esquecidas a pedido do usuário.")
    return jsonify({"ok": True})


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


@bp.route("/baixar/<assunto>")
def baixar(assunto):
    """Leva os numeros da tela para uma planilha.

    Respeita os MESMOS filtros da tela: a URL de download so acrescenta o
    assunto, o resto dos parametros e o que ja estava na barra de endereco.
    Assim o arquivo baixado e exatamente o que estava na tela — nunca "a base
    inteira" quando a pessoa estava vendo um recorte."""
    from flask import Response
    from . import consultas, exportar

    f = _filtros_do_pedido()
    if assunto == "dre":
        linhas = exportar.linhas_do_dre(consultas.dre_linhas(f))
    elif assunto == "despesas":
        linhas = consultas.despesas_por(
            f, quebra=request.args.get("quebra", "grupo"),
            visao=request.args.get("visao", "comprometido"), limite=500)
    elif assunto == "medicoes":
        linhas = consultas.medicoes(f, visao=request.args.get("visao", "todas"),
                                    limite=5000)
    elif assunto == "fluxo":
        linhas = consultas.caixa_por_mes(f)
    elif assunto == "obras":
        linhas = consultas.resultado_por(f, nivel=_nivel_do_pedido(),
                                         medida=request.args.get("medida", "comprometido"),
                                         limite=500)
    elif assunto == "execucao":
        linhas = consultas.comprometido_vs_executado(
            f, nivel=_nivel_do_pedido(),
            tipo=request.args.get("tipo", "pagar"), limite=500)
    elif assunto == "credores":
        linhas = consultas.top_credores(f, limite=500)
    elif assunto in ("quotas", "posicao"):
        calculo = _calcular_prestacao(request.args.get("medida", "comprometido"))
        linhas = calculo["quotas"] if assunto == "quotas" else calculo["posicao"]
    else:
        return jsonify({"ok": False, "erro": f"Não sei exportar '{assunto}'."}), 404

    conteudo = exportar.montar_csv(exportar.COLUNAS[assunto], linhas)
    nome = exportar.nome_do_arquivo(assunto)
    return Response(conteudo, mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nome}"'})


# Um .db de prestacao de contas tem dezenas de KB. O teto e generoso e mesmo
# assim protege a memoria do servico contra um envio absurdo.
MAX_BYTES_IMPORTACAO = 20 * 1024 * 1024


@bp.route("/api/importar-prestacao", methods=["POST"])
def importar_prestacao():
    """Recebe o prestacao_contas.db que rodava no computador do dono.

    E a unica configuracao do painel que ninguem consegue regenerar — socios,
    percentuais, regras de rateio e ajustes. Nao foi versionada no Git de
    proposito: sao nomes de socios e divisao de lucro.
    """
    import os as _os
    import tempfile

    arquivo = request.files.get("arquivo")
    if not arquivo or not arquivo.filename:
        return jsonify({"ok": False, "erro": "Nenhum arquivo enviado."}), 400
    if not arquivo.filename.lower().endswith(".db"):
        return jsonify({"ok": False,
                        "erro": "Envie o arquivo prestacao_contas.db."}), 400

    # grava em disco antes de abrir: o sqlite le de arquivo, e assim o conteudo
    # nao precisa ficar inteiro na memoria do servico
    caminho = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as destino:
            caminho = destino.name
            lidos = 0
            while True:
                pedaco = arquivo.stream.read(256 * 1024)
                if not pedaco:
                    break
                lidos += len(pedaco)
                if lidos > MAX_BYTES_IMPORTACAO:
                    raise ValueError("Arquivo grande demais para ser uma "
                                     "configuração de prestação de contas.")
                destino.write(pedaco)

        from . import prestacao_dados
        contagem = prestacao_dados.importar_do_arquivo_local(caminho)
        logger.info("Painel: configuração da prestação importada (%s).", contagem)
        return jsonify({"ok": True, "importado": contagem})
    except Exception as e:  # noqa: BLE001 — a mensagem vai para a tela
        logger.exception("Painel: falha ao importar a prestação")
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        if caminho:
            try:
                _os.unlink(caminho)
            except OSError:
                pass


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
    from .horario import texto
    saida = dict(d)
    for chave in ("inicio", "fim"):
        if saida.get(chave) is not None:
            saida[chave] = texto(saida[chave])
    return saida
