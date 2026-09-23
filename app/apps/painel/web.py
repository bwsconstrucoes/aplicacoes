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

import os
import re
import time
import logging
import datetime as dt

from flask import (
    Blueprint, g, jsonify, redirect, render_template, request, session, url_for,
)

from . import auth

logger = logging.getLogger("painel.web")

# Hora em que este processo nasceu. Serve de versao quando nao ha commit
# publicado (rodando no PC), para o navegador nao guardar arquivo velho.
import time as _time
_NASCIMENTO = _time.time()

bp = Blueprint("painel", __name__,
               url_prefix="/painel",
               template_folder="templates",
               static_folder="static",
               # relativo ao url_prefix: o arquivo sai em /painel/static/painel.css
               static_url_path="/static")


@bp.before_request
def _porta_de_entrada():
    """Padrao NEGAR: rota que nao esteja na lista de publicas exige login."""
    from . import db
    db.iniciar_medicao()
    g._painel_comeco = time.perf_counter()
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

    def brl_curto(v):
        """O valor em poucas letras — "7 mil", "1,2 mi" — para caber num
        quadradinho do calendario na tela do celular."""
        try:
            v = float(v)
        except (TypeError, ValueError):
            return "—"
        sinal = "−" if v < 0 else "+"
        v = abs(v)
        if v >= 1_000_000:
            texto = f"{v / 1_000_000:.1f}".replace(".", ",") + " mi"
        elif v >= 1_000:
            texto = (f"{v / 1_000:.1f}".replace(".", ",").replace(",0", "")
                     + " mil")
        else:
            texto = f"{v:.0f}"
        return sinal + texto

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

    def link_analitico(**extras):
        """Link para o Analitico preservando os filtros da tela atual.

        Clicar num grupo de despesa tem de levar para os lancamentos DAQUELE
        grupo, dentro do mesmo recorte de ano, projeto e obra — e nao para a
        base inteira."""
        args = {c: request.args.getlist(c) for c in
                ("ano", "projeto", "obra", "conta") if request.args.getlist(c)}
        if request.args.get("trf"):
            args["trf"] = "1"
        args.update({k: v for k, v in extras.items() if v})
        return url_for("painel.analitico", **args)

    def pagina_link(numero):
        """Link para outra pagina da tela ATUAL, guardando todos os filtros.

        Antes apontava para o Analitico com o nome escrito. Com o Extrato, em
        21/09/2026, isso jogaria quem virasse a pagina do extrato para dentro do
        analitico — e o filtro de conta iria junto, mostrando uma tela que nao e
        a que a pessoa estava lendo."""
        args = request.args.to_dict(flat=False)
        args["pagina"] = [str(numero)]
        return url_for(request.endpoint or "painel.analitico", **args)

    def link_baixar(assunto, **extras):
        """Link de download levando os filtros da tela — TODOS eles.

        Existe por causa de um defeito que custou confianca no numero: os botoes
        faziam `url_for(..., **request.args)`, e `**` sobre um MultiDict pega
        UMA valor por chave. Ano, projeto e obra sao multiplos: quem filtrava
        tres obras via 481 lancamentos na tela e baixava um arquivo com so as
        de uma obra — sem aviso nenhum de que o resto tinha ficado para tras.

        `to_dict(flat=False)` mantem a lista inteira, e o `url_for` repete o
        parametro para cada valor. E o mesmo cuidado que o `pagina_link` acima
        ja tomava."""
        args = request.args.to_dict(flat=False)
        args.pop("pagina", None)      # o arquivo e a selecao inteira, nao a pagina
        for chave, valor in extras.items():
            args[chave] = [valor]
        return url_for("painel.baixar", assunto=assunto, **args)

    def estatico(nome):
        """Endereço do arquivo estático COM a versão publicada no fim.

        É o que deixa o navegador guardar por um ano sem risco de ficar com
        arquivo velho: publicar muda o endereço."""
        return url_for("painel.static", filename=nome) + "?v=" + _versao_publicada()

    def com_filtros(rota, **extras):
        """Link de tela levando os filtros da barra lateral junto.

        Estar filtrado numa obra e trocar de tela nao pode jogar o filtro fora:
        quem esta olhando uma obra no DRE quer os lancamentos DAQUELA obra no
        Analitico, nao a base inteira.

        Vale para as ABAS do topo e para qualquer link que va de uma tela a
        outra — inclusive entrar num detalhe e voltar. Em 17/09/2026 o dono
        topou com isso: estava na obra Mercado Barbalha na Receita de Obra,
        entrou numa medicao, clicou em "Voltar as medicoes", e a obra tinha
        sumido do filtro. Quem esta analisando UMA obra nao quer voltar para a
        base inteira.

        `extras` leva o que for do proprio link (a medicao, a medida), sem
        atropelar o filtro."""
        args = {c: request.args.getlist(c) for c in
                ("ano", "projeto", "obra") if request.args.getlist(c)}
        if request.args.get("trf"):
            args["trf"] = "1"
        args.update(extras)
        return url_for(rota, **args)

    def cronometro():
        """Quanto a tela levou e quanto disso foi o banco. Sem isto, 'esta
        lento' nao tem por onde comecar."""
        from . import db
        consultas, segundos_banco = db.medicao()
        comeco = getattr(g, "_painel_comeco", None)
        total = (time.perf_counter() - comeco) if comeco else 0.0
        return {"consultas": consultas,
                "ms_banco": int(segundos_banco * 1000),
                "ms_total": int(total * 1000)}

    return {"brl": brl, "brl_curto": brl_curto, "classe_valor": classe_valor,
            "com_filtros": com_filtros,
            "cronometro": cronometro, "link_baixar": link_baixar,
            "link_analitico": link_analitico, "pagina_link": pagina_link,
            "estatico": estatico}


@bp.after_request
def _guardar_estaticos(resposta):
    """Manda o navegador GUARDAR estilo e scripts, em vez de perguntar sempre.

    Só vale para os arquivos servidos por este blueprint, e só quando o endereço
    traz a versão (`?v=`) — que é o que garante que publicar derruba o que está
    guardado. Sem a versão, nada muda: é o caso de alguém abrir o arquivo na mão.

    Tela do painel NUNCA é guardada: ela mostra número, e número velho é pior
    que tela lenta."""
    if request.endpoint == "painel.static" and request.args.get("v"):
        resposta.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return resposta


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

    senha = request.form.get("senha", "")
    login = (request.form.get("usuario") or "").strip()

    # A SENHA MESTRE DECIDE PRIMEIRO, esteja o campo de usuario preenchido ou
    # nao. Isto NAO e conveniencia: e o conserto de um jeito de trancar o dono
    # para fora do proprio painel.
    #
    # Ate 22/09/2026 o caminho era escolhido pelo campo de usuario estar vazio.
    # So que a tela de login passou a ter DOIS campos, e o navegador do dono
    # tinha a senha dele guardada de quando havia um so: ao abrir a pagina, o
    # gerenciador preenchia o campo novo sozinho, sem ele ver. O pedido saia
    # com usuario preenchido, caia no caminho da pessoa presa a obra, e a
    # resposta era "usuario ou senha incorretos" — com a senha certa digitada.
    #
    # Checar a senha mestre antes nao afrouxa nada: quem a conhece JA e o
    # administrador. O que se perde e so a chance de o navegador escolher o
    # caminho por ele.
    if senha and auth.senha_confere(senha):
        auth.entrar_na_sessao()
        return redirect(url_for("painel.visao_geral"))

    if login:
        from . import usuarios
        pessoa = usuarios.buscar(login)
        if not pessoa or not usuarios.senha_confere(pessoa, senha):
            logger.warning("Painel: entrada recusada para o usuário %r.", login)
            # a mesma resposta para usuário que não existe e senha errada: dizer
            # qual dos dois falhou entrega metade da resposta a quem tenta.
            # O aviso do navegador vem junto porque foi exatamente isso que
            # trancou o dono para fora — e ele não tinha como adivinhar.
            return render_template(
                "painel_login.html", sem_senha=False,
                erro="Usuário ou senha incorretos. Se você é o dono, apague o "
                     "que estiver no campo Usuário — o navegador às vezes "
                     "preenche sozinho — e digite só a senha."), 401
        if not pessoa.get("obras") or not pessoa.get("telas"):
            logger.warning("Painel: %s entrou sem obra ou sem tela liberada.", login)
            return render_template(
                "painel_login.html", sem_senha=False,
                erro="Seu acesso ainda não tem obra ou tela liberada. "
                     "Fale com o responsável pelo painel."), 403
        auth.entrar_na_sessao(usuario_id=pessoa["id"])
        usuarios.marcar_acesso(pessoa["id"])
        primeira = next((a for a in ABAS if a[0] in set(pessoa["telas"])), None)
        return redirect(url_for(primeira[2]) if primeira
                        else url_for("painel.entrar"))

    # Chegou aqui: sem usuario, e a senha ja foi comparada com a mestre la em
    # cima e nao bateu.
    if not auth.senha_configurada():
        return render_template("painel_login.html", sem_senha=True,
                               erro="O painel ainda não tem senha configurada."), 403
    logger.warning("Painel: tentativa de entrada com senha errada.")
    return render_template("painel_login.html", sem_senha=False,
                           erro="Senha incorreta."), 401


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
# ---------------------------------------------------------------------------
# Arquivos estaticos: o navegador guardava NADA
# ---------------------------------------------------------------------------
# Ate 17/09/2026 o painel mandava `Cache-Control: no-cache` no estilo e nos
# scripts. Isso faz o navegador PERGUNTAR AO SERVIDOR por cada um deles em toda
# tela aberta — tres idas e vindas extras por tela, so para ouvir "nao mudou".
#
# Custa duas coisas, e a segunda e pior: o tempo da viagem ate o Render, e a
# cota do `--max-requests` do gunicorn, que reinicia o servico a cada ~150
# requisicoes. Contando os estaticos, o reinicio chegava tres vezes mais cedo —
# e enquanto ele acontece, com um worker so, TODO MUNDO espera.
#
# Agora o navegador guarda por um ano, e o endereco carrega a versao publicada.
# Publicar muda o endereco, o arquivo novo desce na hora, e nada fica velho.

def _versao_publicada() -> str:
    """O commit no ar, ou a hora do processo quando nao houver (PC do dono)."""
    import os
    commit = (os.getenv("RENDER_GIT_COMMIT", "")
              or os.getenv("SOURCE_VERSION", "")).strip()
    return (commit[:12] if commit else str(int(_NASCIMENTO)))


def _filtros_do_pedido():
    """Os filtros da barra lateral — JA PRESOS ao escopo de quem esta olhando.

    ESTE E O UNICO LUGAR onde se decide o que cada tela enxerga, e e de
    proposito. Toda tela, todo download e todo grafico passam por aqui. Amarrar
    o escopo num lugar so significa que NENHUMA TELA PODE ESQUECER — que e
    exatamente como esse tipo de coisa vaza quando se protege tela por tela.

    A regra que mais importa esta no `or`: se a pessoa nao escolheu obra
    nenhuma (ou escolheu uma que nao e dela), o filtro vira A LISTA DELA — e
    nunca "sem filtro". Sem isso, bastaria tirar a obra do endereco para ver a
    empresa inteira."""
    from . import auth
    from .consultas import Filtros
    anos = [int(a) for a in request.args.getlist("ano") if str(a).strip().isdigit()]
    obras = [o for o in request.args.getlist("obra") if o]

    contas = [c for c in request.args.getlist("conta") if c]

    pessoa = auth.usuario_da_sessao()
    if pessoa is not None:
        permitidas = set(pessoa.get("obras") or [])
        obras = [o for o in obras if o in permitidas] or sorted(permitidas)
        # lista vazia aqui seria "todas": um cadastro pela metade nao pode
        # virar acesso total. O guard ja barra antes, e isto e a segunda tranca.
        if not obras:
            obras = ["\u0000nenhuma obra liberada"]
        # A CONTA segue a mesma regra, e pelo mesmo motivo. So que aqui ha uma
        # diferenca: conta liberada e opcional — quem nao tem nenhuma marcada
        # simplesmente nao usa o Extrato, e as outras telas seguem normais.
        contas_ok = set(pessoa.get("contas") or [])
        if contas_ok:
            contas = [c for c in contas if c in contas_ok] or sorted(contas_ok)

    return Filtros(anos=anos,
                   projetos=[p for p in request.args.getlist("projeto") if p],
                   departamentos=obras, contas=contas,
                   excluir_trf=request.args.get("trf") != "1")


ABAS = [
    ("visao", "Visão Geral", "painel.visao_geral"),
    ("dre", "DRE", "painel.dre"),
    ("analitico", "Despesas Analítico", "painel.analitico"),
    ("receita", "Receita de Obra", "painel.receita"),
    ("fluxo", "Fluxo de Caixa", "painel.fluxo"),
    ("calendario", "Calendário", "painel.calendario"),
    ("obras", "Resultado por Obra", "painel.obras"),
    ("execucao", "Comprometido × Executado", "painel.execucao"),
    ("caixa", "Necessidade de Caixa", "painel.necessidade_caixa"),
    ("extrato", "Extrato de Conta", "painel.extrato"),
    ("prestacao", "Prestação de Contas", "painel.prestacao_contas"),
    ("config", "Configurações", "painel.configuracoes"),
]


_DATA_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _faixa_de_data() -> tuple[str, str]:
    """A faixa de data do Analitico, so se vier no formato do calendario.

    Texto que nao seja uma data e descartado em silencio em vez de virar erro:
    a faixa e conveniencia, e uma tela que quebra porque alguem colou algo
    estranho na barra de endereco e pior do que uma faixa ignorada.
    """
    de = (request.args.get("de") or "").strip()
    ate = (request.args.get("ate") or "").strip()
    de = de if _DATA_ISO.match(de) else ""
    ate = ate if _DATA_ISO.match(ate) else ""
    # invertida (fim antes do comeco) devolveria vazio sem explicar por que
    if de and ate and de > ate:
        de, ate = ate, de
    return de, ate


def _nivel_do_pedido() -> str:
    """Agrupar por projeto (o conjunto) ou por obra (o departamento no OMIE)."""
    return "obra" if request.args.get("nivel") == "obra" else "projeto"


def _abas_visiveis():
    """As abas do topo que a pessoa pode abrir.

    Mostrar uma aba que responde "nao encontrado" ao ser clicada seria pior que
    nao mostrar: a pessoa acha que o sistema esta com defeito."""
    from . import auth
    pessoa = auth.usuario_da_sessao()
    if pessoa is None:
        return ABAS
    liberadas = set(pessoa.get("telas") or [])
    return [aba for aba in ABAS if aba[0] in liberadas]


def _opcoes_no_escopo():
    """As listas da barra lateral, sem os nomes das obras que nao sao dela.

    Mostrar a lista inteira entregaria o nome de todas as obras da empresa a
    quem so pode ver uma — informacao que ele nao teria de outro jeito."""
    from . import auth, consultas
    opcoes = consultas.opcoes_de_filtro()
    pessoa = auth.usuario_da_sessao()
    if pessoa is None:
        return opcoes
    permitidas = set(pessoa.get("obras") or [])
    contas_ok = set(pessoa.get("contas") or [])
    return dict(opcoes,
                obras=[o for o in opcoes["obras"] if o in permitidas],
                contas=[c for c in opcoes.get("contas", []) if c in contas_ok])


def _contexto_comum(aba: str):
    """O que toda tela precisa: abas, filtros disponiveis e a data da base."""
    from . import consultas
    return {
        "aba_ativa": aba,
        "abas": _abas_visiveis(),
        # Quem esta preso a obra nao pode baixar o relatorio COMPLETO (ele
        # cruza todas as telas): os botoes do DRE precisam saber para oferecer
        # so o DRE. Sem isto o botao levava a "pagina nao encontrada" —
        # 22/09/2026, visto pelo dono no acesso de um usuario.
        "administrador": auth.e_administrador(),
        "opcoes": _opcoes_no_escopo(),
        "atualizacao": consultas.atualizado_em(),
        "selecao": {
            "anos": request.args.getlist("ano"),
            "projetos": request.args.getlist("projeto"),
            "obras": request.args.getlist("obra"),
            "contas": request.args.getlist("conta"),
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


# As abas de dentro do DRE, na mesma ordem da tela antiga (que usava
# `st.tabs(["Despesas", "Receitas", "Top Credores"])`, com os aportes logo
# abaixo). Cada uma e um link: so as consultas da aba aberta rodam, e a aba
# escolhida entra na URL junto com os filtros.
BLOCOS_DRE = [
    ("despesas", "Despesas"),
    ("receitas", "Receitas"),
    ("credores", "Top Credores"),
    ("aportes", "Aportes e dividendos"),
]


@bp.route("/dre")
def dre():
    """O DRE como a tela antiga o mostrava: os cinco numeros, a tabela linha a
    linha, o Fluxo Financeiro mensal e, embaixo, as abas de detalhe."""
    from . import consultas, graficos
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    quebra = "categoria" if request.args.get("quebra") == "categoria" else "grupo"
    visao = request.args.get("visao", "comprometido")
    medida = "comprometido" if request.args.get("medida") == "comprometido" else "executado"
    bloco = request.args.get("bloco", "despesas")
    if bloco not in dict(BLOCOS_DRE):
        bloco = "despesas"
    mensal = consultas.resultado_mensal(f, medida=medida)

    # So o que a aba aberta precisa. Carregar os quatro blocos de uma vez seria
    # multiplicar por quatro o custo de abrir o DRE, para mostrar um deles.
    extra = {}
    if bloco == "despesas":
        extra["despesas"] = graficos.proporcoes(
            consultas.despesas_por(f, quebra=quebra, visao=visao))
    elif bloco == "receitas":
        extra["medicoes"] = consultas.medicoes(f, limite=60)
        extra["total_receita"] = consultas.total_das_medicoes(f)
        extra["outras"] = consultas.outras_receitas(f)
    elif bloco == "credores":
        extra["credores"] = consultas.top_credores(f, limite=40)
    else:
        aportes = consultas.aportes(f)
        divisao = consultas.resultado_dividendos(f)
        extra["aportes"] = aportes
        # A comparacao com a base inteira e a cascata de conferencia SAIRAM
        # daqui em 22/09/2026 — o dono: "essa informacao nao deveria aparecer
        # aqui, tem que colocar em Configuracoes". Estao la, nas conferencias.
        extra["divisao"] = divisao
        extra["hipotese"] = consultas.hipotese_de_distribuicao(
            aportes["por_socio"], divisao["disponivel"])
        # o dinheiro da obra com os socios dentro: receitas e aportes de um
        # lado, despesas, devolucoes e dividendos do outro (dono, 23/09/2026)
        extra["caixa_socios"] = consultas.caixa_com_socios(f)

    return render_template(
        "painel_dre.html",
        **_contexto_comum("dre"),
        chips=f.resumo(),
        quebra=quebra, visao=visao, medida=medida,
        bloco=bloco, blocos=BLOCOS_DRE,
        dre=consultas.dre_linhas(f),
        # os ultimos 36 meses no grafico; alem disso as barras ficam ilegiveis
        grafico_mensal=graficos.barras_agrupadas(
            mensal[-36:],
            [("receita", "b-receita", "Receita"), ("despesa", "b-despesa", "Despesa")],
            campo_rotulo="rotulo", campo_linha="acumulado"),
        **extra,
    )


@bp.route("/analitico")
def analitico():
    """Despesas lancamento a lancamento — de onde veio cada numero do DRE."""
    from . import consultas
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    grupo = request.args.get("grupo", "")
    categoria = request.args.get("categoria", "")
    credor = request.args.get("credor", "")
    busca = (request.args.get("busca") or "").strip()
    visao = request.args.get("visao", "comprometido")
    ordem = request.args.get("ordem", "valor")
    de, ate = _faixa_de_data()
    base = request.args.get("base", "movimento")
    if base not in consultas.BASES_DE_DATA:
        base = "movimento"
    try:
        pagina = int(request.args.get("pagina") or 1)
    except ValueError:
        pagina = 1
    return render_template(
        "painel_analitico.html",
        **_contexto_comum("analitico"),
        chips=f.resumo(),
        grupo=grupo, categoria=categoria, credor=credor, busca=busca,
        visao=visao, ordem=ordem, de=de, ate=ate, base=base,
        bases=consultas.BASES_DE_DATA,
        # as colunas novas nascem vazias; a tela avisa em vez de mostrar
        # travessão e deixar parecer que o dado não existe
        duas_datas=consultas.duas_datas_prontas(),
        opcoes_analitico=consultas.opcoes_do_analitico(f),
        dados=consultas.analitico_despesas(
            f, grupo=grupo, categoria=categoria, credor=credor, busca=busca,
            visao=visao, ordem=ordem, de=de, ate=ate, base=base, pagina=pagina),
    )


@bp.route("/extrato")
def extrato():
    """O que entrou e o que saiu de uma conta corrente, por data.

    E o Analitico da CONTA, no lugar do da obra — para dar para conferir lado a
    lado com o extrato do proprio OMIE. So o que virou dinheiro, sem coluna de
    vencimento, e SEM o corte do DRE: tarifa e rendimento aparecem aqui, porque
    aconteceram na conta."""
    from . import consultas
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f = _filtros_do_pedido()
    de, ate = _faixa_de_data()
    try:
        pagina = int(request.args.get("pagina") or 1)
    except ValueError:
        pagina = 1
    # Duas visões da mesma tela: os lançamentos da conta, ou as transferências
    # entre contas com OS DOIS LADOS juntos. Ficam aqui, e não em outra aba,
    # porque a pergunta é a mesma — "o que andou nesta conta?" — e os filtros
    # são os mesmos.
    visao = request.args.get("visao", "lancamentos")
    if visao not in ("lancamentos", "transferencias"):
        visao = "lancamentos"

    pessoa = auth.usuario_da_sessao()
    transferencias = None
    if visao == "transferencias":
        transferencias = consultas.transferencias_entre_contas(
            f, de=de, ate=ate, destino=request.args.get("destino", ""),
            contas_visiveis=(pessoa.get("contas") if pessoa else None))

    return render_template(
        "painel_extrato.html",
        **_contexto_comum("extrato"),
        chips=f.resumo(),
        de=de, ate=ate, visao=visao,
        destino=request.args.get("destino", ""),
        busca=(request.args.get("busca") or "").strip(),
        categoria=request.args.get("categoria", ""),
        ordem=request.args.get("ordem", "data"),
        categorias=consultas.categorias_do_extrato(f),
        transferencias=transferencias,
        dados=consultas.extrato_da_conta(
            f, busca=(request.args.get("busca") or "").strip(),
            categoria=request.args.get("categoria", ""),
            de=de, ate=ate, ordem=request.args.get("ordem", "data"),
            pagina=pagina) if visao == "lancamentos" else None,
    )


# ---------------------------------------------------------------------------
# O Calendario — o caixa dia a dia
# ---------------------------------------------------------------------------
# Pedido do dono em 22/09/2026: um calendario grande, do mes, com o resumo de
# cada dia (pago e recebido), filtros como os do Analitico, botoes para o mes
# anterior e o seguinte, KPIs no alto, e o detalhe do dia ao clicar — com o
# link do Pipefy.
_MES_ISO = re.compile(r"^\d{4}-\d{2}$")

NOMES_DOS_MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho",
                   "julho", "agosto", "setembro", "outubro", "novembro",
                   "dezembro"]


def _mes_do_calendario() -> tuple[int, int]:
    """O mes pedido (AAAA-MM) — ou o mes de hoje, quando nao veio ou veio torto."""
    import datetime as _dt
    texto = (request.args.get("mes") or "").strip()
    if _MES_ISO.match(texto):
        ano, mes = int(texto[:4]), int(texto[5:7])
        if 1 <= mes <= 12 and 2000 <= ano <= 2100:
            return ano, mes
    hoje = _dt.date.today()
    return hoje.year, hoje.month


def _filtros_do_calendario():
    """Os filtros da tela do calendario: os da barra lateral, SEM o ano.

    O mes do calendario ja diz o ano; deixar o filtro de ano da barra lateral
    valer aqui faria "ir para o mes seguinte" atravessar a virada do ano e
    encontrar um calendario vazio, sem explicacao."""
    f = _filtros_do_pedido()
    f.anos = []
    return f, {
        "tipo": request.args.get("tipo", "") if request.args.get("tipo", "")
        in consultas_tipos_do_calendario() else "",
        "grupo": request.args.get("grupo", ""),
        "categoria": request.args.get("categoria", ""),
        "busca": (request.args.get("busca") or "").strip(),
    }


def consultas_tipos_do_calendario():
    from . import consultas
    return consultas.TIPOS_DO_CALENDARIO


def _semanas_do_mes(ano: int, mes: int) -> list[list]:
    """As semanas do mes, de domingo a sabado, com zero fora do mes."""
    import calendar as _cal
    return _cal.Calendar(firstweekday=6).monthdayscalendar(ano, mes)


@bp.route("/calendario")
def calendario():
    """O caixa dia a dia, num mes — para bater o olho e ver a evolucao."""
    import datetime as _dt
    from . import consultas
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    f, proprios = _filtros_do_calendario()
    ano, mes = _mes_do_calendario()
    dados = consultas.calendario_do_mes(f, ano, mes, **proprios)
    inicio = dados["inicio"]
    anterior = (inicio - _dt.timedelta(days=1)).replace(day=1)
    seguinte = (inicio + _dt.timedelta(days=32)).replace(day=1)
    return render_template(
        "painel_calendario.html",
        **_contexto_comum("calendario"),
        chips=[c for c in f.resumo() if not c.startswith("Ano:")],
        ano=ano, mes=mes, mes_iso=f"{ano:04d}-{mes:02d}",
        titulo_do_mes=f"{NOMES_DOS_MESES[mes - 1]} de {ano}",
        anterior=anterior.strftime("%Y-%m"), seguinte=seguinte.strftime("%Y-%m"),
        semanas=_semanas_do_mes(ano, mes),
        hoje=_dt.date.today(),
        dados=dados,
        tipos=consultas.TIPOS_DO_CALENDARIO,
        opcoes_analitico=consultas.opcoes_do_analitico(f),
        **proprios,
    )


@bp.route("/calendario/dia")
def calendario_dia():
    """O detalhe de um dia, para a janela que abre ao clicar nele."""
    from . import consultas
    dia = (request.args.get("dia") or "").strip()
    if not _DATA_ISO.match(dia):
        return jsonify({"ok": False, "erro": "Dia inválido."}), 400
    f, proprios = _filtros_do_calendario()
    linhas = consultas.lancamentos_do_dia(f, dia, **proprios)
    for l in linhas:
        l["data"] = l["data"].isoformat() if l.get("data") else ""
    entradas = sum(l["valor"] for l in linhas if l["valor"] > 0)
    saidas = sum(l["valor"] for l in linhas if l["valor"] < 0)
    return jsonify({"ok": True, "dia": dia, "linhas": linhas,
                    "quantos": len(linhas), "entradas": entradas,
                    "saidas": saidas, "liquido": entradas + saidas})


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
    # Os titulos que compoem a medicao — o que da para conferir no OMIE.
    titulos = consultas.titulos_da_medicao(medicao)
    return render_template(
        "painel_medicao.html",
        **_contexto_comum("receita"),
        medicao=medicao, recebimentos=recebimentos, total=total,
        titulos=titulos,
        bruto_dos_titulos=sum(t["bruto"] for t in titulos),
    )


@bp.route("/receita/titulo/<int:codigo>")
def receita_titulo(codigo):
    """UM titulo de receita: os dados dele e os recebimentos que o quitaram.

    Desde 22/09/2026 a linha da Receita de Obra e o titulo, nao a medicao —
    o dono: "nao fica legal agrupado, confunde, tem que separar mesmo os
    titulos". O detalhe acompanha."""
    from . import auth, consultas
    titulo = consultas.titulo_da_receita(codigo, _filtros_do_pedido())
    if titulo is None:
        # fora do recorte ou inexistente: "nao encontrado", nunca "nao pode"
        return auth.nao_encontrado()
    recebimentos = consultas.recebimentos_do_titulo(codigo)
    total = {campo: sum(float(r[campo] or 0) for r in recebimentos)
             for campo in ("valor", "juros", "multa", "desconto")}
    cabecalho = (f"Título {codigo}"
                 + (f" · doc. {titulo['documento']}" if titulo.get("documento") else ""))
    return render_template(
        "painel_medicao.html",
        **_contexto_comum("receita"),
        medicao=cabecalho, recebimentos=recebimentos, total=total,
        titulos=[titulo], bruto_dos_titulos=titulo["bruto"],
        rotulo_da_medicao=titulo.get("medicao") or "",
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


# As chaves da configuracao que decidem O QUE e lido do banco. Duas
# configuracoes que concordam nelas leem exatamente a mesma coisa — e e isso que
# autoriza a tela de cenarios a ler uma vez so e calcular duas.
CHAVES_QUE_MUDAM_A_LEITURA = ("grupo_pessoal", "depto_admin_matriz",
                              "depto_admin_filial")


def _base_da_prestacao(config, medida: str) -> dict:
    """As tres leituras do banco de que a prestacao inteira depende.

    Separadas do calculo porque a tela de cenarios roda a conta DUAS vezes — com
    as regras gravadas e com as do cenario — sobre exatamente os mesmos dados.
    Ler duas vezes seria varrer as 185 mil linhas do fato em dobro para obter o
    mesmo resultado."""
    from . import consultas, prestacao, prestacao_dados
    base = {
        "apuracao": consultas.apuracao_por_obra_mes(medida),
        "pessoal": consultas.custo_de_pessoal_por_obra_mes(
            config["grupo_pessoal"], medida),
        "admin": consultas.despesa_administrativa(
            [config["depto_admin_matriz"], config["depto_admin_filial"]], medida),
        # O caixa e sempre CAIXA, mesmo quando a apuracao esta em comprometido:
        # a regua dos juros e "quem estava sem dinheiro no mes", e conta a
        # pagar nao tira dinheiro de ninguem. Vem com o mes em texto para
        # casar com o resto da prestacao, que usa 'AAAA-MM'.
        "caixa": [(mes.strftime("%Y-%m"), obra, valor)
                  for mes, obra, valor in consultas.caixa_mensal_por_obra()],
    }
    # O que o dono tirou da analise em Parametros sai AQUI, antes de qualquer
    # conta — e por isso vale para a prestacao, para os cenarios de rateio e
    # para todo cenario novo. Pedido dele em 22/09/2026: "na parte de
    # configuracoes da prestacao de conta, pra eu poder eliminar projetos
    # e/ou obras dessa analise".
    fora = _obras_fora(config, prestacao_dados.fora_da_analise(config),
                       base["apuracao"])
    base = prestacao.sem_as_obras(base, fora)
    base["excluidas"] = sorted(fora)
    return base


def _obras_fora(config, itens, apuracao) -> set:
    """As obras que uma lista "obra:/projeto:" tira da analise.

    A estrutura (matriz e filial) nunca e obra: nao entra no que se pode
    tirar, mesmo que esteja no projeto tirado."""
    from . import consultas, prestacao
    if not itens:
        return set()
    deptos = _deptos_administrativos(config)
    return prestacao.obras_fora_da_analise(
        itens, consultas.obra_para_projeto(),
        {(l.get("obra") or "").strip() for l in apuracao} - set(deptos))


def _apurar_com(regras, config, base) -> dict:
    """A parte da prestacao que depende das REGRAS de rateio: quais obras
    existem, quanto de custo administrativo cai em cada uma e o que sobra.

    E exatamente o que a tela de cenarios compara. Socios, quotas e ajustes nao
    entram aqui porque nao mudam com a regra de rateio — e porque cada um deles
    e uma leitura a mais no banco, que seria feita duas vezes para nada."""
    from . import prestacao

    obras = prestacao.classificar_obras(base["apuracao"], config)

    # Os juros de emprestimo saem do bolo da estrutura ANTES do rateio: eles
    # tem regua propria — quem demandou caixa. Deixa-los no bolo faria a obra
    # que se paga sozinha pagar juro so por ter gente, e faria o juro ser
    # contado duas vezes.
    por_deficit = str(config.get("juros_por_deficit", "1")) == "1"
    categoria_juros = (config.get("categoria_juros")
                       or prestacao.CATEGORIA_JUROS_PADRAO)
    admin, juros_por_mes = ((prestacao.separar_juros(base["admin"], categoria_juros))
                            if por_deficit else (base["admin"], {}))

    rateio = prestacao.calcular_rateio(admin, base["pessoal"], obras,
                                       regras, config)
    if por_deficit:
        juros = prestacao.alocar_juros_por_deficit(
            base.get("caixa", []), juros_por_mes, obras,
            rateio_recebido=rateio["alocacoes"],
            sem_deficit=str(config.get("juros_sem_deficit", "sobra")))
    else:
        juros = {"alocacoes": {}, "sobras": [], "memoria": []}

    return {"obras": obras, "rateio": rateio, "juros": juros,
            "apurado": prestacao.apurar(base["apuracao"], obras,
                                        rateio["alocacoes"],
                                        juros["alocacoes"])}


def _calcular_prestacao(medida: str, regras=None, config=None, base=None):
    """Junta a base do banco com a configuracao e roda a conta inteira.

    Fica aqui, e nao dentro da rota, porque a tela de resultado e a de
    configuracao precisam do mesmo calculo — e porque assim da para chamar de
    um teste sem passar por HTTP.

    `regras` e `config` existem para o CENARIO: com eles a mesma conta roda com
    parametros que NAO estao gravados, sem tocar no banco. Sem eles, roda com o
    que esta gravado — que e o caso de todas as outras telas."""
    from . import prestacao, prestacao_dados

    config = config if config is not None else prestacao_dados.config()
    regras = regras if regras is not None else prestacao_dados.regras()
    base = base if base is not None else _base_da_prestacao(config, medida)

    conta = _apurar_com(regras, config, base)
    obras, rateio, apurado = conta["obras"], conta["rateio"], conta["apurado"]
    juros = conta["juros"]
    por_projeto = prestacao.totalizar_por_projeto(apurado)
    quotas = prestacao.quotas_por_socio(por_projeto,
                                        prestacao_dados.participacoes(), config)
    ajustes = prestacao_dados.ajustes()
    return {
        "config": config, "obras": obras, "rateio": rateio, "juros": juros,
        "apurado": apurado, "por_projeto": por_projeto,
        "quotas": quotas, "ajustes": ajustes,
        "posicao": prestacao.posicao_dos_socios(quotas, ajustes),
        "excluidas": list(base.get("excluidas") or []),
        "por_obra": prestacao.totalizar_por_obra(apurado),
    }


@bp.route("/prestacao")
def prestacao_contas():
    """Quanto do resultado de cada obra cabe a cada socio."""
    from . import consultas, prestacao_dados
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    medida = "executado" if request.args.get("medida") == "executado" else "comprometido"
    calculo = _calcular_prestacao(medida)

    from . import prestacao

    projetos = sorted(calculo["por_projeto"].items(),
                      key=lambda kv: -kv[1]["resultado"])
    sem_projeto = sorted(o for o, info in calculo["obras"].items()
                         if not (info.get("projeto") or "").strip()
                         and o != consultas.SEM_OBRA)
    # As duas sobras aparecem juntas: para quem le, "custo que nao coube em
    # obra nenhuma" e uma coisa so, venha ela da estrutura ou do juro.
    sobras = calculo["rateio"]["sobras"] + calculo["juros"]["sobras"]
    juros_por_obra = prestacao.total_por_obra(calculo["juros"]["alocacoes"])
    memoria = [l for l in calculo["juros"]["memoria"] if abs(l["juros"]) > 0.005]
    # A mesma conta, obra a obra — pior primeiro, com o projeto ao lado. E o
    # que o dono pediu em 22/09/2026: "tem a visao de projetos, mas da para
    # ver por obra tambem?"
    por_obra = sorted(calculo["por_obra"].items(),
                      key=lambda kv: kv[1]["resultado"])
    return render_template(
        "painel_prestacao.html",
        **_contexto_comum("prestacao"),
        medida=medida,
        posicao=calculo["posicao"],
        quotas=calculo["quotas"],
        projetos=projetos,
        por_obra=por_obra,
        excluidas=calculo["excluidas"],
        obras=calculo["obras"],
        sobras=sorted(sobras, key=lambda s: abs(s["valor"]), reverse=True)[:40],
        total_sobras=sum(s["valor"] for s in sobras),
        rateio_total=sum(calculo["rateio"]["alocacoes"].values()),
        juros_por_obra=juros_por_obra,
        juros_total=sum(calculo["juros"]["alocacoes"].values()),
        juros_memoria=memoria[-36:],
        juros_por_deficit=str(calculo["config"].get("juros_por_deficit", "1")) == "1",
        tem_participacoes=bool(prestacao_dados.participacoes()),
        sem_projeto=sem_projeto, nao_apropriado=consultas.SEM_OBRA,
    )


# ---------------------------------------------------------------------------
# O CENARIO da prestacao de contas — tudo num ambiente so
# ---------------------------------------------------------------------------
# Pedido do dono em 22/09/2026, depois de olhar o que existia espalhado em
# quatro telas: "eu queria trazer para uma tela de prestacao de conta, onde
# dentro dela eu vou nomear os parceiros, os socios, os percentuais, vou definir
# se vai ser baseado na mao de obra ou no faturamento, quais contas da matriz eu
# vou dividir, em quais percentuais (...) e importantissimo, auditavel".
#
# O cenario e o objeto: trocar de cenario troca o resultado inteiro, e o
# anterior continua intacto para comparar.
def _deptos_administrativos(config) -> list[str]:
    """Os departamentos que sao a ESTRUTURA — o bolo a repartir, nao destino."""
    return [d for d in (config.get("depto_admin_matriz"),
                        config.get("depto_admin_filial")) if d]


def _calcular_cenario(cenario: dict) -> dict:
    """A conta inteira de um cenario, do banco ate a quota de cada pessoa.

    Fica aqui, e nao na rota, porque a tela de montagem e a de resultado
    precisam do mesmo calculo — e porque assim da para chamar de um teste sem
    passar por HTTP."""
    from . import cenarios, consultas, prestacao, prestacao_dados

    config = prestacao_dados.config()
    medida = cenario.get("medida") or "comprometido"
    deptos = _deptos_administrativos(config)

    apuracao = consultas.apuracao_por_obra_mes(medida)
    # O que o cenario deixa FORA da analise sai aqui, antes de qualquer conta:
    # nao vira obra, nao recebe estrutura nem juros, nao pesa na regua.
    itens_fora = cenario.get("excluidas")
    if itens_fora is None:
        itens_fora = cenarios.excluidas(cenario["id"])
    # A lista de Parametros vale para todo cenario; a do cenario soma-se a
    # ela. Assim "tirar o Ceara de tudo" se faz uma vez, e "e se sem a obra
    # X?" continua sendo coisa de um cenario so.
    itens_gerais = prestacao_dados.fora_da_analise(config)
    fora = _obras_fora(config, list(itens_gerais) + list(itens_fora or []),
                       apuracao)
    if fora:
        apuracao = [l for l in apuracao if (l.get("obra") or "").strip() not in fora]
    obras = prestacao.classificar_obras(apuracao, config)
    admin = consultas.despesa_administrativa(deptos, medida)
    # Duas coisas que o dono viu misturadas em 22/09/2026 e que sao diferentes:
    # "(nao apropriado)" e lancamento SEM OBRA; "(sem projeto)" e obra que
    # existe mas nao esta ligada a projeto nenhum no OMIE — dado a corrigir la.
    # A tela lista as obras sem projeto pelo nome, para dar para corrigir.
    sem_projeto = sorted(o for o, info in obras.items()
                         if not (info.get("projeto") or "").strip()
                         and o != consultas.SEM_OBRA)

    pesos = cenario.get("pesos") or cenarios.pesos(cenario["id"])
    excecoes = consultas.lancamentos_administrativos_por_codigo(
        deptos, list((pesos.get("lancamento") or {}).keys()), medida)

    # Os juros saem do bolo ANTES do rateio: regua propria, a do deficit.
    categoria_juros = (config.get("categoria_juros")
                       or prestacao.CATEGORIA_JUROS_PADRAO)
    por_deficit = bool(cenario.get("juros_por_deficit", 1))
    admin_sem_juros, juros_por_mes = (
        prestacao.separar_juros(admin, categoria_juros) if por_deficit
        else (admin, {}))

    # A regua de quem recebe: mao de obra ou faturamento, como o dono escolheu.
    driver = (consultas.receita_por_obra_mes(medida)
              if cenario.get("criterio") == "faturamento"
              else consultas.custo_de_pessoal_por_obra_mes(
                  config["grupo_pessoal"], medida))
    if fora:
        driver = [d for d in driver if d[1] not in fora]

    rateio = prestacao.calcular_rateio_do_cenario(
        admin_sem_juros, excecoes, driver, obras, pesos,
        pct_padrao=float(cenario.get("pct_padrao", 100) or 0),
        janela=str(cenario.get("janela", "1")))

    # O caixa e lido UMA vez e devolvido: a tela de resultado desenha a vida de
    # uma obra com ele, e reler significaria varrer o fato inteiro de novo.
    caixa = [(mes.strftime("%Y-%m"), obra, valor)
             for mes, obra, valor in consultas.caixa_mensal_por_obra()
             if obra not in fora]
    if por_deficit:
        juros = prestacao.alocar_juros_por_deficit(
            caixa, juros_por_mes, obras,
            rateio_recebido=rateio["alocacoes"],
            sem_deficit=str(cenario.get("juros_sem_deficit", "sobra")))
    else:
        juros = {"alocacoes": {}, "sobras": [], "memoria": []}

    apurado = prestacao.apurar(apuracao, obras, rateio["alocacoes"],
                               juros["alocacoes"])
    por_obra = prestacao.totalizar_por_obra(apurado)
    participacoes = cenario.get("participacoes") or cenarios.participacoes(cenario["id"])
    quotas = prestacao.quotas_por_obra(por_obra, participacoes,
                                       float(cenario.get("taxa_adm_pct", 0) or 0))
    ajustes = prestacao_dados.ajustes()
    return {
        "cenario": cenario, "obras": obras, "rateio": rateio, "juros": juros,
        "apurado": apurado, "por_obra": por_obra, "quotas": quotas,
        "driver": driver, "caixa": caixa, "excluidas": sorted(fora),
        "excluidas_gerais": list(itens_gerais),
        "sem_projeto": sem_projeto,
        "tem_nao_apropriado": consultas.SEM_OBRA in obras,
        "posicao": prestacao.posicao_dos_socios(quotas, ajustes),
        "sobras": rateio["sobras"] + juros["sobras"],
    }


def _contas_da_matriz(config, cenario, medida: str) -> dict:
    """A arvore do que a matriz gastou, com o percentual de cada linha — e
    quanto ENTRA no bolo e quanto FICA DE FORA, em dinheiro, em cada nivel.

    Sai do MESMO agregado que a conta usa — nao ha consulta extra para a
    arvore, e o que a tela mostra e por construcao o que entra no calculo.
    Ordenada do maior para o menor: quem esta configurando quer ver primeiro
    o que move o resultado.

    Pedido do dono em 23/09/2026: "so informar um grupo fica complicado para
    quem quiser analisar depois — o que esta dentro daquele grupo? Eu preciso
    ir adentrando ate o lancamento e estipular o que entra e o que nao entra".
    Por isso cada linha diz o valor que entra, considerando as excecoes
    marcadas la embaixo, e a tela lista tudo que foi marcado num lugar so.

    Devolve {"contas": [...], "resumo": {...}, "marcacoes": [...]}."""
    from . import consultas, prestacao
    pesos = cenario.get("pesos") or {}
    padrao = float(cenario.get("pct_padrao", 100) or 0)
    deptos = _deptos_administrativos(config)
    # As categorias de juro seguem a regua do deficit, nao a do rateio: a
    # arvore as mostra, mas nao como coisa que se divide por aqui.
    juros = (prestacao.categorias_de_juros(config.get("categoria_juros")
                                           or prestacao.CATEGORIA_JUROS_PADRAO)
             if bool(cenario.get("juros_por_deficit", 1)) else set())

    # Os lancamentos marcados um a um saem do balde da categoria e entram com
    # o percentual proprio — exatamente como na conta (calcular_rateio_do_cenario)
    marcados = pesos.get("lancamento") or {}
    excecoes = consultas.lancamentos_administrativos_por_codigo(
        deptos, list(marcados.keys()), medida) if marcados else []
    por_categoria: dict[tuple, list] = {}
    for e in excecoes:
        chave = ((e.get("grupo") or "").strip() or "(sem grupo)",
                 (e.get("categoria") or "").strip() or "(sem categoria)")
        por_categoria.setdefault(chave, []).append(e)

    grupos: dict[str, dict] = {}
    for linha in consultas.despesa_administrativa(deptos, medida):
        nome = linha.get("grupo") or "(sem grupo)"
        cat = linha.get("categoria") or "(sem categoria)"
        g = grupos.setdefault(nome, {"grupo": nome, "valor": 0.0, "categorias": {}})
        g["valor"] += linha["valor"]
        c = g["categorias"].setdefault(cat, {"categoria": cat, "valor": 0.0})
        c["valor"] += linha["valor"]

    marcacoes: list[dict] = []
    saida = []
    for g in grupos.values():
        g["pct"] = (pesos.get("grupo") or {}).get(g["grupo"])
        g["pct_efetivo"] = prestacao.peso_da_conta(pesos, g["grupo"], "", "", padrao)
        g["entra"] = g["fora"] = g["juros"] = 0.0
        g["excecoes"] = 0
        if g["pct"] is not None:
            marcacoes.append({"nivel": "grupo", "grupo": g["grupo"], "categoria": "",
                              "chave": g["grupo"], "rotulo": g["grupo"],
                              "valor": g["valor"], "pct": g["pct"]})
        cats = []
        for c in g["categorias"].values():
            c["pct"] = (pesos.get("categoria") or {}).get(c["categoria"])
            c["pct_efetivo"] = prestacao.peso_da_conta(
                pesos, g["grupo"], c["categoria"], "", padrao)
            c["grupo"] = g["grupo"]
            c["e_juros"] = c["categoria"].strip().lower() in juros
            proprias = por_categoria.get((g["grupo"], c["categoria"]), [])
            c["excecoes"] = len(proprias)
            if c["e_juros"]:
                c["entra"], c["fora"], c["juros"] = 0.0, 0.0, c["valor"]
            else:
                no_balde = c["valor"] - sum(e["valor"] for e in proprias)
                entra = no_balde * c["pct_efetivo"] / 100.0
                for e in proprias:
                    pct = prestacao.peso_da_conta(
                        pesos, g["grupo"], c["categoria"], e["codigo"], padrao)
                    entra += e["valor"] * pct / 100.0
                    marcacoes.append({
                        "nivel": "lancamento", "grupo": g["grupo"],
                        "categoria": c["categoria"], "chave": e["codigo"],
                        "rotulo": f"{e.get('credor') or '—'} · {e.get('documento') or 'sem documento'} "
                                  f"({e.get('mes')})",
                        "valor": e["valor"], "pct": pct})
                c["entra"], c["fora"], c["juros"] = entra, c["valor"] - entra, 0.0
            if c["pct"] is not None:
                marcacoes.append({"nivel": "categoria", "grupo": g["grupo"],
                                  "categoria": c["categoria"], "chave": c["categoria"],
                                  "rotulo": f"{g['grupo']} › {c['categoria']}",
                                  "valor": c["valor"], "pct": c["pct"]})
            g["entra"] += c["entra"]
            g["fora"] += c["fora"]
            g["juros"] += c["juros"]
            g["excecoes"] += c["excecoes"]
            cats.append(c)
        g["categorias"] = sorted(cats, key=lambda c: c["valor"])
        saida.append(g)
    contas = sorted(saida, key=lambda g: g["valor"])
    resumo = {
        "total": sum(g["valor"] for g in contas),
        "entra": sum(g["entra"] for g in contas),
        "fora": sum(g["fora"] for g in contas),
        "juros": sum(g["juros"] for g in contas),
        "marcacoes": len(marcacoes),
    }
    ordem = {"grupo": 0, "categoria": 1, "lancamento": 2}
    marcacoes.sort(key=lambda m: (ordem[m["nivel"]], m["grupo"], m["categoria"], m["rotulo"]))
    return {"contas": contas, "resumo": resumo, "marcacoes": marcacoes}


@bp.route("/prestacao/montagem")
def cenario_montagem():
    """A tela unica: a regua, o que se divide, os juros e quem divide."""
    from . import cenarios, consultas, prestacao, prestacao_dados
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))

    lista = cenarios.listar()
    escolhido = request.args.get("cenario")
    atual = None
    if escolhido:
        atual = cenarios.completo(escolhido)
    if atual is None and lista:
        atual = cenarios.completo(lista[0]["id"])

    config = prestacao_dados.config()
    contexto = dict(_contexto_comum("prestacao"))
    arvore = (_contas_da_matriz(config, atual, atual.get("medida", "comprometido"))
              if atual else {"contas": [], "resumo": None, "marcacoes": []})
    aberto_grupo = request.args.get("grupo", "")
    aberto_categoria = request.args.get("categoria", "")
    # o percentual que a categoria aberta passa aos lancamentos sem marcacao
    # propria — a lista mostra, linha a linha, o que cada um divide de fato
    categoria_aberta = next(
        (c for g in arvore["contas"] if g["grupo"] == aberto_grupo
         for c in g["categorias"] if c["categoria"] == aberto_categoria), None)
    return render_template(
        "painel_cenario.html", **contexto,
        cenarios=lista, cenario=atual,
        criterios=cenarios.CRITERIOS, janelas=cenarios.JANELAS,
        contas=arvore["contas"], resumo_matriz=arvore["resumo"],
        marcacoes=arvore["marcacoes"], categoria_aberta=categoria_aberta,
        socios=prestacao_dados.socios(apenas_ativos=True),
        obras_do_painel=_opcoes_no_escopo().get("obras", []),
        projetos_do_painel=_opcoes_no_escopo().get("projetos", []),
        excluidas_gerais=prestacao_dados.fora_da_analise(config),
        # onde estao os juros na base, categoria por categoria — e quais a
        # prestacao esta contando. E a resposta para "na controladoria tem
        # 1,6 milhao e aqui 191 mil".
        juros_conf=consultas.conferencia_dos_juros(
            prestacao.categorias_de_juros(config.get("categoria_juros")
                                          or prestacao.CATEGORIA_JUROS_PADRAO)),
        aberto_grupo=aberto_grupo,
        aberto_categoria=aberto_categoria,
        lancamentos=(consultas.lancamentos_administrativos(
            _deptos_administrativos(config), atual.get("medida", "comprometido"),
            grupo=aberto_grupo, categoria=aberto_categoria)
            if atual and aberto_categoria else []),
        config=config,
    )


@bp.route("/prestacao/montagem", methods=["POST"])
def cenario_gravar():
    """Uma acao por envio; o formulario diz qual em `acao`.

    Tudo volta para a mesma tela, com o mesmo grupo e a mesma categoria
    abertos: quem esta configurando trinta linhas nao pode perder o lugar a
    cada salvada."""
    from . import cenarios
    acao = request.form.get("acao", "")
    cenario_id = request.form.get("cenario_id") or ""

    if acao == "novo":
        cenario_id = cenarios.criar(request.form.get("nome", ""))
    elif acao == "duplicar" and cenario_id:
        cenario_id = cenarios.duplicar(cenario_id, request.form.get("nome", ""))
    elif acao == "apagar" and cenario_id:
        cenarios.apagar(cenario_id)
        cenario_id = ""
    elif acao == "regua" and cenario_id:
        cenarios.atualizar(cenario_id, **{
            c: request.form[c] for c in
            ("nome", "criterio", "janela", "pct_padrao", "medida",
             "juros_sem_deficit", "taxa_adm_pct", "observacao")
            if c in request.form})
        cenarios.atualizar(cenario_id,
                           juros_por_deficit=request.form.get("juros_por_deficit", "0"))
    elif acao == "peso" and cenario_id:
        cenarios.marcar_peso(cenario_id, request.form.get("nivel", ""),
                             request.form.get("chave", ""),
                             request.form.get("pct"))
    elif acao == "peso_em_lote" and cenario_id:
        cenarios.marcar_pesos(cenario_id, request.form.get("nivel", "lancamento"),
                              request.form.getlist("marcado"),
                              request.form.get("pct"))
    elif acao == "participacao" and cenario_id:
        cenarios.salvar_participacao(cenario_id, request.form["socio_id"],
                                     request.form.get("pct", 0),
                                     request.form.get("obra", ""))
    elif acao == "apagar_participacao" and cenario_id:
        cenarios.apagar_participacao(request.form["participacao_id"])
    elif acao == "excluir" and cenario_id:
        for item in request.form.getlist("item"):
            cenarios.excluir(cenario_id, item)
    elif acao == "reincluir" and cenario_id:
        cenarios.reincluir(cenario_id, request.form.get("item", ""))

    return redirect(url_for("painel.cenario_montagem",
                            cenario=cenario_id or None,
                            grupo=request.form.get("grupo") or None,
                            categoria=request.form.get("categoria") or None))


@bp.route("/prestacao/resultado")
def cenario_resultado():
    """O resultado do cenario: por obra, por pessoa, e a conta aberta.

    A mesma tela responde as tres perguntas do dono — "qual o resultado final",
    "quanto e de direito para cada envolvido" e "como foi que deu isso" — porque
    separa-las em telas diferentes foi justamente o que nao funcionou."""
    from . import cenarios, consultas, graficos, prestacao
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))

    lista = cenarios.listar()
    atual = cenarios.completo(request.args.get("cenario") or
                              (lista[0]["id"] if lista else 0))
    if atual is None:
        return redirect(url_for("painel.cenario_montagem"))

    calculo = _calcular_cenario(atual)
    por_obra = sorted(calculo["por_obra"].items(), key=lambda kv: kv[1]["resultado"])

    # O grafico e de UMA obra por vez: sobrepor 174 linhas nao se le. A escolhida
    # vem na URL, e sem escolha vale a que mais consumiu caixa — que e a que o
    # dono vai querer olhar primeiro.
    obras_com_juros = prestacao.total_por_obra(calculo["juros"]["alocacoes"])
    obra = request.args.get("obra_grafico") or (
        obras_com_juros[0]["obra"] if obras_com_juros else
        (por_obra[0][0] if por_obra else ""))
    trilha = []
    if obra:
        trilha = prestacao.trilha_da_obra(
            calculo["caixa"], obra, rateio=calculo["rateio"]["alocacoes"],
            juros=calculo["juros"]["alocacoes"])

    grafico = None
    if trilha:
        grafico = graficos.linhas_com_barras(
            trilha,
            [("acumulado", "var(--azul-claro)", "Caixa acumulado (antes dos juros)"),
             ("com_juros", "var(--vermelho)", "Com os juros que absorveu")],
            barras=("caixa_do_mes", "b-despesa", "Caixa do mes"),
            campo_rotulo="rotulo")

    sobras = calculo["sobras"]
    return render_template(
        "painel_cenario_resultado.html",
        **_contexto_comum("prestacao"),
        cenarios=lista, cenario=atual,
        por_obra=por_obra,
        quotas=calculo["quotas"],
        posicao=calculo["posicao"],
        rateio_memoria=[l for l in calculo["rateio"]["memoria"]
                        if abs(l["pool"]) > 0.005][-36:],
        juros_memoria=[l for l in calculo["juros"]["memoria"]
                       if abs(l["juros"]) > 0.005][-36:],
        juros_por_obra=obras_com_juros,
        rateio_por_obra=prestacao.total_por_obra(calculo["rateio"]["alocacoes"]),
        rateio_total=sum(calculo["rateio"]["alocacoes"].values()),
        juros_total=sum(calculo["juros"]["alocacoes"].values()),
        sobras=sorted(sobras, key=lambda s: abs(s["valor"]), reverse=True)[:40],
        total_sobras=sum(s["valor"] for s in sobras),
        obra_grafico=obra, trilha=trilha, grafico=grafico,
        obras_para_grafico=[nome for nome, _n in por_obra],
        criterios=cenarios.CRITERIOS,
        excluidas=calculo["excluidas"],
        sem_projeto=calculo["sem_projeto"],
        tem_nao_apropriado=calculo["tem_nao_apropriado"],
        nao_apropriado=consultas.SEM_OBRA,
    )


def _cenario_do_pedido(regras_gravadas, args):
    """Le da URL o que a pessoa mexeu: `pct_12=50`, `ativo_12=on`, ...

    O cenario viaja na URL, e nao numa sessao no servidor, por tres motivos: o
    servico roda com UM worker e reinicia a cada ~150 requisicoes (estado em
    memoria nao sobrevive), o link fica compartilhavel — da para mandar o
    cenario para o contador — e recarregar a pagina nao perde o que foi mexido.
    """
    from . import prestacao

    mudancas = {}
    for regra in regras_gravadas:
        rid = str(regra["id"])
        mudanca = {}
        for campo in ("pct", "escopo", "mes_ini", "mes_fim"):
            if f"{campo}_{rid}" in args:
                mudanca[campo] = args.get(f"{campo}_{rid}")
        # Caixa desmarcada NAO chega no formulario. Por isso a marca separada
        # `viu_<id>`, escondida: sem ela nao daria para distinguir "desmarcou"
        # de "nao mexeu", e desativar uma regra seria impossivel.
        if f"viu_{rid}" in args:
            mudanca["ativo"] = 1 if f"ativo_{rid}" in args else 0
        if mudanca:
            mudancas[rid] = mudanca
    return prestacao.regras_do_cenario(regras_gravadas, mudancas)


@bp.route("/prestacao/cenarios")
def prestacao_cenarios():
    """Ajustar as regras de rateio e ver o efeito ANTES de gravar."""
    from . import consultas, graficos, prestacao, prestacao_dados
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))

    medida = "executado" if request.args.get("medida") == "executado" else "comprometido"
    regras_gravadas = prestacao_dados.regras()
    regras_cenario = _cenario_do_pedido(regras_gravadas, request.args)

    config_oficial = prestacao_dados.config()
    config_cenario = dict(config_oficial)
    # O residuo tambem faz parte do cenario: e ele que decide se o que as regras
    # nao pegaram fica no proprio lado ou some da conta.
    if "residual_cenario" in request.args:
        config_cenario["residual"] = "1" if request.args.get("residual") else "0"

    # UMA leitura do banco para as duas contas. O cenario so mexe no residuo e
    # nos parametros das regras — nada que mude o QUE e lido —, entao ler duas
    # vezes seria varrer as 185 mil linhas do fato em dobro pelo mesmo dado.
    assert all(config_cenario[c] == config_oficial[c]
               for c in CHAVES_QUE_MUDAM_A_LEITURA), \
        "cenario mexeu em chave que muda a leitura: a base nao pode ser reusada"
    base = _base_da_prestacao(config_oficial, medida)
    oficial = _apurar_com(regras_gravadas, config_oficial, base)
    cenario = _apurar_com(regras_cenario, config_cenario, base)

    comparacao = prestacao.comparar_por_obra(oficial["apurado"], cenario["apurado"])
    grafico = graficos.barras_agrupadas(
        comparacao[:30], [("delta_resultado", "b-receita", "Δ Resultado")],
        campo_rotulo="obra", classes_por_sinal=("b-receita", "b-despesa"))

    return render_template(
        "painel_prestacao_cenarios.html",
        **_contexto_comum("prestacao"),
        medida=medida,
        regras_gravadas=regras_gravadas,
        regras_cenario={str(r["id"]): r for r in regras_cenario},
        escopos=prestacao.ESCOPOS,
        residual_oficial=str(config_oficial.get("residual", "1")) == "1",
        residual_cenario=str(config_cenario.get("residual", "1")) == "1",
        resumo=prestacao.resumo_do_cenario(oficial["rateio"], cenario["rateio"]),
        comparacao=comparacao,
        grafico=grafico,
        mexeu=(prestacao.cenario_difere(regras_gravadas, regras_cenario)
               or config_cenario != config_oficial),
    )


@bp.route("/prestacao/cenarios/gravar", methods=["POST"])
def prestacao_cenarios_gravar():
    """Promove o cenario a oficial. So com a confirmacao marcada."""
    from . import prestacao_dados

    if not request.form.get("confirma"):
        return redirect(url_for("painel.prestacao_cenarios"))

    regras_gravadas = prestacao_dados.regras()
    regras_cenario = _cenario_do_pedido(regras_gravadas, request.form)
    prestacao_dados.salvar_parametros_das_regras(regras_cenario)
    if "residual_cenario" in request.form:
        prestacao_dados.salvar_config(
            "residual", "1" if request.form.get("residual") else "0")
    logger.info("Painel: cenario de rateio gravado como oficial (%d regras).",
                len(regras_cenario))
    return redirect(url_for("painel.prestacao_contas", gravado="1"))


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
        fora=prestacao_dados.fora_da_analise(),
        obras_do_painel=_opcoes_no_escopo().get("obras", []) if not consultas.base_vazia() else [],
        projetos_do_painel=_opcoes_no_escopo().get("projetos", []) if not consultas.base_vazia() else [],
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
    elif acao == "tirar_da_analise":
        dados.tirar_da_analise(form.getlist("item"))
    elif acao == "voltar_para_analise":
        dados.voltar_para_analise(form.get("item", ""))
    elif acao == "config":
        for chave in ("projeto_matriz", "depto_admin_matriz", "depto_admin_filial",
                      "grupo_pessoal", "taxa_adm_pct", "residual",
                      "categoria_juros", "juros_por_deficit", "juros_sem_deficit"):
            if chave in form:
                dados.salvar_config(chave, form[chave])


# ---------------------------------------------------------------------------
# Explorador de lancamentos — saneamento da base
# ---------------------------------------------------------------------------
# FORA do menu principal, de proposito: e ferramenta de manutencao, nao
# relatorio. Chega-se a ela por Configuracoes. O dono pediu assim — "uma tela
# mais escondida, para nao ser algo tao exposto".
def _pedido_do_explorador():
    """Le da URL o que a pessoa escolheu nos filtros."""
    de, ate = _faixa_de_data()
    return {
        "tipo": request.args.get("tipo", ""),
        "analises": request.args.getlist("analise"),
        "grupos": request.args.getlist("grupo"),
        "categorias": request.args.getlist("categoria"),
        "obras": request.args.getlist("obra"),
        "fornecedores": request.args.getlist("fornecedor"),
        "projetos": request.args.getlist("projeto"),
        "contas": request.args.getlist("conta"),
        "situacoes": request.args.getlist("situacao"),
        "busca": (request.args.get("busca") or "").strip(),
        "com_trf": request.args.get("trf") == "1",
        "de": de, "ate": ate,
    }


@bp.route("/explorador")
def explorador():
    """Procura um lancamento em QUALQUER lugar da base — inclusive fora do DRE.

    As outras telas olham so o DRE. Esta olha tudo, porque ela existe para achar
    o que esta classificado errado, e o erro quase sempre e o lancamento estar
    na analise errada."""
    from . import consultas, saneamento
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))

    pedido = _pedido_do_explorador()
    # so busca quando ha algum filtro: abrir a tela e varrer as 185 mil linhas
    # para mostrar as 3.000 mais recentes nao ajuda ninguem e custa caro
    escolheu = any(pedido[c] for c in ("tipo", "analises", "grupos", "categorias",
                                       "obras", "fornecedores", "projetos",
                                       "contas", "situacoes", "busca", "de", "ate"))
    dados = consultas.explorar(pedido) if escolheu else None
    return render_template(
        "painel_explorador.html",
        aba_ativa="config", abas=ABAS,
        pedido=pedido, escolheu=escolheu, dados=dados,
        resumo=consultas.resumo_do_explorador(pedido) if escolheu else [],
        opcoes=consultas.opcoes_do_explorador(),
        fornecedores=consultas.fornecedores_do_recorte(dados),
        sem_obra=consultas.SEM_OBRA,
        sem_fornecedor=consultas.SEM_FORNECEDOR,
        teto=consultas.TETO_DO_EXPLORADOR,
        teto_do_lote=saneamento.TETO_POR_LOTE,
        escrita_ligada=saneamento.escrita_configurada(),
        categorias_omie=consultas.categorias_para_alterar(),
        obras_omie=consultas.departamentos_para_alterar(),
        alteracao=None, erro_alteracao=None,
        exclusao=None, erro_exclusao=None,
        teto_de_exclusao=saneamento.TETO_DE_EXCLUSAO,
    )


@bp.route("/explorador/alterar", methods=["POST"])
def explorador_alterar():
    """Altera a classificacao de titulos NO OMIE — ou so ensaia.

    A unica rota do painel que escreve num sistema de fora. Quatro protecoes,
    todas em `saneamento.py`: senha propria, simulacao por padrao, trava contra
    desfazer rateio e registro de tudo no banco."""
    from . import saneamento

    # A tela manda uma alteracao POR TITULO — cada linha pode ir para um lugar
    # diferente. As tres listas andam juntas, na mesma ordem.
    alvos = [{"codigo": c, "categoria": cat, "departamento": dep}
             for c, cat, dep in zip(request.form.getlist("alvo_codigo"),
                                    request.form.getlist("alvo_categoria"),
                                    request.form.getlist("alvo_departamento"))]
    # Sem JavaScript sobra a forma antiga: os marcados vao todos para o mesmo
    # lugar. Continua valendo — e o caminho que funciona com o navegador travado.
    categoria = (request.form.get("categoria_nova") or "").strip()
    departamento = (request.form.get("departamento_novo") or "").strip()
    if not alvos:
        alvos = request.form.getlist("codigo")
    # SIMULAR e o padrao: so sai do ensaio quem marcar E acertar a senha
    executar = request.form.get("executar") == "1"
    aceita = request.form.get("aceita_desfazer_rateio") == "1"

    erro = None
    if executar:
        if not saneamento.escrita_configurada():
            erro = ("A alteração no OMIE está desligada neste serviço: falta a "
                    "senha de execução (PAINEL_SENHA_ESCRITA).")
        elif not saneamento.senha_de_escrita_confere(request.form.get("senha", "")):
            logger.warning("Painel: senha de execucao incorreta na alteracao do OMIE.")
            erro = "Senha de execução incorreta. Nada foi enviado ao OMIE."

    resultado = None
    if not erro:
        resultado = saneamento.aplicar(
            alvos, categoria, departamento,
            simulacao=not executar, aceita_desfazer_rateio=aceita)
        if not resultado.get("ok"):
            erro, resultado = resultado.get("erro"), None

    from . import consultas
    pedido = _pedido_do_explorador()
    dados = consultas.explorar(pedido)
    return render_template(
        "painel_explorador.html",
        aba_ativa="config", abas=ABAS,
        pedido=pedido, escolheu=True,
        dados=dados,
        resumo=consultas.resumo_do_explorador(pedido),
        opcoes=consultas.opcoes_do_explorador(),
        fornecedores=consultas.fornecedores_do_recorte(dados),
        sem_obra=consultas.SEM_OBRA,
        sem_fornecedor=consultas.SEM_FORNECEDOR,
        teto=consultas.TETO_DO_EXPLORADOR,
        teto_do_lote=saneamento.TETO_POR_LOTE,
        escrita_ligada=saneamento.escrita_configurada(),
        categorias_omie=consultas.categorias_para_alterar(),
        obras_omie=consultas.departamentos_para_alterar(),
        alteracao=resultado, erro_alteracao=erro,
        exclusao=None, erro_exclusao=None,
        teto_de_exclusao=saneamento.TETO_DE_EXCLUSAO,
    )


@bp.route("/usuarios", methods=["POST"])
def usuarios_salvar():
    """Cadastra, altera ou apaga quem tem acesso proprio ao painel.

    So o administrador chega aqui — o guard ja barra quem entrou por usuario
    proprio, porque `painel.usuarios` nao esta na lista de telas liberaveis."""
    from . import usuarios

    acao = (request.form.get("acao") or "").strip()
    obras = [o for o in request.form.getlist("obra_do_usuario") if o.strip()]
    telas = [t for t in request.form.getlist("tela_do_usuario") if t.strip()]
    contas = [c for c in request.form.getlist("conta_do_usuario") if c.strip()]
    uid = (request.form.get("usuario_id") or "").strip()

    if acao == "criar":
        r = usuarios.criar(request.form.get("novo_usuario", ""),
                           request.form.get("nova_senha", ""),
                           nome=request.form.get("nome", ""),
                           obras=obras, telas=telas, contas=contas)
    elif acao == "apagar" and uid.isdigit():
        r = usuarios.apagar(int(uid))
    elif acao == "salvar" and uid.isdigit():
        r = usuarios.atualizar(
            int(uid), nome=request.form.get("nome"),
            senha=request.form.get("nova_senha"),
            ativo=request.form.get("ativo") == "1",
            obras=obras, telas=telas, contas=contas)
    else:
        r = {"ok": False, "erro": "Pedido não reconhecido."}

    return redirect(url_for("painel.configuracoes",
                            **({"erro_usuario": r["erro"]} if not r.get("ok")
                               else {"usuario_ok": "1"})))


@bp.route("/explorador/excluir", methods=["POST"])
def explorador_excluir():
    """APAGA titulos no OMIE. Nao ha desfazer.

    Rota separada da alteracao de proposito: mesmo formulario, mesmo botao, e
    um dia alguem clica no lugar errado. Aqui vale tudo o que vale na
    alteracao — senha propria, ensaio por padrao, registro no banco — mais um
    teto menor, porque o erro aqui nao se conserta alterando de novo."""
    from . import saneamento

    codigos = request.form.getlist("codigo")
    executar = request.form.get("executar") == "1"

    erro = None
    if executar:
        if not saneamento.escrita_configurada():
            erro = ("A exclusão no OMIE está desligada neste serviço: falta a "
                    "senha de execução (PAINEL_SENHA_ESCRITA).")
        elif not saneamento.senha_de_escrita_confere(request.form.get("senha", "")):
            logger.warning("Painel: senha de execucao incorreta na exclusao do OMIE.")
            erro = "Senha de execução incorreta. Nada foi excluído."
        # Exigir a palavra escrita a mao: marcar uma caixinha por engano
        # acontece; digitar EXCLUIR por engano, nao.
        elif (request.form.get("confirmacao") or "").strip().upper() != "EXCLUIR":
            erro = ("Para excluir de verdade, digite EXCLUIR no campo de "
                    "confirmação. Nada foi excluído.")

    resultado = None
    if not erro:
        resultado = saneamento.excluir(codigos, simulacao=not executar)
        if not resultado.get("ok"):
            erro, resultado = resultado.get("erro"), None

    from . import consultas
    pedido = _pedido_do_explorador()
    dados = consultas.explorar(pedido)
    return render_template(
        "painel_explorador.html",
        aba_ativa="config", abas=ABAS,
        pedido=pedido, escolheu=True,
        dados=dados,
        resumo=consultas.resumo_do_explorador(pedido),
        opcoes=consultas.opcoes_do_explorador(),
        fornecedores=consultas.fornecedores_do_recorte(dados),
        sem_obra=consultas.SEM_OBRA,
        sem_fornecedor=consultas.SEM_FORNECEDOR,
        teto=consultas.TETO_DO_EXPLORADOR,
        teto_do_lote=saneamento.TETO_POR_LOTE,
        teto_de_exclusao=saneamento.TETO_DE_EXCLUSAO,
        escrita_ligada=saneamento.escrita_configurada(),
        categorias_omie=consultas.categorias_para_alterar(),
        obras_omie=consultas.departamentos_para_alterar(),
        exclusao=resultado, erro_exclusao=erro,
        alteracao=None, erro_alteracao=None,
    )


# ---------------------------------------------------------------------------
# Rateio da Administracao — simulacao
# ---------------------------------------------------------------------------
def _escolhas_do_conjunto(prefixo: str):
    """Le da URL os pares (item, %) do conjunto — mesmo formato da Necessidade
    de Caixa, para quem ja conhece uma tela conhecer a outra."""
    itens = request.args.getlist(f"{prefixo}_item")
    pcts = request.args.getlist(f"{prefixo}_pct")
    escolhas = []
    for i, item in enumerate(itens):
        if not item:
            continue
        try:
            pct = float(pcts[i]) if i < len(pcts) and pcts[i] else 100.0
        except ValueError:
            pct = 100.0
        escolhas.append((item, pct))
    return escolhas


def _ajustes_de_caixa():
    """(lado, mes, valor) — recurso que existe mas nao esta na base."""
    lados = request.args.getlist("ajuste_lado")
    meses = request.args.getlist("ajuste_mes")
    valores = request.args.getlist("ajuste_valor")
    saida = []
    for i, lado in enumerate(lados):
        if lado not in ("A", "B") or i >= len(meses) or not meses[i]:
            continue
        try:
            ano, mes = str(meses[i]).split("-")[:2]
            quando = dt.date(int(ano), int(mes), 1)
            valor = float(str(valores[i]).replace(",", ".")) if i < len(valores) else 0.0
        except (ValueError, IndexError):
            continue
        if valor:
            saida.append((lado, quando, valor))
    return saida


@bp.route("/rateio-administracao")
def rateio_administracao():
    """Como o custo da matriz se divide entre dois lados — e quem pagou os juros.

    IGNORA os filtros da barra lateral, como a Necessidade de Caixa: a conta e
    sobre a empresa inteira ao longo do tempo, e recortar por ano ou obra faria
    o acumulado mentir."""
    from . import consultas, graficos, prestacao_dados, rateio_admin
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))

    config = prestacao_dados.config()
    opcoes = consultas.opcoes_de_filtro()
    administrativos = consultas.departamentos_administrativos()
    matriz = (request.args.get("matriz")
              or (administrativos[0] if administrativos else ""))
    categoria_juros = (request.args.get("categoria_juros")
                       or "Juros sobre Empréstimos")
    criterio = request.args.get("criterio", "faturamento")
    if criterio not in rateio_admin.CRITERIOS:
        criterio = "faturamento"
    janela = request.args.get("janela", "12")
    if janela not in rateio_admin.JANELAS:
        janela = "12"

    def _numero(nome, padrao=0.0):
        try:
            return float(str(request.args.get(nome, padrao)).replace(",", "."))
        except ValueError:
            return padrao

    escolhas_a = _escolhas_do_conjunto("a")
    suprimidas = request.args.getlist("suprimir")
    resultado = None
    if escolhas_a and matriz:
        resultado = rateio_admin.simular(
            consultas.caixa_mensal_por_obra(),
            consultas.receita_mensal_por_obra(),
            consultas.pessoal_mensal_por_obra(config["grupo_pessoal"]),
            consultas.matriz_mensal(matriz, categoria_juros, suprimidas),
            consultas.juros_de_emprestimo_mensal(categoria_juros),
            escolhas_a, consultas.obra_para_projeto(), opcoes["obras"],
            depto_matriz=matriz, criterio=criterio, janela=janela,
            pct_fixo=_numero("pct_fixo", 50.0),
            valor_fixo_por_mes=_numero("valor_fixo", 0.0),
            abater_receita_da_matriz=request.args.get("abater", "1") == "1",
            ajustes=_ajustes_de_caixa(),
            juros_sem_deficit=request.args.get("sem_deficit", "criterio"))

    grafico = None
    if resultado and not resultado["vazio"]:
        grafico = graficos.linhas_com_barras(
            resultado["linhas"],
            [("caixa_a", "var(--azul-claro)", "Caixa A (antes dos juros)"),
             ("caixa_b", "var(--roxo)", "Caixa B (antes dos juros)"),
             ("final_a", "var(--verde)", "Posição final A"),
             ("final_b", "var(--vermelho)", "Posição final B")],
            barras=("juros_mes", "b-despesa", "Juros do mês"),
            campo_rotulo="rotulo")

    return render_template(
        "painel_rateio_admin.html",
        aba_ativa="config", abas=ABAS,
        administrativos=administrativos, matriz=matriz,
        categoria_juros=categoria_juros,
        criterios=rateio_admin.CRITERIOS, criterio=criterio,
        janelas=rateio_admin.JANELAS, janela=janela,
        pct_fixo=_numero("pct_fixo", 50.0),
        valor_fixo=_numero("valor_fixo", 0.0),
        abater=request.args.get("abater", "1") == "1",
        sem_deficit=request.args.get("sem_deficit", "criterio"),
        opcoes=opcoes, escolhas_a=escolhas_a,
        categorias_da_matriz=(consultas.custo_da_matriz_por_categoria(matriz)
                              if matriz else []),
        suprimidas=suprimidas,
        ajustes=_ajustes_de_caixa(),
        resultado=resultado, grafico=grafico,
    )


def _conferir(funcao, erros: list, nome: str = "Conferência"):
    """Roda uma conferência sem deixar a falha dela derrubar a tela.

    As conferências SÓ MEDEM — nenhuma altera nada. Então a falha de uma é
    informação, não motivo para esconder as outras três."""
    try:
        return funcao()
    except Exception as e:  # noqa: BLE001 — o erro vai para a tela, não para o log só
        logger.exception("Painel: a conferência %s falhou", nome)
        erros.append({"nome": nome, "erro": str(e)})
        return None


def _obras_para_liberar(estado_migracoes):
    """As obras que dá para marcar no cadastro de acesso."""
    if estado_migracoes["pendentes"]:
        return []
    from . import consultas
    return consultas.opcoes_de_filtro()["obras"]


def _contas_para_liberar(estado_migracoes):
    """As contas correntes que dá para marcar no cadastro de acesso."""
    if estado_migracoes["pendentes"]:
        return []
    from . import consultas
    return consultas.opcoes_de_filtro().get("contas", [])


def _pessoas_do_painel(estado_migracoes):
    """A lista de quem tem acesso. Vazia enquanto a migração 013 não rodar —
    sem isso a tela de Configurações quebraria justamente para quem vai apertar
    o botão que cria a tabela."""
    if estado_migracoes["pendentes"]:
        return []
    from . import usuarios
    try:
        return usuarios.listar()
    except Exception:  # noqa: BLE001
        logger.exception("Painel: não consegui listar os usuários")
        return []


@bp.route("/configuracoes")
def configuracoes():
    from . import migracoes_runner, tarefas
    from . import usuarios as usuarios_mod
    estado_migracoes = migracoes_runner.listar_estado()
    conferencias_com_erro: list[dict] = []
    contexto = {"aba_ativa": "config", "abas": ABAS}
    sincronizacao = tarefas.estado()
    # Se as tabelas ainda nao existem, nem tenta consultar a base.
    if estado_migracoes["pendentes"]:
        atualizacao, vazia, etapas = None, True, []
        conferencia = sumidos = aportes_conf = observacoes = fora = None
        contas_conf = aportes_base = juros_conf = None
        conferir = False
        recarga = None
    else:
        from . import consultas
        # A caixa vermelha logo abaixo ja conta, com etapa e tempo de silencio,
        # que a atualizacao anterior morreu. Quando ela esta na tela, esta linha
        # para de repetir interrupcao e passa a responder outra pergunta, que e
        # a util no momento: quando a base foi atualizada de verdade pela
        # ultima vez.
        atualizacao = consultas.atualizado_em(
            so_concluidas=bool(sincronizacao["interrompida"]))
        vazia = consultas.base_vazia()
        etapas = consultas.etapas_da_carga()
        # Aviso que NAO pode faltar: a migracao 010 arruma o tipo da coluna, mas
        # o centavo que ja se perdeu so volta com uma carga inicial. Sem dizer
        # isso na tela, a migracao daria a impressao de ter resolvido.
        recarga = _conferir(consultas.recarga_total_pendente,
                            conferencias_com_erro, "Aviso de recarga")
        # So mede, nao corrige: quanto dinheiro a carga deu por realizado e as
        # telas nao enxergam. Ver o comentario em `conferencia_do_pago`.
        # AS CONFERENCIAS SO RODAM QUANDO ALGUEM PEDE. Sao ~12 varreduras na
        # base inteira, e Configuracoes e a tela onde se aperta o botao de
        # atualizar: ela TEM de abrir rapido. Deixei as tres ligadas por padrao
        # em 14/09/2026 e a tela parou de abrir para o dono no mesmo dia.
        conferir = request.args.get("conferir") == "1"
        procurado = consultas._valor_procurado(request.args.get("procurar", ""))
        conferencia = sumidos = aportes_conf = observacoes = fora = None
        contas_conf = aportes_base = juros_conf = None
        if not vazia and (conferir or procurado is not None):
            # CADA UMA POR SI. Em 20/09/2026 o dono apertou o botão e "não
            # apresentou resultado" — e não havia como saber se tinha dado erro,
            # porque uma consulta quebrada levava a tela inteira para a página
            # de erro, sem dizer qual das quatro foi. Agora cada conferência cai
            # sozinha e a falha dela aparece em vermelho no lugar do quadro.
            conferencia = _conferir(consultas.conferencia_do_pago,
                                    conferencias_com_erro,
                                    "Dinheiro que as telas não contam")
            sumidos = _conferir(lambda: consultas.titulos_que_sumiram(procurado),
                                conferencias_com_erro, "Busca pelo valor")
            aportes_conf = _conferir(consultas.conferencia_dos_aportes,
                                     conferencias_com_erro, "Aportes do DRE")
            # Veio do bloco do DRE: aportado e devolvido na base inteira, e
            # onde esta o resto, obra por obra.
            aportes_base = _conferir(consultas.aportes_na_base_inteira,
                                     conferencias_com_erro,
                                     "Aportes na base inteira")
            observacoes = _conferir(consultas.cobertura_das_observacoes,
                                    conferencias_com_erro,
                                    "Observações dos títulos")
            fora = _conferir(consultas.movimentos_fora_do_painel,
                             conferencias_com_erro,
                             "Movimentos fora do painel")
            contas_conf = _conferir(consultas.conferencia_das_contas,
                                    conferencias_com_erro,
                                    "Conta de onde o dinheiro saiu")
            from . import prestacao, prestacao_dados
            juros_conf = _conferir(
                lambda: consultas.conferencia_dos_juros(prestacao.categorias_de_juros(
                    prestacao_dados.config().get("categoria_juros")
                    or prestacao.CATEGORIA_JUROS_PADRAO)),
                conferencias_com_erro, "Onde estão os juros de empréstimo")
    return render_template(
        "painel_config.html", **contexto,
        migracoes=estado_migracoes,
        atualizacao=atualizacao,
        base_vazia=vazia,
        primeira=request.args.get("primeira") == "1",
        etapas=etapas,
        tem_secret=bool(os.getenv("PAINEL_SECRET", "").strip()),
        sem_obra=consultas.SEM_OBRA if not estado_migracoes["pendentes"] else "",
        sem_categoria=consultas.SEM_CATEGORIA if not estado_migracoes["pendentes"] else "",
        conferir=conferir,
        recarga=recarga,
        conferencias_com_erro=conferencias_com_erro,
        conferencia=conferencia,
        sumidos=sumidos,
        aportes_conf=aportes_conf,
        aportes_base=aportes_base,
        observacoes=observacoes,
        fora=fora,
        contas_conf=contas_conf,
        juros_conf=juros_conf,
        modos=tarefas.MODOS,
        sincronizacao=sincronizacao,
        pessoas=_pessoas_do_painel(estado_migracoes),
        telas_liberaveis=usuarios_mod.TELAS,
        telas_sugeridas=usuarios_mod.TELAS_SUGERIDAS,
        obras_para_liberar=_obras_para_liberar(estado_migracoes),
        contas_para_liberar=_contas_para_liberar(estado_migracoes),
        erro_usuario=request.args.get("erro_usuario", ""),
        usuario_ok=request.args.get("usuario_ok") == "1",
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
    import os
    from . import tarefas
    dados = request.get_json(silent=True) or {}

    # O SEGREDO E ACEITO DE QUATRO JEITOS, e isso nao e desleixo.
    #
    # Ate 18/09/2026 ele so era lido do corpo JSON. Se o agendador mandasse como
    # formulario — que e o padrao de varios deles — o segredo chegava vazio, a
    # resposta era "nao autorizado", e nao havia como o dono descobrir que o
    # problema era o formato do envio, e nao o valor do segredo. Meia hora de
    # tentativa e erro por causa de um cabecalho.
    segredo = (dados.get("secret")
               or request.form.get("secret")
               or request.args.get("secret")
               or request.headers.get("X-Painel-Secret")
               or "")
    modo = (dados.get("modo") or request.form.get("modo")
            or request.args.get("modo") or "rapida")

    if auth.esta_logado():
        disparo = "manual"
    elif auth.segredo_de_maquina_confere(segredo):
        disparo = "agendado"
    else:
        # A mensagem diz O QUE FAZER, sem entregar o segredo. Dizer so "nao
        # autorizado" deixa quem esta configurando o agendador no escuro.
        if not os.getenv("PAINEL_SECRET", "").strip():
            motivo = ("o serviço não tem PAINEL_SECRET configurada. "
                      "Cadastre essa variável nas Settings do Render.")
        elif not segredo:
            motivo = ("nenhum segredo foi enviado. Mande no corpo do pedido "
                      '{"secret": "...", "modo": "rapida"} com '
                      "Content-Type: application/json, ou no cabeçalho "
                      "X-Painel-Secret.")
        else:
            motivo = ("o segredo enviado não confere com a PAINEL_SECRET do "
                      "serviço.")
        logger.warning("Painel: disparo recusado — %s", motivo)
        return jsonify({"ok": False, "erro": f"Não autorizado: {motivo}"}), 401

    return jsonify(tarefas.disparar(modo, disparo))


def _subtitulo_do_relatorio(f) -> str:
    """A linha de contexto da capa: quando foi gerado e sobre o que.

    Um relatorio financeiro sem o recorte escrito na capa e perigoso: seis meses
    depois ninguem lembra se aquilo era a empresa inteira ou uma obra so."""
    from .horario import agora
    recorte = " · ".join(f.resumo()) or "todos os anos, projetos e obras"
    return (f"Gerado em {agora():%d/%m/%Y às %H:%M} (Brasília)  ·  "
            f"Recorte: {recorte}")


def _resumo_do_relatorio(f) -> list:
    """Os numeros da capa — os mesmos cinco da Visao Geral e do DRE."""
    from . import consultas
    from .pdf import brl
    dre = consultas.resultado_dre(f)
    return [
        ("Resultado (DRE) — comprometido", ""),
        ("Receita líquida", brl(dre["receita"])),
        ("Despesas", brl(dre["despesa"])),
        ("Resultado", brl(dre["resultado"])),
    ]


def _graficos_do_relatorio(f) -> list:
    """O Fluxo Financeiro mensal, desenhado com a MESMA geometria da tela."""
    from . import consultas, graficos
    mensal = consultas.resultado_mensal(f, medida="comprometido")
    if not mensal:
        return []
    return [(graficos.barras_agrupadas(
        mensal[-36:],
        [("receita", "b-receita", "Receita"), ("despesa", "b-despesa", "Despesa")],
        campo_rotulo="rotulo", campo_linha="acumulado"),
        "Fluxo Financeiro mensal — comprometido")]


@bp.route("/baixar/<assunto>")
def baixar(assunto):
    """Leva os numeros para uma planilha do Excel.

    Respeita os MESMOS filtros da tela: a URL do download so acrescenta o
    assunto, o resto e o que ja estava na barra de endereco. Assim o arquivo e
    exatamente o que estava na tela — nunca "a base inteira" quando a pessoa
    estava vendo um recorte.

    `completo` e o relatorio inteiro, uma aba por assunto — era assim na tela
    antiga, e um arquivo so e melhor que oito soltos.
    """
    from flask import Response
    from . import consultas, excel

    # O download passava por fora da protecao das telas — ver `pode_baixar`.
    # Responde "nao encontrado", como o resto do painel: dizer "sem permissao"
    # confirmaria que o arquivo existe.
    if not auth.pode_baixar(assunto):
        logger.warning("Painel: download de '%s' recusado.", assunto)
        return auth.nao_encontrado()

    f = _filtros_do_pedido()
    C = excel.COLUNAS

    def _dre():
        return consultas.dre_linhas(f)["linhas"]

    def _analitico(limite=20000):
        de, ate = _faixa_de_data()
        return consultas.analitico_despesas(
            f, grupo=request.args.get("grupo", ""),
            categoria=request.args.get("categoria", ""),
            credor=request.args.get("credor", ""),
            busca=(request.args.get("busca") or "").strip(),
            visao=request.args.get("visao", "comprometido"),
            ordem=request.args.get("ordem", "valor"),
            de=de, ate=ate, base=request.args.get("base", "movimento"),
            pagina=1, por_pagina=limite)["linhas"]

    def _abas_do_analitico():
        """O relatorio do Analitico com as somas por cima dos lancamentos.

        22/09/2026, o dono: "melhore o relatorio, ta muito pobre. Acho que pode
        ter outras abas." Saia uma aba so, a lista crua.

        As somas sao feitas AQUI, a partir das mesmas linhas da aba de
        lancamentos — e nao por consultas proprias — por um motivo: assim cada
        aba de resumo FECHA com a lista, com os mesmos filtros (inclusive os
        desta tela: grupo, credor, busca, faixa de data), e ninguem precisa
        explicar por que a soma por conta deu diferente da lista."""
        linhas = _analitico()
        de, ate = _faixa_de_data()
        visao = request.args.get("visao", "comprometido")

        resumo = [{"o_que": "Filtro", "valor": chip} for chip in f.resumo()]
        for rotulo, valor in (("Grupo", request.args.get("grupo", "")),
                              ("Categoria", request.args.get("categoria", "")),
                              ("Credor", request.args.get("credor", "")),
                              ("Busca", (request.args.get("busca") or "").strip()),
                              ("De", de), ("Ate", ate)):
            if valor:
                resumo.append({"o_que": rotulo, "valor": valor})
        resumo.append({"o_que": "Visao", "valor": {"executado": "so pagas",
                                                   "aberto": "so a pagar"}.get(visao, "comprometido")})
        pago = sum(l["pago"] for l in linhas)
        aberto = sum(l["a_pagar"] for l in linhas)
        encargos = sum(l["juros"] + l["multa"] for l in linhas)
        resumo += [
            {"o_que": "Lancamentos", "valor": f"{len(linhas):,}".replace(",", ".")},
            {"o_que": "Pago", "valor": round(pago, 2)},
            {"o_que": "A pagar", "valor": round(aberto, 2)},
            {"o_que": "Juros e multa", "valor": round(encargos, 2)},
            {"o_que": "Total", "valor": round(pago + aberto + encargos, 2)},
        ]

        def _quebra(rotulo, chave_de):
            """Uma aba de resumo: soma por uma chave, com o peso de cada linha."""
            agg: dict = {}
            for l in linhas:
                nome, ordem = chave_de(l)
                a = agg.setdefault(nome, {"nome": nome, "lancamentos": 0, "pago": 0.0,
                                          "a_pagar": 0.0, "encargos": 0.0,
                                          "total": 0.0, "_ordem": ordem})
                a["lancamentos"] += 1
                a["pago"] += l["pago"]
                a["a_pagar"] += l["a_pagar"]
                a["encargos"] += l["juros"] + l["multa"]
                a["total"] += l["pago"] + l["a_pagar"] + l["juros"] + l["multa"]
            base = sum(abs(a["total"]) for a in agg.values()) or 1.0
            saida = []
            for a in sorted(agg.values(), key=lambda a: a["_ordem"]):
                a["pct"] = round(abs(a["total"]) / base * 100, 1)
                for campo in ("pago", "a_pagar", "encargos", "total"):
                    a[campo] = round(a[campo], 2)
                a.pop("_ordem")
                saida.append(a)
            colunas = [("nome", rotulo), ("lancamentos", "Lancamentos"),
                       ("pago", "Pago"), ("a_pagar", "A pagar"),
                       ("encargos", "Juros e multa"), ("total", "Total"),
                       ("pct", "% do total")]
            return colunas, saida

        def _por_texto(campo, vazio):
            # maior valor primeiro: o total e negativo, entao o menor vem antes
            return lambda l: ((l.get(campo) or "").strip() or vazio,
                              (l["pago"] + l["a_pagar"] + l["juros"] + l["multa"]))

        def _por_mes(l):
            d = l.get("data")
            if not d:
                return "(sem data)", (9999, 12)
            return f"{d.month:02d}/{d.year}", (d.year, d.month)

        abas = [("Resumo", [("o_que", "O que"), ("valor", "Valor")], resumo)]
        for titulo, rotulo, chave in (
                ("Por grupo", "Grupo", _por_texto("grupo", "(sem grupo)")),
                ("Por categoria", "Categoria", _por_texto("categoria", "(sem categoria)")),
                ("Por credor", "Credor", _por_texto("credor", "(sem fornecedor)")),
                ("Por conta de pagamento", "Conta de pagamento",
                 _por_texto("conta", "(sem conta)")),
                ("Por obra", "Obra", _por_texto("obra", "(sem obra)")),
                ("Por mes", "Mes", _por_mes)):
            colunas, dados = _quebra(rotulo, chave)
            abas.append((titulo, colunas, dados))
        abas.append(("Despesas Analitico", C["analitico"], linhas))
        return abas


    def _abas_do_rateio_admin():
        """A memoria de calculo mes a mes, com os parametros ao lado.

        Os PARAMETROS vao junto de proposito: uma memoria de calculo sem as
        escolhas que a geraram nao da para conferir seis meses depois."""
        from . import prestacao_dados, rateio_admin
        config = prestacao_dados.config()
        opcoes = consultas.opcoes_de_filtro()
        administrativos = consultas.departamentos_administrativos()
        matriz = (request.args.get("matriz")
                  or (administrativos[0] if administrativos else ""))
        categoria_juros = (request.args.get("categoria_juros")
                           or "Juros sobre Empréstimos")
        escolhas = _escolhas_do_conjunto("a")
        criterio = request.args.get("criterio", "faturamento")
        janela = request.args.get("janela", "12")
        suprimidas = request.args.getlist("suprimir")

        def _num(nome, padrao=0.0):
            try:
                return float(str(request.args.get(nome, padrao)).replace(",", "."))
            except ValueError:
                return padrao

        simulado = rateio_admin.simular(
            consultas.caixa_mensal_por_obra(),
            consultas.receita_mensal_por_obra(),
            consultas.pessoal_mensal_por_obra(config["grupo_pessoal"]),
            consultas.matriz_mensal(matriz, categoria_juros, suprimidas),
            consultas.juros_de_emprestimo_mensal(categoria_juros),
            escolhas, consultas.obra_para_projeto(), opcoes["obras"],
            depto_matriz=matriz, criterio=criterio, janela=janela,
            pct_fixo=_num("pct_fixo", 50.0), valor_fixo_por_mes=_num("valor_fixo"),
            abater_receita_da_matriz=request.args.get("abater", "1") == "1",
            ajustes=_ajustes_de_caixa(),
            juros_sem_deficit=request.args.get("sem_deficit", "criterio"))

        parametros = [
            {"o_que": "Departamento da matriz", "valor": matriz},
            {"o_que": "Criterio do rateio", "valor": criterio},
            {"o_que": "Janela do criterio", "valor": janela},
            {"o_que": "% fixo para A", "valor": str(_num("pct_fixo", 50.0))},
            {"o_que": "Tirado do bolo por mes", "valor": str(_num("valor_fixo"))},
            {"o_que": "Abate a receita da matriz",
             "valor": "sim" if request.args.get("abater", "1") == "1" else "nao"},
            {"o_que": "Categoria dos juros", "valor": categoria_juros},
            {"o_que": "Juros sem ninguem negativo",
             "valor": request.args.get("sem_deficit", "criterio")},
        ]
        parametros += [{"o_que": "Lado A", "valor": f"{item} — {pct:.0f}%"}
                       for item, pct in escolhas]
        parametros += [{"o_que": "Suprimida do bolo", "valor": c}
                       for c in suprimidas]
        parametros += [{"o_que": f"Ajuste {lado}",
                        "valor": f"{quando:%m/%Y}: {valor:,.2f}"}
                       for lado, quando, valor in _ajustes_de_caixa()]
        return [
            ("Parametros", [("o_que", "O que"), ("valor", "Valor")], parametros),
            ("Memoria mensal", C["rateio_admin"], simulado.get("linhas", [])),
            ("Matriz por categoria",
             [("categoria", "Categoria"), ("valor", "Total pago")],
             consultas.custo_da_matriz_por_categoria(matriz) if matriz else []),
        ]

    def _abas_do_cenario():
        """O cenario inteiro num arquivo so — e auditavel.

        O dono: "importantissimo, auditavel. Porque eu preciso, se eu quiser,
        gerar um relatorio dos juros, para ver como e que isso ficou
        distribuido, gerar um relatorio da mao de obra, como e que foi
        distribuida, quanto cada obra absorveu daquele, daquele por mes."

        Uma aba por recorte, e os PARAMETROS na frente: memoria de calculo sem
        as escolhas que a geraram nao da para conferir seis meses depois."""
        from . import cenarios, prestacao
        atual = cenarios.completo(request.args.get("cenario") or 0)
        if atual is None:
            return [("Cenario", [("o_que", "O que"), ("valor", "Valor")],
                     [{"o_que": "Cenario", "valor": "nenhum escolhido"}])]
        calculo = _calcular_cenario(atual)

        parametros = [
            {"o_que": "Cenario", "valor": atual["nome"]},
            {"o_que": "Para que serve", "valor": atual["observacao"]},
            {"o_que": "Regua do rateio",
             "valor": cenarios.CRITERIOS.get(atual["criterio"], atual["criterio"])},
            {"o_que": "Janela",
             "valor": cenarios.JANELAS.get(atual["janela"], atual["janela"])},
            {"o_que": "Conta nao marcada divide",
             "valor": f"{atual['pct_padrao']:.0f}%"},
            {"o_que": "Base do calculo", "valor": atual["medida"]},
            {"o_que": "Juros de emprestimo",
             "valor": ("por quem demandou caixa" if atual["juros_por_deficit"]
                       else "junto da estrutura")},
            {"o_que": "Mes sem ninguem no vermelho",
             "valor": atual["juros_sem_deficit"]},
            {"o_que": "Taxa de administracao",
             "valor": f"{atual['taxa_adm_pct']:.2f}%"},
        ]
        for nivel in ("grupo", "categoria", "lancamento"):
            for chave, pct in sorted((atual["pesos"].get(nivel) or {}).items()):
                parametros.append({"o_que": f"Marcado ({nivel}) {chave}",
                                   "valor": f"{pct:.0f}%"})
        for p in atual["participacoes"]:
            parametros.append({"o_que": f"{p['socio']} em {p['obra'] or 'todas as obras'}",
                               "valor": f"{p['pct']:.2f}%"})

        por_obra = [dict(obra=nome, **n) for nome, n in
                    sorted(calculo["por_obra"].items(),
                           key=lambda kv: kv[1]["resultado"])]
        obra = request.args.get("obra_grafico") or ""
        trilha = []
        if obra:
            trilha = prestacao.trilha_da_obra(
                calculo["caixa"], obra, rateio=calculo["rateio"]["alocacoes"],
                juros=calculo["juros"]["alocacoes"])

        abas = [
            ("Parametros", [("o_que", "O que"), ("valor", "Valor")], parametros),
            ("Resultado por Obra", C["cenario_obra"], por_obra),
            ("Posicao de cada um", C["posicao"], calculo["posicao"]),
            ("Quotas", C["cenario_quotas"], calculo["quotas"]),
            ("Estrutura por Obra", C["cenario_por_obra"],
             prestacao.total_por_obra(calculo["rateio"]["alocacoes"])),
            ("Estrutura mes a mes", C["cenario_estrutura_mes"],
             calculo["rateio"]["memoria"]),
            ("Juros por Obra", C["cenario_por_obra"],
             prestacao.total_por_obra(calculo["juros"]["alocacoes"])),
            ("Juros mes a mes", C["cenario_juros_mes"], calculo["juros"]["memoria"]),
            ("Sem dono", C["cenario_sobras"], calculo["sobras"]),
        ]
        if trilha:
            abas.append((f"Mes a mes {obra[:18]}", C["cenario_trilha"], trilha))
        return abas


    def _abas_de_aporte():
        # Na tela os lançamentos são cortados num teto; no arquivo saem todos —
        # é para isso que se baixa o arquivo.
        bloco = consultas.aportes(f)
        divisao = consultas.resultado_dividendos(f)
        return [
            ("Aportes por Socio", C["aporte_socio"], bloco["por_socio"]),
            ("Aportes por Obra", C["aporte_obra"], bloco["por_obra"]),
            ("Dividendos", C["aporte_dividendos"], bloco["dividendos"]),
            ("Lancamentos de Aporte", C["aporte_lancamentos"],
             consultas.lancamentos_de_aporte(f, limite=None)["linhas"]),
            ("Resultado x Dividendos", C["divisao"], divisao["linhas"]),
            ("Caixa com Socios", C["caixa_socios"],
             consultas.caixa_com_socios(f)["linhas"]),
        ]

    def _abas_do_calendario():
        import datetime as _dt
        fc, proprios = _filtros_do_calendario()
        ano, mes = _mes_do_calendario()
        dados = consultas.calendario_do_mes(fc, ano, mes, **proprios)
        dias = [dict(dia=d, **n) for d, n in sorted(dados["dias"].items())]
        lancamentos = []
        for d in sorted(dados["dias"]):
            lancamentos.extend(consultas.lancamentos_do_dia(
                fc, d.isoformat(), limite=5000, **proprios))
        rotulo = f"{NOMES_DOS_MESES[mes - 1]} de {ano}"
        return [(f"Calendario {rotulo}", C["calendario_dias"], dias),
                ("Lancamentos do mes", C["calendario_lancamentos"], lancamentos)]

    montadores = {
        "dre": lambda: [("DRE", C["dre"], _dre())],
        "calendario": _abas_do_calendario,
        "analitico": _abas_do_analitico,
        "extrato": lambda: [("Extrato de Conta", C["extrato"],
                             consultas.extrato_da_conta(
                                 f, busca=(request.args.get("busca") or "").strip(),
                                 categoria=request.args.get("categoria", ""),
                                 de=request.args.get("de", ""),
                                 ate=request.args.get("ate", ""),
                                 ordem=request.args.get("ordem", "data"),
                                 por_pagina=20000)["linhas"])],
        "despesas": lambda: [("Despesas", C["despesas"], consultas.despesas_por(
            f, quebra=request.args.get("quebra", "grupo"),
            visao=request.args.get("visao", "comprometido"), limite=1000))],
        # o explorador leva TUDO que o filtro pegou, nao as 3.000 da tela: e para
        # isso que se baixa o arquivo
        "explorador": lambda: [("Lancamentos", C["explorador"],
                                consultas.explorar(_pedido_do_explorador(),
                                                   limite=50000)["linhas"])],
        "rateio_admin": _abas_do_rateio_admin,
        "credores": lambda: [("Top Credores", C["credores"],
                              consultas.top_credores(f, limite=1000))],
        "medicoes": lambda: [("Receita de Obra", C["medicoes"], consultas.medicoes(
            f, visao=request.args.get("visao", "todas"), limite=20000))],
        "fluxo": lambda: [("Fluxo de Caixa", C["fluxo"], consultas.caixa_por_mes(f))],
        "obras": lambda: [("Resultado por Obra", C["obras"], consultas.resultado_por(
            f, nivel=_nivel_do_pedido(),
            medida=request.args.get("medida", "comprometido"), limite=1000))],
        "execucao": lambda: [("Comprometido x Executado", C["execucao"],
                              consultas.comprometido_vs_executado(
                                  f, nivel=_nivel_do_pedido(),
                                  tipo=request.args.get("tipo", "pagar"), limite=1000))],
        "aportes": _abas_de_aporte,
        "cenario": _abas_do_cenario,
        # o relatorio inteiro, na ordem da tela antiga
        "completo": lambda: [
            ("DRE", C["dre"], _dre()),
            # com a linha dos encargos, senao esta aba nao fecha com a do DRE
            ("Despesas Categoria", C["despesas"],
             consultas.despesas_por_categoria_com_encargo(f, limite=1000)),
            ("Top Credores", C["credores"], consultas.top_credores(f, limite=1000)),
            ("Receita de Obra", C["medicoes"], consultas.medicoes(f, limite=20000)),
            ("Outras Receitas", C["outras"], consultas.outras_receitas(f, limite=500)),
            ("Despesas Analitico", C["analitico"], _analitico()),
            ("Fluxo de Caixa", C["fluxo"], consultas.caixa_por_mes(f)),
            ("Resultado por Obra", C["obras"],
             consultas.resultado_por(f, nivel="obra", limite=1000)),
        ],
    }
    if assunto in ("quotas", "posicao"):
        calculo = _calcular_prestacao(request.args.get("medida", "comprometido"))
        abas = [(("Quotas" if assunto == "quotas" else "Posicao dos Socios"),
                 C[assunto], calculo[assunto])]
        if assunto == "posicao":
            # A planilha da prestacao leva a conta que a sustenta: por projeto
            # e por obra, como na tela.
            abas.append(("Resultado por Projeto", C["prestacao_projeto"],
                         [dict(projeto=nome or "(sem projeto)", **n)
                          for nome, n in sorted(calculo["por_projeto"].items(),
                                                key=lambda kv: -kv[1]["resultado"])]))
            abas.append(("Resultado por Obra", C["prestacao_obra"],
                         [dict(obra=nome,
                               projeto=(calculo["obras"].get(nome) or {}).get("projeto") or "(sem projeto)",
                               **n)
                          for nome, n in sorted(calculo["por_obra"].items(),
                                                key=lambda kv: kv[1]["resultado"])]))
    elif assunto in montadores:
        abas = montadores[assunto]()
    else:
        return jsonify({"ok": False, "erro": f"Não sei exportar '{assunto}'."}), 404

    # O PDF sai das MESMAS abas que a planilha. Nao ha um montador para cada
    # formato: se houvesse, o dia em que os dois discordassem ninguem saberia
    # qual esta certo.
    if request.args.get("formato") == "pdf":
        from . import pdf as pdf_painel
        conteudo = pdf_painel.montar(
            abas, titulo="Relatório Financeiro BWS Construções",
            subtitulo=_subtitulo_do_relatorio(f),
            resumo=_resumo_do_relatorio(f),
            observacao=("Os mesmos números da planilha do botão ao lado. "
                        "As listas longas saem cortadas aqui e completas lá."),
            graficos_iniciais=_graficos_do_relatorio(f) if assunto == "completo" else ())
        return Response(
            conteudo, mimetype="application/pdf",
            headers={"Content-Disposition":
                     f'attachment; filename="{pdf_painel.nome_do_arquivo(assunto)}"'})

    conteudo = excel.montar(abas)
    nome = excel.nome_do_arquivo(assunto)
    return Response(
        conteudo,
        mimetype=("application/vnd.openxmlformats-officedocument"
                  ".spreadsheetml.sheet"),
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
