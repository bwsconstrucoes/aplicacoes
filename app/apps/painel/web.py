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


@bp.record_once
def _ligar_o_vigia(_estado):
    """Liga, no serviço de verdade, o vigia que retoma atualização interrompida
    (dono, 06/10/2026: "não tem como ficar em looping até finalizar?").

    Só sob o gunicorn: nos testes e no `python app/main.py` uma thread presa em
    `sleep` não tem o que vigiar. Uma por processo — o `record_once` garante."""
    import sys
    import threading
    if "gunicorn" not in " ".join(sys.argv) or os.getenv("PAINEL_SEM_VIGIA"):
        return
    from . import tarefas
    threading.Thread(target=tarefas.vigiar_para_sempre, name="painel-vigia",
                     daemon=True).start()
    logger.info("Painel: vigia das atualizações ligado.")


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
        atropelar o filtro.

        A CONTA CORRENTE entrou aqui em 23/09/2026: o dono filtrou o
        Calendario por conta, mudou de mes, e o filtro sumiu — os botoes de
        mes passam por esta funcao, e ela nao levava a conta. Vale para toda
        troca de tela, como os outros tres."""
        args = {c: request.args.getlist(c) for c in
                ("ano", "projeto", "obra", "conta") if request.args.getlist(c)}
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
        primeira = next((a for a in ABAS if a[0] in set(pessoa["telas"])
                         - ABAS_SO_DO_ADMINISTRADOR), None)
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


# A "Prestação de Contas" abre o cenário, que mostra a empresa inteira e a
# parte de cada sócio — só do administrador (11/11/2026). Quem tinha a tela
# liberada no cadastro de acesso não a vê mais no menu.
ABAS_SO_DO_ADMINISTRADOR = frozenset({"prestacao"})


def _abas_visiveis():
    """As abas do topo que a pessoa pode abrir.

    Mostrar uma aba que responde "nao encontrado" ao ser clicada seria pior que
    nao mostrar: a pessoa acha que o sistema esta com defeito."""
    from . import auth
    pessoa = auth.usuario_da_sessao()
    if pessoa is None:
        return ABAS
    liberadas = set(pessoa.get("telas") or []) - ABAS_SO_DO_ADMINISTRADOR
    return [aba for aba in ABAS if aba[0] in liberadas]


def _opcoes_no_escopo():
    """As listas da barra lateral, sem os nomes das obras que nao sao dela.

    Mostrar a lista inteira entregaria o nome de todas as obras da empresa a
    quem so pode ver uma — informacao que ele nao teria de outro jeito."""
    from . import auth, consultas
    opcoes = consultas.opcoes_de_filtro()
    pessoa = auth.usuario_da_sessao()
    obras_pedidas = [o for o in request.args.getlist("obra") if o]
    projetos_pedidos = [p for p in request.args.getlist("projeto") if p]
    contas_pedidas = [c for c in request.args.getlist("conta") if c]

    def _contas_de(obras, projetos):
        # AS CONTAS DA OBRA/PROJETO NA TELA (dono, 08/10/2026): a lista de
        # contas mostra as que tiveram dinheiro do recorte, e a que já está
        # marcada nunca some (senão não dava para desmarcar)
        do_recorte = set(consultas.contas_do_recorte(obras, projetos)) | set(contas_pedidas)
        return [c for c in opcoes.get("contas", []) if c in do_recorte]

    if pessoa is None:
        if obras_pedidas or projetos_pedidos:
            return dict(opcoes, contas=_contas_de(obras_pedidas, projetos_pedidos))
        return opcoes
    permitidas = set(pessoa.get("obras") or [])
    contas_ok = set(pessoa.get("contas") or [])
    # os projetos tambem: so os dela (liberados, ou os das obras dela). A
    # lista inteira entregaria o nome de todo projeto da empresa.
    mapa = consultas.obra_para_projeto() if permitidas else {}
    projetos_ok = (set(pessoa.get("projetos") or [])
                   | {mapa.get(o, "") for o in permitidas}) - {""}
    if contas_ok:
        # contas marcadas no cadastro continuam sendo um LIMITE: só elas
        contas = [c for c in opcoes.get("contas", []) if c in contas_ok]
    else:
        # sem nada marcado: as contas por onde andou dinheiro das obras dela
        # (ou só das escolhidas na barra). Ela já vê esses pagamentos, com a
        # conta, em toda tela — a lista não revela nada novo.
        obras_vistas = [o for o in obras_pedidas if o in permitidas] or sorted(permitidas)
        contas = _contas_de(obras_vistas, [p for p in projetos_pedidos if p in projetos_ok])
    return dict(opcoes,
                obras=[o for o in opcoes["obras"] if o in permitidas],
                projetos=[p for p in opcoes.get("projetos", []) if p in projetos_ok],
                contas=contas)


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
        # o relatório completo: o dono, e quem tem todas as telas que ele junta
        "pode_baixar_completo": auth.pode_baixar("completo"),
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
        # os ultimos 36 meses no grafico; alem disso as barras ficam ilegiveis.
        # E no maximo um ano a frente (06/10/2026), como no PDF.
        grafico_mensal=graficos.barras_agrupadas(
            consultas.ate_um_ano_a_frente(mensal)[-36:],
            [("receita", "b-receita", "Receita"), ("despesa", "b-despesa", "Despesa")],
            campo_rotulo="rotulo", campo_linha="acumulado"),
        **extra,
    )


@bp.route("/dre/medicoes")
def dre_medicoes():
    """As medicoes por tras de um numero do DRE — para a janela que abre ao
    clicar em "Executado" ou "Em aberto" na linha de receita.

    Pedido do dono em 23/09/2026: "num clique, visualizar a receita executada;
    num clique, a receita em aberto — no modal, as medicoes". So e lido quando
    alguem clica, com os mesmos filtros da tela: a mesma consulta da Receita
    de Obra, um titulo por linha."""
    from . import consultas
    f = _filtros_do_pedido()
    visao = request.args.get("visao", "todas")
    if visao not in ("todas", "quitadas", "a_receber"):
        visao = "todas"
    linhas = consultas.medicoes(f, visao=visao, limite=300)
    for l in linhas:
        l["data"] = l["data"].isoformat() if l.get("data") else ""
        l["abrir"] = url_for("painel.receita_titulo", codigo=l["codigo"])
    # a aba Receita de Obra pode nao estar liberada para quem esta olhando:
    # o link so aparece quando abre
    pode_abrir = any(chave == "receita" for chave, _r, _e in _abas_visiveis())
    return jsonify({"ok": True, "visao": visao, "linhas": linhas,
                    "pode_abrir": pode_abrir,
                    "ver_tudo": com_filtros_para_json("painel.receita", visao=visao),
                    "total": consultas.total_das_medicoes(f, visao=visao)})


@bp.route("/dre/despesas")
def dre_despesas():
    """Os lancamentos por tras de um numero de despesa do DRE (dono,
    23/09/2026): "clicar e abrir uma janelinha com as despesas". A mesma
    consulta do Despesas Analitico, com os filtros da tela."""
    from . import consultas
    f = _filtros_do_pedido()
    grupo = request.args.get("grupo", "").strip()
    visao = request.args.get("visao", "comprometido")
    if visao not in ("executado", "aberto", "comprometido"):
        visao = "comprometido"
    dados = consultas.analitico_despesas(f, grupo=grupo, visao=visao,
                                         ordem="valor", por_pagina=300)
    for l in dados["linhas"]:
        for campo in ("data", "data_vencimento", "data_pagamento"):
            l[campo] = l[campo].isoformat() if l.get(campo) else ""
    pode_abrir = any(chave == "analitico" for chave, _r, _e in _abas_visiveis())
    return jsonify({"ok": True, "grupo": grupo, "visao": visao,
                    "linhas": dados["linhas"], "quantos": dados["quantos"],
                    "total_pago": dados["total_pago"],
                    "total_a_pagar": dados["total_a_pagar"],
                    "total_encargo": dados["total_encargo"],
                    "total": dados["total"], "pode_abrir": pode_abrir,
                    "ver_tudo": com_filtros_para_json(
                        "painel.analitico", visao=visao,
                        **({"grupo": grupo} if grupo else {}))})


@bp.route("/dre/dividendos")
def dre_dividendos():
    """Os lancamentos de dividendo por tras de um numero do bloco de aportes
    (dono, 24/09/2026): titulo, data, socio, obra, conta, documento."""
    from . import consultas
    f = _filtros_do_pedido()
    sentido = request.args.get("sentido", "pago")
    if sentido not in ("pago", "recebido", "todos"):
        sentido = "pago"
    dados = consultas.lancamentos_de_dividendo(
        f, socio_id=(request.args.get("socio") or "").strip(),
        obra=(request.args.get("obra_do_dividendo") or "").strip(),
        sentido=sentido,
        com_transferencias=request.args.get("com_trf", "1") != "0")
    for l in dados["linhas"]:
        l["data"] = l["data"].isoformat() if l.get("data") else ""
    return jsonify({"ok": True, "sentido": sentido, **dados})


@bp.route("/dre/conferir-aportes")
def dre_conferir_aportes():
    """Por que um aporte ou dividendo nao aparece no bloco (dono, 06/10/2026):
    os candidatos de uma obra, ou um titulo pelo numero, cada um com o motivo."""
    from . import consultas
    f = _filtros_do_pedido()
    obra = (request.args.get("obra_conferida") or "").strip()
    codigo = (request.args.get("codigo") or "").strip()
    if not obra and not codigo:
        return jsonify({"ok": False, "erro": "Escolha uma obra ou digite o número."}), 400
    dados = consultas.conferir_aportes(f, obra=obra, codigo=codigo,
                                       escopo=_escopo_das_partes(f))
    for l in dados["linhas"]:
        l["data"] = l["data"].isoformat() if l.get("data") else ""
    return jsonify({"ok": True, **dados})


@bp.route("/dre/retencoes")
def dre_retencoes():
    """As retencoes por tras do numero do DRE, abertas por tributo."""
    from . import consultas
    f = _filtros_do_pedido()
    visao = request.args.get("visao", "todas")
    if visao not in ("todas", "quitadas", "a_receber"):
        visao = "todas"
    dados = consultas.retencoes_por_tributo(f, visao=visao)
    for l in dados["linhas"]:
        l["data"] = l["data"].isoformat() if l.get("data") else ""
    return jsonify({"ok": True, "visao": visao, **dados})


def com_filtros_para_json(rota, **extras):
    """O mesmo `com_filtros` das telas, disponivel fora do template."""
    args = {c: request.args.getlist(c) for c in
            ("ano", "projeto", "obra", "conta") if request.args.getlist(c)}
    if request.args.get("trf"):
        args["trf"] = "1"
    args.update(extras)
    return url_for(rota, **args)


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
    # SEMPRE agrupado: um pagamento dividido entre obras é uma linha, com as
    # obras por baixo (dono, 08/10/2026: "tem que ficar o lançamento sempre
    # agrupado e eu poder expandir"). A opção de desagrupar saiu.
    agrupar = True
    return render_template(
        "painel_analitico.html",
        **_contexto_comum("analitico"),
        chips=f.resumo(),
        grupo=grupo, categoria=categoria, credor=credor, busca=busca,
        visao=visao, ordem=ordem, de=de, ate=ate, base=base, agrupar=agrupar,
        bases=consultas.BASES_DE_DATA,
        # as colunas novas nascem vazias; a tela avisa em vez de mostrar
        # travessão e deixar parecer que o dado não existe
        duas_datas=consultas.duas_datas_prontas(),
        opcoes_analitico=consultas.opcoes_do_analitico(f),
        dados=consultas.analitico_despesas(
            f, grupo=grupo, categoria=categoria, credor=credor, busca=busca,
            visao=visao, ordem=ordem, de=de, ate=ate, base=base, pagina=pagina,
            agrupar=agrupar, marcar_partes=True,
            escopo_das_partes=_escopo_das_partes(f)),
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
        "analise": request.args.get("analise", "") if request.args.get("analise", "")
        in consultas_analises_do_calendario() else "",
    }


def consultas_analises_do_calendario():
    from . import consultas
    return consultas.ANALISES_DO_CALENDARIO


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
        analises=consultas.ANALISES_DO_CALENDARIO,
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
    # Um pagamento dividido entre obras vira UMA linha, como no extrato do
    # banco, com as partes por baixo (dono, 06/10/2026).
    grupos = consultas.agrupar_por_movimento(
        linhas, consultas.partes_dos_movimentos(
            [l["codigo"] for l in linhas], _escopo_das_partes(f)))
    for l in linhas:
        l["data"] = l["data"].isoformat() if l.get("data") else ""
    entradas = sum(l["valor"] for l in linhas if l["valor"] > 0 and not l["em_aberto"])
    saidas = sum(l["valor"] for l in linhas if l["valor"] < 0 and not l["em_aberto"])
    a_pagar = sum(l["valor"] for l in linhas if l["em_aberto"])
    return jsonify({"ok": True, "dia": dia, "linhas": linhas, "grupos": grupos,
                    "quantos": len(linhas), "entradas": entradas,
                    "saidas": saidas, "liquido": entradas + saidas,
                    "a_pagar": a_pagar})


def _escopo_das_partes(f):
    """Onde contar as partes de um pagamento dividido entre obras.

    O dono vê a base inteira: o "no extrato" é o débito todo, mesmo com a tela
    filtrada numa obra. Quem está preso a obras só conta as partes que já pode
    ver — somar a da obra de outro revelaria quanto foi para ela."""
    from . import auth
    from .consultas import Filtros
    if auth.usuario_da_sessao() is None:
        return None
    return Filtros(departamentos=f.departamentos, contas=f.contas, excluir_trf=False)


_MES_ISO = re.compile(r"^\d{4}-\d{2}$")
# a conferência de saldo aceita também o ANO inteiro ("2026") — dono, 07/10/2026
_MES_OU_ANO = re.compile(r"^\d{4}(-\d{2})?$")


@bp.route("/conferir/saldo")
def conferir_saldo():
    """O saldo de cada conta no mes, painel x OMIE (dono, 07/10/2026). So do
    administrador (prefixo painel.conferir_)."""
    from . import conferencia_saldo
    mes = (request.args.get("mes") or "").strip()
    if not _MES_OU_ANO.match(mes):
        return jsonify({"ok": False, "erro": "Escolha o mês ou o ano."}), 400
    try:
        return jsonify({"ok": True, **conferencia_saldo.conferir_mes(mes)})
    except Exception as e:  # noqa: BLE001 — credencial, OMIE fora
        logger.exception("Painel: conferencia de saldo de %s falhou", mes)
        return jsonify({"ok": False, "erro": f"Não consegui conferir agora: {e}"}), 502


@bp.route("/conferir/saldo/json")
def conferir_saldo_json():
    """A resposta crua do extrato do OMIE de uma conta num mes."""
    import json as _json
    from flask import Response
    from . import conferencia_saldo
    mes = (request.args.get("mes") or "").strip()
    conta = (request.args.get("conta") or "").strip()
    if not _MES_OU_ANO.match(mes) or not conta.isdigit():
        return jsonify({"ok": False, "erro": "Conta ou período inválido."}), 400
    try:
        resposta = conferencia_saldo.extrato_cru(int(conta), mes)
    except Exception as e:  # noqa: BLE001
        return jsonify({"ok": False, "erro": f"Não consegui ler o OMIE agora: {e}"}), 502
    return Response(_json.dumps(resposta, ensure_ascii=False, indent=2, default=str),
                    mimetype="application/json",
                    headers={"Content-Disposition":
                             f'attachment; filename="omie_extrato_{conta}_{mes}.json"'})


@bp.route("/conferir/lancamento-cc/json")
def conferir_lancamento_cc_json():
    """UM lançamento de conta corrente, cru, como o OMIE responde à consulta
    dele (07/10/2026). É onde mora a apropriação que o movimento financeiro não
    traz; serve para conferir o formato contra o OMIE real. So do
    administrador (prefixo painel.conferir_)."""
    import json as _json
    from flask import Response
    from .sync.omie_client import TETO_DE_ESPERA_NA_TELA, OmieClient
    codigo = (request.args.get("codigo") or "").strip()
    if not codigo.isdigit():
        return jsonify({"ok": False, "erro": "Informe o número do lançamento."}), 400
    try:
        resposta = OmieClient.de_ambiente(
            timeout=30, max_tentativas=3,
            teto_de_espera=TETO_DE_ESPERA_NA_TELA).consultar_lancamento_cc(int(codigo))
    except Exception as e:  # noqa: BLE001 — OMIE fora, número que não existe
        return jsonify({"ok": False, "erro": f"O OMIE não respondeu: {e}"}), 502
    return Response(_json.dumps(resposta, ensure_ascii=False, indent=2, default=str),
                    mimetype="application/json",
                    headers={"Content-Disposition":
                             f'attachment; filename="omie_lancamento_cc_{codigo}.json"'})


@bp.route("/conferir/dia/json")
def conferir_dia_json():
    """O que o OMIE manda num dia, CRU, como arquivo (dono, 07/10/2026: "a
    gente baixa toda a informação do OMIE (...) por que essa falha de
    interpretação?"). É a fonte da verdade para desenhar regra: o que existe
    no JSON e o painel não lê. So do administrador (prefixo painel.conferir_).
    Usa a mesma leitura guardada por 150 s da conferencia — conferir e baixar
    o mesmo dia nao gasta duas chamadas iguais no OMIE."""
    import json as _json
    from . import conferencia_omie
    dia = (request.args.get("dia") or "").strip()
    if not _DATA_ISO.match(dia):
        return jsonify({"ok": False, "erro": "Dia inválido."}), 400
    try:
        registros = conferencia_omie.registros_do_omie(dia)
    except Exception as e:  # noqa: BLE001 — OMIE fora, credencial, limite
        logger.exception("Painel: leitura crua do dia %s no OMIE falhou", dia)
        return jsonify({"ok": False, "erro": f"Não consegui ler o OMIE agora: {e}"}), 502
    corpo = _json.dumps({"dia": dia, "movimentos": registros},
                        ensure_ascii=False, indent=2, default=str)
    from flask import Response
    return Response(corpo, mimetype="application/json",
                    headers={"Content-Disposition":
                             f'attachment; filename="omie_movimentos_{dia}.json"'})


@bp.route("/conferir/dia")
def conferir_dia():
    """Confere UM dia do painel com o OMIE, lendo o OMIE na hora.

    So do administrador (prefixo "painel.conferir_" em SO_DO_ADMINISTRADOR):
    le a empresa inteira do dia e chama a API do OMIE."""
    from . import conferencia_omie
    dia = (request.args.get("dia") or "").strip()
    if not _DATA_ISO.match(dia):
        return jsonify({"ok": False, "erro": "Dia inválido."}), 400
    try:
        resultado = conferencia_omie.conferir_dia(dia)
    except Exception as e:  # noqa: BLE001 — OMIE fora, credencial, limite
        logger.exception("Painel: conferencia do dia %s com o OMIE falhou", dia)
        return jsonify({"ok": False, "erro": f"Não consegui ler o OMIE agora: {e}"}), 502
    return jsonify({"ok": True, **resultado})


@bp.route("/conferir/dia/trazer", methods=["POST"])
def conferir_dia_trazer():
    """Substitui o dia no espelho pelo que o OMIE tem, e refaz os numeros.

    Nao escreve no OMIE. E o mesmo que a atualizacao faria se o dia estivesse
    na janela dela."""
    from . import conferencia_omie, tarefas
    dia = (request.form.get("dia") or request.args.get("dia") or "").strip()
    if not _DATA_ISO.match(dia):
        return jsonify({"ok": False, "erro": "Dia inválido."}), 400
    try:
        feito = conferencia_omie.trazer_dia_do_omie(dia)
    except Exception as e:  # noqa: BLE001
        logger.exception("Painel: trazer o dia %s do OMIE falhou", dia)
        return jsonify({"ok": False, "erro": f"Não consegui trazer do OMIE: {e}"}), 502
    recalculo = tarefas.disparar("so_numeros", "conferência com o OMIE")
    return jsonify({"ok": True, **feito,
                    "recalculo": recalculo.get("ok", False),
                    "aviso": ("Os números estão sendo refeitos — em alguns minutos a "
                              "tela mostra o dia corrigido." if recalculo.get("ok")
                              else recalculo.get("erro", ""))})


@bp.route("/titulo/<int:codigo>/conta")
def conta_do_titulo(codigo):
    """De onde o painel tirou a conta de um titulo — as pernas da baixa no
    espelho do OMIE. So do administrador (a lista SO_DO_ADMINISTRADOR cobre o
    prefixo): mostra contas e movimentos crus, e e ferramenta de conferencia."""
    from . import consultas
    dados = consultas.origem_da_conta(codigo)
    if dados is None:
        return jsonify({"ok": False, "erro": "Título não encontrado."}), 404
    for p in dados["pernas"]:
        p["data"] = str(p["data"] or "")
    return jsonify({"ok": True, **dados})


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


def _cortes(config, itens, apuracao) -> dict:
    """{obra: último mês considerado}, dos itens "até MM/AAAA" (11/11/2026)."""
    from . import consultas, prestacao
    if not itens:
        return {}
    deptos = _deptos_administrativos(config)
    return prestacao.cortes_da_analise(
        itens, consultas.obra_para_projeto(),
        {(l.get("obra") or "").strip() for l in apuracao} - set(deptos))


@bp.route("/prestacao")
def prestacao_contas():
    """A aba "Prestação de Contas" do menu: abre o resultado do cenário.

    Até 11/11/2026 abria a prestação ANTIGA — outra conta, com regras e
    participações próprias (Parâmetros › Regras de rateio e Participações),
    em paralelo ao cenário. O dono: "eu não sei o que é essa prestação de conta
    antiga (…) se não interessa mais, pode tirar". Eram dois caminhos para a
    mesma pergunta, com números que podiam divergir. Ficou um só.

    Só do administrador (está em `auth.SO_DO_ADMINISTRADOR`): o cenário mostra
    a empresa inteira e a parte de cada sócio."""
    from . import cenarios, consultas
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    if cenarios.listar():
        return redirect(url_for("painel.cenario_resultado"))
    return redirect(url_for("painel.cenario_montagem"))


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
    # PROJETO QUE NÃO RECEBE não aparece na prestação (dono, 09/11/2026: "os
    # projetos que não receberam nenhum percentual não precisam aparecer,
    # porque realmente não interessa"). Sai como "fora da análise": não
    # recebe estrutura nem juros, não pesa na régua e não vai para as quotas.
    quem = cenario.get("projetos")
    if quem is None:
        quem = cenarios.projetos(cenario["id"])
    nao_recebem = [f"projeto:{p}" for p, (papel, _d, _a) in (quem.get("regras") or {}).items()
                   if papel == "nao"]
    # ... MAS O CAIXA DELE CONTA NOS JUROS (dono, 10/11/2026): "todos os demais
    # projetos se ajudam — o caixa gerado pelas obras serve para cobrir o
    # buraco de obra que ainda não recebeu". Por isso há duas listas: a de quem
    # está fora de TUDO, e a de quem só não aparece no resultado.
    fora_de_tudo = _obras_fora(config, list(itens_gerais) + list(itens_fora or []),
                               apuracao)
    obras_do_caixa = prestacao.classificar_obras(
        [l for l in apuracao if (l.get("obra") or "").strip() not in fora_de_tudo],
        config)
    fora = _obras_fora(config, list(itens_gerais) + list(itens_fora or []) + nao_recebem,
                       apuracao)
    # SÓ ATÉ UM MÊS (11/11/2026): a obra conta até ele e sai dali em diante —
    # da apuração, da régua e do caixa
    cortes = _cortes(config, list(itens_gerais) + list(itens_fora or []), apuracao)
    if cortes:
        apuracao = [l for l in apuracao
                    if not prestacao.depois_do_corte(l.get("mes"), l.get("obra"), cortes)]
        obras_do_caixa = prestacao.classificar_obras(
            [l for l in apuracao if (l.get("obra") or "").strip() not in fora_de_tudo],
            config)
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
                         and o not in consultas.SEM_OBRA_OU_NAO_LIDA)

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
    if por_deficit:
        excecoes = prestacao.tirar_juros_das_excecoes(excecoes, categoria_juros)

    # A regua de quem recebe: mao de obra ou faturamento, como o dono escolheu.
    pessoal = [t for t in consultas.custo_de_pessoal_por_obra_mes(config["grupo_pessoal"], medida)
               if not prestacao.depois_do_corte(t[0], t[1], cortes)]
    driver = ([t for t in consultas.receita_por_obra_mes(medida)
               if not prestacao.depois_do_corte(t[0], t[1], cortes)]
              if cenario.get("criterio") == "faturamento" else pessoal)
    if fora:
        driver = [d for d in driver if d[1] not in fora]

    # Os dois lados e quem recebe (09/11/2026): o projeto principal ("BWS CE",
    # o da matriz na configuracao, se o cenario nao disser outro) contra os
    # demais; e os projetos que nao recebem, ou so recebem num periodo.
    principal = (quem.get("principal") or config.get("projeto_matriz") or "").strip()
    regras_proj = quem.get("regras") or {}
    # SEM PROJETO NÃO FICA EM CANTO NENHUM (dono, 10/11/2026): "não apropriado
    # eu tenho que analisar e apropriar. E obra sem projeto não é para ter."
    # Enquanto o dado não for corrigido, essas linhas aparecem no resultado
    # (para serem vistas), mas não recebem estrutura nem juros, não pesam na
    # régua e não entram no caixa de lado nenhum.
    def _sem_lado(obra, info):
        return obra in consultas.SEM_OBRA_OU_NAO_LIDA or not (info.get("projeto") or "").strip()

    lado_de = {o: ("principal" if (info.get("projeto") or "").strip() == principal
                   else "demais") for o, info in obras.items()
               if not _sem_lado(o, info)}
    driver = [d for d in driver if d[1] in lado_de]

    def _recebe(obra, mes):
        if obra not in lado_de:
            return False
        projeto = ((obras.get(obra) or {}).get("projeto") or "").strip()
        return cenarios.recebe_no_mes(regras_proj.get(projeto), mes)

    rateio = prestacao.calcular_rateio_do_cenario(
        admin_sem_juros, excecoes, driver, obras, pesos,
        pct_padrao=float(cenario.get("pct_padrao", 100) or 0),
        janela=str(cenario.get("janela", "1")),
        lado_de=lado_de, recebe=_recebe)

    # O caixa e lido UMA vez e devolvido: a tela de resultado desenha a vida de
    # uma obra com ele, e reler significaria varrer o fato inteiro de novo.
    # O aporte que a obra recebeu abate o buraco dela (dono, 10/11/2026).
    caixa = [(mes, obra, valor) for mes, obra, valor in
             [(m.strftime("%Y-%m"), o, v)
              for m, o, v in consultas.caixa_mensal_por_obra()]
             + consultas.aporte_mensal_por_obra()
             if obra not in fora_de_tudo
             and not prestacao.depois_do_corte(mes, obra, cortes)]
    lado_do_caixa = {o: ("principal" if principal and
                         (info.get("projeto") or "").strip() == principal
                         else "demais") for o, info in obras_do_caixa.items()
                     if not _sem_lado(o, info)}
    if por_deficit:
        juros = prestacao.alocar_juros_por_deficit(
            caixa, juros_por_mes, obras_do_caixa,
            rateio_recebido=rateio["alocacoes"],
            sem_deficit=str(cenario.get("juros_sem_deficit", "sobra")),
            # ponto de corte (dono, 10/11/2026): "terminou de gastar com mão
            # de obra, terminou de receber juros"
            corte=prestacao.corte_dos_juros(pessoal, caixa, obras_do_caixa),
            lado_de=lado_do_caixa)
    else:
        juros = {"alocacoes": {}, "sobras": [], "memoria": []}

    apurado = prestacao.apurar(apuracao, obras, rateio["alocacoes"],
                               juros["alocacoes"])
    por_obra = prestacao.totalizar_por_obra(apurado)
    participacoes = cenario.get("participacoes") or cenarios.participacoes(cenario["id"])
    quotas = prestacao.quotas_por_obra(
        por_obra, participacoes, float(cenario.get("taxa_adm_pct", 0) or 0),
        projeto_de={o: info.get("projeto") or "" for o, info in obras.items()})
    ajustes = prestacao_dados.ajustes()
    return {
        "cenario": cenario, "obras": obras, "rateio": rateio, "juros": juros,
        "apurado": apurado, "por_obra": por_obra, "quotas": quotas,
        "driver": driver, "caixa": caixa, "excluidas": sorted(fora),
        "cortes": cortes,
        "excluidas_gerais": list(itens_gerais),
        "sem_projeto": sem_projeto,
        "tem_nao_apropriado": any(o in obras for o in consultas.SEM_OBRA_OU_NAO_LIDA),
        "posicao": prestacao.posicao_dos_socios(quotas, ajustes),
        "sobras": rateio["sobras"] + juros["sobras"],
        # O BATIMENTO (10/11/2026): o custo global da matriz contra a soma do
        # que foi para cada lado, dos juros, do sem dono e do que ficou de
        # fora — o sistema confere, e o relatório deixa conferir à mão.
        "batimento": prestacao.batimento(admin, rateio, juros, lado_de),
        "fora_por_conta": rateio.get("fora_por_conta", []),
        "criticas_participacoes": prestacao.criticas_das_participacoes(participacoes),
        # juro que caiu em obra de projeto que "não recebe": conta no
        # batimento, mas a obra não aparece no resultado
        "juros_fora_do_resultado": sum(v for (o, _m), v in juros["alocacoes"].items()
                                       if o not in obras),
        "principal": principal,
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
    primeiro_mes = ""
    for linha in consultas.despesa_administrativa(deptos, medida):
        if linha["mes"] != consultas.SEM_DATA and (not primeiro_mes or linha["mes"] < primeiro_mes):
            primeiro_mes = linha["mes"]
        nome = linha.get("grupo") or "(sem grupo)"
        cat = linha.get("categoria") or "(sem categoria)"
        g = grupos.setdefault(nome, {"grupo": nome, "valor": 0.0, "categorias": {}})
        g["valor"] += linha["valor"]
        c = g["categorias"].setdefault(cat, {"categoria": cat, "valor": 0.0,
                                             "entra_balde": 0.0})
        c["valor"] += linha["valor"]
        # mês a mês: o percentual pode mudar com o tempo (09/11/2026)
        c["entra_balde"] += linha["valor"] * prestacao.peso_da_conta(
            pesos, nome, cat, "", padrao, mes=linha["mes"]) / 100.0
    per = pesos.get("periodos") or {}

    marcacoes: list[dict] = []
    saida = []
    for g in grupos.values():
        g["pct"] = (pesos.get("grupo") or {}).get(g["grupo"])
        g["pct_efetivo"] = prestacao.peso_da_conta(pesos, g["grupo"], "", "", padrao)
        g["periodos"] = (per.get("grupo") or {}).get(g["grupo"], [])
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
            c["periodos"] = (per.get("categoria") or {}).get(c["categoria"], [])
            # o percentual dela muda com o tempo — dela, ou herdado do grupo
            c["varia"] = bool(c["periodos"] or (g["periodos"] and c["pct"] is None))
            c["e_juros"] = c["categoria"].strip().lower() in juros
            proprias = por_categoria.get((g["grupo"], c["categoria"]), [])
            c["excecoes"] = len(proprias)
            if c["e_juros"]:
                c["entra"], c["fora"], c["juros"] = 0.0, 0.0, c["valor"]
            else:
                # o balde já foi somado mês a mês; o lançamento marcado sai
                # dele (pelo percentual da categoria naquele mês) e entra com
                # o próprio
                entra = c["entra_balde"]
                for e in proprias:
                    mes_e = e.get("mes")
                    entra -= e["valor"] * prestacao.peso_da_conta(
                        pesos, g["grupo"], c["categoria"], "", padrao, mes=mes_e) / 100.0
                    pct = prestacao.peso_da_conta(
                        pesos, g["grupo"], c["categoria"], e["codigo"], padrao, mes=mes_e)
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
    return {"contas": contas, "resumo": resumo, "marcacoes": marcacoes,
            "primeiro_mes": primeiro_mes}


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
        primeiro_mes=arvore.get("primeiro_mes", ""),
        socios=prestacao_dados.socios(apenas_ativos=True),
        obras_do_painel=_opcoes_no_escopo().get("obras", []),
        projetos_do_painel=_opcoes_no_escopo().get("projetos", []),
        **_grade_quem_divide(atual, _opcoes_no_escopo().get("obras", []),
                             _opcoes_no_escopo().get("projetos", [])),
        linhas_fora=_linhas_fora((atual or {}).get("excluidas") or [],
                                 _opcoes_no_escopo().get("projetos", []),
                                 _opcoes_no_escopo().get("obras", [])),
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
        # quem recebe o custo da matriz (09/11/2026)
        principal=((atual or {}).get("projetos", {}).get("principal")
                   or config.get("projeto_matriz") or ""),
        regras_projetos=(atual or {}).get("projetos", {}).get("regras", {}),
        papeis=cenarios.PAPEIS,
    )


def _periodos_do_formulario(form) -> list[tuple]:
    """(nível, chave, desde, percentual, parte do principal) de cada linha de
    período do formulário. A parte do principal pode faltar (formulário
    antigo): vira em branco, sem cortar as outras linhas."""
    niveis = form.getlist("periodo_nivel")
    colunas = [form.getlist(c) for c in ("periodo_chave", "periodo_desde",
                                         "periodo_pct", "periodo_principal")]
    linhas = []
    for i, nivel in enumerate(niveis):
        linhas.append((nivel,) + tuple(c[i] if i < len(c) else "" for c in colunas))
    return linhas


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
    elif acao == "pesos_da_arvore" and cenario_id:
        # a árvore inteira num clique (05/11/2026): grupo e categorias juntos
        cenarios.marcar_pesos_da_arvore(cenario_id, zip(
            request.form.getlist("item_nivel"), request.form.getlist("item_chave"),
            request.form.getlist("item_pct")))
        # e o percentual por período, de quem tem (09/11/2026)
        cenarios.gravar_periodos_da_arvore(
            cenario_id, request.form.getlist("periodo_presente"),
            _periodos_do_formulario(request.form))
    elif acao == "projetos" and cenario_id:
        # quem recebe o custo da matriz, e o projeto principal (09/11/2026)
        cenarios.gravar_projetos(
            cenario_id, request.form.get("principal", ""),
            {p: (papel, d, a) for p, papel, d, a in zip(
                request.form.getlist("proj_nome"), request.form.getlist("proj_papel"),
                request.form.getlist("proj_desde"), request.form.getlist("proj_ate"))})
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
    elif acao == "participacoes_em_grade" and cenario_id:
        try:
            cenarios.gravar_participacoes_em_grade(cenario_id,
                                                   _celulas_da_grade(request.form))
        except ValueError as e:
            return redirect(url_for("painel.cenario_montagem", cenario=cenario_id,
                                    erro=str(e)))
    elif acao == "excluir" and cenario_id:
        for item in request.form.getlist("item"):
            cenarios.excluir(cenario_id, item)
    elif acao == "fora_em_lista" and cenario_id:
        try:
            cenarios.gravar_fora(cenario_id, _itens_fora_do_formulario(request.form))
        except ValueError as e:
            return redirect(url_for("painel.cenario_montagem", cenario=cenario_id,
                                    erro=str(e)))
    elif acao == "reincluir" and cenario_id:
        cenarios.reincluir(cenario_id, request.form.get("item", ""))

    return redirect(url_for("painel.cenario_montagem",
                            cenario=cenario_id or None,
                            grupo=request.form.get("grupo") or None,
                            categoria=request.form.get("categoria") or None))


def _periodos_por_extenso(lista, nome_principal) -> str:
    """ "50% desde 01/2021 · 40% desde 01/2022" — a parte do projeto principal
    de uma conta, para ler sem abrir nada. Vazio quando a conta não separa."""
    partes = []
    for desde, _pct, parte in lista or []:
        if parte is None:
            continue
        quando = ("sempre" if desde == "1900-01"
                  else f"a partir de {desde[5:]}/{desde[:4]}")
        partes.append(f"{parte:.0f}% {quando}")
    return " · ".join(partes)


def _explicacao_do_cenario(cenario, config) -> dict:
    """O cenário dito em português, para a tela "Como é feita" (11/11/2026, o
    dono: "eu quero mostrar essa tela para os outros sócios (…) eles precisam
    bater o olho e entender como é que a gente está fazendo").

    Não calcula nada novo: lê a mesma árvore da montagem e as mesmas listas, e
    as arruma na ordem em que a conta acontece."""
    from . import cenarios, prestacao, prestacao_dados
    P = prestacao.PROJETO_NA_PARTICIPACAO
    medida = cenario.get("medida") or "comprometido"
    arvore = _contas_da_matriz(config, cenario, medida)
    quem = cenario.get("projetos") or {}
    principal = (quem.get("principal") or config.get("projeto_matriz") or "").strip()

    grupos = []
    for g in arvore["contas"]:
        excecoes = []
        for c in g["categorias"]:
            if c.get("e_juros"):
                continue
            parte = _periodos_por_extenso(c.get("periodos"), principal)
            if c.get("pct") is not None or c.get("periodos"):
                excecoes.append({
                    "categoria": c["categoria"], "valor": c["valor"],
                    "entra_pct": ("varia" if c.get("varia") and not parte
                                  else f"{c['pct_efetivo']:.0f}%"),
                    "parte": parte})
        grupos.append({
            "grupo": g["grupo"], "valor": g["valor"], "entra": g["entra"],
            "fora": g["fora"], "juros": g.get("juros") or 0.0,
            "entra_pct": ("varia" if g.get("periodos") and not
                          _periodos_por_extenso(g.get("periodos"), principal)
                          else f"{g['pct_efetivo']:.0f}%"),
            "parte": _periodos_por_extenso(g.get("periodos"), principal),
            "excecoes": excecoes})

    regras = quem.get("regras") or {}
    nao_recebem = sorted(p for p, r in regras.items() if r[0] == "nao")
    so_periodo = sorted((p, r[1], r[2]) for p, r in regras.items() if r[0] == "periodo")

    # quem divide: uma linha por "todas as obras", por projeto e por obra
    socios = {s["id"]: s for s in prestacao_dados.socios()}
    linhas: dict[str, dict] = {}
    for p in cenario.get("participacoes") or []:
        linhas.setdefault(p["obra"] or "", {})[p["socio_id"]] = p["pct"]
    colunas = sorted({sid for l in linhas.values() for sid in l},
                     key=lambda i: (socios.get(i, {}).get("nome") or "").lower())

    def _rotulo(chave):
        if not chave:
            return "Todas as obras"
        return "Projeto " + chave[len(P):] if chave.startswith(P) else chave
    ordem = sorted(linhas, key=lambda k: (k != "", not k.startswith(P), k.lower()))
    quem_divide = [{"rotulo": _rotulo(k),
                    "pcts": [linhas[k].get(sid) for sid in colunas],
                    "soma": sum(linhas[k].values())} for k in ordem]

    itens_fora = list(prestacao_dados.fora_da_analise(config)) + list(cenario.get("excluidas") or [])
    fora_total, fora_ate = [], []
    for item in itens_fora:
        tipo, nome, ate = prestacao.ler_item_fora(item)
        if not tipo:
            continue
        rotulo = ("Projeto " if tipo == "projeto" else "") + nome
        (fora_ate if ate else fora_total).append(
            rotulo + (f" (até {ate[5:]}/{ate[:4]})" if ate else ""))

    return {
        "resumo": arvore["resumo"], "grupos": grupos, "principal": principal,
        "primeiro_mes": arvore.get("primeiro_mes", ""),
        "regua": cenarios.CRITERIOS.get(cenario.get("criterio"), "").split(" — ")[0],
        "regua_e_pessoal": cenario.get("criterio") != "faturamento",
        "janela": cenarios.JANELAS.get(str(cenario.get("janela", "1")), ""),
        "nao_recebem": nao_recebem, "so_periodo": so_periodo,
        "juros_por_deficit": bool(cenario.get("juros_por_deficit", 1)),
        "juros_sem_deficit": str(cenario.get("juros_sem_deficit", "sobra")),
        "categorias_de_juros": sorted(prestacao.categorias_de_juros(
            config.get("categoria_juros") or prestacao.CATEGORIA_JUROS_PADRAO)),
        "socios": [socios.get(i, {"nome": "?", "tipo": ""}) for i in colunas],
        "quem_divide": quem_divide,
        "taxa_adm": float(cenario.get("taxa_adm_pct", 0) or 0),
        "tem_externo": any((socios.get(i, {}).get("tipo") or "") == "Externo" for i in colunas),
        "fora_total": fora_total, "fora_ate": fora_ate,
        "medida_e_caixa": medida == "executado",
    }


@bp.route("/prestacao/como")
def cenario_como():
    """Como esta prestação é feita — o cenário explicado em passos, só para
    ler e mostrar. Nada se edita aqui."""
    from . import cenarios, consultas, prestacao_dados
    if consultas.base_vazia():
        return redirect(url_for("painel.configuracoes", primeira="1"))
    lista = cenarios.listar()
    atual = cenarios.completo(request.args.get("cenario") or
                              (lista[0]["id"] if lista else 0))
    if atual is None:
        return redirect(url_for("painel.cenario_montagem"))
    return render_template(
        "painel_cenario_como.html", **_contexto_comum("prestacao"),
        cenarios=lista, cenario=atual,
        e=_explicacao_do_cenario(atual, prestacao_dados.config()))


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
    # O QUE ESTÁ FORA DA PRESTAÇÃO NÃO APARECE PELO NOME (11/11/2026, o dono:
    # "desconsidera para efeito de rateio, mas ainda aparece na tela. Não quero
    # que apareça"). O caixa de projeto que não recebe conta nos juros, e a
    # obra dele saía na lista de juros; agora vira uma linha só, sem nome.
    visiveis = {nome for nome, _n in por_obra}
    escondidas = [l for l in obras_com_juros if l["obra"] not in visiveis]
    obras_com_juros = [l for l in obras_com_juros if l["obra"] in visiveis]
    if escondidas:
        obras_com_juros.append({
            "obra": f"Obras fora desta prestação ({len(escondidas)})",
            "valor": sum(l["valor"] for l in escondidas)})
    obra = request.args.get("obra_grafico") or (
        next((l["obra"] for l in obras_com_juros if l["obra"] in visiveis), "")
        or (por_obra[0][0] if por_obra else ""))
    if obra not in visiveis:
        obra = por_obra[0][0] if por_obra else ""
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
        batimento=calculo["batimento"],
        fora_por_conta=calculo["fora_por_conta"],
        criticas_participacoes=calculo["criticas_participacoes"],
        juros_fora_do_resultado=calculo["juros_fora_do_resultado"],
        principal=calculo["principal"],
        obra_grafico=obra, trilha=trilha, grafico=grafico,
        obras_para_grafico=[nome for nome, _n in por_obra],
        criterios=cenarios.CRITERIOS,
        excluidas=calculo["excluidas"],
        cortes=calculo.get("cortes") or {},
        sem_projeto=calculo["sem_projeto"],
        tem_nao_apropriado=calculo["tem_nao_apropriado"],
        nao_apropriado=consultas.SEM_OBRA,
    )


@bp.route("/prestacao/cenarios")
def prestacao_cenarios():
    """O simulador de regras da prestação antiga saiu em 11/11/2026 (ver
    `prestacao_contas`). O endereço continua, para o favorito de alguém não
    dar erro: leva à montagem do cenário, que é quem faz isso hoje."""
    return redirect(url_for("painel.cenario_montagem"))


@bp.route("/prestacao/cenarios/gravar", methods=["POST"])
def prestacao_cenarios_gravar():
    return redirect(url_for("painel.cenario_montagem"))


@bp.route("/prestacao/parametros", methods=["GET", "POST"])
def prestacao_parametros():
    """Socios, participacoes, regras de rateio e ajustes."""
    from . import consultas, prestacao_dados

    if request.method == "POST":
        try:
            _aplicar_mudanca_da_prestacao(prestacao_dados, request.form)
        except ValueError as e:
            # engano de digitação volta para a tela dito em português, não
            # como página de erro (11/11/2026, edição na própria linha)
            return redirect(url_for("painel.prestacao_parametros",
                                    aba=request.form.get("aba", "socios"),
                                    erro=str(e)))
        return redirect(url_for("painel.prestacao_parametros",
                                aba=request.form.get("aba", "socios")))

    aba = request.args.get("aba", "socios")
    if aba in ("participacoes", "regras"):
        # eram da prestação antiga (11/11/2026): quem divide e o que se
        # divide moram na montagem do cenário
        return redirect(url_for("painel.cenario_montagem"))
    # SÓ NA ABA QUE USA (11/11/2026, o dono: "clica, demora um tempo para
    # cadastrar"). Estas duas listas varrem a base inteira, e eram lidas a
    # cada salvada de QUALQUER aba — cadastrar um sócio esperava por elas.
    listas = (consultas.grupos_e_categorias()
              if aba == "geral" and not consultas.base_vazia()
              else {"grupos": [], "categorias": []})
    # Só na aba dela: a lista de obras agrupa o rateio inteiro, e as outras
    # abas não precisam disso.
    obras_projetos = prestacao_dados.obras_e_projetos() if aba == "projetos" else []
    return render_template(
        "painel_prestacao_config.html",
        **_contexto_comum("prestacao"),
        aba=aba,
        obras_projetos=obras_projetos,
        config=prestacao_dados.config(),
        socios=prestacao_dados.socios(),
        socios_ativos=prestacao_dados.socios(apenas_ativos=True),
        ajustes=prestacao_dados.ajustes(),
        tipos_ajuste=prestacao_dados.TIPOS_AJUSTE,
        escopos=prestacao_dados.ESCOPOS,
        projetos=consultas.opcoes_de_filtro()["projetos"] if not consultas.base_vazia() else [],
        fora=prestacao_dados.fora_da_analise(),
        linhas_fora=(_linhas_fora(prestacao_dados.fora_da_analise(),
                                  _opcoes_no_escopo().get("projetos", []),
                                  _opcoes_no_escopo().get("obras", []))
                     if aba == "fora" and not consultas.base_vazia() else []),
        obras_do_painel=_opcoes_no_escopo().get("obras", []) if not consultas.base_vazia() else [],
        projetos_do_painel=_opcoes_no_escopo().get("projetos", []) if not consultas.base_vazia() else [],
        # as categorias com cara de juro vêm primeiro na lista dos juros
        parecidas_com_juro=[c for c in listas.get("categorias", [])
                            if re.search(r"jur|iof|empr[eé]st|financ|banc|tarifa", c, re.I)],
        **listas,
    )


def _grade_quem_divide(cenario, obras, projetos=()) -> dict:
    """A grade do "Quem divide" do cenário: "Todas as obras" primeiro, depois
    cada PROJETO (sempre à vista — é por onde se configura), depois as obras
    com percentual próprio; as outras obras aparecem ao procurar."""
    from .prestacao import PROJETO_NA_PARTICIPACAO as P
    grade: dict = {}
    for p in (cenario or {}).get("participacoes") or []:
        grade.setdefault(p["obra"] or "", {})[p["socio_id"]] = p["pct"]
    dos_projetos = sorted({P + n for n in projetos if n}
                          | {k for k in grade if k.startswith(P)}, key=str.lower)
    outras = sorted((set(obras) | {k for k in grade if not k.startswith(P)}) - {""},
                    key=lambda n: (n not in grade, n.lower()))
    linhas = ([{"chave": "", "rotulo": "Todas as obras", "fixa": True}]
              + [{"chave": k, "rotulo": "Projeto — " + k[len(P):], "fixa": True}
                 for k in dos_projetos]
              + [{"chave": o, "rotulo": o} for o in outras])
    return {"grade_quem_divide": grade, "linhas_quem_divide": linhas}


def _itens_fora_do_formulario(form) -> list[str]:
    """A lista "fora da análise" como a tela manda: para cada linha, a chave
    ("obra:X" / "projeto:Y"), o modo ("" considera, "total", "ate") e o mês."""
    from . import prestacao
    itens = []
    for chave, modo, ate in zip(form.getlist("fora_chave"), form.getlist("fora_modo"),
                                form.getlist("fora_ate")):
        tipo, nome, _ = prestacao.ler_item_fora(chave)
        if not tipo or modo not in ("total", "ate"):
            continue
        if modo == "ate":
            ate = (ate or "").strip()[:7]
            if not re.match(r"^\d{4}-\d{2}$", ate):
                raise ValueError(f"Falta o mês em {nome} (\"só até\" pede o mês).")
            itens.append(prestacao.montar_item_fora(tipo, nome, ate))
        else:
            itens.append(prestacao.montar_item_fora(tipo, nome))
    return itens


def _linhas_fora(itens, projetos, obras) -> list[dict]:
    """As linhas da lista "fora da análise": projetos primeiro, depois obras;
    as que já estão fora (ou com mês) em cima."""
    from . import prestacao
    estado = {}
    for item in itens or ():
        tipo, nome, ate = prestacao.ler_item_fora(item)
        if tipo:
            estado[f"{tipo}:{nome}"] = ("ate" if ate else "total", ate or "")
    linhas = []
    for tipo, nomes, rotulo in (("projeto", projetos, "Projeto — "), ("obra", obras, "")):
        todos = set(nomes or []) | {k.split(":", 1)[1] for k in estado
                                    if k.startswith(tipo + ":")}
        for nome in sorted(todos, key=lambda n: (f"{tipo}:{n}" not in estado, n.lower())):
            modo, ate = estado.get(f"{tipo}:{nome}", ("", ""))
            linhas.append({"chave": f"{tipo}:{nome}", "rotulo": rotulo + nome,
                           "modo": modo, "ate": ate})
    return linhas


@bp.app_template_filter("item_fora")
def _filtro_item_fora(item) -> str:
    """ "projeto:ALFA@2024-06" -> "Projeto — ALFA (até 06/2024)" """
    from . import prestacao
    tipo, nome, ate = prestacao.ler_item_fora(item)
    if not tipo:
        return str(item or "")
    texto = ("Projeto — " if tipo == "projeto" else "") + nome
    return texto + (f" (até {ate[5:]}/{ate[:4]})" if ate else "")


def _celulas_da_grade(form) -> dict:
    """A grade de participações vem como `linha` (o projeto, ou a obra, na
    ordem da tela) e células `pct_<i>_<sócio>`. O nome do projeto não vai no
    nome do campo: nome de obra tem espaço, barra e acento."""
    celulas = {}
    for i, linha in enumerate(form.getlist("linha")):
        for chave in form:
            prefixo = f"pct_{i}_"
            if chave.startswith(prefixo) and chave[len(prefixo):].isdigit():
                celulas[(linha, int(chave[len(prefixo):]))] = form.get(chave, "")
    return celulas


def _aplicar_mudanca_da_prestacao(dados, form):
    """Uma acao por envio; o formulario diz qual em `acao`."""
    acao = form.get("acao", "")
    if acao == "socio":
        dados.salvar_socio(form["nome"], form.get("tipo", "Interno"),
                           form.get("socio_id") or None)
    elif acao == "socios_em_lista":
        dados.salvar_socios_em_lista(zip(form.getlist("socio_id"), form.getlist("nome"),
                                         form.getlist("tipo")))
    elif acao == "desativar_socio":
        dados.desativar_socio(form["socio_id"])
    elif acao == "reativar_socio":
        dados.reativar_socio(form["socio_id"])
    elif acao == "participacao":
        dados.salvar_participacao(form["projeto"], form["socio_id"], form["pct"])
    elif acao == "apagar_participacao":
        dados.apagar_participacao(form["participacao_id"])
    elif acao == "participacoes_em_grade":
        dados.gravar_participacoes_em_grade(_celulas_da_grade(form))
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
                            form.get("descricao", ""), form.get("ajuste_id") or None)
    elif acao == "apagar_ajuste":
        dados.apagar_ajuste(form["ajuste_id"])
    elif acao == "fora_em_lista":
        dados.gravar_fora(_itens_fora_do_formulario(form))
    elif acao == "tirar_da_analise":
        dados.tirar_da_analise(form.getlist("item"))
    elif acao == "voltar_para_analise":
        dados.voltar_para_analise(form.get("item", ""))
    elif acao == "projeto_da_obra":
        # Vazio TIRA o projeto dado aqui (a obra volta ao da planilha).
        dados.definir_projeto_da_obra(form.get("codigo", ""),
                                      "" if form.get("tirar") else form.get("projeto", ""))
    elif acao == "config":
        for chave in ("projeto_matriz", "depto_admin_matriz", "depto_admin_filial",
                      "grupo_pessoal", "taxa_adm_pct", "residual",
                      "categoria_juros", "juros_por_deficit", "juros_sem_deficit"):
            if chave in form:
                dados.salvar_config(chave, form[chave])
        # as categorias dos juros, marcadas numa lista (09/11/2026); gravadas
        # no mesmo formato de antes ("a; b; c"), que é o que o cálculo lê
        if "categoria_juros_lista" in form:
            marcadas = []
            for c in form.getlist("categoria_juros_item"):
                c = (c or "").strip()
                if c and c not in marcadas:
                    marcadas.append(c)
            dados.salvar_config("categoria_juros", "; ".join(marcadas))


# ---------------------------------------------------------------------------
# Explorador de lancamentos — saneamento da base
# ---------------------------------------------------------------------------
# FORA do menu principal, de proposito: e ferramenta de manutencao, nao
# relatorio. Chega-se a ela por Configuracoes. O dono pediu assim — "uma tela
# mais escondida, para nao ser algo tao exposto".
def _pedido_do_explorador():
    """Le da URL o que a pessoa escolheu nos filtros."""
    from .consultas import ORDENS_DO_EXPLORADOR
    de, ate = _faixa_de_data()
    ordem = (request.args.get("ordem") or "data").strip()
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
        # a ordem da lista (10/11/2026): só as conhecidas, o resto vira data
        "ordem": ordem if ordem in ORDENS_DO_EXPLORADOR else "data",
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
        ordens=consultas.ORDENS_DO_EXPLORADOR,
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
        dados=dados, ordens=consultas.ORDENS_DO_EXPLORADOR,
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
    projetos = [p for p in request.form.getlist("projeto_do_usuario") if p.strip()]
    uid = (request.form.get("usuario_id") or "").strip()

    if acao == "criar":
        r = usuarios.criar(request.form.get("novo_usuario", ""),
                           request.form.get("nova_senha", ""),
                           nome=request.form.get("nome", ""),
                           obras=obras, telas=telas, contas=contas,
                           projetos=projetos)
    elif acao == "apagar" and uid.isdigit():
        r = usuarios.apagar(int(uid))
    elif acao == "salvar" and uid.isdigit():
        r = usuarios.atualizar(
            int(uid), nome=request.form.get("nome"),
            senha=request.form.get("nova_senha"),
            ativo=request.form.get("ativo") == "1",
            obras=obras, telas=telas, contas=contas, projetos=projetos)
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


def _situacao_das_obras_cc():
    """Quantos lançamentos de conta têm a obra lida do OMIE, e quantos não
    (10/11/2026) — ver `apropriacao_cc.situacao`."""
    from .db import conexao
    from .sync import apropriacao_cc
    with conexao() as conn:
        return apropriacao_cc.situacao(conn)


@bp.route("/conferir/lancamento-cc/painel")
def conferir_lancamento_cc_no_painel():
    """O que o PAINEL tem de um lançamento de conta: o movimento, a resposta
    guardada da consulta da obra (ou o erro) e as linhas que ele virou — com
    uma frase dizendo por que está como está. Não chama o OMIE. So do
    administrador (prefixo painel.conferir_)."""
    import json as _json
    from flask import Response
    from .db import conexao
    from .sync import apropriacao_cc
    codigo = (request.args.get("codigo") or "").strip()
    if not codigo.isdigit():
        return jsonify({"ok": False, "erro": "Informe o número do lançamento."}), 400
    with conexao() as conn:
        dados = apropriacao_cc.diagnostico(conn, int(codigo))
    return Response(_json.dumps(dados, ensure_ascii=False, indent=2, default=str),
                    mimetype="application/json",
                    headers={"Content-Disposition":
                             f'attachment; filename="painel_lancamento_cc_{codigo}.json"'})


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


def _projetos_para_liberar(estado_migracoes):
    """Os projetos que dá para marcar no cadastro de acesso — cada um abre em
    todas as obras dele, inclusive as futuras."""
    if estado_migracoes["pendentes"]:
        return []
    from . import consultas
    return consultas.opcoes_de_filtro().get("projetos", [])


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
    # "Não achou um aporte ou dividendo?" mora aqui desde 07/10/2026 (veio do
    # DRE): a lista de obras para escolher
    try:
        from . import consultas as _c
        contexto["obras_para_conferir"] = _c.opcoes_de_filtro().get("obras", [])
    except Exception:  # noqa: BLE001 — banco fora, migracao pendente
        contexto["obras_para_conferir"] = []
    # A historia das atualizacoes, passo a passo (dono, 06/10/2026: "ninguem
    # entende direito"). Falha aqui nao derruba a tela de configuracao.
    try:
        from . import andamento
        historico_atualizacoes = andamento.historico()
    except Exception:  # noqa: BLE001 — banco fora, migracao 002 pendente
        logger.exception("Painel: historico das atualizacoes indisponivel")
        historico_atualizacoes = None
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
        # Tambem enquanto outra RODA (06/10/2026): a linha mostrava a tentativa
        # anterior, fechada como "interrompida" no instante em que a nova
        # comecou — e o dono leu como se a que estava rodando tivesse caido.
        atualizacao = consultas.atualizado_em(
            so_concluidas=bool(sincronizacao["interrompida"] or sincronizacao["rodando"]))
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
        contas_conf = aportes_base = juros_conf = obras_cc = None
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
            obras_cc = _conferir(_situacao_das_obras_cc, conferencias_com_erro,
                                 "Obras dos lançamentos de conta")
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
        obras_cc=obras_cc,
        contas_conf=contas_conf,
        juros_conf=juros_conf,
        modos=tarefas.MODOS, rotulos_dos_modos=tarefas.ROTULOS,
        anos_da_releitura=_anos_da_releitura(),
        sincronizacao=sincronizacao, historico=historico_atualizacoes,
        pessoas=_pessoas_do_painel(estado_migracoes),
        telas_liberaveis=usuarios_mod.TELAS,
        telas_sugeridas=usuarios_mod.TELAS_SUGERIDAS,
        obras_para_liberar=_obras_para_liberar(estado_migracoes),
        projetos_para_liberar=_projetos_para_liberar(estado_migracoes),
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

    anos = dados.get("anos") if isinstance(dados.get("anos"), list) else None
    try:
        anos = [int(a) for a in (anos or [])]
    except (TypeError, ValueError):
        anos = []
    if modo == "pagamentos":
        return jsonify(tarefas.disparar(modo, disparo, anos=anos))
    if modo == "periodo":
        return jsonify(tarefas.disparar(modo, disparo, periodo={
            "de": dados.get("de") or request.form.get("de", ""),
            "ate": dados.get("ate") or request.form.get("ate", "")}))
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
        consultas.ate_um_ano_a_frente(mensal)[-36:],
        [("receita", "b-receita", "Receita"), ("despesa", "b-despesa", "Despesa")],
        campo_rotulo="rotulo", campo_linha="acumulado"),
        "Fluxo Financeiro mensal — comprometido, até 12 meses à frente")]


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
            # o percentual que muda com o tempo (09/11/2026) — memória de
            # cálculo sem ele não se confere depois
            for chave, lista in sorted(((atual["pesos"].get("periodos") or {})
                                        .get(nivel) or {}).items()):
                for desde, pct, principal in lista:
                    lado = (f", {principal:.0f}% para o projeto principal"
                            if principal is not None else "")
                    parametros.append({
                        "o_que": f"Marcado ({nivel}) {chave}, a partir de {desde[5:]}/{desde[:4]}",
                        "valor": f"{pct:.0f}%{lado}"})
        quem = atual.get("projetos") or {}
        from . import prestacao_dados as _pd
        parametros.append({"o_que": "Projeto principal",
                           "valor": quem.get("principal")
                           or _pd.config().get("projeto_matriz") or "—"})
        for projeto, (papel, desde, ate) in sorted((quem.get("regras") or {}).items()):
            parametros.append({
                "o_que": f"Projeto {projeto}",
                "valor": ("não recebe" if papel == "nao" else
                          f"recebe {('de ' + desde[5:] + '/' + desde[:4]) if desde else ''}"
                          f"{(' até ' + ate[5:] + '/' + ate[:4]) if ate else ''}".strip())})
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
            # o batimento mês a mês, com a linha do total — para conferir à mão
            ("Batimento", C["cenario_batimento"],
             calculo["batimento"]["linhas"]
             + [dict(mes="TOTAL", **calculo["batimento"]["total"])]),
            ("Ficou de fora", C["cenario_fora"], calculo["fora_por_conta"]),
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

    def _abas_da_conferencia():
        from . import conferencia_omie
        dia = (request.args.get("dia") or "").strip()
        if not _DATA_ISO.match(dia):
            return [("Conferencia", [("aviso", "Aviso")], [{"aviso": "Dia inválido."}])]
        resultado = conferencia_omie.conferir_dia(dia)
        contas = conferencia_omie._nomes_das_contas()
        quem = conferencia_omie._quem_e(
            [p["codigo"] for p in conferencia_omie.pernas_do_espelho(dia)])

        def _lado(pernas):
            saida = []
            for p in pernas:
                q = quem.get(p["codigo"], {})
                saida.append({"codigo": p["codigo"], "quem": q.get("quem", ""),
                              "documento": q.get("documento", ""),
                              "obra": q.get("obra", ""),
                              "perna": ("baixa consolidada" if p["liquidado"] == "S"
                                        else "previsão" if p["liquidado"] == "N"
                                        else "baixa bancária"),
                              "conta": conferencia_omie._nome(contas, p["conta"]),
                              "valor": p["valor"]})
            return saida

        no_omie = conferencia_omie.pernas_do_omie(conferencia_omie.registros_do_omie(dia))
        return [
            (f"Diferencas {dia}", C["conferencia_diferencas"],
             conferencia_omie.linhas_para_planilha(resultado)["diferencas"]),
            ("No painel", C["conferencia_lado"], _lado(conferencia_omie.pernas_do_espelho(dia))),
            ("No OMIE", C["conferencia_lado"], _lado(no_omie)),
        ]

    montadores = {
        "dre": lambda: [("DRE", C["dre"], _dre())],
        "conferencia": _abas_da_conferencia,
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
            # aportes e dividendos no mesmo relatório (dono, 06/10/2026: "aí
            # fica completíssimo") — as mesmas abas do arquivo de aportes
            *_abas_de_aporte(),
        ],
    }
    # "quotas" e "posicao" eram a planilha da prestação ANTIGA, que saiu em
    # 11/11/2026; a do cenário é o assunto "cenario".
    if assunto in montadores:
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
    sincronizacao = tarefas.estado()
    passos = []
    if sincronizacao["rodando"]:
        # os passos da que esta rodando, para a tela mostrar a sequencia ao vivo
        try:
            from . import andamento
            atual = andamento.historico(limite=1)["execucoes"]
            passos = [{"etapa": p["etapa"], "estado": p["estado"],
                       "detalhe": p["detalhe"]} for p in (atual[0]["passos"] if atual else [])]
        except Exception:  # noqa: BLE001
            passos = []
    return jsonify({"ok": True, "sincronizacao": sincronizacao, "passos": passos,
                    "ultima": _serializar(consultas.atualizado_em())})


def _anos_da_releitura() -> list[int]:
    from .sync.espelho import anos_da_releitura
    return list(reversed(anos_da_releitura()))


def _serializar(d):
    if not d:
        return None
    from .horario import texto
    saida = dict(d)
    for chave in ("inicio", "fim"):
        if saida.get(chave) is not None:
            saida[chave] = texto(saida[chave])
    return saida
