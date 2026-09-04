# -*- coding: utf-8 -*-
"""
Rotas e telas da Análise de SPs.

Todas as rotas passam pelo guarda em `auth.py`, e todas DECLARAM o que exigem.
Rota que esquecer de declarar é recusada, não liberada.

O blueprint traz o prefixo `/analisesps` embutido, como o ERP e o painel fazem
— assim o `main.py` registra sem `url_prefix` e não há dois lugares dizendo
onde o módulo mora.
"""
from __future__ import annotations

import io
import logging
import os

from flask import (Blueprint, Response, redirect, render_template, request,
                   session, url_for)

from . import auth
from . import preferencias
from .auth import exige_consulta, exige_operador, publica

logger = logging.getLogger("analisesps.web")

bp = Blueprint("analisesps", __name__,
               url_prefix="/analisesps",
               template_folder="templates",
               static_folder="static")

bp.before_request(auth.exigir_login)


@bp.app_template_filter("moeda")
def _filtro_moeda(valor):
    from .formatos import moeda
    return moeda(valor)


@bp.app_template_filter("data_br")
def _filtro_data(valor):
    from .formatos import data_br
    return data_br(valor)


# ---------------------------------------------------------------------------
# Entrada e saída
# ---------------------------------------------------------------------------
@bp.route("/entrar", methods=["GET", "POST"])
@publica("é a própria tela de login; sem ela ninguém consegue entrar")
def entrar():
    configurados = auth.perfis_configurados()
    erro = None

    nome = auth.limpar_nome(request.form.get("nome", ""))

    if request.method == "POST" and configurados:
        perfil = auth.identificar(request.form.get("senha", ""))
        if not nome:
            erro = "Diga o seu nome — é ele que separa o seu lote do dos outros."
        elif perfil:
            auth.entrar_na_sessao(perfil, nome)
            destino = request.args.get("proximo") or ""
            # Só aceita destino interno: um "proximo" apontando para fora
            # viraria um jeito de usar o login da empresa como trampolim.
            if destino.startswith("/analisesps"):
                return redirect(destino)
            return redirect(url_for("analisesps.solicitacoes"))
        else:
            erro = "Senha incorreta."
            logger.warning("Análise de SPs: tentativa de entrada com senha "
                           "errada (nome informado: %r).", nome)

    return render_template("analisesps_login.html",
                           sem_senha=not configurados, erro=erro, nome=nome)


@bp.route("/sair")
@exige_consulta
def sair():
    auth.sair_da_sessao()
    return redirect(url_for("analisesps.entrar"))


@bp.route("/saude")
@publica("checagem de serviço; não devolve nenhum dado da empresa")
def saude():
    """Diz qual versão está publicada.

    Existe para responder "a correção já subiu?" sem precisar abrir nada nem
    perguntar a ninguém — o Render carimba o commit em RENDER_GIT_COMMIT."""
    commit = os.getenv("RENDER_GIT_COMMIT", "")
    return {"ok": True, "modulo": "analisesps",
            "versao": commit[:8] if commit else "desenvolvimento",
            "senhas_configuradas": len(auth.perfis_configurados())}


# ---------------------------------------------------------------------------
# A tela principal
# ---------------------------------------------------------------------------
# As chaves que formam "o filtro". Uma só lista, usada para ler da barra de
# endereço, para guardar e para restaurar — três lugares que não podem
# divergir. `pagina` de propósito fica de fora: ninguém quer voltar amanhã na
# página 7.
CHAVES_FILTRO = ("busca", "status_pgt", "conta", "forma", "status_agend",
                 "tipo_despesa", "projeto", "responsavel", "centro_custo",
                 "situacoes", "periodo_ini", "periodo_fim", "pgt_ini",
                 "pgt_fim", "valor_ini", "valor_fim", "ordem")

# Marca que a barra de endereço JÁ carrega um filtro — mesmo que ele esteja
# vazio. Sem ela não há como distinguir "acabei de chegar nesta tela" de
# "limpei o filtro de propósito": as duas seriam um endereço sem parâmetro
# nenhum, e o filtro limpo voltaria preenchido no instante seguinte.
MARCA_FILTRO = "f"


def _filtro_cru() -> dict:
    """O filtro como veio na barra de endereço, sem conversão nenhuma.

    É esta forma que vai para o banco: texto igual ao que a tela mandou.
    Guardar a versão já convertida (datas viram objetos, valores viram número)
    obrigaria a desconverter na volta, e é aí que aparece a diferença entre o
    que a pessoa marcou e o que ela reencontra."""
    cru = {}
    for chave in CHAVES_FILTRO:
        valores = [v for v in request.args.getlist(chave) if str(v).strip()]
        if valores:
            cru[chave] = valores
    return cru


def _opcoes_dos_filtros() -> dict:
    """O que cada lista suspensa da barra lateral oferece.

    Vive aqui, e não dentro de cada rota, porque Solicitações e Relatório
    mostram a MESMA barra. Duas cópias divergiriam no dia em que alguém
    acrescentasse um filtro em uma só."""
    from . import consultas
    return {
        "status_pgt": consultas.opcoes("status_pgt"),
        "conta": consultas.opcoes("conta"),
        "forma": consultas.opcoes("forma_pagamento"),
        "tipo_despesa": consultas.opcoes("tipo_despesa"),
        "projeto": consultas.opcoes("projeto"),
        "responsavel": consultas.opcoes("responsavel"),
        "centro_custo": consultas.opcoes("centro_custo", limite=200),
        "status_agend": consultas.opcoes_agendamento(),
    }


def _lembrar_filtro(endpoint: str):
    """Guarda o filtro desta tela, ou traz de volta o da última vez.

    Devolve um redirecionamento quando há filtro guardado a restaurar, e None
    quando a tela pode seguir e desenhar.

    O caminho é sempre o mesmo, venha a pessoa de onde vier: chegou com a
    marca, o que está na barra de endereço é a verdade e vira o guardado;
    chegou sem a marca (clicou no menu, digitou o endereço, voltou de outra
    tela), o guardado volta. É isso que faz o filtro de Solicitações valer
    também no Relatório — os dois guardam no mesmo lugar."""
    pessoa = auth.pessoa_atual()

    if request.args.get(MARCA_FILTRO):
        preferencias.gravar(pessoa, preferencias.FILTRO, _filtro_cru())
        return None

    guardado = preferencias.ler(pessoa, preferencias.FILTRO)
    if not guardado:
        return None

    # Preserva o que a tela já tinha e não é filtro (o tipo do relatório, por
    # exemplo), e acrescenta o filtro guardado por cima.
    destino = {c: request.args.getlist(c) for c in request.args
               if c not in CHAVES_FILTRO and c != "pagina"}
    for chave, valores in guardado.items():
        if chave in CHAVES_FILTRO:
            destino[chave] = valores
    destino[MARCA_FILTRO] = "1"
    return redirect(url_for(endpoint, **destino))


def _filtros_do_pedido() -> dict:
    """Lê os filtros da barra de endereço. Tudo opcional."""
    def lista(nome):
        return [v for v in request.args.getlist(nome) if str(v).strip()]

    def numero(nome):
        bruto = (request.args.get(nome) or "").strip()
        if not bruto:
            return None
        from .formatos import para_numero
        return para_numero(bruto)

    def data(nome):
        bruto = (request.args.get(nome) or "").strip()
        if not bruto:
            return None
        from .formatos import para_data
        # O campo de data do navegador manda AAAA-MM-DD; a pessoa que digita à
        # mão manda DD/MM/AAAA. O conversor aceita os dois.
        return para_data(bruto)

    return {
        "busca": request.args.get("busca", "").strip(),
        "status_pgt": lista("status_pgt"),
        "conta": lista("conta"),
        "forma": lista("forma"),
        "status_agend": lista("status_agend"),
        "tipo_despesa": lista("tipo_despesa"),
        "projeto": lista("projeto"),
        "responsavel": lista("responsavel"),
        "centro_custo": lista("centro_custo"),
        "situacoes": lista("situacoes"),
        "periodo_ini": data("periodo_ini"),
        "periodo_fim": data("periodo_fim"),
        "pgt_ini": data("pgt_ini"),
        "pgt_fim": data("pgt_fim"),
        "valor_ini": numero("valor_ini"),
        "valor_fim": numero("valor_fim"),
    }


@bp.route("/")
@exige_consulta
def inicio():
    return redirect(url_for("analisesps.solicitacoes"))


@bp.route("/solicitacoes")
@exige_consulta
def solicitacoes():
    from . import consultas

    base = consultas.base_carregada()
    if not base["pronta"]:
        # Base vazia não é "nada a pagar" — é base não carregada. Dizer isso
        # evita que alguém conclua que não há contas em aberto.
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    voltar = _lembrar_filtro("analisesps.solicitacoes")
    if voltar is not None:
        return voltar

    filtros = _filtros_do_pedido()
    ordem = request.args.get("ordem", "vencimento")
    try:
        pagina = max(1, int(request.args.get("pagina", 1)))
    except ValueError:
        pagina = 1

    linhas = consultas.listar(filtros, ordem=ordem, pagina=pagina)
    resumo = consultas.resumo(filtros)
    ultima = (pagina - 1) * consultas.POR_PAGINA + len(linhas)

    return render_template(
        "analisesps_solicitacoes.html",
        linhas=linhas, resumo=resumo, base=base,
        pagina=pagina, por_pagina=consultas.POR_PAGINA,
        primeira_linha=(pagina - 1) * consultas.POR_PAGINA + 1,
        ultima_linha=ultima,
        tem_proxima=ultima < resumo["quantidade"],
        ordem=ordem, filtros=filtros,
        args=request.args, opcoes=_opcoes_dos_filtros(),
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# De onde a pessoa veio, e para onde "Voltar" tem de levar. Lista fechada de
# propósito: o destino sai da barra de endereço, e um destino livre viraria um
# jeito de usar este módulo como trampolim para fora.
ORIGENS = {"solicitacoes": "analisesps.solicitacoes",
           "lote": "analisesps.tela_lote"}


def _origem_pedida() -> str:
    pedida = (request.args.get("origem") or "").strip()
    return pedida if pedida in ORIGENS else "solicitacoes"


@bp.route("/sp/<sp_id>")
@exige_consulta
def detalhe(sp_id):
    """A ficha da SP. Como página inteira, ou como pedaço para o modal.

    `?modal=1` devolve só o miolo, sem cabeçalho nem menu: é o que o duplo
    clique na linha busca para abrir a ficha POR CIMA da lista, sem perder a
    rolagem, o filtro nem a marcação. Era assim no Streamlit."""
    import re
    from . import consultas, pagamentos

    registro = consultas.uma(sp_id)
    if registro is None:
        if request.args.get("modal"):
            return ('<div class="aviso erro">Esta SP não existe na base.</div>',
                    404)
        return render_template("analisesps_erro.html",
                               titulo="Não encontrado",
                               mensagem="Esta SP não existe na base."), 404

    # O que falta preencher no cadastro do lançamento — mesma conta do
    # Streamlit, e o módulo já tinha a função pronta, sem uso.
    try:
        faltando = pagamentos.pendencias(
            registro.get("forma_pagamento", ""), registro.get("info_pgt", ""),
            registro.get("centro_custo", ""), registro.get("codigo_integracao", ""),
            registro.get("status_pgt", ""))
    except Exception:  # noqa: BLE001 — a ficha abre mesmo sem esse aviso
        logger.exception("Análise de SPs: falhou calcular as pendências da SP")
        faltando = []

    # As SPs que a análise apontou como possível duplicidade. Ficam como link
    # para o card de cada uma — é o que o Streamlit fazia, e é o que resolve a
    # dúvida sem ter de procurar o número na mão.
    analise = str(registro.get("analise_ia") or "")
    apontadas = [i for i in dict.fromkeys(re.findall(r"\d{9,}", analise))
                 if i != str(sp_id)][:8]

    contexto = dict(
        sp=registro, origem=_origem_pedida(),
        pendencias=faltando, risco_ids=apontadas,
        hook_omie=os.getenv("ANALISESPS_HOOK_OMIE", "").strip(),
        pode_operar=auth.pode_operar())

    if request.args.get("modal"):
        return render_template("analisesps_ficha.html", **contexto)

    return render_template("analisesps_detalhe.html", aba="solicitacoes",
                           perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
                           **contexto)


# ---------------------------------------------------------------------------
# Exportação — CSV, do jeito que o Excel em português abre certo
# ---------------------------------------------------------------------------
@bp.route("/exportar")
@exige_consulta
def exportar():
    """Exporta o que o filtro alcança.

    É CSV, não `.xlsx`: gerar Excel de verdade exigiria uma biblioteca nova no
    serviço, e a regra da casa é não acrescentar dependência sem combinar. O
    ponto-e-vírgula, a vírgula decimal e o BOM no começo são os três detalhes
    que fazem o Excel em português abrir o arquivo certo, sem "importar".

    Sai em blocos, direto para o navegador: montar o arquivo inteiro na
    memória antes de enviar é justamente o que a instância de 2 GB não suporta
    com um filtro largo."""
    from . import consultas
    from .formatos import data_br, moeda

    filtros = _filtros_do_pedido()
    ordem = request.args.get("ordem", "vencimento")

    cabecalho = ["ID", "Data", "Vencimento", "Credor", "CPF/CNPJ",
                 "Tipo de Despesa", "Centro de Custo", "Projeto", "Valor",
                 "Responsável", "Status Pgt", "Status Agend", "Autorização",
                 "Forma de Pagamento", "Conta", "Informação p/ Pgt", "Nº NF",
                 "Pedido", "Data do Pagamento", "Anuente", "Validação",
                 "Código de Barras", "Análise IA", "Descrição"]

    def campos(linha):
        return [
            linha["id"], data_br(linha["solicitacao_d"]),
            data_br(linha["vencimento_d"]), linha["credor"], linha["documento"],
            linha["tipo_despesa"], linha["centro_custo"], linha["projeto"],
            moeda(linha["valor_num"]), linha["responsavel"], linha["status_pgt"],
            linha["status_agend"], linha["status_aut"], linha["forma_pagamento"],
            linha["conta"], linha["info_pgt"], linha["nf"], linha["pedido"],
            data_br(linha["data_pagamento_d"]), linha["anuente"],
            linha["validacao"], linha["codigo_barras"], linha["analise_ia"],
            linha["descricao"],
        ]

    def blocos():
        """Percorre por páginas. Uma de cada vez na memória, não as 59 mil
        linhas que um filtro largo alcança."""
        pagina = 1
        while True:
            linhas = consultas.listar(filtros, ordem=ordem, pagina=pagina)
            if not linhas:
                break
            for linha in linhas:
                yield campos(linha)
            if len(linhas) < consultas.POR_PAGINA:
                break
            pagina += 1

    # O BOM, o ponto e vírgula e o escape do texto vivem num lugar só
    # (`exportar.py`). Repetir essas regras em cada tela é como elas passam a
    # divergir — e aí um arquivo abre certo e o outro não.
    from . import exportar as saida
    return saida.resposta("analise_sps", cabecalho, blocos())


# ---------------------------------------------------------------------------
# Alteração — só o Operador
# ---------------------------------------------------------------------------
@bp.route("/api/alterar", methods=["POST"])
@exige_operador
def alterar():
    """Altera uma coluna editável em uma ou mais SPs.

    O caminho é sempre o mesmo, e é o que garante que nada se perca:
      1. grava no banco na hora — quem está na tela vê o efeito imediatamente;
      2. põe a célula na fila de escrita para a planilha;
      3. registra no log.
    O envio para a planilha acontece depois, no processo separado. Se a
    internet cair no meio, a alteração continua na fila e sobe sozinha."""
    from . import colunas
    from .db import conexao

    dados = request.get_json(silent=True) or {}
    ids = [str(i).strip() for i in (dados.get("ids") or []) if str(i).strip()]
    coluna = str(dados.get("coluna") or "").strip()
    valor = str(dados.get("valor") or "").strip()
    acao = str(dados.get("acao") or "Alterar").strip()

    if not ids:
        return {"ok": False, "erro": "Nenhuma SP selecionada."}, 400
    if coluna not in colunas.EDITAVEIS:
        # Só as duas colunas que o operador mexe no dia a dia. Qualquer outra é
        # somente leitura — a planilha é a dona do resto.
        return {"ok": False,
                "erro": f"A coluna '{coluna}' não é alterável por aqui."}, 400
    if len(ids) > 500:
        return {"ok": False,
                "erro": "São no máximo 500 SPs por vez. Refine a seleção."}, 400

    perfil = auth.perfil_atual() or "?"
    quem = auth.nome_atual()
    with conexao() as conn:
        marcadores = ",".join(["?"] * len(ids))
        cur = conn.execute(
            f'SELECT id, "{coluna}" FROM analisesps.sps WHERE id IN ({marcadores})',
            tuple(ids))
        anteriores = {str(r[0]): r[1] for r in cur.fetchall()}
        cur.close()

        faltando = [i for i in ids if i not in anteriores]
        if faltando:
            return {"ok": False,
                    "erro": f"{len(faltando)} SP(s) não existem na base: "
                            + ", ".join(faltando[:5])}, 404

        conn.execute(
            f'UPDATE analisesps.sps SET "{coluna}" = ?, atualizado_em = now() '
            f" WHERE id IN ({marcadores})", (valor,) + tuple(ids))

        for sp_id in ids:
            conn.execute(
                "INSERT INTO analisesps.fila (sp_id, coluna, valor, criado_em, "
                "                             tentativas, ultimo_erro) "
                "VALUES (?, ?, ?, now(), 0, NULL) "
                "ON CONFLICT (sp_id, coluna) DO UPDATE SET "
                "  valor = EXCLUDED.valor, criado_em = now(), "
                "  tentativas = 0, ultimo_erro = NULL",
                (sp_id, coluna, valor))
            conn.execute(
                "INSERT INTO analisesps.log_alteracoes "
                "  (sp_id, coluna, valor, valor_anterior, acao, perfil, "
                "   pessoa, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'pendente')",
                (sp_id, coluna, valor, anteriores.get(sp_id), acao, perfil,
                 quem))
        conn.commit()

    logger.info("Análise de SPs: %s (%s) alterou '%s' de %d SP(s) para '%s'.",
                quem or "sem nome", perfil, coluna, len(ids), valor)

    # Tenta subir já, sem prender a tela: se falhar, fica na fila.
    from . import tarefas
    resultado = tarefas.disparar("fila", disparo="alteração na tela")

    return {"ok": True, "alteradas": len(ids),
            "envio": resultado.get("ok", False),
            "aviso": None if resultado.get("ok") else resultado.get("erro")}


# ---------------------------------------------------------------------------
# Configurações e sincronização
# ---------------------------------------------------------------------------
@bp.route("/configuracoes")
@exige_consulta
def configuracoes():
    from . import consultas, migracoes_runner, tarefas
    try:
        migracoes = migracoes_runner.listar_estado()
        erro_banco = None
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        migracoes = {"aplicadas": [], "pendentes": []}
        erro_banco = str(e)

    return render_template(
        "analisesps_config.html",
        migracoes=migracoes, erro_banco=erro_banco,
        base=consultas.base_carregada(),
        andamento=tarefas.estado(),
        ultima=tarefas.ultima_concluida() if not erro_banco else None,
        modos=tarefas.MODOS,
        versao=os.getenv("RENDER_GIT_COMMIT", "")[:8] or "desenvolvimento",
        pode_operar=auth.pode_operar())


@bp.route("/api/migrar", methods=["POST"])
@exige_operador
def migrar():
    from . import migracoes_runner
    return migracoes_runner.aplicar_pendentes()


@bp.route("/api/andamento")
@exige_consulta
def andamento():
    """Consultada de poucos em poucos segundos enquanto alguém acompanha uma
    carga. Responde só o essencial — é chamada muitas vezes."""
    from . import tarefas
    from .horario import texto
    estado = tarefas.estado()
    detalhe = estado.get("detalhe") or estado.get("interrompida") or {}
    return {
        "ok": True,
        "rodando": estado["rodando"],
        "interrompida": bool(estado.get("interrompida")),
        "etapa": detalhe.get("etapa"),
        "progresso": detalhe.get("progresso"),
        "visto_em": texto(detalhe.get("visto_em")) if detalhe else None,
    }


@bp.route("/api/sincronizar", methods=["POST"])
@publica("chamada por máquina (agendador); protegida por ANALISESPS_SECRET")
def sincronizar():
    """Dispara a sincronização. Dois caminhos, uma porta:

      - o agendador (cron-job.org) manda o segredo do módulo no corpo, mesmo
        arranjo que o `baixabradesco` e o painel já usam;
      - o Operador, logado, aperta o botão na tela de Configurações.

    Quem não é nenhum dos dois é recusado. Note que esta rota é `@publica` no
    guarda porque a máquina não tem sessão — a autenticação dela acontece aqui
    dentro, e está escrita."""
    from . import tarefas

    dados = request.get_json(silent=True) or {}
    modo = str(dados.get("modo") or request.form.get("modo") or "sincronizar")

    if auth.segredo_de_maquina_confere(dados.get("secret", "")):
        disparo = "agendador"
    elif auth.pode_operar():
        disparo = "manual"
    else:
        logger.warning("Análise de SPs: sincronização recusada — sem segredo "
                       "válido e sem sessão de Operador.")
        return {"ok": False, "erro": "Não autorizado."}, 403

    return tarefas.disparar(modo, disparo=disparo)


# ---------------------------------------------------------------------------
# RELATÓRIO
# ---------------------------------------------------------------------------
@bp.route("/relatorio")
@exige_consulta
def relatorio():
    from . import consultas

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    voltar = _lembrar_filtro("analisesps.relatorio")
    if voltar is not None:
        return voltar

    filtros = _filtros_do_pedido()
    tipo = request.args.get("tipo", "geral")
    if tipo not in consultas.TIPOS:
        tipo = "geral"
    periodo = request.args.get("periodo", "tudo")
    if periodo not in consultas.PERIODOS:
        periodo = "tudo"
    dimensao = request.args.get("dimensao", "centro_custo")
    if dimensao not in consultas.DIMENSOES:
        dimensao = "centro_custo"

    return render_template(
        "analisesps_relatorio.html",
        aba="relatorio", base=base,
        numeros=consultas.numeros_do_relatorio(filtros, tipo, periodo),
        por_projeto=consultas.agregar(filtros, "projeto", tipo, periodo, 15),
        por_centro=consultas.agregar(filtros, "centro_custo", tipo, periodo, 15),
        por_tipo=consultas.agregar(filtros, "tipo_despesa", tipo, periodo, 15),
        por_conta=consultas.agregar(filtros, "conta", tipo, periodo, 15),
        quebra=consultas.agregar(filtros, dimensao, tipo, periodo, 100),
        credores=consultas.top_credores(filtros, tipo, periodo, 30),
        aging=consultas.aging_vencidos(filtros, periodo),
        tipo=tipo, periodo=periodo, dimensao=dimensao,
        tipos=consultas.TIPOS, periodos=consultas.PERIODOS,
        dimensoes=consultas.DIMENSOES,
        args=request.args, filtros=filtros, opcoes=_opcoes_dos_filtros(),
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# AUDITORIA
# ---------------------------------------------------------------------------
@bp.route("/auditoria")
@exige_consulta
def auditoria():
    from . import auditoria as checagens
    from . import consultas

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    filtros = _filtros_do_pedido()
    usar_filtros = request.args.get("usar_filtros") == "1"
    checagem = request.args.get("checagem", "")
    if checagem not in checagens.CHECAGENS:
        checagem = ""

    resultado = None
    if checagem == "pontualidade":
        try:
            minimo = max(1, int(request.args.get("minimo", 5)))
        except ValueError:
            minimo = 5
        resultado = checagens.pontualidade(filtros, usar_filtros, minimo)
    elif checagem == "risco_ia":
        resultado = checagens.risco_ia(filtros, usar_filtros)
    elif checagem == "nf_duplicada":
        resultado = checagens.nf_duplicada(filtros, usar_filtros)
    elif checagem == "possivel_duplicidade":
        try:
            dias = max(0, int(request.args.get("dias", 7)))
        except ValueError:
            dias = 7
        resultado = checagens.possivel_duplicidade(filtros, usar_filtros, dias)
    elif checagem == "sem_classificacao":
        resultado = checagens.sem_classificacao(filtros, usar_filtros)
    elif checagem == "sem_integracao":
        resultado = checagens.sem_integracao_omie(filtros, usar_filtros)
    elif checagem == "codigos_barras":
        resultado = checagens.codigos_de_barras(filtros, usar_filtros)

    return render_template(
        "analisesps_auditoria.html",
        aba="auditoria", base=base,
        checagens=checagens.CHECAGENS, checagem=checagem,
        contagens=checagens.resumo(filtros, usar_filtros) if not checagem else None,
        resultado=resultado, usar_filtros=usar_filtros,
        teto=checagens.TETO,
        minimo=request.args.get("minimo", 5),
        dias=request.args.get("dias", 7),
        args=request.args, filtros=filtros,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# LOTE
# ---------------------------------------------------------------------------
@bp.route("/lote", methods=["GET", "POST"])
@exige_consulta
def tela_lote():
    from . import consultas, lote

    # A permissão é conferida ANTES de qualquer outra coisa. Com a base ainda
    # vazia, a checagem de carga respondia primeiro e o perfil Consulta recebia
    # a tela amigável em vez de 403. Nada era alterado — mas a resposta a uma
    # tentativa de escrita sem alçada tem de ser sempre a mesma, independente
    # do estado do banco.
    if request.method == "POST" and not auth.pode_operar():
        return auth._sem_permissao()

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    pessoa = auth.pessoa_atual()
    aviso = None
    if request.method == "POST":
        acao = request.form.get("acao", "salvar")
        conteudo = request.form.get("conteudo", "")
        quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")

        if acao == "extrair":
            achados = lote.extrair_ids(request.form.get("extracao", ""))
            if achados:
                conteudo, titulo = lote.acrescentar_grupo(conteudo, achados)
                aviso = (f"{len(achados)} SP(s) encontrada(s) no texto colado — "
                         f"entraram no grupo \"{titulo}\".")
            else:
                aviso = ("Não achei nenhum número de SP no texto colado. "
                         "Uma SP tem 10 dígitos.")
        elif acao == "receber_ids":
            # Veio da barra de ações das Solicitações: as SPs marcadas entram
            # num grupo novo NO TOPO, e o que já estava fica abaixo. É o
            # "Enviar Lote" do Streamlit, que tinha sumido na conversão.
            crus = [i.strip() for i in
                    (request.form.get("ids") or "").split(",") if i.strip()]
            conteudo = lote.ler(pessoa)["conteudo"]
            if crus:
                conteudo, titulo = lote.acrescentar_grupo(conteudo, crus)
                aviso = f"{len(crus)} SP(s) entraram no grupo \"{titulo}\"."
            else:
                aviso = "Nenhuma SP marcada."
        elif acao == "trazer_antigo":
            antigo_ = lote.lote_de_antes().get("conteudo") or ""
            if antigo_.strip():
                juntos = [t for t in (antigo_.strip(), conteudo.strip()) if t]
                conteudo = "\n\n".join(juntos)
                aviso = ("O lote de quando ele era compartilhado veio para o "
                         "seu. Ele continua guardado onde estava — trazer não "
                         "tira de ninguém.")
            else:
                aviso = "Não há lote antigo guardado."
        elif acao in ("remover_pagos", "remover_cancelados"):
            alvo = {"pago"} if acao == "remover_pagos" else {"cancelado"}
            montado = lote.montar(conteudo)
            status = {i: (l.get("status_pgt") or "")
                      for i, l in montado["linhas"].items()}
            conteudo, quantos = lote.remover_por_status(conteudo, alvo, status)
            rotulo = "paga(s)" if acao == "remover_pagos" else "cancelada(s)"
            aviso = f"{quantos} SP(s) {rotulo} saíram do lote."

        lote.salvar(conteudo, quem, pessoa)
        return redirect(url_for("analisesps.tela_lote", aviso=aviso or ""))

    guardado = lote.ler(pessoa)
    montado = lote.montar(guardado["conteudo"])

    # O lote de quando ele era de todo mundo. Só aparece para quem ainda não
    # tem lote próprio — depois de começar o seu, ninguém quer ser lembrado.
    antes = lote.lote_de_antes() if not guardado["conteudo"].strip() else None
    if antes and not (antes.get("conteudo") or "").strip():
        antes = None

    return render_template(
        "analisesps_lote.html",
        aba="lote", base=base, lote=guardado, montado=montado, antes=antes,
        aviso=request.args.get("aviso") or None,
        pode_operar=auth.pode_operar(), nome=auth.nome_atual(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# CÓDIGOS DE PAGAMENTO — QR Pix e código de barras
# ---------------------------------------------------------------------------
@bp.route("/codigos")
@exige_consulta
def codigos():
    """Monta o QR Pix ou o código de barras das SPs pedidas.

    É a tela que substitui abrir card por card no Pipefy para copiar a chave:
    quem vai pagar abre isto e tem tudo numa página só.

    Teto de SPs por vez, e ele é proposital: cada QR é uma imagem gerada aqui
    dentro. Cinquenta já é mais do que alguém paga de uma sentada."""
    from . import consultas, pagamentos

    pedidos = [i.strip() for i in request.args.getlist("id") if i.strip()][:50]
    if not pedidos:
        return render_template("analisesps_erro.html",
                               titulo="Nada selecionado",
                               mensagem="Marque as SPs na lista e clique em "
                                        "\"QR / Código\"."), 400

    blocos = []
    for sp_id in pedidos:
        registro = consultas.uma(sp_id)
        if registro is None:
            blocos.append({"id": sp_id, "erro": "SP não encontrada na base."})
            continue

        forma = str(registro.get("forma_pagamento") or "").strip().lower()
        bloco = {"id": sp_id, "sp": registro, "tipo": None,
                 "erro": None, "imagem": None, "copia_cola": None}

        try:
            if "boleto" in forma:
                bloco["tipo"] = "boleto"
                svg, situacao = pagamentos.barcode_svg(
                    registro.get("codigo_barras") or "")
                if situacao != "ok":
                    bloco["erro"] = f"Código de barras {situacao}."
                else:
                    bloco["imagem"] = svg
                    bloco["copia_cola"] = str(
                        registro.get("codigo_barras") or "").strip()
            elif "pix" in forma or "beevale" in forma:
                bloco["tipo"] = "pix"
                chave = str(registro.get("info_pgt") or "")
                png, carga = pagamentos.gerar_pix(
                    chave, float(registro.get("valor_num") or 0),
                    str(registro.get("credor") or ""),
                    copia_cola=("00020" in chave))
                import base64
                bloco["imagem"] = base64.b64encode(png).decode("ascii")
                bloco["copia_cola"] = carga
            else:
                bloco["erro"] = (f"Forma de pagamento \"{forma or '—'}\" não "
                                 "gera QR nem código de barras.")
        except Exception as e:  # noqa: BLE001 — uma SP ruim não some com as outras
            logger.exception("Análise de SPs: falhou montar o código da SP %s", sp_id)
            bloco["erro"] = str(e)

        blocos.append(bloco)

    origem = _origem_pedida()
    return render_template("analisesps_codigos.html",
                           aba="lote" if origem == "lote" else "solicitacoes",
                           blocos=blocos, origem=origem,
                           pode_operar=auth.pode_operar(),
                           perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# RATEAR
# ---------------------------------------------------------------------------
@bp.route("/ratear", methods=["GET", "POST"])
@exige_consulta
def ratear():
    from . import rateio, sincronizacao

    resultado = erro = None
    try:
        referencias = sincronizacao.referencias_rateio()
    except Exception as e:  # noqa: BLE001 — banco fora do ar dá recado, não 500
        # Uma tela que estoura com 500 quando o banco cai é justamente a que
        # ninguém consegue usar para entender o que houve. As outras telas
        # degradam com recado; esta passou a fazer o mesmo.
        logger.exception("Análise de SPs: não consegui ler as listas do rateio")
        referencias = {"obras": [], "categorias": []}
        erro = (f"Não consegui ler as listas de obras e categorias: {e}")

    if request.method == "POST":
        if not auth.pode_operar():
            return auth._sem_permissao()
        mapa_obra = {o["nome"]: o["codigo"] for o in referencias["obras"]}
        mapa_categoria = {c["nome"]: c["codigo"] for c in referencias["categorias"]}

        def _linhas(prefixo, mapa, campo):
            saida = []
            nomes = request.form.getlist(f"{prefixo}_nome")
            valores = request.form.getlist(f"{prefixo}_valor")
            for nome, valor in zip(nomes, valores):
                nome = (nome or "").strip()
                if not nome:
                    continue
                saida.append({campo: nome, "codigo": mapa.get(nome, ""),
                              "valor": rateio._to_float(valor)})
            return saida

        try:
            resultado = rateio.gerar_jsons(
                _linhas("cc", mapa_obra, "obra"),
                _linhas("cat", mapa_categoria, "categoria"),
                base_cat=rateio._to_float(request.form.get("base_categoria", "")) or None)
        except Exception as e:  # noqa: BLE001 — o motivo tem de aparecer na tela
            logger.exception("Análise de SPs: falhou gerar o rateio")
            erro = str(e)

    return render_template(
        "analisesps_ratear.html", aba="ratear",
        referencias=referencias, resultado=resultado, erro=erro,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# BRADESCO
# ---------------------------------------------------------------------------
@bp.route("/bradesco", methods=["GET", "POST"])
@exige_consulta
def tela_bradesco():
    from . import bradesco, consultas

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    colado = ""
    resultado = None
    erro = None
    foco = request.form.get("foco", "1") == "1"

    if request.method == "POST":
        colado = request.form.get("extrato", "")
        if not colado.strip():
            erro = "Cole o texto da tela de operações do Bradesco."
        else:
            try:
                resultado = bradesco.cruzar_tudo(colado, _candidatas_bradesco(),
                                                 foco_agendados=foco)
            except Exception as e:  # noqa: BLE001
                logger.exception("Análise de SPs: falhou cruzar o extrato")
                erro = str(e)

    return render_template(
        "analisesps_bradesco.html", aba="bradesco", base=base,
        colado=colado, resultado=resultado, erro=erro, foco=foco,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


def _candidatas_bradesco() -> list[dict]:
    """As SPs que podem ter sido pagas: as que estão a pagar ou já pagas.

    Não traz a base inteira. Uma conferência de extrato olha o que está na fila
    de pagamento — puxar 59 mil linhas para casar com trinta operações do banco
    seria justamente o desperdício de memória que este módulo evita."""
    from . import consultas
    from .db import consultar

    linhas = consultar(
        "SELECT id, credor, valor_num, conta, forma_pagamento, status_pgt, "
        f"       ({consultas.SQL_STATUS_AGEND}) AS status_agend, "
        "       codigo_barras, centro_custo, vencimento, documento, "
        "       coalesce(f.doc_fiscal, '') AS sp_fiscal "
        "  FROM analisesps.sps s "
        "  LEFT JOIN analisesps.sp_fiscal f ON f.sp_id = s.id "
        " WHERE lower(trim(coalesce(status_pgt,''))) IN ('pagar','pago')")
    nomes = ["id", "credor", "valor_num", "conta", "forma_pagamento",
             "status_pgt", "status_agend", "codigo_barras", "centro_custo",
             "vencimento", "documento", "sp_fiscal"]
    return [dict(zip(nomes, linha)) for linha in linhas]


# ---------------------------------------------------------------------------
# AGENDA
# ---------------------------------------------------------------------------
@bp.route("/agenda")
@exige_consulta
def tela_agenda():
    from . import agenda

    try:
        dias = max(1, min(730, int(request.args.get("dias", 90))))
    except ValueError:
        dias = 90

    try:
        proximos = agenda.proximos(dias)
        alertas = agenda.a_vencer()
        compromissos = agenda.listar()
        erro = None
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Análise de SPs: falhou montar a agenda")
        proximos, alertas, compromissos, erro = [], [], [], str(e)

    return render_template(
        "analisesps_agenda.html", aba="agenda",
        proximos=proximos, alertas=alertas, compromissos=compromissos,
        dias=dias, erro=erro,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# LOG
# ---------------------------------------------------------------------------
@bp.route("/log")
@exige_consulta
def log():
    from . import colunas
    from .db import consultar, consultar_um

    try:
        dias = int(request.args.get("dias", 90))
    except ValueError:
        dias = 90
    dias = dias if dias in (7, 30, 90) else 90
    status = request.args.get("status", "todos")
    if status not in ("todos", "pendente", "enviado", "erro"):
        status = "todos"
    busca = (request.args.get("busca") or "").strip()

    onde = ["criado_em >= now() - make_interval(days => ?)"]
    params: list = [dias]
    if status != "todos":
        onde.append("status = ?")
        params.append(status)
    if busca:
        onde.append("sp_id LIKE ?")
        params.append(f"%{busca}%")
    where = " WHERE " + " AND ".join(onde)

    try:
        contagem = consultar(
            "SELECT status, count(*) FROM analisesps.log_alteracoes"
            " WHERE criado_em >= now() - make_interval(days => ?)"
            " GROUP BY status", (dias,))
        registros = consultar(
            "SELECT criado_em, sp_id, coluna, valor, valor_anterior, acao, "
            "       perfil, pessoa, status, enviado_em, erro "
            f"  FROM analisesps.log_alteracoes{where} "
            " ORDER BY criado_em DESC LIMIT 500", tuple(params))
        pendentes = consultar_um("SELECT count(*) FROM analisesps.fila")
        erro = None
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou ler o registro de alterações")
        contagem, registros, pendentes, erro = [], [], (0,), str(e)

    nomes = ["criado_em", "sp_id", "coluna", "valor", "valor_anterior", "acao",
             "perfil", "pessoa", "status", "enviado_em", "erro"]
    linhas = []
    for registro in registros:
        item = dict(zip(nomes, registro))
        item["campo"] = colunas.ROTULOS.get(item["coluna"], item["coluna"])
        linhas.append(item)

    return render_template(
        "analisesps_log.html", aba="log",
        linhas=linhas, contagem=dict(contagem),
        na_fila=(pendentes[0] if pendentes else 0),
        dias=dias, status=status, busca=busca, erro=erro,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# EXPORTAÇÃO DO RELATÓRIO E DA AUDITORIA
#
# A tela de Solicitações já exportava. Estas duas não, e é justamente delas que
# sai o número que vai para uma reunião — copiar da tela à mão é onde o erro
# entra.
# ---------------------------------------------------------------------------
@bp.route("/relatorio/exportar")
@exige_consulta
def exportar_relatorio():
    """O relatório inteiro num arquivo só, em blocos.

    Sai tudo o que está na tela: os números do topo, cada quebra, os credores e
    o aging — um bloco embaixo do outro, com uma linha em branco entre eles.
    Um arquivo por bloco daria seis downloads para montar uma análise."""
    from . import consultas, exportar as saida
    from .formatos import moeda

    filtros = _filtros_do_pedido()
    tipo = request.args.get("tipo", "geral")
    if tipo not in consultas.TIPOS:
        tipo = "geral"
    periodo = request.args.get("periodo", "tudo")
    if periodo not in consultas.PERIODOS:
        periodo = "tudo"

    def blocos():
        numeros = consultas.numeros_do_relatorio(filtros, tipo, periodo)
        yield ["Relatório", consultas.TIPOS[tipo]]
        yield ["Período", consultas.PERIODOS[periodo]]
        yield ["Contagem pela data de",
               "pagamento" if tipo == "pagas" else "vencimento"]
        yield ["Canceladas", "ficam de fora"]
        yield ["Lançamentos", numeros["quantidade"]]
        yield ["Total", moeda(numeros["total"])]
        yield ["Ticket médio", moeda(numeros["ticket"])]
        yield ["Vencidos (quantidade)", numeros["vencidos_qtd"]]
        yield ["Vencidos (valor)", moeda(numeros["vencidos_total"])]

        for dimensao, rotulo in consultas.DIMENSOES.items():
            linhas = consultas.agregar(filtros, dimensao, tipo, periodo, 1000)
            if not linhas:
                continue
            yield []
            yield [f"Por {rotulo.lower()}", "Quantidade", "Total"]
            for l in linhas:
                yield [l["rotulo"], l["quantidade"], moeda(l["total"])]

        credores = consultas.top_credores(filtros, tipo, periodo, 1000)
        if credores:
            yield []
            yield ["CPF/CNPJ", "Credor", "Quantidade", "Total"]
            for c in credores:
                yield [c["documento"], c["credor"], c["quantidade"],
                       moeda(c["total"])]

        aging = consultas.aging_vencidos(filtros, periodo)
        if aging:
            yield []
            yield ["Atraso", "Quantidade", "Total"]
            for f in aging:
                yield [f["faixa"], f["quantidade"], moeda(f["total"])]

    return saida.resposta(f"relatorio_{tipo}", ["Relatório da Análise de SPs"],
                          blocos())


@bp.route("/auditoria/exportar")
@exige_consulta
def exportar_auditoria():
    """A checagem aberta na tela, em arquivo.

    Auditoria serve para alguém ir atrás — e ir atrás quer dizer mandar a lista
    para outra pessoa. Ler da tela e digitar de novo é onde o erro entra."""
    from . import auditoria as checagens
    from . import exportar as saida
    from .formatos import data_br, moeda

    filtros = _filtros_do_pedido()
    usar_filtros = request.args.get("usar_filtros") == "1"
    checagem = request.args.get("checagem", "")
    if checagem not in checagens.CHECAGENS:
        return render_template(
            "analisesps_erro.html", titulo="Nada para exportar",
            mensagem="Abra uma das checagens antes de exportar."), 400

    def sps(linhas, extras):
        yield ["SP", "Credor", "Valor", "Vencimento"] + [r for r, _ in extras]
        for l in linhas:
            yield ([l["id"], l["credor"], moeda(l["valor_num"]),
                    data_br(l.get("vencimento_d"))]
                   + [l.get(campo) for _, campo in extras])

    if checagem == "pontualidade":
        try:
            minimo = max(1, int(request.args.get("minimo", 5)))
        except ValueError:
            minimo = 5

        def blocos():
            yield ["Responsável", "SPs", "Antecedência média (dias)",
                   "Mediana (dias)", "Atrasadas", "% atrasadas",
                   "R$ atrasado", "R$ total"]
            for l in checagens.pontualidade(filtros, usar_filtros, minimo):
                yield [l["responsavel"], l["quantidade"], l["media_dias"],
                       l["mediana_dias"], l["atrasados"],
                       l["percentual_atrasados"], moeda(l["valor_atrasado"]),
                       moeda(l["valor_total"])]

    elif checagem == "codigos_barras":
        def blocos():
            achados = checagens.codigos_de_barras(filtros, usar_filtros)
            yield ["Boletos inválidos"]
            yield from sps(achados["invalidos"],
                           [("Código de barras", "codigo_barras")])
            yield []
            yield ["Boletos repetidos"]
            yield from sps(achados["duplicados"],
                           [("Código de barras", "codigo_barras")])

    else:
        colunas_extras = {
            "risco_ia": [("CPF/CNPJ", "documento"), ("Análise", "analise_ia")],
            "nf_duplicada": [("CPF/CNPJ", "documento"), ("Nº NF", "nf"),
                             ("Quantas", "quantos")],
            "possivel_duplicidade": [("CPF/CNPJ", "documento"),
                                     ("No grupo", "quantos"),
                                     ("Janela (dias)", "janela")],
            "sem_classificacao": [("Falta", "faltando"),
                                  ("Centro de custo", "centro_custo"),
                                  ("Projeto", "projeto")],
            "sem_integracao": [("Status", "status_pgt")],
        }
        funcao = {
            "risco_ia": checagens.risco_ia,
            "nf_duplicada": checagens.nf_duplicada,
            "possivel_duplicidade": checagens.possivel_duplicidade,
            "sem_classificacao": checagens.sem_classificacao,
            "sem_integracao": checagens.sem_integracao_omie,
        }[checagem]

        def blocos():
            yield from sps(funcao(filtros, usar_filtros),
                           colunas_extras[checagem])

    return saida.resposta(f"auditoria_{checagem}",
                          [checagens.CHECAGENS[checagem]], blocos())


@bp.route("/lote/exportar")
@exige_consulta
def exportar_lote():
    """O lote, grupo a grupo, com o total de cada um.

    É o que se manda para quem vai efetivar os pagamentos: a mesma organização
    da tela, com os títulos que quem montou o lote escolheu."""
    from . import exportar as saida
    from . import lote
    from .formatos import data_br, moeda

    # O lote é lido AQUI, antes de a resposta começar a ser enviada. Se fosse
    # lido lá dentro do gerador, o cabeçalho já teria saído com HTTP 200 e a
    # pessoa receberia um arquivo pela metade, sem erro nenhum — pior do que
    # uma mensagem.
    try:
        montado = lote.montar(lote.ler()["conteudo"])
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou montar o lote para exportar")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui montar o lote",
            mensagem=f"{e}"), 500

    def blocos():
        for grupo in montado["grupos"]:
            if not grupo["linhas"] and not grupo["nao_encontrados"]:
                continue
            yield [grupo["titulo_exibido"]]
            yield ["SP", "Vencimento", "Credor", "Valor", "Status",
                   "Agendamento", "Forma", "Conta", "Informação p/ pgt"]
            for l in grupo["linhas"]:
                yield [l["id"], data_br(l["vencimento_d"]), l["credor"],
                       moeda(l["valor_num"]), l["status_pgt"],
                       l["status_agend"], l["forma_pagamento"], l["conta"],
                       l["info_pgt"]]
            yield ["", "", "Total do grupo", moeda(grupo["total"])]
            for perdida in grupo["nao_encontrados"]:
                yield [perdida, "não encontrada na base"]
            yield []
        yield ["", "", "TOTAL GERAL", moeda(montado["total_geral"])]

    return saida.resposta("lote", ["Lote de pagamentos"], blocos())


@bp.route("/relatorio/pdf")
@exige_consulta
def relatorio_pdf():
    """O mesmo relatório da tela, em PDF, para anexar ou imprimir."""
    from flask import Response

    from . import consultas, pdf
    from .horario import agora

    filtros = _filtros_do_pedido()
    tipo = request.args.get("tipo", "geral")
    if tipo not in consultas.TIPOS:
        tipo = "geral"
    periodo = request.args.get("periodo", "tudo")
    if periodo not in consultas.PERIODOS:
        periodo = "tudo"

    try:
        conteudo = pdf.relatorio(filtros, tipo, periodo)
    except Exception as e:  # noqa: BLE001 — falha no PDF nao derruba a tela
        logger.exception("Análise de SPs: falhou gerar o PDF do relatório")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui gerar o PDF",
            mensagem=f"{e}. A exportação em CSV continua disponível."), 500

    nome = f"relatorio_{tipo}_{agora().strftime('%Y-%m-%d_%H%M')}.pdf"
    return Response(conteudo, mimetype="application/pdf",
                    headers={"Content-Disposition":
                             f'attachment; filename="{nome}"'})


@bp.route("/lote/pdf")
@exige_consulta
def lote_pdf():
    """O papel que acompanha a remessa de pagamentos."""
    from flask import Response

    from . import lote, pdf
    from .horario import agora

    try:
        montado = lote.montar(lote.ler()["conteudo"])
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou montar o lote para o PDF")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui montar o lote",
            mensagem=f"{e}"), 500
    if not montado["quantidade"]:
        return render_template(
            "analisesps_erro.html", titulo="Lote vazio",
            mensagem="Não há SPs no lote para pôr no relatório."), 400

    try:
        conteudo = pdf.relatorio_do_lote(montado)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou gerar o PDF do lote")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui gerar o PDF",
            mensagem=f"{e}. A exportação em CSV continua disponível."), 500

    nome = f"lote_{agora().strftime('%Y-%m-%d_%H%M')}.pdf"
    return Response(conteudo, mimetype="application/pdf",
                    headers={"Content-Disposition":
                             f'attachment; filename="{nome}"'})


# ---------------------------------------------------------------------------
# Erros
# ---------------------------------------------------------------------------
@bp.errorhandler(500)
def erro_interno(e):
    logger.exception("Análise de SPs: erro não tratado")
    return render_template(
        "analisesps_erro.html", titulo="Deu erro",
        mensagem="Algo quebrou aqui dentro. O detalhe foi para o log do "
                 "serviço. Se acabou de publicar uma alteração, confira em "
                 "/analisesps/saude qual versão está no ar."), 500
