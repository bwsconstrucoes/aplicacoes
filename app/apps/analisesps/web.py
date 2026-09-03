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

    if request.method == "POST" and configurados:
        perfil = auth.identificar(request.form.get("senha", ""))
        if perfil:
            auth.entrar_na_sessao(perfil)
            destino = request.args.get("proximo") or ""
            # Só aceita destino interno: um "proximo" apontando para fora
            # viraria um jeito de usar o login da empresa como trampolim.
            if destino.startswith("/analisesps"):
                return redirect(destino)
            return redirect(url_for("analisesps.solicitacoes"))
        erro = "Senha incorreta."
        logger.warning("Análise de SPs: tentativa de entrada com senha errada.")

    return render_template("analisesps_login.html",
                           sem_senha=not configurados, erro=erro)


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
        args=request.args,
        opcoes={
            "status_pgt": consultas.opcoes("status_pgt"),
            "conta": consultas.opcoes("conta"),
            "forma": consultas.opcoes("forma_pagamento"),
            "tipo_despesa": consultas.opcoes("tipo_despesa"),
            "projeto": consultas.opcoes("projeto"),
            "responsavel": consultas.opcoes("responsavel"),
            "centro_custo": consultas.opcoes("centro_custo", limite=200),
            "status_agend": consultas.opcoes_agendamento(),
        },
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


@bp.route("/sp/<sp_id>")
@exige_consulta
def detalhe(sp_id):
    from . import consultas
    registro = consultas.uma(sp_id)
    if registro is None:
        return render_template("analisesps_erro.html",
                               titulo="Não encontrado",
                               mensagem="Esta SP não existe na base."), 404
    return render_template("analisesps_detalhe.html", sp=registro,
                           pode_operar=auth.pode_operar())


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

    def limpar(valor):
        """Ponto-e-vírgula e quebra de linha dentro de uma célula quebrariam o
        arquivo. Viram espaço; a aspa dupla é escapada como o CSV manda."""
        texto = "" if valor is None else str(valor)
        texto = texto.replace("\r", " ").replace("\n", " ").replace('"', '""')
        return f'"{texto}"' if (";" in texto or '"' in texto) else texto

    def gerar():
        yield "﻿"                      # BOM: o Excel reconhece o acento
        yield ";".join(cabecalho) + "\r\n"
        pagina = 1
        while True:
            linhas = consultas.listar(filtros, ordem=ordem, pagina=pagina)
            if not linhas:
                break
            for linha in linhas:
                yield ";".join(limpar(c) for c in campos(linha)) + "\r\n"
            if len(linhas) < consultas.POR_PAGINA:
                break
            pagina += 1

    from .horario import agora
    nome = f"analise_sps_{agora().strftime('%Y-%m-%d_%H%M')}.csv"
    return Response(gerar(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f'attachment; filename="{nome}"'})


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
                "  (sp_id, coluna, valor, valor_anterior, acao, perfil, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pendente')",
                (sp_id, coluna, valor, anteriores.get(sp_id), acao, perfil))
        conn.commit()

    logger.info("Análise de SPs: %s alterou '%s' de %d SP(s) para '%s'.",
                perfil, coluna, len(ids), valor)

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
