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
from markupsafe import escape

from . import auth
from . import preferencias
from .auth import exige_consulta, exige_operador, publica

logger = logging.getLogger("analisesps.web")

bp = Blueprint("analisesps", __name__,
               url_prefix="/analisesps",
               template_folder="templates",
               static_folder="static")

bp.before_request(auth.exigir_login)


# ---------------------------------------------------------------------------
# A PÁGINA VAI COMPRIMIDA
#
# Medido com as 59.055 SPs: a tela de Solicitações são 430 KB de HTML — 200
# linhas com vinte colunas cada. O servidor mandava isso CRU, e nada no
# caminho comprimia. Comprimido dá 27 KB, dezesseis vezes menos, e custa
# 1,4 ms de processamento.
#
# É a maior diferença de todas para quem está do outro lado: o banco pode
# responder em 100 ms, mas meio megabyte ainda leva segundos numa internet
# ruim ou no celular na obra. Nenhuma otimização de consulta compensa isso.
#
# TRÊS COISAS FICAM DE FORA, e cada uma por um motivo:
#   - o que sai em fluxo (a exportação CSV, que é escrita em blocos para não
#     abrir a base inteira na memória): comprimir obrigaria a juntar tudo
#     antes, que é exatamente o que aquele caminho evita;
#   - o que já vem comprimido (PDF, PNG, o xlsx do BeeVale) — reapertar um
#     arquivo comprimido só gasta processador e às vezes aumenta;
#   - o que é pequeno demais para valer o esforço.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# A TELA FICA GUARDADA NO NAVEGADOR POR CINCO MINUTOS
#
# Pedido do dono, e a observação dele estava certa: "eu filtro, vou para o
# Lote, volto para Solicitações — e ele refaz tudo de novo. Se eu tivesse duas
# abas do navegador eu alternaria na hora." Hoje toda troca de aba refazia as
# consultas e remontava a tela inteira, mesmo três segundos depois.
#
# Guardada, a volta não vai ao servidor: aparece na hora, com o filtro e tudo.
#
# SÓ AS TELAS DE LEITURA ENTRAM, e a razão é concreta: Lote, Agenda, Ratear e
# Bradesco recebem alterações NO PRÓPRIO ENDEREÇO (o formulário manda para
# elas mesmas). Guardá-las mostraria o estado ANTERIOR à mudança que a pessoa
# acabou de fazer — que é pior do que ser lento. As quatro daqui só são
# alteradas por `/api/...`, e toda alteração por lá termina recarregando a
# tela, o que substitui o que estava guardado.
#
# AS REDES DE PROTEÇÃO JÁ EXISTIAM e continuam valendo na tela guardada:
#   - o relógio no alto diz de quando é o dado ("base de 09/09 às 14:32");
#   - a busca de 90 em 90 segundos continua rodando e avisa se a base mudou.
#
# O QUE FICA EM ABERTO, dito com todas as letras: se OUTRA pessoa alterar algo,
# você pode ver o estado anterior por até cinco minutos. Foi escolha do dono
# em 09/09/2026, com o risco na frente — ele considerou viável para o uso de
# quatro pessoas na mesma empresa.
# ---------------------------------------------------------------------------
SEGUNDOS_GUARDADA = 300
TELAS_QUE_FICAM_GUARDADAS = {
    "analisesps.solicitacoes",
    "analisesps.relatorio",
    "analisesps.calendario",
    "analisesps.auditoria",
    "analisesps.log",
    "analisesps.tela_lote",
}

# O LOTE entrou nesta lista depois das outras, e por isso tem uma ressalva
# própria. Ele é a tela onde se ALTERA coisa — e mostrar o estado anterior a
# uma alteração que a pessoa acabou de fazer seria pior do que ser lento.
#
# O que torna seguro guardá-lo: toda alteração é um POST para o próprio
# endereço, que depois redireciona (o formulário nunca responde direto). A
# tela que volta do POST traz `?aviso=` e NÃO é guardada — senão o recado de
# "salvo" reapareceria minutos depois, dizendo que algo acabou de acontecer
# quando não aconteceu.
#
# E a rede de segurança que não depende de o navegador se comportar: a tela
# carrega a HORA em que o lote foi salvo, e o navegador compara essa hora com
# a última que viu. Se a tela guardada for anterior à última salvada, ela se
# recarrega sozinha. Está em `analisesps.js`, junto do porquê.
def _tela_que_veio_de_alteracao() -> bool:
    """A tela do Lote logo depois de salvar. Não pode ficar guardada."""
    return request.args.get("aviso") is not None


# A folha de estilo e o javascript NÃO MUDAM entre uma publicação e outra, e
# estavam sendo reconferidos com o servidor a CADA tela: dois 304 de ~400 ms
# cada, medidos na produção pelo dono em 09/09/2026. Quase um segundo por
# navegação, gasto para o servidor responder "não mudou nada".
#
# Agora valem um ano, e o endereço deles carrega a VERSÃO publicada — quando
# sai uma publicação nova, o endereço muda e o navegador busca sozinho. Sem a
# versão no endereço, guardar por um ano seria uma armadilha: uma correção de
# tela levaria um ano para chegar em quem já tinha aberto o sistema.
UM_ANO = 365 * 24 * 3600


def versao_publicada() -> str:
    """A publicação em que este código está. Entra no endereço dos arquivos."""
    return os.getenv("RENDER_GIT_COMMIT", "")[:8] or "dev"


# ---------------------------------------------------------------------------
# AS SUBTELAS DA FOLHA
#
# Uma entrada no menu ("Folha PGT") e, por dentro, as telas de cada trabalho —
# do jeito que a planilha que ela substitui é organizada: uma aba para
# alimentação, uma para transporte, uma para diaristas, uma para importar.
#
# ⚠️ AS QUE AINDA NÃO EXISTEM NÃO APARECEM AQUI. Aba que abre vazia promete o
# que não há, e faz a pessoa procurar o que não foi feito — o dono acabou de
# passar por isso procurando telas que eu não tinha escrito.
# ⚠️ AGRUPADAS POR ASSUNTO, e a ordem é correção do dono em 28/09/2026:
#
#   "Não tem lógica nas telas aqui. Tipo assim, tem folha, depois tem ponto,
#    depois tem colaboradores, depois tem feriado, depois tem alimentação e
#    transporte (…) Folha da contabilidade, alimentação, ambos são PAGAMENTOS,
#    né? Então acho que era para estar junto. O que é CADASTRO é para estar
#    junto. Ou seja, tem que ter uma lógica aí nas sequências dessas telas."
#
# Ele está certo, e o erro era meu: a ordem anterior era a ordem em que EU
# construí as peças, não a ordem em que ele trabalha. Agora são três grupos:
#
#   visão   — onde se olha o resultado
#   paga    — o que vira dinheiro saindo
#   base    — o que alimenta o cálculo (cadastro, ponto, calendário, rateio)
#
# Cada entrada é (chave, rótulo, rota, grupo).
# ⚠️ OS RÓTULOS SAÍRAM em 29/09/2026: *"eu não pedi pra colocar Pagamentos e
# Cadastro e base do cálculo. Era apenas pra reorganizar."* Os grupos continuam
# existindo — é o que mantém a ordem com lógica — mas não aparecem escritos. Um
# separador fino entre eles é o suficiente.
GRUPOS_DA_FOLHA = [("visao", ""), ("paga", ""), ("base", "")]

# ⚠️ O PANORAMA SAIU em 30/09/2026, por decisão dele: *"a tela Panorama tá sem
# sentido. A tela que precisamos é Folha da Contabilidade."* O que ele tinha de
# útil — o que já foi pago no mês, por obra e por conta — mora agora na janela
# "Divisão por obra" da própria folha aberta.
SUBTELAS_DA_FOLHA = [
    # OS PAGAMENTOS, na ordem do mês: a folha da contabilidade é a maior e a
    # primeira; os auxílios saem depois; os diaristas fecham.
    ("importar", "Folha da contabilidade", "analisesps.tela_folha_importar",
     "paga"),
    ("auxilios", "Alimentação e transporte", "analisesps.tela_folha_auxilio",
     "paga"),
    ("diaristas", "Diaristas", "analisesps.tela_folha_diaristas", "paga"),
    ("pagamento", "Arquivos gerados", "analisesps.tela_folha_pagamento", "paga"),

    # A BASE. Vem depois porque é o que se arruma quando algo não fecha — mas é
    # onde tudo começa.
    ("colaboradores", "Colaboradores", "analisesps.tela_colaboradores", "base"),
    ("ponto", "Ponto", "analisesps.tela_folha_ponto", "base"),
    ("calendario", "Feriados e férias", "analisesps.tela_folha_calendario",
     "base"),
    ("rateio", "Rateio das obras", "analisesps.tela_folha_rateio", "base"),
]


def subtelas_da_folha() -> list:
    """As subtelas que a pessoa logada alcança.

    Mesmo motivo do menu de cima: aba que responde 404 é pior do que aba
    nenhuma. O Rateio é só do mestre (ele decide para qual obra vai o salário),
    então quem opera a folha não vê essa aba."""
    return [s for s in SUBTELAS_DA_FOLHA
            if not (auth.e_so_do_mestre(s[2]) and not auth.e_mestre())]


def subtelas_agrupadas() -> list:
    """As subtelas em grupos, na ordem dos grupos. `[(rótulo, [subtelas])]`.

    ⚠️ A LÓGICA DOS GRUPOS FICA NO PYTHON, não no HTML: a faixa de abas do alto e
    o menu de tela pequena desenham a MESMA lista, e duas cópias divergiriam no
    dia em que uma tela nova entrasse em uma só."""
    alcancadas = subtelas_da_folha()
    saida = []
    for chave, rotulo in GRUPOS_DA_FOLHA:
        doGrupo = [s for s in alcancadas if s[3] == chave]
        if doGrupo:
            saida.append((rotulo, doGrupo))
    return saida


# ---------------------------------------------------------------------------
# AS TELAS DO MÓDULO, NA ORDEM EM QUE ELE TRABALHA
#
# A ordem é do dono, pedida em 13/09/2026: *"eu queria colocar solicitações
# primeiro, depois lote, aí depois eu queria comprovantes, depois relatório, e
# depois documentação fiscal, aí depois agenda, e pronto, aí pode seguir com os
# demais."* É o caminho do dia dele — pedir, juntar no lote, dar baixa nos
# comprovantes, olhar o resultado, tratar a documentação fiscal.
#
# POR QUE A LISTA VIVE AQUI, e não solta no HTML: ela é desenhada em DOIS
# lugares — a faixa de abas do alto e o menu que se abre em tela pequena. Duas
# cópias divergiriam no dia em que uma tela nova entrasse em uma só, e a que
# ficasse de fora seria justamente a do menu, que é o caminho de quem está no
# celular e não tem como descobrir que faltou.
# O nome que assina o que for feito pela PORTA DE EMERGÊNCIA. Ela não tem
# cadastro por trás, então não tem nome de gente — e deixar vazio faria o
# registro de alterações dizer "—", que não diz nada. Escrito assim, em
# maiúsculas, quem ler a auditoria sabe na hora por onde a pessoa entrou.
NOME_DA_EMERGENCIA = "MESTRE (emergência)"

TELAS = [
    ("solicitacoes",  "Solicitações",  "analisesps.solicitacoes"),
    ("lote",          "Lote",          "analisesps.tela_lote"),
    ("comprovantes",  "Comprovantes",  "analisesps.tela_comprovantes"),
    ("relatorio",     "Relatório",     "analisesps.relatorio"),
    ("fiscal",        "Doc. Fiscal",   "analisesps.tela_fiscal"),
    ("agenda",        "Agenda",        "analisesps.tela_agenda"),
    # ⚠️ O CALENDÁRIO ENTRA DEPOIS DA AGENDA, e não entre Relatório e Doc.
    # Fiscal, embora seja do Relatório que ele seja irmão. As seis primeiras
    # telas são o caminho do dia do dono, dito por ele em 13/09/2026 e travado
    # por teste: mexer nelas para acomodar tela nova é desfazer uma decisão
    # dele. Tela nova entra entre "os demais", que é onde ele mesmo mandou.
    # Ao lado da Agenda também lê bem: são as duas grades de mês do módulo.
    ("calendario",    "Calendário",    "analisesps.calendario"),
    ("conciliacao",   "Conciliação",   "analisesps.tela_conciliacao"),
    ("auditoria",     "Auditoria",     "analisesps.auditoria"),
    ("ratear",        "Ratear",        "analisesps.ratear"),
    # ⚠️ UMA ENTRADA SÓ PARA A FOLHA, e por dentro as subtelas. Correção do dono
    # em 27/09/2026, depois de ver duas entradas novas no menu:
    #
    #   "Eles têm que estar dentro de uma tela só. E lá ter as subtelas, porque
    #    senão vai ficar tela demais, fica até misturado com o restante, que tem
    #    mais a ver com o financeiro. (…) pode chamar uma tela de folha. Pode
    #    botar abreviado, Folha PGT."
    #
    # Ele está certo, e o erro era de desenho meu: cada peça nova da folha viraria
    # uma entrada no menu, e o menu deste módulo é de FINANCEIRO. A folha é uma
    # área com telas próprias por dentro — como as abas da planilha que ela vai
    # substituir (alimentação, transporte, diaristas, importação).
    ("folha",         "Folha PGT",     "analisesps.tela_folha"),
    ("bradesco",      "Bradesco",      "analisesps.tela_bradesco"),
    ("log",           "Log",           "analisesps.log"),
    ("configuracoes", "Configurações", "analisesps.configuracoes"),
]


@bp.app_context_processor
def _versao_para_os_templates():
    # ⚠️ O MENU MOSTRA SÓ AS TELAS DA PESSOA. Deixar no menu uma tela que
    # responde 404 é pior do que não mostrá-la: a pessoa clica, não entende e
    # liga para o dono. O mestre continua vendo todas.
    return {"versao_estatica": versao_publicada(),
            "telas": auth.telas_para_o_menu(TELAS)}


@bp.after_request
def _guardar_no_navegador(resposta):
    """Diz ao navegador que pode reusar esta tela por alguns minutos."""
    try:
        if request.method != "GET" or resposta.status_code != 200:
            return resposta
        if request.endpoint == "analisesps.static":
            # `immutable` é o que faz o navegador NEM PERGUNTAR. O endereço
            # carrega a versão publicada, então o arquivo sob aquele endereço
            # realmente nunca muda.
            resposta.headers["Cache-Control"] = (
                f"public, max-age={UM_ANO}, immutable")
            return resposta
        if request.endpoint not in TELAS_QUE_FICAM_GUARDADAS:
            return resposta
        if _tela_que_veio_de_alteracao():
            return resposta
        if not (resposta.mimetype or "").startswith("text/html"):
            return resposta
        # `private` porque a tela é de UMA pessoa: nada de cache compartilhado
        # no caminho guardando a lista de pagamentos da empresa.
        resposta.headers["Cache-Control"] = (
            f"private, max-age={SEGUNDOS_GUARDADA}")
    except Exception:  # noqa: BLE001 — guardar é conforto; a tela é o que importa
        logger.exception("Análise de SPs: falhou marcar a tela como guardável")
    return resposta


NIVEL_COMPRESSAO = 1        # 6,3% do tamanho por 1,4 ms; o nível 6 chega a
                            # 4,4% mas gasta o dobro, e esta instância tem
                            # 2 GB e histórico de morrer de memória.
MINIMO_PARA_COMPRIMIR = 1024
TIPOS_QUE_COMPRIMEM = ("text/html", "text/css", "text/plain",
                       "application/javascript", "application/json",
                       "image/svg+xml")


@bp.after_request
def _comprimir(resposta):
    """Manda a página comprimida quando o navegador aceita. Ver o bloco acima."""
    try:
        if resposta.direct_passthrough or resposta.is_streamed:
            return resposta
        if resposta.headers.get("Content-Encoding"):
            return resposta
        if resposta.status_code < 200 or resposta.status_code >= 300:
            return resposta
        if "gzip" not in (request.headers.get("Accept-Encoding") or "").lower():
            return resposta
        tipo = (resposta.mimetype or "").lower()
        if not any(tipo.startswith(t) for t in TIPOS_QUE_COMPRIMEM):
            return resposta

        conteudo = resposta.get_data()
        if len(conteudo) < MINIMO_PARA_COMPRIMIR:
            return resposta

        import gzip
        resposta.set_data(gzip.compress(conteudo, NIVEL_COMPRESSAO))
        resposta.headers["Content-Encoding"] = "gzip"
        resposta.headers["Content-Length"] = str(len(resposta.get_data()))
        # Sem isto, um cache no caminho poderia entregar a versão comprimida a
        # um navegador que não pediu — e ele mostraria lixo na tela.
        resposta.headers.add("Vary", "Accept-Encoding")
    except Exception:  # noqa: BLE001 — comprimir é conforto; a página é o que importa
        logger.exception("Análise de SPs: falhou comprimir a resposta")
    return resposta


@bp.app_template_filter("moeda")
def _filtro_moeda(valor):
    from .formatos import moeda
    return moeda(valor)


@bp.app_template_filter("moeda_curta")
def _filtro_moeda_curta(valor):
    """O valor em poucas letras, para onde ele não cabe. Ver `formatos`."""
    from .formatos import moeda_curta
    return moeda_curta(valor)


@bp.app_template_filter("data_br")
def _filtro_data(valor):
    from .formatos import data_br
    return data_br(valor)


@bp.app_template_filter("momento_br")
def _filtro_momento(valor):
    """Data E hora, na hora de Brasília. Para o que só faz sentido com a
    hora — "a base é de quando?"."""
    from .formatos import momento_br
    return momento_br(valor)


@bp.app_template_filter("com_links")
def _filtro_com_links(texto):
    """Texto livre com os endereços já clicáveis, escapado antes de tudo.

    A descrição da SP costuma trazer o link de uma pasta ou de um contrato.
    Como texto puro, era preciso selecionar na mão e colar no navegador."""
    from .formatos import com_links
    return com_links(texto)


# ---------------------------------------------------------------------------
# Entrada e saída
# ---------------------------------------------------------------------------
@bp.route("/entrar", methods=["GET", "POST"])
@publica("é a própria tela de login; sem ela ninguém consegue entrar")
def entrar():
    """A entrada. SÓ USUÁRIO E SENHA desde 25/09/2026.

    A lista de nomes ao lado da senha acabou — pedido do dono: *"elimine do
    login o login via Nomes na lista da entrada. Vamos ficar somente com os
    cadastrados."* Quem entra, entra pelo cadastro, e o nome que assina o lote,
    os filtros e o registro de alterações é o do cadastro dele.

    ⚠️ SOBROU UMA PORTA DE EMERGÊNCIA: a senha do Render, com o campo de
    usuário EM BRANCO. Ela não é o caminho do dia a dia, e a tela diz isso.
    Sem ela, perder o último cadastro de mestre trancaria todo mundo para fora
    sem volta — não há e-mail de recuperação nem outro administrador.
    """
    configurados = auth.perfis_configurados()
    erro = None
    login = (request.form.get("usuario") or "").strip()

    if request.method == "POST":
        senha = request.form.get("senha", "")

        # ⚠️ A SENHA DO RENDER DECIDE PRIMEIRO, com o campo de usuário
        # preenchido ou não. Isto não é conveniência: é o conserto de um jeito
        # de trancar o dono para fora, que aconteceu de verdade no painel em
        # 22/09/2026. A tela passou a ter um campo novo, e o gerenciador de
        # senhas do navegador o preenchia sozinho — o pedido caía no caminho
        # do cadastro e a resposta era "usuário ou senha incorretos", com a
        # senha certa digitada.
        #
        # Conferir a senha geral antes não afrouxa nada: quem a conhece JÁ vê
        # tudo. O que se perde é só a chance de o navegador escolher o caminho.
        perfil = auth.identificar(senha) if configurados else None
        if perfil:
            auth.entrar_na_sessao(perfil, NOME_DA_EMERGENCIA)
            logger.warning("Análise de SPs: entrada pela PORTA DE EMERGÊNCIA "
                           "(senha geral do serviço, sem cadastro por trás).")
            return redirect(_para_onde_depois_de_entrar())
        elif login:
            # Caminho do CADASTRO PRÓPRIO (migração 023).
            from . import usuarios
            pessoa = usuarios.buscar(login)
            if not pessoa or not usuarios.senha_confere(pessoa, senha):
                logger.warning("Análise de SPs: entrada recusada para o "
                               "usuário %r.", login)
                # A MESMA resposta para usuário que não existe e para senha
                # errada: dizer qual dos dois falhou entrega metade da
                # resposta a quem está tentando.
                erro = ("Usuário ou senha incorretos. Se você entra com a "
                        "senha geral do sistema, apague o que estiver no campo "
                        "Usuário — o navegador às vezes preenche sozinho.")
            elif not pessoa.get("telas") and not pessoa.get("mestre"):
                # ⚠️ O MESTRE ESCAPA DESTA TRAVA, e tem de escapar: ele alcança
                # todas as telas por definição, então as caixinhas dele estão
                # vazias por ser desnecessárias — não por estarem faltando.
                # Sem esta exceção, o dono cadastraria a si mesmo como mestre e
                # a própria tela o barraria na entrada seguinte.
                logger.warning("Análise de SPs: %s entrou sem nenhuma tela "
                               "liberada.", login)
                erro = ("O seu acesso ainda não tem nenhuma tela liberada. "
                        "Fale com quem cuida do sistema.")
            else:
                oficial = auth.limpar_nome(pessoa.get("nome") or pessoa["usuario"])
                auth.entrar_na_sessao(
                    auth.OPERADOR if pessoa["pode_operar"] else auth.CONSULTA,
                    oficial, usuario_id=pessoa["id"])
                usuarios.marcar_acesso(pessoa["id"])
                return redirect(_para_onde_depois_de_entrar(
                    None if pessoa.get("mestre") else pessoa["telas"]))
        else:
            erro = ("Digite o seu usuário e a sua senha. Se você cuida do "
                    "sistema e perdeu o acesso, deixe o usuário em branco e "
                    "use a senha geral do serviço.")
            logger.warning("Análise de SPs: entrada recusada — sem usuário, e "
                           "a senha não é a geral.")

    from . import usuarios
    try:
        tem_mestre = usuarios.ha_mestre()
    except Exception:  # noqa: BLE001 — a tela de entrada nunca cai por isto
        logger.exception("Análise de SPs: não consegui saber se há mestre")
        tem_mestre = False

    return render_template(
        "analisesps_login.html", sem_senha=not configurados, erro=erro,
        usuario=login, tem_mestre=tem_mestre)


def _para_onde_depois_de_entrar(telas=None) -> str:
    """Para onde mandar quem acabou de entrar.

    Quem tem cadastro pode NÃO TER a tela de Solicitações — mandá-lo para ela
    daria um 404 logo depois de um login que funcionou, e ele concluiria que o
    acesso não foi criado. Então vai para a primeira tela que ele tem, na
    ordem do menu."""
    destino = request.args.get("proximo") or ""
    # Só aceita destino interno: um "proximo" apontando para fora viraria um
    # jeito de usar o login da empresa como trampolim.
    if destino.startswith("/analisesps"):
        return destino
    if telas:
        permitidas = set(telas)
        primeira = next((t for t in TELAS if t[0] in permitidas), None)
        if primeira:
            return url_for(primeira[2])
    return url_for("analisesps.solicitacoes")


@bp.route("/sair")
@exige_consulta
def sair():
    """Larga a sessão. O NOME continua lembrado neste navegador — sair é
    encerrar o acesso, não esquecer quem você é. Quem quiser trocar de pessoa
    apaga o campo e digita outro; é o mesmo campo."""
    auth.sair_da_sessao()
    resposta = redirect(url_for("analisesps.entrar"))
    # APAGA O QUE FICOU GUARDADO NO NAVEGADOR. Sem isto, num computador
    # compartilhado, apertar Voltar depois de sair mostraria as telas da
    # pessoa anterior pelos minutos que faltassem. Sair tem de sair de
    # verdade.
    resposta.headers["Clear-Site-Data"] = '"cache"'
    resposta.headers["Cache-Control"] = "no-store"
    return resposta


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
                 "situacoes", "fiscais", "periodo_ini", "periodo_fim",
                 "pgt_ini", "pgt_fim", "valor_ini", "valor_fim", "ordem",
                 # Os da tela de notas, que tem recortes próprios — e a VISÃO,
                 # porque sair para outro menu e voltar jogava a pessoa de volta
                 # em "por lançamento". Reclamação do dono em 13/09/2026: *"eu
                 # estava em por nota e fui pra outra tela e voltei; era pra
                 # voltar pra por nota."*
                 "visao", "nota", "busca_nota", "emissao_ini", "emissao_fim")

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


def _TODAS_COLUNAS() -> list:
    from . import tabela
    return tabela.DEFINICOES


def _colunas_da_pessoa() -> list:
    """As colunas que ESTA pessoa vê na tabela. Nunca derruba a tela: se a
    preferência não puder ser lida, mostra o padrão."""
    from . import tabela
    guardado = preferencias.ler(auth.pessoa_atual(), tabela.PREFERENCIA)
    return tabela.escolhidas(guardado)


@bp.route("/colunas", methods=["POST"])
@exige_consulta
def escolher_colunas():
    """Guarda as colunas escolhidas e devolve a pessoa à tela de onde veio.

    É `@exige_consulta`, e não `@exige_operador`, de propósito: escolher o que
    se vê não altera dado nenhum da empresa — é preferência de quem olha, e o
    perfil Consulta olha."""
    from . import tabela

    acao = request.form.get("acao", "")
    if acao == "padrao":
        escolhidas = []          # vazio = volta ao padrão, ver tabela.py
    elif acao == "alternar":
        # Mostrar/esconder UMA coluna, sem abrir a lista inteira. É o caminho
        # da descrição, que se esconde e se mostra dez vezes por dia.
        alvo = request.form.get("coluna", "")
        atuais = [c.chave for c in _colunas_da_pessoa()]
        if alvo in tabela.POR_CHAVE:
            if alvo in atuais:
                atuais.remove(alvo)
            else:
                atuais.append(alvo)
        # Tirar a última coluna deixaria a tabela vazia; nesse caso o padrão
        # volta, e é melhor do que uma tabela sem coluna nenhuma.
        escolhidas = atuais
    else:
        escolhidas = [c for c in request.form.getlist("coluna")
                      if c in tabela.POR_CHAVE]

    # Guarda a escolha E as colunas que existiam agora: é o que faz uma coluna
    # criada depois aparecer para quem já tinha escolhido — ver `tabela.py`.
    preferencias.gravar(auth.pessoa_atual(), tabela.PREFERENCIA,
                        tabela.para_guardar(escolhidas))

    # A volta sai do formulário, então é conferida: destino de fora daqui
    # transformaria esta rota em trampolim.
    voltar = (request.form.get("voltar") or "").strip()
    if voltar.startswith("/analisesps"):
        return redirect(voltar)
    return redirect(url_for("analisesps.solicitacoes"))


def _opcoes_dos_filtros(carimbo=None) -> dict:
    """O que cada lista suspensa da barra lateral oferece.

    Vive aqui, e não dentro de cada rota, porque Solicitações e Relatório
    mostram a MESMA barra. Duas cópias divergiriam no dia em que alguém
    acrescentasse um filtro em uma só.

    As listas ficam guardadas até a próxima carga da planilha — era o pedaço
    mais caro da tela, 194 ms a cada clique no filtro. O porquê está escrito
    em `consultas.opcoes_de_filtro`. Passar o `carimbo` que a tela já tem em
    mãos poupa mais uma consulta."""
    from . import consultas
    return consultas.opcoes_de_filtro(carimbo)


def _recado_dos_filtros(base=None, opcoes=None) -> str:
    """Por que a barra lateral está sem opção nenhuma. "" quando há opções.

    ⚠️ BARRA VAZIA NÃO EXPLICA NADA, e foi o que ele encontrou em 29/09/2026:
    *"alguma coisa de errada aconteceu com os filtros da parte de solicitações.
    Estão todos vazios."* Sem uma frase ali, os três motivos possíveis (carga em
    andamento, base não carregada, colunas sem valor) têm a mesma cara."""
    from . import consultas
    try:
        if base is None:
            base = consultas.base_carregada()
        if opcoes is None:
            opcoes = _opcoes_dos_filtros(base.get("ultima"))
        return consultas.por_que_os_filtros_estao_vazios(opcoes, base)
    except Exception:  # noqa: BLE001 — é um recado, não pode derrubar a tela
        logger.exception("Análise de SPs: falhou explicar os filtros vazios")
        return ""


def _lembrar_filtro(endpoint: str, gaveta: str = None):
    """Guarda o filtro desta tela, ou traz de volta o da última vez.

    Devolve um redirecionamento quando há filtro guardado a restaurar, e None
    quando a tela pode seguir e desenhar.

    O caminho é sempre o mesmo, venha a pessoa de onde vier: chegou com a
    marca, o que está na barra de endereço é a verdade e vira o guardado;
    chegou sem a marca (clicou no menu, digitou o endereço, voltou de outra
    tela), o guardado volta. É isso que faz o filtro de Solicitações valer
    também no Relatório — os dois guardam no mesmo lugar."""
    pessoa = auth.pessoa_atual()
    gaveta = gaveta or preferencias.FILTRO

    if request.args.get(MARCA_FILTRO):
        preferencias.gravar(pessoa, gaveta, _filtro_cru())
        return None

    guardado = preferencias.ler(pessoa, gaveta)
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
        # O recorte da Documentação Fiscal — ver `consultas.SITUACOES_FISCAIS`.
        # Vive no mesmo dicionário que os demais de propósito: é uma montagem
        # de WHERE só, e duas não poderiam divergir.
        "fiscais": lista("fiscais"),
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
    """O endereço de entrada do módulo. Vai para a PRIMEIRA tela que a pessoa tem.

    ⚠️ ERA SEMPRE AS SOLICITAÇÕES, e quem só tinha a Folha caía num erro ao abrir
    o sistema — o dono, 01/10/2026: *"disponibilizei para uma pessoa uma única
    tela, Folha de pagamento. Quando ela entra dá uma mensagem de erro."* O login
    já fazia a escolha certa (`_para_onde_depois_de_entrar`); este endereço, que é
    o que fica salvo no navegador, não fazia."""
    permitidas = auth.telas_permitidas()
    if permitidas is None:
        return redirect(url_for("analisesps.solicitacoes"))
    primeira = next((t for t in TELAS if t[0] in permitidas), None)
    if primeira:
        return redirect(url_for(primeira[2]))
    return render_template(
        "analisesps_erro.html", titulo="Nenhuma tela liberada",
        mensagem="Seu cadastro ainda não tem nenhuma tela liberada. Peça a quem "
                 "administra o sistema para marcar as telas que você usa.",
        sem_voltar=True), 403


@bp.route("/solicitacoes")
@exige_consulta
def solicitacoes():
    from . import consultas, lote

    # O FILTRO GUARDADO É CONFERIDO ANTES DE QUALQUER CONSULTA. Quem clica no
    # menu chega sem filtro na barra de endereço e é redirecionado para o
    # endereço com ele — ou seja, esta função roda DUAS vezes por clique. Tudo
    # o que for perguntado ao banco antes daqui é perguntado à toa na primeira.
    voltar = _lembrar_filtro("analisesps.solicitacoes")
    if voltar is not None:
        return voltar

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

    # A marca de "esta SP já está num lote" — ver `lote.onde_no_lote`. Custa
    # uma consulta a uma tabela de meia dúzia de linhas, e evita separar duas
    # vezes o mesmo pagamento.
    lote.marcar_nas_linhas(linhas, auth.pessoa_atual())

    # Os números que o Streamlit mostrava embaixo da tabela. São SQL, não
    # contas sobre as 200 linhas da página: quem soma é o banco, sobre o
    # filtro inteiro — que é justamente a pergunta ("quanto tem para pagar
    # nisto que estou olhando?").
    #
    # O resumo e a divisão do agendamento saem JUNTOS: eram duas varreduras da
    # mesma tabela filtrada (44 ms + 48 ms medidos), e numa consulta só custam
    # 59 ms. As duas somas por conta e por forma continuam separadas — juntá-las
    # foi tentado e ficou PIOR (66 ms contra 51 ms), porque o banco precisa
    # guardar o resultado do meio.
    resumo, agendamento = consultas.resumo_e_agendamento(filtros)
    ultima = (pagina - 1) * consultas.POR_PAGINA + len(linhas)
    por_conta = consultas.soma_por(filtros, "conta")
    por_forma = consultas.soma_por(filtros, "forma_pagamento")

    return render_template(
        "analisesps_solicitacoes.html",
        linhas=linhas, resumo=resumo, base=base,
        pagina=pagina, por_pagina=consultas.POR_PAGINA,
        primeira_linha=(pagina - 1) * consultas.POR_PAGINA + 1,
        ultima_linha=ultima,
        tem_proxima=ultima < resumo["quantidade"],
        ordem=ordem, filtros=filtros,
        agendamento=agendamento, por_conta=por_conta, por_forma=por_forma,
        colunas=_colunas_da_pessoa(), todas_colunas=_TODAS_COLUNAS(),
        args=request.args, opcoes=_opcoes_dos_filtros(base.get("ultima")),
        recado_dos_filtros=_recado_dos_filtros(base),
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


# De onde a pessoa veio, e para onde "Voltar" tem de levar. Lista fechada de
# propósito: o destino sai da barra de endereço, e um destino livre viraria um
# jeito de usar este módulo como trampolim para fora.
ORIGENS = {"solicitacoes": "analisesps.solicitacoes",
           "lote": "analisesps.tela_lote",
           # Quem abre uma SP a partir da lista de notas órfãs tem de voltar
           # para lá, e não para as Solicitações: a lista é o trabalho, e
           # perder o lugar nela a cada ficha aberta faria desistir dela.
           "fiscal": "analisesps.tela_fiscal"}


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

    # O código de pagamento junto da ficha: quem abre a SP para conferir um
    # dado quase sempre está a caminho de pagar, e voltar à lista só para
    # gerar o QR era um caminho a mais em cada pagamento.
    codigo = _codigo_de_pagamento(str(sp_id), registro)

    contexto = dict(
        sp=registro, origem=_origem_pedida(),
        pendencias=faltando, risco_ids=apontadas, codigo=codigo,
        hook_omie=os.getenv("ANALISESPS_HOOK_OMIE", "").strip(),
        pode_operar=auth.pode_operar())

    if request.args.get("modal"):
        return render_template("analisesps_ficha.html", **contexto)

    return render_template("analisesps_detalhe.html", aba="solicitacoes",
                           perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual(),
                           **contexto)


# ---------------------------------------------------------------------------
# VER A NOTA FISCAL — 17/09/2026
#
# *"Não tem problema abrir o XML em tela. Abre num modal? E desse modal poderia
# exportar em PDF?"*
#
# ⚠️ O PDF É O NAVEGADOR IMPRIMINDO, e é o melhor dos dois mundos: não entra
# biblioteca nova no serviço (a regra da casa), e quem imprime escolhe margem,
# tamanho e se quer papel ou arquivo. `?imprimir=1` abre a mesma leitura numa
# página limpa, já chamando a caixa de impressão.
#
# ⚠️ E NÃO É UM DANFE OFICIAL — ver o comentário no alto de `nfe_leitura.py`.
# A tela diz isso em letras, porque quem recebe um papel com cara de nota
# fiscal supõe que ele vale como uma.
# ---------------------------------------------------------------------------
@bp.route("/nota/<chave>")
@exige_consulta
def nota_documento(chave):
    """A nota fiscal desenhada a partir do XML guardado.

    Três respostas, e cada uma para um lugar: `?modal=1` devolve só o miolo,
    para o modal abrir por cima da lista sem perder a rolagem; `?imprimir=1`
    devolve a página limpa que chama a impressão; sem nada, a página inteira,
    que é o que abre em nova aba."""
    import re

    from . import nfe_leitura, notas_arquivo
    from .drive import ErroDoDrive

    pedaco = bool(request.args.get("modal"))
    imprimir = bool(request.args.get("imprimir"))

    def recado(mensagem, classe="erro", codigo=200):
        """O mesmo problema, dito no formato que quem pediu sabe mostrar."""
        if pedaco:
            return f'<div class="aviso {classe}">{escape(mensagem)}</div>', codigo
        return render_template("analisesps_erro.html",
                               titulo="Nota fiscal",
                               mensagem=mensagem), codigo

    # ⚠️ A CONFERÊNCIA DA CHAVE É AQUI, na entrada, e não lá dentro: endereço
    # com chave inventada não pode virar consulta ao banco nem ida ao Drive.
    # Quem varre números não descobre nada e não custa nada ao serviço.
    if len(re.sub(r"\D", "", str(chave or ""))) != 44:
        return recado("Chave de acesso inválida — ela tem 44 números.",
                      codigo=404)

    try:
        xml = notas_arquivo.baixar_xml(chave)
    except notas_arquivo.SemDocumento as e:
        # ⚠️ NÃO É ERRO, é o estado normal antes da ciência. Dizer "deu erro"
        # aqui mandaria alguém procurar defeito onde só falta esperar.
        return recado(str(e), classe="")
    except ValueError as e:
        return recado(str(e), codigo=404)
    except ErroDoDrive as e:
        logger.exception("Análise de SPs: falhou buscar o XML da nota")
        return recado(f"Não consegui buscar o arquivo da nota. {e}")

    try:
        nota = nfe_leitura.ler(xml)
    except nfe_leitura.XmlIlegivel as e:
        return recado(str(e), classe="")

    if pedaco:
        return render_template("analisesps_nota.html", nota=nota,
                               imprimir=False)
    if imprimir:
        return render_template("analisesps_nota_impressa.html", nota=nota)
    return render_template("analisesps_nota_pagina.html", aba="fiscal",
                           perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
                           nome=auth.nome_atual(), nota=nota)


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
def _gravar_alteracao(ids: list, coluna: str, valor: str, acao: str):
    """O caminho de TODA alteração — e é o que garante que nada se perca:

      1. grava no banco na hora — quem está na tela vê o efeito já;
      2. põe a célula na fila de escrita para a planilha;
      3. registra no log, com o valor anterior e com quem mexeu.

    O envio para a planilha acontece depois, no processo separado. Se a
    internet cair no meio, a alteração continua na fila e sobe sozinha.

    Vive fora da rota porque a validação usa o MESMO caminho: só muda a letra
    da coluna. Duas cópias divergiriam no dia em que uma delas ganhasse um
    passo — e a que ficasse para trás deixaria de enfileirar, ou de registrar,
    sem ninguém notar."""
    from .db import conexao

    from .db import tem_coluna
    perfil = auth.perfil_atual() or "?"
    quem = auth.nome_atual()
    com_pessoa = tem_coluna("log_alteracoes", "pessoa")
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
            if com_pessoa:
                conn.execute(
                    "INSERT INTO analisesps.log_alteracoes "
                    "  (sp_id, coluna, valor, valor_anterior, acao, perfil, "
                    "   pessoa, status) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'pendente')",
                    (sp_id, coluna, valor, anteriores.get(sp_id), acao,
                     perfil, quem))
            else:
                # Banco ainda sem a coluna `pessoa` (migração 003 por
                # aplicar). Registrar sem o nome é muito melhor do que
                # recusar a alteração: o pagamento não pode esperar o botão.
                conn.execute(
                    "INSERT INTO analisesps.log_alteracoes "
                    "  (sp_id, coluna, valor, valor_anterior, acao, perfil, "
                    "   status) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'pendente')",
                    (sp_id, coluna, valor, anteriores.get(sp_id), acao,
                     perfil))
        conn.commit()

    logger.info("Análise de SPs: %s (%s) alterou '%s' de %d SP(s) para '%s'.",
                quem or "sem nome", perfil, coluna, len(ids), valor)

    # Tenta subir já, sem prender a tela: se falhar, fica na fila.
    from . import tarefas
    resultado = tarefas.disparar("fila", disparo="alteração na tela")

    return {"ok": True, "alteradas": len(ids),
            "envio": resultado.get("ok", False),
            "aviso": None if resultado.get("ok") else resultado.get("erro")}


def _ids_do_pedido(dados: dict):
    """Os IDs do corpo, conferidos. Devolve (ids, erro)."""
    ids = [str(i).strip() for i in (dados.get("ids") or []) if str(i).strip()]
    if not ids:
        return None, ({"ok": False, "erro": "Nenhuma SP selecionada."}, 400)
    if len(ids) > 500:
        return None, ({"ok": False,
                       "erro": "São no máximo 500 SPs por vez. Refine a "
                               "seleção."}, 400)
    return ids, None


@bp.route("/api/alterar", methods=["POST"])
@exige_operador
def alterar():
    """Altera uma coluna editável em uma ou mais SPs."""
    from . import colunas

    dados = request.get_json(silent=True) or {}
    ids, erro = _ids_do_pedido(dados)
    if erro:
        return erro

    coluna = str(dados.get("coluna") or "").strip()
    valor = str(dados.get("valor") or "").strip()
    acao = str(dados.get("acao") or "Alterar").strip()

    if coluna not in colunas.EDITAVEIS:
        # As colunas do dia a dia. Qualquer outra é somente leitura POR AQUI —
        # a planilha é a dona do resto. A Validação é a exceção, e tem porta
        # própria (`/api/validar`) porque exige senha própria.
        return {"ok": False,
                "erro": f"A coluna '{coluna}' não é alterável por aqui."}, 400

    return _gravar_alteracao(ids, coluna, valor, acao)


@bp.route("/api/enviar-ao-lote", methods=["POST"])
@exige_operador
def enviar_ao_lote():
    """Manda as SPs marcadas para o lote SEM SAIR DA TELA.

    Pedido do dono em 09/09/2026: *"ao enviar registro ao lote, não quero mudar
    de tela; mantenha-se em Solicitações, apenas avise que foi executada a
    ação"*. Antes o botão mandava um formulário e a pessoa era levada para o
    Lote — perdendo o filtro, a rolagem e a marcação de quem só queria separar
    um grupo e continuar conferindo a lista.

    É a MESMA regra do formulário: um grupo novo no topo, o que já estava fica
    abaixo. Aqui ela é chamada, não copiada."""
    from . import lote

    dados = request.get_json(silent=True) or {}
    ids, erro = _ids_do_pedido(dados)
    if erro:
        return erro

    pessoa = auth.pessoa_atual()
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        conteudo, titulo = lote.acrescentar_grupo(lote.ler(pessoa)["conteudo"], ids)
        lote.salvar(conteudo, quem, pessoa)
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Análise de SPs: falhou enviar ao lote")
        return {"ok": False, "erro": f"Não consegui enviar ao lote: {e}"}, 500

    logger.info("Análise de SPs: %s enviou %d SP(s) ao lote (grupo %r).",
                quem or "sem nome", len(ids), titulo)
    return {"ok": True, "quantas": len(ids), "titulo": titulo}


@bp.route("/api/sem-risco", methods=["POST"])
@exige_operador
def sem_risco():
    """Marca a SP como revisada: o risco de duplicidade foi olhado e não era.

    Escreve na coluna da análise (AL) o mesmo texto do Streamlit, com a data
    da revisão — "SEM RISCO (revisado em DD/MM/AAAA)". A palavra importa: é
    "COM RISCO" no texto que faz a SP aparecer na lista de risco, e é tirando
    essa palavra que ela sai de lá.

    Não é uma alteração comum, então também não passa pela porta comum: quem
    revisa está dizendo "eu conferi e pode pagar", e isso fica registrado com
    o nome de quem disse."""
    from .horario import agora

    dados = request.get_json(silent=True) or {}
    ids, erro = _ids_do_pedido(dados)
    if erro:
        return erro

    quem = auth.nome_atual() or "sem nome"
    texto = (f"SEM RISCO (revisado por {quem} em "
             f"{agora().strftime('%d/%m/%Y')})")
    return _gravar_alteracao(ids, "analise_ia", texto, "Remover risco")


@bp.route("/api/validar", methods=["POST"])
@exige_operador
def validar():
    """Marca Validação = "Sim" nas SPs pedidas.

    É a MESMA gravação de sempre — banco, fila, log, planilha —, só que na
    coluna AH em vez da O ou da AB. O que a separa não é o mecanismo, é o
    significado: a Validação é o que destrava o agendamento, e destravá-la é
    dizer "eu conferi". Por isso, como no Streamlit, ela pede uma SENHA
    PRÓPRIA (SENHA_VALIDACAO): se a senha de Operador servisse, a trava
    perderia o sentido — quem agenda seria o mesmo que autoriza a agendar.

    Sem a senha cadastrada, a ação não existe. Falha fechado, como todo o
    resto deste módulo."""
    from . import credenciais

    dados = request.get_json(silent=True) or {}
    ids, erro = _ids_do_pedido(dados)
    if erro:
        return erro

    try:
        esperada = credenciais.token("SENHA_VALIDACAO", "").strip()
    except Exception:  # noqa: BLE001 — planilha de credenciais fora do ar
        logger.exception("Análise de SPs: não consegui ler a senha de validação")
        esperada = ""

    if not esperada:
        return {"ok": False,
                "erro": "A senha de validação não está cadastrada. Ponha "
                        "SENHA_VALIDACAO na Environment do Render ou na aba "
                        "Credenciais da planilha."}, 409

    # `auth.confere` e não `hmac.compare_digest` direto: com texto, o
    # compare_digest só aceita ASCII, e uma senha de validação com acento
    # estouraria aqui em vez de ser recusada. Ver `auth.confere`.
    if not auth.confere(dados.get("senha") or "", esperada):
        logger.warning("Análise de SPs: %s tentou validar com senha errada.",
                       auth.nome_atual() or "sem nome")
        return {"ok": False, "erro": "Senha de validação incorreta."}, 403

    return _gravar_alteracao(ids, "validacao", "Sim", "Validar")


# ---------------------------------------------------------------------------
# Configurações e sincronização
# ---------------------------------------------------------------------------
@bp.route("/configuracoes")
@exige_consulta
def configuracoes():
    from . import consultas, credenciais, migracoes_runner, tarefas
    try:
        migracoes = migracoes_runner.listar_estado()
        erro_banco = None
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        migracoes = {"aplicadas": [], "pendentes": []}
        erro_banco = str(e)

    # O que o BeeVale precisa para funcionar. Só diz se ESTÁ CONFIGURADO —
    # nunca mostra o valor de um segredo na tela.
    #
    # Dentro de um try porque ler um segredo pode ir à planilha, e ESTA tela é
    # a que conserta o módulo quando algo quebra: ela não pode ser a próxima a
    # cair. Foi assim que o módulo travou na estreia (03/09).
    from . import beevale, pessoas
    try:
        equipe = pessoas.listar()
    except Exception:  # noqa: BLE001 — a tela abre mesmo sem isto
        logger.exception("Análise de SPs: não consegui ler a lista de pessoas")
        equipe = list(pessoas.PADRAO)
    try:
        pasta, origem = beevale.pasta_do_drive()
        integracoes = {
            "ok": True,
            "pasta_drive": bool(pasta),
            "pasta": pasta,
            "origem": origem,
            # Os últimos seis caracteres bastam para o dono reconhecer QUAL
            # pasta ele colou, sem publicar o identificador inteiro na tela.
            "pasta_fim": pasta[-6:] if pasta else "",
            "pipefy": bool(credenciais.token("PIPEFY_TOKEN")),
        }
    except Exception as e:  # noqa: BLE001 — sem internet, a tela ainda abre
        logger.exception("Análise de SPs: não consegui conferir as integrações")
        integracoes = {"ok": False, "erro": str(e)}

    # OS CERTIFICADOS: só de quem são, até quando valem e quem subiu. O
    # conteúdo NÃO passa por aqui, nem por rota nenhuma.
    from . import certificados
    try:
        lista_certificados = certificados.listar()
    except Exception:  # noqa: BLE001 — a tela abre mesmo sem isto
        logger.exception("Análise de SPs: não consegui listar os certificados")
        lista_certificados = []

    # EM QUE PÉ ESTÁ A BUSCA DE CADA CNPJ. *"Eu coloco o certificado (…) mas
    # simplesmente nada é feito, nada é executado, e eu não sei o que está
    # acontecendo."* A resposta tem de estar NESTA tela, que é onde ele acabou
    # de subir o certificado e fica esperando.
    #
    # Fica o pior caso de cada CNPJ (a falha manda sobre o sucesso): duas
    # linhas por CNPJ — NF-e e CT-e — e mostrar só a primeira esconderia
    # justamente a que deu errado.
    #
    # ⚠️ AS DUAS LINHAS SOMAM. São duas buscas por CNPJ — notas e fretes — e
    # mostrar só uma delas fazia a tela informar menos do que sabe: o dono viu
    # "112 documento(s)" quando o número era a soma das duas. A falha continua
    # mandando sobre o sucesso na hora de dizer a situação.
    buscas_por_cnpj: dict = {}
    try:
        from . import sefaz
        for b in sefaz.estado_das_buscas():
            atual = buscas_por_cnpj.get(b["cnpj"])
            if atual is None:
                buscas_por_cnpj[b["cnpj"]] = dict(b)
                continue
            atual["documentos"] = (atual.get("documentos") or 0) + (
                b.get("documentos") or 0)
            atual["faltam"] = (atual.get("faltam") or 0) + (b.get("faltam") or 0)
            if b.get("consultado_em") and (
                    not atual.get("consultado_em")
                    or b["consultado_em"] > atual["consultado_em"]):
                atual["consultado_em"] = b["consultado_em"]
            # A pior situação das duas é a que aparece: uma busca que falhou
            # não pode ficar escondida atrás da outra, que deu certo.
            if b.get("falhou") and not atual.get("falhou"):
                atual["falhou"] = True
                atual["motivo_da_falha"] = b.get("motivo_da_falha", "")
            if b.get("alerta"):
                atual["alerta"] = True
            if b.get("ultimo_recado") and not atual.get("ultimo_recado"):
                atual["ultimo_recado"] = b["ultimo_recado"]
    except Exception:  # noqa: BLE001 — migração 008 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler o estado da busca")

    # QUEM ENTRA COM CADASTRO PRÓPRIO (migração 023). Dentro de um try porque
    # esta é a tela que conserta o módulo: ela não pode ser a próxima a cair.
    from . import usuarios
    try:
        pessoas_com_acesso = usuarios.listar()
        cadastro_pronto = usuarios._pronto()
    except Exception:  # noqa: BLE001 — a tela abre mesmo sem isto
        logger.exception("Análise de SPs: não consegui listar quem tem acesso")
        pessoas_com_acesso, cadastro_pronto = [], False

    return render_template(
        "analisesps_config.html",
        migracoes=migracoes, erro_banco=erro_banco, integracoes=integracoes,
        equipe=equipe, certificados=lista_certificados,
        buscas_por_cnpj=buscas_por_cnpj,
        pessoas_com_acesso=pessoas_com_acesso,
        cadastro_pronto=cadastro_pronto,
        telas_liberaveis=usuarios.telas_liberaveis() if cadastro_pronto else [],
        erro_usuario=request.args.get("erro_usuario") or None,
        usuario_ok=request.args.get("usuario_ok") or None,
        cofre_ok=certificados.cofre_configurado(),
        aviso=request.args.get("aviso") or None,
        base=consultas.base_carregada(),
        andamento=tarefas.estado(),
        ultima=tarefas.ultima_concluida() if not erro_banco else None,
        modos=tarefas.MODOS, modos_da_base=tarefas.MODOS_DA_BASE,
        versao=os.getenv("RENDER_GIT_COMMIT", "")[:8] or "desenvolvimento",
        pode_operar=auth.pode_operar())


@bp.route("/usuarios", methods=["POST"])
@exige_operador
def usuarios_salvar():
    """Cadastra, altera ou apaga quem entra com usuário e senha próprios.

    ⚠️ SÓ O MESTRE CHEGA AQUI. Quem tem cadastro próprio é barrado pelo guarda,
    porque `analisesps.usuarios_` está em `auth.SO_DO_MESTRE_POR_PREFIXO` — sem
    isso, uma pessoa presa a uma tela poderia criar outro acesso, com todas.
    Foi um defeito real do painel, pego por teste.

    Responde com redirect e não JSON porque é formulário de tela: quem acabou
    de cadastrar precisa VER a lista nova, não um `{ok: true}`."""
    from . import usuarios

    acao = (request.form.get("acao") or "").strip()
    telas = [t for t in request.form.getlist("tela_do_usuario") if t.strip()]
    uid = (request.form.get("usuario_id") or "").strip()
    pode_operar = request.form.get("pode_operar") == "1"
    mestre = request.form.get("mestre") == "1"

    if acao == "criar":
        r = usuarios.criar(request.form.get("novo_usuario", ""),
                           request.form.get("nova_senha", ""),
                           nome=request.form.get("nome", ""),
                           telas=telas, pode_operar=pode_operar, mestre=mestre)
    elif acao == "apagar" and uid.isdigit():
        r = usuarios.apagar(int(uid))
    elif acao == "salvar" and uid.isdigit():
        r = usuarios.atualizar(int(uid), nome=request.form.get("nome"),
                               senha=request.form.get("nova_senha"),
                               ativo=request.form.get("ativo") == "1",
                               telas=telas, pode_operar=pode_operar,
                               mestre=mestre)
    else:
        r = {"ok": False, "erro": "Pedido não reconhecido."}

    return redirect(url_for("analisesps.configuracoes",
                            **({"erro_usuario": r["erro"]} if not r.get("ok")
                               else {"usuario_ok": "1"})))


@bp.route("/api/pessoas", methods=["POST"])
@exige_operador
def gravar_pessoas():
    """Guarda quem aparece na lista da tela de entrada.

    Não é cadastro de usuário e não dá acesso a ninguém: quem decide o que se
    pode fazer continua sendo a senha. Isto só evita que a mesma pessoa se
    divida em duas por ter digitado o nome diferente."""
    from . import pessoas

    dados = request.get_json(silent=True) or {}
    bruto = dados.get("pessoas")
    if isinstance(bruto, str):
        # A tela manda um nome por linha — é como se escreve uma lista à mão.
        bruto = bruto.replace(",", "\n").splitlines()

    try:
        lista = pessoas.gravar(bruto or [])
    except ValueError as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Análise de SPs: falhou gravar a lista de pessoas")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500

    logger.info("Análise de SPs: %s gravou a lista de pessoas (%d).",
                auth.nome_atual() or "sem nome", len(lista))
    return {"ok": True, "pessoas": lista}


@bp.route("/api/pasta-drive", methods=["POST"])
@exige_operador
def gravar_pasta_drive():
    """Guarda a pasta do Drive que o dono colou na tela de Configurações.

    Ela não é segredo — é o endereço de uma pasta —, então mora na tabela
    `meta` do próprio módulo e não na planilha de credenciais. Vantagem
    prática: dá para trocar sem entrar no Render, e vale na hora."""
    from . import beevale

    dados = request.get_json(silent=True) or {}
    try:
        salva = beevale.gravar_pasta_do_drive(dados.get("pasta", ""))
    except beevale.ErroDoBeeVale as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Análise de SPs: falhou gravar a pasta do Drive")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500

    logger.info("Análise de SPs: %s %s a pasta do Drive.",
                auth.nome_atual() or "sem nome",
                "gravou" if salva else "apagou")
    return {"ok": True, "pasta": salva,
            "fim": salva[-6:] if salva else ""}


@bp.route("/api/conferir-drive", methods=["POST"])
@exige_operador
def conferir_drive():
    """Olha a pasta do Drive SEM escrever nada.

    Existe para o dono conferir o identificador que acabou de colar sem
    precisar gerar um BeeVale de verdade — e para dizer, na hora, se a pasta é
    de Drive Compartilhado, que é a pegadinha que faz a subida falhar."""
    from . import beevale, drive
    pasta, _origem = beevale.pasta_do_drive()
    if not pasta:
        return {"ok": False,
                "erro": "Nenhuma pasta configurada. Defina DRIVE_FOLDER_ID no "
                        "Render (ou na aba Credenciais)."}
    return drive.conferir_pasta(pasta)


@bp.route("/api/migrar", methods=["POST"])
@exige_operador
def migrar():
    from . import migracoes_runner
    from .db import esquecer_colunas
    resultado = migracoes_runner.aplicar_pendentes()
    # O processo guardava que certas colunas não existiam. Depois de aplicar,
    # elas existem — e sem esquecer, este worker continuaria pelo caminho
    # antigo até o próximo reinício.
    esquecer_colunas()
    return resultado


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
        # ⚠️ QUANDO A ÚLTIMA TAREFA TERMINOU — é isto que faz a tela perceber
        # uma rodada CURTA. Reclamação do dono em 15/09/2026: *"quando clicamos
        # em buscar não vemos em canto nenhum se a busca está de fato
        # acontecendo, apenas uma mensagem dizendo que está sendo buscado."*
        #
        # A tela só se recarregava depois de ter VISTO a tarefa rodando, e ela
        # perguntava de quatro em quatro segundos. Uma busca que falha rápido
        # (foi o caso: a NF-e estourava na hora) começa e termina entre duas
        # perguntas — a tela nunca via nada, nunca recarregava, e ficava para
        # sempre com "Disparado" na cara dele, mostrando o resultado velho.
        #
        # Com este carimbo, terminar é visível mesmo sem nunca ter sido pega no
        # meio: ele muda, a tela relê.
        "ultimo_fim": tarefas.ultimo_fim(),
    }


@bp.route("/api/frescor")
@exige_consulta
def frescor():
    """A pergunta que a tela aberta faz de 90 em 90 segundos.

    Faz duas coisas de uma vez, e é de propósito:

      1. DISPARA a sincronização se a última estiver velha. É o que substitui
         o agendador externo — sem ele, a base só se atualizava quando alguém
         apertasse o botão em Configurações.
      2. Devolve o carimbo da última sincronização, para a tela saber se
         mudou alguma coisa desde que foi aberta.

    A tela NÃO se recarrega sozinha quando há SPs marcadas: recarregar por
    baixo de quem acabou de marcar vinte linhas apagaria a seleção, e isso é
    pior do que ver um dado com dois minutos de idade. Ela mostra um aviso e
    deixa a pessoa decidir."""
    from . import tarefas

    acao = tarefas.manter_fresco()

    # TUDO daqui para baixo dentro da proteção, e não só a leitura da base:
    # esta rota é chamada de fundo de 90 em 90 segundos, e um erro nela
    # apareceria na tela de quem só estava conferindo uma lista. Um teste
    # pegou justamente a chamada que tinha ficado de fora.
    # SÓ O CARIMBO. Antes esta rota chamava `base_carregada()`, que faz um
    # `count(*)` nas 59 mil SPs — uma varredura da tabela inteira, de 90 em 90
    # segundos, por aba aberta, para responder um número que a tela nem usa.
    # O carimbo é uma linha da tabela `meta`, achada pela chave.
    carimbo, rodando = "", False
    try:
        from .db import consultar_um
        linha = consultar_um("SELECT valor FROM analisesps.meta "
                             " WHERE chave = 'ultima_sincronizacao'")
        carimbo = str(linha[0]) if linha and linha[0] else ""
        rodando = tarefas.estado()["rodando"]
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou ler o frescor da base")

    return {"ok": True, "carimbo": carimbo,
            "disparou": acao.get("disparou", False), "rodando": rodando}


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

    resultado = tarefas.disparar(modo, disparo=disparo)

    # ⚠️ FORMULÁRIO DE VERDADE VOLTA PARA A TELA, e não para um JSON na cara de
    # quem apertou. As telas disparam por `fetch`, mas o botão de retomar a fila
    # dos comprovantes é um formulário comum — de propósito, porque ele precisa
    # funcionar mesmo quando o JavaScript não carregou (é o botão de destravar).
    if request.form.get("modo") and "application/json" not in (
            request.headers.get("Accept") or ""):
        # ⚠️ O AVISO TEM DE DIZER SE COMEÇOU MESMO. *"Clico nele e nada
        # acontece"* — e quando outra tarefa já estava rodando, o disparo era
        # recusado e a tela não contava.
        if resultado.get("ok"):
            aviso = ("Retomando a fila. Os lotes parados voltam para a fila e "
                     "são processados — acompanhe aqui embaixo, a tela se "
                     "atualiza sozinha.")
        else:
            aviso = ("Não consegui começar agora: "
                     + (resultado.get("erro") or "motivo desconhecido")
                     + ". Se já há uma tarefa rodando, espere ela terminar e "
                       "tente de novo.")
        return redirect(url_for("analisesps.tela_comprovantes", aviso=aviso))
    return resultado


# ---------------------------------------------------------------------------
# RELATÓRIO
# ---------------------------------------------------------------------------
@bp.route("/relatorio")
@exige_consulta
def relatorio():
    from . import consultas

    # O FILTRO GUARDADO É CONFERIDO ANTES DE QUALQUER CONSULTA. Quem clica no
    # menu chega sem filtro na barra de endereço e é redirecionado para o
    # endereço com ele — ou seja, esta função roda DUAS vezes por clique. Tudo
    # o que for perguntado ao banco antes daqui é perguntado à toa na primeira.
    voltar = _lembrar_filtro("analisesps.relatorio")
    if voltar is not None:
        return voltar

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

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

    # AS CINCO SOMAS SAEM DE UMA VARREDURA SÓ. Eram cinco perguntas sobre
    # exatamente as mesmas linhas, cada uma percorrendo as 59 mil SPs: medido,
    # 183 dos 331 ms desta tela. Ver `consultas.agregar_varias`.
    somas = consultas.agregar_varias(
        filtros, ["projeto", "centro_custo", "tipo_despesa", "conta", dimensao],
        tipo, periodo, 100)

    return render_template(
        "analisesps_relatorio.html",
        aba="relatorio", base=base,
        numeros=consultas.numeros_do_relatorio(filtros, tipo, periodo),
        por_projeto=somas.get("projeto", [])[:15],
        por_centro=somas.get("centro_custo", [])[:15],
        por_tipo=somas.get("tipo_despesa", [])[:15],
        por_conta=somas.get("conta", [])[:15],
        quebra=somas.get(dimensao, []),
        credores=consultas.top_credores(filtros, tipo, periodo, 30),
        aging=consultas.aging_vencidos(filtros, periodo),
        tipo=tipo, periodo=periodo, dimensao=dimensao,
        tipos=consultas.TIPOS, periodos=consultas.PERIODOS,
        dimensoes=consultas.DIMENSOES,
        args=request.args, filtros=filtros, opcoes=_opcoes_dos_filtros(),
        recado_dos_filtros=_recado_dos_filtros(),
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


# ---------------------------------------------------------------------------
# O CALENDÁRIO — o filtro das Solicitações, espalhado no tempo
#
# Pedido do dono em 23/09/2026. O que faz esta tela valer a pena é ela NÃO ter
# filtro próprio: ela lê o mesmo filtro guardado que Solicitações e Relatório,
# pela mesma gaveta. Quem recortou "obra X, a pagar" nas Solicitações e vem
# para cá vê aquele recorte no calendário, sem remontar nada.
# ---------------------------------------------------------------------------
@bp.route("/calendario")
@exige_consulta
def calendario():
    from . import calendario as grade_do_mes
    from . import consultas

    # O FILTRO GUARDADO É CONFERIDO ANTES DE QUALQUER CONSULTA — mesma regra
    # das outras duas telas, e pelo mesmo motivo: quem chega pelo menu é
    # redirecionado, e tudo o que for perguntado ao banco antes daqui é
    # perguntado à toa na primeira passagem.
    voltar = _lembrar_filtro("analisesps.calendario")
    if voltar is not None:
        return voltar

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    filtros = _filtros_do_pedido()

    # ⚠️ O FILTRO DE DATA NÃO VALE AQUI — pedido do dono em 24/09/2026:
    # *"como é um calendário, eu não queria que o filtro de data interferisse
    # nele; o correto é aparecer tudo. Continua mantendo os outros filtros."*
    #
    # E é o certo: quem escolhe a data nesta tela é o MÊS que está aberto. Um
    # recorte de vencimento vindo das Solicitações apagaria dias inteiros do
    # calendário sem nada na tela explicando por quê — a pessoa veria um mês
    # pela metade e concluiria que não há nada a pagar naqueles dias.
    #
    # ⚠️ O FILTRO NÃO É APAGADO, só ignorado AQUI: ele continua guardado e
    # valendo nas Solicitações e no Relatório, que é onde ele foi montado.
    # Apagá-lo faria esta tela mexer no recorte das outras pelas costas.
    DATAS_QUE_NAO_VALEM = ("periodo_ini", "periodo_fim", "pgt_ini", "pgt_fim")
    datas_ignoradas = {k: filtros[k] for k in DATAS_QUE_NAO_VALEM
                       if filtros.get(k)}
    filtros_do_mes = dict(filtros, **{k: None for k in DATAS_QUE_NAO_VALEM})

    # ⚠️ O PADRÃO MUDOU EM 23/09/2026, DE "A PAGAR" PARA A VISÃO GERAL, e a
    # razão é a cor. O dono pediu *"azul para pago, vermelho para vencido e
    # laranja a vencer"* — e no recorte "a pagar" o azul NUNCA apareceria,
    # porque conta paga está fora dele. Abrir numa visão que esconde uma das
    # três cores que ele acabou de pedir seria entregar metade.
    #
    # O preço, dito para não se perder: na visão geral o dia conta pelo
    # VENCIMENTO, inclusive o que já foi pago. Ou seja, o calendário mostra
    # "o que vencia neste dia, e o que aconteceu com aquilo". Para ver o dia
    # em que o dinheiro saiu de fato, o recorte "Contas pagas" continua ali,
    # e ele conta pela data do pagamento.
    tipo = request.args.get("tipo", "geral")
    if tipo not in consultas.TIPOS:
        tipo = "geral"

    ano, mes = grade_do_mes.mes_valido(request.args.get("ano"),
                                       request.args.get("mes"))
    opcoes = _opcoes_dos_filtros(base.get("ultima"))

    # ⚠️ O LINK DO DIA TEM DE TRAZER A MESMA COISA QUE A CÉLULA CONTOU.
    # A célula conta pelo recorte (a pagar / pagas), que compara sem ligar
    # para maiúscula; a lista de Solicitações filtra pelo valor EXATO que
    # está na planilha. Mandar o texto "Pagar" chutado deixaria de fora uma
    # SP gravada como "PAGAR", e aí o dia diria 3 e a lista mostraria 2 —
    # sem nada avisando. Então o valor sai da própria lista de opções, que é
    # o que existe no banco de verdade.
    alvo = {"pagas": "pago", "pagar": "pagar"}.get(tipo)
    status_do_dia = [v for v in opcoes.get("status_pgt", [])
                     if str(v).strip().lower() == alvo] if alvo else []
    primeiro, ultimo = grade_do_mes.limites(ano, mes)
    achado = consultas.calendario_do_mes(filtros_do_mes, primeiro, ultimo, tipo)
    grade = grade_do_mes.grade(ano, mes, achado["dias"])
    anterior, seguinte = grade_do_mes.vizinhos(ano, mes)

    return render_template(
        "analisesps_calendario.html",
        aba="calendario", base=base,
        grade=grade, achado=achado, ano=ano, mes=mes,
        anterior=anterior, seguinte=seguinte,
        meses=grade_do_mes.MESES, dias_da_semana=grade_do_mes.DIAS_DA_SEMANA,
        situacoes_possiveis=grade_do_mes.SITUACOES,
        tipo=tipo, tipos=consultas.TIPOS,
        args=request.args, filtros=filtros, opcoes=opcoes,
        recado_dos_filtros=_recado_dos_filtros(base, opcoes),
        status_do_dia=status_do_dia,
        # A barra de filtros é a mesma das outras telas; estas duas dizem a
        # ela que aqui a data não manda, e se havia alguma marcada.
        datas_nao_valem=True, datas_ignoradas=datas_ignoradas,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


# ---------------------------------------------------------------------------
# CONCILIAÇÃO BANCÁRIA — 24/09/2026
#
# O controle paralelo que vive numa planilha desde sempre, trazido para cá.
# A conciliação de verdade continua no OMIE; esta é a visão do dono, e o lugar
# onde cabe a ANOTAÇÃO, que o OMIE não tem.
#
# ⚠️ O FILTRO É PRÓPRIO, e NÃO o da barra das Solicitações. São universos
# diferentes: lá se filtra SP (credor, obra, projeto); aqui se filtra linha de
# extrato bancário (conta, período, conciliado, observação). Reusar a mesma
# gaveta faria o recorte de uma tela vazar na outra sem sentido nenhum.
# ---------------------------------------------------------------------------
FILTRO_CONCILIACAO = "filtro_conciliacao"


@bp.route("/api/conciliacao/procurar")
@exige_consulta
def conciliacao_procurar():
    """Onde está um lançamento — em TODAS as contas, sem filtro nenhum.

    ⚠️ ELA EXISTE PARA ACABAR COM UMA DISCUSSÃO QUE NÃO TEM COMO SER GANHA NA
    CONVERSA. "Não importou" e "não estou vendo" são a mesma coisa na tela e coisas
    diferentes no banco — e eu não enxergo o banco dele. Isto enxerga."""
    from . import conciliacao as conc
    from .formatos import para_numero

    bruto = (request.args.get("valor") or "").strip()
    texto = (request.args.get("q") or "").strip()
    valor = None
    if bruto:
        valor = para_numero(bruto)
        if valor is None:
            return {"ok": False, "erro": f'não entendi o valor "{bruto}".'}, 400
    if valor is None and not texto:
        return {"ok": False, "erro": "diga um valor ou um pedaço do histórico."}, 400

    try:
        achados = conc.procurar_em_todas(valor=valor, texto=texto)
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Conciliação: falhou a busca em todas as contas")
        return {"ok": False, "erro": f"Não consegui procurar: {e}"}, 500

    return {"ok": True, "quantos": len(achados), "achados": [
        {"conta": a["conta"], "conta_id": a["conta_id"],
         "data": a["data"].strftime("%d/%m/%Y") if a["data"] else "",
         "descricao": a["descricao"], "documento": a["documento"] or "",
         "valor": str(a["valor"]), "origem": a["origem"],
         "conciliado": bool(a["conciliado"]),
         "omie": a["omie_situacao"] or "", "omie_codigo": a["omie_codigo"],
         "arquivo": a["arquivo"]} for a in achados]}


def _filtros_da_conciliacao(contas_cadastradas: list) -> dict:
    """O que a barra desta tela manda, com um padrão sensato para cada coisa."""
    from . import conciliacao as conc
    from .formatos import para_data, para_numero

    def data(nome):
        bruto = (request.args.get(nome) or "").strip()
        return para_data(bruto) if bruto else None

    def numero(nome):
        bruto = (request.args.get(nome) or "").strip()
        return para_numero(bruto) if bruto else None

    # A conta é obrigatória: conciliação sem conta não é conciliação, é uma
    # lista de lançamentos de bancos misturados. Sem escolha, vale a primeira.
    try:
        conta_id = int(request.args.get("conta_id") or 0)
    except ValueError:
        conta_id = 0
    ids = [c["id"] for c in contas_cadastradas]
    if conta_id not in ids:
        conta_id = ids[0] if ids else 0

    situacao = request.args.get("situacao") or "todos"
    if situacao not in conc.SITUACOES:
        situacao = "todos"
    sentido = request.args.get("sentido") or ""
    if sentido not in ("", "entrada", "saida"):
        sentido = ""

    # ⚠️ A BUSCA RÁPIDA DO TOPO NÃO É UM SEGUNDO FILTRO — ela preenche ESTE.
    # Pedido do dono em 24/09/2026: *"se na parte superior eu pudesse já
    # inserir uma data, informação do histórico, um valor, sem precisar ir no
    # filtro, ajudaria demais."* Duas máquinas de filtrar na mesma tela
    # divergiriam no dia em que alguém mexesse numa só, e a pessoa não teria
    # como saber qual das duas está valendo.
    #
    # `data` é um DIA (de e até no mesmo), e `valor` é um valor EXATO em
    # módulo — é assim que se procura numa conciliação: "entrou 1.500 no dia
    # 10?". A faixa continua existindo na barra lateral, para quem precisa.
    dia = data("data")
    exato = numero("valor")

    def texto(nome):
        return (request.args.get(nome) or "").strip()

    return {"conta_id": conta_id, "situacao": situacao, "sentido": sentido,
            "busca": texto("busca"),
            "data_ini": dia or data("data_ini"),
            "data_fim": dia or data("data_fim"),
            "valor_ini": abs(exato) if exato is not None else numero("valor_ini"),
            "valor_fim": abs(exato) if exato is not None else numero("valor_fim"),
            # Os filtros de cabeçalho, um por coluna.
            "historico": texto("historico"),
            "documento": texto("documento"),
            "observacao": texto("observacao"),
            "entrada": numero("entrada"),
            "saida": numero("saida"),
            # Guardados para a tela redesenhar os campos como estavam.
            "dia": dia, "valor": exato}


def _omie_ligado() -> bool:
    """A parte do OMIE já existe no banco? (migração 021)

    ⚠️ ELA É SEPARADA DA CONCILIAÇÃO, e o dono pagou por eu não ter separado:
    a tela se dava por pronta olhando a tabela das CONTAS (migração 019), e o
    formulário dos tipos aparecia inteiro — só o Gravar quebrava, com a frase
    crua do Postgres. Cada pedaço confere a SUA tabela.
    """
    try:
        from . import conciliacao_omie as co
        return co._pronto()
    except Exception:  # noqa: BLE001
        return False


def _listas_do_omie() -> dict:
    """As contas correntes, o plano financeiro e as obras — do espelho.

    ⚠️ NÃO CHAMA A API DO OMIE. A carga do painel já traz tudo isso toda
    noite; chamar de novo daqui seria mais uma credencial para manter e duas
    cópias dos mesmos dados. O preço é a idade da última carga, e está dito na
    tela.
    """
    try:
        from . import conciliacao_omie as co
        return co.listas_do_omie()
    except Exception:  # noqa: BLE001 — tela de configuração não pode não abrir
        return {"contas": [], "categorias": [], "obras": [], "erro": ""}


def _nomear_fornecedores_e_obras(contas: list) -> None:
    """Põe em cada conta o NOME do fornecedor e da obra do OMIE gravados nela —
    a busca do campo mostra nome, e o número sozinho não diz nada a quem lê."""
    try:
        from . import aportes_de_para as dp, conciliacao_omie as co
        nomes = co.nomes_de_fornecedores([c.get("omie_fornecedor") for c in contas])
        try:
            obras = {str(o["codigo"]): o["nome"] for o in dp.obras()}
        except Exception:  # noqa: BLE001
            obras = {}
    except Exception:  # noqa: BLE001 — enfeite, não derruba a tela
        nomes, obras = {}, {}
    for c in contas:
        cod = c.get("omie_fornecedor")
        c["omie_fornecedor_nome"] = nomes.get(int(cod), "") if cod else ""
        dep = str(c.get("omie_departamento") or "")
        c["omie_departamento_nome"] = obras.get(dep, dep)


@bp.route("/api/conciliacao/fornecedores")
@exige_operador
def conciliacao_fornecedores():
    """Procura em TODOS os cadastros do OMIE (fornecedores e clientes), pelo nome
    ou pelo CNPJ — o campo "Fornecedor no OMIE" da conta.

    O dono, 02/10/2026: *"O campo Fornecedor no OMIE (…) não tá exibindo todos os
    fornecedores e nem tá permitindo buscar."* A lista antiga só trazia o que
    tinha cara de banco, num campo de escolha sem busca."""
    from . import conciliacao_omie as co
    termo = " ".join((request.args.get("q") or "").split())
    if len(termo) < 2:
        return {"ok": True, "fornecedores": []}
    try:
        achados = co.bancos_do_omie(termo, limite=40)
    except Exception as e:  # noqa: BLE001
        logger.exception("Conciliação: falhou procurar fornecedor")
        return {"ok": False, "erro": f"Não foi possível consultar os cadastros do "
                                     f"OMIE: {e}"}, 503
    return {"ok": True, "fornecedores": achados}


def _tipos_do_omie() -> list:
    """Os tipos de movimento cadastrados. Nunca derruba a tela."""
    try:
        from . import conciliacao_omie as co
        return co.tipos(so_ativos=False)
    except Exception:  # noqa: BLE001 — antes da migração é estado normal
        return []


def _planilha_da_conciliacao() -> str:
    """O identificador da planilha antiga, se já foi colado uma vez."""
    from . import conciliacao_planilha as cp
    try:
        return cp.planilha_guardada()
    except Exception:  # noqa: BLE001 — é enfeite do campo, não a tela
        return ""


@bp.route("/conciliacao")
@exige_consulta
def tela_conciliacao():
    from . import conciliacao as conc

    estado = conc.estado()
    if not estado["pronto"]:
        # ⚠️ A MIGRAÇÃO AINDA NÃO FOI APLICADA. O código sobe antes do botão
        # ser apertado, e uma tela que estourasse aqui derrubaria a confiança
        # na publicação inteira. Ela diz o que falta, e como fazer.
        return render_template("analisesps_conciliacao.html", estado=estado,
                               contas=[], linhas=[], filtros={}, resumo={},
                               situacoes=conc.SITUACOES, pagina=1,
                               pode_operar=auth.pode_operar(),
                               perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
                               nome=auth.nome_atual())

    contas = conc.contas()
    _nomear_fornecedores_e_obras(contas)
    filtros = _filtros_da_conciliacao(contas)
    try:
        pagina = max(1, int(request.args.get("pagina", 1)))
    except ValueError:
        pagina = 1

    linhas = conc.listar(filtros, pagina) if filtros["conta_id"] else []
    resumo = conc.resumo(filtros) if filtros["conta_id"] else {}
    conta = next((c for c in contas if c["id"] == filtros["conta_id"]), None)

    return render_template(
        "analisesps_conciliacao.html", aba="conciliacao", estado=estado,
        contas=contas, conta=conta, linhas=linhas, filtros=filtros,
        resumo=resumo, situacoes=conc.SITUACOES, pagina=pagina,
        # ⚠️ QUANTAS A CONTA TEM NO TOTAL, para a tela poder dizer o que o filtro
        # está escondendo. O filtro fica guardado de uma visita para a outra, e um
        # período de ontem esconde hoje uma linha que está gravada — foi o que fez
        # o dono concluir, em 29/09/2026, que um lançamento "não foi importado".
        total_da_conta=conc.quantas_na_conta(filtros["conta_id"])
        if filtros["conta_id"] else 0,
        por_pagina=conc.POR_PAGINA,
        tem_proxima=len(linhas) == conc.POR_PAGINA,
        saldo_total=conc.saldo_da_conta(filtros["conta_id"])
        if filtros["conta_id"] else 0,
        ultimos_arquivos=conc.ultimos_arquivos(filtros["conta_id"])
        if filtros["conta_id"] else [],
        planilha_guardada=_planilha_da_conciliacao(),
        tipos_omie=_tipos_do_omie(),
        omie_pronto=_omie_ligado(),
        listas_omie=_listas_do_omie(),
        args=request.args,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/conciliacao/panorama")
@exige_consulta
def conciliacao_panorama():
    """O panorama de todas as contas — "no que eu não posso confiar".

    ⚠️ A PERGUNTA QUE ESTA TELA RESPONDE NÃO É "QUANTO TEM", É ONDE ESTÁ O
    BURACO. Uma conta 100% conciliada cujo último extrato é de três meses
    atrás está pior do que uma com pendências e extrato de ontem — e olhando
    só o percentual ela pareceria a melhor de todas.
    """
    from . import conciliacao as conc
    from .horario import agora

    estado = conc.estado()
    anos = conc.anos_com_movimento() if estado["pronto"] else []
    try:
        ano = int(request.args.get("ano") or 0)
    except ValueError:
        ano = 0
    if ano not in anos:
        ano = anos[0] if anos else agora().date().year

    dados = conc.panorama(ano) if estado["pronto"] else {"contas": []}
    fundo = conc.panorama_do_ano(ano) if estado["pronto"] else {}
    return render_template(
        "analisesps_conciliacao_panorama.html", aba="conciliacao",
        estado=estado, ano=ano, anos=anos, panorama=dados, fundo=fundo,
        meses=conc.MESES_CURTOS,
        pendencias_omie=_pendencias_do_omie(),
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


def _pendencias_do_omie() -> list:
    """O que ficou pelo caminho ao lançar no OMIE. Nunca derruba a tela."""
    try:
        from . import conciliacao_omie as co
        return co.pendencias()
    except Exception:  # noqa: BLE001 — antes da migração é estado normal
        return []


@bp.route("/api/conciliacao/conferir", methods=["POST"])
@exige_operador
def conciliacao_conferir():
    """Lê o OFX e diz o que vai acontecer. NÃO grava nada.

    ⚠️ CONFERIR E GRAVAR SÃO DUAS CHAMADAS, e é o pedido do dono: *"eu quero
    jogar um OFX e o sistema me dizer: todos os lançamentos já estavam
    registrados desse período"*. Uma resposta dada DEPOIS de gravar não teria
    como ser conferida — ela mesma teria mudado o mundo que descreve.
    """
    from . import conciliacao as conc
    from . import conciliacao_ofx

    arquivo = request.files.get("extrato")
    if not arquivo or not arquivo.filename:
        return {"ok": False, "erro": "Escolha o arquivo do extrato (.ofx)."}
    try:
        lido = conciliacao_ofx.ler(arquivo.read())
    except conciliacao_ofx.ErroDoExtrato as e:
        return {"ok": False, "erro": str(e)}

    achada = conc.conta_do_extrato(lido.bankid, lido.acctid)
    pedida = request.form.get("conta_id")
    conta_id = int(pedida) if (pedida or "").strip().isdigit() else (
        achada["id"] if achada else 0)
    if not conta_id:
        # ⚠️ NÃO SE CHUTA A CONTA. Jogar o extrato de uma empresa dentro da
        # conta de outra é um estrago que ninguém percebe olhando a tela.
        return {"ok": False, "desconhecida": True,
                "bankid": lido.bankid, "acctid": lido.acctid,
                "erro": ("Não reconheci de qual conta é este extrato "
                         f"(banco {lido.bankid or '?'}, conta "
                         f"{lido.acctid or '?'}). Escolha a conta abaixo — e "
                         "ela passa a ser reconhecida sozinha da próxima vez.")}

    try:
        conferido = conc.conferir(conta_id, lido)
    except conc.ErroDaConciliacao as e:
        return {"ok": False, "erro": str(e)}

    return {"ok": True, **conc.resumo_para_a_tela(conferido, conta_id,
                                                  arquivo.filename)}


@bp.route("/api/conciliacao/importar", methods=["POST"])
@exige_operador
def conciliacao_importar():
    """Grava o que a conferência mostrou. O arquivo vem de novo, de propósito.

    ⚠️ O ARQUIVO É REENVIADO EM VEZ DE FICAR GUARDADO NO SERVIDOR entre as
    duas chamadas. Guardar exigiria um lugar para ele e uma limpeza depois, e
    com 1 worker e 4 threads um "guardado na memória" vira do outro usuário no
    dia em que duas pessoas importarem ao mesmo tempo. Reenviar custa um
    segundo e não tem esse risco.
    """
    from . import conciliacao as conc
    from . import conciliacao_ofx

    arquivo = request.files.get("extrato")
    conta_id = (request.form.get("conta_id") or "").strip()
    if not arquivo or not arquivo.filename:
        return {"ok": False, "erro": "O arquivo não veio junto."}
    if not conta_id.isdigit():
        return {"ok": False, "erro": "Escolha a conta."}
    try:
        lido = conciliacao_ofx.ler(arquivo.read())
    except conciliacao_ofx.ErroDoExtrato as e:
        return {"ok": False, "erro": str(e)}

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    # Lembrar a conta do arquivo: da próxima vez ele se reconhece sozinho.
    if request.form.get("lembrar") == "1":
        conc.lembrar_conta_do_extrato(int(conta_id), lido.bankid, lido.acctid,
                                      quem)
    try:
        feito = conc.importar(int(conta_id), lido, arquivo.filename, quem)
    except conc.ErroDaConciliacao as e:
        return {"ok": False, "erro": str(e)}
    return {"ok": True, **conc.resumo_para_a_tela(feito, int(conta_id),
                                                  arquivo.filename)}


@bp.route("/api/conciliacao/marcar", methods=["POST"])
@exige_operador
def conciliacao_marcar():
    """Marca ou desmarca linhas. É o gesto mais repetido da tela."""
    from . import conciliacao as conc
    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        mudadas = conc.marcar(dados.get("ids") or [],
                              bool(dados.get("conciliado")), quem)
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Conciliação: falhou marcar")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500
    return {"ok": True, "mudadas": mudadas, "quem": quem}


@bp.route("/api/conciliacao/anotar", methods=["POST"])
@exige_operador
def conciliacao_anotar():
    """A observação de uma linha — o que a planilha tinha e o OMIE não tem."""
    from . import conciliacao as conc
    dados = request.get_json(silent=True) or {}
    linha_id = dados.get("id")
    if not str(linha_id or "").isdigit():
        return {"ok": False, "erro": "Linha não informada."}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        texto = conc.anotar(int(linha_id), dados.get("texto") or "", quem)
    except Exception as e:  # noqa: BLE001
        logger.exception("Conciliação: falhou anotar")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500
    return {"ok": True, "texto": texto}


@bp.route("/api/conciliacao/conta", methods=["POST"])
@exige_operador
def conciliacao_gravar_conta():
    """Cria ou altera uma conta bancária, de dentro da própria tela."""
    from . import conciliacao as conc
    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        conta_id = conc.gravar_conta(dados, quem)
    except conc.ErroDaConciliacao as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("Conciliação: falhou gravar conta")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500
    return {"ok": True, "id": conta_id}


# ---------------------------------------------------------------------------
# TRAZER A PLANILHA ANTIGA — uma aba por vez, com amostra antes de gravar
#
# *"Já tem muita informação aqui, eu quero manter."* São ~20 abas, uma por
# conta, desde 2024. A associação aba → conta é feita por ele, na tela: é o
# único que sabe que "BD IFPE 2541" e a conta do Bradesco terminada em 2541
# são a mesma coisa.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# LANÇAR NO OMIE a partir do extrato — 24/09/2026
#
# ⚠️ ISTO ESCREVE NO OMIE. Ensaiar e lançar são DUAS rotas, e a tela chama as
# duas em ordem: ele vê o que vai acontecer, linha a linha, antes de acontecer.
# É o mesmo desenho dos aportes, e pela mesma razão — lançamento no OMIE não
# se desfaz com um clique.
# ---------------------------------------------------------------------------
def _linhas_para_o_omie(ids: list, conta_id: int) -> tuple:
    """As linhas marcadas, lidas do banco — e nunca do que o navegador mandou.

    ⚠️ O NAVEGADOR MANDA SÓ OS NÚMEROS. Aceitar dele o valor, a data ou a
    descrição deixaria o que vai para o OMIE nas mãos de quem abrir o console
    do navegador — e o que sai daqui é lançamento contábil.
    """
    from . import conciliacao as conc
    from .db import consultar

    numeros = [int(i) for i in (ids or []) if str(i).strip().isdigit()]
    if not numeros:
        return [], None
    marcas = ",".join(["?"] * len(numeros))
    linhas = consultar(
        "SELECT id, conta_id, data, descricao, documento, valor, omie_codigo "
        f"  FROM analisesps.conciliacao_extrato WHERE id IN ({marcas}) "
        "   AND conta_id = ? ORDER BY data, id",
        tuple(numeros + [int(conta_id)]))
    nomes = ["id", "conta_id", "data", "descricao", "documento", "valor",
             "omie_codigo"]
    conta = next((c for c in conc.contas(so_ativas=False)
                  if c["id"] == int(conta_id)), None)
    return [dict(zip(nomes, linha)) for linha in linhas], conta


def _destinos_pedidos(dados: dict) -> dict:
    """`{linha_id: conta}` — as contas de destino escolhidas na tela.

    ⚠️ A CONTA VEM DO BANCO PELO NÚMERO, e nunca do que o navegador mandou:
    o que sai daqui é lançamento contábil em duas contas.
    """
    from . import conciliacao as conc
    cru = dados.get("destinos") or {}
    if not isinstance(cru, dict) or not cru:
        return {}
    todas = {c["id"]: c for c in conc.contas(so_ativas=False)}
    saida = {}
    for linha_id, conta_id in cru.items():
        if str(linha_id).isdigit() and str(conta_id).isdigit():
            achada = todas.get(int(conta_id))
            if achada:
                saida[int(linha_id)] = achada
    return saida


@bp.route("/api/conciliacao/omie/ensaiar", methods=["POST"])
@exige_operador
def conciliacao_omie_ensaiar():
    """O que SERIA lançado. Não fala com o OMIE."""
    from . import conciliacao as conc
    from . import conciliacao_omie as co

    dados = request.get_json(silent=True) or {}
    conta_id = str(dados.get("conta_id") or "")
    if not conta_id.isdigit():
        return {"ok": False, "erro": "Escolha a conta."}
    linhas, conta = _linhas_para_o_omie(dados.get("ids") or [], int(conta_id))
    if not linhas:
        return {"ok": False, "erro": "Marque as linhas primeiro."}
    if not conta:
        return {"ok": False, "erro": "Conta não encontrada."}

    plano = co.planejar(linhas, conta, destinos=_destinos_pedidos(dados))
    return {"ok": True,
            "conta": conta["nome"],
            "contas": [{"id": c["id"], "nome": c["nome"]}
                       for c in conc.contas() if c["id"] != conta["id"]],
            "vai": [{"linha_id": x["linha_id"], "tipo": x["tipo"],
                     "sentido": x["sentido"], "valor": str(x["valor"]),
                     "data": x["data"].strftime("%d/%m/%Y"),
                     "categoria": x["codigo_categoria"],
                     "transferencia": x.get("transferencia", False),
                     "destino": x.get("destino_nome", ""),
                     "descricao": x["descricao"][:90]} for x in plano["vai"]],
            "nao_vai": [{"linha_id": x["id"], "motivo": x["motivo"],
                         "descricao": x["descricao"],
                         "pede_destino": x.get("pede_destino", False),
                         "valor": str(x["valor"] or 0)}
                        for x in plano["nao_vai"]],
            "total": str(plano["total"])}


@bp.route("/api/conciliacao/omie/lancar", methods=["POST"])
@exige_operador
def conciliacao_omie_lancar():
    """Lança de verdade. ⚠️ Escreve no OMIE."""
    from . import conciliacao_omie as co

    dados = request.get_json(silent=True) or {}
    conta_id = str(dados.get("conta_id") or "")
    if not conta_id.isdigit():
        return {"ok": False, "erro": "Escolha a conta."}
    linhas, conta = _linhas_para_o_omie(dados.get("ids") or [], int(conta_id))
    if not linhas or not conta:
        return {"ok": False, "erro": "Marque as linhas primeiro."}

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    plano = co.planejar(linhas, conta, destinos=_destinos_pedidos(dados))
    if not plano["vai"]:
        return {"ok": False,
                "erro": "Nenhuma das linhas marcadas pode ser lançada.",
                "nao_vai": [{"motivo": x["motivo"],
                             "descricao": x["descricao"]}
                            for x in plano["nao_vai"]]}
    try:
        feito = co.lancar(plano["vai"], quem)
    except co.ErroDoLancamento as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Conciliação: falhou lançar no OMIE")
        return {"ok": False, "erro": f"Não consegui: {e}"}, 500
    return {"ok": True, **feito,
            "nao_foram": len(plano["nao_vai"])}


@bp.route("/api/conciliacao/omie/tipo", methods=["POST"])
@exige_operador
def conciliacao_omie_tipo():
    """Cria ou altera um tipo de movimento (tarifa, rentabilidade, PIX)."""
    from . import conciliacao_omie as co

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        tipo_id = co.gravar_tipo(dados, quem)
    except co.ErroDoLancamento as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("Conciliação: falhou gravar tipo")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500
    return {"ok": True, "id": tipo_id}


@bp.route("/api/conciliacao/desfazer", methods=["POST"])
@exige_operador
def conciliacao_desfazer():
    """Desfaz uma importação — pedido do dono: *"tem que ter alguma forma de
    retroceder um erro, né?"*

    ⚠️ DUAS CHAMADAS, como tudo o que estraga: sem `confirmar`, ela só CONTA o
    que sumiria (inclusive quantas foram conciliadas e anotadas por gente) e
    devolve para ele decidir. Apagar contando depois não é escolha, é aviso.
    """
    from . import conciliacao as conc

    dados = request.get_json(silent=True) or {}
    conta_id = str(dados.get("conta_id") or "")
    if not conta_id.isdigit():
        return {"ok": False, "erro": "Escolha a conta."}
    arquivo_id = dados.get("arquivo_id")
    aba = str(dados.get("aba") or "").strip()
    arquivo_id = int(arquivo_id) if str(arquivo_id or "").isdigit() else None
    if not arquivo_id and not aba:
        return {"ok": False, "erro": "Diga o que desfazer."}

    if not dados.get("confirmar"):
        return {"ok": True, "so_contei": True,
                **conc.o_que_o_desfazer_apaga(int(conta_id), arquivo_id, aba)}

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        feito = conc.desfazer(int(conta_id), arquivo_id, aba,
                              bool(dados.get("levar_o_que_esta_no_omie")), quem)
    except conc.ErroDaConciliacao as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Conciliação: falhou desfazer")
        return {"ok": False, "erro": f"Não consegui desfazer: {e}"}, 500
    return {"ok": True, "so_contei": False, **feito}


# ===========================================================================
# O RATEIO DA FOLHA — para quem o ponto não pode apropriar (26/09/2026)
#
# Pedido do dono: *"em algum local a gente eleger as pessoas que vão ser rateadas
# e, para cada uma — ou para um grupo — definir para quais obras o valor dela vai
# ser rateado (…) pode ser que uma obra entre mais que a outra."*
#
# ⚠️ SÓ DO MESTRE. Isto decide para qual obra vai o salário de alguém, toda
# quinzena, até alguém mudar. O DP opera a folha; quem define o rateio é o dono
# (decisão dele em 26/09/2026: *"o usuário do DP faz a leitura, mas o usuário
# master, que sou eu, eu gero o arquivo"*).
# ===========================================================================
@bp.route("/folha/rateio")
@exige_operador
def tela_folha_rateio():
    from . import folha_rateio as fr

    pronto = fr._pronto()
    # A lista de obras é a MESMA do Ratear — a aba "C. Diários", carregada toda
    # noite. Uma segunda lista de obras divergiria da primeira no dia em que
    # alguém cadastrasse obra nova.
    from . import sincronizacao
    try:
        obras = [o["nome"] for o in
                 (sincronizacao.referencias_rateio().get("obras") or [])]
    except Exception:  # noqa: BLE001 — a lista é apoio; sem ela dá recado
        logger.exception("Folha: não consegui ler a lista de obras")
        obras = []
    regras = fr.listar() if pronto else []

    # O CADASTRO ENTRA AQUI PARA DUAS COISAS, as duas pedidas em 27/09/2026:
    #
    #   1. O LINK PARA O CARD DO PIPEFY de cada pessoa. *"É bom ter um link
    #      para clicar nele e ser direcionado, abre o card do Pipefy."* O lugar
    #      de corrigir o auxílio ou a gratificação é o card — daqui só se vai
    #      até lá.
    #   2. O NOME DE VERDADE. A regra de rateio guarda o nome que quem cadastrou
    #      digitou, para reconhecer na tela. Quando a pessoa está no cadastro,
    #      o nome do cadastro é melhor: é o que a folha e o ponto usam.
    #
    # Num `try` porque a migração 028 pode não ter sido aplicada ainda: o código
    # sobe para o Render antes do botão ser apertado, e esta tela não pode cair
    # nesse intervalo.
    from . import colaboradores
    cadastro = {"quando": "", "pessoas": 0, "avisos": [], "pronto": False}
    try:
        cadastro = colaboradores.quando_atualizou()
        cpfs = [p.get("cpf") for r in regras for p in (r.get("pessoas") or [])]
        fichas = colaboradores.muitos_por_cpf(cpfs)
        for regra in regras:
            for pessoa in (regra.get("pessoas") or []):
                ficha = fichas.get(pessoa.get("cpf")) or {}
                pessoa["link_pipefy"] = ficha.get("link_pipefy", "")
                pessoa["no_cadastro"] = bool(ficha)
                pessoa["desligado"] = bool(ficha.get("desligado"))
                # ⚠️ A SITUAÇÃO VEM DA MESMA FUNÇÃO que a tela de Colaboradores
                # usa, e é o que impede as duas telas de divergirem. Uma regra
                # de rateio que aponta para quem saiu apropria salário de
                # ninguém — e o erro fica invisível até a folha não fechar.
                pessoa["situacao"] = ficha.get("situacao", "")
                pessoa["motivo_situacao"] = ficha.get("motivo", "")
                pessoa["desacordo"] = ficha.get("desacordo", "")
                pessoa["alerta"] = bool(ficha.get("alerta"))
                if ficha.get("nome"):
                    pessoa["nome_cadastro"] = ficha["nome"]
    except Exception:  # noqa: BLE001 — a tela abre mesmo sem o cadastro
        logger.exception("Folha: não consegui ler o cadastro de colaboradores")

    return render_template(
        "analisesps_folha_rateio.html", aba="folha", subaba="rateio",
        grupos=subtelas_agrupadas(),
        pronto=pronto, regras=regras, obras=obras, cadastro=cadastro,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/folha")
@exige_consulta
def tela_folha():
    """A porta da área da Folha. Manda para a primeira subtela que a pessoa vê.

    POR QUE REDIRECIONA em vez de mostrar um índice: um índice com dois links
    seria um clique a mais para chegar ao mesmo lugar. A primeira subtela é a
    folha da contabilidade, que por sua vez abre a última folha importada."""
    permitidas = subtelas_da_folha()
    if not permitidas:
        # Não deve acontecer — quem chega aqui já tem a tela "folha". Mas se
        # acontecer, a resposta é 404, não 403: dizer "sem permissão" confirma
        # o que existe do outro lado.
        return render_template("analisesps_erro.html",
                               mensagem="Tela não encontrada."), 404
    return redirect(url_for(permitidas[0][2]))


@bp.route("/folha/<int:folha_id>")
@exige_consulta
def tela_folha_aberta(folha_id: int):
    """A FOLHA ABERTA PARA TRABALHAR: pessoa por pessoa, com obra e seleção.

    ⚠️ ESTA TELA FOI REFEITA EM 29/09/2026, e o motivo é uma cobrança dele:

        *"Eu importo o arquivo e não tenho gestão nenhuma sobre as informações
        dele. Quem vai, quem não vai. (…) Cadê os dados deles, cadê uma tabela
        mostrando as informações, cadê a possibilidade de seleção de quem entra e
        quem não entra, cadê onde gera o arquivo de pagamento?"*

    Antes ela era só leitura — nome, código, CPF, filial e valor — e o ÚNICO
    caminho até aqui era o número embaixo de "Precisam de olho", que desaparece
    quando não há ninguém pendente. Quem importava a folha e não achava aquele
    número não tinha porta nenhuma.

    ⚠️ E O TOTAL POR OBRA SAI DO PONTO, dia por dia, como ele disse no mesmo dia:
    *"aqui já devemos usar a folha de ponto mesmo, visto que tem o rateio diário
    pra formar os totalizadores por obra."*"""
    from . import folha_arquivo as fa
    from . import folha_gestao as fg

    montado = {}
    erro = None
    try:
        # CASA DE NOVO A CADA VISITA: o cadastro pode ter sido atualizado depois
        # da importação, e aí gente que estava pendente passa a casar sem
        # ninguém reimportar nada.
        fa.casar_com_o_cadastro(folha_id)
        montado = fg.montar(folha_id, _filtros_da_folha())
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui abrir a folha %s", folha_id)
        erro = str(e)

    if not montado and erro is None:
        # Fora do escopo responde "não encontrado", nunca "sem permissão".
        return render_template("analisesps_erro.html",
                               mensagem="Folha não encontrada."), 404

    # O QUE JÁ FOI PAGO NO MÊS, todas as verbas, por obra e por conta — era o
    # Panorama, e continua sendo a entrada do rateio do mês. Sai do que está
    # FECHADO. É um bloco da janela "Divisão por obra", não a tela: se estourar,
    # a tela abre sem ele.
    gerencial = {"pronto": False}
    if montado:
        try:
            from . import folha_pagamento as fpg
            f = montado.get("folha") or {}
            gerencial = fpg.gerencial(int(f["ano"]), int(f["mes"]))
        except Exception:  # noqa: BLE001
            logger.exception("Folha: não consegui montar o já pago do mês")

    return render_template(
        "analisesps_folha_aberta.html", aba="folha", subaba="importar",
        grupos=subtelas_agrupadas(), montado=montado, erro=erro,
        folha=montado.get("folha"), folhas=fa.listar(teto=24),
        criticas=fa.criticas(folha_id) if montado else None,
        filiais=fa.totais_por_filial(folha_id) if montado else [],
        setores=fa.totais_por_setor(folha_id) if montado else [],
        gerencial=gerencial,
        situacoes=[(c, fg.ROTULO_DA_SITUACAO[c]) for c in fg.ORDEM_DAS_SITUACOES],
        pode_operar=auth.pode_operar(),
        # A prévia do pagamento é do mestre, como gerar: o arquivo tem nome, CPF
        # e valor de todo mundo. Quem não é mestre nem vê o botão.
        pode_gerar=auth.e_mestre(),
        destinos=_destinos_do_pagamento(),
        fila_do_ponto=_fila_do_ponto_recente(),
        obras_c_diarios=_obras_c_diarios(),
        mudancas=(fa.mudancas(folha_id) if montado else None),
        analitica=(_analitica_da_folha(montado.get("folha")) if montado else None),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


def _filtros_da_folha() -> dict:
    """Os filtros da lateral, lidos do endereço. Cada bloco é de caixinhas
    (padrão das Solicitações), então pode vir mais de um valor por chave. O
    relatório lê daqui também — é o que garante que ele sai igual à tela."""
    from . import folha_gestao as fg
    filtros = {"busca": request.args.get("q") or ""}
    for chave in fg.CHAVES_DE_FILTRO:
        filtros[chave] = [v for v in request.args.getlist(chave) if v]
    return filtros


@bp.route("/folha/<int:folha_id>/relatorio.<formato>")
@exige_consulta
def folha_relatorio(folha_id: int, formato: str):
    """O relatório do que está na tela — Excel ou PDF —, com os agrupamentos.

    Pedido dele em 01/10/2026: *"o relatório do que eu visualizo em tela, além
    de poder ver o agrupamento do pagamento. Por obra, por conta e etc."*"""
    from . import folha_gestao as fg, folha_relatorio as fr

    if formato not in ("xlsx", "pdf"):
        return render_template("analisesps_erro.html", titulo="Não encontrado",
                               mensagem="Este formato de relatório não existe."), 404
    try:
        montado = fg.montar(folha_id, _filtros_da_folha())
        if not montado:
            return render_template("analisesps_erro.html", titulo="Não encontrado",
                                   mensagem="Folha não encontrada."), 404
        # ⚠️ O RECORTE POR CONTA (02/10/2026): "" = todas as contas num
        # arquivo; "__cada" = um arquivo por conta, num .zip; outro valor = só
        # aquela conta. Ver `folha_relatorio.recortar_por_conta`.
        recorte = (request.args.get("relatorio_conta") or "").strip()
        contas = fg._contas_das_obras()
        if formato == "pdf":
            # O contracheque de cada pessoa no PDF (03/10/2026).
            montado = fr.com_contracheques(montado)
        if recorte == "__cada":
            conteudo, nome = fr.zip_por_conta(montado, contas, formato)
            return Response(conteudo, mimetype="application/zip", headers={
                "Content-Disposition": f'attachment; filename="{nome}"'})
        dados = fr.montar(montado, contas, recorte)
        if formato == "xlsx":
            conteudo, tipo = fr.excel(dados), fr.MIME_XLSX
        else:
            conteudo, tipo = fr.pdf(dados), "application/pdf"
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: falhou o relatório %s da folha %s", formato, folha_id)
        return render_template("analisesps_erro.html", titulo="Relatório da folha",
                               mensagem=f"Não foi possível gerar o relatório: {e}"), 500
    nome = fr.nome_do_arquivo(dados, formato)
    return Response(conteudo, mimetype=tipo, headers={
        "Content-Disposition": f'attachment; filename="{nome}"'})


def _resposta_do_relatorio(montado: dict, contas: dict, formato: str):
    """O relatório (Excel ou PDF) de qualquer folha, com o recorte por conta da
    tela (`relatorio_conta`): "" = todas, "__cada" = .zip, outro = uma conta."""
    from . import folha_relatorio as fr
    recorte = (request.args.get("relatorio_conta") or "").strip()
    if recorte == "__cada":
        conteudo, nome = fr.zip_por_conta(montado, contas, formato)
        return Response(conteudo, mimetype="application/zip", headers={
            "Content-Disposition": f'attachment; filename="{nome}"'})
    dados = fr.montar(montado, contas, recorte)
    if formato == "xlsx":
        conteudo, tipo = fr.excel(dados), fr.MIME_XLSX
    else:
        conteudo, tipo = fr.pdf(dados), "application/pdf"
    return Response(conteudo, mimetype=tipo, headers={
        "Content-Disposition": f'attachment; filename="{fr.nome_do_arquivo(dados, formato)}"'})


@bp.route("/folha/diaristas/relatorio.<formato>")
@exige_consulta
def folha_diaristas_relatorio(formato: str):
    """O relatório das diárias — o mesmo da folha da contabilidade (dono,
    02/10/2026: *"Preciso dos relatórios também, PDF, Excel, por conta, total,
    do mesmo jeito"*). Sai com os filtros da tela."""
    from . import folha_diaristas as fd, folha_lista, folha_pagamento as fpg
    from . import folha_relatorio as fr
    if formato not in ("xlsx", "pdf"):
        return render_template("analisesps_erro.html", titulo="Não encontrado",
                               mensagem="Este formato de relatório não existe."), 404
    try:
        ano, mes = int(request.args.get("ano") or 0), int(request.args.get("mes") or 0)
        calculado = fd.calcular(ano, mes, request.args.get("periodo") or "quinzena")
        lista = folha_lista.filtrar(calculado.get("pessoas") or [], request.args,
                                    escondidas=folha_lista.ESCONDIDAS_NOS_DIARISTAS)
        contas = fpg.conta_por_obra()
        montado = fr.montado_das_diarias(calculado, lista["pessoas"],
                                         lista["filtros"], contas)
        return _resposta_do_relatorio(montado, contas, formato)
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: falhou o relatório das diárias")
        return render_template("analisesps_erro.html", titulo="Relatório das diárias",
                               mensagem=f"Não foi possível gerar o relatório: {e}"), 500


@bp.route("/folha/auxilio/relatorio.<formato>")
@exige_consulta
def folha_auxilio_relatorio(formato: str):
    """O relatório da alimentação ou do transporte — o mesmo das outras folhas."""
    from . import folha_auxilio as fx, folha_lista, folha_pagamento as fpg
    from . import folha_relatorio as fr
    if formato not in ("xlsx", "pdf"):
        return render_template("analisesps_erro.html", titulo="Não encontrado",
                               mensagem="Este formato de relatório não existe."), 404
    tipo = request.args.get("tipo") or ""
    if tipo not in fx.TIPOS:
        return render_template("analisesps_erro.html", titulo="Não encontrado",
                               mensagem="Auxílio não reconhecido."), 404
    try:
        ano, mes = int(request.args.get("ano") or 0), int(request.args.get("mes") or 0)
        resultado = fx.calcular(tipo, ano, mes)
        lista = folha_lista.filtrar(list(resultado.get("pessoas") or []),
                                    request.args, campo_da_obra="obra",
                                    escondidas=folha_lista.ESCONDIDAS_NOS_AUXILIOS)
        contas = fpg.conta_por_obra()
        montado = fr.montado_do_auxilio(resultado, lista["pessoas"], lista["filtros"],
                                        contas, fx.ROTULO_DO_TIPO[tipo],
                                        f"{mes:02d}/{ano}")
        return _resposta_do_relatorio(montado, contas, formato)
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou o relatório do auxílio")
        return render_template("analisesps_erro.html", titulo="Relatório do auxílio",
                               mensagem=f"Não foi possível gerar o relatório: {e}"), 500


def _analitica_da_folha(folha) -> dict | None:
    """Se a folha aberta tem a analítica importada. None se não tem (ou falhou)."""
    from . import folha_analitica_guardada as fag
    try:
        return fag.da_folha(folha["ano"], folha["mes"], folha["tipo"]) if folha else None
    except Exception:  # noqa: BLE001
        logger.exception("Folha: não consegui ler a analítica da folha")
        return None


def _obras_c_diarios() -> list:
    """As obras da aba "C. Diários" — a lista de onde se escolhe obra na folha.
    Vazia se falhar: aí os campos aceitam o código digitado, como antes."""
    from . import ponto_edicao
    try:
        return ponto_edicao.obras_permitidas()
    except Exception:  # noqa: BLE001
        logger.exception("Folha: não consegui ler as obras da C. Diários")
        return []


def _fila_do_ponto_recente() -> list:
    """Os pedidos recentes da fila do ponto, para a lateral. Vazia se falhar."""
    from . import ponto_fila
    try:
        return [_item_para_a_tela(i) for i in ponto_fila.recentes()]
    except Exception:  # noqa: BLE001 — bloco da lateral, não a tela
        logger.exception("Ponto: não consegui ler a fila")
        return []


def _destinos_do_pagamento() -> list:
    from . import folha_geracao as fger
    return [(d, fger.ROTULO_DO_DESTINO[d]) for d in fger.DESTINOS]


@bp.route("/folha/<int:folha_id>/previa-pagamento")
@exige_operador
def folha_previa_pagamento(folha_id: int):
    """Baixa a PRÉVIA do arquivo de pagamento desta folha, sem fechar nada.

    Pedido dele em 01/10/2026: *"se eu quiser gerar um arquivo de pagamento sem
    fechar, como fazer? até pra saber como tá saindo"*. Não sobe para o Drive,
    não entra no log — ver `folha_pagamento.previa_zip`."""
    from . import folha_geracao as fger, folha_pagamento as fpg

    destino = str(request.args.get("destino") or fger.BEEVALE).strip().lower()
    try:
        conteudo, nome = fpg.previa_zip(folha_id, destino)
    except (fpg.ErroDoPagamento, fger.ErroDaGeracao) as e:
        return render_template(
            "analisesps_erro.html", aba="folha", titulo="Prévia do pagamento",
            mensagem=f"Não foi possível gerar a prévia: {e}"), 400
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: falhou a prévia do pagamento")
        return render_template(
            "analisesps_erro.html", aba="folha", titulo="Prévia do pagamento",
            mensagem=f"Não foi possível gerar a prévia: {e}"), 500
    return Response(
        conteudo, mimetype="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{nome}"'})


@bp.route("/folha/previa-direta")
@exige_operador
def folha_previa_direta():
    """A PRÉVIA das diárias e dos auxílios, sem fechar nada — o mesmo .zip da
    prévia da folha da contabilidade (dono, 02/10/2026: *"eu queria o botão
    também de gerar a prévia"*). Não sobe para o Drive, não entra no registro."""
    from . import folha_geracao as fger, folha_pagamento as fpg

    origem = str(request.args.get("origem") or "").strip()
    destino = str(request.args.get("destino") or fger.BEEVALE).strip().lower()
    dados = {k: request.args.get(k)
             for k in ("ano", "mes", "periodo", "pagamento", "folha_id")}
    try:
        conteudo, nome = fpg.previa_direta_zip(
            origem, dados, destino, _destinos_por_conta(request.args.get("destinos")))
    except (fpg.ErroDoPagamento, fger.ErroDaGeracao) as e:
        return render_template(
            "analisesps_erro.html", aba="folha", titulo="Prévia do pagamento",
            mensagem=f"Não foi possível gerar a prévia: {e}"), 400
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: falhou a prévia direta do pagamento")
        return render_template(
            "analisesps_erro.html", aba="folha", titulo="Prévia do pagamento",
            mensagem=f"Não foi possível gerar a prévia: {e}"), 500
    return Response(
        conteudo, mimetype="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{nome}"'})


def _divisao_da_tela(calculado: dict, verba: str, apropriar) -> dict:
    """A divisão por obra e conta do que a tela mostra agora. Nunca derruba."""
    from . import folha_pagamento as fpg
    try:
        if not calculado or not calculado.get("pessoas"):
            return {}
        return fpg.divisao(fpg._linhas_do_apropriado(apropriar(calculado), verba))
    except Exception:  # noqa: BLE001 — a janela é apoio, não a tela
        logger.exception("Folha: não consegui montar a divisão por obra (%s)", verba)
        return {}


@bp.route("/api/folha/apropriacao/ajuste", methods=["POST"])
@exige_operador
def folha_apropriacao_ajustar():
    """Quem entra, quem sai, e para qual obra vai o valor de uma pessoa.

    ⚠️ É A "GESTÃO SOBRE O ARQUIVO" que ele cobrou. Três decisões, uma pessoa por
    chamada: tirar do pagamento (com motivo, sempre), jogar tudo numa obra, ou
    dividir entre obras.

    ⚠️ O AJUSTE É DO PAGAMENTO, NÃO DA FOLHA IMPORTADA. Guardar por `folha_id`
    faria uma reimportação — que é normal, ele corrige e manda de novo — apagar o
    trabalho mais caro do processo. Por isso a chave é competência + tipo + CPF,
    igual ao auxílio."""
    from . import folha_apropriacao_guardada as guardada
    from . import folha_arquivo as fa

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        folha_id = int(dados.get("folha_id") or 0)
    except (TypeError, ValueError):
        folha_id = 0
    folha = fa.abrir(folha_id) if folha_id else None
    if not folha:
        # Número que não existe responde "não encontrado", nunca "sem permissão".
        return {"ok": False, "erro": "Folha não encontrada."}, 404

    cpf = str(dados.get("cpf") or "")
    entra = dados.get("entra")
    por_obra = dados.get("por_obra") or []
    obra_unica = str(dados.get("obra") or "").strip()

    # ⚠️ A DIVISÃO ENTRA EM DIAS, E O VALOR É CALCULADO AQUI — nível 3 do ajuste
    # fino (§7.3): *"bota um dia numa obra, um dia em outra obra."* O valor por dia
    # é o líquido dividido pelos dias (coluna I da planilha), e a sobra do centavo
    # segue a mesma regra do ponto. Aceitar valor digitado deixaria a mesma pessoa
    # com dois valores por dia diferentes no mesmo período.
    if dados.get("dias_por_obra"):
        from . import folha_gestao as fg
        try:
            por_obra = fg.dividir_por_dias(dados.get("valor"),
                                           dados.get("dias_por_obra"))
        except fg.ErroDaGestao as e:
            return {"ok": False, "erro": str(e)}, 400
        obra_unica = ""

    try:
        if dados.get("limpar"):
            # Volta a seguir o ponto e a regra: é o desfazer da tela.
            guardada.limpar_ajuste(folha["ano"], folha["mes"], folha["tipo"], cpf)
        else:
            guardada.gravar_ajuste(
                folha["ano"], folha["mes"], folha["tipo"], cpf,
                nome=str(dados.get("nome") or ""),
                fora=(entra is False),
                motivo=str(dados.get("motivo") or ""),
                obra_unica=obra_unica, por_obra=por_obra,
                observacao=str(dados.get("observacao") or ""), quem=quem)
    except guardada.ErroDaApropriacao as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou gravar o ajuste da apropriação")
        return {"ok": False, "erro": f"Não foi possível gravar: {e}"}, 500
    return {"ok": True}


@bp.route("/api/folha/ponto/pessoa", methods=["POST"])
@exige_operador
def folha_ponto_pessoa_atualizar():
    """Dispara a atualização do ponto de UMA pessoa (a página dela no Mobponto).

    Roda no processo separado, como o ponto do mês: são pedidos de minutos à API.
    Quem e de que mês vão para o banco antes de disparar."""
    from . import folha_arquivo as fa
    from .folha_rateio import so_digitos
    dados = request.get_json(silent=True) or {}
    try:
        folha = fa.abrir(int(dados.get("folha_id") or 0))
    except (TypeError, ValueError):
        folha = None
    cpf = so_digitos(dados.get("cpf"))
    if not folha and dados.get("ano") and dados.get("mes") and len(cpf) == 11:
        # AS OUTRAS FOLHAS (diaristas, alimentação e transporte — 01/10/2026) não
        # têm arquivo da contabilidade: vale o mês da tela, para quem está no
        # cadastro.
        from . import colaboradores
        try:
            ano, mes = int(dados["ano"]), int(dados["mes"])
        except (TypeError, ValueError):
            ano = mes = 0
        if 2000 <= ano <= 2100 and 1 <= mes <= 12 and colaboradores.por_cpf(cpf):
            folha = {"ano": ano, "mes": mes}
    if not folha or len(cpf) != 11:
        return {"ok": False, "erro": "Colaborador não encontrado nesta folha."}, 404
    from . import ponto_fila
    if ponto_fila._pronto():
        # A FILA (migração 042): o pedido entra e a tela volta na hora, dizendo
        # quantos estão na frente. Dá para pedir a próxima pessoa em seguida.
        quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
        feito = ponto_fila.enfileirar(ponto_fila.PESSOA, folha["ano"], folha["mes"],
                                      cpf, str(dados.get("nome") or ""), quem=quem)
        ponto_fila.cutucar()
        return {"ok": True, "fila_id": feito["id"], "posicao": feito["posicao"]}
    resultado = _trazer_o_ponto_da_pessoa(folha, cpf, dados.get("nome"))
    if not resultado.get("ok"):
        return {"ok": False, "erro": resultado.get("erro")
                or "Outra tarefa está em execução. Aguarde a conclusão."}, 409
    return {"ok": True}


def _trazer_o_ponto_da_pessoa(folha: dict, cpf: str, nome) -> dict:
    """Dispara, no processo separado, a atualização do ponto de UMA pessoa."""
    from . import sincronizacao, tarefas
    from .db import conexao
    ocupada = _pista_da_pessoa_ocupada()
    if ocupada:
        return ocupada
    nome = str(nome or "").replace("|", " ")[:120]
    with conexao() as conn:
        sincronizacao._meta_gravar(conn, "ponto_pessoa_alvo",
                                   f"{folha['ano']}|{folha['mes']}|{cpf}|{nome}")
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    return tarefas.disparar("ponto_pessoa", disparo=quem or "ponto do colaborador")


def _pista_da_pessoa_ocupada() -> dict | None:
    """Recusa ANTES de escrever o pedido, quando já há tarefa de pessoa rodando.

    ⚠️ O PEDIDO MORA NUM LUGAR SÓ (`ponto_pessoa_alvo`, `ponto_lancar_pedido`), e o
    processo o lê ao começar. Escrever o pedido de B enquanto o de A está para
    começar faria o processo de A trabalhar para B — e o de B seria recusado.
    Por isso a pergunta vem antes da escrita."""
    from . import tarefas
    atual = tarefas.estado("pessoa")
    if atual.get("rodando"):
        detalhe = atual.get("detalhe") or {}
        return {"ok": False,
                "erro": "Já existe uma tarefa de ponto de colaborador em execução ("
                        + (detalhe.get("etapa") or "iniciando") + "). Ela "
                        "prossegue mesmo que a tela seja fechada; aguarde a conclusão "
                        "para solicitar a próxima."}
    return None


def _pedido_de_lancamento():
    """Lê e confere o pedido de lançamento. Devolve (folha, cpf, dados) ou a
    resposta de recusa."""
    from . import folha_arquivo as fa
    from .folha_rateio import so_digitos
    dados = request.get_json(silent=True) or {}
    try:
        folha = fa.abrir(int(dados.get("folha_id") or 0))
    except (TypeError, ValueError):
        folha = None
    cpf = so_digitos(dados.get("cpf"))
    if not folha and dados.get("ano") and dados.get("mes") and len(cpf) == 11:
        # FORA DA FOLHA DA CONTABILIDADE (tela do Ponto, diaristas, auxílio —
        # 02/10/2026): vale o mês informado, para quem está no cadastro.
        from . import colaboradores
        try:
            ano, mes = int(dados["ano"]), int(dados["mes"])
        except (TypeError, ValueError):
            ano = mes = 0
        if 2000 <= ano <= 2100 and 1 <= mes <= 12 and colaboradores.por_cpf(cpf):
            return {"ano": ano, "mes": mes, "linhas": []}, cpf, dados, None
        return None, None, None, ({"ok": False, "erro": "Colaborador não encontrado no cadastro."}, 404)
    cpfs_da_folha = {so_digitos(l.get("cpf")) for l in ((folha or {}).get("linhas") or [])}
    if not folha or len(cpf) != 11 or cpf not in cpfs_da_folha:
        return None, None, None, ({"ok": False, "erro": "Colaborador não encontrado nesta folha."}, 404)
    return folha, cpf, dados, None


@bp.route("/api/folha/ponto/plano", methods=["POST"])
@exige_operador
def folha_ponto_plano():
    """O que SERIA lançado no Mobponto, dia a dia, e o que fica de fora — sem
    lançar nada. É o que a pessoa confere antes de apertar "Lançar".

    De quem OPERA a folha (desde 01/10/2026 não é só do mestre)."""
    from . import ponto_edicao
    folha, cpf, dados, recusa = _pedido_de_lancamento()
    if recusa:
        return recusa
    try:
        obra, _ = ponto_edicao.validar_pedido(dados.get("obra"), "x" * 10)
        plano = ponto_edicao.plano_da_pessoa(
            folha["ano"], folha["mes"], cpf, dados.get("de") or "",
            dados.get("ate") or dados.get("de") or "", obra,
            dados.get("hora_avulsa") or "")
    except (ponto_edicao.ErroDaEdicao, ValueError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Ponto: falhou montar o plano de lançamento")
        return {"ok": False, "erro": f"Não foi possível montar o plano: {e}"}, 500
    return {"ok": True, "plano": plano, "falta": ponto_edicao.o_que_falta()}


@bp.route("/api/folha/ponto/lancar", methods=["POST"])
@exige_operador
def folha_ponto_lancar():
    """Lança no MOBPONTO as batidas que faltam no período, no processo separado.

    O plano é refeito lá, com o ponto trazido de novo antes — ver
    `ponto_edicao.lancar`. ⚠️ De quem OPERA a folha (decisão do dono em
    01/10/2026; era só do mestre). Grava em sistema de terceiro: cada batida
    fica registrada com quem pediu (`ponto_batida_enviada`, `ponto_fila`)."""
    import json as _json

    from . import ponto_edicao, sincronizacao, tarefas
    from .db import conexao
    folha, cpf, dados, recusa = _pedido_de_lancamento()
    if recusa:
        return recusa
    falta = ponto_edicao.o_que_falta()
    if falta:
        return {"ok": False, "erro": falta}, 400
    try:
        obra, texto = ponto_edicao.validar_pedido(dados.get("obra"),
                                                  dados.get("justificativa"))
        # Confere o período agora, para a recusa sair na tela e não na tarefa.
        ponto_edicao.plano_da_pessoa(folha["ano"], folha["mes"], cpf,
                                     dados.get("de") or "",
                                     dados.get("ate") or dados.get("de") or "",
                                     obra, dados.get("hora_avulsa") or "")
    except (ponto_edicao.ErroDaEdicao, ValueError) as e:
        return {"ok": False, "erro": str(e)}, 400
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    pedido = {"ano": folha["ano"], "mes": folha["mes"], "cpf": cpf,
              "nome": str(dados.get("nome") or "")[:160],
              "de": str(dados.get("de") or "")[:10],
              "ate": str(dados.get("ate") or dados.get("de") or "")[:10],
              "obra": obra, "justificativa": texto,
              "hora_avulsa": str(dados.get("hora_avulsa") or "")[:5], "quem": quem}
    from . import ponto_fila
    if ponto_fila._pronto():
        feito = ponto_fila.enfileirar(ponto_fila.LANCAR, folha["ano"], folha["mes"],
                                      cpf, pedido["nome"], pedido, quem=quem)
        ponto_fila.cutucar()
        return {"ok": True, "fila_id": feito["id"], "posicao": feito["posicao"]}
    ocupada = _pista_da_pessoa_ocupada()
    if ocupada:
        return ocupada, 409
    with conexao() as conn:
        sincronizacao._meta_gravar(conn, "ponto_lancar_pedido",
                                   _json.dumps(pedido, ensure_ascii=False))
    resultado = tarefas.disparar("ponto_lancar", disparo=quem or "lançamento de ponto")
    if not resultado.get("ok"):
        return {"ok": False, "erro": resultado.get("erro")
                or "Outra tarefa está em execução. Aguarde a conclusão."}, 409
    return {"ok": True}


def _item_para_a_tela(item: dict) -> dict:
    from .formatos import momento_br
    return {"id": item["id"], "tipo": item["tipo"], "rotulo": item["rotulo"],
            "cpf": item["cpf"], "nome": item["nome"], "situacao": item["situacao"],
            "posicao": item.get("posicao", 0), "progresso": item["progresso"],
            "mensagem": item["mensagem"], "pedido_por": item["pedido_por"],
            "criado_em": momento_br(item.get("criado_em"))}


@bp.route("/api/folha/ponto/fila/<int:item_id>")
@exige_consulta
def folha_ponto_fila_item(item_id: int):
    """Onde está um pedido da fila do ponto — e cutuca a fila (ver `cutucar`)."""
    from . import ponto_fila
    ponto_fila.cutucar()
    item = ponto_fila.item(item_id)
    if item is None:
        return {"ok": False, "erro": "Pedido não encontrado na fila."}, 404
    return {"ok": True, "item": _item_para_a_tela(item)}


@bp.route("/api/folha/ponto/fila")
@exige_consulta
def folha_ponto_fila():
    """Os pedidos recentes da fila do ponto, para a lateral da folha."""
    from . import ponto_fila
    ponto_fila.cutucar()
    return {"ok": True, "itens": [_item_para_a_tela(i) for i in ponto_fila.recentes()]}


@bp.route("/api/folha/ponto/lancar/estado")
@exige_consulta
def folha_ponto_lancar_estado():
    """Como terminou o último lançamento no Mobponto."""
    from . import tarefas
    ultima = tarefas.ultima_do_tipo("ponto_lancar") or {}
    return {"ok": True, "em_andamento": bool(ultima.get("em_andamento")),
            "sucesso": ultima.get("ok"), "mensagem": ultima.get("mensagem") or "",
            "etapa": ultima.get("etapa") or "",
            "progresso": ultima.get("progresso") or ""}


@bp.route("/api/folha/ponto/pessoa/estado")
@exige_consulta
def folha_ponto_pessoa_estado():
    """Como terminou a última atualização do ponto de uma pessoa."""
    from . import tarefas
    ultima = tarefas.ultima_do_tipo("ponto_pessoa") or {}
    return {"ok": True, "em_andamento": bool(ultima.get("em_andamento")),
            "sucesso": ultima.get("ok"), "mensagem": ultima.get("mensagem") or "",
            "etapa": ultima.get("etapa") or "",
            "progresso": ultima.get("progresso") or ""}


@bp.route("/folha/<int:folha_id>/pendente/<id_fortes>")
@exige_consulta
def tela_folha_pendente(folha_id: int, id_fortes: str):
    """Por que esta linha da folha não casou com o cadastro — o miolo da janela.

    Mostra o que o sistema TEM guardado para o código e para o nome, a data da
    última atualização do cadastro, e a frase que diz o que fazer."""
    from . import colaboradores, folha_arquivo as fa
    folha = fa.abrir(folha_id)
    if not folha:
        return '<div class="aviso erro">Folha não encontrada.</div>', 404
    alvo = colaboradores.normalizar_id_fortes(id_fortes)
    linha = next((l for l in folha["linhas"]
                  if colaboradores.normalizar_id_fortes(l.get("id_fortes")) == alvo), None)
    if not linha:
        return '<div class="aviso erro">Código não encontrado nesta folha.</div>', 404
    try:
        d = colaboradores.por_que_nao_casou(alvo, linha.get("nome") or "")
    except Exception as e:  # noqa: BLE001 — a janela tem de dizer o que houve
        logger.exception("Folha: não consegui montar o diagnóstico do cadastro")
        return f'<div class="aviso erro">Não foi possível realizar a busca: {e}</div>', 500
    return render_template("_folha_pendente.html", d=d, linha=linha)


@bp.route("/folha/<int:folha_id>/pessoa/<cpf>")
@exige_consulta
def tela_folha_pessoa(folha_id: int, cpf: str):
    """O analítico de um funcionário na folha — página para imprimir, e (com
    `?parcial=1`) o miolo da janela que abre no nome. Um desenho só para os dois.

    Pessoa ou folha que não existe: 404, "não encontrado"."""
    from . import folha_gestao as fg
    from .horario import agora
    try:
        a = fg.ponto_da_pessoa(folha_id, cpf)
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui montar o analítico")
        if request.args.get("parcial"):
            return (f'<div class="aviso erro">Não foi possível montar o analítico: '
                    f"{e}</div>"), 500
        return render_template("analisesps_erro.html",
                               mensagem=f"Não foi possível montar o analítico: {e}"), 500
    if not a or not a.get("achou"):
        if request.args.get("parcial"):
            return '<div class="aviso erro">Colaborador não encontrado nesta folha.</div>', 404
        return render_template("analisesps_erro.html",
                               mensagem="Colaborador não encontrado nesta folha."), 404
    if request.args.get("parcial"):
        from . import ponto_edicao, ponto_fila
        try:
            fila_da_pessoa = ponto_fila.ultimo_da_pessoa(a["cpf"])
        except Exception:  # noqa: BLE001 — o analítico abre sem isto
            logger.exception("Ponto: não consegui ler a fila desta pessoa")
            fila_da_pessoa = None
        return render_template("_folha_analitico.html", a=a, parcial=True,
                               fila_da_pessoa=fila_da_pessoa,
                               pode_operar=auth.pode_operar(),
                               # LANÇAR O PONTO É DE QUEM OPERA A FOLHA, não só
                               # do mestre — o dono, 01/10/2026, sobre a pessoa do
                               # DP: *"embora eu tenha colocado ela como sendo uma
                               # pessoa que altera as informações, ela não está
                               # conseguindo editar (…) lançar no ponto."*
                               editar_ponto=auth.pode_operar(),
                               falta_para_editar=ponto_edicao.o_que_falta(),
                               # A lista de obras é a da C. Diários — as mesmas
                               # do Mobponto (o dono, 01/10/2026).
                               obras_do_mobponto=(ponto_edicao.obras_permitidas()
                                                  if auth.pode_operar() else []))
    return render_template(
        "analisesps_folha_pessoa.html", a=a, aba="folha", subaba="importar",
        gerado_em=agora().strftime("%d/%m/%Y %H:%M"),
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/folha/<int:folha_id>/pessoa/<cpf>/cadastro")
@exige_consulta
def tela_folha_cadastro_completo(folha_id: int, cpf: str):
    """A ficha inteira da pessoa, lida NA HORA da planilha de cadastro.

    Pedido dele, 01/10/2026: *"em algum canto para clicar do cadastro, que
    abrisse um outro modal mais completo, com o detalhamento do resto das
    informações."* Só de quem está nesta folha — e fora dela, 404."""
    from . import colaboradores, folha_arquivo as fa
    from .folha_rateio import so_digitos
    folha = fa.abrir(folha_id)
    digitos = so_digitos(cpf)
    if not folha or digitos not in {so_digitos(l.get("cpf")) for l in folha["linhas"]}:
        return '<div class="aviso erro">Colaborador não encontrado nesta folha.</div>', 404
    try:
        grupos = colaboradores.ficha_completa(digitos)
    except colaboradores.ErroDoCadastro as e:
        return render_template("_folha_cadastro_completo.html", grupos=[], erro=str(e))
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou ler a ficha completa")
        return render_template("_folha_cadastro_completo.html", grupos=[],
                               erro=f"Não foi possível ler a planilha de cadastro: {e}")
    return render_template("_folha_cadastro_completo.html", grupos=grupos, erro="")


@bp.route("/api/folha/cadastro/pessoa", methods=["POST"])
@exige_operador
def folha_cadastro_pessoa_atualizar():
    """Traz de novo da planilha o cadastro de UMA pessoa — o "Atualizar
    cadastro" do analítico. Segundos: lê a coluna do CPF e a linha dela."""
    from . import colaboradores
    from .folha_rateio import so_digitos
    dados = request.get_json(silent=True) or {}
    cpf = so_digitos(dados.get("cpf"))
    if len(cpf) != 11:
        return {"ok": False, "erro": "CPF incompleto."}, 400
    try:
        r = colaboradores.atualizar_uma(cpf)
    except colaboradores.ErroDoCadastro as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou atualizar o cadastro de uma pessoa")
        return {"ok": False, "erro": f"Não foi possível atualizar o cadastro: {e}"}, 500
    return {"ok": True, "mensagem": f"Cadastro de {r.get('nome') or cpf} atualizado."}


@bp.route("/api/folha/<int:folha_id>/ponto/<cpf>")
@exige_consulta
def folha_ponto_da_pessoa(folha_id: int, cpf: str):
    """O ponto de uma pessoa, dia a dia, para a janela que abre no nome.

    Sai da mesma conta da tela (`folha_gestao.ponto_da_pessoa`). Folha ou pessoa
    que não existe responde 404, "não encontrado" — nunca "sem permissão"."""
    from . import folha_gestao as fg
    try:
        visto = fg.ponto_da_pessoa(folha_id, cpf)
    except Exception as e:  # noqa: BLE001 — a janela tem de dizer o que houve
        logger.exception("Folha: não consegui montar o ponto da pessoa")
        return {"ok": False, "erro": f"Não foi possível montar o ponto: {e}"}, 500
    if not visto or not visto.get("achou"):
        return {"ok": False, "erro": "Colaborador não encontrado nesta folha."}, 404
    return {"ok": True, **visto}


@bp.route("/api/folha/apropriacao/fechar", methods=["POST"])
@exige_operador
def folha_apropriacao_fechar():
    """Congela a apropriação desta folha — o passo antes de gerar o arquivo.

    ⚠️ SEM ESTE BOTÃO NÃO HAVIA COMO GERAR O ARQUIVO DA FOLHA. O gerador só paga
    apropriação fechada (`folha_pagamento.gerar`), e nenhuma tela fechava a verba
    `folha` — então o arquivo era, na prática, impossível de sair."""
    from . import folha_gestao as fg

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        folha_id = int(dados.get("folha_id") or 0)
    except (TypeError, ValueError):
        folha_id = 0
    try:
        feito = fg.fechar(folha_id, quem=quem)
    except fg.ErroDaGestao as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou fechar a apropriação")
        return {"ok": False, "erro": f"Não foi possível concluir o fechamento: {e}"}, 500
    return {"ok": True, **{k: str(v) for k, v in feito.items()}}


@bp.route("/folha/importar")
@exige_consulta
def tela_folha_importar():
    """A folha que a contabilidade manda, importada e guardada.

    ⚠️ QUANDO JÁ HÁ FOLHA IMPORTADA, ESTA TELA NÃO É O DESTINO — ela manda direto
    para a última folha aberta, que é onde se trabalha. Correção do dono em
    29/09/2026: *"eu estou em Folha Contabilidade e a única coisa que aparece é um
    botão pra clicar nas pessoas que 'PRECISAM DE OLHO'."*

    Ele estava certo e o erro era de desenho: esta tela é uma ESTANTE (a área de
    soltar o arquivo e a lista do que já veio), e uma estante não é trabalho. A
    lista continua alcançável — `?lista=1`, e o seletor de competência da própria
    folha aberta aponta para cá — mas quem chega pela aba cai onde há o que fazer.

    Sem folha nenhuma, ela é o destino certo: é onde se solta o arquivo."""
    from . import folha_arquivo as fa

    pronto = fa._pronto()
    if pronto and not request.args.get("lista"):
        ultima = (fa.listar(teto=1) or [None])[0]
        if ultima:
            return redirect(url_for("analisesps.tela_folha_aberta",
                                    folha_id=ultima["id"]))
    folhas = []
    erro = None
    try:
        folhas = fa.listar() if pronto else []
        # ⚠️ AS PESSOAS PENDENTES ENTRAM NA LISTA, e isso é correção do dono em
        # 29/09/2026: *"você tá muito preocupado com os totalizadores do arquivo de
        # importação, quando a preocupação deve ser linha a linha de cada
        # colaborador."*
        #
        # Ele está certo. "Não fecha" é uma pista; o que decide se dá para pagar é
        # QUANTAS PESSOAS estão sem cadastro, já saíram ou estão saindo. Isso já era
        # calculado (`criticas`) e só aparecia abrindo a folha.
        for f in folhas:
            try:
                c = fa.criticas(f["id"])
                f["pendentes"] = len(c.get("pendentes") or [])
                f["sairam"] = len(c.get("sairam") or [])
                f["saindo"] = len(c.get("saindo") or [])
                f["precisa_de_mao"] = (f["pendentes"] + f["sairam"]
                                       + f["saindo"])
            except Exception:  # noqa: BLE001 — uma folha torta não derruba a lista
                logger.exception("Folha: não consegui criticar a folha %s",
                                 f.get("id"))
                f["pendentes"] = f["sairam"] = f["saindo"] = None
                f["precisa_de_mao"] = None
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui listar as folhas importadas")
        erro = str(e)

    return render_template(
        "analisesps_folha_importar.html", aba="folha", subaba="importar",
        grupos=subtelas_agrupadas(), pronto=pronto, folhas=folhas, erro=erro,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/api/folha/importar", methods=["POST"])
@exige_operador
def folha_importar():
    """Recebe o `.xls` da contabilidade, lê e guarda.

    ⚠️ QUANDO NÃO DÁ PARA SABER O PERÍODO pelo título, a resposta volta com
    `pergunte_o_tipo` — e a tela PERGUNTA, em vez de mandar a pessoa tentar de
    novo adivinhando. Adivinhar erraria o período do ponto, e o período errado
    apropria os dias errados nas obras."""
    from . import folha_arquivo as fa

    arquivo = request.files.get("folha")
    if arquivo is None or not (arquivo.filename or "").strip():
        return {"ok": False, "erro": "Nenhum arquivo recebido."}, 400

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    conteudo = arquivo.read()

    # A FOLHA ANALÍTICA ENTRA PELA MESMA PORTA (01/10/2026): o mesmo lugar de
    # soltar arquivo reconhece qual dos dois chegou. A analítica não vira folha
    # nova — ela é guardada ao lado da sintética que ela explica.
    from . import folha_analitica as fan, folha_analitica_guardada as fag
    if fan.e_analitica(conteudo):
        try:
            feito = fag.importar(conteudo, nome_do_arquivo=arquivo.filename, quem=quem)
        except fag.ErroDaAnalitica as e:
            return {"ok": False, "erro": str(e)}, 400
        except Exception as e:  # noqa: BLE001
            logger.exception("Folha: falhou importar a folha analítica")
            return {"ok": False, "erro": f"Não foi possível importar a folha analítica: {e}"}, 500
        return {"ok": True, "analitica": True,
                "ir": url_for("analisesps.tela_folha_aberta", folha_id=feito["folha_id"]),
                "mensagem": (f"Folha analítica de {feito['competencia']} gravada: "
                             f"{feito['pessoas']} pessoa(s), {feito['batem']} com o "
                             "líquido igual ao da sintética.")}

    try:
        resultado = fa.importar(
            conteudo, nome_do_arquivo=arquivo.filename,
            tipo=str(request.form.get("tipo") or ""), quem=quem)
    except fa.ErroDaImportacao as e:
        frase = str(e)
        return {"ok": False, "erro": frase,
                "pergunte_o_tipo": "Escolha na tela" in frase}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou importar o arquivo da contabilidade")
        return {"ok": False, "erro": f"Não foi possível importar: {e}"}, 500
    return {"ok": True, **{k: str(v) if k == "total" else v
                           for k, v in resultado.items()}}


@bp.route("/api/folha/apagar", methods=["POST"])
@exige_operador
def folha_apagar():
    """Apaga uma folha importada. O arquivo original continua com a
    contabilidade, e a apropriação mora em outro lugar — então isto não perde
    decisão nenhuma."""
    from . import folha_arquivo as fa

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        apagou = fa.apagar(int(dados.get("id") or 0), quem=quem)
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou apagar a folha importada")
        return {"ok": False, "erro": f"Não foi possível excluir: {e}"}, 500
    if not apagou:
        return {"ok": False, "erro": "Folha não encontrada."}, 404
    return {"ok": True}


@bp.route("/folha/ponto")
@exige_consulta
def tela_folha_ponto():
    """O ponto do Mobponto, mês por mês.

    ⚠️ É O GARGALO DA FOLHA: sem o ponto não há total por obra, não há diária e
    não há apropriação. A tela existe para trazer o mês e para MOSTRAR QUAIS
    CAMPOS a API manda em cada dia — é com essa lista que se mapeia a obra e as
    marcações, sem palpite."""
    from . import folha_lista, ponto as _ponto
    from .folha_rateio import so_digitos

    pronto = _ponto._pronto()
    cargas = []
    erro = None
    try:
        cargas = _ponto.cargas() if pronto else []
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui listar as cargas do ponto")
        erro = str(e)

    # A COMPETÊNCIA VISTA (02/10/2026: a tela passou a listar os colaboradores e
    # o ponto de cada um). Sem escolha, a carga mais recente.
    vista = None
    try:
        pedido = (int(request.args.get("ano") or 0), int(request.args.get("mes") or 0))
    except (TypeError, ValueError):
        pedido = (0, 0)
    vista = next((c for c in cargas if (c["ano"], c["mes"]) == pedido and not c["interrompida"]),
                 None) or next((c for c in cargas if not c["interrompida"]), None)
    pessoas, lista = [], None
    if vista:
        try:
            pessoas = _ponto.resumo_por_pessoa(vista["ano"], vista["mes"])
        except Exception as e:  # noqa: BLE001
            logger.exception("Folha: não consegui resumir o ponto")
            erro = str(e)
    for p in pessoas:
        p["obras_nomes"] = [o for o, _ in p["obras"]]
    busca = " ".join((request.args.get("q") or "").split())
    obras_marcadas = folha_lista.marcados(request.args, "obra")
    so = folha_lista.marcados(request.args, "so")
    filtradas = []
    for p in pessoas:
        if busca and not (busca.lower() in (p["nome"] or "").lower()
                          or (so_digitos(busca) and so_digitos(busca) in p["cpf"])):
            continue
        if obras_marcadas and not set(p["obras_nomes"]) & set(obras_marcadas):
            continue
        if "faltas" in so and not p["faltas"]:
            continue
        if "sem_marcacao" in so and not p["sem_marcacao"]:
            continue
        filtradas.append(p)
    lista = {"q": busca, "obra": obras_marcadas, "so": so,
             "filtrando": bool(busca or obras_marcadas or so),
             "opcoes_obra": [(o, o) for o in sorted({o for p in pessoas
                                                     for o in p["obras_nomes"]})],
             "opcoes_so": [("faltas", f"com falta ({sum(1 for p in pessoas if p['faltas'])})"),
                           ("sem_marcacao", "com dia sem batida "
                            f"({sum(1 for p in pessoas if p['sem_marcacao'])})")]}

    # ⚠️ O ESTADO DA CARGA AO ABRIR A TELA. Reclamação dele em 28/09/2026: *"a
    # gente bota aqui trazer o ponto, mas aí se o ponto veio, se o ponto não veio,
    # só Deus sabe o que está acontecendo com essa API. Se ela está carregando, se
    # ela não está."*
    #
    # Antes a tela só acompanhava uma carga se VOCÊ tivesse apertado o botão
    # naquela aba. Quem abria depois — ou de outro computador — não via nada. Agora
    # o estado vem do banco junto com a página, e a tela já abre acompanhando.
    from . import tarefas
    andando = {"rodando": False}
    ultima = None
    try:
        estado = tarefas.estado()
        detalhe = estado.get("detalhe") or {}
        andando = {"rodando": bool(estado.get("rodando")),
                   "etapa": detalhe.get("etapa") or "",
                   "progresso": detalhe.get("progresso") or "",
                   "interrompida": bool(estado.get("interrompida"))}
        # ⚠️ A ÚLTIMA TENTATIVA, COM O ERRO DENTRO. É a resposta para a reclamação
        # que ele já fez três vezes: *"clico em trazer o ponto, sistema diz que vai
        # trazer e NÃO TRAZ nada. Não sei se conseguiu conectar, ninguém sabe de
        # nada."* O registro sempre existiu; faltava a tela mostrar.
        ultima = tarefas.ultima_do_tipo("ponto")
    except Exception:  # noqa: BLE001 — é informação de apoio
        logger.exception("Folha: não consegui ler o andamento")

    from .horario import agora
    hoje = agora().date()
    return render_template(
        "analisesps_folha_ponto.html", aba="folha", subaba="ponto",
        grupos=subtelas_agrupadas(), pronto=pronto, cargas=cargas,
        vista=vista, pessoas=filtradas, total_de_pessoas=len(pessoas), lista=lista,
        fila_do_ponto=_fila_do_ponto_recente(),
        erro=erro, configurado=_ponto.configurado(),
        andando=andando, ultima=ultima,
        ano_padrao=hoje.year, mes_padrao=hoje.month,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/api/folha/ponto", methods=["POST"])
@exige_operador
def folha_ponto_carregar():
    """Dispara a carga do ponto do mês escolhido.

    A competência vai para o banco antes de disparar: o trabalho roda no processo
    separado, que pode ser reiniciado, e passar por parâmetro perderia a escolha
    numa retomada."""
    from . import ponto as _ponto, sincronizacao, tarefas
    from .db import conexao

    dados = request.get_json(silent=True) or {}
    try:
        ano = int(dados.get("ano") or 0)
        mes = int(dados.get("mes") or 0)
    except (TypeError, ValueError):
        ano = mes = 0
    if not (2000 <= ano <= 2100) or not (1 <= mes <= 12):
        return {"ok": False, "erro": "Selecione o mês e o ano."}, 400
    if not _ponto.configurado():
        return {"ok": False, "erro":
                "Credenciais do Mobponto não configuradas. Crie "
                "MOBPONTO_AUTHORIZATION e MOBPONTO_API_KEY no Render — os "
                "valores constam nos Apps Script das planilhas do ponto."}, 400

    with conexao() as conn:
        sincronizacao._meta_gravar(conn, "ponto_competencia", f"{ano}-{mes}")

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    resultado = tarefas.disparar("ponto", disparo=quem or "ponto")
    if not resultado.get("ok"):
        return {"ok": False, "erro": resultado.get("erro")
                or "Outra tarefa está em execução. Aguarde a conclusão."}, 409
    return {"ok": True}


@bp.route("/api/folha/ponto/apagar", methods=["POST"])
@exige_operador
def folha_ponto_apagar():
    """Apaga uma carga do ponto. Seguro: é cópia do que o Mobponto tem."""
    from . import ponto as _ponto

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        apagou = _ponto.apagar(int(dados.get("id") or 0), quem=quem)
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou apagar a carga do ponto")
        return {"ok": False, "erro": f"Não foi possível excluir: {e}"}, 500
    if not apagou:
        return {"ok": False, "erro": "Carga não encontrada."}, 404
    return {"ok": True}


@bp.route("/folha/colaboradores")
@exige_consulta
def tela_colaboradores():
    """O cadastro de colaboradores, espelhado da planilha.

    POR QUE ESTA TELA EXISTE, e não é enfeite. Pedido do dono em 27/09/2026:
    ele precisa achar uma pessoa e **pular para o card dela no Pipefy**, porque
    é lá que se corrige o valor do auxílio alimentação, do transporte ou da
    gratificação. Depois de corrigir, aperta "Atualizar cadastro" e o valor novo
    aparece aqui.

    ELA NÃO EDITA NADA, de propósito. Se editasse, a próxima atualização
    apagaria a edição — o dado nasce no Pipefy e desce por automação até a
    planilha. Tela que deixa escrever o que vai ser sobrescrito é armadilha.

    A BUSCA TEM TETO (200): o cadastro tem ~3.500 pessoas, e desenhar todas de
    uma vez não ajuda ninguém e pesa na instância."""
    from . import colaboradores

    procurado = " ".join((request.args.get("q") or "").split())
    # "Mostrar quem saiu" desligado por padrão: quem foi desligado não entra em
    # pagamento novo, e a lista do dia a dia é de quem está na casa.
    incluir_desligados = request.args.get("desligados") == "1"
    # ⚠️ "SÓ QUEM ESTÁ SAINDO" — pedido do dono em 27/09/2026: *"não podemos
    # pagar (…) salário ou diárias pra quem saiu, tá saindo. Tem que ter
    # cuidados e alerta."* O alerta sem um lugar para ver a lista seria só
    # susto; este filtro é o lugar. Ele TRAZ quem saiu junto, senão a lista
    # esconderia metade do que ela existe para mostrar.
    so_saindo = request.args.get("saindo") == "1"

    # ⚠️ O FILTRO POR OBRA é pedido dele em 28/09/2026: *"na parte de
    # colaboradores, a mesma coisa, tem que ter o filtro (…) tem que ter os filtros
    # certinho, para a gente poder estar tratando esse pessoal aqui."*
    obra_filtro = " ".join((request.args.get("obra") or "").split())
    fase_filtro = " ".join((request.args.get("fase") or "").split())
    from .formatos import para_data
    admitido_de = para_data(request.args.get("de") or "")
    admitido_ate = para_data(request.args.get("ate") or "")

    cadastro = {"quando": "", "pessoas": 0, "avisos": [], "pronto": False}
    lista: list = []
    saindo = {"com_sinal": 0, "saiu": 0, "afastado": 0}
    obras_na_lista: list = []
    quadro = {"pronto": False}
    lista_de_fases: list = []
    erro = None
    try:
        cadastro = colaboradores.quando_atualizou()
        if cadastro.get("pronto"):
            quadro = colaboradores.panorama()
            lista_de_fases = colaboradores.fases()
            lista = colaboradores.buscar(
                procurado,
                so_ativos=not (incluir_desligados or so_saindo),
                so_saindo=so_saindo, fase=fase_filtro,
                admitido_de=admitido_de, admitido_ate=admitido_ate)
            # O código da obra resolvido de uma vez para a lista inteira.
            por_nome = colaboradores.codigos_das_obras()
            for ficha in lista:
                ficha["obra"] = colaboradores.resolver_obra(ficha, por_nome)
            obras_na_lista = sorted({f["obra"] for f in lista if f["obra"]})
            if obra_filtro:
                lista = [f for f in lista if f["obra"] == obra_filtro]
            saindo = colaboradores.contar_quem_esta_saindo()
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Análise de SPs: não consegui ler o cadastro")
        erro = str(e)

    return render_template(
        "analisesps_colaboradores.html", aba="folha", subaba="colaboradores",
        grupos=subtelas_agrupadas(),
        cadastro=cadastro, colaboradores=lista, procurado=procurado,
        incluir_desligados=incluir_desligados, so_saindo=so_saindo,
        saindo=saindo, erro=erro, obra_filtro=obra_filtro,
        obras_na_lista=obras_na_lista, quadro=quadro,
        lista_de_fases=lista_de_fases, fase_filtro=fase_filtro,
        de=request.args.get("de") or "", ate=request.args.get("ate") or "",
        filtrando=bool(procurado or obra_filtro or incluir_desligados
                       or so_saindo or fase_filtro or admitido_de
                       or admitido_ate),
        teto=200, no_teto=len(lista) >= 200,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/folha/calendario")
@exige_consulta
def tela_folha_calendario():
    """Feriados e férias — o que tira dias do auxílio.

    AS DUAS COISAS NUMA TELA SÓ, e é a correção que o dono fez em 27/09/2026
    sobre tela demais: são dois cadastros pequenos que servem ao mesmo cálculo.
    Separá-los daria duas entradas para quem procura a mesma resposta."""
    from . import folha_calendario as fc, sincronizacao

    pronto = fc._pronto()
    procurado = " ".join((request.args.get("q") or "").split())
    try:
        ano = int(request.args.get("ano") or 0)
    except (TypeError, ValueError):
        ano = 0

    feriados, ferias, obras = [], [], []
    erro = None
    try:
        if pronto:
            feriados = fc.listar_feriados(ano or None)
            ferias = fc.listar_ferias(procurado)
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui ler feriados e férias")
        erro = str(e)
    try:
        # A MESMA LISTA DE OBRAS do rateio e do Ratear. Uma segunda lista
        # divergiria da primeira no dia em que alguém cadastrasse obra nova.
        obras = [o["nome"] for o in
                 (sincronizacao.referencias_rateio().get("obras") or [])]
    except Exception:  # noqa: BLE001 — a lista é apoio; sem ela dá recado
        logger.exception("Folha: não consegui ler a lista de obras")

    from .horario import agora
    return render_template(
        "analisesps_folha_calendario.html", aba="folha", subaba="calendario",
        grupos=subtelas_agrupadas(), pronto=pronto, feriados=feriados,
        ferias=ferias, obras=obras, procurado=procurado, ano=ano,
        ano_padrao=agora().year, erro=erro,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/folha/auxilios")
@exige_consulta
def tela_folha_auxilio():
    """Auxílio alimentação e auxílio transporte, mês por mês.

    ⚠️ A DIFERENÇA ENTRE AS DUAS FICA ESCRITA NA TELA: a alimentação desconta
    feriado e férias; o transporte desconta férias e não feriado de um dia. É
    decisão do dono, e quem confere precisa saber qual régua está vendo."""
    from . import folha_apropriacao, folha_auxilio as fx
    from .horario import agora

    hoje = agora().date()
    tipo = (request.args.get("tipo") or fx.ALIMENTACAO).strip().lower()
    if tipo not in fx.TIPOS:
        tipo = fx.ALIMENTACAO
    # ⚠️ ATÉ O DIA 10, ABRE NO MÊS ANTERIOR — e aqui pesa duas vezes: o auxílio é
    # pago no mês SEGUINTE ao trabalhado, então nos primeiros dias de outubro o que
    # está sendo pago é setembro. Abrir em outubro mostrava lista vazia.
    padrao_ano, padrao_mes = folha_apropriacao.competencia_sugerida(hoje)
    try:
        ano = int(request.args.get("ano") or padrao_ano)
        mes = int(request.args.get("mes") or padrao_mes)
    except (TypeError, ValueError):
        ano, mes = padrao_ano, padrao_mes
    if not (2000 <= ano <= 2100) or not (1 <= mes <= 12):
        ano, mes = padrao_ano, padrao_mes

    pronto = fx._pronto()
    resultado = None
    erro = None
    try:
        if pronto:
            resultado = fx.calcular(tipo, ano, mes)
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui calcular o auxílio")
        erro = str(e)

    # ⚠️ OS FILTROS SÃO DE PEDIDO DELE, em 28/09/2026: *"era para ter uma na
    # lateral aqui, filtro (…) eu preciso às vezes tratar só uma obra"*. Desde
    # 01/10/2026 são de caixinha, no padrão das Solicitações, e quem já saiu fica
    # fora da lista sem filtro marcado (`folha_lista`).
    #
    # E eles filtram a LISTA MONTADA, não a consulta: os totais do alto continuam
    # sendo os da verba inteira.
    from . import folha_lista
    lista = folha_lista.filtrar(list((resultado or {}).get("pessoas") or []),
                                request.args, campo_da_obra="obra",
                                escondidas=folha_lista.ESCONDIDAS_NOS_AUXILIOS)

    return render_template(
        "analisesps_folha_auxilio.html", aba="folha", subaba="auxilios",
        obras_c_diarios=_obras_c_diarios(),
        grupos=subtelas_agrupadas(), pronto=pronto, resultado=resultado,
        tipo=tipo, ano=ano, mes=mes, erro=erro, pessoas=lista["pessoas"],
        lista=lista, filtrando=lista["filtrando"],
        divisao=_divisao_da_tela(resultado, tipo, fx.apropriado) if resultado else {},
        tipos=[(t, fx.ROTULO_DO_TIPO[t]) for t in fx.TIPOS],
        pagamentos=list(fx.TIPOS_DO_FECHAMENTO.items()),
        ano_padrao=hoje.year,
        pode_operar=auth.pode_operar(), pode_gerar=auth.e_mestre(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/api/folha/auxilio/fechar", methods=["POST"])
@exige_operador
def folha_auxilio_fechar():
    """Congela o auxílio do mês — o passo que faltava para o arquivo sair."""
    from . import folha_auxilio as fx

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        feito = fx.fechar(str(dados.get("tipo") or ""), int(dados.get("ano") or 0),
                          int(dados.get("mes") or 0),
                          str(dados.get("pagamento") or "fim_de_mes"), quem=quem)
    except (fx.ErroDoAuxilio, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou fechar o auxílio")
        return {"ok": False, "erro": f"Não foi possível concluir o fechamento: {e}"}, 500
    return {"ok": True, **{k: str(v) for k, v in feito.items()}}


@bp.route("/api/folha/auxilio/selecao", methods=["POST"])
@exige_operador
def folha_auxilio_selecao():
    """Salva DE UMA VEZ quem vai e quem não vai ser pago.

    ⚠️ SUBSTITUI O "GRAVAR" LINHA A LINHA, por correção do dono em 28/09/2026:
    *"fica muito dificultoso trabalhar da forma que está aqui, a gente vai
    gravando um por um (…) o que eu faço é só selecionar quem vai e quem não vai
    ser pago (…) e eu salvar como um todo, não linha a linha."*"""
    from . import folha_auxilio as fx

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    tipo = str(dados.get("tipo") or "").strip().lower()
    try:
        ano, mes = int(dados.get("ano") or 0), int(dados.get("mes") or 0)
    except (TypeError, ValueError):
        ano = mes = 0
    if tipo not in fx.TIPOS or not (1 <= mes <= 12) or not (2000 <= ano <= 2100):
        return {"ok": False, "erro": "Verba ou competência inválida."}, 400

    try:
        saida = fx.salvar_selecao(tipo, ano, mes, dados.get("decisoes") or [],
                                  quem=quem)
    except fx.ErroDoAuxilio as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou salvar a seleção do auxílio")
        return {"ok": False, "erro": f"Não foi possível salvar: {e}"}, 500
    return {"ok": True, **saida}


@bp.route("/folha/pessoa/<cpf>/ficha")
@exige_consulta
def tela_ficha_do_funcionario(cpf: str):
    """A janela do funcionário nas OUTRAS folhas (diaristas, alimentação e
    transporte): cadastro, ponto do mês dia a dia e os botões de atualizar.

    O dono, 01/10/2026: as outras folhas devem herdar o que a da contabilidade
    ganhou, e *"aquele modal que aparece as informações dele"* é parte disso. A
    janela da folha da contabilidade depende do arquivo da contabilidade (líquido,
    contracheque, apropriação); esta é o pedaço que vale para qualquer folha.
    Pessoa fora do cadastro: 404."""
    from . import colaboradores, ponto
    from .folha_rateio import so_digitos
    from .horario import agora

    hoje = agora().date()
    try:
        ano = int(request.args.get("ano") or hoje.year)
        mes = int(request.args.get("mes") or hoje.month)
    except (TypeError, ValueError):
        ano, mes = hoje.year, hoje.month
    digitos = so_digitos(cpf)
    ficha = colaboradores.por_cpf(digitos) if len(digitos) == 11 else None
    if not ficha:
        return '<div class="aviso erro">Colaborador não encontrado no cadastro.</div>', 404
    try:
        ficha["data_nascimento"] = colaboradores.nascimento_de(digitos)
        ficha["valor_diaria"] = colaboradores.valores_de_diaria([digitos]).get(digitos)
        ficha["obra_resolvida"] = colaboradores.resolver_obra(
            ficha, colaboradores.codigos_das_obras())
    except Exception:  # noqa: BLE001 — a janela abre sem estes detalhes
        logger.exception("Folha: não consegui completar a ficha")
    do_ponto = {"tem_carga": False, "dias": []}
    try:
        do_ponto = ponto.dias_da_pessoa(digitos, ano, mes)
    except Exception:  # noqa: BLE001 — o ponto é um bloco da janela
        logger.exception("Folha: não consegui ler o ponto da pessoa")

    # LANÇAR BATIDAS DAQUI (02/10/2026): o mesmo lançamento do analítico da folha,
    # pelo mês da tela. A obra sugerida é a de mais dias no ponto; sem ponto, a do
    # cadastro — quando estão na lista da C. Diários.
    import calendar
    from . import ponto_edicao
    editar = auth.pode_operar()
    obras_do_mobponto, falta = [], ""
    if editar:
        try:
            falta = ponto_edicao.o_que_falta()
            obras_do_mobponto = ponto_edicao.obras_permitidas()
        except Exception:  # noqa: BLE001
            logger.exception("Ponto: não consegui preparar o lançamento")
    contagem: dict = {}
    for d in do_ponto.get("dias") or []:
        if d.get("obra"):
            contagem[d["obra"]] = contagem.get(d["obra"], 0) + 1
    sugerida = (max(contagem, key=contagem.get) if contagem
                else (ficha.get("obra_resolvida") or "")).upper()
    ultimo = calendar.monthrange(ano, mes)[1]
    return render_template(
        "_folha_ficha.html", p=ficha, ponto=do_ponto, ano=ano, mes=mes,
        pode_operar=auth.pode_operar(), editar_ponto=editar,
        falta_para_editar=falta, obras_do_mobponto=obras_do_mobponto,
        obra_sugerida=sugerida,
        periodo_iso=(f"{ano:04d}-{mes:02d}-01", f"{ano:04d}-{mes:02d}-{ultimo:02d}"))


@bp.route("/folha/pessoa/<cpf>/cadastro")
@exige_consulta
def tela_ficha_cadastro_completo(cpf: str):
    """A ficha inteira da planilha de cadastro, para as outras folhas. Só de
    quem está no cadastro; fora dele, 404."""
    from . import colaboradores
    from .folha_rateio import so_digitos
    digitos = so_digitos(cpf)
    if len(digitos) != 11 or not colaboradores.por_cpf(digitos):
        return '<div class="aviso erro">Colaborador não encontrado no cadastro.</div>', 404
    try:
        grupos = colaboradores.ficha_completa(digitos)
    except colaboradores.ErroDoCadastro as e:
        return render_template("_folha_cadastro_completo.html", grupos=[], erro=str(e))
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou ler a ficha completa")
        return render_template("_folha_cadastro_completo.html", grupos=[],
                               erro=f"Não foi possível ler a planilha de cadastro: {e}")
    return render_template("_folha_cadastro_completo.html", grupos=grupos, erro="")


@bp.route("/api/folha/pessoa/<cpf>")
@exige_consulta
def folha_ficha_da_pessoa(cpf: str):
    """A ficha de uma pessoa: cadastro, observação e o PONTO do mês.

    Pedido do dono em 28/09/2026: *"eu quero também poder visualizar o ponto do
    mês daquela pessoa. Eu clicar e visualizar o ponto da pessoa no modal"* — e de
    lá abrir o card do Pipefy."""
    from . import colaboradores, ponto
    from .horario import agora

    hoje = agora().date()
    try:
        ano = int(request.args.get("ano") or hoje.year)
        mes = int(request.args.get("mes") or hoje.month)
    except (TypeError, ValueError):
        ano, mes = hoje.year, hoje.month

    ficha = None
    try:
        ficha = colaboradores.por_cpf(cpf)
    except Exception:  # noqa: BLE001 — o modal tem de dizer o que houve
        logger.exception("Folha: não consegui ler a ficha da pessoa")
    if not ficha:
        return {"ok": False, "erro": "Colaborador não encontrado no cadastro."}, 404

    do_ponto = {"tem_carga": False, "dias": [], "campos": []}
    try:
        do_ponto = ponto.dias_da_pessoa(cpf, ano, mes)
    except Exception:  # noqa: BLE001 — o ponto é um bloco do modal, não o modal
        logger.exception("Folha: não consegui ler o ponto da pessoa")

    return {"ok": True, "pessoa": {
        "cpf": ficha.get("cpf", ""), "cpf_bonito": ficha.get("cpf_bonito", ""),
        "nome": ficha.get("nome", ""), "cargo": ficha.get("cargo", ""),
        "matricula": ficha.get("matricula", ""),
        "obra_codigo": colaboradores.resolver_obra(
            ficha, colaboradores.codigos_das_obras()),
        "obra_nome": ficha.get("obra_cadastro", ""),
        "fase": ficha.get("fase", ""),
        "tipo_contrato": ficha.get("tipo_contrato", ""),
        "convencao": ficha.get("convencao", ""),
        "modo_alimentacao": ficha.get("modo_alimentacao", ""),
        "modo_transporte": ficha.get("modo_transporte", ""),
        "valor_alimentacao": (None if ficha.get("valor_alimentacao") is None
                              else float(ficha["valor_alimentacao"])),
        "valor_transporte": (None if ficha.get("valor_transporte") is None
                             else float(ficha["valor_transporte"])),
        "observacao": ficha.get("observacao_auxilio", ""),
        "situacao": ficha.get("situacao", ""), "motivo": ficha.get("motivo", ""),
        "link_pipefy": ficha.get("link_pipefy", ""),
    }, "ponto": {
        "tem_carga": bool(do_ponto.get("tem_carga")),
        "competencia": f"{int(mes):02d}/{int(ano)}",
        "campos": do_ponto.get("campos") or [],
        "dias": [{"data": (d["data"].isoformat() if d.get("data") else ""),
                  "matricula": d.get("matricula", ""),
                  "campos": d.get("campos") or {}}
                 for d in (do_ponto.get("dias") or [])],
    }}


@bp.route("/api/folha/auxilio/extras", methods=["POST"])
@exige_operador
def folha_auxilio_extras():
    """O valor acrescentado e o desconto das ausências, por pessoa (ou de vários
    de uma vez, no "aplicar todos"). Dono, 03/10/2026."""
    from . import folha_auxilio as fx

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    tipo = str(dados.get("tipo") or "").strip().lower()
    try:
        ano, mes = int(dados.get("ano") or 0), int(dados.get("mes") or 0)
    except (TypeError, ValueError):
        ano = mes = 0
    if tipo not in fx.TIPOS or not (1 <= mes <= 12) or not (2000 <= ano <= 2100):
        return {"ok": False, "erro": "Verba ou competência inválida."}, 400
    mudancas = {k: dados[k] for k in ("valor_extra", "motivo_extra",
                                      "desconto_ausencias") if k in dados}
    try:
        n = fx.gravar_extras(tipo, ano, mes, dados.get("cpfs") or dados.get("cpf") or [],
                             quem=quem, **mudancas)
    except fx.ErroDoAuxilio as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou gravar o ajuste do auxílio")
        return {"ok": False, "erro": f"Não foi possível salvar: {e}"}, 500
    return {"ok": True, "gravados": n}


@bp.route("/api/folha/auxilio/ajuste", methods=["POST"])
@exige_operador
def folha_auxilio_ajustar():
    """Guarda o que ele mexeu numa pessoa: pagar ou não, dias e obra.

    ⚠️ É O QUE FAZ O AJUSTE SOBREVIVER ao recálculo. Sem isto, cada visita à tela
    apagaria o que ele decidiu, e ele refaria tudo todo mês."""
    from . import folha_auxilio as fx

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    tipo = str(dados.get("tipo") or "").strip().lower()
    try:
        ano, mes = int(dados.get("ano") or 0), int(dados.get("mes") or 0)
    except (TypeError, ValueError):
        ano = mes = 0
    if tipo not in fx.TIPOS or not (1 <= mes <= 12) or not (2000 <= ano <= 2100):
        return {"ok": False, "erro": "Verba ou competência inválida."}, 400

    # `pagar` vem como true / false / null. NULO é "não mexi": diferente de
    # false, que é "decidi não pagar".
    pagar = dados.get("pagar")
    if pagar not in (True, False, None):
        pagar = None

    try:
        if (pagar is None and dados.get("dias") in (None, "", 0)
                and not str(dados.get("obra") or "").strip()
                and not str(dados.get("observacao") or "").strip()):
            # Nada mexido: tira o ajuste em vez de guardar um vazio, para a pessoa
            # voltar a seguir o cálculo.
            fx.limpar_ajuste(tipo, ano, mes, str(dados.get("cpf") or ""))
        else:
            fx.gravar_ajuste(
                tipo, ano, mes, str(dados.get("cpf") or ""), pagar=pagar,
                dias=dados.get("dias") or None,
                obra=str(dados.get("obra") or ""),
                observacao=str(dados.get("observacao") or ""), quem=quem)
    except fx.ErroDoAuxilio as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou gravar o ajuste do auxílio")
        return {"ok": False, "erro": f"Não foi possível gravar: {e}"}, 500
    return {"ok": True}


@bp.route("/folha/diaristas")
@exige_consulta
def tela_folha_diaristas():
    """Os diaristas do período: quem, quantos dias, quanto — e o pagamento.

    *"E cadê os diaristas? Não entrou diaristas."* (dono, 28/09/2026). Desde
    01/10/2026 a tela calcula o valor (a regra da aba "Diaristas" da planilha),
    esconde quem já saiu, filtra por caixinha, guarda quem vai receber e fecha a
    diária para o arquivo sair — o mesmo caminho da folha da contabilidade."""
    from . import folha_diaristas as fd, folha_lista
    from .horario import agora

    hoje = agora().date()
    # ⚠️ DIARISTA É PAGO POR QUINZENA (02/10/2026). Até o dia 10 a tela abre na
    # 2ª quinzena do mês anterior; depois, na 1ª do mês corrente.
    padrao_ano, padrao_mes, padrao_periodo = fd.periodo_sugerido(hoje)
    try:
        ano = int(request.args.get("ano") or padrao_ano)
        mes = int(request.args.get("mes") or padrao_mes)
    except (TypeError, ValueError):
        ano, mes = padrao_ano, padrao_mes
    if not (2000 <= ano <= 2100) or not (1 <= mes <= 12):
        ano, mes = padrao_ano, padrao_mes
    qual = request.args.get("periodo") or padrao_periodo
    if qual not in fd.PERIODOS:
        qual = padrao_periodo

    resultado = {"tem_ponto": False, "pessoas": [], "sem_cadastro": []}
    erro = None
    try:
        resultado = fd.calcular(ano, mes, qual)
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui calcular os diaristas")
        erro = str(e)
    lista = folha_lista.filtrar(resultado.get("pessoas") or [], request.args,
                                escondidas=folha_lista.ESCONDIDAS_NOS_DIARISTAS)
    divisao = (_divisao_da_tela(resultado, "diaria", fd.apropriado)
               if resultado.get("tem_ponto") else {})

    return render_template(
        "analisesps_folha_diaristas.html", aba="folha", subaba="diaristas",
        grupos=subtelas_agrupadas(), r=resultado, erro=erro, lista=lista,
        pessoas=lista["pessoas"], ano=ano, mes=mes, qual=qual, divisao=divisao,
        periodos=list(fd.PERIODOS.items()), ano_padrao=hoje.year,
        pode_operar=auth.pode_operar(), pode_gerar=auth.e_mestre(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/api/folha/diaristas/selecao", methods=["POST"])
@exige_operador
def folha_diaristas_selecao():
    """Salva de uma vez quem vai e quem não vai receber a diária."""
    from . import folha_auxilio as fx, folha_diaristas as fd

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        feito = fd.salvar_selecao(int(dados.get("ano") or 0),
                                  int(dados.get("mes") or 0),
                                  str(dados.get("periodo") or "quinzena"),
                                  dados.get("decisoes") or [], quem=quem)
    except (fx.ErroDoAuxilio, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou salvar a seleção dos diaristas")
        return {"ok": False, "erro": f"Não foi possível salvar: {e}"}, 500
    return {"ok": True, **feito}


@bp.route("/api/folha/diaristas/fechar", methods=["POST"])
@exige_operador
def folha_diaristas_fechar():
    """Congela a diária do período — o passo antes de gerar o arquivo."""
    from . import folha_apropriacao_guardada as guardada, folha_diaristas as fd

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        feito = fd.fechar(int(dados.get("ano") or 0), int(dados.get("mes") or 0),
                          str(dados.get("periodo") or "quinzena"), quem=quem)
    except (guardada.ErroDaApropriacao, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou fechar a diária")
        return {"ok": False, "erro": f"Não foi possível concluir o fechamento: {e}"}, 500
    return {"ok": True, **{k: str(v) for k, v in feito.items()}}


@bp.route("/folha/pagamento")
@exige_operador
def tela_folha_pagamento():
    """Os arquivos gerados (por geração), excluir e lançar no Pipefy.

    Os arquivos nascem no botão "Gerar arquivos" de cada folha; o "Gerar por
    competência" que ficava aqui saiu em 03/10/2026.

    ⚠️ SÓ DO MESTRE (`auth.SO_DO_MESTRE`): o log mostra o link de arquivos com
    nome, CPF e valor de ~500 pessoas."""
    from . import folha_pagamento as fpg

    pronto = fpg._pronto()
    registro, erro = [], None
    try:
        registro = fpg.rodadas(teto=400)
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Folha: não consegui montar a tela de pagamento")
        erro = str(e)

    return render_template(
        "analisesps_folha_pagamento.html", aba="folha", subaba="pagamento",
        grupos=subtelas_agrupadas(), pronto=pronto, rodadas=registro,
        destaque=request.args.get("rodada") or "",
        erro=erro,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


def _destinos_por_conta(bruto) -> dict:
    """`{conta: destino}` do pedido — o seletor de cada conta na janela de
    gerar (02/10/2026). Aceita dict ou o JSON dele (vindo da URL da prévia)."""
    import json as _json
    if isinstance(bruto, str):
        try:
            bruto = _json.loads(bruto or "{}")
        except ValueError:
            bruto = {}
    if not isinstance(bruto, dict):
        return {}
    return {" ".join(str(c or "").split()): str(d or "").strip().lower()
            for c, d in bruto.items() if str(d or "").strip()}


def _pedido_de_geracao_direta():
    from . import folha_geracao as geracao
    dados = request.get_json(silent=True) or {}
    origem = str(dados.get("origem") or "")
    destino = str(dados.get("destino") or "").strip().lower() or geracao.BEEVALE
    return dados, origem, destino


@bp.route("/api/folha/gerar-direto/resumo", methods=["POST"])
@exige_operador
def folha_gerar_direto_resumo():
    """O resumo do que vai ser gerado, a partir da folha aberta na tela. NÃO
    grava nada (02/10/2026: gerar sem sair da folha)."""
    from . import folha_geracao as geracao, folha_pagamento as fpg
    dados, origem, destino = _pedido_de_geracao_direta()
    try:
        plano = fpg.resumo_direto(origem, dados, destino,
                                  _destinos_por_conta(dados.get("destinos")))
    except (fpg.ErroDoPagamento, geracao.ErroDaGeracao, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou montar o resumo da geração")
        return {"ok": False, "erro": f"Não foi possível montar o resumo: {e}"}, 500
    return {"ok": True, "rotulo": plano["rotulo"], "competencia": plano["competencia"],
            "tipo": plano["tipo"], "destino": plano["destino"],
            "rotulo_destino": geracao.ROTULO_DO_DESTINO.get(plano["destino"], ""),
            "resumo": {"arquivos": plano["resumo"]["arquivos"],
                       "pessoas": plano["resumo"]["pessoas"],
                       "total": float(plano["resumo"]["total"]),
                       "pode_gerar": plano["resumo"]["pode_gerar"]},
            "lotes": [{"conta": l["conta"], "quantos": l["quantos"],
                       "destino": l["destino"],
                       "rotulo_destino": geracao.ROTULO_DO_DESTINO.get(l["destino"], ""),
                       "total": float(l["total"]), "criticas": l["criticas"]}
                      for l in plano["lotes"]]}


@bp.route("/api/folha/gerar-direto", methods=["POST"])
@exige_operador
def folha_gerar_direto():
    """Refaz o fechamento com a situação atual e gera os arquivos definitivos —
    de dentro da folha, sem passar pela tela de competência. ⚠️ É O PASSO QUE
    PAGA."""
    from . import folha_apropriacao_guardada as guardada
    from . import folha_auxilio as fx, folha_geracao as geracao
    from . import folha_gestao as fg, folha_pagamento as fpg
    dados, origem, destino = _pedido_de_geracao_direta()
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        saida = fpg.gerar_direto(origem, dados, destino, quem=quem,
                                 forcar=bool(dados.get("forcar")),
                                 destinos=_destinos_por_conta(dados.get("destinos")))
    except (fpg.ErroDoPagamento, geracao.ErroDaGeracao, guardada.ErroDaApropriacao,
            fx.ErroDoAuxilio, fg.ErroDaGestao, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou gerar de dentro da folha")
        return {"ok": False, "erro": f"Não foi possível gerar: {e}"}, 500
    analise = next((a["id"] for a in saida["arquivos"] if a["destino"] == fpg.ANALISE), None)
    return {"ok": True, "competencia": saida["competencia"],
            "arquivos": len(saida["arquivos"]),
            "destino_da_tela": url_for("analisesps.tela_folha_pagamento",
                                       ano=int(saida["competencia"][3:]),
                                       mes=int(saida["competencia"][:2]),
                                       rodada=analise or "")}


@bp.route("/api/folha/arquivos/excluir", methods=["POST"])
@exige_operador
def folha_arquivos_excluir():
    """Exclui do registro as gerações selecionadas e manda os arquivos para a
    lixeira do Drive. Cards do Pipefy não são apagados."""
    from . import folha_pagamento as fpg
    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        feito = fpg.excluir_arquivos(dados.get("ids") or [], quem=quem)
    except (fpg.ErroDoPagamento, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou excluir arquivos gerados")
        return {"ok": False, "erro": f"Não foi possível excluir: {e}"}, 500
    return {"ok": True, **feito}


@bp.route("/api/folha/pipe/conferir", methods=["POST"])
@exige_operador
def folha_pipe_conferir():
    """Confere que os pipes de Despesa e de SP têm todos os campos que o cenário
    do Make usa. NÃO CRIA NADA."""
    from . import folha_cards as fcd

    try:
        return {"ok": True, "pipe": fcd.conferir_pipe()}
    except fcd.ErroDosCards as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou conferir o pipe")
        return {"ok": False, "erro": f"Não foi possível ler o pipe: {e}"}, 500


@bp.route("/api/folha/card/preparar", methods=["POST"])
@exige_operador
def folha_card_preparar():
    """A prévia dos cards: o de Despesa e as SPs de cada conta, com cada valor, e
    o que impede de criar. NÃO CRIA NADA.

    Nasceu do primeiro lançamento de verdade (01/10/2026), recusado pelo Pipefy.
    Os cards seguem o cenário do Make campo a campo (`folha_cards`)."""
    import json as _json
    from . import folha_cards as fcd

    dados = request.get_json(silent=True) or {}
    try:
        vista = fcd.previa(int(dados.get("analise") or 0),
                           contas=dados.get("contas") or None)
        vista.pop("andamento", None)
        # Dinheiro vira texto: o JSON não tem decimal.
        return {"ok": True, **_json.loads(_json.dumps(vista, default=str))}
    except (fcd.ErroDosCards, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou montar a prévia dos cards")
        return {"ok": False, "erro": f"Não foi possível gerar a prévia: {e}"}, 500


@bp.route("/api/folha/card", methods=["POST"])
@exige_operador
def folha_card_lancar():
    """Cria o card de Despesa e as SPs de cada conta. ⚠️ SEM VOLTA.

    Passo separado de propósito (decisão do dono em 26/09/2026): gerar o arquivo
    não cria card, para conferir sem sujar nada lá fora."""
    from . import folha_cards as fcd

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        return fcd.lancar(int(dados.get("analise") or 0), quem=quem,
                          contas=dados.get("contas") or None)
    except (fcd.ErroDosCards, ValueError, TypeError) as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou lançar o card")
        return {"ok": False, "erro": f"Não foi possível lançar: {e}"}, 500


@bp.route("/api/folha/feriado", methods=["POST"])
@exige_operador
def folha_feriado_gravar():
    """Cadastra um feriado, nacional ou de uma obra."""
    from . import folha_calendario as fc

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        novo = fc.gravar_feriado(
            dados.get("data"), str(dados.get("abrangencia") or ""),
            obra=str(dados.get("obra") or ""),
            descricao=str(dados.get("descricao") or ""), quem=quem)
    except fc.ErroDoCalendario as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou gravar o feriado")
        return {"ok": False, "erro": f"Não foi possível gravar: {e}"}, 500
    return {"ok": True, "id": novo}


@bp.route("/api/folha/feriado/apagar", methods=["POST"])
@exige_operador
def folha_feriado_apagar():
    from . import folha_calendario as fc

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    if not fc.apagar_feriado(int(dados.get("id") or 0), quem=quem):
        return {"ok": False, "erro": "Feriado não encontrado."}, 404
    return {"ok": True}


@bp.route("/api/folha/ferias", methods=["POST"])
@exige_operador
def folha_ferias_gravar():
    """Cadastra o período de férias de uma pessoa."""
    from . import folha_calendario as fc

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        novo = fc.gravar_ferias(
            str(dados.get("cpf") or ""), dados.get("inicio"), dados.get("fim"),
            nome=str(dados.get("nome") or ""),
            observacao=str(dados.get("observacao") or ""), quem=quem)
    except fc.ErroDoCalendario as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou gravar as férias")
        return {"ok": False, "erro": f"Não foi possível gravar: {e}"}, 500
    return {"ok": True, "id": novo}


@bp.route("/api/folha/ferias/apagar", methods=["POST"])
@exige_operador
def folha_ferias_apagar():
    from . import folha_calendario as fc

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    if not fc.apagar_ferias(int(dados.get("id") or 0), quem=quem):
        return {"ok": False, "erro": "Período de férias não encontrado."}, 404
    return {"ok": True}


@bp.route("/api/folha/procurar-pessoa")
@exige_consulta
def folha_procurar_pessoa():
    """Procura no cadastro, para a tela oferecer a pessoa ao lançar férias.

    Pedido do dono: *"eu posso buscar pelo nome, pelo CPF e incluo o período."*
    Devolve pouco de propósito — a caixa de sugestão não é lugar de mostrar
    salário nem auxílio."""
    from . import colaboradores

    procurado = " ".join((request.args.get("q") or "").split())
    if len(procurado) < 2:
        return {"ok": True, "pessoas": []}
    try:
        achados = colaboradores.buscar(procurado, so_ativos=False, teto=12)
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou procurar pessoa")
        return {"ok": False, "erro": str(e)}, 500
    return {"ok": True, "pessoas": [
        {"cpf": p["cpf"], "nome": p["nome"], "cargo": p.get("cargo") or "",
         "desligado": bool(p.get("desligado"))} for p in achados]}


@bp.route("/api/folha/rateio/colar", methods=["POST"])
@exige_operador
def folha_rateio_colar():
    """Grava de uma vez a tabela colada. Uma linha por pessoa.

    ⚠️ ISTO SUBSTITUI O QUE ESTÁ VALENDO (desativando, não apagando). Pedido do
    dono em 27/09/2026 — o rateio muda todo mês, e preencher campo por campo
    para dez pessoas em dez obras seriam cem campos."""
    from . import folha_rateio as fr

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    substituir = dados.get("substituir", True) is not False
    try:
        resultado = fr.aplicar_tabela(
            str(dados.get("tabela") or ""), quem=quem, substituir=substituir)
    except fr.ErroDoRateio as e:
        # A frase vai inteira para a tela: ela diz a LINHA e o que consertar.
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou aplicar a tabela de rateio")
        return {"ok": False, "erro": f"Não foi possível gravar: {e}"}, 500
    return {"ok": True, **resultado}


@bp.route("/api/folha/rateio", methods=["POST"])
@exige_operador
def folha_rateio_gravar():
    """Cria ou altera uma regra de rateio."""
    from . import folha_rateio as fr

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        regra_id = fr.gravar(dados, quem)
    except fr.ErroDoRateio as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Folha: falhou gravar a regra de rateio")
        return {"ok": False, "erro": f"Não foi possível gravar: {e}"}, 500
    return {"ok": True, "id": regra_id}


@bp.route("/api/folha/rateio/apagar", methods=["POST"])
@exige_operador
def folha_rateio_apagar():
    """Apaga uma regra. A tela recomenda DESATIVAR antes de oferecer isto."""
    from . import folha_rateio as fr

    dados = request.get_json(silent=True) or {}
    regra_id = str(dados.get("id") or "")
    if not regra_id.isdigit():
        return {"ok": False, "erro": "Informe a regra."}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        apagou = fr.apagar(int(regra_id), quem)
    except fr.ErroDoRateio as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("Folha: falhou apagar a regra de rateio")
        return {"ok": False, "erro": f"Não foi possível excluir: {e}"}, 500
    if not apagou:
        return {"ok": False, "erro": "Regra não encontrada."}
    return {"ok": True}


@bp.route("/api/folha/rateio/simular", methods=["POST"])
@exige_operador
def folha_rateio_simular():
    """Mostra a divisão ANTES de gravar — o dono vê o valor em cada obra.

    ⚠️ USA A MESMA CONTA DA GRAVAÇÃO (`distribuir`). Uma prévia calculada por
    outro caminho divergiria no primeiro arredondamento, e aí a tela prometeria
    um número e o sistema pagaria outro."""
    from . import folha_rateio as fr

    dados = request.get_json(silent=True) or {}
    try:
        partes = fr.distribuir(dados.get("valor") or 0, dados.get("obras"))
    except fr.ErroDoRateio as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "erro": f"Não foi possível calcular: {e}"}, 500
    return {"ok": True, "partes": [
        {"obra": p["obra"], "percentual": str(p["percentual"]),
         "valor": str(p["valor"])} for p in partes]}


@bp.route("/api/conciliacao/apagar-linha", methods=["POST"])
@exige_operador
def conciliacao_apagar_linha():
    """Apaga UMA linha do extrato — pedido do dono em 26/09/2026.

    ⚠️ DUAS CHAMADAS, como o desfazer: sem `confirmar`, ela só DIZ o que a linha
    é (data, valor, se estava conciliada, se tem observação) e se dá para
    apagar. A segunda executa, e exige o motivo.

    Ele pediu a confirmação junto com o pedido: *"a exclusão tem uma
    confirmação, né? Para garantir que a pessoa está fazendo uma coisa correta.
    Porque não é o certo estar excluindo linhas."*
    """
    from . import conciliacao as conc

    dados = request.get_json(silent=True) or {}
    linha_id = str(dados.get("linha_id") or "")
    if not linha_id.isdigit():
        return {"ok": False, "erro": "Diga qual linha."}

    if not dados.get("confirmar"):
        return {"ok": True, "so_contei": True,
                **conc.o_que_apagar_a_linha_leva(int(linha_id))}

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        feito = conc.apagar_linha(int(linha_id), dados.get("motivo") or "", quem)
    except conc.ErroDaConciliacao as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Conciliação: falhou apagar a linha")
        return {"ok": False, "erro": f"Não consegui apagar: {e}"}, 500
    return {"ok": True, "so_contei": False, **feito}


@bp.route("/api/conciliacao/soltar-presas", methods=["POST"])
@exige_operador
def conciliacao_soltar_presas():
    """Solta as linhas da planilha que ficaram presas com um FITID antigo.

    ⚠️ CONSERTA ESTRAGO JÁ FEITO, e por isso existe além da migração 026: as
    linhas adotadas antes dela não sabem qual arquivo as adotou, e uma
    importação desfeita deixava-as com o FITID de um arquivo apagado. Presas
    assim, elas não eram reconhecidas nem adotadas — e o extrato seguinte
    criava a linha de novo. Ver `conciliacao.presas_da_planilha`.

    Soltar não perde nada: se o arquivo que adotou ainda existir, a próxima
    importação dele adota outra vez.
    """
    from . import conciliacao as conc

    dados = request.get_json(silent=True) or {}
    conta_id = str(dados.get("conta_id") or "")
    if not conta_id.isdigit():
        return {"ok": False, "erro": "Escolha a conta."}

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        feito = conc.devolver_presas(int(conta_id), quem)
    except conc.ErroDaConciliacao as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Conciliação: falhou soltar as linhas presas")
        return {"ok": False, "erro": f"Não consegui soltar: {e}"}, 500
    return {"ok": True, **feito}


@bp.route("/api/conciliacao/planilha/abas", methods=["POST"])
@exige_operador
def conciliacao_abas_da_planilha():
    """As abas da planilha, para ele escolher qual trazer."""
    from . import conciliacao_planilha as cp

    dados = request.get_json(silent=True) or {}
    bruto = (dados.get("planilha") or "").strip()
    try:
        planilha_id = cp.guardar_planilha(bruto) if bruto else cp.planilha_guardada()
        if not planilha_id:
            return {"ok": False, "erro": "Cole o endereço da planilha."}
        return {"ok": True, "planilha": planilha_id, "abas": cp.abas(planilha_id)}
    except cp.ErroDaPlanilha as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        logger.exception("Conciliação: falhou listar abas")
        return {"ok": False, "erro": f"Não consegui: {e}"}, 500


@bp.route("/api/conciliacao/planilha/ler", methods=["POST"])
@exige_operador
def conciliacao_ler_aba():
    """Lê UMA aba e devolve a amostra. NÃO grava nada.

    ⚠️ A AMOSTRA É O PONTO DE CONTROLE: é onde ele vê se as colunas foram
    entendidas antes de dois anos de histórico entrarem no sistema.
    """
    from . import conciliacao_planilha as cp

    dados = request.get_json(silent=True) or {}
    planilha_id = (dados.get("planilha") or "").strip() or cp.planilha_guardada()
    try:
        lido = cp.ler_aba(planilha_id, dados.get("aba") or "")
    except cp.ErroDaPlanilha as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("Conciliação: falhou ler aba")
        return {"ok": False, "erro": f"Não consegui ler: {e}"}, 500
    return {"ok": True, **cp.amostra_para_a_tela(lido)}


@bp.route("/api/conciliacao/planilha/importar", methods=["POST"])
@exige_operador
def conciliacao_importar_aba():
    """Grava uma aba na conta escolhida. A aba é lida DE NOVO, de propósito.

    ⚠️ RELER EM VEZ DE GUARDAR o que a amostra leu: com 1 worker e 4 threads,
    um "guardado na memória" entre duas chamadas vira do outro usuário no dia
    em que duas pessoas importarem ao mesmo tempo. Reler custa uma ida ao
    Google e não tem esse risco.
    """
    from . import conciliacao as conc
    from . import conciliacao_planilha as cp

    dados = request.get_json(silent=True) or {}
    conta_id = str(dados.get("conta_id") or "")
    if not conta_id.isdigit():
        return {"ok": False, "erro": "Escolha a conta desta aba."}
    planilha_id = (dados.get("planilha") or "").strip() or cp.planilha_guardada()
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        lido = cp.ler_aba(planilha_id, dados.get("aba") or "")
        feito = conc.importar_da_planilha(int(conta_id), lido, quem)
    except cp.ErroDaPlanilha as e:
        return {"ok": False, "erro": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("Conciliação: falhou importar aba")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500

    # A aba fica anotada na conta: é o que responde "esta conta já veio da
    # planilha?" quando ninguém lembrar mais.
    try:
        conc.gravar_conta({"id": int(conta_id),
                           **{k: v for k, v in (next(
                               (c for c in conc.contas(so_ativas=False)
                                if c["id"] == int(conta_id)), {})).items()
                              if k != "id"},
                           "aba_planilha": lido.get("aba", "")}, quem)
    except Exception:  # noqa: BLE001 — anotar a aba é enfeite, não a entrega
        logger.exception("Conciliação: não consegui anotar a aba na conta")
    return {"ok": True, **feito}


@bp.route("/api/conciliacao/linha", methods=["POST"])
@exige_operador
def conciliacao_linha_a_mao():
    """Uma linha que o banco não trouxe e precisa existir."""
    from . import conciliacao as conc
    from .formatos import para_data, para_numero

    dados = request.get_json(silent=True) or {}
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    data = para_data((dados.get("data") or "").strip())
    valor = para_numero(str(dados.get("valor") or "").strip())
    if not data:
        return {"ok": False, "erro": "Informe a data."}
    if valor is None:
        return {"ok": False, "erro": "Informe o valor (negativo se for saída)."}
    if not str(dados.get("conta_id") or "").isdigit():
        return {"ok": False, "erro": "Escolha a conta."}
    try:
        novo = conc.acrescentar_a_mao(
            int(dados["conta_id"]), data, dados.get("descricao") or "", valor,
            dados.get("documento") or "", dados.get("observacao") or "", quem)
    except Exception as e:  # noqa: BLE001
        logger.exception("Conciliação: falhou acrescentar linha")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500
    return {"ok": True, "id": novo}


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

    # O período da auditoria é um controle SÓ DELA, separado do filtro da
    # lista: auditar a base inteira dá o retrato de sempre; auditar um mês
    # responde "o que entrou errado neste fechamento".
    from .formatos import para_data
    campo = request.args.get("campo_data", "vencimento")
    if campo not in checagens.CAMPOS_PERIODO:
        campo = "vencimento"
    periodo = {"campo": campo,
               "de": para_data((request.args.get("de") or "").strip() or None),
               "ate": para_data((request.args.get("ate") or "").strip() or None)}

    resultado = None
    if checagem == "pontualidade":
        try:
            minimo = max(1, int(request.args.get("minimo", 5)))
        except ValueError:
            minimo = 5
        resultado = checagens.pontualidade(filtros, usar_filtros, minimo, periodo)
    elif checagem == "risco_ia":
        resultado = checagens.risco_ia(filtros, usar_filtros, periodo)
    elif checagem == "nf_duplicada":
        resultado = checagens.nf_duplicada(filtros, usar_filtros, periodo)
    elif checagem == "possivel_duplicidade":
        try:
            dias = max(0, int(request.args.get("dias", 7)))
        except ValueError:
            dias = 7
        resultado = checagens.possivel_duplicidade(filtros, usar_filtros, dias, periodo)
    elif checagem == "sem_classificacao":
        resultado = checagens.sem_classificacao(filtros, usar_filtros, periodo)
    elif checagem == "sem_integracao":
        resultado = checagens.sem_integracao_omie(filtros, usar_filtros, periodo)
    elif checagem == "codigos_barras":
        resultado = checagens.codigos_de_barras(filtros, usar_filtros, periodo)

    return render_template(
        "analisesps_auditoria.html",
        aba="auditoria", base=base,
        checagens=checagens.CHECAGENS, checagem=checagem,
        contagens=(checagens.resumo(filtros, usar_filtros, periodo)
                   if not checagem else None),
        resultado=resultado, usar_filtros=usar_filtros,
        teto=checagens.TETO, periodo=periodo,
        campos_periodo=checagens.CAMPOS_PERIODO,
        minimo=request.args.get("minimo", 5),
        dias=request.args.get("dias", 7),
        args=request.args, filtros=filtros,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


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
        elif acao == "remover_ids":
            # Veio da barra do alto: tira do lote o que estiver marcado, em
            # qualquer grupo. O painel por status embaixo mostra SPs que NÃO
            # estão no lote — marcar uma delas e mandar remover não é erro,
            # simplesmente não há o que tirar, e a tela diz isso.
            pedidos = [i.strip() for i in
                       (request.form.get("ids") or "").split(",") if i.strip()]
            conteudo = lote.ler(pessoa)["conteudo"]
            conteudo, quantos = lote.remover_ids(conteudo, pedidos)
            se_faltou = len(pedidos) - quantos
            if quantos:
                aviso = f"{quantos} SP(s) saíram do lote."
                if se_faltou:
                    aviso += (f" Outra(s) {se_faltou} já não estavam nele — "
                              "provavelmente vieram do painel por status.")
            elif pedidos:
                aviso = ("Nenhuma das SPs marcadas estava no lote. As do "
                         "painel por status embaixo não fazem parte dele.")
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
        elif acao == "remover_duplicados":
            # O mesmo número em dois grupos aparece duas vezes na tela e é
            # somado duas vezes no total. Fica a PRIMEIRA aparição, como o dono
            # pediu: "mantém o registro mais superior".
            conteudo, quantos = lote.remover_duplicados(conteudo)
            aviso = (f"{quantos} repetição(ões) saíram do lote — ficou a "
                     "primeira aparição de cada SP."
                     if quantos else "Não havia nenhuma SP repetida no lote.")

        lote.salvar(conteudo, quem, pessoa)
        return redirect(url_for("analisesps.tela_lote", aviso=aviso or ""))

    guardado = lote.ler(pessoa)
    montado = lote.montar(guardado["conteudo"])

    # O "Painel por status" que ficava embaixo do lote colado, no Streamlit:
    # quatro listas por situação de agendamento, lidas da base inteira e não
    # do lote. É onde se encontra a SP que ficou para trás e que ninguém
    # colou em lote nenhum.
    # No Streamlit estas quatro listas vinham INTEIRAS ("sem teto: exibe todos
    # os registros"), o que lá custava só memória do PC. Aqui cada linha vira
    # HTML que atravessa a internet: com 200 por status a página do Lote
    # passava de 1 MB. Vinte de cada, por vencimento mais próximo, respondem
    # "o que está prestes a vencer e ainda não foi tratado" — que é a pergunta
    # que o painel existe para responder. O resto está a um clique, nas
    # Solicitações já filtradas.
    #
    # AS QUATRO LISTAS SAEM DE UMA VARREDURA SÓ. Eram oito consultas — uma
    # lista e um resumo por status —, e cada uma percorria as 59 mil SPs
    # inteiras: 185 dos 200 ms desta tela eram isto, medido. O porquê e o como
    # estão em `consultas.painel_por_agendamento`.
    NO_PAINEL = 20
    try:
        painel = consultas.painel_por_agendamento(
            ["Agendar", "Agendado", "Falha Agendar", "Verificar"], NO_PAINEL)
    except Exception:  # noqa: BLE001 — o painel é um extra; o lote é o principal
        logger.exception("Análise de SPs: falhou montar o painel por status")
        painel = []

    # O lote de quando ele era de todo mundo. Só aparece para quem ainda não
    # tem lote próprio — depois de começar o seu, ninguém quer ser lembrado.
    antes = lote.lote_de_antes() if not guardado["conteudo"].strip() else None
    if antes and not (antes.get("conteudo") or "").strip():
        antes = None

    # Quem chega com o lote vazio e é nome NOVO por aqui provavelmente
    # digitou o nome diferente da última vez — "Marcelo" ontem, "Marcelo
    # Leitão" hoje. Sem este aviso, ele conclui que o sistema perdeu o
    # trabalho dele.
    outras = []
    if not guardado["conteudo"].strip() and lote.por_pessoa():
        conhecidas = preferencias.pessoas_conhecidas()
        outras = [p for p in conhecidas if p["chave"] != pessoa]
        if len(outras) == len(conhecidas) and not conhecidas:
            outras = []

    return render_template(
        "analisesps_lote.html",
        aba="lote", base=base, lote=guardado, montado=montado, antes=antes,
        outras_pessoas=outras,
        # Quantas cópias sobrando há. O botão de remover duplicados só aparece
        # quando existe o que remover — botão que não faz nada quando apertado
        # é pior do que botão nenhum.
        duplicados=lote.contar_duplicados(guardado["conteudo"]),
        colunas=_colunas_da_pessoa(), todas_colunas=_TODAS_COLUNAS(),
        painel=painel,
        aviso=request.args.get("aviso") or None,
        pode_operar=auth.pode_operar(), nome=auth.nome_atual(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""))


# ---------------------------------------------------------------------------
# CÓDIGOS DE PAGAMENTO — QR Pix e código de barras
# ---------------------------------------------------------------------------
def _codigo_de_pagamento(sp_id: str, registro) -> dict:
    """O QR Pix ou o código de barras de UMA SP.

    Vive fora das rotas porque duas telas o mostram: a de códigos, que monta
    até cinquenta de uma vez, e a ficha, que mostra o da SP aberta. Duas
    cópias divergiriam no dia em que uma delas ganhasse um caso — e a que
    ficasse para trás mostraria um código errado a quem está pagando.

    Nunca levanta: uma SP com dado ruim vira um bloco com o erro escrito, e as
    outras continuam aparecendo."""
    from . import pagamentos

    if registro is None:
        return {"id": sp_id, "sp": None, "tipo": None, "imagem": None,
                "copia_cola": None, "erro": "SP não encontrada na base."}

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
                # O gerador devolve um SVG de ARQUIVO, com cabeçalho XML e
                # DOCTYPE próprios. Colado dentro de uma página HTML isso é
                # inválido — e alguns navegadores param de desenhar o resto a
                # partir dali. Fica só o `<svg>` para dentro.
                inicio = svg.find("<svg")
                bloco["imagem"] = svg[inicio:] if inicio >= 0 else svg
                bloco["copia_cola"] = str(
                    registro.get("codigo_barras") or "").strip()
        elif "pix" in forma or "beevale" in forma:
            bloco["tipo"] = "pix"
            # Passa pela MESMA classificação do alerta laranja: é ela que tira
            # o rótulo ("Chave Pix: ...") e diz se o que veio é uma chave ou um
            # "copia e cola" pronto. Ler o texto cru aqui foi o que pôs o
            # rótulo dentro do QR.
            info = pagamentos.classificar(forma, registro.get("info_pgt"))
            png, carga = pagamentos.gerar_pix(
                info["chave"], float(registro.get("valor_num") or 0),
                str(registro.get("credor") or ""),
                copia_cola=(info["subtipo"] == "copia_cola"))
            import base64
            bloco["imagem"] = base64.b64encode(png).decode("ascii")
            bloco["copia_cola"] = carga
        else:
            bloco["erro"] = (f"Forma de pagamento \"{forma or '—'}\" não "
                             "gera QR nem código de barras.")
    except Exception as e:  # noqa: BLE001 — uma SP ruim não some com as outras
        logger.exception("Análise de SPs: falhou montar o código da SP %s", sp_id)
        bloco["erro"] = str(e)

    return bloco


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

    blocos = [_codigo_de_pagamento(sp_id, consultas.uma(sp_id))
              for sp_id in pedidos]

    origem = _origem_pedida()
    return render_template("analisesps_codigos.html",
                           aba="lote" if origem == "lote" else "solicitacoes",
                           blocos=blocos, origem=origem,
                           pode_operar=auth.pode_operar(),
                           perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


# ---------------------------------------------------------------------------
# BEEVALE — o cartão de benefício dos terceirizados
#
# São DUAS telas, e a diferença entre elas é o que decide o cuidado de cada uma:
#
#   /beevale/cadastro  cola-se a lista e sai um arquivo para baixar. Não
#                      escreve em lugar nenhum. Errou, gera de novo.
#   /beevale/gerar     mostra o que VAI acontecer; o botão dispara a subida no
#                      Drive e a escrita nos cards do Pipefy. SEM DESFAZER.
#
# A segunda é a única coisa neste módulo que altera o Pipefy. Por isso ela é
# POST separado do GET: recarregar a página não pode refazer a operação.
# ---------------------------------------------------------------------------
@bp.route("/beevale/cadastro", methods=["GET", "POST"])
@exige_operador
def beevale_cadastro():
    """Cola-se e-mails ou CPFs; sai a planilha de cadastro para o portal."""
    from . import beevale

    texto = ""
    encontrados: list = []
    nao_achados: list = []
    erro = None

    if request.method == "POST":
        texto = request.form.get("texto", "")
        cpfs = beevale.extrair_cpfs(texto)
        if not cpfs:
            erro = ("Não achei nenhum CPF no que você colou. Vale a lista de "
                    "e-mails que o portal do BeeVale devolve, ou os CPFs "
                    "soltos com 11 dígitos.")
        else:
            try:
                encontrados, nao_achados = beevale.buscar_por_cpf(cpfs)
            except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
                logger.exception("Análise de SPs: falhou o cadastro BeeVale")
                erro = str(e)

    if encontrados and request.form.get("acao") == "baixar":
        from .horario import agora
        conteudo = beevale.cadastro_xlsx(encontrados)
        nome = f"Cadastro_BeeVale_{agora().strftime('%d.%m.%Y_%H.%M.%S')}.xlsx"
        return Response(
            conteudo, mimetype=beevale.MIME_XLSX,
            headers={"Content-Disposition": f'attachment; filename="{nome}"'})

    return render_template(
        "analisesps_beevale_cadastro.html", aba="solicitacoes",
        texto=texto, encontrados=encontrados, nao_achados=nao_achados,
        erro=erro, origem=_origem_pedida(),
        pasta_configurada=bool(beevale.pasta_do_drive()[0]),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual(), pode_operar=True)


@bp.route("/beevale/gerar")
@exige_operador
def beevale_gerar():
    """A CONFERÊNCIA, antes de qualquer coisa sem volta.

    Só lê: mostra o que cada card tem, o que está impedido, e se a pasta do
    Drive está configurada. Nada acontece até o operador apertar o botão, que
    é um POST para a rota abaixo."""
    from . import beevale, consultas

    ids = [i.strip() for i in request.args.getlist("id") if i.strip()]
    if not ids:
        return render_template("analisesps_erro.html",
                               titulo="Nada selecionado",
                               mensagem="Marque as SPs BeeVale na lista e "
                                        "clique em \"Gerar BeeVale\"."), 400

    pasta, _origem = beevale.pasta_do_drive()
    preparado, erro = {"prontos": [], "erros": []}, None
    if pasta:
        try:
            preparado = beevale.preparar(ids)
        except Exception as e:  # noqa: BLE001 — Pipefy fora do ar, token errado…
            logger.exception("Análise de SPs: falhou preparar o BeeVale")
            erro = str(e)

    # As SPs como a base as conhece — para o operador conferir credor e valor
    # sem confiar só no que o Pipefy devolveu.
    registros = {i: consultas.uma(i) for i in ids}

    return render_template(
        "analisesps_beevale_gerar.html", aba="solicitacoes", ids=ids,
        pasta=pasta, preparado=preparado, erro=erro, registros=registros,
        origem=_origem_pedida(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual(), pode_operar=True)


@bp.route("/api/beevale/gerar", methods=["POST"])
@exige_operador
def beevale_executar():
    """O passo SEM DESFAZER: sobe no Drive e escreve nos cards do Pipefy."""
    from . import beevale

    dados = request.get_json(silent=True) or {}
    ids, erro = _ids_do_pedido(dados)
    if erro:
        return erro

    try:
        resultado = beevale.gerar(ids)
    except beevale.ErroDoBeeVale as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Análise de SPs: falhou gerar o BeeVale")
        return {"ok": False, "erro": f"Falhou: {e}"}, 500

    logger.info("Análise de SPs: %s gerou BeeVale de %d SP(s).",
                auth.nome_atual() or "sem nome", len(ids))
    return {"ok": True, **resultado}


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

    # O QUE ESTÁ NOS CAMPOS AGORA. Sai do formulário e volta para a tela, para
    # que apertar "Interpretar" de um lado não apague o que a pessoa já tinha
    # digitado do outro — e para que um erro na geração não zere tudo.
    def _do_formulario(prefixo):
        nomes = request.form.getlist(f"{prefixo}_nome")
        valores = request.form.getlist(f"{prefixo}_valor")
        return [{"nome": (n or "").strip(), "valor": (v or "").strip()}
                for n, v in zip(nomes, valores)]

    linhas_cc = _do_formulario("cc")
    linhas_cat = _do_formulario("cat")
    base_categoria = request.form.get("base_categoria", "")
    avisos_cc: list = []
    avisos_cat: list = []
    # O texto colado volta para a caixa: quem precisa corrigir uma linha não
    # deve ter de colar tudo de novo.
    colagem_cc = colagem_cat = ""

    if request.method == "POST":
        if not auth.pode_operar():
            return auth._sem_permissao()
        acao = request.form.get("acao", "gerar")

        # COLAR UMA TABELA DA PLANILHA, em vez de escolher trinta obras uma a
        # uma. Quem interpreta é o `rateio`, no servidor, onde há teste — e não
        # o navegador, que esta máquina não consegue exercitar. O porquê inteiro
        # está em `rateio.interpretar_colagem`.
        if acao in ("colar_cc", "colar_cat"):
            qual = "obras" if acao == "colar_cc" else "categorias"
            colado = request.form.get(f"colagem_{acao[6:]}", "")
            lido = rateio.interpretar_colagem(colado, referencias[qual])
            if acao == "colar_cc":
                linhas_cc, avisos_cc, colagem_cc = (
                    lido["linhas"], lido["avisos"], colado)
            else:
                linhas_cat, avisos_cat, colagem_cat = (
                    lido["linhas"], lido["avisos"], colado)
            if not lido["linhas"]:
                (avisos_cc if acao == "colar_cc" else avisos_cat).insert(
                    0, "Não reconheci nenhuma linha. Cole duas colunas: o nome "
                       "na primeira e o valor na segunda.")

        else:
            mapa_obra = {o["nome"]: o["codigo"] for o in referencias["obras"]}
            mapa_categoria = {c["nome"]: c["codigo"]
                              for c in referencias["categorias"]}

            def _para_o_json(linhas, mapa, campo):
                return [{campo: l["nome"], "codigo": mapa.get(l["nome"], ""),
                         "valor": rateio._to_float(l["valor"])}
                        for l in linhas if l["nome"]]

            try:
                resultado = rateio.gerar_jsons(
                    _para_o_json(linhas_cc, mapa_obra, "obra"),
                    _para_o_json(linhas_cat, mapa_categoria, "categoria"),
                    base_cat=rateio._to_float(base_categoria) or None)
            except Exception as e:  # noqa: BLE001 — o motivo tem de aparecer na tela
                logger.exception("Análise de SPs: falhou gerar o rateio")
                erro = str(e)

    # Sempre sobram linhas vazias para continuar digitando à mão.
    VAZIAS = 3
    linhas_cc = linhas_cc + [{"nome": "", "valor": ""}] * VAZIAS
    linhas_cat = linhas_cat + [{"nome": "", "valor": ""}] * VAZIAS

    return render_template(
        "analisesps_ratear.html", aba="ratear",
        referencias=referencias, resultado=resultado, erro=erro,
        linhas_cc=linhas_cc, linhas_cat=linhas_cat,
        avisos_cc=avisos_cc, avisos_cat=avisos_cat,
        colagem_cc=colagem_cc, colagem_cat=colagem_cat,
        base_categoria=base_categoria,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


# ---------------------------------------------------------------------------
# BRADESCO
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# DOCUMENTAÇÃO FISCAL — qual nota é de qual SP
#
# O objetivo, nas palavras do dono: *"eu quero minimizar a interação do humano
# (…) é muito falho o olho humano, e nós não temos esse tempo."* A tela entrega
# a análise pronta; quem executa só confere e confirma.
#
# DUAS PILHAS, e elas existem por causa de um risco real. Propor cria fadiga de
# aprovação: se vinte e oito de trinta estão sempre certas, na terceira semana
# ninguém confere mais — é o mesmo olho cansado, só que mais rápido. Por isso o
# que tem dúvida NÃO vem marcado, e é decidido um a um.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# AS AÇÕES DA DOCUMENTAÇÃO FISCAL MORAM NA TELA DELA
#
# Correção do dono em 13/09/2026, com todas as letras: *"ao buscar na Receita
# as notas emitidas contra a BWS, não tem absolutamente nada a ver eu estar com
# um botão desse fora da tela de trabalho. (…) Ler, é pra estar dentro da tela.
# Gravar nos cards, é pra estar dentro da tela. (…) Eu estou trabalhando lá,
# estou tratando lá, e vou operacionalizar por lá."*
#
# Ele está certo e o erro tinha uma causa boba: todas as tarefas longas nascem
# do mesmo lugar (`tarefas.MODOS`), e a tela de Configurações desenha a lista
# INTEIRA de modos como botões. Quem acrescenta um modo ganha um botão lá sem
# querer. As quatro que são trabalho fiscal passam a aparecer aqui, com o nome
# do que fazem — e continuam existindo em Configurações, que é onde se dispara
# carga fora do fluxo de trabalho.
#
# A ORDEM É A DO TRABALHO: primeiro trazer nota (das duas fontes), depois ler o
# que falta, depois devolver o resultado (card e planilha).
ACOES_FISCAIS = [
    {"modo": "notas_receita", "rotulo": "Buscar notas na Receita",
     "ajuda": "Baixa da Receita as NF-e e CT-e emitidas contra os CNPJs que "
              "têm certificado guardado. Continua de onde parou da última vez."},
    # ⚠️ O ÚNICO BOTÃO DESTE MÓDULO QUE ESCREVE NO SISTEMA FISCAL. A ajuda diz
    # isso com todas as letras, de propósito: quem aperta precisa saber que
    # está declarando algo em nome da empresa, e não só lendo.
    {"modo": "notas_ciencia", "rotulo": "Dar ciência e baixar as notas",
     "ajuda": "Declara na Receita, assinado com o certificado da empresa, que "
              "a BWS tomou ciência das notas emitidas contra ela — é o que "
              "libera o XML de cada nota. O documento chega na PRÓXIMA busca e "
              "é guardado no Drive. Vale só para NF-e dos últimos 90 dias, uma "
              "vez por nota."},
    # ⚠️ O BOTÃO DA ABA DO FSIST SAIU DAQUI — 16/09/2026, pedido do dono:
    # *"pra que diabo serve o botão 'Ler a aba do FSist na planilha'? Não tem
    # sentido isso. Vou importar o relatório no sistema."*
    #
    # Ele está certo, e o motivo é de fluxo: ler uma aba da planilha era o
    # caminho de antes de existir o formulário de subir o arquivo. Manter os
    # dois lado a lado obriga quem usa a escolher entre duas portas para a
    # mesma coisa — e a errada depende de alguém ter colado o relatório numa
    # aba antes. Para subir o arquivo há "Subir o relatório do FSist", nesta
    # mesma tela.
    #
    # A varredura das planilhas de apoio (que inclui aquela aba) CONTINUA
    # existindo em Configurações e na sincronização automática: o que saiu foi
    # o atalho no meio do trabalho fiscal, não a função.
    {"modo": "fiscal_ia", "rotulo": "Ler com IA os anexos escolhidos",
     "ajuda": "Só as SPs que você marcou. Cada leitura é cobrada."},
    {"modo": "fiscal", "rotulo": "Gravar no Pipefy o que foi confirmado",
     "ajuda": "Leva para o card do Pipefy a categoria e a chave de tudo que já "
              "foi confirmado aqui (o número \"Falta gravar no card\"). "
              "Enquanto não rodar, a decisão existe só aqui dentro."},
    # O DONO PERGUNTOU O QUE ERA, em 13/09/2026: *"o que é que significa
    # devolver à planilha as alterações?"* A ajuda antiga dizia "as alterações
    # feitas na tela", que não explica NADA para quem não sabe que existe uma
    # fila. Agora diz o que é a fila e por que ela existe.
    {"modo": "fila", "rotulo": "Devolver à planilha as alterações",
     "ajuda": "Nada que você altera nas telas vai direto para a planilha "
              "SPsBD: fica numa fila e sobe de uma vez, para não escrever na "
              "planilha a cada clique (é o que a deixava lenta). Este botão "
              "esvazia essa fila agora, em vez de esperar a próxima "
              "atualização do dia. Não tem nada de fiscal — vale para "
              "qualquer alteração feita em qualquer tela."},
]


@bp.route("/api/fiscal/mao", methods=["POST"])
@exige_operador
def decidir_fiscal_a_mao():
    """Grava a categoria e a chave DIGITADAS por uma pessoa.

    Existe por causa do buraco que o dono achou usando a tela em 13/09/2026:
    *"tudo aquilo que você sugeriu (…) mas o que você não sugeriu, como é que
    eu adiciono a informação? Porque a planilha ela me permite adicionar, e a
    tela não permite."* Sem isto, a tela só sabia aprovar proposta — e o
    trabalho que sobra é justamente o que não tem proposta."""
    from . import consultas, fiscal

    dados = request.get_json(silent=True) or {}
    sp_id = str(dados.get("sp") or "").strip()
    if not sp_id:
        return {"ok": False, "erro": "SP não informada."}, 400

    try:
        sp = consultas.uma(sp_id)
    except Exception:  # noqa: BLE001 — banco fora do ar
        logger.exception("Análise de SPs: falhou ler a SP %r", sp_id)
        sp = None
    if not sp:
        return {"ok": False, "erro": "SP não encontrada na base."}, 404

    quem = auth.nome_atual() or auth.pessoa_atual()
    try:
        gravado = fiscal.decidir_a_mao(
            sp_id, dados.get("documentacao"), dados.get("chave"), quem, sp)
    except fiscal.ErroDeEntrada as e:
        # RECUSA ESPERADA NÃO É FALHA: a mensagem é para a pessoa ler e
        # corrigir, não um erro de sistema.
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou gravar a decisão à mão")
        return {"ok": False, "erro": f"Não consegui gravar: {e}"}, 500

    irmas = gravado.get("parcelas_irmas") or []
    logger.info("Análise de SPs: %s marcou a SP %s como %r à mão%s.",
                quem, sp_id, gravado["documentacao"],
                f" (e mais {len(irmas)} parcela(s) da mesma nota)"
                if irmas else "")
    return {"ok": True, **gravado}


def _cnpjs_com_certificado() -> list:
    """Os CNPJs que já têm certificado guardado. Nunca derruba a tela."""
    from . import certificados
    try:
        return certificados.cnpjs_ativos()
    except Exception:  # noqa: BLE001 — migração 009 ainda não aplicada
        logger.exception("Análise de SPs: não consegui listar os certificados")
        return []


@bp.route("/notas/importar", methods=["POST"])
@exige_operador
def importar_relatorio_fsist():
    """Recebe o relatório do FSist como ARQUIVO e grava as notas.

    Reclamação do dono em 13/09/2026: *"Importar relatório FSist — e cadê a
    opção de incluir o arquivo? De onde vai tirar essa informação, se eu não
    estou nem colocando?"*

    Ele está certo: o botão lia a aba da planilha de apoio, que é o fluxo antigo
    de colar o relatório lá. Funciona, e não era o que o nome prometia. As duas
    portas ficam — colar na aba é o hábito da equipe; subir o arquivo é o
    caminho curto, e é como está o relatório antigo que ele quer trazer."""
    from . import sincronizacao

    arquivo = request.files.get("relatorio")
    if not arquivo or not arquivo.filename:
        return redirect(url_for("analisesps.tela_fiscal", visao="notas", f=1,
                                aviso="Escolha o arquivo do relatório."))
    try:
        saida = sincronizacao.importar_notas_de_arquivo(
            arquivo.read(), arquivo.filename)
    except sincronizacao.ErroDeRelatorio as e:
        # RECUSA ESPERADA NÃO É FALHA: a frase é para a pessoa ler e corrigir.
        return redirect(url_for("analisesps.tela_fiscal", visao="notas", f=1,
                                aviso=f"Não importei: {e}"))
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou importar o relatório")
        return redirect(url_for("analisesps.tela_fiscal", visao="notas", f=1,
                                aviso=f"Não consegui importar: {e}"))

    recado = (f"{saida['lidas']} nota(s) lidas do arquivo: "
              f"{saida['novas']} nova(s), {saida['atualizadas']} atualizada(s)")
    if saida["ignoradas"]:
        recado += f", {saida['ignoradas']} linha(s) sem chave ignorada(s)"
    recado += "."
    if saida["avisos"]:
        recado += " " + " ".join(saida["avisos"])
    logger.info("Análise de SPs: %s importou o relatório %r — %s",
                auth.nome_atual() or auth.pessoa_atual(), arquivo.filename,
                recado)
    return redirect(url_for("analisesps.tela_fiscal", visao="notas", f=1,
                            aviso=recado))


@bp.route("/api/fiscal/comparar")
@exige_consulta
def comparar_fiscal():
    """TUDO o que sustenta (ou derruba) a proposta de uma SP.

    Cobrança do dono em 13/09/2026: *"você sugere e eu quero ver de forma
    completa os dados do que você está sugerindo. Os dados do relatório FSist.
    Como faço? Ou quero ver os dados do registro, não dá pra ver pra validar.
    Isso pra eu ter que confiar somente no que você observou."*

    Ele está certo, e o desenho anterior era ruim: a tela mostrava a conclusão
    e escondia a prova. Numa tela cujo trabalho é achar erro, quem confere sem
    poder ver vira carimbo — e carimbo não acha nada.

    Devolve a SP INTEIRA, a nota INTEIRA (todas as candidatas, não só a
    vencedora), e a conta dos pontos regra a regra, inclusive as que NÃO
    pontuaram — são elas que explicam por que a confiança não foi maior."""
    from . import colunas, consultas, fiscal

    sp_id = (request.args.get("sp") or "").strip()
    if not sp_id:
        return {"ok": False, "erro": "SP não informada."}, 400
    try:
        sp = consultas.uma(sp_id)
    except Exception as e:  # noqa: BLE001 — banco fora do ar
        logger.exception("Análise de SPs: falhou ler a SP %r", sp_id)
        return {"ok": False, "erro": f"Não consegui ler a SP: {e}"}, 500
    if not sp:
        return {"ok": False, "erro": "SP não encontrada na base."}, 404

    try:
        comparacao = fiscal.comparar(sp)
    except Exception as e:  # noqa: BLE001 — migração ainda não aplicada
        logger.exception("Análise de SPs: falhou comparar a SP %r", sp_id)
        return {"ok": False, "erro": f"Não consegui comparar: {e}"}, 500

    # A SP INTEIRA, com o rótulo em português de cada coluna e na ordem da
    # planilha — é a mesma ordem em que ele lê a SPsBD, e ler na ordem
    # conhecida é metade da conferência.
    def texto(v):
        return "" if v is None else str(v)

    # O ENDEREÇO VAI CLICÁVEL. Pedido do dono em 13/09/2026: *"quando clicamos
    # em ver dados, das informações que vêm da planilha vêm alguns links,
    # torná-los clicáveis."* São o anexo no Dropbox e o card do Pipefy — o
    # atalho que ele mais usa para conferir, e que estava obrigando a marcar o
    # texto com o mouse e colar na barra do navegador.
    #
    # Quem monta o HTML é o `com_links` que já existe, e não um segundo
    # transformador escrito no navegador: ele já escapa o texto (a descrição
    # vem da planilha, que qualquer um edita) e já trata a pontuação colada no
    # fim do endereço. Dois lugares fazendo a mesma coisa divergem com o tempo.
    from .formatos import com_links

    comparacao["lancamento"] = [
        {"rotulo": colunas.ROTULOS.get(campo, campo),
         "valor": texto(sp.get(campo)),
         "html": com_links(texto(sp.get(campo)))}
        for campo in colunas.CHAVES if texto(sp.get(campo)).strip()
    ]
    # O PANORAMA DAS PARCELAS, DITO ANTES DO CLIQUE.
    #
    # *"Claramente é a situação de parcelas que informei. Não deveria haver uma
    # associação com as outras parcelas pra vincular logo tudo? Ou avisar que
    # já tá associado com outras?"* Gravar nas irmãs já acontecia — só que ele
    # só descobria DEPOIS. Ação que alcança mais do que se vê tem de ser
    # anunciada antes.
    #
    # A chave "em questão" é a da melhor candidata: é contra ela que se sabe se
    # uma irmã já está com ESTA nota ou com outra.
    melhor = (comparacao.get("candidatas") or [{}])[0]
    try:
        comparacao["parcelas"] = fiscal.panorama_das_parcelas(
            sp, melhor.get("chave") or "")
    except Exception:  # noqa: BLE001 — a janela abre mesmo sem isto
        logger.exception("Análise de SPs: falhou ler o panorama das parcelas")
        comparacao["parcelas"] = {}

    comparacao["ok"] = True
    return comparacao


@bp.route("/api/fiscal/reconferir", methods=["POST"])
@exige_consulta
def reconferir_fiscal():
    """Refaz a conferência das SPs escolhidas AGORA, inclusive as já decididas.

    Pergunta do dono em 13/09/2026: *"se eu quiser selecionar um determinado
    registro e reprocessar ele pra ver se está batendo (…) como é que eu sei
    que isso está sendo analisado?"*

    É `@exige_consulta` de propósito: reconferir NÃO GRAVA NADA — devolve o que
    encontrou. Quem decide continua sendo gente, e olhar não é alterar."""
    from . import consultas, fiscal

    dados = request.get_json(silent=True) or {}
    ids = [str(i).strip() for i in (dados.get("ids") or []) if str(i).strip()]
    if not ids:
        return {"ok": False, "erro": "Nenhuma SP marcada."}, 400
    # Teto por chamada: reconferir é uma busca de notas candidatas por SP, e
    # soltar a lista inteira de uma vez num banco de um décimo de núcleo é o
    # jeito conhecido de derrubar a tela de todo mundo.
    if len(ids) > 200:
        return {"ok": False,
                "erro": "Dá para reconferir até 200 por vez."}, 400

    try:
        linhas = [sp for sp in (consultas.uma(i) for i in ids) if sp]
        resultado = fiscal.resumo_da_reconferencia(fiscal.reconferir(linhas))
    except Exception as e:  # noqa: BLE001 — migração ainda não aplicada
        logger.exception("Análise de SPs: falhou reconferir")
        return {"ok": False, "erro": f"Não consegui reconferir: {e}"}, 500

    faltaram = [i for i in ids if i not in {x["sp"] for x in resultado["itens"]}]
    logger.info("Análise de SPs: %s reconferiu %d SP(s).",
                auth.nome_atual() or auth.pessoa_atual(), len(resultado["itens"]))
    return {"ok": True, "faltaram": faltaram, **resultado}


@bp.route("/api/fiscal/nota")
@exige_consulta
def conferir_nota_fiscal():
    """Diz o que se sabe de uma chave ANTES de ela ser gravada.

    Serve à digitação: colada a chave, a tela responde de quem é a nota, de
    quando e de quanto — e é assim que quem digita percebe que colou a chave
    errada, em vez de descobrir depois no card."""
    from . import fiscal
    try:
        nota = fiscal.uma_nota(request.args.get("chave", ""))
    except Exception:  # noqa: BLE001 — migração ainda não aplicada
        logger.exception("Análise de SPs: falhou conferir a chave")
        return {"ok": False, "nota": None}
    return {"ok": True, "nota": {
        "chave": nota.get("chave"), "numero": nota.get("numero"),
        "emitente": nota.get("emitente"), "status": nota.get("status"),
        "valor": float(nota["valor"]) if nota.get("valor") is not None else None,
        "emissao": nota["emissao"].isoformat() if nota.get("emissao") else None,
    } if nota else None}


@bp.route("/fiscal")
@exige_consulta
def tela_fiscal():
    from . import consultas, fiscal, tarefas

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    # O FILTRO DESTA TELA VOLTA COMO FOI DEIXADO. Reclamação do dono em
    # 13/09/2026: *"eu saio e volto e o filtro que eu estou trabalhando eles
    # somem. Eu vou pra configurações pra fazer alguma coisa, aí volto pra cá e
    # o filtro some."* Ele guarda numa gaveta PRÓPRIA — ver
    # `preferencias.FILTRO_FISCAL`.
    voltar = _lembrar_filtro("analisesps.tela_fiscal",
                             preferencias.FILTRO_FISCAL)
    if voltar is not None:
        return voltar

    filtros = _filtros_do_pedido()
    # ⚠️ O ESCOPO DESTA TELA, e não um filtro a mais. Pedido do dono em
    # 13/09/2026: fora fica o que venceu e foi pago antes de 2026, o que está
    # com Status Pgt "Cancelado" (salvo se ele pedir) e tudo que tem "(TRF)"
    # no tipo de despesa, que é transferência entre contas e não gera nota.
    #
    # Vai no dicionário ANTES de qualquer consulta, porque a lista, o resumo e
    # o painel leem o mesmo dicionário — é isso que impede o painel de contar
    # trabalho que a lista não mostra.
    filtros["escopo_fiscal"] = True
    filtros["mostrar_canceladas"] = request.args.get("canceladas") == "1"
    # O CLIQUE NO QUADRO POR CATEGORIA. Vem do endereço e entra como
    # parâmetro; `sem_categoria` é a pilha do "(sem informação)".
    filtros["categoria"] = request.args.getlist("categoria")
    filtros["sem_categoria"] = request.args.get("sem_categoria") == "1"
    try:
        pagina = max(1, int(request.args.get("pagina", 1)))
    except ValueError:
        pagina = 1
    grupo = request.args.get("grupo") or ""

    # AS VISÕES DE VER, dentro da Documentação Fiscal. Ver `_planilha_fiscal`.
    if request.args.get("visao") in ("dados_sps", "dados_notas"):
        return _planilha_fiscal(base, pagina)

    # A SEGUNDA VISÃO — nota → lançamento. É ela que fecha com a contabilidade:
    # *"se tem uma nota emitida, tem uma despesa para estar associada"*. Nota
    # órfã é problema fiscal, e hoje ninguém a enxerga.
    if request.args.get("visao") == "notas":
        from . import sefaz

        # OS RECORTES DESTA VISÃO SÃO OUTROS: aqui a linha é a NOTA, e os
        # recortes dos lançamentos (obra, tipo de despesa) não se aplicam.
        filtros_nota = {
            "recortes": [v for v in request.args.getlist("nota")
                         if v in fiscal.RECORTES_DE_NOTA],
            "busca": request.args.get("busca_nota", "").strip(),
            "emissao_ini": request.args.get("emissao_ini") or None,
            "emissao_fim": request.args.get("emissao_fim") or None,
        }
        try:
            notas, resumo_notas = fiscal.listar_notas(filtros_nota, pagina)
            # AS SPs CANDIDATAS SÓ DAS ÓRFÃS, e numa busca só para a página.
            # Era uma consulta POR NOTA, e com 200 notas na tela isso virava
            # 200 varreduras da base — 28 segundos medidos aqui, e em produção
            # a tela não abria.
            orfas = [n for n in notas if n.get("orfa")]
            candidatas = fiscal.sps_possiveis_das_notas(orfas)
            # O DOCUMENTO DA NOTA e a ciência, numa consulta só para a página.
            # Uma por nota seriam 200 idas ao banco — a mesma lição que já
            # custou 28 segundos de tela nesta mesma lista.
            from . import notas_arquivo
            chaves_da_pagina = [n.get("chave") for n in notas]
            arquivos_da_nota = notas_arquivo.arquivos_das_notas(chaves_da_pagina)
            ciencia_da_nota = notas_arquivo.ciencia_das_notas(chaves_da_pagina)
            for nota in notas:
                todas = candidatas.get(
                    fiscal.so_digitos(nota.get("chave")), [])
                # DUAS LISTAS, e não uma. A SP que já aponta para outra nota
                # não é sugestão nenhuma — ela aparece à parte, contada e
                # clicável, para dar onde conferir sem virar proposta.
                nota["candidatas"] = [c for c in todas
                                      if not c.get("ja_tem_nota")]
                nota["ja_com_nota"] = [c for c in todas
                                       if c.get("ja_tem_nota")]
            erro = None
        except Exception as e:  # noqa: BLE001 — migração 005 ainda não aplicada
            logger.exception("Análise de SPs: falhou listar as notas")
            arquivos_da_nota, ciencia_da_nota = {}, {}
            notas, resumo_notas, erro = [], {"quantidade": 0, "total": 0,
                                             "sem_lancamento": 0}, (
                "Esta tela precisa da atualização do banco. Vá em "
                f"Configurações e aperte \"Aplicar atualizações do banco\". "
                f"(detalhe: {e})")
        try:
            painel_notas = fiscal.painel_notas()
        except Exception:  # noqa: BLE001 — migração ainda não aplicada
            painel_notas = {}
        try:
            por_dia = fiscal.notas_por_dia()
        except Exception:  # noqa: BLE001
            por_dia = []

        total = resumo_notas["quantidade"]
        ultima = (pagina - 1) * fiscal.NOTAS_POR_PAGINA + len(notas)
        return render_template(
            "analisesps_fiscal_notas.html", aba="fiscal", base=base,
            notas=notas, total=total, erro=erro, pagina=pagina,
            resumo_notas=resumo_notas, por_dia=por_dia,
            arquivos_da_nota=arquivos_da_nota, ciencia_da_nota=ciencia_da_nota,
            filtros_nota=filtros_nota,
            grupos_de_nota=fiscal.GRUPOS_DE_NOTA,
            frases_da_nota=fiscal.FRASE_DA_NOTA,
            buscas=sefaz.estado_das_buscas(),
            # QUANTOS CERTIFICADOS EXISTEM. Sem isso a tela dizia "a busca
            # nunca rodou — falta o certificado" para quem já tinha cadastrado
            # três, e mandava procurar no lugar errado. Reclamação do dono em
            # 13/09/2026.
            cnpjs_com_certificado=_cnpjs_com_certificado(),
            primeira_linha=(pagina - 1) * fiscal.NOTAS_POR_PAGINA + 1,
            ultima_linha=ultima, tem_proxima=ultima < total, args=request.args,
            painel_notas=painel_notas, categorias=fiscal.CATEGORIAS,
            andamento=tarefas.estado(), acoes=ACOES_FISCAIS,
            ultimas=tarefas.ultimas_por_tipo(tarefas.MODOS_FISCAIS),
            aviso=request.args.get("aviso") or None,
            pode_operar=auth.pode_operar(),
            perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
            nome=auth.nome_atual())

    try:
        linhas = consultas.listar(filtros, ordem=request.args.get(
            "ordem", "vencimento"), pagina=pagina)
        conciliadas = fiscal.conciliar(linhas)
        resumo = consultas.resumo(filtros)
        # OS TOTAIS SÃO DA BASE INTEIRA, não da página. Ver
        # `consultas.painel_fiscal`: contar as 200 linhas da tela responderia
        # "o que falta NESTA PÁGINA", que parece certo e não é.
        painel = consultas.painel_fiscal(filtros)
        painel["sem_lancamento"] = fiscal.painel_notas().get("sem_lancamento", 0)
        erro = None
    except Exception as e:  # noqa: BLE001 — migração 005 ainda não aplicada
        logger.exception("Análise de SPs: falhou a conciliação fiscal")
        conciliadas, resumo, painel, erro = [], {"quantidade": 0, "total": 0}, {}, (
            "Esta tela precisa da atualização do banco. Vá em Configurações e "
            f"aperte \"Aplicar atualizações do banco\". (detalhe: {e})")

    # O QUADRO POR CATEGORIA, com o dinheiro ao lado da contagem. Ver
    # `consultas.quadro_por_categoria`: é o "quanto isso em valores" dele.
    #
    # ⚠️ EM BLOCO PRÓPRIO, E ISSO NÃO É ZELO — foi defeito, pego pela suíte no
    # mesmo dia em que o quadro nasceu. Ele estava DENTRO do try da lista, e
    # uma falha aqui derrubava a TELA INTEIRA: a lista sumia, o painel sumia, e
    # o recado dizia "esta tela precisa da atualização do banco" — que nem era
    # verdade.
    #
    # A REGRA GERAL, e vale para o que vier depois: **o acessório não pode
    # derrubar o principal**. O quadro é um extra; sem ele a tela continua
    # fazendo o trabalho dela.
    #
    # ⚠️ Sai do MESMO filtro da lista, então o que o quadro soma e o que a
    # lista mostra não têm como divergir.
    try:
        quadro_categorias = consultas.quadro_por_categoria(filtros)
    except Exception:  # noqa: BLE001 — o quadro é extra; a lista não é
        logger.exception("Análise de SPs: falhou o quadro por categoria")
        quadro_categorias = []

    contagem = fiscal.contar_por_grupo(conciliadas)
    if grupo:
        conciliadas = [c for c in conciliadas if c["grupo"] == grupo]

    ultima = (pagina - 1) * consultas.POR_PAGINA + len(linhas or [])
    return render_template(
        "analisesps_fiscal.html", aba="fiscal", base=base,
        linhas=conciliadas, contagem=contagem, grupo=grupo, erro=erro,
        resumo=resumo, filtros=filtros, args=request.args,
        quadro_categorias=quadro_categorias,
        opcoes=_opcoes_dos_filtros(base.get("ultima")),
        recado_dos_filtros=_recado_dos_filtros(base),
        pagina=pagina, por_pagina=consultas.POR_PAGINA,
        primeira_linha=(pagina - 1) * consultas.POR_PAGINA + 1,
        ultima_linha=ultima, tem_proxima=ultima < resumo["quantidade"],
        categorias=fiscal.CATEGORIAS, painel=painel,
        rotulos_fiscais=consultas.ROTULOS_FISCAIS,
        grupos_de_recorte=consultas.GRUPOS_DE_RECORTE,
        frases_do_recorte=consultas.FRASE_DO_RECORTE,
        andamento=tarefas.estado(), acoes=ACOES_FISCAIS,
        ultimas=tarefas.ultimas_por_tipo(tarefas.MODOS_FISCAIS),
        aviso=request.args.get("aviso") or None,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/api/fiscal/confirmar", methods=["POST"])
@exige_operador
def confirmar_fiscal():
    """Grava as decisões marcadas NO DIÁRIO — ainda não no card.

    A separação é o que permite tentar de novo quando o Pipefy recusa: a
    decisão fica registrada com quem decidiu, e a escrita no card é um passo
    à parte, que volta a tentar sozinho. Se as duas fossem uma coisa só, uma
    falha de rede apagaria a decisão de trinta cards."""
    from . import fiscal

    dados = request.get_json(silent=True) or {}
    itens = dados.get("itens") or []
    if not itens:
        return {"ok": False, "erro": "Nenhuma linha marcada."}, 400
    if len(itens) > 500:
        return {"ok": False,
                "erro": "São no máximo 500 por vez. Refine o filtro."}, 400

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    gravadas, recusadas = 0, []
    for item in itens:
        sp_id = str(item.get("sp") or "").strip()
        documentacao = str(item.get("documentacao") or "").strip()
        if not sp_id or not documentacao:
            continue
        # A categoria TEM de ser uma das 22 do campo do Pipefy. Ele recusa o
        # card inteiro quando o texto não é uma das opções — então um valor
        # inventado aqui não erraria uma SP, derrubaria a gravação do lote.
        if documentacao not in fiscal.CATEGORIAS:
            recusadas.append(f"{sp_id}: categoria desconhecida")
            continue
        try:
            fiscal.guardar_decisao(
                sp_id, documentacao, item.get("chave") or "",
                item.get("motivo") or "", item.get("confianca") or 0, quem)
            gravadas += 1
        except Exception as e:  # noqa: BLE001 — uma linha ruim não derruba as outras
            logger.exception("Análise de SPs: falhou gravar a decisão de %s", sp_id)
            recusadas.append(f"{sp_id}: {e}")

    logger.info("Análise de SPs: %s confirmou %d análise(s) fiscal(is).",
                quem or "sem nome", gravadas)

    # DISPARA A GRAVAÇÃO NOS CARDS, em processo separado. Se já houver outra
    # rodada em andamento o disparo é recusado — e tudo bem: a decisão fica na
    # fila e entra na próxima. Nada se perde por isso.
    from . import tarefas
    disparou = tarefas.disparar("fiscal", disparo=quem or "análise fiscal")
    recado = ("A gravação nos cards começou." if disparou.get("ok")
              else "A gravação nos cards entra na próxima rodada "
                   f"({disparou.get('erro') or 'já há uma em andamento'}).")
    return {"ok": True, "gravadas": gravadas, "recusadas": recusadas,
            "aviso": f"{gravadas} análise(s) confirmada(s). {recado}"}


@bp.route("/api/fiscal/ia", methods=["POST"])
@exige_operador
def pedir_ia_fiscal():
    """Manda a IA ler o anexo das SPs escolhidas.

    NUNCA AUTOMÁTICO, e é decisão do dono, com as palavras dele: *"aí você pode
    até fazer a sugestão, analisar com IA, e a gente seleciona ou não seleciona,
    que me permita selecionar alguns que eu queira testar"*. Quem escolhe é
    ele, SP a SP — e é assim que ele mede se compensa antes de soltar em tudo.

    A leitura NÃO acontece aqui: baixar e ler cada anexo leva segundos por SP.
    Aqui só se enfileira, no banco, e dispara o processo separado."""
    from . import fiscal, tarefas

    dados = request.get_json(silent=True) or {}
    ids = [str(i).strip() for i in (dados.get("ids") or []) if str(i).strip()]
    if not ids:
        return {"ok": False, "erro": "Nenhuma SP marcada."}, 400
    # O TETO É BAIXO DE PROPÓSITO: cada leitura é paga. Quem quiser mandar
    # trezentas manda em levas, e vê o resultado das cinquenta primeiras antes
    # de decidir se vale a pena continuar.
    if len(ids) > 50:
        return {"ok": False,
                "erro": "São no máximo 50 por vez. Cada leitura é cobrada — "
                        "mande uma leva, veja o resultado, e siga."}, 400

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        entraram = fiscal.por_na_fila_da_ia(ids, quem)
    except Exception as e:  # noqa: BLE001 — migração 005 ainda não aplicada
        logger.exception("Análise de SPs: falhou enfileirar para a IA")
        return {"ok": False, "erro": f"Não consegui enfileirar: {e}"}, 500

    disparou = tarefas.disparar("fiscal_ia", disparo=quem or "análise fiscal")
    de_fora = len(ids) - entraram
    aviso = f"{entraram} SP(s) na fila da IA."
    if de_fora:
        aviso += (f" {de_fora} ficou/ficaram de fora por já terem análise "
                  "confirmada — desfaça a decisão antes, se quiser refazer.")
    aviso += (" A leitura começou." if disparou.get("ok")
              else " Entra na próxima rodada.")
    logger.info("Análise de SPs: %s mandou %d SP(s) para a IA.",
                quem or "sem nome", entraram)
    return {"ok": True, "enfileiradas": entraram, "aviso": aviso}


# ---------------------------------------------------------------------------
# CERTIFICADOS DIGITAIS — subidos pela tela, guardados cifrados
#
# ⚠️ A CREDENCIAL MAIS SENSÍVEL DO SISTEMA: com o arquivo e a senha, qualquer um
# emite nota em nome da empresa. Por isso NÃO EXISTE rota que devolva o
# conteúdo — nem para quem subiu. A tela mostra de quem é, até quando vale e
# quem subiu; o arquivo só sai do banco para dentro do próprio sistema, na hora
# de falar com a Receita.
# ---------------------------------------------------------------------------
@bp.route("/certificados/subir", methods=["POST"])
@exige_operador
def subir_certificado():
    """Recebe o .pfx, confere abrindo, e guarda cifrado."""
    from . import certificados

    arquivo = request.files.get("certificado")
    senha = request.form.get("senha") or ""
    apelido = request.form.get("apelido") or ""
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")

    if not arquivo or not arquivo.filename:
        return redirect(url_for("analisesps.configuracoes",
                                aviso="Escolha o arquivo do certificado."))
    try:
        dados = certificados.guardar(arquivo.read(), senha, apelido, quem)
    except (certificados.ErroDeCertificado, certificados.SemCofre) as e:
        return redirect(url_for("analisesps.configuracoes", aviso=str(e)))
    except Exception as e:  # noqa: BLE001 — migração 009 ainda não aplicada
        logger.exception("Análise de SPs: falhou guardar o certificado")
        return redirect(url_for(
            "analisesps.configuracoes",
            aviso=f"Não consegui guardar o certificado: {e}"))

    return redirect(url_for("analisesps.configuracoes", aviso=(
        f"Certificado de {dados['titular']} guardado. Vale até "
        f"{dados['valido_ate'].strftime('%d/%m/%Y') if dados['valido_ate'] else '—'}"
        ". A busca de notas na Receita passa a usá-lo na próxima rodada.")))


@bp.route("/certificados/conferir", methods=["POST"])
@exige_operador
def conferir_certificado():
    """Tenta abrir o .pfx e CONTA o que aconteceu. NÃO GUARDA NADA.

    Pedido do dono em 13/09/2026: *"suspeito que o certificado e a senha estejam
    corretos, mas a mensagem é de certificado inválido ou senha. Existe algum
    canto que eu possa tirar essa prova?"*

    A desconfiança dele tinha fundamento: um espaço colado junto com a senha
    dava exatamente a mesma mensagem de uma senha errada — e quem copia a senha
    de um e-mail traz o espaço junto sem perceber. Ver `certificados._abrir`.

    ⚠️ NÃO GUARDA E NÃO MEXE no que já está guardado: é só uma conferência. Por
    isso dá para experimentar à vontade sem risco de estragar o certificado que
    está em uso."""
    from . import certificados

    arquivo = request.files.get("certificado")
    senha = request.form.get("senha") or ""
    if not arquivo or not arquivo.filename:
        return redirect(url_for("analisesps.configuracoes",
                                aviso="Escolha o arquivo para conferir."))
    try:
        r = certificados.conferir(arquivo.read(), senha)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou conferir o certificado")
        return redirect(url_for("analisesps.configuracoes",
                                aviso=f"Não consegui conferir: {e}"))

    if not r["ok"]:
        aviso = "✕ " + r["motivo"]
    else:
        validade = (r["valido_ate"].strftime("%d/%m/%Y")
                    if r.get("valido_ate") else "—")
        aviso = (f"✔ O certificado ABRIU com esta senha. Titular: "
                 f"{r['titular']} · CNPJ {r['cnpj']} · vale até {validade}.")
        if r.get("senha_ajustada"):
            aviso += (f" ⚠️ Abriu com a senha {r['senha_ajustada']} — ou seja, "
                      "veio espaço junto no copiar e colar. A senha está certa; "
                      "o espaço é que atrapalhava.")
    # O nome do arquivo NÃO vai para o aviso: ele aparece no endereço da
    # página, e nome de arquivo de certificado costuma trazer o CNPJ.
    logger.info("Análise de SPs: %s conferiu um certificado — %s.",
                auth.nome_atual() or auth.pessoa_atual(),
                "abriu" if r["ok"] else "não abriu")
    return redirect(url_for("analisesps.configuracoes", aviso=aviso))


@bp.route("/certificados/remover", methods=["POST"])
@exige_operador
def remover_certificado():
    """Tira o certificado de uso. Apaga de verdade — ver `certificados.py`."""
    from . import certificados

    cnpj = request.form.get("cnpj") or ""
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        saiu = certificados.remover(cnpj, quem)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou remover o certificado")
        return redirect(url_for("analisesps.configuracoes",
                                aviso=f"Não consegui remover: {e}"))
    return redirect(url_for("analisesps.configuracoes", aviso=(
        "Certificado removido. A busca de notas daquele CNPJ para agora."
        if saiu else "Não havia certificado guardado para esse CNPJ.")))


# ---------------------------------------------------------------------------
# CREDORES — o mesmo CNPJ escrito de cinco jeitos
#
# FORA DAS ABAS DE CIMA, e de propósito: isto é arrumação ocasional, não
# trabalho do dia. A barra de abas é para o que se abre todo dia; encher ela
# com manutenção faria o que importa ficar mais longe. Chega-se aqui por
# ===========================================================================
# A PLANILHA — dentro da Documentação Fiscal, e não solta no menu
#
# Cobrança do dono em 13/09: *"desde o começo eu pedi uma tela simples pra
# poder visualizar similar ao que eu visualizo na planilha."* Entregue no dia
# seguinte — e ele olhou e disse o que faltava:
#
#   *"Está lá 'ver os dados', está muito solto, não tem vínculo com nada. Está
#   ruim da forma que está. Aqui tem 'por lançamento', 'por nota' — aí você
#   colocar aqui dentro. E 'ver os dados' também está foda, tem que ter uma
#   nomenclatura melhor."*
#
# ELE ESTÁ CERTO NAS DUAS COISAS, e as duas são a mesma: a tela nasceu **sem
# contexto**. Ela é da Documentação Fiscal — é ali que ele está quando quer
# conferir o dado cru contra o que a tela de trabalho está afirmando. Solta no
# menu de cima, virava um destino sem volta e sem parentesco.
#
# E o nome não dizia nada: "ver os dados" pode ser qualquer coisa. Agora usa a
# palavra que ELE usa o tempo todo — **planilha**.
#
# POR QUE É UMA FUNÇÃO, e não uma rota própria: as quatro visões dividem a
# barra de filtros, a memória do filtro e a barra de visões. Rota separada
# significaria manter tudo isso em dois lugares — e foi exatamente assim que a
# tela ficou órfã da primeira vez.
# ===========================================================================
def _planilha_fiscal(base, pagina: int):
    """As duas visões de VER: todas as colunas, na ordem da planilha."""
    from . import consultas, fiscal

    sub = ("notas" if request.args.get("visao") == "dados_notas"
           else "lancamentos")
    busca = (request.args.get("busca") or "").strip()
    ordem = request.args.get("ordem") or ("emissao" if sub == "notas" else "id")
    # O SENTIDO PADRÃO É DIFERENTE NAS DUAS, e isso é sobre como se lê: a nota
    # mais recente é a que interessa primeiro (acabou de chegar); a SP se lê do
    # começo, pelo número, como na planilha.
    if "desc" in request.args:
        desc = request.args.get("desc") == "1"
    else:
        desc = (sub == "notas")

    # DE ONDE A NOTA VEIO, como recorte da tela. *"Como é que eu sei que eu
    # estou visualizando essas notas que foram baixadas?"* — a pergunta só tem
    # resposta se der para pedir "me mostre só o que a busca trouxe".
    origem = (request.args.get("origem") or "").strip().lower()
    if origem not in ("receita", "fsist", "sem"):
        origem = ""
    # O QUE O QUADRO DO ALTO RECORTA quando ele clica num número. Lista fechada:
    # o que vem do endereço nunca vira SQL por conta própria.
    situacao = (request.args.get("situacao") or "").strip().upper()
    if situacao not in ("AUTORIZADA", "CANCELADA", "DENEGADA"):
        situacao = ""
    sem_lancamento = request.args.get("sem_lancamento") == "1"

    # ⚠️ EM TRY PRÓPRIO: o acessório não pode derrubar o principal. A conta é
    # enfeite; a lista é a tela.
    contagem_origem, quadro_notas = {}, []
    if sub == "notas":
        try:
            contagem_origem = fiscal.contagem_por_origem()
        except Exception:  # noqa: BLE001
            logger.exception("Análise de SPs: falhou contar a origem das notas")
        try:
            quadro_notas = fiscal.quadro_das_notas(busca, origem)
        except Exception:  # noqa: BLE001
            logger.exception("Análise de SPs: falhou montar o quadro das notas")

    erro, linhas, total = None, [], 0
    try:
        if sub == "notas":
            linhas, total = fiscal.planilha_notas(
                busca, ordem, desc, pagina, origem=origem,
                situacao=situacao, sem_lancamento=sem_lancamento)
            cabecalhos = [(c, "", r, t)
                          for c, r, t in fiscal.colunas_da_nota_na_tela()]
            por_pagina = fiscal.POR_PAGINA_PLANILHA
        else:
            linhas, total = consultas.planilha_sps(
                busca, ordem, desc, pagina,
                tudo=request.args.get("tudo") == "1")
            cabecalhos = consultas.COLUNAS_DA_PLANILHA
            por_pagina = consultas.POR_PAGINA_PLANILHA
    except Exception as e:  # noqa: BLE001 — migração 005 ainda não aplicada
        logger.exception("Análise de SPs: falhou montar a planilha")
        cabecalhos, por_pagina = [], consultas.POR_PAGINA_PLANILHA
        erro = ("Esta tela precisa da atualização do banco. Vá em "
                "Configurações e aperte \"Aplicar atualizações do banco\". "
                f"(detalhe: {e})")

    ultima = (pagina - 1) * por_pagina + len(linhas)
    return render_template(
        "analisesps_planilha.html", aba="fiscal", sub=sub, base=base,
        linhas=linhas, cabecalhos=cabecalhos, total=total, erro=erro,
        busca=busca, ordem=ordem, desc=desc, pagina=pagina, origem=origem,
        rotulos_de_origem=fiscal.ROTULOS_DE_ORIGEM,
        contagem_origem=contagem_origem, quadro_notas=quadro_notas,
        situacao=situacao, sem_lancamento=sem_lancamento,
        tudo=request.args.get("tudo") == "1",
        ano_minimo=consultas.ANO_FISCAL_MINIMO,
        primeira_linha=(pagina - 1) * por_pagina + 1, ultima_linha=ultima,
        tem_proxima=ultima < total, args=request.args,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/planilha")
@exige_consulta
def tela_planilha():
    """O endereço antigo, de quando a tela era solta no menu.

    Fica só para não quebrar o que ele tiver guardado nos favoritos. Manda para
    o lugar de verdade, dentro da Documentação Fiscal."""
    destino = ("dados_notas" if request.args.get("aba") == "notas"
               else "dados_sps")
    return redirect(url_for("analisesps.tela_fiscal", visao=destino))


# Configurações, onde a contagem aparece.
# ---------------------------------------------------------------------------
@bp.route("/credores")
@exige_consulta
def tela_credores():
    from . import consultas, credores

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())
    try:
        lista = credores.divergencias_da_base()
        erro = None
    except Exception as e:  # noqa: BLE001 — migração 007 ainda não aplicada
        logger.exception("Análise de SPs: falhou levantar os credores")
        lista, erro = [], (
            "Esta tela precisa da atualização do banco. Vá em Configurações e "
            f"aperte \"Aplicar atualizações do banco\". (detalhe: {e})")

    return render_template(
        "analisesps_credores.html", aba="configuracoes", base=base,
        divergencias=lista, erro=erro,
        aviso=request.args.get("aviso") or None,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/credores/consultar", methods=["POST"])
@exige_operador
def consultar_cnpj_credor():
    """Pergunta à Receita de quem é um CNPJ, UM por vez e por pedido de gente.

    Pedido do dono em 13/09/2026: *"eu quero que você faça a consulta via API do
    credor desse CNPJ."*

    É `@exige_operador` não porque altere dado da empresa — a consulta só lê —,
    mas porque ela fala com um serviço de fora, e quem dispara chamada externa
    é quem opera. E é UM CNPJ por clique de propósito: varrer novecentos
    fornecedores de uma vez é o jeito certo de ser bloqueado por uso excessivo,
    e aí a consulta para de funcionar inclusive no caso em que importa."""
    from . import receita

    # ⚠️ SEM RECARREGAR, quando quem pede é a tela por trás. Reclamação do dono
    # em 13/09/2026: *"quando consulta o nome na Receita, a tela sobe. O
    # resultado deveria aparecer flutuante, ou de forma que não mexa na tela."*
    #
    # O resultado desta consulta é sobre UM fornecedor específico, no meio de
    # uma lista longa: mandá-lo para um aviso no alto da página, depois de
    # recarregar tudo, é entregar a resposta longe da pergunta.
    sem_recarregar = request.headers.get("X-Sem-Recarregar") == "1"

    def falhou(mensagem: str):
        if sem_recarregar:
            return {"ok": False, "erro": mensagem}
        return redirect(url_for("analisesps.tela_credores",
                                aviso=f"Não consegui consultar: {mensagem}"))

    documento = (request.form.get("documento") or "").strip()
    try:
        dados = receita.consultar(documento, forcar=True)
    except receita.ErroDeConsulta as e:
        return falhou(str(e))
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou consultar o CNPJ")
        return falhou(str(e))

    if dados.get("erro"):
        aviso = f"{documento}: {dados['erro']}"
    else:
        aviso = (f"{documento} é \"{dados.get('razao_social') or '(sem nome)'}\""
                 + (f" — nome de fantasia \"{dados['fantasia']}\""
                    if dados.get("fantasia") else "")
                 + (f", situação {dados['situacao']}"
                    if dados.get("situacao") else "") + ".")
    logger.info("Análise de SPs: %s consultou o CNPJ %s.",
                auth.nome_atual() or auth.pessoa_atual(), documento)
    if sem_recarregar:
        # Os campos vão separados para a tela montar a linha do jeito dela —
        # o mesmo formato da linha que já existe quando a página é desenhada.
        return {"ok": True, "documento": documento, "aviso": aviso,
                "erro_receita": dados.get("erro") or "",
                "razao_social": dados.get("razao_social") or "",
                "fantasia": dados.get("fantasia") or "",
                "situacao": dados.get("situacao") or "",
                "municipio": dados.get("municipio") or "",
                "uf": dados.get("uf") or ""}
    return redirect(url_for("analisesps.tela_credores", aviso=aviso))


@bp.route("/credores/sps", methods=["POST"])
@exige_consulta
def sps_do_nome_credor():
    """As SPs escritas com um determinado nome, para conferir antes de decidir.

    Pedido do dono em 13/09/2026: *"aí ele marca aqui uma, duas, três, quatro
    SPs que é de uma outra locadora que não tem nada a ver, ou seja, aqui foi
    claramente um erro. Só que a partir daqui eu não consigo ir a essas SPs que
    estão erradas. Só pra poder confirmar se eu posso realmente aplicar ou não,
    eu precisaria ver essas SPs e entender onde foi o erro."*

    É `@exige_consulta` e não `@exige_operador`: isto só LÊ, e ler o que
    fundamenta uma decisão não pode ser mais difícil do que tomar a decisão."""
    from . import credores
    from .formatos import data_br, moeda

    documento = (request.form.get("documento") or "").strip()
    grafias = [g for g in request.form.getlist("grafia") if g.strip()]
    try:
        sps = credores.sps_do_nome(documento, grafias)
    except Exception as e:  # noqa: BLE001 — migração ainda não aplicada
        logger.exception("Análise de SPs: falhou listar as SPs do nome")
        return {"ok": False, "erro": str(e), "sps": []}
    return {"ok": True, "teto": credores.SPS_POR_NOME, "sps": [{
        "id": str(linha.get("id") or ""),
        "credor": linha.get("credor") or "",
        "valor": moeda(linha.get("valor_num")),
        "vencimento": data_br(linha.get("vencimento_d")),
        "status": linha.get("status_pgt") or "",
        "descricao": (linha.get("descricao") or "")[:120],
        "card": linha.get("card_link") or "",
    } for linha in sps]}


@bp.route("/credores/aplicar", methods=["POST"])
@exige_operador
def aplicar_credor():
    """Grava a escolha e reescreve o nome nas SPs daquele CPF/CNPJ.

    PASSA PELO MESMO CAMINHO DE SEMPRE — `_gravar_alteracao`: banco, fila, log,
    planilha. Não é atalho: é o que garante que a mudança apareça no Log com o
    valor anterior e com quem mexeu, e que chegue à planilha mesmo se a
    internet cair no meio.

    E ENTRA POR PORTA PRÓPRIA, como a Validação e o "Remover risco": a coluna
    do credor NÃO está em `EDITAVEIS`, então ninguém reescreve nome de
    fornecedor pela tela comum. Nome de credor não é campo de trabalho do dia
    a dia."""
    from . import credores

    documentos = [d for d in request.form.getlist("documento") if d.strip()]
    escolhidos = request.form.getlist("nome")
    if len(documentos) != len(escolhidos):
        return redirect(url_for("analisesps.tela_credores",
                                aviso="Pedido incompleto. Tente de novo."))

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    # AS SPs QUE ELE DESMARCOU. *"Às vezes não queremos renomear todos os
    # lançamentos. O erro pode ter sido no CNPJ e não somente o nome. Preciso
    # poder não marcar algum."* Vem uma lista só para a tela inteira; o id da
    # SP é único, então não há como uma exclusão vazar para outro fornecedor.
    de_fora = [i for i in request.form.getlist("nao_reescrever") if i.strip()]
    mudadas, fornecedores, sem_efeito, ficaram_de_fora = 0, 0, 0, 0
    for documento, nome in zip(documentos, escolhidos):
        nome = (nome or "").strip()
        if not nome:
            continue
        # OS GRUPOS DE ESCRITA QUE ELE DESMARCOU — a segunda seleção da tela.
        # *"Tem que ter duas seleções: a do nome, e em quais grupos vamos
        # aplicar."* (15/09/2026)
        #
        # Vem NOMEADO POR FORNECEDOR (`grupo-<documento>`) porque a pilha do
        # "resolve sozinho" manda vários fornecedores no mesmo envio: uma lista
        # única faria a grafia de um calar a do outro sempre que dois
        # fornecedores tivessem a mesma escrita.
        #
        # E vem em DOIS campos de propósito: o escondido diz quais grupos a
        # tela mostrou, o marcado diz quais ele deixou ligados. Caixa
        # desmarcada não é enviada pelo navegador — sem a lista do que existia,
        # o servidor não teria como distinguir "ele desmarcou" de "a tela é
        # antiga e não manda isso".
        mostrados = [g for g in request.form.getlist(f"grupo-{documento}")
                     if str(g).strip()]
        marcados = {g for g in request.form.getlist(f"aplicar-{documento}")
                    if str(g).strip()}
        grupos_fora = [g for g in mostrados if g not in marcados]
        todas = credores.sps_para_reescrever(documento, nome)
        ids = credores.sps_para_reescrever(documento, nome, fora=de_fora,
                                           grafias_fora=grupos_fora)
        ficaram_de_fora += len(todas) - len(ids)
        tipo = request.form.get(f"tipo-{documento}") or credores.DECIDIR
        if ids:
            # Em blocos: uma SP com muitos lançamentos do mesmo fornecedor
            # pode passar do teto de 500 da rota comum.
            for inicio in range(0, len(ids), 400):
                _gravar_alteracao(ids[inicio:inicio + 400], "credor", nome,
                                  "Equalizar nome do credor")
            mudadas += len(ids)
        else:
            sem_efeito += 1
        credores.guardar_escolha(documento, nome, tipo,
                                 tipo != credores.DECIDIR, quem, len(ids))
        fornecedores += 1

    if not fornecedores:
        aviso = "Nenhum nome foi escolhido."
    else:
        aviso = (f"{fornecedores} fornecedor(es) equalizado(s), "
                 f"{mudadas} SP(s) reescrita(s). A planilha é atualizada na "
                 "próxima sincronização.")
        if sem_efeito:
            aviso += (f" {sem_efeito} já estava(m) com o nome certo — ficou só "
                      "a decisão guardada.")
        # O QUE FICOU DE FORA É DITO, e não engolido: ele desmarcou de
        # propósito, e precisa ver que foi respeitado.
        if ficaram_de_fora:
            aviso += (f" {ficaram_de_fora} SP(s) você deixou de fora — "
                      "continuam com o nome como estão.")
    logger.info("Análise de SPs: %s equalizou %d credor(es), %d SP(s).",
                quem or "sem nome", fornecedores, mudadas)

    # ⚠️ SEM RECARREGAR A PÁGINA, quando quem pede é a tela por trás.
    #
    # Reclamação do dono em 13/09/2026, depois de publicado: *"clico 'usar
    # este', continua subindo a tela. Clico em dois e acho que ele somente
    # resolve um."*
    #
    # Cada fornecedor é um formulário próprio, e cada envio era uma página
    # inteira indo e voltando: a rolagem ia para o topo de uma lista longa, e
    # o segundo clique cancelava o primeiro, que ainda estava no ar. Com
    # `fetch` cada linha se resolve onde está e vários podem estar gravando ao
    # mesmo tempo.
    #
    # A resposta continua sendo redirecionamento para quem chega pelo caminho
    # normal — sem JavaScript a tela tem de continuar funcionando.
    if request.headers.get("X-Sem-Recarregar") == "1":
        return {"ok": True, "aviso": aviso, "fornecedores": fornecedores,
                "sps": mudadas, "de_fora": ficaram_de_fora,
                "documentos": documentos,
                "nomes": [str(n or "").strip() for n in escolhidos]}
    return redirect(url_for("analisesps.tela_credores", aviso=aviso))


# ---------------------------------------------------------------------------
# COMPROVANTES — arrastar o PDF e a baixa acontece
#
# O trabalho pesado NÃO É DAQUI: o robô que dá baixa é o `baixabradesco`, que
# roda em produção há meses. Estas rotas são a porta de entrada que o dono
# pediu em 11/09/2026 — *"eu arrasto esses comprovantes pra dentro e dispara a
# automação, sem nem precisar passar pelo Make"* — e a memória do que
# aconteceu, que hoje volta para o Make.com e morre lá.
# ---------------------------------------------------------------------------
@bp.route("/comprovantes")
@exige_consulta
def tela_comprovantes():
    from . import comprovantes, consultas, tarefas

    base = consultas.base_carregada()
    if not base["pronta"]:
        return render_template("analisesps_vazio.html", base=base,
                               pode_operar=auth.pode_operar())

    try:
        historico = comprovantes.historico()
        for lote in historico:
            lote["itens"] = comprovantes.itens_do_lote(lote["id"])
    except Exception as e:  # noqa: BLE001 — migração 006 ainda não aplicada
        logger.exception("Análise de SPs: falhou ler o histórico de comprovantes")
        historico = []
        return render_template(
            "analisesps_comprovantes.html", aba="comprovantes", base=base,
            historico=[], andamento={"rodando": False},
            por_leva=comprovantes.POR_LEVA,
            maximo_mb=comprovantes.MAXIMO_POR_ARQUIVO // (1024 * 1024),
            maximo_arquivos=comprovantes.MAXIMO_DE_ARQUIVOS,
            aviso="Esta tela precisa da atualização do banco. Vá em "
                  "Configurações e aperte \"Aplicar atualizações do banco\". "
                  f"(detalhe: {e})",
            pode_operar=auth.pode_operar(),
            perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
            nome=auth.nome_atual())

    return render_template(
        "analisesps_comprovantes.html", aba="comprovantes", base=base,
        historico=historico, andamento=tarefas.estado(),
        por_leva=comprovantes.POR_LEVA,
        maximo_mb=comprovantes.MAXIMO_POR_ARQUIVO // (1024 * 1024),
        maximo_arquivos=comprovantes.MAXIMO_DE_ARQUIVOS,
        aviso=request.args.get("aviso") or None,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


@bp.route("/comprovantes/enviar", methods=["POST"])
@exige_operador
def enviar_comprovantes():
    """Recebe os arquivos soltos e põe na fila. NÃO dá baixa aqui.

    A baixa fala com Omie, Pipefy, Sheets e Dropbox e leva minutos; dentro do
    worker ela seria morta pelo reinício do gunicorn, como já aconteceu três
    vezes com a carga da planilha. Aqui só se guarda e se dispara o processo
    separado — quem responde à pessoa é a tela, lendo o banco."""
    from . import comprovantes, tarefas

    arquivos = [a for a in request.files.getlist("arquivos")
                if a and a.filename]
    if not arquivos:
        return redirect(url_for("analisesps.tela_comprovantes",
                                aviso="Nenhum arquivo foi escolhido."))
    if len(arquivos) > comprovantes.MAXIMO_DE_ARQUIVOS:
        return redirect(url_for(
            "analisesps.tela_comprovantes",
            aviso=f"São no máximo {comprovantes.MAXIMO_DE_ARQUIVOS} arquivos "
                  "por vez. Mande em duas levas."))

    pessoa = auth.pessoa_atual()
    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    aceitos, recusados = 0, []
    for arquivo in arquivos:
        try:
            comprovantes.guardar(arquivo.read(), arquivo.filename, pessoa, quem)
            aceitos += 1
        except comprovantes.ErroDeComprovante as e:
            recusados.append(str(e))
        except Exception as e:  # noqa: BLE001 — um arquivo ruim não derruba os outros
            logger.exception("Análise de SPs: falhou guardar %r",
                             arquivo.filename)
            recusados.append(f"{arquivo.filename}: {e}")

    if aceitos:
        # Se já houver uma atualização rodando, o disparo é recusado — e tudo
        # bem: o lote fica ESPERANDO e entra na próxima. Dizer isso é melhor
        # do que fingir que já está processando.
        tarefas.disparar("comprovantes", disparo=quem or "comprovantes")

    aviso = (f"{aceitos} arquivo(s) na fila. O resultado aparece aqui embaixo; "
             "pode fechar a tela." if aceitos else "")
    if recusados:
        aviso = (aviso + " " if aviso else "") + " ".join(recusados)
    return redirect(url_for("analisesps.tela_comprovantes", aviso=aviso))


@bp.route("/comprovantes/reprocessar", methods=["POST"])
@exige_operador
def reprocessar_comprovante():
    """Manda UM lote para a fila de novo, sem precisar achar o PDF outra vez.

    Pedido do dono em 17/09/2026: *"às vezes os comprovantes não baixam por
    algum motivo. Eu queria, a partir da tela, poder reenviar um comprovante.
    (…) Opa, esqueci algum detalhe — o título não está no [Omie]."*

    É o caso de todo dia: não baixou porque o título ainda não existe no Omie,
    ou faltou um dado no card. A pessoa conserta lá e aperta aqui.

    ⚠️ FORMULÁRIO COMUM, não `fetch`, pelo mesmo motivo do botão "Retomar a
    fila": é botão de destravar, e tem de funcionar mesmo se o JavaScript não
    carregar."""
    from . import comprovantes, tarefas

    try:
        lote_id = int(request.form.get("lote") or 0)
    except ValueError:
        lote_id = 0
    if not lote_id:
        return redirect(url_for("analisesps.tela_comprovantes",
                                aviso="Não entendi qual comprovante reenviar."))

    resultado = comprovantes.reprocessar_lote(lote_id)
    if not resultado.get("ok"):
        return redirect(url_for("analisesps.tela_comprovantes",
                                aviso=resultado.get("erro", "Não deu para "
                                                    "reenviar este lote.")))

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    disparo = tarefas.disparar("comprovantes", disparo=quem or "reprocessar")
    nome = resultado.get("arquivo") or f"lote {lote_id}"
    # ⚠️ O RECADO DIZ O QUE ACONTECEU DE VERDADE. Quando outra tarefa já está
    # rodando o disparo é recusado, e o lote fica na fila para a próxima — foi
    # metade do "clico e nada acontece" de 16/09. Fingir que já começou seria
    # repetir o mesmo engano.
    if disparo.get("ok"):
        aviso = (f"{nome} voltou para a fila e já está sendo processado. "
                 "O que já tinha baixado no Omie não baixa duas vezes.")
    else:
        aviso = (f"{nome} voltou para a fila. Ainda não começou porque já há "
                 "outra tarefa rodando — ele entra assim que ela terminar.")
    return redirect(url_for("analisesps.tela_comprovantes", aviso=aviso))


@bp.route("/api/comprovantes/estado")
@exige_consulta
def estado_comprovantes():
    """O andamento, para a tela se atualizar sozinha sem recarregar tudo."""
    from . import comprovantes, tarefas
    try:
        lotes = comprovantes.historico(quantos=5)
    except Exception:  # noqa: BLE001 — migração ainda não aplicada
        return {"ok": False, "rodando": False, "lotes": []}
    andamento = tarefas.estado()
    return {
        "ok": True,
        "rodando": bool(andamento.get("rodando")),
        "lotes": [{"id": l["id"], "situacao": l["situacao"],
                   "levas": l["levas"], "levas_feitas": l["levas_feitas"],
                   "pendencias": l["pendencias"], "resolvidos": l["resolvidos"]}
                  for l in lotes],
    }


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
    # A CAIXINHA DESMARCADA NÃO CHEGA NO FORMULÁRIO — é assim que o HTML
    # funciona. Com `.get("foco", "1")` o padrão "1" entrava justamente quando
    # a pessoa DESMARCAVA, e o foco nunca desligava. No GET (primeira visita)
    # ele deve vir ligado; no POST vale o que a caixinha diz.
    foco = (request.form.get("foco") == "1" if request.method == "POST"
            else True)

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
        # As colunas vêm do módulo que monta a linha — ver o comentário grande
        # em `bradesco.py`. Escritas no template, elas já divergiram uma vez e
        # a tela ficou com 47 linhas em branco.
        colunas_boleto=bradesco.COLUNAS_BOLETO,
        colunas_pix=bradesco.COLUNAS_PIX,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


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
@bp.route("/agenda", methods=["GET", "POST"])
@exige_consulta
def tela_agenda():
    from . import agenda

    # A permissão é conferida ANTES de qualquer outra coisa — mesma regra do
    # Lote, e pelo mesmo motivo: a resposta a uma escrita sem alçada tem de
    # ser sempre a mesma, independente do estado do banco.
    if request.method == "POST" and not auth.pode_operar():
        return auth._sem_permissao()

    aviso = erro_form = None
    editando = (request.args.get("editar") or "").strip()

    if request.method == "POST":
        acao = request.form.get("acao", "salvar")
        try:
            if acao == "desligar":
                atual = agenda.um(request.form.get("id", "")) or {}
                if not atual:
                    raise ValueError("Compromisso não encontrado.")
                atual["status"] = "inativo"
                agenda.salvar(agenda.normalizar(atual))
                aviso = (f"\"{atual.get('titulo')}\" foi desligado. Ele não "
                         "some da planilha — para voltar, é só religar.")
            elif acao == "religar":
                atual = agenda.um(request.form.get("id", "")) or {}
                if not atual:
                    raise ValueError("Compromisso não encontrado.")
                atual["status"] = "ativo"
                agenda.salvar(agenda.normalizar(atual))
                aviso = f"\"{atual.get('titulo')}\" voltou a valer."
            else:
                registro = agenda.normalizar(request.form.to_dict(),
                                             criado_por=auth.nome_atual())
                agenda.salvar(registro)
                aviso = (f"\"{registro['titulo']}\" guardado — aqui e na aba "
                         "Agenda da planilha.")
        except ValueError as e:
            erro_form = str(e)
        except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
            logger.exception("Análise de SPs: falhou salvar o compromisso")
            erro_form = (f"Não consegui gravar na planilha: {e}. Nada foi "
                         "salvo — nem aqui, nem lá.")
        if not erro_form:
            return redirect(url_for("analisesps.tela_agenda", aviso=aviso))

    try:
        dias = max(1, min(730, int(request.args.get("dias", 90))))
    except ValueError:
        dias = 90

    # O mês que a grade mostra. Vem da barra de endereço para que "mês
    # seguinte" seja um link — funciona com o botão voltar do navegador e dá
    # para mandar o endereço de um mês para alguém.
    from .horario import agora
    hoje = agora().date()
    try:
        ano = int(request.args.get("ano", hoje.year))
        mes = int(request.args.get("mes", hoje.month))
        if not (1 <= mes <= 12 and 2000 <= ano <= 2100):
            raise ValueError
    except ValueError:
        ano, mes = hoje.year, hoje.month

    try:
        proximos = agenda.proximos(dias)
        alertas = agenda.a_vencer()
        compromissos = agenda.listar()
        grade = agenda.calendario(ano, mes)
        erro = None
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Análise de SPs: falhou montar a agenda")
        proximos, alertas, compromissos, grade, erro = [], [], [], None, str(e)

    anterior = agenda.mes_vizinho(ano, mes, -1)
    seguinte = agenda.mes_vizinho(ano, mes, 1)

    em_edicao = None
    if editando:
        em_edicao = agenda.um(editando)

    return render_template(
        "analisesps_agenda.html", aba="agenda",
        proximos=proximos, alertas=alertas, compromissos=compromissos,
        grade=grade, ano=ano, mes=mes, meses=agenda.MESES,
        anterior=anterior, seguinte=seguinte, hoje=hoje,
        dias=dias, erro=erro,
        em_edicao=em_edicao, novo=(editando == "novo"),
        erro_form=erro_form,
        aviso=request.args.get("aviso") or None,
        categorias=agenda.CATEGORIAS, recorrencias=agenda.RECORRENCIAS,
        ajustes=agenda.AJUSTES, agenda_ativo=agenda.esta_ativo,
        pode_operar=auth.pode_operar(),
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


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

    # A coluna `pessoa` só existe depois da migração 003. Enquanto ela não
    # for aplicada, a tela mostra o registro sem o nome — que é o que havia
    # até 03/09 — em vez de não abrir.
    from .db import tem_coluna
    com_pessoa = tem_coluna("log_alteracoes", "pessoa")

    try:
        contagem = consultar(
            "SELECT status, count(*) FROM analisesps.log_alteracoes"
            " WHERE criado_em >= now() - make_interval(days => ?)"
            " GROUP BY status", (dias,))
        registros = consultar(
            "SELECT criado_em, sp_id, coluna, valor, valor_anterior, acao, "
            "       perfil, " + ("pessoa" if com_pessoa else "NULL") + ", "
            "       status, enviado_em, erro "
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
        perfil=auth.ROTULOS.get(auth.perfil_atual(), ""),
        nome=auth.nome_atual())


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
    # A PESSOA TEM DE SER PASSADA. Sem ela, `ler` devolvia o lote de
    # `pessoa = ''` — que é o LOTE ANTIGO, de quando ele era um só e
    # compartilhado, congelado desde a migração 003. Era isso que fazia a
    # exportação e o PDF saírem desatualizados por mais que a pessoa salvasse
    # o lote dela. Reportado pelo dono em 11/09/2026.
    try:
        montado = lote.montar(lote.ler(auth.pessoa_atual())["conteudo"])
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

    # Ver o comentário em `exportar_lote`: sem a pessoa, sai o lote antigo.
    try:
        montado = lote.montar(lote.ler(auth.pessoa_atual())["conteudo"])
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


def _grupo_do_lote(n: int):
    """(montado só com o grupo `n` — 1, 2, 3… na ordem da tela —, título) ou
    (None, mensagem)."""
    from . import lote
    montado = lote.montar(lote.ler(auth.pessoa_atual())["conteudo"])
    grupos = montado["grupos"]
    if not (1 <= int(n) <= len(grupos)) or not grupos[int(n) - 1]["linhas"]:
        return None, "Este grupo não existe mais no lote, ou está sem SPs."
    g = grupos[int(n) - 1]
    so = {**montado, "grupos": [g], "quantidade": len(g["linhas"]),
          "total_geral": g["total"]}
    return so, g["titulo_exibido"]


def _nome_do_grupo(titulo: str) -> str:
    import re as _re
    limpo = _re.sub(r"[^\w\- ]+", "", str(titulo or "grupo"), flags=_re.UNICODE)
    return (" ".join(limpo.split()) or "grupo")[:60]


@bp.route("/lote/grupo/<int:n>/pdf")
@exige_consulta
def lote_grupo_pdf(n: int):
    """O relatório de UM grupo do lote, em PDF — ao lado do QR do grupo (dono,
    02/10/2026)."""
    from flask import Response

    from . import pdf
    from .horario import agora
    try:
        so, titulo = _grupo_do_lote(n)
        if so is None:
            return render_template("analisesps_erro.html", titulo="Grupo não encontrado",
                                   mensagem=titulo), 404
        conteudo = pdf.relatorio_do_lote(so, titulo=titulo)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou o PDF do grupo do lote")
        return render_template("analisesps_erro.html", titulo="Não consegui gerar o PDF",
                               mensagem=f"{e}"), 500
    nome = f"lote_{_nome_do_grupo(titulo)}_{agora().strftime('%Y-%m-%d_%H%M')}.pdf"
    return Response(conteudo, mimetype="application/pdf",
                    headers={"Content-Disposition": f'attachment; filename="{nome}"'})


@bp.route("/lote/grupo/<int:n>/excel")
@exige_consulta
def lote_grupo_excel(n: int):
    """UM grupo do lote, em Excel."""
    from . import lote_excel
    from .horario import agora
    try:
        so, titulo = _grupo_do_lote(n)
        if so is None:
            return render_template("analisesps_erro.html", titulo="Grupo não encontrado",
                                   mensagem=titulo), 404
        conteudo = lote_excel.de_um_lote(so, titulo)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou o Excel do grupo do lote")
        return render_template("analisesps_erro.html", titulo="Não consegui gerar o Excel",
                               mensagem=f"{e}"), 500
    return _responder_xlsx(
        conteudo, f"lote_{_nome_do_grupo(titulo)}_{agora().strftime('%Y-%m-%d_%H%M')}.xlsx")


# ---------------------------------------------------------------------------
# O LOTE EM EXCEL — .xlsx de verdade, não CSV
#
# Pedido do dono: *"relatório do lote em Excel, por lote e de todos os lotes
# juntos"*. O CSV continua existindo, e não é redundância: ele sai em BLOCOS,
# e é o único que aguenta exportar a base larga sem estourar a memória. O
# Excel monta o arquivo inteiro antes de enviar — por isso é do LOTE, que tem
# dezenas de linhas, e não da base, que tem 59 mil.
# ---------------------------------------------------------------------------
def _responder_xlsx(conteudo: bytes, nome: str):
    from flask import Response
    return Response(
        conteudo,
        mimetype=("application/vnd.openxmlformats-officedocument"
                  ".spreadsheetml.sheet"),
        headers={"Content-Disposition": f'attachment; filename="{nome}"'})


@bp.route("/lote/excel")
@exige_consulta
def lote_excel_rota():
    """O lote DESTA pessoa, em Excel."""
    from . import lote, lote_excel
    from .horario import agora

    pessoa = auth.pessoa_atual()
    # Ver o comentário em `exportar_lote`: sem a pessoa, sai o lote antigo.
    try:
        montado = lote.montar(lote.ler(pessoa)["conteudo"])
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou montar o lote para o Excel")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui montar o lote",
            mensagem=f"{e}"), 500
    if not montado["quantidade"]:
        return render_template(
            "analisesps_erro.html", titulo="Lote vazio",
            mensagem="Não há SPs no lote para pôr na planilha."), 400

    try:
        conteudo = lote_excel.de_um_lote(
            montado, auth.nome_atual() or "Lote")
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou gerar o Excel do lote")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui gerar o Excel",
            mensagem=f"{e}. A exportação em CSV continua disponível."), 500

    return _responder_xlsx(
        conteudo, f"lote_{agora().strftime('%Y-%m-%d_%H%M')}.xlsx")


@bp.route("/lote/excel/todos")
@exige_consulta
def lote_excel_todos():
    """TODOS os lotes, uma aba por pessoa, com um resumo na frente.

    Serve para a pergunta que hoje não tem resposta em lugar nenhum: "quanto
    está separado para pagar, no total, somando o que cada um montou?". A tela
    do Lote mostra só o de quem está olhando."""
    from . import lote, lote_excel, preferencias
    from .horario import agora

    try:
        pessoas = preferencias.pessoas_conhecidas() if lote.por_pessoa() else []
        lotes = []
        for pessoa in pessoas:
            conteudo = lote.ler(pessoa["chave"])["conteudo"]
            if not (conteudo or "").strip():
                continue          # lote vazio não vira aba
            lotes.append({"nome": pessoa.get("nome") or pessoa["chave"],
                          "montado": lote.montar(conteudo)})
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou juntar os lotes para o Excel")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui juntar os lotes",
            mensagem=f"{e}"), 500

    if not lotes:
        return render_template(
            "analisesps_erro.html", titulo="Nenhum lote com SPs",
            mensagem="Ninguém tem SP no lote agora."), 400

    try:
        conteudo = lote_excel.de_todos(lotes)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou gerar o Excel de todos")
        return render_template(
            "analisesps_erro.html", titulo="Não consegui gerar o Excel",
            mensagem=f"{e}"), 500

    logger.info("Análise de SPs: Excel de %d lote(s) gerado.", len(lotes))
    return _responder_xlsx(
        conteudo, f"lotes_todos_{agora().strftime('%Y-%m-%d_%H%M')}.xlsx")


# ---------------------------------------------------------------------------
# APORTES E DEVOLUÇÕES NO OMIE — 20/09/2026
#
# Mora dentro de Configurações por pedido do dono ("ela pode ficar dentro de
# configurações"), e o lugar faz sentido por outro motivo também: a tela tem
# duas metades de naturezas diferentes. Em cima, o DE-PARA — coisa que se
# ajusta uma vez e se confere de vez em quando. Embaixo, o LANÇAMENTO, que é
# uso do dia. Configurações é onde a primeira metade pertence, e a segunda
# fica ao lado dela porque uma não funciona sem a outra estar em dia.
#
# ⚠️ TODAS AS ROTAS DAQUI SÃO `@exige_operador`. Ler não basta: mesmo o ensaio
# monta o pacote que iria para o OMIE, com conta, categoria e valor. E a
# gravação pede AINDA a senha de escrita, por cima do login.
# ---------------------------------------------------------------------------
def _contexto_dos_aportes() -> dict:
    """Tudo que a tela precisa, sem deixar nenhuma falha derrubá-la.

    A tela de aportes é também a que CONSERTA o de-para. Se ela cair porque o
    espelho do painel está vazio, não há por onde arrumar — foi assim que o
    módulo inteiro travou na estreia (03/09), e a lição está no histórico."""
    from . import aportes, aportes_de_para, aportes_omie

    ctx = {
        "operacoes": [(c, aportes.OPERACOES[c])
                      for c in aportes.ORDEM_DAS_OPERACOES],
        # O que cada operação FAZ, para a tela dizer isso assim que ele
        # escolher — antes de pedir valor, data ou qualquer outra coisa.
        "pernas_por_operacao": {c: aportes.resumo_das_pernas(c)
                                for c in aportes.ORDEM_DAS_OPERACOES},
        "papeis": aportes_de_para.PAPEIS,
        "papel_rotulo": aportes.PAPEL_ROTULO,
        "categorias_nomes": aportes.CATEGORIAS,
        "categoria_explicacao": aportes.CATEGORIA_EXPLICACAO,
        "contas_omie": [], "contas_lembradas": {}, "categorias": {},
        "obras": [],
        "fornecedores": [], "faltas": [], "erro_espelho": "",
        "historico": [], "orfaos": [], "em_duvida": [], "situacoes": [],
        "senha_configurada": aportes_omie.senha_configurada(),
    }
    try:
        ctx["contas_omie"] = aportes_de_para.contas_do_omie()
    except aportes_de_para.SemEspelho as e:
        ctx["erro_espelho"] = str(e)
    try:
        ctx["obras"] = aportes_de_para.obras()
    except aportes_de_para.SemEspelho as e:
        ctx["erro_espelho"] = ctx["erro_espelho"] or str(e)
    try:
        ctx["fornecedores"] = aportes_de_para.fornecedores("")
    except aportes_de_para.SemEspelho as e:
        ctx["erro_espelho"] = ctx["erro_espelho"] or str(e)

    # As contas são escolhidas em CADA lançamento (20/09/2026: *"não quero
    # travar a conta Provedora e a da Parceria, tem mais de uma situação"*). O
    # que fica guardado é só a última usada, para vir pré-escolhida.
    ctx["contas_lembradas"] = aportes_de_para.contas_lembradas()
    ctx["categorias"] = aportes_de_para.descobrir_categorias()
    ctx["faltas"] = aportes_de_para.falta_configurar()

    # AS CINCO SITUAÇÕES, do jeito que ele desenhou a tabela: conta + sentido
    # + categoria. Uma categoria aparece em mais de uma linha, e é isso que a
    # lista de quatro categorias escondia.
    # Hoje cada situação tem a SUA categoria, então nenhuma chave se repete.
    # A guarda fica de pé assim mesmo: se um dia duas situações voltarem a
    # dividir uma categoria, dois campos com o mesmo nome fariam o segundo
    # sobrescrever o primeiro com um valor que o dono não olhou.
    ja_vistas = set()
    for papel, sentido, chave in aportes.SITUACOES:
        achado = ctx["categorias"].get(chave) or {}
        ctx["situacoes"].append({
            "primeira": chave not in ja_vistas,
            "papel": papel,
            "papel_rotulo": aportes.PAPEL_ROTULO[papel],
            "sentido": sentido,
            "sentido_rotulo": aportes.SENTIDO_ROTULO[sentido],
            "natureza_rotulo": aportes.NATUREZA_ROTULO[aportes.NATUREZA[sentido]],
            "categoria_chave": chave,
            "categoria_nome": aportes.CATEGORIAS[chave],
            "categoria_explicacao": aportes.CATEGORIA_EXPLICACAO.get(chave, ""),
            "codigo": achado.get("codigo") or "",
            "situacao": achado.get("situacao") or "",
            "transferencia": achado.get("transferencia") or "",
        })
        ja_vistas.add(chave)
    ctx["historico"] = aportes_omie.historico(30)
    ctx["orfaos"] = aportes_omie.orfaos()
    ctx["em_duvida"] = aportes_omie.em_duvida()
    return ctx


@bp.route("/aportes")
@exige_operador
def tela_aportes():
    """Lançar aporte e devolução de aporte no OMIE."""
    return render_template("analisesps_aportes.html", **_contexto_dos_aportes())


@bp.route("/aportes/de-para", methods=["POST"])
@exige_operador
def aportes_de_para_gravar():
    """Aponta qual conta do OMIE é cada papel e confirma o código de cada
    categoria. É a tela em que o dono conserta o de-para, e por isso ela grava
    o que dá e DIZ o que não deu, em vez de recusar o lote inteiro."""
    from . import aportes_de_para

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    recados, problemas = [], []

    # ⚠️ SÓ CATEGORIA. A conta não se cadastra: é escolhida em cada lançamento.
    from .aportes import CATEGORIAS
    for chave in CATEGORIAS:
        campo = request.form.get(f"categoria_{chave}")
        if campo is None:
            continue
        try:
            aportes_de_para.guardar_categoria(chave, campo.strip(), quem)
            recados.append(f"Categoria \"{CATEGORIAS[chave]}\" anotada.")
        except Exception as e:  # noqa: BLE001
            logger.exception("Aportes: falhou guardar a categoria %s", chave)
            problemas.append(str(e))

    aviso = "; ".join(problemas) if problemas else (
        f"{len(recados)} ajuste(s) gravado(s)." if recados else "Nada mudou.")
    return redirect(url_for("analisesps.tela_aportes", aviso=aviso))


def _planos_do_pedido(dados: dict) -> list:
    """Um plano por linha de data e valor. Usado pelo ensaio e pela gravação —
    as duas TÊM de enxergar os mesmos planos, senão o dono confere uma coisa e
    o OMIE recebe outra.

    ⚠️ Os grupos vêm da TELA quando ela os manda de volta. É isso que faz o
    que foi conferido ser exatamente o que é gravado: sem passar os grupos, a
    gravação sortearia números novos e o pacote conferido seria outro."""
    from . import aportes, aportes_de_para

    comuns = dict(
        operacao=str(dados.get("operacao") or "").strip(),
        conta_origem=dados.get("conta_origem") or None,
        conta_destino=dados.get("conta_destino") or None,
        fornecedor=dados.get("fornecedor") or None,
        fornecedor_nome=str(dados.get("fornecedor_nome") or ""),
        obra=str(dados.get("obra") or ""),
        obra_nome=str(dados.get("obra_nome") or ""),
        quem=auth.nome_atual() or "",
        baixar=bool(dados.get("baixar", True)),
        descricoes=aportes_de_para.descricoes_das_contas(),
        categorias=aportes_de_para.categorias_resolvidas(),
        observacoes=dados.get("observacoes") or {},
    )

    parcelas = dados.get("parcelas")
    if not parcelas:
        # Uma linha só: o caminho de sempre, escrito do mesmo jeito.
        parcelas = [{"data": dados.get("data"), "valor": dados.get("valor")}]

    grupos = dados.get("grupos") or []
    planos = []
    for i, (data, valor) in enumerate(aportes.ler_parcelas(parcelas)):
        planos.append(aportes.planejar(
            valor=valor, data=data,
            grupo=str(grupos[i]) if i < len(grupos) else "", **comuns))
    return planos


@bp.route("/api/aportes/ensaiar", methods=["POST"])
@exige_operador
def aportes_ensaiar():
    """O ensaio: mostra o que SERIA gravado, e não grava nada.

    Não pede a senha de escrita de propósito — conferir tem de ser barato, ou
    ninguém confere. A senha vale para o que escreve."""
    from . import aportes, aportes_de_para, aportes_omie

    dados = request.get_json(silent=True) or {}
    try:
        planos = _planos_do_pedido(dados)
    except aportes.ErroDeRegra as e:
        return {"ok": False, "erro": str(e)}, 400
    except Exception as e:  # noqa: BLE001
        logger.exception("Aportes: falhou montar o plano")
        return {"ok": False, "erro": f"Não consegui montar o lançamento: {e}"}, 500

    lancamentos = []
    for plano in planos:
        contas = [t["id_conta_corrente"] for t in plano["titulos"]]
        semelhantes = aportes_de_para.consultar_semelhantes(
            plano["valor"], plano["data"], contas)
        lancamentos.append({
            "plano": _plano_para_tela(plano),
            "ensaio": _ensaio_para_tela(aportes_omie.ensaiar(plano)),
            "avisos": aportes.criticar(plano, semelhantes),
        })
    return {
        "ok": True,
        "lancamentos": lancamentos,
        "grupos": [p["grupo"] for p in planos],
        "quantos": len(planos),
        "total": round(sum(p["valor"] for p in planos), 2),
        "quantos_titulos": sum(len(p["titulos"]) for p in planos),
    }


@bp.route("/api/aportes/gravar", methods=["POST"])
@exige_operador
def aportes_gravar():
    """Grava no OMIE. Pede a senha de escrita POR CIMA do login."""
    from . import aportes, aportes_omie

    dados = request.get_json(silent=True) or {}
    try:
        aportes_omie.conferir_senha(dados.get("senha"))
    except aportes_omie.SemAutorizacao as e:
        return {"ok": False, "erro": str(e)}, 403

    try:
        planos = _planos_do_pedido(dados)
    except aportes.ErroDeRegra as e:
        return {"ok": False, "erro": str(e)}, 400

    quem = auth.nome_atual() or auth.ROTULOS.get(auth.perfil_atual(), "")
    try:
        resultados = aportes_omie.gravar_varios(planos, quem)
    except Exception as e:  # noqa: BLE001 — a mensagem do OMIE vai inteira
        logger.exception("Aportes: falhou gravar no OMIE")
        return {"ok": False, "erro": f"Não consegui falar com o OMIE: {e}"}, 502

    # Lembrar as contas usadas, para virem pré-escolhidas na próxima vez.
    # Só das que deram certo: conta de lançamento que falhou não é exemplo.
    from . import aportes_de_para
    for plano, resultado in zip(planos, resultados):
        if not resultado.get("ok"):
            continue
        for t in plano["titulos"]:
            aportes_de_para.lembrar_conta(plano["operacao"], t["papel"],
                                          t["id_conta_corrente"], quem)

    deram_certo = sum(1 for r in resultados if r.get("ok"))
    logger.info("Análise de SPs: %s lançou %d aporte(s) (%s) — %d ok.",
                quem or "sem nome", len(planos),
                planos[0]["operacao"] if planos else "?", deram_certo)

    return {
        # ⚠️ `ok` só é verdadeiro quando TODAS entraram. Um lote meio gravado
        # com cara de sucesso é o jeito mais rápido de alguém lançar de novo
        # as que já entraram.
        "ok": deram_certo == len(resultados) and bool(resultados),
        "quantos": len(resultados),
        "deram_certo": deram_certo,
        "lancamentos": [{
            "grupo": plano["grupo"],
            "numero": plano["numero_documento"],
            "data": plano["data_br"],
            "valor": plano["valor"],
            "ok": bool(resultado.get("ok")),
            "erro": resultado.get("erro", ""),
            "avisos": resultado.get("avisos", []),
            "orfaos": resultado.get("orfaos", []),
            "titulos": [{"papel": l["titulo"]["papel_rotulo"],
                         "sentido": l["titulo"]["sentido_rotulo"],
                         "conta": l["titulo"]["conta_descricao"],
                         "categoria": l["titulo"]["categoria_nome"],
                         "codigo": l["codigo"], "baixado": l["baixado"],
                         "erro": l["erro"]}
                        for l in resultado.get("titulos", [])],
        } for plano, resultado in zip(planos, resultados)],
    }


@bp.route("/api/aportes/conferir", methods=["POST"])
@exige_operador
def aportes_conferir():
    """Lê de volta no OMIE o que foi gravado, e mostra lado a lado.

    ⚠️ NASCEU DE *"tudo verdinho, e no OMIE não tá aparecendo na conta
    provedora"*. A tela dizia mais do que sabia: "gravado" significava apenas
    que o OMIE aceitou e devolveu um número — não que o título ficou na conta
    que mandamos, com a categoria que mandamos, nem que a baixa pegou.

    É LEITURA. Não altera, não exclui, não baixa. Por isso não pede a senha de
    escrita: conferir tem de ser barato, ou ninguém confere."""
    from . import aportes_omie

    grupo = str((request.get_json(silent=True) or {}).get("grupo") or "").strip()
    if not grupo:
        return {"ok": False, "erro": "Diga qual lançamento conferir."}, 400

    registrados = aportes_omie.titulos_do_grupo(grupo)
    if not registrados:
        return {"ok": False,
                "erro": f"Não tenho registro nenhum do lançamento {grupo}."}, 404

    try:
        no_omie = aportes_omie.conferir_no_omie(
            [{"codigo": r["codigo"], "natureza": r["natureza"]}
             for r in registrados])
    except Exception as e:  # noqa: BLE001
        logger.exception("Aportes: falhou conferir no OMIE")
        return {"ok": False, "erro": f"Não consegui falar com o OMIE: {e}"}, 502

    # O CONFRONTO é a resposta: o que mandamos ao lado do que ele guardou.
    # Divergência aqui é o defeito que nenhuma tela mostraria sozinha.
    linhas = []
    for reg, omie in zip(registrados, no_omie):
        divergencias = []
        if omie.get("achou"):
            if (str(omie.get("id_conta_corrente") or "")
                    != str(reg.get("id_conta_corrente") or "")):
                divergencias.append(
                    f"A conta é outra: mandei {reg.get('id_conta_corrente')}, "
                    f"o OMIE guardou {omie.get('id_conta_corrente')}.")
            if (str(omie.get("codigo_categoria") or "").strip()
                    != str(reg.get("codigo_categoria") or "").strip()):
                divergencias.append(
                    f"A categoria é outra: mandei {reg.get('codigo_categoria')}, "
                    f"o OMIE guardou {omie.get('codigo_categoria')}.")
            baixado_la = str(omie.get("baixa_realizada") or "").upper()
            if reg.get("baixado") and not baixado_la.startswith("S"):
                divergencias.append(
                    "Eu dei a baixa por feita, mas o OMIE diz que o título "
                    "NÃO está baixado — por isso ele não aparece no extrato "
                    "da conta.")
        linhas.append({"registrado": reg, "omie": omie,
                       "divergencias": divergencias})

    return {"ok": True, "grupo": grupo, "linhas": linhas,
            "tudo_certo": all(not l["divergencias"] and l["omie"].get("achou")
                              for l in linhas)}


@bp.route("/api/aportes/fornecedores")
@exige_operador
def aportes_fornecedores():
    """Procura fornecedor pelo nome ou pelo documento."""
    from . import aportes_de_para

    try:
        achados = aportes_de_para.fornecedores(request.args.get("q", ""))
    except aportes_de_para.SemEspelho as e:
        return {"ok": False, "erro": str(e)}, 503
    return {"ok": True, "fornecedores": achados}


def _plano_para_tela(plano: dict) -> dict:
    """O plano sem a data como objeto — JSON não sabe o que fazer com ela."""
    limpo = dict(plano)
    limpo["data"] = plano["data_br"]
    limpo["titulos"] = [dict(t, data=t["data_br"]) for t in plano["titulos"]]
    return limpo


def _ensaio_para_tela(ensaio: list) -> list:
    return [{"url": i["url"], "chamada": i["chamada"], "param": i["param"],
             "baixa": i["baixa"],
             "papel": i["titulo"]["papel_rotulo"],
             "sentido": i["titulo"]["sentido_rotulo"]} for i in ensaio]


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
