# -*- coding: utf-8 -*-
"""
Login da Análise de SPs — o padrão é NEGAR.

São os pagamentos da empresa: credor, valor, CPF/CNPJ, código de barras,
comprovante. Nada abre sem senha, nem a tela que "só olha".

DOIS PERFIS, DUAS SENHAS:

  - CONSULTA   vê tudo e exporta. Não altera nada.
  - OPERADOR   tudo o que o Consulta faz, mais alterar status, agendar, gerar
               BeeVale, validar, cancelar SP, ratear, sincronizar e configurar.

Cada um tem a sua senha, numa variável do Render. Perfil sem senha configurada
simplesmente não existe — ninguém entra por ele. Falha fechado, sempre.

DESDE 25/09/2026 HÁ TAMBÉM CADASTRO PRÓPRIO — e os dois caminhos convivem:

  - **a senha do Render** (as duas acima) é o **MESTRE**: vê todas as telas,
    configura, aplica migração, mexe no certificado e cadastra as pessoas;
  - **usuário e senha próprios** (`usuarios.py`, migração 023) alcançam SÓ as
    telas marcadas, e alteram dado só se estiverem marcados como operador.

⚠️ A SENHA DO RENDER CONTINUA VALENDO, e não é preguiça: é o que impede o dono
de se trancar para fora. Se a migração não tiver rodado, se ele apagar o próprio
cadastro sem querer, se o banco cair — a senha do Render ainda entra. Um
cadastro capaz de trancar o único administrador não é segurança, é armadilha.

O antigo comentário que ficava aqui dizia que cadastro de usuário era "peso sem
retorno" porque o módulo tinha prazo de validade. Deixou de ser verdade quando
o dono pediu o contrário, com todas as letras: *"vou poder cadastrar o operador,
definir a senha, definir as telas que ele tem acesso. Aí vai ter um usuário
master, e os outros a gente define as permissões."*

QUEM AUTENTICA É A SENHA; O NOME APENAS IDENTIFICA. Desde 04/09/2026 a pessoa
também informa o NOME dela ao entrar. Isso não é login: digitar "Marcelo" não
dá poder nenhum a mais — quem decide o que se pode fazer continua sendo a
senha, e só ela. O nome serve para três coisas concretas, todas pedidas pelo
dono: o lote passa a ser de cada um, os filtros ficam guardados por pessoa, e
o registro de alterações passa a dizer QUEM mexeu, não só que perfil.

Não confunda os dois. Se um dia for preciso IMPEDIR que alguém se passe por
outro, o lugar certo continua sendo o cadastro de usuários do ERP — aqui o
nome é uma etiqueta honesta entre colegas, não uma tranca.

A DECLARAÇÃO É OBRIGATÓRIA. Toda rota do blueprint diz o que exige, com
`@exige_consulta`, `@exige_operador` ou `@publica("motivo")`. Rota que esquece
de declarar é RECUSADA pelo guarda, não liberada — é a regra do ERP, e existe
porque o esquecimento é o modo de falha mais comum. Esquecer fecha; nunca abre.
"""
from __future__ import annotations

import hmac
import logging
import os
from functools import wraps

from flask import current_app, redirect, request, session, url_for

logger = logging.getLogger("analisesps.auth")

CHAVE_SESSAO = "analisesps_perfil"
CHAVE_NOME = "analisesps_nome"

# Quem entrou por cadastro próprio guarda o NÚMERO dele aqui. Sessão sem este
# número é o mestre — quem digitou a senha do Render.
#
# ⚠️ GUARDA-SE O NÚMERO, NÃO AS PERMISSÕES. As telas de cada pessoa são lidas
# do banco a cada pedido: tirar uma tela de alguém tem de valer NA HORA, não
# quando ele fechar o navegador.
CHAVE_USUARIO = "analisesps_usuario_id"

# Teto do nome. Não é regra de negócio: é para o campo não virar porta de
# entrada de texto gigante, já que ele vai para o banco e para a tela.
MAX_NOME = 40

# ⚠️ O COOKIE QUE LEMBRAVA O NOME FOI EMBORA em 25/09/2026, junto com a lista
# de nomes da entrada. Não havia mais o que lembrar: quem entra digita o
# usuário do cadastro dele, e o nome vem de lá. Guardar a SENHA "para
# facilitar" seria outra conversa, e a resposta continua sendo não — são os
# pagamentos da empresa.

CONSULTA = "consulta"
OPERADOR = "operador"

ROTULOS = {CONSULTA: "Consulta", OPERADOR: "Operador"}

# Variável de ambiente que guarda a senha de cada perfil.
VARIAVEL_SENHA = {
    CONSULTA: "ANALISESPS_SENHA_CONSULTA",
    OPERADOR: "ANALISESPS_SENHA_OPERADOR",
}

# Atributo que as declarações penduram na função da rota.
_EXIGENCIA = "_analisesps_exigencia"


# ---------------------------------------------------------------------------
# Declarações — uma por rota, obrigatória
# ---------------------------------------------------------------------------
def exige_consulta(f):
    """Basta estar logado, em qualquer perfil. Para telas que só mostram."""
    setattr(f, _EXIGENCIA, CONSULTA)
    return f


def exige_operador(f):
    """Só o Operador. Para tudo que altera dado, aqui ou na planilha."""
    setattr(f, _EXIGENCIA, OPERADOR)
    return f


def publica(motivo: str):
    """Responde sem login. O motivo fica escrito e é lido por quem revisar."""
    def decorador(f):
        setattr(f, _EXIGENCIA, ("publica", motivo))
        return f
    return decorador


# ---------------------------------------------------------------------------
# Senhas
# ---------------------------------------------------------------------------
def confere(digitada: str, esperada: str) -> bool:
    """Compara duas senhas em tempo constante, aceitando acento.

    `hmac.compare_digest` com TEXTO só aceita ASCII: uma senha com "ç" ou "ã"
    faz a comparação ESTOURAR, e o login vira erro 500 em vez de "senha
    incorreta" — quem digitou nunca descobre que só errou a senha. Foi um
    defeito real, encontrado no painel em 05/09/2026 e trazido para cá no
    mesmo dia, porque o código era o mesmo.

    Comparar os BYTES resolve, sem perder o tempo constante: a comparação
    continua não vazando, pelo relógio, quantos caracteres acertaram."""
    return hmac.compare_digest(str(digitada or "").encode("utf-8"),
                               str(esperada or "").encode("utf-8"))


def senha_do_perfil(perfil: str) -> str:
    return os.getenv(VARIAVEL_SENHA[perfil], "").strip()


def perfis_configurados() -> list[str]:
    """Quais perfis têm senha. Lista vazia = ninguém entra, e a tela diz isso."""
    return [p for p in (OPERADOR, CONSULTA) if senha_do_perfil(p)]


def identificar(digitada: str) -> str | None:
    """Descobre o perfil pela senha. Devolve None quando nenhuma confere.

    Compara SEMPRE as duas, mesmo depois de achar — comparação em tempo
    constante só serve se o tempo total também for constante. E compara o
    Operador primeiro: se as duas senhas forem iguais por descuido, quem entra
    fica com o perfil de MAIOR poder, que é o que o dono espera ao digitar a
    senha que ele considera a principal. O aviso disso sai no log."""
    digitada = str(digitada or "")
    achado = None
    for perfil in (OPERADOR, CONSULTA):
        esperada = senha_do_perfil(perfil)
        if not esperada:
            continue
        if confere(digitada, esperada) and achado is None:
            achado = perfil
    if (achado and senha_do_perfil(OPERADOR)
            and senha_do_perfil(OPERADOR) == senha_do_perfil(CONSULTA)):
        logger.warning("Análise de SPs: as senhas de Operador e Consulta estão "
                       "IGUAIS. Todo mundo entra como Operador. Troque uma delas.")
    return achado


# ---------------------------------------------------------------------------
# Sessão
# ---------------------------------------------------------------------------
def perfil_atual() -> str | None:
    perfil = session.get(CHAVE_SESSAO)
    return perfil if perfil in (CONSULTA, OPERADOR) else None


def esta_logado() -> bool:
    return perfil_atual() is not None


def pode_operar() -> bool:
    """Pode alterar dado? Para o mestre é o perfil da senha; para quem tem
    cadastro, a marcação dele — lida do banco a cada pedido."""
    if not esta_logado():
        return False
    if e_porta_de_emergencia():
        return perfil_atual() == OPERADOR
    pessoa = usuario_da_sessao()
    return bool(pessoa and pessoa.get("pode_operar"))


def entrar_na_sessao(perfil: str, nome: str = "", usuario_id=None) -> None:
    session[CHAVE_SESSAO] = perfil
    session[CHAVE_NOME] = limpar_nome(nome)
    if usuario_id is None:
        session.pop(CHAVE_USUARIO, None)       # o mestre
    else:
        session[CHAVE_USUARIO] = int(usuario_id)
    session.permanent = False      # a sessão morre quando o navegador fecha


def sair_da_sessao() -> None:
    session.pop(CHAVE_SESSAO, None)
    session.pop(CHAVE_NOME, None)
    session.pop(CHAVE_USUARIO, None)


def e_mestre() -> bool:
    """Vê tudo, configura e cadastra gente.

    São dois jeitos de ser mestre, e a ordem importa:

      1. entrou pela **porta de emergência** (a senha do Render, com o campo
         de usuário em branco) — não há cadastro por trás, e por isso não há
         o que consultar;
      2. entrou pelo **cadastro** e a pessoa está marcada como mestre
         (migração 024). É o caminho normal desde 25/09/2026.
    """
    if not esta_logado():
        return False
    if not session.get(CHAVE_USUARIO):
        return True                      # porta de emergência
    pessoa = usuario_da_sessao()
    return bool(pessoa and pessoa.get("mestre"))


def usuario_da_sessao():
    """A pessoa cadastrada que está logada, ou None quando é o mestre.

    VAI AO BANCO, e de propósito: tirar uma tela de alguém tem de valer na
    hora. Para não pagar isso várias vezes na mesma tela, o resultado fica
    guardado no `g` do Flask, que morre no fim do pedido.

    Se a pessoa foi apagada ou desativada no meio da sessão, ela cai fora
    agora — não no próximo login."""
    uid = session.get(CHAVE_USUARIO)
    if not uid:
        return None
    from flask import g
    guardado = getattr(g, "_analisesps_usuario", _NAO_PERGUNTEI)
    if guardado is not _NAO_PERGUNTEI:
        return guardado
    from . import usuarios
    try:
        pessoa = usuarios.buscar_por_id(uid)
    except Exception:  # noqa: BLE001 — banco fora do ar não vira acesso total
        logger.exception("Análise de SPs: não consegui ler o usuário %s", uid)
        pessoa = None
    if pessoa is None:
        sair_da_sessao()
    g._analisesps_usuario = pessoa
    return pessoa


_NAO_PERGUNTEI = object()


def telas_permitidas() -> set[str] | None:
    """As telas que a pessoa logada alcança. `None` quer dizer TODAS (o mestre).

    Distinguir `None` de conjunto vazio é o ponto: vazio quer dizer NENHUMA, e
    é o que acontece com um cadastro sem tela marcada."""
    if not esta_logado() or e_mestre():
        return None
    pessoa = usuario_da_sessao()
    return set(pessoa.get("telas") or []) if pessoa else set()


def e_porta_de_emergencia() -> bool:
    """Entrou pela senha do Render, sem cadastro por trás.

    A tela avisa, porque essa porta não é o caminho do dia a dia — e quem
    entra por ela não tem lote nem filtros próprios, já que não tem nome."""
    return esta_logado() and not session.get(CHAVE_USUARIO)


# ---------------------------------------------------------------------------
# A pessoa
# ---------------------------------------------------------------------------
def limpar_nome(bruto: str) -> str:
    """O nome como ele vai aparecer: sem espaço sobrando e sem tamanho absurdo.

    Não corrige maiúscula nem acento — nome de gente é como a pessoa escreve."""
    nome = " ".join(str(bruto or "").split())
    return nome[:MAX_NOME]


def chave_pessoa(nome: str) -> str:
    """A forma usada para GUARDAR (lote e preferências), não para mostrar.

    "Marcelo", "marcelo" e "MARCELO" têm de ser a mesma pessoa — senão quem
    digita com a inicial minúscula um dia encontra o lote vazio e conclui que
    o sistema perdeu o trabalho dele. Acento também não pode separar: "João" e
    "Joao" são a mesma pessoa para quem digita com pressa."""
    import unicodedata
    nome = limpar_nome(nome).lower()
    sem_acento = unicodedata.normalize("NFKD", nome)
    return "".join(c for c in sem_acento if not unicodedata.combining(c))


def nome_atual() -> str:
    """O nome como a pessoa digitou — é o que vai para a tela e para o log."""
    return limpar_nome(session.get(CHAVE_NOME, ""))


def pessoa_atual() -> str:
    """A chave da pessoa logada. Vazia quando ninguém se identificou.

    Chave vazia é um estado legítimo, não um erro: uma sessão aberta antes
    desta mudança continua valendo até a pessoa sair. Ela cai no lote e nas
    preferências de pessoa '' — que é justamente o espaço do "antes"."""
    return chave_pessoa(session.get(CHAVE_NOME, ""))


# ---------------------------------------------------------------------------
# QUAL TELA É CADA ROTA — e o que é só do mestre
# ---------------------------------------------------------------------------
# Isto existe porque proteger tela por tela é como esse tipo de coisa vaza. A
# pergunta "esta pessoa pode abrir esta rota?" é respondida NUM lugar só, com
# uma tabela, e há teste exigindo que toda rota do módulo esteja classificada.
#
# ⚠️ O PADRÃO É NEGAR. Rota que ninguém classificou aqui NÃO ABRE para quem tem
# cadastro próprio — o log diz o que fazer. É a mesma escolha da declaração
# `@exige_*`: esquecer fecha, nunca abre. Para o mestre nada disto se aplica.

# As telas do menu que NÃO se pode liberar para ninguém. Configurações é de
# onde se aplica migração, se troca o certificado digital e se cadastra gente —
# é a tela que conserta o módulo, e ela é do dono.
SO_DO_MESTRE_POR_TELA = frozenset({"configuracoes", "folha_rateio"})
# ⚠️ "folha_rateio" entrou em 26/09/2026: ela define para qual obra vai o
# salário de alguém, toda quinzena, até alguém mudar. O DP opera a folha; o
# rateio é do dono — decisão dele: *"o usuário do DP faz a leitura, mas o
# usuário master, que sou eu, eu gero o arquivo"*.

# Rotas que são só do mestre, por escreverem no OMIE, mexerem em segredo ou
# configurarem o módulo. Nome exato, para não pegar vizinho por engano.
SO_DO_MESTRE = frozenset({
    "analisesps.configuracoes",
    "analisesps.tela_folha_rateio",      # decide o rateio do salário
    "analisesps.folha_rateio_gravar",
    "analisesps.folha_rateio_apagar",
    "analisesps.folha_rateio_simular",
    "analisesps.migrar",                 # aplica migração no banco
    "analisesps.gravar_pessoas",         # a lista de nomes da entrada
    "analisesps.gravar_pasta_drive",
    "analisesps.conferir_drive",
    "analisesps.subir_certificado",      # o A1 da empresa
    "analisesps.conferir_certificado",
    "analisesps.remover_certificado",
    "analisesps.tela_credores",          # conserta nome de credor na base
    "analisesps.aplicar_credor",
    "analisesps.consultar_cnpj_credor",
    "analisesps.sps_do_nome_credor",
})

# Famílias inteiras do mestre, por prefixo — rota nova dentro delas já nasce
# fechada, em vez de esperar alguém lembrar de acrescentá-la acima.
SO_DO_MESTRE_POR_PREFIXO = (
    "analisesps.aportes_",     # lançam aporte e devolução direto no OMIE
    "analisesps.tela_aportes",
    "analisesps.usuarios_",    # o próprio cadastro de acesso
)

# Rota de apoio: não é tela de dado, e vale para qualquer pessoa logada. Cada
# uma com o motivo escrito, porque é uma exceção ao padrão de negar.
APOIO = {
    "analisesps.inicio": "só manda para a primeira tela",
    "analisesps.sair": "encerrar a sessão não pode depender de permissão",
    "analisesps.escolher_colunas": "guarda as colunas DA PRÓPRIA pessoa",
    "analisesps.frescor": "diz se a base mudou; não devolve dado da empresa",
    "analisesps.andamento": "diz se a sincronização está rodando",
}

# Endpoint -> as telas que dão direito a ele. Quem tem QUALQUER uma delas
# entra; quem não tem nenhuma, não.
TELA_DA_ROTA = {
    "analisesps.solicitacoes": ("solicitacoes",),
    "analisesps.detalhe": ("solicitacoes",),
    "analisesps.exportar": ("solicitacoes",),
    "analisesps.alterar": ("solicitacoes",),
    "analisesps.enviar_ao_lote": ("solicitacoes",),
    "analisesps.sem_risco": ("solicitacoes",),
    "analisesps.validar": ("solicitacoes",),
    # O BeeVale e os códigos de pagamento servem as duas pontas do mesmo
    # trabalho: quem monta a lista e quem paga. Uma tela das duas basta.
    "analisesps.beevale_cadastro": ("solicitacoes", "lote"),
    "analisesps.beevale_gerar": ("solicitacoes", "lote"),
    "analisesps.beevale_executar": ("solicitacoes", "lote"),
    "analisesps.codigos": ("solicitacoes", "lote"),
    "analisesps.tela_lote": ("lote",),
    "analisesps.exportar_lote": ("lote",),
    "analisesps.lote_excel_rota": ("lote",),
    "analisesps.lote_excel_todos": ("lote",),
    "analisesps.lote_pdf": ("lote",),
    "analisesps.tela_comprovantes": ("comprovantes",),
    "analisesps.enviar_comprovantes": ("comprovantes",),
    "analisesps.estado_comprovantes": ("comprovantes",),
    "analisesps.reprocessar_comprovante": ("comprovantes",),
    "analisesps.relatorio": ("relatorio",),
    "analisesps.exportar_relatorio": ("relatorio",),
    "analisesps.relatorio_pdf": ("relatorio",),
    "analisesps.tela_fiscal": ("fiscal",),
    "analisesps.tela_planilha": ("fiscal",),
    "analisesps.nota_documento": ("fiscal",),
    "analisesps.conferir_nota_fiscal": ("fiscal",),
    "analisesps.comparar_fiscal": ("fiscal",),
    "analisesps.reconferir_fiscal": ("fiscal",),
    "analisesps.confirmar_fiscal": ("fiscal",),
    "analisesps.decidir_fiscal_a_mao": ("fiscal",),
    "analisesps.pedir_ia_fiscal": ("fiscal",),
    "analisesps.importar_relatorio_fsist": ("fiscal",),
    "analisesps.tela_agenda": ("agenda",),
    "analisesps.tela_conciliacao": ("conciliacao",),
    "analisesps.calendario": ("calendario",),
    "analisesps.auditoria": ("auditoria",),
    "analisesps.exportar_auditoria": ("auditoria",),
    "analisesps.ratear": ("ratear",),
    # O CADASTRO É LEITURA, e por isso não é só do mestre: quem opera a folha
    # precisa conferir o valor do auxílio de alguém e chegar ao card. A tela não
    # escreve nada — o dado nasce no Pipefy. O RATEIO, que decide para qual obra
    # vai o salário, continua só do mestre.
    "analisesps.tela_colaboradores": ("colaboradores",),
    "analisesps.tela_bradesco": ("bradesco",),
    "analisesps.log": ("log",),
}

# Famílias por prefixo, pelo mesmo motivo do mestre: a conciliação tem vinte
# rotas e vai ter mais. Todas são a mesma tela.
TELA_POR_PREFIXO = (
    ("analisesps.conciliacao", ("conciliacao",)),
)


def telas_da_rota(endpoint: str):
    """As telas que dão direito a esta rota, ou None se ninguém classificou."""
    if endpoint in TELA_DA_ROTA:
        return TELA_DA_ROTA[endpoint]
    for prefixo, telas in TELA_POR_PREFIXO:
        if endpoint.startswith(prefixo):
            return telas
    return None


def e_so_do_mestre(endpoint: str) -> bool:
    return (endpoint in SO_DO_MESTRE
            or endpoint.startswith(SO_DO_MESTRE_POR_PREFIXO))


def telas_para_o_menu(telas):
    """Filtra a lista de telas do menu pelo que a pessoa logada alcança.

    Mostrar no menu uma tela que responde 404 seria pior do que não mostrar:
    a pessoa clica, não entende, e liga para o dono."""
    permitidas = telas_permitidas()
    if permitidas is None:
        return list(telas)
    return [t for t in telas if t[0] in permitidas]


# ---------------------------------------------------------------------------
# O guarda
# ---------------------------------------------------------------------------
# A folha de estilo. É o único endpoint que o próprio Flask cria, então não há
# onde pendurar a declaração nele — e sem esta linha a tela de login abre sem
# estilo nenhum. Não revela dado: é o mesmo CSS para todo mundo, e serviria de
# qualquer jeito a quem já vê a tela de entrada.
ENDPOINT_ESTILO = "analisesps.static"


def exigir_login():
    """Roda antes de cada rota do blueprint. None = pode seguir."""
    endpoint = request.endpoint or ""
    if endpoint == ENDPOINT_ESTILO:
        return None
    funcao = current_app.view_functions.get(endpoint)
    exigencia = getattr(funcao, _EXIGENCIA, None)

    if exigencia is None:
        # Rota nova que esqueceu de declarar. Fecha e diz o que fazer — no log,
        # não na tela, para não ensinar a estrutura a quem está fuçando.
        logger.error("Análise de SPs: a rota '%s' não declarou exigência de "
                     "acesso. Ponha @exige_consulta, @exige_operador ou "
                     "@publica('motivo') nela. Enquanto isso, está fechada.",
                     endpoint)
        return _recusar()

    if isinstance(exigencia, tuple):        # @publica("motivo")
        return None

    if not esta_logado():
        return redirect(url_for("analisesps.entrar", proximo=request.full_path))

    # ------------------------------------------------------------------
    # PRIMEIRO O ALCANCE, DEPOIS A ALÇADA — e a ordem importa.
    #
    # Quem tem cadastro próprio alcança só as telas marcadas para ele. Essa
    # conferência vem ANTES da de "pode alterar" de propósito: responder "sem
    # permissão" numa rota que a pessoa não deveria nem saber que existe
    # confirma a existência dela. Fora do alcance é "não encontrado"; dentro
    # do alcance e sem alçada é "sem permissão", que é informação honesta.
    #
    # O mestre (senha do Render) passa direto — ele vê tudo, por definição.
    # ------------------------------------------------------------------
    if not e_mestre():
        pessoa = usuario_da_sessao()
        if pessoa is None:
            # apagado ou desativado no meio da sessão: cai fora agora
            return redirect(url_for("analisesps.entrar"))

        if e_so_do_mestre(endpoint):
            logger.warning("Análise de SPs: %s tentou abrir %s, que é só do "
                           "administrador.", pessoa["usuario"], endpoint)
            return _recusar()

        if endpoint not in APOIO:
            telas = telas_da_rota(endpoint)
            if telas is None:
                # Rota nova que ninguém classificou. Fecha para quem tem
                # cadastro e diz o que fazer — no log, não na tela. Para o
                # mestre continua aberta, o que evita o pior dos mundos: o dono
                # descobrindo a coisa por um 404 na cara.
                logger.error("Análise de SPs: a rota '%s' não está em "
                             "TELA_DA_ROTA nem em APOIO (auth.py). Enquanto "
                             "isso, está fechada para quem tem cadastro "
                             "próprio. Classifique-a.", endpoint)
                return _recusar()
            if not (set(telas) & set(pessoa.get("telas") or [])):
                return _recusar()

    if exigencia == OPERADOR and not pode_operar():
        return _sem_permissao()

    return None


def _recusar():
    from flask import render_template
    return render_template("analisesps_erro.html",
                           titulo="Não encontrado",
                           mensagem="Esta página não existe."), 404


def _sem_permissao():
    """Quem entrou como Consulta tentou uma ação de Operador.

    Aqui dizer "sem permissão" é correto, e diferente do caso do ERP: a pessoa
    já está autenticada e a tela existe para ela — o que falta é alçada. Não há
    existência de registro sendo revelada."""
    from flask import render_template
    return render_template(
        "analisesps_erro.html",
        titulo="Sem permissão",
        mensagem="O seu acesso vê e exporta, mas não altera. Para esta ação, "
                 "é preciso ser operador — quem cuida do sistema pode marcar "
                 "isso no seu cadastro, em Configurações."), 403


def exigir_operador_json(f):
    """Mesma trava, para as rotas que respondem JSON em vez de tela."""
    @wraps(f)
    def dentro(*a, **kw):
        if not pode_operar():
            return {"ok": False,
                    "erro": "O seu acesso vê, mas não altera dados."}, 403
        return f(*a, **kw)
    return dentro


def segredo_de_maquina_confere(recebido: str) -> bool:
    """Autentica a chamada do agendador (cron-job.org), no mesmo padrão dos
    outros módulos do repositório: um segredo por módulo, no corpo do pedido."""
    esperado = os.getenv("ANALISESPS_SECRET", "").strip()
    if not esperado:
        return False
    return confere(recebido, esperado)
