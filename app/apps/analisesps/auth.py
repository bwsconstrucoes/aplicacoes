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

POR QUE SENHA E NÃO CADASTRO DE USUÁRIO. São até quatro pessoas, e este módulo
tem prazo de validade: o ERP vai substituí-lo. Cadastro com nome, hash e tela
de administração é o certo para o que fica; para o que sai de cena, é peso sem
retorno.

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

# Teto do nome. Não é regra de negócio: é para o campo não virar porta de
# entrada de texto gigante, já que ele vai para o banco e para a tela.
MAX_NOME = 40

# O nome fica lembrado NESTE navegador, para a pessoa não redigitá-lo todo
# dia. É um cookie separado da sessão, e de propósito:
#
#   - a SESSÃO morre quando o navegador fecha, e tem de continuar morrendo:
#     é ela que diz que alguém digitou a senha;
#   - o NOME não é segredo nem credencial. Lembrá-lo não abre nada — quem
#     abrir o navegador continua vendo a tela de senha, com o campo do nome
#     já preenchido.
#
# Guardar a senha "para facilitar" seria outra conversa, e a resposta seria
# não. São os pagamentos da empresa.
COOKIE_NOME = "analisesps_ultimo_nome"
DIAS_LEMBRANDO_O_NOME = 180

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
        if hmac.compare_digest(digitada, esperada) and achado is None:
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
    return perfil_atual() == OPERADOR


def entrar_na_sessao(perfil: str, nome: str = "") -> None:
    session[CHAVE_SESSAO] = perfil
    session[CHAVE_NOME] = limpar_nome(nome)
    session.permanent = False      # a sessão morre quando o navegador fecha


def sair_da_sessao() -> None:
    session.pop(CHAVE_SESSAO, None)
    session.pop(CHAVE_NOME, None)


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
        mensagem="Você entrou no perfil Consulta, que vê mas não altera. "
                 "Para esta ação, entre com a senha de Operador."), 403


def exigir_operador_json(f):
    """Mesma trava, para as rotas que respondem JSON em vez de tela."""
    @wraps(f)
    def dentro(*a, **kw):
        if not pode_operar():
            return {"ok": False,
                    "erro": "O perfil Consulta não altera dados."}, 403
        return f(*a, **kw)
    return dentro


def segredo_de_maquina_confere(recebido: str) -> bool:
    """Autentica a chamada do agendador (cron-job.org), no mesmo padrão dos
    outros módulos do repositório: um segredo por módulo, no corpo do pedido."""
    esperado = os.getenv("ANALISESPS_SECRET", "").strip()
    if not esperado:
        return False
    return hmac.compare_digest(str(recebido or ""), esperado)
