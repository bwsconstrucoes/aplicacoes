# -*- coding: utf-8 -*-
"""
Login do painel.

Sao dados financeiros da empresa: nada abre sem senha. O padrao e NEGAR — toda
rota do blueprint passa pelo `before_request`, e quem quiser ser publica precisa
dizer isso explicitamente. Esquecer fecha a rota, nunca abre.

A senha fica na variavel de ambiente PAINEL_SENHA, no Render. Se ela nao estiver
configurada, o painel NAO abre para ninguem — falha fechado, em vez de ficar
acessivel a qualquer um que descubra o endereco.

DESDE 21/09/2026 HA DOIS JEITOS DE ENTRAR, e eles sao bem diferentes:

- **a senha mestre (PAINEL_SENHA)** — e o ADMINISTRADOR. Ve tudo, configura,
  escreve no OMIE e cadastra as pessoas. E o dono;
- **usuario e senha proprios** (`usuarios.py`, migracao 013) — pessoa presa a
  certas OBRAS e a certas TELAS. Nunca escreve no OMIE, nunca abre
  Configuracoes nem o Explorador.

O escopo da pessoa e aplicado NUM LUGAR SO: `_filtros_do_pedido`, no `web.py`,
por onde toda tela passa para saber o que mostrar. Amarrar ali significa que
nenhuma tela pode esquecer — que e exatamente como esse tipo de coisa vaza
quando se protege tela por tela.
"""
from __future__ import annotations

import os
import hmac
import logging
import unicodedata

from flask import redirect, request, session, url_for

logger = logging.getLogger("painel.auth")

CHAVE_SESSAO = "painel_autenticado"
CHAVE_USUARIO = "painel_usuario_id"      # so quando entrou por usuario proprio

# Rotas que NENHUMA pessoa presa a obra alcanca, por escreverem ou por abrirem
# a base inteira. A lista e por PREFIXO do endpoint, para uma rota nova dentro
# de uma dessas areas ja nascer fechada em vez de esperar alguem lembrar.
SO_DO_ADMINISTRADOR = (
    "painel.configuracoes", "painel.explorador", "painel.aplicar_migracoes",
    "painel.sincronizar", "painel.disparar", "painel.rateio",
    "painel.saneamento", "painel.api_",
    # O cadastro de acesso. Faltava aqui, e o teste pegou: sem esta linha, uma
    # pessoa presa a uma obra conseguia CRIAR OUTRO ACESSO — inclusive um com
    # todas as obras. A lista por prefixo existe justamente para isso doer
    # menos na proxima vez, mas so protege o que esta escrito nela.
    "painel.usuarios",
    # O cenario da prestacao de contas: a montagem CONFIGURA a empresa inteira,
    # e o resultado mostra TODAS as obras, com a quota de cada socio. Nenhuma
    # das duas passa pelo filtro de escopo — entao nenhuma das duas e de quem
    # esta preso a uma obra. O prefixo cobre as duas e as proximas.
    "painel.cenario_",
)

# Rotas que podem responder sem login. Cada uma com o motivo escrito.
PUBLICAS = {
    "painel.entrar",       # a propria tela de login
    "painel.saude",        # checagem de servico, nao devolve dado nenhum
    "painel.sincronizar",  # chamada por maquina; protegida por PAINEL_SECRET
    "painel.static",       # folha de estilo
}


def senha_configurada() -> str:
    return os.getenv("PAINEL_SENHA", "").strip()


def _para_comparar(valor) -> bytes:
    """Prepara um segredo para a comparacao em tempo constante.

    DUAS COISAS ACONTECEM AQUI, e as duas custaram uma queda em producao.

    1. VIRA BYTES. O `hmac.compare_digest` recusa TEXTO que tenha qualquer
       caractere fora do ASCII — e nao devolve False: levanta TypeError. Uma
       senha com acento (ou um "ç" digitado sem querer) derrubava a tela de
       login com "comparing strings with non-ASCII characters is not supported"
       em vez de dizer "senha incorreta". E se a senha CONFIGURADA tivesse
       acento, ninguem entrava nunca. Com bytes, a funcao aceita qualquer coisa
       e continua sendo tempo constante.

    2. NORMALIZA (NFC). "ç" pode ser gravado como um caractere ou como "c" mais
       a cedilha separada, dependendo do teclado e do sistema. Os dois parecem
       iguais na tela e NAO sao iguais em bytes. Sem isto, a mesma senha
       digitada no celular e no computador podia nao bater. E o que a RFC 8265
       recomenda para senha, e nao afrouxa nada: texto identico continua
       identico depois de normalizado."""
    return unicodedata.normalize("NFC", str(valor or "")).encode("utf-8")


def senha_confere(digitada: str) -> bool:
    """Compara em tempo constante. Sem senha no ambiente, nada confere."""
    esperada = senha_configurada()
    if not esperada:
        return False
    return hmac.compare_digest(_para_comparar(digitada),
                               _para_comparar(esperada))


def esta_logado() -> bool:
    return bool(session.get(CHAVE_SESSAO))


def entrar_na_sessao(usuario_id=None) -> None:
    session[CHAVE_SESSAO] = True
    if usuario_id is None:
        session.pop(CHAVE_USUARIO, None)
    else:
        session[CHAVE_USUARIO] = int(usuario_id)
    session.permanent = False   # a sessao morre quando o navegador fecha


def sair_da_sessao() -> None:
    session.pop(CHAVE_SESSAO, None)
    session.pop(CHAVE_USUARIO, None)


def e_administrador() -> bool:
    """Entrou pela senha mestre. Quem entrou por usuario proprio, nunca."""
    return esta_logado() and not session.get(CHAVE_USUARIO)


def usuario_da_sessao():
    """A pessoa logada, ou None quando e o administrador.

    Vai ao banco a cada pedido de proposito: tirar a obra de alguem tem de
    valer NA HORA, nao quando ele fechar o navegador. Sao duas consultas por
    chave primaria — mais barato que a consulta mais leve de qualquer tela."""
    uid = session.get(CHAVE_USUARIO)
    if not uid:
        return None
    from . import usuarios
    pessoa = usuarios.buscar_por_id(uid)
    if pessoa is None:
        # apagado ou desativado no meio da sessao: cai fora na hora
        sair_da_sessao()
    return pessoa


def exigir_login():
    """Roda antes de cada rota do painel. Devolve None quando pode seguir."""
    endpoint = request.endpoint or ""
    if endpoint in PUBLICAS:
        return None
    if not esta_logado():
        return redirect(url_for("painel.entrar", proximo=request.full_path))
    if e_administrador():
        return None

    pessoa = usuario_da_sessao()
    if pessoa is None:
        return redirect(url_for("painel.entrar"))

    # 1. area que escreve ou abre a base inteira: nunca, para ninguem preso
    if endpoint.startswith(SO_DO_ADMINISTRADOR):
        logger.warning("Painel: %s tentou abrir %s, que e so do administrador.",
                       pessoa["usuario"], endpoint)
        return _nao_encontrado()

    # 2. tela nao liberada: tambem nao. Sem tela marcada, nenhuma tela abre —
    #    lista vazia quer dizer NENHUMA, nunca "todas".
    aba = _aba_do_endpoint(endpoint)
    if aba and aba not in set(pessoa.get("telas") or []):
        return _nao_encontrado()

    # 3. sem obra marcada nao ha o que mostrar, e mostrar tudo seria o oposto
    if not pessoa.get("obras"):
        return _nao_encontrado()
    return None


def nao_encontrado():
    """A mesma recusa, para quem esta fora deste modulo (o download usa)."""
    return _nao_encontrado()


def _nao_encontrado():
    """Responde "nao existe", e nao "voce nao pode".

    Mesma regra do ERP: dizer "sem permissao" confirma que a tela existe, e
    varrer os enderecos mapearia o sistema sem abrir nada."""
    from flask import render_template
    return render_template("painel_erro.html",
                           erro="Página não encontrada."), 404


# Endpoint -> chave da aba. O que nao estiver aqui nao e tela de dado: e rota de
# apoio (login, estatico, download), tratada pelas outras regras.
_ABAS_POR_ENDPOINT = {
    "painel.visao_geral": "visao",
    "painel.dre": "dre",
    "painel.analitico": "analitico",
    "painel.receita": "receita",
    "painel.medicao": "receita",
    "painel.fluxo": "fluxo",
    "painel.obras": "obras",
    "painel.execucao": "execucao",
    "painel.necessidade_caixa": "caixa",
    "painel.prestacao_contas": "prestacao",
    "painel.extrato": "extrato",
}


def _aba_do_endpoint(endpoint: str):
    return _ABAS_POR_ENDPOINT.get(endpoint)


# ---------------------------------------------------------------------------
# O que cada pessoa pode BAIXAR
# ---------------------------------------------------------------------------
# Achado em 22/09/2026, ao acrescentar o download do cenario: a rota
# `/painel/baixar/<assunto>` nao conferia assunto nenhum. As telas eram
# protegidas uma a uma e o download passava por fora.
#
# A maioria dos assuntos monta o arquivo a partir de `_filtros_do_pedido`, que
# ja prende a pessoa as obras dela — esses estavam certos por acidente. Mas
# quotas, posicao, rateio da administracao e o cenario NAO passam por ali: eles
# leem a empresa inteira, porque a pergunta que respondem e sobre a empresa
# inteira. Quem tinha acesso a uma obra podia baixar a divisao de lucro entre
# os socios.
#
# A regra passa a ser a mesma das telas, e pelo mesmo motivo: o padrao e NEGAR.
# Assunto que ninguem mapeou aqui nao e baixado por quem esta preso a uma obra.

# Assunto -> a tela de que ele e o arquivo. Quem nao tem a tela nao tem o
# arquivo: seria estranho o contrario.
TELA_DO_DOWNLOAD = {
    "dre": "dre", "aportes": "dre",
    "analitico": "analitico", "despesas": "analitico", "credores": "analitico",
    "medicoes": "receita", "fluxo": "fluxo", "obras": "obras",
    "execucao": "execucao", "extrato": "extrato",
}

# Estes leem a empresa INTEIRA, por definicao. Sao do dono.
SO_DO_DONO_PARA_BAIXAR = frozenset({
    "quotas", "posicao", "rateio_admin", "cenario", "explorador", "completo",
})


def pode_baixar(assunto: str) -> bool:
    """O administrador baixa tudo. Quem esta preso a obra, so o arquivo de uma
    tela que ele tem — e nunca o que abre a empresa inteira."""
    if e_administrador():
        return True
    if assunto in SO_DO_DONO_PARA_BAIXAR:
        return False
    tela = TELA_DO_DOWNLOAD.get(assunto)
    if not tela:
        return False                      # assunto novo nasce fechado
    pessoa = usuario_da_sessao()
    return bool(pessoa) and tela in set(pessoa.get("telas") or [])


def segredo_de_maquina_confere(recebido: str) -> bool:
    """Autentica a chamada do agendador (cron-job.org), no mesmo padrao dos
    outros modulos do repositorio: um segredo por modulo, no corpo do pedido."""
    esperado = os.getenv("PAINEL_SECRET", "").strip()
    if not esperado:
        return False
    # mesmo cuidado da senha: um acento aqui derrubaria a carga da madrugada
    return hmac.compare_digest(_para_comparar(recebido),
                               _para_comparar(esperado))
