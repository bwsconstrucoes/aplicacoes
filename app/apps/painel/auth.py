# -*- coding: utf-8 -*-
"""
Login do painel.

Sao dados financeiros da empresa: nada abre sem senha. O padrao e NEGAR — toda
rota do blueprint passa pelo `before_request`, e quem quiser ser publica precisa
dizer isso explicitamente. Esquecer fecha a rota, nunca abre.

A senha fica na variavel de ambiente PAINEL_SENHA, no Render. Se ela nao estiver
configurada, o painel NAO abre para ninguem — falha fechado, em vez de ficar
acessivel a qualquer um que descubra o endereco.

Isto e mais simples que o login do ERP de proposito: o painel tem um usuario so
(o dono), sem perfis nem alcada. Se um dia precisar de mais gente com visoes
diferentes, o lugar certo passa a ser o cadastro de usuarios do ERP.
"""
from __future__ import annotations

import os
import hmac
import logging
import unicodedata

from flask import redirect, request, session, url_for

logger = logging.getLogger("painel.auth")

CHAVE_SESSAO = "painel_autenticado"

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


def entrar_na_sessao() -> None:
    session[CHAVE_SESSAO] = True
    session.permanent = False   # a sessao morre quando o navegador fecha


def sair_da_sessao() -> None:
    session.pop(CHAVE_SESSAO, None)


def exigir_login():
    """Roda antes de cada rota do painel. Devolve None quando pode seguir."""
    endpoint = request.endpoint or ""
    if endpoint in PUBLICAS:
        return None
    if esta_logado():
        return None
    return redirect(url_for("painel.entrar", proximo=request.full_path))


def segredo_de_maquina_confere(recebido: str) -> bool:
    """Autentica a chamada do agendador (cron-job.org), no mesmo padrao dos
    outros modulos do repositorio: um segredo por modulo, no corpo do pedido."""
    esperado = os.getenv("PAINEL_SECRET", "").strip()
    if not esperado:
        return False
    # mesmo cuidado da senha: um acento aqui derrubaria a carga da madrugada
    return hmac.compare_digest(_para_comparar(recebido),
                               _para_comparar(esperado))
