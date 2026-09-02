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


def senha_confere(digitada: str) -> bool:
    """Compara em tempo constante. Sem senha no ambiente, nada confere."""
    esperada = senha_configurada()
    if not esperada:
        return False
    return hmac.compare_digest(str(digitada or ""), esperada)


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
    return hmac.compare_digest(str(recebido or ""), esperado)
