# -*- coding: utf-8 -*-
"""
Quem pode falar com o ponto — o padrão é NEGAR.

DUAS CREDENCIAIS, para dois tipos de quem chama:

  - **Chave de API** (`X-API-Key`, variável `PONTO_API_KEY` no Render): para
    SISTEMAS — Análise de SPs, planilha, conector do iDFace, quem administra
    aparelhos. Sem a variável configurada, toda rota que exige chave responde
    503 "serviço não configurado". Falha fechado, nunca aberto.

  - **Token do aparelho** (`X-Device-Token` + `device_uuid` no corpo): para o
    CELULAR. Chave de API no celular vazaria no primeiro aparelho inspecionado
    — um PWA não tem onde esconder segredo. O token é entregue uma vez, no
    registro; o banco guarda só o hash; bloquear o aparelho mata o token dele
    e só dele.

A DECLARAÇÃO É OBRIGATÓRIA. Toda rota do blueprint diz o que exige
(`@exige_chave`, `@exige_aparelho_ou_chave`, `@publica("motivo")`). Rota que
esquece de declarar é RECUSADA pelo guarda, não liberada — regra do ERP e da
Análise de SPs, porque esquecer é o modo de falha mais comum.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import time
from functools import wraps

from flask import current_app, g, jsonify, request

logger = logging.getLogger("ponto.auth")

VARIAVEL_CHAVE = "PONTO_API_KEY"
CABECALHO_CHAVE = "X-API-Key"
CABECALHO_TOKEN = "X-Device-Token"

_EXIGENCIA = "_ponto_exigencia"
CHAVE = "chave"
APARELHO_OU_CHAVE = "aparelho_ou_chave"

# Registro de aparelho é a única rota sem credencial; este é o teto por IP.
REGISTROS_POR_HORA_POR_IP = 30
_registros: dict[str, list[float]] = {}


# ---------------------------------------------------------------------------
# Declarações
# ---------------------------------------------------------------------------
def exige_chave(f):
    """Só sistemas, com `X-API-Key`."""
    setattr(f, _EXIGENCIA, CHAVE)
    return f


def exige_aparelho_ou_chave(f):
    """Celular com token do aparelho, OU sistema com chave (iDFace, manual)."""
    setattr(f, _EXIGENCIA, APARELHO_OU_CHAVE)
    return f


def publica(motivo: str):
    """Responde sem credencial. O motivo fica escrito para quem revisar."""
    def decorador(f):
        setattr(f, _EXIGENCIA, ("publica", motivo))
        return f
    return decorador


# ---------------------------------------------------------------------------
# Segredos
# ---------------------------------------------------------------------------
def chave_configurada() -> str:
    return (os.environ.get(VARIAVEL_CHAVE) or "").strip()


def confere(recebido: str | None, esperado: str) -> bool:
    """Compara em tempo constante. Em bytes: `compare_digest` com texto fora do
    ASCII estoura, e um segredo com acento viraria erro 500 em vez de 401."""
    if not recebido or not esperado:
        return False
    return hmac.compare_digest(str(recebido).strip().encode("utf-8"),
                               esperado.encode("utf-8"))


def gerar_token() -> str:
    """Token do aparelho: 32 bytes aleatórios, legíveis. Entregue UMA vez."""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.strip().encode("utf-8")).hexdigest()


def confere_hash(token: str | None, hash_esperado: str | None) -> bool:
    if not token or not hash_esperado:
        return False
    return hmac.compare_digest(hash_token(token), hash_esperado)


# ---------------------------------------------------------------------------
# O guarda
# ---------------------------------------------------------------------------
def _negar(status: int, mensagem: str):
    return jsonify({"ok": False, "erro": mensagem}), status


def chave_valida() -> bool:
    esperada = chave_configurada()
    return bool(esperada) and confere(request.headers.get(CABECALHO_CHAVE), esperada)


def ip_de_quem_chama() -> str:
    # O Render põe o IP real em X-Forwarded-For; sem ele, o do socket.
    encaminhado = request.headers.get("X-Forwarded-For", "")
    return (encaminhado.split(",")[0].strip() or request.remote_addr or "?")


def registro_permitido(ip: str, agora: float | None = None) -> bool:
    """Teto de registros de aparelho por IP e hora. Em memória: a produção roda
    com UM processo, então a conta é a mesma para todas as linhas de atendimento."""
    agora = agora if agora is not None else time.time()
    janela = [t for t in _registros.get(ip, []) if agora - t < 3600]
    if len(janela) >= REGISTROS_POR_HORA_POR_IP:
        _registros[ip] = janela
        return False
    janela.append(agora)
    _registros[ip] = janela
    return True


def exigir_credencial():
    """Roda antes de cada rota do blueprint. None = pode seguir."""
    endpoint = request.endpoint or ""
    funcao = current_app.view_functions.get(endpoint)
    exigencia = getattr(funcao, _EXIGENCIA, None)
    g.ponto_via = None

    if exigencia is None:
        logger.error("Ponto: a rota '%s' não declarou exigência de acesso. Ponha "
                     "@exige_chave, @exige_aparelho_ou_chave ou @publica('motivo'). "
                     "Enquanto isso, está fechada.", endpoint)
        return _negar(403, "rota sem declaração de acesso")

    if isinstance(exigencia, tuple):          # @publica("motivo")
        g.ponto_via = "publica"
        return None

    if not chave_configurada() and exigencia == CHAVE:
        logger.error("Ponto: %s não está configurada; a rota '%s' fica fechada.",
                     VARIAVEL_CHAVE, endpoint)
        return _negar(503, "serviço não configurado")

    if chave_valida():
        g.ponto_via = "chave"
        return None

    if exigencia == APARELHO_OU_CHAVE and request.headers.get(CABECALHO_TOKEN):
        # A conferência do token precisa do banco; a rota faz, com o device_uuid
        # do corpo. Aqui só marcamos por onde a pessoa entrou.
        g.ponto_via = "aparelho"
        return None

    logger.warning("Ponto: credencial ausente ou inválida em %s (%s)",
                   endpoint, ip_de_quem_chama())
    return _negar(401, "credencial ausente ou inválida")
