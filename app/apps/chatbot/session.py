# -*- coding: utf-8 -*-
"""
chatbot/session.py
Gerencia o estado da conversa por telefone.
Usa dict em memória com TTL.
"""

import re
import threading
import time
import logging

logger = logging.getLogger(__name__)

SESSION_TTL = 30 * 60

MAX_TENTATIVAS_CPF = 5
BLOQUEIO_DURACAO = 60 * 60

_sessions: dict = {}
_bloqueios: dict = {}
_lock = threading.Lock()


def _agora() -> float:
    return time.time()


def _normalizar_telefone(telefone: str) -> str:
    digits = re.sub(r"\D", "", str(telefone or ""))

    if digits and not digits.startswith("55"):
        digits = "55" + digits

    return digits


def get_session(telefone: str) -> dict | None:
    telefone = _normalizar_telefone(telefone)

    with _lock:
        sess = _sessions.get(telefone)

        if not sess:
            logger.info(f"[session] sem sessão ativa para {telefone[:6]}***")
            return None

        if _agora() - sess["ultimo_acesso"] > SESSION_TTL:
            del _sessions[telefone]
            logger.info(f"[session] sessão expirada para {telefone[:6]}***")
            return None

        sess["ultimo_acesso"] = _agora()

        logger.info(
            f"[session] sessão encontrada para {telefone[:6]}*** "
            f"estado={sess.get('estado')}"
        )

        return dict(sess)


def criar_session(telefone: str, estado: str, dados: dict = None) -> dict:
    telefone = _normalizar_telefone(telefone)

    with _lock:
        sess = {
            "telefone": telefone,
            "estado": estado,
            "dados": dados or {},
            "criado_em": _agora(),
            "ultimo_acesso": _agora(),
        }

        _sessions[telefone] = sess

        logger.info(
            f"[session] sessão criada para {telefone[:6]}*** "
            f"estado={estado}"
        )

        return dict(sess)


def atualizar_session(
    telefone: str,
    estado: str = None,
    dados_extra: dict = None
):
    telefone = _normalizar_telefone(telefone)

    with _lock:
        sess = _sessions.get(telefone)

        if not sess:
            logger.warning(
                f"[session] tentativa de atualizar sessão inexistente "
                f"para {telefone[:6]}***"
            )
            return

        estado_anterior = sess.get("estado")

        if estado:
            sess["estado"] = estado

        if dados_extra:
            sess["dados"].update(dados_extra)

        sess["ultimo_acesso"] = _agora()

        logger.info(
            f"[session] sessão atualizada para {telefone[:6]}*** "
            f"estado={estado_anterior} -> {sess.get('estado')}"
        )


def destruir_session(telefone: str):
    telefone = _normalizar_telefone(telefone)

    with _lock:
        removida = _sessions.pop(telefone, None)

        if removida:
            logger.info(f"[session] sessão removida para {telefone[:6]}***")


def registrar_tentativa_cpf_errado(telefone: str) -> int:
    telefone = _normalizar_telefone(telefone)

    with _lock:
        entrada = _bloqueios.get(
            telefone,
            {
                "tentativas": 0,
                "desde": _agora(),
            }
        )

        entrada["tentativas"] += 1
        _bloqueios[telefone] = entrada

        logger.warning(
            f"[session] CPF errado para {telefone[:6]}*** "
            f"tentativa={entrada['tentativas']}/{MAX_TENTATIVAS_CPF}"
        )

        return entrada["tentativas"]


def esta_bloqueado(telefone: str) -> bool:
    telefone = _normalizar_telefone(telefone)

    with _lock:
        entrada = _bloqueios.get(telefone)

        if not entrada:
            return False

        if entrada["tentativas"] >= MAX_TENTATIVAS_CPF:
            if _agora() - entrada["desde"] < BLOQUEIO_DURACAO:
                logger.warning(f"[session] telefone bloqueado {telefone[:6]}***")
                return True

            del _bloqueios[telefone]
            logger.info(f"[session] bloqueio expirado para {telefone[:6]}***")

        return False


def resetar_tentativas(telefone: str):
    telefone = _normalizar_telefone(telefone)

    with _lock:
        removido = _bloqueios.pop(telefone, None)

        if removido:
            logger.info(f"[session] tentativas resetadas para {telefone[:6]}***")


def limpar_sessoes_expiradas():
    with _lock:
        agora = _agora()

        expiradas = [
            telefone
            for telefone, sess in _sessions.items()
            if agora - sess["ultimo_acesso"] > SESSION_TTL
        ]

        for telefone in expiradas:
            del _sessions[telefone]

        if expiradas:
            logger.info(
                f"[session] {len(expiradas)} sessão(ões) expirada(s) removida(s)."
            )