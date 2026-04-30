# -*- coding: utf-8 -*-
"""
chatbot/session.py
Gerencia o estado da conversa por telefone.
Usa dict em memória com TTL — adequado para o volume atual.
Para escala futura, substituir por Redis.

Estados possíveis:
  AGUARDANDO_CPF       → bot pediu CPF, aguardando resposta
  AGUARDANDO_COMPETENCIA → autenticado, bot pediu mês/ano
  AUTENTICADO          → CPF validado, dados do colaborador disponíveis
"""

import threading
import time
import logging

logger = logging.getLogger(__name__)

# Timeout de sessão em segundos (30 minutos de inatividade)
SESSION_TTL = 30 * 60

# Máx tentativas de CPF errado antes de bloquear
MAX_TENTATIVAS_CPF = 5
BLOQUEIO_DURACAO = 60 * 60  # 1 hora

_sessions: dict = {}
_bloqueios: dict = {}
_lock = threading.Lock()


def _agora() -> float:
    return time.time()


def get_session(telefone: str) -> dict | None:
    """Retorna a sessão do telefone ou None se não existir/expirada."""
    with _lock:
        sess = _sessions.get(telefone)
        if not sess:
            return None
        if _agora() - sess['ultimo_acesso'] > SESSION_TTL:
            del _sessions[telefone]
            return None
        sess['ultimo_acesso'] = _agora()
        return dict(sess)


def criar_session(telefone: str, estado: str, dados: dict = None) -> dict:
    """Cria ou reinicia a sessão do telefone."""
    with _lock:
        sess = {
            'telefone':      telefone,
            'estado':        estado,
            'dados':         dados or {},
            'criado_em':     _agora(),
            'ultimo_acesso': _agora(),
        }
        _sessions[telefone] = sess
        return dict(sess)


def atualizar_session(telefone: str, estado: str = None, dados_extra: dict = None):
    """Atualiza estado e/ou dados da sessão existente."""
    with _lock:
        sess = _sessions.get(telefone)
        if not sess:
            return
        if estado:
            sess['estado'] = estado
        if dados_extra:
            sess['dados'].update(dados_extra)
        sess['ultimo_acesso'] = _agora()


def destruir_session(telefone: str):
    """Remove a sessão (logout / reset)."""
    with _lock:
        _sessions.pop(telefone, None)


def registrar_tentativa_cpf_errado(telefone: str) -> int:
    """Registra tentativa falha de CPF. Retorna o número de tentativas."""
    with _lock:
        entrada = _bloqueios.get(telefone, {'tentativas': 0, 'desde': _agora()})
        entrada['tentativas'] += 1
        _bloqueios[telefone] = entrada
        return entrada['tentativas']


def esta_bloqueado(telefone: str) -> bool:
    """Verifica se o telefone está bloqueado por excesso de tentativas."""
    with _lock:
        entrada = _bloqueios.get(telefone)
        if not entrada:
            return False
        if entrada['tentativas'] >= MAX_TENTATIVAS_CPF:
            if _agora() - entrada['desde'] < BLOQUEIO_DURACAO:
                return True
            else:
                # Bloqueio expirado — reseta
                del _bloqueios[telefone]
        return False


def resetar_tentativas(telefone: str):
    """Reseta tentativas de CPF após sucesso."""
    with _lock:
        _bloqueios.pop(telefone, None)


def limpar_sessoes_expiradas():
    """Limpeza periódica — pode ser chamada em background."""
    with _lock:
        agora = _agora()
        expiradas = [t for t, s in _sessions.items()
                     if agora - s['ultimo_acesso'] > SESSION_TTL]
        for t in expiradas:
            del _sessions[t]
        if expiradas:
            logger.info(f"[session] {len(expiradas)} sessão(ões) expirada(s) removida(s).")
