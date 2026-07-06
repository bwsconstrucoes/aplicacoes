# -*- coding: utf-8 -*-
"""
retry.py — Helper de retry para chamadas gspread.

Resolve dois problemas comuns:
  - HTTP 429 (rate limit): Google Sheets API tem cota de 60 writes/minuto/usuário.
    Quando estourada, retorna 429 com header Retry-After.
  - HTTP 500/502/503/504 (erros transitórios do Google): acontece raramente
    mas é suficiente pra gerar "buracos" entre SPsBD e Log.

Estratégia:
  - Até 5 tentativas
  - Backoff exponencial: 1s, 2s, 4s, 8s, 16s
  - Jitter: ±25% pra evitar thundering herd
  - Respeita Retry-After do servidor quando presente
  - Loga cada tentativa
"""

import time
import random
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

# Códigos HTTP considerados transitórios (vale a pena tentar de novo)
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}

# Trechos de mensagem que também indicam erro transitório
RETRYABLE_MESSAGES = (
    'rate limit',
    'quota exceeded',
    'timeout',
    'temporarily unavailable',
    'service unavailable',
    'internal error',
    'backend error',
    'deadline exceeded',
)

# Trechos que indicam especificamente 429 de quota por minuto/segundo
# (quando aparecem, esperar 60s é a única saída — não adianta backoff exponencial curto)
QUOTA_MINUTE_MESSAGES = (
    'quota exceeded',
    'per minute',
    'per user',
    'quota metric',
    'resource_exhausted',
)

MAX_TENTATIVAS      = 5
BASE_BACKOFF_S      = 1.0
MAX_BACKOFF_S       = 65.0    # >60s para 429 de quota per minute (Google Sheets)
QUOTA_MIN_BACKOFF_S = 30.0    # espera mínima quando detectamos 429 de quota per minute
JITTER_PCT          = 0.25


def _is_retryable(exc: Exception) -> bool:
    """Decide se o erro merece nova tentativa."""
    # gspread.exceptions.APIError tem .response com .status_code
    response = getattr(exc, 'response', None)
    if response is not None:
        status = getattr(response, 'status_code', None)
        if status in RETRYABLE_HTTP_CODES:
            return True

    # Falha sem .response (timeout, conexão derrubada) — checa pela mensagem
    msg = str(exc).lower()
    if any(t in msg for t in RETRYABLE_MESSAGES):
        return True

    # Específico do gspread: APIError com código no corpo
    code = getattr(exc, 'code', None)
    if code in RETRYABLE_HTTP_CODES:
        return True

    return False


def _extrair_retry_after(exc: Exception) -> float | None:
    """Lê o header Retry-After (em segundos) se o erro vier com response."""
    response = getattr(exc, 'response', None)
    if response is None:
        return None
    headers = getattr(response, 'headers', {}) or {}
    retry_after = headers.get('Retry-After') or headers.get('retry-after')
    if not retry_after:
        return None
    try:
        return float(retry_after)
    except (TypeError, ValueError):
        return None


def _is_quota_per_minute(exc: Exception) -> bool:
    """Detecta erro 429 de quota per minute do Google Sheets."""
    msg = str(exc).lower()
    return any(t in msg for t in QUOTA_MINUTE_MESSAGES)


def _calcular_backoff(tentativa: int, exc: Exception = None,
                       retry_after: float | None = None) -> float:
    """Calcula tempo de espera antes da próxima tentativa (em segundos)."""
    if retry_after is not None and retry_after > 0:
        # Respeita o que o servidor pediu (com jitter pequeno pra suavizar concorrência)
        base = min(retry_after, MAX_BACKOFF_S)
    elif exc is not None and _is_quota_per_minute(exc):
        # 429 de quota per minute: espera 30s → 60s → 60s → 60s → 60s
        # (não adianta esperar 1s ou 2s — a janela só reseta após 60s)
        base = min(QUOTA_MIN_BACKOFF_S * (2 ** tentativa), MAX_BACKOFF_S)
    else:
        # Exponencial padrão para erros transitórios não-quota: 1s, 2s, 4s, 8s, 16s
        base = min(BASE_BACKOFF_S * (2 ** tentativa), MAX_BACKOFF_S)
    jitter = base * JITTER_PCT * (random.random() * 2 - 1)
    return max(0.1, min(base + jitter, MAX_BACKOFF_S))


def com_retry(operacao: Callable[[], Any], *, descricao: str = 'operação Sheets') -> Any:
    """
    Executa uma callable com retry. Levanta a exceção original após esgotar tentativas.

    Uso:
        com_retry(lambda: sh.append_row(linha, ...), descricao='append SPsBD')
    """
    ultima_exc = None
    for tentativa in range(MAX_TENTATIVAS):
        try:
            return operacao()
        except Exception as e:
            ultima_exc = e
            if not _is_retryable(e):
                # Erro permanente — não adianta tentar de novo
                logger.warning(
                    f'[retry] {descricao}: erro NÃO-retryable na tentativa {tentativa + 1}: {e}'
                )
                raise
            if tentativa == MAX_TENTATIVAS - 1:
                logger.error(
                    f'[retry] {descricao}: esgotou {MAX_TENTATIVAS} tentativas. '
                    f'Último erro: {e}'
                )
                raise
            espera = _calcular_backoff(tentativa, exc=e,
                                        retry_after=_extrair_retry_after(e))
            logger.warning(
                f'[retry] {descricao}: tentativa {tentativa + 1}/{MAX_TENTATIVAS} '
                f'falhou ({type(e).__name__}: {str(e)[:80]}). '
                f'Tentando de novo em {espera:.1f}s...'
            )
            time.sleep(espera)
    # Defensivo (não deve chegar aqui)
    if ultima_exc:
        raise ultima_exc
