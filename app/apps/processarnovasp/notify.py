# -*- coding: utf-8 -*-
"""
notify.py — Mensagens de falha (módulos 736/741 do Make).

Usa o módulo compartilhado app/apps/notificador (WhatsApp via Z-API +
Telegram via aba TelegramID, com liga/desliga por canal via env
NOTIFICAR_WHATSAPP / NOTIFICAR_TELEGRAM).

Correção 2026-07: a versão anterior chamava validasp.zapi.enviar_texto com
assinatura errada (dict único) — o alerta nunca era enviado e a exceção era
engolida pelo try/except.
"""

import os
import logging
from .utils import as_string

logger = logging.getLogger(__name__)


def alertar_falha_log(sp_id: str, execucao_url: str = '') -> dict:
    """
    Alerta (WhatsApp + Telegram) ao admin quando registro de Log/SPsBD falha.
    Equivalente aos módulos 736 e 741 do Make.
    """
    try:
        # importa lazy (módulo compartilhado do monorepo)
        from app.apps.notificador import notificar

        master = os.getenv('CHATBOT_MASTER_PHONE', '5585987846225')

        msg = (
            f'❌❌ *FALHA PROCESSAMENTO SP*\n\n'
            f'*SP*: {as_string(sp_id)}\n'
            f'*Origem:* Render (processarnovasp)\n'
            f'{(f"*Execução:* {execucao_url}" if execucao_url else "")}'
            f'\n\nAtenciosamente,\nBWS Bot 🤖'
        )

        ack = notificar(telefone=master, mensagem=msg)
        ok = any(bool(r.get('ok')) for r in ack.values())
        if not ok:
            logger.warning('[notify] alerta não entregue em nenhum canal: %s', ack)
        return {'ok': ok, 'ack': ack}
    except Exception as e:
        logger.exception('[notify] falha ao enviar alerta')
        return {'ok': False, 'erro': str(e)}
