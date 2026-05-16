# -*- coding: utf-8 -*-
"""
notify.py — Mensagens de falha via Z-API (módulos 736/741 do Make).

Usa o módulo genérico do validasp/zapi.py (reaproveita conexão Z-API).
"""

import os
import logging
from .utils import as_string

logger = logging.getLogger(__name__)


def alertar_falha_log(sp_id: str, execucao_url: str = '') -> dict:
    """
    Alerta WhatsApp ao admin quando registro de Log/SPsBD falha.
    Equivalente aos módulos 736 e 741 do Make.
    """
    try:
        # importa lazy (módulo está em outro blueprint)
        from app.apps.validasp.zapi import enviar_texto

        instance_id  = os.getenv('ZAPI_INSTANCE_ID', '')
        api_token    = os.getenv('ZAPI_API_TOKEN', '')
        client_token = os.getenv('ZAPI_CLIENT_TOKEN', '')
        master       = os.getenv('CHATBOT_MASTER_PHONE', '5585987846225')

        if not (instance_id and api_token and client_token):
            logger.warning('[notify] credenciais Z-API ausentes; pulando alerta')
            return {'ok': False, 'motivo': 'credenciais ausentes'}

        msg = (
            f'❌❌ *FALHA PROCESSAMENTO SP*\n\n'
            f'*SP*: {as_string(sp_id)}\n'
            f'*Origem:* Render (processarnovasp)\n'
            f'{(f"*Execução:* {execucao_url}" if execucao_url else "")}'
            f'\n\nAtenciosamente,\nBWS Bot 🤖'
        )

        ack = enviar_texto({
            'instance_id':  instance_id,
            'api_token':    api_token,
            'client_token': client_token,
            'telefone':     master,
            'mensagem':     msg,
        })
        return {'ok': True, 'ack': ack}
    except Exception as e:
        logger.exception('[notify] falha ao enviar Z-API')
        return {'ok': False, 'erro': str(e)}
