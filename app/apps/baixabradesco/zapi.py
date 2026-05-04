# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Dict, Any

import os
import requests

from .utils import as_string
from .pipefy import get_field_value, get_current_phase


ZAPI_BASE = 'https://api.z-api.io/instances/{instance}/token/{token}'


def resolve_zapi_auth(payload: dict | None = None) -> dict:
    """Resolve credenciais Z-API por payload ou variáveis de ambiente do Render.

    Prioridade:
    1) payload.zapi.instance_id / api_token / client_token
    2) payload.zapi_instance_id / zapi_api_token / zapi_client_token
    3) env ZAPI_INSTANCE_ID / ZAPI_API_TOKEN / ZAPI_CLIENT_TOKEN
    """
    payload = payload or {}
    zapi = payload.get('zapi') or {}
    return {
        'instanceId': as_string(zapi.get('instance_id') or zapi.get('instanceId') or payload.get('zapi_instance_id') or payload.get('instanceId') or os.getenv('ZAPI_INSTANCE_ID', '')),
        'apiToken': as_string(zapi.get('api_token') or zapi.get('apiToken') or payload.get('zapi_api_token') or payload.get('apiToken') or os.getenv('ZAPI_API_TOKEN', '')),
        'clientToken': as_string(zapi.get('client_token') or zapi.get('clientToken') or payload.get('zapi_client_token') or payload.get('clientToken') or os.getenv('ZAPI_CLIENT_TOKEN', '')),
    }


def validate_zapi_auth(auth: dict) -> str:
    missing = []
    if not auth.get('instanceId'):
        missing.append('ZAPI_INSTANCE_ID')
    if not auth.get('apiToken'):
        missing.append('ZAPI_API_TOKEN')
    if not auth.get('clientToken'):
        missing.append('ZAPI_CLIENT_TOKEN')
    return ', '.join(missing)


def _zapi_headers(auth: dict) -> dict:
    return {'Client-Token': as_string(auth.get('clientToken'))}


def send_text(auth: dict, phone: str, message: str) -> dict:
    base = ZAPI_BASE.format(
        instance=as_string(auth.get('instanceId')),
        token   =as_string(auth.get('apiToken')),
    )
    try:
        resp = requests.post(
            base + '/send-text',
            json={'phone': phone, 'message': message},
            headers=_zapi_headers(auth),
            timeout=20,
        )
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return {'ok': resp.status_code == 200, 'status': resp.status_code, 'body': body}
    except Exception as e:
        return {'ok': False, 'status': 0, 'error': str(e)}


def send_document(auth: dict, phone: str, document_url: str, file_name: str, doc_type: str = 'pdf') -> dict:
    base = ZAPI_BASE.format(
        instance=as_string(auth.get('instanceId')),
        token   =as_string(auth.get('apiToken')),
    )
    try:
        resp = requests.post(
            f'{base}/send-document/{doc_type}',
            json={'phone': phone, 'document': document_url, 'fileName': file_name},
            headers=_zapi_headers(auth),
            timeout=30,
        )
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return {'ok': resp.status_code == 200, 'status': resp.status_code, 'body': body}
    except Exception as e:
        return {'ok': False, 'status': 0, 'error': str(e)}


def send_messages_batch(auth: dict, messages: List[Dict[str, Any]]) -> List[dict]:
    """Envia lista de mensagens (text ou document) sequencialmente."""
    results = []
    for msg in messages:
        if not msg.get('enabled', True):
            results.append({'skipped': True})
            continue
        phone = as_string(msg.get('phone'))
        if not phone or len(phone.replace('+', '').replace(' ', '')) <= 4:
            results.append({'skipped': True, 'reason': 'phone_invalido'})
            continue
        msg_type = as_string(msg.get('type', 'text'))
        if msg_type == 'text':
            results.append(send_text(auth, phone, as_string(msg.get('message'))))
        elif msg_type == 'document':
            results.append(send_document(
                auth, phone,
                document_url=as_string(msg.get('documentUrl') or msg.get('document')),
                file_name   =as_string(msg.get('fileName') or 'Comprovante.pdf'),
                doc_type    =as_string(msg.get('docType') or 'pdf'),
            ))
        else:
            results.append({'skipped': True, 'reason': f'type_desconhecido: {msg_type}'})
    return results


def build_whatsapp_messages(plan, payload: dict) -> List[Dict[str, Any]]:
    """Monta a lista de mensagens a enviar para o responsável pela SP.

    Replica a lógica do módulo 1847 do Make:
    - Mensagem de texto com dados do pagamento
    - PDF do comprovante
    - Mensagem para o Requerente (se houver)
    """
    card_info = plan.responses.get('pipefy_card_info')
    rec = plan.receipt

    # Telefone do responsável — vem do campo Pipefy "Responsável pela Solicitação"
    # formato: ["Nome - 85999999999"]
    resp_field = get_field_value(card_info, 'Responsável pela Solicitação')
    telefone_resp = _extract_phone(resp_field)

    if not telefone_resp:
        return []

    # Centros de custo concatenados
    ccs = []
    for i in range(1, 6):
        cc = get_field_value(card_info, f'Centro de Custo {i}')
        if cc:
            # Remove brackets do Pipefy: ["Valor"] → Valor
            cc = cc.strip('[]"\'')
            ccs.append(cc)
    centro_custo_str = ' - '.join(ccs) if ccs else ''

    descricao = get_field_value(card_info, 'Descrição da Despesa') or ''
    descricao = descricao[:255]

    mensagem = (
        f'*Informação de Pagamento*\n\n'
        f'*Nº da SP:* {plan.match.id}\n'
        f'Data do Pagamento: {rec.data_pagamento}\n'
        f'Solicitante: {_strip_brackets(resp_field)}\n'
        f'Centro de Custo: {centro_custo_str}\n'
        f'Descrição: {descricao}\n'
        f'Forma de Pagamento: {rec.forma_pagamento or rec.tipo_comprovante}'
    )

    msgs: List[Dict[str, Any]] = [
        {
            'enabled': True,
            'type': 'text',
            'phone': telefone_resp,
            'message': mensagem,
        },
    ]

    # Comprovante em PDF
    link_comprovante = rec.drive_link
    if link_comprovante:
        msgs.append({
            'enabled': True,
            'type': 'document',
            'docType': 'pdf',
            'phone': telefone_resp,
            'documentUrl': link_comprovante.replace('?dl=0', '?dl=1').replace('&dl=0', '&dl=1'),
            'fileName': 'Comprovante de Pagamento',
        })

    # Requerente (se houver)
    requerente_field = get_field_value(card_info, 'Requerente')
    telefone_req = _extract_phone(requerente_field)
    if telefone_req and telefone_req != telefone_resp:
        msgs.append({
            'enabled': True,
            'type': 'text',
            'phone': telefone_req,
            'message': mensagem,
        })
        if link_comprovante:
            msgs.append({
                'enabled': True,
                'type': 'document',
                'docType': 'pdf',
                'phone': telefone_req,
                'documentUrl': link_comprovante.replace('?dl=0', '?dl=1').replace('&dl=0', '&dl=1'),
                'fileName': 'Comprovante de Pagamento',
            })

    return msgs


def _extract_phone(field_value: str) -> str:
    """Extrai o telefone do formato Pipefy: ["Nome - 85999999999"] → 5585999999999"""
    v = as_string(field_value).strip('[]"\'')
    if ' - ' in v:
        phone = v.split(' - ', 1)[1].strip()
    else:
        phone = v.strip()
    digits = ''.join(c for c in phone if c.isdigit())
    if len(digits) <= 4:
        return ''
    if not digits.startswith('55'):
        digits = '55' + digits
    return digits


def _strip_brackets(value: str) -> str:
    """Remove brackets do formato Pipefy: ["Valor"] → Valor"""
    return as_string(value).strip('[]"\'')
