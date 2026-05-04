# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Dict, Any

import requests

from .utils import as_string
from .pipefy import get_field_value, pipefy_forma_pagamento

ZAPI_BASE = 'https://api.z-api.io/instances/{instance}/token/{token}'


def normalize_zapi_auth(payload: dict) -> dict:
    zapi = payload.get('zapi') or {}
    return {
        'instanceId': as_string(
            zapi.get('instance_id') or zapi.get('instanceId') or payload.get('zapi_instance_id') or payload.get('instanceId')
        ),
        'apiToken': as_string(
            zapi.get('api_token') or zapi.get('apiToken') or payload.get('zapi_api_token') or payload.get('apiToken')
        ),
        'clientToken': as_string(
            zapi.get('client_token') or zapi.get('clientToken') or payload.get('zapi_client_token') or payload.get('clientToken')
        ),
    }


def _zapi_headers(auth: dict) -> dict:
    return {'Client-Token': as_string(auth.get('clientToken'))}


def send_text(auth: dict, phone: str, message: str) -> dict:
    base = ZAPI_BASE.format(
        instance=as_string(auth.get('instanceId')),
        token=as_string(auth.get('apiToken')),
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
        token=as_string(auth.get('apiToken')),
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
    results = []
    for msg in messages:
        if not msg.get('enabled', True):
            results.append({'skipped': True, 'reason': 'disabled', 'phone': msg.get('phone')})
            continue
        phone = as_string(msg.get('phone'))
        if not phone or len(''.join(c for c in phone if c.isdigit())) <= 4:
            results.append({'skipped': True, 'reason': 'phone_invalido', 'phone': phone})
            continue
        msg_type = as_string(msg.get('type') or 'text')
        if msg_type == 'text':
            results.append(send_text(auth, phone, as_string(msg.get('message'))))
        elif msg_type == 'document':
            results.append(send_document(
                auth,
                phone,
                document_url=as_string(msg.get('documentUrl') or msg.get('document')),
                file_name=as_string(msg.get('fileName') or 'Comprovante de Pagamento'),
                doc_type=as_string(msg.get('docType') or 'pdf'),
            ))
        else:
            results.append({'skipped': True, 'reason': f'type_desconhecido: {msg_type}', 'phone': phone})
    return results


def build_whatsapp_messages(plan, payload: dict, card_info: dict | None = None) -> List[Dict[str, Any]]:
    """Monta as mensagens no padrão do antigo módulo 1847, mas para envio direto pela Z-API.

    Destinatários:
    - Responsável pela Solicitação;
    - Requerente, se houver e for diferente.

    Para cada destinatário envia:
    - texto de Informação de Pagamento;
    - PDF do comprovante, se houver link.
    """
    rec = plan.receipt
    card_info = card_info or {}

    resp_field = get_field_value(card_info, 'Responsável pela Solicitação')
    telefone_resp = _extract_phone(resp_field)
    if not telefone_resp:
        return []

    mensagem = _build_texto_pagamento(plan, card_info)
    link_comprovante = as_string(rec.drive_link).replace('dl=0', 'dl=1')

    msgs: List[Dict[str, Any]] = []

    def add_destino(phone: str):
        if not phone:
            return
        msgs.append({
            'enabled': True,
            'type': 'text',
            'phone': phone,
            'message': mensagem,
        })
        if link_comprovante:
            msgs.append({
                'enabled': True,
                'type': 'document',
                'docType': 'pdf',
                'phone': phone,
                'documentUrl': link_comprovante,
                'fileName': 'Comprovante de Pagamento',
            })

    add_destino(telefone_resp)

    requerente_field = get_field_value(card_info, 'Requerente')
    telefone_req = _extract_phone(requerente_field)
    if telefone_req and telefone_req != telefone_resp:
        add_destino(telefone_req)

    return msgs


def _build_texto_pagamento(plan, card_info: dict) -> str:
    rec = plan.receipt
    resp_field = get_field_value(card_info, 'Responsável pela Solicitação')
    ccs = []
    for i in range(1, 6):
        cc = _strip_pipefy_value(get_field_value(card_info, f'Centro de Custo {i}'))
        if cc:
            ccs.append(cc)
    centro_custo_str = '-'.join(ccs)
    descricao = get_field_value(card_info, 'Descrição da Despesa') or (plan.match.sp.descricao if plan.match.sp else '')
    descricao = as_string(descricao)[:255]
    forma = pipefy_forma_pagamento(plan) or rec.forma_pagamento or rec.tipo_comprovante

    return (
        '*Informação de Pagamento*\n\n'
        f'*Nº da SP:* {plan.match.id}\n'
        f'Data do Pagamento: {rec.data_pagamento}\n'
        f'Solicitante: {_strip_pipefy_value(resp_field)}\n'
        f'Centro de Custo: {centro_custo_str}\n'
        f'Descrição: {descricao}\n'
        f'Forma de Pagamento: {forma}'
    )


def _extract_phone(field_value: str) -> str:
    v = _strip_pipefy_value(field_value)
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


def _strip_pipefy_value(value: str) -> str:
    v = as_string(value).strip()
    # Campos de pessoa/dropdown do Pipefy costumam vir como ["Nome - telefone"]
    if v.startswith('[') and v.endswith(']'):
        v = v[1:-1]
    v = v.strip().strip('"').strip("'")
    return v
