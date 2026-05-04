# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Dict, Any

import requests

from .utils import as_string
from .pipefy import get_field_value, get_current_phase


ZAPI_BASE = 'https://api.z-api.io/instances/{instance}/token/{token}'


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
            'documentUrl': link_comprovante.replace('dl=0', 'dl=1'),
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
                'documentUrl': link_comprovante.replace('dl=0', 'dl=1'),
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
