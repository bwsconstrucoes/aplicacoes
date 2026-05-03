# -*- coding: utf-8 -*-
from __future__ import annotations

import requests
from .utils import as_string


def build_whatsapp_messages(plan, payload: dict) -> list[dict]:
    phone = as_string(payload.get('whatsapp_destino') or payload.get('phone'))
    if not phone:
        return []
    rec = plan.receipt
    msg = (
        f'✅ Baixa Bradesco processada\n\n'
        f'SP: {plan.match.id}\n'
        f'Valor: R$ {rec.valor_pago}\n'
        f'Data: {rec.data_pagamento}\n'
        f'Tipo: {rec.forma_pagamento or rec.tipo_comprovante}\n'
        f'Método: {plan.match.metodo}'
    )
    return [{'enabled': True, 'type': 'text', 'phone': phone, 'message': msg}]


def send_text(auth: dict, phone: str, message: str) -> dict:
    base = 'https://api.z-api.io/instances/' + as_string(auth.get('instanceId')) + '/token/' + as_string(auth.get('apiToken'))
    resp = requests.post(base + '/send-text', json={'phone': phone, 'message': message}, headers={'Client-Token': as_string(auth.get('clientToken'))}, timeout=20)
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    return {'ok': resp.status_code == 200, 'status': resp.status_code, 'body': body}
