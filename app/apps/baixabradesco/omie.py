# -*- coding: utf-8 -*-
from __future__ import annotations

import os, requests
from typing import Dict, Any, List
from .utils import as_string, decimal_to_omie, now_baixa_id, money_to_decimal, decimal_to_br

URL_CONTAPAGAR = 'https://app.omie.com.br/api/v1/financas/contapagar/'


def credentials_from_payload(payload: dict) -> tuple[str, str]:
    return (
        as_string(payload.get('omieAppKey') or os.getenv('OMIE_BWS_APP_KEY')),
        as_string(payload.get('omieAppSecret') or os.getenv('OMIE_BWS_APP_SECRET')),
    )


def omie_body(call: str, param: dict, payload: dict) -> dict:
    app_key, app_secret = credentials_from_payload(payload)
    return {'call': call, 'param': [param], 'app_key': app_key, 'app_secret': app_secret}


def build_consultar_conta_pagar(codigo_integracao: str, payload: dict) -> dict:
    return omie_body('ConsultarContaPagar', {'codigo_lancamento_integracao': codigo_integracao}, payload)


def build_alterar_conta_pagar(plan, payload: dict) -> dict:
    rec = plan.receipt
    codigo = codigo_integracao(plan)
    valor_pago = money_to_decimal(rec.valor_pago)
    acresc = money_to_decimal(rec.acrescimos) or money_to_decimal('0')
    valor_doc = valor_pago
    # Regra herdada do Make: em geral altera título para valor pago - acréscimos; BeeVale ficava valor pago.
    if rec.tipo_comprovante != 'beevale' and valor_pago is not None:
        valor_doc = valor_pago - acresc
    return omie_body('AlterarContaPagar', {
        'valor_documento': decimal_to_omie(decimal_to_br(valor_doc)) if valor_doc is not None else decimal_to_omie(rec.valor_pago),
        'id_conta_corrente': as_string(plan.banco.codigo_omie if plan.banco else ''),
        'codigo_lancamento_integracao': codigo,
    }, payload)


def build_lancar_pagamento(plan, payload: dict) -> dict:
    rec = plan.receipt
    return omie_body('LancarPagamento', {
        'data': rec.data_pagamento,
        'valor': decimal_to_omie(rec.valor_pago),
        'juros': decimal_to_omie(rec.acrescimos or '0,00') or '0.00',
        'codigo_conta_corrente': as_string(plan.banco.codigo_omie if plan.banco else ''),
        'codigo_baixa_integracao': now_baixa_id(),
        'codigo_lancamento_integracao': codigo_integracao(plan),
        'observacao': 'Baixa realizada via baixabradesco',
    }, payload)


def codigo_integracao(plan) -> str:
    sp = plan.match.sp
    existente = as_string(sp.codigo_integracao_omie if sp else '')
    return existente or ('Int' + as_string(plan.match.id))


def execute_omie(body: dict) -> dict:
    resp = requests.post(URL_CONTAPAGAR, json=body, timeout=30)
    try:
        data = resp.json()
    except Exception:
        data = {'raw': resp.text}
    fault = as_string(data.get('faultstring') or data.get('faultcode'))
    return {'ok': 200 <= resp.status_code < 300 and not fault, 'status': resp.status_code, 'body': data, 'raw': resp.text}


def build_omie_plan(plan, payload: dict) -> List[dict]:
    codigo = codigo_integracao(plan)
    reqs = [
        {'step': 'consultar', 'request': build_consultar_conta_pagar(codigo, payload)},
        {'step': 'alterar_se_necessario', 'request': build_alterar_conta_pagar(plan, payload)},
        {'step': 'baixar', 'request': build_lancar_pagamento(plan, payload)},
    ]
    return reqs
