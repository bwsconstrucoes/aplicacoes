# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import requests
from typing import Dict, Any, List, Tuple

from .utils import as_string, decimal_to_omie, now_baixa_id, money_to_decimal, decimal_to_br

URL_CONTAPAGAR = 'https://app.omie.com.br/api/v1/financas/contapagar/'
URL_LANCCC = 'https://app.omie.com.br/api/v1/financas/contacorrentelancamentos/'


# 🔹 NOVO: mascara credenciais para output
def mascarar_omie_request(request: dict) -> dict:
    if not isinstance(request, dict):
        return request

    seguro = dict(request)

    if "app_key" in seguro:
        seguro["app_key"] = "***REDACTED***"

    if "app_secret" in seguro:
        seguro["app_secret"] = "***REDACTED***"

    return seguro


def credentials_from_payload(payload: dict) -> Tuple[str, str]:
    """Retorna (app_key, app_secret) priorizando payload, depois env vars."""
    omie = payload.get('omie') or {}
    app_key = as_string(omie.get('app_key') or payload.get('omieAppKey') or os.getenv('OMIE_BWS_APP_KEY', ''))
    app_secret = as_string(omie.get('app_secret') or payload.get('omieAppSecret') or os.getenv('OMIE_BWS_APP_SECRET', ''))
    return app_key, app_secret


def omie_body(call: str, param: dict, payload: dict) -> dict:
    app_key, app_secret = credentials_from_payload(payload)
    return {'call': call, 'param': [param], 'app_key': app_key, 'app_secret': app_secret}


def build_consultar_conta_pagar(codigo_integracao: str, payload: dict) -> dict:
    return omie_body('ConsultarContaPagar', {'codigo_lancamento_integracao': codigo_integracao}, payload)


def build_alterar_conta_pagar(plan, payload: dict) -> dict:
    """AlterarContaPagar sempre executa para garantir valor correto do título.

    Regra (idêntica ao Make):
    - BeeVale: valor_documento = valor_pago (acréscimo é taxa da plataforma)
    - Demais:  valor_documento = valor_pago - acrescimos (valor original do título)
    """
    rec = plan.receipt
    codigo = codigo_integracao(plan)
    valor_pago = money_to_decimal(rec.valor_pago)
    acresc = money_to_decimal(rec.acrescimos) or money_to_decimal('0')

    if rec.tipo_comprovante == 'beevale' or valor_pago is None:
        valor_doc = valor_pago
    else:
        valor_doc = valor_pago - acresc

    return omie_body('AlterarContaPagar', {
        'valor_documento': decimal_to_omie(valor_doc) if valor_doc is not None else decimal_to_omie(rec.valor_pago),
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


def build_incluir_lanc_cc(plan, payload: dict, codigo_int: str = '') -> dict:
    """Lançamento de transferência entre contas (movimentação sem SP)."""
    rec = plan.receipt
    banco_origem = plan.banco
    return {
        'call': 'IncluirLancCC',
        'app_key': credentials_from_payload(payload)[0],
        'app_secret': credentials_from_payload(payload)[1],
        'param': [{
            'cCodIntLanc': codigo_int or ('IntCC' + now_baixa_id()),
            'cabecalho': {
                'nCodCC': as_string(banco_origem.codigo_omie if banco_origem else ''),
                'dDtLanc': rec.data_pagamento,
                'nValorLanc': decimal_to_omie(rec.valor_pago),
            },
            'transferencia': {
                'nCodCCDestino': as_string(rec.conta_destino_raw or ''),
            },
            'detalhes': {
                'cCodCateg': '0.01.01',
                'cTipo': 'TRA',
                'cObs': rec.forma_pagamento or 'Transferência de Recursos',
            },
        }],
    }


def codigo_integracao(plan) -> str:
    sp = plan.match.sp
    existente = as_string(sp.codigo_integracao_omie if sp else '')
    return existente or ('Int' + as_string(plan.match.id))


def execute_omie(body: dict) -> dict:
    try:
        resp = requests.post(URL_CONTAPAGAR, json=body, timeout=30)
        try:
            data = resp.json()
        except Exception:
            data = {'raw': resp.text}

        fault = as_string(data.get('faultstring') or data.get('faultcode') or '')

        return {
            'ok': 200 <= resp.status_code < 300 and not fault,
            'status': resp.status_code,
            'body': data,
            'raw': resp.text[:500],
        }
    except Exception as e:
        return {'ok': False, 'status': 0, 'body': {}, 'raw': str(e)}


def execute_omie_lanccc(body: dict) -> dict:
    try:
        resp = requests.post(URL_LANCCC, json=body, timeout=30)
        try:
            data = resp.json()
        except Exception:
            data = {'raw': resp.text}

        fault = as_string(data.get('faultstring') or data.get('faultcode') or '')

        return {
            'ok': 200 <= resp.status_code < 300 and not fault,
            'status': resp.status_code,
            'body': data,
        }
    except Exception as e:
        return {'ok': False, 'status': 0, 'body': {}, 'raw': str(e)}


def build_omie_plan(plan, payload: dict) -> List[Dict[str, Any]]:
    """Monta sequência: consultar → alterar (se necessário) → baixar."""
    codigo = codigo_integracao(plan)

    return [
        {'step': 'consultar',             'request': build_consultar_conta_pagar(codigo, payload)},
        {'step': 'alterar_se_necessario', 'request': build_alterar_conta_pagar(plan, payload)},
        {'step': 'baixar',                'request': build_lancar_pagamento(plan, payload)},
    ]