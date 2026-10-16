# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import requests
from typing import Dict, Any, List, Tuple

from .utils import as_string, decimal_to_omie, now_baixa_id, money_to_decimal, decimal_to_br

URL_CONTAPAGAR = 'https://app.omie.com.br/api/v1/financas/contapagar/'


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


# ⚠️ OS NOMES DA CREDENCIAL DO OMIE — 16/09/2026
#
# Este módulo procurava SÓ por `OMIE_BWS_APP_KEY`/`OMIE_BWS_APP_SECRET`. No
# Render, as variáveis chamam-se **`OMIE_KEY`/`OMIE_SECRET`** — que é o nome
# que o resto do monorepo já aceitava (`painel/sync/omie_client.py` e
# `emissaonf/credenciais.py` resolvem os dois apelidos há meses).
#
# O resultado: o painel e a emissão de NF achavam a credencial, e ESTE robô
# não. Pelo Make nunca apareceu, porque o Make manda a chave dentro do pedido.
# Pela tela do Análise de SPs, que depende do ambiente, o pedido saía com a
# chave vazia — e o Omie respondia, com estas palavras:
#
#     A chave de acesso não está preenchida ou não é válida.
#
# "Chave de acesso", no Omie, é a CREDENCIAL DA API. A mensagem parecia falar
# da chave do título, e por isso a investigação olhou dois dias para o lado
# errado.
#
# A ordem dos nomes é a MESMA dos outros dois módulos, de propósito: apelido
# resolvido em três lugares diferentes é apelido que vai divergir.
NOMES_APP_KEY = ('OMIE_KEY', 'OMIE_BWS_APP_KEY', 'OMIE_APP_KEY')
NOMES_APP_SECRET = ('OMIE_SECRET', 'OMIE_BWS_APP_SECRET', 'OMIE_APP_SECRET')


def _do_ambiente(*nomes: str) -> str:
    """O primeiro nome que existir no ambiente."""
    for nome in nomes:
        valor = os.getenv(nome, '')
        if valor and valor.strip():
            return valor
    return ''


def _limpar_credencial(valor: str) -> str:
    """Tira espaço e caractere invisível que vem grudado no copia-e-cola do
    portal do Omie. `app_key` e `app_secret` não têm espaço, então remover
    todos é seguro — é o mesmo tratamento do painel."""
    invisiveis = {0x200b, 0x200c, 0x200d, 0x2060, 0xfeff}
    return ''.join(c for c in as_string(valor)
                   if not (c.isspace() or ord(c) in invisiveis))


def credentials_from_payload(payload: dict) -> Tuple[str, str]:
    """Retorna (app_key, app_secret): primeiro o que veio no pedido (é assim
    que o Make manda), depois o ambiente."""
    omie = payload.get('omie') or {}
    app_key = _limpar_credencial(
        omie.get('app_key') or payload.get('omieAppKey')
        or _do_ambiente(*NOMES_APP_KEY))
    app_secret = _limpar_credencial(
        omie.get('app_secret') or payload.get('omieAppSecret')
        or _do_ambiente(*NOMES_APP_SECRET))
    return app_key, app_secret


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

    # BeeVale: usa valor pago diretamente. Demais: valor pago - acréscimos
    if rec.tipo_comprovante == 'beevale' or valor_pago is None:
        valor_doc = valor_pago
    else:
        valor_doc = valor_pago - (acresc or money_to_decimal('0'))

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


def build_omie_plan(plan, payload: dict) -> List[Dict[str, Any]]:
    """Monta sequência: consultar → alterar (se necessário) → baixar."""
    codigo = codigo_integracao(plan)

    return [
        {'step': 'consultar',             'request': build_consultar_conta_pagar(codigo, payload)},
        {'step': 'alterar_se_necessario', 'request': build_alterar_conta_pagar(plan, payload)},
        {'step': 'baixar',                'request': build_lancar_pagamento(plan, payload)},
    ]
