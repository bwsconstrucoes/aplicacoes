# -*- coding: utf-8 -*-
"""
core.py — Orquestração principal (portado do executeBusinessLogic_ do Principal.gs)
"""

import os
import json
import time
import logging
from base64 import b64decode

import gspread
from google.oauth2.service_account import Credentials

from .utils import as_string, formatar_moeda_br, to_number_br, number_to_br, round2, value_or_empty
from .boleto import secao_validacao_boleto_dda
from .pipefy import secao_pipefy
from .parametros_omie import secao_parametros_omie
from .omie import secao_omie
from .sheets import secao_atualiza_log_e_spsbd, atualizar_spsbd_codigo_integracao_omie

logger = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

CONFIG = {
    'SPREADSHEET_ID': '1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA',
    'SECRET':         os.getenv('ATUALIZASPBOTAO_SECRET', ''),
}


def _get_gc():
    creds_b64 = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
    if not creds_b64:
        raise RuntimeError('GOOGLE_CREDENTIALS_BASE64 não configurado.')
    creds_dict = json.loads(b64decode(creds_b64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def validar_payload(payload: dict):
    if not payload:
        raise ValueError('Payload vazio.')
    if payload.get('secret') != CONFIG['SECRET']:
        raise ValueError('Secret inválido.')
    if not as_string(payload.get('id')):
        raise ValueError('Campo id é obrigatório.')


def executar(payload: dict) -> dict:
    # Mede cada etapa e escreve UMA linha no log do Render ao final — é o que
    # diz onde estão os minutos quando o botão fica lento (13/11/2026: 155 s).
    sp_id  = as_string(payload.get('id'))
    tempos = {}
    marco  = [time.monotonic()]
    inicio = marco[0]

    def _medir(etapa: str):
        agora = time.monotonic()
        tempos[etapa] = round(agora - marco[0], 1)
        marco[0] = agora

    logger.info(f"[atualizaspbotao] SP {sp_id}: começou")
    try:
        return _executar(payload, _medir)
    finally:
        partes = ', '.join(f'{k} {v:.1f}s'.replace('.', ',') for k, v in tempos.items())
        total = f'{time.monotonic() - inicio:.1f}'.replace('.', ',')
        logger.info(f"[atualizaspbotao] SP {sp_id}: terminou em {total} s — {partes}")


def _executar(payload: dict, _medir) -> dict:
    gc = _get_gc()
    spreadsheet_id = payload.get('spreadsheetId') or CONFIG['SPREADSHEET_ID']
    ss = gc.open_by_key(spreadsheet_id)
    _medir('abrir planilha')

    result = {'secoes': {}}
    p = dict(payload)

    # 1. Validação de boleto/DDA
    result['secoes']['validacaoBoletoDDA'] = secao_validacao_boleto_dda(p, ss)
    p = _incorporar_validacao_boleto(p, result['secoes']['validacaoBoletoDDA'])
    _medir('boleto')

    # 2. Pipefy
    result['secoes']['pipefy'] = secao_pipefy(p, result['secoes']['validacaoBoletoDDA'])

    # 3. Parâmetros Omie (via planilha intermediária)
    if 'OmieApiA' in p:
        parametros_omie = secao_parametros_omie(p, gc)
        result['secoes']['parametrosOmie'] = parametros_omie
        p = _incorporar_parametros_omie(p, parametros_omie)
        _medir('planilha de cálculo')
    else:
        result['secoes']['parametrosOmie'] = {
            'ok': False, 'ignorado': True,
            'motivo': 'Campos OmieApiA:OmieApiV não enviados.'
        }

    # 4. Atualiza Log e SPsBD
    # Preenche LogCentro1..5 a partir do OmieRateioMultiplo se vierem vazios
    centros_rateio = _extrair_centros_do_rateio_multiplo(p)
    if centros_rateio:
        logger.info(f"[core] Centros extraídos do OmieRateioMultiplo: {list(centros_rateio.keys())}")
        p.update(centros_rateio)
    result['secoes']['atualizaLogeSPsBD'] = secao_atualiza_log_e_spsbd(ss, p)
    _medir('SPsBD')

    # 5. Omie (API externa)
    if as_string(p.get('omieAppKey')) and as_string(p.get('omieAppSecret')):
        result['secoes']['omie'] = secao_omie(p, result['secoes']['parametrosOmie'])
        _medir('Omie')

        codigo_integracao = as_string(
            result['secoes']['omie'].get('titulo', {}).get('codigo_lancamento_integracao')
        )
        if codigo_integracao:
            result['secoes']['atualizaSpsbdOmie'] = atualizar_spsbd_codigo_integracao_omie(
                ss, p, codigo_integracao
            )
            _medir('código Omie na SPsBD')
        else:
            result['secoes']['atualizaSpsbdOmie'] = {
                'ok': False, 'ignorado': True,
                'motivo': 'codigo_lancamento_integracao não retornado pela Omie.'
            }
    else:
        result['secoes']['omie'] = {
            'ok': False, 'ignorado': True,
            'motivo': 'Campos omieAppKey/omieAppSecret não enviados.'
        }
        result['secoes']['atualizaSpsbdOmie'] = {
            'ok': False, 'ignorado': True,
            'motivo': 'Seção Omie não executada.'
        }

    return result


# ---------------------------------------------------------------------------
# Enriquecimento de payload
# ---------------------------------------------------------------------------

def _incorporar_validacao_boleto(payload: dict, result_boleto: dict) -> dict:
    p = dict(payload)
    if result_boleto and result_boleto.get('executado'):
        p['SPsBDAI'] = value_or_empty(result_boleto.get('spsbdai') or 'INVALIDO')
    else:
        p['SPsBDAI'] = 'INVALIDO'
    return p


def _incorporar_parametros_omie(payload: dict, parametros_result: dict) -> dict:
    p = dict(payload)
    automacao = as_string(p.get('OmieApiAutomacao') or p.get('Automacao') or '')

    if (automacao == 'Gera Parcela' and
            parametros_result and parametros_result.get('ok') and
            parametros_result.get('saida')):
        s = parametros_result['saida']
        p['LogValor1'] = formatar_moeda_br(s.get('Centro de Custo 1 Valor'))
        p['LogValor2'] = formatar_moeda_br(s.get('Centro de Custo 2 Valor'))
        p['LogValor3'] = formatar_moeda_br(s.get('Centro de Custo 3 Valor'))
        p['LogValor4'] = formatar_moeda_br(s.get('Centro de Custo 4 Valor'))
        p['LogValor5'] = formatar_moeda_br(s.get('Centro de Custo 5 Valor'))
    else:
        p['LogValor1'] = _calcular_log_valor1(p)
        p['LogValor2'] = formatar_moeda_br(p.get('OmieApiP') or '')
        p['LogValor3'] = formatar_moeda_br(p.get('OmieApiQ') or '')
        p['LogValor4'] = formatar_moeda_br(p.get('OmieApiR') or '')
        p['LogValor5'] = formatar_moeda_br(p.get('OmieApiS') or '')
    return p


def _calcular_log_valor1(payload: dict) -> str:
    total = to_number_br(payload.get('SPsBDG') or '')
    v1    = to_number_br(payload.get('OmieApiO') or '')
    v2    = to_number_br(payload.get('OmieApiP') or '')
    v3    = to_number_br(payload.get('OmieApiQ') or '')
    v4    = to_number_br(payload.get('OmieApiR') or '')
    v5    = to_number_br(payload.get('OmieApiS') or '')
    calculado = round2(total - (v1 + v2 + v3 + v4 + v5) + v1)
    return number_to_br(calculado)


def _extrair_centros_do_rateio_multiplo(payload: dict) -> dict:
    """
    Quando LogCentro1..5 vierem vazios mas OmieRateioMultiplo tiver distribuicao,
    preenche LogCentro1..5 e LogValor1..5 a partir da distribuicao do rateio.
    O valor proporcional é calculado sobre SPsBDG (valor total).
    """
    import re as _re, json as _json

    tem_centro = any(payload.get(f'LogCentro{i}') for i in range(1, 6))
    if tem_centro:
        return {}

    rateio_raw = as_string(payload.get('OmieRateioMultiplo') or '')
    if not rateio_raw:
        return {}

    txt = rateio_raw.replace('\\"', '"')
    m = _re.search(r'"distribuicao"\s*:\s*(\[.*?\])', txt, _re.DOTALL)
    if not m:
        return {}

    try:
        distribuicao = _json.loads(m.group(1))
    except Exception:
        return {}

    if not distribuicao:
        return {}

    total = to_number_br(payload.get('SPsBDG') or '')
    extra = {}
    for i, item in enumerate(distribuicao[:5], start=1):
        nome = as_string(item.get('cDesDep') or '')
        perc = item.get('nPerDep') or 0
        if nome:
            extra[f'LogCentro{i}'] = nome
            if total and perc:
                valor_cc = round2(total * float(perc) / 100)
                extra[f'LogValor{i}'] = number_to_br(valor_cc)
            else:
                extra[f'LogValor{i}'] = ''

    return extra