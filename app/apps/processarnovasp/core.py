# -*- coding: utf-8 -*-
"""
core.py — Orquestração principal do processarnovasp.

Equivalente ao router raiz do Make (módulo 6) + sub-roteadores 649/729/706/608.

Fluxo:
  1. Determina ROTA (transferencia | pagamento_futuro | padrao)
  2. Telemetria (cosmético, módulo 704)
  3. Executa ramo apropriado:
     - transferencia       → Pipefy 607 + SPsBD 489 + Log 731
     - pagamento_futuro    → Boleto + Pipefy 628 + SPsBD 642 + DDA + Log 738
     - padrao              → Boleto + Rateio + (vincular pedido)
                              + Omie (cliente+título) + Pipefy 601/613
                              + SPsBD 411/626 ou FalhaProcessar
                              + DDA + Log 738
                              + cancelar SP se boleto duplicado
"""

import os
import json
import logging
from base64 import b64decode

import gspread
from google.oauth2.service_account import Credentials

from .utils import as_string, to_number_br
from .lookups import normalizar_codigo_barras
from . import rateio as rateio_mod
from . import boleto as boleto_mod
from . import omie as omie_mod
from . import pipefy as pipefy_mod
from . import sheets as sheets_mod
from . import pedidos as pedidos_mod
from . import notify as notify_mod
from . import payload_adapter

logger = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

CONFIG = {
    'SECRET': os.getenv('PROCESSARNOVASP_SECRET', ''),
}


def _get_gc():
    creds_b64 = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
    if not creds_b64:
        raise RuntimeError('GOOGLE_CREDENTIALS_BASE64 não configurado.')
    creds_dict = json.loads(b64decode(creds_b64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# -----------------------------------------------------------------------------
# Validação
# -----------------------------------------------------------------------------

def validar_payload(payload: dict):
    if not payload:
        raise ValueError('Payload vazio.')
    secret_esperado = CONFIG['SECRET']
    if secret_esperado and payload.get('secret') != secret_esperado:
        raise ValueError('Secret inválido.')
    # id pode estar em payload['id'] (top-level Make ou formato plano)
    if not as_string(payload.get('id')):
        raise ValueError('Campo id é obrigatório.')

    # Este módulo processa APENAS SPs de 1x parcela.
    # SPs parceladas devem ir para o cenário "Processar SPs Parceladas".
    qtd_parc = as_string(payload.get('QuantidadeParcelas') or '').strip()
    if qtd_parc and qtd_parc not in ('', '1', '1x', 'Boleto', 'Único', 'Unico', 'À Vista'):
        # Tenta parsear o número de parcelas — se conseguir e for >1, rejeita
        n = None
        try:
            n = int(qtd_parc.split('x')[0].strip())
        except (ValueError, AttributeError):
            n = None  # campo livre, deixa passar
        if n is not None and n > 1:
            raise ValueError(
                f'SP {payload.get("id")} tem {n} parcelas. '
                f'Este endpoint processa apenas SPs de 1x — '
                f'use o cenário "Processar SPs Parceladas".'
            )


def _determinar_rota(payload: dict) -> str:
    procedimento = as_string(payload.get('Procedimento') or '')
    pag_futuro   = as_string(payload.get('PagamentoFuturoPedido') or '')
    antecipacao  = as_string(payload.get('AntecipacaoEntradaPedido') or '')

    if procedimento == 'Transferência de Recursos':
        return 'transferencia'
    if pag_futuro == 'Sim' or antecipacao == 'Sim':
        return 'pagamento_futuro'
    return 'padrao'


# -----------------------------------------------------------------------------
# Entrada principal
# -----------------------------------------------------------------------------

def executar(payload: dict) -> dict:
    # Adapta payload do Make (estrutura nested com 'fields') para chaves planas
    payload = payload_adapter.adaptar(payload)

    gc = _get_gc()
    result = {'secoes': {}}

    rota = _determinar_rota(payload)
    result['rota'] = rota
    logger.info(f'[core] SP={payload.get("id")} rota={rota}')

    # Telemetria (cosmético)
    if as_string(payload.get('registrar_telemetria') or 'true').lower() == 'true':
        result['secoes']['telemetria'] = sheets_mod.registrar_telemetria(gc, payload.get('telemetria') or {})

    # Dispatch
    if rota == 'transferencia':
        return _executar_transferencia(gc, payload, result)
    if rota == 'pagamento_futuro':
        return _executar_pagamento_futuro(gc, payload, result)
    return _executar_padrao(gc, payload, result)


# -----------------------------------------------------------------------------
# ROTA: Transferência de Recursos
# -----------------------------------------------------------------------------

def _executar_transferencia(gc, payload: dict, result: dict) -> dict:
    # 1. Pipefy: title + autorização + etiqueta (modulo 607)
    try:
        result['secoes']['pipefy'] = pipefy_mod.atualizar_card_transferencia(payload)
    except Exception as e:
        logger.exception('[transferencia] falha Pipefy')
        result['secoes']['pipefy'] = {'ok': False, 'erro': str(e)}

    # 2. SPsBD addRow (modulo 489)
    try:
        result['secoes']['spsbd'] = sheets_mod.inserir_spsbd(gc, payload, rota='transferencia')
    except Exception as e:
        logger.exception('[transferencia] falha SPsBD')
        result['secoes']['spsbd'] = {'ok': False, 'erro': str(e)}

    # 3. Rateio (sem Omie — mas usamos a saída pra montar log com CONS)
    try:
        rat = rateio_mod.calcular(payload, gc)
        result['secoes']['rateio'] = rat
        # 4. Log
        result['secoes']['log'] = sheets_mod.inserir_log(gc, payload, rat['descritivo'])
    except Exception as e:
        logger.exception('[transferencia] falha Log')
        result['secoes']['log'] = {'ok': False, 'erro': str(e)}
        notify_mod.alertar_falha_log(payload.get('id'))

    return result


# -----------------------------------------------------------------------------
# ROTA: Pagamento Futuro / Antecipação
# -----------------------------------------------------------------------------

def _executar_pagamento_futuro(gc, payload: dict, result: dict) -> dict:
    ss = gc.open_by_key(sheets_mod.PLANILHA_PRINCIPAL)

    # 1. Rateio (só pra ter descritivo)
    try:
        rat = rateio_mod.calcular(payload, gc)
        result['secoes']['rateio'] = rat
    except Exception as e:
        logger.exception('[pag_futuro] falha Rateio')
        result['secoes']['rateio'] = {'ok': False, 'erro': str(e)}
        rat = {'descritivo': {'centros_nomes': [], 'valores_cc': [0]*5, 'sp_duplicada': ''}}

    sp_duplicada = rat.get('descritivo', {}).get('sp_duplicada', '') or rat.get('saida', {}).get(21, '')

    # 2. Boleto: valida + adiciona em SPsDDA (se não duplicado)
    try:
        bol = boleto_mod.secao_boleto(payload, ss, sp_duplicada=sp_duplicada)
        result['secoes']['boleto'] = bol
    except Exception as e:
        logger.exception('[pag_futuro] falha boleto')
        result['secoes']['boleto'] = {'ok': False, 'erro': str(e), 'executado': False}
        bol = {}

    # 3. Pipefy (módulo 628) — atualiza title + etiqueta especial + boleto data
    try:
        result['secoes']['pipefy'] = pipefy_mod.atualizar_card_pagamento_futuro(payload, bol)
    except Exception as e:
        logger.exception('[pag_futuro] falha Pipefy')
        result['secoes']['pipefy'] = {'ok': False, 'erro': str(e)}

    # 4. SPsBD (módulo 642)
    try:
        result['secoes']['spsbd'] = sheets_mod.inserir_spsbd(
            gc, payload, rota='pagamento_futuro', boleto_secao=bol
        )
    except Exception as e:
        logger.exception('[pag_futuro] falha SPsBD')
        result['secoes']['spsbd'] = {'ok': False, 'erro': str(e)}

    # 5. Log (módulo 738)
    try:
        result['secoes']['log'] = sheets_mod.inserir_log(gc, payload, rat['descritivo'])
    except Exception as e:
        logger.exception('[pag_futuro] falha Log')
        result['secoes']['log'] = {'ok': False, 'erro': str(e)}
        notify_mod.alertar_falha_log(payload.get('id'))

    return result


# -----------------------------------------------------------------------------
# ROTA: Padrão (vai pro Omie)
# -----------------------------------------------------------------------------

def _executar_padrao(gc, payload: dict, result: dict) -> dict:
    ss = gc.open_by_key(sheets_mod.PLANILHA_PRINCIPAL)

    # 1. Rateio (substitui Bases Resumo)
    try:
        rat = rateio_mod.calcular(payload, gc)
        result['secoes']['rateio'] = rat
    except Exception as e:
        logger.exception('[padrao] falha Rateio')
        raise

    sp_duplicada = rat['descritivo']['sp_duplicada']

    # 2. Se boleto duplicado: cria card "Cancelar SP" e finaliza sem ir pro Omie
    if sp_duplicada:
        try:
            result['secoes']['cancelar_card'] = pipefy_mod.criar_card_cancelar_sp(payload, sp_duplicada)
        except Exception as e:
            logger.exception('[padrao] falha cancelar_card')
            result['secoes']['cancelar_card'] = {'ok': False, 'erro': str(e)}
        result['secoes']['boleto'] = {'executado': True, 'duplicado': True,
                                       'sp_duplicada': sp_duplicada, 'valido': None}
        return result

    # 3. Boleto: valida + adiciona em SPsDDA
    try:
        bol = boleto_mod.secao_boleto(payload, ss, sp_duplicada='')
        result['secoes']['boleto'] = bol
    except Exception as e:
        logger.exception('[padrao] falha boleto')
        result['secoes']['boleto'] = {'ok': False, 'erro': str(e), 'executado': False}
        bol = {}

    # 4. Vinculação a pedidos de compra (se houver)
    try:
        ped = pedidos_mod.vincular(payload, gc)
        result['secoes']['pedido'] = ped
    except Exception as e:
        logger.exception('[padrao] falha pedido')
        result['secoes']['pedido'] = {'ok': False, 'erro': str(e), 'executado': False}

    # 5. Omie (cliente + título) — só se tiver credenciais
    omie_ok = False
    if as_string(payload.get('omieAppKey')) and as_string(payload.get('omieAppSecret')):
        try:
            omie_secao = omie_mod.secao_omie(payload, rat['saida'], rat['descritivo'])
            result['secoes']['omie'] = omie_secao
            omie_ok = omie_secao.get('ok')
        except Exception as e:
            logger.exception('[padrao] falha Omie')
            result['secoes']['omie'] = {'ok': False, 'erro': str(e), 'falha': True}
    else:
        result['secoes']['omie'] = {
            'ok': False, 'ignorado': True,
            'motivo': 'omieAppKey/omieAppSecret não enviados.',
        }

    # 6. Trata 3 desfechos do Omie (routers 225/612)
    omie_secao = result['secoes']['omie']
    if omie_secao.get('falha'):
        # Falha de API → grava FalhaProcessar
        try:
            result['secoes']['falha_processar'] = sheets_mod.registrar_falha_processar(
                gc, payload, motivo='Cadastrar Título Omie'
            )
        except Exception as e:
            logger.exception('[padrao] falha FalhaProcessar')
            result['secoes']['falha_processar'] = {'ok': False, 'erro': str(e)}
        # Não grava SPsBD nem atualiza Pipefy (preserva semântica do Make)
        return result

    # 7. Se sucesso Omie ou duplicidade → atualiza Pipefy + SPsBD
    if omie_secao.get('ok') and not omie_secao.get('duplicado'):
        # Sucesso → Pipefy (módulo 601 ou 613) + conex_o_sp dos pedidos vinculados
        # tudo na MESMA mutation (1 round-trip HTTP em vez de N+1).
        ped_vinc = result['secoes'].get('pedido', {}).get('pedidos_vinculados') or []
        try:
            result['secoes']['pipefy'] = pipefy_mod.atualizar_card_pos_omie(
                payload, omie_secao, bol, pedidos_vinculados=ped_vinc
            )
        except Exception as e:
            logger.exception('[padrao] falha Pipefy')
            result['secoes']['pipefy'] = {'ok': False, 'erro': str(e)}

    # 8. SPsBD (módulo 411 ou 626 — mesmas linhas, só muda código Omie source)
    try:
        result['secoes']['spsbd'] = sheets_mod.inserir_spsbd(
            gc, payload, rota='padrao', omie_secao=omie_secao, boleto_secao=bol
        )
    except Exception as e:
        logger.exception('[padrao] falha SPsBD')
        result['secoes']['spsbd'] = {'ok': False, 'erro': str(e)}

    # 9. Log (módulo 738)
    try:
        result['secoes']['log'] = sheets_mod.inserir_log(gc, payload, rat['descritivo'])
    except Exception as e:
        logger.exception('[padrao] falha Log')
        result['secoes']['log'] = {'ok': False, 'erro': str(e)}
        notify_mod.alertar_falha_log(payload.get('id'))

    return result