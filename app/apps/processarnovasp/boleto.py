# -*- coding: utf-8 -*-
"""
boleto.py — validação de código de barras + adição em SPsDDA + duplicidade

Reaproveita 100% as funções `modulo10`, `modulo11_*`, `validar_*` do atualizaspbotao
(copiadas aqui para o módulo ser autônomo).
"""

import re
from datetime import date, timedelta
from .utils import as_string


# -----------------------------------------------------------------------------
# Algoritmos de DV (idênticos ao atualizaspbotao)
# -----------------------------------------------------------------------------

def modulo10(num: str) -> int:
    soma = 0
    mult = 2
    for ch in reversed(num):
        val = int(ch) * mult
        if val > 9:
            val -= 9
        soma += val
        mult = 1 if mult == 2 else 2
    resto = soma % 10
    return 0 if resto == 0 else 10 - resto


def modulo11_boleto(num: str) -> int:
    soma = 0
    peso = 2
    for ch in reversed(num):
        soma += int(ch) * peso
        peso = 2 if peso == 9 else peso + 1
    resto = soma % 11
    if resto in (0, 1, 10):
        return 1
    return 11 - resto


def modulo11_arrecadacao(num: str) -> int:
    soma = 0
    peso = 2
    for ch in reversed(num):
        soma += int(ch) * peso
        peso = 2 if peso == 9 else peso + 1
    resto = soma % 11
    if resto in (0, 1):
        return 0
    if resto == 10:
        return 1
    return 11 - resto


def interpretar_fator_vencimento(fator: str) -> dict:
    if fator == '0000':
        return {'data': 'Sem vencimento', 'confiavel': False}
    dias = int(fator)
    base = date(1997, 10, 7) + timedelta(days=dias)
    hoje = date.today()
    confiavel = 2020 <= base.year <= hoje.year + 10
    return {'data': base.strftime('%d/%m/%Y'), 'confiavel': confiavel}


def validar_boleto_bancario(linha: str) -> dict:
    blocos = [
        {'valor': linha[0:9],   'dv': int(linha[9])},
        {'valor': linha[10:20], 'dv': int(linha[20])},
        {'valor': linha[21:31], 'dv': int(linha[31])},
    ]
    dv_geral = int(linha[32])
    codigo_barras = (
        linha[0:4] + linha[32] + linha[33:47] +
        linha[4:9] + linha[10:20] + linha[21:31]
    )
    fator = codigo_barras[5:9]
    valor_campo = codigo_barras[9:19]
    valor = float(valor_campo) / 100
    vencimento_info = interpretar_fator_vencimento(fator)
    resultado = {
        'banco': codigo_barras[0:3],
        'moeda': codigo_barras[3],
        'valor': valor,
        'fator_vencimento': fator,
        'vencimento': vencimento_info['data'],
        'vencimento_detectado': vencimento_info['confiavel'],
        'codigo_barras': codigo_barras,
        'linha_digitavel': linha,
        'dvs_blocos': {},
        'dv_geral_valido': False,
        'dv_geral_esperado': None,
    }
    for i, bloco in enumerate(blocos):
        resultado['dvs_blocos'][f'bloco_{i+1}'] = modulo10(bloco['valor']) == bloco['dv']
    base = codigo_barras[0:4] + codigo_barras[5:]
    esperado = modulo11_boleto(base)
    resultado['dv_geral_esperado'] = esperado
    resultado['dv_geral_valido'] = esperado == dv_geral
    return resultado


def validar_arrecadacao(codigo: str) -> dict:
    valor_campo = codigo[4:15]
    valor = float(valor_campo) / 100
    resultado = {
        'tipo_recebedor': codigo[1],
        'moeda': codigo[2],
        'codigo': codigo,
        'valor': valor,
        'dvs_blocos': {},
    }
    for i in range(4):
        bloco = codigo[i*12: i*12 + 11]
        dv = int(codigo[i*12 + 11])
        fn = modulo10 if codigo[2] == '6' else modulo11_arrecadacao
        resultado['dvs_blocos'][f'bloco_{i+1}'] = fn(bloco) == dv
    return resultado


def validar_codigo_barras_generico(codigo: str) -> dict:
    resp = {'tipo': '', 'valido': False, 'detalhes': {}}
    if len(codigo) == 47:
        resp['tipo'] = 'boleto_bancario'
        resp['detalhes'] = validar_boleto_bancario(codigo)
        resp['valido'] = (
            all(resp['detalhes']['dvs_blocos'].values()) and
            resp['detalhes']['dv_geral_valido']
        )
    elif len(codigo) == 48 and codigo.startswith('8'):
        resp['tipo'] = 'arrecadacao'
        resp['detalhes'] = validar_arrecadacao(codigo)
        resp['valido'] = all(resp['detalhes']['dvs_blocos'].values())
    else:
        resp['tipo'] = 'desconhecido'
        resp['detalhes']['erro'] = 'Formato inválido ou número de dígitos incorreto'
    return resp


# -----------------------------------------------------------------------------
# Seção principal: validação + inserção em SPsDDA + flag duplicidade
# -----------------------------------------------------------------------------

def secao_boleto(payload: dict, ss, sp_duplicada: str = '') -> dict:
    """
    Valida o código de barras, e se válido E não duplicado adiciona na fila SPsDDA.

    sp_duplicada: ID da outra SP que já lançou esse código (vindo do rateio.calcular).
                  Se for vazio → segue normalmente.
                  Se for preenchido → não adiciona em SPsDDA (fluxo de cancelamento).
    """
    from .lookups import normalizar_codigo_barras

    tipo_pagamento = as_string(payload.get('TipoPagamento') or '')
    codigo_bruto   = as_string(payload.get('CodigoBarras') or '')
    procedimento   = as_string(payload.get('Procedimento') or '')

    # mesma regra de "deve validar" do atualizaspbotao
    deve_validar = (
        tipo_pagamento == 'Boleto' or
        (codigo_bruto != '' and tipo_pagamento not in ('Pix', 'BeeVale'))
    ) and procedimento != 'Transferência de Recursos'

    if not deve_validar:
        return {
            'ok': True, 'executado': False, 'valido': None,
            'duplicado': False, 'sp_duplicada': '',
            'codigo_barras': '', 'tipo': '',
            'dda': {'adicionado': False, 'classificado': False},
        }

    codigo    = normalizar_codigo_barras(codigo_bruto)
    validacao = validar_codigo_barras_generico(codigo)

    duplicado = bool(sp_duplicada)

    adicionou  = False
    classificou = False

    if validacao['valido'] and not duplicado and ss:
        _adicionar_linha_sps_dda(ss, payload.get('id', ''), codigo)
        adicionou = True
        _classificar_sps_dda(ss)
        classificou = True

    return {
        'ok': True,
        'executado': True,
        'valido': validacao['valido'],
        'duplicado': duplicado,
        'sp_duplicada': sp_duplicada,
        'codigo_barras': codigo,
        'tipo': validacao.get('tipo'),
        'detalhes': validacao.get('detalhes'),
        'dda': {'adicionado': adicionou, 'classificado': classificou},
    }


def _adicionar_linha_sps_dda(ss, id_val, codigo: str):
    from datetime import datetime
    sh = ss.worksheet('SPsDDA')
    agora = datetime.now().strftime('%d/%m/%Y %H:%M')
    sh.append_row(
        [agora, codigo, as_string(id_val), 'Baixar'],
        value_input_option='USER_ENTERED',
        insert_data_option='INSERT_ROWS',
    )


def _classificar_sps_dda(ss):
    """Ordena SPsDDA por coluna A descendente (mais recente no topo)."""
    sh = ss.worksheet('SPsDDA')
    last_row = sh.row_count
    if last_row <= 1:
        return
    try:
        sh.sort((1, 'des'), range=f'A2:D{last_row}')
    except Exception:
        # gspread.sort às vezes falha em planilhas grandes — fallback silencioso
        pass
