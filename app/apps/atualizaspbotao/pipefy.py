# -*- coding: utf-8 -*-
"""pipefy.py — Processamento de etiquetas Pipefy portado do EtiquetaPipefy.gs"""

import re
from .utils import as_string

MAPA_ETIQUETAS = {
    'Cancelada':                  '304753657',
    'Ordem de Pagamento':         '305263439',
    'Transferência de Recursos':  '307726886',
    'Rescisões e Indenizações':   '307726895',
    'Antecipação de Pagamento':   '309483248',
    'Pagamento Futuro':           '310655392',
    'Fundo Fixo':                 '310918018',
    'Parcela 1':                  '312365094',
    'Parcela 2':                  '312365095',
    'Parcela 3':                  '312365096',
    'Parcela 4':                  '312365097',
    'Parcela 5':                  '312365098',
    'Parcela 6':                  '312365099',
    'Parcela 7':                  '312365100',
    'Parcela 8':                  '312365101',
    'Parcela 9':                  '312365102',
    'Parcela 10':                 '312365103',
    'Descontar':                  '312881672',
    'Boleto':                     '313978748',
    'Autorização':                '314046752',
    'Pré-Análise':                '314046813',
    'Realizar Pgt':               '314046829',
    'Pago':                       '314057311',
    'Análise Criteriosa':         '316061620',
    'Movimentação':               '316973266',
    'Despesa com Colaborador':    '317422150',
    'BeeVale':                    '317521565',
}

ID_ETIQUETA_BOLETO = '313978748'


def _adicionar_se_nao_existir(lista: list, valor: str):
    v = as_string(valor)
    if v and v not in lista:
        lista.append(v)


def mapear_etiquetas_pipefy(texto: str) -> list:
    s = as_string(texto)
    if not s:
        return []
    s = re.sub(r'^\[', '', s)
    s = re.sub(r'\]$', '', s)
    partes = [p.strip().strip('"') for p in s.split(',') if p.strip()]
    ids = []
    for nome in partes:
        if nome == 'Boleto':
            continue  # remove boleto da base antiga
        id_etiqueta = MAPA_ETIQUETAS.get(nome)
        if id_etiqueta:
            _adicionar_se_nao_existir(ids, id_etiqueta)
    return ids


def secao_pipefy(payload: dict, result_boleto: dict) -> dict:
    etiquetas_brutas = as_string(payload.get('PipefyEtiquetasBrutas') or '')
    ids_atuais = mapear_etiquetas_pipefy(etiquetas_brutas)

    adicionar_boleto = bool(
        result_boleto and
        result_boleto.get('executado') is True and
        result_boleto.get('valido') is True
    )

    ids_finais = ids_atuais[:]
    if adicionar_boleto:
        _adicionar_se_nao_existir(ids_finais, ID_ETIQUETA_BOLETO)

    return {
        'ok': True,
        'etiquetasBase': ids_atuais,
        'etiquetasBaseCsv': ', '.join(ids_atuais),
        'etiquetasAtualizadas': ids_finais,
        'etiquetasAtualizadasCsv': ', '.join(ids_finais),
        'adicionouEtiquetaBoleto': adicionar_boleto,
    }
