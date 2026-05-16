# -*- coding: utf-8 -*-
"""
rateio.py — Cálculo de rateio em Python puro (substitui a planilha Bases Resumo).

A planilha Bases Resumo recebia 22 inputs em A3:V3 e devolvia 22 outputs em A2:V2.
Aqui replicamos todos os outputs, sem ida-e-volta no Sheets.

ESTRUTURA DO RESULTADO (igual ao 221.`X` do Make):
    0..4  → cCodDep CC1..5    (lookup nome CC -> código Omie)
    5     → codigo_categoria  (lookup tipo despesa -> código)
    6     → vencimento (DD/MM/YYYY) — sem cálculo de feriado, conforme escopo "padrão"
    7,9,11,13,15 → valor_cc{1..5} (R$ x.xxx,xx) — texto humanizado
    8,10,12,14,16 → percentual_cc{1..5}  (% calculado: valor / total * 100)
    17    → valor_documento (soma dos valores rateados ou pago)
    18    → soma_percentual (sanity check, deve dar 100)
    19    → parcelado ('S'/'N')
    20    → cliente_omie     (código Omie ou 'naocadastrado')
    21    → sp_duplicada     ('' se cód. barras inédito ou da própria SP, senão ID da outra SP)
"""

import logging
from typing import Optional
from .utils import (
    as_string, has_value, limpar_colchetes, limpar_documento,
    to_number_br, number_to_br, round2, formatar_moeda_br,
    parse_data_pipefy, formatar_data_br,
)
from . import lookups

logger = logging.getLogger(__name__)


def calcular(p: dict, gc) -> dict:
    """
    p   = payload do webhook + parâmetros (mesmo formato consumido pelo atualizaspbotao)
    gc  = cliente gspread autenticado (para lookups)

    Retorna dict com chaves estilo '221.`X`' como inteiros 0..21
    + saída descritiva em 'descritivo' para uso no observacao do Omie.
    """
    # --- entradas básicas
    procedimento = as_string(p.get('Procedimento') or p.get('SelecioneProcedimento') or '')
    tipo_despesa_bruto = as_string(p.get('TipoDespesa') or '')

    centros_nomes = []
    for i in range(1, 6):
        nome = limpar_colchetes(p.get(f'CentroCusto{i}') or '')
        if not nome or nome == '[]':
            nome = ''
        centros_nomes.append(nome)

    # --- valores brutos do payload (todos em string BR ou US)
    valor_total = to_number_br(p.get('ValorTotalDespesa') or 0)
    valor_pago  = to_number_br(p.get('ValorTotalPago') or 0)

    valores_cc_raw = []
    for i in range(1, 6):
        valores_cc_raw.append(to_number_br(p.get(f'ValorCentroCusto{i}') or 0))

    rateado = as_string(p.get('RateioMultiCC') or '').lower() in ('sim', 's', 'true', '1')

    # --- ajusta valor CC1 quando não é rateado: assume o total
    if not rateado:
        valores_cc_raw[0] = valor_total
        # CC2..5 ficam zerados E sem nome (não vão pro Omie / observação)
        for i in range(1, 5):
            valores_cc_raw[i] = 0.0
            centros_nomes[i]  = ''

    # --- soma efetiva (base do percentual)
    soma = sum(v for v, n in zip(valores_cc_raw, centros_nomes) if n)
    if soma <= 0:
        # fallback: se todo mundo zerado, usa valor_total e distribui só pro CC1
        soma = valor_total
        if centros_nomes[0]:
            valores_cc_raw[0] = valor_total

    # --- percentuais
    perc_cc = []
    for v, n in zip(valores_cc_raw, centros_nomes):
        if not n or soma <= 0:
            perc_cc.append(0.0)
        else:
            perc_cc.append(round(v / soma * 100, 7))

    soma_perc = round2(sum(perc_cc))

    # --- valor_documento (col 17)
    valor_documento = valor_pago if valor_pago > 0 else (soma if soma > 0 else valor_total)

    # --- LOOKUPS ----------------------------------------------------------
    cod_cc = ['', '', '', '', '']
    for i, nome in enumerate(centros_nomes):
        if nome:
            cod_cc[i] = lookups.codigo_centro_custo(gc, nome) or ''

    # tipo de despesa: se procedimento for "Fundo Fixo", usa categoria "Fundo Fixo"
    if procedimento == 'Fundo Fixo':
        nome_tipo_despesa = 'Fundo Fixo'
    else:
        nome_tipo_despesa = limpar_colchetes(tipo_despesa_bruto)

    codigo_categoria = lookups.codigo_tipo_despesa(gc, nome_tipo_despesa) or ''

    # cliente Omie via CPF/CNPJ
    pessoa_tipo  = as_string(p.get('PessoaTipo') or '')   # 'Pessoa Física' / 'Pessoa Jurídica'
    cpf_credor   = as_string(p.get('CPFCredor') or '')
    cnpj_credor  = as_string(p.get('CNPJCredor') or '')
    if pessoa_tipo == 'Pessoa Física' or procedimento == 'Fundo Fixo':
        doc = limpar_documento(cpf_credor)
    else:
        doc = limpar_documento(cnpj_credor) or limpar_documento(cpf_credor)
    cliente_omie = lookups.codigo_cliente_omie(gc, doc) or 'naocadastrado'

    # SP duplicada (col 21): código de barras já usado em outra SP?
    cod_barras    = lookups.normalizar_codigo_barras(as_string(p.get('CodigoBarras') or ''))
    id_sp_atual   = as_string(p.get('id'))
    sp_duplicada  = ''
    if cod_barras:
        outra_sp = lookups.sp_por_codigo_barras(gc, cod_barras)
        if outra_sp and outra_sp != id_sp_atual:
            sp_duplicada = outra_sp

    # --- vencimento (col 6): só repassa o que veio
    venc = formatar_data_br(parse_data_pipefy(p.get('DataVencimento') or ''))

    parcelado = 'S' if rateado else 'N'

    # ----------------------------------------------------------------------
    # Monta saída exatamente igual ao que 221.`X` retornava
    # ----------------------------------------------------------------------
    saida = {
        0:  cod_cc[0],
        1:  cod_cc[1],
        2:  cod_cc[2],
        3:  cod_cc[3],
        4:  cod_cc[4],
        5:  codigo_categoria,
        6:  venc,
        7:  number_to_br(valores_cc_raw[0]),
        8:  perc_cc[0],
        9:  number_to_br(valores_cc_raw[1]),
        10: perc_cc[1],
        11: number_to_br(valores_cc_raw[2]),
        12: perc_cc[2],
        13: number_to_br(valores_cc_raw[3]),
        14: perc_cc[3],
        15: number_to_br(valores_cc_raw[4]),
        16: perc_cc[4],
        17: number_to_br(valor_documento),
        18: soma_perc,
        19: parcelado,
        20: cliente_omie,
        21: sp_duplicada,
    }

    descritivo = {
        'centros_nomes':    centros_nomes,
        'codigos_cc':       cod_cc,
        'valores_cc':       valores_cc_raw,
        'percentuais_cc':   perc_cc,
        'soma':             soma,
        'valor_total':      valor_total,
        'valor_pago':       valor_pago,
        'valor_documento':  valor_documento,
        'codigo_categoria': codigo_categoria,
        'cliente_omie':     cliente_omie,
        'rateado':          rateado,
        'sp_duplicada':     sp_duplicada,
    }

    logger.info(f"[rateio] SP={id_sp_atual} | soma={soma} | total={valor_total} | "
                f"pago={valor_pago} | rateado={rateado} | duplicada={sp_duplicada!r}")

    return {
        'ok':         True,
        'saida':      saida,        # mantém estilo 221.`X`
        'descritivo': descritivo,
    }
