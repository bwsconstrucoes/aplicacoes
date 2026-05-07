# -*- coding: utf-8 -*-
from __future__ import annotations

from decimal import Decimal
from typing import Dict, List
from .models import ExtractedReceipt, SpRecord, MatchResult
from .utils import money_to_decimal, normalize_text, normalize_compact, account_key, clean_account, only_digits

BEEVALE_MULTIPLICADOR = Decimal('1.015')
BEEVALE_TOL = Decimal('0.02')


def match_receipt(receipt: ExtractedReceipt, sps_index: Dict[str, SpRecord], sps_agendar: List[SpRecord]) -> MatchResult:
    if receipt.id_pipefy:
        sp = sps_index.get(receipt.id_pipefy)
        if sp:
            return MatchResult(status='localizado', metodo='id_comprovante', id=receipt.id_pipefy, sp=sp, motivo='ID localizado no comprovante e encontrado na SPsBD.')
        # Mesmo sem SPsBD, mantém ID como forte; depois Pipefy/Omie podem resolver.
        return MatchResult(status='localizado', metodo='id_comprovante_sem_spsbd', id=receipt.id_pipefy, sp=None, motivo='ID localizado no comprovante, mas não encontrado no índice local da SPsBD.')

    if receipt.tipo_comprovante == 'beevale':
        # BeeVale deve procurar na SPsBD completa, pois quando o comprovante chega
        # o registro normalmente já saiu da SPsAgendar e está como 'agendado'.
        cands = match_beevale(receipt, list(sps_index.values()))
        return _result_from_candidates(cands, 'beevale_valor_1015_spsbd', 'BeeVale por valor base = valor pago / 1,015, buscando na SPsBD.')

    if receipt.tipo_comprovante == 'fgts_rescisorio':
        cands = match_fgts(receipt, sps_agendar)
        return _result_from_candidates(cands, 'fgts_valor_natureza', 'FGTS/CEF por valor e natureza/descrição.')


    if receipt.tipo_comprovante == 'boleto':
        # Boleto sem ID deve procurar na SPsBD completa, pois após agendamento
        # normalmente sai da SPsAgendar e fica com Agendado = 'agendado'.
        cands = match_boleto_barcode(receipt, list(sps_index.values()))
        if cands:
            return _result_from_candidates(cands, 'boleto_codigo_barras_spsbd', 'Boleto localizado por código de barras + valor + status/agendamento na SPsBD.')

    cands = match_valor_conta_tipo(receipt, sps_agendar)
    if cands:
        return _result_from_candidates(cands, 'valor_conta_tipo', 'Comprovante sem ID localizado por valor + conta + tipo.')

    # Fallback: PIX/transferência sem ID onde agendador já marcou AB=agendado, O=Pagar
    cands = match_valor_conta_agendado(receipt, list(sps_index.values()))
    if cands:
        return _result_from_candidates(cands, 'valor_conta_agendado', 'Localizado por valor + conta + status Pagar/agendado na SPsBD.')

    if receipt.tipo_comprovante == 'transferencia':
        return MatchResult(status='transferencia_sem_sp', metodo='transferencia_sem_id', motivo='Transferência sem ID e sem SP única encontrada.')

    return MatchResult(status='nao_localizado', metodo='sem_match', motivo='Não foi possível localizar SP automaticamente.')


def _result_from_candidates(cands: List[SpRecord], metodo: str, motivo: str) -> MatchResult:
    if len(cands) == 1:
        return MatchResult(status='localizado', metodo=metodo, id=cands[0].id, sp=cands[0], candidatos=cands, motivo=motivo)
    if len(cands) > 1:
        return MatchResult(status='pendente_validacao', metodo=metodo, candidatos=cands, motivo=f'{motivo} Porém retornou {len(cands)} candidatos.')
    return MatchResult(status='nao_localizado', metodo=metodo, motivo=f'{motivo} Nenhum candidato encontrado.')


def match_beevale(receipt: ExtractedReceipt, records: List[SpRecord]) -> List[SpRecord]:
    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return []
    out = []
    for r in records:
        n = normalize_text(f'{r.nome_credor} {r.info_pgt} {r.tipo_pagamento}')
        if 'beevale' not in n:
            continue
        if normalize_compact(r.status_pgt) != 'pagar':
            continue
        if normalize_compact(r.status_agendamento) not in {'agendado', 'agendar', 'falhaagendar'}:
            continue
        base = money_to_decimal(r.valor_total)
        if base is None:
            continue
        esperado = (base * BEEVALE_MULTIPLICADOR).quantize(Decimal('0.01'))
        if abs(esperado - valor) <= BEEVALE_TOL:
            out.append(r)
    return out


def normalize_barcode(txt: str) -> str:
    return only_digits(txt or '')


def match_boleto_barcode(receipt: ExtractedReceipt, records: List[SpRecord]) -> List[SpRecord]:
    """Localiza boleto sem ID pela SPsBD completa.

    Critérios:
    - Tipo de pagamento = Boleto
    - Status Pgt = Pagar
    - Agendado em agendado/agendar/falhaagendar
    - Valor igual ao comprovante
    - Código de barras igual, normalizado apenas com números
    """
    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return []

    barcode_rec = normalize_barcode(getattr(receipt, 'codigo_barras', '') or '')
    if not barcode_rec:
        return []

    out = []
    for r in records:
        if normalize_compact(r.status_pgt) != 'pagar':
            continue

        if normalize_compact(r.status_agendamento) not in {'agendado', 'agendar', 'falhaagendar'}:
            continue

        if normalize_compact(r.tipo_pagamento) != 'boleto':
            continue

        if money_to_decimal(r.valor_total) != valor:
            continue

        barcode_sp = normalize_barcode(getattr(r, 'codigo_barras', '') or '')
        if barcode_sp and barcode_sp == barcode_rec:
            out.append(r)

    return out



def match_fgts(receipt: ExtractedReceipt, records: List[SpRecord]) -> List[SpRecord]:
    """Localiza FGTS/CEF/Ministério da Fazenda por valor + natureza/descrição."""
    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return []
    keywords = ['fgts', 'cef', 'caixa', 'rescisoes', 'rescisao',
                'ministerio da fazenda', 'ministerio', 'fazenda',
                'inss', 'rfb', 'receita federal']
    out = []
    for r in records:
        txt = normalize_text(f'{r.nome_credor} {r.descricao} {r.centro_custo} {r.tipo_pagamento}')
        if not any(x in txt for x in keywords):
            continue
        if money_to_decimal(r.valor_total) == valor:
            out.append(r)
    return out


def match_valor_conta_tipo(receipt: ExtractedReceipt, records: List[SpRecord]) -> List[SpRecord]:
    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return []
    tipo = normalize_compact(receipt.forma_pagamento)
    conta_key = account_key(receipt.agencia_origem, receipt.conta_origem)
    out = []
    for r in records:
        if money_to_decimal(r.valor_total) != valor:
            continue
        tipo_r = normalize_compact(r.tipo_pagamento)
        if tipo and tipo_r and tipo not in tipo_r and tipo_r not in tipo:
            if not (tipo == 'pix' and 'beevale' in normalize_compact(r.tipo_pagamento + r.info_pgt)):
                continue
        if conta_key:
            # padrão: 0624 | 0022069-8 | Conta-Corrente
            parts = [p.strip() for p in (r.conta_pagamento or '').split('|')]
            if len(parts) >= 2:
                rk = account_key(parts[0], parts[1])
                if rk != conta_key:
                    continue
        out.append(r)
    return out

def match_valor_conta_agendado(receipt: ExtractedReceipt, records: List[SpRecord]) -> List[SpRecord]:
    """Busca na SPsBD por valor + conta de débito + O=Pagar + AB=agendado.
    Cobre PIX sem ID onde o agendador já marcou AB=agendado mas a baixa
    ainda não foi registrada.
    """
    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return []
    conta_rec = normalize_compact(clean_account(receipt.conta_origem or ''))
    if not conta_rec:
        return []
    out = []
    for r in records:
        if normalize_compact(r.status_pgt) != 'pagar':
            continue
        if normalize_compact(r.status_agendamento) != 'agendado':
            continue
        if money_to_decimal(r.valor_total) != valor:
            continue
        conta_sp = normalize_compact(clean_account(r.conta_pagamento or ''))
        if conta_sp and conta_rec and conta_sp != conta_rec:
            continue
        out.append(r)
    return out