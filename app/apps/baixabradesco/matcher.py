# -*- coding: utf-8 -*-
from __future__ import annotations

from decimal import Decimal
from typing import Dict, List
from .models import ExtractedReceipt, SpRecord, MatchResult
from .utils import money_to_decimal, normalize_text, normalize_compact, account_key, clean_account

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
        # O comprovante chega depois do agendamento. Por isso a SP pode não estar
        # mais na visão SPsAgendar, mas continua na SPsBD com AB = agendado.
        cands = match_beevale(receipt, list(sps_index.values()))
        return _result_from_candidates(
            cands,
            'beevale_valor_1015_spsbd',
            'BeeVale por valor base = valor pago / 1,015, buscando na SPsBD.'
        )

    if receipt.tipo_comprovante == 'fgts_rescisorio':
        cands = match_fgts(receipt, sps_agendar)
        return _result_from_candidates(cands, 'fgts_valor_natureza', 'FGTS/CEF por valor e natureza/descrição.')

    cands = match_valor_conta_tipo(receipt, sps_agendar)
    if cands:
        return _result_from_candidates(cands, 'valor_conta_tipo', 'Comprovante sem ID localizado por valor + conta + tipo.')

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
    valor_pago = money_to_decimal(receipt.valor_pago)
    if valor_pago is None:
        return []

    out = []
    for r in records:
        n = normalize_text(f'{r.nome_credor} {r.info_pgt} {r.tipo_pagamento} {r.descricao}')
        if 'beevale' not in n:
            continue

        # Após o agendamento, o registro normalmente está em SPsBD com AB = agendado.
        status_pgt = normalize_compact(r.status_pgt)
        status_ag  = normalize_compact(r.status_agendamento)
        validacao  = normalize_compact((r.raw or {}).get('Validação') or (r.raw or {}).get('Validacao'))
        status_aut = normalize_compact(r.status_aut or (r.raw or {}).get('Status Aut.'))

        if status_pgt and status_pgt != 'pagar':
            continue
        if status_ag and status_ag not in {'agendar', 'agendado', 'falhaagendar'}:
            continue
        if validacao and validacao != 'sim':
            continue
        if status_aut and status_aut not in {'autorizado', 'preautorizado'}:
            continue

        base = money_to_decimal(r.valor_total)
        if base is None:
            continue

        # Regra BeeVale: comprovante = valor base * 1,015.
        esperado = (base * BEEVALE_MULTIPLICADOR).quantize(Decimal('0.01'))
        if abs(esperado - valor_pago) <= BEEVALE_TOL:
            out.append(r)
    return out


def match_fgts(receipt: ExtractedReceipt, records: List[SpRecord]) -> List[SpRecord]:
    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return []
    out = []
    for r in records:
        txt = normalize_text(f'{r.nome_credor} {r.descricao} {r.centro_custo}')
        if not any(x in txt for x in ['fgts', 'cef', 'caixa', 'rescisoes', 'rescisao']):
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
