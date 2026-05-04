# -*- coding: utf-8 -*-
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List
from .models import ExtractedReceipt, SpRecord, MatchResult
from .utils import money_to_decimal, normalize_text, normalize_compact, account_key

BEEVALE_MULTIPLICADOR = Decimal('1.015')
BEEVALE_TOL = Decimal('0.02')
TWOPLACES = Decimal('0.01')


def match_receipt(
    receipt: ExtractedReceipt,
    sps_index: Dict[str, SpRecord],
    sps_agendar: List[SpRecord],
    sps_matching: List[SpRecord] | None = None,
) -> MatchResult:
    """Localiza o comprovante na base de SPs.

    sps_agendar continua disponível, mas comprovantes sem ID devem consultar
    preferencialmente sps_matching, que vem da SPsBD e aceita AB='agendado'.
    """
    base_sem_id = sps_matching or sps_agendar

    if receipt.id_pipefy:
        sp = sps_index.get(receipt.id_pipefy)
        if sp:
            return MatchResult(
                status='localizado', metodo='id_comprovante', id=receipt.id_pipefy, sp=sp,
                motivo='ID localizado no comprovante e encontrado na SPsBD.'
            )
        # Mesmo sem SPsBD, mantém ID como forte; depois Pipefy/Omie podem resolver.
        return MatchResult(
            status='localizado', metodo='id_comprovante_sem_spsbd', id=receipt.id_pipefy, sp=None,
            motivo='ID localizado no comprovante, mas não encontrado no índice local da SPsBD.'
        )

    if receipt.tipo_comprovante == 'beevale':
        cands = match_beevale(receipt, base_sem_id)
        return _result_from_candidates(cands, 'beevale_valor_1015_spsbd', 'BeeVale por valor base = valor pago / 1,015, buscando na SPsBD.')

    if receipt.tipo_comprovante == 'fgts_rescisorio':
        cands = match_fgts(receipt, base_sem_id)
        return _result_from_candidates(cands, 'fgts_valor_natureza_spsbd', 'FGTS/CEF por valor e natureza/descrição, buscando na SPsBD.')

    cands = match_valor_conta_tipo(receipt, base_sem_id)
    if cands:
        return _result_from_candidates(cands, 'valor_conta_tipo_spsbd', 'Comprovante sem ID localizado por valor + conta + tipo, buscando na SPsBD.')

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
    """Localiza BeeVale pelo valor do comprovante.

    O comprovante BeeVale vem com acréscimo de 1,5%. Portanto, se o comprovante
    for R$ 1.776,25, a SP geralmente está em R$ 1.750,00.
    """
    valor_pago = money_to_decimal(receipt.valor_pago)
    if valor_pago is None:
        return []

    valor_base = (valor_pago / BEEVALE_MULTIPLICADOR).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    out = []
    for r in records:
        n = normalize_text(f'{r.nome_credor} {r.info_pgt} {r.tipo_pagamento}')
        if 'beevale' not in n:
            continue
        base = money_to_decimal(r.valor_total)
        if base is None:
            continue
        esperado = (base * BEEVALE_MULTIPLICADOR).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
        if abs(base - valor_base) <= BEEVALE_TOL or abs(esperado - valor_pago) <= BEEVALE_TOL:
            out.append(r)
    return out


def match_fgts(receipt: ExtractedReceipt, records: List[SpRecord]) -> List[SpRecord]:
    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return []
    out = []
    for r in records:
        txt = normalize_text(f'{r.nome_credor} {r.descricao} {r.centro_custo} {r.tipo_pagamento}')
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
        info_r = normalize_compact(r.info_pgt)
        if tipo and tipo_r and tipo not in tipo_r and tipo_r not in tipo:
            if not (tipo == 'pix' and ('beevale' in tipo_r or 'beevale' in info_r)):
                continue
        if conta_key:
            # padrão: 0624 | 0022069-8 | Conta-Corrente
            parts = [p.strip() for p in (r.conta_pagamento or '').split('|')]
            if len(parts) >= 2:
                rk = account_key(parts[0], parts[1])
                if rk != conta_key:
                    continue
            # Se a SP ainda não tem Conta Pagamento, não elimina por conta.
        out.append(r)
    return out
