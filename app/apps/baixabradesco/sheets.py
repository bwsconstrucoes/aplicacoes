# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, base64
from typing import Dict, List, Optional
import gspread
from google.oauth2.service_account import Credentials
from .models import SpRecord, BankAccount
from .utils import as_string, normalize_compact, money_to_decimal, account_key, clean_account, only_digits

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SPS_SHEET_ID = '1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA'
BASE_BANCOS_ID = '1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk'


def get_gc():
    raw = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
    if not raw:
        raise RuntimeError('GOOGLE_CREDENTIALS_BASE64 não configurado.')
    creds = json.loads(base64.b64decode(raw).decode('utf-8'))
    credentials = Credentials.from_service_account_info(creds, scopes=SCOPES)
    return gspread.authorize(credentials)


def get_sheet_rows(gc, spreadsheet_id: str, aba: str) -> List[Dict[str, str]]:
    ws = gc.open_by_key(spreadsheet_id).worksheet(aba)
    values = ws.get_all_values()
    if not values:
        return []
    headers = [as_string(h) for h in values[0]]
    out = []
    for idx, row in enumerate(values[1:], start=2):
        row = row + [''] * (len(headers) - len(row))
        d = {headers[i]: row[i] if i < len(row) else '' for i in range(len(headers))}
        d['_row_number'] = idx
        out.append(d)
    return out


def load_spsbd_index(gc=None) -> Dict[str, SpRecord]:
    gc = gc or get_gc()
    rows = get_sheet_rows(gc, SPS_SHEET_ID, 'SPsBD')
    idx = {}
    for r in rows:
        sp = row_to_sp_record(r)
        if sp.id:
            idx[sp.id] = sp
    return idx


def load_spsagendar(gc=None) -> List[SpRecord]:
    """Lê a aba SPsAgendar, que já é o filtro operacional vindo da SPsBD."""
    gc = gc or get_gc()
    rows = get_sheet_rows(gc, SPS_SHEET_ID, 'SPsAgendar')
    return [row_to_spsagendar_record(r) for r in rows if as_string(r.get('ID') or r.get('A'))]


def row_to_sp_record(r: Dict[str, str]) -> SpRecord:
    # SPsBD headers conhecidos; mantém fallback por posição quando vier sem header esperado.
    return SpRecord(
        row_number=int(r.get('_row_number', 0)),
        id=as_string(r.get('ID') or r.get('A')),
        nome_credor=as_string(r.get('Nome do Credor')),
        cpf_cnpj=as_string(r.get('CPF/CNPJ')),
        descricao=as_string(r.get('Descrição da Despesa')),
        valor_total=as_string(r.get('Valor Total')),
        centro_custo=as_string(r.get('Centro de Custo')),
        tipo_pagamento=as_string(r.get('Tipo de Pagamento')),
        vencimento=as_string(r.get('Vencim.') or r.get('Vencimento')),
        codigo_integracao_omie=as_string(r.get('Código Integração')),
        status_pgt=as_string(r.get('Status Pgt')),
        status_agendamento=as_string(r.get('Status Agendamento') or r.get('Agendado')),
        info_pgt=as_string(r.get('Info de Pgt')),
        numero_nf=as_string(r.get('Nº da NF')),
        conta_pagamento=as_string(r.get('Conta Pagamento')),
        link_card=as_string(r.get('Card Link')),
        raw=r,
    )


def row_to_spsagendar_record(r: Dict[str, str]) -> SpRecord:
    # Query informada pelo usuário: A,D,E,G,Y,AC,J,AI,AB,H,C,F
    # Headers esperados: ID, Nome do Credor, CPF/CNPJ, Valor Total, Info de Pgt, Nº da NF, Tipo de Pagamento, Conta Pagamento, Agendado, Centro de Custo, Vencim., Descrição da Despesa
    return SpRecord(
        row_number=int(r.get('_row_number', 0)),
        id=as_string(r.get('ID')),
        nome_credor=as_string(r.get('Nome do Credor')),
        cpf_cnpj=as_string(r.get('CPF/CNPJ')),
        valor_total=as_string(r.get('Valor Total')),
        info_pgt=as_string(r.get('Info de Pgt')),
        numero_nf=as_string(r.get('Nº da NF')),
        tipo_pagamento=as_string(r.get('Tipo de Pagamento')),
        conta_pagamento=as_string(r.get('Conta Pagamento')),
        status_agendamento=as_string(r.get('Agendado')),
        centro_custo=as_string(r.get('Centro de Custo')),
        vencimento=as_string(r.get('Vencim.') or r.get('Vencimento')),
        descricao=as_string(r.get('Descrição da Despesa')),
        raw=r,
    )


def load_base_bancos(gc=None) -> List[BankAccount]:
    gc = gc or get_gc()
    rows = get_sheet_rows(gc, BASE_BANCOS_ID, 'BaseBancos')
    out = []
    for r in rows:
        # Aceita variações de header. Ajustaremos após validar a aba real.
        agencia = as_string(r.get('Agência') or r.get('Agencia') or r.get('AGENCIA'))
        conta = as_string(r.get('Conta') or r.get('CONTA'))
        codigo_omie = as_string(r.get('Código Omie') or r.get('Codigo Omie') or r.get('nCodCC') or r.get('Código Conta Omie (nCodCC)'))
        codigo_pipefy = as_string(r.get('Código Pipefy') or r.get('Codigo Pipefy') or r.get('Código Conta BWS Pipefy'))
        descricao = as_string(r.get('Descrição') or r.get('Descricao') or r.get('Conta BWS') or r.get('Banco'))
        banco = as_string(r.get('Banco') or r.get('BANCO'))
        ba = BankAccount(
            row_number=int(r.get('_row_number', 0)), banco=banco, agencia=agencia, conta=clean_account(conta),
            chave_normalizada=account_key(agencia, conta), codigo_omie=codigo_omie,
            codigo_pipefy=codigo_pipefy, descricao=descricao, raw=r,
        )
        if ba.codigo_omie or ba.codigo_pipefy or ba.conta:
            out.append(ba)
    return out


def find_bank_account(accounts: List[BankAccount], agencia: str, conta: str) -> Optional[BankAccount]:
    key = account_key(agencia, conta)
    if not key:
        return None
    for a in accounts:
        if a.chave_normalizada == key:
            return a
    # fallback por conta sem agência
    conta_norm = normalize_compact(clean_account(conta))
    for a in accounts:
        if conta_norm and normalize_compact(a.conta) == conta_norm:
            return a
    return None


def build_spsbd_updates(plan) -> List[dict]:
    if not plan.match or not plan.match.id:
        return []
    rec = plan.receipt
    banco = plan.banco
    return [{
        'sheet_id': SPS_SHEET_ID,
        'aba': 'SPsBD',
        'filtros': {'A': '=' + plan.match.id},
        'updates': {
            'X': rec.data_pagamento,       # Data do Pagamento
            'AG': rec.drive_link,          # Comprovante (ajustar se seu header/coluna for diferente)
            'AI': banco.descricao if banco else rec.conta_origem_raw,  # Conta Pagamento
            'AB': 'baixadoomie',           # status operacional sugerido
        }
    }]
