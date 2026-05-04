# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import base64
from typing import Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

from .models import SpRecord, BankAccount
from .utils import as_string, normalize_compact, money_to_decimal, account_key, clean_account, only_digits
from .storage import normalize_shared_link

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SPS_SHEET_ID  = '1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA'
BASE_BANCOS_ID = '1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk'


def get_gc():
    raw = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
    if not raw:
        raise RuntimeError('GOOGLE_CREDENTIALS_BASE64 não configurado.')
    creds_dict = json.loads(base64.b64decode(raw).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_sheet_rows(gc, spreadsheet_id: str, aba: str) -> List[Dict[str, str]]:
    ws = gc.open_by_key(spreadsheet_id).worksheet(aba)
    values = ws.get_all_values()
    if not values:
        return []
    headers = [as_string(h) for h in values[0]]
    out = []
    for idx, row in enumerate(values[1:], start=2):
        row = row + [''] * (len(headers) - len(row))
        d = {headers[i]: (row[i] if i < len(row) else '') for i in range(len(headers))}
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
    """Lê a aba SPsAgendar — filtro operacional vindo de SPsBD."""
    gc = gc or get_gc()
    rows = get_sheet_rows(gc, SPS_SHEET_ID, 'SPsAgendar')
    return [row_to_spsagendar_record(r) for r in rows if as_string(r.get('ID') or r.get('A'))]


def load_spsbd_matching(gc=None) -> List[SpRecord]:
    """Base de busca para comprovantes SEM ID.

    Diferente da aba SPsAgendar, aqui lemos a SPsBD diretamente, porque quando o
    comprovante chega o registro normalmente já saiu da SPsAgendar por estar com
    AB = 'agendado'.

    Critérios equivalentes/ajustados da query operacional:
    - A/ID preenchido
    - O/Status Pgt = Pagar
    - AB/Agendado em agendar, agendado ou falhaagendar
    - AH/Validação = Sim
    - N/Status Aut. em Autorizado, Pré-Autorizado ou vazio
    - J/Tipo de Pagamento elegível: Pix, BeeVale, Boleto válido ou Transferência Bancária
    """
    gc = gc or get_gc()
    rows = get_sheet_rows(gc, SPS_SHEET_ID, 'SPsBD')
    out: List[SpRecord] = []
    for r in rows:
        sp = row_to_sp_record(r)
        if not sp.id:
            continue

        status_pgt = normalize_compact(sp.status_pgt)
        agendado = normalize_compact(sp.status_agendamento)
        validacao = normalize_compact(r.get('Validação'))
        status_aut = normalize_compact(r.get('Status Aut.'))
        tipo = as_string(sp.tipo_pagamento)
        tipo_norm = normalize_compact(tipo)
        info_norm = normalize_compact(sp.info_pgt)
        codigo_barras = as_string(r.get('Código de Barras'))

        if status_pgt != 'pagar':
            continue
        if agendado not in {'agendar', 'agendado', 'falhaagendar'}:
            continue
        if validacao != 'sim':
            continue
        if status_aut not in {'autorizado', 'preautorizado', ''}:
            continue

        elegivel = (
            tipo_norm == 'pix'
            or tipo_norm == 'beevale'
            or (tipo_norm == 'boleto' and 'INVALIDO' not in codigo_barras.upper())
            or tipo_norm == 'transferenciabancaria'
            or (tipo_norm == 'beevale' and 'beevale' in info_norm)
            or ('beevale' in info_norm)
        )
        if not elegivel:
            continue
        out.append(sp)
    return out


def row_to_sp_record(r: Dict[str, str]) -> SpRecord:
    # Mapeamento real da SPsBD (validado pelo usuário em 03/05/2026):
    # A=ID, B=Solicitação, C=Vencim., D=Nome do Credor, E=CPF/CNPJ,
    # F=Descrição da Despesa, G=Valor Total, H=Centro de Custo,
    # I=Tipo de Despesa, J=Tipo de Pagamento, K=Responsável pelo Registro,
    # L=Dt. Autorização, M=Responsável Autorização, N=Status Aut.,
    # O=Status Pgt, P=Código Integração, Q=Anexo Link, R=Card Link,
    # S=Anexo, T=Card, U=Status Aut. Símbolo, V=Status Pgt. Simbolo,
    # W=Pesquisa, X=Data do Pagamento, Y=Info de Pgt, Z=Parcela,
    # AA=Nº da NF, AB=Agendado, AC=Linha, AD=Nº do Pedido, AE=Anuente,
    # AF=Status Anuencia, AG=Comprovante, AH=Validação,
    # AI=Código de Barras, AJ=ID Pipefy Contrato, AK=Conta Pagamento
    return SpRecord(
        row_number             =int(r.get('_row_number', 0)),
        id                     =as_string(r.get('ID')),
        nome_credor            =as_string(r.get('Nome do Credor')),
        cpf_cnpj               =as_string(r.get('CPF/CNPJ')),
        descricao              =as_string(r.get('Descrição da Despesa')),
        valor_total            =as_string(r.get('Valor Total')),
        centro_custo           =as_string(r.get('Centro de Custo')),
        tipo_pagamento         =as_string(r.get('Tipo de Pagamento')),
        vencimento             =as_string(r.get('Vencim.') or r.get('Vencimento')),
        codigo_integracao_omie =as_string(r.get('Código Integração')),
        status_pgt             =as_string(r.get('Status Pgt')),
        status_agendamento     =as_string(r.get('Agendado')),
        info_pgt               =as_string(r.get('Info de Pgt')),
        numero_nf              =as_string(r.get('Nº da NF')),
        conta_pagamento        =as_string(r.get('Conta Pagamento')),
        link_card              =as_string(r.get('Card Link')),
        raw                    =r,
    )


def row_to_spsagendar_record(r: Dict[str, str]) -> SpRecord:
    # Colunas conforme query: A,D,E,G,Y,AC,J,AI,AB,H,C,F
    return SpRecord(
        row_number       =int(r.get('_row_number', 0)),
        id               =as_string(r.get('ID')),
        nome_credor      =as_string(r.get('Nome do Credor')),
        cpf_cnpj         =as_string(r.get('CPF/CNPJ')),
        valor_total      =as_string(r.get('Valor Total')),
        info_pgt         =as_string(r.get('Info de Pgt')),
        numero_nf        =as_string(r.get('Nº da NF')),
        tipo_pagamento   =as_string(r.get('Tipo de Pagamento')),
        conta_pagamento  =as_string(r.get('Conta Pagamento')),
        status_agendamento=as_string(r.get('Agendado')),
        centro_custo     =as_string(r.get('Centro de Custo')),
        vencimento       =as_string(r.get('Vencim.') or r.get('Vencimento')),
        descricao        =as_string(r.get('Descrição da Despesa')),
        raw              =r,
    )


def load_base_bancos(gc=None) -> List[BankAccount]:
    """Lê a aba BaseBancos.

    Formato real da planilha:
      Banco e Conta          | Código Conta Omie | Código | Título | CNPJ | Código Fornecedor Omie | Chave PIX
      Bradesco - 7011-4      | 583772104         | ...
      Bradesco - 22069-8     | 583782056         | ...
      Caixa - 2625-5         | 583779709         | ...

    A coluna 'Banco e Conta' tem o padrão: '<Banco> - <conta>'
    onde <conta> é o número da conta (sem agência separada).
    A agência é sempre 0624 para o Bradesco nas contas operacionais.
    """
    gc = gc or get_gc()
    rows = get_sheet_rows(gc, BASE_BANCOS_ID, 'BaseBancos')
    out = []
    for r in rows:
        banco_conta  = as_string(r.get('Banco e Conta') or r.get('Banco'))
        codigo_omie  = as_string(r.get('Código Conta Omie') or r.get('Código Omie') or r.get('nCodCC'))
        codigo_pipefy = as_string(r.get('Código') or r.get('Código Pipefy'))
        descricao    = as_string(r.get('Título') or r.get('Banco e Conta'))

        if not banco_conta or not codigo_omie:
            continue

        # Extrai banco e conta do padrão "Bradesco - 7011-4"
        if ' - ' in banco_conta:
            partes = banco_conta.split(' - ', 1)
            banco  = partes[0].strip()
            conta  = partes[1].strip()
        else:
            banco = banco_conta
            conta = ''

        # Agência: para Bradesco é sempre 0624; para outros extrai se houver
        agencia = _inferir_agencia(banco)

        ba = BankAccount(
            row_number        =int(r.get('_row_number', 0)),
            banco             =banco,
            agencia           =agencia,
            conta             =clean_account(conta),
            chave_normalizada =account_key(agencia, conta),
            codigo_omie       =codigo_omie,
            codigo_pipefy     =codigo_pipefy,
            descricao         =descricao or banco_conta,
            raw               =r,
        )
        out.append(ba)
    return out


def _inferir_agencia(banco: str) -> str:
    """Retorna a agência padrão por banco conforme operação BWS."""
    n = normalize_compact(banco)
    if 'bradesco' in n:
        return '0624'
    if 'caixa' in n or 'cef' in n:
        return '0477'
    if 'bb' in n or 'brasil' in n:
        return '3337'
    if 'inter' in n:
        return '0001'
    if 'sicredi' in n:
        return '0748'
    return ''


def find_bank_account(accounts: List[BankAccount], agencia: str, conta: str) -> Optional[BankAccount]:
    key = account_key(agencia, conta)
    if not key:
        return None
    for a in accounts:
        if a.chave_normalizada == key:
            return a
    # Fallback por conta sem agência
    conta_norm = normalize_compact(clean_account(conta))
    for a in accounts:
        if conta_norm and normalize_compact(a.conta) == conta_norm:
            return a
    return None


def build_spsbd_updates(plan) -> List[dict]:
    """Monta updates para a SPsBD com colunas validadas pelo usuário:
      O  = Status Pgt       → 'Pago'
      X  = Data do Pagamento
      AG = Comprovante (link Dropbox)
      AK = Conta Pagamento  → gravar só o número ex: '50024-0'
    """
    if not plan.match or not plan.match.id:
        return []
    rec   = plan.receipt
    banco = plan.banco

    # AK: grava só o número da conta, ex: '50024-0' (não 'Bradesco - 50024-0')
    conta = banco.conta if banco else rec.conta_origem

    return [{
        'sheet_id': SPS_SHEET_ID,
        'aba':      'SPsBD',
        'filtros':  {'A': '=' + plan.match.id},
        'updates':  {
            'O':  'Pago',
            'X':  rec.data_pagamento,
            'AG': normalize_shared_link(rec.drive_link),
            'AK': conta,
        },
    }]


def execute_spsbd_updates(updates: list):
    """Executa updates na planilha via gspread (chamada direta, sem GAS)."""
    if not updates:
        return
    gc = get_gc()
    for upd in updates:
        try:
            ss    = gc.open_by_key(upd['sheet_id'])
            sheet = ss.worksheet(upd['aba'])
            all_values = sheet.get_all_values()
            if not all_values:
                continue
            headers = all_values[0]

            # Monta mapa coluna_letra → índice_coluna_1based
            def letra_to_idx(letra: str) -> int:
                letra = letra.upper().strip()
                result = 0
                for ch in letra:
                    result = result * 26 + (ord(ch) - 64)
                return result

            filtros = upd.get('filtros', {})
            updates_cols = upd.get('updates', {})

            for row_idx, row in enumerate(all_values[1:], start=2):
                match = True
                for col_letra, condicao in filtros.items():
                    col_idx = letra_to_idx(col_letra) - 1
                    val = row[col_idx] if col_idx < len(row) else ''
                    op  = condicao[0] if condicao else '='
                    alvo = condicao[1:].strip()
                    if op == '=' and val != alvo:
                        match = False; break
                    elif op == '!' and val == alvo:
                        match = False; break
                if not match:
                    continue

                for col_letra, novo_val in updates_cols.items():
                    col_idx_1based = letra_to_idx(col_letra)
                    sheet.update_cell(row_idx, col_idx_1based, novo_val)
                break  # update_multiple=false: apenas primeira linha

        except Exception:
            pass