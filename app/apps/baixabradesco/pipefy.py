# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import List, Optional, Dict, Any

import requests

from .utils import as_string

PIPEFY_URL = 'https://api.pipefy.com/graphql'
PHASE_PAGO_ALIMENTAR_OMIE = '309521694'

# Campos buscados no get em lote — usados para montar a mensagem WhatsApp e verificar fase
FIELDS_TO_FETCH = [
    'Código Lançamento Integração Omie',
    'Selecione o Procedimento',
    'Automação Form',
    'Etiquetas',
    'Pedido de Suprimentos',
    'Responsável pela Solicitação',
    'Centro de Custo 1',
    'Centro de Custo 2',
    'Centro de Custo 3',
    'Centro de Custo 4',
    'Centro de Custo 5',
    'Descrição da Despesa',
    'Data do Pagamento',
    'Valor Total Pago',
    'Requerente',
    'Conexão DC ID1',
    'Conexão DC ID2',
    'Conexão DC ID3',
    'Conexão DC ID4',
    'Conexão DC ID5',
    'Conexão DC ID6',
    'Conexão DC ID7',
    'Conexão DC ID8',
]


def build_get_cards_query(card_ids: List[str]) -> str:
    """Busca múltiplos cards em uma única chamada GraphQL."""
    ids_unicos = list(dict.fromkeys(str(i) for i in card_ids if i))
    blocks = []
    for i, cid in enumerate(ids_unicos):
        blocks.append(f'''
        c{i}: card(id: "{cid}") {{
          id
          title
          current_phase {{ name id }}
          labels {{ name id }}
          fields {{ name field {{ id label }} value }}
        }}''')
    return 'query {' + '\n'.join(blocks) + '\n}'


def get_field_value(card_info: Optional[Dict[str, Any]], field_name: str) -> str:
    """Extrai o valor de um campo pelo nome a partir do resultado do getCard em lote."""
    if not card_info:
        return ''
    for f in (card_info.get('fields') or []):
        field_meta = f.get('field') or {}
        label = as_string(field_meta.get('label') or f.get('name'))
        if label.lower() == field_name.lower():
            return as_string(f.get('value'))
    return ''


def get_current_phase(card_info: Optional[Dict[str, Any]]) -> str:
    if not card_info:
        return ''
    phase = card_info.get('current_phase') or {}
    return as_string(phase.get('name'))


def execute_graphql(query: str) -> dict:
    token = os.getenv('PIPEFY_API_TOKEN', '')
    if not token:
        raise RuntimeError('PIPEFY_API_TOKEN não configurado.')
    try:
        resp = requests.post(
            PIPEFY_URL,
            json={'query': query},
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            timeout=30,
        )
        try:
            data = resp.json()
        except Exception:
            data = {'raw': resp.text}
        return {'ok': resp.ok and not data.get('errors'), 'status': resp.status_code, 'body': data}
    except Exception as e:
        return {'ok': False, 'status': 0, 'body': {}, 'error': str(e)}


def build_update_card_mutation(plan, card_info: Optional[Dict[str, Any]] = None) -> str:
    """Monta a mutation de atualização do card com todos os campos necessários."""
    rec   = plan.receipt
    cid   = as_string(plan.match.id)
    banco_pipefy = as_string(plan.banco.codigo_pipefy if plan.banco else '')
    _forma_raw = rec.forma_pagamento or (plan.match.sp.tipo_pagamento if plan.match.sp else '')
    _FORMA_MAP = {
        'transferência bancária': 'Transferência',
        'transferencia bancaria': 'Transferência',
        'transferência': 'Transferência',
        'transferencia': 'Transferência',
        'ted': 'TED',
        'boleto': 'Boleto',
        'pix': 'Pix',
        'dda': 'DDA',
        'beevale': 'Pix',
        'débito em conta': 'Transferência',
        'debito em conta': 'Transferência',
    }
    forma = _FORMA_MAP.get(_forma_raw.lower().strip(), _forma_raw)
    link  = rec.drive_link.replace('?dl=0', '?dl=1').replace('&dl=0', '&dl=1') if rec.drive_link else ''

    fase_atual = get_current_phase(card_info)
    procedimento = get_field_value(card_info, 'Selecione o Procedimento')
    banco_destino_pipefy = ''  # será preenchido se for Transferência de Recursos

    aliases = []

    def add(alias: str, field_id: str, value: str):
        v = escape_gql(as_string(value))
        aliases.append(
            f'{alias}: updateCardField(input: {{card_id: "{cid}" field_id: "{field_id}" new_value: "{v}"}}) {{ clientMutationId }}'
        )

    add('n1', 'valida_o_sp_1',           'Sim')
    add('n2', 'autoriza_o_dupla',         'SIM')
    add('n3', 'quantidade_de_parcelas',   'Integração')
    add('n4', 'data_do_pagamento',        rec.data_pagamento)
    add('n5', 'valor_total_pago',         rec.valor_pago)
    add('n6', 'forma_de_pagamento',       forma)
    add('n7', 'banco',                    banco_pipefy)
    add('n8', 'comprovante_html_email',   link.replace('dl=0', 'dl=1') if link else '')

    # Move para fase "Pago / Alimentar Omie" somente se ainda não estiver lá
    if fase_atual != 'Pago / Alimentar Omie':
        aliases.append(
            f'n9: moveCardToPhase(input: {{card_id: "{cid}" destination_phase_id: {PHASE_PAGO_ALIMENTAR_OMIE}}}) {{ clientMutationId }}'
        )

    # Se for Transferência de Recursos, atualiza banco_destino
    if procedimento == 'Transferência de Recursos' and banco_destino_pipefy:
        add('n10', 'banco_destino', banco_destino_pipefy)

    # Move card Conexão DC1 para fase 340593562 (se preenchido)
    dc_id = as_string(get_field_value(card_info, 'Conexão DC1'))
    if dc_id and dc_id.strip():
        aliases.append(
            f'n_dc: moveCardToPhase(input: {{card_id: "{escape_gql(dc_id.strip())}" destination_phase_id: 340593562}}) {{ clientMutationId }}'
        )

    return 'mutation {\n' + '\n'.join(aliases) + '\n}'


def escape_gql(value: str) -> str:
    return value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ').replace('\r', '')