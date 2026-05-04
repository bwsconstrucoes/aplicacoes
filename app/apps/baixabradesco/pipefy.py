# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import List, Optional, Dict, Any

import requests

from .utils import as_string

PIPEFY_URL = 'https://api.pipefy.com/graphql'
PHASE_PAGO_ALIMENTAR_OMIE = '309521694'
PHASE_FALHA_API = '310785170'

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
    """Extrai o valor de um campo pelo nome/label a partir do resultado do getCard."""
    if not card_info:
        return ''
    wanted = as_string(field_name).strip().lower()
    for f in (card_info.get('fields') or []):
        label = as_string(f.get('field', {}).get('label') or f.get('name')).strip().lower()
        fid = as_string(f.get('field', {}).get('id')).strip().lower()
        name = as_string(f.get('name')).strip().lower()
        if wanted in {label, fid, name}:
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


def pipefy_forma_pagamento(plan) -> str:
    """Valor aceito no campo forma_de_pagamento do Pipefy.

    O processo interno pode classificar como BeeVale, mas no Pipefy o campo aceita Pix.
    """
    rec = plan.receipt
    forma = rec.forma_pagamento or (plan.match.sp.tipo_pagamento if plan.match.sp else '')
    if as_string(forma).strip().lower() == 'beevale':
        return 'Pix'
    return forma


def build_update_card_mutation(
    plan,
    card_info: Optional[Dict[str, Any]] = None,
    move_to_falha_api: bool = False,
) -> str:
    """Monta a mutation de atualização do card com todos os campos necessários.

    Se a baixa Omie falhar após retry, atualiza os campos normalmente e move para
    Falha Api (310785170), em vez de mover para Pago / Alimentar Omie.
    """
    rec = plan.receipt
    cid = as_string(plan.match.id)
    banco_pipefy = as_string(plan.banco.codigo_pipefy if plan.banco else '')
    forma = pipefy_forma_pagamento(plan)
    link = rec.drive_link

    fase_atual = get_current_phase(card_info)
    procedimento = get_field_value(card_info, 'Selecione o Procedimento')
    banco_destino_pipefy = ''

    aliases = []

    def add(alias: str, field_id: str, value: str):
        v = escape_gql(as_string(value))
        aliases.append(
            f'{alias}: updateCardField(input: {{card_id: "{cid}" field_id: "{field_id}" new_value: "{v}"}}) {{ clientMutationId }}'
        )

    add('n1', 'valida_o_sp_1', 'Sim')
    add('n2', 'autoriza_o_dupla', 'SIM')
    add('n3', 'quantidade_de_parcelas', 'Integração')
    add('n4', 'data_do_pagamento', rec.data_pagamento)
    add('n5', 'valor_total_pago', rec.valor_pago)
    add('n6', 'forma_de_pagamento', forma)
    add('n7', 'banco', banco_pipefy)
    add('n8', 'comprovante_html_email', link.replace('dl=0', 'dl=1') if link else '')

    if move_to_falha_api:
        aliases.append(
            f'n9: moveCardToPhase(input: {{card_id: "{cid}" destination_phase_id: {PHASE_FALHA_API}}}) {{ clientMutationId }}'
        )
    elif fase_atual != 'Pago / Alimentar Omie':
        aliases.append(
            f'n9: moveCardToPhase(input: {{card_id: "{cid}" destination_phase_id: {PHASE_PAGO_ALIMENTAR_OMIE}}}) {{ clientMutationId }}'
        )

    if procedimento == 'Transferência de Recursos' and banco_destino_pipefy:
        add('n10', 'banco_destino', banco_destino_pipefy)

    return 'mutation {\n' + '\n'.join(aliases) + '\n}'


def build_move_falha_api_mutation(card_id: str) -> str:
    cid = escape_gql(as_string(card_id))
    return f'mutation {{\nmoveFalhaApi: moveCardToPhase(input: {{card_id: "{cid}" destination_phase_id: {PHASE_FALHA_API}}}) {{ clientMutationId }}\n}}'


def escape_gql(value: str) -> str:
    return as_string(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ').replace('\r', '')
