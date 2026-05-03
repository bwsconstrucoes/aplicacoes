# -*- coding: utf-8 -*-
from __future__ import annotations

import os, requests
from .utils import as_string

PIPEFY_URL = 'https://api.pipefy.com/graphql'
PHASE_PAGO_ALIMENTAR_OMIE = '309521694'

FIELDS_TO_FETCH = [
    'Código Lançamento Integração Omie', 'Selecione o Procedimento', 'Automação Form',
    'Etiquetas', 'Pedido de Suprimentos'
]


def build_get_cards_query(card_ids: list[str]) -> str:
    blocks = []
    for i, cid in enumerate(card_ids):
        blocks.append(f'''
        c{i}: card(id: "{cid}") {{
          id
          title
          current_phase {{ name id }}
          labels {{ name id }}
          fields {{ name field {{ id label }} value }}
        }}''')
    return 'query {' + '\n'.join(blocks) + '\n}'


def execute_graphql(query: str) -> dict:
    token = os.getenv('PIPEFY_API_TOKEN', '')
    if not token:
        raise RuntimeError('PIPEFY_API_TOKEN não configurado.')
    resp = requests.post(PIPEFY_URL, json={'query': query}, headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}, timeout=30)
    try:
        data = resp.json()
    except Exception:
        data = {'raw': resp.text}
    return {'ok': resp.ok and not data.get('errors'), 'status': resp.status_code, 'body': data}


def build_update_card_mutation(plan) -> str:
    rec = plan.receipt
    cid = as_string(plan.match.id)
    banco_pipefy = as_string(plan.banco.codigo_pipefy if plan.banco else '')
    forma = rec.forma_pagamento or (plan.match.sp.tipo_pagamento if plan.match.sp else '')
    link = rec.drive_link
    aliases = []
    def add(alias, field_id, value):
        value = escape_gql(as_string(value))
        aliases.append(f'{alias}: updateCardField(input: {{card_id: "{cid}" field_id: "{field_id}" new_value: "{value}"}}) {{ clientMutationId }}')
    add('n1', 'valida_o_sp_1', 'Sim')
    add('n2', 'autoriza_o_dupla', 'SIM')
    add('n3', 'quantidade_de_parcelas', 'Integração')
    add('n4', 'data_do_pagamento', rec.data_pagamento)
    add('n5', 'valor_total_pago', rec.valor_pago)
    add('n6', 'forma_de_pagamento', forma)
    add('n7', 'banco', banco_pipefy)
    add('n8', 'comprovante_html_email', link.replace('dl=0', 'dl=1'))
    # A decisão de mover considera fase atual quando o get em lote estiver implementado em produção.
    aliases.append(f'n9: moveCardToPhase(input: {{card_id: "{cid}" destination_phase_id: {PHASE_PAGO_ALIMENTAR_OMIE}}}) {{ clientMutationId }}')
    return 'mutation {\n' + '\n'.join(aliases) + '\n}'


def escape_gql(value: str) -> str:
    return value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ')
