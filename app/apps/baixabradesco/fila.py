# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .sheets import get_gc, SPS_SHEET_ID, execute_spsbd_updates
from .utils import as_string
from .omie import execute_omie, omie_body
from .pipefy import execute_graphql
from .zapi import resolve_zapi_auth, validate_zapi_auth, send_messages_batch

FILA_SHEET_NAME = 'BaixaBradescoFila'

HEADERS = [
    'Data Registro',
    'Status',
    'Tentativas',
    'Próxima Tentativa',
    'Tipo Falha',
    'ID SP',
    'Código Integração',
    'Arquivo',
    'Página',
    'Fingerprint',
    'Link Comprovante',
    'Etapa',
    'Mensagem Erro',
    'Payload Resumido',
    'Última Execução',
]

STATUS_PENDENTE = 'PENDENTE'
STATUS_CONCLUIDO = 'CONCLUIDO'
STATUS_FALHOU = 'FALHOU'


def now_str() -> str:
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')


def next_try(minutes: int = 10) -> str:
    return (datetime.now() + timedelta(minutes=minutes)).strftime('%d/%m/%Y %H:%M:%S')


def ensure_fila_sheet(gc=None):
    gc = gc or get_gc()
    ss = gc.open_by_key(SPS_SHEET_ID)
    try:
        ws = ss.worksheet(FILA_SHEET_NAME)
    except Exception:
        ws = ss.add_worksheet(title=FILA_SHEET_NAME, rows=1000, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option='USER_ENTERED')
        return ws

    values = ws.get_all_values()
    if not values:
        ws.append_row(HEADERS, value_input_option='USER_ENTERED')
    else:
        atuais = values[0]
        if atuais[:len(HEADERS)] != HEADERS:
            # Não apaga dados. Apenas garante cabeçalho mínimo nas primeiras colunas.
            ws.update('A1:O1', [HEADERS], value_input_option='USER_ENTERED')
    return ws


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({'raw': str(obj)}, ensure_ascii=False)


def _plan_payload_resumido(plan, etapa: str) -> Dict[str, Any]:
    rec = plan.receipt
    banco = plan.banco
    match = plan.match
    codigo_integracao = ''
    if match and match.sp and match.sp.codigo_integracao_omie:
        codigo_integracao = match.sp.codigo_integracao_omie
    elif match and match.id:
        codigo_integracao = 'Int' + as_string(match.id)

    return {
        'etapa': etapa,
        'sp_id': as_string(match.id if match else ''),
        'codigo_integracao': codigo_integracao,
        'arquivo': as_string(rec.filename),
        'pagina': rec.page,
        'fingerprint': as_string(rec.fingerprint),
        'link_comprovante': as_string(rec.drive_link),
        'data_pagamento': as_string(rec.data_pagamento),
        'valor_pago': as_string(rec.valor_pago),
        'acrescimos': as_string(rec.acrescimos or '0,00'),
        'codigo_conta_omie': as_string(banco.codigo_omie if banco else ''),
        'pipefy_update_mutation': as_string(getattr(plan, 'pipefy_update_mutation', '') or ''),
        'sheets_updates': getattr(plan, 'sheets_updates', []) or [],
        'whatsapp_messages': getattr(plan, 'whatsapp_messages', []) or [],
    }


def enqueue_failure(plan, etapa: str, tipo_falha: str, mensagem: str, payload: Optional[dict] = None, retry_minutes: int = 10) -> dict:
    """Registra uma falha temporária na fila. Nunca levanta exceção para não derrubar o fluxo principal."""
    try:
        ws = ensure_fila_sheet()
        rec = plan.receipt
        match = plan.match
        resumido = _plan_payload_resumido(plan, etapa)
        row = [
            now_str(),
            STATUS_PENDENTE,
            0,
            next_try(retry_minutes),
            tipo_falha,
            as_string(match.id if match else ''),
            as_string(resumido.get('codigo_integracao')),
            as_string(rec.filename),
            as_string(rec.page),
            as_string(rec.fingerprint),
            as_string(rec.drive_link),
            etapa,
            as_string(mensagem)[:1000],
            _safe_json(resumido),
            '',
        ]
        ws.append_row(row, value_input_option='USER_ENTERED')
        return {'ok': True, 'status': 'enfileirado', 'etapa': etapa, 'tipo_falha': tipo_falha}
    except Exception as e:
        return {'ok': False, 'status': 'erro_ao_enfileirar', 'error': str(e), 'etapa': etapa, 'tipo_falha': tipo_falha}


def _parse_dt_br(texto: str) -> Optional[datetime]:
    texto = as_string(texto)
    for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M'):
        try:
            return datetime.strptime(texto, fmt)
        except Exception:
            pass
    return None


def _rows_as_dicts(ws) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if not values:
        return []
    headers = values[0]
    out = []
    for idx, row in enumerate(values[1:], start=2):
        row = row + [''] * (len(headers) - len(row))
        d = {headers[i]: row[i] if i < len(row) else '' for i in range(len(headers))}
        d['_row_number'] = idx
        out.append(d)
    return out


def listar_pendentes(gc=None, limite: int = 20, somente_vencidos: bool = True) -> List[Dict[str, Any]]:
    ws = ensure_fila_sheet(gc)
    rows = _rows_as_dicts(ws)
    now = datetime.now()
    out = []
    for r in rows:
        if as_string(r.get('Status')).upper() != STATUS_PENDENTE:
            continue
        if somente_vencidos:
            dt = _parse_dt_br(r.get('Próxima Tentativa'))
            if dt and dt > now:
                continue
        out.append(r)
        if len(out) >= limite:
            break
    return out


def _update_row(ws, row_number: int, status: str, tentativas: int, mensagem: str = '', retry_minutes: int = 10):
    proxima = '' if status == STATUS_CONCLUIDO else next_try(retry_minutes)
    ws.update(f'B{row_number}:D{row_number}', [[status, tentativas, proxima]], value_input_option='USER_ENTERED')
    ws.update(f'M{row_number}:O{row_number}', [[as_string(mensagem)[:1000], '', now_str()]], value_input_option='USER_ENTERED')


def _request_omie(call: str, param: dict, payload: dict) -> dict:
    body = omie_body(call, param, payload)
    return execute_omie(body)


def _retry_omie(item: Dict[str, Any], payload: dict) -> dict:
    resumo = json.loads(item.get('Payload Resumido') or '{}')
    codigo = as_string(resumo.get('codigo_integracao'))
    if not codigo:
        return {'ok': False, 'erro': 'codigo_integracao_ausente'}

    consulta = _request_omie('ConsultarContaPagar', {'codigo_lancamento_integracao': codigo}, payload)
    body = consulta.get('body') or {}
    if as_string(body.get('status_titulo')).upper() == 'PAGO':
        return {'ok': True, 'status': 'ja_pago', 'consulta': consulta}
    if not consulta.get('ok'):
        return {'ok': False, 'erro': 'falha_consulta_omie', 'consulta': consulta}

    alterar = _request_omie('AlterarContaPagar', {
        'codigo_lancamento_integracao': codigo,
        'id_conta_corrente': as_string(resumo.get('codigo_conta_omie')),
        'valor_documento': _money_to_omie_number(resumo.get('valor_pago')),
    }, payload)
    if not alterar.get('ok'):
        return {'ok': False, 'erro': 'falha_alterar_omie', 'consulta': consulta, 'alterar': alterar}

    baixar = _request_omie('LancarPagamento', {
        'codigo_lancamento_integracao': codigo,
        'codigo_conta_corrente': as_string(resumo.get('codigo_conta_omie')),
        'codigo_baixa_integracao': 'Retry' + datetime.now().strftime('%d%m%Y%H%M%S'),
        'data': as_string(resumo.get('data_pagamento')),
        'valor': _money_to_omie_number(resumo.get('valor_pago')),
        'juros': _money_to_omie_number(resumo.get('acrescimos') or '0,00'),
        'observacao': 'Baixa realizada via baixabradesco/retry',
    }, payload)
    return {'ok': bool(baixar.get('ok')), 'consulta': consulta, 'alterar': alterar, 'baixar': baixar}


def _money_to_omie_number(valor: Any) -> str:
    s = as_string(valor)
    if not s:
        return '0.00'
    return s.replace('.', '').replace(',', '.')


def _retry_pipefy(item: Dict[str, Any], payload: dict) -> dict:
    resumo = json.loads(item.get('Payload Resumido') or '{}')
    mutation = as_string(resumo.get('pipefy_update_mutation'))
    if not mutation:
        return {'ok': False, 'erro': 'mutation_ausente'}
    return execute_graphql(mutation)


def _retry_zapi(item: Dict[str, Any], payload: dict) -> dict:
    resumo = json.loads(item.get('Payload Resumido') or '{}')
    msgs = resumo.get('whatsapp_messages') or []
    if not msgs:
        return {'ok': False, 'erro': 'mensagens_ausentes'}
    auth = resolve_zapi_auth(payload)
    missing = validate_zapi_auth(auth)
    if missing:
        return {'ok': False, 'erro': 'credenciais_zapi_ausentes', 'missing': missing}
    resp = send_messages_batch(auth, msgs)
    return {'ok': all(x.get('ok') for x in resp), 'responses': resp}


def _retry_sheets(item: Dict[str, Any], payload: dict) -> dict:
    resumo = json.loads(item.get('Payload Resumido') or '{}')
    updates = resumo.get('sheets_updates') or []
    if not updates:
        return {'ok': False, 'erro': 'updates_ausentes'}
    execute_spsbd_updates(updates)
    return {'ok': True, 'updates': len(updates)}


def reprocessar_fila(payload: dict) -> dict:
    """Reprocessa pendências vencidas da fila.\n\n    Body opcional:\n      {"limite": 10, "somente_vencidos": true, "omie": {...}}\n    """
    gc = get_gc()
    ws = ensure_fila_sheet(gc)
    limite = int(payload.get('limite') or 10)
    somente_vencidos = payload.get('somente_vencidos', True)
    pendentes = listar_pendentes(gc, limite=limite, somente_vencidos=bool(somente_vencidos))
    resultados = []

    for item in pendentes:
        row_number = int(item.get('_row_number'))
        tentativas = int(as_string(item.get('Tentativas') or '0') or 0) + 1
        etapa = as_string(item.get('Etapa')).lower()
        try:
            if etapa == 'omie':
                resp = _retry_omie(item, payload)
            elif etapa == 'pipefy':
                resp = _retry_pipefy(item, payload)
            elif etapa == 'zapi':
                resp = _retry_zapi(item, payload)
            elif etapa == 'sheets':
                resp = _retry_sheets(item, payload)
            else:
                resp = {'ok': False, 'erro': f'etapa_nao_suportada: {etapa}'}

            if resp.get('ok'):
                _update_row(ws, row_number, STATUS_CONCLUIDO, tentativas, 'Reprocessado com sucesso.')
            else:
                _update_row(ws, row_number, STATUS_PENDENTE if tentativas < 5 else STATUS_FALHOU, tentativas, _safe_json(resp))
            resultados.append({'row': row_number, 'etapa': etapa, 'ok': bool(resp.get('ok')), 'response': resp})
        except Exception as e:
            _update_row(ws, row_number, STATUS_PENDENTE if tentativas < 5 else STATUS_FALHOU, tentativas, str(e))
            resultados.append({'row': row_number, 'etapa': etapa, 'ok': False, 'error': str(e)})

    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'reprocessar_fila',
        'pendentes_processados': len(resultados),
        'resultados': resultados,
    }
