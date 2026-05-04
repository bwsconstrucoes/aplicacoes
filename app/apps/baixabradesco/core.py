# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
from typing import Dict, Any, List

import requests

from .models import AttachmentInput, ExecutionPlan
from .utils import b64decode_bytes, fingerprint_bytes, as_string
from .parser_pdf import extract_pdf_pages, extract_single_page_pdf
from .parser_bradesco import parse_bradesco_text
from .sheets import get_gc, load_spsbd_index, load_spsagendar, load_base_bancos, find_bank_account, build_spsbd_updates, execute_spsbd_updates
from .matcher import match_receipt
from .omie import build_omie_plan, build_incluir_lanc_cc, execute_omie, execute_omie_lanccc, codigo_integracao
from .pipefy import build_get_cards_query, build_update_card_mutation, execute_graphql
from .zapi import build_whatsapp_messages, send_messages_batch
from .storage import upload_dropbox_bytes, build_receipt_page_filename


def processar_baixabradesco(payload: Dict[str, Any]) -> Dict[str, Any]:
    modo_teste = bool(payload.get('modo_teste', True))
    attachments = normalize_attachments(payload)
    if not attachments:
        raise ValueError('Nenhum comprovante enviado. Use attachments/comprovantes com filename e base64 ou url.')

    opcoes = payload.get('opcoes') or {}
    executar_omie     = bool(opcoes.get('executar_omie', True))
    atualizar_spsbd   = bool(opcoes.get('atualizar_spsbd', True))
    atualizar_pipefy  = bool(opcoes.get('atualizar_pipefy', True))
    enviar_whatsapp   = bool(opcoes.get('enviar_whatsapp', False))
    salvar_comprovante = bool(opcoes.get('salvar_comprovante', True))
    pasta_dropbox = as_string(payload.get('pasta_dropbox') or '/BWS FINANCEIRO/COMPROVANTESTEMP/COMPROVANTESSP')

    # ── Carrega bases Google uma vez por lote ──────────────────────────────────
    gc = None
    sps_index = {}
    sps_agendar = []
    base_bancos = []
    google_error = ''

    try:
        gc = get_gc()
        sps_index   = load_spsbd_index(gc)
        sps_agendar = load_spsagendar(gc)
        base_bancos = load_base_bancos(gc)
    except Exception as e:
        google_error = str(e)
        if not modo_teste:
            raise RuntimeError(f'Falha ao carregar dados Google Sheets: {e}')

    # ── Processa cada comprovante / cada página ────────────────────────────────
    plans: List[ExecutionPlan] = []
    card_ids_para_get: List[str] = []

    for att in attachments:
        pdf_bytes = load_attachment_bytes(att)
        fp_file   = fingerprint_bytes(pdf_bytes, att.filename)
        pages     = extract_pdf_pages(pdf_bytes)

        for page_num, text in pages:
            if not as_string(text):
                continue

            rec = parse_bradesco_text(
                filename=att.filename,
                page=page_num,
                text=text,
                drive_link='',
                fingerprint=f'{fp_file}:{page_num}',
            )

            # Salva comprovante no Dropbox (em produção)
            storage_info = {'storage': 'dropbox', 'status': 'nao_salvo_modo_teste', 'url': '', 'path': ''}
            page_filename = build_receipt_page_filename(att.filename, page_num, rec.id_pipefy)

            if salvar_comprovante and not modo_teste:
                try:
                    page_pdf = extract_single_page_pdf(pdf_bytes, page_num)
                    storage_info = upload_dropbox_bytes(page_pdf, f'{pasta_dropbox}/{page_filename}')
                    storage_info['status'] = 'salvo'
                    rec.drive_link = storage_info.get('url', '')
                except Exception as e:
                    storage_info['status'] = f'erro_upload: {e}'
            else:
                storage_info['path_previsto'] = f'{pasta_dropbox.rstrip("/")}/{page_filename}'

            match = match_receipt(rec, sps_index, sps_agendar)
            banco = find_bank_account(base_bancos, rec.agencia_origem, rec.conta_origem) if base_bancos else None

            plan = ExecutionPlan(receipt=rec, match=match, banco=banco)
            plan.responses['storage'] = storage_info

            _decidir_execucao(plan, executar_omie, atualizar_pipefy, atualizar_spsbd, enviar_whatsapp)

            plan.omie_requests        = build_omie_plan(plan, payload)       if plan.match.id and executar_omie else []
            plan.pipefy_get_query     = ''                                   # preenchido depois em lote
            plan.pipefy_update_mutation = ''                                 # preenchido depois com dados do get
            plan.sheets_updates       = build_spsbd_updates(plan)           if plan.match.id and atualizar_spsbd else []
            plan.whatsapp_messages    = []  # montado depois do getCard, pois depende dos campos Pipefy

            if plan.match.id:
                card_ids_para_get.append(plan.match.id)

            plans.append(plan)

    # ── GET em lote no Pipefy (uma única chamada para todos os cards) ──────────
    card_data: Dict[str, Any] = {}
    pipefy_get_query = ''
    if card_ids_para_get and (atualizar_pipefy or enviar_whatsapp):
        pipefy_get_query = build_get_cards_query(list(set(card_ids_para_get)))
        for p in plans:
            if p.match.id:
                p.pipefy_get_query = pipefy_get_query

        if not modo_teste:
            try:
                resp  = execute_graphql(pipefy_get_query)
                if resp.get('ok') and resp.get('body', {}).get('data'):
                    for alias, cdata in resp['body']['data'].items():
                        if cdata and cdata.get('id'):
                            card_data[str(cdata['id'])] = cdata
            except Exception:
                pass  # falha no get não deve parar as demais atualizações possíveis

    # ── Executa planos em produção ─────────────────────────────────────────────
    if not modo_teste:
        for plan in plans:
            if not plan.pode_executar:
                continue

            card_info = card_data.get(str(plan.match.id)) if plan.match.id else None
            falha_omie = False

            # 1. Omie com retry curto. Se falhar, mantém as demais atualizações
            # e move o card para Falha Api no Pipefy.
            if plan.omie_requests and executar_omie:
                omie_log, falha_omie = _executar_sequencia_omie(plan)
                plan.responses['omie'] = omie_log
                plan.responses['omie_falhou'] = falha_omie

            # 2. Sheets SPsBD: comprovante bancário confirma pagamento, então marca Pago
            # mesmo que a baixa Omie precise ir para Falha Api.
            if plan.sheets_updates and atualizar_spsbd:
                _executar_sheets_async(plan.sheets_updates)

            # 3. Pipefy: atualiza campos sempre que possível. Se Omie falhou, move para Falha Api.
            if atualizar_pipefy and plan.match.id:
                mutation = build_update_card_mutation(plan, card_info, falha_omie=falha_omie)
                plan.pipefy_update_mutation = mutation
                plan.responses['pipefy'] = _executar_pipefy_batch([mutation])

            # 4. WhatsApp direto via Z-API, montado depois do getCard.
            if enviar_whatsapp and plan.match.id:
                plan.responses['pipefy_card_info'] = card_info
                plan.whatsapp_messages = build_whatsapp_messages(plan, payload)
                plan.responses.pop('pipefy_card_info', None)
                if plan.whatsapp_messages:
                    plan.responses['zapi'] = _executar_zapi(plan.whatsapp_messages, payload)

    # ── Monta output de modo_teste (preview de tudo que seria feito) ───────────
    return {
        'ok': True,
        'app': 'baixabradesco',
        'modo_teste': modo_teste,
        'resumo': {
            'comprovantes_recebidos': len(attachments),
            'paginas_processadas': len(plans),
            'localizados': sum(1 for p in plans if p.match.status == 'localizado'),
            'executaveis': sum(1 for p in plans if p.pode_executar),
            'nao_localizados': sum(1 for p in plans if p.match.status == 'nao_localizado'),
            'pendentes_validacao': sum(1 for p in plans if p.match.status == 'pendente_validacao'),
            'google_error': google_error,
        },
        'planos': [p.to_dict() for p in plans],
    }


# ── Helpers internos ──────────────────────────────────────────────────────────

def _decidir_execucao(plan: ExecutionPlan, executar_omie: bool, atualizar_pipefy: bool,
                      atualizar_spsbd: bool, enviar_whatsapp: bool):
    rec = plan.receipt

    # Movimentação sem SP — lança diretamente no Omie sem card Pipefy
    if rec.tipo_comprovante == 'movimentacao':
        plan.acao = 'lancar_movimentacao_omie'
        plan.pode_executar = True
        return

    if plan.match.status == 'transferencia_sem_sp':
        plan.acao = 'lancar_movimentacao_omie_sem_sp'
        plan.pode_executar = True
        return

    if plan.match.status != 'localizado' or not plan.match.id:
        plan.acao = 'pendente_validacao'
        plan.motivos_bloqueio.append(plan.match.motivo or 'SP não localizada.')
        return

    # Campos mínimos para baixar
    faltas = []
    if not rec.valor_pago:
        faltas.append('valor_pago')
    if not rec.data_pagamento:
        faltas.append('data_pagamento')
    if executar_omie and not (plan.banco and plan.banco.codigo_omie):
        faltas.append('codigo_conta_omie')

    if faltas:
        plan.acao = 'pendente_validacao'
        plan.motivos_bloqueio.append('Campos mínimos ausentes: ' + ', '.join(faltas))
        return

    plan.acao = 'baixar_omie_atualizar_pipefy_sheets'
    plan.pode_executar = True


def _executar_sequencia_omie(plan: ExecutionPlan) -> tuple[List[dict], bool]:
    """Consulta → Altera → Baixa, com 1 retry curto por etapa.

    Retorna (logs, falha_omie). Se o título já está PAGO, não é falha.
    Se qualquer etapa necessária falhar após retry, as demais integrações continuam,
    mas o Pipefy deve ser movido para Falha Api.
    """
    resultados: List[dict] = []
    falha_omie = False

    for req in plan.omie_requests:
        resp = _execute_omie_com_retry(req['request'])
        resultados.append({'step': req['step'], 'response': resp})

        if req['step'] == 'consultar':
            body = resp.get('body') or {}
            if as_string(body.get('status_titulo')).upper() == 'PAGO':
                resultados.append({'step': 'skip', 'motivo': 'Título já consta PAGO no Omie.'})
                return resultados, False
            if not resp.get('ok'):
                falha_omie = True
                resultados.append({'step': 'abort_consulta', 'motivo': 'Falha ao consultar título no Omie após retry.'})
                return resultados, True

        if req['step'] == 'alterar_se_necessario' and not resp.get('ok'):
            falha_omie = True
            resultados.append({'step': 'abort_apos_alterar', 'motivo': 'Falha ao alterar título no Omie após retry.'})
            return resultados, True

        if req['step'] == 'baixar' and not resp.get('ok'):
            falha_omie = True
            resultados.append({'step': 'erro_baixa', 'motivo': 'Falha ao lançar pagamento no Omie após retry.'})
            return resultados, True

    return resultados, falha_omie


def _execute_omie_com_retry(body: dict, tentativas: int = 2, delay: float = 3.0) -> dict:
    ultimo = None
    for i in range(max(1, tentativas)):
        ultimo = execute_omie(body)
        if ultimo.get('ok'):
            return ultimo
        if i < tentativas - 1:
            time.sleep(delay)
    return ultimo or {'ok': False, 'status': 0, 'body': {}, 'raw': 'Falha desconhecida Omie'}

def _executar_sheets_async(updates: list):
    def _run():
        try:
            execute_spsbd_updates(updates)
        except Exception:
            pass
    t = threading.Thread(target=_run, daemon=True)
    t.start()


def _executar_pipefy_batch(mutations: List[str]) -> List[dict]:
    resultados = []
    for mutation in mutations:
        try:
            resp = execute_graphql(mutation)
            resultados.append(resp)
        except Exception as e:
            resultados.append({'ok': False, 'error': str(e)})
    return resultados


def _executar_zapi(msgs: list, payload: dict) -> list:
    try:
        zapi = payload.get('zapi') or {}
        zapi_auth = {
            'instanceId': as_string(zapi.get('instance_id') or zapi.get('instanceId') or payload.get('zapi_instance_id') or payload.get('instanceId')),
            'apiToken':   as_string(zapi.get('api_token')   or zapi.get('apiToken')   or payload.get('zapi_api_token')   or payload.get('apiToken')),
            'clientToken':as_string(zapi.get('client_token')or zapi.get('clientToken')or payload.get('zapi_client_token')or payload.get('clientToken')),
        }
        if zapi_auth['instanceId'] and zapi_auth['apiToken'] and zapi_auth['clientToken']:
            return send_messages_batch(zapi_auth, msgs)
        return [{'ok': False, 'skipped': True, 'reason': 'credenciais_zapi_ausentes'}]
    except Exception as e:
        return [{'ok': False, 'error': str(e)}]

def load_attachment_bytes(att: AttachmentInput) -> bytes:
    if att.base64:
        return b64decode_bytes(att.base64)
    if att.url:
        resp = requests.get(att.url, timeout=60)
        resp.raise_for_status()
        return resp.content
    raise ValueError(f'Comprovante sem base64/url: {att.filename}')


def normalize_attachments(payload: Dict[str, Any]) -> List[AttachmentInput]:
    """Aceita dois formatos:
    1) lote: {"attachments": [...]} ou {"comprovantes": [...]}
    2) simples: {"filename": "...", "base64": "..."}
    """
    arr = payload.get('attachments') or payload.get('comprovantes') or []

    if not arr and (payload.get('filename') or payload.get('fileName') or
                    payload.get('base64') or payload.get('url')):
        arr = [{
            'filename': payload.get('filename') or payload.get('fileName') or payload.get('nome') or 'comprovante.pdf',
            'base64':   payload.get('base64') or payload.get('data') or '',
            'url':      payload.get('url') or payload.get('link') or '',
        }]

    if isinstance(arr, dict):
        arr = [arr]

    out = []
    for item in arr:
        if not item:
            continue
        out.append(AttachmentInput(
            filename=as_string(item.get('filename') or item.get('fileName') or item.get('nome') or 'comprovante.pdf'),
            base64  =as_string(item.get('base64') or item.get('data') or ''),
            url     =as_string(item.get('url') or item.get('link') or ''),
        ))
    return out
