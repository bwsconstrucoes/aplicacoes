# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
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
from .zapi import build_whatsapp_messages, send_messages_batch, normalize_zapi_auth
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
            plan.whatsapp_messages    = []

            if plan.match.id:
                card_ids_para_get.append(plan.match.id)

            plans.append(plan)

    # ── GET em lote no Pipefy (uma única chamada para todos os cards) ──────────
    # Necessário para montar mutation, detectar fase atual e montar WhatsApp.
    card_data: Dict[str, Any] = {}
    get_cards_query = build_get_cards_query(list(set(card_ids_para_get))) if card_ids_para_get else ''
    for plan in plans:
        plan.pipefy_get_query = get_cards_query

    if card_ids_para_get and (atualizar_pipefy or enviar_whatsapp) and not modo_teste:
        try:
            resp = execute_graphql(get_cards_query)
            if resp.get('ok') and resp.get('body', {}).get('data'):
                for alias, cdata in resp['body']['data'].items():
                    if cdata and cdata.get('id'):
                        card_data[str(cdata['id'])] = cdata
            else:
                for plan in plans:
                    if plan.pode_executar:
                        plan.responses['pipefy_get'] = resp
        except Exception as e:
            for plan in plans:
                if plan.pode_executar:
                    plan.responses['pipefy_get'] = {'ok': False, 'error': str(e)}

    # Em modo teste, já monta previews de mutation/WhatsApp sem buscar card real.
    if modo_teste:
        for plan in plans:
            if not plan.pode_executar:
                continue
            card_info = {}
            if atualizar_pipefy and plan.match.id:
                plan.pipefy_update_mutation = build_update_card_mutation(plan, card_info)
            if enviar_whatsapp:
                plan.whatsapp_messages = build_whatsapp_messages(plan, payload, card_info)

    # ── Executa planos em produção ─────────────────────────────────────────────
    if not modo_teste:
        for plan in plans:
            if not plan.pode_executar:
                continue

            card_info = card_data.get(str(plan.match.id)) if plan.match.id else None

            # 1. Omie: tenta normal + retry por request. Se a baixa não confirmar,
            # executa o restante normalmente e move o card para Falha Api.
            omie_baixado_ou_pago = True
            if plan.omie_requests and executar_omie:
                plan.responses['omie'] = _executar_sequencia_omie(plan)
                omie_baixado_ou_pago = _omie_confirmou_pagamento(plan.responses['omie'])
                if not omie_baixado_ou_pago:
                    plan.responses['omie_status'] = {
                        'ok': False,
                        'acao_pipefy': 'mover_falha_api',
                        'fase_destino': '310785170',
                        'motivo': 'Baixa Omie não confirmada após retry. Demais atualizações foram mantidas.'
                    }

            # 2. Sheets SPsBD: comprovante bancário recebido confirma pagamento.
            if plan.sheets_updates and atualizar_spsbd:
                _executar_sheets_async(plan.sheets_updates)

            # 3. Pipefy: atualiza campos normalmente. Se Omie não confirmou baixa,
            # a mesma mutation move o card para Falha Api em vez de Pago / Alimentar Omie.
            if atualizar_pipefy and plan.match.id:
                mutation = build_update_card_mutation(
                    plan,
                    card_info,
                    move_to_falha_api=(executar_omie and not omie_baixado_ou_pago),
                )
                plan.pipefy_update_mutation = mutation
                plan.responses['pipefy'] = _executar_pipefy_batch([mutation])

            # 4. WhatsApp: monta depois do getCard, envia texto + comprovante PDF.
            if enviar_whatsapp:
                plan.whatsapp_messages = build_whatsapp_messages(plan, payload, card_info)
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


def _executar_sequencia_omie(plan: ExecutionPlan) -> List[dict]:
    """Consulta → Altera → Baixa, com retry curto por chamada.

    A existência do comprovante bancário confirma o pagamento para Sheets/Pipefy.
    Se o Omie falhar, seguimos com as demais atualizações e a decisão de mover para
    Falha Api é tomada por _omie_confirmou_pagamento().
    """
    resultados = []
    skip_restante = False

    for req in plan.omie_requests:
        if skip_restante:
            resultados.append({'step': req['step'], 'skipped': True, 'motivo': 'Título já consta PAGO no Omie.'})
            continue

        resp = _execute_omie_with_retry(req['request'])
        resultados.append({'step': req['step'], 'response': resp})

        if req['step'] == 'consultar':
            body = resp.get('body') or {}
            if resp.get('ok') and as_string(body.get('status_titulo')).upper() == 'PAGO':
                resultados.append({'step': 'skip', 'motivo': 'Título já consta PAGO no Omie.'})
                skip_restante = True

    if not _omie_confirmou_pagamento(resultados):
        resultados.append({
            'step': 'falha_omie_final',
            'motivo': 'Baixa Omie não confirmada após retry. Card será movido para Falha Api.',
            'fase_falha_api': '310785170',
        })

    return resultados


def _execute_omie_with_retry(request: dict) -> dict:
    """Executa uma chamada Omie com 1 retry curto se falhar."""
    first = execute_omie(request)
    if first.get('ok'):
        return first

    import time
    time.sleep(3)
    second = execute_omie(request)
    if second.get('ok'):
        second['retry'] = {'used': True, 'first_error': first}
        return second

    return {
        **second,
        'retry': {'used': True, 'first_error': first, 'second_error': second},
    }


def _omie_confirmou_pagamento(resultados: List[dict]) -> bool:
    """Retorna True quando Omie informou PAGO na consulta ou baixou com sucesso."""
    for item in resultados or []:
        step = item.get('step')
        resp = item.get('response') or {}
        body = resp.get('body') or {}
        if step == 'consultar' and resp.get('ok') and as_string(body.get('status_titulo')).upper() == 'PAGO':
            return True
        if step == 'baixar' and resp.get('ok'):
            if as_string(body.get('liquidado')).upper() == 'S' or as_string(body.get('descricao_status')).lower().find('sucesso') >= 0:
                return True
    return False

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


def _executar_zapi(msgs: list, payload: dict):
    try:
        zapi_auth = normalize_zapi_auth(payload)
        if zapi_auth['instanceId'] and zapi_auth['apiToken'] and zapi_auth['clientToken']:
            return send_messages_batch(zapi_auth, msgs)
        return [{'ok': False, 'skipped': True, 'reason': 'Credenciais Z-API ausentes ou incompletas.'}]
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
