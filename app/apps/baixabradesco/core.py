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
from .zapi import build_whatsapp_messages, send_messages_batch, resolve_zapi_auth, validate_zapi_auth
from .storage import upload_dropbox_bytes, build_receipt_page_filename, normalize_dropbox_link


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
                    rec.drive_link = normalize_dropbox_link(storage_info.get('url', ''))
                    storage_info['url'] = rec.drive_link
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
            plan.whatsapp_messages    = []  # montado depois do getCard Pipefy

            if plan.match.id:
                card_ids_para_get.append(plan.match.id)

            plans.append(plan)

    # ── GET em lote no Pipefy (uma única chamada para todos os cards) ──────────
    card_data: Dict[str, Any] = {}
    if card_ids_para_get and atualizar_pipefy and not modo_teste:
        try:
            query = build_get_cards_query(list(set(card_ids_para_get)))
            resp  = execute_graphql(query)
            if resp.get('ok') and resp.get('body', {}).get('data'):
                for alias, cdata in resp['body']['data'].items():
                    if cdata and cdata.get('id'):
                        card_data[str(cdata['id'])] = cdata
        except Exception as e:
            pass  # falha no get não deve parar a execução

    # ── Executa planos em produção ─────────────────────────────────────────────
    pipefy_mutations_batch: List[str] = []

    if not modo_teste:
        for plan in plans:
            if not plan.pode_executar:
                continue

            card_info = card_data.get(str(plan.match.id)) if plan.match.id else None
            if card_info:
                plan.responses['pipefy_card_info'] = card_info

            # 1. Omie
            if plan.omie_requests and executar_omie:
                plan.responses['omie'] = _executar_sequencia_omie(plan)

            # 2. Sheets SPsBD (em background para não atrasar resposta)
            if plan.sheets_updates and atualizar_spsbd:
                _executar_sheets_async(plan.sheets_updates)

            # 3. Pipefy mutation montada com dados do get
            if atualizar_pipefy and plan.match.id:
                mutation = build_update_card_mutation(plan, card_info)
                plan.pipefy_update_mutation = mutation
                try:
                    plan.responses['pipefy'] = [execute_graphql(mutation)]
                except Exception as e:
                    plan.responses['pipefy'] = [{'ok': False, 'error': str(e)}]

            # 4. WhatsApp direto via Z-API, após getCard
            if enviar_whatsapp:
                plan.whatsapp_messages = build_whatsapp_messages(plan, payload)
                if plan.whatsapp_messages:
                    plan.responses['zapi'] = _executar_zapi(plan.whatsapp_messages, payload)
                else:
                    plan.responses['zapi'] = [{'skipped': True, 'reason': 'sem_telefone_ou_campos_pipefy'}]

        # Pipefy e Z-API são executados por plano acima, para manter o output correto.

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
    """Consulta → Altera (se necessário) → Baixa. Retorna log de cada step."""
    resultados = []
    for req in plan.omie_requests:
        resp = execute_omie(req['request'])
        resultados.append({'step': req['step'], 'response': resp})

        if req['step'] == 'consultar':
            body = resp.get('body') or {}
            if as_string(body.get('status_titulo')).upper() == 'PAGO':
                resultados.append({'step': 'skip', 'motivo': 'Título já consta PAGO no Omie.'})
                break
            # Se não encontrou o título, interrompe (não tenta alterar/baixar)
            if not resp.get('ok'):
                faultcode = as_string((body.get('faultcode') or ''))
                if 'nao_encontrado' in faultcode.lower() or resp['status'] == 500:
                    resultados.append({'step': 'abort', 'motivo': 'Título não encontrado no Omie. Inclua o título primeiro.'})
                    break

        if req['step'] == 'alterar_se_necessario':
            # Só altera se valor ou conta divergem (verificação simplificada: tenta sempre, Omie idempotente)
            if not resp.get('ok'):
                resultados.append({'step': 'abort_apos_alterar', 'motivo': 'Falha ao alterar título. Baixa cancelada.'})
                break

        if req['step'] == 'baixar' and not resp.get('ok'):
            resultados.append({'step': 'erro_baixa', 'motivo': 'Falha ao lançar pagamento no Omie.'})

    return resultados


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
        zapi_auth = resolve_zapi_auth(payload)
        missing = validate_zapi_auth(zapi_auth)
        if missing:
            return [{'ok': False, 'skipped': True, 'reason': 'credenciais_zapi_ausentes', 'missing': missing}]
        return send_messages_batch(zapi_auth, msgs)
    except Exception as e:
        return [{'ok': False, 'status': 0, 'error': str(e)}]


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
