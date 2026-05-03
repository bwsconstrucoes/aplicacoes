# -*- coding: utf-8 -*-
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Dict, Any, List

from .models import AttachmentInput, ExecutionPlan
from .utils import b64decode_bytes, fingerprint_bytes, as_string
from .parser_pdf import extract_pdf_pages, extract_single_page_pdf
from .parser_bradesco import parse_bradesco_text
from .sheets import get_gc, load_spsbd_index, load_spsagendar, load_base_bancos, find_bank_account, build_spsbd_updates
from .matcher import match_receipt
from .omie import build_omie_plan, execute_omie
from .pipefy import build_get_cards_query, build_update_card_mutation, execute_graphql
from .zapi import build_whatsapp_messages, send_text
from .storage import upload_dropbox_bytes, build_receipt_page_filename
import requests


def processar_baixabradesco(payload: Dict[str, Any]) -> Dict[str, Any]:
    modo_teste = bool(payload.get('modo_teste', True))
    attachments = normalize_attachments(payload)
    if not attachments:
        raise ValueError('Nenhum comprovante enviado. Use attachments/comprovantes com filename e base64.')

    # Carrega bases uma vez por lote.
    gc = None
    sps_index = {}
    sps_agendar = []
    base_bancos = []
    try:
        gc = get_gc()
        sps_index = load_spsbd_index(gc)
        sps_agendar = load_spsagendar(gc)
        base_bancos = load_base_bancos(gc)
    except Exception as e:
        if not modo_teste:
            raise
        # No modo teste, permite validar parser sem Google configurado.
        google_error = str(e)
    else:
        google_error = ''

    opcoes = payload.get('opcoes') or {}
    salvar_comprovantes = bool(payload.get('salvar_comprovantes', opcoes.get('salvar_comprovante', opcoes.get('salvar_comprovantes', True))))
    salvar_no_modo_teste = bool(payload.get('salvar_no_modo_teste', opcoes.get('salvar_no_modo_teste', False)))
    pasta_dropbox = as_string(payload.get('pasta_dropbox') or payload.get('pasta') or '/BWS FINANCEIRO/COMPROVANTESTEMP/COMPROVANTESSP')

    plans: List[ExecutionPlan] = []
    for att in attachments:
        pdf_bytes = load_attachment_bytes(att)
        fp_file = fingerprint_bytes(pdf_bytes, att.filename)
        pages = extract_pdf_pages(pdf_bytes)
        for page, text in pages:
            if not as_string(text):
                continue
            # Primeiro parse sem link, para detectar ID e nomear melhor a página salva.
            rec = parse_bradesco_text(
                filename=att.filename,
                page=page,
                text=text,
                drive_link='',
                fingerprint=f'{fp_file}:{page}',
            )

            storage_info = {
                'storage': 'dropbox',
                'pasta': pasta_dropbox,
                'status': 'nao_salvo_modo_teste' if modo_teste and not salvar_no_modo_teste else 'pendente',
                'url': '',
                'path': '',
            }
            if salvar_comprovantes and ((not modo_teste) or salvar_no_modo_teste):
                page_pdf = extract_single_page_pdf(pdf_bytes, page)
                page_filename = build_receipt_page_filename(att.filename, page, rec.id_pipefy)
                storage_info = upload_dropbox_bytes(page_pdf, f'{pasta_dropbox}/{page_filename}')
                storage_info['status'] = 'salvo'
                rec.drive_link = storage_info.get('url', '')
            else:
                page_filename = build_receipt_page_filename(att.filename, page, rec.id_pipefy)
                storage_info['path_previsto'] = f'{pasta_dropbox.rstrip("/")}/{page_filename}'

            match = match_receipt(rec, sps_index, sps_agendar)
            banco = find_bank_account(base_bancos, rec.agencia_origem, rec.conta_origem) if base_bancos else None
            plan = ExecutionPlan(receipt=rec, match=match, banco=banco)
            plan.responses['storage'] = storage_info
            decidir_execucao(plan)
            plan.omie_requests = build_omie_plan(plan, payload) if plan.match.id else []
            plan.pipefy_get_query = build_get_cards_query([plan.match.id]) if plan.match.id else ''
            plan.pipefy_update_mutation = build_update_card_mutation(plan) if plan.match.id else ''
            plan.sheets_updates = build_spsbd_updates(plan)
            plan.whatsapp_messages = build_whatsapp_messages(plan, payload)
            plans.append(plan)

    if not modo_teste:
        executar_planos(plans, payload)

    return {
        'ok': True,
        'app': 'baixabradesco',
        'modo_teste': modo_teste,
        'resumo': {
            'comprovantes_recebidos': len(attachments),
            'paginas_processadas': len(plans),
            'localizados': sum(1 for p in plans if p.match.status == 'localizado'),
            'executaveis': sum(1 for p in plans if p.pode_executar),
            'pendentes': sum(1 for p in plans if not p.pode_executar),
            'google_error': google_error,
        },
        'planos': [p.to_dict() for p in plans],
    }



def load_attachment_bytes(att: AttachmentInput) -> bytes:
    if att.base64:
        return b64decode_bytes(att.base64)
    if att.url:
        resp = requests.get(att.url, timeout=60)
        resp.raise_for_status()
        return resp.content
    raise ValueError(f'Comprovante sem base64/url: {att.filename}')


def normalize_attachments(payload: Dict[str, Any]) -> List[AttachmentInput]:
    """Aceita dois formatos de entrada:
    1) simples: {"filename": "...pdf", "base64": "..."}
    2) lote: {"attachments": [{"filename": "...pdf", "base64": "..."}]}
       ou {"comprovantes": [...]}
    """
    arr = payload.get('attachments') or payload.get('comprovantes') or []

    # Formato simples usado no Make após Iterator do anexo.
    if not arr and (payload.get('filename') or payload.get('fileName') or payload.get('base64') or payload.get('url')):
        arr = [{
            'filename': payload.get('filename') or payload.get('fileName') or payload.get('nome') or 'comprovante.pdf',
            'base64': payload.get('base64') or payload.get('data') or '',
            'url': payload.get('url') or payload.get('link') or '',
        }]

    # Segurança: se vier um único objeto em vez de lista.
    if isinstance(arr, dict):
        arr = [arr]

    out = []
    for item in arr:
        if not item:
            continue
        out.append(AttachmentInput(
            filename=as_string(item.get('filename') or item.get('fileName') or item.get('nome') or 'comprovante.pdf'),
            base64=as_string(item.get('base64') or item.get('data') or ''),
            url=as_string(item.get('url') or item.get('link') or ''),
        ))
    return out


def decidir_execucao(plan: ExecutionPlan):
    rec = plan.receipt
    if plan.match.status != 'localizado' or not plan.match.id:
        plan.acao = 'pendente_validacao'
        plan.motivos_bloqueio.append(plan.match.motivo or 'SP não localizada')
        return

    # Regra definida: com ID no comprovante, chance mínima de erro; baixa em produção se campos mínimos existem.
    faltas = []
    if not rec.valor_pago:
        faltas.append('valor_pago')
    if not rec.data_pagamento:
        faltas.append('data_pagamento')
    if not (plan.banco and plan.banco.codigo_omie):
        # Conta pode ser inferida depois por SPsAgendar/BaseBancos; por enquanto bloqueia se não houver código Omie.
        faltas.append('codigo_conta_omie')
    if faltas:
        plan.acao = 'pendente_validacao'
        plan.motivos_bloqueio.append('Campos mínimos ausentes: ' + ', '.join(faltas))
        return

    plan.acao = 'baixar_omie_atualizar_pipefy_sheets'
    plan.pode_executar = True


def executar_planos(plans: List[ExecutionPlan], payload: dict):
    for plan in plans:
        if not plan.pode_executar:
            continue
        plan.responses['omie'] = []
        # Execução conservadora: consulta -> altera -> baixa. Próxima versão decidirá pular alteração quando não necessário.
        for req in plan.omie_requests:
            resp = execute_omie(req['request'])
            plan.responses['omie'].append({'step': req['step'], 'response': resp})
            if req['step'] == 'consultar':
                # Se já estiver pago, não baixa novamente.
                body = resp.get('body') or {}
                if as_string(body.get('status_titulo')).upper() == 'PAGO':
                    plan.responses['omie'].append({'step': 'skip', 'motivo': 'Título já consta como PAGO no Omie.'})
                    break
            if not resp.get('ok') and req['step'] != 'consultar':
                plan.responses['omie'].append({'step': 'erro', 'motivo': 'Falha Omie; demais ações não executadas.'})
                break
        if plan.pipefy_update_mutation:
            plan.responses['pipefy'] = execute_graphql(plan.pipefy_update_mutation)
        # Sheets e Z-API serão ligados após validação das colunas finais e credenciais de envio.
