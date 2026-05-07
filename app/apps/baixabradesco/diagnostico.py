# -*- coding: utf-8 -*-
"""
Endpoint de diagnóstico do módulo baixabradesco.
Chame POST /api/baixabradesco/diagnostico com o mesmo payload do /executar.
Retorna um relatório completo sem executar nada no Omie/Pipefy/Sheets/ZApi.
"""
from __future__ import annotations

import os
import sys
import traceback
import base64
from typing import Any, Dict, List


def _safe(fn, label: str, resultados: list):
    """Executa fn(), registra OK ou ERRO com traceback."""
    try:
        val = fn()
        resultados.append({'etapa': label, 'status': 'ok', 'resultado': val})
        return val
    except Exception as e:
        resultados.append({
            'etapa': label,
            'status': 'erro',
            'erro': str(e),
            'traceback': traceback.format_exc(),
        })
        return None


def executar_diagnostico(payload: Dict[str, Any]) -> Dict[str, Any]:
    resultados: List[dict] = []
    relatorio: Dict[str, Any] = {
        'app': 'baixabradesco',
        'diagnostico': True,
        'etapas': resultados,
    }

    # ── 1. Variáveis de ambiente ───────────────────────────────────────────────
    env_check = {}
    for var in [
        'GOOGLE_CREDENTIALS_BASE64',
        'PIPEFY_API_TOKEN',
        'BAIXABRADESCO_SECRET',
        'BAIXABRADESCO_DEBUG',
        'DROPBOX_APP_KEY',
        'DROPBOX_APP_SECRET',
        'DROPBOX_REFRESH_TOKEN',
    ]:
        val = os.getenv(var, '')
        if not val:
            env_check[var] = '❌ NÃO CONFIGURADA'
        elif var in ('GOOGLE_CREDENTIALS_BASE64', 'PIPEFY_API_TOKEN',
                     'BAIXABRADESCO_SECRET', 'DROPBOX_APP_KEY',
                     'DROPBOX_APP_SECRET', 'DROPBOX_REFRESH_TOKEN'):
            env_check[var] = f'✅ configurada ({len(val)} chars)'
        else:
            env_check[var] = f'✅ {val}'
    resultados.append({'etapa': 'variaveis_ambiente', 'status': 'ok', 'resultado': env_check})

    # ── 2. Imports dos módulos internos ───────────────────────────────────────
    def check_imports():
        imports = {}
        modulos = [
            ('utils',           'app.apps.baixabradesco.utils'),
            ('models',          'app.apps.baixabradesco.models'),
            ('parser_pdf',      'app.apps.baixabradesco.parser_pdf'),
            ('parser_bradesco', 'app.apps.baixabradesco.parser_bradesco'),
            ('matcher',         'app.apps.baixabradesco.matcher'),
            ('sheets',          'app.apps.baixabradesco.sheets'),
            ('omie',            'app.apps.baixabradesco.omie'),
            ('pipefy',          'app.apps.baixabradesco.pipefy'),
            ('storage',         'app.apps.baixabradesco.storage'),
            ('zapi',            'app.apps.baixabradesco.zapi'),
        ]
        for nome, path in modulos:
            try:
                __import__(path)
                imports[nome] = '✅ ok'
            except Exception as e:
                imports[nome] = f'❌ {e}'
        return imports
    _safe(check_imports, 'imports_modulos', resultados)

    # ── 3. Dependências externas ───────────────────────────────────────────────
    def check_deps():
        deps = {}
        for pkg in ['flask', 'pypdf', 'gspread', 'google.oauth2', 'dropbox', 'requests', 'reportlab']:
            try:
                mod = __import__(pkg.split('.')[0])
                version = getattr(mod, '__version__', '?')
                deps[pkg] = f'✅ {version}'
            except Exception as e:
                deps[pkg] = f'❌ {e}'
        deps['python'] = sys.version
        return deps
    _safe(check_deps, 'dependencias', resultados)

    # ── 4. Decodificação do base64 ────────────────────────────────────────────
    pdf_bytes = None
    def decode_b64():
        nonlocal pdf_bytes
        from app.apps.baixabradesco.utils import b64decode_bytes, as_string
        from app.apps.baixabradesco.core import normalize_attachments

        atts = normalize_attachments(payload)
        if not atts:
            return {'erro': 'Nenhum comprovante encontrado no payload.'}

        info = []
        for att in atts:
            if att.base64:
                b = b64decode_bytes(att.base64)
                info.append({
                    'filename': att.filename,
                    'origem': 'base64',
                    'bytes': len(b),
                    'inicio_hex': b[:8].hex(),
                    'is_pdf': b[:4] == b'%PDF',
                })
                if len(atts) == 1:
                    pdf_bytes = b
            elif att.url:
                info.append({'filename': att.filename, 'origem': 'url', 'url': att.url[:80]})
        return info
    _safe(decode_b64, 'decodificacao_base64', resultados)

    # ── 5. Extração de texto do PDF ───────────────────────────────────────────
    pages_text = []
    if pdf_bytes:
        def extract_text():
            nonlocal pages_text
            from app.apps.baixabradesco.parser_pdf import extract_pdf_pages
            pages = extract_pdf_pages(pdf_bytes)
            pages_text = pages
            resultado = []
            for num, txt in pages:
                resultado.append({
                    'pagina': num,
                    'chars': len(txt),
                    'texto_preview': txt[:600].replace('\n', ' ') if txt else '(vazio)',
                    'tem_texto': bool(txt and txt.strip()),
                })
            return resultado
        _safe(extract_text, 'extracao_texto_pdf', resultados)
    else:
        resultados.append({'etapa': 'extracao_texto_pdf', 'status': 'pulado', 'motivo': 'pdf_bytes não disponível'})

    # ── 6. Parse do comprovante ────────────────────────────────────────────────
    receipts = []
    if pages_text:
        def parse_pages():
            from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text
            from app.apps.baixabradesco.core import normalize_attachments
            atts = normalize_attachments(payload)
            filename = atts[0].filename if atts else 'comprovante.pdf'
            resultado = []
            for num, txt in pages_text:
                if not txt or not txt.strip():
                    resultado.append({'pagina': num, 'status': 'sem_texto'})
                    continue
                rec = parse_bradesco_text(filename=filename, page=num, text=txt)
                d = {
                    'pagina':           num,
                    'tipo_comprovante': rec.tipo_comprovante,
                    'id_pipefy':        rec.id_pipefy,
                    'valor_pago':       rec.valor_pago,
                    'data_pagamento':   rec.data_pagamento,
                    'forma_pagamento':  rec.forma_pagamento,
                    'nome_recebedor':   rec.nome_recebedor,
                    'documento_recebedor': rec.documento_recebedor,
                    'agencia_origem':   rec.agencia_origem,
                    'conta_origem':     rec.conta_origem,
                    'conta_origem_raw': rec.conta_origem_raw,
                    'acrescimos':       rec.acrescimos,
                    'tarifa':           rec.tarifa,
                    'descricao':        rec.descricao,
                    'confianca':        rec.confianca,
                    'pendencias':       rec.pendencias,
                }
                resultado.append(d)
                receipts.append(rec)
            return resultado
        _safe(parse_pages, 'parse_comprovante', resultados)
    else:
        resultados.append({'etapa': 'parse_comprovante', 'status': 'pulado', 'motivo': 'nenhuma página com texto'})

    # ── 7. Google Sheets (carregamento das bases) ─────────────────────────────
    sps_index = {}
    sps_agendar = []
    base_bancos = []

    def check_google():
        nonlocal sps_index, sps_agendar, base_bancos
        from app.apps.baixabradesco.sheets import get_gc, load_spsbd_index, load_spsagendar, load_base_bancos
        gc = get_gc()
        sps_index   = load_spsbd_index(gc)
        sps_agendar = load_spsagendar(gc)
        base_bancos = load_base_bancos(gc)
        from app.apps.baixabradesco.sheets import load_spsbd_operacional
        sps_operacional = load_spsbd_operacional(gc)
        target_id = '1345786721'
        target_sp = sps_operacional.get(target_id)
        sample_op = [
            {'id': sp.id, 'nome': sp.nome_credor, 'valor': sp.valor_total,
             'status_pgt': sp.status_pgt, 'status_agend': sp.status_agendamento}
            for sp in list(sps_operacional.values())[:5]
        ]
        return {
            'spsbd_registros':    len(sps_index),
            'spsbd_operacional_registros': len(sps_operacional),
            'spsbd_operacional_sample': sample_op,
            'target_sp_1345786721': {
                'encontrada': target_sp is not None,
                'status_pgt': target_sp.status_pgt if target_sp else None,
                'status_agend': target_sp.status_agendamento if target_sp else None,
                'valor': target_sp.valor_total if target_sp else None,
                'nome': target_sp.nome_credor if target_sp else None,
            },
            'spsagendar_registros': len(sps_agendar),
            'base_bancos_registros': len(base_bancos),
            'sample_ids': list(sps_index.keys())[:5],
            'sample_bancos': [
                {'agencia': b.agencia, 'conta': b.conta, 'codigo_omie': b.codigo_omie}
                for b in base_bancos[:3]
            ],
        }
    _safe(check_google, 'google_sheets', resultados)

    # ── 8. Match dos comprovantes ─────────────────────────────────────────────
    if receipts:
        def check_match():
            from app.apps.baixabradesco.matcher import match_receipt
            from app.apps.baixabradesco.sheets import find_bank_account
            resultado = []
            for rec in receipts:
                match = match_receipt(rec, sps_index, sps_agendar)
                banco = find_bank_account(base_bancos, rec.agencia_origem, rec.conta_origem) if base_bancos else None
                resultado.append({
                    'pagina':     rec.page,
                    'id_pipefy':  rec.id_pipefy,
                    'match_status':  match.status,
                    'match_metodo':  match.metodo,
                    'match_id':      match.id,
                    'match_motivo':  match.motivo,
                    'candidatos':    len(match.candidatos),
                    'banco_encontrado': {
                        'agencia':     banco.agencia,
                        'conta':       banco.conta,
                        'codigo_omie': banco.codigo_omie,
                        'descricao':   banco.descricao,
                    } if banco else None,
                })
            return resultado
        _safe(check_match, 'match_comprovante', resultados)
    else:
        resultados.append({'etapa': 'match_comprovante', 'status': 'pulado', 'motivo': 'sem receipts parseados'})

    # ── 9. Preview Omie (sem executar) ────────────────────────────────────────
    if receipts and sps_index:
        def preview_omie():
            from app.apps.baixabradesco.matcher import match_receipt
            from app.apps.baixabradesco.sheets import find_bank_account
            from app.apps.baixabradesco.models import ExecutionPlan
            from app.apps.baixabradesco.omie import build_omie_plan, codigo_integracao

            previews = []
            for rec in receipts:
                match = match_receipt(rec, sps_index, sps_agendar)
                banco = find_bank_account(base_bancos, rec.agencia_origem, rec.conta_origem) if base_bancos else None
                plan  = ExecutionPlan(receipt=rec, match=match, banco=banco)
                if match.id:
                    reqs = build_omie_plan(plan, payload)
                    previews.append({
                        'pagina': rec.page,
                        'codigo_integracao': codigo_integracao(plan),
                        'steps': [
                            {
                                'step': r['step'],
                                'call': r['request'].get('call'),
                                'param_preview': {
                                    k: v for k, v in (r['request'].get('param') or [{}])[0].items()
                                },
                            }
                            for r in reqs
                        ],
                    })
                else:
                    previews.append({'pagina': rec.page, 'match_status': match.status, 'sem_omie': True})
            return previews
        _safe(preview_omie, 'preview_omie_requests', resultados)

    # ── 10. Preview Pipefy mutation (sem executar) ────────────────────────────
    if receipts:
        def preview_pipefy():
            from app.apps.baixabradesco.matcher import match_receipt
            from app.apps.baixabradesco.sheets import find_bank_account
            from app.apps.baixabradesco.models import ExecutionPlan
            from app.apps.baixabradesco.pipefy import build_update_card_mutation, build_get_cards_query

            previews = []
            ids_com_match = []
            for rec in receipts:
                match = match_receipt(rec, sps_index, sps_agendar)
                banco = find_bank_account(base_bancos, rec.agencia_origem, rec.conta_origem) if base_bancos else None
                plan  = ExecutionPlan(receipt=rec, match=match, banco=banco)
                if match.id:
                    ids_com_match.append(match.id)
                    mutation = build_update_card_mutation(plan, None)
                    previews.append({'pagina': rec.page, 'card_id': match.id, 'mutation_preview': mutation[:400]})
                else:
                    previews.append({'pagina': rec.page, 'sem_pipefy': True})

            if ids_com_match:
                query = build_get_cards_query(ids_com_match)
                previews.append({'get_cards_query_preview': query[:400]})
            return previews
        _safe(preview_pipefy, 'preview_pipefy_mutation', resultados)

    # ── Resumo final ──────────────────────────────────────────────────────────
    erros = [e for e in resultados if e.get('status') == 'erro']
    relatorio['resumo'] = {
        'total_etapas':  len(resultados),
        'etapas_ok':     len([e for e in resultados if e.get('status') == 'ok']),
        'etapas_erro':   len(erros),
        'etapas_puladas':len([e for e in resultados if e.get('status') == 'pulado']),
        'erros_resumo':  [{'etapa': e['etapa'], 'erro': e.get('erro', '')} for e in erros],
    }
    return relatorio