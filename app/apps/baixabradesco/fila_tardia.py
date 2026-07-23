# -*- coding: utf-8 -*-
"""Fila tardia em disco (/tmp) para payloads que falharam na carga do Sheets.

Quando a quota do Google (429) estoura na carga inicial das bases, o payload
é gravado em /tmp/baixabradesco_fila_tardia/ e a resposta ao Make é 200 com
adiado=True — o cenário NÃO é interrompido. O reprocessamento é disparado
pelo cron-job.org via POST /api/baixabradesco/processar-fila-tardia.

Obs: no plano pago do Render o /tmp persiste entre requests (mesma instância).
Um deploy/restart limpa a fila — perda aceitável, pois o comprovante pode ser
reenviado pelo Make.
"""
from __future__ import annotations

import json
import os
import time
import uuid

FILA_DIR = '/tmp/baixabradesco_fila_tardia'
MAX_TENTATIVAS = 10


def _garantir_dir():
    os.makedirs(FILA_DIR, exist_ok=True)


def _listar():
    _garantir_dir()
    return sorted(a for a in os.listdir(FILA_DIR) if a.endswith('.json'))


def adiar_payload(payload: dict, erro: str) -> dict:
    """Grava o payload em disco e retorna resposta 200-ok para o Make."""
    _garantir_dir()
    item = {
        'payload': payload,
        'erro_original': (erro or '')[:500],
        'tentativas': 0,
        'criado_em': time.strftime('%d/%m/%Y %H:%M:%S'),
    }
    nome = f'{int(time.time())}_{uuid.uuid4().hex[:8]}.json'
    with open(os.path.join(FILA_DIR, nome), 'w', encoding='utf-8') as f:
        json.dump(item, f, ensure_ascii=False)
    return {
        'ok': True,
        'app': 'baixabradesco',
        'adiado': True,
        'arquivo': nome,
        'pendentes': len(_listar()),
        'motivo': 'Quota do Google Sheets excedida; payload enfileirado para reprocessamento automático.',
    }


def processar_fila_tardia() -> dict:
    """Reprocessa os payloads adiados, um a um, em ordem de chegada.

    - Sucesso: remove o arquivo.
    - Falha comum: incrementa tentativas (descarta após MAX_TENTATIVAS).
    - Falha por quota (429): para o loop imediatamente — o resto fica para o
      próximo disparo do cron, evitando queimar a janela de quota seguinte.
    """
    from .core import processar_baixabradesco

    resultados = []
    for nome in _listar():
        caminho = os.path.join(FILA_DIR, nome)
        try:
            with open(caminho, encoding='utf-8') as f:
                item = json.load(f)
        except Exception:
            os.remove(caminho)
            continue

        try:
            r = processar_baixabradesco(item.get('payload') or {})
            resultados.append({'arquivo': nome, 'ok': True, 'resumo': r.get('resumo')})
            os.remove(caminho)
        except Exception as e:
            msg = str(e)
            item['tentativas'] = int(item.get('tentativas') or 0) + 1
            item['ultimo_erro'] = msg[:500]
            if item['tentativas'] >= MAX_TENTATIVAS:
                os.remove(caminho)
                resultados.append({'arquivo': nome, 'ok': False, 'descartado': True, 'erro': msg[:200]})
            else:
                with open(caminho, 'w', encoding='utf-8') as f:
                    json.dump(item, f, ensure_ascii=False)
                resultados.append({'arquivo': nome, 'ok': False, 'tentativas': item['tentativas'], 'erro': msg[:200]})
            if '429' in msg or 'Quota exceeded' in msg or 'RESOURCE_EXHAUSTED' in msg:
                break  # quota estourada de novo — tenta no próximo cron

    return {
        'ok': True,
        'app': 'baixabradesco',
        'processados': resultados,
        'pendentes': len(_listar()),
    }