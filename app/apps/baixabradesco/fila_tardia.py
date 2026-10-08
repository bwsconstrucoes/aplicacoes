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


# Quanto o cron drena por disparo. Ele roda de 5 em 5 minutos: 15 por etapa dá
# ~180 itens por hora sem encostar na cota do Google, que é compartilhada com o
# ERP e o painel. A ORDEM mora em `fila.ORDEM_ETAPAS`, num lugar só, porque os
# dois caminhos automáticos já divergiram uma vez.
LIMITES_CRON = {'omie': 15, 'sheets': 15, 'pipefy': 15, 'zapi': 10}

# ── Mutirão único: zerar o que ficou para trás ────────────────────────────────
#
# Pedido do dono em 08/10/2026, vendo a fila com 2.213 pendências cuja mais
# antiga era de 18/06: *"só preciso que rode as coisas desse mês em diante. O que
# tá pra trás, poderia zerar."* A razão é dele e é boa: faz conciliação bancária
# diária, então o que ficou para trás já foi resolvido na mão — a pendência é de
# registro, não de dinheiro.
#
# ⚠️ A data é FIXA de propósito, não "o mês corrente". Ele autorizou zerar o que
# estava para trás *naquele dia*; uma regra que andasse com o calendário ficaria
# dispensando pendência nova todo mês primeiro, o que ele não pediu e seria a
# forma mais silenciosa possível de perder trabalho.
#
# É um mutirão que se encerra sozinho: depois que as linhas antigas estão
# marcadas, nenhuma casa com o critério e a passada fica de graça.
ZERAR_ANTES_DE = '01/10/2026'
ZERAR_POR_DISPARO = 500


def _zerar_atraso_uma_vez(payload: dict | None = None) -> dict:
    """Dispensa, aos poucos, as pendências anteriores ao corte autorizado.

    Em blocos por disparo para não tomar a cota do Google de uma vez — ela é
    por minuto e é compartilhada com o ERP, o painel e o Análise de SPs.
    """
    from .fila import zerar_fila_antiga

    corte = (os.getenv('BAIXABRADESCO_ZERAR_ANTES_DE', '') or ZERAR_ANTES_DE).strip()
    if not corte:
        return {'desligado': True}
    pedido = dict(payload or {})
    pedido.update({'antes_de': corte, 'limite': ZERAR_POR_DISPARO})
    try:
        r = zerar_fila_antiga(pedido)
        return {
            'antes_de': r.get('antes_de'),
            'dispensadas': r.get('dispensadas'),
            'por_etapa': r.get('por_etapa'),
            'erro': r.get('erro'),
        }
    except Exception as e:
        return {'erro': str(e)[:200]}


def _drenar_a_fila(payload: dict | None = None) -> dict:
    from .fila import drenar_por_etapa
    return drenar_por_etapa(LIMITES_CRON, payload)


def processar_fila_tardia(payload: dict | None = None) -> dict:
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
        # O cron já roda de 5 em 5 minutos e já está autenticado: a fila de
        # falhas pega carona nele. Antes ela só andava se alguém chamasse a rota
        # à mão — e, pelos números de 08/10/2026 (2.269 linhas, nenhuma
        # concluída, a mais antiga de 18/06/2026), nunca ninguém chamou.
        # A ORDEM importa: zerar primeiro, drenar depois. Senão a drenagem
        # gastaria a passada inteira nas linhas antigas que vão ser dispensadas
        # dois segundos mais tarde.
        'atraso_zerado': _zerar_atraso_uma_vez(payload),
        'fila_de_falhas': _drenar_a_fila(payload),
    }