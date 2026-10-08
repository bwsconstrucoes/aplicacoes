# -*- coding: utf-8 -*-
"""Conferidor SPsBD × Omie: acha a baixa que ficou pela metade.

Existe por um buraco que a fila de falhas **não** alcança. Até 07/10/2026 o
reprocessamento de planilha marcava o item como "concluído com sucesso"
ignorando o resultado da gravação — então há itens que saíram da fila sem nunca
ter sido gravados, e para a fila eles estão resolvidos. A pendência real, se
existir, só aparece comparando as duas fontes lado a lado: a planilha diz
"Pago", o Omie diz "Aberto".

**Este módulo não grava nada.** Lê a planilha, pergunta ao Omie e relata. A
correção é decisão de quem lê o relatório — baixa é dinheiro, e um conferidor
que também corrige erraria em silêncio na primeira divergência de valor.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .omie import build_consultar_conta_pagar, execute_omie, credentials_from_payload
from .sheets import get_gc, SPS_SHEET_ID
from .utils import as_string

# Colunas da SPsBD que a conferência precisa. Ler A:AK inteiro custa 150-250 MB
# (ver load_spsbd_values) e aqui não há motivo: sete colunas resolvem.
FAIXAS = ['A2:A', 'D2:D', 'G2:G', 'O2:O', 'P2:P', 'X2:X', 'AG2:AG']

DIAS_PADRAO = 60       # janela de conferência
LIMITE_PADRAO = 50     # consultas ao Omie por chamada
PAUSA_OMIE = 0.2       # segundos entre consultas, para não irritar a API


def _coluna(faixas: List[Any], indice: int) -> List[str]:
    try:
        bruto = faixas[indice] or []
    except Exception:
        return []
    return [as_string(c[0]) if c else '' for c in bruto]


def _data_br(texto: str):
    texto = as_string(texto).strip()
    for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y'):
        try:
            return datetime.strptime(texto, fmt)
        except Exception:
            pass
    return None


def candidatas(payload: dict, gc=None) -> Dict[str, Any]:
    """SPs que a planilha diz pagas, dentro da janela — sem perguntar ao Omie.

    Separado de propósito: dá para medir o tamanho do problema sem gastar uma
    consulta ao Omie por linha.
    """
    gc = gc or get_gc()
    ws = gc.open_by_key(SPS_SHEET_ID).worksheet('SPsBD')
    faixas = ws.batch_get(FAIXAS) or []

    ids = _coluna(faixas, 0)
    credores = _coluna(faixas, 1)
    valores = _coluna(faixas, 2)
    status = _coluna(faixas, 3)
    codigos = _coluna(faixas, 4)
    datas = _coluna(faixas, 5)
    comprovantes = _coluna(faixas, 6)
    total = max(len(ids), len(status))

    dias = int(payload.get('dias') or DIAS_PADRAO)
    corte = datetime.now() - timedelta(days=dias)

    def em(lista, i):
        return lista[i] if i < len(lista) else ''

    escolhidas: List[Dict[str, Any]] = []
    pagas_na_janela = 0
    sem_codigo = 0
    for i in range(total):
        if as_string(em(status, i)).strip().lower() != 'pago':
            continue
        if not as_string(em(comprovantes, i)).strip():
            continue
        dt = _data_br(em(datas, i))
        if dt and dt < corte:
            continue
        pagas_na_janela += 1
        codigo = as_string(em(codigos, i)).strip()
        if not codigo:
            # Sem código de integração não há o que perguntar ao Omie. Entra na
            # contagem para não desaparecer do relatório.
            sem_codigo += 1
            continue
        escolhidas.append({
            'sp_id': as_string(em(ids, i)).strip(),
            'linha': i + 2,
            'credor': as_string(em(credores, i)).strip(),
            'valor': as_string(em(valores, i)).strip(),
            'data_pagamento': as_string(em(datas, i)).strip(),
            'codigo_integracao': codigo,
        })

    return {
        'linhas_na_aba': total,
        'pagas_na_janela': pagas_na_janela,
        'sem_codigo_integracao': sem_codigo,
        'conferiveis': escolhidas,
        'dias': dias,
    }


def _frase_da_conferencia(divergentes: int, nao_encontradas: int, erros: int,
                          confirmadas: int, restam: int, dias: int) -> str:
    """Uma frase em português, para quem lê isto pelo celular."""
    if not (divergentes or nao_encontradas or erros):
        base = (f'Nenhuma divergência: as {confirmadas} SPs conferidas estão pagas'
                f' nos dois lugares, na janela de {dias} dias.')
    else:
        partes = []
        if divergentes:
            partes.append(f'{divergentes} SP(s) a planilha diz PAGA e o Omie diz'
                          ' ABERTA — é baixa pela metade, o dinheiro saiu e o'
                          ' título não baixou')
        if nao_encontradas:
            partes.append(f'{nao_encontradas} com código que não existe no Omie'
                          ' (cadastro errado na planilha, não é dinheiro)')
        if erros:
            partes.append(f'{erros} que não deu para consultar agora'
                          ' (vale tentar de novo)')
        base = 'Achei ' + '; '.join(partes) + '.'
        if confirmadas:
            base += f' Outras {confirmadas} estão certas nos dois lugares.'
    if restam:
        base += f' Faltam {restam} para conferir — chame de novo para continuar.'
    return base


def conferir(payload: dict) -> Dict[str, Any]:
    """Compara a planilha com o Omie e relata divergências. Não grava nada.

    Body opcional:
      {"dias": 60, "limite": 50, "pausa_ms": 200, "omie": {...}}

    `apenas_contar: true` devolve só o tamanho do problema, sem consultar o Omie.
    """
    app_key, app_secret = credentials_from_payload(payload)
    if not app_key or not app_secret:
        return {'ok': False, 'app': 'baixabradesco', 'acao': 'conferir',
                'erro': 'credenciais_omie_ausentes'}

    base = candidatas(payload)
    conferiveis = base.pop('conferiveis')

    if payload.get('apenas_contar'):
        return {
            'ok': True,
            'app': 'baixabradesco',
            'acao': 'conferir',
            'apenas_contou': True,
            'a_conferir': len(conferiveis),
            **base,
            'em_portugues': (
                f"{len(conferiveis)} SP(s) entram na comparação, numa janela de "
                f"{base.get('dias')} dias. Nenhuma consulta ao Omie foi gasta. "
                f"Chame sem 'apenas_contar' para comparar de verdade."),
        }

    limite = int(payload.get('limite') or LIMITE_PADRAO)
    try:
        pausa = max(0.0, float(payload.get('pausa_ms', PAUSA_OMIE * 1000)) / 1000.0)
    except Exception:
        pausa = PAUSA_OMIE

    inicio = datetime.now()
    divergentes: List[Dict[str, Any]] = []
    nao_encontradas: List[Dict[str, Any]] = []
    erros: List[Dict[str, Any]] = []
    confirmadas = 0

    for posicao, item in enumerate(conferiveis[:limite]):
        if pausa and posicao:
            time.sleep(pausa)
        resp = execute_omie(build_consultar_conta_pagar(item['codigo_integracao'], payload))
        corpo = resp.get('body') or {}
        status_titulo = as_string(corpo.get('status_titulo')).upper()
        registro = dict(item)
        registro['status_omie'] = status_titulo

        if status_titulo == 'PAGO':
            confirmadas += 1
            continue
        if not resp.get('ok'):
            texto = (as_string(corpo.get('faultstring'))
                     or as_string(resp.get('raw'))[:200])
            registro['erro'] = texto
            # O Omie responde com falha quando o título não existe. É diferente
            # de erro de rede, e a diferença importa: título inexistente é
            # cadastro errado na planilha, não baixa pela metade.
            if 'não encontrado' in texto.lower() or 'nao encontrado' in texto.lower():
                nao_encontradas.append(registro)
            else:
                erros.append(registro)
            continue
        divergentes.append(registro)

    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'conferir',
        **base,
        'a_conferir': len(conferiveis),
        'conferidas_agora': min(limite, len(conferiveis)),
        'restam_para_conferir': max(0, len(conferiveis) - limite),
        'confirmadas_pagas_no_omie': confirmadas,
        'divergentes': divergentes,
        'quantidade_divergentes': len(divergentes),
        'titulos_nao_encontrados': nao_encontradas,
        'erros_de_consulta': erros,
        'segundos': round((datetime.now() - inicio).total_seconds(), 1),
        'aviso': ('Relatório apenas. Nada foi gravado na planilha nem no Omie.'
                  if divergentes or nao_encontradas or erros
                  else 'Nenhuma divergência na janela conferida.'),
        'em_portugues': _frase_da_conferencia(
            len(divergentes), len(nao_encontradas), len(erros), confirmadas,
            max(0, len(conferiveis) - limite), int(payload.get('dias') or DIAS_PADRAO)),
    }
