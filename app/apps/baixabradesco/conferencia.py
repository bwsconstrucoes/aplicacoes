# -*- coding: utf-8 -*-
"""Conferidor SPsBD × Omie: acha a baixa que ficou pela metade.

Existe por um buraco que a fila de falhas **não** alcança. Até 07/10/2026 o
reprocessamento de planilha marcava o item como "concluído com sucesso"
ignorando o resultado da gravação — então há itens que saíram da fila sem nunca
ter sido gravados, e para a fila eles estão resolvidos. A pendência real, se
existir, só aparece comparando as duas fontes lado a lado: a planilha diz
"Pago", o Omie diz "Aberto".

**São DUAS direções, e elas não valem o mesmo.** O dono explicou por quê em
08/10/2026: *"fazemos conciliação bancária diária. No sistema Omie vai estar tudo
atualizado. O furo pode ser mais na planilha e na movimentação do card."*

1. **Planilha diz paga, Omie diz aberta.** Era a única direção que este módulo
   olhava — e, pela conciliação diária, é a menos provável das duas.
2. **Omie diz pago, planilha não diz.** É o furo de verdade: o dinheiro saiu, o
   Omie sabe, e a SP continua aparecendo como "a pagar" para quem usa a planilha.
   Esta direção não aparece em lugar nenhum: a fila de falhas tinha ZERO
   pendências de planilha (a gravação morria antes de chegar nela), então o
   sistema não tinha como saber que deixou de gravar.

A segunda direção é mais caro de achar, e por isso precisa de janela: não há como
partir de "quem está pago" na planilha; parte-se de "quem a planilha diz que
ainda não foi paga" e pergunta-se ao Omie, uma por uma.

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
FAIXAS = ['A2:A', 'C2:C', 'D2:D', 'G2:G', 'O2:O', 'P2:P', 'R2:R', 'X2:X', 'AG2:AG']
# A=ID, C=Vencim., D=Nome do Credor, G=Valor Total, O=Status Pgt,
# P=Código Integração, R=Card Link, X=Data do Pagamento, AG=Comprovante.
# Nove colunas de ~52 mil linhas. Ler A:AK inteiro custa 150-250 MB e foi assim
# que o serviço caiu por memória em julho de 2026.

DIAS_PADRAO = 60       # janela de conferência
LIMITE_PADRAO = 50     # consultas ao Omie por chamada, em cada direção
PAUSA_OMIE = 0.2       # segundos entre consultas, para não irritar a API


def _inteiro(payload: dict, chave: str, padrao: int, minimo: int = 0) -> int:
    """Número vindo da barra do navegador, sem derrubar nada.

    Os parâmetros chegam como TEXTO (`?dias=60`), e `int('sessenta')` levanta
    exceção — que virava 500 com rastro de pilha na tela de quem digitou. Quem
    usa isto digita o endereço no celular; um erro de digitação não pode
    responder com página de erro de programador.
    """
    bruto = payload.get(chave)
    if bruto is None or bruto == '':
        return padrao
    try:
        valor = int(str(bruto).strip())
    except Exception:
        return padrao
    if valor < minimo:
        # Fora de faixa cai no PADRÃO, não no mínimo. `dias=0` virando janela de
        # um dia não acharia quase nada e pareceria "está tudo certo" — a
        # resposta mais perigosa que um conferidor pode dar.
        return max(padrao, minimo)
    return valor


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
    """As duas listas a conferir, sem perguntar nada ao Omie.

    Separado de propósito: dá para medir o tamanho do problema sem gastar uma
    consulta ao Omie por linha.
    """
    gc = gc or get_gc()
    ws = gc.open_by_key(SPS_SHEET_ID).worksheet('SPsBD')
    faixas = ws.batch_get(FAIXAS) or []


    ids = _coluna(faixas, 0)
    vencimentos = _coluna(faixas, 1)
    credores = _coluna(faixas, 2)
    valores = _coluna(faixas, 3)
    status = _coluna(faixas, 4)
    codigos = _coluna(faixas, 5)
    cards = _coluna(faixas, 6)
    datas = _coluna(faixas, 7)
    comprovantes = _coluna(faixas, 8)
    total = max(len(ids), len(status))

    dias = _inteiro(payload, 'dias', DIAS_PADRAO, minimo=1)
    corte = datetime.now() - timedelta(days=dias)

    def em(lista, i):
        return lista[i] if i < len(lista) else ''

    def registro(i, codigo):
        return {
            'sp_id': as_string(em(ids, i)).strip(),
            'linha': i + 2,
            'credor': as_string(em(credores, i)).strip(),
            'valor': as_string(em(valores, i)).strip(),
            'vencimento': as_string(em(vencimentos, i)).strip(),
            'data_pagamento': as_string(em(datas, i)).strip(),
            'status_na_planilha': as_string(em(status, i)).strip(),
            'card': as_string(em(cards, i)).strip(),
            'codigo_integracao': codigo,
        }

    pagas: List[Dict[str, Any]] = []
    nao_pagas: List[Dict[str, Any]] = []
    pagas_na_janela = 0
    nao_pagas_na_janela = 0
    sem_codigo = 0

    for i in range(total):
        codigo = as_string(em(codigos, i)).strip()
        marcada_paga = as_string(em(status, i)).strip().lower() == 'pago'

        if marcada_paga:
            # Direção 1: a planilha diz paga. Janela pela data de pagamento;
            # sem data, entra — é justamente o caso suspeito.
            if not as_string(em(comprovantes, i)).strip():
                continue
            dt = _data_br(em(datas, i))
            if dt and dt < corte:
                continue
            pagas_na_janela += 1
            if not codigo:
                # Sem código de integração não há o que perguntar ao Omie.
                sem_codigo += 1
                continue
            pagas.append(registro(i, codigo))
            continue

        # Direção 2: a planilha NÃO diz paga. É aqui que mora o furo, segundo o
        # dono: a conciliação bancária é diária, então o Omie está certo e a
        # planilha é quem fica para trás. Janela pelo VENCIMENTO, porque não há
        # data de pagamento para usar — a planilha não sabe que foi paga.
        if not codigo:
            continue
        dt = _data_br(em(vencimentos, i))
        if not dt or dt < corte:
            # Sem vencimento legível, fica de fora: a alternativa seria
            # consultar o Omie para as ~52 mil linhas da planilha.
            continue
        if dt > datetime.now() + timedelta(days=dias):
            # Vencimento muito à frente: ainda não deveria estar paga.
            continue
        nao_pagas_na_janela += 1
        nao_pagas.append(registro(i, codigo))

    return {
        'linhas_na_aba': total,
        'pagas_na_janela': pagas_na_janela,
        'nao_pagas_na_janela': nao_pagas_na_janela,
        'sem_codigo_integracao': sem_codigo,
        'conferiveis': pagas,
        'conferiveis_nao_pagas': nao_pagas,
        'dias': dias,
    }


def _consultar(item: Dict[str, Any], payload: dict) -> Dict[str, Any]:
    """Pergunta ao Omie o estado de um título. Não grava nada."""
    resp = execute_omie(build_consultar_conta_pagar(item['codigo_integracao'], payload))
    corpo = resp.get('body') or {}
    saida = dict(item)
    saida['status_omie'] = as_string(corpo.get('status_titulo')).upper()
    if not resp.get('ok'):
        saida['erro'] = (as_string(corpo.get('faultstring'))
                         or as_string(resp.get('raw'))[:200])
    return saida


def _e_nao_encontrado(registro: Dict[str, Any]) -> bool:
    """"Título não encontrado" é cadastro errado; o resto é falha de consulta.

    A diferença importa, e misturar as duas faria o relatório mentir sobre o que
    precisa de ação: uma é planilha com código errado, a outra é rede ou cota.
    """
    texto = as_string(registro.get('erro')).lower()
    return 'não encontrado' in texto or 'nao encontrado' in texto


def _frase_da_conferencia(divergentes: int, nao_encontradas: int, erros: int,
                          confirmadas: int, restam: int, dias: int,
                          planilha_atrasada: int = 0,
                          confirmadas_abertas: int = 0,
                          proximo: int = 0) -> str:
    """Uma frase em português, para quem lê isto pelo celular."""
    partes = []
    if planilha_atrasada:
        partes.append(
            f'{planilha_atrasada} SP(s) o Omie diz PAGA e a planilha NÃO — o'
            ' pagamento saiu e a SP continua aparecendo como a pagar; vale'
            ' olhar o cartão do Pipefy dessas também')
    if divergentes:
        partes.append(
            f'{divergentes} SP(s) a planilha diz PAGA e o Omie diz ABERTA — é'
            ' baixa pela metade, o dinheiro saiu e o título não baixou')
    if nao_encontradas:
        partes.append(f'{nao_encontradas} com código que não existe no Omie'
                      ' (cadastro errado na planilha, não é dinheiro)')
    if erros:
        partes.append(f'{erros} que não deu para consultar agora'
                      ' (vale tentar de novo)')

    if not partes:
        base = (f'Nenhuma divergência nos dois sentidos, na janela de {dias} dias:'
                f' {confirmadas} paga(s) nos dois lugares e {confirmadas_abertas}'
                ' ainda aberta(s) nos dois.')
    else:
        base = 'Achei ' + '; '.join(partes) + '.'
        if confirmadas or confirmadas_abertas:
            base += (f' Outras {confirmadas + confirmadas_abertas} estão de acordo'
                     ' nos dois lugares.')
    if restam:
        base += f' Faltam {restam} para conferir'
        if proximo:
            # O número pronto, para quem lê isto no celular não ter de calcular.
            base += f' — chame de novo com pular={proximo} para seguir daí.'
        else:
            base += '.'
    return base


def conferir(payload: dict) -> Dict[str, Any]:
    """Compara a planilha com o Omie nos DOIS sentidos. Não grava nada.

    Body opcional:
      {"dias": 60, "limite": 50, "pular": 0, "pausa_ms": 200,
       "apenas_contar": false, "sentido": "ambos", "omie": {...}}

    `pular` continua de onde a chamada anterior parou. Sem ele, cada chamada
    reconsultaria as mesmas primeiras linhas: o conferidor não grava nada, então
    nada sai do conjunto entre uma chamada e a seguinte. A resposta já devolve o
    `proximo_pular` pronto.

    `sentido`: "ambos" (padrão), "planilha_paga" (só a direção antiga) ou
    "omie_pago" (só o furo que o dono apontou — Omie pago, planilha para trás).
    """
    app_key, app_secret = credentials_from_payload(payload)
    if not app_key or not app_secret:
        return {'ok': False, 'app': 'baixabradesco', 'acao': 'conferir',
                'erro': 'credenciais_omie_ausentes'}

    base = candidatas(payload)
    pagas = base.pop('conferiveis')
    nao_pagas = base.pop('conferiveis_nao_pagas')

    sentido = as_string(payload.get('sentido') or 'ambos').strip().lower()
    if sentido == 'planilha_paga':
        nao_pagas = []
    elif sentido == 'omie_pago':
        pagas = []

    dias = _inteiro(payload, 'dias', DIAS_PADRAO, minimo=1)

    if payload.get('apenas_contar'):
        return {
            'ok': True,
            'app': 'baixabradesco',
            'acao': 'conferir',
            'apenas_contou': True,
            'sentido': sentido,
            'a_conferir_planilha_paga': len(pagas),
            'a_conferir_planilha_nao_paga': len(nao_pagas),
            **base,
            'em_portugues': (
                f'{len(pagas)} SP(s) que a planilha diz pagas e {len(nao_pagas)}'
                f' que ela diz NÃO pagas entram na comparação, numa janela de'
                f' {dias} dias. Nenhuma consulta ao Omie foi gasta.'
                " Chame sem 'apenas_contar' para comparar de verdade."),
        }

    limite = _inteiro(payload, 'limite', LIMITE_PADRAO, minimo=1)
    # ⚠️ Sem `pular`, a frase "chame de novo para continuar" era mentira: cada
    # chamada reconsultava as MESMAS primeiras linhas, para sempre — o
    # conferidor não corrige nada, então nada sai do conjunto entre chamadas.
    pular = _inteiro(payload, 'pular', 0)
    pausa = _inteiro(payload, 'pausa_ms', int(PAUSA_OMIE * 1000)) / 1000.0

    inicio = datetime.now()
    divergentes: List[Dict[str, Any]] = []
    planilha_atrasada: List[Dict[str, Any]] = []
    nao_encontradas: List[Dict[str, Any]] = []
    erros: List[Dict[str, Any]] = []
    confirmadas = 0
    confirmadas_abertas = 0
    consultas = 0

    lote_pagas = pagas[pular:pular + limite]
    lote_nao_pagas = nao_pagas[pular:pular + limite]

    # Direção 1: a planilha diz paga. O que o Omie negar é baixa pela metade.
    for posicao, item in enumerate(lote_pagas):
        if pausa and consultas:
            time.sleep(pausa)
        registro = _consultar(item, payload)
        consultas += 1
        if registro['status_omie'] == 'PAGO':
            confirmadas += 1
        elif registro.get('erro'):
            (nao_encontradas if _e_nao_encontrado(registro) else erros).append(registro)
        else:
            divergentes.append(registro)

    # Direção 2: a planilha NÃO diz paga. O que o Omie disser PAGO é planilha
    # para trás — o furo que a conciliação bancária diária torna o mais provável.
    for posicao, item in enumerate(lote_nao_pagas):
        if pausa and consultas:
            time.sleep(pausa)
        registro = _consultar(item, payload)
        consultas += 1
        if registro.get('erro'):
            (nao_encontradas if _e_nao_encontrado(registro) else erros).append(registro)
        elif registro['status_omie'] == 'PAGO':
            planilha_atrasada.append(registro)
        else:
            confirmadas_abertas += 1

    restam = (max(0, len(pagas) - pular - limite)
              + max(0, len(nao_pagas) - pular - limite))
    proximo = pular + limite if restam else 0
    achou_algo = bool(divergentes or planilha_atrasada or nao_encontradas or erros)

    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'conferir',
        'sentido': sentido,
        **base,
        'consultas_ao_omie': consultas,
        'pulou': pular,
        'restam_para_conferir': restam,
        'proximo_pular': proximo,
        # Direção 2 primeiro: é a que o dono apontou como o furo de verdade.
        'planilha_atrasada': planilha_atrasada,
        'quantidade_planilha_atrasada': len(planilha_atrasada),
        'divergentes': divergentes,
        'quantidade_divergentes': len(divergentes),
        'titulos_nao_encontrados': nao_encontradas,
        'erros_de_consulta': erros,
        'confirmadas_pagas_nos_dois': confirmadas,
        'confirmadas_abertas_nos_dois': confirmadas_abertas,
        'segundos': round((datetime.now() - inicio).total_seconds(), 1),
        'aviso': ('Relatório apenas. Nada foi gravado na planilha nem no Omie.'
                  if achou_algo
                  else 'Nenhuma divergência na janela conferida.'),
        'em_portugues': _frase_da_conferencia(
            len(divergentes), len(nao_encontradas), len(erros), confirmadas,
            restam, dias, len(planilha_atrasada), confirmadas_abertas, proximo),
    }
