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
import logging
import os
import threading
import time
import uuid

from .utils import as_string

FILA_DIR = '/tmp/baixabradesco_fila_tardia'
MAX_TENTATIVAS = 10

logger = logging.getLogger(__name__)


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
        saida = {
            'antes_de': r.get('antes_de'),
            'dispensadas': r.get('dispensadas'),
            'por_etapa': r.get('por_etapa'),
            'erro': r.get('erro'),
        }
        # Um mutirão precisa saber dizer que acabou, senão alguém fica olhando
        # número sem saber o que esperar — e a varredura segue custando uma
        # leitura da faixa de controle a cada cinco minutos, de graça.
        if r.get('ok') and not r.get('encontradas'):
            saida['concluido'] = True
            saida['em_portugues'] = (
                'Mutirão concluído: não há mais pendência anterior a '
                + as_string(r.get('antes_de'))
                + '. Pode esvaziar BAIXABRADESCO_ZERAR_ANTES_DE no Render para'
                  ' a varredura parar de rodar à toa.')
        return saida
    except Exception as e:
        return {'erro': str(e)[:200]}


def _dispensar_avisos_vencidos(payload: dict | None = None) -> dict:
    """Tira da fila o aviso que já não serve. Vale só para aviso."""
    from .fila import dispensar_avisos_vencidos
    try:
        r = dispensar_avisos_vencidos(dict(payload or {}))
        return {'dispensadas': r.get('dispensadas'), 'erro': r.get('erro')}
    except Exception as e:
        return {'erro': str(e)[:200]}


def _drenar_a_fila(payload: dict | None = None) -> dict:
    from .fila import drenar_por_etapa
    return drenar_por_etapa(LIMITES_CRON, payload)


# ── Quanto um disparo pode durar ──────────────────────────────────────────────
#
# 12/11/2026, log do Render que o dono colou junto com "tá super lento":
#
#   AS 4 LINHAS OCUPADAS: POST /api/baixabradesco/processar-fila-tardia há 93 s
#   AS 4 LINHAS OCUPADAS: POST /api/baixabradesco/processar-fila-tardia há 124 s
#
# A produção atende com 4 linhas (Procfile: --workers 1 --threads 4). Este
# disparo reprocessava a fila adiada INTEIRA e depois drenava até 55 itens da
# fila de falhas, tudo dentro da mesma requisição — segurando uma das quatro
# linhas por minutos, a cada cinco minutos, e qualquer tela de qualquer área
# esperava atrás dele.
#
# Duas mudanças, que valem juntas:
#   1. o trabalho tem TETO por disparo: tempo (PRAZO_POR_DISPARO_S, conferido
#      entre um item e outro) e quantidade de payloads adiados
#      (MAX_ADIADOS_POR_DISPARO). O que sobra fica para o disparo seguinte;
#   2. a rota responde NA HORA e o trabalho roda numa linha própria, fora das
#      quatro que atendem as telas (`disparar_em_segundo_plano`).
#
# O teto é conferido ENTRE itens: o item em curso sempre termina, porque um
# comprovante pela metade (Omie baixado, planilha não gravada) é pior que um
# disparo que passou do tempo. Por isso o prazo é curto — um item demorado
# estoura no máximo um item.
PRAZO_POR_DISPARO_S = 25
MAX_ADIADOS_POR_DISPARO = 10


def _prazo_do_disparo(payload: dict | None) -> float:
    """Segundos de trabalho deste disparo. O ambiente ajusta sem publicar."""
    bruto = (os.getenv('BAIXABRADESCO_PRAZO_CRON_S', '') or '').strip()
    try:
        segundos = float(bruto) if bruto else float(PRAZO_POR_DISPARO_S)
    except ValueError:
        segundos = float(PRAZO_POR_DISPARO_S)
    # Fora de faixa cai no padrão: zero não pode virar "não faz nada nunca",
    # nem um número enorme pode trazer de volta a linha presa por minutos.
    if not 5 <= segundos <= 120:
        segundos = float(PRAZO_POR_DISPARO_S)
    return segundos


def _e_cota(msg: str) -> bool:
    return '429' in msg or 'Quota exceeded' in msg or 'RESOURCE_EXHAUSTED' in msg


def processar_fila_tardia(payload: dict | None = None) -> dict:
    """Reprocessa os payloads adiados, um a um, em ordem de chegada — com teto.

    - Sucesso: remove o arquivo.
    - Falha comum: incrementa tentativas (descarta após MAX_TENTATIVAS).
    - Falha por quota (429): para o loop imediatamente — o resto fica para o
      próximo disparo do cron, evitando queimar a janela de quota seguinte.
    - Teto do disparo (tempo ou quantidade): para entre um item e outro; o
      resto fica no disco, intacto, para o próximo disparo.

    O mesmo prazo vale para o disparo inteiro: o que vem depois (mutirão,
    avisos vencidos, fila de falhas) só roda se ainda houver tempo.
    """
    from .core import processar_baixabradesco

    inicio = time.monotonic()
    prazo = inicio + _prazo_do_disparo(payload)
    parou_por = ''

    resultados = []
    for nome in _listar():
        if len(resultados) >= MAX_ADIADOS_POR_DISPARO:
            parou_por = 'limite_de_itens'
            break
        if time.monotonic() >= prazo:
            parou_por = 'prazo'
            break
        caminho = os.path.join(FILA_DIR, nome)
        try:
            with open(caminho, encoding='utf-8') as f:
                item = json.load(f)
        except FileNotFoundError:
            continue
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
            if _e_cota(msg):
                parou_por = 'cota_do_google'
                break  # quota estourada de novo — tenta no próximo cron

    pendentes = len(_listar())
    saida = {
        'ok': True,
        'app': 'baixabradesco',
        'processados': resultados,
        'pendentes': pendentes,
        'parou_por': parou_por,
        'prazo_s': round(prazo - inicio, 1),
    }

    # O cron já roda de 5 em 5 minutos e já está autenticado: a fila de
    # falhas pega carona nele. Antes ela só andava se alguém chamasse a rota à
    # mão — e, pelos números de 08/10/2026 (2.269 linhas, nenhuma concluída, a
    # mais antiga de 18/06/2026), nunca ninguém chamou.
    # A ORDEM importa: zerar primeiro, drenar depois. Senão a drenagem gastaria
    # a passada inteira nas linhas antigas que vão ser dispensadas dois
    # segundos mais tarde.
    #
    # Cada passo só começa se o prazo do disparo não acabou; o que fica de fora
    # roda no disparo seguinte. Nenhum deles é perdido: a fila de falhas mora
    # na planilha, não nesta requisição.
    adiado = {'adiado_para_o_proximo_disparo': True}
    corpo = dict(payload or {})
    corpo['_prazo'] = prazo
    saida['atraso_zerado'] = (_zerar_atraso_uma_vez(payload)
                              if time.monotonic() < prazo else dict(adiado))
    # Aviso que passou do prazo nunca mais vai ser enviado: deixá-lo PENDENTE
    # era um limbo que travava a contagem em 121 pendências que jamais
    # andariam (números do dono em 08/10/2026).
    saida['avisos_vencidos'] = (_dispensar_avisos_vencidos(payload)
                                if time.monotonic() < prazo else dict(adiado))
    saida['fila_de_falhas'] = (_drenar_a_fila(corpo)
                               if time.monotonic() < prazo else dict(adiado))
    saida['segundos'] = round(time.monotonic() - inicio, 1)
    saida['em_portugues'] = _frase_do_disparo(saida)
    return saida


def _frase_do_disparo(r: dict) -> str:
    feitos = len(r.get('processados') or [])
    pendentes = int(r.get('pendentes') or 0)
    partes = [f'Comprovantes adiados: {feitos} reprocessado(s) neste disparo']
    if pendentes:
        partes.append(f'{pendentes} ainda na fila, para os próximos disparos')
    else:
        partes.append('nenhum na fila')
    motivo = {
        'prazo': 'parou pelo tempo do disparo',
        'limite_de_itens': 'parou pelo limite de itens do disparo',
        'cota_do_google': 'parou porque a cota do Google estourou',
    }.get(r.get('parou_por') or '')
    if motivo:
        partes.append(motivo)
    frase = '; '.join(partes) + '.'
    falhas = r.get('fila_de_falhas') or {}
    if falhas.get('adiado_para_o_proximo_disparo') or falhas.get('parou_por_prazo_na_etapa'):
        frase += (' A fila de falhas não andou inteira por falta de tempo neste'
                  ' disparo; continua no próximo.')
    return frase


# ── Segundo plano ────────────────────────────────────────────────────────────
#
# A rota não espera o trabalho: dispara uma linha própria e responde na hora.
# A trava garante UM disparo por vez — se o cron chegar enquanto o anterior
# ainda roda, ele não começa outro (dois disparos pegariam o mesmo arquivo da
# fila e o mesmo item da planilha). Vale porque a produção tem UM processo
# (`--workers 1`, obrigatório pelo `CLAUDE.md`); com mais de um, cada processo
# teria a sua trava.
#
# A linha NÃO é daemon, de propósito: na renovação do serviço o Python espera
# ela terminar o item em curso, e o `gunicorn.conf.py` põe prazo de 30 s nessa
# espera. Como o disparo dura ~25 s, ele quase sempre termina inteiro; se não
# terminar, o que não foi concluído fica na fila (arquivo no disco, linha
# PENDENTE na planilha) e é retomado depois — repetir é seguro, ver
# `HISTORICO.md` (08/10/2026).
_trava = threading.Lock()
_estado: dict = {'rodando_desde': None, 'ultima': None}


def _rodar(payload: dict) -> None:
    comeco = time.strftime('%d/%m/%Y %H:%M:%S')
    r: dict = {'ok': False, 'erro': 'interrompido antes de terminar'}
    try:
        r = processar_fila_tardia(payload)
        # Disparo comum fica em INFO (o serviço não mostra INFO de módulo no
        # log do Render, e uma linha a cada 5 min seria ruído). Disparo que
        # bateu no teto ou passou do prazo sobe para WARNING, que aparece:
        # é o sinal de que a fila está acumulando ou um item está lento.
        falhas = r.get('fila_de_falhas') or {}
        notavel = bool(r.get('parou_por')
                       or falhas.get('adiado_para_o_proximo_disparo')
                       or falhas.get('parou_por_prazo_na_etapa')
                       or (r.get('segundos') or 0) > (r.get('prazo_s') or 0) + 10)
        (logger.warning if notavel else logger.info)(
            'Fila tardia: %s (%s s)', r.get('em_portugues'),
            f"{r.get('segundos') or 0:.1f}".replace('.', ','))
    except Exception as e:
        logger.exception('Fila tardia: o disparo em segundo plano falhou')
        r = {'ok': False, 'erro': str(e)[:300]}
    finally:
        # O resultado é guardado ANTES de soltar a trava (senão o próximo
        # disparo poderia ler o anterior ao anterior), e a trava é solta
        # SEMPRE — uma trava presa pararia a fila até o serviço reiniciar.
        _estado['ultima'] = {'comecou_em': comeco,
                             'terminou_em': time.strftime('%d/%m/%Y %H:%M:%S'),
                             'resultado': r}
        _estado['rodando_desde'] = None
        _trava.release()


def disparar_em_segundo_plano(payload: dict | None = None) -> dict:
    """Começa o disparo numa linha própria e responde sem esperar.

    A resposta traz o resultado do disparo ANTERIOR (`ultima_execucao`) — é ali
    que se olha `fila_de_falhas`, `atraso_zerado` e o que ficou na fila.
    """
    payload = dict(payload or {})
    base = {'ok': True, 'app': 'baixabradesco', 'pendentes': len(_listar()),
            'ultima_execucao': _estado.get('ultima')}
    if not _trava.acquire(blocking=False):
        base.update({
            'disparado': False,
            'ja_em_andamento': True,
            'rodando_desde': _estado.get('rodando_desde'),
            'em_portugues': ('O disparo anterior ainda está trabalhando; este não'
                             ' começou outro para não processar o mesmo item duas'
                             ' vezes. Segue no próximo.'),
        })
        return base
    _estado['rodando_desde'] = time.strftime('%d/%m/%Y %H:%M:%S')
    try:
        linha = threading.Thread(target=_rodar, args=(payload,),
                                 name='baixabradesco-fila-tardia', daemon=False)
        linha.start()
    except Exception:
        _estado['rodando_desde'] = None
        _trava.release()
        raise
    base.update({
        'disparado': True,
        'em_portugues': ('Disparo iniciado em segundo plano. O resultado aparece'
                         ' em "ultima_execucao" na resposta do próximo disparo.'),
    })
    return base


def processar_agora(payload: dict | None = None) -> dict:
    """Roda o disparo nesta requisição e devolve o resultado (diagnóstico à mão).

    Respeita a mesma trava e o mesmo prazo: não há como, por aqui, voltar a
    segurar uma linha por minutos.
    """
    if not _trava.acquire(blocking=False):
        return {'ok': True, 'app': 'baixabradesco', 'disparado': False,
                'ja_em_andamento': True,
                'rodando_desde': _estado.get('rodando_desde')}
    _estado['rodando_desde'] = time.strftime('%d/%m/%Y %H:%M:%S')
    try:
        r = processar_fila_tardia(payload)
        _estado['ultima'] = {'comecou_em': _estado['rodando_desde'],
                             'terminou_em': time.strftime('%d/%m/%Y %H:%M:%S'),
                             'resultado': r}
        return r
    finally:
        _estado['rodando_desde'] = None
        _trava.release()
