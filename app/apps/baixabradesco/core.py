# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Dict, Any, List

import requests

from .models import AttachmentInput, ExecutionPlan, MatchResult
from .utils import b64decode_bytes, fingerprint_bytes, as_string
from .parser_pdf import extract_pdf_pages, extract_single_page_pdf
from .parser_bradesco import parse_bradesco_text
from .parser_sicredi import is_sicredi, parse_sicredi_text
from .sheets import get_gc, load_spsbd_index, load_spsbd_values, load_spsbd_operacional, load_spsbd_omie_pendente, load_spsagendar, load_base_bancos, load_baixabradesco_values, find_bank_account, build_spsbd_updates, execute_spsbd_updates, load_fingerprints_processados, registrar_fingerprints
from .matcher import match_receipt
from .omie import build_omie_plan, execute_omie, codigo_integracao
from .pipefy import (build_get_cards_query, build_update_card_mutation,
                     build_update_cards_mutation, cards_recusados, execute_graphql)
from .zapi import build_whatsapp_messages, send_messages_batch, resolve_zapi_auth, validate_zapi_auth
from .storage import upload_dropbox_bytes, build_receipt_page_filename, normalize_dropbox_link
from .fila import enqueue_failure
from .fila_tardia import (adiar_payload, guardar_recebido,
                          impressao_do_pedido, soltar_recebido)
from .avisos import enviar_aviso, enviar_relatorio

# Teto por comprovante baixado via URL — evita que um download gigante entre
# 100% na RAM e derrube o worker (limite Render: 2GB).
MAX_ATTACHMENT_BYTES = 50 * 1024 * 1024  # 50 MB


# ── A PORTA: o Make não espera mais, e um lote só roda uma vez ───────────────
#
# ⚠️ DOIS DEFEITOS QUE SE ALIMENTAVAM — relato do dono em 16/11/2026:
#
#   *"o módulo do Make que chama a baixa constantemente dá erro: This module
#   timed out. E suspeito ainda que quando ele tenta reexecutar, ele manda o
#   comprovante todo novamente (…) tem um módulo break no cenário e ele repete
#   a operação."*
#
# Ele estava certo nos dois pontos, e um multiplicava o outro:
#
# 1. **A baixa demorava mais do que o Make espera.** Os registros do Render de
#    16/11 mostram 162 s, 194 s, 246 s, 460 s, 522 s e 602 s na mesma rota. O
#    custo é por comprovante — Omie, Dropbox, planilha, Pipefy — e trinta
#    comprovantes são trinta vezes isso.
#
# 2. **O "break" do cenário remandava o lote inteiro.** E a chamada antiga NÃO
#    para quando o Make desiste: o serviço continua trabalhando. Resultado: duas
#    execuções do mesmo arquivo lado a lado, ocupando DUAS das quatro linhas de
#    atendimento, dobrando o consumo de cota do Google e do Omie — o que deixava
#    tudo mais lento, o que causava mais timeout, o que causava mais reenvio. Nos
#    registros das 20:28 as duas aparecem juntas ("há 477 s" e "há 114 s").
#
# A DECISÃO DO DONO, no mesmo dia, resolveu o lado dele do problema:
#
#   *"no Make eu não preciso de resposta para cada baixa. A gente não está
#   utilizando isso. O cenário manda e acabou-se. (…) Basta dizer: ó, tá
#   processando. (…) O que precisa ser garantido é que, uma vez feita a chamada,
#   a baixa aconteça. (…) O retorno ao final de tudo você me manda no Telegram."*
#
# Então esta porta faz três coisas e só:
#
# · **Guarda o lote em disco** antes de qualquer resposta, e só apaga quando a
#   baixa termina. É o que sustenta a garantia — ver `fila_tardia.guardar_recebido`
#   para o que ela cobre e o que não cobre.
# · **Responde na hora** "recebi, estou processando". O Make nunca mais vê
#   timeout, nunca mais dispara o break.
# · **Recusa o lote repetido.** Mesma impressão digital já rodando = responde na
#   hora, sem refazer nada. É a defesa contra o reenvio que ainda escapar.
#
# A tela do Análise de SPs e a fila de adiados chamam com `inline=True`: as duas
# PRECISAM do resultado completo (uma para mostrar na tela, a outra para decidir
# se apaga o adiado), e nenhuma das duas ocupa linha de atendimento do serviço.
MAX_LOTES_EM_PARALELO = int(os.getenv('BAIXABRADESCO_MAX_LOTES', '') or 6)

_LOTES_TRINCO = threading.Lock()
_LOTES_EM_ANDAMENTO: Dict[str, Dict[str, Any]] = {}

logger = logging.getLogger(__name__)


def lotes_em_andamento() -> List[Dict[str, Any]]:
    """O que está rodando agora. NUNCA devolve o payload: ele traz credencial."""
    agora = time.time()
    with _LOTES_TRINCO:
        itens = list(_LOTES_EM_ANDAMENTO.items())
    return [{
        'lote': chave,
        'arquivos': info.get('arquivos') or [],
        'comprovantes': info.get('comprovantes') or 0,
        'rodando_ha_segundos': int(agora - (info.get('inicio') or agora)),
    } for chave, info in itens]


def _soltar_lote(chave: str) -> None:
    with _LOTES_TRINCO:
        _LOTES_EM_ANDAMENTO.pop(chave, None)


def _e_falta_de_cota(erro: BaseException) -> bool:
    msg = str(erro)
    return ('Falha ao carregar dados Google Sheets' in msg
            and ('429' in msg or 'Quota exceeded' in msg or 'RESOURCE_EXHAUSTED' in msg))


def processar_baixabradesco(payload: Dict[str, Any],
                            em_segundo_plano: bool = False) -> Dict[str, Any]:
    """Porta de entrada da baixa. Ver o comentário acima.

    ⚠️ O SEGUNDO PLANO É SÓ PARA QUEM CHEGA DE FORA, pela rota HTTP — ou seja,
    para o Make. Quem chama esta função POR DENTRO do serviço (a tela do
    Análise de SPs, a fila de adiados) não passa nada e continua recebendo o
    resultado completo: uma precisa dele para mostrar "baixado" na tela, a
    outra para decidir se apaga o adiado. Nenhuma das duas ocupa linha de
    atendimento do serviço, então nenhuma tem o problema que isto resolve.
    """
    if bool(payload.get('modo_teste', True)) or not em_segundo_plano:
        return _processar_lote(payload)

    anexos = normalize_attachments(payload)
    if not anexos:
        raise ValueError('Nenhum comprovante enviado. Use attachments/comprovantes com filename e base64 ou url.')

    # Sem impressão digital (formato inesperado), cada pedido é um lote novo: é
    # melhor repetir trabalho do que travar uma baixa de verdade.
    chave = impressao_do_pedido(payload) or uuid.uuid4().hex[:32]
    nomes = [a.filename for a in anexos][:20]

    with _LOTES_TRINCO:
        em_curso = _LOTES_EM_ANDAMENTO.get(chave)
        if em_curso:
            return {
                'ok': True,
                'app': 'baixabradesco',
                'modo_teste': False,
                'ja_em_andamento': True,
                'lote': chave,
                'rodando_ha_segundos': int(time.time() - (em_curso.get('inicio') or time.time())),
                'comprovantes_recebidos': len(anexos),
                'motivo': ('Estes mesmos comprovantes já estão sendo processados neste '
                           'servidor. Nada foi refeito e nada foi baixado duas vezes — '
                           'o resultado chega no Telegram quando terminar.'),
            }
        if len(_LOTES_EM_ANDAMENTO) >= MAX_LOTES_EM_PARALELO:
            # Nunca deixar a fila de linhas de fundo crescer sem teto: memória é
            # 2 GB e cada lote carrega os PDFs inteiros.
            logger.warning('BaixaBradesco: %s lotes já em andamento; este espera a vez.',
                           MAX_LOTES_EM_PARALELO)
            return {
                'ok': True,
                'app': 'baixabradesco',
                'modo_teste': False,
                'recusado_por_lotacao': True,
                'lotes_em_andamento': MAX_LOTES_EM_PARALELO,
                'comprovantes_recebidos': len(anexos),
                'motivo': ('O servidor já está processando o máximo de lotes ao mesmo '
                           'tempo. Reenvie estes comprovantes daqui a alguns minutos.'),
            }
        _LOTES_EM_ANDAMENTO[chave] = {
            'inicio': time.time(),
            'arquivos': nomes,
            'comprovantes': len(anexos),
        }

    # Disco ANTES da resposta: é isto que sustenta "uma vez chamado, a baixa
    # acontece". Guardado com a mesma conta da fila de adiados, então o serviço
    # automático que já existe termina o que ficou pela metade.
    guardado = guardar_recebido(payload)

    def _rodar():
        try:
            resultado = _processar_lote(payload)
        except BaseException as e:
            _soltar_lote(chave)
            if _e_falta_de_cota(e):
                # Já está guardado em disco; o serviço automático reprocessa.
                logger.warning('BaixaBradesco: lote %s parou por cota do Google; fica guardado.', chave)
                if not guardado:
                    try:
                        adiar_payload(payload, str(e))
                    except Exception:
                        pass
                return
            logger.error('BaixaBradesco: lote %s falhou: %s', chave, e)
            return
        _soltar_lote(chave)
        # Só agora o lote sai do disco: terminou de verdade.
        soltar_recebido(guardado)
        try:
            resumo = resultado.get('resumo') or {}
            logger.info('BaixaBradesco: lote %s concluído — %s', chave, resumo)
        except Exception:
            pass

    threading.Thread(target=_rodar, daemon=True, name=f'baixa-{chave[:8]}').start()

    return {
        'ok': True,
        'app': 'baixabradesco',
        'modo_teste': False,
        'recebido': True,
        'lote': chave,
        'comprovantes_recebidos': len(anexos),
        'arquivos': nomes,
        'guardado_em_disco': bool(guardado),
        'motivo': ('Recebido. A baixa está sendo processada no servidor e o resultado '
                   'de cada comprovante chega no Telegram ao terminar. NÃO reenvie: '
                   'reenviar o mesmo arquivo não adianta nada enquanto este lote roda.'),
    }


def _processar_lote(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Faz o lote inteiro, do começo ao fim. Quem chama é a porta, acima."""
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
    sps_omie_pendente = {}
    fingerprints_processados: Dict[str, str] = {}   # impressão digital → nº da SP
    google_error = ''

    try:
        gc = get_gc()
        sps_agendar = load_spsagendar(gc)   # ~18 registros, leve
        base_bancos = load_base_bancos(gc)  # ~15 registros, leve
        # Uma leitura só da coluna de comprovantes já baixados. É o que impede
        # a mesma página virar baixa duas vezes.
        fingerprints_processados = load_fingerprints_processados(gc)
        # ⚠️ A aba ENXUTA primeiro. Ela espelha a SPsAgendar com as mesmas
        # colunas da SPsBD, e o dono garante que a SP procurada está nela em
        # quase 100% dos casos. São dezenas de linhas no lugar de 52 mil — e as
        # 52 mil eram a causa de fundo de a baixa levar minutos e ocupar uma das
        # quatro linhas de atendimento do serviço (registros de 13/11/2026).
        #
        # A SPsBD inteira passa a ser o PLANO B: só é lida quando a aba enxuta
        # não resolveu. Ver `_completar_com_spsbd`.
        sps_index = load_spsbd_operacional(
            gc, values=load_baixabradesco_values(gc), linha_vem_da_coluna_ac=True)
        leu_da_aba_enxuta = bool(sps_index)
    except Exception as e:
        google_error = str(e)
        if not modo_teste:
            raise RuntimeError(f'Falha ao carregar dados Google Sheets: {e}')

    # ── O plano B existe para UMA coisa só: as SPs já marcadas "Pago" ──────
    #
    # O dono fechou a regra em 14/11/2026 e isso enxugou o desenho:
    #
    #   *"Se a gente só baixa o que está no status agendar, agendado ou falha
    #   agendado, então tudo que está nesses três status vai aparecer na aba
    #   baixabradesco."* — e, perguntado como a aba é alimentada: *"a aba é
    #   alimentada via fórmula, então é instantânea."*
    #
    # Fórmula viva + mesmo conjunto = **reler a SPsBD com o MESMO filtro não
    # pode achar nada novo**. Era custo puro: 52 mil linhas para confirmar o que
    # a aba já tinha dito. Esse pedaço saiu.
    #
    # O que sobra, e é legítimo: a SP pode estar **já marcada "Pago" com o Omie
    # pendente**. Essas, por definição, NÃO estão na aba enxuta (que espelha as
    # abertas), e é o que permite um reenvio concluir baixa pela metade —
    # defeito real consertado em 13/09/2026. Só isso justifica a leitura grande,
    # e só quando o comprovante não casou com nada.
    estado_spsbd = {'leu': False, 'fonte': 'aba_enxuta' if leu_da_aba_enxuta else 'spsbd_direto'}

    def _carregar_omie_pendente():
        """Lê a SPsBD (uma vez por lote) só para as SPs pagas com Omie pendente."""
        nonlocal sps_omie_pendente
        if estado_spsbd['leu'] or modo_teste:
            return
        estado_spsbd['leu'] = True
        try:
            valores = load_spsbd_values(gc)
            sps_omie_pendente = load_spsbd_omie_pendente(gc, values=valores)
            del valores                  # libera as 52k linhas cruas na hora
            estado_spsbd['fonte'] = 'spsbd_omie_pendente'
        except Exception as e:
            # Não derruba o lote: a aba enxuta já resolveu o resto.
            estado_spsbd['erro'] = str(e)[:200]

    if not leu_da_aba_enxuta and not modo_teste:
        # Aba enxuta ausente ou vazia (renomeada, apagada, fórmula com erro):
        # volta ao comportamento antigo, para a baixa não parar por causa disso.
        try:
            _valores = load_spsbd_values(gc)
            sps_index = load_spsbd_operacional(gc, values=_valores)
            sps_omie_pendente = load_spsbd_omie_pendente(gc, values=_valores)
            del _valores
            estado_spsbd['leu'] = True
            estado_spsbd['fonte'] = 'spsbd_completa_aba_indisponivel'
        except Exception as e:
            estado_spsbd['erro'] = str(e)[:200]

    # ── Processa cada comprovante / cada página ────────────────────────────────
    plans: List[ExecutionPlan] = []
    card_ids_para_get: List[str] = []
    recusados: List[Dict[str, Any]] = []   # páginas que o banco não efetivou
    duplicados: List[Dict[str, Any]] = []  # páginas que já haviam sido baixadas
    completados: List[Dict[str, Any]] = []  # reenvios aceitos para concluir baixa pela metade

    for att in attachments:
        pdf_bytes = load_attachment_bytes(att)
        fp_file   = fingerprint_bytes(pdf_bytes, att.filename)
        pages     = extract_pdf_pages(pdf_bytes)

        # Primeira passada: lê e localiza a SP de cada página, sem executar nada.
        # A segunda passada é que salva comprovante e monta os planos. Entre as
        # duas entra o desempate por lote, que precisa enxergar o anexo inteiro.
        analisadas: List[Dict[str, Any]] = []

        for page_num, text in pages:
            if not as_string(text):
                continue

            # Cada banco escreve de um jeito. O Sicredi, por exemplo, põe o
            # valor como "Valor Pago (R$): 10.861,20" e o número da SP em
            # "Descrição do Pagamento" — o leitor do Bradesco não enxerga
            # nenhum dos dois e o comprovante ficaria sem valor e sem SP.
            ler = parse_sicredi_text if is_sicredi(text) else parse_bradesco_text
            rec = ler(
                filename=att.filename,
                page=page_num,
                text=text,
                drive_link='',
                fingerprint=f'{fp_file}:{page_num}',
            )

            # Comprovante que o banco não efetivou: nunca vira baixa. Fica
            # registrado no resumo para não sumir em silêncio.
            if rec.tipo_comprovante == 'operacao_nao_realizada':
                recusados.append({
                    'arquivo': att.filename,
                    'pagina': page_num,
                    'motivo': 'O banco não efetivou a operação (comprovante recusado).',
                })
                continue

            # Esta mesma página já virou baixa antes? Então não vira de novo —
            # a não ser que aquela baixa tenha ficado pela METADE.
            #
            # O Omie e a planilha são gravados em momentos diferentes, e a
            # impressão digital é registrada assim que o Omie aceita. Se a
            # gravação na planilha falhar depois disso, a SP fica "Pagar" para
            # sempre e o comprovante reenviado era barrado como repetido — o
            # pior dos dois mundos. Agora, se a SP daquele comprovante ainda
            # estiver na lista das que faltam pagar, o reenvio passa: o Omie
            # responde "título já pago", o robô pula essa parte e termina o que
            # faltava na planilha.
            if rec.fingerprint and rec.fingerprint in fingerprints_processados:
                sp_registrada = as_string(fingerprints_processados.get(rec.fingerprint))
                # ⚠️ AQUI NÃO SE LÊ A SPsBD INTEIRA, e a razão é o próprio uso.
                #
                # O dono em 14/11/2026: *"reenvio é algo que acontece com alguma
                # frequência, pq às vezes mandamos um arquivo com alguns
                # comprovantes e de repente algum já havia sido baixado."*
                #
                # Comprovante já baixado tem a SP em "Pago", e SP paga NÃO está
                # na aba enxuta (que espelha as abertas). Se a ausência na aba
                # disparasse a leitura das 52 mil linhas, quase todo lote com um
                # repetido pagaria esse preço — ou seja, o caso mais comum
                # anularia a otimização inteira.
                #
                # Então a aba enxuta decide: SP nela = baixa pela metade (o
                # reenvio passa e conclui); SP fora dela = baixa completa (o
                # reenvio é barrado). O dono garante que a SP aberta está na aba
                # em ~100% dos casos.
                #
                # O que se perde no caso raro (aba não espelhou uma SP aberta):
                # o reenvio é recusado como repetido e a SP continua "Pagar".
                # Não há perda de dinheiro, e fica VISÍVEL — por isso o número
                # da SP vai no motivo, para dar para conferir na hora.
                baixa_incompleta = bool(sp_registrada) and sp_registrada in sps_index
                if not baixa_incompleta:
                    duplicados.append({
                        'arquivo': att.filename,
                        'pagina': page_num,
                        'sp': sp_registrada,
                        'motivo': ('Comprovante já baixado antes (consta na '
                                   'LogBaixaBradesco)'
                                   + (f' — SP {sp_registrada}.' if sp_registrada else '.')
                                   + ' Se essa SP ainda estiver como "Pagar" na'
                                     ' planilha, avise: significa que a baixa'
                                     ' ficou pela metade e o reenvio precisa passar.'),
                    })
                    continue
                completados.append({
                    'arquivo': att.filename,
                    'pagina': page_num,
                    'sp': sp_registrada,
                    'motivo': ('Baixa anterior ficou pela metade: o Omie foi baixado mas a '
                               'planilha continua como "Pagar". Reenvio aceito para concluir.'),
                })
            if rec.fingerprint:
                fingerprints_processados.setdefault(rec.fingerprint, '')

            # Primeiro localiza a SP/título. Só depois salva o comprovante.
            # Isso evita gerar arquivos órfãos no Dropbox quando a baixa não puder
            # ser vinculada a nenhuma solicitação financeira.
            match = match_receipt(rec, sps_index, sps_agendar)

            # Não casou com nada. Repetir o mesmo casamento contra a SPsBD seria
            # inútil (a aba é fórmula viva do mesmo conjunto). O único lugar
            # onde a SP ainda pode estar é entre as já marcadas "Pago" com o
            # Omie pendente — e aí sim vale a leitura grande, uma vez por lote.
            if match.status == 'nao_localizado':
                _carregar_omie_pendente()
                if sps_omie_pendente:
                    match_pendente = match_omie_pendente(rec, sps_omie_pendente)
                    if match_pendente.status == 'localizado':
                        match = match_pendente

            analisadas.append({'page_num': page_num, 'rec': rec, 'match': match})

        # Desempate por lote: comprovantes iguais para SPs iguais.
        resolver_empates_do_lote(analisadas)

        # Segunda passada: agora sim, salvar comprovante e montar os planos.
        for item in analisadas:
            page_num = item['page_num']
            rec      = item['rec']
            match    = item['match']
            page_filename = build_receipt_page_filename(att.filename, page_num, rec.id_pipefy)

            banco = find_bank_account(base_bancos, rec.agencia_origem, rec.conta_origem) if base_bancos else None

            storage_info = {
                'storage': 'dropbox',
                'status': 'nao_salvo_sem_match' if match.status != 'localizado' else 'nao_salvo_modo_teste',
                'url': '',
                'path': f'{pasta_dropbox.rstrip("/")}/{page_filename}',
            }

            # Se a SPsBD já possui comprovante, reutiliza o link e não salva novo arquivo.
            link_existente = ''
            if match.status == 'localizado' and match.sp:
                link_existente = as_string((match.sp.raw or {}).get('Comprovante'))

            if link_existente:
                rec.drive_link = normalize_dropbox_link(link_existente)
                storage_info = {
                    'storage': 'dropbox',
                    'status': 'reutilizado_spsbd',
                    'url': rec.drive_link,
                    'path': '',
                    'motivo': 'SPsBD já possuía link de comprovante. Novo upload ignorado.',
                }
            elif match.status == 'localizado' and salvar_comprovante and not modo_teste:
                try:
                    page_pdf = extract_single_page_pdf(pdf_bytes, page_num)
                    storage_info = upload_dropbox_bytes(page_pdf, f'{pasta_dropbox}/{page_filename}')
                    storage_info['status'] = 'salvo'
                    rec.drive_link = normalize_dropbox_link(storage_info.get('url', ''))
                    storage_info['url'] = rec.drive_link
                except Exception as e:
                    storage_info['status'] = f'erro_upload: {e}'
            elif match.status == 'localizado':
                storage_info['path_previsto'] = f'{pasta_dropbox.rstrip("/")}/{page_filename}'

            rec.drive_link = normalize_dropbox_link(rec.drive_link)

            plan = ExecutionPlan(receipt=rec, match=match, banco=banco)
            plan.responses['storage'] = storage_info

            if match.metodo == 'omie_pendente':
                # SP já tem planilha/Pipefy atualizados — só executa Omie
                plan.acao = 'baixar_omie_apenas'
                plan.pode_executar = True
            else:
                _decidir_execucao(plan, executar_omie, atualizar_pipefy, atualizar_spsbd, enviar_whatsapp)

            plan.omie_requests = build_omie_plan(plan, payload) if plan.match.id and executar_omie else []
            plan.pipefy_get_query     = ''                                   # preenchido depois em lote
            plan.pipefy_update_mutation = ''                                 # preenchido depois com dados do get
            plan.sheets_updates       = build_spsbd_updates(plan)           if plan.match.id and atualizar_spsbd else []
            plan.whatsapp_messages    = []  # montado depois do getCard Pipefy

            if plan.match.id:
                card_ids_para_get.append(plan.match.id)

            plans.append(plan)

        # Libera os bytes do PDF deste anexo antes de passar ao próximo
        del pdf_bytes

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

    # ── Executa os planos em produção, EM FASES ───────────────────────────────
    #
    # ⚠️ FASES, E NÃO UM COMPROVANTE POR VEZ — pedido do dono em 17/11/2026:
    #
    #   *"é uma série de tarefas repetidas, né? (…) processar todos os PDFs
    #   primeiro, identificar primeiro cada SP, depois buscar na planilha,
    #   depois lançar um único comando para atualizar a planilha (…) um único
    #   comando pra atualizar o pipefy, fazendo uma mutation e atualizar vários
    #   cards ao mesmo tempo."*
    #
    # Antes, cada comprovante percorria sozinho o caminho inteiro: Omie,
    # planilha (três idas ao Google), card do Pipefy (uma ida), WhatsApp. Trinta
    # comprovantes eram trinta vezes isso, uma espera atrás da outra.
    fases = _executar_em_fases(
        plans, card_data, payload, gc,
        executar_omie=executar_omie, atualizar_spsbd=atualizar_spsbd,
        atualizar_pipefy=atualizar_pipefy, enviar_whatsapp=enviar_whatsapp,
    ) if not modo_teste else {}

    # ── Monta output de modo_teste (preview de tudo que seria feito) ───────────
    resultado = {
        'ok': True,
        'app': 'baixabradesco',
        'modo_teste': modo_teste,
        # De onde saiu a lista de SPs candidatas. 'aba_enxuta' é o caminho
        # rápido; 'spsbd_completa' quer dizer que foi preciso ler as 52 mil
        # linhas. Fica no retorno para dar para medir, em vez de supor.
        'fonte_das_sps': estado_spsbd['fonte'],
        'resumo': {
            'comprovantes_recebidos': len(attachments),
            'paginas_processadas': len(plans),
            'localizados': sum(1 for p in plans if p.match.status == 'localizado'),
            'executaveis': sum(1 for p in plans if p.pode_executar),
            'nao_localizados': sum(1 for p in plans if p.match.status == 'nao_localizado'),
            'pendentes_validacao': sum(1 for p in plans if p.match.status == 'pendente_validacao'),
            'recusados_nao_efetivados': len(recusados),
            'duplicados_ja_baixados': len(duplicados),
            'baixas_concluidas': len(completados),
            'google_error': google_error,
        },
        'fases': fases,
        'recusados': recusados,
        'duplicados': duplicados,
        'completados': completados,
        'planos': [p.to_dict() for p in plans],
    }

    # Drena a fila de falhas pendentes. Antes ela só andava se alguém chamasse a
    # rota à mão — então uma gravação que caiu na fila ficava lá para sempre, o
    # que é a não-atualização silenciosa por outro caminho. Limite baixo para
    # não esticar a resposta; comprovante chega sempre, e a fila anda junto.
    if not modo_teste:
        resultado['fila_reprocessada'] = _drenar_fila(payload)

    # Avisa o dono do que NÃO foi baixado. Só em produção, e nunca derruba a
    # resposta: a baixa já aconteceu, o aviso é sobre o que ficou de fora.
    if not modo_teste:
        resultado['aviso'] = enviar_aviso(resultado, payload)

    # O relatório do lote inteiro, no Telegram. Ele é a resposta que ANTES ia
    # para o Make — e que o dono pediu, em 17/11/2026, para receber no Telegram,
    # já que o Make deixou de esperar: *"manda no Telegram o que você
    # encaminharia no Make."* Nunca derruba a baixa, que já aconteceu.
    if not modo_teste:
        try:
            resultado['relatorio'] = enviar_relatorio(resultado, payload)
        except Exception as e:
            resultado['relatorio'] = {'ok': False, 'erro': str(e)[:200]}

    return resultado


# ── Helpers internos ──────────────────────────────────────────────────────────


def _anotar_motivo(itens: List[Dict[str, Any]], explicacao: str) -> None:
    """Troca o motivo do empate por uma frase que diz o que fazer.

    O motivo que vem do casador é técnico ("retornou 2 candidatos"). Quem recebe
    o aviso precisa saber por que não deu e o que olhar.
    """
    for item in itens:
        m = item['match']
        item['match'] = MatchResult(
            status=m.status, metodo=m.metodo, id=m.id, sp=m.sp,
            candidatos=m.candidatos, motivo=explicacao,
        )


def resolver_empates_do_lote(analisadas: List[Dict[str, Any]]) -> int:
    """Comprovantes iguais para SPs iguais: distribui um para cada.

    O caso que motivou, em 11/09/2026: duas rescisões de R$ 5.532,57, de duas
    pessoas, ambas agendadas — e dois comprovantes de transferência de
    R$ 5.532,57. Um a um, cada comprovante via duas SPs possíveis e parava. Mas
    olhando o lote inteiro são dois pagamentos para duas SPs: dá para baixar as
    duas.

    Decisão do dono: **não importa qual comprovante fica com qual SP.** Os
    papéis são intercambiáveis — mesmo valor, mesma data, mesma conta, e o da
    transferência nem traz o nome do funcionário. O que importa é baixar.

    Três travas, e as três são necessárias:

    1. **Mesma quantidade dos dois lados.** Dois comprovantes para três SPs
       deixaria uma SP paga por engano — não se distribui.
    2. **Pagamentos comprovadamente diferentes.** Se os identificadores se
       repetem, é o mesmo comprovante mandado duas vezes, e aí seriam duas
       baixas para um pagamento só. Sem identificador, também não distribui.
    3. **Emparelhamento estável** (página na ordem, SP na ordem), para o mesmo
       lote reprocessado dar sempre o mesmo resultado.

    Devolve quantos comprovantes saíram de pendente. Só enxerga o anexo atual:
    dois comprovantes em PDFs separados não se encontram (ver HISTORICO).
    """
    grupos: Dict[Any, List[Dict[str, Any]]] = {}
    for item in analisadas:
        m = item['match']
        if m.status != 'pendente_validacao' or not m.candidatos:
            continue
        chave = frozenset(as_string(c.id) for c in m.candidatos if as_string(c.id))
        if chave:
            grupos.setdefault(chave, []).append(item)

    resolvidos = 0
    for itens in grupos.values():
        candidatos = itens[0]['match'].candidatos

        if len(itens) != len(candidatos):
            # Trava 1. Explica no motivo, senão o aviso sai técnico demais e
            # quem lê não sabe o que fazer.
            _anotar_motivo(itens, (
                f'{len(itens)} comprovante(s) de mesmo valor para {len(candidatos)} SPs '
                'de mesmo valor — quantidades diferentes, não dá para distribuir sem '
                'marcar alguma SP como paga sem ter sido.'))
            continue

        identificadores = [as_string(i['rec'].identificador) for i in itens]
        if not all(identificadores) or len(set(identificadores)) != len(identificadores):
            # Trava 2.
            _anotar_motivo(itens, (
                f'{len(itens)} comprovantes de mesmo valor, mas eles parecem ser o MESMO '
                'pagamento (identificador repetido ou ausente). Distribuir baixaria mais '
                'de uma SP para um pagamento só.'))
            continue

        itens_ordenados = sorted(itens, key=lambda i: i['page_num'])
        sps_ordenadas   = sorted(candidatos, key=lambda c: as_string(c.id))

        for item, sp in zip(itens_ordenados, sps_ordenadas):
            metodo = as_string(item['match'].metodo)
            item['match'] = MatchResult(
                status='localizado',
                metodo=f'{metodo}_distribuido_no_lote' if metodo else 'distribuido_no_lote',
                id=sp.id, sp=sp, candidatos=candidatos,
                motivo=(f'{len(itens)} comprovantes distintos para {len(candidatos)} SPs '
                        'de mesmo valor: cada comprovante ficou com uma SP.'),
            )
            resolvidos += 1

    return resolvidos


def match_omie_pendente(receipt: ExtractedReceipt, sps_pendente: Dict[str, SpRecord]) -> MatchResult:
    """Localiza SP com Omie pendente pelo valor + conta de débito.
    Usado quando a planilha já foi atualizada mas o Omie não foi baixado.
    """
    from .matcher import MatchResult
    from .utils import money_to_decimal, normalize_compact, clean_account, account_key

    valor = money_to_decimal(receipt.valor_pago)
    if valor is None:
        return MatchResult(status='nao_localizado', metodo='omie_pendente')

    conta_rec = normalize_compact(clean_account(receipt.conta_origem or ''))
    candidatos = []

    for sp in sps_pendente.values():
        base = money_to_decimal(sp.valor_total)
        if base is None:
            continue
        # Aceita valor exato ou com acréscimo BeeVale 1,5%
        from decimal import Decimal
        BEEVALE_TOL = Decimal('0.02')
        BEEVALE_MULT = Decimal('1.015')
        valor_beevale = (base * BEEVALE_MULT).quantize(Decimal('0.01'))
        if base != valor and abs(valor_beevale - valor) > BEEVALE_TOL:
            continue
        # Conta de débito deve bater
        if conta_rec:
            conta_sp = normalize_compact(clean_account(sp.conta_pagamento or ''))
            if conta_sp and conta_sp != conta_rec:
                continue
        candidatos.append(sp)

    if len(candidatos) == 1:
        return MatchResult(status='localizado', metodo='omie_pendente',
                          id=candidatos[0].id, sp=candidatos[0],
                          motivo='SP localizada como Omie pendente (Pago na planilha, sem data pgt).')
    if len(candidatos) > 1:
        return MatchResult(status='pendente_validacao', metodo='omie_pendente',
                          candidatos=candidatos,
                          motivo=f'Múltiplos candidatos Omie pendente: {len(candidatos)}')
    return MatchResult(status='nao_localizado', metodo='omie_pendente')


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


def _so_digitos(valor) -> str:
    return ''.join(c for c in as_string(valor) if c.isdigit())


def _como_numero(valor):
    """"1.234,56" ou "1234.56" ou 1234.56 -> Decimal. None quando não dá."""
    from decimal import Decimal, InvalidOperation

    texto = as_string(valor).strip()
    if not texto:
        return None
    if ',' in texto:
        texto = texto.replace('.', '').replace(',', '.')
    try:
        return Decimal(texto)
    except (InvalidOperation, ValueError):
        return None


def _precisa_alterar(titulo: dict, pedido_alterar: dict) -> bool:
    """O título no Omie já está como a alteração deixaria?

    Compara só o que a alteração mexe: o valor do documento e a conta
    corrente. Igual nos dois, não há o que alterar — e o passo é pulado.

    ⚠️ NA DÚVIDA, ALTERA. Se a consulta não trouxe os campos (título que veio
    incompleto, resposta em formato novo), esta função responde True e o
    comportamento volta a ser o de antes. Deixar de alterar um título que
    PRECISA seria baixar com o valor errado — isso é pior do que uma alteração
    a mais."""
    if not titulo:
        return True
    try:
        param = (pedido_alterar.get('param') or [{}])[0]
    except (AttributeError, IndexError, TypeError):
        return True

    valor_novo = _como_numero(param.get('valor_documento'))
    valor_atual = _como_numero(titulo.get('valor_documento'))
    if valor_novo is None or valor_atual is None:
        return True
    if abs(valor_novo - valor_atual) > _como_numero('0.01'):
        return True

    conta_nova = _so_digitos(param.get('id_conta_corrente'))
    conta_atual = _so_digitos(titulo.get('id_conta_corrente')
                              or titulo.get('codigo_conta_corrente'))
    if not conta_nova or not conta_atual:
        return True
    return conta_nova != conta_atual


def _executar_sequencia_omie(plan: ExecutionPlan, payload: dict) -> List[dict]:
    """Consulta → Altera (se necessário) → Baixa. Retorna log de cada step.
    """
    resultados = []
    titulo_consultado: dict = {}

    # ⚠️ SEM CREDENCIAL DO OMIE, NÃO COMEÇA — 16/09/2026.
    #
    # Pergunta do dono, e é a pergunta certa: *"só não compreendo por que o
    # baixabradesco no método anterior funciona e via Análise não."*
    #
    # O robô é o mesmo; o que muda é de onde vem a chave de acesso do Omie. O
    # Make manda `app_key` e `app_secret` DENTRO do pedido. O pedido que sai do
    # Análise de SPs não manda — ele conta com as variáveis de ambiente
    # (`OMIE_BWS_APP_KEY` / `OMIE_BWS_APP_SECRET`) do serviço.
    #
    # Se essas variáveis não estiverem no Render, o pedido sai com a chave
    # VAZIA e o Omie responde, com essas palavras:
    #
    #     A chave de acesso não está preenchida ou não é válida.
    #
    # "Chave de acesso", no Omie, é a credencial da API — não é a chave do
    # título. A mensagem parecia falar do título, e por isso a investigação
    # olhou para o lado errado durante dois dias.
    #
    # Agora isto para ANTES de mandar qualquer coisa, e diz o que fazer.
    from .omie import (NOMES_APP_KEY, NOMES_APP_SECRET,
                       credentials_from_payload)

    app_key, app_secret = credentials_from_payload(payload)
    if not app_key or not app_secret:
        # DIZ TODOS OS NOMES ACEITOS, e não um só: o servidor pode ter a
        # variável com qualquer um dos apelidos, e quem vai cadastrar precisa
        # saber quais valem.
        falta = ' e '.join(
            ' ou '.join(nomes)
            for nomes, valor in ((NOMES_APP_KEY, app_key),
                                 (NOMES_APP_SECRET, app_secret))
            if not valor)
        return [{'step': 'abort_sem_credencial',
                 'motivo': ('As credenciais do Omie não chegaram ao robô: '
                            f'falta {falta} no servidor. Nada foi alterado nem '
                            'pago. Cadastre no Render e reenvie o comprovante.')}]

    for req in plan.omie_requests:
        # ⚠️ A ALTERAÇÃO SÓ ACONTECE SE ALGO DIVERGE — 16/09/2026.
        #
        # O que o Omie respondeu, pela boca dele, num comprovante do dono:
        #
        #     Falha ao alterar título. Baixa cancelada.
        #     O Omie respondeu: A chave de acesso não está preenchida ou não
        #     é válida.
        #
        # O passo de alterar rodava SEMPRE, apoiado num comentário que dizia
        # "verificação simplificada: tenta sempre, Omie idempotente" — uma
        # suposição que nunca foi verificada, e que é falsa. Quando o Omie
        # recusa a alteração, a baixa é CANCELADA. Ou seja: um passo que na
        # maioria das vezes não precisava acontecer estava impedindo o que
        # precisava.
        #
        # É exatamente o que o dono cobrou em 14/09: *"se eu estou mandando
        # pra baixar é pra baixar"*.
        #
        # Agora o título consultado manda: se o valor e a conta já estão como
        # devem ficar, a alteração é PULADA — e a baixa segue.
        if req['step'] == 'alterar_se_necessario' and not _precisa_alterar(
                titulo_consultado, req['request']):
            resultados.append({'step': 'alterar_nao_precisou',
                               'motivo': 'O título já está com o valor e a conta certos no Omie.'})
            continue

        resp = execute_omie(req['request'])
        resultados.append({'step': req['step'], 'response': resp})

        if req['step'] == 'consultar':
            body = resp.get('body') or {}
            if as_string(body.get('status_titulo')).upper() == 'PAGO':
                resultados.append({'step': 'skip', 'motivo': 'Título já consta PAGO no Omie.'})
                plan._omie_ja_pago = True
                break
            # ⚠️ CONSULTA QUE NÃO DEU CERTO INTERROMPE, QUALQUER QUE SEJA O
            # MOTIVO. Antes só interrompia com "nao_encontrado" no faultcode ou
            # HTTP 500 — e o Omie responde 200 com `faultstring` em vários
            # casos. Esses passavam adiante e iam alterar um título que ninguém
            # confirmou que existe. Se não deu para confirmar o título, não se
            # mexe nele e não se paga.
            if not resp.get('ok'):
                faultstring = as_string(body.get('faultstring') or '')
                resultados.append({
                    'step': 'abort',
                    'motivo': ('Não consegui confirmar o título no Omie'
                               + (f' — o Omie respondeu: {faultstring}'
                                  if faultstring else '')
                               + '. Nada foi alterado nem pago.')})
                break
            titulo_consultado = body

        if req['step'] == 'alterar_se_necessario':
            if not resp.get('ok'):
                resultados.append({'step': 'abort_apos_alterar', 'motivo': 'Falha ao alterar título. Baixa cancelada.'})
                break

        if req['step'] == 'baixar' and not resp.get('ok'):
            resultados.append({'step': 'erro_baixa', 'motivo': 'Falha ao lançar pagamento no Omie.'})

    # Se houve falha temporária ou baixa não confirmada, registra na fila inteligente.
    try:
        falhou = any((not (r.get('response') or {}).get('ok')) for r in resultados if r.get('response'))
        erro_baixa = any(r.get('step') in {'erro_baixa', 'abort_apos_alterar'} for r in resultados)
        if falhou or erro_baixa:
            plan.responses['fila_omie'] = enqueue_failure(plan, 'omie', 'omie_erro', str(resultados), payload)
    except Exception:
        pass

    return resultados


# Quanto o lote de comprovantes drena de carona. Números pequenos de propósito:
# a resposta ao Make tem 300 segundos de teto e o comprovante deste lote é a
# prioridade — a fila é sobre lotes passados.
LIMITES_LOTE = {'omie': 3, 'sheets': 3, 'pipefy': 2, 'zapi': 2}


def _executar_em_fases(plans: List[ExecutionPlan], card_data: dict, payload: dict, gc,
                       *, executar_omie: bool, atualizar_spsbd: bool,
                       atualizar_pipefy: bool, enviar_whatsapp: bool) -> Dict[str, Any]:
    """Executa o lote por etapa, agrupando o que o sistema do outro lado aceita.

    A ORDEM É A MESMA DE SEMPRE, e isso não é detalhe: o Omie manda na planilha
    e no card. Só muda que agora cada etapa trata o lote inteiro de uma vez, em
    vez de o lote inteiro passar pelas quatro etapas um comprovante por vez.

    O que dá para agrupar e o que não dá:

    · **Omie — NÃO dá.** A API de contas a pagar baixa um título por chamada.
      Fica um de cada vez, e é o que sobra de demora. (As chamadas também não
      são disparadas em paralelo de propósito: o Omie limita pedidos por
      segundo por chave, e uma recusa dele é o único ponto desta rotina onde o
      engano custa dinheiro.)
    · **Impressões digitais — dá.** Uma gravação para todas.
    · **Planilha — dá.** Três idas ao Google para o lote inteiro.
    · **Pipefy — dá.** Uma mutation para vários cards.
    · **WhatsApp — fica por plano**, porque é mensagem para pessoas diferentes.
    """
    executaveis = [p for p in plans if p.pode_executar]
    saida: Dict[str, Any] = {
        'executaveis': len(executaveis), 'omie_confirmados': 0,
        'planilha': {}, 'pipefy': {}, 'impressoes': {},
    }
    if not executaveis:
        return saida

    for plan in executaveis:
        card_info = card_data.get(str(plan.match.id)) if plan.match.id else None
        if card_info:
            plan.responses['pipefy_card_info'] = card_info

    # ── Fase 1: Omie, um título por chamada ───────────────────────────────────
    confirmados: List[ExecutionPlan] = []
    para_registrar: List[tuple] = []

    for plan in executaveis:
        if plan.omie_requests and executar_omie:
            plan.responses['omie'] = _executar_sequencia_omie(plan, payload)

        _omie_ok = any(
            s.get('step') == 'baixar' and s.get('response', {}).get('ok')
            for s in (plan.responses.get('omie') or [])
        )
        _omie_ja_pago = getattr(plan, '_omie_ja_pago', False)
        if (_omie_ok or _omie_ja_pago) and gc:
            para_registrar.append((
                plan.receipt.fingerprint,
                plan.match.id if plan.match else '',
                plan.receipt.valor_pago,
                plan.receipt.data_pagamento,
                plan.receipt.filename,
            ))

        # ⚠️ O OMIE MANDA NA PLANILHA E NO CARD — 16/09/2026.
        #
        # Cobrança do dono em 14/09: *"baixam na planilha, mas não baixam no
        # Omie. Não tem sentido. (…) Se eu estou mandando pra baixar é pra
        # baixar, tem que baixar as duas coisas."*
        #
        # A planilha e o card eram marcados como pagos INDEPENDENTE do que o
        # Omie tivesse respondido. Quando o Omie recusava, a SP ficava "paga"
        # em dois lugares e em aberto no terceiro — e ninguém descobria, porque
        # a tela dizia que tinha dado certo.
        #
        # Agora: se a baixa no Omie era para acontecer e NÃO se confirmou, não
        # se marca nada. É melhor a SP continuar aparecendo como a pagar (e
        # alguém refazer) do que sumir da fila sem ter sido paga no sistema que
        # a contabilidade lê.
        #
        # "Já estava paga no Omie" conta como confirmação: nesse caso a planilha
        # e o card É QUE estão atrasados, e é para atualizá-los que isto existe.
        devia_baixar = bool(plan.omie_requests and executar_omie)
        omie_confirmou = bool(_omie_ok or _omie_ja_pago)
        if devia_baixar and not omie_confirmou:
            plan.responses['nao_marcou'] = (
                'A planilha e o card NÃO foram marcados: o Omie não '
                'confirmou a baixa. Resolva no Omie e reenvie o '
                'comprovante — ele não baixa duas vezes.')
            continue

        confirmados.append(plan)

    saida['omie_confirmados'] = len(confirmados)

    # ── Fase 2: impressões digitais, UMA gravação ─────────────────────────────
    #
    # Em segundo plano como sempre foi: ela protege contra baixa repetida num
    # lote FUTURO, e segurar a resposta por ela não traz nada.
    if para_registrar and gc:
        saida['impressoes'] = {'enviadas': len(para_registrar)}
        _registrar_impressoes_em_segundo_plano(gc, list(para_registrar))

    # ── Fase 3: planilha, UMA gravação para o lote ────────────────────────────
    if atualizar_spsbd:
        alvos = [p for p in confirmados if p.sheets_updates]
        if alvos:
            saida['planilha'] = _gravar_planilha_em_lote(alvos, payload)

    # ── Fase 4: Pipefy, uma mutation para vários cards ────────────────────────
    if atualizar_pipefy:
        alvos = [p for p in confirmados if plan_tem_card(p)]
        if alvos:
            saida['pipefy'] = _atualizar_cards_em_lote(alvos, card_data, payload)

    # ── Fase 5: WhatsApp, por destinatário ────────────────────────────────────
    if enviar_whatsapp:
        for plan in confirmados:
            try:
                plan.whatsapp_messages = build_whatsapp_messages(plan, payload)
                if plan.whatsapp_messages:
                    zapi_resp = _executar_zapi(plan.whatsapp_messages, payload)
                    plan.responses['zapi'] = zapi_resp
                    if any(not r.get('ok') for r in zapi_resp):
                        plan.responses['fila_zapi'] = enqueue_failure(
                            plan, 'zapi', 'zapi_erro', str(zapi_resp), payload)
                else:
                    plan.responses['zapi'] = [{'skipped': True, 'reason': 'sem_telefone_ou_campos_pipefy'}]
            except Exception as e:
                # WhatsApp nunca pode derrubar a baixa; registra no output e segue.
                plan.responses['zapi'] = [{'ok': False, 'status': 0, 'error': str(e)}]
                plan.responses['fila_zapi'] = enqueue_failure(plan, 'zapi', 'zapi_exception', str(e), payload)
                plan.whatsapp_messages = []

    return saida


def plan_tem_card(plan: ExecutionPlan) -> bool:
    return bool(plan.match and plan.match.id)


def _registrar_impressoes_em_segundo_plano(gc, linhas: List[tuple]) -> None:
    def _rodar():
        try:
            registrar_fingerprints(gc, linhas)
        except Exception:
            pass
    threading.Thread(target=_rodar, daemon=True).start()


def _gravar_planilha_em_lote(plans: List[ExecutionPlan], payload: dict) -> Dict[str, Any]:
    """Grava a SPsBD de TODOS os comprovantes confirmados numa conversa só.

    Mantém o que importa do caminho antigo: a gravação é CONFERIDA e acontece
    antes de a resposta sair, e o comprovante que não gravou vai para a fila —
    só ele, não o lote inteiro.
    """
    mapa: List[ExecutionPlan] = []
    updates: List[dict] = []
    for plan in plans:
        for upd in plan.sheets_updates:
            mapa.append(plan)
            updates.append(upd)

    if not updates:
        return {'ok': True, 'gravados': 0}

    def _rodar(lista):
        try:
            return execute_spsbd_updates(lista)
        except Exception as e:
            return {'ok': False, 'gravados': 0, 'erros': [str(e)[:200]],
                    'por_update': [{'ok': False, 'erro': str(e)[:200]} for _ in lista]}

    resultado = _rodar(updates)
    por_update = list(resultado.get('por_update') or
                      [{'ok': False, 'erro': 'sem detalhe'} for _ in updates])
    por_update += [{'ok': False, 'erro': 'sem detalhe'}] * (len(updates) - len(por_update))

    # Segunda tentativa só para o que faltou. Quase sempre é cota do Google,
    # que é por minuto — esperar dois segundos resolve a resposta parcial.
    faltando = [i for i, r in enumerate(por_update) if not r.get('ok')]
    if faltando:
        time.sleep(2)
        segunda = _rodar([updates[i] for i in faltando])
        detalhe = segunda.get('por_update') or []
        for pos, i in enumerate(faltando):
            if pos < len(detalhe):
                por_update[i] = detalhe[pos]

    por_plano: Dict[int, List[dict]] = {}
    for i, plan in enumerate(mapa):
        por_plano.setdefault(id(plan), []).append(por_update[i])

    gravados = 0
    for plan in plans:
        detalhes = por_plano.get(id(plan)) or []
        ok = bool(detalhes) and all(d.get('ok') for d in detalhes)
        plan.responses['sheets'] = {
            'ok': ok,
            'gravados': sum(1 for d in detalhes if d.get('ok')),
            'erros': [d.get('erro') for d in detalhes if not d.get('ok') and d.get('erro')],
            'em_lote': True,
            'tentativas': 2 if faltando else 1,
        }
        if ok:
            gravados += 1
            continue
        try:
            plan.responses['fila_sheets'] = enqueue_failure(
                plan, 'sheets', 'sheets_erro', str(detalhes), payload)
        except Exception as e:
            plan.responses['fila_sheets'] = {'ok': False, 'erro': str(e)[:200]}

    return {'ok': gravados == len(plans), 'gravados': gravados,
            'comprovantes': len(plans), 'chamadas_poupadas': max(0, 3 * len(plans) - 3)}


def _atualizar_cards_em_lote(plans: List[ExecutionPlan], card_data: dict,
                             payload: dict) -> Dict[str, Any]:
    """Atualiza vários cards do Pipefy por mutation.

    O tamanho do pedaço é modesto de propósito: cada card já gasta cerca de dez
    apelidos dentro da mutation, e documento grande demais o Pipefy recusa
    inteiro. Ajustável por BAIXABRADESCO_PIPEFY_LOTE.
    """
    try:
        tamanho = int(os.getenv('BAIXABRADESCO_PIPEFY_LOTE', '') or 5)
    except Exception:
        tamanho = 5
    tamanho = max(1, tamanho)

    atualizados = 0
    recusados = 0
    chamadas = 0

    for inicio in range(0, len(plans), tamanho):
        pedaco = plans[inicio:inicio + tamanho]
        itens = [(p, card_data.get(str(p.match.id))) for p in pedaco]
        try:
            texto, mapa = build_update_cards_mutation(itens)
        except Exception as e:
            texto, mapa = '', {}
            for p in pedaco:
                p.responses['pipefy'] = [{'ok': False, 'error': str(e)}]
        if not texto:
            for p in pedaco:
                recusados += 1
                _enfileirar_pipefy(p, 'pipefy_sem_mutation', 'mutation vazia', payload)
            continue

        try:
            resposta = execute_graphql(texto)
        except Exception as e:
            resposta = {'ok': False, 'status': 0, 'body': {}, 'error': str(e)}
        chamadas += 1

        ruins = cards_recusados(resposta, mapa)
        for idx, plan in enumerate(pedaco):
            if idx in ruins:
                recusados += 1
                plan.responses['pipefy'] = [{'ok': False, 'em_lote': True,
                                             'status': resposta.get('status'),
                                             'erro': str(resposta.get('body') or
                                                         resposta.get('error'))[:300]}]
                _enfileirar_pipefy(plan, 'pipefy_erro', str(resposta)[:300], payload)
            else:
                atualizados += 1
                plan.responses['pipefy'] = [{'ok': True, 'em_lote': True,
                                             'status': resposta.get('status')}]

    return {'ok': recusados == 0, 'atualizados': atualizados,
            'recusados': recusados, 'chamadas': chamadas,
            'chamadas_poupadas': max(0, len(plans) - chamadas)}


def _enfileirar_pipefy(plan: ExecutionPlan, tipo: str, mensagem: str, payload: dict) -> None:
    """A fila reprocessa UM card por vez, e para isso precisa da mutation dele."""
    try:
        plan.pipefy_update_mutation = build_update_card_mutation(
            plan, plan.responses.get('pipefy_card_info'))
    except Exception:
        plan.pipefy_update_mutation = ''
    try:
        plan.responses['fila_pipefy'] = enqueue_failure(plan, 'pipefy', tipo, mensagem, payload)
    except Exception as e:
        plan.responses['fila_pipefy'] = {'ok': False, 'erro': str(e)[:200]}


def _drenar_fila(payload: dict, limites: dict | None = None) -> dict:
    """Reprocessa algumas pendências da fila, junto com o lote.

    ⚠️ Passa pelo `drenar_por_etapa`, e isso corrige dois defeitos de uma vez.
    Antes esta função chamava o reprocessamento direto, sem etapa e sem
    restrição, e por isso:

    1. **Pegava as cinco pendências mais antigas da fila, quaisquer que fossem.**
       Com 1.943 avisos de junho na frente de 238 baixas, o dinheiro ficava para
       o fim.
    2. **Marcava cinco avisos antigos como descartados a cada lote** — ou seja, o
       sistema ia limpando sozinho o acumulado que foi dito ao dono que ele não
       tocaria. O cron já tinha esse cuidado; este caminho, não.

    Nunca levanta erro: a baixa deste lote já aconteceu, e a fila é sobre
    lotes passados.
    """
    try:
        from .fila import drenar_por_etapa
        return drenar_por_etapa(limites or LIMITES_LOTE, payload)
    except Exception as e:
        return {'ok': False, 'erro': str(e)[:200]}


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
        # Download em streaming com teto de tamanho — evita puxar um arquivo
        # gigante inteiro pra RAM de uma vez (mesma proteção do pdf_processor).
        with requests.get(att.url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            chunks = []
            total = 0
            for chunk in resp.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_ATTACHMENT_BYTES:
                    raise ValueError(f'Comprovante excede o limite de {MAX_ATTACHMENT_BYTES // (1024*1024)} MB: {att.filename}')
                chunks.append(chunk)
            return b''.join(chunks)
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