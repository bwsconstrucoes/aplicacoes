# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from .sheets import get_gc, SPS_SHEET_ID, execute_spsbd_updates
from .utils import as_string
from .omie import execute_omie, omie_body, credentials_from_payload
from .pipefy import execute_graphql
from .zapi import resolve_zapi_auth, validate_zapi_auth, send_messages_batch

FILA_SHEET_NAME = 'BaixaBradescoFila'

HEADERS = [
    'Data Registro',
    'Status',
    'Tentativas',
    'Próxima Tentativa',
    'Tipo Falha',
    'ID SP',
    'Código Integração',
    'Arquivo',
    'Página',
    'Fingerprint',
    'Link Comprovante',
    'Etapa',
    'Mensagem Erro',
    'Payload Resumido',
    'Última Execução',
]

STATUS_PENDENTE = 'PENDENTE'
STATUS_CONCLUIDO = 'CONCLUIDO'
STATUS_FALHOU = 'FALHOU'

# A:L é tudo que o filtro da fila precisa ler. A mensagem de erro (M) e o
# payload (N) ficam fora de propósito: são as duas colunas grandes.
FAIXA_CONTROLE = 'A2:L'
COLS_CONTROLE = 12
COL_PAYLOAD = 'N'

# Aviso de falha enfileirado há mais de três dias não é mais aviso, é confusão:
# avisa de um problema que já foi resolvido na mão, e vai para dois celulares.
# Ao drenar fila velha ele é descartado com o motivo escrito, a menos que o
# pedido traga `reenviar_avisos_antigos: true`.
DIAS_AVISO_UTIL = 3


def now_str() -> str:
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')


def next_try(minutes: int = 10) -> str:
    return (datetime.now() + timedelta(minutes=minutes)).strftime('%d/%m/%Y %H:%M:%S')


def ensure_fila_sheet(gc=None):
    gc = gc or get_gc()
    ss = gc.open_by_key(SPS_SHEET_ID)
    try:
        ws = ss.worksheet(FILA_SHEET_NAME)
    except Exception:
        ws = ss.add_worksheet(title=FILA_SHEET_NAME, rows=1000, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option='USER_ENTERED')
        return ws

    # Lê SÓ a linha do cabeçalho. Antes era `get_all_values()`, ou seja: toda
    # vez que alguém enfileirava uma falha ou reprocessava a fila, a aba
    # inteira vinha pela rede — com o JSON do payload em cada linha. Com a aba
    # passando de duas mil linhas isso virou megabytes por chamada, que é
    # exatamente o que CONTEXTO.md §3.7 proíbe.
    try:
        topo = ws.get('A1:O1') or []
    except Exception:
        # Leitura falhou: NÃO mexe no cabeçalho. Escrever por cima do que não se
        # conseguiu ler é como a aba ganhou linhas de lixo (ver abaixo).
        return ws
    atuais = topo[0] if topo else []
    atuais = (list(atuais) + [''] * len(HEADERS))[:len(HEADERS)]
    if atuais != HEADERS:
        # ⚠️ `update('A1:O1')`, nunca `append_row`. Em 08/10/2026 a contagem do
        # dono mostrou três linhas com Status = "STATUS" no meio da fila: eram
        # cabeçalhos escritos por `append_row`, que acrescenta no FIM da aba, não
        # na linha 1. Bastava a leitura de `A1:O1` voltar vazia uma vez — o que
        # acontece num soluço de rede — para nascer uma linha de lixo. Defeito
        # meu, introduzido junto com a leitura limitada.
        ws.update('A1:O1', [HEADERS], value_input_option='USER_ENTERED')
    return ws


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({'raw': str(obj)}, ensure_ascii=False)


def _plan_payload_resumido(plan, etapa: str) -> Dict[str, Any]:
    rec = plan.receipt
    banco = plan.banco
    match = plan.match
    codigo_integracao = ''
    if match and match.sp and match.sp.codigo_integracao_omie:
        codigo_integracao = match.sp.codigo_integracao_omie
    elif match and match.id:
        codigo_integracao = 'Int' + as_string(match.id)

    return {
        'etapa': etapa,
        'sp_id': as_string(match.id if match else ''),
        'codigo_integracao': codigo_integracao,
        'arquivo': as_string(rec.filename),
        'pagina': rec.page,
        'fingerprint': as_string(rec.fingerprint),
        'link_comprovante': as_string(rec.drive_link),
        'data_pagamento': as_string(rec.data_pagamento),
        'valor_pago': as_string(rec.valor_pago),
        'acrescimos': as_string(rec.acrescimos or '0,00'),
        'codigo_conta_omie': as_string(banco.codigo_omie if banco else ''),
        'pipefy_update_mutation': as_string(getattr(plan, 'pipefy_update_mutation', '') or ''),
        'sheets_updates': getattr(plan, 'sheets_updates', []) or [],
        'whatsapp_messages': getattr(plan, 'whatsapp_messages', []) or [],
    }


def enqueue_failure(plan, etapa: str, tipo_falha: str, mensagem: str, payload: Optional[dict] = None, retry_minutes: int = 10) -> dict:
    """Registra uma falha temporária na fila. Nunca levanta exceção para não derrubar o fluxo principal."""
    try:
        ws = ensure_fila_sheet()
        rec = plan.receipt
        match = plan.match
        resumido = _plan_payload_resumido(plan, etapa)
        row = [
            now_str(),
            STATUS_PENDENTE,
            0,
            next_try(retry_minutes),
            tipo_falha,
            as_string(match.id if match else ''),
            as_string(resumido.get('codigo_integracao')),
            as_string(rec.filename),
            as_string(rec.page),
            as_string(rec.fingerprint),
            as_string(rec.drive_link),
            etapa,
            as_string(mensagem)[:1000],
            _safe_json(resumido),
            '',
        ]
        ws.append_row(row, value_input_option='USER_ENTERED')
        return {'ok': True, 'status': 'enfileirado', 'etapa': etapa, 'tipo_falha': tipo_falha}
    except Exception as e:
        return {'ok': False, 'status': 'erro_ao_enfileirar', 'error': str(e), 'etapa': etapa, 'tipo_falha': tipo_falha}


def _parse_dt_br(texto: str) -> Optional[datetime]:
    texto = as_string(texto)
    for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M'):
        try:
            return datetime.strptime(texto, fmt)
        except Exception:
            pass
    return None


def _coluna(faixas: List[Any], indice: int) -> List[str]:
    """Transforma uma faixa de coluna única do batch_get numa lista simples."""
    try:
        bruto = faixas[indice] or []
    except Exception:
        return []
    return [as_string(c[0]) if c else '' for c in bruto]


def _candidatos(ws, limite: int, somente_vencidos: bool, aceitos: set,
                etapas: Optional[set] = None,
                aviso_minimo: Optional[datetime] = None) -> List[Any]:
    """Varre apenas as colunas de controle (A:L) e devolve as linhas que servem.

    O payload de cada linha (coluna N) é um JSON que pode ter alguns kB. Ler a
    aba toda para achar cinco linhas custava a aba toda; aqui o filtro roda
    sobre as colunas leves e o payload é buscado depois, só do que foi escolhido.
    """
    try:
        valores = ws.get(FAIXA_CONTROLE) or []
    except Exception:
        return []
    agora = datetime.now()
    escolhidos = []
    for idx, row in enumerate(valores, start=2):
        row = list(row) + [''] * (COLS_CONTROLE - len(row))
        if as_string(row[1]).upper() not in aceitos:
            continue
        if as_string(row[1]).strip().upper() == 'STATUS':
            # Linha de cabeçalho perdida no meio dos dados. Não é pendência.
            continue
        etapa_linha = as_string(row[11]).lower()
        if etapas is not None and etapa_linha not in etapas:
            continue
        if aviso_minimo is not None and etapa_linha == 'zapi':
            # ⚠️ O descarte precisa acontecer AQUI, na escolha das linhas, e não
            # depois. A fila é lida em ordem: com 1.943 avisos antigos na frente,
            # pedir "dez avisos" devolvia sempre os dez mais velhos, que seriam
            # pulados por idade — e o aviso de ontem, que alguém ainda quer
            # receber, nunca era alcançado. A fila entupia com o que ela mesma
            # ia descartar.
            registro = _parse_dt_br(row[0])
            if registro and registro < aviso_minimo:
                continue
        if somente_vencidos:
            dt = _parse_dt_br(row[3])
            if dt and dt > agora:
                continue
        escolhidos.append((idx, row))
        if len(escolhidos) >= limite:
            break
    return escolhidos


def _buscar_payloads(ws, linhas: List[int]) -> Dict[int, str]:
    """Busca a coluna N só das linhas escolhidas, em blocos de 100 faixas."""
    saida: Dict[int, str] = {}
    for inicio in range(0, len(linhas), 100):
        bloco = linhas[inicio:inicio + 100]
        try:
            resposta = ws.batch_get([f'{COL_PAYLOAD}{n}' for n in bloco]) or []
        except Exception:
            resposta = []
        for i, n in enumerate(bloco):
            texto = ''
            if i < len(resposta):
                celulas = resposta[i] or []
                if celulas and celulas[0]:
                    texto = as_string(celulas[0][0])
            saida[n] = texto
    return saida


def listar_pendentes(gc=None, limite: int = 20, somente_vencidos: bool = True,
                     incluir_falhados: bool = False,
                     etapas: Optional[set] = None,
                     aviso_minimo: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Itens da fila a reprocessar.

    `etapas`: quando informado, só traz essas etapas. Serve para drenar o que
    é dinheiro (`omie`) antes do que é recado (`zapi`) — e sem um depender do
    outro.

    `aviso_minimo`: descarta na ESCOLHA os avisos registrados antes dessa data,
    em vez de trazê-los para serem pulados depois. Sem isso, 1.943 avisos velhos
    na frente da fila fazem com que o aviso de ontem nunca seja alcançado.

    `incluir_falhados`: traz também os que esgotaram as cinco tentativas e
    foram marcados FALHOU. Eles NUNCA voltavam sozinhos — ficavam abandonados na
    planilha, o que é a não-atualização silenciosa com outro nome. O
    reprocessamento automático não os inclui (de nada serve insistir de minuto
    em minuto no que já falhou cinco vezes); quem pede explicitamente, inclui.
    """
    ws = ensure_fila_sheet(gc)
    aceitos = {STATUS_PENDENTE}
    if incluir_falhados:
        aceitos.add(STATUS_FALHOU)
    escolhidos = _candidatos(ws, int(limite), bool(somente_vencidos), aceitos,
                             etapas, aviso_minimo)
    if not escolhidos:
        return []
    payloads = _buscar_payloads(ws, [idx for idx, _ in escolhidos])
    out = []
    for idx, row in escolhidos:
        d: Dict[str, Any] = {HEADERS[c]: row[c] for c in range(COLS_CONTROLE)}
        d['Mensagem Erro'] = ''
        d['Payload Resumido'] = payloads.get(idx, '')
        d['Última Execução'] = ''
        d['_row_number'] = idx
        out.append(d)
    return out


def _update_row(ws, row_number: int, status: str, tentativas: int, mensagem: str = '', retry_minutes: int = 10):
    proxima = '' if status == STATUS_CONCLUIDO else next_try(retry_minutes)
    # Uma chamada em vez de duas: a cota de escrita do Sheets é por minuto, e
    # drenar uma fila grande gastava o dobro do necessário só nisto.
    ws.batch_update([
        {'range': f'B{row_number}:D{row_number}', 'values': [[status, tentativas, proxima]]},
        {'range': f'M{row_number}:O{row_number}', 'values': [[as_string(mensagem)[:1000], '', now_str()]]},
    ], value_input_option='USER_ENTERED')


def _marcar_em_lote(ws, marcas: List[Dict[str, Any]]) -> int:
    """Marca várias linhas numa chamada só.

    Existe por um número concreto: em 08/10/2026 a fila tinha 1.943 avisos
    antigos para descartar. Uma chamada por linha seriam 1.943 escritas, e a
    cota do Google é por minuto e compartilhada com o ERP e o painel — levaria
    meia hora travando os outros. Em blocos de 50 linhas (100 faixas), são 39
    chamadas.
    """
    gravadas = 0
    for inicio in range(0, len(marcas), 50):
        bloco = marcas[inicio:inicio + 50]
        dados = []
        for m in bloco:
            n = m['row']
            proxima = '' if m['status'] == STATUS_CONCLUIDO else next_try(10)
            dados.append({'range': f'B{n}:D{n}',
                          'values': [[m['status'], m['tentativas'], proxima]]})
            dados.append({'range': f'M{n}:O{n}',
                          'values': [[as_string(m.get('mensagem'))[:1000], '', now_str()]]})
        try:
            ws.batch_update(dados, value_input_option='USER_ENTERED')
            gravadas += len(bloco)
        except Exception:
            # Bloco que não gravou fica PENDENTE na planilha, que é o estado
            # verdadeiro. Não se finge que gravou.
            pass
    return gravadas


def _request_omie(call: str, param: dict, payload: dict) -> dict:
    body = omie_body(call, param, payload)
    return execute_omie(body)


def _enfileirar_etapa(item: Dict[str, Any], etapa: str, tipo_falha: str,
                      mensagem: str) -> dict:
    """Cria uma pendência nova para uma etapa, a partir de um item da fila.

    Serve para o caso em que a baixa no Omie é concluída na nova tentativa e
    uma etapa SEGUINTE falha: ela precisa de linha própria, senão desaparece.
    """
    try:
        ws = ensure_fila_sheet()
        ws.append_row([
            now_str(), STATUS_PENDENTE, 0, next_try(10), tipo_falha,
            as_string(item.get('ID SP')),
            as_string(item.get('Código Integração')),
            as_string(item.get('Arquivo')),
            as_string(item.get('Página')),
            as_string(item.get('Fingerprint')),
            as_string(item.get('Link Comprovante')),
            etapa,
            as_string(mensagem)[:1000],
            as_string(item.get('Payload Resumido')),
            '',
        ], value_input_option='USER_ENTERED')
        return {'ok': True, 'etapa': etapa}
    except Exception as e:
        return {'ok': False, 'erro': str(e)[:200], 'etapa': etapa}


def _concluir_plano(resumo: Dict[str, Any], item: Dict[str, Any],
                    payload: dict) -> dict:
    """Depois que o título está pago no Omie, ainda falta o resto do plano.

    ⚠️ Era o furo que o dono relatou em 08/10/2026: *"tenho várias baixas que não
    aconteceram na planilha"*. A fila tinha 182 pendências de `omie` e **zero**
    de `sheets` — e a razão é que a baixa falha no Omie ANTES de a planilha ser
    gravada. Então a planilha nunca foi escrita, e nunca houve pendência de
    planilha para enfileirar. A nova tentativa resolvia o Omie, marcava
    `CONCLUIDO` e **deixava a planilha desatualizada para sempre**.

    Agora a nova tentativa termina o serviço: grava a planilha e move o cartão.

    A gravação da planilha é **obrigatória** para dar o item por concluído: é o
    registro do pagamento, e é o que o dono lê. Repetir é seguro — a consulta ao
    Omie no início devolve `ja_pago` e não lança nada de novo, e a regravação
    escreve os mesmos valores nas mesmas células.

    O cartão do Pipefy **não** bloqueia: se falhar, ganha pendência própria, para
    não segurar um registro de pagamento que já está correto nos dois sistemas.
    """
    etapas: Dict[str, Any] = {}

    updates = resumo.get('sheets_updates') or []
    if updates:
        try:
            r = execute_spsbd_updates(updates)
        except Exception as e:
            r = {'ok': False, 'erros': [str(e)[:200]]}
        etapas['sheets'] = r
        if not r.get('ok'):
            return {'ok': False, 'erro': 'falha_gravar_planilha', 'etapas': etapas}

    mutation = as_string(resumo.get('pipefy_update_mutation'))
    if mutation:
        if not os.getenv('PIPEFY_API_TOKEN', '').strip():
            etapas['pipefy'] = {'ok': False, 'erro': 'credenciais_pipefy_ausentes'}
            etapas['pipefy_enfileirado'] = _enfileirar_etapa(
                item, 'pipefy', 'pipefy_erro',
                'Omie e planilha concluídos; falta o cartão (sem PIPEFY_API_TOKEN).')
        else:
            try:
                r = execute_graphql(mutation)
            except Exception as e:
                r = {'ok': False, 'erro': str(e)[:200]}
            etapas['pipefy'] = r
            if not r.get('ok'):
                etapas['pipefy_enfileirado'] = _enfileirar_etapa(
                    item, 'pipefy', 'pipefy_erro',
                    'Omie e planilha concluídos; o cartão não moveu.')

    return {'ok': True, 'etapas': etapas}


def _retry_omie(item: Dict[str, Any], payload: dict) -> dict:
    # Credencial primeiro, e antes de QUALQUER chamada: sem ela a consulta
    # falha e, pior, a falha seria contada como tentativa gasta. Com 238 baixas
    # na fila, uma drenagem sem credencial apagaria as 238 em cinco passadas.
    try:
        app_key, app_secret = credentials_from_payload(payload)
    except Exception:
        app_key = app_secret = ''
    if not app_key or not app_secret:
        return {'ok': False, 'erro': 'credenciais_omie_ausentes'}

    resumo = json.loads(item.get('Payload Resumido') or '{}')
    codigo = as_string(resumo.get('codigo_integracao'))
    if not codigo:
        return {'ok': False, 'erro': 'codigo_integracao_ausente'}

    consulta = _request_omie('ConsultarContaPagar', {'codigo_lancamento_integracao': codigo}, payload)
    body = consulta.get('body') or {}
    if as_string(body.get('status_titulo')).upper() == 'PAGO':
        # Já pago no Omie — pela conciliação bancária diária, é o caso comum.
        # Mas "pago no Omie" não quer dizer "registrado na planilha": falta o
        # resto do plano, e era justamente isso que ficava para trás.
        resto = _concluir_plano(resumo, item, payload)
        return {'ok': bool(resto.get('ok')), 'status': 'ja_pago',
                'consulta': consulta, 'resto_do_plano': resto}
    if not consulta.get('ok'):
        return {'ok': False, 'erro': 'falha_consulta_omie', 'consulta': consulta}

    alterar = _request_omie('AlterarContaPagar', {
        'codigo_lancamento_integracao': codigo,
        'id_conta_corrente': as_string(resumo.get('codigo_conta_omie')),
        'valor_documento': _money_to_omie_number(resumo.get('valor_pago')),
    }, payload)
    if not alterar.get('ok'):
        return {'ok': False, 'erro': 'falha_alterar_omie', 'consulta': consulta, 'alterar': alterar}

    baixar = _request_omie('LancarPagamento', {
        'codigo_lancamento_integracao': codigo,
        'codigo_conta_corrente': as_string(resumo.get('codigo_conta_omie')),
        'codigo_baixa_integracao': 'Retry' + datetime.now().strftime('%d%m%Y%H%M%S'),
        'data': as_string(resumo.get('data_pagamento')),
        'valor': _money_to_omie_number(resumo.get('valor_pago')),
        'juros': _money_to_omie_number(resumo.get('acrescimos') or '0,00'),
        'observacao': 'Baixa realizada via baixabradesco/retry',
    }, payload)
    if not baixar.get('ok'):
        return {'ok': False, 'erro': 'falha_lancar_pagamento', 'consulta': consulta,
                'alterar': alterar, 'baixar': baixar}

    resto = _concluir_plano(resumo, item, payload)
    return {'ok': bool(resto.get('ok')), 'consulta': consulta, 'alterar': alterar,
            'baixar': baixar, 'resto_do_plano': resto}


def _money_to_omie_number(valor: Any) -> str:
    s = as_string(valor)
    if not s:
        return '0.00'
    return s.replace('.', '').replace(',', '.')


def _retry_pipefy(item: Dict[str, Any], payload: dict) -> dict:
    # Mesmo cuidado do Omie, por um caminho diferente: sem o token,
    # `execute_graphql` LEVANTA exceção, e a exceção caía no `except` geral do
    # laço, que incrementa a tentativa. Cinco passadas sem token marcariam os 88
    # cartões como FALHOU sem nunca ter tentado nada.
    if not os.getenv('PIPEFY_API_TOKEN', '').strip():
        return {'ok': False, 'erro': 'credenciais_pipefy_ausentes'}

    resumo = json.loads(item.get('Payload Resumido') or '{}')
    mutation = as_string(resumo.get('pipefy_update_mutation'))
    if not mutation:
        return {'ok': False, 'erro': 'mutation_ausente'}
    return execute_graphql(mutation)


def _retry_zapi(item: Dict[str, Any], payload: dict) -> dict:
    resumo = json.loads(item.get('Payload Resumido') or '{}')
    msgs = resumo.get('whatsapp_messages') or []
    if not msgs:
        return {'ok': False, 'erro': 'mensagens_ausentes'}
    auth = resolve_zapi_auth(payload)
    missing = validate_zapi_auth(auth)
    if missing:
        return {'ok': False, 'erro': 'credenciais_zapi_ausentes', 'missing': missing}
    resp = send_messages_batch(auth, msgs)
    return {'ok': all(x.get('ok') for x in resp), 'responses': resp}


def _retry_sheets(item: Dict[str, Any], payload: dict) -> dict:
    resumo = json.loads(item.get('Payload Resumido') or '{}')
    updates = resumo.get('sheets_updates') or []
    if not updates:
        return {'ok': False, 'erro': 'updates_ausentes'}
    # O resultado era IGNORADO: a fila marcava "reprocessado com sucesso" mesmo
    # quando a gravação falhava de novo, e o item saía da fila sem ter sido
    # gravado. Era o último lugar onde a perda ainda acontecia em silêncio.
    resultado = execute_spsbd_updates(updates)
    return {'ok': bool(resultado.get('ok')), 'updates': len(updates), 'detalhe': resultado}


def _parece_cota(texto: str) -> bool:
    t = (texto or '').lower()
    return ('429' in t or 'quota exceeded' in t or 'resource_exhausted' in t
            or 'rate_limit' in t)


ROTULOS_ETAPA = {
    'omie': 'baixa no Omie',
    'sheets': 'atualização da planilha',
    'pipefy': 'cartão do Pipefy',
    'zapi': 'aviso de pagamento',
}


def _em_milhar(n: int) -> str:
    return f'{n:,}'.replace(',', '.')


def _frase_do_resumo(por_etapa: Dict[str, int], vencidos: int, agendados: int,
                     falhados: int, concluidos: int, mais_antigo: str) -> str:
    """Uma frase em português, para quem lê isto pelo celular.

    A resposta crua é chave-e-número; o dono olha o chat pelo telefone e não é
    programador. A frase não substitui os números, vem junto.
    """
    total = vencidos + agendados + falhados
    if not total:
        return ('Nenhuma pendência na fila'
                + (f'; {_em_milhar(concluidos)} já concluídas.' if concluidos else '.'))

    partes = []
    for etapa, qtd in sorted(por_etapa.items(), key=lambda x: -x[1]):
        if not qtd:
            continue
        partes.append(f'{_em_milhar(qtd)} de {ROTULOS_ETAPA.get(etapa, etapa)}')

    frase = f'{_em_milhar(total)} pendência(s) na fila'
    if partes:
        frase += ': ' + ', '.join(partes) + '.'
    else:
        frase += '.'
    if falhados:
        frase += (f' {_em_milhar(falhados)} já tentaram cinco vezes e só voltam'
                  ' se forem pedidas explicitamente.')
    if agendados:
        frase += f' {_em_milhar(agendados)} estão agendadas para mais tarde.'
    if mais_antigo:
        frase += f' A mais antiga é de {mais_antigo[:10]}.'
    if concluidos:
        frase += (f' Fora essas, {_em_milhar(concluidos)} linha(s) da aba já são'
                  ' histórico concluído.')
    return frase


# A ordem em que a fila é drenada, e ela não é alfabética: `omie` é dinheiro
# (baixa que não aconteceu), `sheets` é a planilha desatualizada, `pipefy` é o
# cartão no lugar errado, `zapi` é recado. Em 08/10/2026 a fila tinha 1.943
# recados na frente de 238 baixas — na ordem da planilha, o dinheiro sairia por
# último. Um lugar só, usado pelos dois caminhos automáticos (o cron e o lote de
# comprovantes), para não divergirem como já divergiram.
ORDEM_ETAPAS = ('omie', 'sheets', 'pipefy', 'zapi')


def drenar_por_etapa(limites: Dict[str, int], payload: Optional[dict] = None) -> dict:
    """Anda com a fila, uma etapa por vez, na ordem da importância.

    ⚠️ `descartar_avisos_antigos=False` é obrigatório aqui, e não é detalhe:
    limpar em massa o acumulado de avisos antigos é decisão do dono. Quando o
    lote de comprovantes drenava sem este cuidado, cada lote marcava cinco
    avisos velhos como descartados — ou seja, o sistema ia limpando sozinho o
    que foi dito que ele não tocaria.
    """
    base = dict(payload or {})
    saida: Dict[str, Any] = {}
    for etapa in ORDEM_ETAPAS:
        limite = int(limites.get(etapa) or 0)
        if limite <= 0:
            continue
        pedido = dict(base)
        pedido.update({'etapas': [etapa], 'limite': limite,
                       'descartar_avisos_antigos': False})
        try:
            r = reprocessar_fila(pedido)
            saida[etapa] = {
                'processados': r.get('pendentes_processados'),
                'concluidos': r.get('concluidos_agora'),
                'ainda_pendentes': r.get('ainda_pendentes'),
                'bloqueados_por_configuracao': r.get('bloqueados_por_configuracao'),
                'o_que_falta_configurar': r.get('o_que_falta_configurar'),
                'interrompido': r.get('interrompido'),
            }
            if r.get('interrompido') == 'cota_do_google':
                # Cota estourada: para aqui e deixa o resto para a próxima vez.
                saida['parou_por_cota_na_etapa'] = etapa
                break
        except Exception as e:
            saida[etapa] = {'erro': str(e)[:200]}
    return saida


def resumo_fila(gc=None) -> dict:
    """Conta a fila sem reprocessar nada.

    A aba `BaixaBradescoFila` guarda TUDO que já passou por ela, concluído
    inclusive. Então o número de linhas da aba não é o número de pendências —
    e essa diferença é a primeira coisa que alguém precisa saber antes de
    mandar drenar. Lê só cinco colunas leves, numa chamada.
    """
    ws = ensure_fila_sheet(gc)
    try:
        faixas = ws.batch_get(['A2:A', 'B2:B', 'D2:D', 'E2:E', 'L2:L']) or []
    except Exception as e:
        return {'ok': False, 'erro': str(e)[:300]}

    datas = _coluna(faixas, 0)
    status = _coluna(faixas, 1)
    proximas = _coluna(faixas, 2)
    tipos = _coluna(faixas, 3)
    etapas = _coluna(faixas, 4)
    total = max(len(datas), len(status), len(proximas), len(tipos), len(etapas))

    def em(lista, i):
        return lista[i] if i < len(lista) else ''

    agora = datetime.now()
    por_status: Dict[str, int] = {}
    por_etapa: Dict[str, int] = {}
    por_tipo: Dict[str, int] = {}
    vencidos = 0
    agendados = 0
    sem_etapa = 0
    mais_antiga = None
    mais_nova = None

    for i in range(total):
        st = as_string(em(status, i)).upper() or '(vazio)'
        por_status[st] = por_status.get(st, 0) + 1
        if st not in (STATUS_PENDENTE, STATUS_FALHOU):
            continue
        etapa = as_string(em(etapas, i)).lower() or '(vazia)'
        if etapa == '(vazia)':
            sem_etapa += 1
        por_etapa[etapa] = por_etapa.get(etapa, 0) + 1
        tipo = as_string(em(tipos, i)) or '(vazio)'
        por_tipo[tipo] = por_tipo.get(tipo, 0) + 1
        if st == STATUS_PENDENTE:
            dt = _parse_dt_br(em(proximas, i))
            if dt and dt > agora:
                agendados += 1
            else:
                vencidos += 1
        reg = _parse_dt_br(em(datas, i))
        if reg:
            if mais_antiga is None or reg < mais_antiga:
                mais_antiga = reg
            if mais_nova is None or reg > mais_nova:
                mais_nova = reg

    a_reprocessar = vencidos + por_status.get(STATUS_FALHOU, 0)
    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'resumo_fila',
        'linhas_na_aba': total,
        'por_status': por_status,
        'pendentes_vencidos': vencidos,
        'pendentes_agendados': agendados,
        'falhados': por_status.get(STATUS_FALHOU, 0),
        'concluidos': por_status.get(STATUS_CONCLUIDO, 0),
        'a_reprocessar_agora': a_reprocessar,
        'por_etapa': por_etapa,
        'por_tipo_falha': por_tipo,
        'sem_etapa': sem_etapa,
        'registro_mais_antigo': mais_antiga.strftime('%d/%m/%Y %H:%M:%S') if mais_antiga else '',
        'registro_mais_recente': mais_nova.strftime('%d/%m/%Y %H:%M:%S') if mais_nova else '',
        'lotes_de_5_necessarios': (vencidos + 4) // 5,
        'em_portugues': _frase_do_resumo(
            por_etapa, vencidos, agendados, por_status.get(STATUS_FALHOU, 0),
            por_status.get(STATUS_CONCLUIDO, 0),
            mais_antiga.strftime('%d/%m/%Y') if mais_antiga else ''),
    }


# Falha de CONFIGURAÇÃO, não de execução. Não consome tentativa: senão uma
# drenagem disparada sem credencial queimaria as cinco tentativas de todos os
# itens e os marcaria FALHOU — com 238 baixas de dinheiro na fila em 08/10/2026,
# isso apagaria a pendência sem resolver nada. Insistir não ajuda; marcar como
# fracassado mente.
ERROS_DE_CONFIGURACAO = {
    'credenciais_zapi_ausentes',
    'credenciais_omie_ausentes',
    'credenciais_pipefy_ausentes',
    'codigo_integracao_ausente',
    'mutation_ausente',
    'mensagens_ausentes',
    'updates_ausentes',
}


def _e_problema_de_configuracao(resp: Dict[str, Any]) -> bool:
    return as_string(resp.get('erro')) in ERROS_DE_CONFIGURACAO


def _etapas_pedidas(payload: dict) -> Optional[Set[str]]:
    """`etapas: ["omie"]` ou `etapa: "omie"`. Vazio = todas.

    Existe porque as etapas não valem o mesmo: `omie` é dinheiro (baixa que não
    aconteceu), `sheets` é a planilha desatualizada, `pipefy` é o cartão no lugar
    errado e `zapi` é recado. Drenar na ordem da planilha misturaria as quatro, e
    1.943 recados na frente de 238 baixas é a ordem errada.
    """
    bruto = payload.get('etapas') or payload.get('etapa')
    if not bruto:
        return None
    if isinstance(bruto, str):
        bruto = [p for p in bruto.replace(';', ',').split(',') if p.strip()]
    etapas = {as_string(e).strip().lower() for e in bruto}
    etapas.discard('')
    return etapas or None


def _pausa_padrao(limite: int, payload: dict) -> float:
    """Segundos de espera entre itens.

    A cota de escrita do Google é por minuto e é do mesmo usuário de serviço que
    o ERP, o painel e o Análise de SPs usam. Drenar centenas de itens no soco
    estouraria a cota de TODO MUNDO, não só desta fila. Lote pequeno segue sem
    pausa; lote grande anda devagar de propósito.
    """
    bruto = payload.get('pausa_ms')
    if bruto is not None:
        try:
            return max(0.0, float(bruto) / 1000.0)
        except Exception:
            pass
    return 0.0 if limite <= 20 else 1.2


def _aviso_velho_demais(item: Dict[str, Any], payload: dict) -> bool:
    """Aviso de WhatsApp enfileirado há dias não deve ser reenviado."""
    if bool(payload.get('reenviar_avisos_antigos')):
        return False
    registro = _parse_dt_br(item.get('Data Registro'))
    if not registro:
        return False
    dias = int(payload.get('dias_aviso_util') or DIAS_AVISO_UTIL)
    return (datetime.now() - registro) > timedelta(days=dias)


def zerar_fila_antiga(payload: dict) -> dict:
    """Dispensa as pendências registradas antes de uma data. Não apaga nada.

    Pedido do dono em 08/10/2026, vendo a fila com 2.213 pendências cuja mais
    antiga era de 18/06: *"só preciso que rode as coisas desse mês em diante. O
    que tá pra trás, poderia zerar."*

    A razão dele é boa, e vale registrada: ele faz **conciliação bancária
    diária**, então o que ficou para trás já foi resolvido na mão — a pendência
    é de registro, não de dinheiro. Insistir nelas gastaria cota e encheria dois
    celulares de avisos sobre pagamentos de junho.

    **Nada é apagado.** A linha fica onde está, marcada `CONCLUIDO`, com o motivo
    e a data da decisão escritos — quem abrir a planilha depois entende por quê.

    Body:
      {"antes_de": "01/10/2026", "limite": 2000, "etapas": ["zapi"]}

    `antes_de` é obrigatório: um "zerar tudo" sem data é fácil de disparar por
    engano, e desfazer linha por linha seria trabalho de horas.
    """
    corte = _parse_dt_br(as_string(payload.get('antes_de')) + ' 00:00:00') \
        or _parse_dt_br(as_string(payload.get('antes_de')))
    if not corte:
        return {'ok': False, 'app': 'baixabradesco', 'acao': 'zerar_fila_antiga',
                'erro': 'antes_de_ausente_ou_invalido',
                'em_portugues': 'Informe a data de corte no formato dd/mm/aaaa'
                                " (por exemplo, antes_de=01/10/2026)."}

    gc = get_gc()
    ws = ensure_fila_sheet(gc)
    limite = int(payload.get('limite') or 2000)
    etapas = _etapas_pedidas(payload)

    try:
        valores = ws.get(FAIXA_CONTROLE) or []
    except Exception as e:
        return {'ok': False, 'app': 'baixabradesco', 'acao': 'zerar_fila_antiga',
                'erro': str(e)[:300]}

    motivo = ('Dispensada por decisão do dono em ' + datetime.now().strftime('%d/%m/%Y')
              + ': anterior a ' + corte.strftime('%d/%m/%Y')
              + ' e já resolvida pela conciliação bancária.')

    marcas: List[Dict[str, Any]] = []
    por_etapa: Dict[str, int] = {}
    for idx, row in enumerate(valores, start=2):
        row = list(row) + [''] * (COLS_CONTROLE - len(row))
        status = as_string(row[1]).strip().upper()
        if status == 'STATUS':
            continue
        if status not in (STATUS_PENDENTE, STATUS_FALHOU):
            continue
        etapa = as_string(row[11]).lower()
        if etapas is not None and etapa not in etapas:
            continue
        registro = _parse_dt_br(row[0])
        if not registro or registro >= corte:
            continue
        por_etapa[etapa or '(vazia)'] = por_etapa.get(etapa or '(vazia)', 0) + 1
        marcas.append({'row': idx, 'status': STATUS_CONCLUIDO,
                       'tentativas': as_string(row[2]) or 0, 'mensagem': motivo})
        if len(marcas) >= limite:
            break

    gravadas = _marcar_em_lote(ws, marcas) if marcas else 0
    detalhe = ', '.join(f'{q} de {ROTULOS_ETAPA.get(e, e)}'
                        for e, q in sorted(por_etapa.items(), key=lambda x: -x[1]))
    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'zerar_fila_antiga',
        'antes_de': corte.strftime('%d/%m/%Y'),
        'encontradas': len(marcas),
        'dispensadas': gravadas,
        'por_etapa': por_etapa,
        'etapas_pedidas': sorted(etapas) if etapas else 'todas',
        'motivo_gravado': motivo,
        'em_portugues': (
            f'{_em_milhar(gravadas)} pendência(s) anteriores a'
            f' {corte.strftime("%d/%m/%Y")} foram dispensadas'
            + (f' ({detalhe})' if detalhe else '')
            + '. Nada foi apagado: a linha continua na planilha com o motivo'
              ' escrito. A fila agora só tem o que é de'
              f' {corte.strftime("%d/%m/%Y")} em diante.'
            if gravadas else
            f'Nenhuma pendência anterior a {corte.strftime("%d/%m/%Y")}'
            ' encontrada — não havia nada a dispensar.'),
    }


def reprocessar_fila(payload: dict) -> dict:
    """Reprocessa pendências da fila.

    Body opcional:
      {"limite": 10, "somente_vencidos": true, "incluir_falhados": false,
       "pausa_ms": 1200, "reenviar_avisos_antigos": false, "omie": {...}}

    `limite` alto serve para drenar fila acumulada de uma vez; nesse caso entra
    pausa entre itens (ver `_pausa_padrao`) e a varredura para sozinha se a cota
    do Google começar a recusar, em vez de insistir e derrubar os outros apps.
    """
    gc = get_gc()
    ws = ensure_fila_sheet(gc)
    limite = int(payload.get('limite') or 10)
    somente_vencidos = payload.get('somente_vencidos', True)
    incluir_falhados = bool(payload.get('incluir_falhados'))
    etapas = _etapas_pedidas(payload)
    pausa = _pausa_padrao(limite, payload)
    # Quem não vai descartar aviso antigo também não deve carregá-lo: senão ele
    # ocupa a vaga do aviso recente, que é o que ainda interessa a alguém.
    aviso_minimo = None
    if not payload.get('descartar_avisos_antigos', True):
        dias = int(payload.get('dias_aviso_util') or DIAS_AVISO_UTIL)
        aviso_minimo = datetime.now() - timedelta(days=dias)
    pendentes = listar_pendentes(gc, limite=limite,
                                 somente_vencidos=bool(somente_vencidos),
                                 incluir_falhados=incluir_falhados,
                                 etapas=etapas, aviso_minimo=aviso_minimo)
    resultados = []
    inicio = datetime.now()
    cota_seguidas = 0
    interrompido = ''
    descartes: List[Dict[str, Any]] = []
    bloqueados: List[Dict[str, Any]] = []
    pulados: List[Dict[str, Any]] = []

    for posicao, item in enumerate(pendentes):
        if pausa and posicao:
            time.sleep(pausa)
        row_number = int(item.get('_row_number'))
        tentativas = int(as_string(item.get('Tentativas') or '0') or 0) + 1
        etapa = as_string(item.get('Etapa')).lower()
        try:
            if etapa == 'zapi' and _aviso_velho_demais(item, payload):
                if not payload.get('descartar_avisos_antigos', True):
                    # A drenagem automática NÃO limpa o acumulado de avisos
                    # antigos: são 1.943 linhas do dono, e marcá-las em massa é
                    # decisão dele, não do cron. Fica intacto, sem gastar
                    # tentativa, e some da passada.
                    pulados.append({'row': row_number, 'etapa': etapa})
                    resultados.append({'row': row_number, 'etapa': etapa, 'ok': None,
                                       'pulado_por_idade': True})
                    continue
                # Não grava agora: junta e grava em lote no fim. Com 1.943
                # avisos antigos, uma escrita por linha estouraria a cota.
                descartes.append({'row': row_number, 'status': STATUS_CONCLUIDO,
                                  'tentativas': tentativas,
                                  'mensagem': 'Aviso antigo: não reenviado '
                                              '(fora do prazo de utilidade).'})
                resultados.append({'row': row_number, 'etapa': etapa, 'ok': True,
                                   'response': {'ok': True, 'status': 'aviso_descartado_por_idade'}})
                continue

            if etapa == 'omie':
                resp = _retry_omie(item, payload)
            elif etapa == 'pipefy':
                resp = _retry_pipefy(item, payload)
            elif etapa == 'zapi':
                resp = _retry_zapi(item, payload)
            elif etapa == 'sheets':
                resp = _retry_sheets(item, payload)
            else:
                resp = {'ok': False, 'erro': f'etapa_nao_suportada: {etapa}'}

            if resp.get('ok'):
                _update_row(ws, row_number, STATUS_CONCLUIDO, tentativas, 'Reprocessado com sucesso.')
            elif _e_problema_de_configuracao(resp):
                # Fica PENDENTE com as tentativas INTACTAS, e a linha explica o
                # que falta. Quem resolve isso é quem configura, não a insistência.
                bloqueados.append({'row': row_number, 'etapa': etapa,
                                   'erro': as_string(resp.get('erro'))})
                resultados.append({'row': row_number, 'etapa': etapa, 'ok': False,
                                   'bloqueado_por_configuracao': True, 'response': resp})
                continue
            elif incluir_falhados and tentativas >= 5:
                # Pedido explícito: continua FALHOU, mas com a mensagem nova —
                # senão a planilha guarda o erro da primeira vez e engana quem lê.
                _update_row(ws, row_number, STATUS_FALHOU, tentativas, _safe_json(resp))
            else:
                _update_row(ws, row_number, STATUS_PENDENTE if tentativas < 5 else STATUS_FALHOU, tentativas, _safe_json(resp))
            resultados.append({'row': row_number, 'etapa': etapa, 'ok': bool(resp.get('ok')), 'response': resp})

            if resp.get('ok'):
                cota_seguidas = 0
            elif _parece_cota(_safe_json(resp)):
                cota_seguidas += 1
            else:
                cota_seguidas = 0
        except Exception as e:
            try:
                _update_row(ws, row_number, STATUS_PENDENTE if tentativas < 5 else STATUS_FALHOU, tentativas, str(e))
            except Exception:
                pass
            resultados.append({'row': row_number, 'etapa': etapa, 'ok': False, 'error': str(e)})
            cota_seguidas = cota_seguidas + 1 if _parece_cota(str(e)) else 0

        if cota_seguidas >= 3:
            # A cota é compartilhada com os outros apps: insistir aqui tira o
            # ERP e o painel do ar. Para, devolve o que fez e deixa o resto
            # PENDENTE para a próxima passada.
            interrompido = 'cota_do_google'
            break

    descartes_gravados = _marcar_em_lote(ws, descartes) if descartes else 0
    ok = sum(1 for r in resultados if r.get('ok') is True)
    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'reprocessar_fila',
        'pendentes_processados': len(resultados),
        'concluidos_agora': ok,
        'ainda_pendentes': sum(1 for r in resultados if r.get('ok') is False),
        'avisos_descartados_por_idade': len(descartes),
        'descartes_gravados': descartes_gravados,
        'bloqueados_por_configuracao': len(bloqueados),
        'o_que_falta_configurar': sorted({b['erro'] for b in bloqueados}),
        'avisos_antigos_pulados': len(pulados),
        'incluiu_falhados': incluir_falhados,
        'etapas': sorted(etapas) if etapas else 'todas',
        'limite_pedido': limite,
        'pausa_entre_itens_s': pausa,
        'interrompido': interrompido,
        'segundos': round((datetime.now() - inicio).total_seconds(), 1),
        'resultados': resultados,
    }
