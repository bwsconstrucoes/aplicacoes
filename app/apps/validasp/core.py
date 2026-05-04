# -*- coding: utf-8 -*-
"""
validasp/core.py
Fluxo:
  1. Atualiza campo Pipefy
  2. Gera e encurta link de anuência
  3. Envia Z-API (responsável, requerente, anuente se existir)
  4. Retorna response imediatamente
  5. Em background: tenta atualizar SPsBD com retry 90s+90s,
     só depois grava FalhaProcessar
"""

import os
import re
import json
import time
import base64
import logging
import threading
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode

import gspread
from base64 import b64decode
from google.oauth2.service_account import Credentials

from .zapi import enviar_texto, enviar_multiplos
from ..atualizaspbotao.utils import as_string

logger = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

CONFIG = {
    'SECRET':           os.getenv('VALIDASP_SECRET', ''),
    'SPREADSHEET_ID':   '1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA',
    'PIPEFY_FIELD_ID':  'valida_o_sp_1',
    'ANUENCIA_WEBHOOK': 'https://hook.us1.make.com/dxn4jftghu92lpdw7hs9mlzr7s7tpx9m',
    'ENCURTADOR_URL':   'https://aplicacoes.bwsconstrucoes.com.br/encurtador/novo',
    'FALHA_SHEET':      'FalhaProcessar',
    'SPSBD_DELAY_S':    90,   # segundos entre tentativas
    'SPSBD_TENTATIVAS': 2,    # número máximo de tentativas
}


def _decodificar_b64(valor: str) -> str:
    """Tenta decodificar base64. Retorna original se não for base64 válido."""
    try:
        decoded = base64.b64decode(as_string(valor)).decode('utf-8')
        if decoded.isprintable():
            return decoded
    except Exception:
        pass
    return as_string(valor)


def _decodificar_b64_inline(texto: str) -> str:
    """
    Procura tokens base64 dentro de um texto e os decodifica.
    Aplica em loop até não restar tokens — trata base64 duplo ou aninhado.
    Aceita tokens com ou sem padding (=), mínimo 20 caracteres.
    """
    import re
    def tentar_decode(match):
        token = match.group(0)
        try:
            padding = (4 - len(token) % 4) % 4
            decoded = base64.b64decode(token + '=' * padding).decode('utf-8')
            if len(decoded) > 3 and all(c.isprintable() or c in '\n\r\t\xa0' for c in decoded):
                return decoded
        except Exception:
            pass
        return token

    padrao = re.compile(r'[A-Za-z0-9+/]{20,}={0,2}')
    MAX_PASSES = 5
    for _ in range(MAX_PASSES):
        novo = padrao.sub(tentar_decode, texto)
        if novo == texto:
            break  # nenhum token foi substituído — encerra
        texto = novo
    return texto


def _normalizar_payload(p: dict) -> dict:
    """Decodifica campos que podem chegar em base64 do Pipefy."""
    campos_b64 = ['descricaodadespesa', 'nomedocredor', 'nomedotitulardaconta']
    result = dict(p)
    for campo in campos_b64:
        if campo in result:
            result[campo] = _decodificar_b64(result[campo])
        if 'anuencia' in result and campo in (result.get('anuencia') or {}):
            result['anuencia'][campo] = _decodificar_b64(result['anuencia'][campo])
    return result


def validar_payload(payload: dict):
    if not payload:
        raise ValueError('Payload vazio.')
    if payload.get('secret') != CONFIG['SECRET']:
        raise ValueError('Secret inválido.')
    if not as_string(payload.get('id')):
        raise ValueError('Campo id é obrigatório.')


def executar(payload: dict) -> dict:
    """
    Executa as seções síncronas e dispara SPsBD em background.
    Retorna resultado sem esperar o SPsBD.
    """
    resultado = {'secoes': {}}
    p = _normalizar_payload(payload)  # decodifica campos base64

    # Credenciais Z-API do payload
    zapi_config = {
        'instance_id':  as_string(p.get('zapi_instance_id')),
        'api_token':    as_string(p.get('zapi_api_token')),
        'client_token': as_string(p.get('zapi_client_token')),
    }

    # 1. Atualiza campo Pipefy
    resultado['secoes']['pipefy'] = _atualizar_campo_pipefy(p)
    ok_pipefy = resultado['secoes']['pipefy'].get('ok', False)

    # 2. Gera e encurta link de anuência — apenas se anuente existir
    anuente_bloco = p.get('msg_anuente') or {}
    tel_anuente   = as_string(anuente_bloco.get('telefone', ''))
    tem_anuente   = _telefone_valido(tel_anuente)

    if tem_anuente:
        resultado['secoes']['linkAnuencia'] = _gerar_link_anuencia(p)
        short_url = resultado['secoes']['linkAnuencia'].get('short_url', '')
    else:
        resultado['secoes']['linkAnuencia'] = {
            'ok': True, 'ignorado': True,
            'motivo': 'Anuente não informado — link não gerado.'
        }
        short_url = ''

    # 3a. Envia mensagem ao responsável (+ requerente se habilitado)
    resultado['secoes']['zapiResponsavel'] = _enviar_msg_responsavel(
        p, zapi_config
    )

    # 3b. Envia mensagem ao anuente — SOMENTE se anuente existir
    if tem_anuente:
        resultado['secoes']['zapiAnuente'] = _enviar_msg_anuente(
            p, short_url, zapi_config
        )
    else:
        resultado['secoes']['zapiAnuente'] = {
            'ok': True, 'ignorado': True,
            'motivo': 'Anuente não informado — mensagem não enviada.'
        }

    # 3c. Envia msg de erro Pipefy se falhou
    if not ok_pipefy:
        resultado['secoes']['zapiErroPipefy'] = _enviar_msg_erro_pipefy(
            p, zapi_config
        )
    else:
        resultado['secoes']['zapiErroPipefy'] = {
            'ok': True, 'ignorado': True,
            'motivo': 'Pipefy atualizado com sucesso.'
        }

    # 4. SPsBD em background — não bloqueia o response
    resultado['secoes']['spsbd'] = {
        'ok': True, 'background': True,
        'motivo': 'Atualização do SPsBD iniciada em background (até 3 min).'
    }
    threading.Thread(
        target=_spsbd_com_retry,
        args=(p,),
        daemon=True
    ).start()

    return resultado


# ---------------------------------------------------------------------------
# 1. PIPEFY
# ---------------------------------------------------------------------------

def _atualizar_campo_pipefy(p: dict) -> dict:
    token = os.getenv('PIPEFY_API_TOKEN', '')
    if not token:
        return {'ok': False, 'ignorado': True, 'motivo': 'PIPEFY_API_TOKEN não configurado'}

    query = """
    mutation {
      updateCardField(input: {
        card_id: "%s"
        field_id: "%s"
        new_value: "Sim"
      }) { success }
    }
    """ % (as_string(p.get('id')), CONFIG['PIPEFY_FIELD_ID'])

    try:
        resp = requests.post(
            'https://api.pipefy.com/graphql',
            json={'query': query},
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            },
            timeout=15,
        )
        data = resp.json()
        ok = bool(
            data.get('data', {})
                .get('updateCardField', {})
                .get('success')
        )
        return {'ok': ok, 'response': data}
    except Exception as e:
        return {'ok': False, 'erro': str(e)}


# ---------------------------------------------------------------------------
# 2. LINK DE ANUÊNCIA + ENCURTADOR
# ---------------------------------------------------------------------------

def _gerar_link_anuencia(p: dict) -> dict:
    """
    Usa o bloco 'anuencia' do payload (montado pelo Make) para compor
    a URL longa e encurta via /encurtador/novo do próprio Render.
    """
    bloco = p.get('anuencia') or {}

    if bloco:
        link_longo = CONFIG['ANUENCIA_WEBHOOK'] + '?' + urlencode(
            {k: as_string(v) for k, v in bloco.items()}
        )
    else:
        excluir = {'secret', 'zapi_instance_id', 'zapi_api_token',
                   'zapi_client_token', 'msg_responsavel',
                   'msg_anuente', 'msg_erro_pipefy', 'anuencia'}
        link_longo = CONFIG['ANUENCIA_WEBHOOK'] + '?' + urlencode(
            {k: as_string(v) for k, v in p.items() if k not in excluir}
        )

    sp_id  = as_string(p.get('id'))
    codigo = f"{sp_id}_{int(time.time())}"
    expira = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')

    try:
        resp = requests.post(
            CONFIG['ENCURTADOR_URL'],
            data={'codigo': codigo, 'url': link_longo, 'expira_em': expira},
            timeout=10,
        )
        data = resp.json()
        short_url = as_string(data.get('short_url') or data.get('url') or link_longo)
        return {'ok': resp.status_code < 300, 'short_url': short_url, 'codigo': codigo}
    except Exception as e:
        return {'ok': False, 'short_url': link_longo, 'erro': str(e)}


# ---------------------------------------------------------------------------
# 3. MENSAGENS Z-API
# ---------------------------------------------------------------------------

def _telefone_valido(tel: str) -> bool:
    """
    Telefone válido = tem mais de 2 dígitos além do prefixo 55.
    Caso o campo seja vazio/null no Pipefy, o Make gera apenas "55".
    """
    digits = re.sub(r'\D', '', as_string(tel))
    return len(digits) > 4  # mínimo: 55 + DDD + ao menos 1 dígito


def _enviar_msg_responsavel(p: dict, zapi_config: dict) -> dict:
    bloco    = p.get('msg_responsavel') or {}
    tel_resp = as_string(bloco.get('telefone', ''))
    mensagem = _decodificar_b64_inline(as_string(bloco.get('mensagem', '')))

    if not _telefone_valido(tel_resp) or not mensagem:
        return {'ok': False, 'motivo': 'telefone ou mensagem do responsável ausente'}

    mensagens = [
        {'tipo': 'texto', 'telefone': tel_resp, 'mensagem': mensagem, 'enabled': True}
    ]

    # Requerente — envia se telefone válido (mesmo que igual ao responsável)
    tel_requ = as_string(bloco.get('telefone_requerente', ''))
    if _telefone_valido(tel_requ):
        mensagens.append(
            {'tipo': 'texto', 'telefone': tel_requ, 'mensagem': mensagem, 'enabled': True}
        )

    resultados = enviar_multiplos(mensagens, zapi_config=zapi_config)
    return {'ok': any(r.get('ok') for r in resultados), 'envios': resultados}


def _enviar_msg_anuente(p: dict, short_url: str, zapi_config: dict) -> dict:
    bloco    = p.get('msg_anuente') or {}
    tel      = as_string(bloco.get('telefone', ''))
    mensagem = _decodificar_b64_inline(as_string(bloco.get('mensagem', '')))

    if not _telefone_valido(tel) or not mensagem:
        return {'ok': False, 'motivo': 'telefone ou mensagem do anuente ausente'}

    mensagem = mensagem.replace('__link_anuencia__', short_url)
    mensagem = mensagem.replace('__LINK_ANUENCIA__', short_url)  # fallback maiúsculas
    return enviar_texto(tel, mensagem, zapi_config=zapi_config)


def _enviar_msg_erro_pipefy(p: dict, zapi_config: dict) -> dict:
    bloco    = p.get('msg_erro_pipefy') or {}
    tel      = as_string(bloco.get('telefone', ''))
    mensagem = _decodificar_b64_inline(as_string(bloco.get('mensagem', '')))

    if not _telefone_valido(tel) or not mensagem:
        return {'ok': False, 'motivo': 'telefone ou mensagem de erro ausente'}

    return enviar_texto(tel, mensagem, zapi_config=zapi_config)


# ---------------------------------------------------------------------------
# 4. SPSBD EM BACKGROUND — retry com delay de 90s
# ---------------------------------------------------------------------------

def _spsbd_com_retry(p: dict):
    """
    Roda em thread separada.
    Tenta 2 vezes com intervalo de 90s entre cada tentativa.
    Se ambas falharem, grava em FalhaProcessar.
    """
    id_val     = as_string(p.get('id'))
    tentativas = CONFIG['SPSBD_TENTATIVAS']
    delay      = CONFIG['SPSBD_DELAY_S']

    for tentativa in range(1, tentativas + 1):
        logger.info(f"[SPsBD] Tentativa {tentativa}/{tentativas} para SP {id_val} "
                    f"(aguardando {delay}s...)")
        time.sleep(delay)

        try:
            gc = _get_gc()
            ss = gc.open_by_key(CONFIG['SPREADSHEET_ID'])
            sh = ss.worksheet('SPsBD')

            values = sh.col_values(1)
            row    = None
            for i in range(len(values) - 1, 0, -1):
                if as_string(values[i]) == id_val:
                    row = i + 1
                    break

            if row:
                sh.update_cell(row, 34, 'Sim')  # coluna AH = 34
                logger.info(f"[SPsBD] SP {id_val} atualizada na linha {row} "
                            f"(tentativa {tentativa}).")
                return  # sucesso — encerra a thread

            logger.warning(f"[SPsBD] SP {id_val} não encontrada (tentativa {tentativa}).")

        except Exception as e:
            logger.error(f"[SPsBD] Erro na tentativa {tentativa} para SP {id_val}: {e}")

    # Todas as tentativas falharam — grava em FalhaProcessar
    logger.warning(f"[SPsBD] SP {id_val} não encontrada após {tentativas} tentativas. "
                   f"Gravando em FalhaProcessar.")
    try:
        gc = _get_gc()
        ss = gc.open_by_key(CONFIG['SPREADSHEET_ID'])
        sh = ss.worksheet(CONFIG['FALHA_SHEET'])
        agora = datetime.now().strftime('%d/%m/%Y %H:%M')
        sh.append_row([id_val, agora, 'Validar'])
    except Exception as e:
        logger.error(f"[SPsBD] Erro ao gravar FalhaProcessar para SP {id_val}: {e}")


# ---------------------------------------------------------------------------
# AUTH GOOGLE
# ---------------------------------------------------------------------------

def _get_gc():
    creds_b64 = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
    if not creds_b64:
        raise RuntimeError('GOOGLE_CREDENTIALS_BASE64 não configurado.')
    creds_dict = json.loads(b64decode(creds_b64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)