# -*- coding: utf-8 -*-
"""
validasp/core.py
Fluxo:
  1. Guarda a validação na fila (fila.py) — ANTES de qualquer mensagem, para
     ninguém receber "validada" de uma SP cuja marcação pode se perder
  2. Gera e encurta link de anuência
  3. Envia Z-API (responsável, requerente, anuente se existir)
  4. Retorna response imediatamente; o vigia da fila grava o "Sim" na SPsBD
     (coluna AH), confere lendo de volta e insiste até conseguir

Desde 17/10/2026 o validasp NÃO mexe mais no card do Pipefy — pedido do dono.
"""

import os
import re
import time
import base64
import logging
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode

from . import fila
from .zapi import enviar_texto, enviar_multiplos
from ..atualizaspbotao.utils import as_string

logger = logging.getLogger(__name__)

CONFIG = {
    'SECRET':           os.getenv('VALIDASP_SECRET', ''),
    'ANUENCIA_WEBHOOK': 'https://hook.us1.make.com/dxn4jftghu92lpdw7hs9mlzr7s7tpx9m',
    'ENCURTADOR_URL':   'https://aplicacoes.bwsconstrucoes.com.br/encurtador/novo',
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
    Guarda a validação na fila e manda as mensagens.
    Retorna sem esperar a planilha — quem grava o "Sim" é o vigia da fila.
    """
    resultado = {'secoes': {}}
    p = _normalizar_payload(payload)  # decodifica campos base64

    # 1. A validação entra na fila antes de qualquer mensagem sair
    resultado['secoes']['spsbd'] = fila.registrar(as_string(p.get('id')))
    fila.garantir_vigia()

    # Credenciais Z-API do payload
    zapi_config = {
        'instance_id':  as_string(p.get('zapi_instance_id')),
        'api_token':    as_string(p.get('zapi_api_token')),
        'client_token': as_string(p.get('zapi_client_token')),
    }

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

    return resultado


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
