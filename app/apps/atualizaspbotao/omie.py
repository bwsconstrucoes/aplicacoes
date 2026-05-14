# -*- coding: utf-8 -*-
"""omie.py — Integração com API Omie portada do Omie.gs"""

import re
import requests
import logging
from datetime import datetime
from .utils import (
    as_string, value_or_empty, limpar_colchetes, limpar_documento,
    normalizar_numero_omie, normalizar_percentual_omie,
    formatar_moeda_br, number_to_br
)

logger = logging.getLogger(__name__)

URL_CLIENTES    = 'https://app.omie.com.br/api/v1/geral/clientes/'
URL_CONTAPAGAR  = 'https://app.omie.com.br/api/v1/financas/contapagar/'


def secao_omie(payload: dict, parametros_result: dict) -> dict:
    _validar_entrada_omie(payload)
    cliente = _resolver_codigo_cliente(payload, parametros_result)
    titulo  = _executar_operacao_conta_pagar(payload, parametros_result, cliente)
    return {'ok': bool(titulo.get('ok')), 'cliente': cliente, 'titulo': titulo}


def _validar_entrada_omie(payload: dict):
    for campo in ('omieAppKey', 'omieAppSecret'):
        if not as_string(payload.get(campo)):
            raise ValueError(f'Campo obrigatório não enviado: {campo}')


def _executar_chamada_omie(url: str, body: dict) -> dict:
    try:
        resp = requests.post(url, json=body, timeout=30)
        status = resp.status_code
        try:
            body_resp = resp.json()
        except Exception:
            body_resp = {'raw': resp.text}
        has_fault = bool(as_string(body_resp.get('faultstring') or body_resp.get('faultcode')))
        ok = 200 <= status < 300 and not has_fault
        return {'ok': ok, 'status': status, 'body': body_resp, 'raw': resp.text}
    except requests.RequestException as e:
        return {'ok': False, 'status': 0, 'body': {}, 'raw': str(e)}


def _resolver_codigo_cliente(payload: dict, parametros_result: dict) -> dict:
    saida = (parametros_result or {}).get('saida') or {}
    cliente_omie = as_string(saida.get('Cliente Omie'))

    if cliente_omie and cliente_omie.lower() != 'naocadastrado':
        return {
            'ok': True, 'operacao': 'existente',
            'codigo_cliente_fornecedor': cliente_omie,
            'request': None, 'response': None
        }

    request_body = _montar_payload_incluir_cliente(payload)
    resp = _executar_chamada_omie(URL_CLIENTES, request_body)

    codigo_direto = as_string((resp.get('body') or {}).get('codigo_cliente_omie'))
    if resp.get('ok') and codigo_direto:
        return {
            'ok': True, 'operacao': 'incluido',
            'codigo_cliente_fornecedor': codigo_direto,
            'request': request_body, 'response': resp
        }

    codigo_extraido = _extrair_codigo_cliente_do_erro(resp.get('body') or {})
    if codigo_extraido:
        return {
            'ok': True, 'operacao': 'extraido_do_erro',
            'codigo_cliente_fornecedor': codigo_extraido,
            'request': request_body, 'response': resp
        }

    return {
        'ok': False, 'operacao': 'falhou',
        'codigo_cliente_fornecedor': '',
        'request': request_body, 'response': resp
    }


def _montar_payload_incluir_cliente(payload: dict) -> dict:
    tipo = as_string(payload.get('OmiePessoaTipo'))
    proc = as_string(payload.get('OmieSelecioneProcedimento'))
    cnpj_cpf = (
        as_string(payload.get('OmieCPF'))
        if (tipo == 'Pessoa Física' or proc == 'Fundo Fixo')
        else as_string(payload.get('OmieCNPJ'))
    )
    codigo_integracao = 'Int' + datetime.now().strftime('%d%m%Y%H%M%S')
    nome = as_string(payload.get('OmieNomeCredor'))
    return {
        'call': 'IncluirCliente',
        'param': [{
            'tags': [{'tag': 'Fornecedor'}],
            'cnpj_cpf': limpar_documento(cnpj_cpf),
            'razao_social': nome[:59],
            'nome_fantasia': nome[:59],
            'codigo_cliente_integracao': codigo_integracao,
        }],
        'app_key': as_string(payload.get('omieAppKey')),
        'app_secret': as_string(payload.get('omieAppSecret')),
    }


def _extrair_codigo_cliente_do_erro(body: dict) -> str:
    codigo_direto = as_string(body.get('codigo_cliente_omie'))
    if codigo_direto:
        return codigo_direto
    fault = as_string(body.get('faultstring'))
    if not fault:
        return ''
    match = re.search(r'Id\s*\[(\d+)\]', fault, re.IGNORECASE)
    return match.group(1) if match else ''


def _is_erro_lancamento_nao_cadastrado(body: dict) -> bool:
    fault = as_string(body.get('faultstring'))
    code  = as_string(body.get('faultcode'))
    return (
        code == 'SOAP-ENV:Client-103' or
        bool(re.search(r'Lançamento não cadastrado para o Código de Integração', fault, re.IGNORECASE))
    )


def _executar_operacao_conta_pagar(payload: dict, parametros_result: dict, cliente: dict) -> dict:
    if not cliente.get('ok') or not cliente.get('codigo_cliente_fornecedor'):
        return {
            'ok': False, 'operacao': 'nenhuma',
            'codigo_lancamento_integracao': '',
            'codigo_status_http': None, 'codigo_status_omie': '',
            'mensagem': 'Não foi possível resolver codigo_cliente_fornecedor.',
            'request': None, 'response': cliente.get('response')
        }

    codigo_integracao_existente = as_string(payload.get('OmieCodigoLancamentoIntegracao'))
    operacao_inicial = 'alterar' if codigo_integracao_existente else 'incluir'

    body_inicial = _montar_payload_conta_pagar(
        payload, parametros_result,
        cliente['codigo_cliente_fornecedor'], operacao_inicial
    )
    resp_inicial = _executar_chamada_omie(URL_CONTAPAGAR, body_inicial)

    if resp_inicial.get('ok'):
        return _montar_resultado_titulo(operacao_inicial, body_inicial, resp_inicial)

    body_resp = resp_inicial.get('body') or {}

    # Fallback 1: Incluir falhou por duplicidade → tenta Alterar
    if operacao_inicial == 'incluir' and _is_erro_codigo_integracao_ja_cadastrado(body_resp):
        codigo_integracao = as_string(
            (body_inicial.get('param') or [{}])[0].get('codigo_lancamento_integracao')
        )
        body_alterar = _montar_payload_conta_pagar(
            payload, parametros_result,
            cliente['codigo_cliente_fornecedor'], 'alterar', codigo_integracao
        )
        resp_alterar = _executar_chamada_omie(URL_CONTAPAGAR, body_alterar)
        resultado = _montar_resultado_titulo('alterar_apos_duplicidade', body_alterar, resp_alterar)
        resultado['fallback'] = {
            'acionado': True,
            'motivo': 'Inclusão falhou porque o lançamento já existia.',
            'tentativaInicial': {'operacao': 'incluir', 'request': body_inicial, 'response': resp_inicial}
        }
        return resultado

    # Fallback 2: Alterar falhou porque não existe → tenta Incluir
    if operacao_inicial == 'alterar' and _is_erro_lancamento_nao_cadastrado(body_resp):
        logger.warning(f"[omie] AlterarContaPagar falhou com Client-103 — tentando IncluirContaPagar")
        body_incluir = _montar_payload_conta_pagar(
            payload, parametros_result,
            cliente['codigo_cliente_fornecedor'], 'incluir'
        )
        resp_incluir = _executar_chamada_omie(URL_CONTAPAGAR, body_incluir)
        resultado = _montar_resultado_titulo('incluir_apos_nao_cadastrado', body_incluir, resp_incluir)
        resultado['fallback'] = {
            'acionado': True,
            'motivo': 'Alteração falhou porque o lançamento não existia — incluído.',
            'tentativaInicial': {'operacao': 'alterar', 'request': body_inicial, 'response': resp_inicial}
        }
        return resultado

    resultado = _montar_resultado_titulo(operacao_inicial, body_inicial, resp_inicial)
    resultado['fallback'] = {'acionado': False}
    return resultado


def _montar_payload_conta_pagar(payload: dict, parametros_result: dict,
                                 codigo_cliente: str, operacao: str,
                                 codigo_integracao_forcado: str = None) -> dict:
    saida = (parametros_result or {}).get('saida') or {}

    # Incluir → sempre "Int" + id
    # Alterar → usa campo "Código Lançamento Integração Omie" do Pipefy (confirmado no blueprint)
    if codigo_integracao_forcado:
        codigo_integracao = as_string(codigo_integracao_forcado)
    elif operacao == 'alterar':
        codigo_integracao = as_string(payload.get('OmieCodigoLancamentoIntegracao'))
    else:
        codigo_integracao = 'Int' + as_string(payload.get('id'))

    id_val = as_string(payload.get('id'))

    param = {
        'data_emissao':                  as_string(payload.get('OmieDataSolicitacao')),
        'codigo_lancamento_integracao':  codigo_integracao,
        'codigo_cliente_fornecedor':     codigo_cliente,
        'data_vencimento':               as_string(saida.get('Vecimento')),
        'valor_documento':               normalizar_numero_omie(saida.get('Somatório R$')),
        'codigo_categoria':              as_string(saida.get('Tipo de Despesa')),
        'data_previsao':                 as_string(saida.get('Vecimento')),
        'id_conta_corrente':             as_string(payload.get('omieIdContaCorrente')),
        'numero_documento':              'SP' + id_val,
        'numero_parcela':                _montar_numero_parcela(payload),
        'observacao':                    _montar_observacao(payload, parametros_result),
        'retem_cofins': 'N', 'retem_csll': 'N', 'retem_inss': 'N',
        'retem_ir': 'N',     'retem_iss': 'N',  'retem_pis': 'N',
        'valor_cofins': '', 'valor_csll': '', 'valor_inss': '',
        'valor_ir': '',     'valor_iss': '',  'valor_pis': '',
        # Correção: numero_documento_fiscal = "SP" + id (igual ao Make.com, não usa OmieNumeroNotaFiscal)
        'numero_documento_fiscal':       'SP' + id_val,
        'numero_pedido':                 _montar_numero_pedido(payload),
        'distribuicao':                  _montar_distribuicao_base(payload, saida),
    }

    # Correção: data_entrada só no Alterar (igual ao Make módulo 198)
    if operacao == 'alterar':
        param['data_entrada'] = as_string(payload.get('OmieDataSolicitacao'))

    _aplicar_rateio_multiplo(param, payload.get('OmieRateioMultiplo'))
    return {
        'call': 'AlterarContaPagar' if operacao == 'alterar' else 'IncluirContaPagar',
        'app_key':    as_string(payload.get('omieAppKey')),
        'app_secret': as_string(payload.get('omieAppSecret')),
        'param': [param],
    }


def _montar_numero_parcela(payload: dict) -> str:
    title = as_string(payload.get('OmieTitle') or '')
    if '-' in title:
        return title[title.index('-') + 2:]
    return '001/001'


def _montar_numero_pedido(payload: dict) -> str:
    return as_string(payload.get('OmiePedidoSuprimentos') or '')[:15]


def _montar_observacao(payload: dict, parametros_result: dict) -> str:
    saida = (parametros_result or {}).get('saida') or {}
    centros = []
    rateios = []
    for i in range(1, 6):
        bruto   = as_string(payload.get(f'OmieCentro{i}') or '')
        nome    = limpar_colchetes(bruto)
        valor_s = value_or_empty(saida.get(f'Centro de Custo {i} Valor'))
        if nome:
            centros.append(nome)
            rateios.append(f'{nome}: {valor_s}')

    obs = (
        '-'.join(centros) +
        '\nCategoria: '          + limpar_colchetes(payload.get('OmieTipoDespesa') or '') +
        '\nBanco do Pagamento: ' + limpar_colchetes(payload.get('OmieBancoPagamento') or '') +
        '\nData do Pagamento: '  + value_or_empty(payload.get('OmieDataPagamento')) +
        '\nCentro de Custo: '    + ' - '.join(rateios) +
        '\nDescrição: '          + value_or_empty(payload.get('OmieDescricaoDespesa')) +
        '\nSP Pipefy Nº: '       + value_or_empty(payload.get('id'))
    )
    obs = re.sub(r' - \[\]:', '', obs)
    obs = re.sub(r'-\[\]', '', obs)
    return obs


def _montar_distribuicao_base(payload: dict, saida: dict) -> list:
    """
    Mapeamento conforme blueprint (módulos 191/198):
      cCodDep ← saida['Centro de Custo X']   = código Omie do depto (col A→E da planilha resultado)
      cDesDep ← OmieCentroX do payload       = nome limpo do centro de custo
      nPerDep ← saida['Centro de Custo X %'] = percentual (col I,K,M,O,Q da planilha resultado)
    """
    arr = []
    for i in range(1, 6):
        bruto = as_string(payload.get(f'OmieCentro{i}') or '')
        nome  = limpar_colchetes(bruto)
        cod   = as_string(saida.get(f'Centro de Custo {i}'))       # código Omie do depto
        perc  = normalizar_percentual_omie(saida.get(f'Centro de Custo {i} %'))  # percentual
        vazio = not bruto or bruto == '[]'

        if i == 1:
            # CC1 sempre presente (nunca null no Make)
            arr.append({
                'cCodDep': cod or None,
                'cDesDep': nome or None,
                'nPerDep': perc if perc else None,
                'nValDep': None,
            })
        else:
            # CC2→5: null se campo vazio/null no payload
            arr.append({
                'cCodDep': None if vazio else (cod or None),
                'cDesDep': None if (vazio or bruto == '[]') else (nome or None),
                'nPerDep': None if (vazio or perc == 0) else perc,
                'nValDep': None,
            })
    return arr


def _limpar_distribuicoes_nulas(arr: list) -> list:
    result = []
    for item in (arr or []):
        cod = as_string(item.get('cCodDep'))
        des = as_string(item.get('cDesDep'))
        per = item.get('nPerDep')
        val = item.get('nValDep')
        tudo_nulo = (
            not cod and (not des or des == '[]') and
            per in (None, '', 0) and val in (None, '')
        )
        if not tudo_nulo:
            result.append(item)
    return result


def _aplicar_rateio_multiplo(param: dict, rateio_raw):
    import json, re as _re

    txt = as_string(rateio_raw)
    if not txt:
        param['distribuicao'] = _limpar_distribuicoes_nulas(param['distribuicao'])
        if not param['distribuicao']:
            param['distribuicao'] = [{'cCodDep': None, 'cDesDep': None, 'nPerDep': 100, 'nValDep': None}]
        return

    try:
        txt = txt.replace('\\"', '"')
        fragment = {}

        # Extrai distribuicao via regex — robusto contra \n entre os blocos
        m_dist = _re.search(r'"distribuicao"\s*:\s*(\[.*?\])', txt, _re.DOTALL)
        if m_dist:
            try:
                fragment['distribuicao'] = json.loads(m_dist.group(1))
            except ValueError:
                pass

        # Extrai categorias via regex
        m_cat = _re.search(r'"categorias"\s*:\s*(\[.*?\])', txt, _re.DOTALL)
        if m_cat:
            try:
                fragment['categorias'] = json.loads(m_cat.group(1))
            except ValueError:
                pass

        if isinstance(fragment.get('distribuicao'), list):
            param['distribuicao'] = _limpar_distribuicoes_nulas(fragment['distribuicao'])
        if isinstance(fragment.get('categorias'), list):
            param['categorias'] = fragment['categorias']

        if not param.get('distribuicao'):
            param['distribuicao'] = [{'cCodDep': None, 'cDesDep': None, 'nPerDep': 100, 'nValDep': None}]

        logger.info(f"[rateio_multiplo] distribuicao={len(param.get('distribuicao', []))} "
                    f"categorias={len(param.get('categorias', []))}")

    except Exception as e:
        logger.error(f"[rateio_multiplo] Erro ao parsear: {e} | raw={txt[:200]}")
        param['distribuicao'] = _limpar_distribuicoes_nulas(param['distribuicao'])
        if not param['distribuicao']:
            param['distribuicao'] = [{'cCodDep': None, 'cDesDep': None, 'nPerDep': 100, 'nValDep': None}]


def _is_erro_codigo_integracao_ja_cadastrado(body: dict) -> bool:
    fault = as_string(body.get('faultstring'))
    code  = as_string(body.get('faultcode'))
    return (
        code == 'SOAP-ENV:Client-102' or
        bool(re.search(r'Lançamento já cadastrado para o Código de Integração', fault, re.IGNORECASE))
    )


def _montar_resultado_titulo(operacao: str, request_body: dict, resp: dict) -> dict:
    body = resp.get('body') or {}
    codigo = as_string(
        body.get('codigo_lancamento_integracao') or
        body.get('codigo_lancamento_omie') or
        body.get('codigo_lancamento')
    ) or as_string(
        (request_body.get('param') or [{}])[0].get('codigo_lancamento_integracao')
    )
    mensagem = as_string(
        body.get('faultstring') or body.get('mensagem') or
        body.get('descricao_status') or ''
    )
    return {
        'ok':                            bool(resp.get('ok')),
        'operacao':                      operacao,
        'codigo_lancamento_integracao':  codigo,
        'codigo_status_http':            resp.get('status'),
        'codigo_status_omie':            as_string(body.get('codigo_status') or body.get('faultcode') or ''),
        'mensagem':                      mensagem,
        'request':                       request_body,
        'response':                      resp,
    }