# -*- coding: utf-8 -*-
"""
omie.py — Inclusão de Cliente + Inclusão de Conta a Pagar no Omie

Versão simplificada para processarnovasp:
- Sempre tenta IncluirContaPagar (não AlterarContaPagar — SP é nova).
- Se vier 'Lançamento já cadastrado' (Client-102) → considera duplicado (não falha).
- Se vier 'API bloqueada por consumo indevido' → falha → vai pra FalhaProcessar.
"""

import re
import requests
import logging
from datetime import datetime
from .utils import (
    as_string, limpar_colchetes, limpar_documento,
    normalizar_numero_omie, parse_data_pipefy, formatar_data_br,
)

logger = logging.getLogger(__name__)

URL_CLIENTES   = 'https://app.omie.com.br/api/v1/geral/clientes/'
URL_CONTAPAGAR = 'https://app.omie.com.br/api/v1/financas/contapagar/'


def secao_omie(payload: dict, rateio_saida: dict, rateio_descritivo: dict) -> dict:
    """
    Executa fluxo Omie completo para SP nova:
      1. Resolve código do cliente (lookup já feito pelo rateio em col 20)
      2. Se 'naocadastrado' → IncluirCliente
      3. IncluirContaPagar

    Retorna estrutura igual ao atualizaspbotao: {'ok', 'cliente', 'titulo'}.
    """
    _validar_entrada(payload)

    cliente = _resolver_codigo_cliente(payload, rateio_saida)
    titulo  = _incluir_conta_pagar(payload, rateio_saida, rateio_descritivo, cliente)

    return {
        'ok':       bool(titulo.get('ok')),
        'cliente':  cliente,
        'titulo':   titulo,
        'duplicado': bool(titulo.get('duplicado')),
        'falha':    not titulo.get('ok') and not titulo.get('duplicado'),
    }


def _validar_entrada(payload: dict):
    for campo in ('omieAppKey', 'omieAppSecret'):
        if not as_string(payload.get(campo)):
            raise ValueError(f'Campo obrigatório não enviado: {campo}')


def _post_omie(url: str, body: dict) -> dict:
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


# -----------------------------------------------------------------------------
# 1. Cliente (Fornecedor)
# -----------------------------------------------------------------------------

def _resolver_codigo_cliente(payload: dict, saida: dict) -> dict:
    """
    saida[20] = código já encontrado pelo rateio, ou 'naocadastrado'
    """
    cliente_existente = as_string(saida.get(20))
    if cliente_existente and cliente_existente.lower() != 'naocadastrado':
        return {
            'ok': True, 'operacao': 'existente',
            'codigo_cliente_fornecedor': cliente_existente,
            'request': None, 'response': None,
        }

    body = _montar_payload_incluir_cliente(payload)
    resp = _post_omie(URL_CLIENTES, body)

    codigo_direto = as_string((resp.get('body') or {}).get('codigo_cliente_omie'))
    if resp.get('ok') and codigo_direto:
        return {
            'ok': True, 'operacao': 'incluido',
            'codigo_cliente_fornecedor': codigo_direto,
            'request': body, 'response': resp,
        }

    # Mesmo se vier erro de duplicidade ("Cliente já cadastrado para o Id [xxx]")
    # o Omie devolve o ID na mensagem — extraímos.
    codigo_extraido = _extrair_codigo_cliente_do_erro(resp.get('body') or {})
    if codigo_extraido:
        return {
            'ok': True, 'operacao': 'extraido_do_erro',
            'codigo_cliente_fornecedor': codigo_extraido,
            'request': body, 'response': resp,
        }

    return {
        'ok': False, 'operacao': 'falhou',
        'codigo_cliente_fornecedor': '',
        'request': body, 'response': resp,
    }


def _montar_payload_incluir_cliente(payload: dict) -> dict:
    pessoa_tipo  = as_string(payload.get('PessoaTipo') or '')
    procedimento = as_string(payload.get('Procedimento') or '')
    cpf  = as_string(payload.get('CPFCredor') or '')
    cnpj = as_string(payload.get('CNPJCredor') or '')

    if pessoa_tipo == 'Pessoa Física' or procedimento == 'Fundo Fixo':
        cnpj_cpf = limpar_documento(cpf)
    else:
        cnpj_cpf = limpar_documento(cnpj)

    nome = as_string(payload.get('NomeCredor') or '').upper()
    codigo_integracao = 'Int' + datetime.now().strftime('%d%m%Y%H%M%S')

    return {
        'call': 'IncluirCliente',
        'param': [{
            'tags': [{'tag': 'Fornecedor'}],
            'cnpj_cpf':                 cnpj_cpf,
            'razao_social':             nome[:59],
            'nome_fantasia':            nome[:59],
            'codigo_cliente_integracao': codigo_integracao,
        }],
        'app_key':    as_string(payload.get('omieAppKey')),
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


# -----------------------------------------------------------------------------
# 2. Conta a Pagar (Título)
# -----------------------------------------------------------------------------

def _incluir_conta_pagar(payload: dict, saida: dict, descritivo: dict, cliente: dict) -> dict:
    if not cliente.get('ok') or not cliente.get('codigo_cliente_fornecedor'):
        return {
            'ok': False, 'operacao': 'nenhuma',
            'codigo_lancamento_integracao': '',
            'codigo_status_http': None, 'codigo_status_omie': '',
            'mensagem': 'Não foi possível resolver código_cliente_fornecedor.',
            'request': None, 'response': cliente.get('response'),
        }

    body = _montar_payload_conta_pagar(payload, saida, descritivo, cliente['codigo_cliente_fornecedor'])
    resp = _post_omie(URL_CONTAPAGAR, body)

    if resp.get('ok'):
        return _resultado_titulo('incluir', body, resp, duplicado=False)

    body_resp = resp.get('body') or {}

    # Duplicidade: lançamento já cadastrado → tratamos como "já existe, segue baile"
    if _is_erro_ja_cadastrado(body_resp):
        return _resultado_titulo('incluir', body, resp, duplicado=True)

    # API bloqueada ou outro erro → falha
    return _resultado_titulo('incluir', body, resp, duplicado=False)


def _montar_payload_conta_pagar(payload: dict, saida: dict, descritivo: dict,
                                  codigo_cliente: str) -> dict:
    id_sp = as_string(payload.get('id'))

    # Vencimento: pode vir corrigido se status = "Não Atende" e não for boleto
    venc_str = _resolver_data_vencimento(payload)

    # Pedido
    numero_pedido = as_string(payload.get('NumeroPedido') or '')[:15]

    # Banco do pagamento p/ observação
    banco_pagto = limpar_colchetes(payload.get('BancoPagamento') or '')

    # Distribuição (cCodDep / cDesDep / nPerDep) ---------------------------
    distribuicao = []
    for i in range(5):
        cod_dep   = as_string(saida.get(i)) or None        # col 0..4 (código Omie)
        nome_cc   = descritivo['centros_nomes'][i] or None
        perc      = descritivo['percentuais_cc'][i]

        if not nome_cc and not cod_dep:
            continue
        if perc <= 0:
            continue

        distribuicao.append({
            'cCodDep': cod_dep,
            'cDesDep': nome_cc,
            'nPerDep': round(perc, 7),
            'nValDep': None,
        })

    # Se nada veio, default 100% sem cc (caso "Não rateado" sem CC1 selecionado)
    if not distribuicao:
        distribuicao = [{'cCodDep': None, 'cDesDep': None, 'nPerDep': 100, 'nValDep': None}]

    # Aplica Rateio Múltiplo (sobrescreve distribuição se vier preenchido)
    distribuicao_final, categorias_extras = _aplicar_rateio_multiplo(
        payload.get('RateioMultiplo'), distribuicao
    )

    param = {
        'data_emissao':                  '',  # Make manda vazio
        'codigo_lancamento_integracao':  'Int' + id_sp,
        'codigo_cliente_fornecedor':     codigo_cliente,
        'data_vencimento':               venc_str,
        'data_previsao':                 venc_str,
        'valor_documento':               normalizar_numero_omie(saida.get(17)),
        'codigo_categoria':              as_string(saida.get(5)),
        'id_conta_corrente':             as_string(payload.get('omieIdContaCorrente') or '583772104'),
        'numero_documento':              'SP' + id_sp,
        'numero_parcela':                '001/001',
        'observacao':                    _montar_observacao(payload, saida, descritivo, banco_pagto),
        'retem_cofins': 'N', 'retem_csll': 'N', 'retem_inss': 'N',
        'retem_ir': 'N',     'retem_iss': 'N',  'retem_pis': 'N',
        'valor_cofins': '', 'valor_csll': '', 'valor_inss': '',
        'valor_ir': '',     'valor_iss': '',  'valor_pis': '',
        'numero_documento_fiscal':       _resolver_numero_nf(payload),
        'numero_pedido':                 numero_pedido,
        'distribuicao':                  distribuicao_final,
    }
    if categorias_extras:
        param['categorias'] = categorias_extras

    return {
        'call':       'IncluirContaPagar',
        'app_key':    as_string(payload.get('omieAppKey')),
        'app_secret': as_string(payload.get('omieAppSecret')),
        'param':      [param],
    }


def _resolver_data_vencimento(payload: dict) -> str:
    """
    Replica:
      if(Status Vencimento == "Não Atende" & Tipo de Pagamento != "Boleto"
         then Vencimento Corrigido
         else formatDate(Data de Vencimento; DD/MM/YYYY))
    """
    status   = as_string(payload.get('StatusVencimento') or '')
    tipo_pag = as_string(payload.get('TipoPagamento') or '')

    if status == 'Não Atende' and tipo_pag != 'Boleto':
        vc = as_string(payload.get('VencimentoCorrigido') or '')
        if vc:
            return vc
    return formatar_data_br(parse_data_pipefy(payload.get('DataVencimento') or ''))


def _resolver_numero_nf(payload: dict) -> str:
    """Remove '.' e ',' e converte para inteiro como string; vazio se for 0."""
    nf = as_string(payload.get('NumeroNotaFiscal') or '')
    if not nf:
        return ''
    sanit = re.sub(r'[^\d]', '', nf)
    try:
        n = int(sanit)
        return str(n) if n > 0 else ''
    except ValueError:
        return ''


def _montar_observacao(payload: dict, saida: dict, descritivo: dict, banco_pagto: str) -> str:
    """
    Replica exatamente o formato do módulo 223/609 do Make:
      CC1-CC2-CC3-CC4-CC5
      Categoria: <tipo despesa>
      Banco do Pagamento: <banco>
      Data do Pagamento: <data>
      Centro de Custo: CC1: valor1 - CC2: valor2 - ...
      Descrição: <descrição>
      SP Pipefy Nº: <id>
    """
    centros = [n for n in descritivo['centros_nomes'] if n]
    valores = descritivo['valores_cc']

    # primeira linha: CC1-CC2-...
    linha_centros = '-'.join(centros)

    # Linha "Centro de Custo: CCx: valorx - CCy: valory ..."
    rateios = []
    for i, nome in enumerate(descritivo['centros_nomes']):
        if not nome:
            continue
        from .utils import number_to_br
        rateios.append(f'{nome}: {number_to_br(valores[i])}')

    tipo_desp = limpar_colchetes(payload.get('TipoDespesa') or '')
    data_pag  = as_string(payload.get('DataPagamento') or '')
    descricao = as_string(payload.get('DescricaoDespesa') or '')

    obs = (
        f'{linha_centros}\n'
        f'Categoria: {tipo_desp}\n'
        f'Banco do Pagamento: {banco_pagto}\n'
        f'Data do Pagamento: {data_pag}\n'
        f'Centro de Custo: {" - ".join(rateios)}\n'
        f'Descrição: {descricao}\n'
        f'SP Pipefy Nº: {as_string(payload.get("id"))}'
    )

    # Limpa "[]" residual (mesmo padrão do atualizaspbotao)
    obs = re.sub(r' - \[\]:', '', obs)
    obs = re.sub(r'-\[\]', '', obs)
    return obs


def _aplicar_rateio_multiplo(rateio_raw, distribuicao_padrao):
    """
    Se RateioMultiplo vier preenchido, substitui distribuição e injeta categorias.
    Formato esperado:
      "distribuicao": [...]\n"categorias": [...]
    """
    import json
    txt = as_string(rateio_raw)
    if not txt:
        return distribuicao_padrao, None

    txt = txt.replace('\\"', '"')
    distribuicao = distribuicao_padrao
    categorias = None

    m_dist = re.search(r'"distribuicao"\s*:\s*(\[.*?\])', txt, re.DOTALL)
    if m_dist:
        try:
            parsed = json.loads(m_dist.group(1))
            if isinstance(parsed, list) and parsed:
                distribuicao = [d for d in parsed if not _distribuicao_vazia(d)]
                if not distribuicao:
                    distribuicao = distribuicao_padrao
        except json.JSONDecodeError:
            logger.warning('[rateio_multiplo] falha ao parsear distribuicao')

    m_cat = re.search(r'"categorias"\s*:\s*(\[.*?\])', txt, re.DOTALL)
    if m_cat:
        try:
            categorias = json.loads(m_cat.group(1))
        except json.JSONDecodeError:
            logger.warning('[rateio_multiplo] falha ao parsear categorias')

    return distribuicao, categorias


def _distribuicao_vazia(d: dict) -> bool:
    if not isinstance(d, dict):
        return True
    cod = as_string(d.get('cCodDep'))
    des = as_string(d.get('cDesDep'))
    per = d.get('nPerDep')
    val = d.get('nValDep')
    return (
        not cod and (not des or des == '[]') and
        per in (None, '', 0) and val in (None, '')
    )


def _is_erro_ja_cadastrado(body: dict) -> bool:
    """
    Detecta especificamente o erro "Lançamento já cadastrado" do Omie.

    IMPORTANTE: o Omie usa o faultcode `SOAP-ENV:Client-102` para VÁRIOS
    erros de validação distintos (categoria não cadastrada, conta corrente
    inválida, lançamento duplicado, etc.). Por isso a classificação tem
    que se basear na MENSAGEM (faultstring), não no código.

    Casos tratados como duplicidade:
      - "Lançamento já cadastrado para o Código de Integração ..."
      - "Já existe lançamento cadastrado para o código ..."
    """
    fault = as_string(body.get('faultstring'))
    if not fault:
        return False
    return bool(
        re.search(r'lan[çc]amento\s+j[aá]\s+cadastrado', fault, re.IGNORECASE) or
        re.search(r'j[aá]\s+existe\s+lan[çc]amento', fault, re.IGNORECASE)
    )


def _resultado_titulo(operacao: str, request_body: dict, resp: dict, duplicado: bool) -> dict:
    body = resp.get('body') or {}
    codigo = as_string(
        body.get('codigo_lancamento_integracao') or
        body.get('codigo_lancamento_omie') or
        body.get('codigo_lancamento')
    ) or as_string((request_body.get('param') or [{}])[0].get('codigo_lancamento_integracao'))

    mensagem = as_string(
        body.get('faultstring') or body.get('mensagem') or body.get('descricao_status') or ''
    )

    return {
        'ok':                            bool(resp.get('ok')) or duplicado,
        'duplicado':                     duplicado,
        'operacao':                      operacao,
        'codigo_lancamento_integracao':  codigo,
        'codigo_status_http':            resp.get('status'),
        'codigo_status_omie':            as_string(body.get('codigo_status') or body.get('faultcode') or ''),
        'mensagem':                      mensagem,
        'request':                       request_body,
        'response':                      resp,
    }