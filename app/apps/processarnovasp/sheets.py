# -*- coding: utf-8 -*-
"""
sheets.py — Escritas em SPsBD (4 variantes), Log e FalhaProcessar.

Variantes SPsBD (uma é escolhida em runtime):
  - 489: Transferência de Recursos (sem código Omie)
  - 642: Pagamento Futuro / Antecipação (sem código Omie, com boleto)
  - 411: Fluxo padrão, fornecedor novo  (com {{224.data.codigo_lancamento_integracao}})
  - 626: Fluxo padrão, fornecedor cadastrado (com {{610.data.codigo_lancamento_integracao}})

A coluna 15 (P) carrega o código Omie quando aplicável.
A coluna 30 (AE) carrega o anuente.
A coluna 34 (AI) carrega o código de barras (com prefixo INVALIDO se boleto inválido).

A aba Log recebe 1 linha por Centro de Custo preenchido.
"""

import re
import logging
from datetime import datetime
from .utils import (
    as_string, limpar_colchetes, limpar_documento, has_value,
    to_number_br, number_to_br, parse_data_pipefy, formatar_data_br,
    primeiro_token_dash, mes_ano_br, decodificar_b64_inline,
)

logger = logging.getLogger(__name__)

PLANILHA_PRINCIPAL = '1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA'
ABA_SPSBD          = 'SPsBD'
ABA_LOG            = 'Log'
ABA_FALHA          = 'FalhaProcessar'

# Empresa BWS — usada no campo CPF quando é Transferência de Recursos
EMPRESA_CPF_PADRAO = '00.079.526/0001-09'
EMPRESA_NOME       = 'BWS CONSTRUÇÕES LTDA'


# -----------------------------------------------------------------------------
# Telemetria do Make (módulo 704) — opcional, vai pra planilha de logs cosméticos
# -----------------------------------------------------------------------------

PLANILHA_TELEMETRIA = '1cV4J5pYmECmyGofarOWBcpA5Rp4pwl--LN25hHClOU8'
ABA_TELEMETRIA      = 'Página1'


def registrar_telemetria(gc, contexto: dict) -> dict:
    """
    Append cosmético na planilha de telemetria (equivale ao módulo 704 do Make).
    contexto deve trazer: scenario_id, scenario_name, execution_id, etc.
    Se faltar, grava só com os campos que vierem.
    """
    try:
        sh = gc.open_by_key(PLANILHA_TELEMETRIA).worksheet(ABA_TELEMETRIA)
        row = [
            as_string(contexto.get('creditsConsumed')),
            as_string(contexto.get('dataConsumed')),
            as_string(contexto.get('executionId')),
            as_string(contexto.get('executionStartedAt') or datetime.now().isoformat()),
            as_string(contexto.get('executionType') or 'render'),
            as_string(contexto.get('executionUrl')),
            as_string(contexto.get('scenario_id') or 'processarnovasp'),
            as_string(contexto.get('isDLQExecution') or 'false'),
            as_string(contexto.get('scenario_name') or 'FIN - Processar SP (Render)'),
            as_string(contexto.get('operationsConsumed') or '1'),
            as_string(contexto.get('url')),
            as_string(contexto.get('team_id')),
            as_string(contexto.get('team_name')),
            as_string(contexto.get('dataLeft')),
            as_string(contexto.get('organization_id')),
            as_string(contexto.get('organization_name') or 'BWS'),
            as_string(contexto.get('operationsLeft')),
            as_string(contexto.get('zoneDomain')),
        ]
        sh.append_row(row, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS')
        return {'ok': True}
    except Exception as e:
        logger.warning(f'[telemetria] falhou: {e}')
        return {'ok': False, 'erro': str(e)}


# =============================================================================
# SPsBD — escolhe variante com base no rota_e_omie
# =============================================================================

def inserir_spsbd(gc, payload: dict, rota: str,
                  omie_secao: dict = None, boleto_secao: dict = None) -> dict:
    """
    rota = 'transferencia' | 'pagamento_futuro' | 'padrao'
    omie_secao usado quando rota='padrao' para extrair código Omie.
    """
    ss = gc.open_by_key(PLANILHA_PRINCIPAL)
    sh = ss.worksheet(ABA_SPSBD)

    if rota == 'transferencia':
        row = _build_row_489(payload)
    elif rota == 'pagamento_futuro':
        row = _build_row_642(payload, boleto_secao)
    elif rota == 'padrao':
        row = _build_row_411_626(payload, omie_secao, boleto_secao)
    else:
        raise ValueError(f'rota inválida: {rota}')

    sh.append_row(row, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS')
    return {'ok': True, 'rota': rota, 'colunas_preenchidas': sum(1 for c in row if c != '')}


# -----------------------------------------------------------------------------
# Variantes
# -----------------------------------------------------------------------------

def _row_base(payload: dict, num_cols: int = 38) -> list:
    """Esqueleto de linha SPsBD com tamanho fixo."""
    return [''] * num_cols


def _set(row: list, idx: int, value):
    if idx < len(row):
        row[idx] = '' if value is None else value


def _resolver_data_venc_str(payload: dict) -> str:
    status   = as_string(payload.get('StatusVencimento') or '')
    tipo_pag = as_string(payload.get('TipoPagamento') or '')
    if status == 'Não Atende' and tipo_pag != 'Boleto':
        vc = as_string(payload.get('VencimentoCorrigido') or '')
        if vc:
            return vc
    return formatar_data_br(parse_data_pipefy(payload.get('DataVencimento') or ''))


def _nome_credor(payload: dict, rota_transf: bool = False) -> str:
    if rota_transf or as_string(payload.get('Procedimento')) == 'Transferência de Recursos':
        return EMPRESA_NOME
    return as_string(payload.get('NomeCredor') or '').upper()


def _doc_credor(payload: dict, rota_transf: bool = False) -> str:
    if rota_transf or as_string(payload.get('Procedimento')) == 'Transferência de Recursos':
        return EMPRESA_CPF_PADRAO
    pessoa_tipo = as_string(payload.get('PessoaTipo') or '')
    cpf  = as_string(payload.get('CPFCredor') or '')
    cnpj = as_string(payload.get('CNPJCredor') or '')
    if pessoa_tipo == 'Pessoa Física':
        return cpf
    return cnpj or cpf


def _formato_centro_custo_col7(payload: dict) -> str:
    """
    Coluna 7 (H) do SPsBD = lista de centros de custo concatenados, OU "CONS" se rateio
    múltiplo / transferência.
    """
    rateio_multi = as_string(payload.get('RateioMultiplo') or '')
    rateio_mais1 = as_string(payload.get('RateioMultiCC') or '')
    procedimento = as_string(payload.get('Procedimento') or '')

    if rateio_multi and rateio_mais1 == 'Sim':
        return 'CONS'
    if procedimento == 'Transferência de Recursos' or rateio_multi:
        return 'CONS'

    nomes = []
    for i in range(1, 6):
        n = limpar_colchetes(payload.get(f'CentroCusto{i}') or '')
        if n and n != '[]':
            nomes.append(n)
    return ', '.join(nomes)


def _formato_tipo_despesa_col8(payload: dict) -> str:
    proced = as_string(payload.get('Procedimento') or '')
    if proced == 'Fundo Fixo':
        return 'Fundo Fixo'
    return limpar_colchetes(payload.get('TipoDespesa') or '')


def _formato_tipo_pagto_col9(payload: dict, override_transf: bool = False) -> str:
    proced = as_string(payload.get('Procedimento') or '')
    if proced == 'Fundo Fixo':
        return 'Pix'
    if proced == 'Transferência de Recursos':
        return 'Transferência Bancária' if override_transf else 'Pix'
    return as_string(payload.get('TipoPagamento') or '')


def _formato_responsavel_col10(payload: dict) -> str:
    return primeiro_token_dash(payload.get('ResponsavelSolicitacao') or '')


def _formato_anuente_col30(payload: dict) -> str:
    return primeiro_token_dash(payload.get('Anuente') or '')


def _formato_dados_pagto_col24(payload: dict) -> str:
    """
    Replica fórmula longa do Make:
    - Boleto → "Boleto"
    - Transferência Bancária → "Titular: NOME - DOC / BANCO / TIPO / AG-DIG/CONTA-DIG"
    - Pix / Fundo Fixo / BeeVale → "Chave Pix: <chave>"
    """
    tipo = as_string(payload.get('TipoPagamento') or '')
    proc = as_string(payload.get('Procedimento') or '')

    if tipo == 'Boleto':
        return 'Boleto'

    if tipo == 'Transferência Bancária':
        titular  = as_string(payload.get('NomeTitularConta'))
        cpf_t    = as_string(payload.get('CPFTitularConta'))
        cnpj_t   = as_string(payload.get('CNPJTitularConta'))
        banco    = limpar_colchetes(payload.get('Banco'))
        tipo_c   = as_string(payload.get('TipoConta'))
        ag       = as_string(payload.get('Agencia'))
        ag_dig   = as_string(payload.get('AgenciaDigito'))
        conta    = as_string(payload.get('Conta'))
        conta_dig = as_string(payload.get('ContaDigito'))
        doc = cpf_t or cnpj_t
        return (f'Titular: {titular}  - {doc} / {banco} / {tipo_c} / '
                f'{ag}-{ag_dig}/{conta}-{conta_dig}')

    if tipo in ('Pix', 'BeeVale') or proc == 'Fundo Fixo':
        sel = as_string(payload.get('SelecioneChavePix'))
        chaves = {
            'E-mail':    as_string(payload.get('ChavePixEmail')),
            'Telefone':  as_string(payload.get('ChavePixTelefone')),
            'CPF':       as_string(payload.get('ChavePixCPF')),
            'CNPJ':      as_string(payload.get('ChavePixCNPJ')),
            'Aleatória': as_string(payload.get('ChavePixAleatoria')),
        }
        return f'Chave Pix: {chaves.get(sel, "")}'

    return ''


def _formato_nf_col26(payload: dict) -> str:
    nf = as_string(payload.get('NumeroNotaFiscal') or '')
    if not nf:
        return ''
    sanit = re.sub(r'[^\d]', '', nf)
    try:
        n = int(sanit)
        return str(n) if n > 0 else ''
    except ValueError:
        return ''


def _formato_codigo_barras_col34(payload: dict, boleto_secao: dict) -> str:
    cod = as_string(payload.get('CodigoBarras') or '')
    if not cod:
        return ''
    if boleto_secao and boleto_secao.get('executado') and boleto_secao.get('valido') is False:
        return f'INVALIDO{cod}'
    return cod


def _formato_anexo_col16(payload: dict) -> str:
    anexo = as_string(payload.get('AnexoLink') or '')
    if 'http' in anexo:
        return anexo
    if anexo:
        import base64
        try:
            return base64.b64encode(anexo.encode('utf-8')).decode('utf-8')
        except Exception:
            return anexo
    return ''


# -----------------------------------------------------------------------------
# 489 — Transferência de Recursos
# -----------------------------------------------------------------------------

def _build_row_489(payload: dict) -> list:
    row = _row_base(payload)
    sp_id = as_string(payload.get('id'))

    _set(row, 0,  sp_id)
    _set(row, 1,  datetime.now().strftime('%d/%m/%Y'))
    _set(row, 2,  _resolver_data_venc_str(payload))
    _set(row, 3,  _nome_credor(payload, rota_transf=True))
    _set(row, 4,  _doc_credor(payload, rota_transf=True))
    _set(row, 5,  decodificar_b64_inline(as_string(payload.get('DescricaoDespesa'))))
    _set(row, 6,  as_string(payload.get('ValorTotalDespesa')))
    _set(row, 7,  _formato_centro_custo_col7(payload))
    _set(row, 8,  _formato_tipo_despesa_col8(payload))
    _set(row, 9,  _formato_tipo_pagto_col9(payload, override_transf=False))
    _set(row, 10, _formato_responsavel_col10(payload))

    valor = to_number_br(payload.get('ValorTotalDespesa') or 0)
    # cols 11-13 dependem do valor
    if valor >= 2000.01:
        _set(row, 11, '')
        _set(row, 12, '')
        _set(row, 13, 'Autorizar')
    else:
        _set(row, 11, datetime.now().strftime('%d/%m/%Y'))
        _set(row, 12, 'PRÉ-AUTORIZADO')
        _set(row, 13, 'Pré-Autorizado')

    # Trabalhista força Autorizar
    if 'Trabalhista' in as_string(payload.get('TipoDespesa') or ''):
        _set(row, 11, '')
        _set(row, 12, '')
        _set(row, 13, 'Autorizar')

    _set(row, 14, 'Pagar')
    _set(row, 17, f'https://app.pipefy.com/open-cards/{sp_id}')
    _set(row, 24, _formato_dados_pagto_col24(payload))
    _set(row, 26, _formato_nf_col26(payload))
    _set(row, 33, 'Sim' if as_string(payload.get('ValidacaoSP')) == 'Sim' else '')
    _set(row, 37, as_string(payload.get('IA_Descricao')))
    return row


# -----------------------------------------------------------------------------
# 642 — Pagamento Futuro / Antecipação
# -----------------------------------------------------------------------------

def _build_row_642(payload: dict, boleto_secao: dict) -> list:
    """
    Pagamento Futuro / Antecipação:
    - Credor real (não BWS).
    - Tipo de Despesa SEM override de Fundo Fixo (módulo 642 ignora isso).
    - Tipo de Pagamento real (módulo 642 não força Pix).
    - Cols 11-13 dependem só do valor (sem override de trabalhista nem de 1.id != null).
    """
    row = _row_base(payload)
    sp_id = as_string(payload.get('id'))

    _set(row, 0,  sp_id)
    _set(row, 1,  datetime.now().strftime('%d/%m/%Y'))
    _set(row, 2,  _resolver_data_venc_str(payload))
    _set(row, 3,  _nome_credor(payload))                # credor real
    _set(row, 4,  _doc_credor(payload))                 # doc real
    _set(row, 5,  decodificar_b64_inline(as_string(payload.get('DescricaoDespesa'))))
    _set(row, 6,  as_string(payload.get('ValorTotalDespesa')))
    _set(row, 7,  _formato_centro_custo_col7(payload))
    _set(row, 8,  limpar_colchetes(payload.get('TipoDespesa') or ''))  # sem override Fundo Fixo
    _set(row, 9,  _formato_tipo_pagto_col9(payload, override_transf=True))
    _set(row, 10, _formato_responsavel_col10(payload))

    valor = to_number_br(payload.get('ValorTotalDespesa') or 0)
    if valor >= 2000.01:
        _set(row, 11, '')
        _set(row, 12, '')
        _set(row, 13, 'Autorizar')
    else:
        _set(row, 11, datetime.now().strftime('%d/%m/%Y'))
        _set(row, 12, 'PRÉ-AUTORIZADO')
        _set(row, 13, 'Pré-Autorizado')

    _set(row, 14, 'Pagar')
    _set(row, 17, f'https://app.pipefy.com/open-cards/{sp_id}')
    _set(row, 24, _formato_dados_pagto_col24(payload))
    _set(row, 26, _formato_nf_col26(payload))
    _set(row, 29, as_string(payload.get('NumeroPedido')))
    _set(row, 33, 'Sim' if as_string(payload.get('ValidacaoSP')) == 'Sim' else '')
    _set(row, 34, _formato_codigo_barras_col34(payload, boleto_secao))
    _set(row, 37, as_string(payload.get('IA_Descricao')))
    return row


# -----------------------------------------------------------------------------
# 411 / 626 — Fluxo padrão (com código Omie)
# -----------------------------------------------------------------------------

def _build_row_411_626(payload: dict, omie_secao: dict, boleto_secao: dict) -> list:
    row = _row_base(payload)
    sp_id = as_string(payload.get('id'))

    _set(row, 0,  sp_id)
    _set(row, 1,  datetime.now().strftime('%d/%m/%Y'))
    _set(row, 2,  _resolver_data_venc_str(payload))
    _set(row, 3,  _nome_credor(payload))
    _set(row, 4,  _doc_credor(payload))
    _set(row, 5,  decodificar_b64_inline(as_string(payload.get('DescricaoDespesa'))))
    _set(row, 6,  as_string(payload.get('ValorTotalDespesa')))
    _set(row, 7,  _formato_centro_custo_col7(payload))
    _set(row, 8,  _formato_tipo_despesa_col8(payload))

    # col 9: Fundo Fixo→Pix; Transferência→Transferência Bancária; default→TipoPagamento
    proc = as_string(payload.get('Procedimento') or '')
    if proc == 'Fundo Fixo':
        _set(row, 9, 'Pix')
    elif proc == 'Transferência de Recursos':
        _set(row, 9, 'Transferência Bancária')
    else:
        _set(row, 9, as_string(payload.get('TipoPagamento')))

    _set(row, 10, _formato_responsavel_col10(payload))
    # Vindo do Omie 411/626, se 1.id existir, é sempre PRÉ-AUTORIZADO
    _set(row, 11, datetime.now().strftime('%d/%m/%Y'))
    _set(row, 12, 'PRÉ-AUTORIZADO')
    _set(row, 13, 'Pré-Autorizado')
    _set(row, 14, 'Pagar')

    # col 15: código Omie
    codigo_omie = ''
    if omie_secao and isinstance(omie_secao.get('titulo'), dict):
        codigo_omie = as_string(omie_secao['titulo'].get('codigo_lancamento_integracao'))
    _set(row, 15, codigo_omie)

    _set(row, 16, _formato_anexo_col16(payload))
    _set(row, 17, f'https://app.pipefy.com/open-cards/{sp_id}')
    _set(row, 24, _formato_dados_pagto_col24(payload))
    _set(row, 26, _formato_nf_col26(payload))
    _set(row, 29, as_string(payload.get('NumeroPedido')))
    _set(row, 30, _formato_anuente_col30(payload))
    _set(row, 33, 'Sim' if as_string(payload.get('ValidacaoSP')) == 'Sim' else '')
    _set(row, 34, _formato_codigo_barras_col34(payload, boleto_secao))
    _set(row, 35, as_string(payload.get('ContratoLocacao')))
    _set(row, 37, as_string(payload.get('IA_Descricao')))

    return row


# =============================================================================
# Log (append)
# =============================================================================

def inserir_log(gc, payload: dict, rateio_descritivo: dict) -> dict:
    """
    Append em aba Log: 1 linha por Centro de Custo preenchido.
    Equivalente aos módulos 731 (transferência/pag.fut) e 738 (fluxo principal).
    """
    ss = gc.open_by_key(PLANILHA_PRINCIPAL)
    sh = ss.worksheet(ABA_LOG)

    sp_id      = as_string(payload.get('id'))
    hoje       = datetime.now().strftime('%d/%m/%Y')
    data_venc  = _resolver_data_venc_str(payload)
    nome       = _nome_credor(payload)
    doc        = _doc_credor(payload)

    proc          = as_string(payload.get('Procedimento') or '')
    tipo_despesa  = _formato_tipo_despesa_col8(payload)
    descricao_b64 = decodificar_b64_inline(as_string(payload.get('DescricaoDespesa')))
    veiculo       = limpar_colchetes(payload.get('VeiculoMaquina') or '').upper()
    competencia   = mes_ano_br(datetime.now().date())

    # Quem é o "I" (Requerente ou Responsável)
    if as_string(payload.get('RequisicaoTerceiro') or '') == 'Sim':
        responsavel_log = primeiro_token_dash(payload.get('Requerente'))
    else:
        responsavel_log = primeiro_token_dash(payload.get('ResponsavelSolicitacao'))
    responsavel_log = responsavel_log.upper()

    tipo_pgto = as_string(payload.get('TipoPagamento') or '')
    if proc == 'Fundo Fixo':
        tipo_pgto = 'Pix'

    # G da primeira linha — sobreescrita por CONS se necessário
    rateio_multi  = as_string(payload.get('RateioMultiplo') or '')
    rateio_mais1  = as_string(payload.get('RateioMultiCC') or '')
    is_consolidado = (
        proc == 'Transferência de Recursos' or
        (rateio_multi and rateio_mais1 == 'Sim') or
        as_string(payload.get('AutomacaoForm') or '') == 'BeeVale'
    )

    centros = rateio_descritivo.get('centros_nomes', [])
    valores = rateio_descritivo.get('valores_cc', [])

    rows = []
    for i, centro in enumerate(centros):
        if not centro:
            continue

        if i == 0 and is_consolidado:
            g_val = 'CONS'
        else:
            g_val = centro.upper()

        h_val = number_to_br(valores[i] if i < len(valores) else 0)

        # row = lista alinhada às colunas (A=0..U=20)
        row = [''] * 22
        row[0]  = sp_id                              # A
        row[1]  = hoje                               # B
        row[2]  = data_venc                          # C
        row[3]  = nome                               # D
        row[4]  = doc                                # E
        row[5]  = tipo_despesa                       # F
        row[6]  = g_val                              # G
        row[7]  = h_val                              # H
        row[8]  = responsavel_log                    # I
        row[11] = competencia                        # L
        row[12] = descricao_b64                      # M
        row[13] = veiculo                            # N
        row[18] = f'Centro de Custo {i + 1}'         # S
        row[20] = tipo_pgto                          # U
        rows.append(row)

    if rows:
        sh.append_rows(rows, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS')

    return {'ok': True, 'linhas_inseridas': len(rows)}


# =============================================================================
# FalhaProcessar (append) — quando Omie retorna API bloqueada ou erro genérico
# =============================================================================

def registrar_falha_processar(gc, payload: dict, motivo: str = 'Cadastrar Título Omie') -> dict:
    ss = gc.open_by_key(PLANILHA_PRINCIPAL)
    sh = ss.worksheet(ABA_FALHA)
    sp_id = as_string(payload.get('id'))
    agora = datetime.now().strftime('%d/%m/%Y %H:%M')
    sh.append_row([sp_id, agora, motivo],
                  value_input_option='USER_ENTERED',
                  insert_data_option='INSERT_ROWS')
    return {'ok': True}
