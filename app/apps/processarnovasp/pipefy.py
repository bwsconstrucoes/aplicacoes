# -*- coding: utf-8 -*-
"""
pipefy.py — Mutations GraphQL no Pipefy para atualizar o card após o Omie.

Cobre os 4 cenários do blueprint:
  - 607 (label Title, Autoriz., Etiqueta): rota R1 — Pagamento Futuro pré-Omie
  - 601 (após 224 IncluirContaPagar fornecedor novo): atualiza tudo + cód Omie
  - 613 (após 610 IncluirContaPagar fornecedor cadastrado): idem
  - 628 (rota R1 — Pagamento Futuro / Antecipação, após sheets): título + etiq especial

Os 4 fazem o mesmo conjunto + variações condicionais. Aqui resolvemos
todas as condições em Python (não em fórmula Make) e montamos mutation única.
"""

import os
import re
import logging
import requests
from .utils import as_string, limpar_colchetes, to_number_br, parse_data_pipefy, formatar_data_br

logger = logging.getLogger(__name__)

PIPEFY_URL = 'https://api.pipefy.com/graphql'


# Mapa de etiquetas confirmadas pelo blueprint
ETIQUETAS = {
    'Cancelada':                  '304753657',
    'Ordem de Pagamento':         '305263439',
    'Transferência de Recursos':  '307726886',
    'Rescisões e Indenizações':   '307726895',
    'Antecipação de Pagamento':   '309483248',
    'Pagamento Futuro':           '310655392',
    'Fundo Fixo':                 '310918018',
    'Análise Criteriosa':         '316061620',
    'Boleto':                     '313978748',
}


# -----------------------------------------------------------------------------
# Funções públicas
# -----------------------------------------------------------------------------

def atualizar_card_pos_omie(payload: dict, omie_secao: dict, boleto_secao: dict,
                              pedidos_vinculados: list = None) -> dict:
    """
    Equivalente aos blocos 601/613 do Make (fluxo principal R2 do router 6),
    JÁ INCLUINDO o updateCardField de conex_o_sp para CADA pedido vinculado
    (que no Make era feito em uma mutation separada por pedido em 694/695).

    Tudo em UMA ÚNICA mutation GraphQL → 1 round-trip HTTP.

    pedidos_vinculados: lista de dicts {'card_pedido': '<card_id>', 'pedido': '<num>'}
                        retornada por pedidos.vincular().
    """
    sp_id = as_string(payload.get('id'))
    if not sp_id:
        raise ValueError('id da SP é obrigatório')

    titulo_pipefy = _resolver_titulo_card(payload)
    codigo_omie   = as_string((omie_secao.get('titulo') or {}).get('codigo_lancamento_integracao'))

    valor_total      = to_number_br(payload.get('ValorTotalDespesa') or 0)
    tipo_despesa     = limpar_colchetes(payload.get('TipoDespesa') or '')
    procedimento     = as_string(payload.get('Procedimento') or '')
    tipo_pagamento   = as_string(payload.get('TipoPagamento') or '')
    cod_barras       = as_string(payload.get('CodigoBarras') or '')
    boleto_valido    = bool((boleto_secao or {}).get('valido'))
    boleto_invalido  = ((boleto_secao or {}).get('executado')
                        and (boleto_secao or {}).get('valido') is False)
    numero_pedido    = as_string(payload.get('NumeroPedido') or '')

    parts = []
    # n1 — title
    parts.append(_mut_update_card(sp_id, titulo_pipefy))
    # n2 — data_de_pagamento
    parts.append(_mut_field(sp_id, 'data_de_pagamento', titulo_pipefy))
    # n3 — código Omie
    if codigo_omie:
        parts.append(_mut_field(sp_id, 'c_digo_lan_amento_integra_o_omie', codigo_omie))
    # n4 — autorização dupla se ≤ 2000
    if valor_total <= 2000:
        parts.append(_mut_field(sp_id, 'autoriza_o_dupla', 'SIM'))
    # n5 — etiquetas
    etiq_value = _etiquetas_pos_omie(tipo_despesa, procedimento, boleto_valido)
    if etiq_value:
        parts.append(_mut_field_raw(sp_id, 'etiquetas', etiq_value))
    # n6 — número do pedido
    if numero_pedido:
        parts.append(_mut_field(sp_id, 'n_mero_do_pedido_de_suprimentos', numero_pedido))
    # n7 — IA
    ia_txt = _montar_texto_ia(payload)
    if ia_txt:
        parts.append(_mut_field(sp_id, 'intelig_ncia_artificial', ia_txt))
    # n8-n11 — boleto válido → baixa automática + parcelas + código de barras
    if boleto_valido:
        valor_bol = ((boleto_secao or {}).get('detalhes') or {}).get('valor') or 0
        parts.append(_mut_field(sp_id, 'baixa_autom_tica', 'Sim'))
        parts.append(_mut_field(sp_id, 'quantidade_de_parcelas', 'Boleto'))
        parts.append(_mut_field(sp_id, 'c_digo_de_barras', cod_barras))
        parts.append(_mut_field(sp_id, 'c_digo_de_barras_11',
                                 f'{cod_barras}-{valor_bol:.2f}'.replace('.', ',')))
    elif boleto_invalido:
        parts.append(_mut_field(sp_id, 'c_digo_de_barras_11', f'INVALIDO{cod_barras}'))
    # BeeVale → chave pix aleatória
    if tipo_pagamento == 'BeeVale':
        parts.append(_mut_field(sp_id, 'tipo', 'Aleatória'))
        parts.append(_mut_field(sp_id, 'chave_pix_aleat_ria', 'Atualizar Chave Pix'))

    # Pedidos vinculados — updates dos CARDS DE PEDIDO (não do card SP atual)
    # Equivalente aos módulos 694/695 do Make, mas agora dentro da MESMA mutation
    # do card pós-Omie. 1 round-trip HTTP para tudo.
    for ped in (pedidos_vinculados or []):
        card_pedido = as_string(ped.get('card_pedido'))
        if card_pedido:
            parts.append(_mut_field(card_pedido, 'conex_o_sp', sp_id))

    mutation = 'mutation {\n' + '\n'.join(parts) + '\n}'
    return _executar_mutation(mutation)


def atualizar_card_pagamento_futuro(payload: dict, boleto_secao: dict) -> dict:
    """
    Equivalente ao módulo 628 do Make (rota Pagamento Futuro / Antecipação).
    Sem código Omie; etiquetas específicas + dados de boleto.
    """
    sp_id = as_string(payload.get('id'))
    if not sp_id:
        raise ValueError('id da SP é obrigatório')

    titulo_pipefy   = _resolver_titulo_card(payload)
    valor_total     = to_number_br(payload.get('ValorTotalDespesa') or 0)
    pag_futuro      = as_string(payload.get('PagamentoFuturoPedido') or '') == 'Sim'
    antecipacao     = as_string(payload.get('AntecipacaoEntradaPedido') or '') == 'Sim'
    boleto_valido   = bool((boleto_secao or {}).get('valido'))
    boleto_invalido = ((boleto_secao or {}).get('executado')
                       and (boleto_secao or {}).get('valido') is False)
    cod_barras      = as_string(payload.get('CodigoBarras') or '')

    parts = []
    parts.append(_mut_update_card(sp_id, titulo_pipefy))
    parts.append(_mut_field(sp_id, 'data_de_pagamento', titulo_pipefy))
    if valor_total <= 2000:
        parts.append(_mut_field(sp_id, 'autoriza_o_dupla', 'SIM'))

    ia_txt = _montar_texto_ia(payload)
    if ia_txt:
        parts.append(_mut_field(sp_id, 'intelig_ncia_artificial', ia_txt))

    if boleto_valido:
        if pag_futuro:
            etiq = f'["{ETIQUETAS["Pagamento Futuro"]}","{ETIQUETAS["Boleto"]}"]'
        elif antecipacao:
            etiq = f'["{ETIQUETAS["Antecipação de Pagamento"]}","{ETIQUETAS["Boleto"]}"]'
        else:
            etiq = None
        if etiq:
            parts.append(_mut_field_raw(sp_id, 'etiquetas', etiq))

        valor_bol = ((boleto_secao or {}).get('detalhes') or {}).get('valor') or 0
        parts.append(_mut_field(sp_id, 'baixa_autom_tica', 'Sim'))
        parts.append(_mut_field(sp_id, 'quantidade_de_parcelas', 'Boleto'))
        parts.append(_mut_field(sp_id, 'c_digo_de_barras', cod_barras))
        parts.append(_mut_field(sp_id, 'c_digo_de_barras_11',
                                 f'{cod_barras}-{valor_bol:.2f}'.replace('.', ',')))
    elif boleto_invalido:
        parts.append(_mut_field(sp_id, 'c_digo_de_barras_11', f'INVALIDO{cod_barras}'))

    mutation = 'mutation {\n' + '\n'.join(parts) + '\n}'
    return _executar_mutation(mutation)


def atualizar_card_transferencia(payload: dict) -> dict:
    """
    Equivalente ao módulo 607 do Make (rota Transferência de Recursos).
    Title + autorização dupla + etiqueta de transferência.
    """
    sp_id = as_string(payload.get('id'))
    if not sp_id:
        raise ValueError('id da SP é obrigatório')

    titulo_pipefy = _resolver_titulo_card(payload)
    valor_total   = to_number_br(payload.get('ValorTotalDespesa') or 0)
    tipo_despesa  = limpar_colchetes(payload.get('TipoDespesa') or '')

    parts = []
    parts.append(_mut_update_card(sp_id, titulo_pipefy))

    # Autorização dupla se ≤ 2000 E não-trabalhista
    if valor_total <= 2000 and 'Trabalhista' not in tipo_despesa:
        parts.append(_mut_field(sp_id, 'autoriza_o_dupla', 'SIM'))

    # Etiquetas: Transferência sempre; + Rescisões se trabalhista
    if 'Trabalhista' in tipo_despesa:
        etiq = f'["{ETIQUETAS["Transferência de Recursos"]}","{ETIQUETAS["Rescisões e Indenizações"]}"]'
    else:
        etiq = f'["{ETIQUETAS["Transferência de Recursos"]}"]'
    parts.append(_mut_field_raw(sp_id, 'etiquetas', etiq))

    ia_txt = _montar_texto_ia(payload)
    if ia_txt:
        parts.append(_mut_field(sp_id, 'intelig_ncia_artificial', ia_txt))

    mutation = 'mutation {\n' + '\n'.join(parts) + '\n}'
    return _executar_mutation(mutation)


def criar_card_cancelar_sp(payload: dict, sp_duplicada: str) -> dict:
    """
    Equivalente ao módulo 730 do Make.
    Cria card "Cancelar SP" no pipeline 301426645 informando duplicidade de boleto.
    """
    sp_id = as_string(payload.get('id'))
    pipe_id = '301426645'
    motivo = (f'O código de barras informado já havia sido utilizado na SP {sp_duplicada}. '
              f'Verifique com mais detalhe as informações lançadas.')
    from datetime import datetime
    hoje = datetime.now().strftime('%d/%m/%Y')

    mutation = f"""
    mutation {{
      createCard(input: {{
        pipe_id: {pipe_id},
        title: "{hoje}",
        fields_attributes: [
          {{field_id: "data",                       field_value: "{hoje}"}},
          {{field_id: "selecione_o_procedimento",   field_value: "Cancelar SP"}},
          {{field_id: "n_da_solicita_o",            field_value: "{sp_id}"}},
          {{field_id: "motivo",                     field_value: "{motivo}"}},
          {{field_id: "colaborador_solicitante",    field_value: "383926903"}}
        ]
      }}) {{
        card {{ id title }}
      }}
    }}"""
    return _executar_mutation(mutation)


def conectar_sp_a_pedido(card_id_pedido: str, id_sp: str) -> dict:
    """
    DEPRECATED: Esta função fazia 1 chamada HTTP por pedido vinculado.
    Foi substituída pela integração do updateCardField(conex_o_sp) dentro da
    mutation única de `atualizar_card_pos_omie` (passando `pedidos_vinculados`).

    Mantida apenas para compatibilidade caso algum outro módulo a chame.
    Não deve mais ser usada pelo fluxo principal.
    """
    if not card_id_pedido or not id_sp:
        return {'ok': False, 'erro': 'card_id_pedido ou id_sp vazio'}
    mutation = f"""
    mutation {{
      n1: updateCardField(input: {{
        card_id: {card_id_pedido},
        field_id: "conex_o_sp",
        new_value: "{id_sp}"
      }}) {{ clientMutationId }}
    }}"""
    return _executar_mutation(mutation)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _resolver_titulo_card(payload: dict) -> str:
    """Calcula o título do card (= data de pagamento DD/MM/YYYY)."""
    status   = as_string(payload.get('StatusVencimento') or '')
    tipo_pag = as_string(payload.get('TipoPagamento') or '')
    if status == 'Não Atende' and tipo_pag != 'Boleto':
        vc = as_string(payload.get('VencimentoCorrigido') or '')
        if vc:
            return vc
    d = parse_data_pipefy(payload.get('DataVencimento') or '')
    return formatar_data_br(d) if d else ''


def _montar_texto_ia(payload: dict) -> str:
    duplic = as_string(payload.get('IA_Duplicidade') or '')
    categ  = as_string(payload.get('IA_Categoria') or '')
    descr  = as_string(payload.get('IA_Descricao') or '')
    partes = []
    if duplic: partes.append(f'Duplicidade: {duplic}')
    if categ:  partes.append(f'Categoria: {categ}')
    if descr:  partes.append(f'Descrição: {descr}')
    return '\n'.join(partes)


def _etiquetas_pos_omie(tipo_despesa: str, procedimento: str, boleto_valido: bool) -> str:
    """
    Replica a fórmula longa do módulo 601:
      Terceiros/Outros/Outras → Análise Criteriosa [+ Boleto se válido]
      Rescisões              → Rescisões e Indenizações
      Fundo Fixo (proced)    → Fundo Fixo
      Boleto válido (default)→ Boleto
      Senão                  → ''
    """
    if any(x in tipo_despesa for x in ('Terceiros', 'Outros', 'Outras')):
        if boleto_valido:
            return f'["{ETIQUETAS["Análise Criteriosa"]}","{ETIQUETAS["Boleto"]}"]'
        return f'["{ETIQUETAS["Análise Criteriosa"]}"]'
    if 'Rescisões' in tipo_despesa:
        return f'["{ETIQUETAS["Rescisões e Indenizações"]}"]'
    if procedimento == 'Fundo Fixo':
        return f'["{ETIQUETAS["Fundo Fixo"]}"]'
    if boleto_valido:
        return f'["{ETIQUETAS["Boleto"]}"]'
    return ''


def _escape_graphql(s: str) -> str:
    """Escape de strings dentro de queries GraphQL."""
    if not s:
        return ''
    return (s
            .replace('\\', '\\\\')
            .replace('"', '\\"')
            .replace('\n', '\\n')
            .replace('\r', '\\r'))


def _mut_update_card(card_id: str, title: str) -> str:
    title_esc = _escape_graphql(title)
    return f'  n{_seq()}: updateCard(input: {{id: {card_id}, title: "{title_esc}"}}) {{ clientMutationId }}'


def _mut_field(card_id: str, field_id: str, new_value: str) -> str:
    val_esc = _escape_graphql(new_value)
    return (f'  n{_seq()}: updateCardField(input: '
            f'{{card_id: {card_id}, field_id: "{field_id}", new_value: "{val_esc}"}}) '
            f'{{ clientMutationId }}')


def _mut_field_raw(card_id: str, field_id: str, raw_value: str) -> str:
    """Para campos array como etiquetas: new_value: [...]"""
    return (f'  n{_seq()}: updateCardField(input: '
            f'{{card_id: {card_id}, field_id: "{field_id}", new_value: {raw_value}}}) '
            f'{{ clientMutationId }}')


_seq_counter = 0
def _seq() -> int:
    global _seq_counter
    _seq_counter += 1
    return _seq_counter


def _executar_mutation(mutation: str) -> dict:
    token = os.getenv('PIPEFY_API_TOKEN', '')
    try:
        resp = requests.post(
            PIPEFY_URL,
            json={'query': mutation},
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            timeout=20,
        )
        try:
            body = resp.json()
        except Exception:
            body = {'raw': resp.text}
        ok = resp.status_code == 200 and 'errors' not in body
        return {
            'ok':     ok,
            'status': resp.status_code,
            'body':   body,
            'query':  mutation,
        }
    except requests.RequestException as e:
        logger.exception('[pipefy] erro de rede')
        return {'ok': False, 'status': 0, 'body': {}, 'query': mutation, 'erro': str(e)}