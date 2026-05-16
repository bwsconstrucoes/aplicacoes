# -*- coding: utf-8 -*-
"""
payload_adapter.py — Converte o payload do Make.com (estrutura nested) num dict plano
que o resto do módulo consome.

CONTRATO DE ENTRADA (vindo do Make):
{
  "secret":              "<PROCESSARNOVASP_SECRET>",
  "omieAppKey":          "...",
  "omieAppSecret":       "...",
  "omieIdContaCorrente": "583772104",
  "id":   "1234567890",
  "url":  "https://app.pipefy.com/...",
  "ia":   { "Duplicidade": "...", "Descrição": "...", "Categoria": "..." },
  "fields": {
      "Centro de Custo 1": "Nome do CC (sem colchetes)",
      "Centro de Custo 2": "...",
      ...
      "Valor Total da Despesa": "1.000,00",
      "Tipo de Despesa": "Manutenção Veicular",
      "Selecione o Procedimento": "Ordem de Pagamento",
      "Código de Barras": "...",     # já normalizado
      "Descrição da Despesa": "...", # já em base64
      "Status Vencimento": "Atende" | "Não Atende",
      "Vencimento Corrigido": "DD/MM/YYYY",
      "Número do Pedido": "PED-001" | "PED-001, PED-002",
      "AnexoLink": "https://...",
      "ContratoLocacao": "ID...",
      ...
  },
  "telemetria": { ... }  # opcional
}

CONTRATO DE SAÍDA (consumido por core.py):
Mesmo dict, mas com chaves planas em camelCase/snake_case
(CentroCusto1, ValorCentroCusto1, TipoDespesa, etc).
"""

from .utils import as_string


# Mapeamento campo Pipefy → chave interna do módulo
MAPA_FIELDS_PARA_INTERNAL = {
    # Procedimento e fluxo
    'Selecione o Procedimento':              'Procedimento',
    'Pagamento Futuro de Pedido':            'PagamentoFuturoPedido',
    'Antecipação ou Entrada de Pedido':      'AntecipacaoEntradaPedido',

    # Credor
    'Pessoa Física ou Jurídica?':            'PessoaTipo',
    'Nome do Credor':                        'NomeCredor',
    'CPF do Credor':                         'CPFCredor',
    'CNPJ do Credor':                        'CNPJCredor',

    # Despesa
    'Tipo de Despesa':                       'TipoDespesa',
    'Tipo de Pagamento':                     'TipoPagamento',
    'Valor Total da Despesa':                'ValorTotalDespesa',
    'Valor Total Pago':                      'ValorTotalPago',  # nem sempre vem
    'Descrição da Despesa':                  'DescricaoDespesa',
    'Banco do Pagamento':                    'BancoPagamento',
    'Número do Pedido':                      'NumeroPedido',
    'Nº da Nota Fiscal':                     'NumeroNotaFiscal',
    'A despesa gerou emissão de Nota Fiscal?': 'GerouNF',
    'Código de Barras':                      'CodigoBarras',
    'AnexoLink':                             'AnexoLink',
    'ContratoLocacao':                       'ContratoLocacao',
    'Validação SP':                          'ValidacaoSP',
    'Anuente':                               'Anuente',
    'Automação Form':                        'AutomacaoForm',
    'Veículo/Máquina':                       'VeiculoMaquina',
    'Alimentação de Equipe por Fornecedor Terceirizado?': 'AlimentacaoTerceirizada',

    # Solicitação
    'Nº SP':                                 'NumeroSP',
    'Nº da Solicitação':                     'NumeroSolicitacao',
    'Data da Solicitação':                   'DataSolicitacao',
    'Data da Solicitação Mais Antiga':       'DataSolicitacaoMaisAntiga',
    'Data para Resposta':                    'DataParaResposta',
    'Descrição da Solicitação':              'DescricaoSolicitacao',
    'Motivo':                                'Motivo',

    # Vencimento
    'Data de Vencimento':                    'DataVencimento',
    'Vencimento Corrigido':                  'VencimentoCorrigido',
    'Status Vencimento':                     'StatusVencimento',
    'Data do Pagamento':                     'DataPagamento',

    # Parcelas (até 10)
    'Quantidade de Parcelas':                'QuantidadeParcelas',

    # Solicitante
    'Responsável pela Solicitação':          'ResponsavelSolicitacao',
    'Requerente':                            'Requerente',
    'Requisição Solicitada por um Terceiro?':'RequisicaoTerceiro',

    # Rateio CC
    'Ratear entre mais de um Centro de Custo?': 'RateioMultiCC',
    'Rateio Múltiplo':                          'RateioMultiplo',
    'Rateio Múltiplo entre Centros de Custo?':  'RateioMultiCC2',
    'Centro de Custo 1': 'CentroCusto1',
    'Centro de Custo 2': 'CentroCusto2',
    'Centro de Custo 3': 'CentroCusto3',
    'Centro de Custo 4': 'CentroCusto4',
    'Centro de Custo 5': 'CentroCusto5',
    'Valor Centro de Custo 1': 'ValorCentroCusto1',
    'Valor Centro de Custo 2': 'ValorCentroCusto2',
    'Valor Centro de Custo 3': 'ValorCentroCusto3',
    'Valor Centro de Custo 4': 'ValorCentroCusto4',
    'Valor Centro de Custo 5': 'ValorCentroCusto5',

    # Dados bancários (transferência)
    'O credor é o mesmo titular da conta para transferência?': 'CredorTitular',
    'Nome do Titular da Conta':              'NomeTitularConta',
    'Sobre o titular da conta':              'SobreTitular',
    'CPF do titular da conta':               'CPFTitularConta',
    'CNPJ do titular da conta':              'CNPJTitularConta',
    'Banco':                                 'Banco',
    'Tipo de Conta':                         'TipoConta',
    'Agência':                               'Agencia',
    'Agência Dígito':                        'AgenciaDigito',
    'Conta':                                 'Conta',
    'Conta Dígito':                          'ContaDigito',

    # Pix
    'Selecione a Chave Pix a ser utilizada': 'SelecioneChavePix',
    'Chave Pix Email':                       'ChavePixEmail',
    'Chave Pix Telefone':                    'ChavePixTelefone',
    'Chave Pix CPF':                         'ChavePixCPF',
    'Chave Pix CNPJ':                        'ChavePixCNPJ',
    'Chave Pix Aleatória':                   'ChavePixAleatoria',

    # Anexos (vêm como tags textuais: semanexo / anexounico / anexomultiplo)
    'Anexos':                                'AnexosTag',
    'Anexo Prestação de Conta':              'AnexoPrestacaoTag',
    'Anexos Link':                           'AnexosLink',
    'Link para Planilha de Análise':         'LinkPlanilhaAnalise',
    'Conexão DC1':                           'ConexaoDC1',
}


def adaptar(payload: dict) -> dict:
    """
    Recebe payload do Make (com 'fields' aninhado) e retorna dict plano
    com chaves internas + metadados de top-level.
    """
    if not isinstance(payload, dict):
        return {}

    # Se vier no formato antigo (sem 'fields'), assume que já está plano
    if 'fields' not in payload:
        return dict(payload)

    plano = {
        'id':                  as_string(payload.get('id')),
        'url':                 as_string(payload.get('url')),
        'secret':              as_string(payload.get('secret')),
        'omieAppKey':          as_string(payload.get('omieAppKey')),
        'omieAppSecret':       as_string(payload.get('omieAppSecret')),
        'omieIdContaCorrente': as_string(payload.get('omieIdContaCorrente') or '583772104'),
        'registrar_telemetria': payload.get('registrar_telemetria'),
        'telemetria':          payload.get('telemetria') or {},
    }

    fields = payload.get('fields') or {}
    for nome_pipefy, chave_interna in MAPA_FIELDS_PARA_INTERNAL.items():
        valor = fields.get(nome_pipefy)
        plano[chave_interna] = '' if valor is None else valor

    # NOTA: Parcelas (Data de Vencimento 1..10 + Valor Parcela 1..10) e
    # Códigos de Barras múltiplos (Código de Barras 1..10) NÃO são tratados
    # aqui — pertencem ao cenário "Processar SPs Parceladas" (separado).
    # Este módulo processa SOMENTE SPs de 1x.

    # IA (top-level no Make)
    ia = payload.get('ia') or {}
    plano['IA_Duplicidade'] = as_string(ia.get('Duplicidade'))
    plano['IA_Categoria']   = as_string(ia.get('Categoria'))
    plano['IA_Descricao']   = as_string(ia.get('Descrição'))

    # RateioMultiCC vem em 2 campos diferentes — usa o que estiver presente
    if not as_string(plano.get('RateioMultiCC')) and as_string(plano.get('RateioMultiCC2')):
        plano['RateioMultiCC'] = plano['RateioMultiCC2']

    return plano
