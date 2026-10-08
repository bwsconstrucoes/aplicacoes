# -*- coding: utf-8 -*-
"""O conferidor SPsBD × Omie: achar a baixa que ficou pela metade.

A fila de falhas não alcança um grupo de itens: os que o reprocessamento antigo
marcou como "concluído com sucesso" **ignorando o resultado da gravação**
(defeito corrigido em 07/10/2026). Para a fila eles acabaram; a pendência real,
se existir, só aparece comparando as duas fontes — a planilha diz "Pago", o Omie
diz "Aberto".

**São DUAS direções, e o dono apontou qual importa mais** (08/10/2026):

> *"fazemos conciliação bancária diária. No sistema Omie vai estar tudo
> atualizado. O furo pode ser mais na planilha e na movimentação do card."*

1. **Planilha diz paga, Omie diz aberta** — a direção que este módulo olhava
   primeiro, e a menos provável das duas por causa da conciliação diária.
2. **Omie diz pago, planilha não diz** — o furo de verdade: o dinheiro saiu, o
   Omie sabe, e a SP continua aparecendo como "a pagar" para quem usa a planilha.
   Não aparece em lugar nenhum: a fila de falhas tinha ZERO pendências de
   planilha, porque a gravação morria antes de chegar nela.

Quatro coisas que este arquivo trava, porque errar nelas custa dinheiro ou memória:

1. **O conferidor NÃO grava nada.** Relatório é relatório. Um conferidor que
   também corrige erraria em silêncio na primeira divergência de valor.
2. **Ele não lê a aba inteira.** A SPsBD tem ~52 mil linhas × 37 colunas, e ler
   tudo custa 150-250 MB — foi assim que o serviço caiu por memória em julho de
   2026. Nove colunas resolvem.
3. **"Título não encontrado" não é a mesma coisa que "baixa pela metade".** Um é
   cadastro errado na planilha; o outro é dinheiro pago sem baixa. Misturar os
   dois faz o relatório mentir.
4. **As duas direções usam datas diferentes para a janela**, e têm de usar: a
   primeira tem data de pagamento; a segunda não tem — a planilha nem sabe que
   foi paga —, então a janela é pelo vencimento.
"""
from datetime import datetime, timedelta

import pytest

from app.apps.baixabradesco import conferencia as mod


def _dias_atras(n):
    return (datetime.now() - timedelta(days=n)).strftime('%d/%m/%Y')


class _AbaSPsBD:
    """Dublê da SPsBD. Reclama se alguém tentar ler a aba inteira."""

    def __init__(self, linhas):
        # cada linha: (id, vencimento, credor, valor, status_pgt, codigo, card,
        #              data_pgt, comprovante)
        self.linhas = linhas
        self.faixas_lidas = []

    def get_all_values(self):
        raise AssertionError('get_all_values na SPsBD: 52 mil linhas x 37 colunas')

    def get(self, faixa):
        raise AssertionError(f'leitura solta na SPsBD: {faixa}')

    def batch_get(self, faixas):
        self.faixas_lidas.extend(faixas)
        indices = {'A2:A': 0, 'C2:C': 1, 'D2:D': 2, 'G2:G': 3, 'O2:O': 4,
                   'P2:P': 5, 'R2:R': 6, 'X2:X': 7, 'AG2:AG': 8}
        saida = []
        for f in faixas:
            i = indices[f]
            saida.append([[l[i]] for l in self.linhas])
        return saida


class _Planilha:
    def __init__(self, aba):
        self._aba = aba

    def worksheet(self, nome):
        assert nome == 'SPsBD'
        return self._aba


class _Google:
    def __init__(self, aba):
        self._aba = aba

    def open_by_key(self, key):
        return _Planilha(self._aba)


def _linha(sp_id='1443274610', credor='Fornecedor Exemplo Ltda', valor='7.350,48',
           status='Pago', codigo='Int1443274610', dias=3, comprovante='link',
           vencimento=None, card='https://app.pipefy.com/cards/1'):
    return (sp_id,
            vencimento if vencimento is not None else _dias_atras(dias or 3),
            credor, valor, status, codigo, card,
            _dias_atras(dias) if dias is not None else '', comprovante)


@pytest.fixture
def planilha(monkeypatch):
    def _montar(linhas):
        aba = _AbaSPsBD(linhas)
        monkeypatch.setattr(mod, 'get_gc', lambda: _Google(aba))
        return aba
    return _montar


@pytest.fixture
def credenciais(monkeypatch):
    monkeypatch.setattr(mod, 'credentials_from_payload',
                        lambda payload: ('chave', 'segredo'))


def _omie(monkeypatch, respostas):
    """Dubla a consulta ao Omie: uma resposta por código consultado."""
    chamados = []

    def _consultar(codigo, payload):
        return {'codigo': codigo}

    def _executar(body):
        codigo = body['codigo']
        chamados.append(codigo)
        return respostas.get(codigo, {'ok': True, 'body': {'status_titulo': 'PAGO'}})

    monkeypatch.setattr(mod, 'build_consultar_conta_pagar', _consultar)
    monkeypatch.setattr(mod, 'execute_omie', _executar)
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))
    return chamados


# =====================================================================
# 1. o que é candidato a conferência
# =====================================================================

def test_le_so_as_nove_colunas_que_precisa(planilha):
    aba = planilha([_linha()])

    mod.candidatas({})

    assert aba.faixas_lidas == ['A2:A', 'C2:C', 'D2:D', 'G2:G', 'O2:O',
                                'P2:P', 'R2:R', 'X2:X', 'AG2:AG']


def test_so_entra_o_que_a_planilha_diz_pago_e_tem_comprovante(planilha):
    planilha([
        _linha(sp_id='1', status='Pagar'),
        _linha(sp_id='2', status='Pago', comprovante=''),
        _linha(sp_id='3', status='Pago'),
        _linha(sp_id='4', status='PAGO'),
    ])

    r = mod.candidatas({})

    assert [c['sp_id'] for c in r['conferiveis']] == ['3', '4']


def test_pagamento_fora_da_janela_fica_de_fora(planilha):
    planilha([_linha(sp_id='velha', dias=400), _linha(sp_id='nova', dias=5)])

    assert [c['sp_id'] for c in mod.candidatas({})['conferiveis']] == ['nova']
    assert [c['sp_id'] for c in mod.candidatas({'dias': 500})['conferiveis']] \
        == ['velha', 'nova']


def test_sem_data_de_pagamento_entra_porque_e_justamente_o_caso_suspeito(planilha):
    planilha([_linha(sp_id='sem_data', dias=None)])

    assert [c['sp_id'] for c in mod.candidatas({})['conferiveis']] == ['sem_data']


def test_sem_codigo_de_integracao_e_contado_mas_nao_consultado(planilha):
    planilha([_linha(sp_id='sem_codigo', codigo=''), _linha(sp_id='com_codigo')])

    r = mod.candidatas({})

    assert r['sem_codigo_integracao'] == 1
    assert r['pagas_na_janela'] == 2
    assert [c['sp_id'] for c in r['conferiveis']] == ['com_codigo']


def test_a_linha_relatada_e_a_da_planilha(planilha):
    planilha([_linha(sp_id='a'), _linha(sp_id='b')])

    r = mod.candidatas({})

    assert [c['linha'] for c in r['conferiveis']] == [2, 3]


# =====================================================================
# 2. a comparação com o Omie
# =====================================================================

def test_titulo_aberto_no_omie_com_planilha_paga_e_a_baixa_pela_metade(
        planilha, credenciais, monkeypatch):
    planilha([_linha(sp_id='1443274610', codigo='IntA'),
              _linha(sp_id='1445859706', codigo='IntB')])
    _omie(monkeypatch, {
        'IntA': {'ok': True, 'body': {'status_titulo': 'ABERTO'}},
        'IntB': {'ok': True, 'body': {'status_titulo': 'PAGO'}},
    })

    r = mod.conferir({})

    assert r['quantidade_divergentes'] == 1
    assert r['divergentes'][0]['sp_id'] == '1443274610'
    assert r['divergentes'][0]['status_omie'] == 'ABERTO'
    assert r['confirmadas_pagas_nos_dois'] == 1


def test_titulo_inexistente_nao_e_contado_como_baixa_pela_metade(
        planilha, credenciais, monkeypatch):
    """Cadastro errado na planilha é outro problema — e tem outra solução."""
    planilha([_linha(codigo='IntFantasma')])
    _omie(monkeypatch, {
        'IntFantasma': {'ok': False, 'body': {'faultstring': 'Título não encontrado!'},
                        'raw': ''},
    })

    r = mod.conferir({})

    assert r['quantidade_divergentes'] == 0
    assert len(r['titulos_nao_encontrados']) == 1
    assert r['erros_de_consulta'] == []


def test_erro_de_rede_nao_vira_divergencia(planilha, credenciais, monkeypatch):
    planilha([_linha(codigo='IntX')])
    _omie(monkeypatch, {
        'IntX': {'ok': False, 'body': {}, 'raw': 'timeout ao falar com o Omie'},
    })

    r = mod.conferir({})

    assert r['quantidade_divergentes'] == 0
    assert r['titulos_nao_encontrados'] == []
    assert len(r['erros_de_consulta']) == 1


def test_o_conferidor_nao_grava_nada(planilha, credenciais, monkeypatch):
    aba = planilha([_linha(codigo='IntA')])
    _omie(monkeypatch, {'IntA': {'ok': True, 'body': {'status_titulo': 'ABERTO'}}})

    # o dublê não tem update nem batch_update de escrita: qualquer tentativa
    # levantaria AttributeError
    assert not hasattr(aba, 'update')
    r = mod.conferir({})
    assert 'Nada foi gravado' in r['aviso']


def test_o_limite_segura_quantas_consultas_vao_ao_omie(
        planilha, credenciais, monkeypatch):
    planilha([_linha(sp_id=str(i), codigo=f'Int{i}') for i in range(30)])
    chamados = _omie(monkeypatch, {})

    r = mod.conferir({'limite': 4})

    assert len(chamados) == 4
    assert r['consultas_ao_omie'] == 4
    assert r['restam_para_conferir'] == 26


def test_apenas_contar_nao_fala_com_o_omie(planilha, credenciais, monkeypatch):
    planilha([_linha(sp_id=str(i), codigo=f'Int{i}') for i in range(7)])
    chamados = _omie(monkeypatch, {})

    r = mod.conferir({'apenas_contar': True})

    assert chamados == []
    assert r['apenas_contou'] is True
    assert r['a_conferir_planilha_paga'] == 7


def test_sem_credencial_do_omie_ele_recusa_em_vez_de_relatar_errado(
        planilha, monkeypatch):
    planilha([_linha()])
    monkeypatch.setattr(mod, 'credentials_from_payload', lambda payload: ('', ''))

    r = mod.conferir({})

    assert r['ok'] is False
    assert r['erro'] == 'credenciais_omie_ausentes'


def test_sem_divergencia_o_aviso_diz_isso_com_clareza(
        planilha, credenciais, monkeypatch):
    planilha([_linha(codigo='IntA')])
    _omie(monkeypatch, {'IntA': {'ok': True, 'body': {'status_titulo': 'PAGO'}}})

    r = mod.conferir({})

    assert r['quantidade_divergentes'] == 0
    assert 'Nenhuma divergência' in r['aviso']


# =====================================================================
# 3. a direção que o dono apontou: Omie pago, planilha para trás
# =====================================================================

def _nao_paga(sp_id='1445859706', codigo='IntAberta', dias_venc=5,
              card='https://app.pipefy.com/cards/9'):
    """Linha que a planilha diz que NÃO foi paga."""
    return (sp_id, _dias_atras(dias_venc), 'Fornecedor Exemplo Ltda',
            '1.200,00', 'Pagar', codigo, card, '', '')


def test_omie_pago_com_planilha_a_pagar_e_o_furo_de_verdade(
        planilha, credenciais, monkeypatch):
    planilha([_nao_paga(sp_id='1445859706', codigo='IntA')])
    _omie(monkeypatch, {'IntA': {'ok': True, 'body': {'status_titulo': 'PAGO'}}})

    r = mod.conferir({})

    assert r['quantidade_planilha_atrasada'] == 1
    item = r['planilha_atrasada'][0]
    assert item['sp_id'] == '1445859706'
    assert item['status_na_planilha'] == 'Pagar'
    assert item['status_omie'] == 'PAGO'
    # o cartão vem junto: o dono disse que o furo também está na movimentação
    assert item['card'] == 'https://app.pipefy.com/cards/9'


def test_aberta_nos_dois_lugares_nao_e_divergencia(
        planilha, credenciais, monkeypatch):
    planilha([_nao_paga(codigo='IntB')])
    _omie(monkeypatch, {'IntB': {'ok': True, 'body': {'status_titulo': 'ABERTO'}}})

    r = mod.conferir({})

    assert r['quantidade_planilha_atrasada'] == 0
    assert r['confirmadas_abertas_nos_dois'] == 1
    assert 'Nenhuma divergência' in r['aviso']


def test_a_janela_da_direcao_nova_e_pelo_vencimento(planilha):
    """A planilha não sabe que foi paga, então não há data de pagamento para
    usar. Sem janela, seriam ~52 mil consultas ao Omie."""
    planilha([_nao_paga(sp_id='velha', dias_venc=400),
              _nao_paga(sp_id='recente', dias_venc=10)])

    r = mod.candidatas({})

    assert [c['sp_id'] for c in r['conferiveis_nao_pagas']] == ['recente']
    assert r['nao_pagas_na_janela'] == 1


def test_vencimento_muito_a_frente_nao_deveria_estar_paga(planilha):
    """Título que só vence no ano que vem não é planilha atrasada."""
    planilha([_nao_paga(sp_id='futura', dias_venc=-400)])

    r = mod.candidatas({})

    assert r['conferiveis_nao_pagas'] == []


def test_linha_nao_paga_sem_codigo_de_integracao_fica_de_fora(planilha):
    planilha([_nao_paga(codigo='')])

    assert mod.candidatas({})['conferiveis_nao_pagas'] == []


def test_as_duas_direcoes_rodam_na_mesma_chamada(
        planilha, credenciais, monkeypatch):
    planilha([
        _linha(sp_id='paga_mas_aberta', codigo='IntX'),
        _nao_paga(sp_id='aberta_mas_paga', codigo='IntY'),
    ])
    _omie(monkeypatch, {
        'IntX': {'ok': True, 'body': {'status_titulo': 'ABERTO'}},
        'IntY': {'ok': True, 'body': {'status_titulo': 'PAGO'}},
    })

    r = mod.conferir({})

    assert [d['sp_id'] for d in r['divergentes']] == ['paga_mas_aberta']
    assert [d['sp_id'] for d in r['planilha_atrasada']] == ['aberta_mas_paga']
    assert r['consultas_ao_omie'] == 2


def test_da_para_pedir_so_o_furo_e_so_a_direcao_antiga(
        planilha, credenciais, monkeypatch):
    planilha([
        _linha(sp_id='paga', codigo='IntX'),
        _nao_paga(sp_id='nao_paga', codigo='IntY'),
    ])

    chamados = _omie(monkeypatch, {})
    mod.conferir({'sentido': 'omie_pago'})
    assert chamados == ['IntY']

    chamados2 = _omie(monkeypatch, {})
    mod.conferir({'sentido': 'planilha_paga'})
    assert chamados2 == ['IntX']


def test_apenas_contar_mostra_o_tamanho_das_duas_direcoes(
        planilha, credenciais, monkeypatch):
    planilha([_linha(sp_id=str(i), codigo=f'Int{i}') for i in range(3)]
             + [_nao_paga(sp_id=f'n{i}', codigo=f'IntN{i}') for i in range(8)])
    chamados = _omie(monkeypatch, {})

    r = mod.conferir({'apenas_contar': True})

    assert chamados == []
    assert r['a_conferir_planilha_paga'] == 3
    assert r['a_conferir_planilha_nao_paga'] == 8
    assert '3 SP(s) que a planilha diz pagas' in r['em_portugues']
    assert '8' in r['em_portugues']


def test_o_limite_vale_para_cada_direcao_separadamente(
        planilha, credenciais, monkeypatch):
    planilha([_linha(sp_id=str(i), codigo=f'Int{i}') for i in range(10)]
             + [_nao_paga(sp_id=f'n{i}', codigo=f'IntN{i}') for i in range(10)])
    chamados = _omie(monkeypatch, {})

    r = mod.conferir({'limite': 3})

    assert len(chamados) == 6          # 3 de cada lado
    assert r['restam_para_conferir'] == 14


# =====================================================================
# 4. continuar de onde parou — a frase prometia e o código não cumpria
# =====================================================================

def test_sem_pular_cada_chamada_repetiria_as_mesmas_linhas(
        planilha, credenciais, monkeypatch):
    """O conferidor não grava nada, então nada sai do conjunto entre chamadas.

    A frase em português dizia "chame de novo para continuar" e isso era
    mentira: a segunda chamada reconsultaria as mesmas primeiras linhas, para
    sempre. `pular` existe por isso, e a resposta devolve o número pronto.
    """
    planilha([_nao_paga(sp_id=f'n{i}', codigo=f'IntN{i}') for i in range(10)])

    chamados = _omie(monkeypatch, {})
    r1 = mod.conferir({'limite': 4, 'sentido': 'omie_pago'})
    assert chamados == ['IntN0', 'IntN1', 'IntN2', 'IntN3']
    assert r1['restam_para_conferir'] == 6
    assert r1['proximo_pular'] == 4
    assert 'pular=4' in r1['em_portugues']

    chamados2 = _omie(monkeypatch, {})
    r2 = mod.conferir({'limite': 4, 'pular': r1['proximo_pular'],
                       'sentido': 'omie_pago'})
    assert chamados2 == ['IntN4', 'IntN5', 'IntN6', 'IntN7']
    assert r2['pulou'] == 4
    assert r2['proximo_pular'] == 8


def test_na_ultima_pagina_ela_nao_manda_continuar(
        planilha, credenciais, monkeypatch):
    planilha([_nao_paga(sp_id=f'n{i}', codigo=f'IntN{i}') for i in range(6)])
    _omie(monkeypatch, {})

    r = mod.conferir({'limite': 4, 'pular': 4, 'sentido': 'omie_pago'})

    assert r['restam_para_conferir'] == 0
    assert r['proximo_pular'] == 0
    assert 'pular=' not in r['em_portugues']
    assert 'Faltam' not in r['em_portugues']


def test_pular_vale_para_as_duas_direcoes(planilha, credenciais, monkeypatch):
    planilha([_linha(sp_id=str(i), codigo=f'Int{i}') for i in range(6)]
             + [_nao_paga(sp_id=f'n{i}', codigo=f'IntN{i}') for i in range(6)])
    chamados = _omie(monkeypatch, {})

    mod.conferir({'limite': 2, 'pular': 2})

    assert chamados == ['Int2', 'Int3', 'IntN2', 'IntN3']


def test_pular_negativo_nao_quebra(planilha, credenciais, monkeypatch):
    planilha([_nao_paga(codigo='IntA')])
    chamados = _omie(monkeypatch, {})

    r = mod.conferir({'pular': -5, 'sentido': 'omie_pago'})

    assert chamados == ['IntA']
    assert r['pulou'] == 0
