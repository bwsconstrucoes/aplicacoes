# -*- coding: utf-8 -*-
"""A SPsBD pela cópia do Análise de SPs — sem baixar a planilha inteira.

13/11/2026. Cada baixa baixava a SPsBD inteira (`A:AK`, ~52 mil linhas) e
segurava uma das quatro linhas de atendimento do serviço por minutos. Agora os
campos vêm da cópia no banco, e a planilha é lida só nas colunas que decidem
(ID, status, carimbo, agendado, conta).

O que estes testes prendem, e por quê:

- **a cópia**: com a cópia em dia, as duas listas saem IGUAIS às do caminho
  antigo, e nenhuma linha inteira é baixada;
- **a confirmação**: o status é SEMPRE o da planilha. Cópia dizendo "Pagar" de
  uma SP que a planilha já diz "Pago" não pode virar candidata — é a trava
  contra pagar duas vezes;
- **a volta ao caminho antigo**: cópia fora, vazia, velha demais ou planilha
  mexendo no meio da leitura caem na planilha inteira, e cota do Google
  estourada continua subindo como erro (é ela que leva à fila tardia).

Nenhum teste encosta no Google nem no banco: os dois são dublados.
"""
import copy
import datetime as dt
import re

import pytest

from app.apps.baixabradesco import core, sheets
from app.apps.baixabradesco.sheets import (
    CopiaIndisponivel, load_spsbd_omie_pendente, load_spsbd_operacional,
    load_spsbd_pela_copia)

CABECALHO = [
    'ID', 'Solicitação', 'Vencim.', 'Nome do Credor', 'CPF/CNPJ',
    'Descrição da Despesa', 'Valor Total', 'Centro de Custo', 'Tipo de Despesa',
    'Tipo de Pagamento', 'Responsável pelo Registro', 'Dt. Autorização',
    'Responsável Autorização', 'Status Aut.', 'Status Pgt', 'Código Integração',
    'Anexo Link', 'Card Link', 'Anexo', 'Card', 'Conta', 'Carimbo', 'Pesquisa',
    'Data do Pagamento', 'Info de Pgt', 'Parcela', 'Nº da NF', 'Agendado',
    'Linha', 'Nº do Pedido', 'Anuente', 'Status Anuencia', 'Comprovante',
    'Validação', 'Código de Barras', 'ID Pipefy Contrato', 'Conta Pagamento',
]
HOJE = dt.date.today().strftime('%d/%m/%Y')
VELHA = (dt.date.today() - dt.timedelta(days=90)).strftime('%d/%m/%Y')


def linha(sp_id, status='Pagar', agendado='agendado', valor='1.000,00',
          carimbo='2026-11-10 10:00:00', data_pgt='', comprovante='',
          conta='', credor='FORNECEDOR', barras=''):
    celulas = [''] * 37
    celulas[0] = sp_id
    celulas[3] = credor
    celulas[6] = valor
    celulas[9] = 'Pix'
    celulas[14] = status
    celulas[18] = '=FORMULA'          # S: a cópia não guarda
    celulas[21] = carimbo
    celulas[23] = data_pgt
    celulas[27] = agendado
    celulas[32] = comprovante
    celulas[34] = barras
    celulas[36] = conta
    return celulas


# ── Dublês ────────────────────────────────────────────────────────────────────

def _letra_idx(letra):
    n = 0
    for ch in letra:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _cortar(celulas):
    celulas = list(celulas)
    while celulas and celulas[-1] == '':
        celulas.pop()
    return celulas


class AbaFalsa:
    """A aba SPsBD, devolvendo o que o Google devolve: colunas como listas de
    uma célula, linha vazia como [], cauda vazia cortada."""

    def __init__(self, grade):
        self.grade = grade              # grade[0] é o cabeçalho
        self.pedidos = []

    def get(self, faixa):
        raise AssertionError('baixou a planilha inteira: ' + faixa)

    def batch_get(self, faixas):
        self.pedidos.append(list(faixas))
        saida = []
        for f in faixas:
            m = re.fullmatch(r'([A-Z]+)(\d+):([A-Z]+)(\d*)', f)
            c1, l1, c2, l2 = m.group(1), int(m.group(2)), m.group(3), m.group(4)
            i1, i2 = _letra_idx(c1), _letra_idx(c2)
            l2 = int(l2) if l2 else len(self.grade)
            bloco = []
            for n in range(l1, l2 + 1):
                if n - 1 >= len(self.grade):
                    break
                bloco.append(_cortar(self.grade[n - 1][i1:i2 + 1]))
            while bloco and bloco[-1] == []:
                bloco.pop()
            saida.append(bloco)
        return saida

    def linhas_inteiras_relidas(self):
        return [f for p in self.pedidos for f in p
                if re.fullmatch(r'A\d+:AK\d+', f) and f != 'A1:AK1']


class GoogleFalso:
    def __init__(self, aba):
        self.aba = aba

    def open_by_key(self, chave):
        return self

    def worksheet(self, nome):
        assert nome == 'SPsBD'
        return self.aba


class CopiaFalsa:
    """A cópia do banco, tirada da planilha no momento da sincronização.

    Faz o que o banco faz: guarda as colunas A..AK menos as fórmulas e a AK, e
    filtra as candidatas pelo status QUE ELA CONHECE."""

    def __init__(self, grade, erro=None):
        self.erro = erro
        self.linhas = {}
        for celulas in grade[1:]:
            c = list(celulas) + [''] * (37 - len(celulas))
            for idx in (18, 22, 28, 36):   # S, W, AC, AK não sobem
                c[idx] = ''
            if c[0]:
                self.linhas[c[0]] = c

    def situacao_por_sp(self):
        if self.erro:
            raise self.erro
        return {i: (c[21], c[14], c[27]) for i, c in self.linhas.items()}

    def linhas_candidatas(self, dias):
        saida = {}
        for i, c in self.linhas.items():
            st, ag = c[14].strip().lower(), c[27].strip().lower()
            if st == 'pagar' and ag in ('agendar', 'agendado', 'falhaagendar'):
                saida[i] = list(c)
            elif st == 'pago' and c[32].strip() and (
                    not c[23].strip() or sheets._pago_ha_pouco(c[23], dias + 2)):
                saida[i] = list(c)
        return saida


def montar(*linhas):
    return [list(CABECALHO)] + [list(l) for l in linhas]


def resumo(indice):
    return {i: (sp.row_number, sp.status_pgt, sp.status_agendamento, sp.valor_total,
                sp.conta_pagamento, sp.nome_credor, sp.raw.get('Comprovante'))
            for i, sp in indice.items()}


# ── A cópia: mesma resposta, sem baixar a planilha ────────────────────────────

def _planilha_variada():
    return montar(
        linha('100', status='Pagar', agendado='agendado', valor='500,00'),
        linha('101', status='Pagar', agendado='', valor='600,00'),          # não agendada
        linha('102', status='Pago', data_pgt=HOJE, comprovante='https://x/1',
              conta='50024-0'),                                             # Omie pendente
        linha('103', status='Pago', data_pgt=VELHA, comprovante='https://x/2'),  # velha
        linha('104', status='Pago', data_pgt=HOJE, comprovante=''),         # sem comprovante
        linha('105', status='Pagar', agendado='FalhaAgendar', valor='700,00',
              conta='7011-4', barras='23790000'),
        linha('', status='Pagar', agendado='agendado'),                     # linha sem ID
    )


def test_com_a_copia_em_dia_as_listas_saem_iguais_as_do_caminho_antigo():
    grade = _planilha_variada()
    aba = AbaFalsa(grade)
    op, pend, info = load_spsbd_pela_copia(GoogleFalso(aba), copia=CopiaFalsa(grade))

    valores = [_cortar(l) for l in grade]
    assert resumo(op) == resumo(load_spsbd_operacional(values=valores))
    assert resumo(pend) == resumo(load_spsbd_omie_pendente(values=valores))
    assert set(op) == {'100', '105'}
    assert set(pend) == {'102'}
    assert info == {'fonte': 'copia', 'linhas_da_copia': 3, 'relidas_da_planilha': 0}


def test_com_a_copia_em_dia_um_pedido_so_ao_google_e_estreito():
    grade = _planilha_variada()
    aba = AbaFalsa(grade)
    load_spsbd_pela_copia(GoogleFalso(aba), copia=CopiaFalsa(grade))
    assert aba.pedidos == [['A1:AK1', 'A2:A', 'O2:O', 'V2:V', 'AB2:AB', 'AK2:AK']]
    assert aba.linhas_inteiras_relidas() == []


def test_a_conta_vem_da_planilha_porque_a_copia_nao_guarda():
    """A AK decide o casamento por conta; sem ela o filtro afrouxa e um
    comprovante de outra conta poderia casar sozinho."""
    grade = _planilha_variada()
    op, pend, _ = load_spsbd_pela_copia(GoogleFalso(AbaFalsa(grade)),
                                        copia=CopiaFalsa(grade))
    assert op['105'].conta_pagamento == '7011-4'
    assert pend['102'].conta_pagamento == '50024-0'


def test_o_numero_da_linha_e_o_da_planilha():
    grade = _planilha_variada()
    op, _, _ = load_spsbd_pela_copia(GoogleFalso(AbaFalsa(grade)), copia=CopiaFalsa(grade))
    assert op['100'].row_number == 2
    assert op['105'].row_number == 7


# ── A confirmação: o status é sempre o da planilha ────────────────────────────

def test_sp_ja_paga_na_planilha_nao_vira_candidata_mesmo_com_a_copia_dizendo_pagar():
    """A trava contra pagar duas vezes. A cópia ficou para trás: ela ainda diz
    "Pagar", a planilha já diz "Pago" — e o script que marcou nem carimbou."""
    antes = montar(linha('200', status='Pagar', agendado='agendado'))
    copia = CopiaFalsa(antes)
    depois = montar(linha('200', status='Pago', agendado='agendado'))  # mesmo carimbo
    aba = AbaFalsa(depois)

    op, pend, info = load_spsbd_pela_copia(GoogleFalso(aba), copia=copia)

    assert '200' not in op
    assert '200' not in pend            # paga, mas sem comprovante
    assert aba.linhas_inteiras_relidas() == ['A2:AK2']
    assert info['relidas_da_planilha'] == 1


def test_sp_agendada_depois_da_sincronizacao_e_achada():
    """O outro lado do atraso: a cópia nem sabe que a SP foi agendada."""
    antes = montar(linha('300', status='Pagar', agendado=''),
                   linha('301', status='Pagar', agendado='agendado'))
    copia = CopiaFalsa(antes)
    depois = montar(linha('300', status='Pagar', agendado='agendado', valor='42,00',
                          carimbo='2026-11-13 09:00:00'),
                    linha('301', status='Pagar', agendado='agendado'))
    aba = AbaFalsa(depois)

    op, _, _ = load_spsbd_pela_copia(GoogleFalso(aba), copia=copia)

    assert set(op) == {'300', '301'}
    assert op['300'].valor_total == '42,00'
    assert aba.linhas_inteiras_relidas() == ['A2:AK2']


def test_sp_nova_que_a_copia_nao_conhece_e_relida_inteira():
    antes = montar(linha('400'))
    copia = CopiaFalsa(antes)
    depois = montar(linha('400'), linha('401', valor='9,99', credor='NOVO'))
    aba = AbaFalsa(depois)

    op, _, _ = load_spsbd_pela_copia(GoogleFalso(aba), copia=copia)

    assert op['401'].nome_credor == 'NOVO'
    assert aba.linhas_inteiras_relidas() == ['A3:AK3']


def test_campo_alterado_com_carimbo_novo_vem_da_planilha_e_nao_da_copia():
    antes = montar(linha('500', valor='100,00'))
    copia = CopiaFalsa(antes)
    depois = montar(linha('500', valor='150,00', carimbo='2026-11-13 11:11:11'))

    op, _, _ = load_spsbd_pela_copia(GoogleFalso(AbaFalsa(depois)), copia=copia)

    assert op['500'].valor_total == '150,00'


def test_paga_agora_com_comprovante_entra_como_omie_pendente():
    """Paga depois da sincronização: a cópia não tem data nem comprovante. A
    linha mudou de carimbo, então é relida — e entra na lista certa."""
    antes = montar(linha('600', status='Pagar', agendado='agendado'))
    copia = CopiaFalsa(antes)
    depois = montar(linha('600', status='Pago', agendado='agendado', data_pgt=HOJE,
                          comprovante='https://x/600', carimbo='2026-11-13 12:00:00'))

    op, pend, _ = load_spsbd_pela_copia(GoogleFalso(AbaFalsa(depois)), copia=copia)

    assert '600' not in op
    assert pend['600'].raw.get('Comprovante') == 'https://x/600'


def test_linhas_vizinhas_vao_num_trecho_so():
    antes = montar(linha('700'))
    copia = CopiaFalsa(antes)
    depois = montar(linha('700'), linha('701'), linha('702'), linha('703', status='Cancelado'),
                    linha('704'))
    aba = AbaFalsa(depois)
    load_spsbd_pela_copia(GoogleFalso(aba), copia=copia)
    assert aba.linhas_inteiras_relidas() == ['A3:AK4', 'A6:AK6']


# ── A volta ao caminho antigo ─────────────────────────────────────────────────

def test_copia_fora_do_ar_pede_o_caminho_antigo():
    grade = _planilha_variada()
    with pytest.raises(CopiaIndisponivel, match='fora do ar'):
        load_spsbd_pela_copia(GoogleFalso(AbaFalsa(grade)),
                              copia=CopiaFalsa(grade, erro=RuntimeError('sem banco')))


def test_copia_vazia_pede_o_caminho_antigo():
    grade = _planilha_variada()
    with pytest.raises(CopiaIndisponivel, match='vazia'):
        load_spsbd_pela_copia(GoogleFalso(AbaFalsa(grade)), copia=CopiaFalsa(montar()))


def test_copia_velha_demais_pede_o_caminho_antigo():
    antes = montar(linha('800'))
    depois = montar(*[linha(str(900 + i), carimbo='2026-11-13 08:00:00') for i in range(5)])
    with pytest.raises(CopiaIndisponivel, match='mudaram'):
        load_spsbd_pela_copia(GoogleFalso(AbaFalsa(depois)), copia=CopiaFalsa(antes),
                              limite_releitura=3)


def test_planilha_que_muda_de_lugar_no_meio_da_leitura_pede_o_caminho_antigo():
    """Alguém inseriu uma linha acima entre a leitura estreita e a releitura: o
    número da linha passou a ser de OUTRA SP. Melhor desistir que casar errado."""
    antes = montar(linha('1000'))
    copia = CopiaFalsa(antes)
    depois = montar(linha('1000'), linha('1001', carimbo='2026-11-13 08:00:00'))

    class AbaQueMuda(AbaFalsa):
        def batch_get(self, faixas):
            if faixas == ['A3:AK3']:
                self.grade.insert(1, linha('INTRUSA'))
            return super().batch_get(faixas)

    with pytest.raises(CopiaIndisponivel, match='mudou durante a leitura'):
        load_spsbd_pela_copia(GoogleFalso(AbaQueMuda(depois)), copia=copia)


def test_erro_do_google_nao_vira_volta_ao_caminho_antigo():
    """Cota estourada passa adiante: baixar a planilha inteira em seguida
    também esbarraria nela."""
    grade = _planilha_variada()

    class AbaSemCota(AbaFalsa):
        def batch_get(self, faixas):
            raise RuntimeError('APIError: [429]: Quota exceeded')

    with pytest.raises(RuntimeError, match='429') as erro:
        load_spsbd_pela_copia(GoogleFalso(AbaSemCota(grade)), copia=CopiaFalsa(grade))
    assert not isinstance(erro.value, CopiaIndisponivel)


# ── O fio no fluxo da baixa (core) ────────────────────────────────────────────

@pytest.fixture
def caminho_antigo(monkeypatch):
    chamadas = []
    valores = [_cortar(l) for l in _planilha_variada()]

    def _valores(gc):
        chamadas.append('planilha inteira')
        return copy.deepcopy(valores)

    monkeypatch.setattr(core, 'load_spsbd_values', _valores)
    monkeypatch.delenv('BAIXABRADESCO_SPSBD_PELA_COPIA', raising=False)
    return chamadas


def test_o_fluxo_usa_a_copia_quando_ela_serve(monkeypatch, caminho_antigo):
    monkeypatch.setattr(core, 'load_spsbd_pela_copia',
                        lambda gc: ({'1': 'op'}, {'2': 'pend'}, {'fonte': 'copia'}))
    op, pend, fonte = core.carregar_sps_candidatas(object())
    assert (op, pend, fonte['fonte']) == ({'1': 'op'}, {'2': 'pend'}, 'copia')
    assert caminho_antigo == []


def test_sem_a_copia_o_fluxo_volta_a_planilha_inteira(monkeypatch, caminho_antigo):
    def _sem_copia(gc):
        raise CopiaIndisponivel('a cópia do Análise de SPs está vazia')
    monkeypatch.setattr(core, 'load_spsbd_pela_copia', _sem_copia)

    op, pend, fonte = core.carregar_sps_candidatas(object())

    assert caminho_antigo == ['planilha inteira']
    assert set(op) == {'100', '105'} and set(pend) == {'102'}
    assert fonte == {'fonte': 'planilha_inteira',
                     'motivo': 'a cópia do Análise de SPs está vazia'}


def test_defeito_inesperado_no_caminho_novo_tambem_volta_ao_antigo(monkeypatch, caminho_antigo):
    def _quebrado(gc):
        raise KeyError('coluna')
    monkeypatch.setattr(core, 'load_spsbd_pela_copia', _quebrado)
    _, _, fonte = core.carregar_sps_candidatas(object())
    assert fonte['fonte'] == 'planilha_inteira'
    assert caminho_antigo == ['planilha inteira']


def test_cota_estourada_no_caminho_novo_sobe_como_erro(monkeypatch, caminho_antigo):
    """É o erro que a rota transforma em "adiado" na fila tardia."""
    def _sem_cota(gc):
        raise RuntimeError('APIError: [429]: Quota exceeded for quota metric')
    monkeypatch.setattr(core, 'load_spsbd_pela_copia', _sem_cota)
    with pytest.raises(RuntimeError, match='Quota exceeded'):
        core.carregar_sps_candidatas(object())
    assert caminho_antigo == []


def test_a_variavel_desliga_o_caminho_novo(monkeypatch, caminho_antigo):
    def _nao_devia(gc):
        raise AssertionError('a variável mandou não usar a cópia')
    monkeypatch.setattr(core, 'load_spsbd_pela_copia', _nao_devia)
    monkeypatch.setenv('BAIXABRADESCO_SPSBD_PELA_COPIA', '0')
    _, _, fonte = core.carregar_sps_candidatas(object())
    assert fonte['fonte'] == 'planilha_inteira'
    assert 'BAIXABRADESCO_SPSBD_PELA_COPIA' in fonte['motivo']


def test_o_fluxo_da_baixa_nao_baixa_mais_a_planilha_direto():
    import inspect
    fonte = inspect.getsource(core.processar_baixabradesco)
    assert 'carregar_sps_candidatas(gc)' in fonte
    assert 'load_spsbd_values(' not in fonte


# ── O teto de tempo das drenagens (13/11/2026) ────────────────────────────────
#
# Junto da troca da leitura: a rota do cron também drena a fila de FALHAS (até
# 55 itens) e cada baixa drena mais 10 de carona — sem relógio nenhum. O log do
# dono mostrou as duas rotas segurando linha por mais de sete minutos.

class _Relogio:
    def __init__(self):
        self.agora = 1000.0

    def monotonic(self):
        return self.agora

    def sleep(self, s):
        self.agora += s


def test_a_drenagem_para_de_comecar_item_novo_depois_do_prazo(monkeypatch):
    import app.apps.baixabradesco.fila as fila
    relogio = _Relogio()
    monkeypatch.setattr(fila, 'time', relogio)
    pedidos = []

    def _reprocessar(pedido):
        pedidos.append(pedido['etapas'][0])
        relogio.agora += 50          # cada etapa leva 50 s
        return {'pendentes_processados': 1}

    monkeypatch.setattr(fila, 'reprocessar_fila', _reprocessar)
    saida = fila.drenar_por_etapa({'omie': 15, 'sheets': 15, 'pipefy': 15, 'zapi': 10},
                                  {}, prazo_s=90)
    assert pedidos == ['omie', 'sheets']
    assert saida['parou_por_prazo_na_etapa'] == 'pipefy'


def test_sem_prazo_a_drenagem_segue_como_antes(monkeypatch):
    import app.apps.baixabradesco.fila as fila
    pedidos = []
    monkeypatch.setattr(fila, 'reprocessar_fila',
                        lambda p: pedidos.append(p) or {'pendentes_processados': 0})
    fila.drenar_por_etapa({'omie': 1, 'sheets': 1, 'pipefy': 1, 'zapi': 1}, {})
    assert len(pedidos) == 4
    assert all('_prazo_monotonic' not in p for p in pedidos)


def test_item_que_nao_comecou_fica_pendente_sem_gastar_tentativa(monkeypatch):
    """Dentro da etapa: com o prazo vencido, o laço para ANTES de tocar no item
    — nenhuma linha é marcada, então ele continua pendente, com as mesmas
    tentativas, para a passada seguinte."""
    import app.apps.baixabradesco.fila as fila
    relogio = _Relogio()
    monkeypatch.setattr(fila, 'time', relogio)
    monkeypatch.setattr(fila, 'get_gc', lambda: object())
    monkeypatch.setattr(fila, 'ensure_fila_sheet', lambda gc: object())
    itens = [{'_row_number': 2, 'Etapa': 'sheets', 'Tentativas': '1'},
             {'_row_number': 3, 'Etapa': 'sheets', 'Tentativas': '1'}]
    monkeypatch.setattr(fila, 'listar_pendentes', lambda *a, **k: list(itens))
    feitos, marcados = [], []

    def _retry(item, payload):
        feitos.append(item['_row_number'])
        relogio.agora += 100
        return {'ok': True}

    monkeypatch.setattr(fila, '_retry_sheets', _retry)
    monkeypatch.setattr(fila, '_update_row', lambda ws, n, *a, **k: marcados.append(n))

    r = fila.reprocessar_fila({'etapas': ['sheets'], 'limite': 2,
                               '_prazo_monotonic': relogio.agora + 30})

    assert feitos == [2] and marcados == [2]
    assert r['interrompido'] == 'prazo_da_passada'


def test_cron_e_lote_tem_teto_de_tempo(monkeypatch):
    import app.apps.baixabradesco.fila as fila
    from app.apps.baixabradesco import fila_tardia
    prazos = []
    monkeypatch.setattr(fila, 'drenar_por_etapa',
                        lambda limites, payload=None, prazo_s=None: prazos.append(prazo_s) or {})
    core._drenar_fila({})
    fila_tardia._drenar_a_fila({})
    assert prazos == [core.PRAZO_DRENAGEM_LOTE_S, fila_tardia.PRAZO_DRENAGEM_CRON_S]
    assert core.PRAZO_DRENAGEM_LOTE_S <= 60 and fila_tardia.PRAZO_DRENAGEM_CRON_S <= 120
