# -*- coding: utf-8 -*-
"""A fila de falhas precisa ser contável e precisa dar para drenar.

Pergunta do dono em 08/10/2026, vendo a aba `BaixaBradescoFila` com 2.270
linhas: *"e essa fila, de 2270 linhas, vai rodar?"*

Não ia. Três coisas impediam, e as três estão cobertas aqui:

1. **Ninguém sabia quantas das linhas eram pendência de verdade.** A aba guarda
   também tudo que já foi concluído, e não havia como contar sem abrir a
   planilha na mão. Agora existe `resumo_fila`.
2. **Ler a fila custava a aba inteira.** `get_all_values()` trazia as 2.270
   linhas COM o JSON do payload de cada uma, só para achar cinco — o que
   CONTEXTO.md §3.7 proíbe. Agora o filtro lê só A:L e o payload vem apenas das
   linhas escolhidas.
3. **Drenar de uma vez estouraria a cota do Google**, que é por minuto e é do
   mesmo usuário de serviço do ERP e do painel. Agora lote grande anda com
   pausa e para sozinho se a cota começar a recusar.

E uma trava de bom senso: aviso de WhatsApp parado na fila há dias não é mais
aviso. Ao drenar fila velha ele é descartado com o motivo escrito, em vez de ir
para os dois celulares falando de um problema que já foi resolvido na mão.
"""
from datetime import datetime, timedelta

import pytest

from app.apps.baixabradesco import fila as mod


def _hoje(dias=0, horas=0):
    return (datetime.now() - timedelta(days=dias, hours=horas)).strftime('%d/%m/%Y %H:%M:%S')


def _futuro(horas=2):
    return (datetime.now() + timedelta(hours=horas)).strftime('%d/%m/%Y %H:%M:%S')


class _AbaFila:
    """Dublê da aba da fila. Registra o que foi lido, para provar o custo."""

    def __init__(self, linhas):
        # cada linha é uma lista de 15 colunas (A..O)
        self.linhas = linhas
        self.faixas_lidas = []
        self.gravacoes = []
        self.chamadas_batch_update = 0
        self.explodir_cota = False

    # --- leitura ----------------------------------------------------------
    def get_all_values(self):
        raise AssertionError('get_all_values na fila: a aba inteira NÃO pode ser lida')

    def get(self, faixa):
        self.faixas_lidas.append(faixa)
        if faixa == 'A1:O1':
            return [list(mod.HEADERS)]
        if faixa == mod.FAIXA_CONTROLE:
            return [linha[:mod.COLS_CONTROLE] for linha in self.linhas]
        raise AssertionError(f'faixa inesperada: {faixa}')

    def batch_get(self, faixas):
        self.faixas_lidas.extend(faixas)
        saida = []
        for f in faixas:
            if f.endswith('2:A') or f == 'A2:A':
                saida.append([[l[0]] for l in self.linhas])
            elif f == 'B2:B':
                saida.append([[l[1]] for l in self.linhas])
            elif f == 'D2:D':
                saida.append([[l[3]] for l in self.linhas])
            elif f == 'E2:E':
                saida.append([[l[4]] for l in self.linhas])
            elif f == 'L2:L':
                saida.append([[l[11]] for l in self.linhas])
            elif f.startswith(mod.COL_PAYLOAD):
                n = int(f[len(mod.COL_PAYLOAD):])
                saida.append([[self.linhas[n - 2][13]]])
            else:
                raise AssertionError(f'faixa inesperada: {f}')
        return saida

    # --- escrita ----------------------------------------------------------
    def batch_update(self, data, value_input_option=None):
        self.chamadas_batch_update += 1
        if self.explodir_cota:
            raise RuntimeError('Quota exceeded (429)')
        for item in data:
            self.gravacoes.append((item['range'], item['values'][0]))

    def update(self, faixa, valores, value_input_option=None):
        raise AssertionError('update solto: tem de ser batch_update, a cota é por minuto')

    def append_row(self, valores, value_input_option=None):
        self.linhas.append(list(valores))


PAYLOAD_OMIE = ('{"codigo_integracao": "Int1443274610", '
                '"codigo_conta_omie": "1234567", '
                '"valor_pago": "7.350,48", "data_pagamento": "07/10/2026"}')


def _linha(status='PENDENTE', etapa='sheets', proxima='', tentativas='0',
           registro=None, payload='{"sheets_updates": [{"range": "O5", "values": [["Pago"]]}]}',
           tipo='sheets_erro'):
    l = [''] * 15
    l[0] = registro if registro is not None else _hoje()
    l[1] = status
    l[2] = tentativas
    l[3] = proxima
    l[4] = tipo
    l[11] = etapa
    l[13] = payload
    return l


@pytest.fixture
def aba(monkeypatch):
    def _montar(linhas):
        a = _AbaFila(linhas)
        monkeypatch.setattr(mod, 'get_gc', lambda: object())
        monkeypatch.setattr(mod, 'ensure_fila_sheet', lambda gc=None: a)
        return a
    return _montar


# =====================================================================
# 1. contar a fila
# =====================================================================

def test_resumo_separa_pendencia_de_verdade_do_que_ja_foi_concluido(aba):
    """2.270 linhas na aba não são 2.270 pendências — e essa é a conta que falta."""
    linhas = (
        [_linha(status='CONCLUIDO') for _ in range(2000)]
        + [_linha(status='PENDENTE') for _ in range(200)]
        + [_linha(status='PENDENTE', proxima=_futuro()) for _ in range(30)]
        + [_linha(status='FALHOU', tentativas='5', etapa='omie') for _ in range(40)]
    )
    a = aba(linhas)

    r = mod.resumo_fila()

    assert r['ok'] is True
    assert r['linhas_na_aba'] == 2270
    assert r['concluidos'] == 2000
    assert r['pendentes_vencidos'] == 200
    assert r['pendentes_agendados'] == 30
    assert r['falhados'] == 40
    assert r['a_reprocessar_agora'] == 240
    assert r['por_etapa'] == {'sheets': 230, 'omie': 40}
    assert r['lotes_de_5_necessarios'] == 40


def test_resumo_nao_grava_nada_e_le_so_cinco_colunas(aba):
    a = aba([_linha() for _ in range(50)])

    mod.resumo_fila()

    assert a.gravacoes == []
    assert a.chamadas_batch_update == 0
    lidas = [f for f in a.faixas_lidas if f != 'A1:O1']
    assert lidas == ['A2:A', 'B2:B', 'D2:D', 'E2:E', 'L2:L']


def test_resumo_informa_a_idade_do_registro_mais_antigo(aba):
    aba([_linha(registro='01/02/2026 08:00:00'),
         _linha(registro='30/09/2026 19:30:00')])

    r = mod.resumo_fila()

    assert r['registro_mais_antigo'] == '01/02/2026 08:00:00'
    assert r['registro_mais_recente'] == '30/09/2026 19:30:00'


# =====================================================================
# 2. ler a fila sem trazer a aba inteira
# =====================================================================

def test_o_cabecalho_e_conferido_lendo_so_a_primeira_linha(monkeypatch):
    a = _AbaFila([_linha()])

    class _Planilha:
        def worksheet(self, nome):
            return a

    class _Google:
        def open_by_key(self, key):
            return _Planilha()

    monkeypatch.setattr(mod, 'get_gc', lambda: _Google())
    mod.ensure_fila_sheet()

    # o dublê levanta AssertionError em get_all_values; chegar aqui já é o teste
    assert a.faixas_lidas == ['A1:O1']


def test_listar_le_as_colunas_leves_e_o_payload_so_do_que_escolheu(aba):
    a = aba([_linha() for _ in range(300)])

    itens = mod.listar_pendentes(limite=5)

    assert len(itens) == 5
    assert mod.FAIXA_CONTROLE in a.faixas_lidas
    payloads = [f for f in a.faixas_lidas if f.startswith(mod.COL_PAYLOAD)]
    assert payloads == ['N2', 'N3', 'N4', 'N5', 'N6']
    assert itens[0]['Payload Resumido'].startswith('{"sheets_updates"')
    assert itens[0]['_row_number'] == 2


def test_listar_ignora_agendado_para_depois_e_ignora_concluido(aba):
    aba([
        _linha(status='CONCLUIDO'),
        _linha(status='PENDENTE', proxima=_futuro()),
        _linha(status='PENDENTE'),
        _linha(status='FALHOU', tentativas='5'),
    ])

    itens = mod.listar_pendentes(limite=10)

    assert [i['_row_number'] for i in itens] == [4]


def test_falhado_so_volta_quando_e_pedido(aba):
    aba([_linha(status='FALHOU', tentativas='5'), _linha(status='PENDENTE')])

    assert [i['_row_number'] for i in mod.listar_pendentes(limite=10)] == [3]
    assert [i['_row_number'] for i in
            mod.listar_pendentes(limite=10, incluir_falhados=True)] == [2, 3]


# =====================================================================
# 3. marcar a linha custa UMA chamada, não duas
# =====================================================================

def test_marcar_a_linha_gasta_uma_chamada_de_escrita(aba):
    a = aba([_linha()])

    mod._update_row(a, 2, mod.STATUS_CONCLUIDO, 1, 'pronto')

    assert a.chamadas_batch_update == 1
    faixas = [g[0] for g in a.gravacoes]
    assert faixas == ['B2:D2', 'M2:O2']
    # concluído não reagenda próxima tentativa
    assert a.gravacoes[0][1][2] == ''


# =====================================================================
# 4. drenar lote grande sem estourar a cota de todo mundo
# =====================================================================

def test_lote_pequeno_anda_sem_pausa_e_lote_grande_anda_com_pausa():
    assert mod._pausa_padrao(5, {}) == 0.0
    assert mod._pausa_padrao(20, {}) == 0.0
    assert mod._pausa_padrao(500, {}) > 0
    assert mod._pausa_padrao(500, {'pausa_ms': 0}) == 0.0
    assert mod._pausa_padrao(5, {'pausa_ms': 2000}) == 2.0


def test_drenagem_grande_relata_quantos_concluiram_e_quantos_sobraram(aba, monkeypatch):
    aba([_linha() for _ in range(4)])
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))
    chamadas = {'n': 0}

    def _sheets(item, payload):
        chamadas['n'] += 1
        return {'ok': chamadas['n'] % 2 == 1}

    monkeypatch.setattr(mod, '_retry_sheets', _sheets)

    r = mod.reprocessar_fila({'limite': 100})

    assert r['pendentes_processados'] == 4
    assert r['concluidos_agora'] == 2
    assert r['ainda_pendentes'] == 2
    assert r['limite_pedido'] == 100
    assert r['pausa_entre_itens_s'] > 0
    assert r['interrompido'] == ''


def test_a_drenagem_para_sozinha_quando_a_cota_do_google_recusa(aba, monkeypatch):
    aba([_linha() for _ in range(50)])
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))
    monkeypatch.setattr(mod, '_retry_sheets',
                        lambda item, payload: {'ok': False, 'erro': 'Quota exceeded (429)'})

    r = mod.reprocessar_fila({'limite': 50})

    # para na terceira recusa seguida: a cota é compartilhada com o ERP e o painel
    assert r['interrompido'] == 'cota_do_google'
    assert r['pendentes_processados'] == 3


def test_erro_comum_nao_interrompe_a_drenagem(aba, monkeypatch):
    aba([_linha() for _ in range(6)])
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))
    monkeypatch.setattr(mod, '_retry_sheets',
                        lambda item, payload: {'ok': False, 'erro': 'linha nao encontrada'})

    r = mod.reprocessar_fila({'limite': 50})

    assert r['interrompido'] == ''
    assert r['pendentes_processados'] == 6
    assert r['ainda_pendentes'] == 6


# =====================================================================
# 5. aviso velho não volta a ser enviado
# =====================================================================

def test_aviso_parado_ha_dias_e_descartado_com_o_motivo_escrito(aba, monkeypatch):
    a = aba([_linha(etapa='zapi', registro=_hoje(dias=9), tipo='zapi_erro')])
    enviou = {'n': 0}
    monkeypatch.setattr(mod, '_retry_zapi',
                        lambda item, payload: enviou.__setitem__('n', enviou['n'] + 1) or {'ok': True})

    r = mod.reprocessar_fila({'limite': 10})

    assert enviou['n'] == 0
    assert r['avisos_descartados_por_idade'] == 1
    assert r['resultados'][0]['response']['status'] == 'aviso_descartado_por_idade'
    mensagem = [g[1][0] for g in a.gravacoes if g[0].startswith('M')][0]
    assert 'não reenviado' in mensagem


def test_aviso_de_hoje_continua_sendo_reenviado(aba, monkeypatch):
    aba([_linha(etapa='zapi', registro=_hoje(horas=2), tipo='zapi_erro')])
    enviou = {'n': 0}
    monkeypatch.setattr(mod, '_retry_zapi',
                        lambda item, payload: enviou.__setitem__('n', enviou['n'] + 1) or {'ok': True})

    r = mod.reprocessar_fila({'limite': 10})

    assert enviou['n'] == 1
    assert r['avisos_descartados_por_idade'] == 0


def test_quem_quiser_reenviar_aviso_antigo_pede_e_recebe(aba, monkeypatch):
    aba([_linha(etapa='zapi', registro=_hoje(dias=30), tipo='zapi_erro')])
    enviou = {'n': 0}
    monkeypatch.setattr(mod, '_retry_zapi',
                        lambda item, payload: enviou.__setitem__('n', enviou['n'] + 1) or {'ok': True})

    mod.reprocessar_fila({'limite': 10, 'reenviar_avisos_antigos': True})

    assert enviou['n'] == 1


def test_a_trava_de_idade_vale_so_para_aviso_nao_para_baixa(aba, monkeypatch):
    """Baixa de dois meses atrás continua sendo baixa — o dinheiro não envelhece."""
    aba([_linha(etapa='sheets', registro=_hoje(dias=60)),
         _linha(etapa='omie', registro=_hoje(dias=60))])
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))
    monkeypatch.setattr(mod, '_retry_sheets', lambda item, payload: {'ok': True})
    monkeypatch.setattr(mod, '_retry_omie', lambda item, payload: {'ok': True})

    r = mod.reprocessar_fila({'limite': 10})

    assert r['avisos_descartados_por_idade'] == 0
    assert r['concluidos_agora'] == 2


# =====================================================================
# 6. a ordem importa: dinheiro antes de recado
# =====================================================================
# Os números reais da fila em 08/10/2026, lidos em produção:
#   2.269 linhas, TODAS PENDENTE, nenhuma concluída, nenhuma falhada
#   zapi 1.943 | omie 238 | pipefy 88 | sheets 0
#   mais antiga: 18/06/2026
# Ou seja: a fila nunca andou, nem uma vez, em quase quatro meses. E 86% dela é
# recado, na frente de 238 baixas que são dinheiro.

def test_da_para_drenar_so_uma_etapa(aba, monkeypatch):
    aba([
        _linha(etapa='zapi', tipo='zapi_erro'),
        _linha(etapa='omie', tipo='omie_erro'),
        _linha(etapa='zapi', tipo='zapi_erro'),
        _linha(etapa='omie', tipo='omie_erro'),
        _linha(etapa='pipefy', tipo='pipefy_erro'),
    ])
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))
    vistos = []
    monkeypatch.setattr(mod, '_retry_omie',
                        lambda item, payload: vistos.append('omie') or {'ok': True})
    monkeypatch.setattr(mod, '_retry_zapi',
                        lambda item, payload: vistos.append('zapi') or {'ok': True})

    r = mod.reprocessar_fila({'limite': 50, 'etapas': ['omie']})

    assert vistos == ['omie', 'omie']
    assert r['etapas'] == ['omie']
    assert r['pendentes_processados'] == 2


def test_a_etapa_aceita_texto_simples_e_lista(aba):
    linhas = [_linha(etapa='omie'), _linha(etapa='pipefy'), _linha(etapa='zapi')]
    aba(linhas)

    assert mod._etapas_pedidas({'etapa': 'omie'}) == {'omie'}
    assert mod._etapas_pedidas({'etapa': 'omie,pipefy'}) == {'omie', 'pipefy'}
    assert mod._etapas_pedidas({'etapas': ['OMIE']}) == {'omie'}
    assert mod._etapas_pedidas({}) is None


# =====================================================================
# 7. falta de credencial NÃO consome tentativa
# =====================================================================

def test_falta_de_credencial_nao_queima_tentativa_nem_marca_falhou(aba, monkeypatch):
    """Era o jeito de apagar 238 baixas sem resolver nenhuma."""
    a = aba([_linha(etapa='omie', tentativas='4')])
    monkeypatch.setattr(mod, 'credentials_from_payload', lambda payload: ('', ''))

    r = mod.reprocessar_fila({'limite': 10, 'etapas': ['omie']})

    assert a.gravacoes == []          # a linha não foi tocada
    assert r['bloqueados_por_configuracao'] == 1
    assert r['o_que_falta_configurar'] == ['credenciais_omie_ausentes']
    assert r['concluidos_agora'] == 0


def test_com_credencial_a_baixa_e_tentada_normalmente(aba, monkeypatch):
    a = aba([_linha(etapa='omie', payload=PAYLOAD_OMIE)])
    monkeypatch.setattr(mod, 'credentials_from_payload', lambda payload: ('k', 's'))
    monkeypatch.setattr(mod, '_request_omie',
                        lambda call, param, payload:
                        {'ok': True, 'body': {'status_titulo': 'PAGO'}})

    r = mod.reprocessar_fila({'limite': 10, 'etapas': ['omie']})

    assert r['concluidos_agora'] == 1
    assert r['bloqueados_por_configuracao'] == 0
    assert a.chamadas_batch_update == 1


def test_titulo_ja_pago_no_omie_resolve_sem_lancar_nada(aba, monkeypatch):
    """Drenar fila antiga não pode pagar duas vezes."""
    aba([_linha(etapa='omie', payload=PAYLOAD_OMIE)])
    monkeypatch.setattr(mod, 'credentials_from_payload', lambda payload: ('k', 's'))
    chamadas = []

    def _req(call, param, payload):
        chamadas.append(call)
        return {'ok': True, 'body': {'status_titulo': 'PAGO'}}

    monkeypatch.setattr(mod, '_request_omie', _req)

    r = mod.reprocessar_fila({'limite': 10, 'etapas': ['omie']})

    assert chamadas == ['ConsultarContaPagar']   # nada de LancarPagamento
    assert r['concluidos_agora'] == 1


# =====================================================================
# 8. o acumulado de avisos antigos não é limpo pelo cron
# =====================================================================

def test_a_drenagem_automatica_nao_toca_no_aviso_antigo(aba, monkeypatch):
    """Nem envia, nem marca, nem gasta tentativa: ele nem é carregado."""
    a = aba([_linha(etapa='zapi', registro=_hoje(dias=100), tipo='zapi_erro')])
    enviou = {'n': 0}
    monkeypatch.setattr(mod, '_retry_zapi',
                        lambda item, payload: enviou.__setitem__('n', enviou['n'] + 1) or {'ok': True})

    r = mod.reprocessar_fila({'limite': 10, 'descartar_avisos_antigos': False})

    assert enviou['n'] == 0
    assert a.gravacoes == []
    assert r['pendentes_processados'] == 0
    assert r['avisos_descartados_por_idade'] == 0


def test_quem_pede_explicitamente_limpa_e_em_lote(aba, monkeypatch):
    """1.943 linhas não podem custar 1.943 escritas."""
    a = aba([_linha(etapa='zapi', registro=_hoje(dias=100), tipo='zapi_erro')
             for _ in range(120)])
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))

    r = mod.reprocessar_fila({'limite': 200, 'etapas': ['zapi']})

    assert r['avisos_descartados_por_idade'] == 120
    assert r['descartes_gravados'] == 120
    # blocos de 50 linhas: 3 chamadas, não 120
    assert a.chamadas_batch_update == 3


def test_aviso_recente_e_enviado_mesmo_na_drenagem_automatica(aba, monkeypatch):
    aba([_linha(etapa='zapi', registro=_hoje(horas=1), tipo='zapi_erro')])
    enviou = {'n': 0}
    monkeypatch.setattr(mod, '_retry_zapi',
                        lambda item, payload: enviou.__setitem__('n', enviou['n'] + 1) or {'ok': True})

    r = mod.reprocessar_fila({'limite': 10, 'descartar_avisos_antigos': False})

    assert enviou['n'] == 1
    assert r['concluidos_agora'] == 1


# =====================================================================
# 9. o acumulado antigo não pode ocupar a vaga do recado de ontem
# =====================================================================

def test_aviso_velho_nao_entope_a_fila_e_deixa_o_recente_passar(aba, monkeypatch):
    """O defeito: a fila é lida em ordem.

    Com 1.943 avisos de junho na frente, pedir "dez avisos" devolvia sempre os
    dez mais velhos — que seriam pulados por idade. O aviso de ontem, que alguém
    ainda quer receber, nunca era alcançado: a fila entupia com o que ela mesma
    ia descartar.
    """
    linhas = [_linha(etapa='zapi', registro=_hoje(dias=100), tipo='zapi_erro')
              for _ in range(50)]
    linhas.append(_linha(etapa='zapi', registro=_hoje(horas=3), tipo='zapi_erro'))
    aba(linhas)
    enviados = []
    monkeypatch.setattr(mod, '_retry_zapi',
                        lambda item, payload: enviados.append(item['_row_number']) or {'ok': True})

    r = mod.reprocessar_fila({'limite': 10, 'etapas': ['zapi'],
                              'descartar_avisos_antigos': False})

    # a única linha alcançada é a recente (linha 52), não as 50 velhas
    assert enviados == [52]
    assert r['concluidos_agora'] == 1
    assert r['avisos_antigos_pulados'] == 0   # nem foram carregados


def test_quando_o_pedido_e_para_descartar_os_velhos_voltam_a_ser_carregados(
        aba, monkeypatch):
    aba([_linha(etapa='zapi', registro=_hoje(dias=100), tipo='zapi_erro')
         for _ in range(10)])
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))

    r = mod.reprocessar_fila({'limite': 10, 'etapas': ['zapi']})

    assert r['avisos_descartados_por_idade'] == 10


def test_o_corte_por_idade_nao_afeta_as_outras_etapas(aba, monkeypatch):
    """Baixa de junho continua sendo baixa — só o recado envelhece."""
    aba([_linha(etapa='omie', registro=_hoje(dias=100), payload=PAYLOAD_OMIE),
         _linha(etapa='zapi', registro=_hoje(dias=100), tipo='zapi_erro')])
    monkeypatch.setattr(mod, 'credentials_from_payload', lambda payload: ('k', 's'))
    monkeypatch.setattr(mod, '_request_omie',
                        lambda call, param, payload:
                        {'ok': True, 'body': {'status_titulo': 'PAGO'}})
    monkeypatch.setattr(mod, 'time', type('T', (), {'sleep': staticmethod(lambda s: None)}))

    r = mod.reprocessar_fila({'limite': 10, 'descartar_avisos_antigos': False})

    assert r['concluidos_agora'] == 1          # a baixa de junho foi conferida
    assert r['pendentes_processados'] == 1     # o recado de junho nem entrou
