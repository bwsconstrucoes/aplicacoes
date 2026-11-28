# -*- coding: utf-8 -*-
"""A fila deixa de ser invisível: o estado de cada SP, para a tela mostrar.

Pedido do dono em 28/11/2026, no dia em que ele religou o disparo automático:

    *"eu reativei, mas por outro lado não tenho onde ver se tem algo na fila ou
    se não tem (…) deveria ter algo relacionado à essa fila na tela de
    comprovantes. Uma coluna na própria linha dos comprovantes avisando que
    tava na fila e ainda ia baixar na planilha. Assim eu sei que aquela
    informação vai baixar corretamente ainda, ou até forçar a baixa (…) pode
    ter até um indicador de cada comprovante se foi baixado no omie e planilha."*

O problema real: quando o Omie baixa e a planilha não, o robô registra a
pendência na aba da fila e ela se resolve sozinha no disparo seguinte. Só que
ninguém vê isso — para saber, era abrir a planilha e procurar a linha. Na
prática a pendência parecia perda, e a reação natural é refazer à mão, que é o
caminho mais perigoso.

⚠️ **A TELA É DE OUTRA ÁREA** (Análise de SPs). O que mora aqui é a resposta:
quem pergunta "e a SP 123?" recebe o estado dela e uma frase pronta. O contrato
está no `README.md`.

O que este arquivo trava:

1. A consulta responde SÓ pelas SPs pedidas, com UMA leitura da fila.
2. Ela **nunca** devolve o pedido guardado (coluna N) — ele carrega a chave do
   Omie e os tokens do Z-API.
3. A frase em português diz a coisa certa em cada caso: entra sozinho, ou
   precisa de alguém.
4. Forçar uma SP ignora o relógio da próxima tentativa e inclui o que já
   desistiu — senão o botão da tela não serviria para nada.
"""
from datetime import datetime, timedelta

import pytest

from app.apps.baixabradesco import fila as mod

SEGREDO_OMIE = 'chave-do-omie-nao-pode-sair-daqui'


def _agora(horas=0):
    return (datetime.now() - timedelta(hours=horas)).strftime('%d/%m/%Y %H:%M:%S')


def _futuro(horas=2):
    return (datetime.now() + timedelta(hours=horas)).strftime('%d/%m/%Y %H:%M:%S')


def _linha(sp, etapa='sheets', status='PENDENTE', tentativas='1',
           proxima=None, erro='Quota exceeded (429)', tipo='sheets_erro',
           arquivo='comprovantes.pdf', pagina='1'):
    l = [''] * 15
    l[0] = _agora(1)
    l[1] = status
    l[2] = tentativas
    l[3] = _futuro() if proxima is None else proxima
    l[4] = tipo
    l[5] = sp
    l[7] = arquivo
    l[8] = pagina
    l[11] = etapa
    l[12] = erro
    # a coluna N guarda o pedido do Make, COM credencial dentro
    l[13] = '{"omie": {"app_key": "%s"}, "sheets_updates": []}' % SEGREDO_OMIE
    return l


class _AbaFila:
    def __init__(self, linhas):
        self.linhas = linhas
        self.faixas_lidas = []

    def get_all_values(self):
        raise AssertionError('a aba inteira NÃO pode ser lida')

    def get(self, faixa):
        self.faixas_lidas.append(faixa)
        if faixa == 'A1:O1':
            return [list(mod.HEADERS)]
        if faixa == mod.FAIXA_COM_ERRO:
            return [linha[:13] for linha in self.linhas]
        if faixa == mod.FAIXA_CONTROLE:
            return [linha[:mod.COLS_CONTROLE] for linha in self.linhas]
        raise AssertionError(f'faixa inesperada: {faixa}')

    def batch_get(self, faixas):
        self.faixas_lidas.extend(faixas)
        saida = []
        for f in faixas:
            if f.startswith(mod.COL_PAYLOAD):
                n = int(f[len(mod.COL_PAYLOAD):])
                saida.append([[self.linhas[n - 2][13]]])
            else:
                raise AssertionError(f'faixa inesperada: {f}')
        return saida

    def batch_update(self, data, value_input_option=None):
        pass


@pytest.fixture
def aba(monkeypatch):
    def _montar(linhas, omie_registrado=()):
        a = _AbaFila(linhas)
        monkeypatch.setattr(mod, 'get_gc', lambda: object())
        monkeypatch.setattr(mod, 'ensure_fila_sheet', lambda gc=None: a)
        monkeypatch.setattr(
            'app.apps.baixabradesco.sheets.load_fingerprints_processados',
            lambda gc=None: {f'fp{i}': sp for i, sp in enumerate(omie_registrado)})
        return a
    return _montar


# =====================================================================
# 1. Responde só o que foi perguntado, e com uma leitura
# =====================================================================

def test_responde_so_pelas_sps_pedidas(aba):
    aba([_linha('111'), _linha('222'), _linha('333')])

    r = mod.situacao_por_sp(['111', '333'])

    assert sorted(r['sps']) == ['111', '333']
    assert r['sps']['111']['na_fila'] is True
    assert r['sps']['333']['na_fila'] is True


def test_sp_sem_pendencia_responde_que_nao_tem(aba):
    aba([_linha('111')], omie_registrado=('999',))

    r = mod.situacao_por_sp(['999'])

    assert r['sps']['999']['na_fila'] is False
    assert r['sps']['999']['pendencias'] == []
    assert 'Nada pendente' in r['sps']['999']['em_portugues']


def test_uma_leitura_da_fila_para_a_consulta_inteira(aba):
    """A tela vai chamar isto a cada vez que abrir a lista: tem de ser barato."""
    a = aba([_linha(str(i)) for i in range(100, 160)])

    mod.situacao_por_sp([str(i) for i in range(100, 160)], conferir_omie=False)

    assert a.faixas_lidas.count(mod.FAIXA_COM_ERRO) == 1


def test_lista_vazia_nao_le_nada(aba):
    a = aba([_linha('111')])
    r = mod.situacao_por_sp([])
    assert r['sps'] == {}
    assert a.faixas_lidas == []


def test_concluido_e_cabecalho_nao_contam_como_pendencia(aba):
    cabecalho = [''] * 15
    cabecalho[1] = 'STATUS'
    cabecalho[5] = '111'
    aba([_linha('111', status='CONCLUIDO'), cabecalho])

    r = mod.situacao_por_sp(['111'])

    assert r['sps']['111']['na_fila'] is False


# =====================================================================
# 2. A credencial NUNCA sai
# =====================================================================

def test_a_consulta_nao_devolve_o_pedido_guardado(aba):
    """A coluna N tem a chave do Omie e os tokens do Z-API dentro."""
    aba([_linha('111')])

    r = mod.situacao_por_sp(['111'])

    assert SEGREDO_OMIE not in str(r)
    assert 'app_key' not in str(r)
    assert 'sheets_updates' not in str(r)


def test_a_consulta_nao_busca_a_coluna_do_pedido(aba):
    a = aba([_linha('111')])
    mod.situacao_por_sp(['111'], conferir_omie=False)
    assert not any(f.startswith(mod.COL_PAYLOAD) for f in a.faixas_lidas)


# =====================================================================
# 3. A frase que a tela mostra
# =====================================================================

def test_planilha_na_fila_diz_que_entra_sozinho(aba):
    """É a resposta à pergunta dele: "essa informação vai baixar ainda?"."""
    aba([_linha('111', etapa='sheets')], omie_registrado=('111',))

    frase = mod.situacao_por_sp(['111'])['sps']['111']['em_portugues']

    assert 'Baixado no Omie' in frase
    assert 'gravação na planilha' in frase
    assert 'entra sozinho' in frase
    assert 'Próxima tentativa' in frase


def test_omie_pendente_nao_diz_que_baixou_no_omie(aba):
    aba([_linha('111', etapa='omie', tipo='omie_erro')])

    frase = mod.situacao_por_sp(['111'])['sps']['111']['em_portugues']

    assert 'Baixado no Omie' not in frase
    assert 'baixa no Omie' in frase


def test_quem_desistiu_avisa_que_NAO_entra_sozinho(aba):
    """Cinco tentativas e FALHOU: esperar não resolve, e dizer "está na fila"
    seria mentira — é aqui que a pessoa precisa agir."""
    aba([_linha('111', status='FALHOU', tentativas='5')], omie_registrado=('111',))

    sp = mod.situacao_por_sp(['111'])['sps']['111']

    assert sp['desistiu'] is True
    assert 'DESISTIU' in sp['em_portugues']
    assert 'force' in sp['em_portugues'].lower()


def test_duas_etapas_pendentes_aparecem_juntas(aba):
    aba([_linha('111', etapa='sheets'), _linha('111', etapa='pipefy')])

    sp = mod.situacao_por_sp(['111'])['sps']['111']

    assert sp['etapas_pendentes'] == ['pipefy', 'sheets']
    assert len(sp['pendencias']) == 2


def test_o_motivo_do_erro_vem_junto(aba):
    """Sem o motivo, a tela diz "falhou" e ninguém sabe o que fazer."""
    aba([_linha('111', erro='linha não encontrada para {"A": "=111"}')])

    pend = mod.situacao_por_sp(['111'])['sps']['111']['pendencias'][0]

    assert 'linha não encontrada' in pend['erro']
    assert pend['arquivo'] == 'comprovantes.pdf'
    assert pend['tentativas'] == '1'


# =====================================================================
# 4. O indicador do Omie
# =====================================================================

def test_diz_se_o_omie_ja_confirmou(aba):
    aba([_linha('111')], omie_registrado=('111',))
    assert mod.situacao_por_sp(['111'])['sps']['111']['omie_confirmado'] is True


def test_diz_quando_o_omie_NAO_confirmou(aba):
    aba([_linha('111')], omie_registrado=('999',))
    assert mod.situacao_por_sp(['111'])['sps']['111']['omie_confirmado'] is False


def test_sem_conferir_o_omie_responde_desconhecido(aba):
    """A tela pode dispensar essa leitura quando quiser resposta mais rápida."""
    aba([_linha('111')], omie_registrado=('111',))
    r = mod.situacao_por_sp(['111'], conferir_omie=False)
    assert r['sps']['111']['omie_confirmado'] is None


def test_falha_ao_ler_o_registro_do_omie_nao_derruba_a_consulta(aba, monkeypatch):
    aba([_linha('111')])
    monkeypatch.setattr('app.apps.baixabradesco.sheets.load_fingerprints_processados',
                        lambda gc=None: (_ for _ in ()).throw(RuntimeError('cota')))

    r = mod.situacao_por_sp(['111'])

    assert r['ok'] is True
    assert r['sps']['111']['na_fila'] is True
    assert 'aviso_omie' in r


# =====================================================================
# 5. Forçar a baixa de UMA SP
# =====================================================================

def test_forcar_uma_sp_ignora_o_relogio_da_proxima_tentativa(aba, monkeypatch):
    """O botão é apertado por alguém olhando a linha. Esperar duas horas pela
    "próxima tentativa" tornaria o botão decorativo."""
    a = aba([_linha('111', proxima=_futuro(5)), _linha('222', proxima=_futuro(5))])
    vistos = []
    monkeypatch.setattr(mod, '_retry_sheets',
                        lambda item, payload: vistos.append(item.get('ID SP')) or {'ok': True})
    monkeypatch.setattr(mod, '_marcar_em_lote', lambda ws, marcas: len(marcas))
    monkeypatch.setattr(mod, '_update_row', lambda *a, **k: None)

    mod.reprocessar_fila({'sps': ['111']})

    assert vistos == ['111'], 'forçar uma SP mexeu em outra, ou não mexeu em nenhuma'


def test_forcar_inclui_quem_ja_desistiu(aba, monkeypatch):
    a = aba([_linha('111', status='FALHOU', tentativas='5', proxima=_futuro(5))])
    vistos = []
    monkeypatch.setattr(mod, '_retry_sheets',
                        lambda item, payload: vistos.append(item.get('ID SP')) or {'ok': True})
    monkeypatch.setattr(mod, '_marcar_em_lote', lambda ws, marcas: len(marcas))
    monkeypatch.setattr(mod, '_update_row', lambda *a, **k: None)

    mod.reprocessar_fila({'sp': '111'})

    assert vistos == ['111']


def test_a_drenagem_automatica_continua_respeitando_o_relogio(aba, monkeypatch):
    """Sem `sps`, nada muda: o cron não pode insistir no que ainda não venceu
    nem no que já desistiu cinco vezes."""
    aba([_linha('111', proxima=_futuro(5)), _linha('222', status='FALHOU', tentativas='5')])
    vistos = []
    monkeypatch.setattr(mod, '_retry_sheets',
                        lambda item, payload: vistos.append(item.get('ID SP')) or {'ok': True})
    monkeypatch.setattr(mod, '_marcar_em_lote', lambda ws, marcas: len(marcas))
    monkeypatch.setattr(mod, '_update_row', lambda *a, **k: None)

    mod.reprocessar_fila({})

    assert vistos == []


# =====================================================================
# 6. A rota que a tela chama
# =====================================================================

@pytest.fixture
def cliente(monkeypatch):
    monkeypatch.delenv('BAIXABRADESCO_SECRET', raising=False)
    from app.apps.baixabradesco import routes as rotas
    from flask import Flask
    from app.apps.baixabradesco import bp
    app = Flask(__name__)
    app.register_blueprint(bp, url_prefix='/api/baixabradesco')
    monkeypatch.setattr(rotas, 'situacao_por_sp',
                        lambda sps, conferir_omie=True: {'ok': True, 'pedidas': list(sps),
                                                         'conferir_omie': conferir_omie})
    return app.test_client()


def test_a_rota_aceita_as_sps_por_endereco(cliente):
    """A tela chama por GET; o dono também consegue abrir no celular."""
    r = cliente.get('/api/baixabradesco/situacao?sps=111,222')
    assert r.status_code == 200
    assert r.get_json()['pedidas'] == ['111', '222']


def test_a_rota_aceita_as_sps_no_corpo(cliente):
    r = cliente.post('/api/baixabradesco/situacao', json={'sps': ['111']})
    assert r.get_json()['pedidas'] == ['111']


def test_a_rota_recusa_consulta_sem_sp(cliente):
    r = cliente.get('/api/baixabradesco/situacao')
    assert r.status_code == 400


def test_a_rota_tem_teto_de_sps(cliente):
    """Senão a tela pede a fila inteira numa chamada e volta o custo de antes."""
    r = cliente.get('/api/baixabradesco/situacao?sps=' + ','.join(str(i) for i in range(300)))
    assert r.status_code == 400
    assert '200' in r.get_json()['error']


def test_a_rota_deixa_dispensar_a_leitura_do_omie(cliente):
    r = cliente.get('/api/baixabradesco/situacao?sps=111&conferir_omie=0')
    assert r.get_json()['conferir_omie'] is False
