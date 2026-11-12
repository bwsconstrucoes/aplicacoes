# -*- coding: utf-8 -*-
"""A fila tardia: ver o que está nela, e poder limpá-la.

Pergunta do dono em 12/11/2026: *"onde fica gravado a fila tardia?"* e, em
seguida, *"consigo limpar ela?"*. A resposta era **não** nas duas pontas: ela
fica em arquivo dentro do servidor, não havia como ver o que estava nela sem
mandar processar, e não havia como limpá-la a não ser reiniciando o serviço —
que é o que uma publicação faz, de raspão, sem ninguém escolher.

Duas coisas que este arquivo trava:

1. **A listagem NUNCA devolve o payload.** Ele vem do Make e carrega dentro
   dele a chave e o segredo da API do Omie e os tokens do Z-API — é assim que
   essas credenciais chegam hoje. Uma rota de diagnóstico que devolvesse o
   payload cru estaria publicando as credenciais da empresa para quem tivesse o
   endereço.
2. **Limpar exige confirmação explícita.** Isso joga fora comprovante que ainda
   não foi baixado; sem confirmação a rota só mostra o que sairia. Um "limpar"
   que roda sem confirmar é uma forma elaborada de perder trabalho.
"""
import json
import os

import pytest

from app.apps.baixabradesco import fila_tardia as mod

SEGREDO_OMIE = 'chave-secreta-do-omie-nao-pode-sair-daqui'


@pytest.fixture
def pasta(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, 'FILA_DIR', str(tmp_path))
    return tmp_path


def _adiar(pasta, nome='1700000000_abc123.json', arquivos=('comprovante.pdf',),
           criado_em='12/11/2026 09:00:00', tentativas=0):
    item = {
        'payload': {
            'attachments': [{'filename': a, 'base64': 'JVBERi0x...'} for a in arquivos],
            'omie': {'app_key': SEGREDO_OMIE, 'app_secret': SEGREDO_OMIE},
            'zapi': {'api_token': SEGREDO_OMIE},
        },
        'erro_original': 'Falha ao carregar dados Google Sheets: 429 Quota exceeded',
        'tentativas': tentativas,
        'criado_em': criado_em,
    }
    (pasta / nome).write_text(json.dumps(item, ensure_ascii=False), encoding='utf-8')
    return nome


# =====================================================================
# 1. ver o que está na fila, sem processar e sem vazar credencial
# =====================================================================

def test_o_resumo_mostra_os_comprovantes_adiados(pasta):
    _adiar(pasta, arquivos=('pix_fornecedor.pdf',))

    r = mod.resumo_fila_tardia()

    assert r['pendentes'] == 1
    item = r['itens'][0]
    assert item['comprovantes'] == ['pix_fornecedor.pdf']
    assert item['criado_em'] == '12/11/2026 09:00:00'
    assert '429' in item['erro_original']


def test_o_resumo_NUNCA_devolve_o_payload_nem_as_credenciais(pasta):
    """A credencial do Omie chega dentro do pedido do Make e fica no arquivo."""
    _adiar(pasta)

    r = mod.resumo_fila_tardia()

    bruto = json.dumps(r, ensure_ascii=False)
    assert SEGREDO_OMIE not in bruto
    assert 'app_key' not in bruto
    assert 'base64' not in bruto
    assert 'payload' not in r['itens'][0]


def test_o_resumo_avisa_que_publicacao_apaga_a_fila(pasta):
    _adiar(pasta)

    r = mod.resumo_fila_tardia()

    assert 'APAGA' in r['em_portugues']
    assert '12/11/2026' in r['em_portugues']


def test_fila_vazia_diz_isso_sem_rodeio(pasta):
    r = mod.resumo_fila_tardia()

    assert r['pendentes'] == 0
    assert 'vazia' in r['em_portugues']


def test_arquivo_corrompido_nao_derruba_a_listagem(pasta):
    (pasta / '1700000001_ruim.json').write_text('{isso nao e json', encoding='utf-8')
    _adiar(pasta)

    r = mod.resumo_fila_tardia()

    assert r['pendentes'] == 2
    assert any(i.get('ilegivel') for i in r['itens'])


def test_o_resumo_nao_apaga_nada(pasta):
    _adiar(pasta)

    mod.resumo_fila_tardia()

    assert len(os.listdir(pasta)) == 1


# =====================================================================
# 2. limpar, com confirmação
# =====================================================================

def test_sem_confirmar_ele_so_mostra_o_que_sairia(pasta):
    _adiar(pasta, arquivos=('boleto.pdf',))

    r = mod.limpar_fila_tardia({})

    assert r['apagou'] == 0
    assert r['seria_apagado'] == 1
    assert len(os.listdir(pasta)) == 1
    assert 'confirmar: true' in r['em_portugues']
    assert 'reenviado pelo Make' in r['em_portugues']


def test_com_confirmar_ele_apaga_e_diz_o_que_apagou(pasta):
    _adiar(pasta, nome='1700000000_a.json', arquivos=('pix.pdf',))
    _adiar(pasta, nome='1700000002_b.json', arquivos=('boleto.pdf',))

    r = mod.limpar_fila_tardia({'confirmar': True})

    assert r['apagou'] == 2
    assert os.listdir(pasta) == []
    nomes = sorted(c for i in r['itens_descartados'] for c in i['comprovantes'])
    assert nomes == ['boleto.pdf', 'pix.pdf']
    assert 'NÃO foram baixados' in r['em_portugues']


def test_o_relatorio_da_limpeza_tambem_nao_vaza_credencial(pasta):
    _adiar(pasta)

    r = mod.limpar_fila_tardia({'confirmar': True})

    assert SEGREDO_OMIE not in json.dumps(r, ensure_ascii=False)


def test_limpar_fila_vazia_nao_e_erro(pasta):
    r = mod.limpar_fila_tardia({'confirmar': True})

    assert r['ok'] is True
    assert r['apagou'] == 0
    assert 'já estava vazia' in r['em_portugues']


def test_o_que_nao_deu_para_apagar_aparece_no_relatorio(pasta, monkeypatch):
    _adiar(pasta)
    monkeypatch.setattr(mod.os, 'remove',
                        lambda caminho: (_ for _ in ()).throw(OSError('em uso')))

    r = mod.limpar_fila_tardia({'confirmar': True})

    assert r['apagou'] == 0
    assert 'em uso' in r['falhas'][0]['erro']


# =====================================================================
# 3. a fila tardia parava o serviço — três causas somadas
# =====================================================================
# Diagnóstico do dono em 12/11/2026: *"manda um comprovante, aí manda de novo,
# fica na fila tardia, aí aquilo vai comendo processamento. A gente tem quatro
# threads e essa baixa leva muito tempo: tá ocupando as linhas todas (…) e tá
# tentando fazer baixa de coisa que nem precisaria mais."*
#
# Ele estava certo, e eram três defeitos que se multiplicavam:
#   1. o laço não tinha limite — um disparo reprocessava TODOS numa requisição;
#   2. nada impedia dois disparos ao mesmo tempo;
#   3. o mesmo comprovante reenviado entrava na fila várias vezes.

def test_o_mesmo_comprovante_reenviado_nao_entra_duas_vezes(pasta):
    pedido = {'attachments': [{'filename': 'pix.pdf', 'base64': 'JVBERi0xLjQK'}]}

    primeiro = mod.adiar_payload(pedido, 'quota 429')
    segundo = mod.adiar_payload(pedido, 'quota 429')

    assert segundo['ja_estava_na_fila'] is True
    assert segundo['arquivo'] == primeiro['arquivo']
    assert len([n for n in os.listdir(pasta) if n.endswith('.json')]) == 1


def test_comprovante_diferente_entra_normalmente(pasta):
    mod.adiar_payload({'attachments': [{'filename': 'a.pdf', 'base64': 'AAA'}]}, 'x')
    r = mod.adiar_payload({'attachments': [{'filename': 'b.pdf', 'base64': 'BBB'}]}, 'x')

    assert not r.get('ja_estava_na_fila')
    assert len([n for n in os.listdir(pasta) if n.endswith('.json')]) == 2


def test_mesmo_nome_com_conteudo_diferente_nao_e_o_mesmo_comprovante(pasta):
    """Duas rescisões podem chegar com o mesmo nome de arquivo."""
    mod.adiar_payload({'attachments': [{'filename': 'comprovante.pdf', 'base64': 'AAA'}]}, 'x')
    r = mod.adiar_payload({'attachments': [{'filename': 'comprovante.pdf', 'base64': 'ZZZ'}]}, 'x')

    assert not r.get('ja_estava_na_fila')


def test_uma_passada_por_vez(pasta, monkeypatch):
    """Dois disparos ao mesmo tempo refaziam o mesmo trabalho e tomavam as
    quatro linhas de atendimento do serviço."""
    _adiar(pasta)
    import app.apps.baixabradesco.core as core
    monkeypatch.setattr(core, 'processar_baixabradesco', lambda p: {'resumo': {}})

    assert mod._tomar_lock() is True          # alguém já está rodando
    r = mod.processar_fila_tardia({})

    assert r['pulado'] == 'ja_ha_uma_passada_rodando'
    assert 'ao mesmo tempo' in r['em_portugues']
    assert len([n for n in os.listdir(pasta) if n.endswith('.json')]) == 1


def test_a_trava_abandonada_nao_prende_a_fila_para_sempre(pasta):
    """Se o serviço reiniciar no meio, a marca fica. Ela expira."""
    mod._tomar_lock()
    antiga = os.path.getmtime(mod._caminho_lock()) - (mod.LOCK_VALIDADE_MIN + 5) * 60
    os.utime(mod._caminho_lock(), (antiga, antiga))

    assert mod._tomar_lock() is True


def test_a_trava_e_solta_mesmo_quando_da_erro(pasta, monkeypatch):
    _adiar(pasta)
    import app.apps.baixabradesco.core as core
    monkeypatch.setattr(core, 'processar_baixabradesco',
                        lambda p: (_ for _ in ()).throw(RuntimeError('boom')))

    mod.processar_fila_tardia({})

    assert mod._tomar_lock() is True          # a marca não ficou presa


def test_cada_passada_processa_poucos_adiados(pasta, monkeypatch):
    """Cada adiado custa a carga inteira da SPsBD. Dez numa requisição só é o
    que ocupava as quatro linhas do serviço."""
    for i in range(10):
        _adiar(pasta, nome=f'17000000{i:02d}_x.json', arquivos=(f'c{i}.pdf',))
    chamadas = []
    import app.apps.baixabradesco.core as core
    monkeypatch.setattr(core, 'processar_baixabradesco',
                        lambda p: chamadas.append(p) or {'resumo': {}})

    r = mod.processar_fila_tardia({})

    assert len(chamadas) == mod.LIMITE_POR_DISPARO
    assert mod.LIMITE_POR_DISPARO <= 3
    assert r['pendentes'] == 10 - mod.LIMITE_POR_DISPARO


def test_adiado_antigo_e_descartado_sem_gastar_processamento(pasta, monkeypatch):
    """"Tentando fazer baixa de coisa que nem precisaria mais" — exatamente."""
    _adiar(pasta, nome='1600000000_velho.json', criado_em='01/01/2026 08:00:00')
    _adiar(pasta, nome='1700000000_novo.json',
           criado_em=__import__('time').strftime('%d/%m/%Y %H:%M:%S'))
    chamadas = []
    import app.apps.baixabradesco.core as core
    monkeypatch.setattr(core, 'processar_baixabradesco',
                        lambda p: chamadas.append(p) or {'resumo': {}})

    r = mod.processar_fila_tardia({})

    assert len(chamadas) == 1                 # só o novo foi processado
    assert len(r['descartados_por_idade']) == 1
    assert r['descartados_por_idade'][0]['criado_em'] == '01/01/2026 08:00:00'


def test_a_marca_de_execucao_nao_e_confundida_com_comprovante(pasta):
    mod._tomar_lock()

    assert mod.resumo_fila_tardia()['pendentes'] == 0
