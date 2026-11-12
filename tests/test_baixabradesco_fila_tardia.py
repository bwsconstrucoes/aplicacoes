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
