# -*- coding: utf-8 -*-
"""O cron que já roda de 5 em 5 minutos passou a andar com a fila de falhas.

A contagem em produção, 08/10/2026, respondeu a pergunta do dono — *"e essa
fila, de 2.270 linhas, vai rodar?"* — de um jeito que eu não esperava:

    2.269 linhas, TODAS PENDENTE. Nenhuma concluída. Nenhuma falhada.
    A mais antiga: 18/06/2026.

Nenhuma concluída e nenhuma falhada significa que a fila **nunca andou**, nem uma
vez, em quase quatro meses. Não era lentidão: era uma rota que só funcionava se
alguém a chamasse à mão, e ninguém chamou.

A correção não é pedir que alguém chame: é pegar carona no cron que já roda de 5
em 5 minutos e já está autenticado.

Duas coisas que a composição da fila obrigou a travar:

- **A ordem.** 1.943 das 2.269 são recado de WhatsApp; 238 são baixa no Omie, que
  é dinheiro. Na ordem da planilha o dinheiro ficaria para o fim.
- **O acumulado de avisos antigos não é limpo pelo cron.** São 1.943 linhas do
  dono, e marcá-las em massa é decisão dele.
"""
import pytest

from app.apps.baixabradesco import fila_tardia as mod


@pytest.fixture
def sem_payload_adiado(monkeypatch):
    monkeypatch.setattr(mod, '_listar', lambda: [])


def _dublar_fila(monkeypatch, resposta_por_etapa=None):
    chamadas = []

    def _reprocessar(pedido):
        chamadas.append(pedido)
        etapa = (pedido.get('etapas') or ['?'])[0]
        base = {'pendentes_processados': 1, 'concluidos_agora': 1,
                'ainda_pendentes': 0, 'bloqueados_por_configuracao': 0,
                'o_que_falta_configurar': [], 'interrompido': ''}
        base.update((resposta_por_etapa or {}).get(etapa, {}))
        return base

    import app.apps.baixabradesco.fila as f
    monkeypatch.setattr(f, 'reprocessar_fila', _reprocessar)
    return chamadas


def test_o_dinheiro_e_drenado_antes_do_recado(sem_payload_adiado, monkeypatch):
    chamadas = _dublar_fila(monkeypatch)

    mod.processar_fila_tardia({})

    assert [c['etapas'][0] for c in chamadas] == ['omie', 'sheets', 'pipefy', 'zapi']


def test_o_cron_nao_limpa_em_massa_os_avisos_antigos(sem_payload_adiado, monkeypatch):
    chamadas = _dublar_fila(monkeypatch)

    mod.processar_fila_tardia({})

    assert all(c['descartar_avisos_antigos'] is False for c in chamadas)


def test_cada_etapa_tem_limite_proprio_e_modesto(sem_payload_adiado, monkeypatch):
    chamadas = _dublar_fila(monkeypatch)

    mod.processar_fila_tardia({})

    limites = {c['etapas'][0]: c['limite'] for c in chamadas}
    assert limites == {'omie': 15, 'sheets': 15, 'pipefy': 15, 'zapi': 10}
    assert sum(limites.values()) <= 60   # a cota de escrita do Google é por minuto


def test_cota_estourada_para_a_drenagem_e_deixa_o_resto_para_o_proximo_disparo(
        sem_payload_adiado, monkeypatch):
    chamadas = _dublar_fila(monkeypatch, {'omie': {'interrompido': 'cota_do_google'}})

    r = mod.processar_fila_tardia({})

    assert [c['etapas'][0] for c in chamadas] == ['omie']
    assert r['fila_de_falhas']['parou_por_cota_na_etapa'] == 'omie'


def test_o_que_falta_configurar_sobe_no_relatorio(sem_payload_adiado, monkeypatch):
    _dublar_fila(monkeypatch, {'omie': {
        'bloqueados_por_configuracao': 15,
        'o_que_falta_configurar': ['credenciais_omie_ausentes'],
        'concluidos_agora': 0,
    }})

    r = mod.processar_fila_tardia({})

    assert r['fila_de_falhas']['omie']['o_que_falta_configurar'] == \
        ['credenciais_omie_ausentes']


def test_erro_numa_etapa_nao_impede_as_outras(sem_payload_adiado, monkeypatch):
    import app.apps.baixabradesco.fila as f
    chamadas = []

    def _reprocessar(pedido):
        etapa = pedido['etapas'][0]
        chamadas.append(etapa)
        if etapa == 'omie':
            raise RuntimeError('planilha fora do ar')
        return {'pendentes_processados': 0, 'concluidos_agora': 0,
                'ainda_pendentes': 0, 'interrompido': ''}

    monkeypatch.setattr(f, 'reprocessar_fila', _reprocessar)

    r = mod.processar_fila_tardia({})

    assert chamadas == ['omie', 'sheets', 'pipefy', 'zapi']
    assert 'planilha fora do ar' in r['fila_de_falhas']['omie']['erro']


def test_a_drenagem_nao_derruba_o_trabalho_original_do_cron(monkeypatch):
    """O cron existe para reprocessar payload adiado; a fila é carona."""
    monkeypatch.setattr(mod, '_listar', lambda: [])
    import app.apps.baixabradesco.fila as f
    monkeypatch.setattr(f, 'reprocessar_fila',
                        lambda pedido: (_ for _ in ()).throw(RuntimeError('boom')))

    r = mod.processar_fila_tardia({})

    assert r['ok'] is True
    assert r['processados'] == []
