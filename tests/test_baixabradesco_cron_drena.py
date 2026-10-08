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


# =====================================================================
# o mutirão único: zerar o que ficou para trás
# =====================================================================
# Pedido do dono em 08/10/2026: *"só preciso que rode as coisas desse mês em
# diante. O que tá pra trás, poderia zerar."* Eu tinha entregado a ferramenta e
# deixado o gatilho com ele — que não tem como fazer um POST pelo celular. Ou
# seja: entrega pela metade. O mutirão pega carona no mesmo cron.

def _dublar_zerar(monkeypatch, resposta=None):
    pedidos = []
    import app.apps.baixabradesco.fila as f

    def _zerar(pedido):
        pedidos.append(pedido)
        return resposta or {'ok': True, 'antes_de': pedido.get('antes_de'),
                            'dispensadas': 7, 'por_etapa': {'zapi': 7}}

    monkeypatch.setattr(f, 'zerar_fila_antiga', _zerar)
    return pedidos


def test_o_cron_zera_o_atraso_com_a_data_autorizada(sem_payload_adiado, monkeypatch):
    _dublar_fila(monkeypatch)
    pedidos = _dublar_zerar(monkeypatch)

    r = mod.processar_fila_tardia({})

    assert pedidos[0]['antes_de'] == '01/10/2026'
    assert r['atraso_zerado']['dispensadas'] == 7


def test_a_data_do_mutirao_e_fixa_nao_anda_com_o_calendario(sem_payload_adiado,
                                                            monkeypatch):
    """Ele autorizou zerar o que estava para trás NAQUELE dia.

    Uma regra que andasse com o calendário dispensaria pendência nova todo dia
    primeiro — a forma mais silenciosa possível de perder trabalho.
    """
    assert mod.ZERAR_ANTES_DE == '01/10/2026'
    _dublar_fila(monkeypatch)
    pedidos = _dublar_zerar(monkeypatch)

    mod.processar_fila_tardia({})

    assert pedidos[0]['antes_de'] == '01/10/2026'


def test_o_mutirao_anda_em_blocos_para_nao_tomar_a_cota(sem_payload_adiado,
                                                        monkeypatch):
    _dublar_fila(monkeypatch)
    pedidos = _dublar_zerar(monkeypatch)

    mod.processar_fila_tardia({})

    assert pedidos[0]['limite'] == mod.ZERAR_POR_DISPARO
    assert mod.ZERAR_POR_DISPARO <= 500


def test_zera_ANTES_de_drenar(sem_payload_adiado, monkeypatch):
    """Senão a drenagem gastaria a passada nas linhas que vão ser dispensadas
    dois segundos mais tarde."""
    ordem = []
    import app.apps.baixabradesco.fila as f
    monkeypatch.setattr(f, 'zerar_fila_antiga',
                        lambda p: ordem.append('zerar') or {'dispensadas': 0})
    monkeypatch.setattr(f, 'reprocessar_fila',
                        lambda p: ordem.append('drenar') or
                        {'pendentes_processados': 0, 'interrompido': ''})

    mod.processar_fila_tardia({})

    assert ordem[0] == 'zerar'
    assert 'drenar' in ordem


def test_da_para_desligar_o_mutirao_sem_mexer_no_codigo(sem_payload_adiado,
                                                        monkeypatch):
    monkeypatch.setenv('BAIXABRADESCO_ZERAR_ANTES_DE', ' ')
    _dublar_fila(monkeypatch)
    pedidos = _dublar_zerar(monkeypatch)

    r = mod.processar_fila_tardia({})

    assert pedidos == []
    assert r['atraso_zerado'] == {'desligado': True}


def test_o_ambiente_pode_trocar_a_data_do_mutirao(sem_payload_adiado, monkeypatch):
    monkeypatch.setenv('BAIXABRADESCO_ZERAR_ANTES_DE', '15/09/2026')
    _dublar_fila(monkeypatch)
    pedidos = _dublar_zerar(monkeypatch)

    mod.processar_fila_tardia({})

    assert pedidos[0]['antes_de'] == '15/09/2026'


def test_mutirao_que_explode_nao_impede_a_drenagem(sem_payload_adiado, monkeypatch):
    chamadas = _dublar_fila(monkeypatch)
    import app.apps.baixabradesco.fila as f
    monkeypatch.setattr(f, 'zerar_fila_antiga',
                        lambda p: (_ for _ in ()).throw(RuntimeError('planilha fora')))

    r = mod.processar_fila_tardia({})

    assert 'planilha fora' in r['atraso_zerado']['erro']
    assert [c['etapas'][0] for c in chamadas] == ['omie', 'sheets', 'pipefy', 'zapi']


def test_o_mutirao_avisa_quando_acabou_e_diz_o_que_fazer(sem_payload_adiado,
                                                         monkeypatch):
    """Um mutirão precisa saber dizer que acabou.

    Senão alguém fica olhando número sem saber o que esperar — e a varredura
    segue custando uma leitura da faixa de controle a cada cinco minutos, de
    graça.
    """
    _dublar_fila(monkeypatch)
    _dublar_zerar(monkeypatch, {'ok': True, 'antes_de': '01/10/2026',
                                'encontradas': 0, 'dispensadas': 0,
                                'por_etapa': {}})

    r = mod.processar_fila_tardia({})

    assert r['atraso_zerado']['concluido'] is True
    assert 'BAIXABRADESCO_ZERAR_ANTES_DE' in r['atraso_zerado']['em_portugues']
    assert '01/10/2026' in r['atraso_zerado']['em_portugues']


def test_enquanto_ha_atraso_ele_nao_se_declara_concluido(sem_payload_adiado,
                                                         monkeypatch):
    _dublar_fila(monkeypatch)
    _dublar_zerar(monkeypatch, {'ok': True, 'antes_de': '01/10/2026',
                                'encontradas': 500, 'dispensadas': 500,
                                'por_etapa': {'zapi': 500}})

    r = mod.processar_fila_tardia({})

    assert 'concluido' not in r['atraso_zerado']
    assert r['atraso_zerado']['dispensadas'] == 500


def test_o_cron_dispensa_aviso_vencido_antes_de_drenar(sem_payload_adiado,
                                                       monkeypatch):
    """Sem isso, a fila trava em avisos que nunca serão enviados.

    Foi o que aconteceu em 08/10/2026: 116 das 121 pendências restantes eram
    aviso de 01/10 a 05/10, velho demais para enviar e nunca dispensado.
    """
    ordem = []
    import app.apps.baixabradesco.fila as f
    monkeypatch.setattr(f, 'zerar_fila_antiga',
                        lambda p: ordem.append('zerar') or {'dispensadas': 0})
    monkeypatch.setattr(f, 'dispensar_avisos_vencidos',
                        lambda p: ordem.append('avisos') or {'dispensadas': 116})
    monkeypatch.setattr(f, 'reprocessar_fila',
                        lambda p: ordem.append('drenar') or
                        {'pendentes_processados': 0, 'interrompido': ''})

    r = mod.processar_fila_tardia({})

    assert ordem.index('avisos') < ordem.index('drenar')
    assert r['avisos_vencidos']['dispensadas'] == 116


def test_falha_ao_dispensar_aviso_nao_impede_a_drenagem(sem_payload_adiado,
                                                        monkeypatch):
    chamadas = _dublar_fila(monkeypatch)
    import app.apps.baixabradesco.fila as f
    monkeypatch.setattr(f, 'zerar_fila_antiga', lambda p: {'dispensadas': 0})
    monkeypatch.setattr(f, 'dispensar_avisos_vencidos',
                        lambda p: (_ for _ in ()).throw(RuntimeError('planilha fora')))

    r = mod.processar_fila_tardia({})

    assert 'planilha fora' in r['avisos_vencidos']['erro']
    assert [c['etapas'][0] for c in chamadas] == ['omie', 'sheets', 'pipefy', 'zapi']
