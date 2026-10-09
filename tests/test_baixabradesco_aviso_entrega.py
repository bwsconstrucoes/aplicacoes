# -*- coding: utf-8 -*-
"""O aviso tem de dizer QUEM recebeu, e por onde.

Em 13/09/2026 o dono recebeu um aviso de falha **pelo Telegram**. Isso é o
sintoma, não o conforto: o braço do WhatsApp não entregou, e o financeiro — que
não tem Telegram cadastrado — pelo visto não recebeu nada. Ninguém foi avisado
disso, porque o envio devolve sucesso quando QUALQUER canal entrega.

É o mesmo defeito da gravação da SPsBD com outra roupa: um "ok" que mente. E num
aviso de falha ele é especialmente ruim — o aviso existe justamente para quando
algo deu errado, e falhar em silêncio no aviso de falha é falhar duas vezes.

Dois consertos travados aqui:

1. **O resultado diz quem recebeu por WhatsApp, quem recebeu só pelo Telegram e
   quem não recebeu nada**, com um alerta em português quando alguém ficou só no
   Telegram.
2. **Credencial presente com envio falhando ganha segunda tentativa.** Antes a
   segunda tentativa só existia quando a credencial estava *ausente* — então
   instância do Z-API fora do ar significava financeiro sem aviso, sem retentar.
"""
import pytest

from app.apps.baixabradesco import avisos as mod


RESULTADO = {
    'recusados': [
        {'pagina': 1, 'arquivo': 'comprovante.pdf',
         'motivo': 'O banco não efetivou a operação.'},
    ],
}


@pytest.fixture
def dois_telefones(monkeypatch):
    monkeypatch.setattr(mod, 'resolver_telefones',
                        lambda: ['5585996992197', '5585987846225'])


def _zapi(monkeypatch, por_telefone):
    """Dubla o envio Z-API: resposta por telefone."""
    import app.apps.baixabradesco.zapi as z
    monkeypatch.setattr(z, 'resolve_zapi_auth',
                        lambda payload: {'instanceId': 'i', 'apiToken': 'a',
                                         'clientToken': 'c'})
    monkeypatch.setattr(z, 'validate_zapi_auth', lambda auth: '')
    monkeypatch.setattr(z, 'send_text',
                        lambda auth, phone, msg: por_telefone[phone])


def _notificador(monkeypatch, resposta):
    chamados = []
    import app.apps.notificador as n

    def _notificar(**kwargs):
        chamados.append(kwargs.get('telefone'))
        return resposta

    monkeypatch.setattr(n, 'notificar', _notificar)
    return chamados


def _entregue():
    return {'ok': True, 'whatsapp': {'ok': True, 'status': 200},
            'telegram': {'ok': True}}


def _so_telegram():
    return {'ok': True, 'whatsapp': {'ok': False, 'status': 500},
            'telegram': {'ok': True}}


def _nada():
    return {'ok': False, 'whatsapp': {'ok': False, 'status': 500},
            'telegram': {'ok': False}}


# =====================================================================
# 1. o resultado conta a verdade por canal
# =====================================================================

def test_whatsapp_entregue_nos_dois_nao_gera_alerta(dois_telefones, monkeypatch):
    _zapi(monkeypatch, {'5585996992197': _entregue(), '5585987846225': _entregue()})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['ok'] is True
    assert sorted(r['entregues_no_whatsapp']) == ['5585987846225', '5585996992197']
    assert r['so_pelo_telegram'] == []
    assert r['sem_entrega'] == []
    assert 'alerta' not in r


def test_quem_ficou_so_no_telegram_aparece_com_alerta_em_portugues(
        dois_telefones, monkeypatch):
    """O caso real de 13/09: o dono recebeu, o financeiro não."""
    _zapi(monkeypatch, {'5585996992197': _so_telegram(),
                        '5585987846225': _entregue()})
    _notificador(monkeypatch, {'whatsapp': {'ok': False}, 'telegram': {'ok': True}})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['so_pelo_telegram'] == ['5585996992197']
    assert '5585996992197' in r['alerta']
    assert 'WhatsApp não entregou' in r['alerta']
    assert 'Telegram' in r['alerta']


def test_ninguem_recebeu_e_alerta_diferente_e_mais_grave(dois_telefones, monkeypatch):
    _zapi(monkeypatch, {'5585996992197': _nada(), '5585987846225': _nada()})
    _notificador(monkeypatch, {'whatsapp': {'ok': False}, 'telegram': {'ok': False}})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['ok'] is False
    assert sorted(r['sem_entrega']) == ['5585987846225', '5585996992197']
    assert 'Nenhum canal entregou' in r['alerta']


def test_whatsapp_desligado_de_proposito_nao_e_falha(dois_telefones, monkeypatch):
    """NOTIFICAR_WHATSAPP=0 é escolha, não defeito — e não gera alerta."""
    desligado = {'ok': True, 'whatsapp': {'ok': None, 'detalhe': 'canal desativado'},
                 'telegram': {'ok': True}}
    _zapi(monkeypatch, {'5585996992197': desligado, '5585987846225': desligado})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['so_pelo_telegram'] == []
    assert 'alerta' not in r


# =====================================================================
# 2. a segunda tentativa que não existia
# =====================================================================

def test_credencial_presente_com_envio_falhando_ganha_segunda_tentativa(
        monkeypatch):
    """Antes só havia segunda tentativa quando a credencial estava AUSENTE."""
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    _zapi(monkeypatch, {'5585996992197': _so_telegram()})
    chamados = _notificador(monkeypatch,
                            {'whatsapp': {'ok': True}, 'telegram': {'ok': None}})

    r = mod.enviar_aviso(RESULTADO, {})

    assert chamados == ['5585996992197']
    envio = r['envios']['5585996992197']
    assert envio['whatsapp']['ok'] is True
    assert envio['whatsapp']['segunda_tentativa'] == {'ok': True}
    # a segunda tentativa deu certo, então ninguém ficou só no Telegram
    assert r['so_pelo_telegram'] == []
    assert r['entregues_no_whatsapp'] == ['5585996992197']


def test_whatsapp_entregue_na_primeira_nao_tenta_de_novo(monkeypatch):
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    _zapi(monkeypatch, {'5585996992197': _entregue()})
    chamados = _notificador(monkeypatch, {'whatsapp': {'ok': True}})

    mod.enviar_aviso(RESULTADO, {})

    assert chamados == []


def test_whatsapp_desligado_nao_insiste_pelo_notificador(monkeypatch):
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    desligado = {'ok': True, 'whatsapp': {'ok': None, 'detalhe': 'canal desativado'},
                 'telegram': {'ok': True}}
    _zapi(monkeypatch, {'5585996992197': desligado})
    chamados = _notificador(monkeypatch, {'whatsapp': {'ok': True}})

    mod.enviar_aviso(RESULTADO, {})

    assert chamados == []


def test_a_segunda_tentativa_nao_apaga_o_que_a_primeira_informou(monkeypatch):
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    _zapi(monkeypatch, {'5585996992197': _so_telegram()})
    _notificador(monkeypatch, {'whatsapp': {'ok': False}, 'telegram': {'ok': True}})

    r = mod.enviar_aviso(RESULTADO, {})
    envio = r['envios']['5585996992197']

    assert envio['whatsapp']['primeira_tentativa'] == {'ok': False, 'status': 500}
    assert envio['telegram'] == {'ok': True}
    assert envio['notificador']['telegram'] == {'ok': True}
    assert r['so_pelo_telegram'] == ['5585996992197']


def test_avisar_nunca_derruba_a_baixa_que_ja_aconteceu(monkeypatch):
    """A baixa já foi feita; explodir no aviso seria perder o trabalho todo."""
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    import app.apps.baixabradesco.zapi as z
    monkeypatch.setattr(z, 'resolve_zapi_auth',
                        lambda payload: (_ for _ in ()).throw(RuntimeError('boom')))
    _notificador(monkeypatch, {'whatsapp': {'ok': False}, 'telegram': {'ok': False}})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['ok'] is False
    assert 'alerta' in r


# =====================================================================
# 3. sem credencial Z-API — provavelmente o caminho da produção
# =====================================================================
# As credenciais Z-API chegam DENTRO do pedido do Make. O serviço automático
# (o cron) não tem pedido nenhum, então ele cai sempre no notificador — que
# devolve um dicionário POR CANAL, sem `ok` no topo. Devolver isso cru fazia o
# relatório dizer "nenhum canal entregou" mesmo quando o Telegram entregava, e
# zerava o `ok` do aviso inteiro. Mentia justamente onde mais se olha.

def _sem_zapi(monkeypatch):
    import app.apps.baixabradesco.zapi as z
    monkeypatch.setattr(z, 'resolve_zapi_auth',
                        lambda payload: {'instanceId': '', 'apiToken': '',
                                         'clientToken': ''})
    monkeypatch.setattr(z, 'validate_zapi_auth',
                        lambda auth: 'ZAPI_INSTANCE_ID, ZAPI_API_TOKEN')
    monkeypatch.setattr(z, 'send_text',
                        lambda *a, **k: (_ for _ in ()).throw(
                            AssertionError('não deve chamar o Z-API sem credencial')))


def test_notificador_que_entrega_pelo_telegram_nao_e_contado_como_falha(
        monkeypatch):
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585987846225'])
    _sem_zapi(monkeypatch)
    _notificador(monkeypatch, {'whatsapp': {'ok': False},
                               'telegram': {'ok': True, 'chat_id': '701...'}})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['ok'] is True
    assert r['sem_entrega'] == []
    assert r['so_pelo_telegram'] == ['5585987846225']
    assert 'WhatsApp não entregou' in r['alerta']


def test_notificador_que_entrega_pelo_whatsapp_conta_como_whatsapp(monkeypatch):
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    _sem_zapi(monkeypatch)
    _notificador(monkeypatch, {'whatsapp': {'ok': True},
                               'telegram': {'ok': None, 'detalhe': 'não executado'}})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['ok'] is True
    assert r['entregues_no_whatsapp'] == ['5585996992197']
    assert r['so_pelo_telegram'] == []
    assert 'alerta' not in r


def test_notificador_que_nao_entrega_por_canal_nenhum_e_falha_mesmo(monkeypatch):
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    _sem_zapi(monkeypatch)
    _notificador(monkeypatch, {'whatsapp': {'ok': False}, 'telegram': {'ok': False}})

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['ok'] is False
    assert r['sem_entrega'] == ['5585996992197']
    assert 'Nenhum canal entregou' in r['alerta']


def test_a_resposta_do_notificador_fica_guardada_inteira(monkeypatch):
    """Para quem for investigar: nada da resposta original se perde."""
    monkeypatch.setattr(mod, 'resolver_telefones', lambda: ['5585996992197'])
    _sem_zapi(monkeypatch)
    bruto = {'whatsapp': {'ok': False, 'detalhe': 'instância desconectada'},
             'telegram': {'ok': True}}
    _notificador(monkeypatch, bruto)

    r = mod.enviar_aviso(RESULTADO, {})

    assert r['envios']['5585996992197']['notificador'] == bruto
