# -*- coding: utf-8 -*-
"""O que NÃO foi baixado precisa chegar em alguém.

Até 11/09/2026 um comprovante que não casava não gerava nada — nem mensagem,
nem linha em planilha. A explicação existia só dentro da resposta devolvida ao
Make, e quem não fosse procurar não ficava sabendo. Foi assim que uma rescisão
de R$ 452,40 passou o dia sem ser baixada.

O que o dono pediu: avisar o que falhou, não o que deu certo. E por lote, não um
aviso por comprovante.
"""
import os

import pytest

from app.apps.baixabradesco.avisos import coletar_falhas, enviar_aviso, montar_aviso


def plano(pode_executar=True, motivos=None, pagina=1, valor='452,40',
          nome='JOAO DA SILVA EXEMPLO', arquivo='comprovantes.pdf',
          motivo_match='', responses=None):
    return {
        'pode_executar': pode_executar,
        'motivos_bloqueio': motivos or [],
        'match': {'motivo': motivo_match},
        'receipt': {'page': pagina, 'valor_pago': valor,
                    'nome_recebedor': nome, 'filename': arquivo},
        'responses': responses or {},
    }


def resultado(planos=None, recusados=None, executaveis=0):
    return {
        'planos': planos or [],
        'recusados': recusados or [],
        'resumo': {'executaveis': executaveis},
    }


# ── O que entra no aviso ──────────────────────────────────────────────────────

def test_comprovante_que_nao_casou_entra_no_aviso():
    r = resultado([plano(pode_executar=False,
                         motivos=['Nenhum candidato encontrado.'])])
    texto = montar_aviso(r)
    assert 'NÃO foram baixados' in texto
    assert 'Nenhum candidato encontrado.' in texto
    assert 'R$ 452,40' in texto
    assert 'JOAO DA SILVA EXEMPLO' in texto


def test_comprovante_recusado_pelo_banco_entra_no_aviso():
    """Pagamento que o banco não efetivou: alguém precisa saber que não saiu."""
    r = resultado(recusados=[{'arquivo': 'x.pdf', 'pagina': 3,
                              'motivo': 'O banco não efetivou a operação.'}])
    assert 'não efetivou' in montar_aviso(r)


def test_falha_ao_baixar_no_omie_entra_no_aviso():
    """Casou, tentou, e o Omie recusou. É dinheiro parado."""
    r = resultado([plano(responses={'fila_omie': {'ok': True}})])
    assert 'Falha ao baixar no Omie' in montar_aviso(r)


def test_aborto_da_transferencia_entra_no_aviso():
    r = resultado([plano(responses={'omie': [
        {'step': 'transferencia_somapay', 'response': {'ok': False}},
        {'step': 'abort_apos_transferencia', 'motivo': '...'},
    ]})])
    assert 'Omie' in montar_aviso(r)


# ── O que NÃO entra, de propósito ─────────────────────────────────────────────

def test_lote_inteiro_bem_sucedido_nao_gera_aviso():
    """O dono foi explícito: o que baixou normal, não precisa avisar."""
    r = resultado([plano(), plano(pagina=2)], executaveis=2)
    assert montar_aviso(r) == ''
    assert coletar_falhas(r) == []


def test_duplicado_nao_gera_aviso():
    """A trava contra baixar duas vezes fez o trabalho dela — não é falha."""
    r = resultado([plano()], executaveis=1)
    r['duplicados'] = [{'arquivo': 'x.pdf', 'pagina': 1, 'motivo': 'Já baixado antes.'}]
    assert montar_aviso(r) == ''


def test_lote_vazio_nao_gera_aviso():
    assert montar_aviso(resultado()) == ''


# ── Um aviso por lote, e legível ──────────────────────────────────────────────

def test_varias_falhas_viram_um_aviso_so():
    r = resultado([plano(pode_executar=False, motivos=['A'], pagina=1),
                   plano(pode_executar=False, motivos=['B'], pagina=2),
                   plano(pode_executar=False, motivos=['C'], pagina=3)])
    texto = montar_aviso(r)
    assert texto.count('NÃO foram baixados') == 1
    assert '3 comprovante(s)' in texto
    for m in ('A', 'B', 'C'):
        assert m in texto


def test_muitas_falhas_nao_viram_parede_de_texto():
    r = resultado([plano(pode_executar=False, motivos=[f'motivo {i}'], pagina=i)
                   for i in range(1, 26)])
    texto = montar_aviso(r)
    assert '25 comprovante(s)' in texto
    assert 'e mais 15' in texto
    assert 'motivo 20' not in texto


def test_o_aviso_diz_quantos_baixaram_normal():
    r = resultado([plano(pode_executar=False, motivos=['x'])], executaveis=7)
    assert 'Baixados normalmente: 7' in montar_aviso(r)


def test_o_aviso_nao_usa_marcacao_que_o_telegram_quebra():
    """Nome com asterisco ou sublinhado já derrubou envio do Telegram antes."""
    r = resultado([plano(pode_executar=False, motivos=['x'],
                         nome='FULANO_DE_TAL *EXEMPLO*')])
    texto = montar_aviso(r)
    assert '*' not in texto.replace('*EXEMPLO*', '')  # só o que veio do dado
    assert texto.count('_') == texto.count('FULANO_DE_TAL') * 2


# ── O envio nunca pode derrubar a baixa ───────────────────────────────────────

def test_o_aviso_vai_para_o_dono_sem_precisar_configurar_nada(monkeypatch):
    """Reusa a convenção que o chatbot e o processarnovasp já usam."""
    from app.apps.baixabradesco.avisos import resolver_telefone
    monkeypatch.delenv('BAIXABRADESCO_AVISO_TELEFONE', raising=False)
    monkeypatch.delenv('CHATBOT_MASTER_PHONE', raising=False)
    assert resolver_telefone()


def test_o_destino_pode_ser_trocado_por_configuracao(monkeypatch):
    from app.apps.baixabradesco.avisos import resolver_telefone
    monkeypatch.setenv('BAIXABRADESCO_AVISO_TELEFONE', '5511999999999')
    assert resolver_telefone() == '5511999999999'


def test_lote_sem_falha_nao_manda_mensagem(monkeypatch):
    monkeypatch.setenv('BAIXABRADESCO_AVISO_TELEFONE', '5585900000000')
    r = enviar_aviso(resultado([plano()], executaveis=1))
    assert r['skipped'] is True
    assert r['motivo'] == 'nada a avisar'


AUTH = {'zapi': {'instance_id': 'i', 'api_token': 't', 'client_token': 'c'}}


def test_falha_no_envio_nao_levanta_erro(monkeypatch):
    """A baixa já aconteceu. Avisar não pode derrubar nada."""
    monkeypatch.setenv('BAIXABRADESCO_AVISO_TELEFONE', '5585900000000')

    import app.apps.baixabradesco.zapi as zapi
    def explode(*a, **k):
        raise RuntimeError('Z-API fora do ar')
    monkeypatch.setattr(zapi, 'send_text', explode)

    r = enviar_aviso(resultado([plano(pode_executar=False, motivos=['x'])]), AUTH)
    assert r['ok'] is False
    assert 'Z-API fora do ar' in r['erro']


def test_envia_pelo_whatsapp_para_o_telefone_configurado(monkeypatch):
    monkeypatch.setenv('BAIXABRADESCO_AVISO_TELEFONE', '5585900000000')
    enviados = {}

    import app.apps.baixabradesco.zapi as zapi
    def fake(auth, phone, message):
        enviados.update(auth=auth, phone=phone, message=message)
        return {'ok': True, 'whatsapp': {'ok': True}, 'telegram': {'ok': True}}
    monkeypatch.setattr(zapi, 'send_text', fake)

    r = enviar_aviso(resultado([plano(pode_executar=False,
                                      motivos=['Nenhum candidato encontrado.'])]), AUTH)
    assert r['ok'] is True
    assert enviados['phone'] == '5585900000000'
    assert 'Nenhum candidato encontrado.' in enviados['message']


def test_usa_as_credenciais_que_vieram_no_pedido(monkeypatch):
    """É assim que as credenciais Z-API chegam hoje: dentro do pedido do Make."""
    monkeypatch.setenv('BAIXABRADESCO_AVISO_TELEFONE', '5585900000000')
    enviados = {}

    import app.apps.baixabradesco.zapi as zapi
    monkeypatch.setattr(zapi, 'send_text',
                        lambda auth, phone, message: enviados.update(auth=auth) or {'ok': True})

    enviar_aviso(resultado([plano(pode_executar=False, motivos=['x'])]), AUTH)
    assert enviados['auth']['instanceId'] == 'i'
    assert enviados['auth']['apiToken'] == 't'


def test_sem_credenciais_zapi_cai_no_notificador(monkeypatch):
    """Assim o aviso ainda sai, pelo caminho que lê as credenciais do ambiente."""
    monkeypatch.setenv('BAIXABRADESCO_AVISO_TELEFONE', '5585900000000')
    for v in ('ZAPI_INSTANCE_ID', 'ZAPI_API_TOKEN', 'ZAPI_CLIENT_TOKEN'):
        monkeypatch.delenv(v, raising=False)
    chamou = {}

    import app.apps.notificador as notificador
    def fake(**kwargs):
        chamou.update(kwargs)
        return {'whatsapp': {'ok': True}}
    monkeypatch.setattr(notificador, 'notificar', fake)

    enviar_aviso(resultado([plano(pode_executar=False, motivos=['x'])]), {})
    assert chamou['canais'][0] == 'whatsapp'
    assert chamou['politica'] == 'fallback'
