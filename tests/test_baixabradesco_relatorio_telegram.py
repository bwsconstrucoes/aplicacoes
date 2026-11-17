# -*- coding: utf-8 -*-
"""O relatório do lote, no Telegram — 17/11/2026.

Com o Make deixando de esperar a resposta, a lista do que aconteceu com cada
comprovante deixou de ter para onde ir. O dono escolheu o destino:

    *"o retorno, a resposta ao final de tudo, aí você me responde.
    Preferencialmente no Telegram, para não ter problema. Aqui, ó: comprovante
    tal, enviado tal, baixou SP tal, valor tal. Comprovante tal, valor tal, não
    baixou SP, não localizou. (…) Manda no Telegram o que você encaminharia no
    Make."*

"Para não ter problema" é literal: o WhatsApp sai pelo Z-API, que bloqueia
quando o volume sobe — e já deixou aviso sem entregar (13/09/2026).

O que fica travado aqui:

1. O relatório conta o lote INTEIRO, não só o que deu errado (quem faz isso é
   o `enviar_aviso`, que continua existindo e indo para o financeiro).
2. Ele sai pelo Telegram.
3. Ele NUNCA leva credencial — o payload do Make traz a chave do Omie.
4. Falha no envio não derruba a baixa, que já aconteceu.
"""
import pytest

from app.apps.baixabradesco import avisos


def _plano(sp='7001', valor='1.234,56', pode=True, **respostas):
    return {
        'pode_executar': pode,
        'receipt': {'filename': 'comprovantes.pdf', 'page': 1,
                    'valor_pago': valor, 'data_pagamento': '16/11/2026'},
        'match': {'id': sp, 'motivo': ''},
        'responses': respostas,
        'motivos_bloqueio': [],
    }


def _resultado(planos=None, duplicados=None, recusados=None):
    return {
        'planos': planos or [],
        'duplicados': duplicados or [],
        'recusados': recusados or [],
        'resumo': {},
    }


# =====================================================================
# 1. O conteúdo é o que ele pediu
# =====================================================================

def test_o_que_baixou_aparece_com_sp_e_valor():
    msgs = avisos.montar_relatorio(_resultado([_plano()]))
    texto = '\n'.join(msgs)
    assert 'comprovantes.pdf' in texto
    assert 'R$ 1.234,56' in texto
    assert 'SP 7001' in texto
    assert '✅' in texto


def test_o_que_nao_baixou_aparece_com_o_motivo():
    plano = _plano(pode=False)
    plano['motivos_bloqueio'] = ['SP não localizada na planilha.']
    texto = '\n'.join(avisos.montar_relatorio(_resultado([plano])))
    assert 'NÃO baixou' in texto
    assert 'SP não localizada na planilha.' in texto


def test_o_omie_que_nao_confirmou_nao_conta_como_baixado():
    """A SP continua a pagar; dizer "baixado" aqui seria o engano de 14/09."""
    plano = _plano(nao_marcou='A planilha e o card NÃO foram marcados...')
    texto = '\n'.join(avisos.montar_relatorio(_resultado([plano])))
    assert 'NÃO baixou' in texto
    assert 'reenvie' in texto.lower()


def test_baixa_pela_metade_e_dita_como_pela_metade():
    plano = _plano(sheets={'ok': False, 'erros': ['cota']})
    texto = '\n'.join(avisos.montar_relatorio(_resultado([plano])))
    assert '⚠️' in texto
    assert 'planilha na fila' in texto
    assert 'baixou no Omie' in texto


def test_o_repetido_e_o_recusado_pelo_banco_aparecem():
    r = _resultado(
        duplicados=[{'arquivo': 'c.pdf', 'pagina': 2, 'sp': '6990'}],
        recusados=[{'arquivo': 'c.pdf', 'pagina': 3}],
    )
    texto = '\n'.join(avisos.montar_relatorio(r))
    assert 'já tinha sido baixado' in texto and 'SP 6990' in texto
    assert 'não efetivou' in texto


def test_o_cabecalho_conta_quantos_de_cada():
    planos = [_plano(sp='1'), _plano(sp='2'), _plano(sp='3', pode=False)]
    texto = avisos.montar_relatorio(_resultado(planos))[0]
    assert '2 baixado(s)' in texto
    assert '1 sem baixar' in texto


def test_lote_vazio_nao_vira_mensagem():
    assert avisos.montar_relatorio(_resultado()) == []


# =====================================================================
# 2. Cabe no Telegram
# =====================================================================

def test_lote_grande_vira_varias_mensagens():
    planos = [_plano(sp=str(i)) for i in range(120)]
    msgs = avisos.montar_relatorio(_resultado(planos))
    assert len(msgs) > 1
    assert all(len(m) <= avisos.LIMITE_TELEGRAM + 200 for m in msgs)


def test_lote_gigante_nao_vira_vinte_mensagens_no_celular():
    planos = [_plano(sp=str(i)) for i in range(5000)]
    msgs = avisos.montar_relatorio(_resultado(planos))
    assert len(msgs) <= avisos.LIMITE_MENSAGENS
    assert 'continua' in msgs[-1]


# =====================================================================
# 3. Para onde vai, e o que não pode ir junto
# =====================================================================

def test_vai_pelo_telegram(monkeypatch):
    enviados = []
    import app.apps.notificador as notificador
    monkeypatch.setattr(notificador, 'enviar_telegram',
                        lambda telefone, mensagem: enviados.append((telefone, mensagem))
                        or {'ok': True})

    r = avisos.enviar_relatorio(_resultado([_plano()]), {})

    assert r['ok'] is True
    assert len(enviados) == 1
    assert enviados[0][0] == avisos.TELEFONE_DONO


def test_o_relatorio_nao_leva_credencial(monkeypatch):
    enviados = []
    import app.apps.notificador as notificador
    monkeypatch.setattr(notificador, 'enviar_telegram',
                        lambda telefone, mensagem: enviados.append(mensagem) or {'ok': True})

    payload = {'omie': {'app_key': 'CHAVE-SECRETA', 'app_secret': 'SEGREDO'}}
    avisos.enviar_relatorio(_resultado([_plano()]), payload)

    assert 'CHAVE-SECRETA' not in enviados[0]
    assert 'SEGREDO' not in enviados[0]


def test_telefone_do_relatorio_e_trocavel(monkeypatch):
    monkeypatch.setenv('BAIXABRADESCO_RELATORIO_TELEFONE', '5511999998888')
    assert avisos.resolver_telefone_do_relatorio() == '5511999998888'


def test_pode_ser_desligado(monkeypatch):
    monkeypatch.setenv('BAIXABRADESCO_RELATORIO', '0')
    r = avisos.enviar_relatorio(_resultado([_plano()]), {})
    assert r['skipped'] is True


# =====================================================================
# 4. Avisar nunca derruba a baixa
# =====================================================================

def test_telegram_fora_do_ar_nao_derruba_nada(monkeypatch):
    import app.apps.notificador as notificador

    def _explode(**kwargs):
        raise RuntimeError('telegram fora')

    monkeypatch.setattr(notificador, 'enviar_telegram', _explode)
    r = avisos.enviar_relatorio(_resultado([_plano()]), {})
    assert r['ok'] is False
    assert 'telegram fora' in str(r['envios'])


def test_o_fluxo_manda_o_relatorio_no_fim():
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core._processar_lote)
    assert 'enviar_relatorio(resultado, payload)' in fonte
    # e nunca antes do aviso nem fora do try: avisar não pode derrubar a baixa
    assert fonte.index('enviar_aviso(') < fonte.index('enviar_relatorio(')
