# -*- coding: utf-8 -*-
"""Aviso por WhatsApp do que NÃO foi baixado.

Até 11/09/2026 um comprovante que não casava não gerava nada: nem mensagem, nem
linha em planilha. A explicação existia, mas só dentro da resposta devolvida ao
Make — quem não fosse procurar, não ficava sabendo. O dono pediu o contrário:
o que baixou normal não precisa avisar; o que **não** baixou, sim, porque é o
que merece atenção ou mudança de regra.

Um aviso por lote, nunca um por comprovante: comprovante chega em leva, e aviso
demais faz a pessoa parar de ler — aí o que importava se perde no meio.

Fora do aviso, de propósito:
- o que baixou (é o esperado);
- o que foi barrado por já ter sido baixado (a trava fez o trabalho dela).
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

import re

from .utils import as_string, only_digits

LIMITE_ITENS = 10   # comprovantes listados por aviso
LIMITE_SPS = 12     # números de SP listados por comprovante

# Destinos do aviso: o WhatsApp do financeiro (quem resolve) e o do dono (quem
# decide se a regra muda). Os dois recebem a mesma mensagem — decisão dele em
# 11/09/2026. Trocável pela variável BAIXABRADESCO_AVISO_TELEFONE, que aceita
# vários números separados por vírgula ou ponto e vírgula.
TELEFONE_FINANCEIRO = '5585996992197'
TELEFONE_DONO = '5585987846225'
TELEFONES_AVISO = (TELEFONE_FINANCEIRO, TELEFONE_DONO)


def _motivo_do_plano(plano: Dict[str, Any]) -> str:
    motivos = plano.get('motivos_bloqueio') or []
    if motivos:
        return as_string(motivos[0])
    return as_string((plano.get('match') or {}).get('motivo')) or 'Motivo não informado.'


def _falhou_no_omie(plano: Dict[str, Any]) -> bool:
    respostas = plano.get('responses') or {}
    if respostas.get('fila_omie'):
        return True
    passos = respostas.get('omie') or []
    ruins = {'erro_baixa', 'abort', 'abort_apos_alterar', 'abort_apos_transferencia'}
    return any(p.get('step') in ruins for p in passos if isinstance(p, dict))


def _sps_candidatas(plano: Dict[str, Any]) -> str:
    """Os números das SPs que o robô considerou.

    Sem eles a mensagem diz que havia doze candidatas e não diz quais — e quem
    lê não tem por onde começar. Com os números, é abrir a planilha e olhar.
    """
    match = plano.get('match') or {}
    ids = [as_string((c or {}).get('id')) for c in (match.get('candidatos') or [])]
    ids = [i for i in ids if i]
    if not ids:
        return ''
    mostradas = ids[:LIMITE_SPS]
    texto = ', '.join(mostradas)
    if len(ids) > LIMITE_SPS:
        texto += f' (+{len(ids) - LIMITE_SPS})'
    return f'SPs possíveis: {texto}'


def _descrever(plano: Dict[str, Any], motivo: str) -> str:
    rec = plano.get('receipt') or {}
    match = plano.get('match') or {}

    partes = [f"pág. {rec.get('page') or '?'}"]
    if rec.get('valor_pago'):
        partes.append(f"R$ {rec['valor_pago']}")
    if rec.get('nome_recebedor'):
        partes.append(as_string(rec['nome_recebedor'])[:40])

    # O número da SP, quando já se sabe qual é — é por ele que se procura na
    # planilha e no Omie.
    sp_id = as_string(match.get('id')) or as_string(rec.get('id_pipefy'))
    if sp_id:
        partes.append(f'SP {sp_id}')

    linhas = [f"- {' | '.join(partes)}"]
    candidatas = _sps_candidatas(plano) if not sp_id else ''
    if candidatas:
        linhas.append(f'  {candidatas}')
    linhas.append(f'  {motivo}')
    return '\n'.join(linhas)


def coletar_falhas(resultado: Dict[str, Any]) -> List[str]:
    """Linhas do aviso. Lista vazia quando não há nada a avisar."""
    linhas: List[str] = []

    for recusado in resultado.get('recusados') or []:
        linhas.append(
            f"- pág. {recusado.get('pagina') or '?'} | recusado pelo banco\n"
            f"  {recusado.get('motivo') or 'O banco não efetivou a operação.'}"
        )

    for plano in resultado.get('planos') or []:
        if not plano.get('pode_executar'):
            linhas.append(_descrever(plano, _motivo_do_plano(plano)))
        elif _falhou_no_omie(plano):
            linhas.append(_descrever(plano, 'Falha ao baixar no Omie. Está na fila para nova tentativa.'))

    return linhas


def montar_aviso(resultado: Dict[str, Any]) -> str:
    """Texto do aviso, ou string vazia quando deu tudo certo."""
    linhas = coletar_falhas(resultado)
    if not linhas:
        return ''

    resumo = resultado.get('resumo') or {}
    baixados = resumo.get('executaveis') or 0
    arquivos = {
        as_string((p.get('receipt') or {}).get('filename'))
        for p in (resultado.get('planos') or [])
    }
    arquivos |= {as_string(r.get('arquivo')) for r in (resultado.get('recusados') or [])}
    arquivos.discard('')

    cabecalho = f"BaixaBradesco: {len(linhas)} comprovante(s) NÃO foram baixados"
    if arquivos:
        cabecalho += '\nArquivo: ' + ', '.join(sorted(arquivos)[:3])

    mostradas = linhas[:LIMITE_ITENS]
    corpo = '\n\n'.join(mostradas)
    if len(linhas) > LIMITE_ITENS:
        corpo += f"\n\n(e mais {len(linhas) - LIMITE_ITENS} — veja o retorno completo)"

    rodape = f"\n\nBaixados normalmente: {baixados}"
    return f"{cabecalho}\n\n{corpo}{rodape}"


def resolver_telefones() -> List[str]:
    """Para quem vai o aviso.

    Por padrão dois números: o do **financeiro**, que é quem resolve, e o do
    **dono**, que é quem decide se a regra muda. Os dois recebem a mesma
    mensagem — ele pediu assim em 11/09/2026.

    `BAIXABRADESCO_AVISO_TELEFONE` substitui a lista inteira e aceita vários
    números separados por vírgula ou ponto e vírgula.
    """
    configurado = as_string(os.getenv('BAIXABRADESCO_AVISO_TELEFONE', ''))
    if configurado:
        brutos = re.split(r'[;,]', configurado)
    else:
        brutos = list(TELEFONES_AVISO)

    telefones: List[str] = []
    for bruto in brutos:
        numero = only_digits(bruto)
        if numero and numero not in telefones:   # nunca mandar duas vezes ao mesmo
            telefones.append(numero)
    return telefones


def enviar_aviso(resultado: Dict[str, Any], payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Manda o aviso pelo WhatsApp, com o Telegram de espelho.

    Usa o mesmo envio que o módulo já faz para avisar o responsável pela SP —
    aquele funciona em produção e aceita as credenciais Z-API vindas no próprio
    pedido do Make, que é como elas chegam hoje. Se as credenciais não vierem,
    cai no notificador comum, que lê as credenciais do ambiente.

    Nunca levanta erro: avisar não pode derrubar a baixa, que já aconteceu.
    """
    telefones = resolver_telefones()
    if not telefones:
        return {'ok': None, 'skipped': True, 'motivo': 'nenhum telefone de aviso configurado'}

    texto = montar_aviso(resultado)
    if not texto:
        return {'ok': None, 'skipped': True, 'motivo': 'nada a avisar'}

    envios = {t: _enviar_para(t, texto, payload) for t in telefones}

    # ⚠️ Aqui estava um "ok" que mentia, e é o mesmo defeito da gravação da
    # SPsBD com outra roupa: o envio devolve sucesso quando QUALQUER canal
    # entrega, e o Telegram do dono entrega quase sempre. Então o WhatsApp podia
    # falhar para o financeiro — que não tem Telegram — e tudo reportava
    # sucesso. Foi exatamente o que aconteceu em 13/09/2026: o dono recebeu pelo
    # Telegram e o financeiro, pelo visto, não recebeu nada.
    so_telegram = [t for t, r in envios.items() if _so_pelo_telegram(r)]
    sem_entrega = [t for t, r in envios.items() if not r.get('ok')]

    saida: Dict[str, Any] = {
        'ok': any(bool(r.get('ok')) for r in envios.values()),
        'envios': envios,
        'entregues_no_whatsapp': [t for t, r in envios.items() if _whatsapp_entregou(r)],
        'so_pelo_telegram': so_telegram,
        'sem_entrega': sem_entrega,
    }
    if sem_entrega:
        saida['alerta'] = ('Nenhum canal entregou para: ' + ', '.join(sem_entrega))
    elif so_telegram:
        saida['alerta'] = ('WhatsApp não entregou para: ' + ', '.join(so_telegram)
                           + '. Quem não tem Telegram cadastrado não recebeu o aviso.')
    return saida


def _whatsapp_entregou(resultado: Dict[str, Any]) -> bool:
    """O braço do WhatsApp, especificamente, deu certo?"""
    wa = resultado.get('whatsapp')
    if isinstance(wa, dict):
        return bool(wa.get('ok'))
    return False


def _so_pelo_telegram(resultado: Dict[str, Any]) -> bool:
    """Entregou, mas só pelo espelho do Telegram — o WhatsApp não foi."""
    if not resultado.get('ok'):
        return False
    if _whatsapp_entregou(resultado):
        return False
    wa = resultado.get('whatsapp')
    if isinstance(wa, dict) and wa.get('ok') is None:
        # Canal desligado de propósito (NOTIFICAR_WHATSAPP=0) não é falha.
        return False
    return True


def _enviar_para(telefone: str, texto: str, payload: Dict[str, Any] | None) -> Dict[str, Any]:
    """Um destinatário. Falha de um não impede o outro, nem derruba a baixa."""
    primeiro: Dict[str, Any] | None = None
    try:
        from .zapi import resolve_zapi_auth, send_text, validate_zapi_auth
        auth = resolve_zapi_auth(payload or {})
        if not validate_zapi_auth(auth):
            primeiro = send_text(auth, telefone, texto)
            if _whatsapp_entregou(primeiro):
                return primeiro
            wa = primeiro.get('whatsapp')
            if isinstance(wa, dict) and wa.get('ok') is None:
                # WhatsApp desligado de propósito: não insiste.
                return primeiro
    except Exception as e:
        primeiro = {'ok': False, 'erro': str(e)[:200]}

    # Chega aqui em dois casos, e os dois precisavam de segunda tentativa:
    # credencial Z-API ausente (já era tratado) **ou** credencial presente e o
    # envio falhou — este último não tinha tratamento nenhum, e é o que deixa o
    # financeiro sem aviso quando a instância do Z-API está fora.
    try:
        from app.apps.notificador import notificar
        segunda = notificar(telefone=telefone, mensagem=texto,
                            canais=('whatsapp', 'telegram'), politica='fallback')
    except Exception as e:
        segunda = {'ok': False, 'erro': str(e)[:200]}

    if primeiro is None:
        # ⚠️ Sem credencial Z-API, o aviso sai pelo notificador — e ele devolve
        # um dicionário POR CANAL, sem `ok` no topo. Devolver isso cru fazia o
        # relatório dizer "nenhum canal entregou" mesmo quando o Telegram tinha
        # entregado, e ainda zerava o `ok` do aviso inteiro. Pior: este é
        # provavelmente o caminho da produção — as credenciais Z-API chegam
        # dentro do pedido do Make, e o serviço automático não tem pedido
        # nenhum. Então o relatório mentiria justamente onde mais se olha.
        wa = segunda.get('whatsapp') if isinstance(segunda, dict) else None
        return {
            'ok': _notificador_entregou(segunda),
            'whatsapp': wa if isinstance(wa, dict) else {'ok': False},
            'telegram': segunda.get('telegram') if isinstance(segunda, dict) else None,
            'notificador': segunda,
        }

    # Junta as duas tentativas num resultado só, sem esconder nenhuma.
    wa_segunda = segunda.get('whatsapp') if isinstance(segunda, dict) else None
    entregou_wa = (_whatsapp_entregou(primeiro)
                   or (isinstance(wa_segunda, dict) and bool(wa_segunda.get('ok'))))
    juntado: Dict[str, Any] = {
        'ok': bool(primeiro.get('ok')) or _notificador_entregou(segunda),
        'whatsapp': {'ok': entregou_wa,
                     'primeira_tentativa': primeiro.get('whatsapp'),
                     'segunda_tentativa': wa_segunda},
        'telegram': primeiro.get('telegram'),
        'notificador': segunda,
    }
    # A mensagem da primeira falha não se perde: é ela que diz o que aconteceu.
    if primeiro.get('erro'):
        juntado['erro'] = primeiro['erro']
    return juntado


def _notificador_entregou(resposta: Any) -> bool:
    """O notificador devolve um dicionário por canal; basta um ter entregue."""
    if not isinstance(resposta, dict):
        return False
    if 'ok' in resposta:
        return bool(resposta.get('ok'))
    return any(isinstance(v, dict) and v.get('ok') for v in resposta.values())
