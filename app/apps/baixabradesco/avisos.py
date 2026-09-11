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

from .utils import as_string

LIMITE_ITENS = 10   # acima disso o aviso vira parede de texto e ninguém lê


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


def _descrever(plano: Dict[str, Any], motivo: str) -> str:
    rec = plano.get('receipt') or {}
    partes = [f"pág. {rec.get('page') or '?'}"]
    if rec.get('valor_pago'):
        partes.append(f"R$ {rec['valor_pago']}")
    if rec.get('nome_recebedor'):
        partes.append(as_string(rec['nome_recebedor'])[:40])
    elif rec.get('id_pipefy'):
        partes.append(f"SP {rec['id_pipefy']}")
    return f"- {' | '.join(partes)}\n  {motivo}"


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


def resolver_telefone() -> str:
    """Para quem vai o aviso.

    Ordem: `BAIXABRADESCO_AVISO_TELEFONE` (se um dia o destino for outra
    pessoa), depois `CHATBOT_MASTER_PHONE`, que é a convenção já usada pelo
    chatbot e pelo processarnovasp para falar com o dono. Reusar evita ter o
    mesmo número escrito num terceiro lugar do repositório.
    """
    telefone = as_string(os.getenv('BAIXABRADESCO_AVISO_TELEFONE', ''))
    if telefone:
        return telefone
    telefone = as_string(os.getenv('CHATBOT_MASTER_PHONE', ''))
    if telefone:
        return telefone
    try:
        from app.apps.chatbot.auth import TELEFONE_MASTER
        return as_string(TELEFONE_MASTER)
    except Exception:
        return ''


def enviar_aviso(resultado: Dict[str, Any], payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Manda o aviso pelo WhatsApp, com o Telegram de espelho.

    Usa o mesmo envio que o módulo já faz para avisar o responsável pela SP —
    aquele funciona em produção e aceita as credenciais Z-API vindas no próprio
    pedido do Make, que é como elas chegam hoje. Se as credenciais não vierem,
    cai no notificador comum, que lê as credenciais do ambiente.

    Nunca levanta erro: avisar não pode derrubar a baixa, que já aconteceu.
    """
    telefone = resolver_telefone()
    if not telefone:
        return {'ok': None, 'skipped': True, 'motivo': 'nenhum telefone de aviso configurado'}

    texto = montar_aviso(resultado)
    if not texto:
        return {'ok': None, 'skipped': True, 'motivo': 'nada a avisar'}

    try:
        from .zapi import resolve_zapi_auth, send_text, validate_zapi_auth
        auth = resolve_zapi_auth(payload or {})
        if not validate_zapi_auth(auth):
            return send_text(auth, telefone, texto)
    except Exception as e:
        return {'ok': False, 'erro': str(e)[:200]}

    # Sem credenciais Z-API no pedido nem no ambiente: tenta o notificador,
    # que tem as suas próprias e ainda alcança o Telegram.
    try:
        from app.apps.notificador import notificar
        return notificar(telefone=telefone, mensagem=texto,
                         canais=('whatsapp', 'telegram'), politica='fallback')
    except Exception as e:
        return {'ok': False, 'erro': str(e)[:200]}
