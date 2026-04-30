# -*- coding: utf-8 -*-
"""
chatbot/core.py
Orquestrador principal do chatbot.
Máquina de estados robusta com tratamento de todos os casos.

Estados:
  AGUARDANDO_CPF         → aguardando CPF do usuário
  AGUARDANDO_COMPETENCIA → autenticado, aguardando mês/ano
  MENU_PRINCIPAL         → autenticado, aguardando escolha

Palavras reservadas (qualquer estado):
  sair / cancelar / reiniciar → encerra sessão
"""

import re
import logging
import unicodedata
import threading

from . import session, auth, zapi_sender
from .intents import contracheque as intent_contracheque

logger = logging.getLogger(__name__)

PALAVRAS_RESET = {'sair', 'cancelar', 'reset', 'reiniciar', 'inicio'}
PALAVRAS_CONTRACHEQUE = {
    'contracheque', 'holerite', 'salario', 'pagamento', 'folha', 'recibo', '1'
}


def _norm(texto: str) -> str:
    texto = unicodedata.normalize('NFD', texto.strip().lower())
    return ''.join(c for c in texto if unicodedata.category(c) != 'Mn')


def processar_mensagem(telefone: str, texto: str):
    texto_orig = texto.strip()
    texto_norm = _norm(texto_orig)

    logger.info(f"[core] {telefone[:6]}*** estado={_estado_atual(telefone)} msg='{texto_norm[:30]}'")

    if session.esta_bloqueado(telefone):
        zapi_sender.enviar_texto(
            telefone,
            "🔒 Acesso bloqueado por excesso de tentativas. Tente em 1 hora."
        )
        return

    if texto_norm in PALAVRAS_RESET:
        session.destruir_session(telefone)
        zapi_sender.enviar_texto(telefone, "👋 Sessão encerrada. Até logo!")
        return

    sess = session.get_session(telefone)

    if not sess:
        _iniciar_fluxo(telefone)
        return

    estado = sess.get('estado', '')

    if estado == 'AGUARDANDO_CPF':
        _processar_cpf(telefone, texto_orig)
    elif estado == 'AGUARDANDO_COMPETENCIA':
        _processar_competencia(telefone, texto_orig, sess)
    elif estado == 'MENU_PRINCIPAL':
        _processar_menu(telefone, texto_norm, sess)
    else:
        session.destruir_session(telefone)
        _iniciar_fluxo(telefone)


def _estado_atual(telefone: str) -> str:
    sess = session.get_session(telefone)
    return sess.get('estado', 'SEM_SESSAO') if sess else 'SEM_SESSAO'


def _iniciar_fluxo(telefone: str):
    session.criar_session(telefone, 'AGUARDANDO_CPF')
    zapi_sender.enviar_texto(
        telefone,
        "👋 Olá! Bem-vindo ao *Assistente Virtual BWS*.\n\n"
        "Para acessar suas informações, informe seu *CPF* (somente números):"
    )


def _processar_cpf(telefone: str, texto: str):
    cpf = re.sub(r'\D', '', texto)

    if len(cpf) != 11:
        zapi_sender.enviar_texto(
            telefone,
            "❌ CPF inválido. Informe os *11 dígitos* sem pontos ou traços.\n"
            "Exemplo: `12345678901`"
        )
        return

    resultado = auth.validar_acesso(cpf, telefone)

    if not resultado['ok']:
        motivo = resultado['motivo']

        if motivo == 'desligado':
            session.destruir_session(telefone)
            zapi_sender.enviar_texto(
                telefone,
                "ℹ️ Você não faz mais parte do quadro de colaboradores.\n\n"
                "Entre em contato com o *RH* para mais informações."
            )
            return

        tentativas = session.registrar_tentativa_cpf_errado(telefone)
        restantes = max(0, session.MAX_TENTATIVAS_CPF - tentativas)

        if restantes == 0:
            session.destruir_session(telefone)
            zapi_sender.enviar_texto(
                telefone,
                "🔒 Tentativas esgotadas. Acesso bloqueado por 1 hora."
            )
        else:
            zapi_sender.enviar_texto(
                telefone,
                f"❌ CPF não encontrado ou não corresponde a este número.\n"
                f"({restantes} tentativa(s) restante(s))"
            )
        return

    session.resetar_tentativas(telefone)
    colaborador = resultado['colaborador']
    nivel = resultado['nivel']
    nome = colaborador.get('nome', 'Colaborador').title().split()[0]

    session.atualizar_session(
        telefone,
        estado='MENU_PRINCIPAL',
        dados_extra={'colaborador': colaborador, 'nivel': nivel}
    )

    zapi_sender.enviar_texto(
        telefone,
        f"✅ Identidade verificada! Olá, *{nome}*!\n\n"
        f"O que você precisa?\n\n"
        f"*1* — 📄 Contracheque\n\n"
        f"_Para encerrar, envie_ `sair`"
    )


def _processar_menu(telefone: str, texto_norm: str, sess: dict):
    colaborador = sess.get('dados', {}).get('colaborador')
    if not colaborador:
        session.destruir_session(telefone)
        _iniciar_fluxo(telefone)
        return

    if any(p in texto_norm for p in PALAVRAS_CONTRACHEQUE):
        session.atualizar_session(telefone, estado='AGUARDANDO_COMPETENCIA')
        intent_contracheque.solicitar_competencia(telefone)
        return

    nome = colaborador.get('nome', '').title().split()[0] or 'Colaborador'
    zapi_sender.enviar_texto(
        telefone,
        f"Não entendi, {nome}. 😅\n\n"
        f"*1* — 📄 Contracheque\n\n"
        f"_Para encerrar, envie_ `sair`"
    )


def _processar_competencia(telefone: str, texto: str, sess: dict):
    colaborador = sess.get('dados', {}).get('colaborador')
    if not colaborador:
        session.destruir_session(telefone)
        _iniciar_fluxo(telefone)
        return

    from .paystub import parsear_competencia
    resultado = parsear_competencia(texto)

    if not resultado:
        zapi_sender.enviar_texto(
            telefone,
            "❌ Período não reconhecido.\n\n"
            "Informe no formato *MM/AAAA*. Exemplo: `04/2025`"
        )
        return

    ano, mes = resultado
    session.atualizar_session(telefone, estado='MENU_PRINCIPAL')

    zapi_sender.enviar_texto(
        telefone,
        f"⏳ Buscando contracheque de *{mes:02d}/{ano}*...\nAguarde. 🔍"
    )

    threading.Thread(
        target=intent_contracheque.processar_em_background,
        args=(telefone, colaborador, ano, mes),
        daemon=True,
    ).start()
