# -*- coding: utf-8 -*-
"""
chatbot/core.py
Orquestrador principal do chatbot.
Gerencia o fluxo de conversa baseado no estado da sessão.

Máquina de estados:
  (nenhuma sessão)      → bot envia saudação + pede CPF
  AGUARDANDO_CPF        → recebe CPF, valida, cria sessão autenticada
  AGUARDANDO_COMPETENCIA → recebe competência, processa contracheque
  MENU_PRINCIPAL        → (futuro) menu de opções

Palavras-chave especiais:
  'sair', 'cancelar', 'reset' → destroi sessão
  'contracheque', 'holerite'  → inicia fluxo de contracheque
"""

import re
import logging
from . import session, auth, zapi_sender
from .intents import contracheque as intent_contracheque

logger = logging.getLogger(__name__)

PALAVRAS_RESET = {'sair', 'cancelar', 'reset', 'reiniciar', 'inicio', 'início'}
PALAVRAS_CONTRACHEQUE = {'contracheque', 'holerite', 'salario', 'salário',
                          'pagamento', 'folha', 'recibo'}


def _normalizar_texto(texto: str) -> str:
    import unicodedata
    texto = unicodedata.normalize('NFD', texto.strip().lower())
    return ''.join(c for c in texto if unicodedata.category(c) != 'Mn')


def processar_mensagem(telefone: str, texto: str):
    """
    Ponto de entrada principal para cada mensagem recebida.
    Chamado pelo webhook da Z-API.
    """
    texto_norm = _normalizar_texto(texto)

    # Verifica bloqueio por excesso de tentativas
    if session.esta_bloqueado(telefone):
        zapi_sender.enviar_texto(
            telefone,
            "🔒 Acesso temporariamente bloqueado por excesso de tentativas.\n"
            "Tente novamente em 1 hora."
        )
        return

    # Comando de reset global
    if texto_norm in PALAVRAS_RESET:
        session.destruir_session(telefone)
        _enviar_saudacao(telefone)
        return

    sess = session.get_session(telefone)

    # Sem sessão → saudação + pede CPF
    if not sess:
        _enviar_saudacao(telefone)
        session.criar_session(telefone, 'AGUARDANDO_CPF')
        return

    estado = sess.get('estado')

    # ── AGUARDANDO CPF ──────────────────────────────────────────────────────
    if estado == 'AGUARDANDO_CPF':
        _processar_cpf(telefone, texto, sess)

    # ── AGUARDANDO COMPETÊNCIA ──────────────────────────────────────────────
    elif estado == 'AGUARDANDO_COMPETENCIA':
        colaborador = sess.get('dados', {}).get('colaborador')
        if not colaborador:
            session.destruir_session(telefone)
            _enviar_saudacao(telefone)
            return

        reconhecido = intent_contracheque.processar_competencia(
            telefone, texto, colaborador
        )
        if reconhecido:
            # Após enviar, volta para menu (aguarda próxima solicitação)
            session.atualizar_session(telefone, estado='MENU_PRINCIPAL')

    # ── MENU PRINCIPAL ──────────────────────────────────────────────────────
    elif estado == 'MENU_PRINCIPAL':
        _processar_menu(telefone, texto_norm, sess)

    else:
        # Estado desconhecido — reseta
        session.destruir_session(telefone)
        _enviar_saudacao(telefone)
        session.criar_session(telefone, 'AGUARDANDO_CPF')


def _enviar_saudacao(telefone: str):
    msg = (
        "👋 Olá! Bem-vindo ao *Assistente Virtual BWS*.\n\n"
        "Para acessar suas informações, preciso verificar sua identidade.\n\n"
        "Por favor, informe seu *CPF* (apenas números):"
    )
    zapi_sender.enviar_texto(telefone, msg)


def _processar_cpf(telefone: str, texto: str, sess: dict):
    """Valida o CPF informado e autentica o usuário."""
    cpf_digitado = re.sub(r'\D', '', texto)

    if not cpf_digitado or len(cpf_digitado) != 11:
        zapi_sender.enviar_texto(
            telefone,
            "❌ CPF inválido. Por favor, informe os *11 dígitos* do CPF.\n\n"
            "Exemplo: `12345678901`"
        )
        return

    resultado = auth.validar_acesso(cpf_digitado, telefone)

    if not resultado['ok']:
        motivo = resultado['motivo']

        if motivo == 'desligado':
            zapi_sender.enviar_texto(
                telefone,
                "ℹ️ Identificamos que você não faz mais parte do quadro de "
                "colaboradores da empresa.\n\n"
                "Para solicitar informações, entre em contato diretamente com "
                "o setor de *Recursos Humanos*."
            )
            session.destruir_session(telefone)
            return

        if motivo == 'cpf_telefone_nao_corresponde':
            tentativas = session.registrar_tentativa_cpf_errado(telefone)
            restantes  = max(0, auth.MAX_TENTATIVAS_CPF if hasattr(auth, 'MAX_TENTATIVAS_CPF') else 5 - tentativas)
            zapi_sender.enviar_texto(
                telefone,
                f"❌ O CPF informado não corresponde a este número de telefone.\n\n"
                f"Verifique e tente novamente. "
                f"({5 - tentativas} tentativa(s) restante(s))"
            )
            return

        # CPF não encontrado ou outro motivo
        tentativas = session.registrar_tentativa_cpf_errado(telefone)
        zapi_sender.enviar_texto(
            telefone,
            "❌ CPF não encontrado na base de dados.\n\n"
            "Verifique o número e tente novamente."
        )
        return

    # Autenticado com sucesso
    session.resetar_tentativas(telefone)
    colaborador = resultado['colaborador']
    nome = colaborador.get('nome', 'Colaborador').title()

    session.atualizar_session(
        telefone,
        estado='MENU_PRINCIPAL',
        dados_extra={'colaborador': colaborador, 'nivel': resultado['nivel']}
    )

    zapi_sender.enviar_texto(
        telefone,
        f"✅ Identidade verificada!\n\n"
        f"Olá, *{nome}*! O que você precisa?\n\n"
        f"📄 `contracheque` — Solicitar contracheque\n\n"
        f"_Para sair a qualquer momento, envie_ `sair`"
    )


def _processar_menu(telefone: str, texto_norm: str, sess: dict):
    """Processa a escolha do menu principal."""
    colaborador = sess.get('dados', {}).get('colaborador')
    if not colaborador:
        session.destruir_session(telefone)
        _enviar_saudacao(telefone)
        return

    # Contracheque / holerite
    if any(p in texto_norm for p in PALAVRAS_CONTRACHEQUE):
        session.atualizar_session(telefone, estado='AGUARDANDO_COMPETENCIA')
        intent_contracheque.solicitar_competencia(telefone)
        return

    # Opção não reconhecida
    nome = colaborador.get('nome', 'Colaborador').title().split()[0]
    zapi_sender.enviar_texto(
        telefone,
        f"Não entendi, {nome}. 😅\n\n"
        f"Escolha uma das opções disponíveis:\n\n"
        f"📄 `contracheque` — Solicitar contracheque\n\n"
        f"_Para sair, envie_ `sair`"
    )
