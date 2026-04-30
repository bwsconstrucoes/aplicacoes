# -*- coding: utf-8 -*-
"""
chatbot/intents/contracheque.py
Intenção: fornecimento de contracheque ao colaborador.
"""

import logging
from .. import dropbox_client, paystub, zapi_sender

logger = logging.getLogger(__name__)


def solicitar_competencia(telefone: str):
    """Pergunta ao usuário qual competência deseja."""
    from datetime import datetime
    agora = datetime.now()
    mes_atual = f"{agora.month:02d}/{agora.year}"
    mes_ant   = f"{agora.month - 1:02d}/{agora.year}" if agora.month > 1 else f"12/{agora.year - 1}"

    zapi_sender.enviar_texto(
        telefone,
        "📄 *Solicitação de Contracheque*\n\n"
        "Para qual competência?\n\n"
        f"• `{mes_atual}` — mês atual\n"
        f"• `{mes_ant}` — mês anterior\n\n"
        "Digite no formato *MM/AAAA*:"
    )


def processar_em_background(telefone: str, colaborador: dict, ano: int, mes: int):
    """
    Executa em thread separada:
    baixa PDF, extrai contracheque e envia via Z-API.
    """
    cpf  = colaborador.get('cpf', '')
    nome = colaborador.get('nome', 'Colaborador')

    logger.info(f"[contracheque] Iniciando CPF={cpf[:3]}*** {mes:02d}/{ano}")

    pdf_bytes = dropbox_client.baixar_pdf(ano, mes)
    if not pdf_bytes:
        zapi_sender.enviar_texto(
            telefone,
            f"❌ Contracheque de *{mes:02d}/{ano}* não encontrado.\n\n"
            "Verifique o período ou entre em contato com o RH."
        )
        return

    contracheque = paystub.extrair_contracheque_por_cpf(pdf_bytes, cpf)
    if not contracheque:
        zapi_sender.enviar_texto(
            telefone,
            f"❌ Seu contracheque de *{mes:02d}/{ano}* não foi localizado no arquivo.\n\n"
            "Entre em contato com o RH."
        )
        return

    nome_arquivo = f"Contracheque_{mes:02d}_{ano}_{nome[:15].replace(' ', '_')}.pdf"
    caption      = f"📄 Contracheque {mes:02d}/{ano} — {nome.title()}"

    resultado = zapi_sender.enviar_documento_bytes(
        telefone, contracheque, nome_arquivo, caption
    )

    if resultado.get('ok'):
        logger.info(f"[contracheque] Enviado com sucesso para {telefone[:6]}***")
        zapi_sender.enviar_texto(
            telefone,
            "✅ Contracheque enviado!\n\n"
            "Para outra solicitação, envie *1* ou `contracheque`.\n"
            "Para encerrar, envie `sair`."
        )
    else:
        logger.error(f"[contracheque] Falha no envio: {resultado}")
        zapi_sender.enviar_texto(
            telefone,
            "❌ Erro ao enviar o arquivo. Tente novamente."
        )
