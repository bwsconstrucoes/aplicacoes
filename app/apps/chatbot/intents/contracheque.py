# -*- coding: utf-8 -*-
"""
chatbot/intents/contracheque.py
Intenção: fornecimento de contracheque ao colaborador.

Fluxo:
  1. Usuário autenticado solicita contracheque
  2. Bot pergunta a competência (mês/ano)
  3. Bot busca PDF no Dropbox e extrai contracheque do CPF
  4. Bot envia o PDF individual via Z-API
"""

import logging
import threading
from datetime import datetime

from .. import dropbox_client, paystub, zapi_sender

logger = logging.getLogger(__name__)


def solicitar_competencia(telefone: str):
    """Pergunta ao usuário qual competência deseja."""
    from datetime import datetime
    agora = datetime.now()
    mes_atual = f"{agora.month:02d}/{agora.year}"
    mes_anterior = f"{agora.month - 1:02d}/{agora.year}" if agora.month > 1 else f"12/{agora.year - 1}"

    msg = (
        "📄 *Solicitação de Contracheque*\n\n"
        "Para qual competência você deseja o contracheque?\n\n"
        f"Exemplos:\n"
        f"  • `{mes_atual}` (mês atual)\n"
        f"  • `{mes_anterior}` (mês anterior)\n"
        f"  • `01/2025`\n\n"
        "Digite no formato *MM/AAAA*:"
    )
    zapi_sender.enviar_texto(telefone, msg)


def processar_competencia(telefone: str, texto: str, colaborador: dict) -> bool:
    """
    Processa a competência informada pelo usuário.
    Retorna True se a competência foi reconhecida (mesmo que o PDF não exista).
    Dispara o processamento em background.
    """
    resultado = paystub.parsear_competencia(texto)
    if not resultado:
        zapi_sender.enviar_texto(
            telefone,
            "❌ Não entendi a competência informada.\n\n"
            "Por favor, informe no formato *MM/AAAA*.\n"
            "Exemplo: `04/2025`"
        )
        return False

    ano, mes = resultado

    # Responde imediatamente para não dar timeout no webhook
    zapi_sender.enviar_texto(
        telefone,
        f"⏳ Buscando seu contracheque de *{mes:02d}/{ano}*...\n"
        "Aguarde um momento."
    )

    # Processa em background
    threading.Thread(
        target=_processar_em_background,
        args=(telefone, colaborador, ano, mes),
        daemon=True,
    ).start()

    return True


def _processar_em_background(telefone: str, colaborador: dict, ano: int, mes: int):
    """Baixa PDF, extrai contracheque e envia via Z-API."""
    cpf  = colaborador['cpf']
    nome = colaborador.get('nome', 'Colaborador')

    logger.info(f"[contracheque] Iniciando busca para CPF {cpf[:3]}*** competência {mes:02d}/{ano}")

    # 1. Baixa PDF do Dropbox
    pdf_bytes = dropbox_client.baixar_pdf(ano, mes)
    if not pdf_bytes:
        zapi_sender.enviar_texto(
            telefone,
            f"❌ O contracheque de *{mes:02d}/{ano}* não foi encontrado.\n\n"
            "Isso pode ocorrer porque:\n"
            "• O arquivo ainda não foi disponibilizado\n"
            "• A competência informada está incorreta\n\n"
            "Tente novamente ou entre em contato com o RH."
        )
        return

    # 2. Extrai contracheque do colaborador
    contracheque_bytes = paystub.extrair_contracheque_por_cpf(pdf_bytes, cpf)
    if not contracheque_bytes:
        zapi_sender.enviar_texto(
            telefone,
            f"❌ Seu contracheque de *{mes:02d}/{ano}* não foi localizado no arquivo.\n\n"
            "Entre em contato com o RH para mais informações."
        )
        return

    # 3. Envia o PDF individual
    nome_arquivo = f"Contracheque_{mes:02d}_{ano}_{nome[:20].replace(' ', '_')}.pdf"
    caption      = f"📄 Contracheque de {mes:02d}/{ano}\n{nome.title()}"

    resultado = zapi_sender.enviar_documento_bytes(
        telefone, contracheque_bytes, nome_arquivo, caption
    )

    if resultado.get('ok'):
        logger.info(f"[contracheque] Enviado com sucesso para {telefone[:6]}***")
        zapi_sender.enviar_texto(
            telefone,
            "✅ Contracheque enviado com sucesso!\n\n"
            "Para solicitar outro, envie qualquer mensagem."
        )
    else:
        logger.error(f"[contracheque] Falha ao enviar para {telefone[:6]}***: {resultado}")
        zapi_sender.enviar_texto(
            telefone,
            "❌ Ocorreu um erro ao enviar o arquivo. Tente novamente."
        )
