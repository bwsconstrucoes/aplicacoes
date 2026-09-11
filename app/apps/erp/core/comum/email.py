# ============================================================================
# ERP — core/comum/email.py
# Mandar e-mail pela conta da EMPRESA, e registrar que mandou.
#
# O ERP não tinha envio de e-mail nenhum, e 109 dos 111 fornecedores da BWS só
# recebem cotação por e-mail. Este arquivo é a ponte.
#
# POR QUE SMTP DA PRÓPRIA EMPRESA, E NÃO UM SERVIÇO DE ENVIO
#
# Porque o fornecedor RESPONDE. Se a cotação sair de um endereço de serviço, a
# resposta cai num lugar onde o comprador não olha. Saindo de compras@ da
# empresa, a conversa continua onde ela já acontece hoje. O custo disso é que
# o SMTP conta menos: ele confirma que o servidor de saída ACEITOU a
# mensagem, não que ela foi entregue, e muito menos lida. Está dito assim na
# tela, com estas palavras, para ninguém confundir registro com garantia.
#
# O QUE ESTE ARQUIVO PROMETE
#
#   - nunca deixa passar em silêncio: ou registra ENVIADO, ou registra FALHOU
#     com o motivo em português;
#   - nunca derruba a operação por causa de e-mail. Cotação fechada com um
#     fornecedor fora do ar continua sendo uma cotação; o que muda é a
#     situação daquele destinatário, e o botão de reenviar;
#   - nunca guarda senha em claro (ver core/comum/segredos.py).
# ============================================================================
from __future__ import annotations

import json
import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.comum.segredos import decifrar
from app.apps.erp.db.models.cadastros import Empresa, EnvioEmail, Usuario

logger = logging.getLogger(__name__)

TEMPO_LIMITE = 30            # segundos; servidor mudo não pode travar a tela
SEGURANCAS = ("STARTTLS", "SSL", "NENHUMA")


class ErroDeEnvio(Exception):
    """Falha ao entregar a mensagem ao servidor de saída, já em português."""


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


def endereco_valido(endereco: str) -> bool:
    _nome, e = parseaddr(_texto(endereco))
    return "@" in e and "." in e.split("@")[-1] and " " not in e


def remetente_de(empresa: Empresa) -> str:
    """Como o destinatário vê quem mandou."""
    escrito = _texto(getattr(empresa, "smtp_remetente", ""))
    if escrito:
        return escrito
    conta = _texto(getattr(empresa, "smtp_usuario", "")) or \
        _texto(getattr(empresa, "email", ""))
    nome = _texto(getattr(empresa, "nome_fantasia", "")) or \
        _texto(getattr(empresa, "razao_social", ""))
    return formataddr((nome, conta)) if conta else nome


def conta_configurada(empresa: Optional[Empresa]) -> tuple[bool, str]:
    """(dá para enviar?, o que falta). A tela mostra o segundo valor."""
    if empresa is None:
        return False, "Nenhuma empresa foi escolhida para este envio."
    faltando = []
    if not _texto(getattr(empresa, "smtp_servidor", "")):
        faltando.append("o servidor de saída")
    if not getattr(empresa, "smtp_porta", None):
        faltando.append("a porta")
    if not _texto(getattr(empresa, "smtp_usuario", "")):
        faltando.append("o usuário")
    if not _texto(getattr(empresa, "smtp_senha_cifrada", "")):
        faltando.append("a senha")
    if faltando:
        nome = _texto(getattr(empresa, "razao_social", "a empresa"))
        return False, (f"A conta de e-mail de {nome} está incompleta: falta "
                       f"{', '.join(faltando)}. Preencha em Administração › "
                       f"Empresas.")
    return True, ""


# ---------------------------------------------------------------------------
# O envio em si
# ---------------------------------------------------------------------------
def _montar(empresa: Empresa, para: list[str], assunto: str, corpo: str,
            copia: list[str], responder_para: Optional[str],
            anexos: list[dict[str, Any]]) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = remetente_de(empresa)
    msg["To"] = ", ".join(para)
    if copia:
        msg["Cc"] = ", ".join(copia)
    resposta = _texto(responder_para) or \
        _texto(getattr(empresa, "smtp_responder_para", "")) or \
        _texto(getattr(empresa, "email", ""))
    if resposta:
        msg["Reply-To"] = resposta
    msg["Subject"] = assunto
    msg.set_content(corpo)
    for anexo in anexos or []:
        conteudo = anexo.get("conteudo")
        if not conteudo:
            continue
        tipo = (anexo.get("mime") or "application/octet-stream").split("/", 1)
        msg.add_attachment(conteudo, maintype=tipo[0],
                           subtype=tipo[1] if len(tipo) > 1 else "octet-stream",
                           filename=anexo.get("nome") or "anexo")
    return msg


def _entregar(empresa: Empresa, msg: EmailMessage, destinos: list[str]) -> None:
    """Entrega ao servidor de saída. Levanta ErroDeEnvio já em português."""
    senha = decifrar(getattr(empresa, "smtp_senha_cifrada", None))
    servidor = _texto(empresa.smtp_servidor)
    porta = int(empresa.smtp_porta or 0)
    seguranca = (empresa.smtp_seguranca or "STARTTLS").upper()

    try:
        if seguranca == "SSL":
            conexao = smtplib.SMTP_SSL(servidor, porta, timeout=TEMPO_LIMITE,
                                       context=ssl.create_default_context())
        else:
            conexao = smtplib.SMTP(servidor, porta, timeout=TEMPO_LIMITE)
        with conexao as c:
            if seguranca == "STARTTLS":
                c.starttls(context=ssl.create_default_context())
            c.login(empresa.smtp_usuario, senha)
            c.send_message(msg, to_addrs=destinos)
    except smtplib.SMTPAuthenticationError:
        raise ErroDeEnvio(
            "O servidor recusou o usuário ou a senha. No Gmail e no Google "
            "Workspace com verificação em duas etapas, a senha aqui tem de "
            "ser uma SENHA DE APLICATIVO, não a senha de entrar no e-mail.")
    except smtplib.SMTPRecipientsRefused as e:
        recusados = ", ".join(sorted((e.recipients or {}).keys()))
        raise ErroDeEnvio(f"O servidor recusou o(s) endereço(s): {recusados}.")
    except smtplib.SMTPSenderRefused:
        raise ErroDeEnvio(
            f"O servidor recusou o remetente {remetente_de(empresa)!r}. "
            f"Normalmente o remetente tem de ser a mesma conta que entrou.")
    except (smtplib.SMTPConnectError, OSError) as e:
        raise ErroDeEnvio(
            f"Não foi possível falar com {servidor}:{porta} ({e}). Confira o "
            f"servidor, a porta e o tipo de segurança no cadastro da empresa.")
    except smtplib.SMTPException as e:
        raise ErroDeEnvio(f"O servidor de e-mail recusou o envio: {e}")


def enviar(s: Session, *, empresa: Empresa, para: list[str], assunto: str,
           corpo: str, entidade_tipo: str, entidade_id: int,
           usuario: Optional[Usuario] = None, copia: Optional[list[str]] = None,
           responder_para: Optional[str] = None,
           anexos: Optional[list[dict[str, Any]]] = None,
           destinatario_tipo: Optional[str] = None,
           destinatario_id: Optional[int] = None) -> EnvioEmail:
    """Manda, e registra o que aconteceu — deu certo ou não.

    Nunca levanta por falha de envio: devolve o registro com situação FALHOU e
    o motivo. Quem chama decide o que fazer, e a tela mostra o botão de
    reenviar. Levanta apenas quando o pedido em si não faz sentido (sem
    destinatário, sem assunto, conta não configurada).
    """
    para = [e for e in ( _texto(x) for x in (para or []) ) if e]
    copia = [e for e in ( _texto(x) for x in (copia or []) ) if e]
    anexos = anexos or []

    if not para:
        raise ErroValidacao("Sem endereço de destino, não há o que enviar.")
    invalidos = [e for e in para + copia if not endereco_valido(e)]
    if invalidos:
        raise ErroValidacao(
            f"Endereço de e-mail com formato inválido: {', '.join(invalidos)}.")
    if len(_texto(assunto)) < 3:
        raise ErroValidacao("Escreva um assunto para a mensagem.")

    pode, falta = conta_configurada(empresa)
    if not pode:
        raise ErroValidacao(falta)

    registro = EnvioEmail(
        empresa_id=empresa.id, entidade_tipo=entidade_tipo,
        entidade_id=entidade_id, destinatario_tipo=destinatario_tipo,
        destinatario_id=destinatario_id, para=para, copia=copia,
        responder_para=_texto(responder_para) or None,
        assunto=_texto(assunto), corpo=corpo,
        anexos=[{"nome": a.get("nome"), "mime": a.get("mime"),
                 "bytes": len(a.get("conteudo") or b"")} for a in anexos],
        enviado_por=usuario.id if usuario else None, situacao="ENVIADO")

    try:
        msg = _montar(empresa, para, _texto(assunto), corpo, copia,
                      responder_para, anexos)
        _entregar(empresa, msg, para + copia)
        logger.info("ERP/e-mail: %s %s enviado para %s pela empresa %s",
                    entidade_tipo, entidade_id, para, empresa.id)
    except ErroDeEnvio as e:
        registro.situacao = "FALHOU"
        registro.erro = str(e)
        logger.warning("ERP/e-mail: %s %s NÃO saiu para %s — %s",
                       entidade_tipo, entidade_id, para, e)
    except Exception as e:                                  # pragma: no cover
        registro.situacao = "FALHOU"
        registro.erro = f"Falha inesperada no envio: {e}"
        logger.exception("ERP/e-mail: falha inesperada")

    s.add(registro)
    s.flush()
    return registro


def testar_conta(s: Session, empresa: Empresa, para: str,
                 usuario: Optional[Usuario] = None) -> EnvioEmail:
    """Manda uma mensagem de teste. É o que transforma 'acho que configurei'
    em 'está configurado' — e o que carimba `smtp_conferido_em`."""
    registro = enviar(
        s, empresa=empresa, para=[para],
        assunto=f"Teste de envio — {empresa.razao_social}",
        corpo=("Esta é uma mensagem de teste do ERP BWS.\n\n"
               "Se você está lendo isto, a conta de e-mail desta empresa está "
               "configurada e as cotações vão sair por ela.\n"),
        entidade_tipo="empresa", entidade_id=empresa.id, usuario=usuario)
    if registro.situacao == "ENVIADO":
        from datetime import datetime, timezone
        empresa.smtp_conferido_em = datetime.now(timezone.utc)
    return registro


# ---------------------------------------------------------------------------
# Consulta: o que já saiu
# ---------------------------------------------------------------------------
def historico(s: Session, entidade_tipo: str,
              entidade_id: int) -> list[dict[str, Any]]:
    """Os envios de uma entidade, do mais novo para o mais velho."""
    from sqlalchemy import select
    achados = [e for e in s.scalars(select(EnvioEmail)).all()
               if e.entidade_tipo == entidade_tipo and e.entidade_id == entidade_id]
    achados.sort(key=lambda e: (e.id or 0), reverse=True)
    return [{
        "id": e.id, "para": list(e.para or []), "copia": list(e.copia or []),
        "assunto": e.assunto, "corpo": e.corpo,
        "situacao": e.situacao, "erro": e.erro,
        "destinatario_tipo": e.destinatario_tipo,
        "destinatario_id": e.destinatario_id,
        "anexos": list(e.anexos or []),
        "quando": e.criado_em.isoformat() if e.criado_em else None,
        "enviado_por": e.enviado_por,
    } for e in achados]
