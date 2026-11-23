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
from email.utils import formataddr, make_msgid, parseaddr
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
    """Como o destinatário vê quem mandou — SEMPRE com um endereço dentro.

    ⚑ CONSERTO DE 18/11/2026, e o defeito era meu. O dono configurou a conta
    do DP e a do sistema e as duas recusaram o envio com "o servidor recusou o
    endereço", nas contas que a empresa usa há anos sem restrição nenhuma.

    A CAUSA: este campo era devolvido CRU. A tela pede "como aparece para quem
    recebe", e quem lê isso escreve um NOME — "DP — BWS Construções". Esse
    texto virava o `From` da mensagem, e um `From` sem `@` não é endereço: o
    `smtplib` tira dele o remetente do envelope, manda um envelope vazio ou
    torto, e aí o servidor recusa o DESTINATÁRIO (é nessa hora que a recusa
    aparece, o que jogava a suspeita para o lado errado — o endereço de
    destino, que estava certo o tempo todo).

    A REGRA AGORA: se o que está escrito já tem endereço (`fulano@x` ou
    `Nome <fulano@x>`), vale como está. Se é só um nome, ele vira o NOME e o
    endereço da conta entra junto. Assim a tela pode continuar pedindo "como
    aparece", que é o que a pessoa entende, sem produzir mensagem inválida.
    """
    conta = _texto(getattr(empresa, "smtp_usuario", "")) or \
        _texto(getattr(empresa, "email", ""))
    escrito = _texto(getattr(empresa, "smtp_remetente", ""))
    if escrito:
        # ⚠️ A CONTA DO "@" É NO TEXTO INTEIRO, e não no que o `parseaddr`
        # devolve: para "ERP BWS" ele responde ("", "ERP"), engolindo metade
        # do nome. Sem arroba em lugar nenhum, o que está escrito é um NOME.
        if "@" in escrito:
            return escrito
        return formataddr((escrito, conta)) if conta else escrito
    nome = _texto(getattr(empresa, "nome_fantasia", "")) or \
        _texto(getattr(empresa, "razao_social", ""))
    return formataddr((nome, conta)) if conta else nome


def conta_de_envio(s: Optional[Session], empresa: Optional[Empresa]) -> Optional[Empresa]:
    """De QUAL empresa saem servidor, usuário e senha deste envio.

    Desde 14/09/2026 (migração 067) uma empresa pode **usar a conta de outra**:
    o e-mail sai pela conta principal, mas aparece com o remetente dela. Pedido
    do dono: *"o ideal seria que a gente continuasse utilizando um e-mail
    principal para encaminhar de outras empresas"*.

    **Um pulo só, de propósito.** Emprestar de quem também está emprestando
    seria uma corrente — e uma corrente com um elo mexido em outro dia é a
    receita do "parou de mandar e ninguém sabe por quê". O cadastro recusa
    apontar para quem empresta (ver `cadastros/empresas.py`), e aqui o pulo
    único é a segunda tranca.
    """
    if empresa is None:
        return None
    dona_id = getattr(empresa, "conta_email_de_id", None)
    if not dona_id or dona_id == empresa.id or s is None:
        return empresa
    dona = s.get(type(empresa), dona_id)
    return dona or empresa


def conta_configurada(empresa: Optional[Empresa],
                      s: Optional[Session] = None) -> tuple[bool, str]:
    """(dá para enviar?, o que falta). A tela mostra o segundo valor.

    Com a sessão, confere a conta DE ONDE o e-mail sai de verdade — que pode
    ser a de outra empresa. Sem ela, confere a própria: é o comportamento de
    antes, e o que vale enquanto a migração 067 não rodou.
    """
    if empresa is None:
        return False, "Nenhuma empresa foi escolhida para este envio."
    dona = conta_de_envio(s, empresa) or empresa
    if dona.id != empresa.id:
        pode, falta = conta_configurada(dona)
        if not pode:
            nome_dona = _texto(getattr(dona, "razao_social", "a outra empresa"))
            return False, (f"Esta empresa usa a conta de {nome_dona}, e é essa "
                           f"conta que está incompleta. {falta}")
        return True, ""
    empresa = dona
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
            anexos: list[dict[str, Any]],
            corpo_html: Optional[str] = None) -> EmailMessage:
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
    # O MESSAGE-ID É NOSSO, E ISSO IMPORTA: a resposta traz este valor em
    # In-Reply-To/References, e é por ele que a leitura da caixa de entrada casa
    # a resposta com o pedido que a originou — sem depender de ninguém manter o
    # assunto intacto ao responder. Sem gerar aqui, cada servidor de saída
    # inventa o seu e nós não ficamos sabendo qual foi.
    msg["Message-ID"] = make_msgid(domain=(parseaddr(msg["From"])[1]
                                           .split("@")[-1] or None))
    msg.set_content(corpo)
    # ⚑ VERSÃO FORMATADA, OPCIONAL (21/11/2026). O dono: "quando o texto fica
    # simples demais dá a impressão de que não é nem uma coisa de verdade".
    # O texto puro CONTINUA indo junto, como alternativa: é o que aparece em
    # leitor que não abre formatação, e o que pesa a favor nos filtros de spam.
    if corpo_html:
        msg.add_alternative(corpo_html, subtype="html")
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
        # ⚑ O MOTIVO DO SERVIDOR ENTRA NA FRASE (18/11/2026). Antes dizíamos só
        # "recusou o endereço X" — e o dono ficou parado com isso na tela, sem
        # ter como saber por quê. A informação ESTAVA aqui: `e.recipients` é um
        # dicionário {endereço: (código, motivo)}, e nós jogávamos o motivo
        # fora. Erro que esconde a causa custa uma ida e volta inteira.
        partes = []
        for endereco, resposta in sorted((e.recipients or {}).items()):
            try:
                codigo, texto_ = resposta
                motivo = (texto_.decode(errors="replace")
                          if isinstance(texto_, bytes) else str(texto_))
                partes.append(f"{endereco} — {codigo} {motivo.strip()}")
            except Exception:                           # noqa: BLE001
                partes.append(str(endereco))
        raise ErroDeEnvio(
            "O servidor recusou o destinatário: " + "; ".join(partes)
            + ". Isto quase nunca é o endereço estar errado: é o provedor "
            "não deixar esta conta mandar para fora (relay). Confira se o "
            "endereço que entrou é o mesmo que aparece como remetente, e se "
            "a conta tem permissão de enviar para endereços externos.")
    except smtplib.SMTPSenderRefused:
        raise ErroDeEnvio(
            f"O servidor recusou o remetente. Normalmente ele tem de ser a "
            f"mesma conta que entrou ({empresa.smtp_usuario}) ou um endereço "
            f"do mesmo domínio, liberado como alias no provedor. Se esta "
            f"empresa usa a conta de outra, é isto que está acontecendo: o "
            f"provedor não deixa mandar com endereço de fora.")
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
           destinatario_id: Optional[int] = None,
           corpo_registrado: Optional[str] = None,
           corpo_html: Optional[str] = None,
           guardar_nos_enviados: bool = False) -> EnvioEmail:
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

    pode, falta = conta_configurada(empresa, s)
    if not pode:
        raise ErroValidacao(falta)
    # De onde SAI (servidor, usuário, senha) pode ser outra empresa; COMO
    # APARECE é sempre desta. É a separação que o dono pediu.
    dona = conta_de_envio(s, empresa) or empresa

    registro = EnvioEmail(
        empresa_id=empresa.id, entidade_tipo=entidade_tipo,
        entidade_id=entidade_id, destinatario_tipo=destinatario_tipo,
        destinatario_id=destinatario_id, para=para, copia=copia,
        responder_para=_texto(responder_para) or None,
        # `corpo_registrado` (21/11/2026): quando o conteúdo vai num PDF anexo
        # e o corpo do e-mail é só o aviso, o REGISTRO guarda o conteúdo por
        # extenso. O registro existe para provar o que foi mandado — e um
        # "segue em anexo" não prova nada.
        assunto=_texto(assunto), corpo=corpo_registrado or corpo,
        anexos=[{"nome": a.get("nome"), "mime": a.get("mime"),
                 "bytes": len(a.get("conteudo") or b"")} for a in anexos],
        enviado_por=usuario.id if usuario else None, situacao="ENVIADO")

    try:
        msg = _montar(empresa, para, _texto(assunto), corpo, copia,
                      responder_para, anexos, corpo_html)
        # Guardado no registro para quem chamou poder casar a resposta depois.
        registro.message_id = msg["Message-ID"]
        _entregar(dona, msg, para + copia)
        logger.info("ERP/e-mail: %s %s enviado para %s pela empresa %s",
                    entidade_tipo, entidade_id, para, empresa.id)
        if guardar_nos_enviados:
            registro.copia_nos_enviados = guardar_copia_nos_enviados(s, dona, msg)
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


# ---------------------------------------------------------------------------
# A CÓPIA EM "ENVIADOS" (23/11/2026)
#
# Ele: *"os e-mails que eu envio via sistema, eles não ficam salvos no e-mail
# (…) estou procurando dentre os enviados (…) não estou localizando."*
#
# O SMTP só ENTREGA; quem guarda em "Enviados" é o programa de e-mail. Gmail e
# Outlook/Microsoft 365 guardam sozinhos o que sai pelo SMTP deles; os outros
# provedores não. Aqui o ERP faz o que o programa faria: depois de entregar,
# grava a mesma mensagem na pasta de enviados da conta (IMAP APPEND).
#
# Usa a caixa de entrada configurada (`email_entrada`) e SÓ se ela for a mesma
# conta que enviou — não se grava cópia na caixa de outra pessoa. Falhar aqui
# NÃO desfaz o envio: o e-mail já saiu; a resposta diz que a cópia não ficou.
# Custo: uma conexão IMAP a mais por envio (um ou dois segundos).
# ---------------------------------------------------------------------------
GUARDAM_SOZINHOS = ("gmail", "google", "office365", "outlook", "hotmail")
NOMES_DE_ENVIADOS = ("Sent", "INBOX.Sent", "Enviados", "INBOX.Enviados",
                     "Itens Enviados", "Sent Items", "Sent Messages",
                     "[Gmail]/Sent Mail", "INBOX/Sent")


def _pasta_de_enviados(caixa) -> Optional[str]:
    """A pasta marcada como \\Sent (RFC 6154); sem marca, um nome conhecido."""
    ok, linhas = caixa.list()
    if ok != "OK":
        return None
    nomes = []
    for bruta in linhas or []:
        linha = bruta.decode("utf-8", "replace") if isinstance(bruta, bytes) else str(bruta)
        nome = linha.rsplit(' "', 1)[-1].rstrip('"') if '"' in linha else linha.split()[-1]
        if "\\Sent" in linha:
            return nome
        nomes.append(nome)
    for candidato in NOMES_DE_ENVIADOS:
        for n in nomes:
            if n.lower() == candidato.lower():
                return n
    # ⚑ Sem marca e sem nome conhecido (23/11/2026, a caixa do DP é da
    # Locaweb): a pasta cujo nome diz "enviad" ou "sent" — "INBOX.Itens
    # Enviados", "INBOX.Sent Items". Nunca a lixeira nem os rascunhos.
    for n in nomes:
        baixo = n.lower()
        if ("enviad" in baixo or "sent" in baixo) and not any(
                x in baixo for x in ("trash", "lixeira", "draft", "rascunho")):
            return n
    return None


def guardar_copia_nos_enviados(s: Session, conta: Any, msg: EmailMessage) -> str:
    """Grava a mensagem enviada em "Enviados". Devolve o que aconteceu, em
    português. Nunca levanta."""
    import imaplib
    import time
    servidor = _texto(getattr(conta, "smtp_servidor", "")).lower()
    if any(x in servidor for x in GUARDAM_SOZINHOS):
        return "o próprio provedor guarda em Enviados"
    try:
        from app.apps.erp.core.comum import email_entrada
        c = email_entrada.configuracao(s)
        usuario = _texto(getattr(conta, "smtp_usuario", "")).lower()
        if not c.get("usavel") or _texto(c.get("usuario")).lower() != usuario:
            return ("sem cópia em Enviados: a caixa de e-mail configurada no ERP "
                    "não é a mesma conta que enviou")
        senha = decifrar(email_entrada._ler(s, "senha"))
        caixa = imaplib.IMAP4_SSL(c["servidor"], c["porta"], timeout=TEMPO_LIMITE)
        try:
            caixa.login(c["usuario"], senha or "")
            pasta = _pasta_de_enviados(caixa)
            if not pasta:
                return "sem cópia em Enviados: não achei a pasta de enviados"
            alvo = f'"{pasta}"' if " " in pasta else pasta
            ok, _ = caixa.append(alvo, "(\\Seen)", imaplib.Time2Internaldate(time.time()),
                                 msg.as_bytes())
            return "guardado em Enviados" if ok == "OK" else \
                f"sem cópia em Enviados: o servidor recusou ({ok})"
        finally:
            try:
                caixa.logout()
            except Exception:  # noqa: BLE001
                pass
    except Exception as e:  # noqa: BLE001
        logger.warning("ERP/e-mail: cópia em Enviados falhou — %s", e)
        return f"sem cópia em Enviados: {e}"


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
