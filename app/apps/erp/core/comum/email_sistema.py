# -*- coding: utf-8 -*-
"""
A CONTA DE E-MAIL DO PRÓPRIO SISTEMA.

Pedido dele em 09/11/2026, desenhando o primeiro acesso:

> *"Aí, qual vai ser o e-mail que vai disparar essa informação? A gente
> precisaria cadastrar também o e-mail do sistema. Precisaria ter um e-mail
> específico para isso, configurado e habilitado para o sistema, para enviar
> informações via sistema. Que a gente vai ter e-mail do DP, e-mail do
> compras, e-mail do sistema."*

**POR QUE UMA CONTA SEPARADA, e não a da empresa que já existe.** As contas de
e-mail que o ERP tinha até aqui são **das empresas** (cadastro de Empresas), e
elas falam em nome de um CNPJ: a cotação sai de `compras@`, o pedido à
contabilidade sai da empresa que registra a pessoa. O convite de primeiro
acesso não é nenhuma das duas coisas — **quem fala é o sistema**, e sair por
`compras@` faria a pessoa achar que compras mandou, e responder para lá.

E tem um motivo prático que pesa mais: se esta conta ficasse pendurada numa
empresa, trocar a conta daquela empresa (ou arquivá-la) derrubaria o convite de
acesso de toda a casa, sem ninguém relacionar uma coisa com a outra.

**COMO ELA É GUARDADA.** Em `parametros`, como a caixa de entrada do agente
(`email_entrada.py`) — mesma mecânica, mesmo lugar, e **a senha cifrada**
(`core/comum/segredos.py`). Nada de variável de ambiente nova: o `CLAUDE.md`
proíbe, e com razão — credencial em envvar pede deploy para trocar, e quem
troca senha de e-mail é o dono numa tarde, não um deploy.

**COMO ELA MANDA.** Reusando `core/comum/email.py` inteiro, sem uma linha
duplicada: aquele módulo pede um objeto com os campos de SMTP, e `conta()`
devolve exatamente isso. Assim o registro do envio, o tratamento de erro em
português, o `Message-ID` próprio e o histórico continuam sendo um só —
inclusive para o e-mail do sistema.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.comum.segredos import cifrar
from app.apps.erp.db.models.cadastros import Parametro, Usuario

logger = logging.getLogger(__name__)

CHAVES = {
    "servidor": "email_sistema_servidor",
    "porta": "email_sistema_porta",
    "seguranca": "email_sistema_seguranca",
    "usuario": "email_sistema_usuario",
    "senha": "email_sistema_senha",            # cifrada
    "remetente": "email_sistema_remetente",    # como aparece para quem recebe
    "responder_para": "email_sistema_responder_para",
}
SEGURANCAS = ("STARTTLS", "SSL", "NENHUMA")


@dataclass
class ContaDoSistema:
    """Uma conta de envio que NÃO é de uma empresa.

    Os nomes dos campos são de propósito os mesmos do cadastro de Empresas: é
    o que permite `core/comum/email.py` mandar por aqui sem saber que esta
    conta é diferente. `id = None` é o que marca, no registro do envio, que o
    e-mail não saiu em nome de CNPJ nenhum.
    """
    id: Optional[int] = None
    razao_social: str = "ERP BWS"
    nome_fantasia: str = ""
    email: Optional[str] = None
    smtp_servidor: Optional[str] = None
    smtp_porta: Optional[int] = None
    smtp_seguranca: str = "STARTTLS"
    smtp_usuario: Optional[str] = None
    smtp_senha_cifrada: Optional[str] = None
    smtp_remetente: Optional[str] = None
    smtp_responder_para: Optional[str] = None
    conta_email_de_id: Optional[int] = None


def _ler(s: Session, chave: str, padrao: str = "") -> str:
    linha = s.get(Parametro, CHAVES[chave])
    return (linha.valor if linha is not None else padrao) or padrao


def _gravar(s: Session, chave: str, valor: str,
            usuario: Optional[Usuario] = None) -> None:
    nome = CHAVES[chave]
    linha = s.get(Parametro, nome)
    if linha is None:
        s.add(Parametro(chave=nome, valor=valor))
        return
    linha.valor = valor
    if hasattr(linha, "alterado_por"):
        linha.alterado_por = usuario.id if usuario else None


def configuracao(s: Session) -> dict[str, Any]:
    """O que está configurado. **A SENHA NUNCA SAI** — só se ela existe."""
    senha = _ler(s, "senha")
    servidor = _ler(s, "servidor")
    usuario = _ler(s, "usuario")
    return {
        "servidor": servidor,
        "porta": int(_ler(s, "porta", "587") or 587),
        "seguranca": _ler(s, "seguranca", "STARTTLS") or "STARTTLS",
        "usuario": usuario,
        "remetente": _ler(s, "remetente"),
        "responder_para": _ler(s, "responder_para"),
        "tem_senha": bool(senha),
        # "usável" é diferente de "preenchido": sem servidor, usuário e senha,
        # o convite não sai — e a tela precisa dizer isso ANTES de alguém
        # cadastrar dez pessoas achando que elas receberam o link.
        "usavel": bool(servidor and usuario and senha),
    }


def configurar(s: Session, dados: dict[str, Any],
               usuario: Optional[Usuario] = None) -> dict[str, Any]:
    for campo in ("servidor", "usuario", "remetente", "responder_para"):
        if campo in dados:
            _gravar(s, campo, str(dados[campo] or "").strip(), usuario)
    if "porta" in dados:
        try:
            _gravar(s, "porta", str(int(dados["porta"] or 587)), usuario)
        except (TypeError, ValueError):
            raise ErroValidacao("A porta tem de ser um número (587 ou 465).")
    if "seguranca" in dados:
        v = str(dados["seguranca"] or "STARTTLS").strip().upper()
        if v not in SEGURANCAS:
            raise ErroValidacao(
                f"Segurança desconhecida: {v}. Use STARTTLS, SSL ou NENHUMA.")
        _gravar(s, "seguranca", v, usuario)
    if dados.get("senha"):
        # CIFRADA, NUNCA EM CLARO — e a falta da chave do cofre não faz perder
        # o resto do que foi digitado.
        _gravar(s, "senha", cifrar(str(dados["senha"])), usuario)
    s.flush()
    return configuracao(s)


def conta(s: Session) -> Optional[ContaDoSistema]:
    """A conta pronta para mandar, ou `None` quando não dá.

    `None` e não exceção: quem chama (o convite) precisa poder dizer "cadastrei
    a pessoa, mas o e-mail não saiu porque a conta do sistema não está
    configurada" — e não perder o cadastro por causa disso.
    """
    c = configuracao(s)
    if not c["usavel"]:
        return None
    return ContaDoSistema(
        razao_social="ERP BWS",
        email=c["usuario"],
        smtp_servidor=c["servidor"], smtp_porta=c["porta"],
        smtp_seguranca=c["seguranca"], smtp_usuario=c["usuario"],
        smtp_senha_cifrada=_ler(s, "senha"),
        smtp_remetente=c["remetente"] or None,
        smtp_responder_para=c["responder_para"] or None)


def o_que_falta(s: Session) -> str:
    """A frase que a tela mostra quando não dá para mandar. Vazia = dá."""
    c = configuracao(s)
    if c["usavel"]:
        return ""
    faltando = [nome for nome, tem in (
        ("o servidor de saída", c["servidor"]), ("o usuário", c["usuario"]),
        ("a senha", c["tem_senha"])) if not tem]
    return ("A conta de e-mail do sistema está incompleta — falta "
            + ", ".join(faltando)
            + ". Configure em Configurações › E-mail do sistema.")


def testar(s: Session, para: str, usuario: Optional[Usuario] = None):
    """Manda uma mensagem de teste. É o que transforma "acho que configurei"
    em "está configurado"."""
    from app.apps.erp.core.comum import email as svc_email

    c = conta(s)
    if c is None:
        raise ErroValidacao(o_que_falta(s))
    return svc_email.enviar(
        s, empresa=c, para=[para],
        assunto="Teste do e-mail do sistema — ERP BWS",
        corpo=("Esta é uma mensagem de teste do ERP BWS.\n\n"
               "Se você está lendo isto, a conta de e-mail DO SISTEMA está "
               "configurada. É por ela que saem o convite de primeiro acesso "
               "e os avisos do próprio sistema — não os pedidos à "
               "contabilidade nem as cotações, que saem pela conta da "
               "empresa.\n"),
        entidade_tipo="sistema", entidade_id=0, usuario=usuario)
