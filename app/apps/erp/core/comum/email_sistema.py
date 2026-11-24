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

================================================================================
⚑ UMA CONTA POR ÁREA DA EMPRESA (18/11/2026) — e o arquivo deixou de ser só
   "o e-mail do sistema".

Ele, ao perguntar como isto estava sendo tratado:

> *"A empresa tem áreas diferentes. O que eu vou encaminhar a partir do DP é
> uma coisa, é um e-mail. O que eu vou encaminhar a partir do suprimento é
> outro e-mail. E a parte de sistema da empresa, notificações, avisos, é outro
> e-mail. E eu acho que a gente não está tendo essa diferenciação."*

**ELE ESTAVA CERTO, e não era hipótese.** Antes disto, os eixos eram dois —
a conta POR EMPRESA (CNPJ, no cadastro de Empresas) e a conta DO SISTEMA — e a
ÁREA não era eixo nenhum. O resultado em produção: a cotação para o fornecedor
e o pedido à contabilidade **saíam do mesmo endereço**, e a resposta dos dois
voltava para a mesma caixa.

**O DESENHO, com as palavras dele:**

| Área | Conta | Por quê |
|---|---|---|
| **Suprimento** | uma **por empresa** (a que já existe, no cadastro de Empresas) | *"suprimento é um e-mail por empresa"* — a cotação é documento comercial, e o fornecedor fatura contra um CNPJ |
| **DP** | **uma só**, geral | *"o DP, por enquanto, eu quero um e-mail geral"* — e a caixa de entrada que o agente monitora é a DELE: *"eu tenho que associar a caixa própria, configurar isso no próprio DP"* |
| **Sistema** | **uma só**, geral | *"sistema um e-mail geral"* — convite de acesso e aviso do próprio sistema |

⚑ **O SUPRIMENTO NÃO MUDOU UMA LINHA**, e isso é o que mostra que o desenho
   está certo: o que ele quer para o suprimento é exatamente o que já havia.
   O que faltava era a conta do DP — que estava emprestada da empresa.

⚑ **POR QUE AQUI, e não num módulo novo:** este arquivo já fazia, para uma
   área, tudo o que as outras precisam — guardar em `parametros`, cifrar a
   senha, montar um objeto que o `email.py` manda sem saber a diferença. Área
   nova é uma linha em `AREAS`, não um módulo. E as chaves de `SISTEMA`
   continuam sendo `email_sistema_*`: nada a migrar, o que já estava
   configurado continua valendo.
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

# As áreas que têm conta PRÓPRIA (uma só, geral). O suprimento não está aqui
# de propósito: a conta dele é a da empresa, por CNPJ, e já existia.
AREAS: dict[str, dict[str, str]] = {
    "SISTEMA": {
        "nome": "E-mail do sistema",
        "para_que": ("convite de primeiro acesso e avisos do próprio "
                     "sistema — fala em nome do ERP, não de um CNPJ"),
        "onde": "Configurações › E-mail do sistema",
        # ⚠️ PREFIXO ANTIGO, DE PROPÓSITO: é o que já está gravado em produção.
        # Trocar para `email_area_sistema_*` pediria migração e faria o ERP
        # esquecer uma conta configurada — por elegância nenhuma.
        "prefixo": "email_sistema",
    },
    "DP": {
        "nome": "E-mail do DP",
        "para_que": ("pedidos à contabilidade e o que o DP encaminha. É esta "
                     "conta que o agente monitora de volta"),
        "onde": "Pessoal › Contabilidade",
        "prefixo": "email_dp",
    },
}
PADRAO = "SISTEMA"
CAMPOS = ("servidor", "porta", "seguranca", "usuario", "senha", "remetente",
          "responder_para")
# Mantida para quem já importava: as chaves da área SISTEMA.
CHAVES = {c: f"email_sistema_{c}" for c in CAMPOS}
SEGURANCAS = ("STARTTLS", "SSL", "NENHUMA")


def _area(area: Optional[str]) -> dict[str, str]:
    a = AREAS.get((area or PADRAO).strip().upper())
    if a is None:
        raise ErroValidacao(
            f"Área de e-mail desconhecida: {area}. As que existem: "
            + ", ".join(AREAS))
    return a


def _chave(area: Optional[str], campo: str) -> str:
    return f"{_area(area)['prefixo']}_{campo}"


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


def _ler(s: Session, chave: str, padrao: str = "",
         area: Optional[str] = None) -> str:
    linha = s.get(Parametro, _chave(area, chave))
    return (linha.valor if linha is not None else padrao) or padrao


def _gravar(s: Session, chave: str, valor: str,
            usuario: Optional[Usuario] = None,
            area: Optional[str] = None) -> None:
    nome = _chave(area, chave)
    linha = s.get(Parametro, nome)
    if linha is None:
        s.add(Parametro(chave=nome, valor=valor))
        return
    linha.valor = valor
    if hasattr(linha, "alterado_por"):
        linha.alterado_por = usuario.id if usuario else None


def configuracao(s: Session, area: Optional[str] = None) -> dict[str, Any]:
    """O que está configurado. **A SENHA NUNCA SAI** — só se ela existe."""
    meta = _area(area)
    senha = _ler(s, "senha", area=area)
    servidor = _ler(s, "servidor", area=area)
    usuario = _ler(s, "usuario", area=area)
    return {
        "area": (area or PADRAO).upper(),
        "area_nome": meta["nome"], "para_que": meta["para_que"],
        "onde": meta["onde"],
        "servidor": servidor,
        "porta": int(_ler(s, "porta", "587", area=area) or 587),
        "seguranca": _ler(s, "seguranca", "STARTTLS", area=area) or "STARTTLS",
        "usuario": usuario,
        "remetente": _ler(s, "remetente", area=area),
        "responder_para": _ler(s, "responder_para", area=area),
        "tem_senha": bool(senha),
        # "usável" é diferente de "preenchido": sem servidor, usuário e senha,
        # o convite não sai — e a tela precisa dizer isso ANTES de alguém
        # cadastrar dez pessoas achando que elas receberam o link.
        "usavel": bool(servidor and usuario and senha),
    }


def configurar(s: Session, dados: dict[str, Any],
               usuario: Optional[Usuario] = None,
               area: Optional[str] = None) -> dict[str, Any]:
    for campo in ("servidor", "usuario", "remetente", "responder_para"):
        if campo in dados:
            _gravar(s, campo, str(dados[campo] or "").strip(), usuario, area)
    if "porta" in dados:
        try:
            _gravar(s, "porta", str(int(dados["porta"] or 587)), usuario, area)
        except (TypeError, ValueError):
            raise ErroValidacao("A porta tem de ser um número (587 ou 465).")
    if "seguranca" in dados:
        v = str(dados["seguranca"] or "STARTTLS").strip().upper()
        if v not in SEGURANCAS:
            raise ErroValidacao(
                f"Segurança desconhecida: {v}. Use STARTTLS, SSL ou NENHUMA.")
        _gravar(s, "seguranca", v, usuario, area)
    if dados.get("senha"):
        # CIFRADA, NUNCA EM CLARO — e a falta da chave do cofre não faz perder
        # o resto do que foi digitado.
        _gravar(s, "senha", cifrar(str(dados["senha"])), usuario, area)
    s.flush()
    return configuracao(s, area)


def conta(s: Session, area: Optional[str] = None) -> Optional[ContaDoSistema]:
    """A conta pronta para mandar, ou `None` quando não dá.

    `None` e não exceção: quem chama (o convite) precisa poder dizer "cadastrei
    a pessoa, mas o e-mail não saiu porque a conta do sistema não está
    configurada" — e não perder o cadastro por causa disso.
    """
    c = configuracao(s, area)
    if not c["usavel"]:
        return None
    return ContaDoSistema(
        razao_social=c["area_nome"],
        email=c["usuario"],
        smtp_servidor=c["servidor"], smtp_porta=c["porta"],
        smtp_seguranca=c["seguranca"], smtp_usuario=c["usuario"],
        smtp_senha_cifrada=_ler(s, "senha", area=area),
        smtp_remetente=c["remetente"] or None,
        smtp_responder_para=c["responder_para"] or None)


def o_que_falta(s: Session, area: Optional[str] = None) -> str:
    """A frase que a tela mostra quando não dá para mandar. Vazia = dá."""
    c = configuracao(s, area)
    if c["usavel"]:
        return ""
    faltando = [nome for nome, tem in (
        ("o servidor de saída", c["servidor"]), ("o usuário", c["usuario"]),
        ("a senha", c["tem_senha"])) if not tem]
    return (f"A conta de {c['area_nome'].lower()} está incompleta — falta "
            + ", ".join(faltando)
            + f". Configure em {c['onde']}.")


def testar(s: Session, para: str, usuario: Optional[Usuario] = None,
           area: Optional[str] = None):
    """Manda uma mensagem de teste. É o que transforma "acho que configurei"
    em "está configurado"."""
    from app.apps.erp.core.comum import email as svc_email

    c = conta(s, area)
    if c is None:
        raise ErroValidacao(o_que_falta(s, area))
    meta = _area(area)
    return svc_email.enviar(
        s, empresa=c, para=[para],
        assunto=f"Teste do {meta['nome'].lower()} — ERP BWS",
        corpo=("Esta é uma mensagem de teste do ERP BWS.\n\n"
               f"Se você está lendo isto, a conta de {meta['nome'].lower()} "
               f"está configurada. É por ela que saem: {meta['para_que']}.\n\n"
               "Cada área da empresa tem a sua conta, e esta é só de uma "
               "delas — as cotações do suprimento saem pela conta da empresa "
               "(por CNPJ), no cadastro de Empresas.\n"),
        entidade_tipo="sistema", entidade_id=0, usuario=usuario)
