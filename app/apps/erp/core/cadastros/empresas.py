# ============================================================================
# ERP — core/cadastros/empresas.py
# A empresa (CNPJ) que executa a obra, compra e fatura.
#
# A BWS passa a operar com mais de um CNPJ. Três consequências práticas, e
# todas passam por aqui:
#
#   1. a cotação sai do e-mail DA EMPRESA da obra — senão o fornecedor
#      responde para uma caixa que ninguém lê;
#   2. o relatório mandado ao fornecedor leva a logo e os dados de quem
#      compra, e são outros a cada CNPJ;
#   3. a obra diz por qual empresa ela corre.
#
# A senha da conta de e-mail nunca fica em claro: vai cifrada por
# `core/comum/segredos.py`, com a chave na Environment do Render. Se a chave
# não estiver lá, gravar a senha é RECUSADO com o recado do que falta — o
# resto do cadastro salva normalmente.
# ============================================================================
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros.validadores import cnpj_valido, somente_digitos
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.core.comum.email import SEGURANCAS, conta_configurada, endereco_valido
from app.apps.erp.core.comum.segredos import cifrar, ha_chave
from app.apps.erp.db.models.cadastros import Empresa, Obra, Usuario

logger = logging.getLogger(__name__)

# Um logo de relatório não passa disso. O limite existe para ninguém subir uma
# foto de 8 MB e deixar toda leitura de empresa lenta — a logo é lida junto.
LIMITE_LOGO = 512 * 1024
MIMES_DE_LOGO = {"image/png", "image/jpeg", "image/jpg", "image/webp",
                 "image/svg+xml"}

CAMPOS = ("razao_social", "nome_fantasia", "inscricao_estadual",
          "inscricao_municipal", "cep", "logradouro", "numero", "complemento",
          "bairro", "municipio", "uf", "telefone", "email", "site")


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


def obter(s: Session, empresa_id: int, *, travar: bool = False) -> Empresa:
    empresa = s.get(Empresa, empresa_id, with_for_update=travar,
                    populate_existing=travar)
    if empresa is None:
        raise ErroNaoEncontrado("Empresa não encontrada.")
    return empresa


def _por_documento(s: Session, cnpj: str) -> Optional[Empresa]:
    alvo = somente_digitos(cnpj)
    for e in s.scalars(select(Empresa)).all():
        if somente_digitos(e.cnpj) == alvo:
            return e
    return None


# ---------------------------------------------------------------------------
# Criar e editar
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> Empresa:
    razao = _texto(dados.get("razao_social"))
    if len(razao) < 3:
        raise ErroValidacao("Informe a razão social da empresa.")

    cnpj = somente_digitos(dados.get("cnpj"))
    if not cnpj_valido(cnpj):
        raise ErroValidacao(
            f"CNPJ inválido (o dígito verificador não confere): "
            f"{dados.get('cnpj')!r}")
    if _por_documento(s, cnpj) is not None:
        raise ErroValidacao(f"Já existe empresa cadastrada com o CNPJ {cnpj}.")

    email = _texto(dados.get("email"))
    if email and not endereco_valido(email):
        raise ErroValidacao(f"E-mail com formato inválido: {email!r}")

    # `padrao=False` explícito: o padrão do BANCO só vale depois de gravar, e
    # até lá o objeto em memória diria None — que não é "não".
    empresa = Empresa(cnpj=cnpj, razao_social=razao.upper(), ativo=True,
                      padrao=False, smtp_seguranca="STARTTLS")
    for campo in CAMPOS:
        if campo == "razao_social":
            continue
        valor = _texto(dados.get(campo))
        if campo == "uf":
            valor = valor.upper()
        setattr(empresa, campo, valor or None)
    s.add(empresa)
    s.flush()

    # A primeira empresa cadastrada vira a padrão sozinha: sem isso, o
    # primeiro disparo de cotação falharia com "nenhuma empresa escolhida" e
    # ninguém entenderia por quê.
    if not [e for e in s.scalars(select(Empresa)).all()
            if e.padrao and e.id != empresa.id]:
        empresa.padrao = True

    registrar_evento(s, "empresa", empresa.id, "CRIADA",
                     {"cnpj": cnpj, "razao_social": razao},
                     usuario.id if usuario else None)
    return empresa


def editar(s: Session, empresa_id: int, dados: dict[str, Any],
           usuario: Usuario) -> Empresa:
    """CNPJ não se edita — empresa errada se inativa e se cadastra outra, que
    é o que preserva as obras e os envios já ligados a ela."""
    empresa = obter(s, empresa_id, travar=True)
    mudou: dict[str, Any] = {}

    for campo in CAMPOS:
        if campo not in dados:
            continue
        valor = _texto(dados.get(campo))
        if campo == "razao_social":
            if len(valor) < 3:
                raise ErroValidacao("Informe a razão social da empresa.")
            valor = valor.upper()
        if campo == "uf":
            valor = valor.upper()
        if campo == "email" and valor and not endereco_valido(valor):
            raise ErroValidacao(f"E-mail com formato inválido: {valor!r}")
        antigo = getattr(empresa, campo)
        if (valor or None) != antigo:
            mudou[campo] = {"de": antigo, "para": valor or None}
            setattr(empresa, campo, valor or None)

    if "ativo" in dados:
        empresa.ativo = bool(dados.get("ativo"))
        mudou["ativo"] = empresa.ativo
    if dados.get("padrao"):
        definir_padrao(s, empresa_id, usuario)
        mudou["padrao"] = True

    if mudou:
        empresa.atualizado_em = datetime.now(timezone.utc)
        registrar_evento(s, "empresa", empresa.id, "EDITADA", mudou,
                         usuario.id if usuario else None)
    return empresa


def definir_padrao(s: Session, empresa_id: int, usuario: Usuario) -> Empresa:
    """Só uma empresa é a padrão. O banco tem índice único para isso; aqui
    tira a marca da anterior antes de pôr na nova, na mesma transação."""
    empresa = obter(s, empresa_id, travar=True)
    for outra in s.scalars(select(Empresa)).all():
        if outra.id != empresa_id and outra.padrao:
            outra.padrao = False
    s.flush()
    empresa.padrao = True
    registrar_evento(s, "empresa", empresa.id, "VIROU_PADRAO", {},
                     usuario.id if usuario else None)
    return empresa


# ---------------------------------------------------------------------------
# Logo
# ---------------------------------------------------------------------------
def definir_logo(s: Session, empresa_id: int, nome: str, mime: str,
                 conteudo: bytes, usuario: Usuario) -> Empresa:
    empresa = obter(s, empresa_id, travar=True)
    mime = (mime or "").split(";")[0].strip().lower()
    if mime not in MIMES_DE_LOGO:
        raise ErroValidacao(
            "A logo tem de ser PNG, JPG, WEBP ou SVG. "
            f"O arquivo enviado é {mime or 'de tipo desconhecido'}.")
    if not conteudo:
        raise ErroValidacao("O arquivo da logo veio vazio.")
    if len(conteudo) > LIMITE_LOGO:
        raise ErroValidacao(
            f"A logo tem {len(conteudo) // 1024} KB e o limite é "
            f"{LIMITE_LOGO // 1024} KB. Ela é lida junto com a empresa em toda "
            f"tela — arquivo grande deixa o sistema lento inteiro.")
    empresa.logo = conteudo
    empresa.logo_mime = mime
    empresa.logo_nome = _texto(nome) or "logo"
    registrar_evento(s, "empresa", empresa.id, "LOGO_DEFINIDA",
                     {"nome": empresa.logo_nome, "bytes": len(conteudo)},
                     usuario.id if usuario else None)
    return empresa


def remover_logo(s: Session, empresa_id: int, usuario: Usuario) -> Empresa:
    empresa = obter(s, empresa_id, travar=True)
    empresa.logo = None
    empresa.logo_mime = None
    empresa.logo_nome = None
    registrar_evento(s, "empresa", empresa.id, "LOGO_REMOVIDA", {},
                     usuario.id if usuario else None)
    return empresa


# ---------------------------------------------------------------------------
# A conta de envio
# ---------------------------------------------------------------------------
def definir_conta_de_email(s: Session, empresa_id: int, dados: dict[str, Any],
                           usuario: Usuario) -> Empresa:
    """Servidor, porta, usuário, segurança e (opcionalmente) a senha.

    A senha só vem quando está sendo TROCADA: a tela manda o campo em branco
    quando não se quer mexer nela, e aí a senha guardada continua valendo.
    Isso evita o clássico "editei o telefone e apaguei a senha sem querer".
    """
    empresa = obter(s, empresa_id, travar=True)

    seguranca = _texto(dados.get("smtp_seguranca")).upper() or "STARTTLS"
    if seguranca not in SEGURANCAS:
        raise ErroValidacao(
            f"Tipo de segurança desconhecido: {seguranca}. "
            f"Use {', '.join(SEGURANCAS)}.")

    porta = dados.get("smtp_porta")
    if porta not in (None, ""):
        try:
            porta = int(porta)
        except (TypeError, ValueError):
            raise ErroValidacao("A porta do servidor tem de ser um número.")
        if not 1 <= porta <= 65535:
            raise ErroValidacao("Porta fora da faixa (1 a 65535).")
    else:
        porta = None

    for campo in ("smtp_remetente", "smtp_responder_para"):
        valor = _texto(dados.get(campo))
        if valor and not endereco_valido(valor):
            raise ErroValidacao(f"Endereço com formato inválido: {valor!r}")

    empresa.smtp_servidor = _texto(dados.get("smtp_servidor")) or None
    empresa.smtp_porta = porta
    empresa.smtp_usuario = _texto(dados.get("smtp_usuario")) or None
    empresa.smtp_seguranca = seguranca
    empresa.smtp_remetente = _texto(dados.get("smtp_remetente")) or None
    empresa.smtp_responder_para = _texto(dados.get("smtp_responder_para")) or None

    senha = dados.get("smtp_senha")
    if senha:
        if not ha_chave():
            raise ErroValidacao(
                "A senha não foi guardada: falta a chave ERP_CHAVE_SEGREDOS na "
                "Environment do Render. O resto da conta foi salvo — assim que "
                "a chave existir, digite a senha de novo.")
        empresa.smtp_senha_cifrada = cifrar(str(senha))
        empresa.smtp_conferido_em = None      # senha nova, teste de novo

    registrar_evento(s, "empresa", empresa.id, "CONTA_DE_EMAIL_DEFINIDA",
                     {"servidor": empresa.smtp_servidor,
                      "porta": empresa.smtp_porta,
                      "usuario": empresa.smtp_usuario,
                      "seguranca": seguranca,
                      "senha_trocada": bool(senha)},
                     usuario.id if usuario else None)
    return empresa


# ---------------------------------------------------------------------------
# Quem manda o quê
# ---------------------------------------------------------------------------
def empresa_da_obra(s: Session, obra_id: Optional[int]) -> Optional[Empresa]:
    """A empresa que executa a obra; se a obra não disser, a padrão."""
    obra = s.get(Obra, obra_id) if obra_id else None
    if obra is not None and getattr(obra, "empresa_id", None):
        empresa = s.get(Empresa, obra.empresa_id)
        if empresa is not None:
            return empresa
    return padrao(s)


def padrao(s: Session) -> Optional[Empresa]:
    empresas = [e for e in s.scalars(select(Empresa)).all() if e.ativo is not False]
    for e in empresas:
        if e.padrao:
            return e
    return empresas[0] if len(empresas) == 1 else None


def definir_empresa_da_obra(s: Session, obra_id: int,
                            empresa_id: Optional[int],
                            usuario: Usuario) -> Obra:
    obra = s.get(Obra, obra_id, with_for_update=True, populate_existing=True)
    if obra is None:
        raise ErroNaoEncontrado("Obra não encontrada.")
    if empresa_id:
        obter(s, int(empresa_id))          # existe? senão, 404
        obra.empresa_id = int(empresa_id)
    else:
        obra.empresa_id = None
    registrar_evento(s, "obra", obra.id, "EMPRESA_DEFINIDA",
                     {"empresa_id": obra.empresa_id},
                     usuario.id if usuario else None)
    return obra


# ---------------------------------------------------------------------------
# Para a tela
# ---------------------------------------------------------------------------
def _resumo(s: Session, e: Empresa, obras_por_empresa: dict[int, int]) -> dict[str, Any]:
    pode, falta = conta_configurada(e)
    return {
        "id": e.id, "razao_social": e.razao_social,
        "nome_fantasia": e.nome_fantasia or "", "cnpj": e.cnpj,
        "inscricao_estadual": e.inscricao_estadual or "",
        "inscricao_municipal": e.inscricao_municipal or "",
        "cep": e.cep or "", "logradouro": e.logradouro or "",
        "numero": e.numero or "", "complemento": e.complemento or "",
        "bairro": e.bairro or "", "municipio": e.municipio or "",
        "uf": e.uf or "", "telefone": e.telefone or "",
        "email": e.email or "", "site": e.site or "",
        "tem_logo": e.logo is not None, "logo_nome": e.logo_nome or "",
        "smtp_servidor": e.smtp_servidor or "", "smtp_porta": e.smtp_porta,
        "smtp_usuario": e.smtp_usuario or "",
        "smtp_seguranca": e.smtp_seguranca or "STARTTLS",
        "smtp_remetente": e.smtp_remetente or "",
        "smtp_responder_para": e.smtp_responder_para or "",
        "tem_senha": bool(e.smtp_senha_cifrada),
        "conferido_em": (e.smtp_conferido_em.isoformat()
                         if e.smtp_conferido_em else None),
        "pode_enviar": pode, "o_que_falta": falta,
        "obras": obras_por_empresa.get(e.id, 0),
        "ativo": e.ativo is not False, "padrao": bool(e.padrao),
    }


def gerenciar(s: Session) -> dict[str, Any]:
    """A tela de empresas: a lista, as obras de cada uma, e as obras órfãs."""
    obras = s.scalars(select(Obra)).all()
    por_empresa: dict[int, int] = {}
    sem_empresa = []
    for o in obras:
        if getattr(o, "status", None) == "ENCERRADA":
            continue
        if getattr(o, "empresa_id", None):
            por_empresa[o.empresa_id] = por_empresa.get(o.empresa_id, 0) + 1
        else:
            sem_empresa.append({"id": o.id, "codigo": o.codigo, "nome": o.nome})

    linhas = [_resumo(s, e, por_empresa)
              for e in s.scalars(select(Empresa)).all()]
    linhas.sort(key=lambda x: x["razao_social"])
    ativas = [l for l in linhas if l["ativo"]]
    return {
        "empresas": linhas,
        "obras_sem_empresa": sorted(sem_empresa, key=lambda o: o["codigo"]),
        "tem_chave_de_segredos": ha_chave(),
        "seguranca_opcoes": list(SEGURANCAS),
        "indicadores": {
            "total": len(linhas),
            "ativas": len(ativas),
            # Estes dois são a razão de a tela existir: empresa sem conta não
            # dispara cotação nenhuma, e obra sem empresa não sabe por qual
            # e-mail sair.
            "sem_conta_de_email": sum(1 for l in ativas if not l["pode_enviar"]),
            "sem_logo": sum(1 for l in ativas if not l["tem_logo"]),
            "obras_sem_empresa": len(sem_empresa),
        },
    }
