# ============================================================================
# ERP — core/cadastros/certificado.py
# O certificado digital A1 de cada empresa.
#
# POR QUE ELE PRECISA MORAR NO SISTEMA
#
# Hoje o arquivo .pfx vive no computador de alguém, com a senha num papel ou
# num bilhete de conversa. Aí ele vence num sábado, ninguém sabe, e a obra
# para de faturar na segunda-feira. Duas coisas já construídas dependiam dele:
# a emissão automática da nota (no padrão nacional a DPS vai ASSINADA) e o
# aviso de vencimento na agenda.
#
# TRÊS DECISÕES QUE ATRAVESSAM ESTE ARQUIVO
#
#   1. O ARQUIVO E A SENHA VÃO CIFRADOS, com a mesma chave que já protege a
#      senha de e-mail (`ERP_CHAVE_SEGREDOS`, na Environment do Render).
#      Certificado digital é a ASSINATURA DA EMPRESA: quem o tem, assina no
#      nome dela. Uma cópia do banco não pode bastar. Sem a chave, o sistema
#      RECUSA gravar — guardar em claro "só desta vez" é como uma assinatura
#      de empresa vaza sem ninguém perceber.
#
#   2. A VALIDADE É LIDA DE DENTRO DO ARQUIVO, nunca digitada. Campo de data
#      que a pessoa preenche é campo que ela erra ou esquece — e aqui o erro só
#      apareceria no dia em que a nota não sai. De quebra, ler o arquivo PROVA
#      que a senha está certa: certificado que não abre não entra.
#
#   3. O ARQUIVO NUNCA VOLTA PELA TELA. A tela mostra titular, validade e
#      emissor; os bytes só saem por dentro, para quem vai assinar. Não há
#      rota de download — o que não tem porta não é arrombado.
# ============================================================================
from __future__ import annotations

import base64
import logging
from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum import segredos
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Empresa, EmpresaCertificado, Usuario

logger = logging.getLogger(__name__)

# Teto do arquivo. Um A1 tem alguns kilobytes; megabytes aqui é outra coisa
# no lugar errado — e cifrar coisa grande enche o banco à toa.
MAX_BYTES = 512 * 1024

# A partir de quantos dias antes o vencimento vira aviso. Certificado se
# renova com a contadora, e isso leva dias — avisar na véspera não ajuda.
AVISO_DIAS = 45


class ErroCertificado(ErroValidacao):
    """Erro que a pessoa consegue resolver: senha errada, arquivo trocado."""


def _ler_pfx(conteudo: bytes, senha: str) -> dict[str, Any]:
    """Abre o .pfx e conta o que há dentro.

    Abrir o arquivo é o que PROVA que a senha está certa. Aceitar a senha sem
    conferir deixaria o erro para o dia da emissão — quando ninguém vai
    lembrar que o certificado foi cadastrado três meses antes.
    """
    from cryptography.hazmat.primitives.serialization import pkcs12

    try:
        _, cert, _ = pkcs12.load_key_and_certificates(
            conteudo, (senha or "").encode())
    except Exception:
        raise ErroCertificado(
            "Não consegui abrir o certificado. Quase sempre é a senha — "
            "confira e tente de novo. Se a senha estiver certa, verifique se o "
            "arquivo é mesmo o .pfx (ou .p12) do certificado A1.")
    if cert is None:
        raise ErroCertificado(
            "O arquivo abriu, mas não tem certificado dentro. Confira se é o "
            "arquivo certo.")

    def _nome(atributos) -> str:
        try:
            partes = [a.value for a in atributos if isinstance(a.value, str)]
            return partes[0] if partes else ""
        except Exception:                                  # pragma: no cover
            return ""

    from cryptography.x509.oid import NameOID
    titular = _nome(cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME))
    emissor = _nome(cert.issuer.get_attributes_for_oid(NameOID.COMMON_NAME))

    # O CNPJ vem colado no nome do titular ("EMPRESA LTDA:11222333000181"),
    # que é como a ICP-Brasil monta o certificado de pessoa jurídica.
    documento = ""
    if ":" in titular:
        titular, _, documento = titular.partition(":")

    def _quando(campo_novo: str, campo_velho: str) -> datetime:
        # `not_valid_after_utc` é o nome novo; o antigo some em versões
        # futuras da biblioteca. Tentar os dois evita quebrar na atualização.
        valor = getattr(cert, campo_novo, None) or getattr(cert, campo_velho)
        return valor

    return {
        "titular": titular.strip(),
        "documento": "".join(c for c in documento if c.isdigit()),
        "emissor": emissor.strip(),
        "numero_serie": format(cert.serial_number, "x"),
        "valido_de": _quando("not_valid_before_utc", "not_valid_before").date(),
        "valido_ate": _quando("not_valid_after_utc", "not_valid_after").date(),
    }


def guardar(s: Session, empresa_id: int, conteudo: bytes, senha: str, *,
            nome_arquivo: str = "", observacao: str = "",
            usuario: Optional[Usuario] = None) -> EmpresaCertificado:
    """Guarda o certificado — cifrado — depois de abrir para conferir."""
    empresa = s.get(Empresa, empresa_id)
    if empresa is None:
        raise ErroValidacao("Empresa não encontrada.")
    if not conteudo:
        raise ErroCertificado("Anexe o arquivo do certificado (.pfx ou .p12).")
    if len(conteudo) > MAX_BYTES:
        raise ErroCertificado(
            f"O arquivo tem {len(conteudo) // 1024} KB. Um certificado A1 tem "
            f"alguns kilobytes — confira se anexou o arquivo certo.")
    if not (senha or "").strip():
        raise ErroCertificado("Informe a senha do certificado.")
    if not segredos.ha_chave():
        # A recusa é o comportamento certo: sem a chave, guardar significaria
        # guardar em claro.
        raise ErroCertificado(
            "A chave de segredos do ERP não está configurada, e sem ela o "
            "certificado não pode ser guardado com segurança. Defina "
            "ERP_CHAVE_SEGREDOS na Environment do Render e tente de novo.")

    dados = _ler_pfx(conteudo, senha)

    # Certificado já vencido não entra: assinar com ele não funciona, e
    # guardá-lo só criaria a impressão de que a empresa está em dia.
    if dados["valido_ate"] < date.today():
        raise ErroCertificado(
            f"Este certificado venceu em "
            f"{dados['valido_ate'].strftime('%d/%m/%Y')}. Envie o novo.")

    # O CNPJ do certificado tem de ser o da empresa. Trocar os arquivos de
    # duas empresas é o erro fácil de cometer e difícil de descobrir: as notas
    # sairiam assinadas pelo CNPJ errado.
    cnpj = "".join(c for c in (empresa.cnpj or "") if c.isdigit())
    if dados["documento"] and cnpj and dados["documento"] != cnpj:
        raise ErroCertificado(
            f"Este certificado é do CNPJ {dados['documento']}, e a empresa "
            f"{empresa.nome_fantasia or empresa.razao_social} é o {cnpj}. "
            f"Confira se não trocou os arquivos.")

    # O anterior não é apagado: a nota assinada em março foi assinada com
    # AQUELE certificado, e um dia alguém vai perguntar com qual.
    anterior = s.scalars(select(EmpresaCertificado).where(
        EmpresaCertificado.empresa_id == empresa_id,
        EmpresaCertificado.situacao == "ATIVO")).first()
    if anterior is not None:
        anterior.situacao = "SUBSTITUIDO"
        s.flush()

    novo = EmpresaCertificado(
        empresa_id=empresa_id,
        arquivo_cifrado=segredos.cifrar(base64.b64encode(conteudo).decode()),
        senha_cifrada=segredos.cifrar(senha),
        nome_arquivo=(nome_arquivo or "").strip()[:200] or None,
        observacao=(observacao or "").strip() or None,
        enviado_por=usuario.id if usuario else None,
        situacao="ATIVO", **dados)
    s.add(novo)
    s.flush()
    registrar_evento(s, "empresa", empresa_id, "CERTIFICADO_GUARDADO", {
        "titular": dados["titular"], "valido_ate": dados["valido_ate"].isoformat(),
        "emissor": dados["emissor"], "substituiu": bool(anterior)},
        usuario.id if usuario else None)
    logger.info("ERP/certificado: %s guardado para a empresa %s, válido até %s",
                dados["titular"], empresa.cnpj, dados["valido_ate"])
    return novo


def ler(s: Session, empresa_id: int) -> dict[str, Any]:
    """O que a TELA mostra. Nunca o arquivo, nunca a senha."""
    c = s.scalars(select(EmpresaCertificado).where(
        EmpresaCertificado.empresa_id == empresa_id,
        EmpresaCertificado.situacao == "ATIVO")).first()
    if c is None:
        return {"tem": False, "ha_chave": segredos.ha_chave()}
    dias = (c.valido_ate - date.today()).days
    return {
        "tem": True, "ha_chave": segredos.ha_chave(),
        "id": c.id, "titular": c.titular or "", "documento": c.documento or "",
        "emissor": c.emissor or "", "numero_serie": c.numero_serie or "",
        "valido_de": c.valido_de.isoformat() if c.valido_de else None,
        "valido_ate": c.valido_ate.isoformat(),
        "dias": dias,
        "vencido": dias < 0,
        "vencendo": 0 <= dias <= AVISO_DIAS,
        "situacao_texto": ("VENCIDO há {} dia(s)".format(-dias) if dias < 0
                           else "vence hoje" if dias == 0
                           else "vence em {} dia(s)".format(dias)),
        "nome_arquivo": c.nome_arquivo or "",
        "observacao": c.observacao or "",
        "enviado_em": c.criado_em.isoformat() if c.criado_em else None,
    }


def historico(s: Session, empresa_id: int) -> list[dict[str, Any]]:
    """Os certificados anteriores. A nota de março foi assinada com um deles."""
    return [{"id": c.id, "titular": c.titular or "",
             "valido_de": c.valido_de.isoformat() if c.valido_de else None,
             "valido_ate": c.valido_ate.isoformat(),
             "situacao": c.situacao,
             "guardado_em": c.criado_em.isoformat() if c.criado_em else None}
            for c in s.scalars(select(EmpresaCertificado).where(
                EmpresaCertificado.empresa_id == empresa_id)
                .order_by(EmpresaCertificado.criado_em.desc())).all()]


def material_para_assinar(s: Session, empresa_id: int) -> tuple[bytes, str]:
    """Os bytes e a senha, em claro — para quem vai ASSINAR.

    Fica isolado numa função só para ser fácil de auditar quem usa: nenhuma
    tela chama isto, e nenhuma resposta JSON devolve o resultado. É o mesmo
    cuidado do token da prefeitura.
    """
    c = s.scalars(select(EmpresaCertificado).where(
        EmpresaCertificado.empresa_id == empresa_id,
        EmpresaCertificado.situacao == "ATIVO")).first()
    if c is None:
        raise ErroCertificado(
            "Esta empresa não tem certificado digital cadastrado. Sem ele a "
            "nota não pode ser assinada.")
    if c.valido_ate < date.today():
        raise ErroCertificado(
            f"O certificado da empresa venceu em "
            f"{c.valido_ate.strftime('%d/%m/%Y')}. Envie o novo antes de emitir.")
    conteudo = base64.b64decode(segredos.decifrar(c.arquivo_cifrado) or "")
    return conteudo, (segredos.decifrar(c.senha_cifrada) or "")


def vencendo(s: Session, *, dias: int = AVISO_DIAS,
             hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """Os certificados perto de vencer — é daqui que a agenda avisa."""
    hoje = hoje or date.today()
    limite = hoje + timedelta(days=dias)
    saida = []
    for c in s.scalars(select(EmpresaCertificado).where(
            EmpresaCertificado.situacao == "ATIVO",
            EmpresaCertificado.valido_ate <= limite)).all():
        empresa = s.get(Empresa, c.empresa_id)
        saida.append({
            "certificado_id": c.id, "empresa_id": c.empresa_id,
            "empresa": (getattr(empresa, "nome_fantasia", "")
                        or getattr(empresa, "razao_social", "")),
            "titular": c.titular or "",
            "valido_ate": c.valido_ate,
            "dias": (c.valido_ate - hoje).days,
        })
    saida.sort(key=lambda x: x["valido_ate"])
    return saida
