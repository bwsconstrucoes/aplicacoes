# ============================================================================
# ERP — core/arquivo/catalogo.py
# O catálogo de tipos de documento da BWS.
#
# Este arquivo traz o catálogo INICIAL, do §4 da `GESTAO_DOCUMENTOS.md`. Ele não
# é a verdade final: o catálogo mora no banco e é editável pela tela, porque
# quem sabe quais documentos a BWS usa toda semana é a BWS.
#
# Aplicar de novo é seguro (mesma ideia do plano de contas): cria o que falta,
# atualiza o nome de quem não foi personalizado, e NUNCA apaga — tipo apagado
# deixaria documento órfão, que é o problema que este módulo existe para
# resolver.
# ============================================================================
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.financeiro import Documento, DocumentoTipo

logger = logging.getLogger(__name__)

GRUPOS = {
    "CADASTRAL": "Cadastrais da empresa",
    "CERTIDAO": "Certidões",
    "LICITACAO": "Licitação e técnico",
    "OBRA": "Obra",
    "FISCAL": "Fiscal e trabalhista",
    "PESSOA": "Pessoas",
    "FINANCEIRO": "Financeiro e parceiros",
}

DONOS = ("EMPRESA", "OBRA", "PESSOA", "PARCEIRO", "LANCAMENTO")
SIGILOS = ("ABERTO", "RESTRITO", "PESSOAL")

# (código, nome, grupo, dono, vence, avisar_dias, por_competência, sigilo)
#
# `avisar_dias` é menor para documento de vida curta: certidão que vale 30 dias
# não pode avisar com 30 de antecedência — avisaria no dia da emissão.
CATALOGO: tuple[tuple[Any, ...], ...] = (
    # ---- cadastrais da empresa
    ("CONTRATO-SOCIAL", "Contrato social e alterações", "CADASTRAL", "EMPRESA", False, None, False, "ABERTO"),
    ("CARTAO-CNPJ", "Cartão CNPJ", "CADASTRAL", "EMPRESA", False, None, False, "ABERTO"),
    ("BALANCO", "Balanço patrimonial", "CADASTRAL", "EMPRESA", True, 60, False, "RESTRITO"),
    ("FATURAMENTO-12M", "Declaração de faturamento (12 meses)", "CADASTRAL", "EMPRESA", True, 30, False, "RESTRITO"),
    ("COMPROVANTE-ENDERECO", "Comprovante de endereço", "CADASTRAL", "EMPRESA", True, 15, False, "ABERTO"),
    ("INSCRICAO-ESTADUAL", "Inscrição estadual", "CADASTRAL", "EMPRESA", False, None, False, "ABERTO"),
    ("INSCRICAO-MUNICIPAL", "Inscrição municipal", "CADASTRAL", "EMPRESA", False, None, False, "ABERTO"),
    ("ALVARA", "Alvará de funcionamento", "CADASTRAL", "EMPRESA", True, 30, False, "ABERTO"),
    ("PROCURACAO", "Procuração", "CADASTRAL", "EMPRESA", True, 30, False, "RESTRITO"),
    # ---- certidões: todas vencem, e é justamente esse o valor
    ("CND-FEDERAL", "Certidão negativa federal (RFB/PGFN)", "CERTIDAO", "EMPRESA", True, 15, False, "ABERTO"),
    ("CND-ESTADUAL", "Certidão negativa estadual", "CERTIDAO", "EMPRESA", True, 15, False, "ABERTO"),
    ("CND-MUNICIPAL", "Certidão negativa municipal", "CERTIDAO", "EMPRESA", True, 15, False, "ABERTO"),
    ("CRF-FGTS", "Certificado de regularidade do FGTS", "CERTIDAO", "EMPRESA", True, 10, False, "ABERTO"),
    ("CNDT", "Certidão negativa de débitos trabalhistas", "CERTIDAO", "EMPRESA", True, 15, False, "ABERTO"),
    ("CND-FALENCIA", "Certidão de falência e concordata", "CERTIDAO", "EMPRESA", True, 10, False, "ABERTO"),
    ("CERTIDAO-CREA", "Certidão de registro no CREA", "CERTIDAO", "EMPRESA", True, 30, False, "ABERTO"),
    # ---- licitação e técnico
    ("ATESTADO-CAPACIDADE", "Atestado de capacidade técnica", "LICITACAO", "EMPRESA", False, None, False, "ABERTO"),
    ("CAT", "Certidão de acervo técnico (CAT)", "LICITACAO", "EMPRESA", False, None, False, "ABERTO"),
    ("DECLARACAO", "Declaração para licitação", "LICITACAO", "EMPRESA", False, None, False, "ABERTO"),
    ("PROPOSTA-LICITACAO", "Proposta apresentada", "LICITACAO", "EMPRESA", False, None, False, "RESTRITO"),
    ("EDITAL", "Edital do certame", "LICITACAO", "EMPRESA", False, None, False, "ABERTO"),
    # ---- obra
    ("CONTRATO-OBRA", "Contrato da obra", "OBRA", "OBRA", True, 60, False, "RESTRITO"),
    ("ADITIVO", "Termo aditivo", "OBRA", "OBRA", True, 60, False, "RESTRITO"),
    ("OS", "Ordem de serviço", "OBRA", "OBRA", False, None, False, "ABERTO"),
    ("ART", "ART / RRT", "OBRA", "OBRA", False, None, False, "ABERTO"),
    ("MATRICULA-CEI-CNO", "Matrícula CNO/CEI", "OBRA", "OBRA", False, None, False, "ABERTO"),
    ("LICENCA", "Licença ambiental ou urbanística", "OBRA", "OBRA", True, 45, False, "ABERTO"),
    ("SEGURO", "Apólice de seguro", "OBRA", "OBRA", True, 30, False, "RESTRITO"),
    ("MEDICAO", "Medição", "OBRA", "OBRA", False, None, True, "RESTRITO"),
    ("DIARIO-OBRA", "Diário de obra", "OBRA", "OBRA", False, None, True, "ABERTO"),
    ("PROJETO", "Projeto e memorial", "OBRA", "OBRA", False, None, False, "ABERTO"),
    # ---- fiscal e trabalhista, por competência
    ("FOLHA", "Folha de pagamento", "FISCAL", "OBRA", False, None, True, "RESTRITO"),
    ("RELATORIO-FGTS", "Relatório de FGTS", "FISCAL", "OBRA", False, None, True, "RESTRITO"),
    ("GUIA-FGTS", "Guia do FGTS", "FISCAL", "OBRA", False, None, True, "RESTRITO"),
    ("COMPROVANTE-FGTS", "Comprovante de pagamento do FGTS", "FISCAL", "OBRA", False, None, True, "RESTRITO"),
    ("DCTFWEB-RECIBO", "Recibo da DCTFWeb", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("DCTFWEB-CREDITOS", "Resumo de créditos da DCTFWeb", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("DARF-INSS", "DARF do INSS", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("COMPROVANTE-INSS", "Comprovante do INSS", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("DARF-PIS-COFINS", "DARF de PIS/Cofins", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("COMPROVANTE-PIS-COFINS", "Comprovante de PIS/Cofins", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("GPS", "GPS", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("ESOCIAL-RECIBO", "Recibo do eSocial", "FISCAL", "EMPRESA", False, None, True, "RESTRITO"),
    ("RESCISAO", "Termo de rescisão", "FISCAL", "PESSOA", False, None, False, "PESSOAL"),
    # ---- pessoas
    ("DOC-IDENTIDADE", "RG / CNH / CPF", "PESSOA", "PESSOA", False, None, False, "PESSOAL"),
    ("CTPS", "Carteira de trabalho", "PESSOA", "PESSOA", False, None, False, "PESSOAL"),
    ("FICHA-REGISTRO", "Ficha de registro", "PESSOA", "PESSOA", False, None, False, "PESSOAL"),
    ("CONTRATO-TRABALHO", "Contrato de trabalho", "PESSOA", "PESSOA", False, None, False, "PESSOAL"),
    ("ASO", "Atestado de saúde ocupacional", "PESSOA", "PESSOA", True, 30, False, "PESSOAL"),
    ("EPI-FICHA", "Ficha de entrega de EPI", "PESSOA", "PESSOA", False, None, False, "PESSOAL"),
    ("CERTIFICADO-NR", "Certificado de NR", "PESSOA", "PESSOA", True, 45, False, "PESSOAL"),
    ("CONTRATO-SOCIO", "Documentos de sócio", "PESSOA", "PESSOA", False, None, False, "PESSOAL"),
    # ---- financeiro e parceiros
    ("NOTA-FISCAL", "Nota fiscal", "FINANCEIRO", "LANCAMENTO", False, None, False, "ABERTO"),
    ("COMPROVANTE", "Comprovante de pagamento", "FINANCEIRO", "LANCAMENTO", False, None, False, "RESTRITO"),
    ("BOLETO", "Boleto", "FINANCEIRO", "LANCAMENTO", False, None, False, "RESTRITO"),
    ("CONTRATO-EMPRESTIMO", "Contrato de empréstimo bancário", "FINANCEIRO", "EMPRESA", True, 60, False, "RESTRITO"),
    ("CONTRATO-FORNECEDOR", "Contrato com fornecedor", "FINANCEIRO", "PARCEIRO", True, 45, False, "RESTRITO"),
    ("PROPOSTA", "Proposta comercial", "FINANCEIRO", "PARCEIRO", False, None, False, "ABERTO"),
    ("OUTRO", "Outro documento", "FINANCEIRO", "EMPRESA", False, None, False, "RESTRITO"),
)


def aplicar(s: Session) -> dict[str, int]:
    """Cria o que falta e atualiza o nome do que não foi personalizado.

    NUNCA apaga: tipo removido deixaria documento órfão, que é exatamente o
    problema que este módulo existe para resolver. Tipo que não serve mais é
    DESATIVADO na tela — some das listas e continua explicando o que já existe.
    """
    existentes = {t.codigo: t for t in s.scalars(select(DocumentoTipo)).all()}
    criados = atualizados = 0
    for ordem, (codigo, nome, grupo, dono, vence, avisar, comp, sigilo) in enumerate(CATALOGO, 1):
        t = existentes.get(codigo)
        if t is None:
            s.add(DocumentoTipo(codigo=codigo, nome=nome, grupo=grupo, dono=dono,
                                vence=vence, avisar_dias=avisar,
                                por_competencia=comp, sigilo=sigilo, ordem=ordem))
            criados += 1
            continue
        if t.nome != nome or t.ordem != ordem:
            t.nome, t.ordem = nome, ordem
            atualizados += 1
    s.flush()
    logger.info("ERP/arquivo: catálogo — %d criado(s), %d atualizado(s)",
                criados, atualizados)
    return {"criados": criados, "atualizados": atualizados,
            "total": len(CATALOGO)}


def listar(s: Session, *, incluir_inativos: bool = False) -> list[dict[str, Any]]:
    stmt = select(DocumentoTipo).order_by(DocumentoTipo.ordem, DocumentoTipo.codigo)
    if not incluir_inativos:
        stmt = stmt.where(DocumentoTipo.ativo.is_(True))
    return [{
        "codigo": t.codigo, "nome": t.nome, "grupo": t.grupo,
        "grupo_nome": GRUPOS.get(t.grupo, t.grupo),
        "dono": t.dono, "vence": t.vence, "avisar_dias": t.avisar_dias,
        "por_competencia": t.por_competencia, "sigilo": t.sigilo,
        "ativo": t.ativo,
    } for t in s.scalars(stmt).all()]


def obter(s: Session, codigo: str) -> DocumentoTipo:
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    t = s.get(DocumentoTipo, (codigo or "").strip().upper())
    if t is None:
        raise ErroValidacao(f"Tipo de documento desconhecido: {codigo}.")
    return t


def em_uso(s: Session, codigo: str) -> int:
    from sqlalchemy import func as _f
    return s.scalar(select(_f.count(Documento.id)).where(
        Documento.tipo_codigo == codigo)) or 0
