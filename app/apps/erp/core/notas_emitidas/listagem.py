# ============================================================================
# ERP — core/notas_emitidas/listagem.py
# A tela de controle das notas emitidas.
#
# POR QUE ESTA TELA NÃO É A DE TÍTULOS A RECEBER
#
# O próprio dono levantou a dúvida — *"talvez isso seja a mesma coisa que o
# título a receber, ou não, não sei. Aí você vai fazer essa crítica."* — e a
# resposta é que NÃO é, por dois motivos que dão trabalho de verdade:
#
#   - o título a receber inclui coisa que não é medição e não tem nota;
#   - uma medição pode virar DUAS notas (faturamento parcial), e uma nota pode
#     ser cancelada e substituída sem o título mudar uma vírgula.
#
# São dois eixos diferentes. Forçá-los na mesma tela esconde exatamente os
# casos que precisam aparecer. Ficam duas telas irmãs, ligadas nos dois
# sentidos: do título se chega à nota, da nota se chega ao título.
#
# O PROPÓSITO, nas palavras dele: *"às vezes a contabilidade precisa gerar um
# relatório das informações — valor da nota, tributos e tal."* Por isso as
# retenções aparecem em coluna separada, uma por tributo, e não somadas: a
# contabilidade lança cada uma numa conta diferente.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (ContaBancaria, Empresa,
                                              Fornecedor, Obra)
from app.apps.erp.db.models.financeiro import (NotaEmitida, Pagamento, Parcela,
                                               Titulo)

logger = logging.getLogger(__name__)
CENT = Decimal("0.01")

# Os tributos que a contabilidade pede, confirmados pelo dono em 09/09/2026.
# A ordem é a da guia, não alfabética — é assim que ele lê.
TRIBUTOS: tuple[tuple[str, str], ...] = (
    ("iss", "ISS"),
    ("ir", "IR"),
    ("inss", "INSS"),
    ("pis", "PIS"),
    ("cofins", "COFINS"),
    ("csll", "CSLL"),
)

SITUACAO_ROTULOS = {
    "RESERVADA": "Reservada",
    "EMITIDA": "Emitida",
    "FALHADA": "Falhou",
    "CANCELADA": "Cancelada",
    "SUBSTITUIDA": "Substituída",
}


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal(0)


def _f(v: Decimal) -> float:
    return float(v.quantize(CENT))


def _retencoes_de(nota: NotaEmitida) -> tuple[dict[str, float], Decimal]:
    """Cada tributo em sua coluna, e o total retido.

    Somar tudo num número só pouparia colunas e estragaria o relatório: a
    contabilidade lança ISS numa conta e INSS em outra, e refazer a separação
    a partir do total é impossível.
    """
    guardado = nota.retencoes if isinstance(nota.retencoes, dict) else {}
    saida, total = {}, Decimal(0)
    for chave, _ in TRIBUTOS:
        valor = _dec(guardado.get(chave))
        saida[chave] = _f(valor)
        total += valor
    return saida, total


def _recebimento_do_titulo(s: Session, titulo_id: Optional[int]) -> dict[str, Any]:
    """Quanto entrou, quando, e em que conta.

    Vem do TÍTULO, não da nota: quem recebe é o título, e a nota é o documento.
    Nota parcial de um título já recebido mostraria valor demais se copiasse o
    recebimento inteiro — por isso o valor vem rotulado como "do título", e a
    tela diz isso na coluna.
    """
    vazio = {"valor": 0.0, "em": None, "conta": ""}
    if not titulo_id:
        return vazio
    pagos = list(s.scalars(
        select(Pagamento).join(Parcela, Pagamento.parcela_id == Parcela.id)
        .where(Parcela.titulo_id == titulo_id)).all())
    if not pagos:
        return vazio
    total = sum((_dec(p.valor_pago) for p in pagos), Decimal(0))
    datas = [p.data_pagamento for p in pagos if p.data_pagamento]
    ultimo = max(pagos, key=lambda p: (p.data_pagamento or date.min))
    conta = (s.get(ContaBancaria, ultimo.conta_bancaria_id)
             if ultimo.conta_bancaria_id else None)
    return {"valor": _f(total),
            "em": max(datas).isoformat() if datas else None,
            "conta": getattr(conta, "descricao", "") or ""}


def ler(s: Session, nota: NotaEmitida) -> dict[str, Any]:
    empresa = s.get(Empresa, nota.empresa_id) if nota.empresa_id else None
    titulo = s.get(Titulo, nota.titulo_id) if nota.titulo_id else None
    obra = s.get(Obra, nota.obra_id) if nota.obra_id else None
    cliente = (s.get(Fornecedor, titulo.fornecedor_id)
               if titulo is not None and titulo.fornecedor_id else None)
    retencoes, retido = _retencoes_de(nota)
    bruto = _dec(nota.valor_bruto)
    # O líquido guardado manda; sem ele, bruto menos retido — que é a conta
    # que a contabilidade faz de qualquer jeito.
    liquido = _dec(nota.valor_liquido) if nota.valor_liquido is not None else bruto - retido
    return {
        "id": nota.id,
        "numero_nota": nota.numero_nota or "",
        "numero_dps": nota.numero_dps,
        "serie": nota.serie,
        "ambiente": nota.ambiente,
        "modo": nota.modo,
        "empresa": (getattr(empresa, "nome_fantasia", "")
                    or getattr(empresa, "razao_social", "")),
        "empresa_id": nota.empresa_id,
        "obra": getattr(obra, "codigo", "") or "",
        "obra_id": nota.obra_id,
        "titulo_id": nota.titulo_id,
        "numero_sp": getattr(titulo, "numero_sp", "") or "",
        "medicao": getattr(titulo, "numero_medicao", "") or "",
        "periodo": {
            "inicio": (titulo.periodo_inicio.isoformat()
                       if titulo is not None and titulo.periodo_inicio else None),
            "fim": (titulo.periodo_fim.isoformat()
                    if titulo is not None and titulo.periodo_fim else None),
        },
        "cliente": getattr(cliente, "razao_social", "") or "",
        "competencia": nota.competencia.isoformat() if nota.competencia else None,
        "emissao": nota.data_emissao.isoformat() if nota.data_emissao else None,
        "valor_bruto": _f(bruto),
        "retencoes": retencoes,
        "retido": _f(retido),
        "valor_liquido": _f(liquido),
        "recebimento": _recebimento_do_titulo(s, nota.titulo_id),
        "situacao": nota.situacao,
        "situacao_nome": SITUACAO_ROTULOS.get(nota.situacao, nota.situacao),
        "motivo": nota.motivo or "",
        "codigo_verificacao": nota.codigo_verificacao or "",
        "chave_acesso": nota.chave_acesso or "",
        "substituida_por": nota.substituida_por,
        "observacao": nota.observacao or "",
    }


def listar(s: Session, *, empresa_id: Optional[int] = None,
           obra_id: Optional[int] = None, situacao: str = "",
           ambiente: str = "", serie: str = "",
           desde: Optional[date] = None, ate: Optional[date] = None,
           busca: str = "", limite: int = 500) -> dict[str, Any]:
    """As notas emitidas, do número maior para o menor.

    A ordenação decrescente é pedido dele — quem abre a tela quer a última
    nota, não a primeira de 2019.
    """
    stmt = select(NotaEmitida)
    if empresa_id:
        stmt = stmt.where(NotaEmitida.empresa_id == empresa_id)
    if obra_id:
        stmt = stmt.where(NotaEmitida.obra_id == obra_id)
    if situacao:
        stmt = stmt.where(NotaEmitida.situacao == situacao)
    if ambiente:
        stmt = stmt.where(NotaEmitida.ambiente == ambiente)
    if serie:
        stmt = stmt.where(NotaEmitida.serie == serie)
    if desde:
        stmt = stmt.where(NotaEmitida.data_emissao >= desde)
    if ate:
        stmt = stmt.where(NotaEmitida.data_emissao <= ate)
    stmt = stmt.order_by(NotaEmitida.numero_dps.desc()).limit(limite)

    linhas = [ler(s, n) for n in s.scalars(stmt).all()]

    if busca:
        alvo = busca.strip().lower()
        linhas = [l for l in linhas if alvo in " ".join(str(l[c] or "") for c in (
            "numero_nota", "numero_sp", "medicao", "cliente", "obra", "empresa",
            "chave_acesso", "codigo_verificacao")).lower()]

    # O que a tela mostra em cima. Só EMITIDA entra nos totais: nota cancelada
    # somada com nota válida é como um relatório fiscal começa a mentir.
    validas = [l for l in linhas if l["situacao"] == "EMITIDA"]
    por_tributo = {chave: round(sum(l["retencoes"][chave] for l in validas), 2)
                   for chave, _ in TRIBUTOS}
    return {
        "notas": linhas,
        "resumo": {
            "quantidade": len(linhas),
            "emitidas": len(validas),
            "canceladas": sum(1 for l in linhas if l["situacao"] == "CANCELADA"),
            "reservadas": sum(1 for l in linhas if l["situacao"] == "RESERVADA"),
            "falhadas": sum(1 for l in linhas if l["situacao"] == "FALHADA"),
            "bruto": round(sum(l["valor_bruto"] for l in validas), 2),
            "retido": round(sum(l["retido"] for l in validas), 2),
            "liquido": round(sum(l["valor_liquido"] for l in validas), 2),
            "recebido": round(sum(l["recebimento"]["valor"] for l in validas), 2),
            "por_tributo": por_tributo,
            "em_homologacao": sum(1 for l in linhas if l["ambiente"] == "HOMOLOGACAO"),
        },
        "tributos": [{"chave": c, "nome": n} for c, n in TRIBUTOS],
    }


def registrar_manual(s: Session, *, empresa_id: int, titulo_id: Optional[int],
                     numero_nota: str, emissao: Optional[date] = None,
                     valor_bruto: Optional[Decimal] = None,
                     retencoes: Optional[dict[str, Any]] = None,
                     codigo_verificacao: str = "", chave_acesso: str = "",
                     observacao: str = "", usuario=None) -> NotaEmitida:
    """A nota que saiu PELO PORTAL da prefeitura, registrada aqui.

    Existe por um motivo prático: enquanto a emissão automática não estiver de
    pé para as duas empresas, alguém vai emitir pelo portal — e se isso não for
    registrado, a conferência da numeração acusa buraco e ninguém sabe por quê.
    Registrar é o que mantém a sequência fechada.
    """
    from app.apps.erp.core.notas_emitidas import numeracao

    empresa = s.get(Empresa, empresa_id)
    if empresa is None:
        raise ErroValidacao("Empresa não encontrada.")
    numero_nota = (numero_nota or "").strip()
    if not numero_nota:
        raise ErroValidacao("Informe o número da nota que a prefeitura devolveu.")
    quando = emissao or date.today()
    if quando > date.today():
        raise ErroValidacao("A data de emissão não pode ser no futuro.")

    titulo = s.get(Titulo, titulo_id) if titulo_id else None
    if titulo_id and titulo is None:
        raise ErroValidacao("O título informado não foi encontrado.")

    # A obra sai do RATEIO quando o título tem uma só — é o que liga a nota à
    # obra sem ninguém digitar. Rateado entre várias, fica sem obra: escolher
    # uma seria inventar.
    from app.apps.erp.db.models.financeiro import Rateio
    obra_id = None
    if titulo is not None:
        obras = {r.obra_id for r in s.scalars(
            select(Rateio).where(Rateio.titulo_id == titulo.id)).all() if r.obra_id}
        if len(obras) == 1:
            obra_id = obras.pop()

    bruto = (_dec(valor_bruto) if valor_bruto is not None
             else _dec(getattr(titulo, "valor_liquido", 0)))
    retido_por_tributo = {c: _f(_dec((retencoes or {}).get(c))) for c, _ in TRIBUTOS}
    retido = sum((_dec(v) for v in retido_por_tributo.values()), Decimal(0))

    nota = numeracao.reservar(
        s, empresa, titulo_id=titulo_id, obra_id=obra_id,
        competencia=getattr(titulo, "competencia", None),
        modo="MANUAL", valor_bruto=bruto, valor_liquido=bruto - retido,
        observacao=observacao, usuario=usuario)
    # As retenções vão em `confirmar`, e NÃO gravadas antes: confirmar
    # sobrescreve o campo, e escrever nos dois lugares perderia o valor calado.
    return numeracao.confirmar(s, nota.id, numero_nota=numero_nota,
                               data_emissao=quando,
                               codigo_verificacao=codigo_verificacao,
                               chave_acesso=chave_acesso,
                               retencoes=retido_por_tributo, usuario=usuario)
