# ============================================================================
# BWS ERP — core/pagamentos/service.py
# Baixa de pagamentos, importação de extrato (OFX) e conciliação automática.
#
# Conciliação (item 12 da triagem — matching do spsbd adaptado):
#   candidato = lançamento de DÉBITO no extrato, ainda não conciliado, com
#   |valor| == valor pago e data dentro de ±N dias (padrão 3).
#   confiança = 1.0 − 0.1·|Δdias| + bônus de similaridade de nome
#   (difflib entre nome da contraparte no extrato e razão social do credor).
#   AUTO quando candidato ÚNICO com confiança ≥ 0.75; senão vai para a fila
#   de conciliação manual com os candidatos ranqueados.
# ============================================================================
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from difflib import SequenceMatcher
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.core.pagamentos.ofx import LancamentoOFX, extrair_nome_contraparte, parsear_ofx
from app.apps.erp.db.models.cadastros import ContaBancaria, FormaPagamento, PerfilUsuario, Usuario
from app.apps.erp.db.models.financeiro import (
    Conciliacao, Extrato, Pagamento, Parcela, StatusParcela, StatusTitulo, Titulo,
)

_CENT = Decimal("0.01")
JANELA_DIAS_PADRAO = 3
CONFIANCA_AUTO = Decimal("0.750")


# ---------------------------------------------------------------------------
# Baixa de pagamento
# ---------------------------------------------------------------------------
def registrar_pagamento(s: Session, *, parcela_id: int, conta_bancaria_id: int,
                        data_pagamento: date, valor_pago: Any = None,
                        meio: Optional[str] = None, usuario: Optional[Usuario] = None,
                        robo: bool = False,
                        comprovante_anexo_id: Optional[int] = None) -> Pagamento:
    parcela = s.get(Parcela, parcela_id, options=[selectinload(Parcela.titulo)])
    if parcela is None:
        raise ErroValidacao(f"Parcela {parcela_id} não encontrada.")
    if parcela.status == StatusParcela.PAGA:
        raise ErroValidacao(f"Parcela {parcela.numero} do título {parcela.titulo.numero_sp} já está PAGA.")
    if parcela.status == StatusParcela.CANCELADA:
        raise ErroValidacao("Parcela cancelada não recebe pagamento.")
    titulo: Titulo = parcela.titulo
    if titulo.status not in (StatusTitulo.APROVADO, StatusTitulo.PAGO_PARCIAL):
        raise ErroValidacao(
            f"Título {titulo.numero_sp} está {titulo.status.value} — pagamento exige "
            f"APROVADO (segregação: análise/aprovação antes do caixa).")

    conta = s.get(ContaBancaria, conta_bancaria_id)
    if conta is None or not conta.ativo:
        raise ErroValidacao("Conta bancária da empresa inexistente ou inativa.")

    if valor_pago in (None, "", "None"):
        valor = Decimal(parcela.valor).quantize(_CENT)   # baixa pelo valor da parcela
    else:
        valor = Decimal(str(valor_pago).replace(",", ".")).quantize(_CENT)
    if valor <= 0:
        raise ErroValidacao("Valor pago deve ser maior que zero.")
    if abs(valor - Decimal(parcela.valor)) > Decimal("0.01"):
        raise ErroValidacao(
            f"Valor pago (R$ {valor}) difere da parcela (R$ {parcela.valor}). "
            f"Diferenças (juros/desconto) exigem estorno+relançamento — imutabilidade contábil.")

    meio_pg = FormaPagamento(meio.upper()) if meio else titulo.forma_pagamento

    pg = Pagamento(parcela_id=parcela.id, conta_bancaria_id=conta.id,
                   data_pagamento=data_pagamento, valor_pago=valor, meio=meio_pg,
                   comprovante_anexo_id=comprovante_anexo_id,
                   executado_por=(usuario.id if usuario else None),
                   executado_por_robo=robo)
    s.add(pg)
    parcela.status = StatusParcela.PAGA

    abertas = [p for p in titulo.parcelas if p.id != parcela.id
               and p.status in (StatusParcela.ABERTA, StatusParcela.AGENDADA)]
    titulo.status = StatusTitulo.PAGO_PARCIAL if abertas else StatusTitulo.PAGO
    s.flush()

    registrar_evento(s, "pagamento", pg.id, "REGISTRADO", {
        "titulo": titulo.numero_sp, "parcela": parcela.numero,
        "valor": str(valor), "data": data_pagamento.isoformat(),
        "meio": meio_pg.value, "robo": robo,
    }, usuario.id if usuario else None)
    return pg


# ---------------------------------------------------------------------------
# Importação de extrato OFX
# ---------------------------------------------------------------------------
def importar_ofx(s: Session, conteudo: bytes, conta_bancaria_id: int,
                 usuario: Optional[Usuario] = None) -> dict[str, int]:
    conta = s.get(ContaBancaria, conta_bancaria_id)
    if conta is None:
        raise ErroValidacao("Conta bancária inexistente.")
    lancs = parsear_ofx(conteudo, conta_bancaria_id)

    existentes = set(s.scalars(select(Extrato.hash_linha).where(
        Extrato.hash_linha.in_([l.hash_linha for l in lancs]))).all())

    novos = 0
    for l in lancs:
        if l.hash_linha in existentes:
            continue
        s.add(Extrato(conta_bancaria_id=conta_bancaria_id, data_lancamento=l.data,
                      valor=l.valor, historico=(l.memo or l.tipo)[:500],
                      documento=l.documento,
                      nome_contraparte=extrair_nome_contraparte(l),
                      hash_linha=l.hash_linha))
        novos += 1
    s.flush()
    registrar_evento(s, "extrato", conta_bancaria_id, "OFX_IMPORTADO",
                     {"transacoes_no_arquivo": len(lancs), "novas": novos,
                      "ja_existentes": len(lancs) - novos},
                     usuario.id if usuario else None)
    return {"no_arquivo": len(lancs), "novas": novos, "duplicadas": len(lancs) - novos}


# ---------------------------------------------------------------------------
# Conciliação automática
# ---------------------------------------------------------------------------
def _similaridade(a: Optional[str], b: Optional[str]) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.upper().strip(), b.upper().strip()).ratio()


def _candidatos(s: Session, pg: Pagamento, credor_nome: str,
                janela_dias: int) -> list[tuple[Extrato, Decimal]]:
    ini = pg.data_pagamento - timedelta(days=janela_dias)
    fim = pg.data_pagamento + timedelta(days=janela_dias)
    ja_conciliados = select(Conciliacao.extrato_id).where(Conciliacao.desfeita_em.is_(None))
    exts = s.scalars(select(Extrato).where(
        Extrato.conta_bancaria_id == pg.conta_bancaria_id,
        Extrato.valor == -Decimal(pg.valor_pago),
        Extrato.data_lancamento.between(ini, fim),
        Extrato.id.not_in(ja_conciliados))).all()
    ranqueados = []
    for e in exts:
        delta = abs((e.data_lancamento - pg.data_pagamento).days)
        conf = Decimal("1.0") - Decimal(delta) * Decimal("0.1")
        sim = _similaridade(e.nome_contraparte, credor_nome)
        if sim >= 0.55:
            conf += Decimal("0.10")
        elif e.nome_contraparte and sim < 0.30:
            conf -= Decimal("0.20")     # nome diverge: cautela
        ranqueados.append((e, max(conf, Decimal("0")).quantize(Decimal("0.001"))))
    ranqueados.sort(key=lambda x: x[1], reverse=True)
    return ranqueados


def conciliar_automatico(s: Session, *, janela_dias: int = JANELA_DIAS_PADRAO,
                         usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Percorre pagamentos sem conciliação e casa com o extrato.
    Retorna resumo + pendências com candidatos para a tela manual."""
    ja = select(Conciliacao.pagamento_id).where(Conciliacao.desfeita_em.is_(None))
    pendentes = s.scalars(
        select(Pagamento).where(Pagamento.id.not_in(ja),
                                Pagamento.estorna_pagamento_id.is_(None))
        .options(selectinload(Pagamento.parcela).selectinload(Parcela.titulo)
                 .selectinload(Titulo.fornecedor))).all()

    conciliados, fila_manual = 0, []
    for pg in pendentes:
        credor = pg.parcela.titulo.fornecedor.razao_social
        cands = _candidatos(s, pg, credor, janela_dias)
        if len(cands) == 1 and cands[0][1] >= CONFIANCA_AUTO:
            ext, conf = cands[0]
            s.add(Conciliacao(pagamento_id=pg.id, extrato_id=ext.id,
                              metodo="AUTO_VALOR_DATA_NOME", confianca=conf,
                              conciliado_por=(usuario.id if usuario else None)))
            conciliados += 1
        else:
            fila_manual.append({
                "pagamento_id": pg.id,
                "titulo": pg.parcela.titulo.numero_sp,
                "credor": credor,
                "valor": str(pg.valor_pago),
                "data": pg.data_pagamento.isoformat(),
                "candidatos": [{"extrato_id": e.id, "data": e.data_lancamento.isoformat(),
                                "historico": (e.historico or "")[:80],
                                "nome": e.nome_contraparte,
                                "confianca": str(c)} for e, c in cands[:5]],
            })
    s.flush()
    if conciliados:
        registrar_evento(s, "conciliacao", 0, "AUTO_EXECUTADA",
                         {"conciliados": conciliados, "pendentes_manuais": len(fila_manual)},
                         usuario.id if usuario else None)
    return {"conciliados_auto": conciliados, "pendentes": fila_manual}


def conciliar_manual(s: Session, pagamento_id: int, extrato_id: int,
                     usuario: Usuario) -> Conciliacao:
    pg = s.get(Pagamento, pagamento_id)
    ext = s.get(Extrato, extrato_id)
    if pg is None or ext is None:
        raise ErroValidacao("Pagamento ou extrato inexistente.")
    if abs(Decimal(ext.valor) + Decimal(pg.valor_pago)) > Decimal("0.01"):
        raise ErroValidacao(
            f"Valores não conferem: extrato R$ {ext.valor} × pagamento R$ {pg.valor_pago}. "
            f"Conciliação manual não força divergência de valor.")
    c = Conciliacao(pagamento_id=pg.id, extrato_id=ext.id, metodo="MANUAL",
                    confianca=None, conciliado_por=usuario.id)
    s.add(c)
    s.flush()
    registrar_evento(s, "conciliacao", c.id, "MANUAL",
                     {"pagamento_id": pg.id, "extrato_id": ext.id}, usuario.id)
    return c


def desfazer_conciliacao(s: Session, conciliacao_id: int, motivo: str,
                         usuario: Usuario) -> Conciliacao:
    if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
        raise ErroPermissao("Desfazer conciliação restrito a FINANCEIRO/ADMIN.")
    c = s.get(Conciliacao, conciliacao_id)
    if c is None:
        raise ErroValidacao("Conciliação inexistente.")
    if c.desfeita_em is not None:
        return c
    motivo = (motivo or "").strip()
    if len(motivo) < 5:
        raise ErroValidacao("Informe o motivo.")
    c.desfeita_em = datetime.now(timezone.utc)
    registrar_evento(s, "conciliacao", c.id, "DESFEITA", {"motivo": motivo}, usuario.id)
    return c


def extratos_nao_conciliados(s: Session, conta_bancaria_id: Optional[int] = None,
                             apenas_debitos: bool = True, limite: int = 500) -> list[Extrato]:
    ja = select(Conciliacao.extrato_id).where(Conciliacao.desfeita_em.is_(None))
    stmt = select(Extrato).where(Extrato.id.not_in(ja)).order_by(
        Extrato.data_lancamento.desc()).limit(limite)
    if conta_bancaria_id:
        stmt = stmt.where(Extrato.conta_bancaria_id == conta_bancaria_id)
    if apenas_debitos:
        stmt = stmt.where(Extrato.valor < 0)
    return list(s.scalars(stmt).all())
