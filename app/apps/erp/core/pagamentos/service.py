# ============================================================================
# BWS ERP — core/pagamentos/service.py
# Baixa de pagamentos e importação de extrato (OFX).
#
# O CASAMENTO do extrato com a baixa NÃO mora aqui — mora em
# `conciliacao.py`, e só lá. Ver a nota na seção "Conciliação", mais abaixo.
# ============================================================================
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.core.pagamentos.ofx import extrair_nome_contraparte, parsear_ofx
from app.apps.erp.db.models.cadastros import ContaBancaria, FormaPagamento, PerfilUsuario, Usuario
from app.apps.erp.db.models.financeiro import (
    Conciliacao, Extrato, Pagamento, Parcela, StatusParcela, StatusTitulo, Titulo,
)

_CENT = Decimal("0.01")


# ---------------------------------------------------------------------------
# Baixa de pagamento
# ---------------------------------------------------------------------------
def registrar_pagamento(s: Session, *, parcela_id: int, conta_bancaria_id: int,
                        data_pagamento: date, valor_pago: Any = None,
                        meio: Optional[str] = None, usuario: Optional[Usuario] = None,
                        robo: bool = False,
                        comprovante_anexo_id: Optional[int] = None) -> Pagamento:
    # TRAVA DE LINHA: duas pessoas clicando "baixar" na mesma parcela no mesmo
    # instante liam as duas o status ABERTA e gravavam DOIS pagamentos — o
    # dinheiro sairia uma vez e o ERP registraria duas. Com FOR UPDATE a
    # segunda espera a primeira terminar e aí encontra a parcela PAGA. A
    # segunda linha de defesa é a restrição única da migração 062, no banco,
    # para o caso de um caminho novo esquecer a trava.
    parcela = s.get(Parcela, parcela_id, options=[selectinload(Parcela.titulo)],
                    with_for_update=True, populate_existing=True)
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
# Conciliação
#
# A conciliação MORA EM `conciliacao.py`, e só lá. Até 11/09/2026 existia aqui
# uma SEGUNDA implementação de "conciliar automático" e "conciliar manual",
# mais antiga, que nenhuma tela chamava e que já divergia da de verdade (a
# daqui nem conferia se o extrato era da mesma conta bancária do pagamento).
# Duas versões da escrita mais sensível do sistema é como a errada acaba
# ligada num botão algum dia — foram apagadas na varredura adversarial.
#
# O que continua aqui é o que não é escrita de casamento: desfazer (que muda
# uma conciliação existente) e a lista de linhas do extrato ainda livres.
# ---------------------------------------------------------------------------
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
