# ============================================================================
# BWS ERP — core/titulos/analise.py
# Motor de análise automática v1 — subconjunto executável dos Blocos A–F da
# especificação, rodando no ato do lançamento. As verificações que dependem
# de captura DFe/SEFAZ entram na fase fiscal, no mesmo formato de crítica.
#
# Saída: registro em `analises` (criticas JSONB, score, resultado) e ajuste
# do status do título:
#   score 0        → AGUARDANDO_APROVACAO (aprovação humana pela alçada)
#   1 ≤ score < 40 → AGUARDANDO_APROVACAO (críticas visíveis ao aprovador)
#   score ≥ 40     → BLOQUEADO (revisão obrigatória do FINANCEIRO)
# ============================================================================
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import registrar_evento
from app.apps.erp.db.models.cadastros import FormaPagamento, StatusConta
from app.apps.erp.db.models.financeiro import Analise, StatusTitulo, Titulo

MOTOR_VERSAO = "1.0"

# severidade → pontos de risco
_PESO = {"ALERTA": 5, "CRITICA": 15, "BLOQUEIA": 40}


def _critica(lista: list, codigo: str, severidade: str, msg: str) -> None:
    lista.append({"codigo": codigo, "severidade": severidade, "msg": msg})


def analisar_titulo(s: Session, t: Titulo, *, criticas_extra: Optional[list[str]] = None,
                    possivel_duplicidade: Optional[str] = None) -> Analise:
    criticas: list[dict] = []

    # ---- pendências herdadas do modo transição (A10/B1)
    for msg in (criticas_extra or []):
        cod = msg.split(":", 1)[0].strip() if ":" in msg else "TRANS"
        _critica(criticas, cod, "CRITICA", msg)

    # ---- C7(d): possível duplicidade credor+valor+vencimento
    if possivel_duplicidade:
        _critica(criticas, "C7d", "CRITICA",
                 f"Possível duplicidade: mesmo credor, mesmo valor e vencimento "
                 f"próximo do título {possivel_duplicidade}. Confirmar antes de aprovar.")

    # ---- E2: vencimento no passado ou muito próximo
    hoje = date.today()
    venc1 = min(p.vencimento for p in t.parcelas)
    if venc1 < hoje:
        _critica(criticas, "E2", "CRITICA",
                 f"1ª parcela vencida em {venc1:%d/%m/%Y} — apurar quem deu causa "
                 f"antes de aceitar encargos (E4).")
    elif venc1 <= hoje + timedelta(days=1):
        _critica(criticas, "E2", "ALERTA",
                 "Vencimento em D+0/D+1 — sem janela para o ciclo de análise.")

    # ---- A6: emissão do documento posterior ao vencimento ou futura
    if t.data_emissao_doc:
        if t.data_emissao_doc > hoje:
            _critica(criticas, "A6", "CRITICA", "Data de emissão do documento no futuro.")
        if t.data_emissao_doc > venc1:
            _critica(criticas, "A6", "ALERTA",
                     "Documento emitido após o vencimento da 1ª parcela — verificar reemissão.")

    # ---- E8: sanidade de valor
    if Decimal(t.valor_liquido) >= Decimal("100000.00"):
        _critica(criticas, "E8", "ALERTA",
                 f"Valor elevado (R$ {t.valor_liquido}) — confirmação reforçada recomendada.")

    # ---- C2/C3: conta do fornecedor
    if t.forma_pagamento in (FormaPagamento.PIX, FormaPagamento.TED):
        conta = None
        if t.fornecedor_conta_id:
            from app.apps.erp.db.models.cadastros import FornecedorConta
            conta = s.get(FornecedorConta, t.fornecedor_conta_id)
        if conta is None:
            _critica(criticas, "C2", "BLOQUEIA",
                     "PIX/TED sem conta homologada selecionada.")
        else:
            if conta.status != StatusConta.HOMOLOGADA:
                _critica(criticas, "C2", "BLOQUEIA",
                         f"Conta do fornecedor com status {conta.status.value} (exigido HOMOLOGADA).")
            if conta.homologada_em and (hoje - conta.homologada_em.date()).days <= 7:
                _critica(criticas, "C2", "ALERTA",
                         "Conta homologada há menos de 7 dias — janela típica do golpe da "
                         "troca de conta; confirmar por canal independente.")

    # ---- D19: dedutibilidade
    if not t.dedutivel:
        custo = (Decimal(t.valor_liquido) * Decimal("0.34")).quantize(Decimal("0.01"))
        _critica(criticas, "D19", "ALERTA",
                 f"Título INDEDUTÍVEL — custo tributário estimado de R$ {custo} "
                 f"(34% IRPJ+CSLL) reportado no dossiê mensal.")

    # ---- consolidação
    score = min(sum(_PESO.get(c["severidade"], 0) for c in criticas), 99)
    if any(c["severidade"] == "BLOQUEIA" for c in criticas) or score >= 40:
        resultado = "BLOQUEADO"
        t.status = StatusTitulo.BLOQUEADO
    else:
        resultado = "APROVACAO_HUMANA"
        t.status = StatusTitulo.AGUARDANDO_APROVACAO
    t.score_risco = score

    analise = Analise(titulo_id=t.id, motor_versao=MOTOR_VERSAO,
                      resultado=resultado, score=score, criticas=criticas)
    s.add(analise)
    registrar_evento(s, "titulo", t.id, "ANALISADO",
                     {"resultado": resultado, "score": score,
                      "criticas": len(criticas)}, None)
    return analise
