# ============================================================================
# ERP — core/comum/ia_custo.py
# Registro do consumo de IA.
#
# A resposta da OpenAI traz quantos tokens foram usados; guardando isso com o
# preço do modelo, sabe-se quanto custou cada leitura. Sem esse número, a
# decisão de usar mais ou menos IA vira palpite.
#
# Os preços são por milhão de tokens, em dólar, e ficam aqui para poderem ser
# atualizados sem tocar no resto do código.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# US$ por milhão de tokens (entrada, saída) — conferir na página de preços
PRECOS = {
    "gpt-4o-mini": (Decimal("0.15"), Decimal("0.60")),
    "gpt-4o": (Decimal("2.50"), Decimal("10.00")),
    "gpt-4.1-mini": (Decimal("0.40"), Decimal("1.60")),
    "gpt-4.1": (Decimal("2.00"), Decimal("8.00")),
}
PRECO_PADRAO = (Decimal("0.50"), Decimal("2.00"))
_MILHAO = Decimal("1000000")


def custo(modelo: str, entrada: int, saida: int) -> Decimal:
    p_in, p_out = PRECOS.get((modelo or "").lower(), PRECO_PADRAO)
    return ((Decimal(entrada) * p_in + Decimal(saida) * p_out) / _MILHAO).quantize(
        Decimal("0.000001"))


def registrar(s: Session, *, modelo: str, operacao: str, resposta: Any = None,
              duracao_ms: Optional[int] = None, usuario_id: Optional[int] = None,
              referencia: str = "", sucesso: bool = True, erro: str = "") -> None:
    """Grava o consumo de uma chamada. Nunca derruba a operação principal."""
    from app.apps.erp.db.models.financeiro import IaUso
    try:
        entrada = saida = 0
        uso = getattr(resposta, "usage", None) if resposta is not None else None
        if uso is not None:
            entrada = int(getattr(uso, "prompt_tokens", 0) or
                          getattr(uso, "input_tokens", 0) or 0)
            saida = int(getattr(uso, "completion_tokens", 0) or
                        getattr(uso, "output_tokens", 0) or 0)
        s.add(IaUso(modelo=modelo or "?", operacao=operacao,
                    tokens_entrada=entrada, tokens_saida=saida,
                    custo_usd=custo(modelo, entrada, saida),
                    duracao_ms=duracao_ms, sucesso=sucesso,
                    erro=(erro or "")[:400] or None,
                    usuario_id=usuario_id, referencia=(referencia or "")[:120] or None))
        s.flush()
    except Exception as e:                      # registro não pode quebrar nada
        logger.warning("ERP/IA: não foi possível registrar o consumo (%s)", e)


def painel(s: Session, dias: int = 90) -> dict[str, Any]:
    """Quanto se gastou, em que, por quem e com qual modelo."""
    from app.apps.erp.db.models.cadastros import Usuario
    from app.apps.erp.db.models.financeiro import IaUso

    desde = datetime.now(timezone.utc) - timedelta(days=dias)
    base = select(IaUso).where(IaUso.criado_em >= desde)

    total = s.execute(select(
        func.count(IaUso.id), func.coalesce(func.sum(IaUso.custo_usd), 0),
        func.coalesce(func.sum(IaUso.tokens_entrada + IaUso.tokens_saida), 0),
        func.coalesce(func.avg(IaUso.duracao_ms), 0),
        func.count(IaUso.id).filter(IaUso.sucesso.is_(False)),
    ).where(IaUso.criado_em >= desde)).first()

    por_operacao = [{
        "operacao": op, "chamadas": n, "custo": float(c or 0),
        "tokens": int(t or 0),
        "custo_medio": float((c or 0) / n) if n else 0.0,
    } for op, n, c, t in s.execute(
        select(IaUso.operacao, func.count(IaUso.id),
               func.sum(IaUso.custo_usd),
               func.sum(IaUso.tokens_entrada + IaUso.tokens_saida))
        .where(IaUso.criado_em >= desde)
        .group_by(IaUso.operacao).order_by(func.sum(IaUso.custo_usd).desc())).all()]

    por_mes = [{
        "mes": m.strftime("%m/%Y") if m else "—", "chamadas": n,
        "custo": float(c or 0),
    } for m, n, c in s.execute(
        select(func.date_trunc("month", IaUso.criado_em).label("m"),
               func.count(IaUso.id), func.sum(IaUso.custo_usd))
        .where(IaUso.criado_em >= desde)
        .group_by("m").order_by("m")).all()]

    por_modelo = [{
        "modelo": mod, "chamadas": n, "custo": float(c or 0),
    } for mod, n, c in s.execute(
        select(IaUso.modelo, func.count(IaUso.id), func.sum(IaUso.custo_usd))
        .where(IaUso.criado_em >= desde)
        .group_by(IaUso.modelo).order_by(func.sum(IaUso.custo_usd).desc())).all()]

    por_pessoa = [{
        "pessoa": nome or "—", "chamadas": n, "custo": float(c or 0),
    } for nome, n, c in s.execute(
        select(Usuario.nome, func.count(IaUso.id), func.sum(IaUso.custo_usd))
        .join(Usuario, Usuario.id == IaUso.usuario_id, isouter=True)
        .where(IaUso.criado_em >= desde)
        .group_by(Usuario.nome).order_by(func.sum(IaUso.custo_usd).desc()).limit(15)).all()]

    hoje = date.today()
    mes_atual = s.execute(select(func.coalesce(func.sum(IaUso.custo_usd), 0))
                          .where(IaUso.criado_em >= datetime(hoje.year, hoje.month, 1,
                                                             tzinfo=timezone.utc))).scalar()
    dias_corridos = max(1, hoje.day)
    projecao = float(mes_atual or 0) / dias_corridos * 30

    return {
        "periodo_dias": dias,
        "chamadas": total[0] or 0, "custo_total": float(total[1] or 0),
        "tokens": int(total[2] or 0), "duracao_media_ms": int(total[3] or 0),
        "falhas": total[4] or 0,
        "custo_mes_atual": float(mes_atual or 0),
        "projecao_mes": round(projecao, 2),
        "por_operacao": por_operacao, "por_mes": por_mes,
        "por_modelo": por_modelo, "por_pessoa": por_pessoa,
        "precos": {m: [float(a), float(b)] for m, (a, b) in PRECOS.items()},
    }
