# ============================================================================
# ERP — core/suprimentos/pagamento.py
# De "30% + 28/56 dias" para parcelas com data e valor.
#
# A planilha de compras tinha 121 formas de pagamento escritas à mão. Todas
# cabem em duas informações — quanto entra na hora e em quantos dias vencem as
# demais —, e é assim que ficam guardadas (tabela `condicoes_pagamento`).
# Este módulo é quem lê essa regra e devolve as parcelas.
#
# Por que importa: é este cálculo que liga o pedido de compra ao financeiro que
# já existe. Errar aqui é gerar título com vencimento errado.
# ============================================================================
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

from app.apps.erp.core.comum.auditoria import ErroValidacao

CENTAVO = Decimal("0.01")


@dataclass(frozen=True)
class Parcela:
    numero: int
    vencimento: date
    valor: Decimal
    entrada: bool = False

    def __repr__(self) -> str:  # pragma: no cover - só para ler em teste
        marca = " (entrada)" if self.entrada else ""
        return f"<{self.numero}ª {self.vencimento} R$ {self.valor}{marca}>"


def gerar_parcelas(valor_total, data_base: date, entrada_percentual=0,
                   dias: list[int] | None = None) -> list[Parcela]:
    """As parcelas de uma compra, na ordem em que vencem.

    `entrada_percentual` é o que se paga no ato (vencimento na própria
    `data_base`); `dias` são os prazos das demais, contados da data base.
    À vista é entrada de 100% e nenhum prazo.

    O resto da divisão vai para a ÚLTIMA parcela, não para a primeira: assim o
    fornecedor não recebe um centavo a mais logo na entrada, e a soma fecha
    exatamente com o total — que é o que a conciliação vai cobrar depois.
    """
    total = Decimal(str(valor_total)).quantize(CENTAVO, rounding=ROUND_HALF_UP)
    if total <= 0:
        raise ErroValidacao("Valor da compra tem de ser maior que zero.")

    entrada_pct = Decimal(str(entrada_percentual or 0))
    if entrada_pct < 0 or entrada_pct > 100:
        raise ErroValidacao("Entrada tem de estar entre 0% e 100%.")
    prazos = sorted(int(d) for d in (dias or []))
    if any(d < 0 for d in prazos):
        raise ErroValidacao("Prazo de vencimento não pode ser negativo.")
    if not prazos and entrada_pct <= 0:
        raise ErroValidacao(
            "Condição sem entrada e sem prazo não gera parcela nenhuma.")
    if entrada_pct >= 100 and prazos:
        raise ErroValidacao(
            "Entrada de 100% não deixa saldo para as parcelas seguintes.")

    parcelas: list[Parcela] = []
    valor_entrada = Decimal("0")
    if entrada_pct > 0:
        valor_entrada = (total * entrada_pct / 100).quantize(CENTAVO,
                                                             rounding=ROUND_HALF_UP)
        parcelas.append(Parcela(1, data_base, valor_entrada, entrada=True))

    saldo = total - valor_entrada
    if prazos:
        cota = (saldo / len(prazos)).quantize(CENTAVO, rounding=ROUND_HALF_UP)
        for i, dia in enumerate(prazos, start=1):
            parcelas.append(Parcela(len(parcelas) + 1,
                                    data_base + timedelta(days=dia), cota))

    # Fecha o centavo na última: a soma tem de bater com o total, sempre.
    diferenca = total - sum(p.valor for p in parcelas)
    if diferenca:
        ultima = parcelas[-1]
        parcelas[-1] = Parcela(ultima.numero, ultima.vencimento,
                               ultima.valor + diferenca, ultima.entrada)
    return parcelas


def parcelas_da_condicao(condicao, valor_total, data_base: date) -> list[Parcela]:
    """O mesmo, a partir da linha de `condicoes_pagamento`."""
    if condicao is None:
        raise ErroValidacao("Escolha a condição de pagamento.")
    return gerar_parcelas(valor_total, data_base,
                          getattr(condicao, "entrada_percentual", 0) or 0,
                          list(getattr(condicao, "dias", None) or []))


def descrever(entrada_percentual=0, dias: list[int] | None = None) -> str:
    """A condição em palavras, para conferir na tela o que foi cadastrado."""
    entrada = Decimal(str(entrada_percentual or 0))
    prazos = sorted(int(d) for d in (dias or []))
    if entrada >= 100 and not prazos:
        return "à vista"
    partes = []
    if entrada > 0:
        partes.append(f"{entrada.normalize():f}% de entrada".replace(".", ","))
    if prazos:
        partes.append("/".join(str(d) for d in prazos) + " dias")
    return " + ".join(partes)
