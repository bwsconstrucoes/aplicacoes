# ============================================================================
# ERP — core/comum/formato.py
# Número e dinheiro escritos como se lê em português.
#
# POR QUE EXISTE UM MÓDULO SÓ PARA ISSO. Estas duas funções nasceram dentro do
# relatório de cotação, e logo o alerta da locação precisou das mesmas: "R$
# 576.00 de aluguel depois do combinado" é o computador falando, não o ERP.
# Copiar seria pior — duas cópias divergem, e aí o mesmo valor sai de dois
# jeitos em duas telas. Fica aqui, e quem precisar importa.
#
# Quem escreve para TELA em JavaScript tem o par delas em erp_base.html
# (`moeda` e `numero`). Estas aqui são para o que o servidor monta: relatório,
# PDF, mensagem de alerta, texto de e-mail.
# ============================================================================
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


def _dinheiro_br(valor: Any) -> str:
    """4155.68 → "4.155,68". O relatório é lido por gente e vai para o papel:
    ponto decimal e quatro casas ("35.9000") não se lê em português."""
    if valor in (None, ""):
        return ""
    try:
        d = Decimal(str(valor))
    except (InvalidOperation, ValueError):
        return str(valor)
    # duas casas por padrão; mantém mais só quando o preço realmente as tem
    # (item barato cotado a 0,1250 existe e arredondar mentiria)
    texto = f"{d:.4f}".rstrip("0")
    casas = max(2, len(texto.split(".")[1]) if "." in texto else 0)
    d = d.quantize(Decimal("1." + "0" * casas), rounding=ROUND_HALF_UP)
    inteiro, _, decimais = f"{d:.{casas}f}".partition(".")
    negativo = inteiro.startswith("-")
    inteiro = inteiro.lstrip("-")
    grupos = []
    while len(inteiro) > 3:
        grupos.insert(0, inteiro[-3:])
        inteiro = inteiro[:-3]
    grupos.insert(0, inteiro)
    return ("-" if negativo else "") + ".".join(grupos) + "," + decimais


def _quantidade_br(valor: Any) -> str:
    """"14.000" no banco é catorze, não catorze mil — a casa decimal só
    aparece quando existe de verdade."""
    if valor in (None, ""):
        return ""
    try:
        d = Decimal(str(valor))
    except (InvalidOperation, ValueError):
        return str(valor)
    if d == d.to_integral_value():
        return str(int(d))
    return f"{d.normalize():f}".replace(".", ",")
