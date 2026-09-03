# -*- coding: utf-8 -*-
"""
Conversão entre o jeito brasileiro de escrever e o jeito do banco.

A planilha guarda tudo como texto, no formato que uma pessoa lê: "6.750,00" e
"31/12/2026". O banco precisa de número e de data de verdade, senão não soma
nem ordena — como texto, "10/01/2026" viria antes de "9/01/2026".

Aqui não entra `pandas`, de propósito. O painel provou que dá para fazer este
trabalho em Python puro sem custo perceptível, e `pandas` não é dependência
deste serviço — acrescentá-la para converter datas seria pagar caro por pouco.
"""
from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal, InvalidOperation

# Tudo que não seja dígito, vírgula, ponto ou sinal de menos.
_LIXO = re.compile(r"[^\d,.\-]")


def para_numero(texto) -> Decimal | None:
    """"6.750,00" -> 6750.00. Devolve None quando não há número nenhum.

    None e zero são coisas diferentes e precisam continuar sendo: uma SP sem
    valor preenchido não é uma SP de R$ 0,00. Somar as duas como zero
    esconderia o preenchimento faltando.

    Aceita as formas que aparecem de fato na planilha: com e sem separador de
    milhar, com "R$" na frente, e o negativo entre parênteses do estilo
    contábil — "(1.000,00)" é menos mil."""
    s = str(texto or "").strip()
    if not s:
        return None

    negativo = s.startswith("(") and s.endswith(")")
    s = _LIXO.sub("", s)
    if not s or s in ("-", ".", ","):
        return None

    if "," in s:
        # Vírgula presente: ela é o decimal, e o ponto é separador de milhar.
        s = s.replace(".", "").replace(",", ".")
    # Sem vírgula, o ponto pode ser milhar ("1.234") ou decimal ("1234.56").
    # Três dígitos depois do último ponto e nenhuma vírgula à vista: é milhar.
    elif re.fullmatch(r"-?\d{1,3}(\.\d{3})+", s):
        s = s.replace(".", "")

    try:
        valor = Decimal(s)
    except InvalidOperation:
        return None
    return -valor if negativo and valor > 0 else valor


# Fora desta faixa é erro de digitação na planilha, não data.
#
# Não é regra inventada: a base real tem SPs com ano 202, 204, 260 e 2925 —
# dedo escorregado em "2024", "2025". Aceitar essas datas seria pior do que
# recusá-las. Um vencimento no ano 202 encabeça qualquer lista ordenada por
# data, e um no ano 2925 nunca vence — os dois envenenariam silenciosamente
# todo filtro por período. Recusadas, elas aparecem como data em branco, que é
# visível e cobrável de quem preencheu.
ANO_MINIMO = 2000
ANO_MAXIMO = 2100


def para_data(texto) -> dt.date | None:
    """"31/12/2026" -> data. Devolve None quando não dá para ler.

    Aceita também o formato do banco (2026-12-31) e o ano com dois dígitos, que
    aparece em lançamentos antigos. Data impossível (31/02) devolve None em vez
    de estourar — dado ruim na planilha não pode derrubar a carga inteira."""
    s = str(texto or "").strip()
    if not s:
        return None

    # Fica só o primeiro pedaço. Descarta a hora quando vier junto
    # ("31/12/2026 14:30") e resolve as 1.664 SPs cuja data de autorização foi
    # gravada em duplicidade, separada por quebra de linha
    # ("24/07/2024\n24/07/2024") — provavelmente uma automação que escreveu
    # duas vezes. As duas metades são iguais; vale a primeira.
    s = s.split("T")[0].split()[0] if s.split() else ""
    if not s:
        return None

    for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y", "%d-%m-%Y"):
        try:
            data = dt.datetime.strptime(s, formato).date()
        except ValueError:
            continue
        return data if ANO_MINIMO <= data.year <= ANO_MAXIMO else None
    return None


# ---------------------------------------------------------------------------
# O caminho de volta: do banco para a tela
# ---------------------------------------------------------------------------
def moeda(valor) -> str:
    """1234.5 -> "1.234,50". Sem o "R$", que a tela põe quando quiser."""
    if valor is None:
        return ""
    try:
        n = Decimal(str(valor))
    except (InvalidOperation, ValueError):
        return ""
    inteiro, _, centavos = f"{abs(n):.2f}".partition(".")
    grupos = []
    while len(inteiro) > 3:
        grupos.insert(0, inteiro[-3:])
        inteiro = inteiro[:-3]
    grupos.insert(0, inteiro)
    return ("-" if n < 0 else "") + ".".join(grupos) + "," + centavos


def data_br(valor) -> str:
    """Data do banco -> "31/12/2026". Vazio vira vazio, nunca "None"."""
    if valor is None:
        return ""
    if isinstance(valor, dt.datetime):
        valor = valor.date()
    if isinstance(valor, dt.date):
        return valor.strftime("%d/%m/%Y")
    return str(valor)
