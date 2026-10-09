# -*- coding: utf-8 -*-
"""
Conferir e equalizar os TRIBUTOS do título no Omie, a partir da base de notas.

As três operações que o dono quer manter (09/10/2026): **consulta**,
**equalização** e **atualização** dos tributos no Omie. Nada além disso — ele foi
explícito que a dezena de outras funções criadas no Apps Script não é para
sobreviver.

**A direção da verdade é: a NOTA manda, o título obedece.** A nota fiscal não se
desfaz; o título do Omie é registro interno. Então a equalização nunca muda a
nota: ela calcula o que o título DEVERIA ter (a soma dos tributos das notas
válidas daquele título) e, só com autorização explícita, grava isso no Omie.

**Um título cobre VÁRIAS notas** — a medição é faturada em partes. Por isso:

- para *mostrar* quanto do título cabe a cada nota, o valor do título é **rateado**
  proporcionalmente ao valor bruto de cada nota, fechando ao centavo
  (`omie.ratear`);
- para *equalizar*, o que vale é a **soma** das notas do título.

Nota **cancelada fica fora de tudo** — não entra na soma, não recebe rateio. Era
o que o Apps Script já fazia, e é o que mantém a conta certa.
"""
from __future__ import annotations

from decimal import Decimal

import base_faturamento as bfat
import omie

TRIBUTOS = ("pis", "cofins", "csll", "ir", "iss", "inss")


def agrupar_por_titulo(linhas: list[dict]) -> dict:
    """{codigo_integracao: [linhas da base]}. Sem código, a nota fica de fora —
    não há título para conferir, e inventar um seria pior."""
    grupos: dict[str, list] = {}
    for d in linhas:
        codigo = bfat._txt(d.get("omie_codigo_integracao"))
        if not codigo:
            continue
        grupos.setdefault(codigo, []).append(d)
    return grupos


def notas_que_contam(linhas: list[dict]) -> list[dict]:
    """As válidas. Cancelada e substituída ficam fora da soma e do rateio."""
    return [d for d in linhas
            if bfat._txt(d.get("status")) in ("", bfat.STATUS_VALIDA)]


def somar_tributos(linhas: list[dict]) -> dict:
    """O que o título DEVERIA ter: a soma dos tributos das notas válidas.

    Só soma o que foi de fato RETIDO. Somar o valor de um imposto não retido
    inflaria a retenção do título e a baixa sairia errada — que é exatamente o
    problema que o dono descreve quando o líquido não fecha.
    """
    total = {t: Decimal("0") for t in TRIBUTOS}
    for d in notas_que_contam(linhas):
        for t in TRIBUTOS:
            if bfat._txt(d.get(f"retem_{t}")).upper().startswith("S"):
                total[t] += bfat._decimal(d.get(t))
    return total


def ratear_titulo(linhas: list[dict], titulo: dict) -> dict:
    """{nota_numero: {tributo: parte}} — quanto do título cabe a cada nota.

    O peso é o valor bruto da nota, como no Apps Script. Uma nota só leva o
    título inteiro.
    """
    validas = notas_que_contam(linhas)
    if not validas:
        return {}
    pesos = [bfat._decimal(d.get("valor_total")) for d in validas]
    if sum(pesos) <= 0:
        # Sem valor em nenhuma nota não há proporção possível. Dividir igualmente
        # seria inventar um critério; melhor devolver vazio e a tela dizer isso.
        return {}
    partes: dict = {bfat._txt(d.get("nota_numero")): {} for d in validas}
    for t in TRIBUTOS:
        valor = titulo.get(t) or Decimal("0")
        if Decimal(str(valor)) <= 0:
            continue
        for d, parte in zip(validas, omie.ratear(valor, pesos)):
            partes[bfat._txt(d.get("nota_numero"))][t] = parte
    return partes


def aplicar_no_registro(d: dict, titulo: dict, parte: dict | None = None) -> dict:
    """Escreve na linha da base o que o Omie tem, e recalcula as divergências."""
    parte = parte or {}
    for t in TRIBUTOS:
        if t in parte:
            d[f"omie_{t}"] = f"{parte[t]:.2f}"
    d["omie_codigo_lancamento"] = bfat._txt(titulo.get("codigo_lancamento"))
    d["omie_numero_documento"] = bfat._txt(titulo.get("numero_documento"))
    v = titulo.get("valor_titulo")
    if v is not None:
        d["omie_valor_titulo"] = f"{Decimal(str(v)):.2f}"
    d["omie_conferido_em"] = bfat._agora()
    d["divergencia_tributos"] = bfat.conferir_tributos(d)
    d["divergencia_recebimento"] = bfat.conferir_recebimento(d)
    return d


def precisa_equalizar(titulo: dict, desejado: dict, tolerancia="0.01") -> list[str]:
    """Quais tributos do título não batem com a soma das notas.

    Devolve a LISTA dos que divergem, e não um sim/não: a tela mostra quais, e
    gravar no Omie sem dizer o que vai mudar é o tipo de ação que não se faz num
    sistema financeiro.
    """
    fora = []
    for t in TRIBUTOS:
        no_omie = Decimal(str(titulo.get(t) or 0))
        deve = Decimal(str(desejado.get(t) or 0))
        if abs(no_omie - deve) > Decimal(tolerancia):
            fora.append(t)
    return fora
