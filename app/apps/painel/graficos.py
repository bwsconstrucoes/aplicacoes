# -*- coding: utf-8 -*-
"""
Geometria dos gráficos.

As telas desenham SVG na própria página, sem biblioteca de gráfico. Motivos:
o serviço tem 2 GB de memória dividida com 14 módulos, e o Plotly do painel
antigo pesava mais de 3 MB só de JavaScript por abertura de tela. Barras e uma
linha resolvem tudo que estas telas precisam.

Este módulo faz só a conta: recebe os valores e devolve as coordenadas prontas.
Assim a matemática do desenho fica testável, fora do HTML.
"""
from __future__ import annotations

LARGURA = 900
ALTURA = 320
MARGEM_ESQ = 78
MARGEM_DIR = 12
MARGEM_TOPO = 14
MARGEM_BASE = 30


def _passo_bonito(bruto: float) -> float:
    """Escolhe um passo redondo (1, 2, 5 × potência de 10) para a régua."""
    if bruto <= 0:
        return 1.0
    import math
    expoente = math.floor(math.log10(bruto))
    base = 10 ** expoente
    for multiplo in (1, 2, 5, 10):
        if bruto <= multiplo * base:
            return multiplo * base
    return 10 * base


def _rotulo_curto(v: float) -> str:
    """Valor em escala legível: 1,2 mi / 340 mil / 850."""
    sinal = "−" if v < 0 else ""
    a = abs(v)
    if a >= 1_000_000:
        return f"{sinal}{a/1_000_000:,.1f} mi".replace(".", ",")
    if a >= 1_000:
        return f"{sinal}{a/1_000:,.0f} mil".replace(",", ".")
    return f"{sinal}{a:,.0f}".replace(",", ".")


def eixo_vertical(valores, divisoes: int = 5) -> dict:
    """A régua da esquerda: onde ficam as linhas de grade e o zero.

    Sempre inclui o zero, senão uma barra negativa não tem de onde partir."""
    limpos = [float(v) for v in valores if v is not None]
    maximo = max(limpos + [0.0])
    minimo = min(limpos + [0.0])
    if maximo == minimo:
        maximo = maximo + 1.0
    passo = _passo_bonito((maximo - minimo) / divisoes)
    topo = passo * (int(maximo / passo) + (1 if maximo % passo else 0))
    base = passo * (int(minimo / passo) - (1 if minimo % passo else 0))
    if topo == base:
        topo = base + passo

    altura_util = ALTURA - MARGEM_TOPO - MARGEM_BASE

    def y(valor: float) -> float:
        return MARGEM_TOPO + (topo - float(valor)) / (topo - base) * altura_util

    marcas = []
    atual = base
    while atual <= topo + passo / 2:
        marcas.append({"valor": atual, "y": round(y(atual), 2),
                       "rotulo": _rotulo_curto(atual)})
        atual += passo
    return {"y": y, "marcas": marcas, "y_zero": round(y(0.0), 2),
            "topo": topo, "base": base}


# Largura aproximada de um rotulo do eixo, em px do desenho. "06/2025" e o pior
# caso corrente; sobra folga para "2025" nao ficar espremido.
LARGURA_ROTULO = 54


def _passo_de_rotulo(itens, largura_util: float, campo_rotulo: str) -> int:
    """De quantos em quantos periodos escrever o rotulo do eixo.

    Mede pelo rotulo MAIS LONGO da serie: um eixo de anos ("2025") comporta o
    dobro de marcas de um eixo de meses ("06/2025"), e fixar um numero so
    deixaria um dos dois errado.
    """
    if not itens:
        return 1
    mais_longo = max((len(str(i.get(campo_rotulo, ""))) for i in itens), default=4)
    largura = max(mais_longo * 7.5 + 10, 24)          # ~7,5px por caractere
    cabem = max(int(largura_util // largura), 1)
    return max(-(-len(itens) // cabem), 1)            # divisao para cima


def barras_agrupadas(itens, campos, campo_rotulo="ano", campo_linha=None) -> dict:
    """Monta um gráfico de barras lado a lado, com uma linha por cima.

    `itens`  — lista de dicionários, um por período.
    `campos` — [(chave, classe_css, nome_na_legenda), ...] das barras.
    `campo_linha` — chave opcional desenhada como linha (o resultado, o acumulado).

    Devolve tudo pronto para o template: retângulos, pontos e a régua.
    """
    if not itens:
        return {"vazio": True, "largura": LARGURA, "altura": ALTURA}

    todos = []
    for item in itens:
        todos.extend(float(item.get(chave) or 0) for chave, _, _ in campos)
        if campo_linha:
            todos.append(float(item.get(campo_linha) or 0))
    eixo = eixo_vertical(todos)

    largura_util = LARGURA - MARGEM_ESQ - MARGEM_DIR
    passo_x = largura_util / len(itens)
    # 72% da fatia vira barra; o resto é respiro entre os períodos
    largura_grupo = passo_x * 0.72
    largura_barra = largura_grupo / max(len(campos), 1)

    # Quantos rotulos cabem sem um encostar no outro. Com 6 anos de historia
    # sao 70 meses no eixo, e "06/2025" ocupa uns 48px: escrever todos vira uma
    # tarja preta ilegivel, que foi exatamente o que aconteceu no Fluxo
    # Financeiro. Escreve-se um a cada N, sempre incluindo o ultimo, que e o
    # mes em que a pessoa esta.
    passo_rotulo = _passo_de_rotulo(itens, largura_util, campo_rotulo)

    barras, rotulos_x, pontos = [], [], []
    for i, item in enumerate(itens):
        centro = MARGEM_ESQ + passo_x * (i + 0.5)
        if i % passo_rotulo == 0 or i == len(itens) - 1:
            rotulos_x.append({"x": round(centro, 2),
                              "texto": str(item.get(campo_rotulo, ""))})
        for j, (chave, classe, _nome) in enumerate(campos):
            valor = float(item.get(chave) or 0)
            y_valor = eixo["y"](valor)
            topo = min(y_valor, eixo["y_zero"])
            altura = abs(y_valor - eixo["y_zero"])
            barras.append({
                "x": round(centro - largura_grupo / 2 + j * largura_barra, 2),
                "y": round(topo, 2),
                "largura": round(max(largura_barra - 2, 1), 2),
                "altura": round(max(altura, 0.5), 2),
                "classe": classe,
                "titulo": f"{item.get(campo_rotulo, '')}: {_moeda(valor)}",
            })
        if campo_linha:
            valor = float(item.get(campo_linha) or 0)
            pontos.append({"x": round(centro, 2), "y": round(eixo["y"](valor), 2),
                           "titulo": f"{item.get(campo_rotulo, '')}: {_moeda(valor)}"})

    caminho = ""
    if pontos:
        caminho = "M " + " L ".join(f"{p['x']},{p['y']}" for p in pontos)

    return {
        "vazio": False, "largura": LARGURA, "altura": ALTURA,
        "margem_esq": MARGEM_ESQ, "margem_dir": MARGEM_DIR,
        "barras": barras, "rotulos_x": rotulos_x, "pontos": pontos,
        "caminho": caminho, "marcas": eixo["marcas"], "y_zero": eixo["y_zero"],
        "legenda": [{"classe": classe, "nome": nome} for _, classe, nome in campos],
    }


def _moeda(v: float) -> str:
    texto = f"{abs(float(v)):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("−R$ " if v < 0 else "R$ ") + texto


def proporcoes(itens, campo="valor"):
    """Largura relativa de cada barra horizontal num ranking (0 a 100)."""
    if not itens:
        return itens
    maior = max(abs(float(i.get(campo) or 0)) for i in itens) or 1.0
    for item in itens:
        item["pct"] = round(abs(float(item.get(campo) or 0)) / maior * 100, 1)
    return itens


def linhas_com_barras(itens, series, barras=None, campo_rotulo="rotulo",
                      max_rotulos=14) -> dict:
    """Várias linhas sobre barras — o gráfico da Necessidade de Caixa.

    `series` — [(chave, cor_css, nome), ...] desenhadas como linha.
    `barras` — (chave, classe, nome) opcional, desenhada atrás das linhas.

    Diferente de `barras_agrupadas`, aqui as linhas é que são o assunto: o
    acumulado de cada conjunto ao longo do tempo. As barras são o contexto
    (quando entrou empréstimo).
    """
    if not itens:
        return {"vazio": True, "largura": LARGURA, "altura": ALTURA}

    todos = []
    for item in itens:
        todos.extend(float(item.get(chave) or 0) for chave, _, _ in series)
        if barras:
            todos.append(float(item.get(barras[0]) or 0))
    eixo = eixo_vertical(todos)

    largura_util = LARGURA - MARGEM_ESQ - MARGEM_DIR
    passo_x = largura_util / len(itens)

    def _x(i):
        return MARGEM_ESQ + passo_x * (i + 0.5)

    desenhos = []
    for chave, cor, nome in series:
        pontos = [(round(_x(i), 2), round(eixo["y"](float(item.get(chave) or 0)), 2))
                  for i, item in enumerate(itens)]
        desenhos.append({
            "nome": nome, "cor": cor,
            "caminho": "M " + " L ".join(f"{x},{y}" for x, y in pontos),
            # um ponto a cada N para o gráfico não virar um colar de bolinhas
            "pontos": [{"x": x, "y": y} for j, (x, y) in enumerate(pontos)
                       if len(pontos) <= 24 or j % max(len(pontos) // 24, 1) == 0],
        })

    retangulos = []
    if barras:
        chave, classe, _nome = barras
        largura_barra = max(passo_x * 0.55, 1.5)
        for i, item in enumerate(itens):
            valor = float(item.get(chave) or 0)
            if abs(valor) < 0.005:
                continue
            y_valor = eixo["y"](valor)
            retangulos.append({
                "x": round(_x(i) - largura_barra / 2, 2),
                "y": round(min(y_valor, eixo["y_zero"]), 2),
                "largura": round(largura_barra, 2),
                "altura": round(max(abs(y_valor - eixo["y_zero"]), 0.5), 2),
                "classe": classe,
                "titulo": f"{item.get(campo_rotulo, '')}: {_moeda(valor)}",
            })

    # com 120 meses os rótulos se sobrepõem; mostra um a cada N
    passo_rotulo = max(len(itens) // max_rotulos, 1)
    rotulos_x = [{"x": round(_x(i), 2), "texto": str(item.get(campo_rotulo, ""))}
                 for i, item in enumerate(itens)
                 if i % passo_rotulo == 0 or i == len(itens) - 1]

    return {
        "vazio": False, "largura": LARGURA, "altura": ALTURA,
        "margem_esq": MARGEM_ESQ, "margem_dir": MARGEM_DIR,
        "series": desenhos, "barras": retangulos, "rotulos_x": rotulos_x,
        "marcas": eixo["marcas"], "y_zero": eixo["y_zero"],
    }
