# -*- coding: utf-8 -*-
"""
Necessidade de Caixa — a conta da simulação.

A pergunta que esta tela responde: **um conjunto de obras se paga sozinho, ou
foi o resto da empresa (ou o banco) que segurou?**

Você monta o conjunto A escolhendo obras ou projetos e o quanto de cada um
entra. O "resto" é tudo que sobrou — inclusive a fração das obras que entraram
parcialmente em A. E a linha "empresa inteira" é fixa: não muda com a
simulação, porque é a régua contra a qual você compara.

A conta mora aqui, fora do HTML e fora do SQL, para poder ser conferida sozinha.
Ela não sabe de banco nem de tela: recebe listas de números e devolve listas de
números.

Uma ressalva que a tela repete e que não é detalhe: **o acumulado parte de zero
no primeiro mês com dado**. Se a empresa tinha caixa antes disso, todas as
linhas sobem esse valor. É para isso que serve o campo "saldo inicial".
"""
from __future__ import annotations

import datetime as dt


def meses_do_periodo(primeiro: dt.date, ultimo: dt.date) -> list[dt.date]:
    """Todos os meses entre os dois, inclusive — mesmo os sem movimento.

    Mês vazio no meio não pode virar buraco no gráfico: o acumulado tem de
    continuar reto, senão parece que o caixa saltou."""
    meses, atual = [], dt.date(primeiro.year, primeiro.month, 1)
    fim = dt.date(ultimo.year, ultimo.month, 1)
    while atual <= fim:
        meses.append(atual)
        atual = (dt.date(atual.year + 1, 1, 1) if atual.month == 12
                 else dt.date(atual.year, atual.month + 1, 1))
    return meses


def pesos_do_conjunto(escolhas, obra_para_projeto: dict, obras: list[str]) -> dict:
    """Traduz as escolhas da tela em quanto de cada obra entra (0 a 1).

    `escolhas` são pares (item, percentual), onde o item é uma obra
    ("obra:CASA") ou um projeto ("projeto:ALFA") — projeto se abre em todas as
    suas obras. A mesma obra em duas linhas soma, com teto de 100%: pedir 60% e
    depois 70% da mesma obra dá 100%, não 130%."""
    pesos = {obra: 0.0 for obra in obras}
    for item, percentual in escolhas:
        try:
            fracao = float(percentual) / 100.0
        except (TypeError, ValueError):
            fracao = 1.0
        if fracao <= 0 or not item:
            continue
        if item.startswith("projeto:"):
            alvo = item[len("projeto:"):]
            for obra in obras:
                if obra_para_projeto.get(obra, "") == alvo:
                    pesos[obra] += fracao
        elif item.startswith("obra:"):
            obra = item[len("obra:"):]
            if obra in pesos:
                pesos[obra] += fracao
    return {obra: min(max(p, 0.0), 1.0) for obra, p in pesos.items()}


def _acumular(serie_mensal: dict, meses: list[dt.date]) -> list[float]:
    total, saida = 0.0, []
    for mes in meses:
        total += serie_mensal.get(mes, 0.0)
        saida.append(round(total, 2))
    return saida


def simular(linhas_por_obra, financeiro, escolhas_a, mapa_projeto,
            saldo_inicial: float = 0.0, incluir_aportes: bool = True,
            desde: dt.date | None = None) -> dict:
    """A simulação inteira.

    `linhas_por_obra` — (mês, obra, valor de caixa no mês).
    `financeiro`      — dicionários por mês com empréstimo, aporte e dividendo.
    `escolhas_a`      — o conjunto A, como (item, percentual).

    Devolve as séries acumuladas prontas para o gráfico e para a leitura.
    """
    if not linhas_por_obra and not financeiro:
        return {"vazio": True}

    todas_as_datas = ([m for m, _o, _v in linhas_por_obra]
                      + [f["mes"] for f in financeiro])
    meses_todos = meses_do_periodo(min(todas_as_datas), max(todas_as_datas))
    obras = sorted({obra for _m, obra, _v in linhas_por_obra})

    pesos_a = pesos_do_conjunto(escolhas_a, mapa_projeto, obras)
    pesos_resto = {obra: 1.0 - peso for obra, peso in pesos_a.items()}

    # ---- séries mensais, antes de acumular ----
    mensal_a, mensal_resto, mensal_obra = {}, {}, {}
    for mes, obra, valor in linhas_por_obra:
        mensal_a[mes] = mensal_a.get(mes, 0.0) + valor * pesos_a.get(obra, 0.0)
        mensal_resto[mes] = mensal_resto.get(mes, 0.0) + valor * pesos_resto.get(obra, 1.0)
        mensal_obra[obra] = mensal_obra.get(obra, 0.0) + valor

    fin = {f["mes"]: f for f in financeiro}
    aportes_liq = {m: (f["aporte_recebido"] + f["dividendo_pago"]) for m, f in fin.items()}
    emprestimo_liq = {m: (f["emprestimo_tomado"] + f["emprestimo_pago"])
                      for m, f in fin.items()}
    outros = {m: f["outros"] for m, f in fin.items()}

    # ---- acumulados sobre TODO o período (o corte é só de exibição) ----
    def _serie(dados):
        return dict(zip(meses_todos, _acumular(dados, meses_todos)))

    ac_a_tudo = _serie(mensal_a)
    ac_resto_tudo = _serie(mensal_resto)
    ac_aportes_tudo = _serie(aportes_liq if incluir_aportes else {})
    ac_aportes_reais = _serie(aportes_liq)
    ac_emprestimo_tudo = _serie(emprestimo_liq)
    ac_outros_tudo = _serie(outros)

    meses = [m for m in meses_todos if desde is None or m >= desde]

    linhas = []
    for mes in meses:
        a = ac_a_tudo[mes]
        resto = ac_resto_tudo[mes]
        aportes = ac_aportes_tudo[mes]
        empresa = round(a + resto + aportes, 2)
        # O caixa reconstruído soma TUDO que explica o dinheiro em conta:
        # obras + aportes − dividendos + empréstimo líquido + outros + o saldo
        # que já existia antes do primeiro mês.
        caixa = round(a + resto + ac_aportes_reais[mes] + ac_emprestimo_tudo[mes]
                      + ac_outros_tudo[mes] + float(saldo_inicial or 0.0), 2)
        linhas.append({
            "mes": mes,
            "rotulo": mes.strftime("%m/%Y"),
            "conjunto_a": a,
            "resto": resto,
            "aportes": aportes,
            "empresa": empresa,
            "emprestimo_liquido": ac_emprestimo_tudo[mes],
            "emprestimo_tomado_no_mes": fin.get(mes, {}).get("emprestimo_tomado", 0.0),
            "caixa_reconstruido": caixa,
        })

    composicao = []
    for obra in obras:
        composicao.append({
            "obra": obra,
            "projeto": mapa_projeto.get(obra, "") or "(sem projeto)",
            "pct_a": round(pesos_a[obra] * 100),
            "acumulado": round(mensal_obra.get(obra, 0.0), 2),
        })
    composicao.sort(key=lambda c: (-c["pct_a"], c["acumulado"]))

    return {
        "vazio": not linhas,
        "linhas": linhas,
        "obras": obras,
        "composicao": composicao,
        "conjunto_montado": any(p > 0 for p in pesos_a.values()),
        "leitura": _ler(linhas, incluir_aportes),
    }


# ---------------------------------------------------------------------------
# A leitura em português
# ---------------------------------------------------------------------------
# O gráfico mostra o quê; isto diz o que ele significa. É a parte do painel
# antigo que mais valia: alguém que não lê gráfico entende o parágrafo.

def _brl(v: float) -> str:
    texto = f"{abs(float(v)):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("−R$ " if v < 0 else "R$ ") + texto


def _janelas_negativas(linhas, campo):
    """Os períodos SEGUIDOS em que a série ficou abaixo de zero.

    Interessa a janela, não o mês solto: "ficou seis meses no vermelho" é uma
    informação diferente de "ficou negativo em seis meses espalhados"."""
    janelas, inicio = [], None
    for posicao, linha in enumerate(linhas):
        negativo = linha[campo] < -0.5
        if negativo and inicio is None:
            inicio = posicao
        elif not negativo and inicio is not None:
            janelas.append((linhas[inicio], linhas[posicao - 1]))
            inicio = None
    if inicio is not None:
        janelas.append((linhas[inicio], linhas[-1]))
    return janelas


def _ler(linhas, incluir_aportes: bool) -> list[str]:
    if not linhas:
        return []
    frases = []

    # 1) a empresa inteira precisou do banco?
    janelas = _janelas_negativas(linhas, "empresa")
    if janelas:
        janelas.sort(key=lambda j: (j[1]["mes"] - j[0]["mes"]).days, reverse=True)
        inicio, fim = janelas[0]
        dentro = [l for l in linhas if inicio["mes"] <= l["mes"] <= fim["mes"]]
        pior = min(dentro, key=lambda l: l["empresa"])
        tomado = sum(l["emprestimo_tomado_no_mes"] for l in dentro)
        quantas = sum(1 for l in dentro if abs(l["emprestimo_tomado_no_mes"]) > 0.5)
        extra = (f" (há {len(janelas) - 1} outra(s) janela(s) negativa(s), menores)"
                 if len(janelas) > 1 else "")
        frases.append(
            f"<b>A empresa precisou do banco.</b> De <b>{inicio['rotulo']}</b> a "
            f"<b>{fim['rotulo']}</b> o caixa gerado pela própria empresa, sem contar "
            f"empréstimo, ficou abaixo de zero — chegou a <b>{_brl(pior['empresa'])}</b> "
            f"em {pior['rotulo']}. Nesse período entraram <b>{quantas} tomada(s)</b> de "
            f"empréstimo, somando <b>{_brl(tomado)}</b>{extra}.")

        # quem cavou o buraco no pior mês
        candidatos = [("o conjunto A", pior["conjunto_a"]),
                      ("o resto das obras", pior["resto"])]
        if incluir_aportes:
            candidatos.append(("os dividendos e devoluções já pagos, "
                               "descontados os aportes recebidos", pior["aportes"]))
        negativos = sorted([c for c in candidatos if c[1] < -0.5], key=lambda c: c[1])
        positivos = [c for c in candidatos if c[1] > 0.5]
        if negativos:
            puxaram = " e ".join(f"<b>{nome}</b> ({_brl(v)})" for nome, v in negativos)
            seguravam = (" — enquanto " + ", ".join(f"{nome} estava em {_brl(v)}"
                                                    for nome, v in positivos)
                         if positivos else "")
            sem_a = ""
            if pior["conjunto_a"] < -0.5 and (pior["resto"] + pior["aportes"]) >= 0:
                sem_a = " Sem o conjunto A, a empresa teria ficado positiva."
            frases.append(f"<b>Quem cavou o buraco</b> no pior mês ({pior['rotulo']}): "
                          f"{puxaram}{seguravam}.{sem_a}")
    else:
        frases.append(
            "<b>A empresa não precisou do banco</b> neste período: o caixa que ela "
            "gerou sozinha nunca ficou abaixo de zero.")

    # 2) o conjunto A
    negativos_a = [l for l in linhas if l["conjunto_a"] < -0.5]
    fim_a = linhas[-1]["conjunto_a"]
    if negativos_a:
        pior_a = min(negativos_a, key=lambda l: l["conjunto_a"])
        nunca = (" — e nunca voltou a ficar positivo." if fim_a < 0 else ".")
        frases.append(
            f"<b>O conjunto A</b> ficou no vermelho em <b>{len(negativos_a)} de "
            f"{len(linhas)} meses</b>, com fundo de <b>{_brl(pior_a['conjunto_a'])}</b> "
            f"em {pior_a['rotulo']}, e termina em <b>{_brl(fim_a)}</b>{nunca}")
    else:
        frases.append(f"<b>O conjunto A</b> nunca ficou no vermelho; termina em "
                      f"<b>{_brl(fim_a)}</b>.")

    # 3) o resto
    pico = max(linhas, key=lambda l: l["resto"])
    frases.append(f"<b>O resto das obras</b> termina em "
                  f"<b>{_brl(linhas[-1]['resto'])}</b> (pico de "
                  f"{_brl(pico['resto'])} em {pico['rotulo']}).")

    if incluir_aportes and abs(linhas[-1]["aportes"]) > 0.5:
        valor = linhas[-1]["aportes"]
        explicacao = ("a empresa distribuiu e devolveu mais do que recebeu de aportes "
                      "— isso reduz o caixa que as obras geraram."
                      if valor < 0 else
                      "entrou mais aporte do que saiu em dividendo.")
        frases.append(f"<b>Aportes menos dividendos</b> acumulam <b>{_brl(valor)}</b>: "
                      f"{explicacao}")

    # 4) o teste de sanidade
    negativos_caixa = [l for l in linhas if l["caixa_reconstruido"] < -0.5]
    fim_caixa = linhas[-1]["caixa_reconstruido"]
    if negativos_caixa:
        pior_c = min(negativos_caixa, key=lambda l: l["caixa_reconstruido"])
        frases.append(
            f"<b>Teste do caixa reconstruído: não fechou</b> em "
            f"{len(negativos_caixa)} mês(es), com fundo de "
            f"<b>{_brl(pior_c['caixa_reconstruido'])}</b> em {pior_c['rotulo']}. "
            f"Caixa não fica negativo na vida real: nesses meses entrou dinheiro de "
            f"uma fonte que o OMIE não classifica — empréstimo lançado em outra "
            f"categoria, cheque especial, antecipação de recebível, ou um saldo "
            f"inicial maior do que o informado. <b>É exatamente aí que investigar.</b> "
            f"Termina em {_brl(fim_caixa)}.")
    else:
        pior_c = min(linhas, key=lambda l: l["caixa_reconstruido"])
        frases.append(
            f"<b>Teste do caixa reconstruído: passou</b> — nunca ficou negativo "
            f"(mínimo de {_brl(pior_c['caixa_reconstruido'])} em {pior_c['rotulo']}); "
            f"termina em {_brl(fim_caixa)}. As fontes de dinheiro que o painel "
            f"conhece explicam o caixa, ao menos no total.")

    # 5) o fechamento
    ultimo = linhas[-1]
    parcelas = (f"A ({_brl(ultimo['conjunto_a'])}) + resto ({_brl(ultimo['resto'])})")
    if incluir_aportes:
        parcelas += f" + aportes menos dividendos ({_brl(ultimo['aportes'])})"
    frases.append(f"<b>A empresa inteira</b> termina em "
                  f"<b>{_brl(ultimo['empresa'])}</b> = {parcelas}.")

    frases.append(
        "<i>Ressalva:</i> o acumulado começa em zero no primeiro mês com dado. "
        "Se a empresa já tinha caixa antes disso, todas as linhas sobem esse "
        "valor — use o campo de saldo inicial para corrigir.")
    return frases
