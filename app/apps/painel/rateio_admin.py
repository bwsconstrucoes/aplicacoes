# -*- coding: utf-8 -*-
"""
Rateio da Administração — a conta da simulação.

A pergunta: **como o custo da matriz se divide entre dois lados da empresa, mês
a mês — e quem pagou os juros do banco?**

Você monta o Lado A (obras ou projetos, com o quanto de cada um entra). O Lado B
é o complemento — tudo o que sobrou, inclusive a fração das obras que entraram
parcialmente em A. A matriz fica **fora dos dois**: ela é o bolo a repartir.

DUAS REPARTIÇÕES DIFERENTES, e a distinção é o ponto da tela:

- **O custo da matriz** é repartido pelo *critério* escolhido — faturamento ou
  pessoal, numa janela de meses. Quem produziu mais carrega mais estrutura.
- **Os juros do banco** são repartidos pelo *déficit*: quem estava com o caixa
  negativo naquele mês. Juros não é estrutura, é o preço de faltar dinheiro —
  e quem fez faltar é quem deve pagar.

E não há circularidade: os juros são alocados pelo déficit **antes** dos juros.

A conta mora aqui, fora do HTML e fora do SQL, para poder ser conferida sozinha.
Ela não sabe de banco nem de tela: recebe listas de números e devolve listas de
números.

Veio do `pagina_rateio_administracao.py` do painel Streamlit (documento de
passagem de 08/09/2026), reusando `pesos_do_conjunto` e `meses_do_periodo` da
Necessidade de Caixa, como o próprio documento sugere.
"""
from __future__ import annotations

import datetime as dt

from .simulacao import meses_do_periodo, pesos_do_conjunto  # noqa: F401 (reuso)

CRITERIOS = {
    "faturamento": "Faturamento — quem recebeu mais carrega mais",
    "pessoal": "Pessoal — quem tem mais gente carrega mais",
    "fixo": "Percentual fixo, escolhido por você",
}

# Quantos meses somar para calcular a participação de cada lado. Janela curta
# reage rápido e balança; janela longa é estável e demora a reagir. 12 é o
# padrão porque um ano fecha a sazonalidade de obra.
JANELAS = {"1": "o próprio mês", "3": "3 meses", "12": "12 meses",
           "acumulado": "tudo desde o início"}


def _por_mes(trios, pesos: dict) -> dict:
    """Soma uma série (mês, obra, valor) aplicando o peso de cada obra."""
    saida: dict[dt.date, float] = {}
    for mes, obra, valor in trios:
        peso = pesos.get(obra, 0.0)
        if peso:
            saida[mes] = saida.get(mes, 0.0) + float(valor) * peso
    return saida


def _janela(serie: list[float], janela: str) -> list[float]:
    """A soma corrida da série, na janela pedida."""
    if janela == "acumulado":
        total, saida = 0.0, []
        for v in serie:
            total += v
            saida.append(total)
        return saida
    try:
        n = max(int(janela), 1)
    except (TypeError, ValueError):
        n = 12
    return [sum(serie[max(0, i - n + 1):i + 1]) for i in range(len(serie))]


def participacao(serie_a: list[float], serie_b: list[float], janela: str,
                 pct_fixo: float | None = None) -> list[float]:
    """Quanto do bolo cabe ao lado A em cada mês, de 0 a 1.

    Quando os dois lados somam zero na janela — mês parado, começo da série —
    NÃO se inventa meio a meio: repete-se a última participação válida, porque
    a empresa não mudou de perfil só porque um mês foi fraco. Sem nenhuma
    anterior, aí sim metade para cada."""
    if pct_fixo is not None:
        return [min(max(float(pct_fixo) / 100.0, 0.0), 1.0)] * len(serie_a)

    ja, jb = _janela(serie_a, janela), _janela(serie_b, janela)
    saida, ultima = [], None
    for a, b in zip(ja, jb):
        total = a + b
        if abs(total) > 0.005:
            ultima = min(max(a / total, 0.0), 1.0)
            saida.append(ultima)
        else:
            saida.append(ultima if ultima is not None else 0.5)
    return saida


def simular(operacional, receita, pessoal, matriz, juros, escolhas_a,
            mapa_projeto, obras, *, depto_matriz: str,
            criterio: str = "faturamento", janela: str = "12",
            pct_fixo: float = 50.0, valor_fixo_por_mes: float = 0.0,
            abater_receita_da_matriz: bool = True, ajustes=(),
            juros_sem_deficit: str = "criterio") -> dict:
    """A simulação inteira, mês a mês.

    `operacional`, `receita` e `pessoal` são listas de (mês, obra, valor).
    `matriz` é (mês, despesa, receita) e `juros` é (mês, valor) — os dois já
    vêm somados, porque a matriz está fora dos lados e os juros não são de
    obra nenhuma."""
    meses_com_dado = ({m for m, _o, _v in operacional} | {m for m, _d, _r in matriz}
                      | {m for m, _v in juros})
    if not meses_com_dado:
        return {"vazio": True, "linhas": [], "meses": []}
    meses = meses_do_periodo(min(meses_com_dado), max(meses_com_dado))

    # A matriz fica FORA dos dois lados: ela é o bolo, não um pedaço dele.
    obras_dos_lados = [o for o in obras if o != depto_matriz]
    pesos_a = pesos_do_conjunto(escolhas_a, mapa_projeto, obras_dos_lados)
    pesos_b = {o: 1.0 - pesos_a.get(o, 0.0) for o in obras_dos_lados}

    op_a, op_b = _por_mes(operacional, pesos_a), _por_mes(operacional, pesos_b)
    rec_a, rec_b = _por_mes(receita, pesos_a), _por_mes(receita, pesos_b)
    pes_a, pes_b = _por_mes(pessoal, pesos_a), _por_mes(pessoal, pesos_b)
    desp_matriz = {m: d for m, d, _r in matriz}
    rec_matriz = {m: r for m, _d, r in matriz}
    juros_mes = dict(juros)

    ajuste_a: dict[dt.date, float] = {}
    ajuste_b: dict[dt.date, float] = {}
    for lado, mes, valor in ajustes:
        alvo = ajuste_a if lado == "A" else ajuste_b
        alvo[mes] = alvo.get(mes, 0.0) + float(valor or 0)

    # O bolo do mês: a despesa da matriz, abatida da receita dela (quando se
    # pede) e do valor fixo — a parte que NÃO deve ser rateada, tipicamente a
    # fatia dos salários que pertence a um lado só.
    #
    # NUNCA vira positivo: matriz que num mês recebeu mais do que gastou não
    # distribui lucro, e uma sobra dessas viraria caixa fantasma nas obras.
    pool = []
    for mes in meses:
        bruto = desp_matriz.get(mes, 0.0)
        if abater_receita_da_matriz:
            bruto += rec_matriz.get(mes, 0.0)
        bruto += abs(float(valor_fixo_por_mes or 0))
        pool.append(min(bruto, 0.0))

    if criterio == "fixo":
        share = [min(max(float(pct_fixo) / 100.0, 0.0), 1.0)] * len(meses)
    else:
        base_a = pes_a if criterio == "pessoal" else rec_a
        base_b = pes_b if criterio == "pessoal" else rec_b
        share = participacao([base_a.get(m, 0.0) for m in meses],
                             [base_b.get(m, 0.0) for m in meses], janela)

    linhas, cx_a, cx_b, ac_ja, ac_jb = [], 0.0, 0.0, 0.0, 0.0
    for i, mes in enumerate(meses):
        rat_a = pool[i] * share[i]
        rat_b = pool[i] - rat_a
        cx_a += op_a.get(mes, 0.0) + rat_a + ajuste_a.get(mes, 0.0)
        cx_b += op_b.get(mes, 0.0) + rat_b + ajuste_b.get(mes, 0.0)

        # Quem estava no vermelho neste mês, e quanto. É esta a régua dos juros.
        def_a, def_b = max(-cx_a, 0.0), max(-cx_b, 0.0)
        total_def = def_a + def_b
        do_mes = juros_mes.get(mes, 0.0)
        if total_def > 0.005:
            shj_a = def_a / total_def
        elif juros_sem_deficit == "criterio":
            shj_a = share[i]
        else:
            shj_a = 0.0
        juros_a = do_mes * shj_a
        juros_b = (do_mes - juros_a) if (total_def > 0.005
                                         or juros_sem_deficit == "criterio") else 0.0
        ac_ja += juros_a
        ac_jb += juros_b

        linhas.append({
            "mes": mes,
            "rotulo": f"{mes.month:02d}/{mes.year}",
            "receita_a": rec_a.get(mes, 0.0), "receita_b": rec_b.get(mes, 0.0),
            "pessoal_a": pes_a.get(mes, 0.0), "pessoal_b": pes_b.get(mes, 0.0),
            "pct_matriz_a": round(share[i] * 100, 2),
            "pool": round(pool[i], 2),
            "matriz_a": round(rat_a, 2), "matriz_b": round(rat_b, 2),
            "operacional_a": round(op_a.get(mes, 0.0), 2),
            "operacional_b": round(op_b.get(mes, 0.0), 2),
            "ajuste_a": round(ajuste_a.get(mes, 0.0), 2),
            "ajuste_b": round(ajuste_b.get(mes, 0.0), 2),
            "caixa_a": round(cx_a, 2), "caixa_b": round(cx_b, 2),
            "deficit_a": round(def_a, 2), "deficit_b": round(def_b, 2),
            "juros_mes": round(do_mes, 2),
            "pct_juros_a": round(shj_a * 100, 2),
            "juros_a": round(juros_a, 2), "juros_b": round(juros_b, 2),
            "final_a": round(cx_a + ac_ja, 2), "final_b": round(cx_b + ac_jb, 2),
        })

    return {
        "vazio": False,
        "linhas": linhas,
        "meses": meses,
        "pesos_a": {o: p for o, p in pesos_a.items() if p > 0},
        "resumo": _resumo(linhas),
    }


def _resumo(linhas) -> dict:
    """Os números do topo — e a leitura que eles permitem."""
    if not linhas:
        return {}
    ultima = linhas[-1]
    total_pool = sum(l["pool"] for l in linhas)
    matriz_a = sum(l["matriz_a"] for l in linhas)
    juros_total = sum(l["juros_mes"] for l in linhas)
    juros_a = sum(l["juros_a"] for l in linhas)
    return {
        "matriz_a": round(matriz_a, 2),
        "matriz_b": round(sum(l["matriz_b"] for l in linhas), 2),
        # o "+ 0.0" existe para 0 dividido por negativo nao virar "-0,0%" na tela
        "pct_matriz_a": (round(matriz_a / total_pool * 100, 1) + 0.0) if total_pool else 0.0,
        "juros_a": round(juros_a, 2),
        "juros_b": round(sum(l["juros_b"] for l in linhas), 2),
        "pct_juros_a": (round(juros_a / juros_total * 100, 1) + 0.0) if juros_total else 0.0,
        "final_a": ultima["final_a"], "final_b": ultima["final_b"],
        "meses_negativos_a": sum(1 for l in linhas if l["final_a"] < -0.005),
        "meses_negativos_b": sum(1 for l in linhas if l["final_b"] < -0.005),
        "meses": len(linhas),
    }
