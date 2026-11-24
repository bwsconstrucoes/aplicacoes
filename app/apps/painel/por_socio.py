# -*- coding: utf-8 -*-
"""
O RESULTADO DE CADA OBRA, SÓCIO POR SÓCIO — a grade obra × sócio.

Pedido do dono em 24/11/2026:

    "A gente tem aproximadamente uns 6 sócios cadastrados. Veja como fica
    interessante visualizar o resultado por obra de cada sócio. E ao final ter
    o somatório também. Que aí cada sócio bate o olho e já vê quanto é que está
    sendo apropriado para ele em relação àquela obra. Se é resultado, quanto de
    resultado; se prejuízo, quanto de prejuízo. (…) E ao final poderia ter a
    apropriação dos custos administrativos e de juros."

Duas visões da mesma grade, escolhidas na tela:

- **antes** (a que abre): em cada obra, a parte do sócio no RESULTADO DIRETO —
  o que a obra deu sozinha, antes de carregar estrutura e juros. As linhas do
  fim fazem a passagem: soma do direto, menos a estrutura, menos os juros (e a
  taxa de administração, quando há obra com parceiro) = resultado final;
- **final**: em cada obra, a QUOTA do sócio — o que a tela de Resultado já
  mostrava em lista.

NÃO HÁ CONTA NOVA: tudo vem das quotas que `prestacao.quotas_por_obra`
calcula. As quatro parcelas de cada quota somam a quota; por isso a última
linha das duas visões é a mesma, e é o saldo que a "posição de cada um" usa
antes dos ajustes.

Sem banco e sem Flask: recebe o cálculo do cenário, devolve dicionários.
"""
from __future__ import annotations

VISOES = ("antes", "final")
PARCELAS = ("parte_direto", "parte_estrutura", "parte_juros", "parte_taxa")


def montar(calculo: dict, ordem_dos_socios=()) -> dict:
    """A grade inteira. `ordem_dos_socios` é a lista de nomes na ordem do
    cadastro (a posição que o dono escolheu); quem não estiver nela vai para o
    fim, por nome."""
    quotas = calculo.get("quotas") or []
    por_obra = calculo.get("por_obra") or {}
    obras_info = calculo.get("obras") or {}

    lugar = {nome: i for i, nome in enumerate(ordem_dos_socios)}
    nomes = sorted({q["socio"] for q in quotas},
                   key=lambda n: (lugar.get(n, len(lugar)), n))
    tipo_de = {q["socio"]: q.get("tipo") or "Interno" for q in quotas}

    da_obra: dict[str, dict[str, dict]] = {}
    for q in quotas:
        da_obra.setdefault(q["obra"], {})[q["socio"]] = q

    linhas = []
    for obra, n in por_obra.items():
        gente = da_obra.get(obra) or {}
        celulas = []
        for nome in nomes:
            q = gente.get(nome)
            celulas.append(None if q is None else {
                "pct": float(q["pct"]),
                "antes": float(q["parte_direto"]),
                "final": float(q["quota"]),
            })
        soma_pct = sum(c["pct"] for c in celulas if c)
        linhas.append({
            "obra": obra,
            "projeto": ((obras_info.get(obra) or {}).get("projeto") or "").strip()
                       or "(sem projeto)",
            "antes": float(n.get("resultado_direto", 0.0)),
            "final": float(n.get("resultado", 0.0)),
            "estrutura": float(n.get("rateio", 0.0)),
            "juros": float(n.get("juros", 0.0)),
            "celulas": celulas,
            # ninguém definido, ou percentuais que não somam 100: a soma das
            # colunas não fecha com a da obra, e a tela precisa dizer por quê
            "sem_dono": not gente,
            "soma_pct": round(soma_pct, 2),
            "nao_fecha": bool(gente) and abs(soma_pct - 100.0) > 0.01,
            "com_parceiro": any((q.get("visao") or "").startswith("Parceria")
                                for q in gente.values()),
        })
    linhas.sort(key=lambda l: (l["projeto"], l["obra"]))

    def _soma(parcela):
        """A soma de uma parcela: a das obras (com quem divide) e a de cada sócio."""
        por_socio = [sum(float(q[parcela]) for q in quotas if q["socio"] == nome)
                     for nome in nomes]
        return {"total": sum(por_socio), "por_socio": por_socio}

    rodape = {p: _soma(p) for p in PARCELAS}
    rodape["final"] = {
        "por_socio": [sum(rodape[p]["por_socio"][i] for p in PARCELAS)
                      for i in range(len(nomes))]}
    rodape["final"]["total"] = sum(rodape["final"]["por_socio"])

    return {
        "socios": [{"nome": n, "tipo": tipo_de.get(n, "Interno")} for n in nomes],
        "linhas": linhas, "rodape": rodape,
        "tem_taxa": abs(rodape["parte_taxa"]["total"]) > 0.005
                    or any(abs(v) > 0.005 for v in rodape["parte_taxa"]["por_socio"]),
        "sem_dono": [l["obra"] for l in linhas if l["sem_dono"]],
        "nao_fecham": [l["obra"] for l in linhas if l["nao_fecha"]],
        # o resultado de todas as obras da prestação, com ou sem quem divida
        "total_das_obras": {v: sum(l[v] for l in linhas) for v in VISOES},
    }


def visao_valida(pedida: str) -> str:
    return pedida if pedida in VISOES else "antes"


# ---------------------------------------------------------------------------
# O relatório: a mesma grade, para a planilha e para o PDF
# ---------------------------------------------------------------------------
def _colunas(dados: dict, primeira: str) -> list[tuple]:
    # "valor" no título é o que faz a planilha e o PDF formatarem em reais
    return ([("projeto", "Projeto"), ("obra", "Obra"), ("da_obra", primeira)]
            + [(f"s{i}", f"{s['nome']} — valor") for i, s in enumerate(dados["socios"])])


def _linhas_da_grade(dados: dict, visao: str) -> list[dict]:
    saida = []
    for l in dados["linhas"]:
        linha = {"projeto": l["projeto"], "obra": l["obra"], "da_obra": l[visao]}
        for i, c in enumerate(l["celulas"]):
            linha[f"s{i}"] = c[visao] if c else ""
        saida.append(linha)
    r = dados["rodape"]

    def _rodape(titulo, parcela):
        linha = {"projeto": "", "obra": titulo, "da_obra": r[parcela]["total"]}
        for i, v in enumerate(r[parcela]["por_socio"]):
            linha[f"s{i}"] = v
        return linha
    if visao == "antes":
        saida.append(_rodape("SOMA DO RESULTADO DIRETO", "parte_direto"))
        saida.append(_rodape("Estrutura apropriada", "parte_estrutura"))
        saida.append(_rodape("Juros apropriados", "parte_juros"))
        if dados["tem_taxa"]:
            saida.append(_rodape("Taxa de administração das parcerias", "parte_taxa"))
    saida.append(_rodape("RESULTADO FINAL DE CADA UM", "final"))
    return saida


def abas(dados: dict) -> list[tuple]:
    """As abas do relatório, no formato de `excel.montar` e `pdf.montar`."""
    r = dados["rodape"]
    resumo = []
    for i, s in enumerate(dados["socios"]):
        resumo.append({
            "socio": s["nome"], "tipo": s["tipo"],
            "direto": r["parte_direto"]["por_socio"][i],
            "estrutura": r["parte_estrutura"]["por_socio"][i],
            "juros": r["parte_juros"]["por_socio"][i],
            "taxa": r["parte_taxa"]["por_socio"][i],
            "final": r["final"]["por_socio"][i],
        })
    return [
        ("Antes da estrutura e juros",
         _colunas(dados, "Resultado direto da obra"), _linhas_da_grade(dados, "antes")),
        ("Resultado final",
         _colunas(dados, "Resultado final da obra"), _linhas_da_grade(dados, "final")),
        ("Resumo por socio",
         [("socio", "Sócio"), ("tipo", "Tipo"),
          ("direto", "Resultado direto (parte dele)"),
          ("estrutura", "Estrutura apropriada"), ("juros", "Juros apropriados"),
          ("taxa", "Valor da taxa de administração"),
          ("final", "Resultado final")], resumo),
    ]
