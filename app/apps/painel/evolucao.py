# -*- coding: utf-8 -*-
"""
A EVOLUÇÃO DO RATEIO — a mesma conta do cenário, aberta mês a mês.

Pedido do dono em 16/11/2026, depois de mostrar a prestação a um dos sócios:

    "Os sócios não vão se conter apenas com o cálculo final, sem compreender na
    minúcia como é que aqueles valores estão chegando e sendo apropriados. (…)
    A gente está rateando isso, baseado nisso, e essa obra está absorvendo
    tantos %, o que é que está acontecendo dentro daquele mês, como é que está
    o caixa da obra. Aí ela absorveu tanto de juros, de um juro de tanto. (…)
    Precisa poder observar por obra, por projeto, por estrutura."

ESTE MÓDULO NÃO FAZ CONTA NOVA. Ele só reorganiza o que `web._calcular_cenario`
já devolve — as alocações, as parcelas guardadas em `detalhe`, o caixa e a
régua — em três visões (estrutura › projeto › obra) e numa ficha mês a mês.
É de propósito: uma segunda conta ao lado da primeira seria um segundo número
para o mesmo fato, e num documento que vai ser discutido entre sócios isso é o
pior defeito possível. Se a regra do rateio mudar, muda em `prestacao.py` e
esta tela acompanha sozinha.

SOBRE O SINAL. No painel, despesa é número negativo. Aqui o que a obra
ABSORVEU é dito como custo, positivo ("absorveu R$ 41.239") — como na página
"Como é feita". O caixa mantém o sinal: negativo é buraco.

Sem banco e sem Flask: recebe dicionários, devolve dicionários. A tela e o
relatório (planilha e PDF) saem daqui, os dois — por isso não divergem.
"""
from __future__ import annotations

from markupsafe import Markup, escape

from .prestacao import NOME_DOS_DEMAIS, SEM_DATA, TOLERANCIA_DO_BATIMENTO

# os campos de uma linha mensal que se SOMAM quando se junta obra em projeto e
# projeto em estrutura (o acumulado e os percentuais são refeitos da soma)
_SOMAVEIS = ("regua", "fracao", "fracao_do_lado", "livre", "lado", "filial",
             "estrutura", "caixa", "juros", "buraco")

VISOES = ("estrutura", "projeto", "obra")


def _rotulo(mes: str) -> str:
    ano, _, m = (mes or "").partition("-")
    return f"{m}/{ano}" if m else mes


def brl(v) -> str:
    """1234.5 -> "R$ 1.234,50"; negativo com o sinal na frente."""
    v = float(v or 0)
    texto = f"{abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-R$ " if v < -0.004 else "R$ ") + texto


def pct(fracao, casas: int = 1) -> str:
    """0.161 -> "16,1%"."""
    return f"{float(fracao or 0) * 100:.{casas}f}".replace(".", ",") + "%"


# ---------------------------------------------------------------------------
# 1. Cada obra, mês a mês
# ---------------------------------------------------------------------------
def _linhas_das_obras(calculo: dict, nomes: set) -> dict:
    """{obra: {mes: linha}} — só os meses em que a obra teve alguma coisa."""
    rateio = calculo["rateio"]
    juros = calculo["juros"]
    detalhe = (rateio.get("detalhe") or {}).get("por_obra_mes") or {}
    buracos = (juros.get("detalhe") or {}).get("buracos") or {}

    por_obra: dict[str, dict[str, dict]] = {o: {} for o in nomes}

    def _linha(obra, mes):
        return por_obra[obra].setdefault(mes, {c: 0.0 for c in _SOMAVEIS})

    for mes, obra, valor in calculo.get("driver") or ():
        if obra in nomes and mes != SEM_DATA:
            _linha(obra, mes)["regua"] += abs(float(valor or 0))
    for mes, obra, valor in calculo.get("caixa") or ():
        if obra in nomes and mes != SEM_DATA:
            _linha(obra, mes)["caixa"] += float(valor or 0)
    for (obra, mes), valor in (rateio.get("alocacoes") or {}).items():
        if obra in nomes and mes != SEM_DATA:
            _linha(obra, mes)["estrutura"] -= float(valor or 0)
    for (obra, mes), d in detalhe.items():
        if obra in nomes and mes != SEM_DATA:
            linha = _linha(obra, mes)
            linha["livre"] -= d.get("livre", 0.0)
            linha["lado"] -= d.get("lado", 0.0)
            linha["filial"] -= d.get("filial", 0.0)
            linha["fracao"] += d.get("fracao", 0.0)
            linha["fracao_do_lado"] += d.get("fracao_do_lado", 0.0)
    for (obra, mes), valor in (juros.get("alocacoes") or {}).items():
        if obra in nomes and mes != SEM_DATA:
            _linha(obra, mes)["juros"] -= float(valor or 0)
    # o buraco entra só nos meses que a obra já tem: mês parado, sem juro, não
    # vira linha — mas o buraco dele conta na soma do projeto (ver `_juntar`)
    for obra, meses in por_obra.items():
        for mes, linha in meses.items():
            linha["buraco"] = float(buracos.get((obra, mes), 0.0))
    return por_obra


def _juntar(obras: list, por_obra: dict, buracos: dict, juros_da_empresa: dict) -> list[dict]:
    """A ficha de um conjunto de obras (uma só, um projeto, uma estrutura):
    uma linha por mês, com o acumulado e os percentuais refeitos da soma."""
    meses = sorted({m for o in obras for m in por_obra.get(o, {})})
    linhas, acumulado = [], 0.0
    for mes in meses:
        linha = {c: 0.0 for c in _SOMAVEIS}
        for o in obras:
            da_obra = por_obra.get(o, {}).get(mes)
            if da_obra:
                for c in _SOMAVEIS:
                    if c != "buraco":
                        linha[c] += da_obra[c]
            # o buraco de quem não se mexeu no mês também conta no conjunto
            linha["buraco"] += float(buracos.get((o, mes), 0.0))
        # o caixa que conta para o buraco é o da obra JÁ com a estrutura que
        # ela recebeu — é assim que o juro é repartido (`alocar_juros_por_deficit`)
        acumulado += linha["caixa"] - linha["estrutura"]
        juro_do_mes = abs(float(juros_da_empresa.get(mes, 0.0)))
        linha.update({
            "mes": mes, "rotulo": _rotulo(mes),
            "acumulado": acumulado,
            "juros_da_empresa": juro_do_mes,
            "pct_dos_juros": (linha["juros"] / juro_do_mes) if juro_do_mes > 0.005 else 0.0,
            "absorvido": linha["estrutura"] + linha["juros"],
        })
        linhas.append(linha)
    return linhas


def _total(linhas: list) -> dict:
    total = {c: sum(l[c] for l in linhas)
             for c in ("regua", "livre", "lado", "filial", "estrutura", "caixa", "juros")}
    total["absorvido"] = total["estrutura"] + total["juros"]
    total["acumulado"] = linhas[-1]["acumulado"] if linhas else 0.0
    return total


# ---------------------------------------------------------------------------
# 2. As três visões
# ---------------------------------------------------------------------------
def _no(chave, nome, tipo, obras, totais_das_obras, extra=None) -> dict:
    campos = ("regua", "livre", "lado", "filial", "estrutura", "juros", "absorvido",
              "receita_liquida", "despesas", "resultado_direto", "resultado")
    no = {"chave": chave, "nome": nome, "tipo": tipo, "obras": sorted(obras),
          "n_obras": len(obras), "filhos": []}
    for c in campos:
        no[c] = sum(totais_das_obras[o].get(c, 0.0) for o in obras)
    no.update(extra or {})
    return no


def montar(calculo: dict, *, principal: str = "", criterio: str = "pessoal",
           janela: str = "1", juros_por_deficit: bool = True) -> dict:
    """Tudo o que a tela e o relatório de evolução precisam, de uma vez.

    Só entram as obras que estão NA prestação e têm lado (projeto). O que o
    cenário deixou de fora não aparece — decisão do dono, 16/11/2026: "obras
    fora da prestação não é para se exibir"."""
    obras_info = calculo.get("obras") or {}
    lado_de = calculo.get("lado_de") or {}
    nomes = {o for o in obras_info if o in lado_de}
    tem_lados = any(lado_de[o] == "principal" for o in nomes)
    nome_do_lado = {"principal": principal or "Projeto principal",
                    "demais": NOME_DOS_DEMAIS}

    por_obra = _linhas_das_obras(calculo, nomes)
    buracos = ((calculo["juros"].get("detalhe") or {}).get("buracos") or {})
    juros_da_empresa = {m: v for m, v in (calculo.get("juros_por_mes") or {}).items()
                        if m != SEM_DATA}

    # o total de cada obra: o que ela absorveu, mais o resultado que a tela de
    # Resultado mostra (para o relatório ligar uma coisa à outra)
    do_resultado = calculo.get("por_obra") or {}
    totais: dict[str, dict] = {}
    for o in nomes:
        t = _total(_juntar([o], por_obra, buracos, juros_da_empresa))
        for c in ("receita_liquida", "despesas", "resultado_direto", "resultado"):
            t[c] = float((do_resultado.get(o) or {}).get(c, 0.0))
        totais[o] = t
    # obra que não absorveu nada e não pesa na régua só faria volume na árvore
    ativas = {o for o in nomes
              if abs(totais[o]["absorvido"]) > 0.005 or totais[o]["regua"] > 0.005}

    projeto_de = {o: (obras_info[o].get("projeto") or "").strip() for o in ativas}

    def _de_obra(o):
        return _no(f"obra:{o}", o, "obra", [o], totais,
                   {"projeto": projeto_de[o], "qual_lado": lado_de[o],
                    "lado_nome": nome_do_lado[lado_de[o]]})

    def _de_projeto(p):
        dele = [o for o in ativas if projeto_de[o] == p]
        lado = lado_de[dele[0]]
        no = _no(f"projeto:{p}", p, "projeto", dele, totais,
                 {"projeto": p, "qual_lado": lado, "lado_nome": nome_do_lado[lado]})
        no["filhos"] = sorted((_de_obra(o) for o in dele),
                              key=lambda n: -n["absorvido"])
        return no

    def _de_lado(lado):
        dele = [o for o in ativas if lado_de[o] == lado]
        no = _no(f"lado:{lado}", nome_do_lado[lado], "estrutura", dele, totais,
                 {"projeto": "", "qual_lado": lado, "lado_nome": nome_do_lado[lado]})
        projetos = sorted({projeto_de[o] for o in dele})
        # o lado principal É um projeto só: abrir "projeto" dentro dele seria
        # repetir a mesma linha — as obras vêm direto
        if lado == "principal" and len(projetos) <= 1:
            no["filhos"] = sorted((_de_obra(o) for o in dele),
                                  key=lambda n: -n["absorvido"])
        else:
            no["filhos"] = sorted((_de_projeto(p) for p in projetos),
                                  key=lambda n: -n["absorvido"])
        return no

    arvores = {
        "estrutura": ([_de_lado(l) for l in ("principal", "demais")
                       if any(lado_de[o] == l for o in ativas)] if tem_lados else []),
        "projeto": sorted((_de_projeto(p) for p in sorted(set(projeto_de.values()))),
                          key=lambda n: -n["absorvido"]),
        "obra": sorted((_de_obra(o) for o in ativas), key=lambda n: -n["absorvido"]),
    }

    geral = {c: sum(totais[o][c] for o in ativas)
             for c in ("regua", "livre", "lado", "filial", "estrutura", "juros", "absorvido")}

    def _percentuais(no):
        no["pct_regua"] = no["regua"] / geral["regua"] if geral["regua"] > 0.005 else 0.0
        no["pct_absorvido"] = (no["absorvido"] / geral["absorvido"]
                               if abs(geral["absorvido"]) > 0.005 else 0.0)
        for filho in no["filhos"]:
            _percentuais(filho)
    for arvore in arvores.values():
        for no in arvore:
            _percentuais(no)

    dados = {
        "tem_lados": tem_lados, "nome_do_lado": nome_do_lado,
        "arvores": arvores, "geral": geral,
        "criterio": criterio, "janela": str(janela),
        "juros_por_deficit": bool(juros_por_deficit),
        "sem_lado": sorted(o for o in obras_info if o not in lado_de),
        # guardados para as fichas, que são feitas sob pedido
        "_por_obra": por_obra, "_buracos": buracos,
        "_juros_da_empresa": juros_da_empresa,
        "_partes": (calculo["rateio"].get("detalhe") or {}).get("partes_do_mes") or {},
        "_criterio_dos_juros": (calculo["juros"].get("detalhe") or {}).get("criterio") or {},
    }
    dados["meses"] = _mes_a_mes(calculo, dados, ativas, lado_de)
    dados["caminho"] = _caminho(calculo, dados, ativas, lado_de, totais)
    return dados


def visao_valida(dados: dict, pedida: str) -> str:
    """A visão pedida, se existir neste cenário. Sem projeto principal não há
    duas estruturas para comparar — cai para "por projeto"."""
    if pedida in VISOES and dados["arvores"].get(pedida):
        return pedida
    return "estrutura" if dados["arvores"]["estrutura"] else "projeto"


def achar(dados: dict, chave: str):
    """O nó com esta chave, em qualquer das árvores."""
    def _procurar(nos):
        for no in nos:
            if no["chave"] == chave:
                return no
            achado = _procurar(no["filhos"])
            if achado:
                return achado
        return None
    for visao in VISOES:
        achado = _procurar(dados["arvores"][visao])
        if achado:
            return achado
    return None


def linhas_da_arvore(dados: dict, visao: str) -> list[dict]:
    """A árvore aplainada, na ordem de desenhar: cada linha com o nível e a
    chave do pai — a tela esconde e mostra os filhos sem ir ao servidor."""
    saida = []

    def _descer(no, nivel, pai, caminho):
        chave_da_linha = caminho + "/" + no["chave"] if caminho else no["chave"]
        saida.append(dict(no, nivel=nivel, pai=pai, linha=chave_da_linha,
                          tem_filhos=bool(no["filhos"])))
        for filho in no["filhos"]:
            _descer(filho, nivel + 1, chave_da_linha, chave_da_linha)
    for no in dados["arvores"][visao]:
        _descer(no, 0, "", "")
    return saida


# ---------------------------------------------------------------------------
# 3. O caminho do dinheiro e o mês a mês da empresa
# ---------------------------------------------------------------------------
def _mes_a_mes(calculo, dados, ativas, lado_de) -> list[dict]:
    """Uma linha por mês: o que a matriz e a filial gastaram, como se dividiu,
    e quanto cada estrutura absorveu."""
    partes = dados["_partes"]
    por_obra = dados["_por_obra"]
    meses = sorted(set(partes) | set(dados["_juros_da_empresa"])
                   | {m for o in ativas for m in por_obra[o]})
    sem_dono: dict[str, float] = {}
    for sobra in calculo.get("sobras") or ():
        sem_dono[sobra["mes"]] = sem_dono.get(sobra["mes"], 0.0) - float(sobra["valor"] or 0)

    saida = []
    for mes in meses:
        p = partes.get(mes) or {}
        linha = {
            "mes": mes, "rotulo": _rotulo(mes),
            "fica": -p.get("principal", 0.0), "da_filial": -p.get("demais", 0.0),
            "rateia": -p.get("livre", 0.0), "filial": -p.get("filial", 0.0),
            "juros_da_empresa": abs(float(dados["_juros_da_empresa"].get(mes, 0.0))),
            "estrutura_principal": 0.0, "estrutura_demais": 0.0,
            "juros_principal": 0.0, "juros_demais": 0.0,
            "sem_dono": sem_dono.get(mes, 0.0),
        }
        linha["matriz"] = linha["fica"] + linha["da_filial"] + linha["rateia"]
        for o in ativas:
            da_obra = por_obra[o].get(mes)
            if da_obra:
                linha["estrutura_" + lado_de[o]] += da_obra["estrutura"]
                linha["juros_" + lado_de[o]] += da_obra["juros"]
        linha["juros_absorvidos"] = linha["juros_principal"] + linha["juros_demais"]
        saida.append(linha)
    return saida


def _caminho(calculo, dados, ativas, lado_de, totais) -> dict:
    """De onde saiu o custo e em que estrutura ele foi parar — e se fecha."""
    meses = dados["meses"]
    soma = lambda c: sum(l[c] for l in meses)  # noqa: E731
    sobras = calculo.get("sobras") or ()
    sem_dono = -sum(float(s["valor"] or 0) for s in sobras)
    # custo sem data nunca entrou no bolo de mês nenhum: aparece só como sobra
    sem_data = -sum(float(s["valor"] or 0) for s in sobras if s["mes"] == SEM_DATA)
    juros_fora = -float(calculo.get("juros_fora_do_resultado") or 0.0)
    juros_total = sum(abs(float(v or 0)) for v in dados["_juros_da_empresa"].values())

    por_lado = {}
    for lado in ("principal", "demais"):
        dele = [o for o in ativas if lado_de[o] == lado]
        por_lado[lado] = {
            "nome": (dados["nome_do_lado"][lado] if dados["tem_lados"] else "As obras"),
            "n_obras": len(dele),
            "estrutura": sum(totais[o]["estrutura"] for o in dele),
            "juros": sum(totais[o]["juros"] for o in dele),
        }
        por_lado[lado]["absorvido"] = por_lado[lado]["estrutura"] + por_lado[lado]["juros"]

    saiu = soma("matriz") + soma("filial") + juros_total
    chegou = (sum(p["absorvido"] for p in por_lado.values())
              + (sem_dono - sem_data) + juros_fora)
    diferenca = saiu - chegou
    return {
        "matriz": soma("matriz"), "fica": soma("fica"), "da_filial": soma("da_filial"),
        "rateia": soma("rateia"), "filial": soma("filial"), "juros": juros_total,
        "por_lado": por_lado, "sem_dono": sem_dono, "juros_fora": juros_fora,
        "saiu": saiu, "chegou": chegou, "diferenca": diferenca,
        "fecha": abs(diferenca) <= TOLERANCIA_DO_BATIMENTO * max(len(meses), 1),
    }


# ---------------------------------------------------------------------------
# 4. A ficha de um nó e a conta de um mês, em palavras
# ---------------------------------------------------------------------------
def ficha(dados: dict, no: dict, com_a_conta: bool = False) -> dict:
    """A vida de uma obra, de um projeto ou de uma estrutura, mês a mês.

    `com_a_conta` acrescenta a cada mês a conta dele em palavras — só a tela
    pede; o relatório quer a tabela."""
    linhas = _juntar(no["obras"], dados["_por_obra"], dados["_buracos"],
                     dados["_juros_da_empresa"])
    total = _total(linhas)
    total["pct_regua"] = no.get("pct_regua", 0.0)
    total["juros_da_empresa"] = sum(l["juros_da_empresa"] for l in linhas)
    if com_a_conta:
        for linha in linhas:
            linha["conta"] = conta_do_mes(dados, no, linha)
    return {"no": no, "linhas": linhas, "total": total}


def _b(texto) -> Markup:
    return Markup("<b>{}</b>").format(texto)


def conta_do_mes(dados: dict, no: dict, linha: dict) -> list[Markup]:
    """A conta de UM mês de um nó, passo a passo, em português.

    Devolve frases prontas para a tela (com os números em negrito). Os nomes
    vêm do OMIE e passam por `escape` — nome de obra com "<" não vira HTML."""
    mes = linha["mes"]
    p = dados["_partes"].get(mes) or {}
    fica, da_filial = -p.get("principal", 0.0), -p.get("demais", 0.0)
    rateia, filial = -p.get("livre", 0.0), -p.get("filial", 0.0)
    matriz = fica + da_filial + rateia
    lado = no["qual_lado"]
    principal = escape(dados["nome_do_lado"]["principal"])
    demais = escape(dados["nome_do_lado"]["demais"])
    este = {"obra": "esta obra", "projeto": "este projeto",
            "estrutura": "esta estrutura"}[no["tipo"]]
    Este = este[0].upper() + este[1:]
    deste = "d" + este
    passos: list[Markup] = []

    # 1. o que havia para dividir
    if dados["tem_lados"]:
        frase = Markup("Em {} a matriz gastou {}. Pelas regras do cenário, {} ficaram no {}, "
                       "{} são da {} e {} foram rateados entre todas as obras.").format(
            _b(linha["rotulo"]), _b(brl(matriz)), _b(brl(fica)), principal,
            _b(brl(da_filial)), demais, _b(brl(rateia)))
        if abs(filial) > 0.005:
            frase += Markup(" A filial gastou mais {}, só para as obras da {}.").format(
                _b(brl(filial)), demais)
    else:
        frase = Markup("Em {} a matriz gastou {}, rateados entre todas as obras.").format(
            _b(linha["rotulo"]), _b(brl(matriz)))
    passos.append(frase)

    # 2. a régua
    nome_da_regua = ("o faturamento" if dados["criterio"] == "faturamento"
                     else "o custo de mão de obra")
    if linha["fracao"] <= 0:
        passos.append(Markup("A régua é {}. {} não pesou na régua neste mês — por isso "
                             "não absorveu estrutura.").format(nome_da_regua, Este))
    elif dados["janela"] == "1":
        todas = linha["regua"] / linha["fracao"]
        passos.append(Markup("A régua é {} do mês. {} teve {}; todas as obras que recebem, "
                             "juntas, {}. É {} da régua.").format(
            nome_da_regua, Este, _b(brl(linha["regua"])), _b(brl(todas)),
            _b(pct(linha["fracao"]))))
    else:
        quanto = ("desde o começo" if not dados["janela"].isdigit()
                  else f"dos últimos {dados['janela']} meses")
        passos.append(Markup("A régua é {} somado {} (no mês, {} teve {}). Por ela, "
                             "pesava {} entre todas as obras que recebem.").format(
            nome_da_regua, quanto, este, _b(brl(linha["regua"])), _b(pct(linha["fracao"]))))

    # 3. as parcelas
    if linha["fracao"] > 0:
        passos.append(Markup("Do que foi rateado entre todas, absorveu {} × {} = {}.").format(
            pct(linha["fracao"]), brl(rateia), _b(brl(linha["livre"]))))
        if dados["tem_lados"] and lado == "principal" and abs(linha["lado"]) > 0.005:
            passos.append(Markup("Do que ficou no {} (dividido só entre as obras dele), "
                                 "absorveu {} × {} = {}.").format(
                principal, pct(linha["fracao_do_lado"]), brl(fica),
                _b(brl(linha["lado"]))))
        if dados["tem_lados"] and lado == "demais" and (
                abs(linha["lado"]) > 0.005 or abs(linha["filial"]) > 0.005):
            passos.append(Markup("Do que é da {} (dividido só entre as obras dela), absorveu "
                                 "{} da parte da matriz e {} do custo da filial — "
                                 "{} da régua dentro da {}.").format(
                demais, _b(brl(linha["lado"])), _b(brl(linha["filial"])),
                pct(linha["fracao_do_lado"]), demais))
        passos.append(Markup("Estrutura absorvida no mês: {}.").format(
            _b(brl(linha["estrutura"]))))

    # 4. o caixa
    verbo = "gerou" if linha["caixa"] >= 0 else "consumiu"
    frase = Markup("No mês, {} {} {} de caixa próprio. Com a estrutura absorvida, "
                   "o acumulado ficou em {}").format(
        este, verbo, _b(brl(abs(linha["caixa"]))), _b(brl(linha["acumulado"])))
    frase += Markup(" — no vermelho.") if linha["acumulado"] < -0.005 else Markup(".")
    passos.append(frase)

    # 5. os juros
    juro = linha["juros_da_empresa"]
    if not dados["juros_por_deficit"]:
        passos.append(Markup("Neste cenário os juros de empréstimo entram junto com a "
                             "estrutura, pela mesma régua."))
    elif juro <= 0.005:
        passos.append(Markup("Não houve juros de empréstimo neste mês."))
    elif abs(linha["juros"]) > 0.005:
        if dados["_criterio_dos_juros"].get(mes) == "régua da estrutura":
            passos.append(Markup("Os juros de empréstimo do mês foram {}. Ninguém estava no "
                                 "vermelho, então foram divididos pela régua da estrutura: "
                                 "absorveu {}.").format(_b(brl(juro)), _b(brl(linha["juros"]))))
        else:
            passos.append(Markup("Os juros de empréstimo do mês foram {}. O buraco de caixa "
                                 "{} era de {}; por ele, absorveu {} — {} dos juros "
                                 "do mês.").format(
                _b(brl(juro)), deste, _b(brl(linha["buraco"])), _b(brl(linha["juros"])),
                _b(pct(linha["pct_dos_juros"]))))
    elif linha["buraco"] > 0.005:
        passos.append(Markup("Os juros de empréstimo do mês foram {}. {} estava no vermelho, "
                             "mas não absorveu juros: o caixa das outras obras do mesmo lado "
                             "cobria o buraco.").format(_b(brl(juro)), Este))
    elif linha["acumulado"] < -0.005:
        passos.append(Markup("Os juros de empréstimo do mês foram {}. {} não absorveu juros: "
                             "já tinha passado do ponto de corte (depois do último mês com "
                             "mão de obra, o buraco deixa de contar).").format(
            _b(brl(juro)), Este))
    else:
        passos.append(Markup("Os juros de empréstimo do mês foram {}. Como não estava no "
                             "vermelho, {} não absorveu juros.").format(_b(brl(juro)), este))
    return passos


def colunas(linhas: list, campos: tuple) -> list[dict]:
    """As colunas empilhadas de um gráfico mensal: a altura de cada mês (em %
    da maior) e a fatia de cada campo dentro dela. Valor negativo (um estorno)
    não desenha — coluna não tem como descer.

    A marca embaixo da coluna é o ano, só em janeiro (e no primeiro mês): com
    oitenta meses na tela, um rótulo por coluna vira borrão."""
    totais = [sum(max(float(l.get(c) or 0), 0.0) for c in campos) for l in linhas]
    maior = max(totais, default=0.0)
    saida = []
    for i, (linha, total) in enumerate(zip(linhas, totais)):
        mes = linha["mes"]
        saida.append({
            "rotulo": linha["rotulo"], "total": total,
            "altura": round(100.0 * total / maior, 1) if maior > 0.005 else 0.0,
            "partes": [round(100.0 * max(float(linha.get(c) or 0), 0.0) / total, 1)
                       if total > 0.005 else 0.0 for c in campos],
            "marca": mes[:4] if (i == 0 or mes[5:7] == "01") else "",
        })
    return saida


# ---------------------------------------------------------------------------
# 5. O relatório: as MESMAS tabelas, para a planilha e para o PDF
# ---------------------------------------------------------------------------
COLUNAS_DO_CAMINHO = [("o_que", "O que"), ("valor", "Valor")]
COLUNAS_DO_GRUPO_BASE = ("rotulo", "grupo", "valor", "fica", "da_filial", "rateia")


def colunas_do_relatorio(dados: dict) -> dict:
    """Os títulos das colunas, com o NOME das duas estruturas no lugar de
    "principal" e "demais" — quem lê o relatório conhece BWSCE e BWSFILIAL,
    não os apelidos de dentro do código.

    Os títulos também decidem o formato: coluna com "valor", "estrutura",
    "juros", "caixa", "custo"… sai em reais; com "%", em percentual
    (`excel._e_dinheiro`)."""
    principal = dados["nome_do_lado"]["principal"]
    demais = dados["nome_do_lado"]["demais"]
    regua = "Faturamento" if dados["criterio"] == "faturamento" else "Mão de obra"
    do_lado = ("Estrutura — parte só do lado" if dados["tem_lados"]
               else "Estrutura — parte do lado")
    ficha = [
        ("rotulo", "Mês"), ("regua", f"{regua} — valor da régua"), ("pct_regua", "% da régua"),
        ("livre", "Estrutura — rateio entre todas"), ("lado", do_lado),
        ("filial", "Estrutura — custo da filial"), ("estrutura", "Estrutura absorvida"),
        ("caixa", "Caixa do mês"), ("acumulado", "Caixa acumulado"),
        ("juros_da_empresa", "Juros da empresa"), ("buraco", "Buraco de caixa"),
        ("pct_dos_juros", "% dos juros do mês"), ("juros", "Juros absorvidos"),
    ]
    nomes = [("estrutura_nome", "Estrutura"), ("projeto", "Projeto"), ("obra", "Obra")]
    return {
        "arvore": nomes + [
            ("regua", f"{regua} — valor da régua"), ("pct_regua", "% da régua"),
            ("livre", "Estrutura — rateio entre todas"), ("lado", do_lado),
            ("filial", "Estrutura — custo da filial"), ("estrutura", "Estrutura absorvida"),
            ("juros", "Juros absorvidos"), ("absorvido", "Total absorvido"),
            ("pct_absorvido", "% do total"),
            ("resultado_direto", "Resultado direto"), ("resultado", "Resultado final"),
        ],
        "ficha": ficha,
        "obra_mes": nomes + ficha,
        "mes": [
            ("rotulo", "Mês"), ("matriz", "Valor gasto pela matriz"),
            ("fica", f"Valor que fica no {principal}"),
            ("da_filial", f"Valor que é da {demais}"),
            ("rateia", "Valor rateado entre todas"),
            ("filial", "Valor gasto pela filial"),
            ("estrutura_principal", f"Estrutura absorvida — {principal}"),
            ("estrutura_demais", f"Estrutura absorvida — {demais}"),
            ("juros_da_empresa", "Juros da empresa"),
            ("juros_principal", f"Juros absorvidos — {principal}"),
            ("juros_demais", f"Juros absorvidos — {demais}"),
            ("sem_dono", "Valor sem dono"),
        ],
        "grupo": [
            ("rotulo", "Mês"), ("grupo", "Grupo de despesa"), ("valor", "Valor do grupo"),
            ("fica", f"Valor que fica no {principal}"),
            ("da_filial", f"Valor que é da {demais}"),
            ("rateia", "Valor rateado entre todas"),
        ],
    }


_CAMPOS_DA_ARVORE = ("regua", "livre", "lado", "filial", "estrutura", "juros",
                     "absorvido", "resultado_direto", "resultado")


def _linha_de_no(no: dict) -> dict:
    linha = {c: no.get(c, 0.0) for c in _CAMPOS_DA_ARVORE}
    linha.update({
        "estrutura_nome": no.get("lado_nome", ""), "projeto": no.get("projeto", ""),
        "obra": no["nome"] if no["tipo"] == "obra" else "",
        "pct_regua": round(no.get("pct_regua", 0.0) * 100, 2),
        "pct_absorvido": round(no.get("pct_absorvido", 0.0) * 100, 2),
    })
    return linha


def _com_total(nos: list) -> list[dict]:
    """As linhas de uma lista de nós, com a linha do TOTAL embaixo — para
    conferir à mão que a soma das partes dá o todo."""
    linhas = [_linha_de_no(n) for n in nos]
    if len(linhas) > 1:
        total = {c: sum(l[c] for l in linhas) for c in _CAMPOS_DA_ARVORE}
        total.update({"estrutura_nome": "TOTAL", "projeto": "", "obra": "",
                      "pct_regua": round(sum(l["pct_regua"] for l in linhas), 2),
                      "pct_absorvido": round(sum(l["pct_absorvido"] for l in linhas), 2)})
        linhas.append(total)
    return linhas


def _linhas_de_ficha(f: dict, com_total: bool = True) -> list[dict]:
    saida = []
    for l in f["linhas"]:
        saida.append({
            "rotulo": l["rotulo"], "regua": l["regua"],
            "pct_regua": round(l["fracao"] * 100, 2),
            "livre": l["livre"], "lado": l["lado"], "filial": l["filial"],
            "estrutura": l["estrutura"], "caixa": l["caixa"],
            "acumulado": l["acumulado"], "juros_da_empresa": l["juros_da_empresa"],
            "buraco": l["buraco"],
            "pct_dos_juros": round(l["pct_dos_juros"] * 100, 2), "juros": l["juros"],
        })
    if com_total and saida:
        t = f["total"]
        saida.append({
            "rotulo": "TOTAL", "regua": t["regua"],
            "pct_regua": round(t["pct_regua"] * 100, 2),
            "livre": t["livre"], "lado": t["lado"], "filial": t["filial"],
            "estrutura": t["estrutura"], "caixa": t["caixa"],
            "acumulado": t["acumulado"], "juros_da_empresa": t["juros_da_empresa"],
            "buraco": "", "pct_dos_juros": "", "juros": t["juros"],
        })
    return saida


def linhas_do_caminho(dados: dict) -> list[dict]:
    """O quadro do caminho do dinheiro, em linhas — para a capa do relatório."""
    c = dados["caminho"]
    principal = dados["nome_do_lado"]["principal"]
    demais = dados["nome_do_lado"]["demais"]
    linhas = [{"o_que": "A matriz gastou", "valor": c["matriz"]}]
    if dados["tem_lados"]:
        linhas += [
            {"o_que": f"· fica no {principal}", "valor": c["fica"]},
            {"o_que": f"· é da {demais}", "valor": c["da_filial"]},
            {"o_que": "· rateia entre todas as obras", "valor": c["rateia"]},
        ]
    if abs(c["filial"]) > 0.005:
        linhas.append({"o_que": f"A filial gastou (só para a {demais})", "valor": c["filial"]})
    linhas.append({"o_que": "Juros de empréstimo", "valor": c["juros"]})
    for lado in ("principal", "demais"):
        p = c["por_lado"][lado]
        if not p["n_obras"]:
            continue
        linhas += [
            {"o_que": f"{p['nome']}: total absorvido ({p['n_obras']} obras)", "valor": p["absorvido"]},
            {"o_que": "· de estrutura", "valor": p["estrutura"]},
            {"o_que": "· de juros", "valor": p["juros"]},
        ]
    if abs(c["sem_dono"]) > 0.005:
        linhas.append({"o_que": "Ficou sem dono (ver a tela de Resultado)", "valor": c["sem_dono"]})
    if abs(c["juros_fora"]) > 0.005:
        linhas.append({"o_que": "Juros em obras fora desta prestação", "valor": c["juros_fora"]})
    linhas.append({"o_que": ("Diferença (a conta fecha)" if c["fecha"]
                             else "A CONTA NÃO FECHA — diferença"),
                   "valor": round(c["diferenca"], 2)})
    return linhas


def _nome_de_aba(prefixo: str, nome: str, usados: set) -> str:
    """Nome de aba do Excel: até 31 caracteres, sem os sinais que ele recusa,
    e sem repetir — duas obras com o mesmo começo de nome viram "(2)"."""
    limpo = "".join("-" if ch in '[]:*?/\\' else ch for ch in f"{prefixo} {nome}").strip()
    base, n, tentativa = limpo[:31], 1, limpo[:31]
    while tentativa.lower() in usados:
        n += 1
        sufixo = f" ({n})"
        tentativa = base[:31 - len(sufixo)] + sufixo
    usados.add(tentativa.lower())
    return tentativa


_PREFIXO = {"estrutura": "Estrutura", "projeto": "Projeto", "obra": "Obra"}


def abas(dados: dict, calculo: dict, chave: str = "",
         ficha_de_cada_obra: bool = False) -> list[tuple]:
    """As abas do relatório, no formato de `excel.montar` e `pdf.montar`.

    Sem `chave` é a empresa inteira: as três visões, a empresa mês a mês, a
    matriz por grupo de despesa, a ficha de cada estrutura e de cada projeto,
    e todas as obras mês a mês numa lista só. Com `ficha_de_cada_obra` a lista
    dá lugar a uma ficha por obra — é o PDF "com tudo", que o dono pediu
    ("um relatório mais amplo, mais completo"); demora mais para montar, por
    isso é uma escolha de quem baixa, não o padrão.

    Com `chave` (uma estrutura, um projeto ou uma obra) é o recorte dela: a
    ficha do nó e a de cada filho direto."""
    C = colunas_do_relatorio(dados)
    usados: set = set()
    saida = [("Caminho do dinheiro", COLUNAS_DO_CAMINHO, linhas_do_caminho(dados))]

    def _ficha(no):
        return (_nome_de_aba(_PREFIXO[no["tipo"]], no["nome"], usados), C["ficha"],
                _linhas_de_ficha(ficha(dados, no)))

    no = achar(dados, chave) if chave else None
    if no is not None:
        # a primeira linha é o próprio nó — ela é o total das de baixo
        saida.append(("Resumo", C["arvore"],
                      [_linha_de_no(no)] + [_linha_de_no(f) for f in no["filhos"]]))
        saida.append(_ficha(no))
        for filho in no["filhos"]:
            saida.append(_ficha(filho))
        return saida

    if dados["arvores"]["estrutura"]:
        saida.append(("Por estrutura", C["arvore"], _com_total(dados["arvores"]["estrutura"])))
    saida.append(("Por projeto", C["arvore"], _com_total(dados["arvores"]["projeto"])))
    saida.append(("Por obra", C["arvore"], _com_total(dados["arvores"]["obra"])))
    saida.append(("Empresa mes a mes", C["mes"], dados["meses"]))

    por_grupo = (calculo["rateio"].get("detalhe") or {}).get("por_grupo_mes") or {}
    saida.append(("Matriz por grupo e mes", C["grupo"], [
        {"rotulo": _rotulo(mes), "grupo": grupo, "valor": -g["valor"],
         "fica": -g["fica"], "da_filial": -g["filial"],
         "rateia": -(g["valor"] - g["fica"] - g["filial"])}
        for (mes, grupo), g in sorted(por_grupo.items()) if abs(g["valor"]) > 0.005]))

    for n in dados["arvores"]["estrutura"]:
        saida.append(_ficha(n))
    # o projeto principal É a estrutura principal: a ficha dele já saiu acima
    ja_saiu = {tuple(n["obras"]) for n in dados["arvores"]["estrutura"]}
    for n in dados["arvores"]["projeto"]:
        if tuple(n["obras"]) not in ja_saiu:
            saida.append(_ficha(n))

    em_ordem = sorted(dados["arvores"]["obra"],
                      key=lambda n: (n["qual_lado"] != "principal", n["projeto"], n["nome"]))
    if ficha_de_cada_obra:
        for n in em_ordem:
            saida.append(_ficha(n))
        return saida
    obra_mes = []
    for n in em_ordem:
        for linha in _linhas_de_ficha(ficha(dados, n), com_total=False):
            obra_mes.append(dict(linha, estrutura_nome=n["lado_nome"],
                                 projeto=n["projeto"], obra=n["nome"]))
    saida.append(("Obra mes a mes", C["obra_mes"], obra_mes))
    return saida
