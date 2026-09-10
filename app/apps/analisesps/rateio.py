# -*- coding: utf-8 -*-
"""
rateio.py — gera os JSONs de rateio para atualizar título no Omie.

Portado do Apps Script `gerarRateiosJSON`:
  - Situação 1 (Centro de Custo): "distribuicao" com percentuais (nValDep:null).
  - Situação 2 (Categoria de Despesa): "categorias" com percentual E valor,
    distribuído sobre uma base (valor informado ou a soma dos valores).
Ambos usam arredondamento de "menor erro" (maior resto) para fechar 100% / a base.
"""

import math
import re


def _percentuais_min_erro(valores, casas: int = 7):
    total = sum(valores)
    n = len(valores)
    if not (total > 0):
        return [0.0] * n
    scale = 10 ** casas
    raws = [v / total * 100 for v in valores]
    base = [math.floor(r * scale) / scale for r in raws]
    soma_base = sum(base)
    delta = round(100 - soma_base, casas + 3)
    steps = round(delta * scale)
    idxs = [[i, (r * scale) - math.floor(r * scale)] for i, r in enumerate(raws)]
    if steps > 0:
        idxs.sort(key=lambda a: a[1], reverse=True)
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            base[j] = round(base[j] + 1 / scale, casas)
    elif steps < 0:
        steps = -steps
        idxs.sort(key=lambda a: a[1])
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            base[j] = round(base[j] - 1 / scale, casas)
    return [round(x, casas) for x in base]


def _alocar_valores(percentuais, base, casas: int = 2):
    scale = 10 ** casas
    raws = [base * (p / 100) for p in percentuais]
    btrunc = [math.floor(r * scale) / scale for r in raws]
    soma_base = sum(btrunc)
    delta = round(base - soma_base, casas + 3)
    steps = round(delta * scale)
    idxs = [[i, (r * scale) - math.floor(r * scale)] for i, r in enumerate(raws)]
    if steps > 0:
        idxs.sort(key=lambda a: a[1], reverse=True)
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            btrunc[j] = round(btrunc[j] + 1 / scale, casas)
    elif steps < 0:
        steps = -steps
        idxs.sort(key=lambda a: a[1])
        for k in range(steps):
            j = idxs[k % len(idxs)][0]
            btrunc[j] = round(btrunc[j] - 1 / scale, casas)
    return [round(x, casas) for x in btrunc]


def _num(x, casas: int) -> str:
    """Número 'enxuto' como o Number(x.toFixed(n)) do JS: sem zeros à toa."""
    x = round(float(x), casas)
    s = ("%.*f" % (casas, x)).rstrip("0").rstrip(".")
    return s if s not in ("", "-0", "-") else "0"


def _to_float(v) -> float:
    """Interpreta valores em padrão CONTÁBIL BR ('1.234,56', 'R$ 994,12', '1.234',
    '(1.000,00)' = negativo) e também colagens em padrão US ('1,234.56')."""
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return 0.0 if (isinstance(v, float) and math.isnan(v)) else float(v)
    s = str(v).strip().replace("\u00a0", "").replace(" ", "")
    s = s.replace("R$", "").replace("r$", "")
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg, s = True, s[1:-1]
    if s.startswith("-"):
        neg, s = True, s[1:]
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):          # BR: 1.234,56
            s = s.replace(".", "").replace(",", ".")
        else:                                     # US: 1,234.56
            s = s.replace(",", "")
    elif "," in s:
        # vírgula única = decimal BR; várias = milhar US (1,234,567)
        s = s.replace(",", ".") if s.count(",") == 1 else s.replace(",", "")
    elif "." in s:
        # só ponto: grupos de 3 = milhar BR (1.234 / 1.234.567); senão decimal
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
            s = s.replace(".", "")
    try:
        x = float(s)
    except ValueError:
        return 0.0
    return -x if neg else x


def gerar_jsons(linhas_cc, linhas_cat, base_cat=None) -> dict:
    """
    linhas_cc:  [{"obra","codigo","valor"}]  (Situação 1)
    linhas_cat: [{"categoria","codigo","valor"}] (Situação 2)
    base_cat:   valor a ratear na categoria (F5); se None/0 usa a soma.
    Retorna {"distribuicao": str|None, "categorias": str|None, "erro": str|None}.
    """
    map_cc = {}
    for l in linhas_cc or []:
        cod = str(l.get("codigo", "")).strip()
        if not cod:
            continue
        obra = str(l.get("obra", "")).strip()
        key = (obra + "||" + cod).upper()
        if key not in map_cc:
            map_cc[key] = {"obra": obra, "codigo": cod, "valor": 0.0}
        map_cc[key]["valor"] += _to_float(l.get("valor"))
    lcc = list(map_cc.values())

    map_cat = {}
    for l in linhas_cat or []:
        cod = str(l.get("codigo", "")).strip()
        if not cod:
            continue
        cat = str(l.get("categoria", "")).strip()
        key = (cat + "||" + cod).upper()
        if key not in map_cat:
            map_cat[key] = {"categoria": cat, "codigo": cod, "valor": 0.0}
        map_cat[key]["valor"] += _to_float(l.get("valor"))
    lcat = list(map_cat.values())

    gerar_s1 = len(lcc) > 0
    gerar_s2 = len(lcat) > 0 and gerar_s1

    if not gerar_s1:
        if lcat:
            return {"distribuicao": None, "categorias": None,
                    "erro": "A Categoria só é gerada junto com o Centro de Custo. "
                            "Preencha ao menos um Centro de Custo (com código)."}
        return {"distribuicao": None, "categorias": None,
                "erro": "Nada a gerar: informe ao menos um Centro de Custo com código."}

    out = {"distribuicao": None, "categorias": None, "erro": None}

    perc1 = _percentuais_min_erro([l["valor"] for l in lcc], 7)
    partes1 = ['{"cCodDep":"%s","cDesDep":"%s","nPerDep":%s,"nValDep":null}'
               % (l["codigo"], l["obra"].replace('"', '\\"'), _num(perc1[i], 7))
               for i, l in enumerate(lcc)]
    out["distribuicao"] = '"distribuicao":\n[' + ",".join(partes1) + ']'

    if gerar_s2:
        perc2 = _percentuais_min_erro([l["valor"] for l in lcat], 7)
        soma_f = sum(l["valor"] for l in lcat)
        base_cat = _to_float(base_cat) if base_cat not in (None, "") else None
        base_val = base_cat if (base_cat and base_cat > 0) else soma_f
        vals = _alocar_valores(perc2, base_val, 2)
        partes2 = ['{"codigo_categoria":"%s","percentual":%s,"valor":%s}'
                   % (l["codigo"], _num(perc2[i], 7), _num(vals[i], 2))
                   for i, l in enumerate(lcat)]
        out["categorias"] = '"categorias":\n[' + ",".join(partes2) + ']'

    return out


# ---------------------------------------------------------------------------
# COLAR UMA TABELA DA PLANILHA
#
# "Imagina que eu tenho trinta obras para ratear. Se eu for colocar uma a uma
# é trabalhoso, e essa informação normalmente vem de uma planilha do Excel."
# Pedido do dono em 10/09/2026.
#
# A interpretação fica AQUI, e não no navegador, por um motivo aprendido nesta
# mesma sessão: o que roda no navegador esta máquina não consegue exercitar, e
# um rateio errado não avisa — ele vira lançamento no lugar errado no Omie.
# Aqui tem teste de verdade, e reusa o `_to_float`, que já sabe ler
# "1.234,56", "R$ 994,12" e até colagem em padrão americano.
#
# O QUE NUNCA SE FAZ AQUI: adivinhar. Nome que não bate com nenhuma obra
# conhecida NÃO entra — volta escrito na tela para a pessoa resolver. Toda
# interpretação que não seja o nome exato aparece no recado, porque acertar a
# obra errada é pior do que não achar nenhuma.
# ---------------------------------------------------------------------------
SEPARADOR_DE_COLUNA = re.compile(r"\t+|;|\s{2,}")

# Cabeçalho colado junto — some sem reclamar, porque é o caso comum de quem
# seleciona a tabela inteira no Excel.
CABECALHOS = {
    # a coluna do nome
    "obra", "obras", "centro de custo", "centrodecusto", "centro custo", "cc",
    "categoria", "categorias", "categoria de despesa", "descricao",
    "plano financeiro", "codigo primario", "item", "conta",
    # a coluna do valor
    "valor", "valores", "vlr", "total", "montante", "r$", "valor (r$)",
    # a do código, quando vem junto
    "codigo", "codigo omie",
}


def _sem_acento(texto: str) -> str:
    import unicodedata
    sem = unicodedata.normalize("NFKD", str(texto or ""))
    return "".join(c for c in sem if not unicodedata.combining(c))


def _arrumar(texto: str) -> str:
    """A forma usada para COMPARAR nomes: sem acento, sem caixa, sem espaço
    sobrando. "Obra São João" e "obra sao joao " são a mesma obra para quem
    copia de uma planilha."""
    return " ".join(_sem_acento(texto).split()).strip().lower()


def _parece_valor(pedaco: str) -> bool:
    """Este pedaço da linha é o valor, e não parte do nome?

    Exige pelo menos um dígito e só caracteres de número — assim "OBRA-12" não
    é confundido com valor, e "R$ 1.234,56" é."""
    limpo = str(pedaco or "").strip()
    if not limpo:
        return False
    limpo = limpo.replace("R$", "").replace("r$", "").strip()
    return bool(re.fullmatch(r"[-+()\d.,\s ]+", limpo)) and any(
        c.isdigit() for c in limpo)


def _quebrar_linha(linha: str) -> tuple[str, str]:
    """Separa a linha em (nome, valor).

    O VALOR É O ÚLTIMO PEDAÇO QUE PARECE NÚMERO, e o nome é tudo o que vem
    antes. É isso que faz funcionar com qualquer separador — a tabulação do
    Excel, o ponto e vírgula do CSV, ou dois espaços — e com nome que tem
    espaço no meio, que é a regra e não a exceção ("CRECHE SWAP 3")."""
    pedacos = [p.strip() for p in SEPARADOR_DE_COLUNA.split(linha) if p.strip()]
    if len(pedacos) == 1:
        # Sem separador claro: tenta o último espaço simples ("OBRA-1 1.000,00").
        partes = pedacos[0].rsplit(" ", 1)
        if len(partes) == 2 and _parece_valor(partes[1]):
            return partes[0].strip(), partes[1].strip()
        return pedacos[0], ""
    if _parece_valor(pedacos[-1]):
        return " ".join(pedacos[:-1]).strip(), pedacos[-1]
    return " ".join(pedacos).strip(), ""


def interpretar_colagem(texto: str, referencias: list) -> dict:
    """Lê uma tabela colada e devolve as linhas prontas para a tela.

    `referencias` é a lista de {"nome", "codigo"} da obra ou da categoria.

    Devolve {"linhas": [{"nome", "valor"}], "avisos": [str], "achadas": int}:
      - `linhas` são as que casaram com uma obra/categoria conhecida, com o
        valor já em texto brasileiro para voltar ao campo;
      - `avisos` é tudo o que a pessoa precisa conferir — o que não bateu, o
        que bateu por caminho indireto, e o que apareceu duas vezes.
    """
    conhecidas = [{"nome": str(r.get("nome") or ""),
                   "codigo": str(r.get("codigo") or "")} for r in referencias]
    por_nome = {_arrumar(r["nome"]): r["nome"] for r in conhecidas}
    por_codigo = {_arrumar(r["codigo"]): r["nome"]
                  for r in conhecidas if r["codigo"]}

    linhas: list[dict] = []
    avisos: list[str] = []
    ja_vistas: set[str] = set()

    for numero, bruta in enumerate(str(texto or "").splitlines(), start=1):
        if not bruta.strip():
            continue
        nome_cru, valor_cru = _quebrar_linha(bruta)
        arrumado = _arrumar(nome_cru)

        if not valor_cru:
            # CABEÇALHO COLADO JUNTO SOME SEM RECLAMAR: quem seleciona a tabela
            # no Excel pega o cabeçalho junto, e reclamar disso seria implicar
            # com o jeito normal de copiar. Vale só quando TODOS os pedaços da
            # linha são palavra de cabeçalho — assim uma obra de verdade sem
            # valor continua sendo apontada.
            pedacos = [_arrumar(p) for p in SEPARADOR_DE_COLUNA.split(bruta)
                       if p.strip()]
            if not arrumado or (pedacos and all(p in CABECALHOS for p in pedacos)):
                continue
            avisos.append(f'linha {numero}: não achei o valor em "{bruta.strip()}".')
            continue

        valor = _to_float(valor_cru)
        if valor <= 0:
            avisos.append(f'linha {numero}: o valor "{valor_cru}" não é um '
                          "número positivo.")
            continue

        # 1. O nome exato — o caminho normal, e o único silencioso.
        nome = por_nome.get(arrumado)

        # 2. O código do Omie no lugar do nome. Acontece com quem monta a
        #    tabela a partir do relatório do Omie, e é seguro: código é único.
        if nome is None and arrumado in por_codigo:
            nome = por_codigo[arrumado]
            avisos.append(f'linha {numero}: entendi o código "{nome_cru}" '
                          f'como "{nome}".')

        # 3. O nome pela metade. Resolve os dois casos reais de quem monta a
        #    tabela à mão: escrever "OBRA-12" quando a lista tem
        #    "OBRA-12 - CRECHE SWAP", e o contrário.
        #
        #    A COMPARAÇÃO É POR COMEÇO, e nunca "um contém o outro" — foi o
        #    primeiro jeito que escrevi e ele casava "OBRA-1" com "OBRA-12",
        #    que é exatamente o erro que não pode acontecer: rateio na obra
        #    errada o Omie aceita sem reclamar.
        #
        #    Em qualquer dúvida NÃO ESCOLHE: reclama e deixa a linha de fora.
        if nome is None and len(arrumado) >= 3:
            comecam_com_ele = sorted(k for k in por_nome if k.startswith(arrumado))
            if len(comecam_com_ele) == 1:
                nome = por_nome[comecam_com_ele[0]]
            elif len(comecam_com_ele) > 1:
                quais = ", ".join(por_nome[k] for k in comecam_com_ele[:3])
                avisos.append(f'linha {numero}: "{nome_cru}" combina com mais '
                              f"de uma ({quais}…). Escreva o nome inteiro.")
                continue
            else:
                # O colado é mais completo que o nome da lista. Vale o nome
                # MAIS LONGO que couber no começo dele — "OBRA-12" ganha de
                # "OBRA-1" —, e só se esse mais longo for único.
                cabem = sorted((k for k in por_nome if arrumado.startswith(k)),
                               key=len, reverse=True)
                if len(cabem) == 1 or (len(cabem) > 1
                                       and len(cabem[0]) > len(cabem[1])):
                    nome = por_nome[cabem[0]]
                elif len(cabem) > 1:
                    quais = ", ".join(por_nome[k] for k in cabem[:3])
                    avisos.append(f'linha {numero}: "{nome_cru}" combina com '
                                  f"mais de uma ({quais}…). Escreva o nome "
                                  "exato da lista.")
                    continue
            if nome is not None:
                avisos.append(f'linha {numero}: entendi "{nome_cru}" como '
                              f'"{nome}" — confira.')

        if nome is None:
            avisos.append(f'linha {numero}: não encontrei "{nome_cru}" na '
                          "lista. Ela não entrou.")
            continue

        if nome in ja_vistas:
            avisos.append(f'linha {numero}: "{nome}" aparece mais de uma vez — '
                          "deixei as duas, confira se é isso mesmo.")
        ja_vistas.add(nome)
        # Volta ao campo com as duas casas, como dinheiro se escreve: "1000,50"
        # e não "1000,5". O `_to_float` lê os dois, mas quem confere na tela lê
        # melhor o primeiro.
        linhas.append({"nome": nome, "valor": f"{valor:.2f}".replace(".", ",")})

    return {"linhas": linhas, "avisos": avisos, "achadas": len(linhas)}
