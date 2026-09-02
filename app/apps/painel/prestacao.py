# -*- coding: utf-8 -*-
"""
Prestação de Contas entre sócios — a conta.

O que esta tela responde: **quanto do resultado de cada obra cabe a cada
sócio.** Três coisas acontecem, nesta ordem:

1. **Rateio administrativo.** O custo da matriz e da filial não é de nenhuma
   obra em particular, mas é real. Ele é distribuído entre as obras na
   proporção do custo de pessoal de cada uma — obra com mais gente consome mais
   estrutura. Quais despesas entram, em que percentual e em que período, é
   você quem decide, nas regras.

2. **Apuração por obra.** Receita líquida menos despesas diretas dá o resultado
   direto; somando o rateio recebido, o resultado com estrutura.

3. **Divisão entre sócios.** Num projeto só da BWS, cada sócio leva seu
   percentual do resultado com rateio. Num projeto **com sócio externo** a
   conta muda, e essa é a parte que merece atenção — está explicada em
   `quotas_por_socio`.

A conta mora aqui, longe do HTML e do SQL, para poder ser conferida sozinha.
Nada aqui sabe de banco: entram listas de dicionários, saem listas de
dicionários. É a mesma regra que rodava no computador, sem pandas.
"""
from __future__ import annotations

import json

MATRIZ, FILIAL = "MATRIZ", "FILIAL"


# ---------------------------------------------------------------------------
# 1. Quem é obra, quem é estrutura
# ---------------------------------------------------------------------------
def classificar_obras(apuracao, config) -> dict:
    """Obra -> (projeto, lado). Os departamentos administrativos ficam de fora:
    eles são a estrutura que vai ser rateada, não destino de rateio.

    O lado (matriz ou filial) sai do projeto: o que é do projeto da matriz é
    matriz, o resto é filial. É o que permite uma regra dizer "isto só se
    divide entre as obras da filial"."""
    admin = {config["depto_admin_matriz"], config["depto_admin_filial"]}
    contagem: dict[str, dict[str, int]] = {}
    for linha in apuracao:
        obra = (linha["obra"] or "").strip()
        if not obra or obra in admin or obra.lower() == "nan":
            continue
        projeto = (linha["projeto"] or "").strip()
        contagem.setdefault(obra, {})
        contagem[obra][projeto] = contagem[obra].get(projeto, 0) + 1

    obras = {}
    for obra, projetos in contagem.items():
        # projeto dominante: o que mais aparece nos lançamentos da obra
        projeto = max(projetos.items(), key=lambda kv: (kv[1], kv[0]))[0]
        lado = MATRIZ if projeto == config["projeto_matriz"] else FILIAL
        obras[obra] = {"projeto": projeto, "lado": lado}
    return obras


def _dentro_da_vigencia(mes: str, mes_inicial: str, mes_final: str) -> bool:
    """Regras podem valer só num período — 'isto foi compartilhado até out/25'."""
    inicial, final = (mes_inicial or "").strip(), (mes_final or "").strip()
    if inicial and mes < inicial:
        return False
    if final and mes > final:
        return False
    return True


# ---------------------------------------------------------------------------
# 2. O rateio administrativo
# ---------------------------------------------------------------------------
def calcular_rateio(despesa_admin, pessoal, obras, regras, config) -> dict:
    """Distribui o custo administrativo entre as obras.

    Devolve as alocações — (obra, mês) -> valor — e as **sobras**: o que não
    coube em obra nenhuma, com o motivo. A sobra não é detalhe: ela é custo da
    empresa que ficou sem dono, e a tela mostra isso em vez de esconder.
    """
    # driver: custo de pessoal por (mês, obra), só das obras
    driver: dict[str, dict[str, float]] = {}
    for mes, obra, valor in pessoal:
        if obra in obras and valor > 0:
            driver.setdefault(mes, {})[obra] = driver.get(mes, {}).get(obra, 0.0) + valor

    alocacoes: dict[tuple, float] = {}
    sobras: list[dict] = []
    capturado: dict[tuple, float] = {}     # (depto admin, mês) -> já pego por regra

    SEM_DATA = "(sem data)"

    def _distribuir(mes: str, valor: float, escopo: str, origem: str):
        """Divide um valor entre as obras do escopo, na proporção do pessoal."""
        if abs(valor) <= 0.005:
            return
        if mes == SEM_DATA:
            # O valor é real e conta no resultado, mas sem mês não há em que
            # proporção dividi-lo. Vira sobra, visível, com o motivo.
            sobras.append({"origem": origem, "mes": mes, "valor": valor,
                           "motivo": "lançamento sem data: não dá para ratear por mês"})
            return
        pesos = driver.get(mes, {})
        if escopo in (MATRIZ, FILIAL):
            pesos = {o: p for o, p in pesos.items() if obras[o]["lado"] == escopo}
        total = sum(pesos.values())
        if total <= 0.005:
            motivo = ("nenhuma obra com custo de pessoal neste mês"
                      if not pesos else f"nenhum custo de pessoal no lado {escopo}")
            sobras.append({"origem": origem, "mes": mes, "valor": valor,
                           "motivo": motivo})
            return
        for obra, peso in pesos.items():
            chave = (obra, mes)
            alocacoes[chave] = alocacoes.get(chave, 0.0) + valor * peso / total

    # ---- as regras que você escreveu ----
    for regra in regras:
        if not int(regra.get("ativo", 1)):
            continue
        grupos = set(json.loads(regra.get("grupos") or "[]"))
        categorias = set(json.loads(regra.get("categorias") or "[]"))
        todas = bool(int(regra.get("todas", 0)))
        pool: dict[str, float] = {}
        for linha in despesa_admin:
            if linha["depto"] != regra["depto"]:
                continue
            if not todas and linha["grupo"] not in grupos and linha["categoria"] not in categorias:
                continue
            if not _dentro_da_vigencia(linha["mes"], regra.get("mes_ini"),
                                       regra.get("mes_fim")):
                continue
            pool[linha["mes"]] = pool.get(linha["mes"], 0.0) + linha["valor"]

        fracao = float(regra.get("pct", 100)) / 100.0
        for mes, bruto in pool.items():
            valor = bruto * fracao
            if abs(valor) <= 0.005:
                continue
            capturado[(regra["depto"], mes)] = (
                capturado.get((regra["depto"], mes), 0.0) + valor)
            _distribuir(mes, valor, regra.get("escopo", "AMBAS"),
                        f"Regra: {regra['nome']}")

    # ---- o resíduo ----
    # O que as regras não pegaram fica 100% no próprio lado. Sem isso, parte do
    # custo administrativo sumiria da conta e o resultado da empresa mudaria só
    # por causa do rateio — o que seria errado: rateio move custo, não o cria
    # nem o apaga.
    if str(config.get("residual", "1")) == "1":
        total_admin: dict[tuple, float] = {}
        for linha in despesa_admin:
            chave = (linha["depto"], linha["mes"])
            total_admin[chave] = total_admin.get(chave, 0.0) + linha["valor"]
        for (depto, mes), total in total_admin.items():
            resto = total - capturado.get((depto, mes), 0.0)
            if abs(resto) <= 0.005:
                continue
            lado = MATRIZ if depto == config["depto_admin_matriz"] else FILIAL
            _distribuir(mes, resto, lado, f"Resíduo: {depto}")

    return {"alocacoes": alocacoes, "sobras": sobras}


# ---------------------------------------------------------------------------
# 3. A apuração por obra
# ---------------------------------------------------------------------------
def apurar(apuracao, obras, alocacoes) -> list[dict]:
    """Junta a receita e a despesa de cada obra com o rateio que ela recebeu."""
    por_chave: dict[tuple, dict] = {}
    for linha in apuracao:
        obra = (linha["obra"] or "").strip()
        if obra not in obras:
            continue
        chave = (obras[obra]["projeto"], obra, linha["mes"])
        registro = por_chave.setdefault(chave, {
            "projeto": chave[0], "obra": obra, "mes": linha["mes"],
            "receita_liquida": 0.0, "retencoes": 0.0, "despesas": 0.0, "rateio": 0.0,
        })
        registro["receita_liquida"] += linha["receita_liquida"]
        registro["retencoes"] += linha["retencoes"]
        registro["despesas"] += linha["despesas"]

    # rateio pode cair em obra/mês sem movimento próprio: a chave é criada aqui
    for (obra, mes), valor in alocacoes.items():
        if obra not in obras:
            continue
        chave = (obras[obra]["projeto"], obra, mes)
        registro = por_chave.setdefault(chave, {
            "projeto": chave[0], "obra": obra, "mes": mes,
            "receita_liquida": 0.0, "retencoes": 0.0, "despesas": 0.0, "rateio": 0.0,
        })
        registro["rateio"] += valor

    saida = []
    for registro in por_chave.values():
        registro["receita_bruta"] = registro["receita_liquida"] + registro["retencoes"]
        registro["resultado_direto"] = registro["receita_liquida"] + registro["despesas"]
        registro["resultado"] = registro["resultado_direto"] + registro["rateio"]
        saida.append(registro)
    saida.sort(key=lambda r: (r["projeto"], r["obra"], r["mes"]))
    return saida


def totalizar_por_projeto(apurado) -> dict:
    """Soma a apuração por projeto — é o nível em que os sócios participam."""
    campos = ("receita_bruta", "receita_liquida", "retencoes", "despesas",
              "rateio", "resultado_direto", "resultado")
    total: dict[str, dict] = {}
    for linha in apurado:
        alvo = total.setdefault(linha["projeto"], {c: 0.0 for c in campos})
        for campo in campos:
            alvo[campo] += linha[campo]
    return total


# ---------------------------------------------------------------------------
# 4. A divisão entre os sócios
# ---------------------------------------------------------------------------
def quotas_por_socio(por_projeto, participacoes, config) -> list[dict]:
    """Quanto cabe a cada sócio, projeto a projeto.

    **Projeto só da BWS:** simples — cada um leva o seu percentual do resultado
    com rateio.

    **Projeto com sócio externo:** a conta é outra, e a razão é justa. O sócio
    externo entrou na obra, não na BWS: não é ele quem paga a estrutura
    administrativa da construtora. Então:

      - cobra-se da parceria uma **taxa de administração** (um percentual da
        receita bruta). Para a parceria é custo; para os sócios da BWS é receita;
      - a base que todos dividem é o resultado **direto** menos essa taxa;
      - o rateio administrativo da obra e a taxa cobrada voltam **só para os
        sócios internos**, na proporção entre eles.

    O resultado disso: a soma de todas as quotas fecha com o resultado do
    projeto. Ninguém some, e nada aparece duas vezes.
    """
    try:
        taxa = float(str(config.get("taxa_adm_pct", "1.5")).replace(",", ".")) / 100.0
    except ValueError:
        taxa = 0.0

    tem_externo = {p["projeto"] for p in participacoes
                   if (p.get("tipo") or "").lower() == "externo"}
    soma_interna: dict[str, float] = {}
    for p in participacoes:
        if (p.get("tipo") or "").lower() != "externo":
            soma_interna[p["projeto"]] = soma_interna.get(p["projeto"], 0.0) + float(p["pct"])

    saida = []
    for p in participacoes:
        projeto = p["projeto"]
        numeros = por_projeto.get(projeto)
        if not numeros:
            continue
        externo = (p.get("tipo") or "").lower() == "externo"
        fracao = float(p["pct"]) / 100.0
        taxa_adm = taxa * numeros["receita_bruta"]

        if projeto in tem_externo:
            base = numeros["resultado_direto"] - taxa_adm
            quota = base * fracao
            credito = 0.0
            if not externo:
                interna = soma_interna.get(projeto, 0.0)
                proporcao = (float(p["pct"]) / interna) if interna > 0 else 0.0
                credito = (taxa_adm + numeros["rateio"]) * proporcao
                quota += credito
            visao = ("Parceria — resultado direto menos taxa de administração"
                     if externo else
                     "Parceria, lado BWS — mais a taxa e o rateio")
        else:
            base = numeros["resultado"]
            quota = base * fracao
            credito = 0.0
            visao = "Projeto só da BWS — resultado com rateio"

        saida.append({
            "socio": p["socio"], "tipo": p.get("tipo") or "Interno",
            "projeto": projeto, "pct": float(p["pct"]),
            "base": round(base, 2), "quota": round(quota, 2),
            "credito_bws": round(credito, 2), "visao": visao,
            "taxa_adm": round(taxa_adm, 2),
            "resultado": numeros["resultado"],
            "resultado_direto": numeros["resultado_direto"],
            "rateio": numeros["rateio"],
        })
    saida.sort(key=lambda q: (q["socio"], q["projeto"]))
    return saida


def efeito_do_ajuste(tipo: str, valor: float) -> float:
    """Como cada tipo de ajuste manual mexe na posição do sócio.

    "Valor percebido" é dinheiro que ele já tirou: abate. "Dívida assumida" é
    obrigação que ele pegou para si: soma. "Outro" vai como veio, com o sinal
    que a pessoa digitou."""
    try:
        v = float(valor)
    except (TypeError, ValueError):
        return 0.0
    if tipo.startswith("Valor Percebido"):
        return -abs(v)
    if tipo.startswith("Dívida Assumida"):
        return abs(v)
    return v


def posicao_dos_socios(quotas, ajustes) -> list[dict]:
    """A posição final de cada sócio: quanto lhe cabe, menos o que já recebeu."""
    posicao: dict[str, dict] = {}
    for q in quotas:
        alvo = posicao.setdefault(q["socio"], {
            "socio": q["socio"], "tipo": q["tipo"], "quota": 0.0,
            "ajustes": 0.0, "projetos": 0})
        alvo["quota"] += q["quota"]
        alvo["projetos"] += 1
    for a in ajustes:
        alvo = posicao.setdefault(a["socio"], {
            "socio": a["socio"], "tipo": "Interno", "quota": 0.0,
            "ajustes": 0.0, "projetos": 0})
        alvo["ajustes"] += efeito_do_ajuste(a["tipo"], a["valor"])
    for alvo in posicao.values():
        alvo["saldo"] = round(alvo["quota"] + alvo["ajustes"], 2)
        alvo["quota"] = round(alvo["quota"], 2)
        alvo["ajustes"] = round(alvo["ajustes"], 2)
    return sorted(posicao.values(), key=lambda p: -p["saldo"])
