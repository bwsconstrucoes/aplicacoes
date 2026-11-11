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
import re

MATRIZ, FILIAL = "MATRIZ", "FILIAL"
SEM_DATA = "(sem data)"


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
# 2b. Os juros de empréstimo: quem demandou o caixa é quem paga
# ---------------------------------------------------------------------------
# A régua aqui é OUTRA, e a diferença é o ponto:
#
# - o custo da estrutura se reparte pelo PESSOAL — obra com mais gente consome
#   mais administração, gaste ela caixa ou não;
# - o juro de empréstimo se reparte pelo DÉFICIT — ele não é estrutura, é o
#   preço de faltar dinheiro. Obra que se paga sozinha não deveria pagar juro
#   nenhum; obra que só anda com dinheiro emprestado paga o do mês em que
#   estava no vermelho.
#
# E não há circularidade: o juro é alocado pelo déficit de ANTES do juro.

CATEGORIA_JUROS_PADRAO = "Juros sobre Empréstimos"

# O que fazer com o juro de um mês em que NINGUÉM estava negativo.
SEM_DEFICIT = {
    "sobra": "ninguém paga — fica visível como custo sem dono",
    "estrutura": "segue a mesma régua da estrutura naquele mês",
}


def categorias_de_juros(configurado) -> set:
    """As categorias que contam como juro de empréstimo, em minúsculas.

    Aceita VÁRIAS, separadas por ponto-e-vírgula: no OMIE o juro pode estar em
    mais de um nome ("Juros sobre Empréstimos", "Juros Bancários", "IOF"…), e
    o dono viu 1,6 milhão na controladoria contra 191 mil aqui (22/09/2026)
    — parte da diferença é nome que a configuração não alcançava."""
    if isinstance(configurado, (list, tuple, set)):
        partes = configurado
    else:
        partes = str(configurado or "").split(";")
    return {p.strip().lower() for p in partes if p and p.strip()}


def separar_juros(despesa_admin, categoria_juros):
    """Tira do bolo da estrutura o que for juro de empréstimo.

    Sem isto o juro seria rateado DUAS vezes — uma pelo pessoal, junto com o
    resto da administração, e outra pelo déficit. Devolve (resto, juros por
    mês), e o resto é o que as regras de rateio vão continuar dividindo.

    Juro lançado direto numa obra não passa por aqui: ele já é despesa daquela
    obra, e mexer nisso seria tirar de quem o assumiu."""
    alvos = categorias_de_juros(categoria_juros)
    if not alvos:
        return list(despesa_admin), {}
    resto, juros = [], {}
    for linha in despesa_admin:
        if (linha.get("categoria") or "").strip().lower() in alvos:
            juros[linha["mes"]] = juros.get(linha["mes"], 0.0) + linha["valor"]
        else:
            resto.append(linha)
    return resto, juros


def _meses_ordenados(*conjuntos) -> list[str]:
    """Todos os meses envolvidos, do mais antigo ao mais novo, sem o sem-data.

    O formato é 'AAAA-MM', então a ordem alfabética JÁ é a cronológica."""
    meses = set()
    for conjunto in conjuntos:
        meses |= {m for m in conjunto if m and m != SEM_DATA}
    return sorted(meses)


def tirar_juros_das_excecoes(excecoes, categoria_juros) -> list:
    """Os lançamentos marcados um a um, sem os de juro de empréstimo.

    11/11/2026, o dono: *"acho que o juro de empréstimo está sendo contado duas
    vezes, dentro de rateio recebido e em juros de empréstimo"*. Estava, quando
    o lançamento de juro tinha percentual próprio na árvore: `separar_juros`
    o tirava do agregado e o levava para o déficit, mas a exceção entrava no
    bolo do rateio do mesmo jeito. O juro segue SÓ a régua do déficit."""
    alvos = categorias_de_juros(categoria_juros)
    return [e for e in (excecoes or [])
            if (e.get("categoria") or "").strip().lower() not in alvos]


def alocar_juros_por_deficit(caixa_por_obra_mes, juros_por_mes, obras, *,
                             rateio_recebido=None,
                             sem_deficit: str = "sobra",
                             corte=None, lado_de=None) -> dict:
    """Divide o juro de cada mês entre as obras que estavam no vermelho nele.

    Proporcional ao tamanho do buraco: quem devia o dobro paga o dobro.

    Devolve as alocações — (obra, mês) -> valor, negativo como toda despesa —,
    as **sobras** (juro que não coube em obra nenhuma, com o motivo) e a
    **memória** mês a mês, para a conta poder ser conferida linha por linha em
    vez de acreditada.

    `corte` é {obra: último mês em que ela ainda paga juro} — ver
    `corte_dos_juros`. Depois dele o buraco da obra deixa de contar: obra
    encerrada no prejuízo não fica pagando juro para sempre.

    `lado_de` é {obra: "principal" | "demais"}. Com ele, O CAIXA É DE CADA
    LADO (dono, 10/11/2026): "o caixa gerado em BWSCE dá suporte a obras que
    ainda não receberam de um lado. E do outro lado, todos os demais projetos
    se ajudam — o caixa gerado pelas obras serve para cobrir o buraco de obra
    que ainda não recebeu". Então o que demanda dinheiro emprestado é o buraco
    LÍQUIDO do lado: soma-se o acumulado de todas as obras dele, as que sobram
    cobrindo as que faltam. O juro do mês se reparte entre os lados pelo buraco
    líquido de cada um e, dentro do lado, entre as obras no vermelho, pelo
    tamanho do buraco de cada uma. Lado que se cobre sozinho não paga juro."""
    corte = corte or {}
    if sem_deficit not in SEM_DEFICIT:
        sem_deficit = "sobra"

    # O acumulado de cada obra, mês a mês, agora sem buracos: o déficit de um
    # mês sem movimento é o mesmo do mês anterior.
    saldo: dict[str, dict[str, float]] = {}
    for mes, obra, valor in caixa_por_obra_mes:
        if obra in obras and mes != SEM_DATA:
            saldo.setdefault(obra, {})
            saldo[obra][mes] = saldo[obra].get(mes, 0.0) + float(valor or 0)
    for (obra, mes), valor in (rateio_recebido or {}).items():
        if obra in obras and mes != SEM_DATA:
            saldo.setdefault(obra, {})
            saldo[obra][mes] = saldo[obra].get(mes, 0.0) + float(valor or 0)

    meses = _meses_ordenados({m for serie in saldo.values() for m in serie},
                             set(juros_por_mes))
    acumulado = {obra: 0.0 for obra in saldo}
    alocacoes: dict[tuple, float] = {}
    sobras: list[dict] = []
    memoria: list[dict] = []

    rateio_do_mes: dict[str, dict[str, float]] = {}
    for (obra, mes), valor in (rateio_recebido or {}).items():
        if obra in obras:
            rateio_do_mes.setdefault(mes, {})[obra] = (
                rateio_do_mes.get(mes, {}).get(obra, 0.0) + abs(float(valor or 0)))

    for mes in meses:
        for obra, serie in saldo.items():
            acumulado[obra] += serie.get(mes, 0.0)
        def _ativa(o, _mes=mes):
            return not (corte.get(o) and _mes > corte[o])

        deficits = {o: -v for o, v in acumulado.items()
                    if v < -0.005 and _ativa(o)}
        juro = juros_por_mes.get(mes, 0.0)

        pesos, criterio = deficits, "déficit de caixa"
        necessidade: dict[str, float] = {}
        if lado_de is not None:
            # o buraco LÍQUIDO de cada lado: as obras que sobram cobrem as que
            # faltam. O buraco de obra já encerrada (depois do corte) não
            # conta; o que ela deixou de sobra continua ajudando.
            pesos, criterio = {}, "déficit de caixa, por lado"
            for lado in ("principal", "demais"):
                # obra sem lado (sem projeto, ou "(não apropriado)") não
                # entra em canto nenhum
                do_lado = [o for o in acumulado if lado_de.get(o) == lado]
                liquido = sum(acumulado[o] for o in do_lado
                              if acumulado[o] > 0 or _ativa(o))
                necessidade[lado] = max(0.0, -liquido)
                buracos = {o: deficits[o] for o in do_lado if o in deficits}
                soma = sum(buracos.values())
                if necessidade[lado] > 0.005 and soma > 0:
                    for o, buraco in buracos.items():
                        pesos[o] = necessidade[lado] * buraco / soma
        total = sum(pesos.values())
        if total <= 0.005:
            pesos, criterio = {}, "ninguém no vermelho"
            if sem_deficit == "estrutura" and rateio_do_mes.get(mes):
                pesos, criterio = rateio_do_mes[mes], "régua da estrutura"
                total = sum(pesos.values())

        if abs(juro) > 0.005:
            if pesos and total > 0.005:
                for obra, peso in pesos.items():
                    chave = (obra, mes)
                    alocacoes[chave] = alocacoes.get(chave, 0.0) + juro * peso / total
            else:
                sobras.append({
                    "origem": "Juros de empréstimo", "mes": mes, "valor": juro,
                    "motivo": "nenhuma obra estava com o caixa negativo neste mês",
                })

        memoria.append({
            "mes": mes,
            "juros": round(juro, 2),
            "criterio": criterio,
            "deficit_total": round(sum(deficits.values()), 2),
            "falta_principal": round(necessidade.get("principal", 0.0), 2),
            "falta_demais": round(necessidade.get("demais", 0.0), 2),
            "obras_no_vermelho": len(deficits),
            "maior": (max(deficits.items(), key=lambda kv: kv[1])[0]
                      if deficits else ""),
        })

    juros_sem_data = juros_por_mes.get(SEM_DATA, 0.0)
    if abs(juros_sem_data) > 0.005:
        sobras.append({
            "origem": "Juros de empréstimo", "mes": SEM_DATA,
            "valor": juros_sem_data,
            "motivo": "lançamento sem data: não dá para saber de que mês é o buraco",
        })

    return {"alocacoes": alocacoes, "sobras": sobras, "memoria": memoria}


def corte_dos_juros(pessoal, caixa, obras) -> dict:
    """{obra: último mês em que ela ainda recebe juro}.

    Decisão do dono em 10/11/2026: "o projeto que terminou com prejuízo tem
    que encerrar. A gente tem que fazer um ponto de corte. Terminou de gastar
    com mão de obra, terminou de receber juros." O corte é o último mês com
    custo de pessoal da obra. Obra que nunca teve pessoal usa o último mês
    em que mexeu no caixa — senão ela nunca pagaria juro nenhum, mesmo tendo
    vivido de dinheiro emprestado."""
    ultimo: dict[str, str] = {}
    for mes, obra, valor in pessoal or ():
        if obra in obras and mes != SEM_DATA and abs(float(valor or 0)) > 0.005:
            if mes > ultimo.get(obra, ""):
                ultimo[obra] = mes
    reserva: dict[str, str] = {}
    for mes, obra, valor in caixa or ():
        if obra in obras and obra not in ultimo and mes != SEM_DATA:
            if mes > reserva.get(obra, ""):
                reserva[obra] = mes
    reserva.update(ultimo)
    return reserva


def total_por_obra(alocacoes) -> list[dict]:
    """Soma uma alocação por obra, da maior para a menor — o formato da tela."""
    total: dict[str, float] = {}
    for (obra, _mes), valor in alocacoes.items():
        total[obra] = total.get(obra, 0.0) + valor
    return sorted(({"obra": o, "valor": round(v, 2)} for o, v in total.items()),
                  key=lambda l: l["valor"])


# FORA TOTALMENTE, OU SÓ DEPOIS DE UM MÊS (11/11/2026, o dono: "colocar
# totalmente fora da análise ou colocar até determinado período. É isso que eu
# preciso em relação a considerar a obra ou projeto, ou não"). O item guarda o
# mês no fim: "obra:CASA" está fora inteira; "obra:CASA@2024-06" é considerada
# até 06/2024 e sai dali em diante. Texto, e não coluna nova: vale igual na
# lista de Parâmetros (um texto na configuração) e na de cada cenário, sem
# migração.
_ITEM_FORA = re.compile(r"^(obra|projeto):(.+?)(?:@(\d{4}-\d{2}))?$")


def ler_item_fora(item) -> tuple:
    """("obra"|"projeto", nome, "AAAA-MM" ou None). Item ilegível: (None, "", None)."""
    m = _ITEM_FORA.match((item or "").strip())
    if not m:
        return None, "", None
    return m.group(1), m.group(2).strip(), m.group(3)


def montar_item_fora(tipo: str, nome: str, ate: str | None = None) -> str:
    return f"{tipo}:{nome.strip()}" + (f"@{ate}" if ate else "")


def _obras_do_item(tipo, nome, mapa_projeto, obras) -> set:
    if tipo == "obra":
        return {nome}
    if tipo == "projeto":
        return {o for o in obras if (mapa_projeto.get(o) or "") == nome}
    return set()


def obras_fora_da_analise(itens, mapa_projeto: dict, obras) -> set:
    """As obras que ficam fora TOTALMENTE: as nomeadas uma a uma, e todas as
    obras dos projetos nomeados. O que sai daqui não recebe estrutura, não
    recebe juros e não entra na quota de ninguém. O item com mês ("até")
    não entra aqui — ver `cortes_da_analise`."""
    fora = set()
    for item in itens or ():
        tipo, nome, ate = ler_item_fora(item)
        if tipo and not ate:
            fora |= _obras_do_item(tipo, nome, mapa_projeto, obras)
    return fora


def cortes_da_analise(itens, mapa_projeto: dict, obras) -> dict:
    """{obra: último mês considerado} — as obras que só contam ATÉ um mês.
    Obra nomeada duas vezes (por ela e pelo projeto) fica com o mês mais cedo."""
    cortes: dict[str, str] = {}
    for item in itens or ():
        tipo, nome, ate = ler_item_fora(item)
        if not (tipo and ate):
            continue
        for obra in _obras_do_item(tipo, nome, mapa_projeto, obras):
            cortes[obra] = min(cortes.get(obra, ate), ate)
    return cortes


def depois_do_corte(mes, obra, cortes) -> bool:
    """A linha (mês, obra) fica de fora pelo corte? Linha SEM DATA de obra com
    corte também fica: não dá para dizer que ela é de antes do mês."""
    ate = cortes.get((obra or "").strip()) if cortes else None
    if not ate:
        return False
    mes = str(mes or "")
    return mes == SEM_DATA or not mes or mes[:7] > ate


# ---------------------------------------------------------------------------
# 2c. O rateio do CENÁRIO: uma régua escolhida, percentuais em hierarquia
# ---------------------------------------------------------------------------
# A diferença para o `calcular_rateio` acima não é de conta, é de operação. Lá,
# cada pedaço do custo da matriz exigia cadastrar uma regra com nome, escopo e
# vigência. O dono disse que ficou complicado, e tinha razão.
#
# Aqui a conta da matriz herda o percentual padrão do cenário; marcar o GRUPO
# sobrepõe; marcar a CATEGORIA sobrepõe o grupo; marcar um LANÇAMENTO sobrepõe
# a categoria. Configurar é tocar em poucas linhas, não em todas.
#
# E a régua de quem recebe é escolhida: MÃO DE OBRA (o custo de pessoal de cada
# obra) ou FATURAMENTO. A razão de a mão de obra ser o padrão está na frase do
# dono: "o custo de despesas com o pessoal é um indicador da quantidade de
# energia que aquela obra requer (…) o DP vai ter mais trabalho, a engenharia
# vai ter mais trabalho".


def itens_fora_da_analise(texto) -> list[str]:
    """A lista gravada em Parâmetros ("obra:NOME;projeto:NOME"), como itens.

    Vive na configuração da prestação porque é o que vale para TUDO: a
    prestação antiga, os cenários de rateio e todo cenário novo. A lista de
    cada cenário soma-se a esta — nunca a substitui."""
    partes = re.split(r"[;\n]", texto or "")
    saida: list[str] = []
    for parte in partes:
        parte = parte.strip()
        if parte and parte not in saida:
            saida.append(parte)
    return saida


def sem_depois_do_corte(base: dict, cortes) -> dict:
    """A base sem o que cada obra com corte fez DEPOIS do mês dela — nas três
    leituras que dependem de obra e mês (apuração, pessoal e caixa)."""
    if not cortes:
        return base
    saida = dict(base)
    saida["apuracao"] = [l for l in base.get("apuracao", [])
                         if not depois_do_corte(l.get("mes"), l.get("obra"), cortes)]
    saida["pessoal"] = [t for t in base.get("pessoal", [])
                        if not depois_do_corte(t[0], t[1], cortes)]
    saida["caixa"] = [t for t in base.get("caixa", [])
                      if not depois_do_corte(t[0], t[1], cortes)]
    return saida


def sem_as_obras(base: dict, fora) -> dict:
    """A base da prestação sem as obras que ficaram fora da análise.

    Tira das três leituras que dependem de obra — a apuração, o pessoal (a
    régua) e o caixa (a régua dos juros). O que sai daqui não é obra para
    o cálculo: não recebe estrutura nem juros, não entra na quota de ninguém
    e não pesa na régua. As demais chaves passam intactas."""
    fora = set(fora or ())
    if not fora:
        return base
    saida = dict(base)
    saida["apuracao"] = [l for l in base.get("apuracao", [])
                         if (l.get("obra") or "").strip() not in fora]
    saida["pessoal"] = [t for t in base.get("pessoal", []) if t[1] not in fora]
    saida["caixa"] = [t for t in base.get("caixa", []) if t[1] not in fora]
    return saida


def peso_e_divisao(pesos: dict, grupo: str, categoria: str, codigo: str,
                   pct_padrao: float, mes: str | None = None) -> tuple:
    """(quanto desta conta entra no bolo, parte do projeto principal | None).

    Do mais específico ao mais amplo. Zero e "não marcado" são coisas
    DIFERENTES: zero quer dizer "esta conta não se divide"; não marcado quer
    dizer "segue o nível de cima".

    O PERCENTUAL PODE MUDAR COM O TEMPO (09/11/2026, migração 024): com o `mes`
    (AAAA-MM), vale o último período que começou até ele — "50% a partir de
    01/2021, 60% a partir de 01/2022". Antes do primeiro período, vale o
    percentual fixo da linha; sem ele, o nível de cima.

    E O PERÍODO PODE SEPARAR OS DOIS LADOS: "40% para a BWS CE, 60% para os
    demais projetos". A parte do principal vem do mesmo período que deu o
    percentual; None quer dizer "sem separar" — a conta se divide entre todas
    as obras que recebem, pela régua."""
    periodos = pesos.get("periodos") or {}
    # O percentual vem do nível mais específico que disser algum. A parte do
    # principal TAMBÉM — mas em branco quer dizer "segue o nível de cima", como
    # no percentual (10/11/2026): afinar uma categoria dentro de um grupo
    # "40% para o BWS CE" não pode fazê-la perder os dois lados.
    pct, principal = None, None
    for nivel, chave in (("lancamento", codigo), ("categoria", categoria),
                         ("grupo", grupo)):
        if not chave:
            continue
        lista = (periodos.get(nivel) or {}).get(chave) or []
        valendo = ([p for p in lista if p[0] <= mes]
                   if lista and mes and mes != SEM_DATA else [])
        if valendo:
            ultimo = valendo[-1]
            if pct is None:
                pct = float(ultimo[1])
            if principal is None and len(ultimo) > 2 and ultimo[2] is not None:
                principal = ultimo[2]
        elif pct is None and chave in (pesos.get(nivel) or {}):
            pct = float(pesos[nivel][chave])
        if pct is not None and principal is not None:
            break
    return (float(pct_padrao) if pct is None else pct), principal


def peso_da_conta(pesos: dict, grupo: str, categoria: str, codigo: str,
                  pct_padrao: float, mes: str | None = None) -> float:
    """Só o percentual que entra no bolo — ver `peso_e_divisao`."""
    return peso_e_divisao(pesos, grupo, categoria, codigo, pct_padrao, mes)[0]


def pesos_por_mes(driver, meses, janela: str = "1") -> dict:
    """Quanto de cada obra em cada mês, de 0 a 1, pela régua escolhida.

    `driver` é (mês, obra, valor) — o custo de pessoal ou o faturamento. A
    janela soma os meses anteriores antes de comparar: janela curta reage
    rápido e balança, janela longa é estável e demora a reagir.

    Mês em que a régua inteira deu zero não inventa divisão: devolve vazio, e
    quem chama transforma isso numa sobra visível."""
    por_obra: dict[str, dict[str, float]] = {}
    for mes, obra, valor in driver:
        if mes == SEM_DATA:
            continue
        por_obra.setdefault(obra, {})
        por_obra[obra][mes] = por_obra[obra].get(mes, 0.0) + abs(float(valor or 0))

    ordem = list(meses)
    try:
        n = max(int(janela), 1)
    except (TypeError, ValueError):
        n = None                                   # 'acumulado'

    saida: dict[str, dict[str, float]] = {}
    for i, mes in enumerate(ordem):
        recorte = ordem[:i + 1] if n is None else ordem[max(0, i - n + 1):i + 1]
        bruto = {obra: sum(serie.get(m, 0.0) for m in recorte)
                 for obra, serie in por_obra.items()}
        total = sum(bruto.values())
        saida[mes] = ({obra: v / total for obra, v in bruto.items() if v > 0}
                      if total > 0.005 else {})
    return saida


def calcular_rateio_do_cenario(despesa_admin, excecoes, driver, obras, pesos,
                               *, pct_padrao: float = 100.0,
                               janela: str = "1", lado_de=None,
                               recebe=None) -> dict:
    """Divide o custo da matriz entre as obras, pelo cenário.

    `despesa_admin` é o agregado por (mês, grupo, categoria); `excecoes` são os
    lançamentos marcados um a um — eles saem do agregado e entram com o
    percentual próprio, senão o mesmo dinheiro contaria duas vezes.

    Devolve as alocações — (obra, mês) -> valor —, as **sobras** (custo que não
    coube em obra nenhuma, com o motivo) e a **memória** mês a mês. A sobra não
    é detalhe: é custo real da empresa que ficou sem dono, e a tela mostra isso
    em vez de esconder."""
    pesos = pesos or {}
    # Os DOIS LADOS (09/11/2026): o projeto principal ("BWS CE") e os demais.
    # `lado_de` diz o lado de cada obra; `recebe(obra, mes)` diz se o projeto
    # dela recebe custo da matriz naquele mês. Sem eles, tudo como antes.
    lado_de = lado_de or {}
    recebe = recebe or (lambda _obra, _mes: True)
    # sem projeto principal não há dois lados: tudo pela régua, entre todas
    separa = any(lado == "principal" for lado in lado_de.values())
    # O QUE FICOU DE FORA (10/11/2026): a parte de cada conta que não entrou
    # no bolo. Antes sumia sem rastro; agora é contada, por mês e por conta,
    # para o batimento acusar e a tela criticar.
    fora_por_mes: dict[str, float] = {}
    fora_por_conta: dict[tuple, float] = {}

    def _ficou_fora(mes, grupo, categoria, valor):
        if abs(valor) > 0.005:
            fora_por_mes[mes] = fora_por_mes.get(mes, 0.0) + valor
            chave = (grupo or "(sem grupo)", categoria or "(sem categoria)")
            fora_por_conta[chave] = fora_por_conta.get(chave, 0.0) + valor

    # 1. As exceções saem do balde a que pertencem.
    fora: dict[tuple, float] = {}
    pool: dict[str, float] = {}
    # parte de cada bolo mensal que já vem destinada a um lado
    para_lado: dict[tuple, float] = {}
    sobras: list[dict] = []

    def _destinar(mes, parte, principal):
        """Soma ao bolo do mês; se o período separa os lados, guarda quanto
        vai para cada um."""
        pool[mes] = pool.get(mes, 0.0) + parte
        if principal is not None and separa:
            p = max(min(float(principal), 100.0), 0.0) / 100.0
            para_lado[(mes, "principal")] = para_lado.get((mes, "principal"), 0.0) + parte * p
            para_lado[(mes, "demais")] = para_lado.get((mes, "demais"), 0.0) + parte * (1 - p)
    for e in excecoes or []:
        chave = (e["mes"], e.get("grupo") or "", e.get("categoria") or "")
        fora[chave] = fora.get(chave, 0.0) + float(e["valor"] or 0)
        pct, principal = peso_e_divisao(pesos, e.get("grupo"), e.get("categoria"),
                                        str(e.get("codigo") or ""), pct_padrao, mes=e["mes"])
        _ficou_fora(e["mes"], e.get("grupo"), e.get("categoria"),
                    float(e["valor"] or 0) * (1 - pct / 100.0))
        if e["mes"] == SEM_DATA:
            if abs(float(e["valor"] or 0) * pct / 100.0) > 0.005:
                sobras.append({"origem": f"Lançamento {e.get('codigo')}",
                               "mes": SEM_DATA, "valor": float(e["valor"]) * pct / 100.0,
                               "motivo": "lançamento sem data: não dá para ratear por mês"})
            continue
        _destinar(e["mes"], float(e["valor"]) * pct / 100.0, principal)

    # 2. O que sobrou de cada balde entra pelo percentual do seu nível.
    # A mesma conta pode vir em mais de uma linha (matriz e filial têm a mesma
    # categoria no mesmo mês). Soma-se ANTES de tirar as exceções: tirando
    # linha a linha, o lançamento marcado era descontado uma vez por
    # departamento (achado em 10/11/2026).
    baldes: dict[tuple, float] = {}
    for linha in despesa_admin:
        chave = (linha["mes"], linha.get("grupo") or "", linha.get("categoria") or "")
        baldes[chave] = baldes.get(chave, 0.0) + float(linha["valor"] or 0)
    for chave, bruto in baldes.items():
        mes_l, grupo_l, categoria_l = chave
        valor = bruto - fora.get(chave, 0.0)
        if abs(valor) <= 0.005:
            continue
        pct, principal = peso_e_divisao(pesos, grupo_l, categoria_l,
                                        "", pct_padrao, mes=mes_l)
        parte = valor * pct / 100.0
        _ficou_fora(mes_l, grupo_l, categoria_l, valor - parte)
        if abs(parte) <= 0.005:
            continue
        if mes_l == SEM_DATA:
            sobras.append({"origem": f"{grupo_l or '(sem grupo)'} › "
                                     f"{categoria_l or '(sem categoria)'}",
                           "mes": SEM_DATA, "valor": parte,
                           "motivo": "lançamento sem data: não dá para ratear por mês"})
            continue
        _destinar(mes_l, parte, principal)

    # 3. O bolo de cada mês se divide pela régua escolhida.
    meses = _meses_ordenados(set(pool), {m for m, _o, _v in driver})
    fracoes = pesos_por_mes([(m, o, v) for m, o, v in driver if o in obras],
                            meses, janela)

    alocacoes: dict[tuple, float] = {}
    memoria: list[dict] = []
    for mes in meses:
        bolo = pool.get(mes, 0.0)
        # só quem RECEBE neste mês (projeto que não recebe, ou que já saiu,
        # fica de fora) — a régua se refaz entre os que ficam
        fatias = {o: f for o, f in (fracoes.get(mes) or {}).items() if recebe(o, mes)}
        do_lado = {lado: {o: f for o, f in fatias.items() if lado_de.get(o) == lado}
                   for lado in ("principal", "demais")}
        a_principal = para_lado.get((mes, "principal"), 0.0)
        a_demais = para_lado.get((mes, "demais"), 0.0)
        # NADA FICA SEM DONO entre os lados (dono, 09/11/2026: "ou é da BWS
        # CE, ou é dos demais"): o lado sem ninguém recebendo no mês passa a
        # parte dele para o outro
        if not do_lado["principal"]:
            a_demais, a_principal = a_demais + a_principal, 0.0
        if not do_lado["demais"]:
            a_principal, a_demais = a_principal + a_demais, 0.0
        livre = bolo - a_principal - a_demais
        if abs(bolo) > 0.005 and not fatias:
            sobras.append({"origem": "Custo da matriz", "mes": mes, "valor": bolo,
                           "motivo": "nenhuma obra que recebe, com movimento na régua "
                                     "escolhida, neste mês"})
            fatias = {}
        for valor, grupo_de_obras in ((livre, fatias),
                                      (a_principal, do_lado["principal"]),
                                      (a_demais, do_lado["demais"])):
            total = sum(grupo_de_obras.values())
            if abs(valor) <= 0.005 or total <= 0:
                continue
            for obra, fracao in grupo_de_obras.items():
                parte = valor * fracao / total
                if abs(parte) > 0.005:
                    alocacoes[(obra, mes)] = alocacoes.get((obra, mes), 0.0) + parte
        memoria.append({
            "mes": mes,
            "pool": round(bolo, 2),
            "obras": len(fatias),
            "maior": (max(fatias.items(), key=lambda kv: kv[1])[0] if fatias else ""),
            "maior_pct": (round(max(fatias.values()) * 100, 1) if fatias else 0.0),
        })

    return {"alocacoes": alocacoes, "sobras": sobras, "memoria": memoria,
            "fora_por_mes": fora_por_mes,
            "fora_por_conta": sorted(
                ({"grupo": g, "categoria": c, "valor": round(v, 2)}
                 for (g, c), v in fora_por_conta.items()),
                key=lambda l: l["valor"])}


# ---------------------------------------------------------------------------
# 2d. O batimento: tudo o que a matriz gastou tem de aparecer em algum lugar
# ---------------------------------------------------------------------------
# Pedido do dono em 10/11/2026: "é muito importante esse batimento (…) o que é
# custo, o que está apropriado, rateado do BWS CE, o que está apropriado de
# juros, e o próprio sistema fazer esse somatório e identificar que bate com o
# valor global. É importante o próprio sistema fazer essa checagem e a gente,
# com o relatório, poder fazer a checagem manual também."

# Centavos se perdem ao dividir um valor entre dezenas de obras; acima disto,
# num mês, é diferença de verdade.
TOLERANCIA_DO_BATIMENTO = 1.00


def batimento(despesa_admin, rateio, juros, lado_de=None) -> dict:
    """Confere, mês a mês, que o custo da matriz foi inteiro para algum lugar.

    custo da matriz = estrutura para o principal + estrutura para os demais
                    + juros nas obras + sem dono + ficou de fora

    `despesa_admin` é o gasto BRUTO dos departamentos administrativos (com os
    juros dentro) — o número global, antes de qualquer escolha do cenário.
    "Fecha" quer dizer: nada sumiu na conta E nada ficou de fora."""
    lado_de = lado_de or {}
    colunas = ("matriz", "principal", "demais", "juros", "sem_dono", "fora")
    meses: dict[str, dict] = {}

    def _somar(mes, coluna, valor):
        linha = meses.setdefault(mes, {c: 0.0 for c in colunas})
        linha[coluna] += float(valor or 0)

    for linha in despesa_admin:
        _somar(linha["mes"], "matriz", linha["valor"])
    for (obra, mes), valor in (rateio.get("alocacoes") or {}).items():
        _somar(mes, "principal" if lado_de.get(obra) == "principal" else "demais", valor)
    for (_obra, mes), valor in (juros.get("alocacoes") or {}).items():
        _somar(mes, "juros", valor)
    for sobra in list(rateio.get("sobras") or []) + list(juros.get("sobras") or []):
        _somar(sobra["mes"], "sem_dono", sobra["valor"])
    for mes, valor in (rateio.get("fora_por_mes") or {}).items():
        _somar(mes, "fora", valor)

    linhas = []
    total = {c: 0.0 for c in colunas}
    for mes in sorted(meses):
        linha = meses[mes]
        linha["diferenca"] = linha["matriz"] - sum(linha[c] for c in colunas[1:])
        for c in colunas:
            total[c] += linha[c]
        linhas.append({"mes": mes, **{c: round(v, 2) for c, v in linha.items()},
                       "bate": abs(linha["diferenca"]) <= TOLERANCIA_DO_BATIMENTO})
    total["diferenca"] = total["matriz"] - sum(total[c] for c in colunas[1:])
    meses_ruins = [l["mes"] for l in linhas if not l["bate"]]
    soma_fecha = not meses_ruins
    return {
        "linhas": linhas,
        "total": {c: round(v, 2) for c, v in total.items()},
        "meses_que_nao_batem": meses_ruins,
        # a soma confere (nada sumiu na conta)
        "soma_fecha": soma_fecha,
        # e tudo teve destino (nada de fora) — é o "tem que somar 100%"
        "tudo_distribuido": abs(total["fora"]) <= TOLERANCIA_DO_BATIMENTO,
        "fecha": soma_fecha and abs(total["fora"]) <= TOLERANCIA_DO_BATIMENTO,
    }


def criticas_das_participacoes(participacoes) -> list[dict]:
    """Onde os percentuais de quem divide não somam 100% — obra a obra, e na
    lista geral. Com a soma errada, as quotas não fecham com o resultado."""
    somas: dict[str, float] = {}
    for p in participacoes or ():
        obra = p.get("obra") or ""
        somas[obra] = somas.get(obra, 0.0) + float(p.get("pct") or 0)
    return [{"obra": obra or "todas as obras", "soma": round(soma, 2)}
            for obra, soma in sorted(somas.items()) if abs(soma - 100.0) > 0.01]


# ---------------------------------------------------------------------------
# 3. A apuração por obra
# ---------------------------------------------------------------------------
def apurar(apuracao, obras, alocacoes, juros=None) -> list[dict]:
    """Junta a receita e a despesa de cada obra com o que ela recebeu de fora.

    Duas coisas vêm de fora, e ficam separadas de propósito: o **rateio** da
    estrutura, repartido pelo pessoal, e os **juros** de empréstimo, repartidos
    pelo déficit de caixa. Juntá-las numa coluna só esconderia a diferença que
    é justamente o ponto — obra que se paga sozinha carrega estrutura, mas não
    carrega juro."""
    por_chave: dict[tuple, dict] = {}
    for linha in apuracao:
        obra = (linha["obra"] or "").strip()
        if obra not in obras:
            continue
        chave = (obras[obra]["projeto"], obra, linha["mes"])
        registro = por_chave.setdefault(chave, {
            "projeto": chave[0], "obra": obra, "mes": linha["mes"],
            "receita_liquida": 0.0, "retencoes": 0.0, "despesas": 0.0,
            "rateio": 0.0, "juros": 0.0,
        })
        registro["receita_liquida"] += linha["receita_liquida"]
        registro["retencoes"] += linha["retencoes"]
        registro["despesas"] += linha["despesas"]

    # rateio e juros podem cair em obra/mês sem movimento próprio: a chave é
    # criada aqui
    for campo, vindos in (("rateio", alocacoes), ("juros", juros or {})):
        for (obra, mes), valor in vindos.items():
            if obra not in obras:
                continue
            chave = (obras[obra]["projeto"], obra, mes)
            registro = por_chave.setdefault(chave, {
                "projeto": chave[0], "obra": obra, "mes": mes,
                "receita_liquida": 0.0, "retencoes": 0.0, "despesas": 0.0,
                "rateio": 0.0, "juros": 0.0,
            })
            registro[campo] += valor

    saida = []
    for registro in por_chave.values():
        registro["receita_bruta"] = registro["receita_liquida"] + registro["retencoes"]
        registro["resultado_direto"] = registro["receita_liquida"] + registro["despesas"]
        registro["resultado"] = (registro["resultado_direto"] + registro["rateio"]
                                 + registro["juros"])
        saida.append(registro)
    saida.sort(key=lambda r: (r["projeto"], r["obra"], r["mes"]))
    return saida


def totalizar_por_projeto(apurado) -> dict:
    """Soma a apuração por projeto — é o nível em que os sócios participam."""
    campos = ("receita_bruta", "receita_liquida", "retencoes", "despesas",
              "rateio", "juros", "resultado_direto", "resultado")
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
      - a base que todos dividem é o resultado **direto**, mais os **juros** que
        a obra fez a empresa pagar, menos essa taxa. O juro entra na base de
        todos porque não é estrutura da construtora: é o preço do dinheiro que
        financiou AQUELA obra, e quem participa do resultado dela participa
        também do custo de bancá-la;
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
            base = numeros["resultado_direto"] + numeros.get("juros", 0.0) - taxa_adm
            quota = base * fracao
            credito = 0.0
            if not externo:
                interna = soma_interna.get(projeto, 0.0)
                proporcao = (float(p["pct"]) / interna) if interna > 0 else 0.0
                credito = (taxa_adm + numeros["rateio"]) * proporcao
                quota += credito
            visao = ("Parceria — resultado direto, mais os juros, menos a taxa"
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
            "juros": numeros.get("juros", 0.0),
        })
    saida.sort(key=lambda q: (q["socio"], q["projeto"]))
    return saida


# ---------------------------------------------------------------------------
# 4b. A divisão entre sócios, OBRA a obra
# ---------------------------------------------------------------------------
# A `quotas_por_socio` acima divide por PROJETO. O cenário divide por OBRA, e a
# razão é do dono: ele fala de "as obras do Ceará", e parceiro entra em obra,
# não na construtora. Quem participa de tudo entra uma vez só, com a obra em
# branco — senão seriam 174 linhas iguais.


def participantes_da_obra(participacoes, obra: str) -> list[dict]:
    """Quem divide ESTA obra: quem foi nomeado nela; se ninguém foi, quem
    participa de todas. O específico ganha do geral — é o que permite dizer
    "nesta obra entrou um parceiro" sem refazer o resto."""
    nomeados = [p for p in participacoes if (p.get("obra") or "") == obra]
    return nomeados or [p for p in participacoes if not (p.get("obra") or "")]


def quotas_por_obra(por_obra: dict, participacoes, taxa_adm_pct: float = 0.0) -> list[dict]:
    """Quanto cabe a cada um, obra a obra. Mesma conta da divisão por projeto.

    **Obra só da BWS:** cada um leva seu percentual do resultado com rateio.

    **Obra com parceiro:** o parceiro entrou na obra, não na BWS — não é ele
    quem paga a estrutura da construtora. Então cobra-se da obra uma taxa de
    administração sobre a receita bruta; a base que todos dividem é o resultado
    direto MAIS os juros MENOS essa taxa; e a taxa somada ao rateio da estrutura
    volta só para os sócios internos, na proporção entre eles.

    O juro entra na base de todos porque não é estrutura: é o preço do dinheiro
    que financiou aquela obra.

    A soma de todas as quotas de uma obra fecha com o resultado dela."""
    taxa = float(taxa_adm_pct or 0) / 100.0
    saida = []
    for obra, n in por_obra.items():
        gente = participantes_da_obra(participacoes, obra)
        if not gente:
            continue
        tem_externo = any((p.get("tipo") or "").lower() == "externo" for p in gente)
        soma_interna = sum(float(p["pct"]) for p in gente
                           if (p.get("tipo") or "").lower() != "externo")
        taxa_adm = taxa * n.get("receita_bruta", 0.0)

        for p in gente:
            externo = (p.get("tipo") or "").lower() == "externo"
            fracao = float(p["pct"]) / 100.0
            if tem_externo:
                base = (n.get("resultado_direto", 0.0) + n.get("juros", 0.0)
                        - taxa_adm)
                quota = base * fracao
                credito = 0.0
                if not externo and soma_interna > 0:
                    credito = ((taxa_adm + n.get("rateio", 0.0))
                               * float(p["pct"]) / soma_interna)
                    quota += credito
                visao = ("Parceria — resultado direto, mais os juros, menos a taxa"
                         if externo else "Parceria, lado BWS — mais a taxa e o rateio")
            else:
                base = n.get("resultado", 0.0)
                quota = base * fracao
                credito = 0.0
                visao = "Obra só da BWS — resultado com rateio e juros"

            saida.append({
                "socio": p["socio"], "tipo": p.get("tipo") or "Interno",
                "obra": obra, "pct": float(p["pct"]),
                "base": round(base, 2), "quota": round(quota, 2),
                "credito_bws": round(credito, 2), "taxa_adm": round(taxa_adm, 2),
                "visao": visao,
                "resultado": n.get("resultado", 0.0),
                "resultado_direto": n.get("resultado_direto", 0.0),
                "rateio": n.get("rateio", 0.0), "juros": n.get("juros", 0.0),
            })
    saida.sort(key=lambda q: (q["socio"], q["obra"]))
    return saida


def totalizar_por_obra(apurado) -> dict:
    """Soma a apuração por obra — é o nível em que o cenário divide."""
    campos = ("receita_bruta", "receita_liquida", "retencoes", "despesas",
              "rateio", "juros", "resultado_direto", "resultado")
    total: dict[str, dict] = {}
    for linha in apurado:
        alvo = total.setdefault(linha["obra"], {c: 0.0 for c in campos})
        for campo in campos:
            alvo[campo] += linha[campo]
    return total


def trilha_da_obra(caixa_por_obra_mes, obra: str, *, rateio=None, juros=None) -> list[dict]:
    """A vida de UMA obra, mês a mês — para o gráfico e para a conferência.

    O dono: "a parte gráfica é muito interessante para você entender o que
    aconteceu, como aconteceu (…) mostrando a evolução da obra, do consumo de
    caixa, como é que ela se comportou, por que que ela precisou puxar juros".

    É isso que esta lista responde, uma linha por mês: o que a obra gerou ou
    consumiu, o acumulado (onde ela ficou no vermelho), a estrutura que recebeu
    e o juro que absorveu por causa do buraco."""
    proprio: dict[str, float] = {}
    for mes, nome, valor in caixa_por_obra_mes:
        if nome == obra and mes != SEM_DATA:
            proprio[mes] = proprio.get(mes, 0.0) + float(valor or 0)

    do_rateio = {m: v for (o, m), v in (rateio or {}).items() if o == obra}
    do_juros = {m: v for (o, m), v in (juros or {}).items() if o == obra}

    linhas, acumulado, juros_ac = [], 0.0, 0.0
    for mes in _meses_ordenados(set(proprio), set(do_rateio), set(do_juros)):
        no_mes = proprio.get(mes, 0.0) + do_rateio.get(mes, 0.0)
        acumulado += no_mes
        juros_ac += do_juros.get(mes, 0.0)
        ano, _, m = mes.partition("-")
        linhas.append({
            "mes": mes, "rotulo": f"{m}/{ano}",
            "caixa_do_mes": round(no_mes, 2),
            "acumulado": round(acumulado, 2),
            "rateio": round(do_rateio.get(mes, 0.0), 2),
            "juros": round(do_juros.get(mes, 0.0), 2),
            "juros_acumulado": round(juros_ac, 2),
            "com_juros": round(acumulado + juros_ac, 2),
        })
    return linhas


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


# ---------------------------------------------------------------------------
# 6. Cenários de rateio: ajustar e comparar ANTES de gravar
# ---------------------------------------------------------------------------
# Mudar uma regra de rateio muda quanto de custo administrativo cai em cada
# obra — e, por consequência, quanto cabe a cada sócio. Gravar para depois
# olhar o efeito é caro: se ficou pior, é preciso lembrar como estava antes.
#
# O cenário resolve isso: as regras são alteradas EM MEMÓRIA, o cálculo inteiro
# roda duas vezes (com as regras gravadas e com as do cenário) e a tela mostra
# obra a obra o que mudaria. Nada toca o banco enquanto você não mandar.

ESCOPOS = ("AMBAS", "MATRIZ", "FILIAL")


def normalizar_regra_do_cenario(regra: dict, mudanca: dict) -> dict:
    """Aplica sobre uma regra gravada o que a pessoa mexeu na tela do cenário.

    Só os parâmetros entram: **%**, **escopo**, **vigência** e **ativa**. Quais
    grupos e categorias a regra pega continua sendo coisa da tela de Regras —
    é lá que existe a lista para escolher, e duplicar essa escolha aqui seria
    duas telas para a mesma decisão.

    Valor fora da faixa não vira erro na cara de quem está simulando: é preso
    no limite (0 a 100), como fazia a tela antiga."""
    nova = dict(regra)

    if "pct" in mudanca:
        try:
            pct = float(str(mudanca["pct"]).replace(",", "."))
        except (TypeError, ValueError):
            pct = float(regra.get("pct") or 100.0)
        nova["pct"] = min(max(pct, 0.0), 100.0)

    if "escopo" in mudanca:
        escopo = str(mudanca["escopo"] or "").strip().upper()
        nova["escopo"] = escopo if escopo in ESCOPOS else "AMBAS"

    for campo in ("mes_ini", "mes_fim"):
        if campo in mudanca:
            nova[campo] = str(mudanca[campo] or "").strip()

    if "ativo" in mudanca:
        nova["ativo"] = 1 if mudanca["ativo"] in (1, True, "1", "on", "true", "True") else 0

    return nova


def regras_do_cenario(regras_gravadas, mudancas: dict) -> list[dict]:
    """As regras gravadas com as alterações do cenário por cima.

    `mudancas` é {id da regra: {campo: valor}}. Regra que ninguém mexeu passa
    inteira — assim um cenário que altera uma linha só não precisa carregar as
    outras trinta."""
    saida = []
    for regra in regras_gravadas:
        mudanca = mudancas.get(str(regra["id"]), {})
        saida.append(normalizar_regra_do_cenario(regra, mudanca) if mudanca
                     else dict(regra))
    return saida


def _por_obra(apurado) -> dict:
    """Soma rateio e resultado por obra — é a granularidade em que a diferença
    entre dois cenários faz sentido de olhar."""
    total: dict[str, dict] = {}
    for linha in apurado:
        alvo = total.setdefault(linha["obra"], {"rateio": 0.0, "resultado": 0.0})
        alvo["rateio"] += linha["rateio"]
        alvo["resultado"] += linha["resultado"]
    return total


# Diferença abaixo de meio centavo é ruído de arredondamento, não mudança.
LIMITE_DE_RUIDO = 0.005


def comparar_por_obra(apurado_oficial, apurado_cenario) -> list[dict]:
    """O efeito do cenário, obra a obra.

    `delta_resultado` NEGATIVO significa que a obra passa a receber MAIS custo
    administrativo — e portanto piora. É contraintuitivo o suficiente para estar
    escrito também na tela.

    Obra que não mudou fica de fora: numa lista de cem obras, mostrar as noventa
    que continuam iguais esconde as dez que interessam."""
    oficial = _por_obra(apurado_oficial)
    cenario = _por_obra(apurado_cenario)

    linhas = []
    for obra in sorted(set(oficial) | set(cenario)):
        a = oficial.get(obra, {"rateio": 0.0, "resultado": 0.0})
        b = cenario.get(obra, {"rateio": 0.0, "resultado": 0.0})
        delta_resultado = b["resultado"] - a["resultado"]
        delta_rateio = b["rateio"] - a["rateio"]
        if abs(delta_resultado) <= LIMITE_DE_RUIDO and abs(delta_rateio) <= LIMITE_DE_RUIDO:
            continue
        linhas.append({
            "obra": obra,
            "rateio_oficial": a["rateio"], "rateio_cenario": b["rateio"],
            "delta_rateio": delta_rateio,
            "resultado_oficial": a["resultado"], "resultado_cenario": b["resultado"],
            "delta_resultado": delta_resultado,
        })
    # pior primeiro: quem passa a receber mais custo é o que se quer ver antes
    linhas.sort(key=lambda l: l["delta_resultado"])
    return linhas


def resumo_do_cenario(rateio_oficial, rateio_cenario) -> dict:
    """Os quatro números do topo: quanto foi rateado e quanto sobrou, dos dois
    lados. A sobra importa tanto quanto o rateio — ela é custo da empresa que
    ficou sem dono, e um cenário que rateia mais só porque empurrou valor para a
    sobra não melhorou nada."""
    def _totais(rateio):
        return (sum(rateio["alocacoes"].values()),
                sum(s["valor"] for s in rateio["sobras"]))

    rateado_of, sobra_of = _totais(rateio_oficial)
    rateado_cen, sobra_cen = _totais(rateio_cenario)
    return {
        "rateado_oficial": rateado_of, "rateado_cenario": rateado_cen,
        "delta_rateado": rateado_cen - rateado_of,
        "sobra_oficial": sobra_of, "sobra_cenario": sobra_cen,
        "delta_sobra": sobra_cen - sobra_of,
    }


# Só estes cinco o cenário mexe. Grupos e categorias são da tela de Regras.
CAMPOS_DO_CENARIO = ("pct", "escopo", "mes_ini", "mes_fim", "ativo")


def cenario_difere(regras_gravadas, regras_cenario) -> bool:
    """O cenário mudou alguma coisa em relação ao gravado?

    Não dá para comparar os dicionários direto: o formulário devolve tudo como
    texto e o banco devolve `pct` como número, então uma regra intocada
    pareceria alterada e a tela diria "mexeu" sempre. A comparação é campo a
    campo, com os dois lados no mesmo tipo."""
    def _chave(regra):
        return (round(float(regra.get("pct") or 0), 4),
                str(regra.get("escopo") or "AMBAS").upper(),
                str(regra.get("mes_ini") or "").strip(),
                str(regra.get("mes_fim") or "").strip(),
                int(regra.get("ativo", 1) or 0))

    if len(regras_gravadas) != len(regras_cenario):
        return True
    return any(_chave(a) != _chave(b)
               for a, b in zip(regras_gravadas, regras_cenario))
