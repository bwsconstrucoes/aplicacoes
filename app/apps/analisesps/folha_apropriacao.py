# -*- coding: utf-8 -*-
"""
A APROPRIAÇÃO: de quem é o dinheiro da folha, obra por obra.

É o coração do processo. Entra a folha da contabilidade (quem e quanto) e sai
**para qual obra vai cada real** — porque é a obra que decide de qual conta
corrente o dinheiro sai, e é ela que carrega o custo.

⚠️ TUDO AQUI É FUNÇÃO PURA, SEM BANCO E SEM TELA. Não é preciosismo: esta é a
conta que decide dinheiro de ~500 pessoas por quinzena, e conta que só pode ser
verificada abrindo uma tela não é verificada. Quem lê do banco e quem desenha
ficam fora; aqui entra dado e sai dado.

AS REGRAS, todas decididas pelo dono e todas com data:

1. **O dia pertence à obra do PONTO daquele dia** (26/09/2026): *"cada dia ele vai
   estar presente em uma determinada obra, então o dia é o dia daquela obra"*.
2. **São quatro marcações por dia, cada uma com a sua obra.** Vale a que mais
   aparece. No empate 2×2, vale a da primeira marcação — e o empate fica anotado,
   porque *"pessoa em duas obras no mesmo dia é, na maioria dos casos, erro de
   batida de ponto"*.
3. **O período é o do pagamento, não o do mês**: quinzena lê os dias 1 a 15; fim
   de mês, 16 até o último dia. *"Se eu estou pagando a quinzena, eu só vou fazer
   a leitura dos dias de 1 ao 15."*
4. **A sobra do centavo vai para a obra com MAIS DIAS.**
5. **Quem o ponto não apropria tem regra de rateio** (ver `folha_rateio.py`):
   supervisor que não bate ponto, e quem bate na matriz.
6. **O ajuste de mão vence tudo** — e fica marcado como de mão.

⚠️ CADA VALOR SABE DE ONDE VEIO: `ponto`, `regra` ou `mao`. Pedido do dono em
26/09/2026: *"saber até de onde é que foi que veio aquela informação, se foi do
ponto, se foi colocada de forma manual"*. Sem isso, o relatório de auditoria não
tem como explicar nada — e um relatório que não se explica não serve de prova.
"""
from __future__ import annotations

import calendar
import datetime as dt
import logging
from decimal import ROUND_DOWN, Decimal

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")

# De onde veio a obra de um dia (ou de um valor).
DO_PONTO = "ponto"
DA_REGRA = "regra"
DA_MAO = "mao"

# As situações do ponto que NÃO são dia trabalhado.
#
# ⚠️ FERIADO E COMPENSAÇÃO NÃO CONTAM COMO DIA DE OBRA, e isto precisa estar
# escrito porque é uma escolha, não um fato: o dono disse que o dia vale pela obra
# em que a pessoa bateu ponto, e em feriado ela não bateu em obra nenhuma. Se um
# dia ele quiser que feriado siga a obra do dia anterior, é aqui que muda — e é
# uma linha.
SEM_OBRA = ("feriado", "compensação", "compensacao", "")


def periodo_do_pagamento(ano: int, mes: int, tipo: str) -> tuple:
    """Os dias que este pagamento enxerga. `tipo` é "quinzena" ou "fim_de_mes".

    ⚠️ "FIM DE MÊS" É 16 ATÉ O ÚLTIMO DIA, e não "16 a 31": fevereiro tem 28 ou
    29. Escrever 31 faria a leitura do ponto procurar dias que não existem e, pior,
    daria a impressão de que o período está certo."""
    if tipo == "quinzena":
        return dt.date(ano, mes, 1), dt.date(ano, mes, 15)
    if tipo == "fim_de_mes":
        ultimo = calendar.monthrange(ano, mes)[1]
        return dt.date(ano, mes, 16), dt.date(ano, mes, ultimo)
    raise ValueError(f"tipo de pagamento desconhecido: {tipo!r}")


def obra_do_dia(marcacoes, presenca: str = "", falta: str = "") -> dict:
    """A obra de um dia, a partir das quatro marcações.

    Devolve `{"obra": str|None, "empate": bool, "marcou": int, "motivo": str}`.

    `marcacoes` é a lista na ORDEM do dia: entrada, almoço, retorno, saída. Vazio
    ou None onde não houve marcação.

    ⚠️ A ORDEM IMPORTA NO EMPATE. Decisão do dono: *"utiliza o ponto das duas
    primeiras marcações"* — ou seja, num 2×2 vale a obra em que o dia COMEÇOU.
    Ordenar as marcações antes de contar (por nome de obra, por exemplo) faria o
    desempate virar sorteio alfabético, e ninguém notaria."""
    situacao = str(presenca or "").strip().lower()
    limpas = [str(m or "").strip().upper() for m in (marcacoes or [])]
    presentes = [m for m in limpas if m]

    if not presentes:
        # Dia sem marcação. FALTA declarada é uma coisa; silêncio é outra — e o
        # dono pediu para separar as duas: *"vai ter dias que a pessoa não tem
        # falta mas também não bateu o ponto"*.
        if str(falta or "").strip():
            return {"obra": None, "empate": False, "marcou": 0,
                    "motivo": "falta"}
        if situacao and situacao.split()[0].strip("-[] ") not in ("presença",
                                                                 "presenca"):
            return {"obra": None, "empate": False, "marcou": 0,
                    "motivo": situacao}
        return {"obra": None, "empate": False, "marcou": 0,
                "motivo": "sem marcação e sem falta"}

    if situacao and any(s in situacao for s in SEM_OBRA if s):
        return {"obra": None, "empate": False, "marcou": len(presentes),
                "motivo": situacao}

    contagem: dict = {}
    for m in presentes:
        contagem[m] = contagem.get(m, 0) + 1
    maior = max(contagem.values())
    lideres = [m for m in presentes if contagem[m] == maior]
    # `presentes` preserva a ordem do dia, então `lideres[0]` é a obra em que o
    # dia começou entre as empatadas.
    return {"obra": lideres[0], "empate": len(set(lideres)) > 1,
            "marcou": len(presentes), "motivo": ""}


def _dias_uteis_do_ponto(dias, ini, fim) -> list:
    """Os dias COM obra dentro do período, já resolvidos. Ordenados por data."""
    saida = []
    for dia in dias or []:
        data = dia.get("data")
        if not isinstance(data, dt.date) or not (ini <= data <= fim):
            continue
        resolvido = obra_do_dia(dia.get("marcacoes"), dia.get("presenca", ""),
                               dia.get("falta", ""))
        saida.append({"data": data, **resolvido})
    return sorted(saida, key=lambda d: d["data"])


def _repartir(valor: Decimal, pesos: list) -> list:
    """Divide `valor` na proporção de `pesos` (inteiros), sem perder centavo.

    ⚠️ A SOBRA VAI PARA O MAIOR PESO — decisão do dono para a obra com mais dias.
    Arredondar cada parte solta deixa sobra ou falta, e essa diferença vira uma
    obra com custo errado e um arquivo de pagamento que não bate com a folha.

    Arredonda cada parte PARA BAIXO e distribui o que sobrou: assim a soma nunca
    passa do valor no meio da conta, e a correção é sempre para cima."""
    total_peso = sum(pesos)
    if total_peso <= 0:
        return [Decimal("0.00") for _ in pesos]
    partes = [(valor * Decimal(p) / Decimal(total_peso)).quantize(
        CENTAVO, rounding=ROUND_DOWN) for p in pesos]
    sobra = (valor - sum(partes)).quantize(CENTAVO)
    if sobra and partes:
        # O maior peso; no empate, o primeiro — que é determinístico.
        maior = max(range(len(pesos)), key=lambda i: (pesos[i], -i))
        partes[maior] = (partes[maior] + sobra).quantize(CENTAVO)
    return partes


def apropriar_pelo_ponto(valor, dias_uteis) -> dict:
    """O caminho normal: o valor vai para as obras na proporção dos DIAS.

    Devolve `{"por_obra": [...], "por_dia": [...], "dias": n}`.

    Cada nível fecha: os dias de uma obra somam o total da obra, e as obras somam
    o valor. Um nível que não fecha faz o relatório analítico contradizer o
    resumido — e aí nenhum dos dois serve de prova.
    """
    total = Decimal(str(valor or 0)).quantize(CENTAVO)
    com_obra = [d for d in dias_uteis if d["obra"]]
    if not com_obra:
        return {"por_obra": [], "por_dia": [], "dias": 0}

    ordem = []
    dias_por_obra: dict = {}
    for dia in com_obra:
        if dia["obra"] not in dias_por_obra:
            dias_por_obra[dia["obra"]] = []
            ordem.append(dia["obra"])
        dias_por_obra[dia["obra"]].append(dia)

    pesos = [len(dias_por_obra[o]) for o in ordem]
    valores = _repartir(total, pesos)

    por_obra = []
    por_dia = []
    for obra, quantos, parte in zip(ordem, pesos, valores):
        por_obra.append({"obra": obra, "dias": quantos, "valor": parte,
                         "origem": DO_PONTO})
        # Dentro da obra, o valor é dividido igualmente entre os dias dela; a
        # sobra fica no primeiro dia, que é o único critério que não precisa de
        # explicação.
        dentro = _repartir(parte, [1] * quantos)
        for dia, valor_dia in zip(dias_por_obra[obra], dentro):
            por_dia.append({"data": dia["data"], "obra": obra,
                            "obra_ponto": dia["obra"], "valor": valor_dia,
                            "origem": DO_PONTO, "empate": dia["empate"]})
    por_dia.sort(key=lambda d: d["data"])
    return {"por_obra": por_obra, "por_dia": por_dia, "dias": len(com_obra)}


def apropriar_pela_regra(valor, regra) -> dict:
    """Quem o ponto não apropria: a divisão sai da regra de rateio cadastrada."""
    from . import folha_rateio

    partes = folha_rateio.distribuir(valor, regra.get("obras") or [])
    return {"por_obra": [{"obra": p["obra"], "dias": 0, "valor": p["valor"],
                          "origem": DA_REGRA,
                          "percentual": p["percentual"]} for p in partes],
            "por_dia": [], "dias": 0, "regra": regra.get("nome", "")}


def apropriar_pessoa(linha, dias_uteis=None, regra=None, ajuste=None) -> dict:
    """A apropriação de UMA pessoa, com a origem de cada valor.

    A ordem de quem manda, do mais forte para o mais fraco:

    1. **o ajuste de mão** — se alguém disse para onde vai, vai;
    2. **a regra de rateio** — se a pessoa tem uma, ela vale;
    3. **o ponto** — o caminho normal.

    ⚠️ A REGRA VENCE O PONTO DE PROPÓSITO. Quem bate ponto na matriz TEM dias no
    ponto, e são justamente os dias que não servem: apontam para a matriz. Se o
    ponto viesse primeiro, a regra dessa pessoa nunca pegaria — e o erro seria
    invisível, porque a apropriação existiria, só estaria na obra errada.
    """
    valor = Decimal(str(getattr(linha, "valor", None) or
                        (linha or {}).get("valor") or 0)).quantize(CENTAVO)
    dias_uteis = dias_uteis or []

    base = {
        "id_fortes": getattr(linha, "id_fortes", None) or linha.get("id_fortes"),
        "cpf": getattr(linha, "cpf", None) or (linha.get("cpf") if
                                               isinstance(linha, dict) else ""),
        "nome": getattr(linha, "nome", None) or (linha.get("nome") if
                                                 isinstance(linha, dict) else ""),
        "valor": valor,
        "filial_contabilidade": (
            getattr(linha, "filial_nome", None)
            or (linha.get("filial_nome") if isinstance(linha, dict) else "")),
        "dias_no_ponto": len([d for d in dias_uteis if d["obra"]]),
        "fora": False, "regra": "", "criticas": [],
    }

    if ajuste and ajuste.get("fora"):
        return dict(base, fora=True, motivo_fora=str(ajuste.get("motivo") or ""),
                    por_obra=[], por_dia=[], origem=DA_MAO)

    if ajuste and ajuste.get("por_obra"):
        # Ajuste explícito: obra e valor vindos da mão. Confere o total, porque
        # ajuste que não soma o valor da pessoa esconde ou inventa dinheiro.
        partes = [{"obra": str(p["obra"]).strip().upper(),
                   "dias": int(p.get("dias") or 0),
                   "valor": Decimal(str(p["valor"])).quantize(CENTAVO),
                   "origem": DA_MAO} for p in ajuste["por_obra"]]
        soma = sum((p["valor"] for p in partes), Decimal("0"))
        if soma != valor:
            base["criticas"].append(
                f"o ajuste de mão soma {soma} e o valor da pessoa é {valor}")
        return dict(base, por_obra=partes, por_dia=[], origem=DA_MAO)

    if ajuste and ajuste.get("obra_unica"):
        obra = str(ajuste["obra_unica"]).strip().upper()
        quantos = len([d for d in dias_uteis if d["obra"]]) or 0
        return dict(base, origem=DA_MAO,
                    por_obra=[{"obra": obra, "dias": quantos, "valor": valor,
                               "origem": DA_MAO}],
                    por_dia=[{"data": d["data"], "obra": obra,
                              "obra_ponto": d["obra"], "valor": v,
                              "origem": DA_MAO, "empate": d["empate"]}
                             for d, v in zip(
                                 [x for x in dias_uteis if x["obra"]],
                                 _repartir(valor, [1] * quantos))]
                    if quantos else [])

    if regra:
        feito = apropriar_pela_regra(valor, regra)
        return dict(base, origem=DA_REGRA, regra=feito.get("regra", ""),
                    por_obra=feito["por_obra"], por_dia=[])

    feito = apropriar_pelo_ponto(valor, dias_uteis)
    if not feito["por_obra"]:
        # ⚠️ NÃO INVENTA OBRA. Sem dia útil e sem regra, esta pessoa fica SEM
        # apropriação e com crítica — é ela que vai para a faixa "precisa da sua
        # mão". Chutar a obra da contabilidade aqui pareceria funcionar e poria o
        # custo na obra errada em silêncio.
        base["criticas"].append(
            "sem dia de ponto no período e sem regra de rateio — diga para onde "
            "vai o valor desta pessoa")
        return dict(base, origem="", por_obra=[], por_dia=[])

    if any(d["empate"] for d in feito["por_dia"]):
        quantos = len({d["data"] for d in feito["por_dia"] if d["empate"]})
        base["criticas"].append(
            f"{quantos} dia(s) com marcação empatada entre duas obras — valeu a "
            "obra em que o dia começou")
    return dict(base, origem=DO_PONTO, por_obra=feito["por_obra"],
                por_dia=feito["por_dia"])


# ---------------------------------------------------------------------------
# QUEM TEM PONTO E NÃO ESTÁ NESTA FOLHA
# ---------------------------------------------------------------------------
# Aviso do dono em 26/09/2026, e ele mudou uma crítica que eu havia proposto:
#
#     *"No ponto tem mais informação de pessoas do que tem na folha de pagamento,
#     no arquivo. Até porque esse arquivo é de parte do pessoal. Outros entram num
#     outro método de pagamento — o pessoal que não vem da contabilidade, mas tem
#     ponto batido. Aí depois a gente vai criar uma outra tela para verificar
#     eles."*
#
# ⚠️ EU IA TRANSFORMAR ISSO NUM ALERTA, E SERIA RUÍDO. Na minha lista de críticas
# estava "pessoa ativa, com presença, e SEM linha na folha" — que para essas
# pessoas é o estado NORMAL, não um erro. Centenas de alertas esperados por
# quinzena é o jeito mais rápido de fazer ninguém ler mais nenhum alerta desta
# tela, inclusive os que importam.
#
# ⚠️ E QUEM DECIDE SE UM DIA É CTPS OU DIÁRIA É `folha_vinculo`, que é a tradução
# fiel da fórmula que já roda na planilha (coluna AH da aba Mobponto). Ele mandou
# a fórmula em 26/09/2026, depois de eu classificar errado — eu olhava só o `Tipo
# de Cadastro` da pessoa.
#
# A diferença que isso faz: **a classificação é POR DIA.** Quem foi admitido no
# dia 10 tem dias de diária (antes) e dias de CTPS (depois) na MESMA quinzena.
# Classificando por pessoa, metade do dinheiro dela iria para o método de
# pagamento errado.


def com_ponto_fora_da_folha(linhas, dias_por_cpf, cadastro_por_id,
                            periodo) -> dict:
    """Quem bateu ponto no período e NÃO está nesta folha.

    Devolve `{"deveria_estar": [...], "outro_metodo": [...], "sem_cadastro": [...]}`
    — ver o aviso acima para o porquê de serem três listas e não uma.
    """
    ini, fim = periodo or (dt.date.min, dt.date.max)
    na_folha = set()
    for linha in linhas or []:
        id_fortes = str(getattr(linha, "id_fortes", None)
                        or (linha or {}).get("id_fortes") or "")
        cadastro = cadastro_por_id.get(id_fortes) or {}
        if cadastro.get("cpf"):
            na_folha.add(cadastro["cpf"])

    por_cpf = {}
    for cadastro in (cadastro_por_id or {}).values():
        if cadastro.get("cpf"):
            por_cpf[cadastro["cpf"]] = cadastro

    from . import folha_vinculo

    saida = {"deveria_estar": [], "outro_metodo": [], "sem_cadastro": [],
             "falta_data": []}
    for cpf, dias in (dias_por_cpf or {}).items():
        if cpf in na_folha:
            continue
        uteis = [d for d in _dias_uteis_do_ponto(dias, ini, fim) if d["obra"]]
        if not uteis:
            continue        # não bateu ponto NESTE período: não é assunto daqui
        cadastro = por_cpf.get(cpf)
        vinculos = folha_vinculo.classificar_dias(
            [d["data"] for d in uteis], cadastro)
        quem = {"cpf": cpf,
                "nome": (cadastro or {}).get("nome") or "",
                "tipo": (cadastro or {}).get("tipo") or "",
                "dias": len(uteis),
                "obras": sorted({d["obra"] for d in uteis}),
                # Quantos dias de cada vínculo — é isto que mostra o caso de quem
                # foi admitido no meio do período.
                "vinculos": vinculos}
        if cadastro is None:
            saida["sem_cadastro"].append(quem)
        elif vinculos.get(folha_vinculo.CTPS, 0):
            # ⚠️ ALGUM dia de CTPS já manda para o alerta. Exigir que TODOS fossem
            # CTPS esconderia justamente quem foi admitido no meio da quinzena —
            # o caso que mais dá confusão.
            saida["deveria_estar"].append(quem)
        elif vinculos.get(folha_vinculo.FALTA_DATA, 0):
            # Cadastro pela metade: não é diarista nem CTPS, é pendência.
            saida["falta_data"].append(quem)
        else:
            saida["outro_metodo"].append(quem)
    for lista in saida.values():
        lista.sort(key=lambda q: (q["nome"] or q["cpf"]))
    return saida


def apropriar(linhas, dias_por_cpf=None, regras_por_cpf=None,
              ajustes_por_cpf=None, cadastro_por_id=None,
              periodo=None) -> dict:
    """A apropriação da folha inteira. Devolve as pessoas e os totais.

    `cadastro_por_id` é o de/para **ID Fortes → {cpf, nome}**, que é a ponte entre
    a folha da contabilidade (que só traz o ID) e o ponto (que só traz o CPF).

    ⚠️ SEM O ID FORTES NO CADASTRO, A PESSOA FICA PENDENTE — E CONTINUA NA LISTA.
    Correção do dono em 26/09/2026, sobre uma decisão minha que estava errada:

        *"Pessoas sem ID Fortes no cadastro não entram. Na verdade, ela vai
        entrar após tratamento. Vamos tratar para poder entrar. Então não pode
        ficar oculto, escondido."*

    Eu havia tirado essas pessoas da lista e posto numa lista à parte. Ele está
    certo e o erro é grave: lista à parte é lista que alguém esquece de abrir, e
    aí a pessoa **desaparece da folha** — trabalhou e não recebeu, sem nada na
    tela gritando.

    Agora ela fica na MESMA lista, marcada `pendente_cadastro`, sem CPF e sem
    apropriação, com a crítica escrita. E como ela continua contando no total a
    pagar sem ter obra, **a folha não fecha** enquanto ela não for tratada — o que
    é exatamente o que tem de acontecer. `sem_cadastro` continua existindo, mas
    como atalho para a tela montar a faixa "precisa da sua mão"; são os MESMOS
    objetos, não uma cópia que pode divergir.

    O que "tratar" significa: preencher o ID Fortes no cadastro (o certo, porque
    resolve para sempre) ou amarrar o CPF ali na tela (rápido, e o sistema fica
    avisando que o cadastro continua sem o ID).
    """
    dias_por_cpf = dias_por_cpf or {}
    regras_por_cpf = regras_por_cpf or {}
    ajustes_por_cpf = ajustes_por_cpf or {}
    cadastro_por_id = cadastro_por_id or {}
    ini, fim = periodo or (dt.date.min, dt.date.max)

    pessoas = []
    sem_cadastro = []
    for linha in linhas or []:
        id_fortes = getattr(linha, "id_fortes", None) or linha.get("id_fortes")
        cadastro = cadastro_por_id.get(str(id_fortes))
        if not cadastro or not cadastro.get("cpf"):
            # ⚠️ FICA NA LISTA, VISÍVEL E PENDENTE — ver o aviso no alto desta
            # função. Não é lista à parte, é a mesma lista.
            pendente = {
                "id_fortes": id_fortes,
                "cpf": "",
                "nome": getattr(linha, "nome", None) or linha.get("nome") or "",
                "nome_cadastro": "",
                "valor": Decimal(str(getattr(linha, "valor", None)
                                     or linha.get("valor") or 0)).quantize(CENTAVO),
                "filial_contabilidade": (
                    getattr(linha, "filial_nome", None)
                    or (linha.get("filial_nome") if isinstance(linha, dict) else "")),
                "dias_no_ponto": 0, "fora": False, "regra": "",
                "pendente_cadastro": True, "origem": "",
                "por_obra": [], "por_dia": [],
                "criticas": [
                    f"o ID Fortes {id_fortes} não está no cadastro de "
                    "colaboradores — sem ele não há CPF, e sem CPF não há como "
                    "pagar. Preencha o ID no cadastro, ou amarre o CPF aqui."],
            }
            pessoas.append(pendente)
            sem_cadastro.append(pendente)   # o MESMO objeto, não uma cópia
            continue
        cpf = cadastro["cpf"]
        pessoa = apropriar_pessoa(
            linha,
            _dias_uteis_do_ponto(dias_por_cpf.get(cpf), ini, fim),
            regras_por_cpf.get(cpf), ajustes_por_cpf.get(cpf))
        pessoa["cpf"] = cpf
        pessoa["pendente_cadastro"] = False
        # O nome do CADASTRO manda no arquivo de pagamento: é o que o banco
        # confere contra o CPF. O da contabilidade fica para a conferência.
        pessoa["nome_cadastro"] = cadastro.get("nome") or pessoa["nome"]
        pessoas.append(pessoa)

    por_obra: dict = {}
    for pessoa in pessoas:
        if pessoa["fora"]:
            continue
        for parte in pessoa["por_obra"]:
            alvo = por_obra.setdefault(
                parte["obra"], {"obra": parte["obra"], "valor": Decimal("0"),
                                "pessoas": 0, "origens": set()})
            alvo["valor"] += parte["valor"]
            alvo["pessoas"] += 1
            alvo["origens"].add(parte["origem"])

    apropriado = sum((o["valor"] for o in por_obra.values()), Decimal("0"))
    # ⚠️ O PENDENTE CONTA NO "A PAGAR", e é isso que faz a folha NÃO FECHAR
    # enquanto ele não for tratado. Tirá-lo da soma faria a tela dizer "fecha" com
    # gente de fora — o pior resultado possível, porque é o que convence alguém a
    # apertar o botão.
    a_pagar = sum((p["valor"] for p in pessoas if not p["fora"]),
                  Decimal("0"))
    return {
        "pessoas": pessoas,
        "sem_cadastro": sem_cadastro,
        "total_da_folha": sum((p["valor"] for p in pessoas), Decimal("0")),
        "por_obra": sorted(
            [{**o, "origens": sorted(o["origens"])} for o in por_obra.values()],
            key=lambda o: o["obra"]),
        "total_a_pagar": a_pagar,
        "total_apropriado": apropriado,
        # ⚠️ A CONFERÊNCIA QUE FECHA TUDO: o que se paga tem de ser exatamente o
        # que foi apropriado. Se não bate, alguém ficou sem obra (e está na lista
        # de críticas) ou uma conta perdeu centavo.
        "fecha": a_pagar == apropriado,
        "sem_apropriacao": [p for p in pessoas
                            if not p["fora"] and not p["por_obra"]],
        "fora": [p for p in pessoas if p["fora"]],
        # Quem bateu ponto e não está nesta folha — separado por tipo, para o
        # esperado não virar alerta. Ver `com_ponto_fora_da_folha`.
        "fora_da_folha": com_ponto_fora_da_folha(
            linhas, dias_por_cpf, cadastro_por_id, (ini, fim)),
    }
