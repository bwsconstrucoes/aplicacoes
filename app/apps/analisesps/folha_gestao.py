# -*- coding: utf-8 -*-
"""
A GESTÃO DA FOLHA DA CONTABILIDADE: a tela onde o arquivo importado se trabalha.

⚠️ POR QUE ESTE MÓDULO EXISTE, nas palavras do dono em 29/09/2026:

    *"Eu importo o arquivo e não tenho gestão nenhuma sobre as informações dele.
    Quem vai, quem não vai. (…) Cadê as informações de cada funcionário, cadê os
    dados deles, cadê uma tabela mostrando as informações, cadê a possibilidade de
    seleção deles de quem entra e quem não entra, cadê onde gera o arquivo de
    pagamento?"*

E, no mesmo dia, o que decide o total por obra:

    *"A informação do arquivo não é absoluta e é toda gerenciável, e ainda tem toda
    a relação com o ponto. Mas aqui já devemos usar a folha de ponto mesmo, visto
    que tem o rateio diário pra formar os totalizadores por obra."*

⚠️ O QUE ESTAVA ERRADO NÃO ERA A CONTA — ERA A FALTA DE PORTA. A apropriação pelo
ponto (`folha_apropriacao.py`), o ajuste à mão (`folha_apropriacao_guardada.py`),
os layouts de arquivo (`folha_geracao.py`) e o log (`folha_pagamento.py`) já
estavam escritos e testados. O que não existia era **uma tela onde tudo isso
aparecesse junto** — e o único caminho para a lista pessoa por pessoa era o número
embaixo de "Precisam de olho", que some quando não há ninguém pendente. Quem
importava a folha e não achava aquele número não tinha porta nenhuma.

A DIVISÃO DE TRABALHO, que este módulo NÃO quebra:

    folha_sintetica.py             lê o `.xls` (nenhum banco)
    folha_arquivo.py               guarda a folha crua
    ponto.py                       guarda e devolve o ponto, dia por dia
    folha_apropriacao.py           a conta pura: de qual obra é cada real
    folha_apropriacao_guardada.py  o ajuste dele e o fechamento
    folha_geracao.py               o layout dos arquivos
    folha_pagamento.py             gera, sobe no Drive e registra
    ESTE                           junta tudo numa tela e filtra

⚠️ NADA DE CONTA NOVA AQUI. Se aparecer aritmética de dinheiro neste arquivo,
está no lugar errado: a conta é verificável sem banco em `folha_apropriacao.py`, e
duplicá-la aqui faria a tela e o arquivo de pagamento divergirem no dia em que
alguém corrigisse só um dos dois.
"""
from __future__ import annotations

import logging
from decimal import Decimal

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")
CEM = Decimal("100")

# ---------------------------------------------------------------------------
# AS SITUAÇÕES DE UMA LINHA DA FOLHA
#
# ⚠️ UMA SÓ POR PESSOA, e a ordem abaixo é a ordem de prioridade. Ela existe
# porque a coluna é lida pela COR: duas situações na mesma linha obrigariam a
# pessoa a ler o texto de 491 linhas para saber o que fazer.
#
# A ordem é a ordem do que IMPEDE pagar, do mais grave para o menos:
#   1. sem cadastro  — não há CPF, então não há como pagar
#   2. sem obra      — não há para onde jogar o custo
#   3. fora          — ele tirou do pagamento de propósito
#   4. saiu          — não se paga a quem saiu
#   5. saindo        — pode ter valor devido; é para conferir
#   6. empate        — dia em duas obras; quase sempre erro de batida
#   7. paga          — o normal
# ---------------------------------------------------------------------------
SEM_CADASTRO = "sem_cadastro"
SEM_OBRA = "sem_obra"
FORA = "fora"
SAIU = "saiu"
SAINDO = "saindo"
EMPATE = "empate"
PAGA = "paga"

ORDEM_DAS_SITUACOES = (SEM_CADASTRO, SEM_OBRA, FORA, SAIU, SAINDO, EMPATE, PAGA)

ROTULO_DA_SITUACAO = {
    SEM_CADASTRO: "sem cadastro",
    SEM_OBRA: "sem obra",
    FORA: "fora do pagamento",
    SAIU: "já saiu",
    SAINDO: "está saindo",
    EMPATE: "dia empatado",
    PAGA: "vai receber",
}

# ⚠️ A COR DE CADA SITUAÇÃO, e o `selo.pagar` é VERMELHO neste módulo — é o selo
# de "pendente/urgente" do padrão das Solicitações, não de "vai pagar". Já usei
# errado uma vez e o verde de quem recebe é `aprovado`.
SELO_DA_SITUACAO = {
    SEM_CADASTRO: "risco",
    SEM_OBRA: "pagar",
    FORA: "cancelado",
    SAIU: "risco",
    SAINDO: "saindo",
    EMPATE: "saindo",
    PAGA: "aprovado",
}

# Teto de linhas desenhadas de uma vez. A folha tem ~500 pessoas; o teto existe
# para um filtro torto não montar uma tela de dez mil linhas.
TETO_DA_LISTA = 2000


class ErroDaGestao(RuntimeError):
    """Não deu para montar. A frase vai inteira para a tela."""


def _dinheiro(valor) -> Decimal:
    try:
        return Decimal(str(valor or 0)).quantize(CENTAVO)
    except Exception:  # noqa: BLE001
        return Decimal("0.00")


def _por_dia(valor, dias) -> Decimal | None:
    """O líquido dividido pelos dias de presença. `None` quando não há dia.

    ⚠️ `None` E NÃO ZERO: quem tem zero dias de ponto é justamente o caso que
    precisa da mão dele (o "PT" da planilha), e "R$ 0,00 por dia" pareceria um
    valor calculado. Traço na tela é pergunta aberta; zero é resposta errada."""
    dias = int(dias or 0)
    if dias <= 0:
        return None
    return (_dinheiro(valor) / dias).quantize(CENTAVO)


# ---------------------------------------------------------------------------
# O RESUMO DAS OBRAS DE UMA PESSOA
# ---------------------------------------------------------------------------
def resumo_das_obras(por_obra) -> str:
    """`"ABC · 10d + DEF · 2d"` — o que cabe numa célula.

    ⚠️ MOSTRA OS DIAS, não o percentual. Pedido do dono, que trabalha por dia:
    o percentual é consequência, e conferir "10 dias na obra tal" contra o ponto
    é possível; conferir "83,33%" não é."""
    partes = []
    for p in (por_obra or []):
        dias = int(p.get("dias") or 0)
        partes.append(f"{p.get('obra')} · {dias}d" if dias else str(p.get("obra")))
    return " + ".join(partes)


def obra_principal(por_obra) -> str:
    """A obra de mais dias — e, no empate de dias, a de maior valor.

    Serve para FILTRAR e ORDENAR, nunca para pagar: quem paga é a divisão inteira.
    """
    melhor = None
    for p in (por_obra or []):
        chave = (int(p.get("dias") or 0), _dinheiro(p.get("valor")))
        if melhor is None or chave > melhor[0]:
            melhor = (chave, str(p.get("obra") or ""))
    return melhor[1] if melhor else ""


# ---------------------------------------------------------------------------
# A SITUAÇÃO DE UMA LINHA
# ---------------------------------------------------------------------------
def situacao_da_pessoa(pessoa, ficha) -> str:
    """Uma situação só, pela ordem de prioridade do alto do arquivo."""
    from . import colaboradores

    if pessoa.get("pendente_cadastro") or not pessoa.get("cpf"):
        return SEM_CADASTRO
    if pessoa.get("fora"):
        return FORA
    if not pessoa.get("por_obra"):
        return SEM_OBRA
    situacao_cadastral = (ficha or {}).get("situacao")
    if situacao_cadastral == colaboradores.SITUACAO_SAIU:
        return SAIU
    if situacao_cadastral == colaboradores.SITUACAO_SAINDO:
        return SAINDO
    if any(d.get("empate") for d in (pessoa.get("por_dia") or [])):
        return EMPATE
    return PAGA


# ---------------------------------------------------------------------------
# O TOTAL POR OBRA
# ---------------------------------------------------------------------------
def totais_por_obra(por_obra) -> list:
    """Obra por obra: quantas pessoas, quantos dias, quanto e que percentual.

    ⚠️ O PERCENTUAL SAI DE `folha_geracao.percentuais_por_obra`, não de uma divisão
    escrita aqui: é o mesmo número que vai para o card do Pipefy e para o arquivo de
    análise, com as mesmas sete casas e o mesmo fechamento de 100%. Duas contas de
    percentual divergiriam no centavo, e aí a tela e o card diriam coisas
    diferentes sobre o mesmo rateio."""
    from . import folha_geracao

    percentuais = {p["obra"]: p["percentual"] for p in
                   folha_geracao.percentuais_por_obra(
                       [{"obra": o.get("obra"), "total": o.get("valor")}
                        for o in (por_obra or [])])}
    saida = []
    for o in (por_obra or []):
        obra = str(o.get("obra") or "")
        saida.append({
            "obra": obra,
            "pessoas": int(o.get("pessoas") or 0),
            "valor": _dinheiro(o.get("valor")),
            "origens": list(o.get("origens") or []),
            "percentual": percentuais.get(obra),
        })
    return sorted(saida, key=lambda o: -o["valor"])


# ---------------------------------------------------------------------------
# POR CONTA CORRENTE — o bloco que existia na planilha e faltava aqui
#
# ⚠️ ELE NÃO PRECISOU PEDIR: está na planilha (o "bloco da direita", §3), está na
# fórmula (coluna `AF` das abas Quinzena e Fim de Mês, §7.10.5) e está no desenho
# que EU escrevi e ele aprovou (§5.2: a prévia mostra "quantas pessoas, quanto, por
# obra, **por conta corrente**, e a lista de críticas").
#
# Para que serve, e é por isso que não é enfeite: cada conta corrente vira uma **SP
# de Transferência de Recursos** e um arquivo de pagamento próprio. Sem este bloco
# ele não tem como saber, ANTES de gerar, quantos arquivos vão sair nem quanto sai
# de cada conta — que é exatamente o que ele confere hoje olhando a planilha.
# ---------------------------------------------------------------------------
def totais_por_conta(por_obra) -> list:
    """Conta corrente por conta corrente: quais obras, quanto, e o que falta.

    Obra sem conta cadastrada entra numa linha própria, marcada — ela é a crítica
    que segura o arquivo (§7.14.4), e some-la numa conta qualquer seria o padrão
    silencioso que a planilha tem e que este sistema existe para não repetir."""
    from . import folha_pagamento

    try:
        contas = folha_pagamento.conta_por_obra()
    except Exception:  # noqa: BLE001 — é um bloco da tela, não a tela
        logger.exception("Folha: não consegui ler as contas das obras")
        contas = {}

    juntas: dict = {}
    for o in (por_obra or []):
        obra = str(o.get("obra") or "").upper()
        conta = contas.get(obra, "")
        alvo = juntas.setdefault(conta, {"conta": conta, "obras": [],
                                         "valor": Decimal("0.00"),
                                         "pessoas": 0, "sem_conta": not conta})
        alvo["obras"].append(obra)
        alvo["valor"] += _dinheiro(o.get("valor"))
        alvo["pessoas"] += int(o.get("pessoas") or 0)
    # A sem conta vem PRIMEIRO, porque é a que impede gerar; depois pela maior.
    return sorted(juntas.values(), key=lambda c: (not c["sem_conta"], -c["valor"]))


# ---------------------------------------------------------------------------
# A CONTA DA FOLHA, NUM LUGAR SÓ
#
# ⚠️ A TELA E O FECHAMENTO PASSAM PELOS MESMOS OLHOS. Montar a apropriação em dois
# lugares é o jeito certo de, um dia, congelar um número diferente do que estava
# desenhado na tela — e aí o arquivo de pagamento não explicaria mais a tela que o
# autorizou. Quem precisa da conta chama isto.
# ---------------------------------------------------------------------------
def apropriar_a_folha(folha) -> dict:
    """A apropriação de uma folha inteira, com o que a alimentou.

    ⚠️ SEMPRE A FOLHA INTEIRA, nunca a lista filtrada: apropriar o que está
    aparecendo na tela congelaria meia folha, e o arquivo sairia faltando gente."""
    from . import colaboradores, folha_apropriacao, folha_rateio
    from . import folha_apropriacao_guardada as guardada
    from . import ponto as mod_ponto

    ano, mes, tipo = folha["ano"], folha["mes"], folha["tipo"]
    periodo = folha_apropriacao.periodo_do_pagamento(ano, mes, tipo)

    regras_por_cpf = {}
    for regra in folha_rateio.listar(so_ativas=True):
        for pessoa in regra.get("pessoas") or []:
            regras_por_cpf[pessoa["cpf"]] = regra

    ajustes = guardada.ajustes_do_pagamento(ano, mes, tipo)
    dias_por_cpf = mod_ponto.dias_por_cpf(ano, mes)

    return {
        "periodo": periodo,
        "ajustes": ajustes,
        "dias_por_cpf": dias_por_cpf,
        # ⚠️ A CONTA NÃO ESTÁ AQUI, e não pode vir para cá: ela é função pura em
        # `folha_apropriacao.py`, verificável sem subir banco nem tela.
        "apropriado": folha_apropriacao.apropriar(
            folha["linhas"], dias_por_cpf=dias_por_cpf,
            regras_por_cpf=regras_por_cpf, ajustes_por_cpf=ajustes,
            cadastro_por_id=colaboradores.de_para_do_fortes(),
            periodo=periodo),
    }


# ---------------------------------------------------------------------------
# MONTAR A TELA
# ---------------------------------------------------------------------------
def montar(folha_id: int, filtros=None) -> dict:
    """Tudo o que a tela da folha desenha, já filtrado.

    ⚠️ OS TOTAIS DE CIMA SÃO DA FOLHA INTEIRA, NÃO DO FILTRO — e isso é decisão,
    não descuido. O dono marca gente olhando o total andar (*"na planilha à medida
    que vamos marcando já vamos vendo os valores"*); um total que mudasse ao
    filtrar diria que ele tirou alguém do pagamento quando ele só escondeu uma
    linha. O que o filtro mostra tem o seu próprio subtotal, dito como tal.
    """
    from . import colaboradores, folha_arquivo, folha_rateio
    from . import folha_apropriacao_guardada as guardada
    from . import ponto as mod_ponto

    filtros = filtros or {}
    folha = folha_arquivo.abrir(folha_id)
    if folha is None:
        return {}

    ano, mes, tipo = folha["ano"], folha["mes"], folha["tipo"]
    feito = apropriar_a_folha(folha)
    apropriado = feito["apropriado"]
    periodo = feito["periodo"]
    ajustes = feito["ajustes"]
    dias_por_cpf = feito["dias_por_cpf"]
    carga = mod_ponto.carga_do_mes(ano, mes)

    # --- o cadastro de quem está nesta folha ------------------------------
    cpfs = [p["cpf"] for p in apropriado["pessoas"] if p.get("cpf")]
    fichas = colaboradores.muitos_por_cpf(cpfs, ate=periodo[1] if periodo else None)
    obras_por_nome = colaboradores.codigos_das_obras()

    # --- a linha da tela --------------------------------------------------
    pessoas = []
    for pessoa in apropriado["pessoas"]:
        ficha = fichas.get(pessoa.get("cpf") or "") or {}
        ajuste = ajustes.get(pessoa.get("cpf") or "") or {}
        situacao = situacao_da_pessoa(pessoa, ficha)
        linha = dict(pessoa)
        linha.update({
            "nome_na_tela": (pessoa.get("nome_cadastro")
                             or pessoa.get("nome") or ""),
            "nome_contabilidade": pessoa.get("nome") or "",
            "cpf_bonito": folha_rateio.cpf_bonito(pessoa.get("cpf") or ""),
            "fase": ficha.get("fase") or "",
            "cargo": ficha.get("cargo") or "",
            "link_pipefy": ficha.get("link_pipefy") or "",
            "motivo_cadastral": ficha.get("motivo") or "",
            # ⚠️ A OBRA DO CADASTRO É O SEGUNDO RECURSO, e só isso. Decisão do dono
            # em 29/09/2026: a obra é a do ponto; sem ponto, a do cadastro. Ela
            # NÃO entra na conta — aparece na tela para ele decidir, porque
            # apropriar por ela em silêncio poria o custo na obra errada.
            "obra_do_cadastro": colaboradores.resolver_obra(ficha, obras_por_nome),
            # ⚠️ O "VALOR X DIA" É COLUNA DA PLANILHA (col I das abas Quinzena e Fim
            # de Mês: `F/H`, o líquido dividido pelos dias). Anotado em
            # `docs/FOLHA_DE_PAGAMENTO.md` §3 como o número COM O QUAL o valor é
            # rateado — e é o que ele confere: "valor por dia × dias na obra" se
            # verifica de cabeça, o total da obra não.
            "valor_por_dia": _por_dia(pessoa.get("valor"),
                                      pessoa.get("dias_no_ponto")),
            "obras_resumo": resumo_das_obras(pessoa.get("por_obra")),
            "obra_principal": obra_principal(pessoa.get("por_obra")),
            "situacao": situacao,
            "situacao_rotulo": ROTULO_DA_SITUACAO.get(situacao, situacao),
            "selo": SELO_DA_SITUACAO.get(situacao, ""),
            "entra": not pessoa.get("fora"),
            "tem_ajuste": bool(ajuste),
            "ajuste": ajuste,
            "dias_empatados": sorted({d["data"] for d in
                                      (pessoa.get("por_dia") or [])
                                      if d.get("empate")}),
        })
        pessoas.append(linha)

    # --- OS TOTAIS DA FOLHA INTEIRA --------------------------------------
    contagem = {chave: 0 for chave in ORDEM_DAS_SITUACOES}
    for p in pessoas:
        contagem[p["situacao"]] = contagem.get(p["situacao"], 0) + 1

    entram = [p for p in pessoas if p["entra"]]
    totais = {
        "pessoas": len(pessoas),
        "total_da_folha": _dinheiro(apropriado["total_da_folha"]),
        "entram": len(entram),
        "valor_entra": sum((_dinheiro(p["valor"]) for p in entram),
                           Decimal("0.00")),
        "fora": contagem.get(FORA, 0),
        "valor_fora": sum((_dinheiro(p["valor"]) for p in pessoas
                           if not p["entra"]), Decimal("0.00")),
        "sem_obra": contagem.get(SEM_OBRA, 0),
        "sem_cadastro": contagem.get(SEM_CADASTRO, 0),
        "empatados": contagem.get(EMPATE, 0),
        "total_apropriado": _dinheiro(apropriado["total_apropriado"]),
        "fecha": bool(apropriado["fecha"]),
        # ⚠️ O QUE FALTA APROPRIAR é o número que decide se dá para gerar o
        # arquivo, e é ele que a tela põe em vermelho.
        "falta_apropriar": (_dinheiro(apropriado["total_a_pagar"])
                            - _dinheiro(apropriado["total_apropriado"])),
    }

    # --- O TOTAL POR OBRA, que é o que ele usa para decidir o rateio ------
    por_obra = totais_por_obra(apropriado["por_obra"])
    por_conta = totais_por_conta(por_obra)

    # --- os filtros -------------------------------------------------------
    mostradas = _filtrar(pessoas, filtros)
    subtotal = sum((_dinheiro(p["valor"]) for p in mostradas), Decimal("0.00"))

    return {
        "folha": folha,
        "periodo": periodo,
        "pessoas": mostradas[:TETO_DA_LISTA],
        "quantas_mostradas": len(mostradas),
        "passou_do_teto": len(mostradas) > TETO_DA_LISTA,
        "subtotal_do_filtro": subtotal,
        "totais": totais,
        "contagem": contagem,
        "por_obra": por_obra,
        "por_conta": por_conta,
        "obras_sem_conta": [o for c in por_conta if c["sem_conta"]
                            for o in c["obras"]],
        "fases": sorted({p["fase"] for p in pessoas if p["fase"]}),
        "obras": sorted({p["obra_principal"] for p in pessoas
                         if p["obra_principal"]}),
        "ponto": {
            "tem_carga": bool(carga),
            "carga": carga,
            # Carga terminada mas com menos páginas do que a API prometeu: a
            # folha em cima dela sai com gente faltando dia, e isso tem de
            # estar escrito na tela, não só na tela do Ponto.
            "completa": bool(carga.get("completa", True)) if carga else True,
            "pessoas_no_ponto": len(dias_por_cpf),
        },
        "fechamento": guardada.fechamento(ano, mes, tipo),
        "fora_da_folha": apropriado["fora_da_folha"],
        "filtros": dict(filtros),
    }


def _filtrar(pessoas, filtros) -> list:
    """Recorta a lista. Filtro vazio não recorta nada.

    ⚠️ RECORTA NO SERVIDOR, e é de propósito: esconder linha no navegador faria o
    subtotal do filtro mentir, porque ele é somado aqui."""
    from .folha_rateio import so_digitos

    busca = " ".join(str(filtros.get("busca") or "").split()).lower()
    digitos = so_digitos(busca)
    obra = " ".join(str(filtros.get("obra") or "").split()).upper()
    fase = " ".join(str(filtros.get("fase") or "").split())
    situacao = str(filtros.get("situacao") or "").strip()
    origem = str(filtros.get("origem") or "").strip()

    saida = []
    for p in pessoas:
        if busca:
            alvo = f"{p['nome_na_tela']} {p['nome_contabilidade']}".lower()
            achou = busca in alvo
            if not achou and len(digitos) >= 3:
                achou = digitos in (p.get("cpf") or "")
            if not achou:
                achou = busca == str(p.get("id_fortes") or "").lower()
            if not achou:
                continue
        if obra:
            # A obra casa em QUALQUER parte da divisão, não só na principal: quem
            # tem 2 dias numa obra também é dessa obra, e quem procura pela obra
            # quer ver o custo dela inteiro.
            if obra not in {str(x.get("obra") or "").upper()
                            for x in (p.get("por_obra") or [])}:
                continue
        if fase and (p.get("fase") or "") != fase:
            continue
        if situacao and p.get("situacao") != situacao:
            continue
        if origem and (p.get("origem") or "") != origem:
            continue
        saida.append(p)
    return saida


# ---------------------------------------------------------------------------
# DIVIDIR OS DIAS DE UMA PESSOA ENTRE OBRAS — o nível 3 do ajuste fino
#
# ⚠️ ELE JÁ PEDIU ISTO, com estas palavras (26/09/2026, §7.3):
#
#     *"Às vezes eu distribuo em várias obras: bota um dia numa obra, um dia em
#     outra obra. Aí eu altero a planilha do ponto de onde ela puxa."*
#
# ⚠️ ENTRA DIA, NÃO VALOR, e isto não é atalho: o valor por dia é o líquido dividido
# pelos dias de presença (coluna I da planilha), e é ele que mantém a conta
# verificável de cabeça. Se ele digitasse valores, duas obras poderiam ficar com
# valores por dia diferentes para a mesma pessoa no mesmo período — e aí o relatório
# não explicaria mais nada.
#
# A sobra do centavo segue a MESMA regra do ponto (`_repartir`): fica com a obra de
# mais dias. Duas regras de centavo fariam a tela e o arquivo divergirem em um real
# a cada quinhentas pessoas, que é o tipo de diferença que ninguém acha.
# ---------------------------------------------------------------------------
def dividir_por_dias(valor, partes) -> list:
    """`[{obra, dias}]` + o valor da pessoa → `[{obra, dias, valor}]`.

    Levanta `ErroDaGestao` quando não há dia nenhum: dividir por zero dias não é
    divisão, é apagar o valor da pessoa."""
    from .folha_apropriacao import _repartir

    limpas = []
    for p in (partes or []):
        obra = " ".join(str((p or {}).get("obra") or "").split()).upper()
        dias = int((p or {}).get("dias") or 0)
        if not obra:
            raise ErroDaGestao("uma das linhas da divisão está sem obra.")
        if dias <= 0:
            raise ErroDaGestao(
                f'a obra "{obra}" está com zero dia. Tire a linha ou diga os dias.')
        limpas.append({"obra": obra, "dias": dias})
    if not limpas:
        raise ErroDaGestao("diga em quais obras entram os dias desta pessoa.")

    repetida = next((p["obra"] for p in limpas
                     if [x["obra"] for x in limpas].count(p["obra"]) > 1), "")
    if repetida:
        raise ErroDaGestao(
            f'a obra "{repetida}" aparece mais de uma vez. Junte os dias dela '
            "numa linha só.")

    valores = _repartir(_dinheiro(valor), [p["dias"] for p in limpas])
    return [{**p, "valor": v} for p, v in zip(limpas, valores)]


# ---------------------------------------------------------------------------
# FECHAR A APROPRIAÇÃO DESTA FOLHA
# ---------------------------------------------------------------------------
def fechar(folha_id: int, quem: str = "") -> dict:
    """Congela a apropriação desta folha, para o arquivo poder sair.

    ⚠️ É AQUI QUE O BOTÃO "GERAR" COMEÇA A FAZER SENTIDO. `folha_pagamento.gerar`
    só paga apropriação FECHADA (e o motivo está na docstring dele: arquivo que
    sai de cálculo em memória muda de explicação quando o ponto é recarregado).
    Antes desta função não havia nenhum caminho de tela até um fechamento da verba
    `folha` — então o arquivo da folha da contabilidade era, na prática,
    impossível de gerar.

    Não decide se PODE: quem recusa por crítica é a tela e o `gerar`. Aqui
    congela-se o que há, inclusive quando não fecha — ver `guardada.fechar`."""
    from . import folha_apropriacao_guardada as guardada
    from . import folha_arquivo

    folha = folha_arquivo.abrir(folha_id)
    if folha is None:
        raise ErroDaGestao("esta folha não está mais aqui.")
    apropriado = apropriar_a_folha(folha)["apropriado"]

    novo = guardada.fechar(folha["ano"], folha["mes"], folha["tipo"],
                           apropriado, verba=guardada.VERBA_FOLHA, quem=quem)
    return {"id": novo, "fecha": bool(apropriado["fecha"]),
            "pessoas": len([p for p in apropriado["pessoas"]
                            if not p.get("fora")]),
            "total": _dinheiro(apropriado["total_apropriado"]),
            "competencia": folha["competencia"]}
