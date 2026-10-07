# -*- coding: utf-8 -*-
"""
Auxílio alimentação e auxílio transporte.

As duas verbas são quase a mesma conta, e a diferença é decisão do dono, de
27/09/2026:

    "O fato de se estar de férias e um feriado em dia de semana poderia sim afetar
     o cálculo do auxílio transporte. (…) Um único dia não precisaria, mas férias,
     como são mais dias, sim, deveríamos proporcionalizar."

| | Alimentação | Transporte |
|---|---|---|
| desconta **feriado** | sim | **não** |
| desconta **férias** | sim | sim |

⚠️ O TRANSPORTE NÃO DESCONTAVA NADA na planilha (§7.14.7): as colunas de feriado
existiam, eram calculadas e **não entravam na conta**. Ele decidiu que férias
passam a descontar. Isto aqui é a decisão dele, não o que a planilha fazia.

A CONTA, do jeito que a planilha faz (§7.14.6), por modalidade:

    Mês               valor fixo, não conta dia
    Mensal            todos os dias do mês − 3 (alimentação); no TRANSPORTE,
                      valor fixo do mês, como "Mês" — correção do dono, 03/10/2026
    Segunda à Sexta   dias úteis (sábado e domingo fora)
    Segunda à Quinta  dias úteis menos as sextas

E depois: `dias = base − feriados − férias + ajuste`, `valor = valor do dia × dias`.

⚠️ QUEM SAIU, QUEM ESTÁ AFASTADO E QUEM TEM "CARTÃO" NÃO RECEBE. As duas primeiras
são a mesma exclusão que as abas da planilha já fazem pela fase; a terceira é só do
transporte, e também vem da planilha (`AA != 'Cartão'`).
"""
from __future__ import annotations

import calendar
import datetime as dt
import logging
from decimal import Decimal

from . import colaboradores, folha_calendario

logger = logging.getLogger("analisesps.folha_auxilio")

ALIMENTACAO = "alimentacao"
TRANSPORTE = "transporte"
TIPOS = (ALIMENTACAO, TRANSPORTE)

ROTULO_DO_TIPO = {ALIMENTACAO: "Auxílio alimentação",
                  TRANSPORTE: "Auxílio transporte"}

CENTAVO = Decimal("0.01")

# As modalidades que o cadastro usa, como a planilha as escreve. A comparação é
# sem acento e sem caixa — o cadastro é digitado por gente.
MODO_FIXO = "mes"                    # "Mês": valor fechado, não conta dia
MODO_MENSAL = "mensal"               # alimentação: dias do mês − 3; transporte: fixo
MODO_SEG_SEX = "segunda a sexta"
MODO_SEG_QUI = "segunda a quinta"

# O que o transporte NÃO paga em dinheiro, porque a pessoa tem cartão.
MODO_CARTAO = "cartao"

# ⚠️ AS CATEGORIAS "DIÁRIAS" DO CADASTRO (07/10/2026). Eram "categoria não
# reconhecida" — zero dias, cadastro incompleto, e a pessoa SUMIA da lista e do
# arquivo. O dono comparou com a base do script: 27 de 38 colaboradores que
# desapareceram do transporte eram "Diário", "Vale Transporte" e "Diário e Vale
# Transporte". A regra dele: *"Diário: valor cadastrado × quantidade de
# auxílios, depois os ajustes de dias"* — a quantidade é a dos dias úteis (22 em
# setembro/2026 nos exemplos dele), a mesma régua de "Segunda à Sexta". "Vale
# Transporte" e "Diário e Vale Transporte" seguem a mesma conta (o exemplo dele:
# 9,00 × 22 = 198,00) — SUPOSIÇÃO para "Vale Transporte" puro, dita na tela.
MODOS_DIARIOS = ("diario", "vale transporte", "diario e vale transporte")
# Valor "por dia" acima disto quase certamente é o valor do MÊS cadastrado numa
# categoria diária: multiplicar por 22 pagaria 22 vezes. Não paga — e diz por quê.
TETO_DO_VALOR_DIARIO = Decimal("100.00")


class ErroDoAuxilio(RuntimeError):
    """Não deu para calcular ou gravar. A frase vai para a tela."""


def _pronto() -> bool:
    """A migração 033 já rodou? Enquanto não, a tela avisa em vez de estourar."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.auxilio_ajuste LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


def _sem_acento(texto) -> str:
    """Para comparar modalidade: "Segunda à Quinta" e "segunda a quinta" são a
    mesma coisa, e o cadastro é digitado por gente."""
    import unicodedata
    cru = unicodedata.normalize("NFKD", str(texto or ""))
    return " ".join("".join(c for c in cru if not unicodedata.combining(c))
                    .lower().split())


def periodo_do_mes(ano: int, mes: int) -> tuple:
    """O primeiro e o último dia do mês.

    ⚠️ O AUXÍLIO É POR MÊS INTEIRO, não por quinzena — é o que a planilha faz
    (§7.14.6). Usar o período da quinzena pagaria metade do transporte de alguém
    duas vezes."""
    ultimo = calendar.monthrange(int(ano), int(mes))[1]
    return dt.date(int(ano), int(mes), 1), dt.date(int(ano), int(mes), ultimo)


def valor_fechado(modo: str, tipo: str = "") -> bool:
    """O valor do cadastro já é o do MÊS — não se multiplica por dia.

    ⚠️ "MENSAL" NO TRANSPORTE É VALOR DO MÊS (dono, 03/10/2026): *"Mensal é
    mensal. Aquele valor que está lá já é o valor mensal. Aí você está
    multiplicando a base, quantidade de dias, pelo valor que é mensal."* A conta
    "dias do mês − 3" veio da aba de alimentação da planilha (§7.14.6) e foi
    aplicada ao transporte também; no transporte ela multiplicava o valor do mês
    por ~27. Na alimentação a regra da planilha continua (lá "Mês" e "Mensal"
    convivem e são coisas diferentes)."""
    limpo = _sem_acento(modo)
    return limpo == MODO_FIXO or (tipo == TRANSPORTE and limpo == MODO_MENSAL)


def dias_da_modalidade(modo: str, inicio, fim, tipo: str = "") -> tuple:
    """`(dias, conta_sabado, conta_sexta)` da modalidade, antes dos descontos.

    Devolve também COMO contar, porque o desconto de feriado e de férias tem de
    usar a mesma régua: descontar um feriado de sexta de quem não trabalha sexta
    tiraria um dia que ninguém ia pagar."""
    limpo = _sem_acento(modo)
    if valor_fechado(modo, tipo):
        # Valor fechado: um "dia" só, que multiplica o valor inteiro.
        return 1, False, True
    if limpo == MODO_MENSAL:
        # Todos os dias do mês menos três — é a fórmula da planilha, e o "−3" é
        # dela, não meu. Não desconta fim de semana, então a régua conta tudo.
        return max(0, (fim - inicio).days + 1 - 3), True, True
    if limpo == MODO_SEG_QUI:
        return folha_calendario.dias_uteis(inicio, fim, sexta=False), False, False
    if limpo == MODO_SEG_SEX or limpo in MODOS_DIARIOS:
        return folha_calendario.dias_uteis(inicio, fim), False, True
    # Modalidade desconhecida: NÃO chuta. Zero dias, e a tela diz que não sabe —
    # pagar por uma régua inventada é pior que não pagar e alguém reclamar.
    return 0, False, True


def _valor(bruto) -> Decimal | None:
    if bruto is None:
        return None
    try:
        return Decimal(str(bruto)).quantize(CENTAVO)
    except Exception:  # noqa: BLE001
        return None


def calcular_pessoa(tipo: str, ficha: dict, inicio, fim,
                    ajuste: dict | None = None,
                    codigo_da_obra: str = "",
                    obra_do_ponto: str = "", dias_na_obra: int = 0,
                    ausencias=None) -> dict:
    """A conta de uma pessoa. Devolve tudo o que a tela mostra, passo a passo.

    ⚠️ DEVOLVE O CAMINHO INTEIRO, não só o valor: base, feriados, férias, ajuste e
    total. É o que ele pediu para o relatório — *"saber até de onde é que foi que
    veio aquela informação"*. Um valor sozinho não se audita."""
    ajuste = ajuste or {}
    e_alimentacao = tipo == ALIMENTACAO
    modo = (ficha.get("modo_alimentacao") if e_alimentacao
            else ficha.get("modo_transporte")) or ""
    valor_unitario = _valor(ficha.get("valor_alimentacao") if e_alimentacao
                            else ficha.get("valor_transporte"))

    saida = {
        "cpf": ficha.get("cpf", ""),
        "cpf_bonito": ficha.get("cpf_bonito", ""),
        "nome": ficha.get("nome", ""),
        "cargo": ficha.get("cargo", ""),
        "matricula": ficha.get("matricula", ""),
        "link_pipefy": ficha.get("link_pipefy", ""),
        # A coluna BQ da planilha: é onde o DP escreve o porquê de uma exceção, e
        # é o que ele quer ver clicando na pessoa.
        "observacao_cadastro": ficha.get("observacao_auxilio", ""),
        "modo": modo,
        "valor_unitario": valor_unitario,
        # ⚠️ A OBRA É O CÓDIGO, e NÃO é campo de digitar. Correção do dono em
        # 28/09/2026: *"em obra tem que colocar o código da obra e não a obra por
        # extenso"* e *"eu não sei por que você colocou um campo editável"*. Obra
        # digitada aqui divergiria do cadastro e do rateio, e ninguém saberia qual
        # das duas manda.
        #
        # ⚠️ DE ONDE ELA VEM MUDOU EM 29/09/2026, por pedido dele: *"em Alimentação
        # a informação de obra deveria ser a do Ponto. Caso não tenha, usar a de
        # cadastro."* Quem escolhe é `calcular`; aqui guarda-se a escolha E A
        # ORIGEM. A origem não é enfeite: é a obra que PAGA — *"a questão da obra
        # que paga é fundamental"* — e uma obra de cadastro desatualizado tiraria
        # dinheiro da conta errada sem ninguém perceber.
        "obra": codigo_da_obra or "",
        "obra_do_ponto": obra_do_ponto or "",
        "dias_na_obra": int(dias_na_obra or 0),
        "obra_de_onde": ("ponto" if obra_do_ponto
                         else ("cadastro" if codigo_da_obra else "")),
        "obra_do_cadastro": "", "sem_obra": False,
        "obra_nome": ficha.get("obra_cadastro") or "",
        # A Fase Atual, que ele pediu duas vezes. Vem do cadastro (coluna AX da
        # planilha) e é o corte mais usado da lista.
        "fase": ficha.get("fase") or "",
        # A situação do cadastro: é o que a lista usa para esconder quem já saiu
        # (`folha_lista`), como a planilha faz.
        "situacao": ficha.get("situacao") or "",
        "desligado": ficha.get("situacao") == colaboradores.SITUACAO_SAIU,
        "dias_base": 0, "feriados": 0, "ferias": 0, "valor_fechado": False,
        # Saída no meio do mês: proporcional até a data (dono, 03/10/2026).
        "saida_no_mes": None, "proporcao": "",
        # As ausências do ponto (só transporte) e o desconto que elas propõem.
        # O desconto só vale quando ele aplica (`desconto_aplicado`).
        "ausencias": [], "desconto_proposto": Decimal("0.00"),
        "desconto_aplicado": False,
        # Desconto parcial (05/10/2026): o valor de UM dia de ausência, quantos
        # dias foram descontados, o valor descontado e a justificativa dos dias
        # relevados.
        "valor_ausencia_dia": Decimal("0.00"), "dias_descontados": 0,
        "desconto_valor": Decimal("0.00"), "motivo_relevadas": "",
        # O valor acrescentado à mão neste mês, e o porquê.
        "valor_extra": Decimal("0.00"), "motivo_extra": "",
        "valor_calculado": Decimal("0.00"),
        "dias_ajuste": int(ajuste.get("dias") or 0),
        "dias": 0, "valor": Decimal("0.00"),
        "observacao": ajuste.get("observacao", ""),
        # O QUE ELE MARCOU À MÃO, do jeito que está guardado: True, False ou NULO.
        # A tela precisa dos três estados separados para desenhar o seletor —
        # "segue o cálculo" não é a mesma coisa que "não pagar", e mostrar os dois
        # iguais faria o padrão parecer decisão dele.
        "ajuste_pagar": ajuste.get("pagar"),
        "motivos": [],
        "pagar": True,
        # True quando NÃO HÁ o que pagar (falta valor ou modalidade no cadastro).
        # Diferente de `pagar=False`, que é decisão. Ver o comentário abaixo.
        "impossivel": False,
    }

    # ⚠️ A ORDEM AQUI IMPORTA, e foi um teste que me obrigou a arrumá-la.
    #
    # Há dois motivos diferentes para alguém não receber, e eles NÃO são a mesma
    # coisa:
    #
    #   POLÍTICA  — saiu, está afastada, tem cartão. O valor DÁ para calcular; o
    #               que se decidiu é não pagar. Aqui a última palavra é dele: se
    #               ele marcar, paga o valor calculado.
    #   IMPOSSÍVEL— o cadastro não diz o valor, ou não diz a modalidade. Não há
    #               valor nenhum para pagar, e marcar "pagar" pagaria ZERO em
    #               silêncio — que é pior do que não pagar.
    #
    # Na primeira versão os dois caminhos saíam da função antes de consultar o
    # ajuste, e o "pagar mesmo assim" não funcionava justamente nos casos que mais
    # precisam dele.
    situacao = ficha.get("situacao")
    # ⚠️ A SAÍDA, CORRIGIDA EM 03/10/2026. O auxílio da competência é pago no
    # mês SEGUINTE e é o benefício daquele mês. O dono: *"Se ele já saiu, ele não
    # recebe mais."* Então:
    #   - saiu ATÉ o fim da competência  → não recebe (situação "saiu");
    #   - sai DENTRO do mês do pagamento → proporcional até a data de saída
    #     (*"proporcionalize o pagamento considerando data de saída"*);
    #   - sai depois                     → recebe inteiro, com o aviso.
    # Na primeira versão (leva 161) quem saiu no meio da competência recebia
    # proporcional — e ele viu alguém desligado em 19/09 aparecendo para pagar.
    data_saida = ficha.get("data_saida")
    pag_ini = fim + dt.timedelta(days=1)
    pag_fim = pag_ini.replace(day=calendar.monthrange(pag_ini.year, pag_ini.month)[1])
    ultimo_dia = ficha.get("ultimo_dia")
    if situacao in (colaboradores.SITUACAO_SAIU,
                    colaboradores.SITUACAO_AFASTADO):
        saida["pagar"] = False
        saida["motivos"].append(ficha.get("motivo")
                                or "colaborador inativo no cadastro.")
    elif (situacao == colaboradores.SITUACAO_SAINDO and not data_saida
            and ultimo_dia and ultimo_dia <= fim):
        # ⚠️ O ÚLTIMO DIA TRABALHADO DENTRO DA COMPETÊNCIA, sem data de saída
        # lançada (05/10/2026): a pessoa já saiu — e *"se ele já saiu, ele não
        # recebe mais"*. Até aqui ela ficava "em desligamento", recebendo, e
        # aparecia entre os "sem obra" que ele queria tratar.
        saida["pagar"] = False
        saida["desligado"] = True
        saida["motivos"].append(
            f"último dia trabalhado em {ultimo_dia.strftime('%d/%m/%Y')}, dentro da "
            "competência (sem data de saída lançada) — não recebe.")
    elif (situacao == colaboradores.SITUACAO_SAINDO and data_saida
            and pag_ini <= data_saida <= pag_fim):
        saida["saida_no_mes"] = data_saida
        saida["motivos"].append(
            f"sai em {data_saida.strftime('%d/%m/%Y')}, no mês do pagamento — "
            "pago proporcional até a data de saída.")
    elif situacao == colaboradores.SITUACAO_SAINDO:
        # Não trava: pode haver valor devido até o último dia. Mas fica dito.
        saida["motivos"].append(ficha.get("motivo") or "em processo de desligamento.")

    if not e_alimentacao and _sem_acento(modo) == MODO_CARTAO:
        saida["pagar"] = False
        saida["motivos"].append(
            "modalidade \"Cartão\" no cadastro — o auxílio transporte não é pago em dinheiro.")

    # OS IMPEDIMENTOS DE VERDADE: sem estes não há o que pagar, e o ajuste dele
    # não muda isso — marcar "pagar" pagaria zero sem dizer.
    if valor_unitario is None:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            "o cadastro não informa o valor deste auxílio. Corrija no card do Pipefy "
            "e atualize o cadastro.")
        return _decidir(saida, ajuste)
    if not modo:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            "o cadastro não informa a modalidade (Mês, Mensal, Segunda à Sexta ou "
            "Segunda à Quinta).")
        return _decidir(saida, ajuste)

    base, sabado, sexta = dias_da_modalidade(modo, inicio, fim, tipo)
    if base <= 0:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            f'modalidade "{modo}" não reconhecida para contagem de dias. Modalidades '
            "aceitas: Mês, Mensal, Segunda à Sexta e Segunda à Quinta.")
        return _decidir(saida, ajuste)
    saida["dias_base"] = base
    if _sem_acento(modo) in MODOS_DIARIOS:
        if valor_unitario > TETO_DO_VALOR_DIARIO:
            saida["pagar"] = False
            saida["motivos"].append(
                f'categoria "{modo}" é por dia, mas o valor cadastrado é R$ '
                f"{valor_unitario} — parece o valor do MÊS. Não pago para não "
                "multiplicar por dia; corrija a categoria ou o valor no cadastro.")
        elif _sem_acento(modo) != "diario":
            saida["motivos"].append(
                f'categoria "{modo}": calculada como diária (valor × dias úteis).')

    # VALOR FIXO ("Mês"; "Mensal" no transporte) não desconta dia: é valor fechado.
    if valor_fechado(modo, tipo):
        saida["valor_fechado"] = True
        saida["dias"] = 1
        saida["valor"] = valor_unitario
        if saida["dias_ajuste"]:
            saida["motivos"].append(
                "o ajuste de dias não se aplica à modalidade de valor fixo "
                f'("{modo}").')
        # Férias no mês: o valor fechado sai CHEIO, e fica dito — o desconto
        # proporcional (§7.16.1) ainda não tem regra decidida.
        try:
            ferias = folha_calendario.dias_de_ferias_no_periodo(
                saida["cpf"], inicio, fim, sabado=False, sexta=True)
        except Exception:  # noqa: BLE001 — aviso, não conta
            logger.exception("Folha: não consegui ler as férias de %s", saida["cpf"])
            ferias = 0
        if ferias:
            saida["motivos"].append(
                f"{ferias} dia(s) útil(eis) de férias no mês: o valor mensal sai "
                "cheio. Se não for para pagar, desmarque.")
        _proporcional_a_saida(saida, tipo, modo, pag_ini, pag_fim)
        return _decidir(_acrescimos(saida, ajuste, tipo, modo, inicio, fim,
                                    ausencias), ajuste)

    # OS DESCONTOS, com a MESMA régua da modalidade.
    if e_alimentacao:
        saida["feriados"] = folha_calendario.dias_de_feriado_no_periodo(
            # ⚠️ PELO NOME, não pelo código: o feriado por obra é cadastrado na
            # tela de Feriados, que oferece a lista de obras POR NOME. Passar o
            # código aqui faria o feriado municipal deixar de descontar, em
            # silêncio. Se um dia o feriado passar a guardar código, muda aqui.
            inicio, fim, saida["obra_nome"], sabado=sabado, sexta=sexta)
    saida["ferias"] = folha_calendario.dias_de_ferias_no_periodo(
        saida["cpf"], inicio, fim, sabado=sabado, sexta=sexta)

    dias = base - saida["feriados"] - saida["ferias"] + saida["dias_ajuste"]
    # ⚠️ NUNCA NEGATIVO: um ajuste torto não pode virar valor a devolver.
    saida["dias"] = max(0, dias)
    saida["valor"] = (valor_unitario * saida["dias"]).quantize(CENTAVO)
    if saida["dias"] == 0:
        saida["motivos"].append("nenhum dia a pagar nesta competência.")
    _proporcional_a_saida(saida, tipo, modo, pag_ini, pag_fim)
    return _decidir(_acrescimos(saida, ajuste, tipo, modo, inicio, fim,
                                ausencias), ajuste)


def _proporcional_a_saida(saida: dict, tipo: str, modo: str, pag_ini, pag_fim) -> None:
    """Quem sai DENTRO do mês do pagamento recebe a parte do mês até a saída.

    A régua: dias úteis (sem a sexta, no "Segunda à Quinta") no transporte e na
    alimentação por dia — *"Transporte valor mensal, vamos considerar os dias
    úteis"* (03/10/2026); dias corridos na alimentação de valor fechado."""
    data = saida.get("saida_no_mes")
    if not data or not saida.get("valor"):
        return
    limpo = _sem_acento(modo)
    if tipo == TRANSPORTE or limpo in (MODO_SEG_SEX, MODO_SEG_QUI):
        sexta = limpo != MODO_SEG_QUI
        feitos = folha_calendario.dias_uteis(pag_ini, data, sexta=sexta)
        total = folha_calendario.dias_uteis(pag_ini, pag_fim, sexta=sexta) or 1
        rotulo = "dias úteis"
    else:
        feitos = (data - pag_ini).days + 1
        total = (pag_fim - pag_ini).days + 1
        rotulo = "dias"
    saida["valor"] = (saida["valor"] * feitos / total).quantize(CENTAVO)
    saida["proporcao"] = f"{feitos}/{total} {rotulo} até {data.strftime('%d/%m')}"


# ---------------------------------------------------------------------------
# AUSÊNCIAS NO PONTO E VALOR ACRESCENTADO — 03/10/2026
#
# O dono: *"se a pessoa no mês anterior faltou algum dia, aquele dia deveria ser
# descontado. Proporcional (…) que isso fosse uma opção de aplicar ou não o
# desconto (…) a alimentação não pode fazer essa dedução, mas o transporte sim
# (…) um botão confirmar, aplicar desconto (…) visualizar o motivo daquele
# desconto, que são tantas faltas, tantas ausências."*
#
# E: *"mês anterior esquecemos de colocar um determinado valor (…) onde eu
# pudesse adicionar um valor ao pagamento daquele mês do funcionário."*
# ---------------------------------------------------------------------------
# O que o ponto escreve num dia em que a pessoa NÃO FALTOU, apesar de não ter
# marcação: não entra como ausência (férias já são descontadas à parte).
NAO_E_AUSENCIA = ("ferias", "folga", "feriado", "dsr", "compens", "presenca",
                  "pagar extra")


def ausencia_do_dia(dia: dict) -> str:
    """O motivo da ausência num dia do ponto, ou "" quando não é ausência.

    Ausência é dia SEM MARCAÇÃO com falta declarada ou com uma situação que não
    seja presença (atestado, falta justificada ou não, licença…). Dia sem
    marcação e sem nada escrito NÃO conta: pode ser folga, e descontar no
    escuro tiraria dinheiro devido."""
    from .folha_apropriacao import obra_do_dia
    from .folha_diaristas import situacao_do_ponto
    decidido = obra_do_dia(dia.get("marcacoes"), dia.get("presenca", ""),
                           dia.get("falta", ""))
    if decidido.get("obra") or decidido.get("marcou"):
        return ""
    texto = (" ".join(str(dia.get("falta") or "").split())
             or situacao_do_ponto(dia.get("presenca")))
    if not texto:
        return ""
    limpo = _sem_acento(texto)
    if "falta" not in limpo and any(p in limpo for p in NAO_E_AUSENCIA):
        return ""
    return texto


def ausencias_do_mes(dias, inicio, fim, modo: str) -> list:
    """`[{"data", "motivo"}]` — as ausências dentro do período, só nos dias que a
    modalidade paga (quem é "Segunda à Quinta" não tem a sexta descontada)."""
    limpo = _sem_acento(modo)
    ultimo_dia_da_semana = 3 if limpo == MODO_SEG_QUI else 4   # seg=0 … sex=4
    saida = []
    for dia in dias or []:
        data = dia.get("data")
        if not isinstance(data, dt.date) or not (inicio <= data <= fim):
            continue
        if data.weekday() > ultimo_dia_da_semana:
            continue
        motivo = ausencia_do_dia(dia)
        if motivo:
            saida.append({"data": data, "motivo": motivo})
    return sorted(saida, key=lambda a: a["data"])


def datas_relevadas(texto) -> set:
    """As datas guardadas em `ausencias_relevadas` ("2026-09-12,2026-09-15")."""
    saida = set()
    for parte in str(texto or "").replace(";", ",").split(","):
        parte = parte.strip()
        if not parte:
            continue
        try:
            if "/" in parte:
                d, m, a = parte.split("/")
                saida.add(dt.date(int(a), int(m), int(d)))
            else:
                saida.add(dt.date.fromisoformat(parte))
        except (ValueError, TypeError):
            continue
    return saida


def resumo_das_ausencias(ausencias) -> str:
    """"2 FALTA NÃO JUSTIFICADA, 1 ATESTADO" — o motivo, contado."""
    contagem: dict = {}
    for a in ausencias or []:
        contagem[a["motivo"]] = contagem.get(a["motivo"], 0) + 1
    return ", ".join(f"{n} {m}" for m, n in sorted(contagem.items(),
                                                     key=lambda x: -x[1]))


def _acrescimos(saida: dict, ajuste: dict, tipo: str, modo: str, inicio, fim,
                ausencias) -> dict:
    """O desconto das ausências (só se aplicado) e o valor acrescentado à mão,
    sobre o valor calculado.

    ⚠️ NAS DUAS VERBAS desde 05/10/2026. Era só no transporte (dono,
    03/10/2026: *"isso serve só para o transporte"*); dois dias depois: *"é meio
    que espelho uma coisa da outra (…) a única coisa que difere é o valor, a
    categoria, o método de cálculo (…) no resto, na exibição das informações, é
    para ser tudo muito igual"*. Na alimentação de valor fechado ("Mês"), o dia
    ausente vale o mês dividido pelos dias úteis, como no transporte."""
    saida["valor_calculado"] = saida["valor"]
    if ausencias:
        lista = ausencias_do_mes(ausencias, inicio, fim, modo)
        saida["ausencias"] = lista
        if lista and saida["valor_unitario"] is not None:
            n = len(lista)
            if saida["valor_fechado"]:
                # Valor do mês: o dia vale o mês dividido pelos dias úteis
                # (segunda a sexta) do mês inteiro.
                ini_mes = inicio.replace(day=1)
                fim_mes = ini_mes.replace(
                    day=calendar.monthrange(ini_mes.year, ini_mes.month)[1])
                uteis = folha_calendario.dias_uteis(ini_mes, fim_mes) or 1
                por_dia = saida["valor_unitario"] / uteis
            else:
                por_dia = saida["valor_unitario"]
            proposto = min(saida["valor"], (por_dia * n).quantize(CENTAVO))
            saida["desconto_proposto"] = proposto
            saida["valor_ausencia_dia"] = por_dia.quantize(CENTAVO)
            saida["ausencias_resumo"] = resumo_das_ausencias(lista)
            # DESCONTO PARCIAL (dono, 05/10/2026: *"pode ser que de 5 dias, um
            # tenha justificativa e vamos descontar somente 4"*): os dias
            # relevados ficam fora do desconto, com a justificativa.
            relevadas = datas_relevadas(ajuste.get("ausencias_relevadas"))
            for a in lista:
                a["descontar"] = a["data"] not in relevadas
            # O motivo fica na coluna Ajustes da tela (não se repete aqui).
            if ajuste.get("desconto_ausencias"):
                n_desc = sum(1 for a in lista if a["descontar"])
                valor_desc = min(saida["valor"], (por_dia * n_desc).quantize(CENTAVO))
                saida["desconto_aplicado"] = True
                saida["dias_descontados"] = n_desc
                saida["desconto_valor"] = valor_desc
                saida["motivo_relevadas"] = (ajuste.get("motivo_relevadas") or ""
                                             if n_desc < n else "")
                saida["valor"] = (saida["valor"] - valor_desc).quantize(CENTAVO)
    extra = ajuste.get("valor_extra")
    if extra:
        saida["valor_extra"] = Decimal(str(extra)).quantize(CENTAVO)
        saida["motivo_extra"] = ajuste.get("motivo_extra") or ""
        saida["valor"] = max(Decimal("0.00"),
                             (saida["valor"] + saida["valor_extra"]).quantize(CENTAVO))
    return saida


def _decidir(saida: dict, ajuste: dict) -> dict:
    """A última palavra é dele: `pagar` marcado à mão ganha do cálculo.

    ⚠️ NULO É DIFERENTE DE FALSO. Nulo quer dizer "não mexi" — vale o cálculo.
    Falso é "eu decidi não pagar". Confundir os dois faria o padrão virar
    decisão.

    ⚠️ E GUARDA AS DUAS DECISÕES: `pagar_calculado` é o que a conta diz sozinha,
    `pagar` é o que vale depois do ajuste. Parece redundante e não é — é o que
    permite à tela saber se uma caixinha desmarcada é decisão dele ou resultado da
    conta, e é o que permite salvar a seleção guardando SÓ as exceções. Sem isso
    eu estava adivinhando a diferença comparando textos de motivo, que quebra no
    dia em que alguém reescreve uma frase."""
    saida["pagar_calculado"] = bool(saida["pagar"])
    if ajuste.get("pagar") is False:
        saida["pagar"] = False
        saida["motivos"].append("colaborador desmarcado manualmente.")
    elif ajuste.get("pagar") is True and not saida["pagar"]:
        if saida.get("desligado"):
            # ⚠️ DESLIGADO NÃO RECEBE, NEM MARCADO (06/10/2026). Era "política, a
            # última palavra é dele" — e um "marcar todos" salvo pôs 77
            # desligados (R$ 17,8 mil) no arquivo de setembro, escondidos da
            # lista. O dono: *"nós já havíamos combinado essa regra"* — a de
            # 03/10, *"se ele já saiu, ele não recebe mais"*. A marcação antiga
            # fica no banco, sem efeito.
            saida["motivos"].append(
                "desligado — não recebe o auxílio, mesmo marcado (regra de 03/10/2026).")
        elif saida.get("impossivel"):
            # Marcar não resolve falta de valor no cadastro: pagaria zero em
            # silêncio. O recado diz o que consertar, e onde.
            saida["motivos"].append(
                "marcado manualmente para pagamento, mas o cadastro está incompleto — "
                "sem esse dado não há valor a pagar.")
        else:
            saida["pagar"] = True
            saida["motivos"].append("marcado manualmente para pagamento, apesar da pendência.")
    return saida


def _ponto_do_mes(ano: int, mes: int) -> dict:
    """`{cpf: [dias]}` do ponto do mês — `{}` sem ponto (nunca estoura)."""
    from . import ponto
    try:
        return ponto.dias_por_cpf(ano, mes) or {}
    except Exception:  # noqa: BLE001 — o ponto é apoio aqui, não o cálculo
        logger.exception("Auxílio: não consegui ler o ponto de %02d/%s", mes, ano)
        return {}


# ⚠️ A OBRA QUE PAGA É A DOS ÚLTIMOS 15 DIAS DO PONTO (dono, 03/10/2026): *"a
# obra que vai pagar, que é a obra do ponto anterior. Vamos considerar aí os
# últimos 15 dias, a obra que a pessoa mais trabalhou, é a obra que vai pagar a
# alimentação e o transporte do colaborador."* Antes era a de mais dias no mês
# inteiro.
DIAS_DA_JANELA_DA_OBRA = 15


def _obra_do_ponto_por_cpf(ano: int, mes: int, inicio, fim,
                           dias_por_cpf=None) -> dict:
    """`{cpf: {"obra", "dias"}}` — a obra em que cada pessoa mais trabalhou nos
    ÚLTIMOS 15 DIAS do ponto (`DIAS_DA_JANELA_DA_OBRA`).

    A janela termina no último dia com ponto carregado (até o fim do mês). Sem
    ponto da competência, usa o do mês anterior. Quem não tem dia com obra na
    janela fica com a obra de mais dias do mês; sem nenhuma, cai no cadastro.
    A chave especial `"_janela"` diz qual janela valeu, para a tela mostrar.

    ⚠️ NÃO ESTOURA SEM PONTO, e não pode: o auxílio é calculado todo mês, e um mês
    cujo ponto ainda não foi trazido tem de mostrar a lista com a obra do cadastro,
    não uma tela de erro. Devolve `{}` e quem chama cai no cadastro."""
    from . import folha_apropriacao

    if dias_por_cpf is None:
        dias_por_cpf = _ponto_do_mes(ano, mes)
    if not dias_por_cpf:
        # Sem ponto da competência: o mês anterior, que é o "ponto anterior".
        ano_ant, mes_ant = (int(ano) - 1, 12) if int(mes) == 1 else (int(ano), int(mes) - 1)
        dias_por_cpf = _ponto_do_mes(ano_ant, mes_ant)
        inicio, fim = periodo_do_mes(ano_ant, mes_ant)
    datas = [d.get("data") for dias in (dias_por_cpf or {}).values() for d in dias
             if isinstance(d.get("data"), dt.date) and inicio <= d.get("data") <= fim]
    if not datas:
        return {}
    ultimo = max(datas)
    janela_ini = max(inicio, ultimo - dt.timedelta(days=DIAS_DA_JANELA_DA_OBRA - 1))

    saida = {"_janela": (janela_ini, ultimo)}
    for cpf, dias in (dias_por_cpf or {}).items():
        achado = folha_apropriacao.obra_com_mais_dias(
            folha_apropriacao._dias_uteis_do_ponto(dias, janela_ini, ultimo))
        if not achado["obra"]:
            achado = folha_apropriacao.obra_com_mais_dias(
                folha_apropriacao._dias_uteis_do_ponto(dias, inicio, fim))
        if achado["obra"]:
            saida[cpf] = achado
    return saida


def calcular(tipo: str, ano: int, mes: int) -> dict:
    """A verba inteira do mês. Devolve as pessoas e os totais."""
    if tipo not in TIPOS:
        raise ErroDoAuxilio(f'verba "{tipo}" não reconhecida.')

    inicio, fim = periodo_do_mes(ano, mes)
    ate = fim
    fichas = colaboradores.buscar(so_ativos=False, teto=5000, ate=ate)
    ajustes = ajustes_do_mes(tipo, ano, mes)
    # Uma consulta só para todas as obras: resolver pessoa por pessoa faria uma
    # ida ao banco por linha, e a lista tem centenas.
    obras_por_nome = colaboradores.codigos_das_obras()

    # ⚠️ A OBRA QUE PAGA VEM DO PONTO. *"Em Alimentação a informação de obra
    # deveria ser a do Ponto. Caso não tenha, usar a de cadastro."* (29/09/2026)
    #
    # Uma leitura do mês inteiro, não uma por pessoa: são centenas de linhas. Sem
    # carga do mês o dicionário sai vazio e TODO MUNDO cai no cadastro — a tela diz
    # isso, em vez de mostrar obra de cadastro como se fosse do ponto.
    ponto_do_mes = _ponto_do_mes(ano, mes)
    obra_do_ponto_por_cpf = _obra_do_ponto_por_cpf(ano, mes, inicio, fim,
                                                   ponto_do_mes)
    janela_da_obra = obra_do_ponto_por_cpf.pop("_janela", None)

    from . import folha_rateio
    regras = folha_rateio.regras_ativas_por_cpf()

    pessoas = []
    for ficha in fichas:
        # SÓ QUEM TEM ESTE AUXÍLIO NO CADASTRO entra na lista. É o que a planilha
        # faz (`WHERE Y != ''`): quem não recebe a verba não é problema a
        # resolver, é gente que não recebe.
        tem = (ficha.get("valor_alimentacao") if tipo == ALIMENTACAO
               else ficha.get("valor_transporte"))
        modo = (ficha.get("modo_alimentacao") if tipo == ALIMENTACAO
                else ficha.get("modo_transporte"))
        if tem is None and not modo:
            continue
        do_ponto = obra_do_ponto_por_cpf.get(ficha["cpf"]) or {}
        do_cadastro = colaboradores.resolver_obra(ficha, obras_por_nome)
        pessoas.append(calcular_pessoa(
            tipo, ficha, inicio, fim, ajustes.get(ficha["cpf"]),
            # ⚠️ SÓ O PONTO, desde 03/10/2026. O dono: *"Não utilizar obra de
            # cadastro automático, precisa ser ajustado, isso porque o correto
            # seria corrigir o ponto. Mas se não for, vamos selecionar e isso
            # precisa ter destaque, já que é pendência, conforme funciona na
            # folha da contabilidade."* Sem ponto, a obra fica vazia — pendência
            # — e o cadastro vai como SUGESTÃO para ele escolher.
            codigo_da_obra=do_ponto.get("obra") or "",
            obra_do_ponto=do_ponto.get("obra") or "",
            dias_na_obra=do_ponto.get("dias") or 0,
            # As ausências do ponto, nas duas verbas (05/10/2026).
            ausencias=ponto_do_mes.get(ficha["cpf"])))
        p = pessoas[-1]
        p["obra_do_cadastro"] = do_cadastro or ""
        # A ordem de quem manda, como na folha da contabilidade: a obra
        # escolhida à mão, a regra de rateio, o ponto.
        escolhida = str((ajustes.get(ficha["cpf"]) or {}).get("obra") or "").strip()
        if escolhida:
            p["obra"], p["obra_de_onde"] = escolhida, "mao"
        elif regras.get(ficha["cpf"]):
            aplicar_regra_de_rateio(p, regras[ficha["cpf"]])
        # SEM OBRA = PENDÊNCIA (só de quem vai receber): não há conta de onde o
        # dinheiro saia, e o fechamento não passa.
        p["sem_obra"] = bool(p["pagar"] and p["valor"] > 0 and not p.get("obra")
                             and not p.get("rateio"))

    # ⚠️ QUEM PRECISA DE MÃO VEM PRIMEIRO. `False` ordena antes de `True`, então a
    # chave é `pagar` direto — na primeira versão eu escrevi `not pagar`, e a
    # lista saía ao contrário: os certos na frente e os problemas no fim, onde
    # ninguém rola até.
    pessoas.sort(key=lambda p: (p["pagar"], (p["nome"] or "").lower()))
    a_pagar = [p for p in pessoas if p["pagar"] and p["valor"] > 0]

    por_obra: dict = {}
    for p in a_pagar:
        for parte in partes_por_obra(p):
            chave = parte["obra"] or "(sem obra)"
            atual = por_obra.setdefault(chave, {"obra": chave, "pessoas": 0,
                                                "total": Decimal("0.00"),
                                                "do_cadastro": 0, "da_regra": 0})
            atual["pessoas"] += 1
            atual["total"] += parte["valor"]
            # ⚠️ QUANTAS PESSOAS DESTA OBRA VIERAM DO CADASTRO, não do ponto. É a
            # medida de confiança da linha: obra que paga sustentada em cadastro
            # desatualizado tira dinheiro da conta errada.
            if p.get("obra_de_onde") == "cadastro":
                atual["do_cadastro"] += 1
            elif p.get("obra_de_onde") == "regra":
                atual["da_regra"] += 1

    return {
        "tipo": tipo,
        "rotulo": ROTULO_DO_TIPO[tipo],
        "ano": int(ano), "mes": int(mes),
        "competencia": f"{int(mes):02d}/{int(ano)}",
        # ⚠️ O AUXÍLIO É PAGO NO MÊS SEGUINTE AO TRABALHADO. Dito por ele em
        # 28/09/2026: *"o auxílio transporte, alimentação, a gente sempre paga o
        # mês seguinte."* A tela mostra as duas coisas — a competência (os dias
        # que foram contados) e o mês em que o dinheiro sai. Sem isso, quem abre a
        # tela em outubro procura outubro e encontra o mês errado.
        "pagamento_em": mes_do_pagamento(ano, mes),
        "inicio": inicio, "fim": fim,
        "pessoas": pessoas,
        "quantos": len(pessoas),
        "quantos_a_pagar": len(a_pagar),
        "total": sum((p["valor"] for p in a_pagar), Decimal("0.00")),
        "com_problema": [p for p in pessoas if not p["pagar"]],
        "por_obra": sorted(por_obra.values(), key=lambda o: -o["total"]),
        "desconta_feriado": tipo == ALIMENTACAO,
        # A tela avisa quando NÃO houve ponto: sem isso, a coluna Obra mostraria
        # cadastro para todo mundo sem dizer que é cadastro.
        "tem_ponto": bool(obra_do_ponto_por_cpf),
        # Os 15 dias do ponto que decidiram a obra que paga (início, fim).
        "janela_da_obra": janela_da_obra,
        "quantos_do_ponto": len([p for p in pessoas
                                 if p.get("obra_de_onde") == "ponto"]),
        "fases": sorted({p["fase"] for p in pessoas if p.get("fase")}),
        # Descontos de ausência ainda não decididos — a lateral os aponta.
        "sem_obra": [p for p in pessoas if p.get("sem_obra")],
        "com_ausencia": [p for p in pessoas if p.get("ausencias")
                         and not p.get("desconto_aplicado")
                         and p.get("desconto_proposto")],
        "fechamento": fechamento(tipo, ano, mes),
    }


# ---------------------------------------------------------------------------
# Os ajustes dele
# ---------------------------------------------------------------------------
def ajustes_do_mes(tipo: str, ano: int, mes: int) -> dict:
    """`{cpf: {...}}` do que foi mexido à mão nesta verba e competência."""
    from .db import consultar
    if not _pronto():
        return {}
    extras = _tem_extras()
    relevadas = extras and _tem_relevadas()
    linhas = consultar(
        "SELECT cpf, pagar, dias, obra, observacao, alterado_por"
        + (", valor_extra, motivo_extra, desconto_ausencias" if extras else "")
        + (", ausencias_relevadas, motivo_relevadas" if relevadas else "")
        + "  FROM analisesps.auxilio_ajuste "
        " WHERE tipo = ? AND ano = ? AND mes = ?",
        (tipo, int(ano), int(mes)))
    saida = {}
    for l in linhas:
        saida[l[0]] = {"pagar": l[1], "dias": l[2], "obra": l[3],
                       "observacao": l[4], "alterado_por": l[5]}
        if extras:
            saida[l[0]].update(valor_extra=l[6], motivo_extra=l[7] or "",
                               desconto_ausencias=l[8])
        if relevadas:
            saida[l[0]].update(ausencias_relevadas=l[9] or "",
                               motivo_relevadas=l[10] or "")
    return saida


def _tem_relevadas() -> bool:
    """A migração 048 já rodou? (desconto parcial das ausências)"""
    from .db import tem_coluna
    return tem_coluna("auxilio_ajuste", "ausencias_relevadas")


def _tem_extras() -> bool:
    """A migração 046 já rodou? (valor acrescentado e desconto de ausências)"""
    from .db import tem_coluna
    return tem_coluna("auxilio_ajuste", "desconto_ausencias")


def gravar_extras(tipo: str, ano: int, mes: int, cpfs, quem: str = "",
                  **mudancas) -> int:
    """Grava o valor acrescentado (`valor_extra`, `motivo_extra`) e/ou a decisão
    do desconto de ausências (`desconto_ausencias`) — SEM tocar no resto do
    ajuste. `cpfs`: um ou vários (o "aplicar todos"). Devolve quantos."""
    from .db import conexao
    from .folha_rateio import so_digitos

    if tipo not in TIPOS:
        raise ErroDoAuxilio(f'verba "{tipo}" não reconhecida.')
    if not _pronto() or not _tem_extras():
        raise ErroDoAuxilio(
            'atualização do banco pendente (046). Clique em "Aplicar atualizações '
            'do banco" em Configurações.')
    colunas = {}
    if "valor_extra" in mudancas:
        bruto = mudancas["valor_extra"]
        if bruto in (None, ""):
            valor = None
        else:
            try:
                valor = Decimal(str(bruto).replace(".", "").replace(",", ".")
                                if "," in str(bruto) else str(bruto)).quantize(CENTAVO)
            except Exception:  # noqa: BLE001
                raise ErroDoAuxilio("o valor acrescentado deve ser numérico.")
            # Negativo REDUZ (dono, 03/10/2026: *"deve aceitar também número
            # negativo pra reduzir valor"*); zero tira o ajuste.
            if valor == 0:
                valor = None
            elif abs(valor) > Decimal("5000"):
                raise ErroDoAuxilio(
                    f"valor de R$ {valor} acima de R$ 5.000,00 — verifique o número.")
        colunas["valor_extra"] = valor
        colunas["motivo_extra"] = " ".join(
            str(mudancas.get("motivo_extra") or "").split())[:300] if valor else ""
    if "desconto_ausencias" in mudancas:
        d = mudancas["desconto_ausencias"]
        colunas["desconto_ausencias"] = None if d is None else bool(d)
        # Os dias relevados (desconto parcial, 048). Desfazer o desconto limpa a
        # escolha; aplicar sem dizer quais relevar = desconta todos.
        relevadas = mudancas.get("ausencias_relevadas") or []
        if isinstance(relevadas, str):
            relevadas = relevadas.split(",")
        datas = sorted(datas_relevadas(",".join(str(x) for x in relevadas)))
        motivo = " ".join(str(mudancas.get("motivo_relevadas") or "").split())[:300]
        if datas and not d:
            datas, motivo = [], ""
        if datas and not motivo:
            raise ErroDoAuxilio("informe a justificativa dos dias que não serão "
                                "descontados — ela vai para o relatório.")
        if _tem_relevadas():
            colunas["ausencias_relevadas"] = ",".join(x.isoformat() for x in datas)
            colunas["motivo_relevadas"] = motivo
        elif datas:
            raise ErroDoAuxilio(
                'atualização do banco pendente (048) para o desconto parcial. Clique '
                'em "Aplicar atualizações do banco" em Configurações.')
    if "obra" in mudancas:
        # A obra escolhida à mão para quem não tem ponto (vazio = tira).
        colunas["obra"] = " ".join(str(mudancas["obra"] or "").split()).upper()[:60]
    if not colunas:
        return 0

    lista = [so_digitos(c) for c in (cpfs if isinstance(cpfs, (list, tuple))
                                      else [cpfs])]
    lista = [c for c in lista if len(c) == 11]
    if not lista:
        raise ErroDoAuxilio("CPF do colaborador não reconhecido.")
    nomes = list(colunas)
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in nomes)
    with conexao() as conn:
        for cpf in lista:
            conn.execute(
                "INSERT INTO analisesps.auxilio_ajuste "
                f"  (tipo, ano, mes, cpf, alterado_por, {', '.join(nomes)}) "
                f"VALUES (?,?,?,?,?, {', '.join('?' for _ in nomes)}) "
                " ON CONFLICT (tipo, ano, mes, cpf) DO UPDATE SET "
                f"  {sets}, alterado_em = now(), "
                "   alterado_por = EXCLUDED.alterado_por",
                (tipo, int(ano), int(mes), cpf, str(quem or "")[:120],
                 *[colunas[c] for c in nomes]))
        conn.commit()
    logger.info("Folha: %s de %02d/%d — %s gravado(s) para %d pessoa(s) por %s",
                tipo, int(mes), int(ano), ", ".join(nomes), len(lista),
                quem or "(sem nome)")
    return len(lista)


def gravar_ajuste(tipo: str, ano: int, mes: int, cpf: str, pagar=None,
                  dias=None, obra: str = "", observacao: str = "",
                  quem: str = "") -> None:
    """Guarda o que ele mexeu. É o que sobrevive ao recálculo."""
    from .db import conexao
    from .folha_rateio import so_digitos

    if not _pronto():
        raise ErroDoAuxilio(
            'tabela de ajustes não encontrada no banco. Clique em "Aplicar '
            'atualizações do banco" em Configurações.')
    if tipo not in TIPOS:
        raise ErroDoAuxilio(f'verba "{tipo}" não reconhecida.')
    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        raise ErroDoAuxilio("CPF do colaborador não reconhecido.")

    if dias is not None:
        try:
            dias = int(dias)
        except (TypeError, ValueError):
            raise ErroDoAuxilio("o ajuste de dias deve ser numérico.")
        # Um ajuste absurdo é quase sempre digitação, e mudaria o valor em
        # centenas de reais sem ninguém notar.
        if abs(dias) > 62:
            raise ErroDoAuxilio(
                f"ajuste de {dias} dias excede dois meses. Verifique o número.")

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.auxilio_ajuste "
            "  (tipo, ano, mes, cpf, pagar, dias, obra, observacao, "
            "   alterado_por) VALUES (?,?,?,?,?,?,?,?,?) "
            " ON CONFLICT (tipo, ano, mes, cpf) DO UPDATE SET "
            "   pagar = EXCLUDED.pagar, dias = EXCLUDED.dias, "
            "   obra = EXCLUDED.obra, observacao = EXCLUDED.observacao, "
            "   alterado_em = now(), alterado_por = EXCLUDED.alterado_por",
            (tipo, int(ano), int(mes), digitos, pagar, dias,
             " ".join(str(obra or "").split()).upper(),
             " ".join(str(observacao or "").split())[:300],
             str(quem or "")[:120]))
        conn.commit()


def limpar_ajuste(tipo: str, ano: int, mes: int, cpf: str) -> bool:
    """Tira o ajuste — a pessoa volta a seguir o cálculo."""
    from .db import conexao
    from .folha_rateio import so_digitos
    if not _pronto():
        return False
    with conexao() as conn:
        if _tem_extras():
            # ⚠️ O valor acrescentado e o desconto são OUTRA decisão: voltar a
            # seleção ao cálculo não pode apagá-los. Tira-se a escolha de pagar
            # e só se apaga a linha que ficar vazia.
            conn.execute(
                "UPDATE analisesps.auxilio_ajuste SET pagar = NULL, dias = NULL "
                " WHERE tipo = ? AND ano = ? AND mes = ? AND cpf = ?",
                (tipo, int(ano), int(mes), so_digitos(cpf)))
            cur = conn.execute(
                "DELETE FROM analisesps.auxilio_ajuste "
                " WHERE tipo = ? AND ano = ? AND mes = ? AND cpf = ? "
                "   AND valor_extra IS NULL AND desconto_ausencias IS NULL "
                "   AND coalesce(obra, '') = '' AND coalesce(observacao, '') = ''",
                (tipo, int(ano), int(mes), so_digitos(cpf)))
        else:
            cur = conn.execute(
                "DELETE FROM analisesps.auxilio_ajuste "
                " WHERE tipo = ? AND ano = ? AND mes = ? AND cpf = ?",
                (tipo, int(ano), int(mes), so_digitos(cpf)))
        apagou = bool(cur.rowcount and cur.rowcount > 0)
        cur.close()
        conn.commit()
    return apagou


def mes_do_pagamento(ano: int, mes: int) -> str:
    """O mês em que a competência é paga: o SEGUINTE ao trabalhado.

    *"O auxílio transporte, alimentação, a gente sempre paga o mês seguinte."*
    (dono, 28/09/2026). Devolve "10/2026" para a competência 09/2026."""
    ano, mes = int(ano), int(mes)
    return f"01/{ano + 1}" if mes == 12 else f"{mes + 1:02d}/{ano}"


# ---------------------------------------------------------------------------
# A SELEÇÃO EM BLOCO — como ele trabalha de verdade
# ---------------------------------------------------------------------------
# Correção do dono em 28/09/2026, e ela muda o desenho da tela:
#
#   "Fica muito dificultoso trabalhar da forma que está aqui, a gente vai gravando
#    um por um. (…) A princípio tudo que está atendendo os critérios que a gente
#    definiu se paga. Ela exibe tudo que tem coerência, já faz o cálculo, já deixa
#    tudo pronto. O que eu faço é só selecionar quem vai e quem não vai ser pago.
#    (…) e eu salvar como um todo, não linha a linha."
#
# ⚠️ O QUE SE GUARDA É A EXCEÇÃO, NÃO A LISTA. Se eu gravasse uma linha por
# pessoa, o padrão ("paga") viraria uma decisão registrada — e no mês seguinte
# ninguém saberia mais o que ele decidiu e o que o sistema calculou. Guardando só
# quem ele DESMARCOU (e quem ele mandou pagar apesar do cálculo), a tabela tem
# três linhas em vez de quinhentas, e cada linha é uma decisão de verdade.
# ---------------------------------------------------------------------------
def salvar_selecao(tipo: str, ano: int, mes: int, decisoes, quem: str = "") -> dict:
    """Guarda de uma vez quem vai e quem não vai ser pago.

    `decisoes`: `[{"cpf": "...", "pagar": True/False}]` — o estado das caixinhas
    como a tela as mostra. Devolve o que mudou, para a tela poder dizer."""
    if tipo not in TIPOS:
        raise ErroDoAuxilio(f'verba "{tipo}" não reconhecida.')
    if not _pronto():
        raise ErroDoAuxilio(
            'tabela de ajustes não encontrada no banco. Clique em "Aplicar '
            'atualizações do banco" em Configurações.')

    calculado = calcular(tipo, ano, mes)
    por_cpf = {p["cpf"]: p for p in calculado["pessoas"]}
    ajustes = ajustes_do_mes(tipo, ano, mes)

    gravados, limpos, ignorados = 0, 0, []
    for item in (decisoes or []):
        cpf = str((item or {}).get("cpf") or "")
        from .folha_rateio import so_digitos
        cpf = so_digitos(cpf)
        pessoa = por_cpf.get(cpf)
        if not pessoa:
            ignorados.append(cpf)
            continue
        querido = bool((item or {}).get("pagar"))

        # O que o CÁLCULO diria sem ajuste nenhum — vem pronto de
        # `calcular_pessoa`, e é a comparação que decide se isto é exceção.
        ajuste_atual = ajustes.get(cpf) or {}
        do_calculo = bool(pessoa.get("pagar_calculado"))

        if querido == do_calculo:
            # Bate com o cálculo: não é exceção. Tira o ajuste se havia um.
            # (Linha só com valor acrescentado ou desconto não é escolha de pagar.)
            if ajuste_atual.get("pagar") is not None or ajuste_atual.get("dias"):
                limpar_ajuste(tipo, ano, mes, cpf)
                limpos += 1
            continue
        # ⚠️ MARCAR NÃO RESOLVE FALTA DE DADO NO CADASTRO: pagaria zero em
        # silêncio. A pessoa fica de fora e a tela diz por quê.
        if querido and (pessoa.get("impossivel") or pessoa.get("desligado")):
            ignorados.append(cpf)
            continue
        gravar_ajuste(tipo, ano, mes, cpf, pagar=querido, quem=quem)
        gravados += 1

    logger.info(
        "Folha: seleção do auxílio %s de %02d/%d salva por %s — %d exceção(ões) "
        "gravada(s), %d voltaram ao cálculo, %d ignorada(s).",
        tipo, int(mes), int(ano), quem or "(sem nome)", gravados, limpos,
        len(ignorados))
    return {"gravados": gravados, "limpos": limpos, "ignorados": ignorados}


# ---------------------------------------------------------------------------
# FECHAR — o passo que faltava para o arquivo sair (01/10/2026)
#
# A tela de gerar só paga verba com apropriação FECHADA, e nenhuma tela fechava
# alimentação nem transporte: o botão "Conferir e gerar" levava a uma tela que
# dizia "nada fechado". O mesmo buraco que a folha da contabilidade teve.
# ---------------------------------------------------------------------------
TIPOS_DO_FECHAMENTO = {"fim_de_mes": "Fim de mês", "quinzena": "Quinzena"}


def fechamento(tipo: str, ano: int, mes: int) -> dict | None:
    """O fechamento desta verba no mês, em qualquer dos dois pagamentos."""
    from . import folha_apropriacao_guardada as guardada
    for pagamento in TIPOS_DO_FECHAMENTO:
        try:
            achado = guardada.fechamento(ano, mes, pagamento, tipo)
        except Exception:  # noqa: BLE001
            logger.exception("Auxílio: não consegui ler o fechamento")
            achado = None
        if achado:
            return achado
    return None


def aplicar_regra_de_rateio(p: dict, regra: dict) -> dict:
    """O auxílio de quem tem REGRA DE RATEIO ativa vai para as obras da regra,
    nos percentuais dela — a regra manda sobre o ponto, como na folha da
    contabilidade. Dono, 03/10/2026: *"o rateio das obras serve sim para
    alimentação e transporte e diaristas"*."""
    from . import folha_rateio
    partes = folha_rateio.distribuir(p["valor"], regra.get("obras") or [])
    if not partes:
        return p
    p["rateio"] = sorted(partes, key=lambda x: -x["valor"])
    p["obra"] = p["rateio"][0]["obra"]
    p["obra_de_onde"] = "regra"
    p["regra"] = regra.get("nome") or ""
    return p


# A lista agrupada da tela (dono, 05/10/2026: *"tanto em alimentação como em
# transporte, possa ser realizado o agrupamento e desagrupamento (…) por conta,
# por obra, etc."*). Quem tem rateio entra no grupo da obra da MAIOR parte.
AGRUPAMENTOS = [("obra", "Obra"), ("conta", "Conta"), ("modo", "Categoria"),
                ("fase", "Fase Atual"), ("", "Sem agrupar")]
AGRUPAMENTO_PADRAO = "obra"


def agrupar(pessoas, campo: str, contas: dict) -> list:
    """`folha_lista.agrupar` com a conta de cada pessoa (a da obra que paga, na
    aba "C. Diários") e a pendência do auxílio (dado faltando ou sem obra)."""
    from . import folha_lista
    for p in pessoas:
        p["conta"] = contas.get(" ".join(str(p.get("obra") or "").split()).upper(), "")
    return folha_lista.agrupar(
        pessoas, campo, pendente=lambda p: bool(p.get("impossivel") or p.get("sem_obra")))


# ---------------------------------------------------------------------------
# A AUDITORIA DA VERBA — 07/10/2026
#
# O dono, comparando a saída com a base do script: *"nenhum colaborador que
# tenha benefício cadastrado deve simplesmente desaparecer da saída. Se não for
# pagar alguém, ele deve permanecer na tabela de auditoria com Pagar? = Não e um
# motivo explícito (…) Nunca excluir silenciosamente."* E a validação: base,
# processados, a pagar, não pagos, sem correspondência, a lista dos não pagos,
# o total antes e depois dos ajustes e a diferença por colaborador.
# ---------------------------------------------------------------------------
def valor_base(p: dict) -> Decimal:
    """O valor do cadastro ANTES de qualquer ajuste: o mensal inteiro, ou o do
    dia × a quantidade de dias da categoria. Zero quando não há como calcular."""
    unitario = p.get("valor_unitario")
    if unitario is None or not p.get("dias_base"):
        return Decimal("0.00")
    if p.get("valor_fechado"):
        return Decimal(str(unitario)).quantize(CENTAVO)
    return (Decimal(str(unitario)) * int(p["dias_base"])).quantize(CENTAVO)


def linha_da_auditoria(p: dict) -> dict:
    vai = bool(p.get("pagar") and (p.get("valor") or 0) > 0)
    motivos = list(p.get("motivos") or [])
    if p.get("proporcao"):
        motivos.append(f"proporcional à saída: {p['proporcao']}")
    if not vai and not motivos:
        motivos.append("sem dias elegíveis nesta competência."
                       if (p.get("valor") or 0) <= 0 else "não selecionado.")
    if vai and p.get("sem_obra"):
        motivos.append("sem obra do ponto — escolha a obra antes de gerar.")
    return {
        "cpf": p.get("cpf_bonito") or p.get("cpf") or "",
        "nome": p.get("nome") or "",
        "categoria": p.get("modo") or "",
        "valor_cadastrado": p.get("valor_unitario"),
        "qtd": int(p.get("dias_base") or 0),
        "valor_base": valor_base(p),
        "faltas": int(p.get("dias_descontados") or 0),
        "desconto_faltas": p.get("desconto_valor") or Decimal("0.00"),
        "ferias": int(p.get("ferias") or 0),
        "feriados": int(p.get("feriados") or 0),
        "ajuste_dias": int(p.get("dias_ajuste") or 0),
        "adicao": p.get("valor_extra") or Decimal("0.00"),
        "cc_cadastro": p.get("obra_do_cadastro") or "",
        "cc_ponto": p.get("obra_do_ponto") or "",
        "cc_considerado": p.get("obra") or "",
        "cc_de_onde": {"ponto": "ponto", "mao": "escolhida à mão",
                       "regra": "regra de rateio", "cadastro": "cadastro"}.get(
                           p.get("obra_de_onde") or "", ""),
        "valor_final": (p.get("valor") or Decimal("0.00")) if vai else Decimal("0.00"),
        "valor_calculado": p.get("valor") or Decimal("0.00"),
        "pagar": vai,
        "motivo": " · ".join(motivos),
    }


def auditoria(tipo: str, ano: int, mes: int, calculado: dict | None = None) -> dict:
    """A tabela de auditoria e a validação da verba no mês. Todo mundo que tem
    o benefício no cadastro entra — quem não vai receber, com o motivo."""
    calculado = calculado or calcular(tipo, ano, mes)
    linhas = sorted((linha_da_auditoria(p) for p in calculado["pessoas"]),
                    key=lambda l: (l["pagar"], l["nome"].lower()))
    pagos = [l for l in linhas if l["pagar"]]
    nao_pagos = [l for l in linhas if not l["pagar"]]
    total_base = sum((l["valor_base"] for l in linhas), Decimal("0.00"))
    total_final = sum((l["valor_final"] for l in linhas), Decimal("0.00"))
    diferencas = [dict(l, diferenca=(l["valor_base"] - l["valor_final"]))
                  for l in linhas if l["valor_base"] != l["valor_final"]]
    return {
        "linhas": linhas, "nao_pagos": nao_pagos,
        "diferencas": sorted(diferencas, key=lambda l: -l["diferenca"]),
        "resumo": {
            "base": len(linhas),
            "processados": len(linhas),
            "a_pagar": len(pagos),
            "nao_pagos": len(nao_pagos),
            "sem_correspondencia": sum(1 for l in linhas if not l["cc_ponto"]),
            "total_base": total_base,
            "total_final": total_final,
            "diferenca": total_base - total_final,
        },
    }


def auditoria_xlsx(tipo: str, ano: int, mes: int, dados: dict | None = None) -> bytes:
    """A auditoria em Excel: Auditoria, Validação, Não pagos e Diferenças."""
    import io
    from openpyxl import Workbook
    dados = dados or auditoria(tipo, ano, mes)
    livro = Workbook()
    cab = ["CPF", "Colaborador", "Categoria", "Valor Cadastrado", "Qtd. Auxílios",
           "Valor Base Calculado", "Faltas (dias descontados)", "Desconto das faltas",
           "Férias (dias)", "Feriados (dias)", "Ajuste de dias", "Adição",
           "Centro de Custo Cadastrado", "Centro de Custo Mobponto",
           "Centro de Custo Considerado", "De onde veio o CC", "Valor Calculado",
           "Valor Final", "Pagar?", "Motivo da Exclusão ou Ajuste"]

    def fila(l):
        return [l["cpf"], l["nome"], l["categoria"],
                float(l["valor_cadastrado"]) if l["valor_cadastrado"] is not None else None,
                l["qtd"], float(l["valor_base"]), l["faltas"], float(l["desconto_faltas"]),
                l["ferias"], l["feriados"], l["ajuste_dias"], float(l["adicao"]),
                l["cc_cadastro"], l["cc_ponto"], l["cc_considerado"], l["cc_de_onde"],
                float(l["valor_calculado"]), float(l["valor_final"]),
                "Sim" if l["pagar"] else "Não", l["motivo"]]

    aba = livro.active
    aba.title = "Auditoria"
    aba.append(cab)
    for l in dados["linhas"]:
        aba.append(fila(l))
    r = dados["resumo"]
    val = livro.create_sheet("Validação")
    rotulo = ROTULO_DO_TIPO.get(tipo, tipo)
    for linha in [
            [f"{rotulo} — {int(mes):02d}/{int(ano)}"], [],
            ["1. Colaboradores com o benefício no cadastro", r["base"]],
            ["2. Processados", r["processados"]],
            ["3. A pagar", r["a_pagar"]],
            ["4. Não pagos", r["nao_pagos"]],
            ["5. Sem correspondência no ponto (sem obra do Mobponto)",
             r["sem_correspondencia"]],
            ["6. Lista dos não pagos, com o motivo", "aba \"Não pagos\""],
            ["7. Total da base antes dos ajustes", float(r["total_base"])],
            ["8. Total após os ajustes (a pagar)", float(r["total_final"])],
            ["9. Diferença total (decomposta na aba \"Diferenças\")",
             float(r["diferenca"])]]:
        val.append(linha)
    nao = livro.create_sheet("Não pagos")
    nao.append(cab)
    for l in dados["nao_pagos"]:
        nao.append(fila(l))
    dif = livro.create_sheet("Diferenças")
    dif.append(["CPF", "Colaborador", "Categoria", "Valor Base Calculado",
                "Valor Final", "Diferença", "Pagar?", "Motivo"])
    for l in dados["diferencas"]:
        dif.append([l["cpf"], l["nome"], l["categoria"], float(l["valor_base"]),
                    float(l["valor_final"]), float(l["diferenca"]),
                    "Sim" if l["pagar"] else "Não", l["motivo"]])
    for planilha in livro.worksheets:
        planilha.freeze_panes = "A2"
        for coluna in planilha.columns:
            largura = max(len(str(c.value or "")) for c in coluna)
            planilha.column_dimensions[coluna[0].column_letter].width = min(60, max(10, largura + 2))
    memoria = io.BytesIO()
    livro.save(memoria)
    return memoria.getvalue()


def partes_por_obra(p: dict) -> list:
    """`[{obra, valor}]` — de onde sai o dinheiro desta pessoa: as obras da
    regra de rateio, ou a obra que paga inteira."""
    if p.get("rateio"):
        return [{"obra": x["obra"], "valor": x["valor"],
                 "percentual": x.get("percentual")} for x in p["rateio"]]
    return [{"obra": p.get("obra") or "", "valor": p["valor"]}]


def apropriado(calculado: dict) -> dict:
    """O auxílio no formato que `folha_apropriacao_guardada` guarda: cada
    colaborador com o valor na obra que paga (ou nas obras da regra)."""
    pessoas = [{"cpf": p["cpf"], "nome": p["nome"], "nome_cadastro": p["nome"],
                "fora": not (p["pagar"] and p["valor"] > 0),
                "por_obra": ([{"obra": x["obra"], "dias": int(p["dias"] or 0),
                               "valor": x["valor"],
                               "origem": p.get("obra_de_onde") or ""}
                              for x in partes_por_obra(p)]
                             if p["pagar"] and p["valor"] > 0 else [])}
               for p in calculado["pessoas"]]
    total = calculado["total"]
    return {"pessoas": pessoas, "total_da_folha": total,
            "total_apropriado": total, "fecha": True}


def fechar(tipo: str, ano: int, mes: int, pagamento: str = "fim_de_mes",
           quem: str = "") -> dict:
    """Congela o auxílio do mês: quem recebe, quanto e de qual obra sai."""
    from . import folha_apropriacao_guardada as guardada
    if pagamento not in TIPOS_DO_FECHAMENTO:
        raise ErroDoAuxilio("selecione se o auxílio será pago na quinzena ou no fim de mês.")
    calculado = calcular(tipo, ano, mes)
    a_pagar = [p for p in calculado["pessoas"] if p["pagar"] and p["valor"] > 0]
    if not a_pagar:
        raise ErroDoAuxilio("nenhum colaborador marcado para receber este auxílio.")
    sem_obra = [p["nome"] for p in a_pagar if not p.get("obra")]
    if sem_obra:
        raise ErroDoAuxilio(
            f"{len(sem_obra)} colaborador(es) a receber sem obra do ponto: "
            + ", ".join(sem_obra[:5]) + ("…" if len(sem_obra) > 5 else "")
            + '. Corrija o ponto ou escolha a obra na linha ("usar esta obra" / '
            '"outra obra…"). Sem obra não há conta de pagamento.')
    # Fechar um pagamento tira o fechamento do outro — senão a mesma verba do
    # mês sairia duas vezes.
    for outro in TIPOS_DO_FECHAMENTO:
        if outro != pagamento:
            guardada.reabrir(ano, mes, outro, tipo, quem=quem)
    novo = guardada.fechar(ano, mes, pagamento, apropriado(calculado),
                           verba=tipo, quem=quem)
    return {"id": novo, "pessoas": len(a_pagar), "total": calculado["total"],
            "pagamento": pagamento}
