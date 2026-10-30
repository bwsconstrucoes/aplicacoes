# -*- coding: utf-8 -*-
"""
A apuração do dia: das batidas ao que a gestão, o "meu ponto" e os alertas leem.

É FUNÇÃO PURA, de propósito. Recebe a escala do dia, as batidas, se é feriado e
se há ocorrência que abona (atestado, férias, folga compensatória…) e devolve o
resultado do dia. Não lê banco, não escreve nada — o mesmo cálculo serve à tela
de gestão, ao celular do colaborador, ao banco de horas e aos alertas, e é
testável sem servidor. Regra de número mora em código testado; a inteligência
artificial resume e lê documento, nunca calcula saldo (decisão da casa: um
número errado com cara de certo é pior que nenhum — `CLAUDE.md`).

AS REGRAS DA CLT QUE ESTÃO AQUI, cada uma com a fonte:

  - **Tolerância** (art. 58, §1º): variações de até 5 min por marcação, somando
    no máximo 10 min no dia, não são descontadas nem viram extra. Passou do
    limite, conta-se o tempo TODO (Súmula 366 do TST), não só o excesso.
  - **Falta**: dia com jornada prevista, nenhuma batida e nenhuma ocorrência
    que abone.
  - **Falta parcial** (decisão do dono, 29/10/2026): dia em que faltou batida —
    só a manhã, só a tarde, três batidas, uma só. *"Quem garante que ele não
    saiu, apenas bateu o ponto e foi embora? Ele tem a obrigação de bater."* Vale
    o que está COMPROVADO: só os períodos da escala com as duas pontas batidas
    contam como trabalhados; o resto é débito. O ajuste aprovado desfaz.
  - **Dia de descanso e feriado**: previsto zero; o que for trabalhado é extra
    em dia de descanso (a tela e a folha decidem o adicional; aqui só se marca).
    **Fora a 12x36** (art. 59-A, parágrafo único: os feriados "serão considerados
    compensados"): o feriado que cai numa noite do ciclo é dia normal de escala —
    previsto o turno, extra só o que passar dele (31/10/2026, o dono: "tá errado,
    ele trabalha em escala"). Na folga do ciclo, o feriado trabalhado segue extra.
  - **Intervalo** (art. 71): jornada acima de 6 h pede ao menos 1 h de intervalo;
    entre 4 e 6 h, 15 min. Menos que isso vira alerta.
  - **Limite de extra** (art. 59): mais de 2 h extras no dia vira alerta — fora
    da escala 12x36 (art. 59-A), que tem regra própria.
  - **Interjornada** (art. 66): menos de 11 h entre o fim de um dia e o começo
    do seguinte vira alerta. Quem calcula é `interjornada_minutos`, porque
    precisa de dois dias.
  - **Hora noturna** (art. 73): minutos trabalhados entre 22 h e 5 h, e o
    equivalente em hora reduzida (52 min 30 s valem 1 h). ⚠️ A prorrogação do
    noturno depois das 5 h (Súmula 60, II) NÃO está calculada — a folha decide.

O QUE NÃO ESTÁ AQUI e é decisão do dono/contabilidade: o adicional de cada
caso, a convenção coletiva da construção (que pode mudar tolerância, banco e
intervalo) e quem tem banco de horas. Os números saem em MINUTOS; quem converte
em dinheiro é a folha.

ESCALA, como este módulo entende:

  - `SEMANAL`: para cada dia da semana (0 = segunda … 6 = domingo), uma lista de
    períodos (entrada, saída) em minutos desde a meia-noite. Saída maior que
    1440 atravessa a meia-noite (turno da noite).
  - `CICLO_12X36`: um período só, num dia sim e outro não, contando de
    `data_base`.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from itertools import combinations
from typing import Optional

TOLERANCIA_POR_MARCACAO = 5
TOLERANCIA_DIARIA = 10
LIMITE_EXTRA_DIA = 120
INTERJORNADA_MINIMA = 11 * 60
NOTURNO_INICIO = 22 * 60
NOTURNO_FIM = 5 * 60
MINUTOS_HORA_NOTURNA = 52.5

# A falta parcial (29/10/2026) e o nome antigo do dia com batida faltando — quem lê a situação do dia
# confere as duas, para nada que ainda diga "INCOMPLETO" passar batido.
SITUACOES_PARCIAIS = ("FALTA_PARCIAL", "INCOMPLETO")

# Ocorrências que abonam o dia: a pessoa não trabalhou e não deve nada.
ABONADORAS = frozenset({"ATESTADO", "LICENCA", "FERIAS", "FOLGA_COMPENSATORIA",
                        "ABONO", "AFASTAMENTO"})


# O CICLO 12x36 AUTOMÁTICO (30/10/2026, o dono: "essa questão do dia em que o ciclo começa, eu acho que
# acaba atrapalhando (…) isso poderia ser considerado de forma automática"). Quais dias são de trabalho
# sai das batidas: nos últimos 30 dias, a paridade (dia sim, dia não) em que a pessoa mais trabalhou.
JANELA_CICLO_AUTOMATICO = 30


def dia_do_ciclo(trabalhados: tuple, dia: dt.date, data_base: Optional[dt.date] = None) -> Optional[bool]:
    """PURA. O `dia` é de trabalho no 12x36? `trabalhados` = dias com batida, em ordem.
    - Nos 30 dias ANTES dele, conta-se em quantos a pessoa trabalhou na mesma paridade do `dia`
      (diferença par) e em quantos na outra; ganha a maioria. Empate: vale o mais antigo dos que
      votaram (o ciclo que já vinha — uma noite a mais no começo não vira o ciclo). O próprio `dia`
      não vota — por isso a noite trabalhada fora do ciclo é extra, não vira o ciclo.
    - Com menos de 2 noites antes (o começo do ponto ou da escala), ou com empate, votam também os 30 dias DEPOIS.
      (30/10/2026, o vigia que trabalha nos dias ímpares desde 17/10: sem ninguém antes, o dia 16
      caía no "dia de trabalho" informado ao dar a escala e virava falta.)
    - Ninguém votou: a paridade do último dia trabalhado, por mais antigo que seja.
    - Nunca trabalhou: o dia informado ao dar a escala, se houver; senão nada é previsto (None),
      a não ser no próprio dia com batida, que abre o ciclo."""
    janela = dt.timedelta(days=JANELA_CICLO_AUTOMATICO)

    def placar(votos):
        mesma = sum(1 for w in votos if (dia - w).days % 2 == 0)
        return mesma, len(votos) - mesma
    votos = [w for w in trabalhados if dia - janela <= w < dia]
    depois = [w for w in trabalhados if dia < w <= dia + janela]
    mesma, outra = placar(votos)
    # Poucos votos antes, ou EMPATE (31/10/2026: o vigia que trocou o plantão e trabalhou duas noites seguidas — 18 e
    # 19 — empatava e o ciclo virava; a falta caía no 20 e o 21 virava extra): votam também os 30 dias depois.
    if len(votos) < 2 or mesma == outra:
        votos = votos + depois
        mesma, outra = placar(votos)
    if votos:
        if mesma != outra:
            return mesma > outra
        return (dia - votos[0]).days % 2 == 0
    antes = [w for w in trabalhados if w < dia]
    if antes:
        return (dia - antes[-1]).days % 2 == 0
    if dia in trabalhados:
        return True
    if data_base:
        return (dia - data_base).days % 2 == 0
    return None


@dataclass(frozen=True)
class Escala:
    tipo: str                                         # SEMANAL | CICLO_12X36
    semana: dict = field(default_factory=dict)        # {0: [(420, 660), (720, 1020)], ...}
    periodo_ciclo: tuple = ()                         # (420, 1140) para 7h–19h
    data_base: Optional[dt.date] = None               # primeiro dia trabalhado do ciclo
    # O ciclo automático: os dias com batida da pessoa. None = ciclo fixo pela `data_base`.
    trabalhados: Optional[tuple] = None

    def periodos(self, dia: dt.date) -> list[tuple[int, int]]:
        """Os períodos previstos para o dia (vazio = descanso)."""
        if self.tipo == "CICLO_12X36":
            if not self.periodo_ciclo:
                return []
            if self.trabalhados is not None:
                trabalha = dia_do_ciclo(self.trabalhados, dia, self.data_base)
            else:
                trabalha = bool(self.data_base) and (dia - self.data_base).days % 2 == 0
            return [tuple(self.periodo_ciclo)] if trabalha else []
        return [tuple(p) for p in self.semana.get(dia.weekday(), [])]


@dataclass
class Dia:
    data: dt.date
    previsto: int = 0
    trabalhado: int = 0
    extra: int = 0
    debito: int = 0
    atraso: int = 0
    saida_antecipada: int = 0
    intervalo: Optional[int] = None
    noturno_relogio: int = 0
    noturno_reduzido: int = 0
    falta: bool = False
    abonado: bool = False
    descanso: bool = False
    feriado: bool = False
    feriado_compensado: bool = False      # feriado numa noite do ciclo 12x36: dia normal (art. 59-A)
    batidas: int = 0
    situacao: str = "OK"            # OK | FALTA | FALTA_PARCIAL | ABONADO | DESCANSO | SEM_ESCALA
    alertas: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Auxiliares
# ---------------------------------------------------------------------------
def minutos_desde(dia: dt.date, momento: dt.datetime) -> int:
    """Minutos de `momento` contados da meia-noite do `dia` (passa de 1440 no
    dia seguinte — é assim que o turno da noite fecha)."""
    inicio = dt.datetime.combine(dia, dt.time(0, 0), tzinfo=momento.tzinfo)
    return int((momento - inicio).total_seconds() // 60)


def hhmm(texto: str) -> int:
    """'07:30' → 450; '31:00' → 1860 (07h do dia seguinte)."""
    h, m = str(texto).strip().split(":")
    return int(h) * 60 + int(m)


def _noturnos(inicio: int, fim: int) -> int:
    """Minutos de [inicio, fim) que caem entre 22h e 5h, em qualquer dia."""
    total = 0
    for base in range(-1440, max(fim, 0) + 1440, 1440):
        janela_ini, janela_fim = base + NOTURNO_INICIO, base + 1440 + NOTURNO_FIM
        total += max(0, min(fim, janela_fim) - max(inicio, janela_ini))
    return total


# ---------------------------------------------------------------------------
# O dia
# ---------------------------------------------------------------------------
BATIDA_REPETIDA_MIN = 10   # duas batidas a menos que isto são a mesma, para o cálculo


def sem_repetidas(marcas: list[int], janela: int = BATIDA_REPETIDA_MIN) -> tuple[list[int], int]:
    """PURA. A batida a menos de `janela` minutos da anterior conta como a MESMA no
    cálculo (13/10/2026: "e se eu bater duas vezes o ponto de entrada, como será o
    tratamento?"). Sem isto, a entrada repetida das 07:05 virava a saída para o
    intervalo e o dia inteiro saía errado. A marcação continua no registro — o
    ponto eletrônico não pode impedir nem apagar marcação (Portaria 671); só o
    cálculo a desconsidera, e o dia ganha o aviso BATIDA_REPETIDA. Devolve
    (marcas que contam, quantas ficaram de fora)."""
    contam: list[int] = []
    for m in sorted(marcas):
        if contam and m - contam[-1] < janela:
            continue
        contam.append(m)
    return contam, len(marcas) - len(contam)


def sem_batidas_no_meio_da_noite(marcas: list[int], periodos: list[tuple[int, int]],
                                 escala: Optional["Escala"]) -> tuple[list[int], int]:
    """PURA. O TURNO DA NOITE SEM INTERVALO (30/10/2026, o dono, num vigia com 17:05, 22:56, 04:48 e 07:07:
    "imagino que essas batidas tenham sido erro (…) não tem como o vigia sair e voltar"). No turno de um
    período só que passa da meia-noite, a batida do meio não é saída: o turno vai da primeira à última, e o
    dia ganha o aviso BATIDA_NO_MEIO_DO_TURNO. A marcação continua no registro (Portaria 671) — só o
    cálculo não a usa. Vale também na noite de folga do 12x36 trabalhada. Devolve (marcas, quantas saíram)."""
    if len(marcas) <= 2 or escala is None:
        return marcas, 0
    if periodos:
        noite = len(periodos) == 1 and periodos[0][1] > 1440
    else:
        noite = escala.tipo == "CICLO_12X36" and bool(escala.periodo_ciclo) and escala.periodo_ciclo[1] > 1440
    if not noite:
        return marcas, 0
    return [marcas[0], marcas[-1]], len(marcas) - 2


def intervalo_pre_assinalado(dia: dt.date, escala: Optional["Escala"], batidas: list[dt.datetime],
                             feriado: bool = False) -> list[int]:
    """PURA. Os horários de intervalo que o cálculo completou no dia de pré-assinalado (os minutos, em ordem) —
    para o espelho MOSTRAR, diferenciados, que o almoço foi considerado sem batida (31/10/2026, o dono: "teve batida
    no início e no fim, aparece a batida do intervalo diferenciada"). Vazio quando nada foi completado."""
    if escala is None or feriado:
        return []
    marcas, _ = sem_repetidas([minutos_desde(dia, b) for b in batidas])
    completas = completar_pre_assinalado(marcas, escala.periodos(dia))
    return completas[1:-1] if len(completas) > len(marcas) else []


def completar_pre_assinalado(marcas: list[int], periodos: list[tuple[int, int]]) -> list[int]:
    """PURA. Na obra de intervalo PRÉ-ASSINALADO (CLT art. 74, § 2º; migração 009
    — pedido do dono, 09/10/2026: "por convenção, só é para bater o ponto de
    entrada e de saída"), o dia com só a entrada e a saída ganha os horários de
    intervalo da escala, como se tivessem sido batidos: o dia fica completo e o
    almoço não vira hora extra. Só quando as duas batidas abraçam o intervalo
    inteiro — quem bateu as quatro, ou saiu antes do almoço, conta o que bateu."""
    if len(marcas) != 2 or len(periodos) < 2:
        return marcas
    internos = [m for i in range(len(periodos) - 1) for m in (periodos[i][1], periodos[i + 1][0])]
    if marcas[0] < internos[0] and internos[-1] < marcas[1]:
        return [marcas[0], *internos, marcas[1]]
    return marcas


def pares_comprovados(marcas: list[int], periodos: list[tuple[int, int]]) -> Optional[list[tuple[int, int]]]:
    """PURA. A FALTA PARCIAL (29/10/2026): os pares (entrada, saída) que as batidas COMPROVAM, ou None
    quando o dia não é de falta parcial (as batidas cobrem do começo ao fim previsto).

    - Número ímpar de batidas, menos que as previstas: cada batida vai para a posição prevista mais
      perto (na ordem); só conta o período da escala com as duas pontas batidas. Ex.: 11h, 12h, 17h
      numa escala 7–11 / 12–17 comprova só a tarde (a entrada das 7h não foi batida).
    - Número par, menos que as previstas: os pares em sequência, como sempre — mas, se a primeira
      batida não está na entrada do dia ou a última não está na saída (só a manhã, só a tarde), é
      falta parcial. Só a entrada e a saída (sem o almoço) NÃO é: a pessoa estava no começo e no fim.
    - Número ímpar, igual ou maior que as previstas: os pares em sequência; a última fica sem par."""
    esperadas = [m for p in periodos for m in p]
    k, n = len(marcas), len(esperadas)
    if not k or not n:
        return None
    if k % 2 == 0 and k >= n:
        return None
    pares_em_sequencia = [(marcas[i], marcas[i + 1]) for i in range(0, k - 1, 2)]
    if k >= n:
        return pares_em_sequencia
    melhor = min(combinations(range(n), k),
                 key=lambda idx: sum(abs(marcas[i] - esperadas[j]) for i, j in enumerate(idx)))
    if k % 2 == 1:
        pos = dict(zip(melhor, marcas))
        return [(pos[2 * p], pos[2 * p + 1]) for p in range(len(periodos)) if 2 * p in pos and 2 * p + 1 in pos]
    if melhor[0] == 0 and melhor[-1] == n - 1:
        return None
    return pares_em_sequencia


def _comprovado(pares: list[tuple[int, int]], periodos: list[tuple[int, int]]) -> int:
    """PURA. O tempo dos pares DENTRO dos períodos da escala (a falta parcial não ganha extra), com a
    tolerância de 5 min de cada batida (art. 58, §1º) puxando-a para o horário previsto."""
    esperadas = [m for p in periodos for m in p]

    def puxa(m):
        perto = min(esperadas, key=lambda e: abs(e - m))
        return perto if abs(perto - m) <= TOLERANCIA_POR_MARCACAO else m
    total = 0
    for e, s in pares:
        e, s = puxa(e), puxa(s)
        total += sum(max(0, min(s, ps) - max(e, pe)) for pe, ps in periodos)
    return total


def apurar_dia(dia: dt.date, escala: Optional[Escala], batidas: list[dt.datetime], *,
               feriado: bool = False, ocorrencia: Optional[str] = None,
               intervalo_pre_assinalado: bool = False) -> Dia:
    """Apura um dia. `batidas` são as marcações VÁLIDAS (ou ajustadas) cuja
    `data_referencia` é este dia, com fuso."""
    r = Dia(data=dia, feriado=feriado, batidas=len(batidas))
    marcas, repetidas = sem_repetidas([minutos_desde(dia, b) for b in batidas])
    if repetidas:
        r.alertas.append("BATIDA_REPETIDA")
    periodos = [] if escala is None else escala.periodos(dia)
    if feriado and periodos and escala.tipo == "CICLO_12X36":
        r.feriado_compensado = True             # 12x36: o feriado já é compensado na escala (art. 59-A)
    elif feriado:
        periodos = []
    marcas, no_meio = sem_batidas_no_meio_da_noite(marcas, periodos, escala)
    if no_meio:
        r.alertas.append("BATIDA_NO_MEIO_DO_TURNO")
    if intervalo_pre_assinalado:
        marcas = completar_pre_assinalado(marcas, periodos)
    r.previsto = sum(max(0, s - e) for e, s in periodos)
    r.descanso = escala is not None and not periodos

    # --- o que foi trabalhado, por pares entrada/saída ---------------------
    pares = [(marcas[i], marcas[i + 1]) for i in range(0, len(marcas) - 1, 2)]
    r.trabalhado = sum(s - e for e, s in pares)
    if len(pares) >= 2:
        r.intervalo = sum(pares[i + 1][0] - pares[i][1] for i in range(len(pares) - 1))
    r.noturno_relogio = sum(_noturnos(e, s) for e, s in pares)
    r.noturno_reduzido = round(r.noturno_relogio * 60 / MINUTOS_HORA_NOTURNA)
    incompleto = len(marcas) % 2 == 1

    if escala is None:
        r.situacao = "SEM_ESCALA"
        r.alertas.append("SEM_ESCALA")
        if incompleto:
            r.alertas.append("BATIDA_FALTANDO")
        return r

    # --- abono, falta, descanso --------------------------------------------
    if ocorrencia and ocorrencia.upper() in ABONADORAS:
        r.abonado = True
        r.situacao = "ABONADO"
        if marcas:
            r.alertas.append("BATIDA_EM_DIA_ABONADO")
        return r
    if r.previsto and not marcas:
        r.falta = True
        r.debito = r.previsto
        r.situacao = "FALTA"
        r.alertas.append("FALTA")
        return r
    if not r.previsto:
        r.situacao = "DESCANSO"
        r.extra = r.trabalhado
        if r.trabalhado:
            r.alertas.append("TRABALHO_EM_FERIADO" if feriado else "TRABALHO_EM_DESCANSO")
        if incompleto:
            r.alertas.append("BATIDA_FALTANDO")
        return r

    # --- dia de trabalho: falta parcial, tolerância, atraso, extra ----------
    # Até 29/10/2026 o dia com batida faltando ficava "INCOMPLETO", sem débito, esperando o ajuste; o
    # dono mudou: vale o comprovado, o resto é falta parcial (ver `pares_comprovados`).
    pares_ok = pares_comprovados(marcas, periodos)
    if pares_ok is not None:
        r.trabalhado = _comprovado(pares_ok, periodos)
        r.debito = max(0, r.previsto - r.trabalhado)
        if r.debito:
            r.situacao = "FALTA_PARCIAL"
            r.alertas.append("FALTA_PARCIAL")
        if incompleto:              # uma batida a mais, sem par, num dia já coberto: só o aviso
            r.alertas.append("BATIDA_FALTANDO")
        return r

    esperadas = [m for p in periodos for m in p]
    variacoes = ([abs(a - b) for a, b in zip(marcas, esperadas)]
                 if len(marcas) == len(esperadas) else None)
    dentro_da_tolerancia = (variacoes is not None
                            and all(v <= TOLERANCIA_POR_MARCACAO for v in variacoes)
                            and sum(variacoes) <= TOLERANCIA_DIARIA)
    r.atraso = max(0, marcas[0] - esperadas[0])
    r.saida_antecipada = max(0, esperadas[-1] - marcas[-1])
    if dentro_da_tolerancia:
        # Dentro da tolerância o dia vale o previsto: nada de extra, nada de débito.
        r.atraso = r.saida_antecipada = 0
    else:
        # Fora dela, conta o tempo todo (Súmula 366), não só o que passou de 10 min.
        saldo = r.trabalhado - r.previsto
        r.extra, r.debito = max(0, saldo), max(0, -saldo)
    if r.atraso > TOLERANCIA_POR_MARCACAO:
        r.alertas.append("ATRASO")
    if r.saida_antecipada > TOLERANCIA_POR_MARCACAO:
        r.alertas.append("SAIDA_ANTECIPADA")
    if r.extra > LIMITE_EXTRA_DIA and escala.tipo != "CICLO_12X36":
        r.alertas.append("EXTRA_ACIMA_DE_2H")
    if escala.tipo != "CICLO_12X36":
        precisa = 60 if r.trabalhado > 360 else (15 if r.trabalhado > 240 else 0)
        if precisa and (r.intervalo or 0) < precisa:
            r.alertas.append("INTERVALO_CURTO")
    return r


def interjornada_minutos(fim_anterior: Optional[dt.datetime],
                         inicio_seguinte: Optional[dt.datetime]) -> Optional[int]:
    """Descanso entre a última batida de um dia e a primeira do seguinte."""
    if not fim_anterior or not inicio_seguinte:
        return None
    return int((inicio_seguinte - fim_anterior).total_seconds() // 60)


def interjornada_curta(fim_anterior, inicio_seguinte, escala: Optional[Escala]) -> bool:
    """Menos de 11 h de descanso (art. 66). A 12x36 descansa 36 h por definição;
    se cair aqui, é escala errada — vale o alerta do mesmo jeito."""
    m = interjornada_minutos(fim_anterior, inicio_seguinte)
    return m is not None and m < INTERJORNADA_MINIMA


def resumo_do_periodo(dias: list[Dia]) -> dict:
    """Somatório que o "meu ponto" e o espelho mostram."""
    return {
        "dias": len(dias),
        "previsto": sum(d.previsto for d in dias),
        "trabalhado": sum(d.trabalhado for d in dias),
        "extra": sum(d.extra for d in dias),
        "debito": sum(d.debito for d in dias),
        "faltas": sum(1 for d in dias if d.falta),
        "abonados": sum(1 for d in dias if d.abonado),
        "incompletos": sum(1 for d in dias if d.situacao in SITUACOES_PARCIAIS),
        "noturno_reduzido": sum(d.noturno_reduzido for d in dias),
        "alertas": sorted({a for d in dias for a in d.alertas}),
    }
