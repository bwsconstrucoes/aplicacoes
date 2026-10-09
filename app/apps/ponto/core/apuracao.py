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
  - **Dia de descanso e feriado**: previsto zero; o que for trabalhado é extra
    em dia de descanso (a tela e a folha decidem o adicional; aqui só se marca).
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
from typing import Optional

TOLERANCIA_POR_MARCACAO = 5
TOLERANCIA_DIARIA = 10
LIMITE_EXTRA_DIA = 120
INTERJORNADA_MINIMA = 11 * 60
NOTURNO_INICIO = 22 * 60
NOTURNO_FIM = 5 * 60
MINUTOS_HORA_NOTURNA = 52.5

# Ocorrências que abonam o dia: a pessoa não trabalhou e não deve nada.
ABONADORAS = frozenset({"ATESTADO", "LICENCA", "FERIAS", "FOLGA_COMPENSATORIA",
                        "ABONO", "AFASTAMENTO"})


@dataclass(frozen=True)
class Escala:
    tipo: str                                         # SEMANAL | CICLO_12X36
    semana: dict = field(default_factory=dict)        # {0: [(420, 660), (720, 1020)], ...}
    periodo_ciclo: tuple = ()                         # (420, 1140) para 7h–19h
    data_base: Optional[dt.date] = None               # primeiro dia trabalhado do ciclo

    def periodos(self, dia: dt.date) -> list[tuple[int, int]]:
        """Os períodos previstos para o dia (vazio = descanso)."""
        if self.tipo == "CICLO_12X36":
            if not self.data_base or not self.periodo_ciclo:
                return []
            return [tuple(self.periodo_ciclo)] if (dia - self.data_base).days % 2 == 0 else []
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
    batidas: int = 0
    situacao: str = "OK"            # OK | FALTA | ABONADO | INCOMPLETO | DESCANSO | SEM_ESCALA
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


def apurar_dia(dia: dt.date, escala: Optional[Escala], batidas: list[dt.datetime], *,
               feriado: bool = False, ocorrencia: Optional[str] = None,
               intervalo_pre_assinalado: bool = False) -> Dia:
    """Apura um dia. `batidas` são as marcações VÁLIDAS (ou ajustadas) cuja
    `data_referencia` é este dia, com fuso."""
    r = Dia(data=dia, feriado=feriado, batidas=len(batidas))
    marcas = sorted(minutos_desde(dia, b) for b in batidas)
    periodos = [] if (escala is None or feriado) else escala.periodos(dia)
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

    # --- dia de trabalho: tolerância, atraso, extra ------------------------
    if incompleto:
        r.situacao = "INCOMPLETO"
        r.alertas.append("BATIDA_FALTANDO")
        # Sem par, não dá para dizer quanto trabalhou: não se lança extra nem
        # débito. Quem resolve é o ajuste, não o cálculo.
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
        "incompletos": sum(1 for d in dias if d.situacao == "INCOMPLETO"),
        "noturno_reduzido": sum(d.noturno_reduzido for d in dias),
        "alertas": sorted({a for d in dias for a in d.alertas}),
    }
