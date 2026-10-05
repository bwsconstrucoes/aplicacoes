# -*- coding: utf-8 -*-
"""
O pedido de ajuste de batida feito A PARTIR DO DIA — e as travas que impedem o
pedido errado.

Pedido do dono, 04/10/2026, contando o que acontecia no Mobponto com o Pipefy:
*"o pessoal errava muito. Botava período que já tem ponto no meio batido,
solicitava dia que já tinha ponto… às vezes solicitava a correção de um dia que
é falta, já tinha sido registrada a falta."*

A RESPOSTA É NÃO DAR O FORMULÁRIO EM BRANCO. A pessoa abre o "Meu mês", toca no
dia que está com problema, e o sistema já mostra o que tem e o que FALTA:
"faltou a volta do intervalo (previsto 12:00)". Ela marca o que esqueceu,
confere o horário sugerido pela escala, diz o motivo e manda. Dia certo não tem
o botão; batida que já existe não aparece para pedir.

E o servidor confere de novo, porque tela não é trava (`conferir_pedido`):
  · dia no futuro, ou horário que ainda não chegou → não;
  · mês fechado → não (regra que já existia);
  · dia já justificado por atestado, férias, licença aprovados → não;
  · horário a menos de 30 min de uma batida que já existe → não ("já existe
    batida às 07:02");
  · horário a menos de 30 min de outro pedido esperando decisão → não;
  · dois horários do mesmo pedido a menos de 30 min um do outro → não;
  · dia que já tem todas as batidas da escala → não;
  · horário que não pertence àquele dia de trabalho → não.

Funções puras aqui; quem lê o banco é `ocorrencias.criar`.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from ..erros import ErroDeValidacao

PERTO_MIN = 30           # dois horários a menos que isto são "o mesmo"
CASAR_MIN = 120          # batida a até 2 h de um horário previsto conta como ele
MAX_SEM_ESCALA = 6       # sem escala, no máximo tantas batidas no dia

MOTIVOS = {
    "CELULAR": "Celular quebrado, sem bateria ou sem crédito",
    "INTERNET": "Sem internet ou sem sinal na obra",
    "APARELHO": "Tablet ou aparelho da obra com problema",
    "ESQUECI": "Esqueci de bater",
    "FORA": "Estava em serviço fora da obra",
    "OUTRO": "Outro motivo",
}


def _hhmm(minutos: int) -> str:
    m = int(minutos) % 1440
    return f"{m // 60:02d}:{m % 60:02d}"


def marcas_previstas(periodos: list[tuple[int, int]]) -> list[dict]:
    """Os horários que a escala do dia espera, com o nome de cada um.
    `periodos` em minutos desde a meia-noite (fim > 1440 atravessa a noite)."""
    if not periodos:
        return []
    marcas = []
    if len(periodos) == 2:
        nomes = ["Entrada", "Saída para o intervalo", "Volta do intervalo", "Saída"]
    elif len(periodos) == 1:
        nomes = ["Entrada", "Saída"]
    else:
        nomes = [f"{'Entrada' if i % 2 == 0 else 'Saída'} {i // 2 + 1}" for i in range(2 * len(periodos))]
    for (ini, fim) in periodos:
        marcas += [ini, fim]
    return [{"minuto": m, "hora": _hhmm(m), "rotulo": nomes[i]} for i, m in enumerate(marcas)]


def faltantes(previstas: list[dict], batidas_min: list[int], *, casar_min: int = CASAR_MIN) -> list[dict]:
    """PURA. Quais horários previstos não têm batida perto. Cada batida casa com
    um previsto só (o mais perto), e só a até `casar_min` minutos."""
    livres = sorted(batidas_min)
    faltam = []
    for p in previstas:
        perto = min(livres, key=lambda b: abs(b - p["minuto"]), default=None)
        if perto is not None and abs(perto - p["minuto"]) <= casar_min:
            livres.remove(perto)
        else:
            faltam.append({"rotulo": p["rotulo"], "sugestao": p["hora"], "minuto": p["minuto"]})
    return faltam


def minutos_do_dia(momento: dt.datetime, dia: dt.date) -> int:
    """Minutos desde a meia-noite DO DIA DE TRABALHO (passa de 1440 de madrugada)."""
    return int((momento.replace(tzinfo=None) - dt.datetime.combine(dia, dt.time(0))).total_seconds() // 60)


def conferir_pedido(*, dia: dict, data: dt.date, novos_min: list[int], existentes_min: list[int],
                    pendentes_min: list[int], previstas: list[dict], agora_min: Optional[int]) -> None:
    """PURA. Levanta ErroDeValidacao com a frase que a pessoa vai ler.

    `dia` é o dia do espelho; os minutos são do dia de trabalho; `agora_min`
    é o agora em minutos do mesmo dia (None se o dia já passou)."""
    if dia.get("situacao") == "FUTURO":
        raise ErroDeValidacao("esse dia ainda não chegou", campo="data")
    if dia.get("situacao") == "FORA_DO_CONTRATO":
        raise ErroDeValidacao("esse dia está fora do período de contrato (antes do início ou depois "
                              "da saída)", campo="data")
    if dia.get("ocorrencia"):
        o = dia["ocorrencia"]
        raise ErroDeValidacao(f"esse dia já está justificado ({o['rotulo'].lower()} nº {o['id']}); "
                              "não cabe ajuste de batida", campo="data")
    for i, n in enumerate(novos_min):
        if agora_min is not None and n > agora_min:
            raise ErroDeValidacao(f"{_hhmm(n)} ainda não chegou", campo="horario")
        if n < 0 or n >= 1440 + 12 * 60:
            raise ErroDeValidacao(f"o horário {_hhmm(n)} não é desse dia de trabalho", campo="horario")
        for e in existentes_min:
            if abs(n - e) < PERTO_MIN:
                raise ErroDeValidacao(f"já existe batida às {_hhmm(e)} — esse horário não precisa de ajuste",
                                      campo="horario")
        for p in pendentes_min:
            if abs(n - p) < PERTO_MIN:
                raise ErroDeValidacao(f"já existe um pedido esperando decisão para as {_hhmm(p)}",
                                      campo="horario")
        for outro in novos_min[i + 1:]:
            if abs(n - outro) < PERTO_MIN:
                raise ErroDeValidacao(f"{_hhmm(n)} e {_hhmm(outro)} estão perto demais — é a mesma batida?",
                                      campo="horario")
    limite = len(previstas) if previstas else MAX_SEM_ESCALA
    ja_tem = len(existentes_min) + len(pendentes_min)
    if previstas and ja_tem >= limite:
        raise ErroDeValidacao(f"esse dia já tem as {limite} batidas da escala (contando os pedidos "
                              "esperando decisão)", campo="data")
    if ja_tem + len(novos_min) > max(limite, len(existentes_min)):
        raise ErroDeValidacao(f"pedido demais para um dia: a escala prevê {limite} batidas e já há {ja_tem}",
                              campo="horario")
