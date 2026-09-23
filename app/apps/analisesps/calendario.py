# -*- coding: utf-8 -*-
"""
A grade do mês para o calendário das SPs.

Só a matemática de calendário mora aqui: quais dias entram na grade, qual é
"hoje", o que é feriado. O que cai em cada dia vem do banco
(`consultas.calendario_do_mes`), e quem junta os dois é a rota.

⚠️ NÃO É A AGENDA. A Agenda tem o calendário dela, e é de outra coisa:
compromissos que se repetem, cadastrados à mão. Este é o calendário das SPs
que já existem, recortadas pelo mesmo filtro das Solicitações e do Relatório.
Duas telas com grade de mês e nenhuma linha de código em comum seria pior —
por isso a marcação dos dias (`fora`, `hoje`, `nao-util`) é a MESMA da
Agenda, e as duas usam o mesmo desenho na folha de estilo.
"""
from __future__ import annotations

import calendar as _cal
from datetime import date

MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
         "agosto", "setembro", "outubro", "novembro", "dezembro"]

DIAS_DA_SEMANA = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]


def mes_valido(ano, mes) -> tuple[int, int]:
    """Ano e mês vindos da barra de endereço, contidos no que existe.

    Chegam de fora, então não dá para confiar: mês 13 ou ano 90000 viraria
    exceção dentro do `calendar`, e a tela inteira cairia por causa de um
    endereço malformado."""
    from .horario import agora
    hoje = agora().date()
    try:
        ano = int(ano)
        mes = int(mes)
    except (TypeError, ValueError):
        return hoje.year, hoje.month
    if not 1 <= mes <= 12 or not 2000 <= ano <= 2100:
        return hoje.year, hoje.month
    return ano, mes


def vizinhos(ano: int, mes: int) -> tuple[tuple, tuple]:
    """O mês anterior e o seguinte, para os botões de navegar."""
    anterior = (ano - 1, 12) if mes == 1 else (ano, mes - 1)
    seguinte = (ano + 1, 1) if mes == 12 else (ano, mes + 1)
    return anterior, seguinte


def limites(ano: int, mes: int) -> tuple[date, date]:
    """O primeiro e o último dia DA GRADE — não do mês.

    A grade começa no domingo anterior e termina no sábado seguinte, e o que
    cai nesses dias vizinhos tem de ser consultado junto: um vencimento no
    dia 30 do mês passado aparece na primeira linha, e mostrá-lo vazio seria
    mentira."""
    semanas = _cal.Calendar(firstweekday=6).monthdatescalendar(ano, mes)
    return semanas[0][0], semanas[-1][-1]


def grade(ano: int, mes: int, por_dia: dict) -> dict:
    """A grade do mês, semana a semana, com o que o banco achou em cada dia.

    Cada semana traz o próprio total, na coluna da direita: é a pergunta que
    se faz olhando um calendário de pagamentos ("quanto sai nesta semana?") e
    que a soma do mês não responde.
    """
    from .agenda import _todos_feriados
    from .horario import agora

    hoje = agora().date()
    semanas_datas = _cal.Calendar(firstweekday=6).monthdatescalendar(ano, mes)
    inicio, fim = semanas_datas[0][0], semanas_datas[-1][-1]
    feriados = _todos_feriados({inicio.year, fim.year})

    semanas = []
    for semana_datas in semanas_datas:
        dias = []
        soma = 0
        quantas = 0
        for d in semana_datas:
            achado = por_dia.get(d) or {}
            # O total da semana conta a semana INTEIRA, inclusive os dias do
            # mês vizinho que aparecem nela. É o que a pessoa vê na linha.
            soma += achado.get("total") or 0
            quantas += achado.get("quantidade") or 0
            dias.append({
                "data": d,
                "dia": d.day,
                "do_mes": d.month == mes,
                "hoje": d == hoje,
                "feriado": d in feriados,
                "fim_de_semana": d.weekday() >= 5,
                "quantidade": achado.get("quantidade") or 0,
                "total": achado.get("total") or 0,
                "vencidas": achado.get("vencidas") or 0,
            })
        semanas.append({"dias": dias, "total": soma, "quantidade": quantas})

    # O dia mais pesado do MÊS manda na intensidade da cor. Tirar a escala do
    # maior dia da grade inteira faria um vencimento gordo do mês vizinho
    # apagar todo o resto.
    maior = 0
    for semana in semanas:
        for d in semana["dias"]:
            if d["do_mes"]:
                maior = max(maior, d["total"] or 0)

    return {"ano": ano, "mes": mes, "semanas": semanas, "maior": maior,
            "inicio": inicio, "fim": fim}
