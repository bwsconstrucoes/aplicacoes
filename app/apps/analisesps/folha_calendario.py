# -*- coding: utf-8 -*-
"""
Feriados e férias — o que tira dias do auxílio.

PARA QUE SERVE, e é a razão de existir: o auxílio **alimentação** desconta os dias
de feriado e os dias de férias; o **transporte** desconta férias mas **não**
feriado de um dia. Essa diferença é decisão do dono, de 27/09/2026:

    "Um único dia não precisaria, mas férias, como são mais dias, sim, deveríamos
     proporcionalizar."

Sem este módulo, as duas telas de auxílio calculam o mês cheio para todo mundo — o
que paga refeição de quem estava de férias.

⚠️ FERIADO É POR OBRA, NÃO POR MUNICÍPIO. Ele falou dos dois — *"quais são os
feriados que é por obra, porque as obras são em municípios diferentes"*. Guardar
por município exigiria um de/para obra → município que o sistema não tem, e
inventá-lo agora seria mais uma peça para dar errado. Por obra ele escolhe da lista
que já existe (a mesma do rateio), e a conta sai certa.
"""
from __future__ import annotations

import datetime as dt
import logging

from . import formatos

logger = logging.getLogger("analisesps.folha_calendario")

NACIONAL = "nacional"
POR_OBRA = "obra"

ROTULO_DA_ABRANGENCIA = {NACIONAL: "Nacional", POR_OBRA: "Só nesta obra"}


class ErroDoCalendario(RuntimeError):
    """Feriado ou férias que não dá para gravar. A frase vai para a tela."""


def _pronto() -> bool:
    """A migração 032 já rodou? Enquanto não, a tela avisa em vez de estourar."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.feriado LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


# ---------------------------------------------------------------------------
# FERIADOS
# ---------------------------------------------------------------------------
def gravar_feriado(data, abrangencia: str, obra: str = "",
                   descricao: str = "", quem: str = "") -> int:
    """Cadastra um feriado. Devolve o id.

    ⚠️ RECUSA O REPETIDO com frase, em vez de deixar o banco estourar: um 7 de
    setembro cadastrado duas vezes descontaria dois dias do auxílio de todo
    mundo."""
    from .db import conexao
    if not _pronto():
        raise ErroDoCalendario(
            'a tabela dos feriados ainda não existe. Aperte "Aplicar '
            'atualizações do banco" em Configurações e tente de novo.')

    quando = formatos.para_data(data)
    if quando is None:
        raise ErroDoCalendario(
            f'"{data}" não é uma data que eu consiga ler. Use dia/mês/ano.')

    abrangencia = str(abrangencia or "").strip().lower()
    if abrangencia not in (NACIONAL, POR_OBRA):
        raise ErroDoCalendario(
            "diga se o feriado é nacional ou de uma obra só.")
    obra = " ".join(str(obra or "").split()).upper()
    if abrangencia == POR_OBRA and not obra:
        raise ErroDoCalendario("escolha a obra deste feriado.")
    if abrangencia == NACIONAL:
        # Feriado nacional COM obra escrita deixaria a consulta ambígua. O banco
        # também recusa; aqui a frase explica antes.
        obra = ""

    descricao = " ".join(str(descricao or "").split())[:200]

    if ja_tem_feriado(quando, abrangencia, obra):
        onde = "nacional" if abrangencia == NACIONAL else f'da obra "{obra}"'
        raise ErroDoCalendario(
            f"o feriado {quando.strftime('%d/%m/%Y')} {onde} já está cadastrado.")

    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.feriado "
            "  (data, abrangencia, obra, descricao, criado_por) "
            " VALUES (?,?,?,?,?) RETURNING id",
            (quando, abrangencia, obra, descricao, str(quem or "")[:120]))
        novo = cur.fetchone()[0]
        cur.close()
        conn.commit()
    logger.info("Análise de SPs: feriado %s (%s %s) cadastrado por %s.",
                quando, abrangencia, obra or "-", quem or "(sem nome)")
    return novo


def ja_tem_feriado(data, abrangencia: str, obra: str = "") -> bool:
    from .db import consultar_um
    if not _pronto():
        return False
    achado = consultar_um(
        "SELECT 1 FROM analisesps.feriado "
        " WHERE data = ? AND abrangencia = ? AND obra = ?",
        (data, abrangencia, obra or ""))
    return bool(achado)


def listar_feriados(ano: int | None = None) -> list:
    """Os feriados, do mais recente para o mais antigo. Filtra por ano."""
    from .db import consultar
    if not _pronto():
        return []
    if ano:
        linhas = consultar(
            "SELECT id, data, abrangencia, obra, descricao, criado_por "
            "  FROM analisesps.feriado "
            " WHERE data >= ? AND data <= ? "
            " ORDER BY data DESC, obra",
            (dt.date(int(ano), 1, 1), dt.date(int(ano), 12, 31)))
    else:
        linhas = consultar(
            "SELECT id, data, abrangencia, obra, descricao, criado_por "
            "  FROM analisesps.feriado ORDER BY data DESC, obra")
    return [{"id": l[0], "data": l[1], "abrangencia": l[2], "obra": l[3],
             "descricao": l[4], "criado_por": l[5],
             "rotulo": ROTULO_DA_ABRANGENCIA.get(l[2], l[2])} for l in linhas]


def apagar_feriado(feriado_id: int, quem: str = "") -> bool:
    from .db import conexao
    if not _pronto():
        return False
    with conexao() as conn:
        cur = conn.execute("DELETE FROM analisesps.feriado WHERE id = ?",
                           (int(feriado_id),))
        apagou = bool(cur.rowcount and cur.rowcount > 0)
        cur.close()
        conn.commit()
    return apagou


def feriados_no_periodo(inicio, fim, obra: str = "") -> list:
    """Os feriados que caem no período, para uma obra (ou só os nacionais).

    ⚠️ O NACIONAL VALE SEMPRE; o da obra só quando a obra é a mesma. Somar os dois
    sem distinguir descontaria o feriado de Olinda do auxílio de quem trabalha em
    Sobral."""
    from .db import consultar
    if not _pronto():
        return []
    obra = " ".join(str(obra or "").split()).upper()
    linhas = consultar(
        "SELECT data, abrangencia, obra, descricao FROM analisesps.feriado "
        " WHERE data >= ? AND data <= ? "
        "   AND (abrangencia = ? OR (abrangencia = ? AND obra = ?)) "
        " ORDER BY data",
        (inicio, fim, NACIONAL, POR_OBRA, obra))
    return [{"data": l[0], "abrangencia": l[1], "obra": l[2],
             "descricao": l[3]} for l in linhas]


# ---------------------------------------------------------------------------
# FÉRIAS
# ---------------------------------------------------------------------------
def gravar_ferias(cpf: str, inicio, fim, nome: str = "",
                  observacao: str = "", quem: str = "") -> int:
    """Cadastra um período de férias. Devolve o id.

    ⚠️ RECUSA PERÍODO QUE SE SOBREPÕE a outro da mesma pessoa. Férias fracionadas
    são a regra, então dois períodos no ano são normais — mas dois que se cruzam
    descontariam o mesmo dia duas vezes, e o auxílio sairia a menos."""
    from .db import conexao
    from .folha_rateio import cpf_bonito, cpf_valido, so_digitos

    if not _pronto():
        raise ErroDoCalendario(
            'a tabela das férias ainda não existe. Aperte "Aplicar atualizações '
            'do banco" em Configurações e tente de novo.')

    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        raise ErroDoCalendario("escolha a pessoa — o CPF não veio completo.")
    if not cpf_valido(digitos):
        raise ErroDoCalendario(
            f"o CPF {cpf_bonito(digitos)} tem dígito verificador errado.")

    de = formatos.para_data(inicio)
    ate = formatos.para_data(fim)
    if de is None or ate is None:
        raise ErroDoCalendario("informe o primeiro e o último dia das férias.")
    if ate < de:
        raise ErroDoCalendario(
            "o último dia é antes do primeiro — confira as datas.")
    # Um período absurdo é quase sempre ano digitado errado, e descontaria meses
    # de auxílio de alguém.
    if (ate - de).days > 400:
        raise ErroDoCalendario(
            f"o período tem {(ate - de).days} dias. Confira o ano das datas.")

    conflito = ferias_que_cruzam(digitos, de, ate)
    if conflito:
        outro = conflito[0]
        raise ErroDoCalendario(
            f"esta pessoa já tem férias de "
            f"{outro['inicio'].strftime('%d/%m/%Y')} a "
            f"{outro['fim'].strftime('%d/%m/%Y')}, e os períodos se cruzam. "
            "Apague o outro ou ajuste as datas.")

    nome = " ".join(str(nome or "").split())[:160]
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.ferias "
            "  (cpf, nome, inicio, fim, observacao, criado_por) "
            " VALUES (?,?,?,?,?,?) RETURNING id",
            (digitos, nome, de, ate,
             " ".join(str(observacao or "").split())[:300],
             str(quem or "")[:120]))
        novo = cur.fetchone()[0]
        cur.close()
        conn.commit()
    logger.info("Análise de SPs: férias de %s (%s a %s) por %s.",
                digitos, de, ate, quem or "(sem nome)")
    return novo


def ferias_que_cruzam(cpf: str, inicio, fim) -> list:
    """Os períodos desta pessoa que se sobrepõem ao informado.

    Dois períodos se cruzam quando um começa antes do outro acabar, dos dois
    lados — é a regra de sempre, e escrevê-la ao contrário é o erro clássico."""
    from .db import consultar
    from .folha_rateio import so_digitos
    if not _pronto():
        return []
    linhas = consultar(
        "SELECT id, inicio, fim FROM analisesps.ferias "
        " WHERE cpf = ? AND inicio <= ? AND fim >= ? ORDER BY inicio",
        (so_digitos(cpf), fim, inicio))
    return [{"id": l[0], "inicio": l[1], "fim": l[2]} for l in linhas]


def listar_ferias(texto: str = "", teto: int = 200) -> list:
    """As férias cadastradas, da mais recente para a mais antiga.

    `texto` procura por nome ou por CPF — é a busca que a tela oferece."""
    from .db import consultar
    from .folha_rateio import cpf_bonito, so_digitos
    if not _pronto():
        return []

    condicoes = []
    params: list = []
    procurado = " ".join(str(texto or "").split())
    if procurado:
        digitos = so_digitos(procurado)
        if len(digitos) >= 3:
            condicoes.append("(lower(nome) LIKE ? OR cpf LIKE ?)")
            params += [f"%{procurado.lower()}%", f"%{digitos}%"]
        else:
            condicoes.append("lower(nome) LIKE ?")
            params.append(f"%{procurado.lower()}%")

    onde = (" WHERE " + " AND ".join(condicoes)) if condicoes else ""
    linhas = consultar(
        "SELECT id, cpf, nome, inicio, fim, observacao, criado_por "
        "  FROM analisesps.ferias " + onde +
        " ORDER BY inicio DESC, lower(nome) LIMIT ?",
        tuple(params) + (int(teto),))
    return [{"id": l[0], "cpf": l[1], "cpf_bonito": cpf_bonito(l[1]),
             "nome": l[2], "inicio": l[3], "fim": l[4], "observacao": l[5],
             "criado_por": l[6], "dias": (l[4] - l[3]).days + 1}
            for l in linhas]


def apagar_ferias(ferias_id: int, quem: str = "") -> bool:
    from .db import conexao
    if not _pronto():
        return False
    with conexao() as conn:
        cur = conn.execute("DELETE FROM analisesps.ferias WHERE id = ?",
                           (int(ferias_id),))
        apagou = bool(cur.rowcount and cur.rowcount > 0)
        cur.close()
        conn.commit()
    return apagou


# ---------------------------------------------------------------------------
# O QUE O CÁLCULO DO AUXÍLIO PERGUNTA
# ---------------------------------------------------------------------------
def dias_uteis(inicio, fim, sabado: bool = False, sexta: bool = True) -> int:
    """Quantos dias úteis há no período.

    `sabado=True` conta o sábado; `sexta=False` tira a sexta (é a modalidade
    "Segunda à Quinta", que existe de verdade no cadastro).

    ⚠️ CONTA DIA A DIA em vez de fazer aritmética de semanas: o mês começa e
    termina em dias da semana diferentes, e a conta fechada erra na borda. São no
    máximo 31 iterações."""
    if inicio is None or fim is None or fim < inicio:
        return 0
    total = 0
    dia = inicio
    while dia <= fim:
        semana = dia.weekday()          # 0 = segunda … 6 = domingo
        if semana <= 4:
            if semana == 4 and not sexta:
                pass
            else:
                total += 1
        elif semana == 5 and sabado:
            total += 1
        dia += dt.timedelta(days=1)
    return total


def dias_de_ferias_no_periodo(cpf: str, inicio, fim, sabado: bool = False,
                              sexta: bool = True) -> int:
    """Quantos DIAS ÚTEIS de férias esta pessoa tem dentro do período.

    ⚠️ DIA ÚTIL, NÃO DIA DE CALENDÁRIO — é o que a planilha faz
    (`NETWORKDAYS.INTL`), e faz sentido: não se desconta refeição de um domingo
    que já não era pago."""
    total = 0
    for periodo in ferias_no_periodo(cpf, inicio, fim):
        de = max(periodo["inicio"], inicio)
        ate = min(periodo["fim"], fim)
        total += dias_uteis(de, ate, sabado=sabado, sexta=sexta)
    return total


def ferias_no_periodo(cpf: str, inicio, fim) -> list:
    """Os períodos de férias desta pessoa que encostam no período pedido."""
    return ferias_que_cruzam(cpf, inicio, fim)


def dias_de_feriado_no_periodo(inicio, fim, obra: str = "",
                               sabado: bool = False, sexta: bool = True) -> int:
    """Quantos feriados caem em DIA ÚTIL dentro do período.

    ⚠️ FERIADO QUE CAI NO FIM DE SEMANA NÃO DESCONTA NADA, porque aquele dia já
    não contava. É a mesma sutileza que a planilha resolve contando os feriados de
    sexta à parte para a modalidade "Segunda à Quinta" (§7.14.6 do documento) —
    aqui ela sai de graça, porque a conta é dia a dia."""
    total = 0
    for feriado in feriados_no_periodo(inicio, fim, obra):
        dia = feriado["data"]
        semana = dia.weekday()
        if semana <= 4:
            if semana == 4 and not sexta:
                continue
            total += 1
        elif semana == 5 and sabado:
            total += 1
    return total
