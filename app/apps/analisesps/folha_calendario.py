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
# FERIADOS — VÊM DO PONTO (04/11/2026)
#
# O dono: *"a parte de feriados deve vir do ponto. Dessa forma ficaremos somente
# com uma base de feriados."* Até aqui eram duas: a tabela `analisesps.feriado`
# (cadastrada nesta tela, nacional ou por obra) e a `ponto.feriados` (nacional,
# estadual, municipal ou de obra). Um feriado municipal lançado só no ponto não
# descontava o auxílio; um lançado só aqui não tirava a falta do ponto.
#
# AGORA A FOLHA LÊ SÓ O PONTO, e com A MESMA REGRA dele (`ponto/core/feriados.py`,
# `Calendario.feriado`): nacional vale sempre; estadual pela UF da obra; municipal
# pelo município; o de obra, só naquela obra. Reusar a regra, e não reescrevê-la,
# é o que garante que o ponto e a folha nunca discordem sobre um dia.
#
# A OBRA AQUI É TEXTO (o nome do cadastro do rateio, ou o código que vem do ponto);
# lá é a linha de `public.obras`. O casamento é pelo código (o da C. Diários, o
# mesmo dos dois lados), pelo código secundário da planilha e, por último, pelo
# nome. Obra que não casa com nenhuma conta só o feriado nacional — e fica no log.
#
# A tabela `analisesps.feriado` NÃO foi apagada: o que foi cadastrado nela e o
# ponto não tem aparece na tela, com o botão de levar para o ponto
# (`levar_ao_ponto`). Ela não entra mais em conta nenhuma.
# ---------------------------------------------------------------------------
ROTULO_DO_PONTO = {"NACIONAL": "Nacional", "ESTADUAL": "Estadual",
                   "MUNICIPAL": "Municipal", "OBRA": "Só nesta obra"}

# Quanto tempo a leitura do ponto vale. O auxílio pergunta pessoa por pessoa, com
# o mesmo período: sem guardar, seriam três consultas por pessoa da folha.
VALIDADE_DA_LEITURA_S = 60
_leituras: dict = {}


def _normal(texto) -> str:
    from app.apps.ponto.core.feriados import _sem_acento
    return _sem_acento(texto)


class FeriadosDoPonto:
    """Os feriados do ponto num período, respondendo "este dia é feriado nesta
    obra?" com a regra do próprio ponto. Lê tudo de uma vez (feriados do período
    e as obras) e depois responde sem ir ao banco."""

    def __init__(self, inicio, fim):
        from app.apps.ponto import db as banco_do_ponto
        from app.apps.ponto.core import base_obras, feriados as feriados_do_ponto

        self.inicio, self.fim = inicio, fim
        self._por_texto: dict[str, int] = {}
        with banco_do_ponto.conexao() as conn:
            self._calendario = feriados_do_ponto.Calendario(conn, inicio, fim)
            t = base_obras.trechos_sql(conn)
            if t:
                sql = (f"SELECT o.id, o.codigo, {t['nome']} AS nome, {t['uf']} AS uf, "
                       f"{t['municipio']} AS municipio, op.codigo AS codigo_planilha, "
                       f"op.codigo_secundario AS codigo_secundario "
                       f"FROM public.obras o {t['join']}")
            else:
                sql = ("SELECT o.id, o.codigo, o.nome, o.uf, o.municipio, "
                       "NULL AS codigo_planilha, NULL AS codigo_secundario FROM public.obras o")
            obras = banco_do_ponto.todos(conn, sql)
        # O calendário do ponto busca a obra no banco na hora; aqui ela já vem
        # carregada, e a conexão (que já fechou) nunca é usada.
        self._calendario._conn = None
        for o in obras:
            self._calendario._obras[o["id"]] = {"id": o["id"], "uf": o.get("uf"),
                                                 "municipio": o.get("municipio")}
        # O código vence o nome: dois nomes iguais não podem roubar a obra de um código.
        for campo in ("codigo", "codigo_planilha", "codigo_secundario", "nome"):
            for o in obras:
                chave = _normal(o.get(campo))
                if chave:
                    self._por_texto.setdefault(chave, o["id"])
        # O nome do cadastro do rateio (a lista de obras da tela) → o código dele.
        try:
            from . import sincronizacao
            for r in sincronizacao.referencias_rateio().get("obras") or []:
                obra_id = self._por_texto.get(_normal(r.get("codigo")))
                if obra_id:
                    self._por_texto.setdefault(_normal(r.get("nome")), obra_id)
        except Exception:  # noqa: BLE001 — sem a lista, casa só pelo código e nome do ERP
            logger.warning("Feriados: não consegui ler a lista de obras do rateio",
                           exc_info=True)
        self._avisadas: set = set()

    def obra_id(self, obra) -> int | None:
        chave = _normal(obra)
        if not chave:
            return None
        achada = self._por_texto.get(chave)
        if achada is None and chave not in self._avisadas:
            self._avisadas.add(chave)
            logger.warning('Feriados: a obra "%s" não casa com nenhuma obra do ponto; '
                           "conta só o feriado nacional.", obra)
        return achada

    def feriado(self, dia, obra: str = "") -> str | None:
        """O nome do feriado que vale neste dia para esta obra, ou None."""
        if dia < self.inicio or dia > self.fim:
            return None
        return self._calendario.feriado(dia, self.obra_id(obra))


def feriados_do_ponto(inicio, fim) -> FeriadosDoPonto:
    """A leitura do período, guardada por um minuto (ver VALIDADE_DA_LEITURA_S)."""
    import time
    chave = (inicio, fim)
    guardada = _leituras.get(chave)
    if guardada and time.monotonic() - guardada[0] < VALIDADE_DA_LEITURA_S:
        return guardada[1]
    lida = FeriadosDoPonto(inicio, fim)
    if len(_leituras) > 50:
        _leituras.clear()
    _leituras[chave] = (time.monotonic(), lida)
    return lida


def esquecer_leituras() -> None:
    _leituras.clear()


def feriados_no_periodo(inicio, fim, obra: str = "") -> list:
    """Os feriados que caem no período, para uma obra (ou só os nacionais).

    ⚠️ O NACIONAL VALE SEMPRE; o estadual, o municipal e o da obra, só onde valem.
    Somar tudo sem distinguir descontaria o feriado de Olinda do auxílio de quem
    trabalha em Sobral.

    Sem o ponto (fora do ar, migração por aplicar), devolve lista vazia e deixa
    no log: a folha segue, sem descontar feriado — o mesmo que acontecia antes
    com a tabela vazia. A tela de Feriados mostra o erro."""
    try:
        calendario = feriados_do_ponto(inicio, fim)
    except Exception:  # noqa: BLE001
        logger.exception("Feriados: não consegui ler os feriados do ponto")
        return []
    saida, dia = [], inicio
    while dia <= fim:
        nome = calendario.feriado(dia, obra)
        if nome:
            saida.append({"data": dia, "descricao": nome, "obra": obra or ""})
        dia += dt.timedelta(days=1)
    return saida


def listar_feriados(ano: int | None = None) -> list:
    """Os feriados do PONTO, para a tela (do mais recente para o mais antigo)."""
    from app.apps.ponto import db as banco_do_ponto
    from app.apps.ponto.core import feriados as feriados_do_ponto
    with banco_do_ponto.conexao() as conn:
        lista = feriados_do_ponto.listar(conn, ano=ano or None)
    saida = []
    for f in sorted(lista, key=lambda x: x["data"], reverse=True):
        a = f["abrangencia"]
        if a == "ESTADUAL":
            onde = f.get("uf") or ""
        elif a == "MUNICIPAL":
            onde = f"{f.get('municipio') or ''} / {f.get('uf') or ''}"
        elif a == "OBRA":
            onde = f.get("obra_codigo") or ""
        else:
            onde = ""
        saida.append({"data": f["data"], "abrangencia": a,
                      "rotulo": ROTULO_DO_PONTO.get(a, a), "onde": onde,
                      "descricao": f.get("nome") or ""})
    return saida


# ---------------------------------------------------------------------------
# OS FERIADOS ANTIGOS DESTA TELA — o que o ponto ainda não tem
# ---------------------------------------------------------------------------
def _antigos() -> list:
    from .db import consultar
    if not _pronto():
        return []
    return [{"id": l[0], "data": l[1], "abrangencia": l[2], "obra": l[3] or "",
             "descricao": l[4] or ""}
            for l in consultar("SELECT id, data, abrangencia, obra, descricao "
                               "  FROM analisesps.feriado ORDER BY data")]


def antigos_fora_do_ponto() -> list:
    """Os feriados cadastrados AQUI que o ponto não cobre — eles deixaram de
    contar. Coberto quer dizer: naquele dia, para aquela obra (ou nacional), o
    ponto já diz que é feriado, seja pelo nacional, estadual, municipal ou da obra."""
    antigos = _antigos()
    if not antigos:
        return []
    calendario = FeriadosDoPonto(antigos[0]["data"], antigos[-1]["data"])
    saida = []
    for f in antigos:
        obra = f["obra"] if f["abrangencia"] == POR_OBRA else ""
        if calendario.feriado(f["data"], obra):
            continue
        f["rotulo"] = ROTULO_DA_ABRANGENCIA.get(f["abrangencia"], f["abrangencia"])
        f["obra_no_ponto"] = bool(calendario.obra_id(obra)) if obra else True
        saida.append(f)
    return saida


def levar_ao_ponto(quem: str = "") -> dict:
    """Grava no ponto os feriados antigos que ele não cobre, pela gravação do
    próprio ponto (as mesmas validações). O de obra só vai quando a obra casa com
    uma obra do ponto; o que não casa volta na resposta, com o nome da obra."""
    from app.apps.ponto import db as banco_do_ponto
    from app.apps.ponto.core import feriados as feriados_do_ponto
    from app.apps.ponto.erros import ErroDeValidacao

    faltam = antigos_fora_do_ponto()
    if not faltam:
        return {"levados": 0, "sem_obra": []}
    calendario = FeriadosDoPonto(faltam[0]["data"], faltam[-1]["data"])
    levados, sem_obra = 0, []
    with banco_do_ponto.conexao() as conn:
        for f in faltam:
            dados = {"data": f["data"].isoformat(),
                     "nome": f["descricao"] or "Feriado"}
            if f["abrangencia"] == POR_OBRA:
                obra_id = calendario.obra_id(f["obra"])
                if not obra_id:
                    sem_obra.append(f["obra"])
                    continue
                dados.update(abrangencia="OBRA", obra_id=obra_id)
            else:
                dados["abrangencia"] = "NACIONAL"
            try:
                feriados_do_ponto.gravar(conn, dados)
                levados += 1
            except ErroDeValidacao as e:
                if "já está cadastrado" not in str(e):
                    raise
    esquecer_leituras()
    logger.info("Análise de SPs: %s levou %d feriado(s) antigo(s) para o ponto "
                "(%d sem obra no ponto).", quem or "(sem nome)", levados, len(sem_obra))
    return {"levados": levados, "sem_obra": sorted(set(sem_obra))}


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
            'tabela de férias não encontrada no banco. Clique em "Aplicar atualizações '
            'do banco" em Configurações e repita a operação.')

    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        raise ErroDoCalendario("selecione o colaborador — CPF incompleto.")
    if not cpf_valido(digitos):
        raise ErroDoCalendario(
            f"o CPF {cpf_bonito(digitos)} tem dígito verificador inválido.")

    de = formatos.para_data(inicio)
    ate = formatos.para_data(fim)
    if de is None or ate is None:
        raise ErroDoCalendario("informe o primeiro e o último dia das férias.")
    if ate < de:
        raise ErroDoCalendario(
            "a data final é anterior à data inicial — verifique as datas.")
    # Um período absurdo é quase sempre ano digitado errado, e descontaria meses
    # de auxílio de alguém.
    if (ate - de).days > 400:
        raise ErroDoCalendario(
            f"período de {(ate - de).days} dias. Verifique o ano das datas.")

    conflito = ferias_que_cruzam(digitos, de, ate)
    if conflito:
        outro = conflito[0]
        raise ErroDoCalendario(
            f"o colaborador já possui férias de "
            f"{outro['inicio'].strftime('%d/%m/%Y')} a "
            f"{outro['fim'].strftime('%d/%m/%Y')}, e os períodos se cruzam. "
            "Exclua o outro período ou ajuste as datas.")

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
    # De onde veio (049): importada da Listagem de Férias do Fortes, com
    # retorno, abono e líquido; lançada à mão fica com a origem vazia.
    from .db import tem_coluna
    importadas = tem_coluna("ferias", "origem")
    linhas = consultar(
        "SELECT id, cpf, nome, inicio, fim, observacao, criado_por"
        + (", origem, retorno, abono_dias, liquido" if importadas else "")
        + "  FROM analisesps.ferias " + onde +
        " ORDER BY inicio DESC, lower(nome) LIMIT ?",
        tuple(params) + (int(teto),))
    return [{"id": l[0], "cpf": l[1], "cpf_bonito": cpf_bonito(l[1]),
             "nome": l[2], "inicio": l[3], "fim": l[4], "observacao": l[5],
             "criado_por": l[6], "dias": (l[4] - l[3]).days + 1,
             "origem": l[7] if importadas else "",
             "retorno": l[8] if importadas else None,
             "abono_dias": l[9] if importadas else 0,
             "liquido": l[10] if importadas else None}
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
