# -*- coding: utf-8 -*-
"""
Escalas de trabalho — configuráveis pela gestão (decisão do dono, 03/10/2026:
"horários configuráveis, deve ser assim mesmo").

Uma escala é um modelo ("Obra seg–qui 7–17, sex 7–16"); cada pessoa recebe uma
escala com DATA DE INÍCIO. Trocar a escala de alguém não reescreve o passado: o
espelho de agosto continua usando a escala que valia em agosto.

O formato guardado é o que a pessoa lê ("07:00"), e a conversão para minutos
— que é o que o cálculo do dia usa — acontece aqui, num lugar só.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import apuracao

TIPOS = ("SEMANAL", "CICLO_12X36")
DIAS = ("segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo")
_HORA = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _minutos(texto: str, campo: str) -> int:
    valor = str(texto or "").strip()
    if not _HORA.match(valor):
        raise ErroDeValidacao(f"horário inválido: {texto!r} (use HH:MM)", campo=campo)
    h, m = valor.split(":")
    return int(h) * 60 + int(m)


def _periodo(entrada: str, saida: str, campo: str) -> tuple[int, int]:
    """Converte um par "HH:MM"; saída menor que a entrada é no dia seguinte."""
    e, s = _minutos(entrada, campo), _minutos(saida, campo)
    if s <= e:
        s += 1440
    if s - e > 16 * 60:
        raise ErroDeValidacao(f"período de {entrada} a {saida} passa de 16 horas", campo=campo)
    return e, s


def validar_semana(semana) -> dict[str, list[list[str]]]:
    """Normaliza {"0": [["07:00","11:00"], …], …}. Confere sobreposição."""
    if isinstance(semana, str):
        try:
            semana = json.loads(semana or "{}")
        except json.JSONDecodeError as e:
            raise ErroDeValidacao("semana ilegível", campo="semana") from e
    if not isinstance(semana, dict):
        raise ErroDeValidacao("semana deve ser um dicionário por dia", campo="semana")
    saida: dict[str, list[list[str]]] = {}
    for chave, periodos in semana.items():
        try:
            dia = int(chave)
        except (TypeError, ValueError) as e:
            raise ErroDeValidacao(f"dia da semana inválido: {chave!r}", campo="semana") from e
        if not 0 <= dia <= 6:
            raise ErroDeValidacao(f"dia da semana inválido: {chave!r}", campo="semana")
        if not periodos:
            continue
        convertidos = sorted(_periodo(p[0], p[1], f"semana.{dia}") for p in periodos)
        for (_, s1), (e2, _) in zip(convertidos, convertidos[1:]):
            if e2 < s1:
                raise ErroDeValidacao(f"períodos se sobrepõem na {DIAS[dia]}", campo=f"semana.{dia}")
        saida[str(dia)] = [[str(p[0]).strip(), str(p[1]).strip()] for p in
                           sorted(periodos, key=lambda p: _minutos(p[0], "semana"))]
    return saida


def para_apuracao(escala: dict, data_base: Optional[dt.date] = None,
                  trabalhados: Optional[tuple] = None) -> apuracao.Escala:
    """A escala do banco → o objeto que o cálculo do dia entende. No 12x36, com os
    dias trabalhados da pessoa, o ciclo é o automático (`apuracao.dia_do_ciclo`)."""
    if escala["tipo"] == "CICLO_12X36":
        return apuracao.Escala(
            tipo="CICLO_12X36",
            periodo_ciclo=_periodo(escala["ciclo_entrada"], escala["ciclo_saida"], "ciclo"),
            data_base=data_base, trabalhados=trabalhados)
    semana = escala["semana"] if isinstance(escala["semana"], dict) else json.loads(escala["semana"] or "{}")
    return apuracao.Escala(tipo="SEMANAL", semana={
        int(d): [_periodo(p[0], p[1], "semana") for p in periodos]
        for d, periodos in semana.items() if periodos})


def horas_semanais(escala: dict) -> float:
    """Para a tela: quantas horas a escala prevê numa semana (12x36 ≈ 42 h)."""
    if escala["tipo"] == "CICLO_12X36":
        e, s = _periodo(escala["ciclo_entrada"], escala["ciclo_saida"], "ciclo")
        return round((s - e) * 3.5 / 60, 1)
    esc = para_apuracao(escala)
    return round(sum(s - e for ps in esc.semana.values() for e, s in ps) / 60, 1)


# ---------------------------------------------------------------------------
# Banco
# ---------------------------------------------------------------------------
def listar(conn: Connection, *, so_ativas: bool = False) -> list[dict]:
    sql = "SELECT * FROM ponto.escalas" + (" WHERE ativo" if so_ativas else "") + " ORDER BY nome"
    linhas = db.todos(conn, sql)
    for e in linhas:
        e["horas_semanais"] = horas_semanais(e)
        e["pessoas"] = int(db.um(conn, """
            SELECT count(DISTINCT colaborador_id) AS n FROM ponto.colaborador_escalas ce
             WHERE ce.escala_id = :id AND ce.vigencia_inicio = (
                   SELECT max(x.vigencia_inicio) FROM ponto.colaborador_escalas x
                    WHERE x.colaborador_id = ce.colaborador_id AND x.vigencia_inicio <= CURRENT_DATE)
        """, id=e["id"])["n"])
    return linhas


def obter(conn: Connection, escala_id: int) -> dict:
    e = db.um(conn, "SELECT * FROM ponto.escalas WHERE id = :id", id=escala_id)
    if not e:
        raise NaoEncontrado("escala não encontrada")
    return e


def gravar(conn: Connection, dados: dict, escala_id: int | None = None) -> dict:
    nome = str(dados.get("nome") or "").strip()
    if len(nome) < 3:
        raise ErroDeValidacao("dê um nome à escala (ex.: Obra seg–sex 7h–17h)", campo="nome")
    tipo = str(dados.get("tipo") or "SEMANAL").strip().upper()
    if tipo not in TIPOS:
        raise ErroDeValidacao(f"tipo de escala desconhecido: {tipo}", campo="tipo")
    semana: dict = {}
    entrada = saida = None
    if tipo == "SEMANAL":
        semana = validar_semana(dados.get("semana") or {})
        if not semana:
            raise ErroDeValidacao("a escala precisa de ao menos um dia com horário", campo="semana")
    else:
        entrada, saida = str(dados.get("ciclo_entrada") or ""), str(dados.get("ciclo_saida") or "")
        _periodo(entrada, saida, "ciclo")
    params = dict(nome=nome, tipo=tipo, semana=json.dumps(semana), e=entrada, s=saida,
                  d=str(dados.get("descricao") or "")[:300],
                  a=bool(dados.get("ativo", True)))
    duplicada = db.um(conn, "SELECT id FROM ponto.escalas WHERE lower(nome) = lower(:n) "
                            "AND id <> COALESCE(:id, 0)", n=nome, id=escala_id)
    if duplicada:
        raise ErroDeValidacao("já existe uma escala com esse nome", campo="nome")
    if escala_id:
        obter(conn, escala_id)
        db.executar(conn, """
            UPDATE ponto.escalas SET nome = :nome, tipo = :tipo, semana = CAST(:semana AS jsonb),
                   ciclo_entrada = :e, ciclo_saida = :s, descricao = :d, ativo = :a,
                   atualizado_em = now() WHERE id = :id
        """, id=escala_id, **params)
        # Horário mudou (ex.: virou turno da noite): o dia das batidas de quem está nela é refeito.
        for v in db.todos(conn, "SELECT colaborador_id, min(vigencia_inicio) AS desde FROM ponto.colaborador_escalas "
                                "WHERE escala_id = :id GROUP BY colaborador_id", id=escala_id):
            realinhar_dias(conn, int(v["colaborador_id"]), v["desde"])
        return obter(conn, escala_id)
    linha = db.um(conn, """
        INSERT INTO ponto.escalas (nome, tipo, semana, ciclo_entrada, ciclo_saida, descricao, ativo)
        VALUES (:nome, :tipo, CAST(:semana AS jsonb), :e, :s, :d, :a) RETURNING id
    """, **params)
    return obter(conn, int(linha["id"]))


def atribuir(conn: Connection, colaborador_id: int, escala_id: int, vigencia_inicio,
             *, ciclo_data_base=None, por: str = "") -> None:
    escala = obter(conn, escala_id)
    if not escala["ativo"]:
        raise ErroDeValidacao("escala inativa", campo="escala_id")
    try:
        inicio = dt.date.fromisoformat(str(vigencia_inicio))
    except ValueError as e:
        raise ErroDeValidacao("data de início ilegível (AAAA-MM-DD)", campo="vigencia_inicio") from e
    # O 12x36 descobre o ciclo pelas batidas (30/10/2026); o dia informado, se vier, só vale
    # enquanto a pessoa ainda não tem batida nenhuma.
    base = None
    if escala["tipo"] == "CICLO_12X36" and ciclo_data_base:
        try:
            base = dt.date.fromisoformat(str(ciclo_data_base))
        except ValueError as e:
            raise ErroDeValidacao("data-base do ciclo ilegível", campo="ciclo_data_base") from e
    db.executar(conn, """
        INSERT INTO ponto.colaborador_escalas (colaborador_id, escala_id, vigencia_inicio,
                                               ciclo_data_base, definido_por)
        VALUES (:c, :e, :i, :b, :p)
        ON CONFLICT (colaborador_id, vigencia_inicio) DO UPDATE
           SET escala_id = :e, ciclo_data_base = :b, definido_por = :p, criado_em = now()
    """, c=colaborador_id, e=escala_id, i=inicio, b=base, p=(por or "")[:120])
    realinhar_dias(conn, colaborador_id, inicio)


def historico_da_pessoa(conn: Connection, colaborador_id: int) -> list[dict]:
    return db.todos(conn, """
        SELECT ce.*, e.nome AS escala_nome, e.tipo AS escala_tipo
          FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
         WHERE ce.colaborador_id = :c ORDER BY ce.vigencia_inicio DESC
    """, c=colaborador_id)


# A ESCALA PADRÃO DA EMPRESA (pedido do dono, 06/10/2026: "falta de batida de um
# dia, no dia seguinte o sistema já deveria apresentar como falta"). Sem escala o
# espelho não sabe o que era esperado e não aponta falta; com a escala padrão
# escolhida na Configuração, quem ainda não tem escala própria é julgado por ela
# — antes da primeira escala própria, e só nesse intervalo.
PARAMETRO_PADRAO = "escalas.padrao"


def escala_padrao(conn: Connection) -> Optional[dict]:
    if not db.tem_coluna(conn, "parametros", "valor"):
        return None
    from . import parametros
    valor = (parametros.ler(conn, PARAMETRO_PADRAO, "") or "").strip()
    if not valor.isdigit():
        return None
    return db.um(conn, "SELECT * FROM ponto.escalas WHERE id = :id AND ativo", id=int(valor))


def gravar_padrao(conn: Connection, escala_id, por: str) -> Optional[dict]:
    from . import parametros
    if escala_id in (None, "", 0, "0"):
        parametros.gravar(conn, PARAMETRO_PADRAO, "", por)
        return None
    e = db.um(conn, "SELECT * FROM ponto.escalas WHERE id = :id AND ativo", id=int(escala_id))
    if not e:
        raise ErroDeValidacao("escala não encontrada ou inativa", campo="escala_id")
    if e["tipo"] != "SEMANAL":
        raise ErroDeValidacao("a escala padrão precisa ser semanal (12x36 depende do dia de início de cada um)",
                              campo="escala_id")
    parametros.gravar(conn, PARAMETRO_PADRAO, str(int(escala_id)), por)
    return e


class EscalasDaPessoa:
    """As vigências de uma pessoa, carregadas UMA vez para um período inteiro."""

    def __init__(self, conn: Connection, colaborador_id: int):
        self._conn, self._colaborador_id, self._trabalhados = conn, colaborador_id, None
        self.vigencias = db.todos(conn, """
            SELECT ce.vigencia_inicio, ce.ciclo_data_base, e.*
              FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
             WHERE ce.colaborador_id = :c ORDER BY ce.vigencia_inicio
        """, c=colaborador_id)
        self.padrao = escala_padrao(conn)
        self._cache: dict = {}

    def no_dia(self, dia: dt.date) -> tuple[Optional[apuracao.Escala], Optional[dict]]:
        atual = None
        for v in self.vigencias:
            if v["vigencia_inicio"] <= dia:
                atual = v
            else:
                break
        if atual is None:
            if self.padrao is None:
                return None, None
            if "padrao" not in self._cache:
                self._cache["padrao"] = para_apuracao(self.padrao, None)
            return self._cache["padrao"], {**self.padrao, "nome": f"{self.padrao['nome']} (padrão da empresa)",
                                            "vigencia_inicio": None, "ciclo_data_base": None}
        chave = (atual["id"], atual["vigencia_inicio"])
        if chave not in self._cache:
            self._cache[chave] = para_apuracao(
                atual, atual["ciclo_data_base"],
                # só os dias desta vigência: dar a escala de novo a partir de um dia recomeça o ciclo
                tuple(w for w in self.trabalhados() if w >= atual["vigencia_inicio"])
                if atual["tipo"] == "CICLO_12X36" else None)
        return self._cache[chave], atual

    def trabalhados(self) -> tuple:
        """Os dias com batida da pessoa (para o ciclo 12x36 automático), lidos uma vez."""
        if self._trabalhados is None:
            self._trabalhados = dias_trabalhados(self._conn, [self._colaborador_id]).get(self._colaborador_id, ())
        return self._trabalhados


def dias_trabalhados(conn: Connection, ids: list[int]) -> dict[int, tuple]:
    """{pessoa: (dias com batida válida, em ordem)} — o que o ciclo 12x36 automático lê."""
    if not ids:
        return {}
    saida: dict[int, list] = {}
    # Só o dia em que a pessoa ENTROU vota no ciclo (31/10/2026, o vigia com um 07:01 solto no dia 16 — a saída do
    # turno da véspera, de antes do ponto valer): o dia com uma batida só, de manhã, não é dia trabalhado — sem
    # isto, o 16 empatava com o 17 e o 18, folga, virava falta. Conta o dia com duas batidas ou mais, ou com
    # alguma a partir do meio-dia.
    for r in db.todos(conn, """
            SELECT colaborador_id, data_referencia FROM ponto.marcacoes
             WHERE colaborador_id = ANY(:ids) AND status <> 'REJEITADA'
             GROUP BY colaborador_id, data_referencia
            HAVING count(*) >= 2
                OR max(EXTRACT(HOUR FROM timestamp_servidor AT TIME ZONE 'America/Fortaleza')) >= 12
             ORDER BY colaborador_id, data_referencia""", ids=[int(i) for i in ids]):
        saida.setdefault(int(r["colaborador_id"]), []).append(r["data_referencia"])
    # A TROCA DE PLANTÃO lançada como compensação (31/10/2026, o dono, dois vigias da mesma obra que trocaram a
    # noite): o dia trabalhado NO LUGAR do outro não vota no ciclo, e a noite que ele deixou (a folga
    # compensatória) vota como se tivesse sido trabalhada — senão a troca puxa o ciclo da pessoa para o lado errado.
    trocas = db.todos(conn, """
            SELECT colaborador_id, data_inicio AS folga, dia_trabalhado FROM ponto.ocorrencias
             WHERE colaborador_id = ANY(:ids) AND tipo = 'COMPENSACAO' AND status = 'APROVADA'
               AND dia_trabalhado IS NOT NULL""", ids=[int(i) for i in ids])
    for t in trocas:
        dias = set(saida.get(int(t["colaborador_id"]), []))
        dias.discard(t["dia_trabalhado"])
        dias.add(t["folga"])
        saida[int(t["colaborador_id"])] = sorted(dias)
    return {k: tuple(v) for k, v in saida.items()}


# ---------------------------------------------------------------------------
# O TURNO DA NOITE (30/10/2026, o dono: "se eu já estou dizendo que é vigia
# noturno, o dia que ele inicia de tarde (…) é para ser considerado o horário de
# entrada dele. E amanhã, no dia seguinte, é de saída"). Antes, só a marcação
# "vigia noturno" na jornada da pessoa levava a batida da madrugada para o dia
# anterior; com a escala 17h–6h e a jornada padrão, a saída das 6h caía no dia
# seguinte e o turno se partia em dois (falta parcial num dia, batida solta no
# outro). Agora a ESCALA decide: se o turno dela passa da meia-noite, a batida
# antes das 10h é do dia em que o turno começou.
# ---------------------------------------------------------------------------
def _vira(periodos) -> bool:
    return any(s > 1440 for _, s in periodos)


def turno_da_noite(escala: Optional[apuracao.Escala], dia_anterior: dt.date) -> bool:
    """PURA. A batida da manhã seguinte a `dia_anterior` ainda é do turno dele?
    12x36 que passa da meia-noite: sempre (mesmo que o ciclo diga folga — a
    noite trabalhada fora do ciclo continua inteira num dia só). Semanal: se o
    turno daquele dia passa da meia-noite, ou se TODOS os turnos da escala passam
    (a escala só de noite, com uma noite a mais trabalhada)."""
    if escala is None:
        return False
    if escala.tipo == "CICLO_12X36":
        return bool(escala.periodo_ciclo) and escala.periodo_ciclo[1] > 1440
    if _vira(escala.semana.get(dia_anterior.weekday(), [])):
        return True
    todos = [p for ps in escala.semana.values() for p in ps]
    return bool(todos) and all(s > 1440 for _, s in todos)


def dia_de_trabalho(conn: Connection, pessoa: dict, momento: dt.datetime,
                    escalas_da_pessoa: Optional["EscalasDaPessoa"] = None) -> dt.date:
    """O dia de trabalho a que a batida pertence: a jornada "vigia noturno" OU a
    escala com turno da noite levam a batida antes das 10h para o dia anterior."""
    local = horario.para_local(momento)
    if pessoa.get("tipo_jornada") == "VIGIA_NOTURNO_2" or local.hour >= horario.HORA_DE_CORTE_NOTURNO:
        return horario.data_referencia(momento, pessoa.get("tipo_jornada") or "")
    anterior = local.date() - dt.timedelta(days=1)
    vig = escalas_da_pessoa or EscalasDaPessoa(conn, int(pessoa["id"]))
    esc, info = vig.no_dia(anterior)
    if not (info and info.get("vigencia_inicio")):
        # A véspera é de ANTES da primeira escala própria (31/10/2026: a escala do vigia começou no dia 16, junto
        # com o ponto, e o 07:01 do dia 16 — a saída do turno do dia 15 — ficou no 16, pela escala padrão de dia).
        # Se a escala própria começa justamente HOJE e é da noite, a batida da manhã é da véspera.
        hoje_esc, hoje_info = vig.no_dia(local.date())
        if hoje_info and hoje_info.get("vigencia_inicio") == local.date():
            esc = hoje_esc
    return anterior if turno_da_noite(esc, anterior) else local.date()


def realinhar_dias(conn: Connection, colaborador_id: int, desde: dt.date) -> int:
    """Refaz o dia de trabalho das batidas da pessoa a partir de `desde` (a
    véspera, para pegar a saída da madrugada), depois de a escala mudar. Só o
    `data_referencia` muda — a hora, a obra e o hash encadeado ficam como estão
    (Portaria 671). Mês fechado não é tocado. Devolve quantas batidas mudaram."""
    from . import cadastros, competencias
    pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
    if not pessoa:
        return 0
    vigencias = EscalasDaPessoa(conn, colaborador_id)
    mudou = 0
    for m in db.todos(conn, """
            SELECT id, timestamp_servidor, data_referencia FROM ponto.marcacoes
             WHERE colaborador_id = :c AND data_referencia >= :d
        """, c=colaborador_id, d=desde - dt.timedelta(days=1)):
        novo = dia_de_trabalho(conn, pessoa, m["timestamp_servidor"], vigencias)
        if novo == m["data_referencia"]:
            continue
        if competencias.esta_fechada(conn, novo) or competencias.esta_fechada(conn, m["data_referencia"]):
            continue
        db.executar(conn, "UPDATE ponto.marcacoes SET data_referencia = :d WHERE id = :id", d=novo, id=m["id"])
        mudou += 1
    return mudou


# ---------------------------------------------------------------------------
# AS ESCALAS DAS PESSOAS, EM LOTE (26/10/2026, o dono: "eu quero aplicar a escala,
# selecionar as pessoas e aplicar; a escala dos vigias: selecionar os vigias todos
# e aplicar (…) e cada funcionário novo já entrasse na escala de produção").
# O funcionário novo entra pela ESCALA PADRÃO: quem não tem escala própria é
# julgado por ela, desde o primeiro dia. Aqui se dá escala própria a muitos de uma
# vez — a mesma `atribuir` de uma pessoa, na mesma transação para todos.
# ---------------------------------------------------------------------------
MAX_EM_LOTE = 1000


def pessoas_e_escalas(conn: Connection, pessoas: list[dict], hoje: dt.date) -> list[dict]:
    """Cada pessoa com a escala própria que vale hoje (ou a próxima, se só tem futura)."""
    ids = [int(p["id"]) for p in pessoas]
    vig: dict[int, list[dict]] = {}
    for v in db.todos(conn, """
        SELECT ce.colaborador_id, ce.vigencia_inicio, e.id, e.nome, e.tipo
          FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
         WHERE ce.colaborador_id = ANY(:ids) ORDER BY ce.vigencia_inicio""", ids=ids):
        vig.setdefault(int(v["colaborador_id"]), []).append(v)
    saida = []
    for p in pessoas:
        lista = vig.get(int(p["id"]), [])
        atual = next((v for v in reversed(lista) if v["vigencia_inicio"] <= hoje), None)
        futura = next((v for v in lista if v["vigencia_inicio"] > hoje), None)
        saida.append({
            "id": int(p["id"]), "nome": p["nome"], "cpf": p.get("cpf"), "funcao": p.get("funcao"),
            "obra": p.get("obra_codigo"), "tipo_jornada": p.get("tipo_jornada"),
            "escala_id": atual["id"] if atual else None, "escala": atual["nome"] if atual else None,
            "desde": atual["vigencia_inicio"].isoformat() if atual else None,
            "proxima": ({"escala": futura["nome"], "desde": futura["vigencia_inicio"].isoformat()} if futura else None),
        })
    return saida


def aplicar_em_lote(conn: Connection, colaboradores: list[int], escala_id: int, vigencia_inicio,
                    *, ciclo_data_base=None, por: str = "") -> int:
    """A escala para todos os `colaboradores`, a partir de `vigencia_inicio` — e só ela
    dali em diante: troca de escala marcada para DEPOIS dessa data é substituída (senão
    "aplicar desde o início do ponto" ficaria escondido atrás da troca seguinte). Tudo
    numa transação: ou todos ficam com a escala, ou nenhum."""
    ids = sorted({int(c) for c in colaboradores or []})
    if not ids:
        raise ErroDeValidacao("marque ao menos uma pessoa", campo="colaboradores")
    if len(ids) > MAX_EM_LOTE:
        raise ErroDeValidacao(f"no máximo {MAX_EM_LOTE} pessoas de uma vez", campo="colaboradores")
    try:
        inicio = dt.date.fromisoformat(str(vigencia_inicio))
    except ValueError as e:
        raise ErroDeValidacao("data de início ilegível (AAAA-MM-DD)", campo="vigencia_inicio") from e
    for cid in ids:
        atribuir(conn, cid, int(escala_id or 0), inicio.isoformat(), ciclo_data_base=ciclo_data_base, por=por)
        db.executar(conn, "DELETE FROM ponto.colaborador_escalas WHERE colaborador_id = :c AND vigencia_inicio > :i",
                    c=cid, i=inicio)
    return len(ids)
