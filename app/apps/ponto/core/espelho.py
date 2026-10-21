# -*- coding: utf-8 -*-
"""
O espelho de ponto: cada dia de uma pessoa, com as batidas, a escala, o
feriado, a ocorrência e o resultado do cálculo do dia.

É o que a gestão mostra, o que o "meu ponto" do celular mostra, o que o banco de
horas soma e o que os alertas leem. Um lugar só — se a regra mudar, muda para
todos ao mesmo tempo, e ninguém vê um número na gestão e outro no celular.

O QUE ENTRA NO CÁLCULO:
  · batidas VALIDA, AJUSTADA e EM_ANALISE. A em análise entra porque ainda
    não foi rejeitada — e o dia mostra que tem batida em análise, para ninguém
    tomar o número como definitivo. REJEITADA não entra.
  · ocorrências APROVADAS. As pendentes aparecem no dia, mas não mudam a conta.

DIAS QUE NÃO SE JULGAM: o futuro, o dia de hoje enquanto não acabou (sem
batida às 8h não é falta), antes da admissão e depois da demissão.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import ajustes, apuracao, cadastros, escalas, feriados
from . import vigencia as vigencia_do_ponto

MAX_DIAS = 400

# Ocorrência aprovada → o código que o cálculo do dia entende como abono.
ABONO_POR_TIPO = {
    "ATESTADO": "ATESTADO", "LICENCA": "LICENCA", "FERIAS": "FERIAS",
    "AFASTAMENTO": "AFASTAMENTO", "ABONO": "ABONO",
    "FOLGA_BANCO": "FOLGA_COMPENSATORIA", "COMPENSACAO": "FOLGA_COMPENSATORIA",
}
ROTULO_TIPO = {
    "ATESTADO": "Atestado", "LICENCA": "Licença", "FERIAS": "Férias",
    "AFASTAMENTO": "Afastamento", "ABONO": "Abono", "AJUSTE_BATIDA": "Ajuste de batida",
    "COMPENSACAO": "Compensação", "FOLGA_BANCO": "Folga do banco de horas",
}


def _dias(inicio: dt.date, fim: dt.date):
    atual = inicio
    while atual <= fim:
        yield atual
        atual += dt.timedelta(days=1)


class _EscalasCarregadas(escalas.EscalasDaPessoa):
    """As vigências de uma pessoa, já lidas pelo Lote (sem ir ao banco de novo)."""

    def __init__(self, vigencias: list[dict], padrao):
        self.vigencias, self.padrao, self._cache = vigencias, padrao, {}


class Lote:
    """O que o espelho lê do banco, para MUITAS pessoas de uma vez (21/10/2026, o
    quadro do ponto: "o dia de todas as pessoas"). Sem isto, cada pessoa custava
    uma dezena de consultas; com 500 pessoas, milhares. O cálculo do dia continua
    o MESMO do espelho — só a leitura muda —, e por isso o quadro e o espelho de
    cada um nunca divergem. As rondas do vigia não entram (o quadro não as mostra)."""

    def __init__(self, conn: Connection, pessoas: list[dict], inicio: dt.date, fim: dt.date):
        ids = [int(p["id"]) for p in pessoas]
        self.pessoas = {int(p["id"]): p for p in pessoas}
        extra = (", m.sem_internet, m.recebida_em" if db.tem_coluna(conn, "marcacoes", "sem_internet")
                 else ", FALSE AS sem_internet, NULL::timestamptz AS recebida_em")
        self.batidas: dict[int, list[dict]] = {}
        for b in db.todos(conn, """
            SELECT m.colaborador_id, m.id, m.nsr, m.timestamp_servidor, m.data_referencia, m.obra_id, m.status,
                   m.dentro_da_cerca, m.distancia_metros, m.origem, m.foto_id, m.motivo_analise,
                   m.registrado_por, m.latitude, m.longitude, o.codigo AS obra_codigo""" + extra + """
              FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
             WHERE m.colaborador_id = ANY(:ids) AND m.data_referencia BETWEEN :i AND :f
               AND m.status <> 'REJEITADA'
             ORDER BY m.timestamp_servidor""", ids=ids, i=inicio - dt.timedelta(days=1), f=fim):
            self.batidas.setdefault(int(b["colaborador_id"]), []).append(b)
        self.ocorrencias: dict[int, list[dict]] = {}
        for o in db.todos(conn, """
            SELECT colaborador_id, id, tipo, status, data_inicio, data_fim, dia_trabalhado, descricao, minutos, horario
              FROM ponto.ocorrencias
             WHERE colaborador_id = ANY(:ids) AND status NOT IN ('NEGADA', 'CANCELADA')
               AND ((data_fim >= :i AND data_inicio <= :f) OR (dia_trabalhado BETWEEN :i AND :f))
             ORDER BY id""", ids=ids, i=inicio, f=fim):
            self.ocorrencias.setdefault(int(o["colaborador_id"]), []).append(o)
        self.pre_assinaladas = _pre_assinaladas(conn)
        padrao = escalas.escala_padrao(conn)
        vig: dict[int, list[dict]] = {}
        for v in db.todos(conn, """
            SELECT ce.colaborador_id AS dono, ce.vigencia_inicio, ce.ciclo_data_base, e.*
              FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
             WHERE ce.colaborador_id = ANY(:ids) ORDER BY ce.vigencia_inicio""", ids=ids):
            vig.setdefault(int(v.pop("dono")), []).append(v)
        self.escalas = {cid: _EscalasCarregadas(vig.get(cid, []), padrao) for cid in ids}
        self.calendario = feriados.Calendario(conn, inicio, fim)


def _pre_assinaladas(conn: Connection) -> set[int]:
    """As obras de intervalo pré-assinalado (só entrada e saída — migração 009)."""
    return ({int(l["obra_id"]) for l in db.todos(
        conn, "SELECT obra_id FROM ponto.obra_config WHERE intervalo_pre_assinalado")}
        if db.tem_coluna(conn, "obra_config", "intervalo_pre_assinalado") else set())


def montar(conn: Connection, colaborador_id: int, inicio: dt.date, fim: dt.date, *,
           hoje: Optional[dt.date] = None, agora: Optional[dt.datetime] = None,
           lote: Optional[Lote] = None) -> dict:
    """O espelho de `inicio` a `fim` (inclusive). Com `lote`, lê dele (o período do
    lote tem de cobrir o pedido)."""
    if fim < inicio:
        raise ErroDeValidacao("fim antes do início", campo="fim")
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ErroDeValidacao(f"período máximo do espelho: {MAX_DIAS} dias", campo="fim")
    pessoa = (lote.pessoas.get(int(colaborador_id)) if lote is not None
              else cadastros.colaborador_por_id(conn, colaborador_id))
    if not pessoa:
        raise NaoEncontrado("pessoa não encontrada")
    hoje = hoje or horario.hoje()
    agora = agora or horario.agora()

    # A batida guardada sem internet (migração 012): o espelho mostra quando chegou.
    extra = (", m.sem_internet, m.recebida_em" if db.tem_coluna(conn, "marcacoes", "sem_internet")
             else ", FALSE AS sem_internet, NULL::timestamptz AS recebida_em")
    batidas = lote.batidas.get(int(colaborador_id), []) if lote is not None else db.todos(conn, """
        SELECT m.id, m.nsr, m.timestamp_servidor, m.data_referencia, m.obra_id, m.status,
               m.dentro_da_cerca, m.distancia_metros, m.origem, m.foto_id, m.motivo_analise,
               m.registrado_por, m.latitude, m.longitude, o.codigo AS obra_codigo""" + extra + """
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.colaborador_id = :c AND m.data_referencia BETWEEN :i AND :f
           AND m.status <> 'REJEITADA'
         ORDER BY m.timestamp_servidor
    """, c=colaborador_id, i=inicio - dt.timedelta(days=1), f=fim)
    # O ponto vale a partir de 16/10/2026 (vigencia.py): o que veio antes não entra.
    vale_desde = vigencia_do_ponto.inicio(conn)
    por_dia: dict[dt.date, list[dict]] = {}
    for b in batidas:
        if b["data_referencia"] >= vale_desde:
            por_dia.setdefault(b["data_referencia"], []).append(b)

    if lote is not None:
        ocorrencias = lote.ocorrencias.get(int(colaborador_id), [])
        pre_assinaladas = lote.pre_assinaladas
        vigencias = lote.escalas[int(colaborador_id)]
        calendario = lote.calendario
        rondas_do_periodo: dict = {}
    else:
        ocorrencias = db.todos(conn, """
            SELECT id, tipo, status, data_inicio, data_fim, dia_trabalhado, descricao, minutos, horario
              FROM ponto.ocorrencias
             WHERE colaborador_id = :c AND status NOT IN ('NEGADA', 'CANCELADA')
               AND ((data_fim >= :i AND data_inicio <= :f)
                    OR (dia_trabalhado BETWEEN :i AND :f))
             ORDER BY id
        """, c=colaborador_id, i=inicio, f=fim)
        pre_assinaladas = _pre_assinaladas(conn)
        vigencias = escalas.EscalasDaPessoa(conn, colaborador_id)
        calendario = feriados.Calendario(conn, inicio, fim)
        # As rondas surpresa do vigia noturno (13/10/2026): ficam no dia da entrada do
        # turno. Não entram na conta da jornada — só aparecem.
        from . import rondas as _rondas
        rondas_do_periodo = _rondas.da_pessoa(conn, colaborador_id, inicio, fim)
    admissao, demissao = pessoa.get("admissao"), pessoa.get("demissao")

    dias, anterior_ultima = [], None
    vespera = por_dia.get(inicio - dt.timedelta(days=1)) or []
    if vespera:
        anterior_ultima = vespera[-1]["timestamp_servidor"]
    for dia in _dias(inicio, fim):
        do_dia = por_dia.get(dia, [])
        escala, vigencia = vigencias.no_dia(dia)
        obra_do_dia = do_dia[0]["obra_id"] if do_dia else pessoa.get("obra_id")
        nome_feriado = calendario.feriado(dia, obra_do_dia)
        aprovadas = [o for o in ocorrencias if o["status"] == "APROVADA"
                     and o["data_inicio"] <= dia <= o["data_fim"] and o["tipo"] in ABONO_POR_TIPO]
        pendentes = [o for o in ocorrencias if o["status"].startswith("AGUARDANDO")
                     and (o["data_inicio"] <= dia <= o["data_fim"] or o["dia_trabalhado"] == dia)]
        compensa = [o for o in ocorrencias if o["status"] == "APROVADA"
                    and o["tipo"] == "COMPENSACAO" and o["dia_trabalhado"] == dia]
        if dia < vale_desde:                  # antes do ponto valer: nada conta, nada aparece
            aprovadas, pendentes, compensa = [], [], []
        abono = ABONO_POR_TIPO[aprovadas[0]["tipo"]] if aprovadas else None

        so_duas = obra_do_dia in pre_assinaladas
        # A hora de Fortaleza, não a do banco (que vem em UTC): a apuração conta
        # minutos desde a meia-noite do fuso da batida — em UTC, o atraso, a
        # tolerância e a hora noturna saíam 3 h deslocados (achado em 09/10/2026).
        r = apuracao.apurar_dia(dia, escala, [horario.para_local(b["timestamp_servidor"]) for b in do_dia],
                                feriado=bool(nome_feriado), ocorrencia=abono,
                                intervalo_pre_assinalado=so_duas)
        if compensa and r.extra:
            # O dia trabalhado no lugar de outro não é extra: ele paga a folga.
            r.extra = 0
            r.alertas = [a for a in r.alertas if a not in ("TRABALHO_EM_DESCANSO",
                                                           "TRABALHO_EM_FERIADO")]
            r.situacao = "COMPENSADO"
        if do_dia and anterior_ultima and escala is not None:
            if apuracao.interjornada_curta(anterior_ultima, do_dia[0]["timestamp_servidor"], escala):
                r.alertas.append("INTERJORNADA_CURTA")
        if do_dia:
            anterior_ultima = do_dia[-1]["timestamp_servidor"]

        # Dias que não se julgam.
        julgavel = True
        if dia < vale_desde:
            r.situacao, julgavel = vigencia_do_ponto.SITUACAO_ANTES, False
        elif (admissao and dia < admissao) or (demissao and dia > demissao):
            r.situacao, julgavel = "FORA_DO_CONTRATO", False
        elif dia > hoje:
            r.situacao, julgavel = "FUTURO", False
        elif dia == hoje and (not do_dia or r.situacao in ("FALTA", "INCOMPLETO")):
            r.situacao, julgavel = "EM_ANDAMENTO", False
        if not julgavel:
            r.falta, r.debito, r.extra = False, 0, 0
            r.alertas = []
        if dia < vale_desde:
            r.previsto = r.trabalhado = r.noturno_reduzido = r.atraso = r.saida_antecipada = 0

        # O que a escala espera e o que falta — é isto que o "Meu mês" mostra
        # no "corrigir este dia", em vez de um formulário em branco (ajustes.py).
        previstas = (ajustes.marcas_previstas(escala.periodos(dia))
                     if escala is not None and dia >= vale_desde else [])
        if nome_feriado:
            # Feriado não espera batida (12/10/2026): sem isto, o "corrigir" do feriado
            # vinha com os horários da escala marcados como "faltou", e o ajuste de
            # vários dias pedia o feriado inteiro. Quem trabalhou digita os horários.
            previstas = []
        if so_duas:
            previstas = ajustes.so_entrada_e_saida(previstas)
        faltando = []
        if previstas and not aprovadas and (julgavel or dia == hoje):
            # o horário que já tem pedido esperando decisão também não "falta"
            ja_pedidos = [o["horario"] for o in pendentes if o["tipo"] == "AJUSTE_BATIDA" and o["horario"]]
            faltando = ajustes.faltantes(previstas, [
                ajustes.minutos_do_dia(horario.para_local(m), dia)
                for m in [b["timestamp_servidor"] for b in do_dia] + ja_pedidos])
            if dia == hoje:      # hoje: só o que já devia ter sido batido
                agora_min = ajustes.minutos_do_dia(horario.para_local(agora), dia)
                faltando = [f for f in faltando if f["minuto"] <= agora_min]
        dias.append({
            "data": dia.isoformat(),
            "previstas": [{"hora": p["hora"], "rotulo": p["rotulo"]} for p in previstas],
            "faltando": [{"rotulo": f["rotulo"], "sugestao": f["sugestao"], "minuto": f["minuto"]} for f in faltando],
            "dia_semana": escalas.DIAS[dia.weekday()],
            "escala": vigencia["nome"] if vigencia else None,
            "feriado": nome_feriado,
            "rondas": rondas_do_periodo.get(dia.isoformat(), []),
            "previsto": r.previsto, "trabalhado": r.trabalhado, "extra": r.extra,
            "debito": r.debito, "atraso": r.atraso, "saida_antecipada": r.saida_antecipada,
            "intervalo": r.intervalo, "noturno": r.noturno_reduzido,
            "falta": r.falta, "abonado": r.abonado, "situacao": r.situacao,
            "alertas": r.alertas,
            "em_analise": sum(1 for b in do_dia if b["status"] == "EM_ANALISE"),
            "ocorrencia": ({"id": aprovadas[0]["id"], "tipo": aprovadas[0]["tipo"],
                            "rotulo": ROTULO_TIPO[aprovadas[0]["tipo"]]} if aprovadas else None),
            "compensacao": ({"id": compensa[0]["id"]} if compensa else None),
            "pendentes": [{"id": o["id"], "tipo": o["tipo"], "rotulo": ROTULO_TIPO[o["tipo"]],
                           "status": o["status"],
                           "hora": (horario.para_local(o["horario"]).strftime("%H:%M")
                                    if o["tipo"] == "AJUSTE_BATIDA" and o.get("horario") else None)}
                          for o in pendentes],
            "so_entrada_e_saida": so_duas,
            "batidas": [{
                "id": b["id"], "nsr": b["nsr"],
                "hora": horario.para_local(b["timestamp_servidor"]).strftime("%H:%M"),
                "data_hora": horario.texto(b["timestamp_servidor"]),
                "status": b["status"], "obra": b["obra_codigo"], "origem": b["origem"],
                "fora_da_cerca": b["dentro_da_cerca"] is False,
                "distancia_metros": (float(b["distancia_metros"])
                                     if b["distancia_metros"] is not None else None),
                "motivo_analise": b["motivo_analise"], "tem_foto": b["foto_id"] is not None,
                # onde o celular estava na batida ("ver no mapa", 14/10/2026)
                "latitude": float(b["latitude"]) if b.get("latitude") is not None else None,
                "longitude": float(b["longitude"]) if b.get("longitude") is not None else None,
                "registrado_por": b["registrado_por"],
                "sem_internet": bool(b.get("sem_internet")),
                "recebida_em": horario.texto(b.get("recebida_em")) if b.get("sem_internet") else None,
            } for b in do_dia],
        })

    contaveis = [d for d in dias if d["situacao"] not in ("FUTURO", "EM_ANDAMENTO",
                                                          "FORA_DO_CONTRATO", vigencia_do_ponto.SITUACAO_ANTES)]
    resumo = {
        "dias": len(contaveis),
        "previsto": sum(d["previsto"] for d in contaveis),
        "trabalhado": sum(d["trabalhado"] for d in contaveis),
        "extra": sum(d["extra"] for d in contaveis),
        "debito": sum(d["debito"] for d in contaveis),
        "faltas": sum(1 for d in contaveis if d["falta"]),
        "abonados": sum(1 for d in contaveis if d["abonado"]),
        "incompletos": sum(1 for d in contaveis if d["situacao"] == "INCOMPLETO"),
        "noturno": sum(d["noturno"] or 0 for d in contaveis),
        "em_analise": sum(d["em_analise"] for d in dias),
        "atrasos": sum(1 for d in contaveis if "ATRASO" in d["alertas"]),
        "sem_escala": sum(1 for d in contaveis if d["situacao"] == "SEM_ESCALA"),
    }
    return {"pessoa": {"id": pessoa["id"], "nome": pessoa["nome"], "cpf": pessoa["cpf"],
                       "obra_id": pessoa.get("obra_id"), "obra": pessoa.get("obra_codigo"),
                       "tipo_jornada": pessoa["tipo_jornada"],
                       "regime_banco": pessoa.get("regime_banco", "SEM_BANCO")},
            "inicio": inicio.isoformat(), "fim": fim.isoformat(),
            "dias": dias, "resumo": resumo}


def do_mes(conn: Connection, colaborador_id: int, competencia, **kw) -> dict:
    from . import competencias
    comp = competencias.primeiro_dia(competencia or horario.hoje())
    esp = montar(conn, colaborador_id, comp, competencias.ultimo_dia(comp), **kw)
    esp["competencia"] = comp.strftime("%Y-%m")
    esp["competencia_fechada"] = competencias.esta_fechada(conn, comp)
    return esp
