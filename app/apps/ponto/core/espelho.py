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
from . import apuracao, cadastros, escalas, feriados

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


def montar(conn: Connection, colaborador_id: int, inicio: dt.date, fim: dt.date, *,
           hoje: Optional[dt.date] = None, agora: Optional[dt.datetime] = None) -> dict:
    """O espelho de `inicio` a `fim` (inclusive)."""
    if fim < inicio:
        raise ErroDeValidacao("fim antes do início", campo="fim")
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ErroDeValidacao(f"período máximo do espelho: {MAX_DIAS} dias", campo="fim")
    pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
    if not pessoa:
        raise NaoEncontrado("pessoa não encontrada")
    hoje = hoje or horario.hoje()
    agora = agora or horario.agora()

    batidas = db.todos(conn, """
        SELECT m.id, m.nsr, m.timestamp_servidor, m.data_referencia, m.obra_id, m.status,
               m.dentro_da_cerca, m.distancia_metros, m.origem, m.foto_id, m.motivo_analise,
               m.registrado_por, o.codigo AS obra_codigo
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.colaborador_id = :c AND m.data_referencia BETWEEN :i AND :f
           AND m.status <> 'REJEITADA'
         ORDER BY m.timestamp_servidor
    """, c=colaborador_id, i=inicio - dt.timedelta(days=1), f=fim)
    por_dia: dict[dt.date, list[dict]] = {}
    for b in batidas:
        por_dia.setdefault(b["data_referencia"], []).append(b)

    ocorrencias = db.todos(conn, """
        SELECT id, tipo, status, data_inicio, data_fim, dia_trabalhado, descricao, minutos
          FROM ponto.ocorrencias
         WHERE colaborador_id = :c AND status NOT IN ('NEGADA', 'CANCELADA')
           AND ((data_fim >= :i AND data_inicio <= :f)
                OR (dia_trabalhado BETWEEN :i AND :f))
         ORDER BY id
    """, c=colaborador_id, i=inicio, f=fim)

    vigencias = escalas.EscalasDaPessoa(conn, colaborador_id)
    calendario = feriados.Calendario(conn, inicio, fim)
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
        abono = ABONO_POR_TIPO[aprovadas[0]["tipo"]] if aprovadas else None

        r = apuracao.apurar_dia(dia, escala, [b["timestamp_servidor"] for b in do_dia],
                                feriado=bool(nome_feriado), ocorrencia=abono)
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
        if (admissao and dia < admissao) or (demissao and dia > demissao):
            r.situacao, julgavel = "FORA_DO_CONTRATO", False
        elif dia > hoje:
            r.situacao, julgavel = "FUTURO", False
        elif dia == hoje and (not do_dia or r.situacao in ("FALTA", "INCOMPLETO")):
            r.situacao, julgavel = "EM_ANDAMENTO", False
        if not julgavel:
            r.falta, r.debito, r.extra = False, 0, 0
            r.alertas = []

        dias.append({
            "data": dia.isoformat(),
            "dia_semana": escalas.DIAS[dia.weekday()],
            "escala": vigencia["nome"] if vigencia else None,
            "feriado": nome_feriado,
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
                           "status": o["status"]} for o in pendentes],
            "batidas": [{
                "id": b["id"], "nsr": b["nsr"],
                "hora": horario.para_local(b["timestamp_servidor"]).strftime("%H:%M"),
                "data_hora": horario.texto(b["timestamp_servidor"]),
                "status": b["status"], "obra": b["obra_codigo"], "origem": b["origem"],
                "fora_da_cerca": b["dentro_da_cerca"] is False,
                "distancia_metros": (float(b["distancia_metros"])
                                     if b["distancia_metros"] is not None else None),
                "motivo_analise": b["motivo_analise"], "tem_foto": b["foto_id"] is not None,
                "registrado_por": b["registrado_por"],
            } for b in do_dia],
        })

    contaveis = [d for d in dias if d["situacao"] not in ("FUTURO", "EM_ANDAMENTO",
                                                          "FORA_DO_CONTRATO")]
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
