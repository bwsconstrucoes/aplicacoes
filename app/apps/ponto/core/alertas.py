# -*- coding: utf-8 -*-
"""
Alertas do ponto: o que a gestão precisa ver sem ter de procurar.

TUDO AQUI É REGRA ESCRITA E TESTADA, nunca palpite. Pedido do dono: "usar o
sistema de inteligência para gerar alerta que facilite a gestão". A
inteligência que entra é de dois tipos:

  · regra da CLT e do processo, sobre o cálculo do dia (falta, intervalo curto,
    extra acima de 2 h, interjornada, pedido parado…);
  · PADRÃO no histórico, que olho humano não pega numa lista de 400 pessoas:
    a pessoa que atrasa toda segunda, faltas seguidas, quem bate fora da cerca
    dia após dia, a mesma pessoa em dois lugares longe em poucos minutos.

A IA de texto NÃO decide alerta e não calcula número: um número errado com cara
de certo é pior que nenhum (CLAUDE.md). Ela entra na leitura do atestado.

Cada alerta tem uma `chave` (ex.: FALTA:123:2026-10-02): gerar de novo não
duplica. Alerta de dia que deixou de ser verdade (a falta virou atestado
aprovado) é RESOLVIDO sozinho na próxima rodada.
"""
from __future__ import annotations

import datetime as dt
import logging
from collections import Counter
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from . import banco, cadastros, escalas, espelho, geo

logger = logging.getLogger("ponto.alertas")

GRAVIDADE = {
    "FALTA": "ATENCAO", "BATIDA_FALTANDO": "ATENCAO", "ATRASO": "INFO",
    "SAIDA_ANTECIPADA": "INFO", "EXTRA_ACIMA_DE_2H": "ATENCAO", "INTERVALO_CURTO": "ATENCAO",
    "INTERJORNADA_CURTA": "ATENCAO", "TRABALHO_EM_DESCANSO": "INFO", "TRABALHO_EM_FERIADO": "INFO",
    "BATIDA_EM_DIA_ABONADO": "ATENCAO", "SEM_ESCALA": "INFO",
    "FALTAS_SEGUIDAS": "URGENTE", "ATRASO_RECORRENTE": "ATENCAO",
    "FORA_DA_CERCA_REPETIDO": "ATENCAO", "DOIS_LUGARES": "URGENTE",
    "PEDIDO_PARADO": "ATENCAO", "BATIDA_EM_ANALISE_PARADA": "ATENCAO",
    "APARELHO_PENDENTE": "INFO", "BANCO_VENCENDO": "ATENCAO", "BANCO_NEGATIVO": "ATENCAO",
}
ROTULO = {
    "FALTA": "Falta sem justificativa", "BATIDA_FALTANDO": "Batida faltando",
    "ATRASO": "Atraso", "SAIDA_ANTECIPADA": "Saída antecipada",
    "EXTRA_ACIMA_DE_2H": "Extra acima de 2 h", "INTERVALO_CURTO": "Intervalo curto",
    "INTERJORNADA_CURTA": "Menos de 11 h de descanso", "TRABALHO_EM_DESCANSO": "Trabalho em dia de descanso",
    "TRABALHO_EM_FERIADO": "Trabalho em feriado", "BATIDA_EM_DIA_ABONADO": "Bateu ponto em dia abonado",
    "SEM_ESCALA": "Sem escala cadastrada", "FALTAS_SEGUIDAS": "Faltas seguidas",
    "ATRASO_RECORRENTE": "Atraso que se repete", "FORA_DA_CERCA_REPETIDO": "Fora da cerca várias vezes",
    "DOIS_LUGARES": "Dois lugares ao mesmo tempo", "PEDIDO_PARADO": "Pedido esperando decisão",
    "BATIDA_EM_ANALISE_PARADA": "Batida em análise esperando", "APARELHO_PENDENTE": "Aparelho esperando aprovação",
    "BANCO_VENCENDO": "Banco de horas vencendo", "BANCO_NEGATIVO": "Banco de horas negativo",
}
# Os que nascem do cálculo de UM dia — podem ser resolvidos sozinhos.
DO_DIA = {"FALTA", "BATIDA_FALTANDO", "ATRASO", "SAIDA_ANTECIPADA", "EXTRA_ACIMA_DE_2H",
          "INTERVALO_CURTO", "INTERJORNADA_CURTA", "TRABALHO_EM_DESCANSO", "TRABALHO_EM_FERIADO",
          "BATIDA_EM_DIA_ABONADO"}

JANELA_DIAS = 7            # alertas de dia: só a última semana (sem enxurrada de histórico)
JANELA_PADRAO = 28         # padrões: as últimas 4 semanas
FALTAS_SEGUIDAS_MIN = 3
ATRASOS_MESMO_DIA_MIN = 3
FORA_DA_CERCA_MIN = 3
DOIS_LUGARES_KM = 2.0
DOIS_LUGARES_MIN = 30
PARADO_DIAS = 2
BANCO_AVISO_DIAS = 30
BANCO_NEGATIVO_MIN = -600  # −10 h


def _hhmm(minutos: int) -> str:
    sinal = "-" if minutos < 0 else ""
    m = abs(int(minutos))
    return f"{sinal}{m // 60}h{m % 60:02d}"


def _mensagem(codigo: str, nome: str, dia: Optional[str], d: dict | None = None) -> str:
    quando = f" em {dt.date.fromisoformat(dia):%d/%m}" if dia else ""
    extra = ""
    if d:
        if codigo == "ATRASO":
            extra = f" ({d['atraso']} min)"
        elif codigo == "SAIDA_ANTECIPADA":
            extra = f" ({d['saida_antecipada']} min)"
        elif codigo == "EXTRA_ACIMA_DE_2H":
            extra = f" ({_hhmm(d['extra'])} de extra)"
        elif codigo == "INTERVALO_CURTO":
            extra = f" (intervalo de {d.get('intervalo') or 0} min)"
    return f"{nome}: {ROTULO[codigo].lower()}{quando}{extra}"


def _registrar(conn: Connection, vistos: set, *, chave: str, codigo: str, mensagem: str,
               colaborador_id=None, obra_id=None, data=None) -> None:
    vistos.add(chave)
    db.executar(conn, """
        INSERT INTO ponto.alertas (chave, codigo, gravidade, colaborador_id, obra_id,
                                   data_referencia, mensagem)
        VALUES (:k, :c, :g, :p, :o, :d, :m)
        ON CONFLICT (chave) DO UPDATE SET mensagem = :m
    """, k=chave, c=codigo, g=GRAVIDADE[codigo], p=colaborador_id, o=obra_id, d=data, m=mensagem)


def gerar(conn: Connection, *, ate: Optional[dt.date] = None,
          colaborador_ids: Optional[list[int]] = None) -> dict:
    """Uma rodada completa. `ate` padrão = ontem (o dia de hoje ainda não fechou)."""
    ate = ate or (horario.hoje() - dt.timedelta(days=1))
    inicio_dia = ate - dt.timedelta(days=JANELA_DIAS - 1)
    inicio_padrao = ate - dt.timedelta(days=JANELA_PADRAO - 1)
    pessoas = cadastros.listar_colaboradores(conn, so_ativos=True)
    if colaborador_ids is not None:
        pessoas = [p for p in pessoas if p["id"] in set(colaborador_ids)]
    vistos: set[str] = set()
    contagem: Counter = Counter()

    for p in pessoas:
        esp = espelho.montar(conn, p["id"], inicio_padrao, ate, hoje=ate + dt.timedelta(days=1))
        dias = esp["dias"]
        if any(d["situacao"] == "SEM_ESCALA" for d in dias[-JANELA_DIAS:]):
            _registrar(conn, vistos, chave=f"SEM_ESCALA:{p['id']}", codigo="SEM_ESCALA",
                       mensagem=f"{p['nome']}: sem escala cadastrada — o espelho não julga falta "
                                "nem atraso até alguém cadastrar", colaborador_id=p["id"],
                       obra_id=p.get("obra_id"))
            contagem["SEM_ESCALA"] += 1
        for d in dias:
            if d["data"] < inicio_dia.isoformat():
                continue
            for codigo in d["alertas"]:
                if codigo not in DO_DIA:
                    continue
                _registrar(conn, vistos, chave=f"{codigo}:{p['id']}:{d['data']}", codigo=codigo,
                           mensagem=_mensagem(codigo, p["nome"], d["data"], d),
                           colaborador_id=p["id"], obra_id=p.get("obra_id"), data=d["data"])
                contagem[codigo] += 1

        # --- padrões -----------------------------------------------------
        seguidas, maior, fim_maior = 0, 0, None
        for d in dias:
            if d["falta"]:
                seguidas += 1
                if seguidas > maior:
                    maior, fim_maior = seguidas, d["data"]
            elif d["previsto"]:
                seguidas = 0
        if maior >= FALTAS_SEGUIDAS_MIN:
            _registrar(conn, vistos, chave=f"FALTAS_SEGUIDAS:{p['id']}:{fim_maior}",
                       codigo="FALTAS_SEGUIDAS",
                       mensagem=f"{p['nome']}: {maior} faltas seguidas até {dt.date.fromisoformat(fim_maior):%d/%m} "
                                "— confirmar se a pessoa ainda trabalha na obra",
                       colaborador_id=p["id"], obra_id=p.get("obra_id"), data=fim_maior)
            contagem["FALTAS_SEGUIDAS"] += 1
        atrasos = Counter(dt.date.fromisoformat(d["data"]).weekday()
                          for d in dias if "ATRASO" in d["alertas"])
        for dia_semana, n in atrasos.items():
            if n >= ATRASOS_MESMO_DIA_MIN:
                _registrar(conn, vistos, chave=f"ATRASO_RECORRENTE:{p['id']}:{dia_semana}:{ate:%Y-%W}",
                           codigo="ATRASO_RECORRENTE",
                           mensagem=f"{p['nome']}: atrasou em {n} {escalas.DIAS[dia_semana]}s "
                                    "das últimas 4 semanas",
                           colaborador_id=p["id"], obra_id=p.get("obra_id"), data=ate)
                contagem["ATRASO_RECORRENTE"] += 1
        fora = sorted({d["data"] for d in dias for b in d["batidas"] if b["fora_da_cerca"]})
        if len(fora) >= FORA_DA_CERCA_MIN:
            _registrar(conn, vistos, chave=f"FORA_DA_CERCA_REPETIDO:{p['id']}:{ate:%Y-%W}",
                       codigo="FORA_DA_CERCA_REPETIDO",
                       mensagem=f"{p['nome']}: bateu fora da cerca em {len(fora)} dias das "
                                "últimas 4 semanas — conferir a cerca da obra ou o aparelho",
                       colaborador_id=p["id"], obra_id=p.get("obra_id"), data=ate)
            contagem["FORA_DA_CERCA_REPETIDO"] += 1

        if p.get("regime_banco") in ("BANCO_6_MESES", "BANCO_12_MESES"):
            try:
                b = banco.saldo(conn, p["id"], ate=ate)
            except Exception:  # noqa: BLE001 — banco mal configurado não derruba a rodada
                logger.warning("Ponto: banco de %s não calculou", p["id"], exc_info=True)
                b = None
            if b:
                for v in b["vencimentos"]:
                    vence = dt.date.fromisoformat(v["vence_em"])
                    if (vence - ate).days <= BANCO_AVISO_DIAS:
                        _registrar(conn, vistos, chave=f"BANCO_VENCENDO:{p['id']}:{v['mes']}",
                                   codigo="BANCO_VENCENDO",
                                   mensagem=f"{p['nome']}: {_hhmm(v['minutos'])} do banco de "
                                            f"{v['mes']} vencem em {vence:%d/%m/%Y}",
                                   colaborador_id=p["id"], obra_id=p.get("obra_id"), data=vence)
                        contagem["BANCO_VENCENDO"] += 1
                if b["saldo"] <= BANCO_NEGATIVO_MIN:
                    _registrar(conn, vistos, chave=f"BANCO_NEGATIVO:{p['id']}:{ate:%Y-%m}",
                               codigo="BANCO_NEGATIVO",
                               mensagem=f"{p['nome']}: banco de horas em {_hhmm(b['saldo'])}",
                               colaborador_id=p["id"], obra_id=p.get("obra_id"), data=ate)
                    contagem["BANCO_NEGATIVO"] += 1

    contagem.update(_dois_lugares(conn, vistos, inicio_dia, ate))
    contagem.update(_parados(conn, vistos))

    # O que era alerta de dia na janela e não apareceu de novo deixou de ser verdade.
    abertos = db.todos(conn, """
        SELECT id, chave, codigo FROM ponto.alertas
         WHERE situacao = 'ABERTO' AND data_referencia BETWEEN :i AND :f
    """, i=inicio_dia, f=ate)
    resolvidos = 0
    for a in abertos:
        if a["codigo"] in DO_DIA and a["chave"] not in vistos:
            if colaborador_ids is not None and int(a["chave"].split(":")[1]) not in colaborador_ids:
                continue
            db.executar(conn, "UPDATE ponto.alertas SET situacao = 'RESOLVIDO', tratado_por = "
                              "'o próprio sistema (deixou de valer)', tratado_em = now() WHERE id = :id",
                        id=a["id"])
            resolvidos += 1
    logger.info("Ponto: rodada de alertas até %s — %d pessoas, %d alertas, %d resolvidos sozinhos",
                ate, len(pessoas), sum(contagem.values()), resolvidos)
    return {"ate": ate.isoformat(), "pessoas": len(pessoas), "alertas": dict(contagem),
            "resolvidos_sozinhos": resolvidos}


def _dois_lugares(conn: Connection, vistos: set, inicio: dt.date, fim: dt.date) -> Counter:
    linhas = db.todos(conn, """
        SELECT m.id, m.colaborador_id, m.obra_id, m.timestamp_servidor, m.latitude, m.longitude,
               m.data_referencia, c.nome
          FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
         WHERE m.data_referencia BETWEEN :i AND :f AND m.latitude IS NOT NULL
           AND m.status <> 'REJEITADA'
         ORDER BY m.colaborador_id, m.timestamp_servidor
    """, i=inicio, f=fim)
    contagem: Counter = Counter()
    for a, b in zip(linhas, linhas[1:]):
        if a["colaborador_id"] != b["colaborador_id"]:
            continue
        minutos = (b["timestamp_servidor"] - a["timestamp_servidor"]).total_seconds() / 60
        if minutos > DOIS_LUGARES_MIN:
            continue
        km = geo.distancia_metros(a["latitude"], a["longitude"], b["latitude"], b["longitude"]) / 1000
        if km >= DOIS_LUGARES_KM:
            _registrar(conn, vistos, chave=f"DOIS_LUGARES:{a['id']}:{b['id']}", codigo="DOIS_LUGARES",
                       mensagem=f"{a['nome']}: duas batidas a {km:.1f} km uma da outra em "
                                f"{minutos:.0f} min ({horario.para_local(a['timestamp_servidor']):%d/%m %H:%M}) "
                                "— possível batida por outra pessoa",
                       colaborador_id=a["colaborador_id"], obra_id=b["obra_id"],
                       data=b["data_referencia"])
            contagem["DOIS_LUGARES"] += 1
    return contagem


def _parados(conn: Connection, vistos: set) -> Counter:
    contagem: Counter = Counter()
    limite = horario.agora() - dt.timedelta(days=PARADO_DIAS)
    from .espelho import ROTULO_TIPO
    for o in db.todos(conn, """
        SELECT oc.id, oc.tipo, oc.colaborador_id, oc.criado_em, c.nome, c.obra_id
          FROM ponto.ocorrencias oc JOIN public.colaboradores c ON c.id = oc.colaborador_id
         WHERE oc.status IN ('AGUARDANDO_SUPERVISOR', 'AGUARDANDO_DP') AND oc.criado_em < :l
    """, l=limite):
        dias = (horario.agora() - o["criado_em"]).days
        _registrar(conn, vistos, chave=f"PEDIDO_PARADO:{o['id']}", codigo="PEDIDO_PARADO",
                   mensagem=f"{o['nome']}: pedido de {ROTULO_TIPO[o['tipo']].lower()} (nº {o['id']}) "
                            f"esperando decisão há {dias} dias",
                   colaborador_id=o["colaborador_id"], obra_id=o["obra_id"])
        contagem["PEDIDO_PARADO"] += 1
    for m in db.todos(conn, """
        SELECT m.id, m.nsr, m.colaborador_id, m.obra_id, m.data_referencia, c.nome
          FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
         WHERE m.status = 'EM_ANALISE' AND m.criado_em < :l
    """, l=limite):
        _registrar(conn, vistos, chave=f"BATIDA_EM_ANALISE_PARADA:{m['id']}",
                   codigo="BATIDA_EM_ANALISE_PARADA",
                   mensagem=f"{m['nome']}: batida de {m['data_referencia']:%d/%m} (NSR {m['nsr']}) "
                            "em análise há mais de 2 dias",
                   colaborador_id=m["colaborador_id"], obra_id=m["obra_id"],
                   data=m["data_referencia"])
        contagem["BATIDA_EM_ANALISE_PARADA"] += 1
    for d in db.todos(conn, "SELECT id, descricao FROM ponto.dispositivos WHERE status = 'PENDENTE'"):
        _registrar(conn, vistos, chave=f"APARELHO_PENDENTE:{d['id']}", codigo="APARELHO_PENDENTE",
                   mensagem=f"Aparelho “{d['descricao'] or 'sem nome'}” esperando aprovação")
        contagem["APARELHO_PENDENTE"] += 1
    return contagem


# ---------------------------------------------------------------------------
# Leitura e tratamento
# ---------------------------------------------------------------------------
def listar(conn: Connection, *, obras: Optional[list[int]] = None, situacao: str = "ABERTO",
           gravidade: Optional[str] = None, limite: int = 500) -> list[dict]:
    sql = """SELECT a.*, c.nome AS colaborador, o.codigo AS obra
               FROM ponto.alertas a
               LEFT JOIN public.colaboradores c ON c.id = a.colaborador_id
               LEFT JOIN public.obras o ON o.id = a.obra_id WHERE 1 = 1"""
    params: dict = {"lim": max(1, min(int(limite), 2000))}
    if situacao:
        sql += " AND a.situacao = :s"
        params["s"] = situacao
    if gravidade:
        sql += " AND a.gravidade = :g"
        params["g"] = gravidade
    if obras is not None:
        sql += " AND a.obra_id = ANY(:obras)"
        params["obras"] = list(obras) or [0]
    sql += """ ORDER BY CASE a.gravidade WHEN 'URGENTE' THEN 0 WHEN 'ATENCAO' THEN 1 ELSE 2 END,
                        a.data_referencia DESC NULLS LAST, a.id DESC LIMIT :lim"""
    saida = []
    for a in db.todos(conn, sql, **params):
        a["rotulo"] = ROTULO.get(a["codigo"], a["codigo"])
        a["data_referencia"] = a["data_referencia"].isoformat() if a["data_referencia"] else None
        a["criado_em"] = horario.texto(a["criado_em"])
        a["tratado_em"] = horario.texto(a["tratado_em"])
        saida.append(a)
    return saida


def tratar(conn: Connection, alerta_id: int, *, situacao: str, nota: str, por: str,
           obras: Optional[list[int]] = None) -> dict:
    from ..erros import ErroDeValidacao, NaoEncontrado
    situacao = str(situacao or "").upper()
    if situacao not in ("RESOLVIDO", "DISPENSADO", "ABERTO"):
        raise ErroDeValidacao("situação inválida", campo="situacao")
    a = db.um(conn, "SELECT * FROM ponto.alertas WHERE id = :id", id=alerta_id)
    if not a or (obras is not None and a["obra_id"] not in obras):
        raise NaoEncontrado("alerta não encontrado")
    if situacao == "DISPENSADO" and len((nota or "").strip()) < 5:
        raise ErroDeValidacao("diga por que o alerta não precisa de ação", campo="nota")
    db.executar(conn, "UPDATE ponto.alertas SET situacao = :s, nota = :n, tratado_por = :p, "
                      "tratado_em = now() WHERE id = :id",
                s=situacao, n=(nota or "").strip()[:500] or None, p=por[:120], id=alerta_id)
    return db.um(conn, "SELECT * FROM ponto.alertas WHERE id = :id", id=alerta_id)


def resumo_do_dia(conn: Connection, dia: Optional[dt.date] = None,
                  obras: Optional[list[int]] = None) -> str:
    """O texto que vai por WhatsApp. Números contados aqui, não escritos por IA."""
    dia = dia or horario.hoje()
    params: dict = {"d": dia}
    filtro = ""
    if obras is not None:
        filtro = " AND c.obra_id = ANY(:obras)"
        params["obras"] = list(obras) or [0]
    bateram = db.um(conn, f"""
        SELECT count(DISTINCT m.colaborador_id) AS n FROM ponto.marcacoes m
          JOIN public.colaboradores c ON c.id = m.colaborador_id
         WHERE m.data_referencia = :d AND m.status <> 'REJEITADA' {filtro}""", **params)["n"]
    em_analise = db.um(conn, f"""
        SELECT count(*) AS n FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
         WHERE m.status = 'EM_ANALISE' {filtro}""", **{k: v for k, v in params.items() if k != 'd'})["n"]
    pedidos = db.um(conn, f"""
        SELECT count(*) AS n FROM ponto.ocorrencias oc JOIN public.colaboradores c ON c.id = oc.colaborador_id
         WHERE oc.status IN ('AGUARDANDO_SUPERVISOR', 'AGUARDANDO_DP') {filtro}""",
                   **{k: v for k, v in params.items() if k != 'd'})["n"]
    abertos = listar(conn, obras=obras, situacao="ABERTO", limite=2000)
    por_codigo = Counter(a["codigo"] for a in abertos)
    urgentes = [a for a in abertos if a["gravidade"] == "URGENTE"][:5]
    linhas = [f"📋 Ponto — {dia:%d/%m/%Y}",
              f"• {bateram} pessoa(s) bateram ponto hoje",
              f"• {em_analise} batida(s) em análise esperando decisão",
              f"• {pedidos} pedido(s) esperando aprovação"]
    if por_codigo:
        linhas.append("Alertas abertos:")
        for codigo, n in por_codigo.most_common(8):
            linhas.append(f"  – {ROTULO.get(codigo, codigo)}: {n}")
    if urgentes:
        linhas.append("Urgentes:")
        linhas.extend(f"  ⚠️ {a['mensagem']}" for a in urgentes)
    linhas.append("Detalhes no ERP › Ponto.")
    return "\n".join(linhas)
