# -*- coding: utf-8 -*-
"""
O QUANTO UM AJUSTE DE BATIDA É CRÍVEL (28/10/2026).

O dono: *"a quantidade de solicitações de ajuste ainda é muito alta (…) eu queria que o sistema
conseguisse sugerir entre ajustes mais críveis e outros que merecem mais atenção. Os críveis a gente
analisa e aprova mais rapidamente (…) um ponto pedido pelo administrativo da obra é para ser mais
confiável; uma única batida faltando de uma pessoa que tem as outras batidas na obra também. Agora
tem as outras coisas que têm que ter mais atenção: problemas com foto, de localização…"*

Cada pedido de ajuste ganha um NÍVEL e os MOTIVOS, em português, a favor e de atenção:
- CRÍVEL: nada pede atenção, e há um sinal forte a favor — pedido por outra pessoa (o responsável,
  o administrativo da obra, a gestão) ou uma batida só faltando num dia em que as outras foram
  batidas — e a hora pedida casa com a escala (quando há escala).
- ATENÇÃO: algum sinal de cuidado — dia sem batida nenhuma, foto que não conferiu ou batida em
  conferência no dia, tentativa fora da área da obra, obra pedida diferente da das batidas do dia,
  hora longe da escala, dia de descanso ou feriado, pedido muitos dias depois, muitos ajustes no mês,
  pedido negado há pouco.
- NORMAL: o resto.

É SUGESTÃO, não decisão: o nível só ordena e filtra a fila; quem aprova continua sendo uma pessoa,
e os motivos ficam à vista para ela conferir.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario

CRIVEL, NORMAL, ATENCAO = "CRIVEL", "NORMAL", "ATENCAO"
NIVEIS = (CRIVEL, NORMAL, ATENCAO)
PERTO_DA_ESCALA_MIN = 30          # a hora pedida casa com a escala
LONGE_DA_ESCALA_MIN = 90          # passou disto, é atenção
PEDIDO_TARDIO_DIAS = 7
MUITOS_AJUSTES_DIAS = 6           # dias com ajuste nos últimos 30
ORIGENS_DE_OUTRA_PESSOA = ("APARELHO", "RESPONSAVEL", "GESTAO")


def _minutos(hhmm: str) -> Optional[int]:
    try:
        h, m = str(hhmm)[:5].split(":")
        return int(h) * 60 + int(m)
    except (ValueError, AttributeError):
        return None


def classificar(a_favor: list[tuple[str, bool]], atencao: list[str], *, escala_ok: Optional[bool]) -> str:
    """PURA. `a_favor`: (frase, forte?). `escala_ok`: a hora casa com a escala (None = sem escala)."""
    if atencao:
        return ATENCAO
    if any(forte for _, forte in a_favor) and escala_ok is not False:
        return CRIVEL
    return NORMAL


def sinais(*, pedido_por: dict, origem: str, hora: str, obra: Optional[str], batidas: list[dict],
           pedidos_no_dia: int, previstas: list[str], situacao: str, feriado: Optional[str],
           fotos_ruins: list[str], em_conferencia: list[str], fora_da_area: list[str],
           dias_depois: int, dias_com_ajuste: int, negados_recentes: int) -> dict:
    """PURA. Os motivos de um pedido de ajuste e o nível."""
    a_favor: list[tuple[str, bool]] = []
    atencao: list[str] = []
    if origem in ORIGENS_DE_OUTRA_PESSOA and not pedido_por.get("propria") and pedido_por.get("nome"):
        a_favor.append((f"pedido por {pedido_por['nome']}{', ' + pedido_por['como'] if pedido_por.get('como') else ''}"
                        " — não pela própria pessoa", True))
    n = len(batidas)
    if n == 0:
        atencao.append("nenhuma batida no dia — o dia inteiro por ajuste")
    elif pedidos_no_dia <= 1:
        a_favor.append((f"só esta batida falta: as outras {n} do dia foram batidas", True))
    else:
        a_favor.append((f"{n} batida(s) no dia; {pedidos_no_dia} pedidas por ajuste", False))
    obras_do_dia = sorted({b["obra"] for b in batidas if b.get("obra")})
    if obra and obras_do_dia:
        if obra in obras_do_dia:
            a_favor.append((f"na mesma obra das batidas do dia ({obra})", False))
        else:
            atencao.append(f"obra pedida ({obra}) diferente da das batidas do dia ({', '.join(obras_do_dia)})")
    escala_ok: Optional[bool] = None
    m = _minutos(hora)
    prev = [x for x in (_minutos(p) for p in previstas) if x is not None]
    if feriado:
        atencao.append(f"feriado ({feriado}) — não havia batida prevista")
    elif situacao == "DESCANSO":
        atencao.append("dia de descanso pela escala — não havia batida prevista")
    elif prev and m is not None:
        perto = min(prev, key=lambda x: abs(x - m))
        dif = abs(perto - m)
        alvo = f"{perto // 60:02d}:{perto % 60:02d}"
        if dif <= PERTO_DA_ESCALA_MIN:
            escala_ok = True
            a_favor.append((f"no horário da escala ({alvo})", False))
        elif dif > LONGE_DA_ESCALA_MIN:
            escala_ok = False
            atencao.append(f"longe do horário da escala: pedido {hora}, a escala esperava {alvo}")
        else:
            escala_ok = False
    atencao += [f"foto de batida do dia: {f}" for f in fotos_ruins]
    atencao += [f"batida do dia em conferência: {c}" for c in em_conferencia]
    atencao += [f"tentou bater fora da área da obra: {f}" for f in fora_da_area]
    if dias_depois > PEDIDO_TARDIO_DIAS:
        atencao.append(f"pedido {dias_depois} dias depois do dia")
    if dias_com_ajuste >= MUITOS_AJUSTES_DIAS:
        atencao.append(f"{dias_com_ajuste} dias com ajuste nos últimos 30 dias")
    if negados_recentes:
        atencao.append(f"{negados_recentes} pedido(s) negado(s) nos últimos 30 dias")
    nivel = classificar(a_favor, atencao, escala_ok=escala_ok)
    return {"nivel": nivel, "a_favor": [t for t, _ in a_favor], "atencao": atencao}


def avaliar(conn: Connection, pedidos: list[dict]) -> dict[int, dict]:
    """Os pedidos de AJUSTE_BATIDA (os itens das Validações), de uma vez: {id: sinais}."""
    from . import cadastros, espelho, ocorrencias
    ajustes = [p for p in pedidos if p.get("tipo") == "AJUSTE_BATIDA" and p.get("colaborador_id") and p.get("data")]
    if not ajustes:
        return {}
    dias = [dt.date.fromisoformat(str(p["data"])[:10]) for p in ajustes]
    inicio, fim = min(dias), max(dias)
    if (fim - inicio).days + 1 > espelho.MAX_DIAS:
        inicio = fim - dt.timedelta(days=espelho.MAX_DIAS - 1)
    ids = sorted({int(p["colaborador_id"]) for p in ajustes})
    pessoas = [p for p in (cadastros.colaborador_por_id(conn, i) for i in ids) if p]
    lote = espelho.Lote(conn, pessoas, inicio, fim) if pessoas else None
    hoje = horario.hoje()
    dia_de: dict[tuple, dict] = {}
    for p in pessoas:
        cid = int(p["id"])
        try:
            esp = espelho.montar(conn, cid, inicio, fim, lote=lote)
        except Exception:  # noqa: BLE001 — sem espelho, o pedido fica sem nível (NORMAL)
            continue
        for d in esp["dias"]:
            dia_de[(cid, d["data"])] = d
    # foto e conferência por pessoa e dia
    fotos = {}
    for r in db.todos(conn, """
        SELECT m.colaborador_id, m.data_referencia, r.resultado, to_char(m.timestamp_servidor AT TIME ZONE 'America/Fortaleza', 'HH24:MI') AS hora
          FROM ponto.conferencias_rosto r JOIN ponto.marcacoes m ON m.id = r.marcacao_id
         WHERE m.colaborador_id = ANY(:ids) AND m.data_referencia BETWEEN :i AND :f
           AND r.resultado IN ('OUTRA_PESSOA', 'SEM_ROSTO') AND m.status <> 'VALIDA'""", ids=ids, i=inicio, f=fim):
        txt = f"{r['hora']} — {'o rosto não confere' if r['resultado'] == 'OUTRA_PESSOA' else 'sem rosto na foto'}"
        fotos.setdefault((int(r["colaborador_id"]), r["data_referencia"].isoformat()), []).append(txt)
    # tentativas fora da área (as recusas guardam o CPF informado)
    cpf_de = {int(p["id"]): "".join(ch for ch in (p.get("cpf") or "") if ch.isdigit()) for p in pessoas}
    fora = {}
    if any(cpf_de.values()):
        for r in db.todos(conn, """
            SELECT regexp_replace(cpf_informado, '\\D', '', 'g') AS cpf, (criado_em AT TIME ZONE 'America/Fortaleza')::date AS dia,
                   to_char(criado_em AT TIME ZONE 'America/Fortaleza', 'HH24:MI') AS hora, motivo
              FROM ponto.recusas
             WHERE regexp_replace(coalesce(cpf_informado, ''), '\\D', '', 'g') = ANY(:cpfs)
               AND motivo ILIKE '%fora da área%'
               AND (criado_em AT TIME ZONE 'America/Fortaleza')::date BETWEEN :i AND :f
             ORDER BY criado_em""", cpfs=[c for c in cpf_de.values() if c], i=inicio, f=fim):
            fora.setdefault((r["cpf"], r["dia"].isoformat()), []).append(r["hora"])
    # o histórico de 30 dias de cada um: dias com ajuste e pedidos negados
    hist = {int(r["colaborador_id"]): r for r in db.todos(conn, """
        SELECT colaborador_id,
               count(DISTINCT data_inicio) FILTER (WHERE status NOT IN ('CANCELADA')) AS dias,
               count(*) FILTER (WHERE status = 'NEGADA') AS negados
          FROM ponto.ocorrencias
         WHERE tipo = 'AJUSTE_BATIDA' AND colaborador_id = ANY(:ids) AND data_inicio >= :d
         GROUP BY colaborador_id""", ids=ids, d=hoje - dt.timedelta(days=30))}
    pedidos_no_dia: dict[tuple, int] = {}
    for p in ajustes:
        k = (int(p["colaborador_id"]), str(p["data"])[:10])
        pedidos_no_dia[k] = pedidos_no_dia.get(k, 0) + 1
    saida = {}
    for p in ajustes:
        cid, dia = int(p["colaborador_id"]), str(p["data"])[:10]
        d = dia_de.get((cid, dia))
        if d is None:
            continue
        hora = str(p.get("quando") or "")[-5:]
        criado = horario.ler_iso(p["desde"]) if p.get("desde") else None
        dias_depois = (horario.para_local(criado).date() - dt.date.fromisoformat(dia)).days if criado else 0
        h = hist.get(cid) or {}
        em_conf = [f"{b['hora']} — {b.get('motivo_analise') or 'em conferência'}" for b in d["batidas"]
                   if b.get("status") == "EM_ANALISE"]
        fora_txt = [f"às {x}" for x in fora.get((cpf_de.get(cid, ""), dia), [])]
        saida[int(p["id"])] = sinais(
            pedido_por=p.get("pedido_por") or ocorrencias.pedido_por(p.get("origem"), p.get("solicitado_por"), p.get("pessoa")),
            origem=p.get("origem") or "", hora=hora, obra=p.get("obra"),
            batidas=d["batidas"], pedidos_no_dia=pedidos_no_dia.get((cid, dia), 1),
            previstas=[x["hora"] for x in d.get("previstas") or []], situacao=d.get("situacao") or "",
            feriado=d.get("feriado"), fotos_ruins=fotos.get((cid, dia), []), em_conferencia=em_conf,
            fora_da_area=fora_txt, dias_depois=dias_depois, dias_com_ajuste=int(h.get("dias") or 0),
            negados_recentes=int(h.get("negados") or 0))
    return saida
