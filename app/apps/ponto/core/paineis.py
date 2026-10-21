# -*- coding: utf-8 -*-
"""
O PAINEL DA PESSOA e O LOCAL DA BATIDA — as janelas que abrem de qualquer tela.

Pedido do dono, 21/10/2026: *"eu quero clicar naquela pessoa e abrir um modal que
me mostre os pontos que ela coletou num determinado dia (…) poder voltar no dia
anterior (…) visualizar o cadastro dela com todas as informações, o que ela fez,
os pontos que ela bateu, as solicitações que ela requereu, se já foram atendidas
(…) e a partir daí já atender ou não atender"*, e *"clicar e ver a geolocalização,
no modal, avisando qual é a obra em que ela está"*. E a regra de sempre do ERP:
*"se uma pessoa bate ponto da outra, haver aquela relação"* — por isso o dia de
quem responde por um ponto da obra traz também quem bateu no aparelho dele.

Tudo de uma pessoa só: o custo é o de um espelho do mês, sem conta pesada.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import cadastros, espelho, geo, ocorrencias, validacao
from .ocorrencias import Quem


def _dia(valor) -> dt.date:
    if not valor:
        return horario.hoje()
    try:
        return dt.date.fromisoformat(str(valor)[:10])
    except ValueError as e:
        raise ErroDeValidacao("data: use AAAA-MM-DD", campo="data") from e


def decidir_em(p: dict) -> Optional[str]:
    """PURA. O endereço que decide o pedido na etapa em que ele está."""
    if not p.get("etapa_atual"):
        return None
    return (f"/erp/api/ponto/afastamentos/{p['id']}/etapa-dp" if p["etapa_atual"] == "DP"
            else f"/erp/api/ponto/ocorrencias/{p['id']}/etapa-supervisor")


def _no_aparelho_dela(conn: Connection, quem: Quem, colaborador_id: int, dia: dt.date) -> list[dict]:
    """Quem bateu no aparelho de que a pessoa é responsável (ponto da obra ou de equipe), no dia."""
    linhas = db.todos(conn, """
        SELECT m.id, m.colaborador_id, c.nome, m.timestamp_servidor, m.status, m.obra_id, o.codigo AS obra,
               d.descricao AS aparelho
          FROM ponto.marcacoes m
          JOIN ponto.dispositivos d ON d.id = m.dispositivo_id
          JOIN public.colaboradores c ON c.id = m.colaborador_id
          JOIN public.obras o ON o.id = m.obra_id
         WHERE d.colaborador_id = :c AND d.perfil IN ('COMPARTILHADO', 'LISTA') AND m.data_referencia = :d
           AND m.status <> 'REJEITADA'
         ORDER BY c.nome, m.timestamp_servidor""", c=colaborador_id, d=dia)
    pessoas: dict[int, dict] = {}
    for l in linhas:
        if not quem.alcanca_obra(l["obra_id"]):
            continue
        p = pessoas.setdefault(int(l["colaborador_id"]), {"colaborador_id": int(l["colaborador_id"]), "nome": l["nome"],
                                                          "batidas": [], "aparelho": l["aparelho"]})
        p["batidas"].append({"id": l["id"], "hora": horario.para_local(l["timestamp_servidor"]).strftime("%H:%M"),
                             "status": l["status"], "obra": l["obra"]})
    return list(pessoas.values())


def _em_conferencia(conn: Connection, quem: Quem, colaborador_id: int) -> list[dict]:
    """As batidas da pessoa esperando decisão, com o endereço da decisão."""
    regra = validacao.quem_valida_batida(conn)
    saida = []
    for m in db.todos(conn, """
        SELECT m.id, m.timestamp_servidor, m.data_referencia, m.motivo_analise, m.obra_id, o.codigo AS obra
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.colaborador_id = :c AND m.status = 'EM_ANALISE'
         ORDER BY m.timestamp_servidor DESC LIMIT 60""", c=colaborador_id):
        pode = (quem.dp if regra == validacao.DP else quem.supervisor) and quem.alcanca_obra(m["obra_id"])
        saida.append({"id": m["id"], "quando": f"{horario.para_local(m['timestamp_servidor']):%d/%m/%Y às %H:%M}",
                      "data": m["data_referencia"].isoformat(), "obra": m["obra"], "motivo": m["motivo_analise"],
                      "pode_decidir": bool(pode),
                      "decidir_em": (f"/erp/api/ponto/marcacoes/{m['id']}/decidir-dp" if regra == validacao.DP
                                     else f"/erp/api/ponto/marcacoes/{m['id']}/decidir")})
    return saida


def da_pessoa(conn: Connection, quem: Quem, colaborador_id: int, data=None, competencia=None) -> dict:
    """O dia (com as batidas e o lugar de cada uma), quem bateu no aparelho dela, o
    mês resumido dia a dia, os pedidos e as batidas em conferência. A ficha
    (cadastro, aparelhos, etiquetas) vem à parte — e é ela que confere o alcance."""
    dia = _dia(data)
    d = espelho.montar(conn, colaborador_id, dia, dia)["dias"][0]
    mes = espelho.do_mes(conn, colaborador_id, competencia or dia.strftime("%Y-%m"))
    pedidos = ocorrencias.listar(conn, quem, colaborador_id=colaborador_id, limite=60)
    for p in pedidos:
        p["decidir_em"] = decidir_em(p) if p.get("pode_decidir") else None
    return {
        "data": dia.isoformat(), "dia": d,
        "no_aparelho_dela": _no_aparelho_dela(conn, quem, colaborador_id, dia),
        "mes": {"competencia": mes["competencia"], "competencia_fechada": mes["competencia_fechada"],
                "resumo": mes["resumo"],
                "dias": [{"data": x["data"], "dia_semana": x["dia_semana"], "situacao": x["situacao"],
                          "batidas": [b["hora"] for b in x["batidas"]], "faltando": [f["rotulo"] for f in x["faltando"]],
                          "feriado": x["feriado"], "ocorrencia": (x.get("ocorrencia") or {}).get("rotulo"),
                          "pendentes": len(x.get("pendentes") or [])} for x in mes["dias"]]},
        "pedidos": pedidos,
        "em_conferencia": _em_conferencia(conn, quem, colaborador_id),
    }


def local_da_batida(conn: Connection, quem: Quem, marcacao_id: int) -> dict:
    """Onde o aparelho estava na batida, a obra da batida (com a cerca) e a distância.
    Fora do alcance de quem pergunta: "não encontrada"."""
    m = db.um(conn, """
        SELECT m.id, m.colaborador_id, m.obra_id, m.timestamp_servidor, m.latitude, m.longitude,
               m.distancia_metros, m.dentro_da_cerca, m.status, m.origem, m.registrado_por, c.nome
          FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
         WHERE m.id = :id""", id=marcacao_id)
    if not m or not quem.alcanca_obra(m["obra_id"]) or not ocorrencias.pessoa_no_alcance(conn, quem, m["colaborador_id"]):
        raise NaoEncontrado("batida não encontrada")
    o = cadastros.obra_por_id(conn, int(m["obra_id"]))
    obra = cadastros.obra_para_json(o) if o else None
    lat = float(m["latitude"]) if m["latitude"] is not None else None
    lon = float(m["longitude"]) if m["longitude"] is not None else None
    dist = (float(m["distancia_metros"]) if m["distancia_metros"] is not None
            else geo.distancia_metros(lat, lon, obra["latitude"], obra["longitude"])
            if obra and None not in (lat, lon, obra["latitude"], obra["longitude"]) else None)
    return {"id": m["id"], "nome": m["nome"], "colaborador_id": m["colaborador_id"],
            "quando": f"{horario.para_local(m['timestamp_servidor']):%d/%m/%Y às %H:%M}",
            "latitude": lat, "longitude": lon, "obra": obra, "distancia_metros": dist,
            "dentro_da_cerca": m["dentro_da_cerca"], "status": m["status"], "origem": m["origem"],
            "registrado_por": m["registrado_por"]}
