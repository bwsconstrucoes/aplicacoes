# -*- coding: utf-8 -*-
"""
A NEGATIVA AVISADA A QUEM PEDIU (28/10/2026, migração 014).

O dono: *"quando eu nego uma validação, o ideal era que a pessoa recebesse, quem solicitou
recebesse, a negativa, e a gente ia colocar algum motivo. Para a pessoa saber que foi negado,
ou então entender que foi pedido errado e precisa ser pedido novamente."* E, sobre o WhatsApp:
*"o padrão é para enviar, mas (…) tem uma opção de não enviar"*.

Quando um pedido é negado (o motivo já era obrigatório):
- **A. a tela inicial do aplicativo** mostra o aviso — para a pessoa e, se outra pessoa pediu
  por ela (o responsável, o administrativo, quem estava no ponto da obra), para quem pediu.
  Some quando é tocado.
- **B. o Meu mês e o espelho** mostram o negado no dia, com o motivo, enquanto aquele horário
  continuar em aberto (pediu de novo, ou a batida apareceu: o negado sai do dia).
- **C. quem pediu** é guardado no pedido (`solicitado_colaborador_id`) a partir da 014.
- **D. o WhatsApp**, para os mesmos dois, PADRÃO LIGADO; quem nega pode desmarcar. Vai pela fila
  do ponto (`envios`, tipo NEGATIVA), no horário de atendimento (6h–22h), nunca dentro da
  requisição. Os negados do MESMO dia da MESMA pessoa viram UMA mensagem (negar as 4 batidas
  de uma vez não manda 4 WhatsApps): a mensagem espera um minuto antes de sair, e cada negado
  que chega nesse minuto entra nela.

Sem a 014 aplicada, nada disto liga — e a negativa continua funcionando como antes.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario

logger = logging.getLogger("ponto.negativas")

ESPERA_PARA_JUNTAR_S = 60        # a mensagem do dia espera os outros negados do mesmo clique
AVISOS_RECENTES_DIAS = 30        # aviso mais velho que isso não aparece mais na tela inicial
PERTO_DA_HORA_MIN = 30           # a batida que apareceu perto da hora negada fecha o assunto


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "ocorrencias", "solicitado_colaborador_id")


# ---------------------------------------------------------------------------
# Puras (o teste percorre estas sem banco)
# ---------------------------------------------------------------------------
def quando_do_pedido(o: dict) -> str:
    """O dia (e a hora, no ajuste) do pedido, como a pessoa fala: "21/10 às 17:00"."""
    inicio, fim = o.get("data_inicio"), o.get("data_fim")
    if o.get("tipo") == "AJUSTE_BATIDA" and o.get("horario"):
        h = horario.para_local(o["horario"])
        return f"{h:%d/%m} às {h:%H:%M}"
    if o.get("tipo") == "COMPENSACAO" and o.get("dia_trabalhado"):
        return f"trabalhar {o['dia_trabalhado']:%d/%m}, folgar {inicio:%d/%m}"
    if inicio and fim and fim != inicio:
        return f"{inicio:%d/%m} a {fim:%d/%m}"
    return f"{inicio:%d/%m}" if inicio else ""


def _e(itens: list[str]) -> str:
    return itens[0] if len(itens) == 1 else ", ".join(itens[:-1]) + " e " + itens[-1]


def juntar(negados: list[dict]) -> list[dict]:
    """Os do mesmo tipo e mesmo motivo viram uma linha: quatro ajustes do dia negados com o mesmo
    motivo são "21/10 às 07:00, 11:00, 12:00 e 17:00", e o motivo vai uma vez."""
    grupos: dict[tuple, list[dict]] = {}
    for n in negados:
        grupos.setdefault((n["rotulo"], n["motivo"]), []).append(n)
    saida = []
    for (rotulo, motivo), itens in grupos.items():
        horas = [i["quando"].split(" às ")[1] for i in itens if " às " in i["quando"]]
        if len(itens) > 1 and len(horas) == len(itens) and len({i["quando"].split(" às ")[0] for i in itens}) == 1:
            quando = f"{itens[0]['quando'].split(' às ')[0]} às {_e(horas)}"
        else:
            quando = _e([i["quando"] for i in itens])
        saida.append({"rotulo": rotulo, "quando": quando, "motivo": motivo})
    return saida


def texto_da_mensagem(*, destinatario: str, pessoa: str, para_quem_pediu: bool, negados: list[dict]) -> str:
    """O WhatsApp. `negados`: [{rotulo, quando, motivo}], do mesmo dia da mesma pessoa."""
    primeiro = (destinatario or "").split(" ")[0].title()
    um = len(negados) == 1
    if para_quem_pediu:
        abre = (f"o pedido que você fez para {pessoa} foi NEGADO." if um
                else f"{len(negados)} pedidos que você fez para {pessoa} foram NEGADOS.")
    else:
        abre = "um pedido seu foi NEGADO." if um else f"{len(negados)} pedidos seus foram NEGADOS."
    linhas = [f"BWS Ponto — {primeiro}, {abre}", ""]
    for n in juntar(negados):
        linhas.append(f"• {n['rotulo']} — {n['quando']}")
        linhas.append(f"  Motivo: {n['motivo']}")
    linhas += ["", "Se o pedido estava errado, peça de novo — pelo aplicativo do ponto ou com o "
                   "administrativo da obra."]
    return "\n".join(linhas)


def ainda_em_aberto(n: dict, dia: dict) -> bool:
    """O negado ainda diz alguma coisa no dia? Sai quando o assunto se resolveu: no ajuste, a
    batida apareceu perto da hora, ou há outro pedido esperando para a mesma hora; nos outros
    pedidos, o dia ganhou um pedido aprovado ou esperando."""
    if n["tipo"] == "AJUSTE_BATIDA" and n.get("hora"):
        alvo = _min(n["hora"])
        if any(alvo is not None and _min(b["hora"]) is not None and abs(_min(b["hora"]) - alvo) <= PERTO_DA_HORA_MIN
               for b in dia.get("batidas") or []):
            return False
        return not any(p.get("hora") == n["hora"] for p in dia.get("pendentes") or [])
    return not dia.get("ocorrencia") and not any(p["tipo"] != "AJUSTE_BATIDA" for p in dia.get("pendentes") or [])


def _min(hhmm) -> Optional[int]:
    try:
        h, m = str(hhmm)[:5].split(":")
        return int(h) * 60 + int(m)
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Na hora de negar
# ---------------------------------------------------------------------------
def registrar(conn: Connection, o: dict, *, avisar_whatsapp: bool, quem_negou: str) -> dict:
    """Chamada por `ocorrencias.decidir`, na MESMA transação da negativa. `o` é o pedido
    (bruto) antes de negar. Põe a mensagem na fila (se for o caso) e devolve para quem
    ela vai: {"whatsapp": [nomes], "sem_telefone": [nomes]}."""
    saida = {"whatsapp": [], "sem_telefone": []}
    if not disponivel(conn):
        return saida
    db.executar(conn, "UPDATE ponto.ocorrencias SET negativa_whatsapp = :w WHERE id = :id",
                w=bool(avisar_whatsapp), id=o["id"])
    if not avisar_whatsapp:
        logger.info("Ponto: pedido %s negado por %s sem aviso pelo WhatsApp", o["id"], quem_negou)
        return saida
    from . import cadastros, envios
    pessoa = cadastros.colaborador_por_id(conn, int(o["colaborador_id"]))
    destinos = [(pessoa, False)]
    sol = o.get("solicitado_colaborador_id")
    if sol and int(sol) != int(o["colaborador_id"]):
        destinos.append((cadastros.colaborador_por_id(conn, int(sol)), True))
    for dest, para_quem_pediu in destinos:
        if not dest:
            continue
        tel = envios.telefone_valido(dest.get("telefone"))
        if not tel:
            saida["sem_telefone"].append(dest["nome"])
            envios.registrar_nao_enviado("ponto.pedido_negado", "sem celular com DDD no cadastro",
                                         cpf=dest.get("cpf") or "", texto=f"aviso de pedido negado para {dest['nome']}",
                                         status=envios.SEM_DESTINO)
            continue
        _enfileirar_do_dia(conn, o, dest, pessoa, para_quem_pediu, tel, quem_negou)
        saida["whatsapp"].append(dest["nome"])
    return saida


def _negados_do_dia(conn: Connection, colaborador_id: int, dia: dt.date) -> list[dict]:
    """Os negados com aviso do mesmo dia da mesma pessoa, decididos há pouco: são os que entram
    na mesma mensagem."""
    from .espelho import rotulo_da_ocorrencia
    linhas = db.todos(conn, """
        SELECT id, tipo, subtipo, data_inicio, data_fim, dia_trabalhado, horario, motivo_negativa, descricao
          FROM ponto.ocorrencias
         WHERE colaborador_id = :c AND data_inicio = :d AND status = 'NEGADA' AND negativa_whatsapp
           AND atualizado_em >= now() - interval '15 minutes'
         ORDER BY horario NULLS LAST, id""", c=colaborador_id, d=dia)
    return [{"rotulo": rotulo_da_ocorrencia(x), "quando": quando_do_pedido(x),
             "motivo": (x.get("motivo_negativa") or "").strip()} for x in linhas]


def _enfileirar_do_dia(conn, o, dest, pessoa, para_quem_pediu, tel, quem_negou) -> None:
    from . import envios
    base = f"negativa:{dest['id']}:{o['colaborador_id']}:{o['data_inicio']}"
    texto = texto_da_mensagem(destinatario=dest["nome"], pessoa=pessoa["nome"] if pessoa else "",
                              para_quem_pediu=para_quem_pediu,
                              negados=_negados_do_dia(conn, int(o["colaborador_id"]), o["data_inicio"]))
    esperando = db.um(conn, """
        SELECT id FROM ponto.envios WHERE tipo = 'NEGATIVA' AND status = 'PENDENTE'
           AND referencia LIKE :b ORDER BY id DESC LIMIT 1""", b=base + ":%")
    if esperando:
        db.executar(conn, "UPDATE ponto.envios SET texto = :t WHERE id = :id", t=texto[:2000], id=esperando["id"])
        return
    envios.enfileirar(conn, tipo="NEGATIVA", referencia=f"{base}:{o['id']}", telefone=tel, texto=texto,
                      colaborador_id=int(dest["id"]), pedido_por=quem_negou,
                      agendado_para=horario.agora() + dt.timedelta(seconds=ESPERA_PARA_JUNTAR_S))


# ---------------------------------------------------------------------------
# No aplicativo
# ---------------------------------------------------------------------------
def avisos(conn: Connection, colaborador_id: int) -> list[dict]:
    """Os negados que esta pessoa ainda não viu: os dela, e os que ela pediu por outra pessoa."""
    if not disponivel(conn):
        return []
    from .espelho import rotulo_da_ocorrencia
    linhas = db.todos(conn, """
        SELECT oc.id, oc.tipo, oc.subtipo, oc.data_inicio, oc.data_fim, oc.dia_trabalhado, oc.horario,
               oc.motivo_negativa, oc.descricao, oc.colaborador_id, c.nome AS pessoa,
               (oc.colaborador_id <> :eu) AS para_outro
          FROM ponto.ocorrencias oc JOIN public.colaboradores c ON c.id = oc.colaborador_id
         WHERE oc.status = 'NEGADA' AND oc.atualizado_em >= now() - make_interval(days => :dias)
           AND ((oc.colaborador_id = :eu AND oc.negativa_vista_em IS NULL)
             OR (oc.solicitado_colaborador_id = :eu AND oc.colaborador_id <> :eu
                 AND oc.negativa_vista_solicitante_em IS NULL))
         ORDER BY oc.atualizado_em DESC LIMIT 20""", eu=colaborador_id, dias=AVISOS_RECENTES_DIAS)
    return [{"id": int(x["id"]), "tipo": x["tipo"], "rotulo": rotulo_da_ocorrencia(x),
             "quando": quando_do_pedido(x), "dia": x["data_inicio"].isoformat(),
             "motivo": x.get("motivo_negativa") or "", "para_outro": bool(x["para_outro"]),
             "pessoa_id": int(x["colaborador_id"]), "pessoa": x["pessoa"]} for x in linhas]


def marcar_visto(conn: Connection, colaborador_id: int, ocorrencia_id: int) -> bool:
    """O aviso some para quem tocou nele — a pessoa, ou quem pediu por ela."""
    if not disponivel(conn):
        return False
    n = db.executar(conn, """
        UPDATE ponto.ocorrencias SET
               negativa_vista_em = CASE WHEN colaborador_id = :eu THEN COALESCE(negativa_vista_em, now())
                                        ELSE negativa_vista_em END,
               negativa_vista_solicitante_em = CASE WHEN colaborador_id <> :eu AND solicitado_colaborador_id = :eu
                                                    THEN COALESCE(negativa_vista_solicitante_em, now())
                                                    ELSE negativa_vista_solicitante_em END
         WHERE id = :id AND status = 'NEGADA' AND (colaborador_id = :eu OR solicitado_colaborador_id = :eu)""",
        eu=colaborador_id, id=ocorrencia_id)
    return n > 0


def nos_dias(conn: Connection, colaborador_id: int, dias: list[dict]) -> None:
    """Põe em cada dia do mês (`espelho`) os negados que ainda estão em aberto: `dia["negados"]`."""
    for d in dias:
        d["negados"] = []
    if not dias:
        return
    from .espelho import rotulo_da_ocorrencia
    inicio, fim = dias[0]["data"], dias[-1]["data"]
    por_dia: dict[str, list[dict]] = {}
    for x in db.todos(conn, """
        SELECT id, tipo, subtipo, data_inicio, data_fim, dia_trabalhado, horario, motivo_negativa, descricao
          FROM ponto.ocorrencias
         WHERE colaborador_id = :c AND status = 'NEGADA' AND data_fim >= :i AND data_inicio <= :f
         ORDER BY id""", c=colaborador_id, i=inicio, f=fim):
        n = {"id": int(x["id"]), "tipo": x["tipo"], "rotulo": rotulo_da_ocorrencia(x),
             "hora": horario.para_local(x["horario"]).strftime("%H:%M") if x["tipo"] == "AJUSTE_BATIDA" and x["horario"] else None,
             "motivo": x.get("motivo_negativa") or ""}
        d = x["data_inicio"]
        while d <= x["data_fim"]:
            por_dia.setdefault(d.isoformat(), []).append(n)
            d += dt.timedelta(days=1)
    for d in dias:
        d["negados"] = [n for n in por_dia.get(d["data"], []) if ainda_em_aberto(n, d)]
