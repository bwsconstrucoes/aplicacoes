# -*- coding: utf-8 -*-
"""
CANCELAR SP PELO SISTEMA (26/11/2026).

O dono: *"um botão tanto na parte de solicitações quanto em lote, que vai ser
cancelar SP. E também no modal (…) O que é que esse botão vai fazer? Você vai criar
um novo card no Pipefy (…) pipe 301426645, (…) em mutation, se tiver mais de um,
para economizar chamadas de API (…) Quando vai cancelar, antes de cancelar, abre um
modal listando o que vai ser cancelado, para a gente em seguida fazer a confirmação
(…) Isso aí vai disparar o Pipefy e aí tem uma automação no make.com que vai
terminar o trabalho."*

O QUE ESTE MÓDULO FAZ, E O QUE NÃO FAZ: cria o card de PEDIDO de cancelamento (um
por SP) no pipe de procedimentos. Quem cancela de fato é o Make, disparado pelo card.
Este módulo NÃO muda o status da SP na planilha nem no banco: a SP aparece como
"Cancelado" quando o Make terminar e a sincronização trouxer.

Os campos do card, ditados pelo dono:
    selecione_o_procedimento = "Cancelar SP"
    n_da_solicita_o          = o número da SP
    motivo                   = "Cancelado pelo Sistema AnaliseSPs"
    colaborador_solicitante  = 620439304  (conexão: vai como lista)

SÓ VAI A SP COM STATUS "PAGAR" E SEM AGENDAMENTO (`bloqueio`) — paga, agendada ou já
cancelada fica de fora, e a janela diz o que fazer antes.

O PEDIDO FICA NO LOG (`log_alteracoes`, ação "Cancelar SP"): quem pediu, quando e o
card criado — e a janela avisa se a SP já teve cancelamento pedido antes, para não
criar um segundo card sem querer.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("analisesps.cancelar_sp")

PIPE_DE_PROCEDIMENTOS = "301426645"
ACAO = "Cancelar SP"
MOTIVO = "Cancelado pelo Sistema AnaliseSPs"
COLABORADOR_SOLICITANTE = "620439304"


class ErroDoCancelamento(RuntimeError):
    """Frase pronta para a tela."""


def valores_do_card(sp_id: str) -> list:
    return [{"campo": "selecione_o_procedimento", "valor": ACAO},
            {"campo": "n_da_solicita_o", "valor": str(sp_id)},
            {"campo": "motivo", "valor": MOTIVO},
            {"campo": "colaborador_solicitante", "valor": [COLABORADOR_SOLICITANTE]}]


def _cancelada(status: str) -> bool:
    return str(status or "").strip().lower() == "cancelado"


# A TRAVA (dono, 26/11/2026): *"se a solicitação de pagamento tiver com algum
# agendamento, tiver em agendado, ou tiver pago, não permitir o cancelamento. Tem que
# alterar primeiro o status para sem agendamento e pagar. O que tiver cancelado (…)
# não cancela também."* Só vai a SP com Status Pgt = "Pagar" e o campo Agendado vazio.
def bloqueio(status: str, agendado: str) -> str:
    """"" quando pode cancelar; senão, a frase do que fazer antes."""
    st = str(status or "").strip()
    ag = str(agendado or "").strip()
    if _cancelada(st):
        return "já cancelada"
    if st.lower() != "pagar":
        return f"está {st or 'sem status'} — mude para Pagar antes"
    if ag:
        return f"tem agendamento ({ag}) — desagende antes"
    return ""


def previa(ids: list) -> dict:
    """O que a janela de confirmação mostra: cada SP com credor, valor, vencimento e
    status; as já canceladas (não vão) e as que já tiveram pedido antes (aviso)."""
    from .db import consultar
    ids = [str(i) for i in ids]
    marcas = ",".join("?" for _ in ids)
    linhas = consultar(
        "SELECT id, credor, valor_num, vencimento_d, trim(coalesce(status_pgt,'')), "
        "       centro_custo, trim(coalesce(agendado,'')) "
        "  FROM analisesps.sps WHERE id IN (" + marcas + ")", tuple(ids))
    por_id = {str(l[0]): l for l in linhas}
    pedidos = {}
    for sp, quando, quem in consultar(
            "SELECT sp_id, max(criado_em), max(pessoa) FROM analisesps.log_alteracoes "
            " WHERE acao = ? AND sp_id IN (" + marcas + ") GROUP BY sp_id",
            (ACAO,) + tuple(ids)):
        pedidos[str(sp)] = {"em": quando, "por": quem or ""}
    sps = []
    for sp in ids:
        l = por_id.get(sp)
        if not l:
            sps.append({"id": sp, "achada": False})
            continue
        sps.append({"id": sp, "achada": True, "credor": l[1] or "", "valor": l[2],
                    "vencimento": l[3], "status": l[4], "obra": l[5] or "",
                    "agendado": l[6] or "", "cancelada": _cancelada(l[4]),
                    "bloqueio": bloqueio(l[4], l[6]), "pedido": pedidos.get(sp)})
    return {"sps": sps,
            "vao": [s["id"] for s in sps if s.get("achada") and not s.get("bloqueio")]}


def pedir(ids: list, quem: str = "", perfil: str = "", criar=None) -> dict:
    """Cria os cards de cancelamento das SPs (as já canceladas ficam de fora) e
    registra cada pedido no Log. `criar(pipe, cards)` é o `pipefy.criar_cards` (o
    teste passa um falso). Devolve `{"criados": [{sp, card, link}], "erros": {sp: frase},
    "pulados": [sp]}`."""
    from . import pipefy
    from .db import conexao
    criar = criar or pipefy.criar_cards
    vista = previa(ids)
    vao = vista["vao"]
    pulados = [s["id"] for s in vista["sps"] if s["id"] not in vao]
    if not vao:
        raise ErroDoCancelamento("Nenhuma das SPs marcadas pode ser cancelada: só vai a SP "
                                 "com status Pagar e sem agendamento (as outras estão pagas, "
                                 "agendadas, já canceladas ou não foram encontradas).")
    feito = criar(PIPE_DE_PROCEDIMENTOS,
                  [{"chave": sp, "titulo": f"Cancelar SP {sp}", "valores": valores_do_card(sp)}
                   for sp in vao])
    criados, erros = [], {}
    for sp in vao:
        r = feito.get(sp) or {"erro": "sem resposta do Pipefy"}
        if r.get("id"):
            criados.append({"sp": sp, "card": r["id"], "link": r.get("link") or ""})
        else:
            erros[sp] = str(r.get("erro") or "")[:300]
    if criados:
        with conexao() as conn:
            for c in criados:
                conn.execute(
                    "INSERT INTO analisesps.log_alteracoes (sp_id, coluna, valor, acao, "
                    "  perfil, pessoa, status, enviado_em) "
                    "VALUES (?, 'pipefy', ?, ?, ?, ?, 'enviado', now())",
                    (c["sp"], f"card {c['card']} — {MOTIVO}", ACAO, perfil, quem))
            conn.commit()
    logger.info("Cancelar SP: %s pediu %d; %d card(s) criados, %d erro(s).",
                quem or "?", len(vao), len(criados), len(erros))
    return {"criados": criados, "erros": erros, "pulados": pulados}
