# -*- coding: utf-8 -*-
"""
A ADMISSÃO CAMINHA DENTRO DA SOLICITAÇÃO — e só sai da fila no fim.

⚑ PEDIDO DELE, 19/11/2026:

> *"eu recebi um cadastro em solicitações (…) eu coloco atender. Aquele
> cadastro vai para a inicial (…) ficam duas telas diferentes (…) Aí ela sumiu
> da parte de solicitações (…) mas foi uma solicitação que não foi atendida na
> sua completude ainda. (…) as solicitações, elas devem ser tratadas em
> solicitações. O passo a passo, o caminho delas, ela vai avançando dentro da
> parte de solicitações, até a conclusão. (…) Quando uma admissão for
> concluída, é que a solicitação do DP foi completamente atendida e aí é o
> momento dela sumir."*

E as decisões que fecharam o desenho, no mesmo dia: *"Quando o cadastro chega
em concluído, (…) pode ir sozinho."* E sobre o PJ: *"o passo é a anexação do
contrato de prestação de serviço (…) não tem nada a ver com assinar."*

**O QUE MUDA:** atender a admissão não fecha mais o pedido quando ainda falta
coisa. Ele fica **Em andamento**, com os passos à vista, e **fecha sozinho**
quando o cadastro chega a **Concluído** — por qualquer caminho: o retorno da
contabilidade, o contrato anexado, o DP concluindo na ficha.

Os passos dependem do vínculo:

| Vínculo | Até concluir |
|---|---|
| CTPS | pedido de Assinatura de CTPS à contabilidade → o registro volta |
| PJ | o contrato de prestação de serviço anexado no cadastro |
| outro prestador | nada: conclui no atendimento |

⚑ O MESMO DESENHO DO DESLIGAMENTO (`desligamento_processo`): o sistema LÊ a
prova (o pedido à contabilidade, o documento no cadastro, a etapa) em vez de
pedir marcação. A diferença é o fim: o desligamento se encerra com um clique
(há pagamentos a conferir); a admissão fecha sozinha, porque "Concluído" já é
a conferência.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

EM_ANDAMENTO = "EM_ANDAMENTO"
# Os papéis que valem como o contrato do PJ — os mesmos que a trava de
# concluir aceita (`pessoas._o_que_impede_concluir`).
CONTRATOS_DO_PJ = ("CONTRATO-TRABALHO", "CONTRATO-FORNECEDOR")


def e_processo(tipo: str) -> bool:
    # ⚑ A ADMISSÃO DE PRESTADOR (prestador → CTPS) entrou em 20/11/2026: ela
    # também só termina com o registro da carteira.
    return tipo in ("CADASTRO", "ADMISSAO_PRESTADOR")


def _pessoa(s: Session, colaborador_id: Optional[int]) -> Optional[dict]:
    if not colaborador_id:
        return None
    linha = s.execute(text("""
        SELECT id, etapa, vinculo, tipo_contrato FROM colaboradores
         WHERE id = :i"""), {"i": colaborador_id}).mappings().first()
    return dict(linha) if linha else None


def passos(s: Session, sol: dict[str, Any]) -> dict[str, Any]:
    """Os passos da admissão desta solicitação, lidos do que já existe."""
    p = _pessoa(s, sol.get("colaborador_id"))
    vinculo = ((p or {}).get("vinculo") or "CTPS").upper()
    pj = vinculo == "PRESTADOR" and (p or {}).get("tipo_contrato") == "PJ"
    etapa = ((p or {}).get("etapa") or "").upper()
    atendido = sol.get("situacao") in (EM_ANDAMENTO, "ATENDIDA")
    saida: list[dict[str, Any]] = []

    def item(codigo, rotulo, quem, estado, detalhe, **extra):
        saida.append({"codigo": codigo, "rotulo": rotulo, "quem": quem,
                      "como": "lido", "estado": estado, "detalhe": detalhe,
                      "por": "", "em": None, "observacao": "", **extra})

    item("ATENDIDO", "O DP conferiu e completou o cadastro", "DP",
         "FEITO" if atendido else "PENDENTE",
         "atendido" if atendido else "atenda o pedido para completar o que é do DP")

    if vinculo == "CTPS" and p:
        ped = s.execute(text("""
            SELECT id, numero, situacao FROM dp_pedidos
             WHERE colaborador_id = :c AND template = 'ASSINATURA_CTPS'
               AND situacao <> 'CANCELADO'
             ORDER BY (situacao = 'CONCLUIDO') DESC, id DESC LIMIT 1"""),
            {"c": p["id"]}).mappings().first()
        if ped is None:
            item("CONTABILIDADE", "Registro da carteira pedido à contabilidade",
                 "DP", "PENDENTE", "monte o pedido de Assinatura de CTPS")
        elif ped["situacao"] == "RASCUNHO":
            item("CONTABILIDADE", "Registro da carteira pedido à contabilidade",
                 "DP", "PENDENTE", f"{ped['numero']} montado e ainda não enviado",
                 pedido={"id": ped["id"], "numero": ped["numero"]})
        else:
            item("CONTABILIDADE", "Registro da carteira pedido à contabilidade",
                 "DP", "FEITO", f"{ped['numero']} enviado",
                 pedido={"id": ped["id"], "numero": ped["numero"]})
        voltou = ped is not None and ped["situacao"] == "CONCLUIDO"
        item("REGISTRO", "A contabilidade devolveu o registro", "Contabilidade",
             "FEITO" if voltou else ("AGUARDANDO" if ped is not None
                                     and ped["situacao"] != "RASCUNHO"
                                     else "PENDENTE"),
             "registro concluído" if voltou else
             ("esperando a contabilidade" if ped is not None else
              "depois do pedido"))
    elif pj and p:
        tem = s.execute(text("""
            SELECT 1 FROM documentos WHERE colaborador_id = :c
               AND tipo_codigo = ANY(:t) LIMIT 1"""),
            {"c": p["id"], "t": list(CONTRATOS_DO_PJ)}).first() is not None
        item("CONTRATO", "Contrato de prestação de serviço anexado", "DP",
             "FEITO" if tem else "PENDENTE",
             "anexado no cadastro" if tem
             else "jogue o contrato na aba Arquivos do cadastro")

    concluido = etapa == "CONCLUIDO"
    item("CONCLUIDO", "Cadastro concluído", "sistema",
         "FEITO" if concluido else "PENDENTE",
         "o pedido fecha sozinho" if not concluido else "concluído")
    cumpridos = sum(1 for x in saida if x["estado"] == "FEITO")
    proximo = next((x for x in saida if x["estado"] != "FEITO"), None)
    return {"passos": saida, "cumpridos": cumpridos, "total": len(saida),
            "pode_encerrar": False, "fecha_sozinho": True,
            "proximo": proximo["rotulo"] if proximo else "",
            "marcacoes_disponiveis": True}


def avancar_no_atendimento(s: Session, colaborador_id: int,
                           usuario: Optional[Usuario]) -> bool:
    """Depois de atender: o prestador que já pode ser concluído, conclui.

    Devolve se o cadastro chegou a Concluído. O que a trava de concluir recusar
    (o contrato do PJ, um pedido à contabilidade aberto) não é erro: o pedido
    fica Em andamento, com o passo à vista.
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    from app.apps.erp.core.comum.auditoria import ErroValidacao

    p = _pessoa(s, colaborador_id)
    if p is None:
        return False
    if (p["etapa"] or "").upper() == "CONCLUIDO":
        return True
    if (p["vinculo"] or "CTPS").upper() != "PRESTADOR":
        return False
    if (p["etapa"] or "").upper() not in ("CADASTRO_INICIAL", "CONTRATO_A_ASSINAR",
                                          "EM_CONFERENCIA"):
        return False
    try:
        svc_pessoas.mover_etapa(s, colaborador_id, "CONCLUIDO", usuario,
                                motivo="admissão de prestador atendida")
        return True
    except ErroValidacao:
        return False


def fechar_concluidas(s: Session, colaborador_id: int,
                      usuario: Optional[Usuario] = None) -> int:
    """O cadastro chegou a Concluído: as admissões em andamento desta pessoa
    estão completamente atendidas. Chamada por `pessoas.mover_etapa`."""
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    ids = s.execute(text("""
        SELECT id FROM dp_solicitacoes
         WHERE colaborador_id = :c
           AND tipo IN ('CADASTRO', 'ADMISSAO_PRESTADOR')
           AND situacao = :sit"""),
        {"c": colaborador_id, "sit": EM_ANDAMENTO}).scalars().all()
    from app.apps.erp.core import contabil_processo
    ids = [i for i in ids if not contabil_processo.extras_abertos(s, int(i))]
    for i in ids:
        s.execute(text("""UPDATE dp_solicitacoes SET situacao = 'ATENDIDA',
                                 atualizado_em = now() WHERE id = :i"""), {"i": i})
        _evento(s, i, "ADMISSAO_CONCLUIDA",
                {"texto": "o cadastro chegou a Concluído — pedido atendido"},
                usuario)
    if ids:
        logger.info("ERP/DP: %s admissão(ões) fechada(s) com o cadastro %s "
                    "concluído", len(ids), colaborador_id)
    return len(ids)
