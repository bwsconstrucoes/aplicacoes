# -*- coding: utf-8 -*-
"""
OS PEDIDOS QUE PASSAM PELA CONTABILIDADE — só fecham quando ela devolve.

⚑ PEDIDO DELE, 20/11/2026, sobre a alteração de cargo, o dependente e as
férias: *"tem todo o procedimento de encaminhamento para contabilidade,
acompanhamento, retorno, o agente tem que acompanhar. A mesma coisa vale para
o dependente, porque isso impacta em salário-família (…) E a mesma coisa serve
para as férias (…) o administrativo de obra pode pedir as férias. E o DP tem
que fazer a mesma coisa, solicitar a contabilidade, aguardar retorno e etc.
Para então finalizar."*

É o desenho da admissão (`admissao_processo`) estendido: atender grava o que
é do DP e o pedido fica **Em andamento**, com três passos — o DP atendeu, o
pedido à contabilidade saiu, a contabilidade devolveu — e **fecha sozinho**
quando o pedido à contabilidade é concluído (`contabilidade.concluir` chama
`fechar_pelo_pedido`).

Quando se aplica, e por quê (decisões minhas, escritas para ele trocar):

| Pedido | Passa pela contabilidade quando | Modelo |
|---|---|---|
| Alteração de cargo | a pessoa é CTPS (prestador não tem registro a alterar) | ALTERACAO_CARGO |
| Dependente | CTPS e com salário-família ou dependente de IR marcado | DEPENDENTE |
| Férias | sempre (só existe para CTPS) | FERIAS |
| Cancelar desligamento | a rescisão JÁ tinha sido pedida à contabilidade | CANCELAMENTO_DESLIGAMENTO |

O pedido à contabilidade sai MONTADO com o que a solicitação já tem — o DP
confere o texto e envia; não redigita.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

EM_ANDAMENTO = "EM_ANDAMENTO"
TEMPLATE_DO_TIPO = {
    "ALTERACAO_CARGO": "ALTERACAO_CARGO",
    "DEPENDENTE": "DEPENDENTE",
    "FERIAS": "FERIAS",
    "CANCELAR_DESLIGAMENTO": "CANCELAMENTO_DESLIGAMENTO",
    # A admissão usa o mesmo botão para o registro da carteira.
    "CADASTRO": "ASSINATURA_CTPS",
    "ADMISSAO_PRESTADOR": "ASSINATURA_CTPS",
}


def e_processo(tipo: str) -> bool:
    return tipo in ("ALTERACAO_CARGO", "DEPENDENTE", "FERIAS",
                    "CANCELAR_DESLIGAMENTO")


def _vinculo(s: Session, colaborador_id: Optional[int]) -> str:
    if not colaborador_id:
        return ""
    return str(s.execute(text("SELECT vinculo FROM colaboradores WHERE id = :i"),
                         {"i": colaborador_id}).scalar() or "CTPS").upper()


def aplica(s: Session, tipo: str, colaborador_id: Optional[int],
           pedido: dict[str, Any]) -> bool:
    """Este pedido precisa passar pela contabilidade?"""
    vinculo = _vinculo(s, colaborador_id)
    if tipo in ("ALTERACAO_CARGO", "FERIAS"):
        return vinculo == "CTPS"
    if tipo == "DEPENDENTE":
        marcou = lambda v: str(v or "") == "1"
        varios = pedido.get("dependentes")
        if isinstance(varios, list) and varios:
            algum = any(marcou((d or {}).get("salario_familia"))
                        or marcou((d or {}).get("dependente_ir")) for d in varios)
        else:
            algum = False
        algum = algum or marcou(pedido.get("salario_familia")) \
            or marcou(pedido.get("dependente_ir"))
        return vinculo == "CTPS" and algum
    return False


def _pedido_ligado(s: Session, sol: dict[str, Any], template: str):
    return s.execute(text("""
        SELECT id, numero, situacao FROM dp_pedidos
         WHERE template = :t AND situacao <> 'CANCELADO'
           AND (solicitacao_id = :sid
                OR (solicitacao_id IS NULL AND colaborador_id = :c
                    AND criado_em >= :desde))
         ORDER BY (solicitacao_id = :sid) DESC NULLS LAST, id DESC LIMIT 1"""),
        {"t": template, "sid": sol["id"], "c": sol.get("colaborador_id"),
         "desde": sol["criado_em"]}).mappings().first()


def passos(s: Session, sol: dict[str, Any]) -> dict[str, Any]:
    template = TEMPLATE_DO_TIPO[sol["tipo"]]
    from app.apps.erp.core import contabilidade as svc_ct
    nome = svc_ct.TEMPLATES[template]["nome"]
    atendido = sol.get("situacao") in (EM_ANDAMENTO, "ATENDIDA")
    ped = _pedido_ligado(s, sol, template)
    saida = []

    def item(codigo, rotulo, quem, estado, detalhe, **extra):
        saida.append({"codigo": codigo, "rotulo": rotulo, "quem": quem,
                      "como": "lido", "estado": estado, "detalhe": detalhe,
                      "por": "", "em": None, "observacao": "", **extra})

    item("ATENDIDO", "O DP analisou e registrou", "DP",
         "FEITO" if atendido else "PENDENTE",
         "atendido" if atendido else "atenda o pedido")
    if ped is None:
        item("CONTABILIDADE", f"Pedido à contabilidade: {nome}", "DP", "PENDENTE",
             "monte o pedido — ele sai preenchido com o que a solicitação tem")
    else:
        enviado = ped["situacao"] != "RASCUNHO"
        item("CONTABILIDADE", f"Pedido à contabilidade: {nome}", "DP",
             "FEITO" if enviado else "PENDENTE",
             f"{ped['numero']} " + ("enviado" if enviado
                                    else "montado e ainda não enviado"),
             pedido={"id": ped["id"], "numero": ped["numero"]})
    voltou = ped is not None and ped["situacao"] == "CONCLUIDO"
    item("RETORNO", "A contabilidade devolveu", "Contabilidade",
         "FEITO" if voltou else ("AGUARDANDO" if ped is not None
                                 and ped["situacao"] != "RASCUNHO" else "PENDENTE"),
         "concluído — o pedido fecha sozinho" if voltou else
         ("esperando a contabilidade" if ped is not None else "depois do pedido"))
    cumpridos = sum(1 for x in saida if x["estado"] == "FEITO")
    proximo = next((x for x in saida if x["estado"] != "FEITO"), None)
    return {"passos": saida, "cumpridos": cumpridos, "total": len(saida),
            "pode_encerrar": False, "fecha_sozinho": True,
            "proximo": proximo["rotulo"] if proximo else "",
            "marcacoes_disponiveis": True}


def _dados_do_pedido(s: Session, sol: dict[str, Any]) -> dict[str, Any]:
    """O que a solicitação já sabe, no vocabulário do modelo da contabilidade."""
    d = sol.get("dados") or {}
    tipo = sol["tipo"]
    if tipo == "ALTERACAO_CARGO":
        cargo = d.get("cargo_deferido_id") or d.get("cargo_pleiteado_id")
        nome_cargo = s.execute(text("SELECT nome FROM cargos WHERE id = :i"),
                               {"i": int(cargo)}).scalar() if str(cargo or "").isdigit() else ""
        return {"cargo_novo": nome_cargo or "", "salario_novo": d.get("salario"),
                "vigencia": d.get("data_inicio"), "motivo": d.get("justificativa")}
    if tipo == "DEPENDENTE":
        um = (d.get("dependentes") or [d])[0] if isinstance(d.get("dependentes"), list) \
            else d
        return {"dependente_nome": um.get("nome"),
                "dependente_nascimento": um.get("data_nascimento"),
                "dependente_cpf": um.get("cpf") or d.get("cpf"),
                "parentesco": um.get("parentesco"),
                "salario_familia": "sim" if str(um.get("salario_familia") or
                                                d.get("salario_familia")) == "1" else "",
                "dependente_ir": "sim" if str(um.get("dependente_ir") or
                                              d.get("dependente_ir")) == "1" else ""}
    if tipo == "FERIAS":
        return {"inicio": d.get("ferias_inicio"), "dias": d.get("ferias_dias"),
                "abono": "sim" if str(d.get("abono_pecuniario")) == "1" else "não",
                "adiantamento_13": "sim" if str(d.get("adiantamento_13")) == "1"
                else "não", "observacao": d.get("observacao")}
    if tipo == "CANCELAR_DESLIGAMENTO":
        return {"comunicado_em": d.get("desligamento_comunicado_em"),
                "motivo": d.get("observacao")}
    return {}


def montar_pedido(s: Session, sol: dict[str, Any],
                  usuario: Optional[Usuario]) -> dict[str, Any]:
    """Monta (rascunho) o pedido à contabilidade desta solicitação."""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    template = TEMPLATE_DO_TIPO.get(sol["tipo"])
    if not template:
        raise ErroValidacao("Este pedido não passa pela contabilidade.")
    if not sol.get("colaborador_id"):
        raise ErroValidacao("O pedido não tem pessoa ligada.")
    if _pedido_ligado(s, sol, template) is not None:
        raise ErroValidacao("O pedido à contabilidade já foi montado.")
    r = svc_ct.abrir(s, {"template": template,
                         "colaborador_id": sol["colaborador_id"],
                         "solicitacao_id": sol["id"],
                         "dados": {k: v for k, v in _dados_do_pedido(s, sol).items()
                                   if v not in (None, "")}}, usuario)
    _evento(s, sol["id"], "PEDIDO_A_CONTABILIDADE",
            {"numero": r["numero"], "template": template}, usuario)
    return r


def fechar_pelo_pedido(s: Session, pedido: dict[str, Any],
                       usuario: Optional[Usuario] = None) -> int:
    """A contabilidade devolveu e o pedido foi concluído: as solicitações em
    andamento que esperavam por ele estão completamente atendidas."""
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    tipos = [t for t, tpl in TEMPLATE_DO_TIPO.items()
             if tpl == pedido["template"] and e_processo(t)]
    if not tipos:
        return 0
    ids = s.execute(text("""
        SELECT id FROM dp_solicitacoes
         WHERE situacao = :sit AND tipo = ANY(:t)
           AND (id = :sid OR (:sid IS NULL AND colaborador_id = :c))"""),
        {"sit": EM_ANDAMENTO, "t": tipos, "sid": pedido.get("solicitacao_id"),
         "c": pedido.get("colaborador_id")}).scalars().all()
    for i in ids:
        s.execute(text("UPDATE dp_solicitacoes SET situacao = 'ATENDIDA', "
                       "atualizado_em = now() WHERE id = :i"), {"i": i})
        _evento(s, i, "CONTABILIDADE_DEVOLVEU",
                {"texto": f"{pedido.get('numero')} concluído — pedido atendido"},
                usuario)
    return len(ids)


def em_andamento(s: Session, *, limite: int = 300) -> list[dict[str, Any]]:
    """As solicitações em andamento que não são desligamento — para o vigia."""
    linhas = s.execute(text("""
        SELECT d.id, d.numero, d.tipo, d.colaborador_id, d.dados, d.criado_em,
               d.situacao, d.atendido_em, COALESCE(c.nome, d.nome_informado) AS pessoa
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
         WHERE d.situacao = :sit AND d.tipo <> 'DESLIGAMENTO'
         ORDER BY d.atendido_em NULLS LAST, d.id LIMIT :l"""),
        {"sit": EM_ANDAMENTO, "l": limite}).mappings().all()
    saida = []
    hoje = date.today()
    for l in linhas:
        sol = dict(l)
        if e_processo(sol["tipo"]):
            est = passos(s, sol)
        else:
            from app.apps.erp.core import admissao_processo
            est = admissao_processo.passos(s, sol)
        saida.append({"id": l["id"], "numero": l["numero"], "tipo": l["tipo"],
                      "colaborador_id": l["colaborador_id"],
                      "pessoa": l["pessoa"] or "", "dados": l["dados"] or {},
                      "proximo": est["proximo"],
                      "dias": ((hoje - l["atendido_em"].date()).days
                               if l["atendido_em"] else None)})
    return saida
