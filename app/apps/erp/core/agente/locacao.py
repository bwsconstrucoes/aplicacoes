# ============================================================================
# ERP — core/agente/locacao.py
# O primeiro assunto que o agente cobra: a conferência mensal dos equipamentos.
#
# Foi escolhido a dedo para ser o primeiro. É a única pendência do ERP cujo
# responsável NÃO fica na frente de um computador o dia todo — é o
# administrativo da obra, no canteiro, com o telefone na mão. Se o agente
# funciona aqui, funciona em qualquer lugar; e se não funcionar, é aqui que se
# descobre por quê.
#
# Este módulo não decide nada sobre cobrança: ele só traduz "conferência aberta
# há N dias" para o formato que o agente entende. A escada, o texto e o envio
# são do agente, e valem igual para todo assunto.
# ============================================================================
from __future__ import annotations

from sqlalchemy.orm import Session

from app.apps.erp.core import agente as ag
from app.apps.erp.core import locacoes_conferencia as conf


def listar(s: Session) -> list["ag.Pendencia"]:
    """As conferências abertas, com quantos dias estão esperando.

    O cálculo de atraso já existe em `locacoes_conferencia.pendentes` e é de lá
    que ele vem — se um dia a regra do prazo mudar, muda num lugar só, e a tela
    e o agente continuam contando a mesma coisa.
    """
    saida = []
    for p in conf.pendentes(s, todas=True):
        saida.append(ag.Pendencia(
            assunto="locacao_conferencia",
            referencia_id=p["id"],
            responsavel_id=p["responsavel_id"],
            resumo=(f"A conferência dos equipamentos locados de "
                    f"{p['competencia']} da obra {p['obra']} "
                    f"(contrato {p['contrato']}) ainda não foi respondida."),
            link=f"/erp/suprimentos/locacoes?conferencia={p['id']}",
            dias_de_atraso=p["dias_de_atraso"],
            detalhes={"obra": p["obra"], "contrato": p["contrato"],
                      "competencia": p["competencia"]},
        ))
    return saida


ag.registrar_assunto("locacao_conferencia", listar)
