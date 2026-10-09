# -*- coding: utf-8 -*-
"""
QUEM VALIDA O QUÊ — configurável pela tela (Ponto › Configuração).

Pedido do dono, 04/10/2026: *"todas essas solicitações precisam ser validadas
(…) a gente vai definir quem é que vai validar. Provavelmente (…) o próprio DP
(…) talvez umas coisas a gente ponha para o encarregado de obra, outras coisas
para o próprio RH, dependendo do que seja."*

  ENCARREGADO         quem trata o ponto da obra (`tratar_ponto`, no recorte de
                      obras dele)
  DP                  quem aprova afastamento (`aprovar_afastamento`)
  ENCARREGADO_E_DP    os dois, nessa ordem

O PADRÃO É O DP em tudo (a sugestão dele). O que NÃO se configura, e por quê:
  · atestado e afastamento: dado de saúde — só o DP vê (LGPD, decisão do dono
    de 03/10/2026);
  · férias e abono: nascem no próprio DP;
  · o mosaico: quem confere é o responsável escolhido em cada obra.

TROCAR A REGRA COM PEDIDOS NA FILA: o que esperava o encarregado e passou a ser
só do DP vai para o DP; o que esperava o DP sem ter passado pelo encarregado e
passou a ser só do encarregado vai para o encarregado. Nada fica preso numa
etapa que deixou de existir.
"""
from __future__ import annotations

import json
import logging

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao
from . import parametros

logger = logging.getLogger("ponto.validacao")

PARAMETRO = "validacao.quem"
ENCARREGADO, DP, AMBOS = "ENCARREGADO", "DP", "ENCARREGADO_E_DP"
OPCOES = (ENCARREGADO, DP, AMBOS)
# O rótulo diz "administração da obra" (09/10/2026: "o termo não é encarregado (…) pode ser o
# encarregado, o almoxarife, o DP"); o valor gravado continua ENCARREGADO.
ROTULO_OPCAO = {ENCARREGADO: "Administração da obra", DP: "DP",
                AMBOS: "Administração da obra e depois o DP"}

# O que se configura, com o padrão (DP) e as opções de cada um.
CONFIGURAVEIS = {
    "AJUSTE_BATIDA": {"rotulo": "Ajuste de batida (esqueci de bater, celular quebrado)", "opcoes": OPCOES},
    "COMPENSACAO": {"rotulo": "Compensação (trabalhar num dia no lugar de outro)", "opcoes": OPCOES},
    "FOLGA_BANCO": {"rotulo": "Folga do banco de horas", "opcoes": OPCOES},
    "LICENCA": {"rotulo": "Licença (casamento, luto, paternidade…)", "opcoes": OPCOES},
    "BATIDA_EM_ANALISE": {"rotulo": "Batida em conferência (fora do normal, sem foto, borda da cerca…)",
                          "opcoes": (ENCARREGADO, DP)},
}
FIXOS = {
    "ATESTADO": "DP — dado de saúde, só o DP vê",
    "AFASTAMENTO": "DP — dado de saúde, só o DP vê",
    "FERIAS": "DP — lançado pelo próprio DP",
    "ABONO": "DP — lançado pelo próprio DP",
    "MOSAICO": "o responsável escolhido em cada obra (Mosaico de fotos por obra)",
}
PADRAO = DP

# Opção → etapas do pedido (os nomes internos das etapas, de ocorrencias.py).
_ETAPAS = {ENCARREGADO: ("SUPERVISOR",), DP: ("DP",), AMBOS: ("SUPERVISOR", "DP")}


def ler(conn: Connection) -> dict[str, str]:
    """A regra em vigor, com o padrão onde ninguém escolheu."""
    bruto = {}
    try:
        bruto = json.loads(parametros.ler(conn, PARAMETRO, "") or "{}")
    except (ValueError, TypeError):
        logger.warning("Ponto: parâmetro %s ilegível; valendo o padrão", PARAMETRO)
    return {k: (bruto.get(k) if bruto.get(k) in v["opcoes"] else PADRAO) for k, v in CONFIGURAVEIS.items()}


def etapas_do_tipo(conn: Connection, tipo: str) -> tuple[str, ...]:
    if tipo in CONFIGURAVEIS:
        return _ETAPAS[ler(conn)[tipo]]
    return ("DP",)                      # ATESTADO, AFASTAMENTO, FERIAS, ABONO


def quem_valida_batida(conn: Connection) -> str:
    return ler(conn)["BATIDA_EM_ANALISE"]


def gravar(conn: Connection, novos: dict, por: str) -> dict:
    atual = ler(conn)
    for tipo, opcao in (novos or {}).items():
        if tipo not in CONFIGURAVEIS:
            raise ErroDeValidacao(f"isto não se configura: {tipo}", campo=tipo)
        if opcao not in CONFIGURAVEIS[tipo]["opcoes"]:
            raise ErroDeValidacao(f"opção inválida para {CONFIGURAVEIS[tipo]['rotulo'].lower()}", campo=tipo)
        atual[tipo] = opcao
    parametros.gravar(conn, PARAMETRO, json.dumps(atual, ensure_ascii=False), por)
    movidos = _realinhar_a_fila(conn, atual)
    logger.info("Ponto: quem valida o quê alterado por %s — %s (%d pedido(s) realinhados)", por, atual, movidos)
    return {"regra": atual, "pedidos_realinhados": movidos}


def _realinhar_a_fila(conn: Connection, regra: dict) -> int:
    movidos = 0
    for tipo, opcao in regra.items():
        if tipo == "BATIDA_EM_ANALISE":
            continue
        if opcao == DP:
            movidos += db.executar(conn, """
                UPDATE ponto.ocorrencias SET status = 'AGUARDANDO_DP', atualizado_em = now()
                 WHERE tipo = :t AND status = 'AGUARDANDO_SUPERVISOR'""", t=tipo)
        elif opcao == ENCARREGADO:
            movidos += db.executar(conn, """
                UPDATE ponto.ocorrencias SET status = 'AGUARDANDO_SUPERVISOR', atualizado_em = now()
                 WHERE tipo = :t AND status = 'AGUARDANDO_DP' AND supervisor_em IS NULL""", t=tipo)
    return movidos


def para_tela(conn: Connection) -> dict:
    regra = ler(conn)
    return {
        "configuraveis": [{"tipo": t, "rotulo": v["rotulo"], "valor": regra[t],
                           "opcoes": [{"valor": o, "rotulo": ROTULO_OPCAO[o]} for o in v["opcoes"]]}
                          for t, v in CONFIGURAVEIS.items()],
        "fixos": [{"tipo": t, "explicacao": e} for t, e in FIXOS.items()],
    }
