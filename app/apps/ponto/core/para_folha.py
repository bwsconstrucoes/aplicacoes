# -*- coding: utf-8 -*-
"""
O PONTO PARA A FOLHA DO ANÁLISE DE SPs — a base mista.

Pedido do dono, 21/10/2026: *"na folha de pagamento do Análise de SPs a gente
importa o ponto do MobPonto. Até 15 de outubro é MobPonto; do dia 16 em diante,
a base do ponto já vai ser esta (…) busca uma forma de interligar. A gente vai
ter uma base mista."*

A folha (rateio por obra, diaristas, alimentação e transporte, a ficha do
funcionário) lê o ponto num formato só — o DIA DO MOBPONTO, `analisesps.ponto
._dia_lido`: as quatro marcações (a OBRA de cada uma), as quatro horas, a
presença, a falta, o total de horas e o dia da semana. Este módulo entrega os
dias do ponto eletrônico NESSE formato, por CPF de 11 dígitos — e a folha troca,
a partir do início do ponto (16/10/2026, `vigencia.py`), o dia do MobPonto
pelo daqui. Os cálculos da folha não mudam uma linha.

⚠️ NASCE DESLIGADO (o parâmetro `folha.le_o_ponto`, em Configuração › Geral).
A folha é dinheiro e é área de outro chat: liga quem conferiu a ficha de algumas
pessoas e o rateio de uma quinzena.

COMO O DIA DAQUI VIRA O DIA DE LÁ (decisões, para a folha conferir):
  · as marcações são as batidas na ordem do dia, com o CÓDIGO da obra de cada
    uma (a folha já casa obra por código, HISTORICO do Análise de SPs, 29/09);
    dia de duas batidas (vigia, obra de intervalo pré-assinalado) ocupa a
    entrada e a saída; da quinta em diante, ficam de fora da contagem;
  · presença: "Presença" quando houve batida (inclusive em feriado e descanso:
    quem trabalhou, trabalhou na obra); "Falta" na falta sem justificativa;
    o nome do abono (Atestado, Férias, Licença…) no dia justificado; "Feriado"
    e "Descanso" no dia sem batida; vazio no dia sem escala;
  · falta: "Falta sem justificativa" só na falta;
  · dia futuro e fora do contrato não entram; hoje só entra se já tem batida.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from . import cadastros, espelho, parametros, vigencia

PARAMETRO = "folha.le_o_ponto"
MAX_DIAS = 62


def ligado(conn: Connection) -> bool:
    return (parametros.ler(conn, PARAMETRO, "") or "").strip() == "1"


def ligar(conn: Connection, sim: bool, por: str) -> bool:
    parametros.gravar(conn, PARAMETRO, "1" if sim else "0", por)
    return bool(sim)


def corte(conn: Connection) -> dt.date:
    """O primeiro dia que a folha lê daqui (o início do ponto)."""
    return vigencia.inicio(conn)


def cpf11(cpf) -> str:
    d = "".join(ch for ch in str(cpf or "") if ch.isdigit())
    return d.zfill(11) if d else ""


def _horas(minutos: Optional[int]) -> str:
    m = int(minutos or 0)
    return f"{m // 60:02d}:{m % 60:02d}" if m else ""


def quatro(batidas: list[dict]) -> tuple[list[str], list[str]]:
    """PURA. (obras, horas) nas quatro posições da folha: entrada, almoço, retorno, saída."""
    b = batidas[:4]
    if len(b) == 2:                         # entrada e saída, sem intervalo batido
        b = [b[0], None, None, b[1]]
    b = (b + [None] * 4)[:4]
    return ([(x.get("obra") or "").upper() if x else "" for x in b],
            [x.get("hora") or "" if x else "" for x in b])


def presenca_e_falta(d: dict) -> tuple[str, str]:
    """PURA. O texto de presença e o de falta, como a folha lê do MobPonto."""
    if d.get("batidas"):
        return "Presença", ""
    s = d.get("situacao")
    if s == "FALTA":
        return "Falta", "Falta sem justificativa"
    if s in ("ABONADO", "COMPENSADO"):
        return ((d.get("ocorrencia") or {}).get("rotulo") or ("Compensação" if s == "COMPENSADO" else "Abonado")), ""
    if d.get("feriado"):
        return "Feriado", ""
    if s == "DESCANSO":
        return "Descanso", ""
    return "", ""


def dia_da_folha(d: dict) -> dict:
    """PURA. Um dia do espelho no formato do dia da folha (`analisesps.ponto._dia_lido`)."""
    obras, horas = quatro(d.get("batidas") or [])
    presenca, falta = presenca_e_falta(d)
    return {"data": dt.date.fromisoformat(d["data"]), "marcacoes": obras, "horas": horas,
            "presenca": presenca, "falta": falta, "total_de_horas": _horas(d.get("trabalhado")),
            "dia_da_semana": d.get("dia_semana") or "", "fonte": "PONTO"}


def dias_por_cpf(conn: Connection, inicio: dt.date, fim: dt.date,
                 cpfs: Optional[list[str]] = None) -> dict[str, dict]:
    """`{cpf11: {"nome", "dias": [dia da folha]}}` de `inicio` a `fim` (nunca antes do
    início do ponto, nunca depois de hoje). Quem entra: quem está ativo no ponto e
    quem tem batida no período (o desligado no meio do mês também recebe)."""
    inicio = max(inicio, corte(conn))
    fim = min(fim, horario.hoje())
    if fim < inicio:
        return {}
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ValueError(f"no máximo {MAX_DIAS} dias de uma vez")
    quer = {cpf11(c) for c in cpfs} if cpfs else None
    pessoas = {int(p["id"]): p for p in cadastros.listar_colaboradores(conn, so_ativos=True)}
    for l in db.todos(conn, """
        SELECT DISTINCT colaborador_id FROM ponto.marcacoes
         WHERE data_referencia BETWEEN :i AND :f AND status <> 'REJEITADA'""", i=inicio, f=fim):
        cid = int(l["colaborador_id"])
        if cid not in pessoas:
            p = cadastros.colaborador_por_id(conn, cid)
            if p:
                pessoas[cid] = p
    lista = [p for p in pessoas.values() if cpf11(p.get("cpf")) and (quer is None or cpf11(p["cpf"]) in quer)]
    if not lista:
        return {}
    lote = espelho.Lote(conn, lista, inicio, fim)
    hoje = horario.hoje()
    saida: dict[str, dict] = {}
    for p in lista:
        esp = espelho.montar(conn, int(p["id"]), inicio, fim, lote=lote)
        dias = []
        for d in esp["dias"]:
            if d["situacao"] in ("FUTURO", "FORA_DO_CONTRATO", vigencia.SITUACAO_ANTES):
                continue
            if d["data"] == hoje.isoformat() and not d["batidas"]:
                continue                     # hoje sem batida ainda não é falta
            dias.append(dia_da_folha(d))
        if dias:
            saida[cpf11(p["cpf"])] = {"nome": p["nome"], "dias": dias}
    return saida
