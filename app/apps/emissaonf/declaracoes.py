# -*- coding: utf-8 -*-
"""
Registro das declarações enviadas à prefeitura e ainda sem nota.

Por que isto existe, e é a parte mais importante deste módulo: entre a
prefeitura **aceitar** a declaração e a nota **ficar pronta** existe um tempo que
não depende de nós — é uma fila do lado dela, e o portal dela chama esse estado
de *"Aguardando Transmissão"*.

Durante esse intervalo, o único registro da declaração era a **tela aberta no
navegador do dono**. Se ele fechasse a aba, publicasse o serviço ou a conexão
caísse, a identificação se perdia — e aconteceu em 07/10/2026: a identificação
só foi reencontrada porque ele foi procurar no portal da prefeitura.

Então a declaração passa a ser gravada **antes de qualquer espera**, numa aba
própria. Dali qualquer um fecha o serviço depois: a tela "Conferir declaração"
lista o que está em aberto, sem precisar de identificador nenhum.

Aba "Declaracoes", na mesma planilha das notas:
  A id_dps | B numero | C card_id | D obra | E med | F ambiente
  G enviada_em | H status | I chave | J numero_nota | K observacao

`status`: aguardando | concluida | recusada
"""
from __future__ import annotations

import datetime

ABA = "Declaracoes"
CAB = ["id_dps", "numero", "card_id", "obra", "med", "ambiente",
       "enviada_em", "status", "chave", "numero_nota", "observacao"]

AGUARDANDO = "aguardando"
CONCLUIDA = "concluida"
RECUSADA = "recusada"

FUSO_BRASILIA = datetime.timezone(datetime.timedelta(hours=-3))


def _ws(planilha):
    try:
        return planilha.worksheet(ABA)
    except Exception:
        ws = planilha.add_worksheet(title=ABA, rows=2000, cols=len(CAB) + 2)
        ws.update("A1:K1", [CAB])
        return ws


def _linhas(ws) -> list[dict]:
    vals = ws.get_all_values()
    saida = []
    for i, row in enumerate(vals[1:], start=2):
        row = (row + [""] * len(CAB))[:len(CAB)]
        if not row[0].strip():
            continue
        d = dict(zip(CAB, row))
        d["_linha"] = i
        saida.append(d)
    return saida


def registrar(planilha, id_dps: str, numero, card_id: str, obra: str = "",
              med: str = "", producao: bool = True) -> bool:
    """Grava a declaração como AGUARDANDO. Não duplica se já estiver lá.

    Chamada **logo depois** de a prefeitura aceitar, antes de qualquer espera: é
    esse "antes" que garante que nada se perde se a tela morrer.
    """
    ws = _ws(planilha)
    id_dps = str(id_dps or "").strip()
    if not id_dps:
        return False
    if any(d["id_dps"].strip() == id_dps for d in _linhas(ws)):
        return False
    agora = datetime.datetime.now(FUSO_BRASILIA).strftime("%d/%m/%Y %H:%M")
    ws.append_row([id_dps, str(numero or ""), str(card_id or ""), obra, med,
                   "producao" if producao else "homologacao", agora,
                   AGUARDANDO, "", "", ""],
                  value_input_option="USER_ENTERED", table_range="A1")
    return True


# Depois de quantas horas uma declaração em aberto deixa de ser "fila" e passa a
# ser "travada". Veio de 07/10/2026: a fila da plataforma nacional costuma levar
# segundos, e uma declaração parada por horas é assunto para a prefeitura, não
# para esperar mais.
HORAS_ATE_SUSPEITAR = 2


def _horas_desde(texto: str):
    """Quantas horas desde 'DD/MM/AAAA HH:MM'. None se não der para ler."""
    try:
        quando = datetime.datetime.strptime(texto.strip(), "%d/%m/%Y %H:%M")
        quando = quando.replace(tzinfo=FUSO_BRASILIA)
        return (datetime.datetime.now(FUSO_BRASILIA) - quando).total_seconds() / 3600
    except Exception:
        return None


def listar_abertas(planilha) -> list[dict]:
    """As declarações que ainda não viraram nota nem foram recusadas.

    Cada uma vem com `horas_aberta` e `travada` — é o que permite a tela parar de
    dizer "espere" para algo que já está parado há horas.
    """
    saida = []
    for d in _linhas(_ws(planilha)):
        if d["status"].strip().lower() != AGUARDANDO:
            continue
        horas = _horas_desde(d.get("enviada_em", ""))
        d["horas_aberta"] = horas
        d["travada"] = bool(horas is not None and horas >= HORAS_ATE_SUSPEITAR)
        saida.append(d)
    return saida


def _atualizar(planilha, id_dps: str, status: str, chave="", numero_nota="",
               observacao="") -> bool:
    ws = _ws(planilha)
    alvo = str(id_dps or "").strip()
    for d in _linhas(ws):
        if d["id_dps"].strip() == alvo:
            ws.update(f"H{d['_linha']}:K{d['_linha']}",
                      [[status, chave, str(numero_nota or ""), observacao]],
                      value_input_option="USER_ENTERED")
            return True
    return False


def marcar_concluida(planilha, id_dps: str, numero_nota, chave: str) -> bool:
    return _atualizar(planilha, id_dps, CONCLUIDA, chave=chave, numero_nota=numero_nota)


def marcar_recusada(planilha, id_dps: str, motivos) -> bool:
    """Recusada quer dizer que NÃO existe nota: o número volta a estar livre, e a
    mesma declaração pode ser reenviada com a correção."""
    texto = "; ".join(motivos or [])[:400]
    return _atualizar(planilha, id_dps, RECUSADA, observacao=texto)
