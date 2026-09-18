# ============================================================================
# ERP — core/suprimentos/desempenho.py
# A MEMÓRIA DO FORNECEDOR: o que vai tornar o sistema inteligente.
#
# PEDIDO DO DONO, 18/09/2026:
#
#   "A gente não pode já deixar ele pronto para isso? Enquanto não tem
#   inteligência, ele trabalha de forma automática. Eu entendi que o sistema
#   precisaria de dados para funcionar bem, mas enquanto nós não temos (…) aí
#   você já tem que deixar ele pronto para se tornar inteligente: os dados que
#   a gente vai trabalhar já estarem sendo guardados, para que os compradores
#   vão aos poucos alimentando."
#
# É EXATAMENTE A ORDEM CERTA, e vale escrever por quê: um sistema que ordena
# fornecedor por regra fixa ("mesma cidade vale 3 pontos") está chutando com
# educação. Um que ordena por resultado ("respondeu 9 das últimas 10, entregou
# no prazo em 8") está medindo. A diferença entre os dois NÃO é o algoritmo —
# é o histórico. E histórico não se compra: só se acumula, a partir do dia em
# que alguém começa a guardar.
#
# O QUE ESTE MÓDULO MEDE, e de onde tira cada coisa (nada aqui é digitado por
# ninguém — tudo sai do trabalho normal do comprador):
#
#   convites          quantas vezes o fornecedor entrou num mapa que foi
#                     disparado                     ← envios_email
#   respostas         quantas vezes respondeu, de qualquer jeito
#                                                   ← preço lançado, ou a marca
#                                                     "respondeu por WhatsApp"
#   dias para responder  a média                    ← envio → resposta
#   recusas           quantas vezes disse "não vou cotar"  ← sem_interesse
#   pedidos           quantas vezes ganhou a compra ← pedidos_compra
#   entregas          quantas chegaram              ← recebimentos
#   atraso médio      previsão de entrega × chegada ← pedido × recebimento
#
# A TRAVA QUE IMPEDE O NÚMERO BONITO E MENTIROSO: abaixo de `MINIMO_PARA_MEDIR`
# convites, este módulo devolve `confiavel=False` e quem chama IGNORA os
# números. Um fornecedor que foi chamado uma vez e respondeu tem 100% de
# resposta — e ordenar a lista por isso poria o desconhecido na frente de quem
# atende a empresa há dois anos. Enquanto não há amostra, vale a regra fixa.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import (
    CotacaoFornecedor, CotacaoPreco, EnvioEmail, PedidoCompra, PedidoItem,
    Recebimento, RecebimentoItem, StatusPedidoCompra,
)

logger = logging.getLogger(__name__)

# Abaixo disto os números existem mas não mandam em nada. Três é pouco para
# estatística e é muito para o comprador esperar — fica em três de propósito:
# o objetivo não é rigor acadêmico, é parar de tratar quem nunca respondeu
# igual a quem sempre responde.
MINIMO_PARA_MEDIR = 3

# O peso de cada coisa na ordenação, quando há amostra. São três números, e
# estão aqui em cima justamente para o dono poder discordar deles sem procurar.
PESO_RESPOSTA = 6      # responder é o mínimo: sem isso não há cotação
PESO_PRAZO = 4         # entregar no dia combinado é o que a obra sente
PESO_GANHOU = 2        # já ter vendido para nós diz que o preço fecha


def _primeiro_envio_por_coluna(s: Session) -> dict[int, datetime]:
    saida: dict[int, datetime] = {}
    for e in s.scalars(select(EnvioEmail)).all():
        if e.destinatario_tipo != "cotacao_fornecedor" or not e.destinatario_id:
            continue
        if (e.situacao or "") != "ENVIADO" or e.criado_em is None:
            continue
        atual = saida.get(e.destinatario_id)
        if atual is None or e.criado_em < atual:
            saida[e.destinatario_id] = e.criado_em
    return saida


def _dias(de: datetime, ate: datetime) -> int:
    a = de.date() if isinstance(de, datetime) else de
    b = ate.date() if isinstance(ate, datetime) else ate
    return max(0, (b - a).days)


def por_fornecedor(s: Session) -> dict[int, dict[str, Any]]:
    """O histórico de TODOS os fornecedores, de uma vez.

    De uma vez porque quem chama é o planejamento, que precisa de todos: pedir
    um por vez varreria as mesmas tabelas cinquenta vezes numa tela só.
    """
    envios = _primeiro_envio_por_coluna(s)
    com_preco: dict[int, datetime] = {}
    for p in s.scalars(select(CotacaoPreco)).all():
        quando = p.registrado_em
        if quando is None:
            continue
        atual = com_preco.get(p.cotacao_fornecedor_id)
        if atual is None or quando < atual:
            com_preco[p.cotacao_fornecedor_id] = quando

    saida: dict[int, dict[str, Any]] = {}

    def ficha(fid: int) -> dict[str, Any]:
        return saida.setdefault(fid, {
            "convites": 0, "respostas": 0, "recusas": 0, "sem_resposta": 0,
            "soma_dias_resposta": 0, "respostas_com_prazo": 0,
            "pedidos": 0, "entregas": 0, "entregas_no_prazo": 0,
            "soma_dias_atraso": 0, "entregas_com_prazo": 0,
        })

    for coluna in s.scalars(select(CotacaoFornecedor)).all():
        saiu = envios.get(coluna.id)
        if saiu is None:
            # Nunca foi disparada para ele: não é convite, e contar como tal
            # puniria o fornecedor por um mapa que o comprador montou e nunca
            # mandou.
            continue
        f = ficha(coluna.fornecedor_id)
        f["convites"] += 1
        if getattr(coluna, "sem_interesse", False):
            f["recusas"] += 1
            continue
        quando = com_preco.get(coluna.id) or coluna.respondido_em
        if quando is None:
            f["sem_resposta"] += 1
            continue
        f["respostas"] += 1
        f["soma_dias_resposta"] += _dias(saiu, quando)
        f["respostas_com_prazo"] += 1

    # --- o que aconteceu DEPOIS da compra ---------------------------------
    itens_do_pedido: dict[int, list[int]] = {}
    for it in s.scalars(select(PedidoItem)).all():
        itens_do_pedido.setdefault(it.pedido_id, []).append(it.id)
    chegada_do_pedido: dict[int, date] = {}
    recebimentos = {r.id: r for r in s.scalars(select(Recebimento)).all()}
    itens_recebidos: dict[int, set[int]] = {}
    for ri in s.scalars(select(RecebimentoItem)).all():
        rec = recebimentos.get(ri.recebimento_id)
        if rec is None:
            continue
        itens_recebidos.setdefault(rec.pedido_id, set()).add(ri.pedido_item_id)
        atual = chegada_do_pedido.get(rec.pedido_id)
        if atual is None or (rec.data and rec.data > atual):
            chegada_do_pedido[rec.pedido_id] = rec.data

    for ped in s.scalars(select(PedidoCompra)).all():
        if ped.status in (StatusPedidoCompra.RECUSADO, StatusPedidoCompra.CANCELADO):
            continue
        f = ficha(ped.fornecedor_id)
        f["pedidos"] += 1
        esperados = set(itens_do_pedido.get(ped.id) or [])
        chegaram = itens_recebidos.get(ped.id) or set()
        # Só conta entrega quando o pedido chegou INTEIRO: contar a primeira
        # remessa como "entregue" diria que o fornecedor que mandou 10% no
        # prazo cumpriu o prazo.
        if not esperados or not esperados.issubset(chegaram):
            continue
        f["entregas"] += 1
        if ped.previsao_entrega and chegada_do_pedido.get(ped.id):
            f["entregas_com_prazo"] += 1
            atraso = (chegada_do_pedido[ped.id] - ped.previsao_entrega).days
            if atraso <= 0:
                f["entregas_no_prazo"] += 1
            else:
                f["soma_dias_atraso"] += atraso

    for fid, f in saida.items():
        _fechar(f)
    return saida


def _fechar(f: dict[str, Any]) -> None:
    """Transforma as somas em números que uma pessoa lê, e diz se dá para
    confiar neles."""
    chamadas = f["convites"]
    f["confiavel"] = chamadas >= MINIMO_PARA_MEDIR
    respondeu = f["respostas"] + f["recusas"]
    f["taxa_resposta"] = round(respondeu / chamadas, 2) if chamadas else None
    f["dias_resposta"] = (round(f["soma_dias_resposta"] / f["respostas_com_prazo"], 1)
                          if f["respostas_com_prazo"] else None)
    f["taxa_prazo"] = (round(f["entregas_no_prazo"] / f["entregas_com_prazo"], 2)
                       if f["entregas_com_prazo"] else None)
    atrasadas = f["entregas_com_prazo"] - f["entregas_no_prazo"]
    f["dias_atraso"] = (round(f["soma_dias_atraso"] / atrasadas, 1)
                        if atrasadas > 0 else None)
    f["pontos"] = _pontos(f)
    f["resumo"] = _resumo(f)


def _pontos(f: dict[str, Any]) -> int:
    """O que o histórico acrescenta (ou tira) da ordenação do planejamento.

    Zero quando não há amostra — e é isso que faz o sistema continuar
    funcionando por regra fixa enquanto a memória não existe, sem nenhuma
    troca de chave e sem ninguém precisar lembrar de ligar nada.
    """
    if not f["confiavel"]:
        return 0
    pontos = 0
    taxa = f["taxa_resposta"]
    if taxa is not None:
        # 100% responde → +6; 50% → 0; 0% → −6. Quem nunca responde tem de
        # cair na lista, não só deixar de subir.
        pontos += round(PESO_RESPOSTA * (taxa - 0.5) * 2)
    prazo = f["taxa_prazo"]
    if prazo is not None:
        pontos += round(PESO_PRAZO * (prazo - 0.5) * 2)
    if f["pedidos"] > 0:
        pontos += PESO_GANHOU
    return int(pontos)


def _resumo(f: dict[str, Any]) -> str:
    """A frase que vai na tela, ao lado do nome. Números sem frase viram
    oráculo, e contra oráculo ninguém discorda com fundamento."""
    if not f["confiavel"]:
        faltam = MINIMO_PARA_MEDIR - f["convites"]
        base = (f"pouco histórico ({f['convites']} cotação(ões)) — "
                f"faltam {faltam} para o sistema medir")
        # Fornecedor de COMPRA DIRETA: nunca foi cotado, mas já vendeu. Dizer
        # só "0 cotações" faria parecer que nunca trabalhamos com ele.
        if f["pedidos"]:
            base += f" · já vendeu para nós {f['pedidos']} vez(es)"
        return base
    partes = [f"respondeu {f['respostas'] + f['recusas']} de {f['convites']}"]
    if f["dias_resposta"] is not None:
        partes.append(f"em média {f['dias_resposta']} dia(s)")
    if f["taxa_prazo"] is not None:
        partes.append(f"{f['entregas_no_prazo']} de {f['entregas_com_prazo']} "
                      f"entrega(s) no prazo")
    elif f["pedidos"]:
        partes.append(f"{f['pedidos']} pedido(s) fechado(s)")
    return " · ".join(partes)


def de_um(s: Session, fornecedor_id: int) -> dict[str, Any]:
    """A ficha de um fornecedor só, para a tela de cadastro."""
    todos = por_fornecedor(s)
    f = todos.get(int(fornecedor_id))
    if f is None:
        f = {"convites": 0, "respostas": 0, "recusas": 0, "sem_resposta": 0,
             "soma_dias_resposta": 0, "respostas_com_prazo": 0, "pedidos": 0,
             "entregas": 0, "entregas_no_prazo": 0, "soma_dias_atraso": 0,
             "entregas_com_prazo": 0}
        _fechar(f)
        f["resumo"] = "nunca foi chamado para cotar"
    return f
