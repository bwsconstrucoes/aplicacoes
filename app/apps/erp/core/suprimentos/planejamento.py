# ============================================================================
# ERP — core/suprimentos/planejamento.py
# O DISPARO AUTOMÁTICO DE COTAÇÃO: o sistema monta, a pessoa confere e dispara.
#
# PEDIDO DO DONO, 18/09/2026:
#
#   "A gente poderia receber uma demanda de suprimento e já disparar cotações.
#   Você já pega ali tudo que tem pendente, e o sistema, através de categoria
#   de fornecedor e tal, ele já planeja um disparo (…) Aí, quando ele montar,
#   naquela tela de disparo eu estou visualizando o que vai ser disparado e
#   para quem vai ser disparado. E a partir dali eu posso editar (…) Ele vai
#   apenas validar aquela sugestão do sistema, editar uma ou outra coisa e
#   disparar."
#
# O QUE ESTE MÓDULO É, E O QUE ELE NÃO É
#
# Ele NÃO decide comprar. Ele faz o trabalho BRAÇAL que hoje é do comprador:
# olhar tudo que está pendente, agrupar o que faz sentido pedir junto, e achar
# quem vende aquilo. A decisão — para quem mandar, o que tirar, o que juntar —
# continua com gente, na tela, antes de qualquer e-mail sair.
#
# E a distinção não é modéstia: é o que separa uma sugestão útil de um sistema
# que manda e-mail errado sozinho. O custo de uma sugestão ruim é um clique
# para desmarcar; o custo de um disparo errado é a empresa pedindo preço de
# cimento para quem vende cabo — e o fornecedor bom parando de responder.
#
# COMO ELE AGRUPA, e por quê:
#
#   por CATEGORIA DE INSUMO, porque é assim que o fornecedor é cadastrado e é
#   assim que ele responde — um pedido com cimento e luminária junto volta pela
#   metade dos dois lados;
#
#   e dentro da categoria, por OBRA quando as obras são de municípios
#   diferentes, porque frete e prazo mudam com a distância e um preço único
#   para duas pontas do estado não é um preço.
#
# A ORDEM é por urgência: o que já passou da previsão de entrega primeiro,
# depois o de prioridade ALTA, depois o resto. Quem abre a tela de manhã tem de
# ver em cima o que, se atrasar, para a obra.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.suprimentos import fornecedores as svc_forn
from app.apps.erp.db.models.cadastros import (
    Fornecedor, FornecedorCategoria, Insumo, InsumoCategoria, Obra,
    StatusItemSuprimento, SuprimentoItem, SuprimentoSolicitacao,
)

logger = logging.getLogger(__name__)

# Os estados em que o item AINDA PRECISA de cotação. Item que já está em
# análise de propostas ou adiante não entra: cotar de novo o que já foi cotado
# é o jeito mais rápido de o fornecedor achar que a BWS não se organiza.
PEDINDO_COTACAO = (StatusItemSuprimento.SOLICITACAO,
                   StatusItemSuprimento.SALA_TECNICA,
                   StatusItemSuprimento.COTACAO)

# Quantos fornecedores o sistema sugere por bloco. Três é o mínimo que a
# comparação exige e o máximo que a maioria responde — dez pedidos de preço
# para o mesmo item treinam o fornecedor a ignorar.
FORNECEDORES_POR_BLOCO = 4

URGENCIAS = {"ATRASADO": 0, "ALTA": 1, "MEDIA": 2, "NORMAL": 3}


def _hoje() -> date:
    return date.today()


def _urgencia(sol: SuprimentoSolicitacao, hoje: date) -> tuple[str, str]:
    """Quão urgente é isto, e por quê — dito em português.

    O motivo viaja junto porque a tela mostra a ordem, e ordem sem motivo
    parece arbitrária: quem lê precisa saber se o primeiro item está em cima
    porque atrasou ou porque alguém marcou "alta".
    """
    previsao = getattr(sol, "previsao_entrega", None)
    if previsao and previsao < hoje:
        return "ATRASADO", (f"a obra pediu para {previsao.strftime('%d/%m')} e "
                            f"já passou {(hoje - previsao).days} dia(s)")
    prioridade = getattr(sol, "prioridade", None)
    chave = prioridade.value if hasattr(prioridade, "value") else str(prioridade or "NORMAL")
    if chave == "ALTA":
        return "ALTA", "marcada como prioridade alta por quem pediu"
    if chave == "MEDIA":
        return "MEDIA", "prioridade média"
    if previsao:
        faltam = (previsao - hoje).days
        if faltam <= 7:
            return "MEDIA", f"a obra precisa em {faltam} dia(s)"
    return "NORMAL", ""


def planejar(s: Session, *, obras_permitidas: Optional[list[int]] = None,
             hoje: Optional[date] = None) -> dict[str, Any]:
    """O que o sistema PROPÕE disparar hoje, agrupado e com os fornecedores.

    Não grava nada. É a "tela de planejamento" que o dono pediu: ele olha, tira
    um fornecedor, acrescenta outro, desmarca um insumo — e só então dispara.
    """
    hoje = hoje or _hoje()

    itens = [i for i in s.scalars(select(SuprimentoItem)).all()
             if i.status in PEDINDO_COTACAO and (i.saldo or 0) > 0]

    # O QUE JÁ ESTÁ NUMA COTAÇÃO ABERTA FICA DE FORA. O status do item nem
    # sempre denuncia isso — item em SOLICITACAO pode ter sido posto numa
    # cotação hoje de manhã —, e sugerir o que a cotação recusaria na hora de
    # montar faz a tela prometer trabalho que sempre falha. Achado no primeiro
    # teste da tela, em 18/09/2026: nove blocos sugeridos, nove recusados.
    from app.apps.erp.core.suprimentos.cotacao import _itens_em_cotacao_aberta
    ja_cotando = _itens_em_cotacao_aberta(s, [i.id for i in itens])
    em_cotacao_aberta = len(ja_cotando)
    itens = [i for i in itens if i.id not in ja_cotando]
    if obras_permitidas is not None:
        alcance = set(obras_permitidas)
        itens = [i for i in itens if i.obra_id in alcance]
    if not itens:
        return {"blocos": [], "itens_pendentes": 0,
                "em_cotacao_aberta": em_cotacao_aberta, "sem_categoria": [],
                "observacao": (
                    "Nenhum item esperando cotação."
                    if not em_cotacao_aberta else
                    f"Nada novo: os {em_cotacao_aberta} item(ns) pendentes já "
                    f"estão numa cotação aberta.")}

    solicitacoes = {x.id: x for x in s.scalars(select(SuprimentoSolicitacao)).all()}
    insumos = {x.id: x for x in s.scalars(select(Insumo)).all()}
    obras = {o.id: o for o in s.scalars(select(Obra)).all()}
    categorias = {c.id: c for c in s.scalars(select(InsumoCategoria)).all()}
    catalogo_forn = svc_forn.gerenciar(s)["fornecedores"]

    # agrupa por (categoria do insumo, município da obra)
    blocos: dict[tuple[Any, str], dict[str, Any]] = {}
    sem_categoria: list[dict[str, Any]] = []
    for item in itens:
        insumo = insumos.get(item.insumo_id)
        # `categoria_insumo_id`, NÃO `categoria_id`: a segunda é a conta do
        # plano financeiro, e usá-la aqui buscaria fornecedor por conta
        # contábil — que não é como ninguém vende nada.
        categoria_id = getattr(insumo, "categoria_insumo_id", None) if insumo else None
        obra = obras.get(item.obra_id)
        municipio = (getattr(obra, "municipio", "") or "").strip().upper()
        sol = solicitacoes.get(item.solicitacao_id)
        urgencia, motivo = _urgencia(sol, hoje) if sol else ("NORMAL", "")

        linha = {
            "item_id": item.id,
            "solicitacao": getattr(sol, "numero", ""),
            "titulo": getattr(sol, "titulo", ""),
            "insumo_id": item.insumo_id,
            "insumo": getattr(insumo, "descricao", "") if insumo else "",
            "especificacao": item.especificacao or "",
            "quantidade": str(item.saldo),
            "unidade": item.unidade,
            "obra_id": item.obra_id,
            "obra": (f"{obra.codigo} — {obra.nome}" if obra else ""),
            "urgencia": urgencia, "motivo": motivo,
            "previsao": (sol.previsao_entrega.isoformat()
                         if sol and sol.previsao_entrega else None),
        }
        if categoria_id is None:
            # Insumo sem categoria não tem como achar fornecedor: é o mesmo
            # buraco que a tela de gestão já conta, aparecendo aqui com nome.
            sem_categoria.append(linha)
            continue

        chave = (categoria_id, municipio)
        bloco = blocos.setdefault(chave, {
            "categoria_id": categoria_id,
            "categoria": getattr(categorias.get(categoria_id), "nome", ""),
            "municipio": municipio or "(sem município na obra)",
            "itens": [], "urgencia": "NORMAL", "motivo": ""})
        bloco["itens"].append(linha)
        if URGENCIAS.get(urgencia, 9) < URGENCIAS.get(bloco["urgencia"], 9):
            bloco["urgencia"], bloco["motivo"] = urgencia, motivo

    saida = []
    for (categoria_id, municipio), bloco in blocos.items():
        bloco["fornecedores"] = _fornecedores_do_bloco(
            catalogo_forn, categoria_id, municipio)
        bloco["quantos_itens"] = len(bloco["itens"])
        bloco["obras"] = sorted({i["obra"] for i in bloco["itens"] if i["obra"]})
        saida.append(bloco)

    saida.sort(key=lambda b: (URGENCIAS.get(b["urgencia"], 9),
                              -b["quantos_itens"], b["categoria"]))
    return {
        "blocos": saida,
        "itens_pendentes": len(itens),
        "em_cotacao_aberta": em_cotacao_aberta,
        "sem_categoria": sem_categoria,
        "observacao": (
            "O sistema agrupou por CATEGORIA e por município da obra, e "
            "sugeriu quem vende cada coisa. Nada foi disparado: tire, "
            "acrescente e desmarque antes de mandar."
            + (f" {em_cotacao_aberta} item(ns) ficaram de fora por já estarem "
               f"numa cotação aberta." if em_cotacao_aberta else "")),
    }


def _fornecedores_do_bloco(catalogo: list[dict[str, Any]], categoria_id: int,
                           municipio: str) -> list[dict[str, Any]]:
    """Quem vende esta categoria, do mais provável para o menos.

    A ordem é uma OPINIÃO do sistema, e por isso cada linha carrega o porquê:
    quem é da mesma cidade entrega mais rápido, quem é fábrica costuma ter
    preço melhor em quantidade. Sem o porquê, a lista viraria um oráculo — e o
    comprador não teria como discordar com fundamento.
    """
    candidatos = []
    for f in catalogo:
        if not f["ativo"] or not f.get("cotacao_automatica", True):
            continue
        if categoria_id not in (f["categorias_ids"] or []):
            continue
        mesma_cidade = bool(municipio) and (f["municipio"] or "").upper() == municipio
        porte = f["porte"] or ""
        pontos = 0
        motivos = []
        if mesma_cidade:
            pontos += 3
            motivos.append("na mesma cidade da obra")
        if porte in ("FABRICA", "REP_FABRICA"):
            pontos += 2
            motivos.append("fábrica ou representante")
        elif porte == "DISTRIBUIDOR":
            pontos += 1
            motivos.append("distribuidor")
        elif porte == "LOCAL":
            motivos.append("fornecedor local")
        if not f["contatos"]:
            motivos.append("⚠ sem pessoa de contato")
            pontos -= 2
        if "EMAIL" in (f["canais"] or []) and not f["email"] and not any(
                c.get("email") for c in f["contatos"]):
            motivos.append("⚠ sem e-mail")
            pontos -= 3
        candidatos.append({
            "id": f["id"], "razao_social": f["razao_social"],
            "nome_fantasia": f["nome_fantasia"],
            "municipio": f["municipio"], "uf": f["uf"],
            "porte": porte, "porte_rotulo": f["porte_rotulo"],
            "contatos": f["contatos"],
            "pontos": pontos,
            "por_que": ", ".join(motivos) or "vende esta categoria",
            # O sistema MARCA os melhores; os outros ficam na lista para o
            # comprador acrescentar com um clique, sem procurar.
            "sugerido": False,
        })
    candidatos.sort(key=lambda c: (-c["pontos"], c["razao_social"]))
    for c in candidatos[:FORNECEDORES_POR_BLOCO]:
        c["sugerido"] = True
    return candidatos


# ---------------------------------------------------------------------------
# Do plano para as cotações de verdade
# ---------------------------------------------------------------------------
def criar_cotacoes(s: Session, blocos: list[dict[str, Any]], usuario) -> dict[str, Any]:
    """Cria UMA cotação por bloco confirmado, já com os fornecedores escolhidos.

    Recebe o que a TELA devolveu, e não o que o `planejar` sugeriu: a diferença
    é o ponto inteiro deste módulo. O comprador tirou um fornecedor, desmarcou
    um item, acrescentou outro — é essa versão que vira cotação.

    Um bloco que falhar não derruba os outros: a pessoa não deveria perder oito
    cotações boas por causa da nona.
    """
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.core.suprimentos import cotacao as svc_cot

    criadas, falhas = [], []
    for bloco in blocos or []:
        itens = [int(x) for x in (bloco.get("itens") or [])]
        fornecedores = [int(x) for x in (bloco.get("fornecedores") or [])]
        titulo = " ".join((bloco.get("titulo") or "").split())
        if not itens:
            continue
        if not titulo:
            titulo = (bloco.get("categoria") or "Cotação")[:60]
        try:
            cot = svc_cot.criar(s, {"titulo": titulo, "itens": itens,
                                    "observacoes": bloco.get("observacoes") or ""},
                                usuario)
            s.flush()
            for fid in fornecedores:
                svc_cot.adicionar_fornecedor(s, cot.id, {"fornecedor_id": fid}, usuario)
            s.flush()
            criadas.append({"id": cot.id, "numero": cot.numero, "titulo": titulo,
                            "itens": len(itens), "fornecedores": len(fornecedores)})
        except ErroValidacao as erro:
            falhas.append({"titulo": titulo, "motivo": str(erro)})
    return {
        "criadas": criadas, "falhas": falhas,
        "recado": (f"{len(criadas)} cotação(ões) montada(s)."
                   + (f" {len(falhas)} não deu(ram) certo." if falhas else "")
                   + " Elas ficam ABERTAS: o disparo dos e-mails continua sendo "
                     "um clique seu, na tela de Cotações."),
    }
