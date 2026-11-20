# ============================================================================
# ERP — core/suprimentos/recebimento.py
# O material chegou na obra.
#
# Quem confere é a OBRA, não o suprimento — é lá que o material desce. E o que
# não chegou vira PENDÊNCIA, que aqui é o SALDO DO PRÓPRIO ITEM, e não um
# registro novo em outra tabela.
#
# Por que assim: com duas tabelas, o mesmo material passa a ter duas verdades e
# o histórico se perde. Com saldo, dá para contar a história inteira — "este
# item foi pedido em janeiro, chegou pela metade em fevereiro, o resto em
# março" — e o que falta pode entrar numa cotação nova sem perder a origem.
# É o mesmo padrão que já funciona na medição de empreita.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    PedidoCompra, PedidoItem, PrevisaoPagamento, Recebimento, RecebimentoItem,
    StatusItemSuprimento, StatusPedidoCompra, SuprimentoItem, Usuario,
)

logger = logging.getLogger(__name__)


def _quantidade(valor: Any, campo: str) -> Decimal:
    texto = str(valor if valor is not None else "").strip()
    if not texto:
        raise ErroValidacao(f"{campo} é obrigatória.")
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto)
    except InvalidOperation:
        raise ErroValidacao(f"{campo} tem de ser um número.")


def registrar(s: Session, pedido_id: int, dados: dict[str, Any],
              usuario: Usuario) -> Recebimento:
    """Registra o que chegou. Aceita recebimento parcial — que é o normal."""
    pedido = s.get(PedidoCompra, pedido_id, with_for_update=True, populate_existing=True)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    if pedido.status is not StatusPedidoCompra.AUTORIZADO:
        raise ErroValidacao(
            f"O pedido {pedido.numero} não está autorizado — não há o que receber.")

    entradas = dados.get("itens") or []
    if not entradas:
        raise ErroValidacao("Diga o que chegou.")

    data_recebimento = date.today()
    if dados.get("data"):
        try:
            data_recebimento = date.fromisoformat(str(dados["data"])[:10])
        except ValueError:
            raise ErroValidacao("Data do recebimento inválida.")
    if data_recebimento > date.today():
        raise ErroValidacao("Não dá para receber material no futuro.")

    linhas_do_pedido = {i.id: i for i in s.scalars(select(PedidoItem)).all()
                        if i.pedido_id == pedido.id}
    # Quem está preso a obra só recebe material DAS OBRAS DELE. None = vê tudo.
    obras_da_pessoa = _obras_de_quem_recebe(s, usuario)

    recebimento = Recebimento(
        pedido_id=pedido.id, obra_id=dados.get("obra_id") or None,
        data=data_recebimento,
        nota_numero=(dados.get("nota_numero") or "").strip() or None,
        anexo_id=dados.get("anexo_id") or None,
        observacoes=(dados.get("observacoes") or "").strip() or None,
        recebido_por=usuario.id)
    s.add(recebimento)
    s.flush()

    resumo = []
    for entrada in entradas:
        linha = linhas_do_pedido.get(int(entrada.get("pedido_item_id") or 0))
        if linha is None:
            raise ErroValidacao("Item não pertence a este pedido.")
        quantidade = _quantidade(entrada.get("quantidade"), "Quantidade recebida")
        if quantidade <= 0:
            continue                      # nada a registrar nesta linha
        item = s.get(SuprimentoItem, linha.suprimento_item_id,
                     with_for_update=True, populate_existing=True)
        if item is None:
            raise ErroValidacao("Item da solicitação não encontrado.")
        if obras_da_pessoa is not None and item.obra_id not in obras_da_pessoa:
            # 404, e não "sem permissão": dizer que o item existe e é de outra
            # obra já seria contar o que a pessoa não pode ver.
            raise ErroNaoEncontrado("Pedido não encontrado.")

        saldo = Decimal(str(item.quantidade)) - Decimal(str(item.quantidade_recebida or 0))
        if quantidade > saldo:
            raise ErroValidacao(
                f"Chegou mais do que foi pedido: {quantidade} para um saldo de "
                f"{saldo}. Confira a nota — sobra não é recebimento, é outra conversa.")

        item.quantidade_recebida = (Decimal(str(item.quantidade_recebida or 0))
                                    + quantidade)
        novo_saldo = Decimal(str(item.quantidade)) - item.quantidade_recebida
        item.status = (StatusItemSuprimento.RECEBIDO if novo_saldo == 0
                       else StatusItemSuprimento.PENDENCIA)
        s.add(RecebimentoItem(recebimento_id=recebimento.id, pedido_item_id=linha.id,
                              quantidade=quantidade))
        resumo.append({"item": item.id, "recebido": str(quantidade),
                       "saldo": str(novo_saldo)})
    s.flush()

    if not resumo:
        raise ErroValidacao("Nenhuma quantidade informada.")

    registrar_evento(s, "pedido_compra", pedido.id, "RECEBIMENTO",
                     {"recebimento_id": recebimento.id, "nota": recebimento.nota_numero,
                      "itens": resumo}, usuario.id)
    return recebimento


def situacao(s: Session, pedido_id: int) -> dict[str, Any]:
    """Como está o pedido dos dois lados — o que chegou e o que já é dívida.

    É o encontro entre suprimento e financeiro: o comprador precisa ver as DUAS
    pendências no mesmo lugar, senão descobre tarde que a obra não lançou.
    """
    from app.apps.erp.db.models.cadastros import Insumo

    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")

    itens = []
    tudo_recebido = True
    for linha in sorted([i for i in s.scalars(select(PedidoItem)).all()
                         if i.pedido_id == pedido_id], key=lambda x: x.numero or 0):
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        pedido_qtd = Decimal(str(linha.quantidade))
        recebido = Decimal(str(getattr(item, "quantidade_recebida", 0) or 0))
        saldo = pedido_qtd - recebido
        if saldo > 0:
            tudo_recebido = False
        itens.append({
            "pedido_item_id": linha.id,
            "insumo": getattr(insumo, "descricao", ""),
            "especificacao": getattr(item, "especificacao", None),
            "unidade": getattr(item, "unidade", ""),
            "pedido": str(pedido_qtd), "recebido": str(recebido), "saldo": str(saldo),
            "status": item.status.value if item is not None and item.status else None,
        })

    previsoes = sorted([p for p in s.scalars(select(PrevisaoPagamento)).all()
                        if p.pedido_id == pedido_id], key=lambda x: x.numero or 0)
    sem_titulo = [p for p in previsoes if not p.titulo_id]
    vencidas_sem_material = [
        {"numero": p.numero, "vencimento": p.vencimento.isoformat(), "valor": str(p.valor)}
        for p in sem_titulo
        if p.vencimento and p.vencimento <= date.today() and not tudo_recebido]

    avisos = []
    if vencidas_sem_material:
        avisos.append(
            "Há parcela vencida com material ainda não recebido por inteiro. "
            "Antes de pagar, confirme com o suprimento.")
    if tudo_recebido and sem_titulo:
        avisos.append(
            "Material recebido e a parcela ainda não virou título no financeiro. "
            "Falta lançar a nota.")
    if (pedido.previsao_entrega and pedido.previsao_entrega < date.today()
            and not tudo_recebido):
        avisos.append(
            f"A entrega estava prevista para {pedido.previsao_entrega.isoformat()} "
            "e o material não chegou. Peça nova previsão ao fornecedor.")

    return {
        "pedido_id": pedido.id, "numero": pedido.numero,
        "tudo_recebido": tudo_recebido,
        "itens": itens,
        "previsoes": [{"numero": p.numero, "vencimento": p.vencimento.isoformat(),
                       "valor": str(p.valor), "titulo_id": p.titulo_id}
                      for p in previsoes],
        # Não bloqueia nada: avisa, como as demais críticas do ERP.
        "avisos": avisos,
    }


def pendencias(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """Os itens que ficaram com saldo — o que a obra ainda espera.

    Pendência tem tratamento mais urgente que solicitação nova: já foi pedida
    uma vez, e alguém está esperando.
    """
    from app.apps.erp.core.suprimentos.solicitacao import listar_itens
    return [i for i in listar_itens(s, usuario)
            if i["status"] == StatusItemSuprimento.PENDENCIA.value]


# ---------------------------------------------------------------------------
# A FILA DA OBRA (20/11/2026)
#
# O dono: "o material precisa ser recebido pela obra pra poder anexar a nota e
# lançar boletos" — e "o recebimento do material deve identificar o que foi
# pedido e o que foi efetivamente entregue".
#
# Até aqui o recebimento só existia dentro da tela de Pedidos, que a obra não
# abre. Esta é a porta dela: o que está para chegar NAS OBRAS DELA, item a
# item, sem preço — quem recebe confere quantidade, não valor.
# ---------------------------------------------------------------------------
def _obras_de_quem_recebe(s: Session, usuario: Usuario) -> Optional[set[int]]:
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    if usuario is None or getattr(usuario, "perfil", None) is None:
        return None            # chamada interna / dublê sem perfil: sem recorte
    obras = obras_de_registro_sem_autor(s, usuario)
    return None if obras is None else set(obras)


def exigir_pedido_no_escopo(s: Session, usuario: Usuario, pedido_id: int) -> None:
    """O pedido tem ao menos um item numa obra desta pessoa? Senão, 404."""
    obras = _obras_de_quem_recebe(s, usuario)
    if obras is None:
        return
    do_pedido = s.scalars(
        select(SuprimentoItem.obra_id)
        .join(PedidoItem, PedidoItem.suprimento_item_id == SuprimentoItem.id)
        .where(PedidoItem.pedido_id == pedido_id)).all()
    if not (set(do_pedido) & obras):
        raise ErroNaoEncontrado("Pedido não encontrado.")


def a_receber(s: Session, usuario: Usuario, *,
              hoje: Optional[date] = None) -> dict[str, Any]:
    """O que está para chegar nas obras da pessoa, e o que chegou sem nota.

    A segunda lista é o lembrete que ataca o problema na origem: "muitas vezes
    chega material e a obra atrasa o lançamento das notas e boleto". Quem
    recebeu vê, na mesma tela, que ainda deve a nota.
    """
    from app.apps.erp.core.suprimentos import acompanhamento as acomp
    from app.apps.erp.core.suprimentos.entrega import (
        endereco_da_obra, rotulo_da_obra)
    from app.apps.erp.db.models.cadastros import Obra

    hoje = hoje or date.today()
    obras = _obras_de_quem_recebe(s, usuario)
    pedidos = acomp.pedidos_no_escopo(s, usuario, (StatusPedidoCompra.AUTORIZADO,))
    base = acomp.carregar(s, pedidos)

    cadastro_obras: dict[int, Any] = {}
    esperando, sem_nota = [], []
    for pedido in pedidos:
        itens = [i for i in base["itens"].get(pedido.id, [])
                 if obras is None or i["obra_id"] in obras]
        retrato = acomp.retrato(pedido, base, hoje)
        if retrato["nota"]["recebido_sem_nota"] > 0:
            sem_nota.append({
                "pedido_id": pedido.id, "numero": pedido.numero,
                "fornecedor": retrato["fornecedor"],
                "chegou_em": retrato["entrega"]["ultima_chegada"],
                "dias": retrato["nota"]["dias_sem_nota"],
                "obras": retrato["obras"]})
        com_saldo = [i for i in itens if i["saldo"] > 0]
        if not com_saldo:
            continue
        desvios = {d.obra_id: d for d in base["desvios"].get(pedido.id, [])}
        linhas = []
        for i in itens:
            if i["obra_id"] not in cadastro_obras:
                cadastro_obras[i["obra_id"]] = s.get(Obra, i["obra_id"])
            obra = cadastro_obras[i["obra_id"]]
            desvio = desvios.get(i["obra_id"])
            linhas.append({
                "pedido_item_id": i["pedido_item_id"], "insumo": i["insumo"],
                "especificacao": i["especificacao"], "unidade": i["unidade"],
                "obra_id": i["obra_id"], "obra": rotulo_da_obra(obra),
                "solicitacao": i["solicitacao"],
                "pedido": float(i["quantidade"]), "recebido": float(i["recebido"]),
                "saldo": float(i["saldo"]),
                "entrega_em": (f"{desvio.rotulo} — {desvio.endereco}" if desvio
                               else endereco_da_obra(obra)),
                "fora_da_obra": desvio is not None,
            })
        esperando.append({
            "pedido_id": pedido.id, "numero": pedido.numero,
            "fornecedor": retrato["fornecedor"],
            "previsao_entrega": retrato["previsao_entrega"],
            "dias_de_atraso": retrato["entrega"]["dias_de_atraso"],
            "ja_chegou_parte": retrato["entrega"]["chegadas"] > 0,
            "observacoes": pedido.observacoes,
            "itens": linhas,
        })
    # O atrasado primeiro, depois o que tem data mais próxima.
    esperando.sort(key=lambda p: (-p["dias_de_atraso"],
                                  p["previsao_entrega"] or "9999"))
    sem_nota.sort(key=lambda p: -p["dias"])
    return {"hoje": hoje.isoformat(), "esperando": esperando, "sem_nota": sem_nota}
