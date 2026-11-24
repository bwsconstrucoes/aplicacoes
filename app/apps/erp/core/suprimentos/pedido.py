# ============================================================================
# ERP — core/suprimentos/pedido.py
# O pedido de compra: fechamento, autorização e a previsão de pagamento.
#
# Duas regras que o dono deixou claras e que este módulo existe para cumprir:
#
#   1. O COMPRADOR NÃO COMPRA SOZINHO. O pedido nasce aguardando autorização,
#      seja ele fechado a partir do mapa ou direto, sem cotação.
#   2. PEDIDO AUTORIZADO GERA PREVISÃO DE PAGAMENTO — e antes de autorizado não
#      gera nada. Um pedido que ninguém liberou não é obrigação da empresa.
#
# A previsão não é título: vira título quando a nota fiscal chegar. Separar as
# duas coisas é o que deixa claro, no financeiro, o que já é dívida documentada
# e o que ainda é compromisso assumido.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.core.suprimentos.cotacao import registrar_no_banco_de_precos
from app.apps.erp.core.suprimentos.pagamento import gerar_parcelas
from app.apps.erp.db.models.cadastros import (
    CondicaoPagamento, Cotacao, CotacaoFornecedor, CotacaoItem, CotacaoPreco,
    Fornecedor, ModoEntrega, PedidoCompra, PedidoItem, PedidoItemReserva,
    PrevisaoPagamento, StatusItemSuprimento, StatusPedidoCompra, SuprimentoItem,
    TipoPreco, Usuario,
)

logger = logging.getLogger(__name__)

CENTAVO = Decimal("0.01")
VIVOS = (StatusPedidoCompra.AGUARDANDO_AUTORIZACAO, StatusPedidoCompra.AUTORIZADO)


def _agora() -> datetime:
    return datetime.now(timezone.utc)


def _numero(valor: Any, campo: str) -> Decimal:
    texto = str(valor if valor is not None else "").strip()
    if not texto:
        raise ErroValidacao(f"{campo} é obrigatório.")
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto)
    except InvalidOperation:
        raise ErroValidacao(f"{campo} tem de ser um número.")


def proximo_numero(s: Session) -> str:
    numeros = []
    for p in s.scalars(select(PedidoCompra)).all():
        numero = getattr(p, "numero", "") or ""
        sufixo = numero.split("-", 1)[1] if numero.startswith("PC-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return f"PC-{(max(numeros) + 1) if numeros else 1:04d}"


# ---------------------------------------------------------------------------
# Fechar
# ---------------------------------------------------------------------------
def fechar_do_mapa(s: Session, cotacao_id: int, cotacao_fornecedor_id: int,
                   cotacao_itens: list[int], dados: dict[str, Any],
                   usuario: Usuario) -> PedidoCompra:
    """Fecha com UM fornecedor os itens escolhidos no mapa.

    Um mesmo mapa gera vários pedidos, um por fornecedor. Os itens fechados
    saem de disputa; os não fechados continuam disponíveis para outro pedido.
    """
    coluna = s.get(CotacaoFornecedor, cotacao_fornecedor_id)
    if coluna is None or coluna.cotacao_id != cotacao_id:
        raise ErroNaoEncontrado("Fornecedor não está neste mapa.")
    if not cotacao_itens:
        raise ErroValidacao("Escolha os itens que vão neste pedido.")

    precos = {(p.cotacao_fornecedor_id, p.cotacao_item_id): p
              for p in s.scalars(select(CotacaoPreco).where(
                  CotacaoPreco.cotacao_fornecedor_id == coluna.id)).all()}
    linhas = []
    for linha_id in cotacao_itens:
        linha = s.get(CotacaoItem, int(linha_id))
        if linha is None or linha.cotacao_id != cotacao_id:
            raise ErroValidacao(f"Item {linha_id} não é deste mapa.")
        preco = precos.get((coluna.id, linha.id))
        if preco is None:
            raise ErroValidacao(
                "Não dá para fechar um item sem preço deste fornecedor.")
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        if item is None:
            raise ErroValidacao("Item da solicitação não encontrado.")
        linhas.append((item, Decimal(str(preco.preco_unitario))))

    pedido = _criar(s, fornecedor_id=coluna.fornecedor_id, linhas=linhas,
                    dados={**dados,
                           "condicao_pagamento_id": dados.get("condicao_pagamento_id")
                                                    or coluna.condicao_pagamento_id,
                           "entrega": dados.get("entrega")
                                      or (coluna.entrega.value if coluna.entrega else None),
                           "frete": dados.get("frete") if dados.get("frete") is not None
                                    else coluna.frete,
                           "desconto": dados.get("desconto") if dados.get("desconto") is not None
                                       else coluna.desconto},
                    usuario=usuario, cotacao_id=cotacao_id,
                    contato_id=coluna.contato_id)
    return pedido


def fechar_direto(s: Session, dados: dict[str, Any], usuario: Usuario) -> PedidoCompra:
    """Pedido sem mapa — valor pequeno, ou já se sabe o melhor preço."""
    itens = dados.get("itens") or []
    if not itens:
        raise ErroValidacao("Escolha os itens que vão neste pedido.")
    # ⚑ SEM COTAÇÃO, COM JUSTIFICATIVA (22/11/2026). O dono: "digamos que a
    # gente compra um insumo e sabe que só tem um fornecedor (...) eu já fecho
    # direto. Mas o compras fecha encaminhando para autorização." A
    # justificativa é o que quem autoriza lê para decidir.
    justificativa = " ".join(str(dados.get("justificativa_sem_cotacao") or "").split())
    if len(justificativa) < 10:
        raise ErroValidacao(
            "Escreva por que este pedido está sendo fechado sem cotação "
            "(ex.: fornecedor único na região). Quem autoriza vai ler.")
    linhas = []
    for bruto in itens:
        item = s.get(SuprimentoItem, int(bruto.get("suprimento_item_id") or 0))
        if item is None:
            raise ErroValidacao("Item da solicitação não encontrado.")
        preco = _numero(bruto.get("preco_unitario"), "Preço")
        if preco <= 0:
            raise ErroValidacao("Preço tem de ser maior que zero.")
        linhas.append((item, preco))
    pedido = _criar(s, fornecedor_id=int(dados.get("fornecedor_id") or 0),
                    linhas=linhas, dados=dados, usuario=usuario)
    pedido.justificativa_sem_cotacao = justificativa
    return pedido


def _criar(s: Session, *, fornecedor_id: int, linhas: list[tuple[SuprimentoItem, Decimal]],
           dados: dict[str, Any], usuario: Usuario,
           cotacao_id: Optional[int] = None,
           contato_id: Optional[int] = None) -> PedidoCompra:
    forn = s.get(Fornecedor, fornecedor_id)
    if forn is None:
        raise ErroValidacao("Fornecedor não encontrado.")

    from app.apps.erp.core.suprimentos.solicitacao import exigir_liberados
    exigir_liberados([i for i, _ in linhas])
    ids_das_linhas = [i.id for i, _ in linhas]
    reservados = {r.suprimento_item_id for r in s.scalars(
        select(PedidoItemReserva).where(
            PedidoItemReserva.suprimento_item_id.in_(ids_das_linhas or [0]))).all()}
    repetidos = [i.id for i, _ in linhas if i.id in reservados]
    if repetidos:
        raise ErroValidacao(
            f"O item {repetidos[0]} já está num pedido em aberto. "
            "Comprar duas vezes o mesmo material é o erro que isto evita.")

    antecipado = bool(dados.get("antecipado"))
    if antecipado and not _tem_dados_de_pagamento(s, forn):
        raise ErroValidacao(
            f"{forn.razao_social} não tem chave Pix nem dados bancários no "
            "cadastro. Compra à vista sem isso não tem como ser paga.")

    entrega = (dados.get("entrega") or "").strip().upper() if dados.get("entrega") else None
    if entrega and entrega not in {m.value for m in ModoEntrega}:
        raise ErroValidacao("Modo de entrega inválido.")

    previsao = None
    if dados.get("previsao_entrega"):
        try:
            previsao = date.fromisoformat(str(dados["previsao_entrega"])[:10])
        except ValueError:
            raise ErroValidacao("Data de previsão de entrega inválida.")

    pedido = PedidoCompra(
        numero=proximo_numero(s), cotacao_id=cotacao_id, fornecedor_id=forn.id,
        contato_id=contato_id or (int(dados["contato_id"]) if dados.get("contato_id") else None),
        condicao_pagamento_id=(int(dados["condicao_pagamento_id"])
                               if dados.get("condicao_pagamento_id") else None),
        entrega=ModoEntrega(entrega) if entrega else None,
        frete=Decimal(str(dados.get("frete") or 0)),
        desconto=Decimal(str(dados.get("desconto") or 0)),
        previsao_entrega=previsao, antecipado=antecipado,
        codigo_barras=(dados.get("codigo_barras") or "").strip() or None,
        observacoes=(dados.get("observacoes") or "").strip() or None,
        status=StatusPedidoCompra.AGUARDANDO_AUTORIZACAO, criado_por=usuario.id)
    s.add(pedido)
    s.flush()

    for n, (item, preco) in enumerate(linhas, start=1):
        s.add(PedidoItem(pedido_id=pedido.id, suprimento_item_id=item.id, numero=n,
                         quantidade=item.quantidade, preco_unitario=preco))
        s.add(PedidoItemReserva(suprimento_item_id=item.id, pedido_id=pedido.id))
        item.status = StatusItemSuprimento.AUTORIZACAO
    s.flush()

    # ONDE DESCE (migração 120). A regra é a obra; o desvio — a sede, um
    # depósito — é decidido aqui, por quem fecha, e por obra dentro do pedido.
    from app.apps.erp.core.suprimentos.entrega import definir_locais
    locais = definir_locais(s, pedido, dados.get("locais_entrega"),
                            {i.obra_id for i, _ in linhas}, usuario)

    registrar_evento(s, "pedido_compra", pedido.id, "FECHADO",
                     {"numero": pedido.numero, "fornecedor_id": forn.id,
                      "itens": len(linhas), "total": str(total(s, pedido)),
                      "cotacao_id": cotacao_id, "antecipado": antecipado,
                      "entrega_fora_da_obra": locais},
                     usuario.id)
    return pedido


def redefinir_entrega(s: Session, pedido_id: int, locais: list[dict[str, Any]],
                      usuario: Usuario) -> PedidoCompra:
    """Troca o local de entrega depois do fechamento.

    Vale enquanto o pedido está de pé e NADA chegou: depois do primeiro
    recebimento o material já desceu em algum lugar, e trocar o endereço no
    papel faria o pedido contar uma história que não aconteceu.
    """
    from app.apps.erp.core.suprimentos.entrega import definir_locais
    from app.apps.erp.db.models.cadastros import Recebimento

    pedido = s.get(PedidoCompra, pedido_id, with_for_update=True, populate_existing=True)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    if pedido.status not in VIVOS:
        raise ErroValidacao(f"O pedido {pedido.numero} já está "
                            f"{pedido.status.value.lower().replace('_', ' ')}.")
    if any(r.pedido_id == pedido.id for r in s.scalars(
            select(Recebimento).where(Recebimento.pedido_id == pedido.id)).all()):
        raise ErroValidacao(
            "Já chegou material deste pedido. O local de entrega não muda mais "
            "— o que falta resolver vai na observação do recebimento.")
    obras = set()
    for linha in _itens(s, pedido.id):
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        if item is not None:
            obras.add(item.obra_id)
    gravados = definir_locais(s, pedido, locais, obras, usuario)
    registrar_evento(s, "pedido_compra", pedido.id, "ENTREGA_REDEFINIDA",
                     {"entrega_fora_da_obra": gravados}, usuario.id)
    return pedido


def locais_possiveis(s: Session, obras_ids: list[int]) -> list[dict[str, Any]]:
    """Para cada obra: o endereço do canteiro e a sede que pode recebê-lo no
    lugar. É o que o fechamento — do mapa ou direto — oferece em "onde desce"."""
    from app.apps.erp.core.suprimentos.entrega import (
        endereco_da_obra, rotulo_da_obra, sede_da_obra)
    from app.apps.erp.db.models.cadastros import Obra
    saida = []
    for obra_id in sorted({int(x) for x in obras_ids if x}):
        obra = s.get(Obra, obra_id)
        if obra is None:
            continue
        saida.append({"id": obra_id, "rotulo": rotulo_da_obra(obra),
                      "endereco": endereco_da_obra(obra),
                      "sede": sede_da_obra(s, obra)})
    return saida


def preparar_fechamento(s: Session, cotacao_id: int) -> dict[str, Any]:
    """O que a tela precisa para FECHAR um pedido a partir do mapa.

    Por fornecedor: o que ele cotou, a que preço, e o que ainda está livre
    (item que já entrou em outro pedido não pode ser fechado de novo). Por
    obra: o endereço do canteiro e a sede que pode recebê-lo no lugar.
    """
    from app.apps.erp.core.suprimentos.cotacao import montar_mapa
    from app.apps.erp.core.suprimentos.entrega import (
        endereco_da_obra, rotulo_da_obra, sede_da_obra)
    from app.apps.erp.db.models.cadastros import Obra

    mapa = montar_mapa(s, cotacao_id)
    ids_itens = [i["suprimento_item_id"] for i in mapa["itens"]]
    reservados = {r.suprimento_item_id: r.pedido_id for r in s.scalars(
        select(PedidoItemReserva).where(
            PedidoItemReserva.suprimento_item_id.in_(ids_itens or [0]))).all()
        if r.suprimento_item_id in ids_itens}
    numeros = {}
    for pedido_id in set(reservados.values()):
        p = s.get(PedidoCompra, pedido_id)
        numeros[pedido_id] = getattr(p, "numero", "")

    obras: dict[int, dict[str, Any]] = {}
    fornecedores = []
    for coluna in mapa["fornecedores"]:
        bruta = s.get(CotacaoFornecedor, coluna["id"])
        itens = []
        for i in mapa["itens"]:
            celula = (i.get("precos") or {}).get(coluna["id"])
            if not celula:
                continue
            item = s.get(SuprimentoItem, i["suprimento_item_id"])
            obra_id = getattr(item, "obra_id", None)
            if obra_id and obra_id not in obras:
                obra = s.get(Obra, obra_id)
                obras[obra_id] = {"id": obra_id, "rotulo": rotulo_da_obra(obra),
                                  "endereco": endereco_da_obra(obra),
                                  "sede": sede_da_obra(s, obra)}
            preso = reservados.get(i["suprimento_item_id"])
            itens.append({
                "cotacao_item_id": i["id"], "insumo": i["insumo"],
                "especificacao": i.get("especificacao"),
                "quantidade": i["quantidade"], "unidade": i["unidade"],
                "obra_id": obra_id, "obra": i.get("obra"),
                "preco_unitario": celula["preco_unitario"], "total": celula["total"],
                "menor": i.get("menor_preco_de") == coluna["id"],
                "livre": preso is None,
                "motivo": (None if preso is None
                           else f"já está no pedido {numeros.get(preso, '')}".strip()),
            })
        fornecedores.append({
            "cotacao_fornecedor_id": coluna["id"], "fornecedor": coluna["fornecedor"],
            "fornecedor_id": getattr(bruta, "fornecedor_id", None),
            "condicao_pagamento_id": getattr(bruta, "condicao_pagamento_id", None),
            "condicao": coluna.get("condicao"),
            "entrega": coluna.get("entrega"),
            "frete": str(getattr(bruta, "frete", 0) or 0),
            "desconto": str(getattr(bruta, "desconto", 0) or 0),
            "itens": itens,
            "livres": sum(1 for x in itens if x["livre"]),
        })
    return {"cotacao_id": cotacao_id, "numero": mapa["numero"],
            "titulo": mapa["titulo"], "fornecedores": fornecedores,
            "obras": list(obras.values())}


def _tem_dados_de_pagamento(s: Session, fornecedor: Fornecedor) -> bool:
    """Compra à vista não gera boleto depois: ou há Pix/conta no cadastro, ou
    não há como pagar."""
    contas = getattr(fornecedor, "contas", None)
    if contas:
        return True
    from app.apps.erp.db.models.cadastros import FornecedorConta
    return any(c.fornecedor_id == fornecedor.id
               for c in s.scalars(select(FornecedorConta)).all())


# ---------------------------------------------------------------------------
# Autorizar
# ---------------------------------------------------------------------------
def autorizar(s: Session, pedido_id: int, usuario: Usuario,
              itens_recusados: Optional[list[int]] = None, *,
              conferir_autoria: bool = True) -> PedidoCompra:
    """Libera o pedido e gera a previsão de pagamento.

    Quem autoriza pode recusar PARTE do pedido: os itens recusados saem, voltam
    a ficar disponíveis, e a previsão nasce só sobre o que ficou.
    """
    pedido = _pedido_pendente(s, pedido_id)
    # ⚑ QUEM FECHOU NÃO AUTORIZA (22/11/2026). O dono: "o comprador lança o
    # pedido, ele cotou, ele lançou, e alguém da alçada superior vai autorizar
    # e disparar." Hoje comprar e autorizar estão nos mesmos dois perfis
    # (admin e diretor) — sem esta linha, uma pessoa só fazia a compra inteira.
    # `conferir_autoria=False` é só da compra tardia, onde a separação já é
    # outra: quem lançou a nota não dá a ciência.
    # ⚑ MENOS O ADMINISTRADOR (24/11/2026). O dono: "o administrador precisa
    # poder, até para quando a gente colocar em produção a coisa rodar (...) é
    # bom que o administrador possa fazer tudo. Pode ser que tenha uma obra que
    # tem alguma coisa que tem que autorizar, mas não tem ninguém cadastrado."
    # Continua registrado, no evento, quem fechou e quem autorizou.
    from app.apps.erp.db.models.cadastros import PerfilUsuario
    if (conferir_autoria and pedido.criado_por == usuario.id
            and usuario.perfil is not PerfilUsuario.ADMIN):
        raise ErroValidacao(
            "Quem fechou o pedido não autoriza o próprio pedido. A autorização "
            "é de outra pessoa, com alçada.")
    linhas = _itens(s, pedido.id)
    recusados = set(itens_recusados or [])
    se_ficam = [l for l in linhas if l.id not in recusados]
    if not se_ficam:
        raise ErroValidacao(
            "Recusar todos os itens não é autorizar — use Recusar o pedido.")

    for linha in linhas:
        if linha.id in recusados:
            item = s.get(SuprimentoItem, linha.suprimento_item_id)
            if item is not None:
                item.status = StatusItemSuprimento.SOLICITACAO
            _liberar_reserva(s, linha.suprimento_item_id)
            s.delete(linha)

    pedido.status = StatusPedidoCompra.AUTORIZADO
    pedido.autorizado_por = usuario.id
    pedido.autorizado_em = _agora()
    for linha in se_ficam:
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        if item is not None:
            item.status = StatusItemSuprimento.PEDIDO_EMITIDO
        registrar_no_banco_de_precos(
            s, item=item, preco_unitario=Decimal(str(linha.preco_unitario)),
            fornecedor_id=pedido.fornecedor_id, tipo=TipoPreco.COMPRADO,
            cotacao_id=pedido.cotacao_id,
            condicao_pagamento_id=pedido.condicao_pagamento_id)
    s.flush()

    parcelas = gerar_previsao(s, pedido, usuario)
    registrar_evento(s, "pedido_compra", pedido.id, "AUTORIZADO",
                     {"itens_recusados": sorted(recusados),
                      "parcelas": len(parcelas),
                      "total": str(total(s, pedido))}, usuario.id)
    return pedido


def gerar_previsao(s: Session, pedido: PedidoCompra,
                   usuario: Usuario) -> list[PrevisaoPagamento]:
    """As parcelas que o pedido vai gerar, pela condição de pagamento.

    Sem condição cadastrada, cai numa parcela única na data prevista de entrega
    (ou hoje) — melhor uma previsão grosseira do que nenhuma: o financeiro
    precisa enxergar o compromisso.
    """
    valor = total(s, pedido)
    if valor <= 0:
        raise ErroValidacao("Pedido sem valor não gera previsão.")

    condicao = (s.get(CondicaoPagamento, pedido.condicao_pagamento_id)
                if pedido.condicao_pagamento_id else None)
    base = pedido.previsao_entrega or date.today()
    if condicao is None:
        parcelas = gerar_parcelas(valor, base, 100, [])
    else:
        parcelas = gerar_parcelas(valor, base, condicao.entrada_percentual or 0,
                                  list(condicao.dias or []))

    criadas = []
    for p in parcelas:
        previsao = PrevisaoPagamento(pedido_id=pedido.id, numero=p.numero,
                                     vencimento=p.vencimento, valor=p.valor,
                                     entrada=p.entrada)
        s.add(previsao)
        criadas.append(previsao)
    s.flush()
    return criadas


def recusar(s: Session, pedido_id: int, motivo: str, usuario: Usuario) -> PedidoCompra:
    """Recusa o pedido inteiro, com motivo, e devolve os itens à fila."""
    pedido = _pedido_pendente(s, pedido_id)
    motivo = " ".join((motivo or "").split())
    if len(motivo) < 5:
        raise ErroValidacao("Escreva o motivo — quem comprou precisa saber.")
    pedido.status = StatusPedidoCompra.RECUSADO
    pedido.motivo = motivo
    pedido.autorizado_por = usuario.id
    pedido.autorizado_em = _agora()
    _devolver_itens(s, pedido)
    registrar_evento(s, "pedido_compra", pedido.id, "RECUSADO",
                     {"motivo": motivo}, usuario.id)
    return pedido


def cancelar(s: Session, pedido_id: int, motivo: str, usuario: Usuario) -> PedidoCompra:
    """Cancela um pedido já autorizado — o fornecedor desistiu, a obra parou.

    A previsão de pagamento cai junto: compromisso cancelado não pode continuar
    aparecendo como dinheiro a sair.
    """
    pedido = s.get(PedidoCompra, pedido_id, with_for_update=True, populate_existing=True)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    if pedido.status not in VIVOS:
        raise ErroValidacao(f"O pedido {pedido.numero} já está "
                            f"{pedido.status.value.lower().replace('_', ' ')}.")
    motivo = " ".join((motivo or "").split())
    if len(motivo) < 5:
        raise ErroValidacao("Escreva o motivo do cancelamento.")

    com_titulo = [p for p in _previsoes(s, pedido.id) if p.titulo_id]
    if com_titulo:
        raise ErroValidacao(
            "Este pedido já virou título no financeiro. Cancele por lá, para "
            "a baixa e o cancelamento não contarem histórias diferentes.")

    for previsao in _previsoes(s, pedido.id):
        s.delete(previsao)
    pedido.status = StatusPedidoCompra.CANCELADO
    pedido.motivo = motivo
    _devolver_itens(s, pedido)
    registrar_evento(s, "pedido_compra", pedido.id, "CANCELADO",
                     {"motivo": motivo}, usuario.id)
    return pedido


def _devolver_itens(s: Session, pedido: PedidoCompra) -> None:
    for linha in _itens(s, pedido.id):
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        if item is not None:
            item.status = StatusItemSuprimento.SOLICITACAO
        _liberar_reserva(s, linha.suprimento_item_id)


def _liberar_reserva(s: Session, suprimento_item_id: int) -> None:
    for r in s.scalars(select(PedidoItemReserva).where(
            PedidoItemReserva.suprimento_item_id == suprimento_item_id)).all():
        if r.suprimento_item_id == suprimento_item_id:
            s.delete(r)


def _pedido_pendente(s: Session, pedido_id: int) -> PedidoCompra:
    pedido = s.get(PedidoCompra, pedido_id, with_for_update=True, populate_existing=True)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    if pedido.status is not StatusPedidoCompra.AGUARDANDO_AUTORIZACAO:
        raise ErroValidacao(
            f"O pedido {pedido.numero} já está "
            f"{pedido.status.value.lower().replace('_', ' ')}.")
    return pedido


def _itens(s: Session, pedido_id: int) -> list[PedidoItem]:
    # O `.where` é o que o banco usa; o `if` fica para a sessão dublada dos
    # testes, que ignora `WHERE`. Sem o `.where`, cada chamada lia a tabela
    # inteira — e a lista de pedidos chamava isto duas vezes POR PEDIDO.
    return sorted([i for i in s.scalars(select(PedidoItem).where(
                       PedidoItem.pedido_id == pedido_id)).all()
                   if i.pedido_id == pedido_id], key=lambda x: x.numero or 0)


def _previsoes(s: Session, pedido_id: int) -> list[PrevisaoPagamento]:
    return [p for p in s.scalars(select(PrevisaoPagamento).where(
                PrevisaoPagamento.pedido_id == pedido_id)).all()
            if p.pedido_id == pedido_id]


def total(s: Session, pedido: PedidoCompra) -> Decimal:
    """Itens + frete − desconto. É o valor que vira previsão de pagamento."""
    soma = Decimal("0")
    for linha in _itens(s, pedido.id):
        soma += Decimal(str(linha.quantidade)) * Decimal(str(linha.preco_unitario))
    soma += Decimal(str(pedido.frete or 0)) - Decimal(str(pedido.desconto or 0))
    return max(soma, Decimal("0")).quantize(CENTAVO, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def detalhar(s: Session, pedido_id: int) -> dict[str, Any]:
    """O pedido com tudo que quem autoriza precisa ver — inclusive o mapa de
    origem, quando houver, porque julgar a escolha exige ver as alternativas."""
    from app.apps.erp.core.suprimentos.cotacao import montar_mapa
    from app.apps.erp.db.models.cadastros import Insumo, Obra

    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    forn = s.get(Fornecedor, pedido.fornecedor_id)
    cond = (s.get(CondicaoPagamento, pedido.condicao_pagamento_id)
            if pedido.condicao_pagamento_id else None)

    from app.apps.erp.core.suprimentos.entrega import (
        desvios_do_pedido, local_de_entrega)
    desvios = desvios_do_pedido(s, pedido.id)

    itens = []
    for linha in _itens(s, pedido.id):
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        obra = s.get(Obra, item.obra_id) if item is not None else None
        itens.append({
            "id": linha.id, "numero": linha.numero,
            "insumo": getattr(insumo, "descricao", ""),
            "especificacao": getattr(item, "especificacao", None),
            "quantidade": str(linha.quantidade), "unidade": getattr(item, "unidade", ""),
            "preco_unitario": str(linha.preco_unitario),
            "total": str((Decimal(str(linha.quantidade)) *
                          Decimal(str(linha.preco_unitario))).quantize(CENTAVO)),
            "obra": getattr(obra, "codigo", ""), "obra_id": getattr(item, "obra_id", None),
            "entrega_em": local_de_entrega(
                obra, desvios.get(getattr(item, "obra_id", None))),
        })

    return {
        "id": pedido.id, "numero": pedido.numero,
        "status": pedido.status.value if pedido.status else None,
        "fornecedor": getattr(forn, "razao_social", ""),
        "fornecedor_id": pedido.fornecedor_id,
        "condicao": getattr(cond, "nome", None),
        "entrega": pedido.entrega.value if pedido.entrega else None,
        "frete": str(pedido.frete or 0), "desconto": str(pedido.desconto or 0),
        "antecipado": bool(pedido.antecipado),
        "previsao_entrega": (pedido.previsao_entrega.isoformat()
                             if pedido.previsao_entrega else None),
        "observacoes": pedido.observacoes, "motivo": pedido.motivo,
        "justificativa_sem_cotacao": getattr(pedido, "justificativa_sem_cotacao", None),
        "itens": itens, "total": str(total(s, pedido)),
        "cotacao_id": pedido.cotacao_id,
        # O mapa vai junto: quem autoriza precisa ver as alternativas que o
        # comprador tinha, não só a escolha final.
        "mapa": montar_mapa(s, pedido.cotacao_id) if pedido.cotacao_id else None,
        "previsoes": [{"numero": p.numero, "vencimento": p.vencimento.isoformat(),
                       "valor": str(p.valor), "entrada": bool(p.entrada),
                       "titulo_id": p.titulo_id}
                      for p in sorted(_previsoes(s, pedido.id),
                                      key=lambda x: x.numero or 0)],
    }


def relatorio_para_o_fornecedor(s: Session, pedido_id: int) -> dict[str, Any]:
    """O pedido como o fornecedor precisa lê-lo: agrupado por ENDEREÇO DE ENTREGA.

    Versão estruturada, para quem quiser montar o documento de outro jeito
    (imprimir, WhatsApp). O texto que sai POR E-MAIL é montado em
    `core/suprimentos/envio.montar_pedido` — lá vão também os preços, o total
    e a condição de pagamento, porque é aquele documento que firma a compra.

    Um mesmo pedido pode levar material para obras diferentes, e o motorista
    precisa saber o que desce em cada lugar. Obras que compartilham endereço
    entram no mesmo bloco — foi o que o dono pediu ("permitir entrega de mais
    de uma obra em um único endereço").
    """
    from app.apps.erp.db.models.cadastros import Insumo, Obra

    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    forn = s.get(Fornecedor, pedido.fornecedor_id)
    cond = (s.get(CondicaoPagamento, pedido.condicao_pagamento_id)
            if pedido.condicao_pagamento_id else None)

    from app.apps.erp.core.suprimentos.entrega import (
        desvios_do_pedido, local_de_entrega)
    desvios = desvios_do_pedido(s, pedido.id)

    blocos: dict[str, dict[str, Any]] = {}
    for linha in _itens(s, pedido.id):
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        obra = s.get(Obra, item.obra_id) if item is not None else None
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        local = local_de_entrega(obra, desvios.get(getattr(item, "obra_id", None)))
        endereco = local["endereco"]
        bloco = blocos.setdefault(endereco, {"endereco": endereco, "obras": [],
                                             "itens": [],
                                             "na_obra": local["na_obra"],
                                             "local": local["rotulo"],
                                             "mapa": local["mapa"]})
        rotulo_obra = f"{getattr(obra, 'codigo', '')} · {getattr(obra, 'nome', '')}".strip(" ·")
        if rotulo_obra and rotulo_obra not in bloco["obras"]:
            bloco["obras"].append(rotulo_obra)
        bloco["itens"].append({
            "insumo": getattr(insumo, "descricao", ""),
            "especificacao": getattr(item, "especificacao", None),
            "quantidade": str(linha.quantidade),
            "unidade": getattr(item, "unidade", ""),
            "obra": getattr(obra, "codigo", ""),
        })

    return {
        "numero": pedido.numero,
        "fornecedor": getattr(forn, "razao_social", ""),
        "condicao": getattr(cond, "nome", None),
        "entrega": pedido.entrega.value if pedido.entrega else None,
        "previsao_entrega": (pedido.previsao_entrega.isoformat()
                             if pedido.previsao_entrega else None),
        "observacoes": pedido.observacoes,
        "total": str(total(s, pedido)),
        "locais": list(blocos.values()),
    }


def _endereco(obra) -> str:
    """O endereço de entrega em uma linha.

    Delega ao módulo comum (`core/suprimentos/entrega`) porque a cotação usa a
    mesma resposta: se o endereço da obra mudar de forma, os dois documentos
    têm de mudar juntos.
    """
    from app.apps.erp.core.suprimentos.entrega import endereco_da_obra
    return endereco_da_obra(obra)


def listar(s: Session, *, status: Optional[str] = None) -> list[dict[str, Any]]:
    """A fila de pedidos. TRÊS consultas, com dez pedidos ou com vinte mil.

    Até 20/11/2026 eram duas leituras da tabela de itens INTEIRA para cada
    pedido da lista, mais uma ida ao banco por fornecedor. A numeração antiga
    da empresa passa de 19 mil pedidos.
    """
    stmt = select(PedidoCompra)
    alvo = status.strip().upper() if status else None
    if alvo and alvo in {x.value for x in StatusPedidoCompra}:
        stmt = stmt.where(PedidoCompra.status == StatusPedidoCompra(alvo))
    pedidos = [p for p in s.scalars(stmt).all()
               if not alvo or (p.status and p.status.value == alvo)]
    ids = [p.id for p in pedidos]
    por_pedido: dict[int, list[PedidoItem]] = {}
    for item in s.scalars(select(PedidoItem).where(
            PedidoItem.pedido_id.in_(ids or [0]))).all():
        por_pedido.setdefault(item.pedido_id, []).append(item)
    nomes = {f.id: f.razao_social for f in s.scalars(select(Fornecedor).where(
        Fornecedor.id.in_({p.fornecedor_id for p in pedidos} or {0}))).all()}
    saida = []
    for p in sorted(pedidos, key=lambda x: x.id or 0, reverse=True):
        itens = por_pedido.get(p.id, [])
        soma = sum((Decimal(str(i.quantidade)) * Decimal(str(i.preco_unitario))
                    for i in itens), Decimal("0"))
        soma += Decimal(str(p.frete or 0)) - Decimal(str(p.desconto or 0))
        saida.append({
            "id": p.id, "numero": p.numero,
            "fornecedor": nomes.get(p.fornecedor_id, ""),
            "status": p.status.value if p.status else None,
            "total": str(max(soma, Decimal("0")).quantize(
                CENTAVO, rounding=ROUND_HALF_UP)),
            "antecipado": bool(p.antecipado),
            "itens": len(itens),
            "cotacao_id": p.cotacao_id,
            "criado_em": p.criado_em.isoformat() if p.criado_em else None,
        })
    return saida
