# ============================================================================
# ERP — core/suprimentos/esperando.py
# O que está esperando POR ESTA PESSOA — para o portal mostrar.
#
# O dono, em 24/11/2026, sobre como avisar quem precisa agir: "eu acho que a
# gente pode utilizar o portal. Inclusive padronizar ele como local de fazer
# solicitações e visualizar o que tem de pendente, para todos (...) ele
# entrando, vai identificar, vai poder fazer alguma requisição, vai poder
# visualizar se tem alguma pendência."
#
# Antes disto NINGUÉM era avisado de nada: a engenharia só descobria um pedido
# esperando o ok se abrisse a tela certa; o mesmo para quem autoriza pedido,
# dá ciência de compra por fora, dá o aval e cadastra insumo. Foi o beco nº 1
# da caminhada de ponta a ponta.
#
# CADA LINHA É UMA CONTAGEM E UM LINK — não uma lista. O portal já tinha ficado
# grande demais uma vez (ele reclamou em 23/11); aqui a pessoa vê "3 pedidos de
# material esperando o seu ok" e toca para ir à tela que resolve.
#
# CUSTO, contado: até oito consultas, só as das ações que a pessoa TEM. É
# carregado depois do resto do portal, em chamada própria, para não segurar a
# abertura da tela. Uma consulta que falhe não derruba as outras.
# ============================================================================
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor, pode
from app.apps.erp.db.models.cadastros import (
    InsumoSolicitacao, PedidoCompra, PerfilUsuario, StatusItemSuprimento,
    StatusPedidoCompra, StatusSolicitacaoInsumo, SuprimentoItem, Usuario,
)

logger = logging.getLogger(__name__)


def _seguro(s: Session, conta: Callable[[], int]) -> int:
    """Uma contagem que falhe (migração atrasada, área fora de uso) vale zero —
    e não leva as outras junto."""
    try:
        with s.begin_nested():
            return int(conta() or 0)
    except Exception:
        logger.exception("ERP/portal: não consegui contar uma pendência")
        return 0


def comigo(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """As pendências que dependem desta pessoa, cada uma com onde resolver."""
    saida: list[dict[str, Any]] = []
    obras: Optional[list[int]] = obras_de_registro_sem_autor(s, usuario)
    admin = usuario.perfil is PerfilUsuario.ADMIN

    def linha(chave: str, quantos: int, um: str, varios: str, link: str) -> None:
        if quantos > 0:
            saida.append({"chave": chave, "quantos": quantos, "link": link,
                          "texto": um if quantos == 1 else varios.format(n=quantos)})

    if pode(usuario, "liberar_solicitacao"):
        def a_liberar() -> int:
            q = select(func.count(SuprimentoItem.id)).where(
                SuprimentoItem.liberacao == "PENDENTE",
                SuprimentoItem.status == StatusItemSuprimento.SOLICITACAO)
            if obras is not None:
                q = q.where(SuprimentoItem.obra_id.in_(obras or [0]))
            return s.execute(q).scalar()
        linha("liberar", _seguro(s, a_liberar),
              "1 material pedido pela obra espera o seu ok para ser comprado",
              "{n} materiais pedidos pela obra esperam o seu ok para serem comprados",
              "/erp/suprimentos")

    if pode(usuario, "autorizar_pedido"):
        def a_autorizar() -> int:
            q = select(func.count(PedidoCompra.id)).where(
                PedidoCompra.status == StatusPedidoCompra.AGUARDANDO_AUTORIZACAO)
            if not admin:        # quem fechou não autoriza o próprio (menos o administrador)
                q = q.where(PedidoCompra.criado_por != usuario.id)
            return s.execute(q).scalar()
        linha("autorizar", _seguro(s, a_autorizar),
              "1 pedido de compra espera a sua autorização",
              "{n} pedidos de compra esperam a sua autorização",
              "/erp/suprimentos/pedidos?status=AGUARDANDO_AUTORIZACAO")

    def por_fora() -> list[dict[str, Any]]:
        from app.apps.erp.core.suprimentos import fora_do_fluxo
        try:
            with s.begin_nested():
                return fora_do_fluxo.listar(s, usuario)
        except Exception:
            return []
    lista_fora = None
    if pode(usuario, "autorizar_compra_fora_do_fluxo"):
        lista_fora = por_fora()
        linha("ciencia", len([x for x in lista_fora if not x["ciente"]
                              and (admin or x["lancado_por_id"] != usuario.id)]),
              "1 compra feita sem pedido espera a sua ciência",
              "{n} compras feitas sem pedido esperam a sua ciência",
              "/erp/suprimentos/painel")
    if pode(usuario, "receber_material"):
        if lista_fora is None:
            lista_fora = por_fora()
        linha("informar", len([x for x in lista_fora if not x["regularizado"]
                               and x["lancado_por_id"] == usuario.id]),
              "1 compra sem pedido que você lançou espera você informar o que foi comprado",
              "{n} compras sem pedido que você lançou esperam você informar o que foi comprado",
              "/erp/suprimentos/painel")

        def do_almoxarifado() -> int:
            from app.apps.erp.core.suprimentos.recebimento import do_almoxarifado as lista
            return len(lista(s, usuario))
        linha("almoxarifado", _seguro(s, do_almoxarifado),
              "1 material enviado do almoxarifado espera você confirmar que chegou",
              "{n} materiais enviados do almoxarifado esperam você confirmar que chegaram",
              "/erp/suprimentos/receber")

        def sem_nota() -> int:
            from app.apps.erp.core.suprimentos.recebimento import a_receber
            return len(a_receber(s, usuario)["sem_nota"])
        linha("sem_nota", _seguro(s, sem_nota),
              "1 material já chegou e está sem a nota lançada",
              "{n} materiais já chegaram e estão sem a nota lançada",
              "/erp/suprimentos/receber")

    if pode(usuario, "administrar_insumos"):
        linha("insumo", _seguro(s, lambda: s.execute(
            select(func.count(InsumoSolicitacao.id)).where(
                InsumoSolicitacao.status == StatusSolicitacaoInsumo.PENDENTE)).scalar()),
            "1 material novo espera você cadastrar o insumo",
            "{n} materiais novos esperam você cadastrar o insumo",
            "/erp/suprimentos/configuracoes")

    if pode(usuario, "avalizar"):
        def avais() -> int:
            from app.apps.erp.core.titulos.aval import pendentes
            return len(pendentes(s, usuario))
        linha("aval", _seguro(s, avais),
              "1 pagamento espera o seu aval",
              "{n} pagamentos esperam o seu aval", "/erp/confirmar")
    return saida
