# ============================================================================
# ERP — core/suprimentos/limpeza.py
# Apagar o que foi lançado em Suprimentos durante um teste.
#
# Isto é um botão que APAGA DADOS EM PRODUÇÃO. O desenho inteiro existe para
# tornar difícil o estrago:
#
#   1. APAGA SÓ O MOVIMENTO. Solicitação, cotação, mapa, pedido, previsão,
#      recebimento e o histórico de preços. Insumo, fornecedor, unidade e
#      condição de pagamento NÃO saem — trazê-los das planilhas dá trabalho, e
#      eles não são "lançamento".
#   2. NÃO ENCOSTA NO FINANCEIRO. Nenhuma tabela de título, parcela, pagamento
#      ou conciliação entra na lista.
#   3. RECUSA SE JÁ VIROU DINHEIRO. Se alguma previsão de pagamento já virou
#      título, isto deixou de ser teste — e a limpeza para, dizendo qual.
#   4. MOSTRA ANTES. O resumo conta linha por linha o que sairia, e só então se
#      executa, com a frase de confirmação digitada.
#   5. DEIXA RASTRO. Quem fez, quando, e quantas linhas de cada tabela.
# ============================================================================
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

FRASE_DE_CONFIRMACAO = "ZERAR SUPRIMENTOS"

# Ordem importa: filho antes de pai, senão a chave estrangeira barra.
# Cada passo diz a tabela, o rótulo em português e o corpo do comando.
# `:desde` é opcional — nulo significa "todo o movimento do módulo".
PASSOS: tuple[tuple[str, str, str], ...] = (
    ("recebimento_itens", "itens recebidos",
     """recebimento_itens WHERE recebimento_id IN
        (SELECT id FROM recebimentos WHERE (:desde IS NULL OR criado_em >= :desde))"""),
    ("recebimentos", "recebimentos",
     "recebimentos WHERE (:desde IS NULL OR criado_em >= :desde)"),
    ("previsoes_pagamento", "previsões de pagamento",
     """previsoes_pagamento WHERE pedido_id IN
        (SELECT id FROM pedidos_compra WHERE (:desde IS NULL OR criado_em >= :desde))"""),
    ("pedido_item_reserva", "reservas de item",
     """pedido_item_reserva WHERE pedido_id IN
        (SELECT id FROM pedidos_compra WHERE (:desde IS NULL OR criado_em >= :desde))"""),
    ("pedido_itens", "itens de pedido",
     """pedido_itens WHERE pedido_id IN
        (SELECT id FROM pedidos_compra WHERE (:desde IS NULL OR criado_em >= :desde))"""),
    ("pedidos_compra", "pedidos de compra",
     "pedidos_compra WHERE (:desde IS NULL OR criado_em >= :desde)"),
    ("precos_historico", "preços no banco de preços",
     "precos_historico WHERE (:desde IS NULL OR criado_em >= :desde)"),
    ("cotacao_precos", "preços lançados no mapa",
     """cotacao_precos WHERE cotacao_item_id IN
        (SELECT ci.id FROM cotacao_itens ci JOIN cotacoes c ON c.id = ci.cotacao_id
         WHERE (:desde IS NULL OR c.criado_em >= :desde))"""),
    ("cotacao_fornecedores", "fornecedores no mapa",
     """cotacao_fornecedores WHERE cotacao_id IN
        (SELECT id FROM cotacoes WHERE (:desde IS NULL OR criado_em >= :desde))"""),
    ("cotacao_itens", "itens no mapa",
     """cotacao_itens WHERE cotacao_id IN
        (SELECT id FROM cotacoes WHERE (:desde IS NULL OR criado_em >= :desde))"""),
    ("cotacoes", "cotações",
     "cotacoes WHERE (:desde IS NULL OR criado_em >= :desde)"),
    # Os arquivos de proposta anexados às colunas do mapa. Sem este passo,
    # sobraria peso no banco apontando para um mapa que não existe mais.
    ("anexos", "propostas anexadas",
     """anexos WHERE entidade_tipo = 'cotacao_fornecedor'
        AND (:desde IS NULL OR criado_em >= :desde)"""),
    ("suprimento_itens", "itens de solicitação",
     """suprimento_itens WHERE solicitacao_id IN
        (SELECT id FROM suprimento_solicitacoes
         WHERE (:desde IS NULL OR criado_em >= :desde))"""),
    ("suprimento_solicitacoes", "solicitações de material",
     "suprimento_solicitacoes WHERE (:desde IS NULL OR criado_em >= :desde)"),
    ("insumo_solicitacoes", "pedidos de cadastro de insumo",
     "insumo_solicitacoes WHERE (:desde IS NULL OR criado_em >= :desde)"),
)

# O que NUNCA sai — escrito para aparecer na tela antes de alguém confirmar.
PRESERVADO = (
    "insumos e suas categorias",
    "fornecedores, contatos e contas bancárias",
    "unidades de compra e condições de pagamento",
    "obras, operadores e todo o financeiro (títulos, pagamentos, conciliação)",
)


def _desde(valor: Any) -> Optional[datetime]:
    if not valor:
        return None
    try:
        return datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    except ValueError:
        raise ErroValidacao("Data inválida. Use o formato AAAA-MM-DD.")


def _previsoes_viradas_em_titulo(s: Session, desde: Optional[datetime]) -> list[str]:
    """Previsão com título é dinheiro no financeiro — não é mais teste."""
    linhas = s.execute(text(
        "SELECT p.numero, v.titulo_id FROM previsoes_pagamento v "
        "JOIN pedidos_compra p ON p.id = v.pedido_id "
        "WHERE v.titulo_id IS NOT NULL "
        "AND (:desde IS NULL OR p.criado_em >= :desde)"),
        {"desde": desde}).all()
    return [f"{numero} (título {titulo_id})" for numero, titulo_id in linhas]


def resumo(s: Session, desde_bruto: Any = None) -> dict[str, Any]:
    """O que sairia, linha por linha — sem executar nada."""
    desde = _desde(desde_bruto)
    detalhe, total = [], 0
    for tabela, rotulo, corpo in PASSOS:
        quantas = int(s.execute(
            text(f"SELECT count(*) FROM (SELECT 1 FROM {corpo}) AS t"),
            {"desde": desde}).scalar() or 0)
        if quantas:
            detalhe.append({"tabela": tabela, "rotulo": rotulo, "linhas": quantas})
        total += quantas
    return {
        "desde": desde.isoformat() if desde else None,
        "total": total,
        "detalhe": detalhe,
        "preservado": list(PRESERVADO),
        "impedimentos": _previsoes_viradas_em_titulo(s, desde),
        "frase": FRASE_DE_CONFIRMACAO,
    }


def zerar(s: Session, confirmacao: str, usuario: Usuario,
          desde_bruto: Any = None) -> dict[str, Any]:
    """Limpa o movimento de Suprimentos. Só o ADMIN chega aqui (a rota exige)."""
    if (confirmacao or "").strip().upper() != FRASE_DE_CONFIRMACAO:
        raise ErroValidacao(
            f'Para confirmar, digite exatamente "{FRASE_DE_CONFIRMACAO}".')

    desde = _desde(desde_bruto)
    impedimentos = _previsoes_viradas_em_titulo(s, desde)
    if impedimentos:
        raise ErroValidacao(
            "Não dá para zerar: há pedido cuja previsão já virou título no "
            f"financeiro ({', '.join(impedimentos[:3])}). Isto deixou de ser "
            "teste — resolva pelo financeiro primeiro.")

    feitos, total = [], 0
    for tabela, rotulo, corpo in PASSOS:
        resultado = s.execute(text(f"DELETE FROM {corpo}"), {"desde": desde})
        quantas = int(resultado.rowcount or 0)
        if quantas:
            feitos.append({"tabela": tabela, "rotulo": rotulo, "linhas": quantas})
        total += quantas

    registrar_evento(s, "suprimentos", 0, "MOVIMENTO_ZERADO",
                     {"desde": desde.isoformat() if desde else None,
                      "total": total, "detalhe": feitos},
                     usuario.id if usuario else None)
    logger.warning("ERP/suprimentos: movimento zerado por %s — %d linha(s): %s",
                   getattr(usuario, "nome", "?"), total, feitos)
    return {"total": total, "detalhe": feitos,
            "desde": desde.isoformat() if desde else None}
