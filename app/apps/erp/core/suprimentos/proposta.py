# ============================================================================
# ERP — core/suprimentos/proposta.py
# A proposta do fornecedor vira a coluna do mapa.
#
# As respostas chegam como PDF, foto do WhatsApp, e-mail ou texto colado, cada
# fornecedor com a sua nomenclatura. O que a IA faz aqui é mais fácil do que
# parece, e por um motivo: ela não precisa adivinhar o que é o material —
# precisa casar o que veio com os itens QUE JÁ ESTÃO NO MAPA.
#
# E, como em todo lugar deste sistema, ela SUGERE. Nada é gravado sem alguém
# olhar: item que não casou volta marcado, e preço ambíguo volta com aviso.
# ============================================================================
from __future__ import annotations

import logging
from difflib import SequenceMatcher
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.db.models.cadastros import (
    CotacaoFornecedor, CotacaoItem, Insumo, SuprimentoItem, Usuario,
)

logger = logging.getLogger(__name__)

DICA = (
    "Isto é uma PROPOSTA COMERCIAL de um fornecedor de material de construção. "
    "Devolva em 'itens' uma entrada por material cotado, com: descricao (como "
    "está escrito na proposta), quantidade, unidade e valor (o preço UNITÁRIO; "
    "se só houver o total, divida pela quantidade e diga isso em observacao). "
    "Em observacoes, informe a forma de pagamento, o prazo, o valor do frete e "
    "se a entrega é por conta do fornecedor ou se é coleta. "
    "Não invente item que não esteja na proposta."
)

MINIMO = 0.60
ALTA = 0.85


def ler(s: Session, cotacao_fornecedor_id: int, *, conteudo: Optional[bytes] = None,
        nome_arquivo: str = "", texto: str = "",
        usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Lê a proposta e devolve os preços casados com as linhas do mapa.

    O arquivo, quando houver, fica ANEXADO à coluna daquele fornecedor. Serve
    para duas coisas ao mesmo tempo: alimentar a leitura e ficar guardado como
    prova — na hora de autorizar, dá para abrir a proposta original e conferir
    se os preços lançados batem com o que o fornecedor mandou.
    """
    coluna = s.get(CotacaoFornecedor, cotacao_fornecedor_id)
    if coluna is None:
        raise ErroNaoEncontrado("Fornecedor não está neste mapa.")
    if not conteudo and not (texto or "").strip():
        raise ErroValidacao("Anexe a proposta ou cole o texto dela.")

    from app.apps.erp.core.comum.ia_custo import contexto
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento
    from app.apps.erp.core.documentos import armazenamento

    anexo_id = None
    try:
        with contexto(operacao="proposta_cotacao"):
            if conteudo:
                lido = ler_documento(conteudo, nome_arquivo, dica_usuario=DICA)
            else:
                from app.apps.erp.core.documentos.leitor import _chamar_ia
                lido = _chamar_ia(texto=texto, dica=DICA)
    except ErroLeitura as e:
        raise ErroValidacao(f"Não consegui ler a proposta: {e}")
    except Exception as e:                       # pragma: no cover - rede/serviço
        logger.exception("ERP/suprimentos: falha ao ler a proposta")
        raise ErroValidacao(f"Não consegui ler a proposta: {e}")

    if conteudo:
        # Guardar DEPOIS de ler: se a leitura falhar, não sobra anexo órfão de
        # uma proposta que ninguém conseguiu aproveitar.
        anexo = armazenamento.salvar(
            s, conteudo, nome_arquivo or "proposta", entidade_tipo="cotacao_fornecedor",
            entidade_id=coluna.id, categoria="PROPOSTA", usuario=usuario,
            descricao="Proposta do fornecedor")
        anexo_id = anexo.id
        coluna.anexo_id = anexo_id

    linhas_mapa = _linhas_do_mapa(s, coluna.cotacao_id)
    sugestoes, nao_casados = [], []
    for bruto in (lido.get("itens") or []):
        descricao = (bruto.get("descricao") or "").strip()
        if not descricao:
            continue
        alvo, escore = _mais_parecido(descricao, linhas_mapa)
        preco = _preco_unitario(bruto)
        if alvo is None or escore < MINIMO:
            nao_casados.append({"descricao": descricao, "preco": preco})
            continue
        sugestoes.append({
            "cotacao_item_id": alvo["cotacao_item_id"],
            "insumo": alvo["descricao"],
            "descricao_lida": descricao,
            "preco": preco,
            "confianca": "ALTA" if escore >= ALTA else "MEDIA",
            "observacao": (bruto.get("observacao") or "").strip() or None,
        })

    sem_preco = [x for x in sugestoes if not x["preco"]]
    return {
        "anexo_id": anexo_id,
        "sugestoes": [x for x in sugestoes if x["preco"]],
        "sem_preco": sem_preco,
        "nao_casados": nao_casados,
        "condicoes_lidas": (lido.get("observacoes") or "").strip() or None,
        "itens_do_mapa_sem_proposta": [
            l["cotacao_item_id"] for l in linhas_mapa
            if l["cotacao_item_id"] not in {x["cotacao_item_id"] for x in sugestoes}],
        "resumo": (f"{len(sugestoes)} item(ns) casados com o mapa, "
                   f"{len(nao_casados)} sem correspondência"
                   + (f", {len(sem_preco)} sem preço legível" if sem_preco else "") + "."),
    }


def _linhas_do_mapa(s: Session, cotacao_id: int) -> list[dict[str, Any]]:
    saida = []
    for linha in s.scalars(select(CotacaoItem)).all():
        if linha.cotacao_id != cotacao_id:
            continue
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        saida.append({
            "cotacao_item_id": linha.id,
            "descricao": getattr(insumo, "descricao", ""),
            "especificacao": getattr(item, "especificacao", "") or "",
        })
    return saida


def _mais_parecido(descricao: str, linhas: list[dict[str, Any]]):
    """Casa contra o insumo E a especificação juntos: "Vergalhão CA50" e
    "Vergalhão CA60" só se distinguem pela especificação."""
    alvo = descricao.upper()
    melhor, escore = None, 0.0
    for linha in linhas:
        completo = f"{linha['descricao']} {linha['especificacao']}".strip().upper()
        r = max(SequenceMatcher(None, alvo, completo).ratio(),
                SequenceMatcher(None, alvo, linha["descricao"].upper()).ratio())
        if r > escore:
            melhor, escore = linha, r
    return melhor, escore


def _preco_unitario(bruto: dict[str, Any]) -> str:
    """O preço unitário da linha lida. Sem chute: se não der para ler, volta
    vazio e a tela pede para a pessoa digitar."""
    import re
    for chave in ("valor_unitario", "valor", "preco_unitario", "preco"):
        texto = str(bruto.get(chave) or "").strip()
        if not texto:
            continue
        achado = re.search(r"\d[\d.,]*", texto)
        if not achado:
            continue
        numero = achado.group(0)
        if "," in numero:
            numero = numero.replace(".", "").replace(",", ".")
        try:
            if float(numero) > 0:
                return numero
        except ValueError:
            continue
    return ""
