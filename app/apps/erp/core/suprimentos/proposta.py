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
    "Devolva TAMBÉM, no campo 'condicoes', um objeto com estas chaves, cada "
    "uma vazia quando a proposta não disser: "
    "forma_pagamento (como está escrito: 'à vista', '28 dias', '30/60/90'), "
    "prazo_entrega_dias (só o número de dias), "
    "frete (o valor em reais do frete, se houver; 0 quando disser frete grátis "
    "ou incluso), "
    "entrega ('ENTREGA' quando o fornecedor entrega — CIF, frete incluso, "
    "posto obra — ou 'COLETA' quando somos nós que retiramos — FOB, a retirar, "
    "na loja), "
    "validade (a data até quando o preço vale, no formato AAAA-MM-DD), "
    "desconto (valor em reais do desconto no total, se houver). "
    "Em observacoes, escreva o resto que não couber nessas chaves. "
    "Não invente item nem condição que não esteja na proposta."
)

# CIF e FOB não aparecem em metade das propostas: o vendedor escreve "posto
# obra" ou "a retirar". O que a IA devolve passa por aqui antes de virar campo.
PALAVRAS_ENTREGA = {
    "ENTREGA": ("ENTREGA", "CIF", "POSTO OBRA", "POSTO-OBRA", "ENTREGUE",
                "FRETE INCLUSO", "FRETE GRATIS", "FRETE GRÁTIS", "INCLUSO"),
    "COLETA": ("COLETA", "FOB", "RETIRAR", "RETIRADA", "NA LOJA", "BALCAO",
               "BALCÃO"),
}

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
    condicoes = _condicoes(s, lido)
    return {
        "anexo_id": anexo_id,
        "sugestoes": [x for x in sugestoes if x["preco"]],
        "sem_preco": sem_preco,
        "nao_casados": nao_casados,
        "condicoes": condicoes,
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


# ---------------------------------------------------------------------------
# As CONDIÇÕES, que mudam a decisão tanto quanto o preço
# ---------------------------------------------------------------------------
# Pedido do dono, 18/09/2026: *"não só do mapa com os valores, mas de condição
# de pagamento, frete, valor de frete, se é frete CIF, se é FOB"*. Ele está
# certo sobre o peso disso: um preço 3% menor com frete por nossa conta e
# pagamento à vista é, quase sempre, o pior negócio da página — e até aqui
# essas três coisas voltavam da leitura como uma frase solta que ninguém
# transportava para os campos.
def _condicoes(s: Session, lido: dict[str, Any]) -> dict[str, Any]:
    """Traduz o que a IA leu para os campos da coluna do mapa.

    Nada disso é gravado: volta como SUGESTÃO, para a tela preencher e a pessoa
    conferir. Escaneado torto e vendedor criativo acontecem toda semana.
    """
    bruto = lido.get("condicoes") or {}
    if not isinstance(bruto, dict):
        bruto = {}
    texto_todo = " ".join(str(v) for v in bruto.values() if v)
    texto_todo += " " + str(lido.get("observacoes") or "")

    entrega = _entrega(str(bruto.get("entrega") or ""), texto_todo)
    forma = (str(bruto.get("forma_pagamento") or "")).strip()
    saida = {
        "forma_pagamento_lida": forma or None,
        "condicao_pagamento_id": _casar_condicao(s, forma),
        "prazo_entrega_dias": _inteiro(bruto.get("prazo_entrega_dias"), teto=365),
        "frete": _dinheiro(bruto.get("frete")),
        "desconto": _dinheiro(bruto.get("desconto")),
        "entrega": entrega,
        "validade": _data(bruto.get("validade")),
    }
    saida["resumo"] = _resumo_condicoes(saida)
    return saida


def _entrega(valor: str, texto: str) -> Optional[str]:
    alvo = f"{valor} {texto}".upper()
    for modo, palavras in PALAVRAS_ENTREGA.items():
        if any(p in alvo for p in palavras):
            # "frete incluso" e "a retirar" na mesma proposta acontece quando o
            # vendedor manda duas opções. Aí não dá para escolher por ele.
            outro = "COLETA" if modo == "ENTREGA" else "ENTREGA"
            if any(p in alvo for p in PALAVRAS_ENTREGA[outro]):
                return None
            return modo
    return None


def _casar_condicao(s: Session, texto: str) -> Optional[int]:
    """Acha a condição de pagamento CADASTRADA que mais se parece com o que a
    proposta diz. Abaixo de `MINIMO` não sugere nada: preencher errado o campo
    que define o vencimento é pior do que deixar em branco."""
    texto = (texto or "").strip()
    if not texto:
        return None
    from app.apps.erp.db.models.cadastros import CondicaoPagamento
    alvo = texto.upper()
    melhor, escore = None, 0.0
    for c in s.scalars(select(CondicaoPagamento)).all():
        if not getattr(c, "ativo", True):
            continue
        nome = (c.nome or "").upper()
        r = SequenceMatcher(None, alvo, nome).ratio()
        if nome and (nome in alvo or alvo in nome):
            r = max(r, 0.9)
        if r > escore:
            melhor, escore = c, r
    return melhor.id if melhor is not None and escore >= MINIMO else None


def _inteiro(valor: Any, *, teto: int) -> Optional[int]:
    import re
    achado = re.search(r"\d+", str(valor or ""))
    if not achado:
        return None
    n = int(achado.group(0))
    return n if 0 <= n <= teto else None


def _dinheiro(valor: Any) -> Optional[str]:
    """Zero é resposta, não ausência: "frete grátis" vira 0,00 e precisa chegar
    ao campo, senão o total do mapa herda o frete de outra proposta."""
    import re
    texto = str(valor if valor is not None else "").strip()
    if not texto:
        return None
    if texto.upper() in ("GRATIS", "GRÁTIS", "INCLUSO", "ISENTO", "SEM FRETE"):
        return "0"
    achado = re.search(r"\d[\d.,]*", texto)
    if not achado:
        return None
    numero = achado.group(0)
    if "," in numero:
        numero = numero.replace(".", "").replace(",", ".")
    try:
        n = float(numero)
    except ValueError:
        return None
    return f"{n:.2f}" if n >= 0 else None


def _data(valor: Any) -> Optional[str]:
    import re
    texto = str(valor or "").strip()
    achado = re.search(r"(\d{4})-(\d{2})-(\d{2})", texto)
    if achado:
        return achado.group(0)
    achado = re.search(r"(\d{2})/(\d{2})/(\d{4})", texto)
    if achado:
        d, m, a = achado.groups()
        return f"{a}-{m}-{d}"
    return None


ROTULO_ENTREGA = {"ENTREGA": "o fornecedor entrega (CIF)",
                  "COLETA": "nós retiramos (FOB)"}


def _resumo_condicoes(c: dict[str, Any]) -> str:
    partes = []
    if c["forma_pagamento_lida"]:
        partes.append(f"pagamento: {c['forma_pagamento_lida']}"
                      + ("" if c["condicao_pagamento_id"]
                         else " (não achei essa condição no cadastro)"))
    if c["prazo_entrega_dias"] is not None:
        partes.append(f"entrega em {c['prazo_entrega_dias']} dia(s)")
    if c["frete"] is not None:
        partes.append("frete grátis" if c["frete"] == "0.00"
                      else f"frete R$ {c['frete']}")
    if c["entrega"]:
        partes.append(ROTULO_ENTREGA[c["entrega"]])
    if c["validade"]:
        partes.append(f"preço vale até {c['validade']}")
    return " · ".join(partes) or "a proposta não diz as condições"
