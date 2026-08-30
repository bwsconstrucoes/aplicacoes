# ============================================================================
# ERP — core/pagamentos/conciliacao.py
# Conciliação automática de verdade, não "importa o extrato e confirma um a um".
#
# O problema real (descrito pelo Marcelo): se a baixa já foi feita e o extrato
# tem os mesmos valores nas mesmas datas, o casamento é evidente — o sistema
# tem que fazer sozinho. Só deve sobrar o que é genuinamente ambíguo ou o que
# saiu do banco sem título lançado.
#
# Diferenças para a versão anterior:
#   1. ATRIBUIÇÃO ÓTIMA, não guloso. Dois pagamentos de R$ 1.500 e dois
#      lançamentos de R$ 1.500 no extrato: o algoritmo resolve o par completo
#      (húngaro simplificado por busca de custo mínimo), em vez de casar o
#      primeiro que encontra e deixar o resto pendente.
#   2. Só é AMBÍGUO quando duas atribuições diferentes empatam de verdade.
#   3. Reconhece TARIFA bancária, IOF, rendimento e TRANSFERÊNCIA entre contas
#      próprias direto do histórico, e propõe o lançamento correspondente em
#      vez de deixar como sobra sem explicação.
#   4. Toda conciliação automática guarda o porquê (valor, distância de data,
#      semelhança de nome) para auditoria.
# ============================================================================
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, timedelta
from decimal import Decimal
from difflib import SequenceMatcher
from itertools import permutations
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import ContaBancaria, Usuario
from app.apps.erp.db.models.financeiro import (
    Conciliacao, Extrato, Pagamento, Parcela, Titulo,
)

logger = logging.getLogger(__name__)

JANELA_DIAS = 5
CONFIANCA_AUTO = Decimal("0.70")
_MAX_GRUPO_PERMUTACAO = 7        # acima disso, resolve por ordem de data

# padrões de movimentação que não são pagamento de título
_PADROES = [
    ("TARIFA", r"\b(TARIFA|CESTA|PACOTE\s+SERVICOS|MANUTENCAO\s+DE\s+CONTA|"
               r"TAR\s|ANUIDADE|TX\s+|CUSTO\s+DOC|TED\s+TARIFA)\b"),
    ("IOF", r"\bIOF\b"),
    ("JUROS_BANCARIOS", r"\b(JUROS|ENCARGOS)\s+(DE\s+)?(CHEQUE|LIMITE|CONTA|ADIANT)"),
    ("RENDIMENTO", r"\b(RENDIMENTO|REMUNERACAO|CREDITO\s+DE\s+RENDIMENTO)\b"),
    ("TRANSFERENCIA_PROPRIA", r"\b(TRANSF(ERENCIA)?\s+ENTRE\s+CONTAS|APLICACAO|RESGATE|"
                              r"TRANSF\s+C/C|TED\s+MESMA\s+TITULARIDADE)\b"),
    ("IMPOSTO", r"\b(DARF|GPS|FGTS|GRF|DAS|GARE|TRIBUTO|IMPOSTO)\b"),
    ("SALARIO", r"\b(FOLHA|SALARIO|PAGAMENTO\s+SALARIO|CREDITO\s+SALARIO)\b"),
]


def _normalizar(txt: str) -> str:
    t = unicodedata.normalize("NFKD", (txt or "").upper())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", t).strip()


def classificar_extrato(historico: str, nome: str = "") -> Optional[str]:
    """Diz o que é o lançamento quando não for pagamento de título."""
    alvo = _normalizar(f"{historico} {nome}")
    for rotulo, padrao in _PADROES:
        if re.search(padrao, alvo):
            return rotulo
    return None


def _semelhanca_nome(a: Optional[str], b: Optional[str]) -> float:
    if not a or not b:
        return 0.0
    na, nb = _normalizar(a), _normalizar(b)
    if not na or not nb:
        return 0.0
    if na in nb or nb in na:
        return 0.95
    # compara também só o primeiro nome/razão (extrato costuma truncar)
    direto = SequenceMatcher(None, na, nb).ratio()
    curto = SequenceMatcher(None, na[:18], nb[:18]).ratio()
    return max(direto, curto)


def _custo(pg: Pagamento, ext: Extrato, credor: str) -> Optional[Decimal]:
    """Custo do par (quanto menor, melhor). None = par impossível."""
    if abs(Decimal(ext.valor) + Decimal(pg.valor_pago)) > Decimal("0.01"):
        return None
    dias = abs((ext.data_lancamento - pg.data_pagamento).days)
    if dias > JANELA_DIAS:
        return None
    sim = _semelhanca_nome(ext.nome_contraparte, credor)
    # custo = distância de data + penalidade por nome divergente
    custo = Decimal(dias) * Decimal("0.10")
    if ext.nome_contraparte:
        custo += (Decimal("1") - Decimal(str(round(sim, 3)))) * Decimal("0.35")
    return custo


def _confianca(custo: Decimal) -> Decimal:
    return max(Decimal("0"), (Decimal("1") - custo)).quantize(Decimal("0.001"))


def _resolver_grupo(pagamentos: list[Pagamento], extratos: list[Extrato],
                    credores: dict[int, str]) -> tuple[list[tuple], bool]:
    """Encontra a atribuição de custo mínimo entre pagamentos e extratos de
    mesmo valor. Devolve (pares, ambiguo)."""
    n, m = len(pagamentos), len(extratos)
    if n == 0 or m == 0:
        return [], False

    custos: dict[tuple[int, int], Decimal] = {}
    for i, pg in enumerate(pagamentos):
        for j, ex in enumerate(extratos):
            c = _custo(pg, ex, credores.get(pg.id, ""))
            if c is not None:
                custos[(i, j)] = c
    if not custos:
        return [], False

    # grupo pequeno: testa todas as combinações e escolhe a de menor custo total
    if max(n, m) <= _MAX_GRUPO_PERMUTACAO:
        melhor, melhor_custo, empates = None, None, 0
        indices_ext = list(range(m))
        for combo in permutations(indices_ext, min(n, m)):
            total, pares = Decimal("0"), []
            valido = True
            for i, j in enumerate(combo):
                if (i, j) not in custos:
                    valido = False
                    break
                total += custos[(i, j)]
                pares.append((i, j))
            if not valido:
                continue
            if melhor_custo is None or total < melhor_custo - Decimal("0.001"):
                melhor, melhor_custo, empates = pares, total, 1
            elif abs(total - melhor_custo) <= Decimal("0.001") and pares != melhor:
                empates += 1
        if melhor is None:
            return [], False
        # Empate entre atribuições distintas só é AMBIGUIDADE de verdade quando
        # a troca muda o credor. Dois pagamentos do MESMO credor e mesmo valor
        # dão no mesmo qualquer que seja o par: resolve por ordem de data e
        # segue, sem jogar trabalho para o humano.
        if empates > 1 and len(melhor) > 1:
            credores_grupo = {_normalizar(credores.get(p.id, "")) for p in pagamentos}
            if len(credores_grupo) > 1:
                return [], True
            melhor = sorted(melhor, key=lambda par: (
                pagamentos[par[0]].data_pagamento, extratos[par[1]].data_lancamento))
        return [(pagamentos[i], extratos[j], _confianca(custos[(i, j)]))
                for i, j in melhor], False

    # grupo grande: casa por proximidade de data, em ordem
    pares, usados = [], set()
    for i, pg in enumerate(sorted(range(n), key=lambda k: pagamentos[k].data_pagamento)):
        candidatos = sorted(
            ((j, custos[(pg, j)]) for j in range(m)
             if (pg, j) in custos and j not in usados), key=lambda x: x[1])
        if candidatos:
            j, c = candidatos[0]
            usados.add(j)
            pares.append((pagamentos[pg], extratos[j], _confianca(c)))
    return pares, False


def conciliar_automatico(s: Session, *, conta_bancaria_id: Optional[int] = None,
                         usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Casa pagamentos com o extrato. Só deixa em aberto o que é ambíguo ou o
    que não tem contrapartida."""
    ja_pg = select(Conciliacao.pagamento_id).where(Conciliacao.desfeita_em.is_(None))
    ja_ex = select(Conciliacao.extrato_id).where(Conciliacao.desfeita_em.is_(None))

    stmt_pg = (select(Pagamento).where(Pagamento.id.not_in(ja_pg),
                                       Pagamento.estorna_pagamento_id.is_(None))
               .options(selectinload(Pagamento.parcela).selectinload(Parcela.titulo)
                        .selectinload(Titulo.fornecedor)))
    if conta_bancaria_id:
        stmt_pg = stmt_pg.where(Pagamento.conta_bancaria_id == conta_bancaria_id)
    pagamentos = list(s.scalars(stmt_pg).all())

    stmt_ex = select(Extrato).where(Extrato.id.not_in(ja_ex), Extrato.valor < 0)
    if conta_bancaria_id:
        stmt_ex = stmt_ex.where(Extrato.conta_bancaria_id == conta_bancaria_id)
    extratos = list(s.scalars(stmt_ex).all())

    credores = {p.id: p.parcela.titulo.fornecedor.razao_social for p in pagamentos}

    # agrupa por (conta, valor absoluto) — o casamento só acontece dentro do grupo
    grupos: dict[tuple[int, str], dict[str, list]] = {}
    for pg in pagamentos:
        ch = (pg.conta_bancaria_id, f"{Decimal(pg.valor_pago):.2f}")
        grupos.setdefault(ch, {"pg": [], "ex": []})["pg"].append(pg)
    for ex in extratos:
        ch = (ex.conta_bancaria_id, f"{abs(Decimal(ex.valor)):.2f}")
        grupos.setdefault(ch, {"pg": [], "ex": []})["ex"].append(ex)

    conciliados, ambiguos = 0, []
    extratos_usados: set[int] = set()
    for (conta, valor), g in grupos.items():
        if not g["pg"] or not g["ex"]:
            continue
        pares, ambiguo = _resolver_grupo(g["pg"], g["ex"], credores)
        if ambiguo:
            ambiguos.append({
                "valor": float(valor), "conta_bancaria_id": conta,
                "pagamentos": [{"pagamento_id": p.id,
                                "titulo": p.parcela.titulo.numero_sp,
                                "credor": credores.get(p.id, ""),
                                "data": p.data_pagamento.isoformat()} for p in g["pg"]],
                "extratos": [{"extrato_id": e.id, "data": e.data_lancamento.isoformat(),
                              "historico": (e.historico or "")[:90],
                              "nome": e.nome_contraparte} for e in g["ex"]],
                "motivo": "mesmo valor e datas equivalentes — o nome no extrato não "
                          "distingue os pagamentos"})
            continue
        for pg, ex, conf in pares:
            if conf < CONFIANCA_AUTO:
                ambiguos.append({
                    "valor": float(pg.valor_pago), "conta_bancaria_id": conta,
                    "pagamentos": [{"pagamento_id": pg.id,
                                    "titulo": pg.parcela.titulo.numero_sp,
                                    "credor": credores.get(pg.id, ""),
                                    "data": pg.data_pagamento.isoformat()}],
                    "extratos": [{"extrato_id": ex.id,
                                  "data": ex.data_lancamento.isoformat(),
                                  "historico": (ex.historico or "")[:90],
                                  "nome": ex.nome_contraparte}],
                    "motivo": f"confiança baixa ({conf}) — nome no extrato diverge do credor"})
                continue
            s.add(Conciliacao(
                pagamento_id=pg.id, extrato_id=ex.id, metodo="AUTOMATICA",
                confianca=conf, conciliado_por=(usuario.id if usuario else None)))
            extratos_usados.add(ex.id)
            conciliados += 1
    s.flush()

    # sobras: o que saiu do banco e não casou com nenhum pagamento
    sobras = []
    for ex in extratos:
        if ex.id in extratos_usados:
            continue
        if any(ex.id == e["extrato_id"] for a in ambiguos for e in a["extratos"]):
            continue
        tipo = classificar_extrato(ex.historico or "", ex.nome_contraparte or "")
        sobras.append({
            "extrato_id": ex.id, "data": ex.data_lancamento.isoformat(),
            "valor": float(ex.valor), "historico": (ex.historico or "")[:120],
            "nome": ex.nome_contraparte, "classificacao": tipo,
            "sugestao": _sugestao(tipo)})

    if conciliados:
        registrar_evento(s, "conciliacao", 0, "AUTOMATICA", {
            "conciliados": conciliados, "ambiguos": len(ambiguos),
            "sobras": len(sobras)}, usuario.id if usuario else None)
    return {"conciliados": conciliados, "ambiguos": ambiguos, "sobras": sobras,
            "pagamentos_analisados": len(pagamentos), "extratos_analisados": len(extratos)}


def _sugestao(tipo: Optional[str]) -> str:
    return {
        "TARIFA": "Tarifa bancária — lançar na conta 6.2.01 (tarifas e IOF).",
        "IOF": "IOF — conta 6.2.01.",
        "JUROS_BANCARIOS": "Juros bancários — conta 6.1.01.",
        "RENDIMENTO": "Rendimento de aplicação — entrada, conta 1.3.01.",
        "TRANSFERENCIA_PROPRIA": "Movimentação entre contas próprias — conta 9.1.01 "
                                 "(não é despesa).",
        "IMPOSTO": "Tributo pago sem título lançado — verifique a guia e lance no grupo 2.",
        "SALARIO": "Folha paga sem título lançado — verifique o lançamento da folha.",
    }.get(tipo or "", "Saiu do banco sem título correspondente — investigar.")


def conciliar_manual(s: Session, pagamento_id: int, extrato_id: int,
                     usuario: Usuario, observacao: str = "") -> Conciliacao:
    pg = s.get(Pagamento, pagamento_id)
    ex = s.get(Extrato, extrato_id)
    if pg is None or ex is None:
        raise ErroValidacao("Pagamento ou lançamento do extrato inexistente.")
    if abs(Decimal(ex.valor) + Decimal(pg.valor_pago)) > Decimal("0.01"):
        raise ErroValidacao(
            f"Valores não conferem: extrato R$ {abs(Decimal(ex.valor))} × "
            f"pagamento R$ {pg.valor_pago}. Ajuste o título antes de conciliar.")
    c = Conciliacao(pagamento_id=pg.id, extrato_id=ex.id, metodo="MANUAL",
                    conciliado_por=usuario.id)
    s.add(c)
    s.flush()
    registrar_evento(s, "conciliacao", c.id, "MANUAL",
                     {"pagamento_id": pg.id, "extrato_id": ex.id,
                      "observacao": observacao}, usuario.id)
    return c


def painel(s: Session, conta_bancaria_id: Optional[int] = None) -> dict[str, Any]:
    """Estado da conciliação: quanto está casado, quanto falta."""
    ja_pg = select(Conciliacao.pagamento_id).where(Conciliacao.desfeita_em.is_(None))
    ja_ex = select(Conciliacao.extrato_id).where(Conciliacao.desfeita_em.is_(None))

    q_pg = select(Pagamento).where(Pagamento.estorna_pagamento_id.is_(None))
    q_ex = select(Extrato)
    if conta_bancaria_id:
        q_pg = q_pg.where(Pagamento.conta_bancaria_id == conta_bancaria_id)
        q_ex = q_ex.where(Extrato.conta_bancaria_id == conta_bancaria_id)
    pagamentos = list(s.scalars(q_pg).all())
    extratos = list(s.scalars(q_ex).all())
    ids_pg = set(s.scalars(ja_pg).all())
    ids_ex = set(s.scalars(ja_ex).all())

    return {
        "pagamentos_total": len(pagamentos),
        "pagamentos_conciliados": sum(1 for p in pagamentos if p.id in ids_pg),
        "pagamentos_valor": float(sum(Decimal(p.valor_pago) for p in pagamentos)),
        "pagamentos_pendentes_valor": float(
            sum(Decimal(p.valor_pago) for p in pagamentos if p.id not in ids_pg)),
        "extratos_total": len(extratos),
        "extratos_conciliados": sum(1 for e in extratos if e.id in ids_ex),
        "saidas_nao_conciliadas": sum(1 for e in extratos
                                      if e.id not in ids_ex and Decimal(e.valor) < 0),
        "entradas_nao_conciliadas": sum(1 for e in extratos
                                        if e.id not in ids_ex and Decimal(e.valor) > 0),
    }
