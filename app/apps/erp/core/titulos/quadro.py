# ============================================================================
# ERP — core/titulos/quadro.py
# O quadro financeiro do contrato: as medições e os totais.
#
# POR QUE ENTRAR PELO CONTRATO, E NÃO PELA MEDIÇÃO
#
# O dono mudou de ideia no meio da fala, e a segunda ideia é a certa: *"até
# pensei que seria melhor ir pela obra, pelo contrato, ou pela medição. Aí
# talvez seja até interessante pelo contrato, né? Que lá vai estar relacionando
# todas as medições."*
#
# O motivo é estrutural: contrato é o que tem começo, meio e fim; medição é um
# evento dentro dele. Quem pergunta "como está essa obra?" quer o contrato;
# quem pergunta "e a medição 3?" chega nela pelo contrato.
#
# OS SETE TOTAIS respondem perguntas DIFERENTES, e é por isso que são sete e
# não um. Hoje cada um deles se responde somando planilha:
#
#   contratado  o valor original do contrato
#   aditivado   o que os aditivos acrescentaram
#   vigente     contratado + aditivado — é o teto de verdade
#   medido      o que já foi medido (inclusive reajuste)
#   faturado    o que virou NOTA — medir não é faturar
#   recebido    o que entrou na conta — faturar não é receber
#   a_receber   faturado − recebido
#   saldo       vigente − medido: quanto ainda dá para medir
# ============================================================================
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import medicao as svc_medicao
from app.apps.erp.db.models.cadastros import Contrato, Obra, ObraAditivo
from app.apps.erp.db.models.financeiro import (MedicaoTipo, NotaEmitida,
                                               Pagamento, Parcela, Titulo)

logger = logging.getLogger(__name__)
CENT = Decimal("0.01")


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal(0)


def _f(v: Decimal) -> float:
    return float(v.quantize(CENT))


def quadro(s: Session, contrato_id: int) -> dict[str, Any]:
    """As medições do contrato e os totais. É a tela que o dono desenhou."""
    contrato = s.get(Contrato, contrato_id)
    if contrato is None:
        raise ErroValidacao("Contrato não encontrado.")
    obra = s.get(Obra, contrato.obra_id) if contrato.obra_id else None

    medicoes = list(s.scalars(select(Titulo).where(
        Titulo.contrato_id == contrato_id,
        Titulo.numero_medicao.is_not(None)).order_by(Titulo.id)).all())

    linhas = [svc_medicao.ler(s, t) for t in medicoes]

    # ---- o que virou NOTA, e o que entrou na conta
    notas = {}
    for n in s.scalars(select(NotaEmitida).where(
            NotaEmitida.titulo_id.in_([t.id for t in medicoes] or [-1]),
            NotaEmitida.situacao == "EMITIDA")).all():
        notas.setdefault(n.titulo_id, []).append(n)

    recebido_por_titulo: dict[int, Decimal] = {}
    for p in s.scalars(select(Pagamento).join(
            Parcela, Pagamento.parcela_id == Parcela.id).where(
            Parcela.titulo_id.in_([t.id for t in medicoes] or [-1]))).all():
        parcela = s.get(Parcela, p.parcela_id)
        if parcela is not None:
            recebido_por_titulo[parcela.titulo_id] = (
                recebido_por_titulo.get(parcela.titulo_id, Decimal(0))
                + _dec(p.valor_pago))

    for linha, t in zip(linhas, medicoes):
        das_notas = notas.get(t.id, [])
        linha["notas"] = [{"numero": n.numero_nota, "em": (n.data_emissao.isoformat()
                                                           if n.data_emissao else None)}
                          for n in das_notas]
        linha["faturado"] = _f(sum((_dec(n.valor_bruto) for n in das_notas), Decimal(0)))
        linha["recebido"] = _f(recebido_por_titulo.get(t.id, Decimal(0)))

    # ---- os sete totais
    contratado = _dec(contrato.valor_total)
    aditivado = Decimal(0)
    if obra is not None:
        aditivado = sum((_dec(a.valor) for a in s.scalars(
            select(ObraAditivo).where(ObraAditivo.obra_id == obra.id)).all()),
            Decimal(0))
    vigente = contratado + aditivado
    medido = sum((_dec(t.valor_liquido) for t in medicoes), Decimal(0))
    faturado = sum((Decimal(str(l["faturado"])) for l in linhas), Decimal(0))
    recebido = sum((Decimal(str(l["recebido"])) for l in linhas), Decimal(0))

    # O reajuste é medição também, mas separá-lo importa: no quadro do contrato
    # ele NÃO consome saldo do contratado — é acréscimo por índice, não obra
    # executada a mais.
    de_reajuste = sum((_dec(t.valor_liquido) for t, l in zip(medicoes, linhas)
                       if l["e_reajuste"]), Decimal(0))

    return {
        "contrato": {
            "id": contrato.id, "objeto": contrato.objeto,
            "obra": getattr(obra, "codigo", ""),
            "obra_id": getattr(obra, "id", None),
            "status": contrato.status,
            "vigencia_inicio": (contrato.vigencia_inicio.isoformat()
                                if contrato.vigencia_inicio else None),
            "vigencia_fim": (contrato.vigencia_fim.isoformat()
                             if contrato.vigencia_fim else None),
            "indice_reajuste": contrato.indice_reajuste or "",
        },
        "medicoes": linhas,
        "totais": {
            "contratado": _f(contratado),
            "aditivado": _f(aditivado),
            "vigente": _f(vigente),
            "medido": _f(medido),
            "medido_sem_reajuste": _f(medido - de_reajuste),
            "reajuste": _f(de_reajuste),
            "faturado": _f(faturado),
            "recebido": _f(recebido),
            # "A receber" nunca é negativo: o que passa do faturado NÃO é uma
            # dívida ao contrário, é dinheiro que entrou sem nota emitida —
            # outra coisa, e das que a contabilidade precisa ver. Mostrar
            # "-465.000 a receber" seria uma frase sem sentido no lugar de um
            # alerta fiscal.
            "a_receber": _f(max(faturado - recebido, Decimal(0))),
            "recebido_sem_nota": _f(max(recebido - faturado, Decimal(0))),
            "saldo": _f(vigente - (medido - de_reajuste)),
            # A RÉGUA DO RECEBIMENTO. O dono desfez a pergunta "quanto falta
            # receber" em quatro leituras, em 10/09/2026, e todas as quatro são
            # legítimas — elas são etapas de uma mesma esteira:
            #
            #   CONTRATO (+aditivos) → MEDIDO → FATURADO → RECEBIDO
            #
            # Duas já estavam aqui ("a_receber", que é faturado − recebido, e
            # "saldo", que é o que falta medir). Faltavam estas: o que falta
            # receber do CONTRATO INTEIRO, tenha sido medido ou não, e o que
            # falta receber DO QUE JÁ ESTÁ MEDIDO.
            #
            # Elas existem para a resposta mostrar a régua toda em vez de um
            # número solto: assim a leitura que a pessoa queria já está na
            # tela, e ela não precisa ter acertado a pergunta.
            "falta_receber_do_contrato": _f(max(vigente - recebido, Decimal(0))),
            "falta_receber_do_medido": _f(max(medido - recebido, Decimal(0))),
            "falta_faturar_do_medido": _f(max(medido - faturado, Decimal(0))),
            "medido_pct": (round(float((medido - de_reajuste) / vigente * 100), 1)
                           if vigente else None),
        },
        # A pergunta que a tela responde de olho: o que já foi medido e ainda
        # NÃO virou nota, e o que virou nota e ainda não entrou.
        "pendencias": {
            "medido_sem_nota": [l["numero_sp"] for l in linhas
                                if not l["notas"] and l["valor"] > 0],
            "faturado_sem_receber": [l["numero_sp"] for l in linhas
                                     if l["notas"] and l["recebido"] < l["faturado"] - 0.01],
            "sem_protocolo": [l["numero_sp"] for l in linhas if not l["protocolo"]],
        },
        "tempo_de_recebimento": svc_medicao.tempo_de_recebimento(
            s, contrato_id=contrato_id),
    }


def listar_contratos(s: Session, *, obra_id: Optional[int] = None,
                     limite: int = 200) -> list[dict[str, Any]]:
    """Os contratos, com o essencial do quadro — para a lista antes de abrir.

    A aritmética é a MESMA do quadro, e isso não é zelo: na primeira versão a
    lista descontava o reajuste do saldo e o quadro não, e os dois números
    apareciam na mesma sessão, diferentes, sobre o mesmo contrato. Quem visse
    isso perderia a confiança nos dois — com razão.
    """
    stmt = select(Contrato).order_by(Contrato.id.desc()).limit(limite)
    if obra_id:
        stmt = stmt.where(Contrato.obra_id == obra_id)
    saida = []
    for c in s.scalars(stmt).all():
        obra = s.get(Obra, c.obra_id) if c.obra_id else None
        aditivado = Decimal(0)
        if obra is not None:
            aditivado = sum((_dec(a.valor) for a in s.scalars(
                select(ObraAditivo).where(ObraAditivo.obra_id == obra.id)).all()),
                Decimal(0))
        medicoes = list(s.scalars(select(Titulo).where(
            Titulo.contrato_id == c.id,
            Titulo.numero_medicao.is_not(None))).all())
        reajuste = {t.codigo for t in s.scalars(
            select(MedicaoTipo).where(MedicaoTipo.e_reajuste.is_(True))).all()}
        medido = sum((_dec(t.valor_liquido) for t in medicoes), Decimal(0))
        de_reajuste = sum((_dec(t.valor_liquido) for t in medicoes
                           if t.medicao_tipo in reajuste), Decimal(0))
        vigente = _dec(c.valor_total) + aditivado
        saida.append({
            "id": c.id, "objeto": c.objeto, "obra": getattr(obra, "codigo", ""),
            "status": c.status,
            "contratado": _f(_dec(c.valor_total)),
            "aditivado": _f(aditivado),
            "vigente": _f(vigente),
            "medido": _f(medido),
            "reajuste": _f(de_reajuste),
            "medicoes": len(medicoes),
            # O reajuste é acréscimo por índice, não obra executada a mais:
            # não consome saldo do contrato. Mesma regra do quadro.
            "saldo": _f(vigente - (medido - de_reajuste)),
        })
    return saida
