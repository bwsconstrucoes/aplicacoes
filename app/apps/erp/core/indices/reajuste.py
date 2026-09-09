# ============================================================================
# ERP — core/indices/reajuste.py
# A previsão de reajuste da medição, e a previsão virando título.
#
# A REGRA, COMO O DONO DESCREVEU (09/09/2026):
#
#   - o índice normal é o INCC (INCC-DI, confirmado por ele);
#   - o direito nasce DOZE MESES depois da data-base;
#   - qual data-base vale MUDA POR CONTRATO — pode ser a do orçamento, pode ser
#     a da proposta da licitação.
#
# A CONTA
#
#   fator = produto de (1 + variação_do_mês/100), da data-base até o mês da
#           medição
#   reajuste = valor_da_medição × (fator − 1)
#
# Guardar a variação mensal e acumular na hora (em vez de guardar o acumulado)
# é o que permite calcular qualquer período — e refazer a conta de dois anos
# atrás e chegar no mesmo número, que é o que o órgão vai pedir.
#
# O QUE O SISTEMA NÃO FAZ, E É DE PROPÓSITO
#
# Ele não fecha o valor. Palavras dele: *"pode ser que o órgão tenha algum
# entendimento e mude algum centavo"*. O sistema ESTIMA; quem fecha é o órgão.
# Por isso a previsão vira um título com valor EDITÁVEL, e o título guarda o
# fator usado — assim a diferença entre o previsto e o aprovado fica visível em
# vez de sumir.
#
# E ele não inventa mês que falta. Se a tabela do índice não cobre o período
# inteiro, a resposta é "não dá para calcular ainda, faltam estes meses" — e
# não um número menor calculado com metade da série, que é o erro que passaria
# despercebido.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.indices import bcb
from app.apps.erp.db.models.cadastros import Contrato, IndiceEconomico, Obra
from app.apps.erp.db.models.financeiro import Rateio, Titulo

logger = logging.getLogger(__name__)
CENT = Decimal("0.01")
MESES_PADRAO = 12


def _mes(d: date) -> date:
    return d.replace(day=1)


def _proximo_mes(d: date) -> date:
    return (d.replace(year=d.year + 1, month=1) if d.month == 12
            else d.replace(month=d.month + 1))


def _meses_entre(de: date, ate: date) -> int:
    return (ate.year - de.year) * 12 + (ate.month - de.month)


def _reais(v: Any) -> str:
    from app.apps.erp.core.comum.formato import _dinheiro_br
    return _dinheiro_br(v)


def _pct_br(v: Any) -> str:
    return f"{float(v):.4f}".rstrip("0").rstrip(".").replace(".", ",") + "%"


def _mes_br(iso: str) -> str:
    """"2025-02-01" vira "02/2025". Mês em ISO no meio de uma frase em
    português é o tipo de detalhe que faz o texto parecer de máquina."""
    ano, mes = iso[:4], iso[5:7]
    return f"{mes}/{ano}"


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal(0)


# ---------------------------------------------------------------------------
# De onde vem a configuração
# ---------------------------------------------------------------------------
def configuracao(s: Session, contrato: Contrato) -> dict[str, Any]:
    """Data-base, índice e periodicidade — do contrato, ou herdados da obra.

    A herança existe porque a obra JÁ tinha `data_base_orcamento` e
    `indice_reajuste` antes de o contrato ter: sem ela, todo contrato antigo
    apareceria como "não configurado" mesmo com o dado no sistema. A resposta
    diz de ONDE veio, para ninguém achar que preencheu o que não preencheu.
    """
    obra = s.get(Obra, contrato.obra_id) if contrato.obra_id else None

    data_base, origem_do_dado = contrato.data_base, "contrato"
    if data_base is None and obra is not None and obra.data_base_orcamento:
        data_base, origem_do_dado = obra.data_base_orcamento, "obra"

    indice = (contrato.indice_reajuste or "").strip().upper()
    if not indice and obra is not None:
        indice = (obra.indice_reajuste or "").strip().upper()
    # "INCC" sozinho é o que se escreve no contrato; a série é a do INCC-DI,
    # confirmada pelo dono. Traduzir aqui evita um cadastro que não bate com
    # nenhuma linha da tabela e um reajuste que some sem explicação.
    if indice in ("INCC", "INCC DI", "INCCDI"):
        indice = "INCC-DI"

    return {
        "data_base": data_base,
        "data_base_origem": contrato.data_base_origem or "",
        "veio_de": origem_do_dado if data_base else "",
        "indice": indice or bcb.PADRAO,
        "indice_declarado": bool(indice),
        "meses": contrato.reajuste_meses or MESES_PADRAO,
        "configurado": bool(data_base),
    }


# ---------------------------------------------------------------------------
# O acumulado
# ---------------------------------------------------------------------------
def acumulado(s: Session, *, indice: str, de: date, ate: date) -> dict[str, Any]:
    """O fator acumulado do índice, do mês SEGUINTE à data-base até `ate`.

    Começa no mês seguinte de propósito: a data-base é o ponto zero, e o mês
    dela já está dentro do preço contratado. Incluí-lo cobraria um mês a mais.
    """
    indice = (indice or bcb.PADRAO).strip().upper()
    inicio, fim = _proximo_mes(_mes(de)), _mes(ate)
    if fim < inicio:
        return {"fator": Decimal(1), "meses": 0, "faltando": [], "completo": True,
                "de": inicio.isoformat(), "ate": fim.isoformat()}

    guardados = {i.competencia: i for i in s.scalars(select(IndiceEconomico).where(
        IndiceEconomico.codigo == indice,
        IndiceEconomico.competencia >= inicio,
        IndiceEconomico.competencia <= fim)).all()}

    fator, meses, faltando = Decimal(1), 0, []
    atual = inicio
    while atual <= fim:
        linha = guardados.get(atual)
        if linha is None:
            faltando.append(atual.isoformat())
        else:
            fator *= (Decimal(1) + _dec(linha.variacao_pct) / 100)
            meses += 1
        atual = _proximo_mes(atual)

    return {
        "fator": fator, "meses": meses, "faltando": faltando,
        "completo": not faltando,
        "de": inicio.isoformat(), "ate": fim.isoformat(),
        "variacao_pct": float(((fator - 1) * 100).quantize(Decimal("0.0001"))),
    }


# ---------------------------------------------------------------------------
# A previsão de uma medição
# ---------------------------------------------------------------------------
def prever(s: Session, titulo_id: int) -> dict[str, Any]:
    """Quanto de reajuste esta medição tem direito, e por quê.

    Devolve sempre um dicionário — inclusive quando não dá para calcular —,
    porque a TELA também chama isto para mostrar, e tela não deve quebrar por
    cadastro incompleto.
    """
    titulo = s.get(Titulo, titulo_id)
    if titulo is None:
        raise ErroValidacao("Título não encontrado.")
    contrato = s.get(Contrato, titulo.contrato_id) if titulo.contrato_id else None
    if contrato is None:
        return _sem("Esta medição não está ligada a um contrato — é do contrato "
                    "que vêm a data-base e o índice do reajuste.")

    cfg = configuracao(s, contrato)
    if not cfg["configurado"]:
        return _sem("O contrato não tem data-base do reajuste. Informe se é a do "
                    "orçamento ou a da proposta, no cadastro do contrato — muda "
                    "por contrato, e é dela que a conta parte.", cfg)

    # O mês de referência é o FIM do período medido: é o custo daquele mês que
    # o índice corrige. Sem período, cai na competência do título.
    referencia = _mes(titulo.periodo_fim or titulo.competencia or date.today())
    data_base = cfg["data_base"]
    idade = _meses_entre(_mes(data_base), referencia)
    if idade < cfg["meses"]:
        faltam = cfg["meses"] - idade
        return _sem(f"Ainda não faz aniversário: o direito nasce {cfg['meses']} "
                    f"meses depois da data-base ({data_base.strftime('%d/%m/%Y')}), "
                    f"e faltam {faltam} mês(es).", cfg,
                    extra={"meses_desde_data_base": idade})

    acc = acumulado(s, indice=cfg["indice"], de=data_base, ate=referencia)
    if not acc["completo"]:
        faltando = acc["faltando"]
        return _sem(f"Faltam {len(faltando)} mês(es) do {cfg['indice']} na tabela "
                    f"({', '.join(faltando[:6])}{'…' if len(faltando) > 6 else ''}). "
                    f"Atualize os índices em Configurações, ou lance o mês à mão "
                    f"pelo boletim da FGV. Calcular sem eles daria um valor menor "
                    f"com cara de certo.", cfg, extra={"faltando": faltando})

    base = _dec(titulo.valor_liquido)
    valor = (base * (acc["fator"] - 1)).quantize(CENT, rounding=ROUND_HALF_UP)
    return {
        "pode": True, "motivo": "",
        "titulo_id": titulo.id, "numero_sp": titulo.numero_sp,
        "medicao": titulo.numero_medicao,
        "indice": cfg["indice"],
        "data_base": data_base.isoformat(),
        "data_base_origem": cfg["data_base_origem"],
        "veio_de": cfg["veio_de"],
        "meses_exigidos": cfg["meses"],
        "meses_desde_data_base": idade,
        "referencia": referencia.isoformat(),
        "meses_no_calculo": acc["meses"],
        "fator": float(acc["fator"].quantize(Decimal("0.00000001"))),
        "variacao_pct": acc["variacao_pct"],
        "base": float(base),
        "valor": float(valor),
        # A conta escrita como uma pessoa lê. Formato americano aqui seria
        # lido errado justamente por quem vai defender o número no órgão.
        "explicacao": (
            f"{cfg['indice']} acumulado de {_mes_br(acc['de'])} a "
            f"{_mes_br(acc['ate'])} ({acc['meses']} meses) = "
            f"{_pct_br(acc['variacao_pct'])}. Reajuste = {_reais(base)} × "
            f"{_pct_br(acc['variacao_pct'])} = {_reais(valor)}."),
        "ja_gerado": _reajuste_ja_gerado(s, titulo),
    }


def _sem(motivo: str, cfg: Optional[dict] = None,
         extra: Optional[dict] = None) -> dict[str, Any]:
    d = {"pode": False, "motivo": motivo, "valor": 0.0,
         "indice": (cfg or {}).get("indice", bcb.PADRAO),
         "data_base": (cfg["data_base"].isoformat()
                       if cfg and cfg.get("data_base") else None),
         "meses_exigidos": (cfg or {}).get("meses", MESES_PADRAO)}
    d.update(extra or {})
    return d


def _reajuste_ja_gerado(s: Session, titulo: Titulo) -> Optional[dict[str, Any]]:
    """O reajuste desta medição já virou título? Sem isto, dois cliques geram
    dois títulos e o contrato passa a cobrar o reajuste em dobro."""
    outro = s.scalars(select(Titulo).where(
        Titulo.medicao_de_id == titulo.id)).first()
    if outro is None:
        return None
    return {"titulo_id": outro.id, "numero_sp": outro.numero_sp,
            "medicao": outro.numero_medicao,
            "valor": float(outro.valor_liquido or 0),
            "status": (outro.status.value if hasattr(outro.status, "value")
                       else str(outro.status))}


def previsao_do_contrato(s: Session, contrato_id: int) -> dict[str, Any]:
    """A previsão de todas as medições do contrato, somada.

    É o número que o dono quer ver antes de qualquer título existir: quanto de
    reajuste esta obra tem para receber.
    """
    contrato = s.get(Contrato, contrato_id)
    if contrato is None:
        raise ErroValidacao("Contrato não encontrado.")
    cfg = configuracao(s, contrato)

    linhas, total, pendentes = [], Decimal(0), []
    for t in s.scalars(select(Titulo).where(
            Titulo.contrato_id == contrato_id,
            Titulo.numero_medicao.is_not(None)).order_by(Titulo.id)).all():
        # Medição que JÁ É reajuste não se reajusta de novo — seria juros sobre
        # juros, e o órgão não paga isso.
        if t.medicao_de_id:
            continue
        p = prever(s, t.id)
        linhas.append(p)
        if p["pode"]:
            if p["ja_gerado"]:
                continue                      # já virou título; não conta duas vezes
            total += _dec(p["valor"])
            pendentes.append(t.numero_medicao)

    return {
        "contrato_id": contrato_id,
        "configuracao": {
            "data_base": cfg["data_base"].isoformat() if cfg["data_base"] else None,
            "data_base_origem": cfg["data_base_origem"],
            "veio_de": cfg["veio_de"],
            "indice": cfg["indice"],
            "meses": cfg["meses"],
            "configurado": cfg["configurado"],
        },
        "medicoes": linhas,
        "a_gerar": float(total.quantize(CENT)),
        "medicoes_a_gerar": pendentes,
    }


# ---------------------------------------------------------------------------
# A previsão virando título de verdade
# ---------------------------------------------------------------------------
def gerar_titulo(s: Session, titulo_id: int, *, valor: Optional[Any] = None,
                 numero_medicao: str = "", usuario=None) -> Titulo:
    """A previsão vira título a receber — com valor EDITÁVEL.

    O dono foi claro: *"pode ser que o órgão tenha algum entendimento e mude
    algum centavo"*. Então o valor calculado é sugestão, não sentença; e o
    título guarda o fator usado, para a diferença entre o previsto e o aprovado
    ficar visível em vez de sumir.
    """
    from app.apps.erp.core.titulos import medicao as svc_medicao
    from app.apps.erp.core.titulos import receber as svc_receber

    origem = s.get(Titulo, titulo_id)
    if origem is None:
        raise ErroValidacao("Medição não encontrada.")
    ja = _reajuste_ja_gerado(s, origem)
    if ja:
        raise ErroValidacao(
            f"O reajuste da medição {origem.numero_medicao} já foi gerado em "
            f"{ja['numero_sp']}. Se o valor mudou, corrija aquele título — "
            f"gerar outro cobraria o reajuste duas vezes.")

    p = prever(s, titulo_id)
    if not p["pode"]:
        raise ErroValidacao(p["motivo"])

    valor_final = _dec(valor) if valor not in (None, "") else _dec(p["valor"])
    if valor_final <= 0:
        raise ErroValidacao("O valor do reajuste tem de ser maior que zero.")

    obra_id = next((r.obra_id for r in s.scalars(
        select(Rateio).where(Rateio.titulo_id == origem.id)).all() if r.obra_id), None)
    if obra_id is None:
        raise ErroValidacao("A medição de origem não está ligada a nenhuma obra.")

    numero = (numero_medicao or "").strip() or f"{origem.numero_medicao}R"
    novo = svc_receber.criar_medicao(s, {
        "obra_id": obra_id,
        "contrato_id": origem.contrato_id,
        "cliente_id": origem.fornecedor_id,
        "numero_medicao": numero,
        "periodo_inicio": origem.periodo_inicio,
        "periodo_fim": origem.periodo_fim,
        "valor_bruto": str(valor_final),
        "descricao": (f"Reajuste da medição {origem.numero_medicao} — "
                      f"{p['indice']} acumulado desde {p['data_base']}"),
        "vencimento": date.today().isoformat(),
        "origem": "REAJUSTE",
    }, usuario)

    # A correlação é o que faz o quadro do contrato somar certo: sem ela o
    # reajuste vira medição solta e passa a consumir saldo de obra executada.
    svc_medicao.classificar(s, novo.id, tipo="REAJUSTE",
                            medicao_de_id=origem.id, usuario=usuario)
    novo.reajuste_indice = p["indice"]
    novo.reajuste_data_base = date.fromisoformat(p["data_base"])
    novo.reajuste_ate = date.fromisoformat(p["referencia"])
    novo.reajuste_fator = Decimal(str(p["fator"]))
    novo.reajuste_previsto = _dec(p["valor"])
    s.flush()
    logger.info("ERP/reajuste: %s gerado a partir de %s (previsto %s, lançado %s)",
                novo.numero_sp, origem.numero_sp, p["valor"], valor_final)
    return novo
