# ============================================================================
# ERP — core/suprimentos/precos.py
# "QUANTO ISSO COSTUMA CUSTAR?" — a resposta ao lado do item, na hora de pedir.
#
# PEDIDO DO DONO, 18/09/2026:
#
#   "Isso é interessante inclusive para quando a gente está na tela das
#   solicitações já poder estar visualizando: ó, o insumo, onde está o último
#   menor preço, qual foi o fornecedor, qual o valor. Isso dá para montar uma
#   tela bem bacana."
#
# POR QUE ISTO NÃO REUSA `cotacao.historico_de_precos`
#
# Aquela função carrega a tabela INTEIRA de preços na memória e filtra em
# Python. Funcionava com algumas centenas de linhas nascidas aqui dentro. Com
# o histórico antigo importado (migração 077) são dezenas de milhares, e a tela
# de Solicitações precisa do resumo de VÁRIOS insumos de uma vez — o que, por
# aquele caminho, seria varrer tudo uma vez por item da lista.
#
# Aqui a conta é feita pelo BANCO, com WHERE e ORDER BY de verdade, e por isso
# esta parte tem teste com Postgres (`@pytest.mark.banco`): o dublê da suíte
# ignora WHERE, e um resumo de preços que ignora o WHERE responde o preço de
# outro material com cara de certo.
#
# AS TRÊS RESPOSTAS, e por que são três e não uma:
#
#   ÚLTIMO   o preço mais recente. É o que diz quanto custa HOJE — e é a única
#            das três que serve para orçar.
#   MENOR    o menor dos últimos doze meses, com quem deu. É a régua da
#            negociação: "da última vez fulano fez por 11,03".
#   MÉDIA    a média do período. Existe para mostrar quando o menor foi um
#            ponto fora da curva — menor de 8,65 numa média de 14 diz mais
#            sobre aquele dia do que sobre o material.
#
# A JANELA É DE DOZE MESES porque preço de material de construção de dois anos
# atrás não é referência, é curiosidade. O histórico inteiro continua no banco
# e aparece na tela do Banco de preços; o que a Solicitação mostra é o recente.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Fornecedor, PrecoHistorico

logger = logging.getLogger(__name__)

JANELA_DIAS = 365


def resumo_por_insumo(s: Session, insumo_ids: list[int], *,
                      hoje: Optional[date] = None) -> dict[int, dict[str, Any]]:
    """Último, menor e média dos últimos doze meses, para vários insumos.

    Uma consulta por pergunta, não uma por item da tela: a Solicitação pode ter
    quarenta linhas, e quarenta idas ao banco por linha é o que faz uma tela
    boa parecer travada.
    """
    ids = sorted({int(i) for i in (insumo_ids or []) if i})
    if not ids:
        return {}
    hoje = hoje or date.today()
    desde = hoje - timedelta(days=JANELA_DIAS)

    saida: dict[int, dict[str, Any]] = {}

    # --- o resumo do período (menor, maior, média, quantas vezes) ----------
    resumo = s.execute(
        select(PrecoHistorico.insumo_id,
               func.min(PrecoHistorico.preco_unitario),
               func.max(PrecoHistorico.preco_unitario),
               func.avg(PrecoHistorico.preco_unitario),
               func.count(PrecoHistorico.id))
        .where(PrecoHistorico.insumo_id.in_(ids))
        .where(PrecoHistorico.data >= desde)
        .group_by(PrecoHistorico.insumo_id)).all()
    for insumo_id, menor, maior, media, quantos in resumo:
        saida[insumo_id] = {
            "menor": _texto(menor), "maior": _texto(maior),
            "media": _texto(media, casas=2), "ocorrencias": int(quantos or 0),
            "desde": desde.isoformat(),
        }

    # --- quem deu o MENOR, e quando ---------------------------------------
    # Precisa da linha inteira, não só do número: a pergunta do comprador é
    # "quem fez esse preço?", e um valor sem fornecedor não serve de régua.
    for insumo_id, dados in saida.items():
        menor = s.scalars(
            select(PrecoHistorico)
            .where(PrecoHistorico.insumo_id == insumo_id)
            .where(PrecoHistorico.data >= desde)
            .order_by(PrecoHistorico.preco_unitario.asc(),
                      PrecoHistorico.data.desc())
            .limit(1)).first()
        if menor is not None:
            dados["menor_de"] = _quem(s, menor)
            dados["menor_em"] = menor.data.isoformat() if menor.data else None

    # --- o ÚLTIMO, que é o que serve para orçar -----------------------------
    for insumo_id in ids:
        ultimo = s.scalars(
            select(PrecoHistorico)
            .where(PrecoHistorico.insumo_id == insumo_id)
            .order_by(PrecoHistorico.data.desc(), PrecoHistorico.id.desc())
            .limit(1)).first()
        if ultimo is None:
            continue
        dados = saida.setdefault(insumo_id, {
            "menor": None, "maior": None, "media": None, "ocorrencias": 0,
            "desde": desde.isoformat()})
        dados["ultimo"] = _texto(ultimo.preco_unitario)
        dados["ultimo_de"] = _quem(s, ultimo)
        dados["ultimo_em"] = ultimo.data.isoformat() if ultimo.data else None
        dados["ultimo_unidade"] = ultimo.unidade
        # FORA DA JANELA: o preço existe mas é velho. Dizer isso é o ponto —
        # um preço de 2023 mostrado sem aviso vira orçamento errado.
        dados["ultimo_fora_da_janela"] = bool(ultimo.data and ultimo.data < desde)

    for insumo_id, dados in saida.items():
        dados["resumo"] = _frase(dados)
    return saida


def _quem(s: Session, registro: PrecoHistorico) -> Optional[str]:
    if not registro.fornecedor_id:
        return None
    forn = s.get(Fornecedor, registro.fornecedor_id)
    return (getattr(forn, "nome_fantasia", "") or
            getattr(forn, "razao_social", "") or None)


def _texto(valor: Any, casas: int = 4) -> Optional[str]:
    if valor is None:
        return None
    return str(Decimal(str(valor)).quantize(Decimal("1." + "0" * casas)))


def _frase(d: dict[str, Any]) -> str:
    """A linha que vai ao lado do item. Escrita como o comprador diria."""
    partes = []
    if d.get("ultimo"):
        frase = f"último R$ {_br(d['ultimo'])}"
        if d.get("ultimo_de"):
            frase += f" ({d['ultimo_de']})"
        if d.get("ultimo_em"):
            frase += f" em {_dia(d['ultimo_em'])}"
        if d.get("ultimo_fora_da_janela"):
            frase += " — mais de um ano atrás"
        partes.append(frase)
    if d.get("menor") and d.get("menor") != d.get("ultimo"):
        frase = f"menor no ano R$ {_br(d['menor'])}"
        if d.get("menor_de"):
            frase += f" ({d['menor_de']})"
        partes.append(frase)
    if d.get("ocorrencias"):
        partes.append(f"{d['ocorrencias']} cotação(ões) no último ano")
    return " · ".join(partes) or "sem preço no histórico"


def _br(texto: str) -> str:
    valor = Decimal(texto).quantize(Decimal("1.00"))
    inteiro, centavos = f"{valor:.2f}".split(".")
    milhar = f"{int(inteiro):,}".replace(",", ".")
    return f"{milhar},{centavos}"


def _dia(iso: str) -> str:
    a, m, d = iso.split("-")
    return f"{d}/{m}/{a}"
