# ============================================================================
# ERP — core/titulos/tributacao.py
# Retenções da medição calculadas a partir do CADASTRO DA OBRA, no mesmo
# modelo que o módulo emissaonf já usa nas notas reais da BWS.
#
# Regras (conferidas contra app/apps/emissaonf/tributacao.py):
#   INSS  — 11% sobre a parcela de SERVIÇO. Em empreitada com material, a base
#           costuma ser 50% do valor (11% × 50% = 5,5% efetivos). "Não retém"
#           é base zero, não alíquota zero.
#   ISS   — alíquota do MUNICÍPIO, gravada na obra. Se o município aceita
#           dedução de material, a base é só a parcela de serviço; se não
#           aceita, incide sobre o valor cheio. Pode ser retido pelo tomador
#           ou recolhido pela BWS.
#   Federais — IR 1,2%, PIS 0,65%, COFINS 3%, CSLL 1%, cada um retido ou não
#           conforme o contrato/tomador. Incidem sobre o valor total do serviço.
#
# Nomenclatura (dúvida levantada pelo Marcelo):
#   IRRF é o correto na nota — é o Imposto de Renda RETIDO NA FONTE pelo
#   tomador, antecipação do IRPJ que a BWS apura depois. IRPJ é o tributo sobre
#   o lucro da empresa, apurado no balanço; não aparece como retenção da nota.
#   PCC é a sigla usual de PIS+COFINS+CSLL retidos em conjunto (4,65%), também
#   chamada CSRF. Quando os três vêm juntos, o sistema soma numa linha PCC;
#   quando o contrato retém só alguns, cada um vai na sua linha.
# ============================================================================
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional

from app.apps.erp.db.models.cadastros import Obra

ALIQ_INSS = Decimal("0.11")
ALIQ_IR = Decimal("0.012")
ALIQ_PIS = Decimal("0.0065")
ALIQ_COFINS = Decimal("0.03")
ALIQ_CSLL = Decimal("0.01")

FEDERAIS = {"IR": ALIQ_IR, "PIS": ALIQ_PIS, "COFINS": ALIQ_COFINS, "CSLL": ALIQ_CSLL}


def _q(v: Any) -> Decimal:
    return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass
class Retencao:
    tipo: str
    base_calculo: Decimal
    aliquota: Decimal        # em % (11.00, 2.00…)
    valor: Decimal
    explicacao: str = ""


@dataclass
class CalculoMedicao:
    valor_bruto: Decimal
    base_servico_inss: Decimal
    base_iss: Decimal
    retencoes: list[Retencao] = field(default_factory=list)
    total_retencoes: Decimal = Decimal("0.00")
    valor_liquido: Decimal = Decimal("0.00")
    avisos: list[str] = field(default_factory=list)

    def como_dict(self) -> dict[str, Any]:
        return {
            "valor_bruto": float(self.valor_bruto),
            "base_servico_inss": float(self.base_servico_inss),
            "base_iss": float(self.base_iss),
            "retencoes": [{"tipo": r.tipo, "base_calculo": str(r.base_calculo),
                           "aliquota": str(r.aliquota), "valor": str(r.valor),
                           "explicacao": r.explicacao} for r in self.retencoes],
            "total_retencoes": float(self.total_retencoes),
            "valor_liquido": float(self.valor_liquido),
            "avisos": self.avisos,
        }


def calcular(obra: Obra, valor_bruto: Any, *,
             pct_servico_iss: Optional[Any] = None,
             pct_servico_inss: Optional[Any] = None,
             sem_deducao: bool = False,
             aliquota_iss: Optional[Any] = None) -> CalculoMedicao:
    """Calcula as retenções da medição. Os percentuais da obra podem ser
    sobrepostos pontualmente (medição de reajuste, por exemplo, costuma ser
    100% serviço)."""
    bruto = _q(valor_bruto)
    avisos: list[str] = []

    # ---- base do INSS
    if not obra.inss_retido:
        base_inss = Decimal("0.00")
    else:
        pct = pct_servico_inss if pct_servico_inss not in (None, "") else obra.pct_servico_inss
        if sem_deducao or pct in (None, ""):
            if pct in (None, "") and not sem_deducao:
                avisos.append("Obra sem percentual de serviço para o INSS — usando 100%. "
                              "Em empreitada com material o usual é 50%.")
            pct = Decimal("100")
        base_inss = _q(bruto * Decimal(str(pct)) / 100)

    # ---- base do ISS
    aliq_iss = aliquota_iss if aliquota_iss not in (None, "") else obra.aliquota_iss_pct
    if aliq_iss in (None, ""):
        aliq_iss = Decimal("0")
        avisos.append("Obra sem alíquota de ISS cadastrada — ISS não calculado. "
                      "Informe a alíquota do município no cadastro da obra.")
    aliq_iss = Decimal(str(aliq_iss))

    if sem_deducao or not obra.aceita_deducao_material:
        base_iss = bruto
        if not obra.aceita_deducao_material and not sem_deducao:
            avisos.append(f"O município de {obra.municipio or 'obra'} não aceita dedução de "
                          f"material: ISS sobre o valor cheio.")
    else:
        pct = pct_servico_iss if pct_servico_iss not in (None, "") else obra.pct_servico_iss
        if pct in (None, ""):
            base_iss = bruto
            avisos.append("Município aceita dedução de material, mas a obra não tem o "
                          "percentual de serviço — ISS calculado sobre o valor cheio.")
        else:
            base_iss = _q(bruto * Decimal(str(pct)) / 100)

    retencoes: list[Retencao] = []
    if base_inss > 0:
        retencoes.append(Retencao(
            "INSS", base_inss, ALIQ_INSS * 100, _q(base_inss * ALIQ_INSS),
            f"11% sobre {base_inss} (base de serviço)"
            + (f" — {(base_inss / bruto * 100):.0f}% do valor" if bruto else "")))
    if aliq_iss > 0 and obra.iss_retido:
        retencoes.append(Retencao(
            "ISS", base_iss, aliq_iss, _q(base_iss * aliq_iss / 100),
            f"{aliq_iss}% sobre {base_iss}"
            + (" (com dedução de material)" if base_iss < bruto else " (sem dedução)")))
    elif aliq_iss > 0 and not obra.iss_retido:
        avisos.append(f"ISS de {aliq_iss}% NÃO é retido pelo tomador — a BWS recolhe "
                      f"em guia própria (conta 2.1.01).")

    federais = [f.upper() for f in (obra.federais_retidos or [])]
    if {"PIS", "COFINS", "CSLL"} <= set(federais):
        total_pcc = ALIQ_PIS + ALIQ_COFINS + ALIQ_CSLL      # 4,65%
        retencoes.append(Retencao(
            "PCC", bruto, total_pcc * 100, _q(bruto * total_pcc),
            "PIS 0,65% + COFINS 3% + CSLL 1% retidos em conjunto (4,65%)"))
        federais = [f for f in federais if f not in ("PIS", "COFINS", "CSLL")]
    for f in federais:
        if f in FEDERAIS:
            aliq = FEDERAIS[f]
            tipo = "IRRF" if f == "IR" else f
            retencoes.append(Retencao(
                tipo, bruto, aliq * 100, _q(bruto * aliq),
                f"{aliq * 100}% sobre o valor da nota"))

    total = _q(sum((r.valor for r in retencoes), Decimal("0")))
    return CalculoMedicao(valor_bruto=bruto, base_servico_inss=base_inss, base_iss=base_iss,
                          retencoes=retencoes, total_retencoes=total,
                          valor_liquido=_q(bruto - total), avisos=avisos)


def resumo_tributacao(obra: Obra) -> str:
    """Frase curta com o regime da obra, para exibir no cadastro."""
    partes = []
    if obra.inss_retido:
        pct = obra.pct_servico_inss or 100
        efetiva = Decimal("11") * Decimal(str(pct)) / 100
        partes.append(f"INSS 11% sobre {pct}% ({efetiva:.2f}% efetivos)")
    else:
        partes.append("INSS não retido")
    if obra.aliquota_iss_pct:
        base = "com dedução de material" if obra.aceita_deducao_material else "sobre o valor cheio"
        partes.append(f"ISS {obra.aliquota_iss_pct}% {base}"
                      + ("" if obra.iss_retido else " (não retido)"))
    else:
        partes.append("ISS não cadastrado")
    fed = [f for f in (obra.federais_retidos or [])]
    partes.append("federais: " + (", ".join(fed) if fed else "nenhum"))
    return " · ".join(partes)
