# -*- coding: utf-8 -*-
"""
Motor de cálculo tributário das notas da BWS (substitui a aba Emissão).

Padrão ÚNICO aceito (4 blocos, separados por hífen):
    ONERADA - <Ded.INSS> - <Ded.ISS> - <Impostos Retidos>

  Ded.INSS / Ded.ISS : "NN/MM" (NN = % serviço, MM = % material), "SD" (NÃO retém, base 0) ou "100/0" (retém sobre 100%)
  Impostos Retidos   : lista por vírgula de {IR, PIS, COFINS, CSLL} ou "SEM RETENÇÃO"

Qualquer categoria fora desse padrão (CPRB, tags, 3 blocos, etc.) é recusada com crítica.
"""

from __future__ import annotations
import re
import unicodedata
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP

# alíquotas padrão (conferidas nas notas reais); sobreponíveis se necessário
ALIQ_INSS = Decimal("0.11")
ALIQ_PIS = Decimal("0.0065")
ALIQ_COFINS = Decimal("0.03")
ALIQ_IR = Decimal("0.012")
ALIQ_CSLL = Decimal("0.01")

TRIBUTACOES_VALIDAS = {"ONERADA"}
FEDERAIS_VALIDOS = {"IR", "PIS", "COFINS", "CSLL"}


class CategoriaInvalida(ValueError):
    """Crítica: categoria de tributação fora do padrão. Emissão deve ser barrada."""


class DadoObrigatorioAusente(ValueError):
    """Crítica: dado obrigatório faltando (ex.: alíquota ISS com dedução)."""


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return s.strip().upper()


def _q(v) -> Decimal:
    return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass
class Categoria:
    bruta: str
    tributacao: str
    ded_inss_serv: Decimal     # fração de serviço p/ base INSS (0 se SD = não retém)
    ded_iss_serv: Decimal      # fração de serviço p/ base ISS (0 se SD = não retém)
    retencoes_federais: set    # subconjunto de {IR, PIS, COFINS, CSLL}


def _parse_deducao(bloco: str, qual: str) -> Decimal:
    b = _norm(bloco)
    if b == "SD":
        return Decimal("0")          # SD = NÃO retém (base 0). Para 100%, usar "100/0".
    m = re.fullmatch(r"(\d{1,3})/(\d{1,3})", b)
    if not m:
        raise CategoriaInvalida(
            f"Dedução {qual} fora do padrão: '{bloco}'. Use 'NN/MM' ou 'SD'."
        )
    serv, mat = int(m.group(1)), int(m.group(2))
    if serv + mat != 100:
        raise CategoriaInvalida(
            f"Dedução {qual} '{bloco}' não soma 100 ({serv}+{mat})."
        )
    return Decimal(serv) / Decimal(100)


def parse_categoria(texto: str) -> Categoria:
    """Faz o parsing estrito da categoria. Levanta CategoriaInvalida se fora do padrão."""
    if not texto or not texto.strip():
        raise CategoriaInvalida("Categoria de tributação vazia.")
    blocos = [b.strip() for b in texto.split("-")]
    if len(blocos) != 4:
        raise CategoriaInvalida(
            f"Categoria '{texto}' fora do padrão: esperados 4 blocos "
            f"(Tributação - Ded.INSS - Ded.ISS - Impostos), encontrados {len(blocos)}."
        )
    trib = _norm(blocos[0])
    if trib not in TRIBUTACOES_VALIDAS:
        raise CategoriaInvalida(
            f"Tributação '{blocos[0]}' não suportada. Aceito: {', '.join(sorted(TRIBUTACOES_VALIDAS))}."
        )
    ded_inss = _parse_deducao(blocos[1], "INSS")
    ded_iss = _parse_deducao(blocos[2], "ISS")

    ret_txt = _norm(blocos[3])
    if ret_txt in ("SEM RETENCAO", "SEM RETENCAO."):
        retencoes = set()
    else:
        retencoes = set()
        for tok in blocos[3].split(","):
            t = _norm(tok)
            if not t:
                continue
            if t not in FEDERAIS_VALIDOS:
                raise CategoriaInvalida(
                    f"Imposto retido '{tok}' inválido. Aceito: IR, PIS, COFINS, CSLL ou SEM RETENÇÃO."
                )
            retencoes.add(t)
    return Categoria(texto.strip(), trib, ded_inss, ded_iss, retencoes)


@dataclass
class ResultadoCalculo:
    valor_total: Decimal
    bdi_diferenciado: Decimal
    base_servico: Decimal
    base_inss: Decimal
    base_iss: Decimal
    inss: Decimal
    iss: Decimal
    iss_retido: bool
    aliquota_iss: Decimal
    pis: Decimal
    cofins: Decimal
    ir: Decimal
    csll: Decimal
    federais_retidos: dict       # {nome: valor} apenas os retidos
    total_retencoes: Decimal
    valor_liquido: Decimal
    categoria: Categoria


@dataclass
class Overrides:
    """Campos opcionais do card que substituem o padrão da C. Diários.
    Vazio/None em tudo => comportamento idêntico ao original (puro pela categoria)."""
    sem_deducao: bool = False     # Tipo de Medição = "Reajuste (Sem Dedução)" -> 100% serviço
    usar_aliquotas: bool = False  # dropdown 'Informar Alíquota e ou Dedução' inclui Alíquotas
    usar_deducoes: bool = False   # ... inclui Deduções
    split_iss: str = ""           # "60/40" (serviço/materiais) aplicado ao ISS
    aliq_inss: str = ""           # percentuais (ex.: "11", "1,2"); vazio = padrão
    aliq_iss: str = ""
    aliq_ir: str = ""
    aliq_pis: str = ""
    aliq_cofins: str = ""
    aliq_csll: str = ""


def _taxa(v):
    """'1,2' -> Decimal('0.012'); vazio -> None."""
    if v is None or str(v).strip() == "":
        return None
    return Decimal(str(v).strip().replace(",", ".")) / Decimal(100)


def valor_base_nota(card: dict):
    """Valor que vira base da nota: parcial se 'Sim (Valor Parcial)', senão o da medição."""
    emi = _norm(card.get("emissao_nf", ""))
    vp = card.get("valor_parcial")
    if "PARCIAL" in emi and vp not in (None, "", []):
        return vp
    return card.get("valor_medicao")


def overrides_do_card(card: dict) -> "Overrides":
    """Constrói os Overrides a partir dos campos da fase Medições."""
    tm = _norm(card.get("tipo_medicao", ""))
    drop = _norm(card.get("informar_aliq_ded", ""))
    return Overrides(
        sem_deducao=("SEM DEDUCAO" in tm),
        usar_aliquotas=("ALIQUOTA" in drop),
        usar_deducoes=("DEDUCO" in drop or "DEDUCA" in drop),
        split_iss=card.get("deducoes_split", "") or "",
        aliq_inss=card.get("aliq_inss", "") or "",
        aliq_iss=card.get("aliq_iss", "") or "",
        aliq_ir=card.get("aliq_ir", "") or "",
        aliq_pis=card.get("aliq_pis", "") or "",
        aliq_cofins=card.get("aliq_cofins", "") or "",
        aliq_csll=card.get("aliq_csll", "") or "",
    )


def calcular(valor_total, categoria: Categoria, aliquota_iss=None,
             bdi_diferenciado="0", iss_retido=True, overrides: "Overrides" = None) -> ResultadoCalculo:
    """Calcula todas as retenções da nota a partir da categoria já parseada.
    overrides (opcional) substitui partes do padrão conforme os campos do card."""
    ov = overrides or Overrides()
    valor = Decimal(str(valor_total))
    bdi = Decimal(str(bdi_diferenciado or "0"))
    base = valor - bdi  # base de serviço (sobre ela incidem INSS e ISS)

    # --- frações de serviço (split) ---
    if ov.sem_deducao:
        ded_inss = Decimal("1")          # 100% serviço
        ded_iss = Decimal("1")
    else:
        ded_inss = categoria.ded_inss_serv               # INSS: sempre da categoria (50/50 etc.)
        if ov.usar_deducoes and str(ov.split_iss).strip():
            ded_iss = _parse_deducao(ov.split_iss, "ISS (campo do card)")  # override só do ISS
        else:
            ded_iss = categoria.ded_iss_serv

    # --- alíquota de ISS ---
    iss_ov = _taxa(ov.aliq_iss) if ov.usar_aliquotas else None
    if iss_ov is not None:
        aliq_iss = iss_ov
    else:
        if aliquota_iss is None or str(aliquota_iss).strip() == "":
            raise DadoObrigatorioAusente(
                "Alíquota de ISS é obrigatória (a categoria define dedução de ISS) e está vazia na C. Diários."
            )
        aliq_iss = Decimal(str(aliquota_iss)) / Decimal(100)
    aliq_iss_pct = (aliq_iss * Decimal(100))

    # --- INSS ---
    inss_rate = (_taxa(ov.aliq_inss) if ov.usar_aliquotas else None) or ALIQ_INSS
    base_inss = (ded_inss * base)
    inss = _q(inss_rate * base_inss)
    base_iss = (ded_iss * base)
    iss = _q(aliq_iss * base_iss)

    # --- federais (sobre o VALOR TOTAL) ---
    if ov.usar_aliquotas:
        # os campos definem as retenções; ignora o 4º bloco da categoria.
        # cada federal é RETIDO somente se seu campo de alíquota estiver preenchido.
        pad = {"PIS": ALIQ_PIS, "COFINS": ALIQ_COFINS, "IR": ALIQ_IR, "CSLL": ALIQ_CSLL}
        ovr = {"PIS": _taxa(ov.aliq_pis), "COFINS": _taxa(ov.aliq_cofins),
               "IR": _taxa(ov.aliq_ir), "CSLL": _taxa(ov.aliq_csll)}
        todos = {k: _q((ovr[k] if ovr[k] is not None else pad[k]) * valor) for k in pad}
        federais_retidos = {k: v for k, v in todos.items() if ovr[k] is not None}
    else:
        todos = {
            "PIS": _q(ALIQ_PIS * valor),
            "COFINS": _q(ALIQ_COFINS * valor),
            "IR": _q(ALIQ_IR * valor),
            "CSLL": _q(ALIQ_CSLL * valor),
        }
        federais_retidos = {k: v for k, v in todos.items() if k in categoria.retencoes_federais}

    total_ret = inss + sum(federais_retidos.values()) + (iss if iss_retido else Decimal("0"))
    liquido = _q(valor - total_ret)

    return ResultadoCalculo(
        valor_total=_q(valor), bdi_diferenciado=_q(bdi), base_servico=_q(base),
        base_inss=_q(base_inss), base_iss=_q(base_iss), inss=inss, iss=iss,
        iss_retido=iss_retido, aliquota_iss=_q(aliq_iss_pct),
        pis=todos["PIS"], cofins=todos["COFINS"], ir=todos["IR"], csll=todos["CSLL"],
        federais_retidos=federais_retidos, total_retencoes=_q(total_ret),
        valor_liquido=liquido, categoria=categoria,
    )
