# -*- coding: utf-8 -*-
"""
Compara o que o card CALCULA hoje com o que a nota REALMENTE destacou.

⚠️ **Por que isto existe, e é dinheiro.** A tela `/emissao/recuperar` com
"concluir completo" marcado roda o pós-emissão inteiro: grava a linha da
'Notas BWS' e, principalmente, **escreve as retenções no título do Omie**. Os
números que ela grava não saem do XML da nota — saem de um recálculo do card,
feito na hora.

E a conclusão de uma nota **limpa doze campos de entrada do card** (entre eles
o `valor_bdi_diferenciado`, o `valor_parcial` e as alíquotas). Então, numa nota
que JÁ concluiu uma vez — que é exatamente o caso de uma substituição manual,
ou de um conserto dias depois — o recálculo acontece com o BDI em branco. Na
nota de 09/11/2026, de BDI R$ 1.313.502,08, isso daria **R$ 72.242,62 de INSS e
R$ 13.135,02 de ISS a mais** do que a nota destacou, gravados em silêncio no
Omie e na planilha.

O XML é a fonte da verdade: ele é o documento que foi emitido. Então, antes de
gravar, confere-se um contra o outro. Divergiu, não grava — explica e devolve a
decisão para o dono, que pode repreencher o campo no card e rodar de novo, ou
mandar gravar assim mesmo.

O que NÃO é divergência: tributo federal não retido. O modelo nacional só traz
no XML o tributo que foi **retido** (é a regra do E0699), e o motor fiscal só
põe em `federais_retidos` o que retém — então comparam-se os dois conjuntos de
retidos, e não as alíquotas cheias.
"""
from __future__ import annotations
from decimal import Decimal

# Um centavo de folga: os dois lados arredondam, e a planilha escreve com duas
# casas. Diferença de arredondamento não é erro de base de cálculo.
TOLERANCIA = Decimal("0.01")


def _d(v) -> Decimal:
    try:
        return Decimal(str(v).strip() or 0)
    except Exception:
        return Decimal("0")


def retidos_do_xml(xml_texto) -> dict:
    """O que a nota realmente destacou: total, ISS, INSS e os federais RETIDOS.

    Aceita os dois modelos (nacional e o antigo ABRASF), porque quem conserta uma
    nota antiga não tem como saber em qual deles ela saiu.
    """
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_texto.encode("utf-8") if isinstance(xml_texto, str) else xml_texto)
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]

    def t(*caminhos):
        for c in caminhos:
            el = root.find(".//" + c)
            if el is not None and (el.text or "").strip():
                return el.text.strip()
        return ""

    if root.find(".//infNFSe") is not None:           # modelo NACIONAL
        total = _d(t("vServ"))
        inss, iss = _d(t("vRetCP")), _d(t("vISSQN"))
        fed = {"IR": _d(t("vRetIRRF")), "PIS": _d(t("vPis", "vRetPIS")),
               "COFINS": _d(t("vCofins", "vRetCofins")), "CSLL": _d(t("vRetCSLL"))}
    else:                                             # modelo antigo (ABRASF)
        total = _d(t("ValorServicos"))
        inss, iss = _d(t("ValorInss")), _d(t("ValorIss"))
        fed = {"IR": _d(t("ValorIr")), "PIS": _d(t("ValorPis")),
               "COFINS": _d(t("ValorCofins")), "CSLL": _d(t("ValorCsll"))}

    return {"valor_total": total, "inss": inss, "iss": iss,
            "federais": {k: v for k, v in fed.items() if v > 0}}


def conferivel(xml_texto) -> bool:
    """O XML traz valores para conferir?

    XML de nota sempre traz. Mas há XML parcial arquivado (e o do modelo antigo
    nem sempre vem com o bloco de valores), e aí não há o que comparar — nesse
    caso não se trava: avisa-se que não foi conferido e segue. Travar por falta
    de informação impediria um conserto legítimo sem proteger nada.
    """
    try:
        return retidos_do_xml(xml_texto)["valor_total"] > 0
    except Exception:
        return False


def divergencias(r, xml_texto) -> list[str]:
    """Lista, em português, o que o recálculo do card diz de diferente do XML.

    Lista vazia quer dizer "pode gravar": o card continua produzindo exatamente a
    nota que foi emitida. **Também volta vazia quando o XML não traz valores** —
    ver `conferivel`, que é quem o chamador usa para dizer no log que não conferiu.
    """
    emit = retidos_do_xml(xml_texto)
    if emit["valor_total"] <= 0:
        return []
    fora = []

    def _cmp(nome, calculado, emitido):
        calculado, emitido = _d(calculado), _d(emitido)
        if abs(calculado - emitido) > TOLERANCIA:
            fora.append(f"{nome}: o card calcula R$ {calculado:,.2f} e a nota "
                        f"destacou R$ {emitido:,.2f}")

    _cmp("Valor da nota", r.valor_total, emit["valor_total"])
    _cmp("ISS", r.iss, emit["iss"])
    _cmp("INSS", r.inss, emit["inss"])

    calc_fed = dict(r.federais_retidos or {})
    for nome in ("IR", "PIS", "COFINS", "CSLL"):
        tem_calc, tem_emit = nome in calc_fed, nome in emit["federais"]
        if tem_calc and tem_emit:
            _cmp(nome, calc_fed[nome], emit["federais"][nome])
        elif tem_calc and not tem_emit:
            fora.append(f"{nome}: o card retém R$ {_d(calc_fed[nome]):,.2f} e a nota "
                        f"não retém este tributo")
        elif tem_emit and not tem_calc:
            fora.append(f"{nome}: a nota retém R$ {_d(emit['federais'][nome]):,.2f} e o "
                        f"card não retém este tributo")
    return fora
