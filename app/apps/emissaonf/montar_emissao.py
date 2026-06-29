# -*- coding: utf-8 -*-
"""
Fase 2: traduz a saída do worker (card + obra + cálculo) para o DadosRps do ABRASF
e monta o XML de emissão. Usado em DRY-RUN: mostra o XML, não envia.
"""
from __future__ import annotations
import re
from lxml import etree

from el_nfse_abrasf import (
    DadosRps, montar_gerarnfse_assinado, montar_rps, NS_ABRASF,
)
from preview import montar_discriminacao
from municipios_ibge import resolver
from tomador import buscar_tomador


def parse_endereco_tomador(texto: str, cache: dict):
    """Best-effort: 'LOGRADOURO, NUM - BAIRRO - CIDADE/UF - CEP' -> campos estruturados.
    Devolve (logradouro, numero, bairro, cmun_ibge, uf, cep, avisos)."""
    t = (texto or "").strip()
    avisos = []
    cep = ""
    m = re.search(r"(\d{5}-?\d{3})", t)
    if m:
        cep = re.sub(r"\D", "", m.group(1))
    uf = cmun = cidade = ""
    mc = re.search(r"([A-Za-zÀ-ú .]+?)\s*[/\-]\s*([A-Z]{2})\b", t)
    if mc:
        cidade = mc.group(1).strip().rstrip(",").strip()
        uf = mc.group(2)
        try:
            cmun = resolver(f"{cidade}-{uf}", cache)
        except Exception:
            avisos.append(f"município do tomador '{cidade}-{uf}' não resolveu no IBGE")
    else:
        avisos.append("não consegui identificar cidade/UF do tomador")
    partes = [p.strip() for p in t.split(" - ")]
    logr = partes[0] if partes else ""
    numero = ""
    mn = re.match(r"(.+?),\s*(\d+[A-Za-z]?)", logr)
    if mn:
        logr, numero = mn.group(1).strip(), mn.group(2)
    bairro = partes[1] if len(partes) >= 4 else ""
    if not (logr and cmun and cep):
        avisos.append("endereço do tomador incompleto — confira antes de emitir de verdade")
    return logr, numero, bairro, cmun, uf, cep, avisos


def montar_dados_rps(card, obra, r, numero_rps, ibge_obra, data_emissao, cache) -> tuple[DadosRps, list]:
    cnpj_tomador = (card.get("cnpj_contratante") or obra.cnpj_cliente)
    avisos = []
    # 1ª fonte: CNPJ via BrasilAPI; reserva: texto livre da C. Diários
    tom = buscar_tomador(cnpj_tomador, cache)
    if tom.ok and tom.logradouro and tom.cmun_ibge:
        logr, num, bairro = tom.logradouro, tom.numero, tom.bairro
        cmun, uf, cep = tom.cmun_ibge, tom.uf, tom.cep
        razao = tom.razao_social or card.get("contratante") or obra.cliente
        avisos += tom.avisos
    else:
        avisos += tom.avisos
        logr, num, bairro, cmun, uf, cep, av2 = parse_endereco_tomador(obra.endereco_cliente, cache)
        avisos += av2 + ["endereço do tomador veio da C. Diários (reserva), não do CNPJ"]
        razao = card.get("contratante") or obra.cliente

    fed = r.federais_retidos
    end_tomador = {"razao": razao, "logradouro": logr, "numero": num, "bairro": bairro,
                   "municipio": (tom.municipio if tom.ok else ""), "uf": uf, "cep": cep, "cmun": cmun}
    dados = DadosRps(
        numero_rps=numero_rps, serie_rps="1", data_emissao=data_emissao,
        competencia=data_emissao,
        valor_servicos=str(r.valor_total),
        valor_iss=str(r.iss), aliquota=str(r.aliquota_iss),
        valor_inss=str(r.inss),
        valor_ir=str(r.ir) if "IR" in fed else "0.00",
        valor_csll=str(r.csll) if "CSLL" in fed else "0.00",
        valor_pis=str(r.pis) if "PIS" in fed else "0.00",
        valor_cofins=str(r.cofins) if "COFINS" in fed else "0.00",
        iss_retido=1 if r.iss_retido else 2,
        exigibilidade_iss=1,
        item_lista_servico="07.02",          # ABRASF LC116 (exigido pelo XSD)
        codigo_tributacao_municipio="702",
        codigo_servico_nacional="070202",    # Código Serviço Nacional (das notas reais BWS)
        discriminacao=montar_discriminacao(card, obra, r),
        codigo_municipio_servico=str(ibge_obra), municipio_incidencia=str(ibge_obra),
        toma_doc=(card.get("cnpj_contratante") or obra.cnpj_cliente),
        toma_razao=razao,
        toma_logradouro=logr, toma_numero=num, toma_bairro=bairro,
        toma_cmun=cmun, toma_uf=uf, toma_cep=cep,
        optante_simples=2, incentivo_fiscal=2,
    )
    return dados, avisos, end_tomador


def gerar_xml_preview(dados: DadosRps, chave_pem=None, cert_pem=None) -> str:
    """Monta o GerarNfseEnvio. Com cert: assinado. Sem cert: estrutura sem assinatura."""
    if chave_pem and cert_pem:
        xb = montar_gerarnfse_assinado(dados, chave_pem, cert_pem)
        return etree.tostring(etree.fromstring(xb), pretty_print=True, encoding="unicode")
    rps, _ = montar_rps(dados)
    envio = etree.Element("{%s}GerarNfseEnvio" % NS_ABRASF, nsmap={None: NS_ABRASF})
    envio.append(rps)
    return etree.tostring(envio, pretty_print=True, encoding="unicode")