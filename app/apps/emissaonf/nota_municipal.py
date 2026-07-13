# -*- coding: utf-8 -*-
"""
Gera o PDF da NFS-e MUNICIPAL (layout "Prefeitura Municipal de Eusébio", provedor E&L)
a partir do XML ABRASF 2.04 que a própria emissão devolve. Sem navegador (fpdf2 + qrcode).

A chave de acesso e os textos longos de serviço (nacional/municipal) não estão no XML
ABRASF — quando o XML nacional estiver disponível, passe-o em `xml_nacional` para enriquecer.

    from nota_municipal import gerar_nota_municipal_pdf
    gerar_nota_municipal_pdf(xml_abrasf, "nfse_municipal_3072.pdf", xml_nacional=xml_nac)
"""
from __future__ import annotations
import os
import tempfile
import xml.etree.ElementTree as ET

from danfse import _l1, _brl, _cnpj, _cep, _data, _mun, NS as NS_NAC, LOGRADOURO_PRESTADOR_FORCADO, _requebrar_discriminacao

# contatos fixos do prestador (não vêm no XML ABRASF; são dados da própria BWS)
EMIT_EMAIL = "FINANCEIRO@BWSCONSTRUCOES.COM.BR"
EMIT_FONE = "8598322004"
QR_BASE = "https://www.nfse.gov.br/consultapublica/?tpc=1&chave="
BRASAO_MUN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nota_mun_brasao.png")

# textos dos códigos de serviço (fallback quando não houver XML nacional)
TXT_NAC = {"070202": ("Execução, por empreitada ou subempreitada, de obras de construção civil, "
                      "hidráulica ou elétrica e de outras obras semelhantes, inclusive sondagem, "
                      "perfuração de poços, escavação, drenagem e irrigação, terraplanagem, "
                      "pavimentação, concretagem e a instalação e montagem de produtos, peças e "
                      "equipamentos (exceto o fornecimento de mercadorias produzidas pelo prestador "
                      "de serviços fora do local da prestação dos serviços, que fica sujeito ao ICMS).")}
TXT_MUN = {"702": "EXECUÇÃO, POR ADMINISTRAÇÃO, EMPREITADA OU SUBEMPREITADA, DE OBRAS DE CONSTRUÇÃO CIVIL, HIDRÁULICA"}


def _t(base, *tags):
    el = base
    for tg in tags:
        if el is None:
            return ""
        el = el.find(tg)
    return (el.text or "").strip() if el is not None else ""


def valor_bruto_nf(xml_abrasf: str):
    """Valor BRUTO da NF emitida = ValorServicos do XML ABRASF. É a fonte da
    verdade (independe do card, que pode já ter sido limpo). float ou None."""
    import re
    m = re.search(r"ValorServicos>\s*([0-9]+(?:\.[0-9]+)?)\s*<", xml_abrasf)
    return float(m.group(1)) if m else None


def parse_nfse_municipal(xml: str) -> dict:
    root = ET.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
    # remove namespace default (alguns retornos ABRASF vêm com xmlns) p/ os .find funcionarem
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]

    def _fe(base, path):
        """find encadeado tolerante a None (não estoura se um nível faltar)."""
        return base.find(path) if base is not None else None

    inf = _fe(root, ".//InfNfse")
    presS = _fe(inf, "PrestadorServico")
    presE = _fe(presS, "Endereco")
    decl = _fe(inf, "DeclaracaoPrestacaoServico/InfDeclaracaoPrestacaoServico")
    serv = _fe(decl, "Servico")
    val = _fe(serv, "Valores")
    toma = _fe(decl, "TomadorServico")
    tomaE = _fe(toma, "Endereco")

    return {
        "numero": _t(inf, "Numero"),
        "cod_verif": _t(inf, "CodigoVerificacao"),
        "data_emissao": _data(_t(inf, "DataEmissao")),
        "rps_num": _t(decl, "Rps", "IdentificacaoRps", "Numero"),
        "rps_serie": _t(decl, "Rps", "IdentificacaoRps", "Serie"),
        "competencia": _data(_t(decl, "Competencia")),
        "local_prest": (lambda c: f"{c} - {_mun(c)}" if _mun(c) != c else c)(_t(serv, "CodigoMunicipio")),
        "exig_iss": "Exigível" if _t(serv, "ExigibilidadeISS") == "1" else "-",
        "iss_retido": "Retido na Fonte" if _t(serv, "IssRetido") == "1" else "Não retido",
        "optante": "Não Optante" if _t(decl, "OptanteSimplesNacional") == "2" else "Optante",
        # prestador
        "emit_razao": _t(presS, "RazaoSocial"),
        "emit_end": f"{LOGRADOURO_PRESTADOR_FORCADO or _t(presE,'Endereco')}, {_t(presE,'Numero')} - {_t(presE,'Bairro')}",
        "emit_mun": _mun(_t(presE, "CodigoMunicipio"), _t(presE, "Uf")),
        "emit_cep": _cep(_t(presE, "Cep")),
        "emit_im": _t(decl, "Prestador", "InscricaoMunicipal"),
        "emit_cnpj": _cnpj(_t(decl, "Prestador", "CpfCnpj", "Cnpj")),
        # tomador
        "toma_razao": _t(toma, "RazaoSocial"),
        "toma_end": f"{_t(tomaE,'Endereco')}, {_t(tomaE,'Numero')} - {_t(tomaE,'Bairro')}",
        "toma_mun": _mun(_t(tomaE, "CodigoMunicipio"), _t(tomaE, "Uf")),
        "toma_cep": _cep(_t(tomaE, "Cep")),
        "toma_cnpj": _cnpj(_t(toma, "IdentificacaoTomador", "CpfCnpj", "Cnpj")),
        # serviço
        "cod_nac": _t(serv, "CodigoServicoNacional"),
        "cod_mun": _t(serv, "CodigoTributacaoMunicipio"),
        "discriminacao": _t(serv, "Discriminacao"),
        # valores
        "vServ": _brl(_t(val, "ValorServicos")),
        "vDed": _brl(_t(val, "ValorDeducoes")),
        "vBC": _brl(_t(inf, "ValoresNfse", "BaseCalculo")),
        "aliq": _t(val, "Aliquota"),
        "vISS": _brl(_t(val, "ValorIss")),
        "vINSS": _brl(_t(val, "ValorInss")),
        "vIR": _brl(_t(val, "ValorIr")),
        "vCSLL": _brl(_t(val, "ValorCsll")),
        "vCOFINS": _brl(_t(val, "ValorCofins")),
        "vPIS": _brl(_t(val, "ValorPis")),
        "vOutras": _brl(_t(val, "OutrasRetencoes")),
        "vDescCond": _brl(_t(val, "DescontoCondicionado")),
        "vDescIncond": _brl(_t(val, "DescontoIncondicionado")),
        "vLiq": _brl(_t(inf, "ValoresNfse", "ValorLiquidoNfse")),
    }


def _enriquecer(d: dict, xml_nacional: str | None):
    """Pega chave, textos de serviço e data/hora do XML nacional, se houver."""
    d["chave"] = ""
    d["txt_nac"] = TXT_NAC.get(d["cod_nac"], "")
    d["txt_mun"] = TXT_MUN.get(d["cod_mun"], "")
    if not xml_nacional:
        return
    try:
        root = ET.fromstring(xml_nacional.encode("utf-8") if isinstance(xml_nacional, str) else xml_nacional)
        inf = root.find(NS_NAC + "infNFSe")
        d["chave"] = (inf.get("Id") or "").replace("NFS", "")
        xn = inf.find(NS_NAC + "xTribNac")
        xm = inf.find(NS_NAC + "xTribMun")
        if xn is not None and (xn.text or "").strip():
            d["txt_nac"] = xn.text.strip()
        if xm is not None and (xm.text or "").strip():
            d["txt_mun"] = xm.text.strip()
        dh = inf.find(NS_NAC + "dhProc")
        if dh is not None and dh.text:
            d["data_emissao_hora"] = f"{_data(dh.text)} {dh.text[11:19]}"
        xdesc = inf.find(".//" + NS_NAC + "xDescServ")   # discriminação limpa (com acentos)
        if xdesc is not None and (xdesc.text or "").strip():
            d["discriminacao"] = xdesc.text.strip()
    except Exception:
        pass


def _qr(chave: str):
    if not chave:
        return None
    try:
        import qrcode
        f = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        qrcode.make(QR_BASE + chave).save(f.name)
        return f.name
    except Exception:
        return None


def gerar_nota_municipal_pdf(xml_abrasf: str, saida_pdf: str, xml_nacional: str | None = None,
                             discriminacao: str | None = None) -> str:
    from fpdf import FPDF
    d = parse_nfse_municipal(xml_abrasf)
    _enriquecer(d, xml_nacional)
    if discriminacao:                       # override limpo (quando não há nacional ainda)
        d["discriminacao"] = discriminacao

    pdf = FPDF(format="A4")
    pdf.set_auto_page_break(False)
    pdf.add_page()
    ML, MR = 8, 8
    W = pdf.w - ML - MR
    pdf.set_draw_color(110, 110, 110)
    pdf.set_line_width(0.2)

    def secao(titulo, y, h=4):
        pdf.set_fill_color(230, 230, 230)
        pdf.rect(ML, y, W, h, "DF")
        pdf.set_xy(ML, y + 0.5)
        pdf.set_font("Helvetica", "B", 7)
        pdf.cell(W, h - 1, _l1(titulo), align="C")
        return y + h

    def campo(x, y, w, h, rot, val, vs=7.5, bold=False, center=False):
        pdf.rect(x, y, w, h)
        pdf.set_xy(x + 1, y + 0.4)
        pdf.set_font("Helvetica", "", 5)
        pdf.cell(w - 2, 2, _l1(rot))
        pdf.set_xy(x + 1, y + 2.4)
        pdf.set_font("Helvetica", "B" if bold else "", vs)
        pdf.cell(w - 2, h - 3, _l1(val or "-"), align="C" if center else "L")

    def bloco(rot, val, y, h=3.6, fs=7):
        pdf.set_xy(ML + 1, y)
        pdf.set_font("Helvetica", "B", fs)
        pdf.write(h, _l1(rot))
        pdf.set_font("Helvetica", "", fs)
        pdf.write(h, _l1(val))
        return y + h

    y = 8
    # ---- cabeçalho ----
    cabH = 20
    pdf.rect(ML, y, W, cabH)
    if os.path.exists(BRASAO_MUN):
        pdf.image(BRASAO_MUN, x=ML + 2, y=y + 2, w=15)
    pdf.set_xy(ML, y + 2)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(W * 0.78, 4, "NOTA FISCAL DE SERVIÇOS ELETRÔNICA - NFSe", align="C")
    pdf.set_xy(ML, y + 6)
    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(W * 0.78, 4, "Prefeitura Municipal de Eusébio", align="C")
    pdf.set_xy(ML, y + 10)
    pdf.set_font("Helvetica", "", 6.5)
    pdf.cell(W * 0.78, 3, _l1(f"Codigo de Verificação para Autenticação: {d['cod_verif']}"), align="C")
    pdf.set_xy(ML + 19, y + 14)
    pdf.set_font("Helvetica", "", 5.5)
    pdf.multi_cell(W * 0.78 - 21, 2.4, _l1("Endereço: Eusébio, Ceará, CE, 61764-010\n"
                                           "CNPJ: 23.563.067/0001-30, E-mail:"))
    # número grande
    nx = ML + W * 0.78
    pdf.rect(nx, y, W * 0.22, cabH)
    qr = _qr(d.get("chave", ""))
    if qr:
        pdf.image(qr, x=nx + (W * 0.22 - 13) / 2, y=y + 1, w=13)
    pdf.set_xy(nx, y + 14)
    pdf.set_font("Helvetica", "", 5)
    pdf.cell(W * 0.22, 2.5, _l1(f"Emitido em {d.get('data_emissao_hora', d['data_emissao'])}"), align="C")
    pdf.set_xy(nx, y + 16.5)
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(W * 0.22, 4, d["numero"], align="C")
    y += cabH

    # ---- linha de identificação ----
    h = 7
    c = [W * 0.18, W * 0.18, W * 0.22, W * 0.14, W * 0.10, W * 0.18]
    campo(ML, y, c[0], h, "Data Fato Gerador", d["data_emissao"], vs=6.5)
    campo(ML + c[0], y, c[1], h, "Exigibilidade de ISS", d["exig_iss"], vs=6.5)
    campo(ML + sum(c[:2]), y, c[2], h, "Regime Tributário", "Tributacao Normal", vs=6.5)
    campo(ML + sum(c[:3]), y, c[3], h, "Número RPS", d["rps_num"], vs=6.5)
    campo(ML + sum(c[:4]), y, c[4], h, "Série RPS", d["rps_serie"], vs=6.5)
    campo(ML + sum(c[:5]), y, c[5], h, "Nº da Nota Fiscal", d["numero"], bold=True, center=True)
    y += h
    c = [W * 0.18, W * 0.18, W * 0.32, W * 0.32]
    campo(ML, y, c[0], h, "Tipo de Recolhimento", d["iss_retido"], vs=6)
    campo(ML + c[0], y, c[1], h, "Simples", d["optante"], vs=6.5)
    campo(ML + sum(c[:2]), y, c[2], h, "Local de Prestação", d["local_prest"], vs=6.5)
    campo(ML + sum(c[:3]), y, c[3], h, "Local de Recolhimento", d["local_prest"], vs=6.5)
    y += h

    # ---- PRESTADOR ----
    y = secao("PRESTADOR", y)
    hP = 16
    pdf.rect(ML, y, W, hP)
    yy = bloco("Razão Social: ", d["emit_razao"], y + 1)
    yy = bloco("Endereço: ", f"{d['emit_end']}  -  {d['emit_mun']}  -  CEP: {d['emit_cep']}", yy)
    yy = bloco("E-mail: ", f"{EMIT_EMAIL}   -   Fone: {EMIT_FONE}", yy)
    yy = bloco("Inscrição Municipal: ", f"{d['emit_im']}    -    CPF/CNPJ: {d['emit_cnpj']}", yy)
    y += hP

    # ---- TOMADOR ----
    y = secao("TOMADOR", y)
    hT = 13
    pdf.rect(ML, y, W, hT)
    yy = bloco("Razão Social: ", d["toma_razao"], y + 1)
    yy = bloco("Endereço: ", f"{d['toma_end']}  -  {d['toma_mun']}  -  CEP: {d['toma_cep']}", yy)
    yy = bloco("CPF/CNPJ: ", d["toma_cnpj"], yy)
    y += hT

    # caixa de texto com altura dinâmica (cresce com o conteúdo, respeitando um mínimo)
    def caixa(texto, yy, min_h, fs=6.5, lh=2.7, bold=False):
        pdf.set_xy(ML + 1, yy + 1)
        pdf.set_font("Helvetica", "B" if bold else "", fs)
        pdf.multi_cell(W - 2, lh, _l1(texto))
        h = max(min_h, (pdf.get_y() - yy) + 1.5)
        pdf.rect(ML, yy, W, h)
        return yy + h

    # ---- SERVIÇO NACIONAL ----
    y = secao("SERVIÇO NACIONAL", y)
    y = caixa(f"{d['cod_nac']} - {d['txt_nac']}", y, 10, fs=6, lh=2.6)

    # ---- SERVIÇO (municipal) ----
    y = secao("SERVIÇO", y)
    y = caixa(f"{d['cod_mun']} - {d['txt_mun']}", y, 5, fs=6, lh=2.6, bold=True)

    # ---- DISCRIMINAÇÃO ----
    y = secao("DISCRIMINAÇÃO DOS SERVIÇOS", y)
    y = caixa(_requebrar_discriminacao(d["discriminacao"]), y, 22, fs=6.5, lh=2.7)

    # ---- OBSERVAÇÃO ----
    y = secao("OBSERVAÇÃO", y)
    hObs = 12
    pdf.rect(ML, y, W, hObs)
    y += hObs

    # ---- valores: linha 1 ----
    h = 8
    c6 = [W * 0.20, W * 0.14, W * 0.18, W * 0.20, W * 0.12, W * 0.16]
    campo(ML, y, c6[0], h, "VALOR SERVIÇO (R$)", d["vServ"])
    campo(ML + c6[0], y, c6[1], h, "DEDUÇÕES (R$)", d["vDed"])
    campo(ML + sum(c6[:2]), y, c6[2], h, "DESC. INCONDICIONAL (R$)", d["vDescIncond"], vs=6.5)
    campo(ML + sum(c6[:3]), y, c6[3], h, "BASE CÁLCULO (R$)", d["vBC"])
    campo(ML + sum(c6[:4]), y, c6[4], h, "ALÍQUOTA (%)", d["aliq"])
    campo(ML + sum(c6[:5]), y, c6[5], h, "ISS (R$)", d["vISS"], bold=True)
    y += h
    # valores: retenções federais + líquido
    c7 = [W * 0.12, W * 0.12, W * 0.12, W * 0.12, W * 0.12, W * 0.14, W * 0.26]
    campo(ML, y, c7[0], h, "INSS (R$)", d["vINSS"])
    campo(ML + sum(c7[:1]), y, c7[1], h, "IR (R$)", d["vIR"])
    campo(ML + sum(c7[:2]), y, c7[2], h, "CSLL (R$)", d["vCSLL"])
    campo(ML + sum(c7[:3]), y, c7[3], h, "COFINS (R$)", d["vCOFINS"])
    campo(ML + sum(c7[:4]), y, c7[4], h, "PIS (R$)", d["vPIS"])
    campo(ML + sum(c7[:5]), y, c7[5], h, "OUTRAS RET. (R$)", d["vOutras"], vs=6.5)
    campo(ML + sum(c7[:6]), y, c7[6], h, "VALOR LÍQUIDO (R$)", d["vLiq"], bold=True, vs=9)
    y += h

    # ---- OUTRAS INFORMAÇÕES ----
    y = secao("OUTRAS INFORMAÇÕES", y)
    hO = 10
    pdf.rect(ML, y, W, hO)
    pdf.set_xy(ML + 1, y + 1)
    pdf.set_font("Helvetica", "", 6)
    chave_txt = f"Chave de acesso Ambiente de Dados Nacional: {d.get('chave','')}" if d.get("chave") else \
                "Chave de acesso Ambiente de Dados Nacional: (em processamento)"
    pdf.multi_cell(W - 2, 2.6, _l1(chave_txt + "\n(Valor Líquido = Valor Serviço - INSS - IR - CSLL - "
                                   "Outras Retenções - COFINS - PIS - Descontos - ISS Retido - Desconto Incondicional)"))
    y += hO

    pdf.output(saida_pdf)
    if qr and os.path.exists(qr):
        try:
            os.remove(qr)
        except Exception:
            pass
    return saida_pdf
