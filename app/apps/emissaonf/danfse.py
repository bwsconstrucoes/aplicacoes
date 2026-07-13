# -*- coding: utf-8 -*-
"""
Gera a DANFSe (Documento Auxiliar da NFS-e) em PDF a partir do XML nacional
(padrão `sped.fazenda.gov.br/nfse`, v1.00). Sem navegador — fpdf2 + qrcode.
Pronto para Render.

Uso:
    from danfse import gerar_danfse_pdf
    gerar_danfse_pdf(xml_nacional_str, "danfse_3072.pdf")
"""
from __future__ import annotations
import os
import re
import tempfile
import xml.etree.ElementTree as ET

NS = "{http://www.sped.fazenda.gov.br/nfse}"
QR_BASE = "https://www.nfse.gov.br/consultapublica/?tpc=1&chave="   # confirmar URL do QR

# Logradouro do prestador forçado: corrige o "Rua MARANHAO" que vem errado do
# cadastro da prefeitura. Coloque None quando o cadastro da IM 101084492 for
# corrigido (aí volta a refletir exatamente o que está no XML).
LOGRADOURO_PRESTADOR_FORCADO = "Rua Luís Moreira Gomes"
_AQUI = os.path.dirname(os.path.abspath(__file__))
CACHE_MUN = os.path.join(_AQUI, "municipios_ibge.json")

_SUBST = {"–": "-", "—": "-", "“": '"', "”": '"', "‘": "'", "’": "'", "\u00a0": " "}


def _requebrar_discriminacao(texto) -> str:
    """Reinsere as quebras de linha na discriminação quando o texto vem 'corrido'
    (o sistema nacional / xDescServ colapsa os \\n num parágrafo só). Quebra antes
    dos marcadores conhecidos, deixando a apresentação igual à 1ª nota municipal."""
    if not texto:
        return texto
    t = " ".join(str(texto).split())          # tudo numa linha, espaços normalizados
    marc = (r"PER[IÍ]ODO DA OBRA:", r"Valor da Nota:",
            r"Base de C[aá]lculo", r"Conta p/ Pagamento:")
    for m in marc:
        t = re.sub(r"\s*(" + m + r")", r"\n\1", t)
    # linhas de valor de imposto: IR/PIS/COFINS/CSLL/INSS/ISS seguidos de "(dígito"
    t = re.sub(r"\s*((?:IR|PIS|COFINS|CSLL|INSS|ISS) \(\d)", r"\n\1", t)
    return t.strip()


def _l1(t) -> str:
    s = str(t if t is not None else "")
    for a, b in _SUBST.items():
        s = s.replace(a, b)
    return s.encode("latin-1", "replace").decode("latin-1")


def _brl(v) -> str:
    try:
        n = float(v)
    except Exception:
        return "-"
    return f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _cnpj(v) -> str:
    d = "".join(c for c in str(v or "") if c.isdigit())
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    if len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    return v or "-"


def _cep(v) -> str:
    d = "".join(c for c in str(v or "") if c.isdigit())
    return f"{d[:5]}-{d[5:]}" if len(d) == 8 else (v or "-")


def _data(iso) -> str:
    if not iso:
        return "-"
    iso = iso[:10]
    try:
        a, m, d = iso.split("-")
        return f"{d}/{m}/{a}"
    except Exception:
        return iso


def _datahora(iso) -> str:
    if not iso:
        return "-"
    try:
        d = _data(iso)
        h = iso[11:19] if len(iso) >= 19 else ""
        return f"{d} {h}".strip()
    except Exception:
        return iso


def _trib_fmt(c) -> str:
    d = "".join(ch for ch in str(c or "") if ch.isdigit())
    return f"{d[:2]}.{d[2:4]}.{d[4:]}" if len(d) == 6 else (c or "-")


_REVMUN = None


def _mun(cod, uf_hint=""):
    """Resolve código IBGE -> 'Nome - UF' usando o cache; senão devolve o código."""
    global _REVMUN
    cod = str(cod or "").strip()
    if not cod:
        return "-"
    if _REVMUN is None:
        _REVMUN = {}
        try:
            import json
            with open(CACHE_MUN, encoding="utf-8") as fh:
                for chave, c in json.load(fh).items():
                    nome, uf = chave.rsplit("|", 1)
                    _REVMUN[str(c)] = f"{nome.title()} - {uf}"
        except Exception:
            _REVMUN = {}
    return _REVMUN.get(cod, f"{cod}{(' - ' + uf_hint) if uf_hint else ''}")


def parse_nfse_nacional(xml: str) -> dict:
    root = ET.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
    inf = root.find(NS + "infNFSe")

    def t(base, *tags):
        el = base
        for tg in tags:
            if el is None:
                return ""
            el = el.find(NS + tg)
        return (el.text or "").strip() if el is not None else ""

    emit = inf.find(NS + "emit")
    valN = inf.find(NS + "valores")
    dps = inf.find(NS + "DPS").find(NS + "infDPS")
    toma = dps.find(NS + "toma")
    serv = dps.find(NS + "serv")
    valD = dps.find(NS + "valores")
    trib = valD.find(NS + "trib") if valD is not None else None
    tribFed = trib.find(NS + "tribFed") if trib is not None else None
    tribMun = trib.find(NS + "tribMun") if trib is not None else None

    emit_end = emit.find(NS + "enderNac")
    toma_end = toma.find(NS + "end") if toma is not None else None
    toma_endnac = toma_end.find(NS + "endNac") if toma_end is not None else None

    op = t(dps.find(NS + "prest"), "regTrib", "opSimpNac")
    simples = {"1": "Não optante", "2": "Optante - MEI", "3": "Optante - ME/EPP"}.get(op, "-")

    return {
        "chave": (inf.get("Id") or "").replace("NFS", ""),
        "nNFSe": t(inf, "nNFSe"),
        "dCompet": _data(t(dps, "dCompet")),
        "dhProc": _datahora(t(inf, "dhProc")),
        "nDPS": t(dps, "nDPS"),
        "serieDPS": str(int(t(dps, "serie") or "0")) if t(dps, "serie") else "-",
        "dhEmiDPS": _datahora(t(dps, "dhEmi")),
        # emitente
        "emit_cnpj": _cnpj(t(emit, "CNPJ")),
        "emit_im": t(emit, "IM") or "-",
        "emit_nome": t(emit, "xNome"),
        "emit_end": f"{LOGRADOURO_PRESTADOR_FORCADO or t(emit_end,'xLgr')}, {t(emit_end,'nro')}, {t(emit_end,'xBairro')}",
        "emit_mun": (f"{t(inf,'xLocEmi')} - {t(emit_end,'UF')}" if t(inf, "xLocEmi")
                     else _mun(t(emit_end, "cMun"), t(emit_end, "UF"))),
        "emit_cep": _cep(t(emit_end, "CEP")),
        "emit_simples": simples,
        # tomador
        "toma_cnpj": _cnpj(t(toma, "CNPJ")),
        "toma_nome": t(toma, "xNome"),
        "toma_end": f"{t(toma_end,'xLgr')}, {t(toma_end,'nro')}, {t(toma_end,'xBairro')}",
        "toma_mun": _mun(t(toma_endnac, "cMun")),
        "toma_cep": _cep(t(toma_endnac, "CEP")),
        # serviço
        "cTribNac": _trib_fmt(t(serv, "cServ", "cTribNac")),
        "xTribNac": t(inf, "xTribNac"),
        "cTribMun": t(serv, "cServ", "cIntContrib") or "-",
        "xTribMun": t(inf, "xTribMun"),
        "locPrest": t(inf, "xLocPrestacao"),
        "desc": t(serv, "cServ", "xDescServ"),
        "cObra": t(serv, "obra", "cObra") or "-",
        # tributação municipal
        "issqn_trib": "Operação Tributável" if t(tribMun, "tribISSQN") == "1" else "-",
        "mun_incid": t(inf, "xLocIncid"),
        "ret_issqn": "Retido pelo Tomador" if t(tribMun, "tpRetISSQN") == "2" else "Não retido",
        "vBC": _brl(t(valN, "vBC")),
        "pAliq": _brl(t(valN, "pAliqAplic")),
        "vISSQN": _brl(t(valN, "vISSQN")),
        # tributação federal
        "vIRRF": _brl(t(tribFed, "vRetIRRF")) if t(tribFed, "vRetIRRF") else "-",
        "vINSS": _brl(t(tribFed, "vRetCP")) if t(tribFed, "vRetCP") else "-",
        "vCSLL": _brl(t(tribFed, "vRetCSLL")) if t(tribFed, "vRetCSLL") else "-",
        "vPIS": _brl(t(tribFed, "vRetPIS")) if t(tribFed, "vRetPIS") else "-",
        "vCOFINS": _brl(t(tribFed, "vRetCofins")) if t(tribFed, "vRetCofins") else "-",
        # totais
        "vServ": _brl(t(valD, "vServPrest", "vServ")),
        "vLiq": _brl(t(valN, "vLiq")),
        "vTotRetFed": _brl(sum(float(t(tribFed, k) or 0) for k in
                              ("vRetCP", "vRetIRRF", "vRetCSLL", "vRetPIS", "vRetCofins"))) if tribFed is not None else "-",
    }


def _qr_png(chave: str) -> str | None:
    try:
        import qrcode
        img = qrcode.make(QR_BASE + chave)
        f = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        img.save(f.name)
        return f.name
    except Exception:
        return None


LOGO_NFSE = os.path.join(_AQUI, "danfse_logo_nfse.png")
BRASAO_EUSEBIO = os.path.join(_AQUI, "danfse_brasao_eusebio.png")


def gerar_danfse_pdf(xml: str, saida_pdf: str) -> str:
    """Gera a DANFSe nacional (layout v1.0) fiel ao oficial."""
    from fpdf import FPDF
    d = parse_nfse_nacional(xml)
    pdf = FPDF(format="A4")
    pdf.set_auto_page_break(False)
    pdf.add_page()
    ML, MR, MT = 6, 6, 7
    W = pdf.w - ML - MR
    X = [ML, ML + W * 0.32, ML + W * 0.57, ML + W * 0.80]   # 4 colunas
    pdf.set_draw_color(120, 120, 120)
    pdf.set_line_width(0.2)

    def sep(y):
        pdf.line(ML, y, ML + W, y)

    def rv(x, y, rot, val, bold=False, vs=8, w=70, vsizew=None):
        pdf.set_text_color(95, 95, 95)
        pdf.set_font("Helvetica", "", 5.5)
        pdf.set_xy(x, y)
        pdf.cell(w, 2.2, _l1(rot))
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B" if bold else "", vs)
        pdf.set_xy(x, y + 2.4)
        pdf.cell(vsizew or w, 3.0, _l1(val if val not in (None, "") else "-"))

    def secbar(titulo, y, h=4.6):
        sep(y)
        sep(y + h)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B", 7.5)
        pdf.set_xy(ML + 1, y + 0.7)
        pdf.cell(W - 2, h - 1.2, _l1(titulo))
        return y + h

    y = MT
    # ===== cabeçalho =====
    if os.path.exists(LOGO_NFSE):
        pdf.image(LOGO_NFSE, x=ML, y=y + 2, w=40)
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_xy(ML + W * 0.30, y + 2)
    pdf.cell(W * 0.30, 5, "DANFSe v1.0", align="C")
    pdf.set_font("Helvetica", "", 8.5)
    pdf.set_xy(ML + W * 0.30, y + 7.5)
    pdf.cell(W * 0.30, 4, _l1("Documento Auxiliar da NFS-e"), align="C")
    if os.path.exists(BRASAO_EUSEBIO):
        pdf.image(BRASAO_EUSEBIO, x=ML + W * 0.63, y=y, w=11)
    pdf.set_xy(ML + W * 0.69, y + 0.5)
    pdf.set_font("Helvetica", "B", 6)
    pdf.multi_cell(W * 0.18, 2.3, _l1("MUNICÍPIO DE EUSÉBIO"))
    pdf.set_xy(ML + W * 0.69, y + 3)
    pdf.set_font("Helvetica", "", 5.5)
    pdf.multi_cell(W * 0.18, 2.1, _l1("SECRETARIA DE FINANÇAS E PLANEJAMENTO\n(85)3260-1596\niss@eusebio.ce.gov.br"))
    qr = _qr_png(d["chave"])
    if qr:
        pdf.image(qr, x=ML + W - 17, y=y, w=16)
    pdf.set_xy(ML + W - 30, y + 16.5)
    pdf.set_font("Helvetica", "", 4.6)
    pdf.multi_cell(30, 1.8, _l1("A autenticidade desta NFS-e pode ser verificada pela leitura "
                                "deste código QR ou pela consulta da chave de acesso no portal "
                                "nacional da NFS-e"), align="C")
    y += 22
    sep(y)

    # ===== chave de acesso =====
    y += 1
    rv(ML, y, "Chave de Acesso da NFS-e", d["chave"], vs=8.5, w=W)
    y += 6.5
    sep(y)

    # ===== números NFS-e / DPS =====
    y += 1
    rv(X[0], y, "Número da NFS-e", d["nNFSe"], bold=True)
    rv(X[1], y, "Competência da NFS-e", d["dCompet"])
    rv(X[2], y, "Data e Hora da emissão da NFS-e", d["dhProc"], w=90)
    y += 7
    rv(X[0], y, "Número da DPS", d["nDPS"])
    rv(X[1], y, "Série da DPS", d["serieDPS"])
    rv(X[2], y, "Data e Hora da emissão da DPS", d["dhEmiDPS"], w=90)
    y += 7
    sep(y)

    # ===== EMITENTE =====
    y += 1
    pdf.set_font("Helvetica", "B", 7.5)
    pdf.set_xy(X[0], y)
    pdf.cell(W * 0.30, 3, _l1("EMITENTE DA NFS-e"))
    rv(X[0], y + 3.2, "Prestador do Serviço", "")
    rv(X[1], y + 1, "CNPJ / CPF / NIF", d["emit_cnpj"])
    rv(X[2], y + 1, "Inscrição Municipal", d["emit_im"])
    rv(X[3], y + 1, "Telefone", "-")
    y += 8.5
    rv(X[0], y, "Nome / Nome Empresarial", d["emit_nome"], w=W * 0.55)
    rv(X[2], y, "E-mail", "-", w=W * 0.40)
    y += 7
    rv(X[0], y, "Endereço", d["emit_end"], w=W * 0.55)
    rv(X[2], y, "Município", d["emit_mun"])
    rv(X[3], y, "CEP", d["emit_cep"])
    y += 7
    rv(X[0], y, "Simples Nacional na Data de Competência", d["emit_simples"], w=W * 0.55)
    rv(X[2], y, "Regime de Apuração Tributária pelo SN", "-", w=W * 0.43)
    y += 7
    sep(y)

    # ===== TOMADOR =====
    y += 1
    pdf.set_font("Helvetica", "B", 7.5)
    pdf.set_xy(X[0], y)
    pdf.cell(W * 0.30, 3, _l1("TOMADOR DO SERVIÇO"))
    rv(X[1], y + 1, "CNPJ / CPF / NIF", d["toma_cnpj"])
    rv(X[2], y + 1, "Inscrição Municipal", "-")
    rv(X[3], y + 1, "Telefone", "-")
    y += 8.5
    rv(X[0], y, "Nome / Nome Empresarial", d["toma_nome"], w=W * 0.55)
    rv(X[2], y, "E-mail", "-", w=W * 0.40)
    y += 7
    rv(X[0], y, "Endereço", d["toma_end"], w=W * 0.55)
    rv(X[2], y, "Município", d["toma_mun"])
    rv(X[3], y, "CEP", d["toma_cep"])
    y += 7
    sep(y)

    # ===== intermediário =====
    y += 0.5
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_xy(ML, y)
    pdf.cell(W, 3, _l1("INTERMEDIÁRIO DO SERVIÇO NÃO IDENTIFICADO NA NFS-e"), align="C")
    y += 3.5
    sep(y)

    # ===== SERVIÇO PRESTADO =====
    y = secbar("SERVIÇO PRESTADO", y) + 1

    def rv_wrap(x, yy, rot, val, colw, max_chars, vs=6.5):
        pdf.set_text_color(95, 95, 95)
        pdf.set_font("Helvetica", "", 5.5)
        pdf.set_xy(x, yy)
        pdf.cell(colw, 2.2, _l1(rot))
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", vs)
        txt = _l1(val)
        if len(txt) > max_chars:
            txt = txt[:max_chars].rstrip() + "..."
        pdf.set_xy(x, yy + 2.4)
        pdf.multi_cell(colw, 2.5, txt)

    rv_wrap(X[0], y, "Código de Tributação Nacional", f"{d['cTribNac']} - {d['xTribNac']}", W * 0.30 - 2, 95)
    rv_wrap(X[1], y, "Código de Tributação Municipal", f"- {d['xTribMun']}", W * 0.23 - 2, 70)
    rv(X[2], y, "Local da Prestação", d["locPrest"], vs=6.5)
    rv(X[3], y, "País da Prestação", "-", vs=6.5)
    y += 11
    sep(y)
    y += 0.5
    rv(X[0], y, "Descrição do Serviço", "", w=W)
    pdf.set_xy(ML, y + 2.4)
    pdf.set_font("Helvetica", "", 6.5)
    pdf.multi_cell(W, 2.7, _l1(_requebrar_discriminacao(d["desc"])))
    # altura dinâmica: usa o quanto o texto realmente ocupou (mín. 15mm, como antes),
    # senão a discriminação com várias linhas invade a seção seguinte.
    y = max(y + 15, pdf.get_y() + 1.5)
    sep(y)

    # ===== TRIBUTAÇÃO MUNICIPAL =====
    y = secbar("TRIBUTAÇÃO MUNICIPAL", y) + 1
    rv(X[0], y, "Tributação do ISSQN", d["issqn_trib"], vs=7)
    rv(X[1], y, "País Result. da Prestação", "-", vs=7)
    rv(X[2], y, "Município de Incidência do ISSQN", d["mun_incid"], vs=7, w=90)
    rv(X[3], y, "Regime Especial de Tributação", "Nenhum", vs=7, w=90)
    y += 7
    rv(X[0], y, "Tipo de Imunidade", "-", vs=7)
    rv(X[1], y, "Suspensão da Exigibilidade do ISSQN", "Não", vs=7, w=90)
    rv(X[2], y, "Número Processo Suspensão", "-", vs=7)
    rv(X[3], y, "Benefício Municipal", "-", vs=7)
    y += 7
    rv(X[0], y, "Valor do Serviço", "R$ " + d["vServ"])
    rv(X[1], y, "Desconto Incondicionado", "-")
    rv(X[2], y, "Total Deduções/Reduções", "-")
    rv(X[3], y, "Cálculo do BM", "-")
    y += 7
    rv(X[0], y, "BC ISSQN", "R$ " + d["vBC"])
    rv(X[1], y, "Alíquota Aplicada", d["pAliq"] + "%")
    rv(X[2], y, "Retenção do ISSQN", d["ret_issqn"], vs=7)
    rv(X[3], y, "ISSQN Apurado", "R$ " + d["vISSQN"], bold=True)
    y += 7
    sep(y)

    # ===== TRIBUTAÇÃO FEDERAL =====
    y = secbar("TRIBUTAÇÃO FEDERAL", y) + 1
    rv(X[0], y, "IRRF", ("R$ " + d["vIRRF"]) if d["vIRRF"] != "-" else "-")
    rv(X[1], y, "Contribuição Previdenciária - Retida", ("R$ " + d["vINSS"]) if d["vINSS"] != "-" else "-", vs=7, w=90)
    rv(X[2], y, "Contribuições Sociais - Retidas", "-", vs=7, w=90)
    rv(X[3], y, "Descrição Contrib. Sociais - Retidas", "-", vs=7, w=90)
    y += 7
    rv(X[0], y, "PIS - Débito Apuração Própria", "-", vs=7, w=90)
    rv(X[1], y, "COFINS - Débito Apuração Própria", "-", vs=7, w=90)
    y += 7
    sep(y)

    # ===== VALOR TOTAL =====
    y = secbar("VALOR TOTAL DA NFS-E", y) + 1
    rv(X[0], y, "Valor do Serviço", "R$ " + d["vServ"])
    rv(X[1], y, "Desconto Condicionado", "-")
    rv(X[2], y, "Desconto Incondicionado", "-")
    rv(X[3], y, "ISSQN Retido", "R$ " + d["vISSQN"])
    y += 7
    rv(X[0], y, "Total das Retenções Federais", "R$ " + d["vTotRetFed"], w=90)
    rv(X[1], y, "PIS/COFINS - Débito Apur. Própria", "-", w=90)
    rv(X[3], y, "Valor Líquido da NFS-e", "R$ " + d["vLiq"], bold=True, vs=9)
    y += 7
    sep(y)

    # ===== TOTAIS APROXIMADOS =====
    y = secbar("TOTAIS APROXIMADOS DOS TRIBUTOS", y) + 1
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(95, 95, 95)
    for i, lab in enumerate(("Federais", "Estaduais", "Municipais")):
        pdf.set_xy(ML + W * i / 3, y)
        pdf.cell(W / 3, 2.4, lab, align="C")
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 7)
    for i in range(3):
        pdf.set_xy(ML + W * i / 3, y + 2.4)
        pdf.cell(W / 3, 3, "-", align="C")
    y += 6.5
    sep(y)

    # ===== INFORMAÇÕES COMPLEMENTARES =====
    y = secbar("INFORMAÇÕES COMPLEMENTARES", y) + 1
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_xy(ML, y)
    pdf.cell(W, 3, _l1(f"Cod Obra: {d['cObra']}"))
    y += 5

    pdf.output(saida_pdf)
    if qr and os.path.exists(qr):
        try:
            os.remove(qr)
        except Exception:
            pass
    return saida_pdf
